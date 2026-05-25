// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache Software License 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0, or the MIT license
// which is available at https://opensource.org/licenses/MIT.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Wide-entry sidetable types backing the publish-subscribe forwarding
//! completion-queue protocol.
//!
//! The completion queue's wire format is **not changed** by forwarding —
//! it continues to carry bare [`PointerOffset`](crate::shared_memory::PointerOffset)
//! values. When (and only when) a per-(publisher, subscriber) connection
//! is established on a service that has declared a non-empty
//! `forwards_into`, an additional [`WideEntrySidetable`] is allocated in
//! shared memory alongside the completion queue. The subscriber writes
//! a [`CompletionEntry`] into `sidetable[next_index % capacity]` before
//! pushing the offset onto the queue; the publisher reads the matching
//! [`CompletionEntry`] from the sidetable after popping. The queue's
//! existing SPSC discipline provides both mutual exclusion (subscriber
//! and publisher always work on different slots) and cross-process
//! visibility (the queue's release/acquire on its tail counter orders
//! the sidetable write happens-before the corresponding read).
//!
//! For native-only services (`forwards_into` empty) the sidetable is
//! never allocated and these types are unused.
//!
//! See `doc/design-documents/publish-subscribe-forwarding.md` for the
//! end-to-end protocol.

use core::{alloc::Layout, fmt::Debug};

use iceoryx2_bb_concurrency::atomic::{AtomicBool, Ordering};
use iceoryx2_bb_concurrency::cell::UnsafeCell;
use iceoryx2_bb_derive_macros::ZeroCopySend;
use iceoryx2_bb_elementary::math::unaligned_mem_size;
use iceoryx2_bb_elementary::relocatable_ptr::RelocatablePointer;
use iceoryx2_bb_elementary_traits::{
    owning_pointer::OwningPointer, pointer_trait::PointerTrait,
    relocatable_container::RelocatableContainer, zero_copy_send::ZeroCopySend,
};
use iceoryx2_log::{fail, fatal_panic};

/// Discriminator for a [`CompletionEntry`] — encodes whether the entry
/// represents an ordinary release (`Drop`), a forward to a target
/// service without releasing the subscriber's own borrow (`Forward`), or
/// the fused "drop and forward" operation (`DropAndForward`).
#[repr(C)]
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Hash, ZeroCopySend)]
pub enum CompletionEntryTag {
    /// The subscriber is releasing its borrow on the bucket. The
    /// publisher decrements the refcount; if it drops to zero, the
    /// bucket is reclaimed. This is the only variant ever emitted on
    /// native-only services (which is why the sidetable isn't needed at
    /// all there — the bare offset on the queue is sufficient).
    #[default]
    Drop,
    /// The subscriber wants the bucket forwarded onto the target
    /// identified by `target_index` (an index into the source service's
    /// declared `forwards_into` list) **without** releasing its own
    /// borrow. The publisher consults the per-bucket forwarding-history
    /// bitmap (R10) and, if this is the first forward to that target,
    /// fans out the offset onto the target service's subscribers.
    Forward,
    /// Fused drop-and-forward: release the subscriber's borrow *and*
    /// forward to `target_index`. Refcount math is `+K_T - 1` on first
    /// dispatch, `-1` on a deduplicated dispatch.
    DropAndForward,
}

/// A single entry that flows through the completion-queue sidetable on
/// forwarding-enabled connections. Plain `repr(C)` data — no atomics,
/// no synchronization primitives of its own. Visibility between
/// subscriber writes and publisher reads is provided by the completion
/// queue's existing release/acquire on its tail counter.
#[repr(C)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, ZeroCopySend)]
pub struct CompletionEntry {
    /// Which kind of entry this is.
    pub tag: CompletionEntryTag,
    /// For [`CompletionEntryTag::Forward`] and
    /// [`CompletionEntryTag::DropAndForward`]: index into the source
    /// service's `forwards_into` list identifying the target service.
    /// Ignored for [`CompletionEntryTag::Drop`].
    pub target_index: u8,
    /// The [`PointerOffset`](crate::shared_memory::PointerOffset) value
    /// the subscriber is acting on. Same offset the publisher dispatched
    /// to the subscriber's submission queue; same offset the bare
    /// completion-queue u64 carries in parallel for direct decode.
    pub offset: u64,
}

impl Default for CompletionEntry {
    fn default() -> Self {
        Self {
            tag: CompletionEntryTag::Drop,
            target_index: 0,
            offset: 0,
        }
    }
}

/// Convenient owning alias for tests and process-local use.
pub type OwningWideEntrySidetable = details::WideEntrySidetable<OwningPointer<UnsafeCell<CompletionEntry>>>;

/// Convenient relocatable alias for SHM-backed sidetables.
pub type RelocatableWideEntrySidetable =
    details::WideEntrySidetable<RelocatablePointer<UnsafeCell<CompletionEntry>>>;

pub mod details {
    use super::*;

    /// Relocatable, SHM-compatible fixed-capacity array of
    /// [`CompletionEntry`] slots. Sits next to the completion queue in
    /// a forwarding-enabled connection's SHM region; allocated by the
    /// same [`BumpAllocator`](iceoryx2_bb_memory::bump_allocator::BumpAllocator)
    /// that backs the queue.
    ///
    /// This is **not a queue** — there are no head/tail counters, no
    /// atomics, no FIFO discipline. It is a plain ring of cells
    /// addressed by `position % capacity()` from both producer and
    /// consumer sides. The completion queue's existing SPSC ordering on
    /// its tail is what makes cross-process visibility of writes / reads
    /// consistent: see the design doc's "lifecycle queue protocol"
    /// section.
    #[repr(C)]
    #[derive(Debug)]
    pub struct WideEntrySidetable<PointerType: PointerTrait<UnsafeCell<CompletionEntry>>> {
        pub(super) data_ptr: PointerType,
        pub(super) capacity: usize,
        pub(super) is_memory_initialized: AtomicBool,
    }

    unsafe impl<PointerType: PointerTrait<UnsafeCell<CompletionEntry>>> Sync
        for WideEntrySidetable<PointerType>
    {
    }
    unsafe impl<PointerType: PointerTrait<UnsafeCell<CompletionEntry>>> Send
        for WideEntrySidetable<PointerType>
    {
    }

    impl WideEntrySidetable<OwningPointer<UnsafeCell<CompletionEntry>>> {
        /// Constructs a [`WideEntrySidetable`] backed by heap memory
        /// owned outright by this struct. Useful for tests and
        /// process-local exercises; SHM-backed sidetables go through
        /// `RelocatableContainer::new_uninit` + `init` against a bump
        /// allocator instead.
        pub fn new(capacity: usize) -> Self {
            let mut data_ptr =
                OwningPointer::<UnsafeCell<CompletionEntry>>::new_with_alloc(capacity);
            for i in 0..capacity {
                unsafe {
                    data_ptr
                        .as_mut_ptr()
                        .add(i)
                        .write(UnsafeCell::new(CompletionEntry::default()));
                }
            }
            Self {
                data_ptr,
                capacity,
                is_memory_initialized: AtomicBool::new(true),
            }
        }
    }

    impl RelocatableContainer
        for WideEntrySidetable<RelocatablePointer<UnsafeCell<CompletionEntry>>>
    {
        unsafe fn new_uninit(capacity: usize) -> Self {
            Self {
                data_ptr: unsafe { RelocatablePointer::new_uninit() },
                capacity,
                is_memory_initialized: AtomicBool::new(false),
            }
        }

        unsafe fn init<T: iceoryx2_bb_elementary_traits::allocator::BaseAllocator>(
            &mut self,
            allocator: &T,
        ) -> Result<(), iceoryx2_bb_elementary_traits::allocator::AllocationError> {
            if self.is_memory_initialized.load(Ordering::Relaxed) {
                fatal_panic!(from self,
                    "Memory already initialized. Initializing it twice may lead to undefined behavior.");
            }
            // Capacity 0 marks a "disabled" sidetable — used on native
            // connections where forwarding is not in play. There's no
            // backing memory to allocate; mark the sidetable as
            // initialized so subsequent `is_initialized()` checks pass,
            // but no slots exist. Read/write must not be called on such
            // a sidetable; callers gate by checking `capacity() > 0`.
            if self.capacity == 0 {
                self.is_memory_initialized.store(true, Ordering::Relaxed);
                return Ok(());
            }
            unsafe {
                self.data_ptr.init(fail!(from self, when allocator
                    .allocate(Layout::from_size_align_unchecked(
                        core::mem::size_of::<UnsafeCell<CompletionEntry>>() * self.capacity,
                        core::mem::align_of::<UnsafeCell<CompletionEntry>>())),
                    "Failed to initialize since the allocation of the data memory failed."));

                // Pre-fill all slots with default CompletionEntry values
                // so a slot read before it has been explicitly written
                // returns a well-defined value rather than uninitialized
                // memory.
                for i in 0..self.capacity {
                    (self.data_ptr.as_ptr() as *mut UnsafeCell<CompletionEntry>)
                        .add(i)
                        .write(UnsafeCell::new(CompletionEntry::default()));
                }
            }
            self.is_memory_initialized
                .store(true, Ordering::Relaxed);
            Ok(())
        }

        fn memory_size(capacity: usize) -> usize {
            Self::const_memory_size(capacity)
        }
    }

    impl<PointerType: PointerTrait<UnsafeCell<CompletionEntry>> + Debug>
        WideEntrySidetable<PointerType>
    {
        /// Returns the amount of memory required to back a
        /// [`WideEntrySidetable`] of the given capacity. Excludes the
        /// size of the [`WideEntrySidetable`] struct itself.
        ///
        /// Returns `0` for `capacity == 0` (disabled sidetable). Native
        /// pub/sub connections use this case to avoid paying any
        /// bump-allocated memory cost for forwarding infrastructure
        /// they don't use.
        pub const fn const_memory_size(capacity: usize) -> usize {
            if capacity == 0 {
                0
            } else {
                unaligned_mem_size::<UnsafeCell<CompletionEntry>>(capacity)
            }
        }

        /// Returns the maximum number of slots this sidetable can hold.
        /// Equal to the completion queue's capacity it sits next to.
        pub fn capacity(&self) -> usize {
            self.capacity
        }

        #[inline(always)]
        fn verify_init(&self, source: &str) {
            debug_assert!(
                self.is_memory_initialized.load(Ordering::Relaxed),
                "Undefined behavior when calling WideEntrySidetable::{source} and the object is not initialized."
            );
        }

        unsafe fn cell_at(&self, position: usize) -> &UnsafeCell<CompletionEntry> {
            unsafe { &*self.data_ptr.as_ptr().add(position % self.capacity) }
        }

        /// Writes `entry` into the slot at `position % capacity`.
        ///
        /// # Safety
        ///
        /// The caller must guarantee that no other party is concurrently
        /// reading or writing the slot at this position. In the
        /// publish-subscribe forwarding use case this is provided by
        /// the completion queue's SPSC discipline: the subscriber writes
        /// at `subscriber_next_index % capacity` only after observing
        /// that the publisher hasn't yet read past that position.
        pub unsafe fn write(&self, position: usize, entry: CompletionEntry) {
            self.verify_init("write()");
            unsafe { *self.cell_at(position).get() = entry };
        }

        /// Reads the [`CompletionEntry`] at `position % capacity`.
        ///
        /// # Safety
        ///
        /// The caller must guarantee that no other party is concurrently
        /// writing the slot at this position. See [`Self::write`] for
        /// the SPSC discipline that gives us this property in the
        /// forwarding protocol.
        pub unsafe fn read(&self, position: usize) -> CompletionEntry {
            self.verify_init("read()");
            unsafe { *self.cell_at(position).get() }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_through_owning_sidetable() {
        let table = OwningWideEntrySidetable::new(4);
        assert_eq!(table.capacity(), 4);

        let entry = CompletionEntry {
            tag: CompletionEntryTag::Forward,
            target_index: 3,
            offset: 0xDEADBEEF,
        };
        unsafe { table.write(0, entry) };
        let read_back = unsafe { table.read(0) };
        assert_eq!(read_back, entry);
    }

    #[test]
    fn write_at_position_wraps_modulo_capacity() {
        let table = OwningWideEntrySidetable::new(4);
        let entry_a = CompletionEntry {
            tag: CompletionEntryTag::DropAndForward,
            target_index: 1,
            offset: 100,
        };
        let entry_b = CompletionEntry {
            tag: CompletionEntryTag::Forward,
            target_index: 0,
            offset: 200,
        };

        unsafe {
            table.write(0, entry_a);
            table.write(4, entry_b); // wraps to slot 0
        };
        assert_eq!(unsafe { table.read(0) }, entry_b);
        assert_eq!(unsafe { table.read(4) }, entry_b); // same slot
    }

    #[test]
    fn default_completion_entry_is_drop() {
        let entry = CompletionEntry::default();
        assert_eq!(entry.tag, CompletionEntryTag::Drop);
    }

    #[test]
    fn slots_are_initialized_to_drop_on_construction() {
        let table = OwningWideEntrySidetable::new(8);
        for i in 0..8 {
            let entry = unsafe { table.read(i) };
            assert_eq!(entry.tag, CompletionEntryTag::Drop);
        }
    }

    #[test]
    fn writes_are_independent_per_slot() {
        let table = OwningWideEntrySidetable::new(4);
        for i in 0..4 {
            let entry = CompletionEntry {
                tag: CompletionEntryTag::Forward,
                target_index: i as u8,
                offset: (i as u64) * 1024,
            };
            unsafe { table.write(i, entry) };
        }
        for i in 0..4 {
            let entry = unsafe { table.read(i) };
            assert_eq!(entry.target_index, i as u8);
            assert_eq!(entry.offset, (i as u64) * 1024);
        }
    }

    #[test]
    fn const_memory_size_grows_with_capacity() {
        let small = OwningWideEntrySidetable::const_memory_size(8);
        let large = OwningWideEntrySidetable::const_memory_size(16);
        assert!(large > small);
    }
}
