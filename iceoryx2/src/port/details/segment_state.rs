// Copyright (c) 2025 Contributors to the Eclipse Foundation
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

use iceoryx2_bb_concurrency::atomic::Ordering;

use alloc::vec::Vec;

use iceoryx2_bb_concurrency::atomic::{AtomicU64, AtomicUsize};

/// Per-bucket state maintained by the publisher in process-local memory.
///
/// Holds the reference counter for each sample-sized bucket in the data
/// segment and, when the source service declares forwarding targets, a
/// per-bucket forwarding-history bitmap (one bit per declared target) used
/// by R10 to deduplicate `Forward` / `DropAndForward` dispatches to the
/// same target across the bucket's lifetime.
///
/// The bitmap is unused until [M3e](
/// `doc/design-documents/publish-subscribe-forwarding.md`) wires it into
/// the publisher's bookkeeping dispatch path. M3a only lands the storage
/// and the helper methods that the dispatch path will call.
///
/// All state in this struct is publisher-process-local — it lives in the
/// publisher's heap, not in shared memory, so a publisher crash discards
/// the per-bucket accounting along with the rest of the publisher's
/// process state. Subscribers never observe it directly.
#[derive(Debug)]
pub(crate) struct SegmentState {
    sample_reference_counter: Vec<AtomicU64>,
    forwarding_history: Vec<AtomicU64>,
    payload_size: AtomicUsize,
}

impl SegmentState {
    pub(crate) fn new(number_of_samples: usize) -> Self {
        let mut sample_reference_counter = Vec::with_capacity(number_of_samples);
        let mut forwarding_history = Vec::with_capacity(number_of_samples);
        for _ in 0..number_of_samples {
            sample_reference_counter.push(AtomicU64::new(0));
            forwarding_history.push(AtomicU64::new(0));
        }

        Self {
            sample_reference_counter,
            forwarding_history,
            payload_size: AtomicUsize::new(0),
        }
    }

    pub(crate) fn set_payload_size(&self, value: usize) {
        self.payload_size.store(value, Ordering::Relaxed);
    }

    pub(crate) fn payload_size(&self) -> usize {
        self.payload_size.load(Ordering::Relaxed)
    }

    pub(crate) fn sample_index(&self, distance_to_chunk: usize) -> usize {
        debug_assert!(distance_to_chunk % self.payload_size() == 0);
        distance_to_chunk / self.payload_size()
    }

    pub(crate) fn borrow_sample(&self, distance_to_chunk: usize) -> u64 {
        self.sample_reference_counter[self.sample_index(distance_to_chunk)]
            .fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn release_sample(&self, distance_to_chunk: usize) -> u64 {
        let index = self.sample_index(distance_to_chunk);
        let prev = self.sample_reference_counter[index].fetch_sub(1, Ordering::Relaxed);
        // When the bucket's refcount drops to zero (i.e., prev was 1), the
        // bucket is being reclaimed and will be reused by a future loan().
        // Reset its forwarding history so the next bucket lifecycle starts
        // with a clean R10 bitmap.
        if prev == 1 {
            self.forwarding_history[index].store(0, Ordering::Relaxed);
        }
        prev
    }

    /// Returns the per-bucket forwarding-history bitmap for the bucket at
    /// the given chunk distance. Each bit corresponds to a target index in
    /// the source service's declared `forwards_into` list. Used by M3e's
    /// dispatch path to decide whether a `Forward(T)` / `DropAndForward(T)`
    /// entry should result in a fanout (bit unset) or a no-op (bit set).
    ///
    /// Currently unused — the bitmap is allocated and reset here for M3a
    /// but is not yet read or written by any caller.
    #[allow(dead_code)]
    pub(crate) fn forwarding_history(&self, distance_to_chunk: usize) -> u64 {
        self.forwarding_history[self.sample_index(distance_to_chunk)].load(Ordering::Relaxed)
    }

    /// Tests whether the bit for `target_index` is set in the per-bucket
    /// forwarding-history bitmap, and if not, sets it.
    ///
    /// Returns `true` if this call set the bit (i.e., this is the first
    /// forward to that target across the bucket's lifetime), `false` if it
    /// was already set (a duplicate forward — caller should no-op the
    /// dispatch).
    ///
    /// Because the publisher's access to `SegmentState` is serialized by
    /// the type system (single-threaded service variants) or by a mutex
    /// (threadsafe variants), this load-test-store sequence is safe under
    /// `Relaxed` ordering without a CAS loop. The atomic operations are
    /// used as a convention; under the publisher's serialized access they
    /// compile to plain integer ops.
    #[allow(dead_code)]
    pub(crate) fn check_and_set_forwarding_bit(
        &self,
        distance_to_chunk: usize,
        target_index: u8,
    ) -> bool {
        debug_assert!((target_index as u32) < 64);
        let index = self.sample_index(distance_to_chunk);
        let mask = 1u64 << (target_index as u32);
        let previous = self.forwarding_history[index].load(Ordering::Relaxed);
        if previous & mask != 0 {
            return false;
        }
        self.forwarding_history[index].store(previous | mask, Ordering::Relaxed);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAYLOAD_SIZE: usize = 8;
    const NUMBER_OF_SAMPLES: usize = 4;

    fn make() -> SegmentState {
        let s = SegmentState::new(NUMBER_OF_SAMPLES);
        s.set_payload_size(PAYLOAD_SIZE);
        s
    }

    #[test]
    fn forwarding_history_is_zero_after_new() {
        let s = make();
        for i in 0..NUMBER_OF_SAMPLES {
            assert_eq!(s.forwarding_history(i * PAYLOAD_SIZE), 0);
        }
    }

    #[test]
    fn check_and_set_forwarding_bit_is_idempotent_after_first_set() {
        let s = make();
        let chunk = 0;
        assert!(s.check_and_set_forwarding_bit(chunk, 0));
        assert!(!s.check_and_set_forwarding_bit(chunk, 0));
        assert!(!s.check_and_set_forwarding_bit(chunk, 0));
    }

    #[test]
    fn check_and_set_forwarding_bit_is_independent_per_target() {
        let s = make();
        let chunk = 0;
        assert!(s.check_and_set_forwarding_bit(chunk, 0));
        assert!(s.check_and_set_forwarding_bit(chunk, 1));
        assert!(s.check_and_set_forwarding_bit(chunk, 7));
        assert!(!s.check_and_set_forwarding_bit(chunk, 0));
        assert!(!s.check_and_set_forwarding_bit(chunk, 1));
        assert!(!s.check_and_set_forwarding_bit(chunk, 7));
        // Bits 2..7 not yet set.
        for target in 2..7 {
            assert!(s.check_and_set_forwarding_bit(chunk, target as u8));
        }
    }

    #[test]
    fn check_and_set_forwarding_bit_is_independent_per_bucket() {
        let s = make();
        assert!(s.check_and_set_forwarding_bit(0, 3));
        assert!(s.check_and_set_forwarding_bit(PAYLOAD_SIZE, 3));
        assert!(s.check_and_set_forwarding_bit(2 * PAYLOAD_SIZE, 3));
        assert!(!s.check_and_set_forwarding_bit(0, 3));
    }

    #[test]
    fn forwarding_history_clears_when_bucket_refcount_returns_to_zero() {
        let s = make();
        let chunk = 0;
        // Simulate the bucket being loaned: refcount = 1.
        s.borrow_sample(chunk);
        // Mark some targets as forwarded-to during the bucket's life.
        assert!(s.check_and_set_forwarding_bit(chunk, 0));
        assert!(s.check_and_set_forwarding_bit(chunk, 5));
        assert_ne!(s.forwarding_history(chunk), 0);

        // Releasing brings the refcount to zero — the bucket is reclaimed
        // and its forwarding history must be cleared for the next reuse.
        let prev = s.release_sample(chunk);
        assert_eq!(prev, 1);
        assert_eq!(s.forwarding_history(chunk), 0);

        // After clearing, the bits can be re-set on the next loan.
        s.borrow_sample(chunk);
        assert!(s.check_and_set_forwarding_bit(chunk, 0));
        assert!(s.check_and_set_forwarding_bit(chunk, 5));
    }

    #[test]
    fn forwarding_history_does_not_clear_while_refcount_is_above_zero() {
        let s = make();
        let chunk = 0;
        s.borrow_sample(chunk); // refcount 1
        s.borrow_sample(chunk); // refcount 2
        assert!(s.check_and_set_forwarding_bit(chunk, 2));

        // First release drops refcount from 2 to 1 — not a reclaim yet.
        let prev = s.release_sample(chunk);
        assert_eq!(prev, 2);
        assert_ne!(s.forwarding_history(chunk), 0);
        // The bit must still be set so a subsequent forward to target 2
        // would correctly observe "already forwarded."
        assert!(!s.check_and_set_forwarding_bit(chunk, 2));
    }
}
