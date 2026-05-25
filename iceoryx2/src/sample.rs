// Copyright (c) 2023 Contributors to the Eclipse Foundation
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

//! # Example
//!
//! ```
//! use iceoryx2::prelude::*;
//! # fn main() -> Result<(), Box<dyn core::error::Error>> {
//! # let node = NodeBuilder::new().create::<ipc::Service>()?;
//! # let service = node.service_builder(&"My/Funk/ServiceName".try_into()?)
//! #   .publish_subscribe::<u64>()
//! #   .open_or_create()?;
//! # let subscriber = service.subscriber_builder().create()?;
//!
//! while let Some(sample) = subscriber.receive()? {
//!     println!("received: {:?}", *sample);
//!     println!("header publisher id {:?}", sample.header().publisher_id());
//! }
//!
//! # Ok(())
//! # }
//! ```

use core::{cell::Cell, fmt::Debug, ops::Deref};

use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;
use iceoryx2_bb_posix::unique_system_id::UniqueSystemId;
use iceoryx2_cal::arc_sync_policy::ArcSyncPolicy;
use iceoryx2_cal::zero_copy_connection::ChannelId;

use crate::identifiers::UniquePublisherId;
use crate::port::details::chunk_details::ChunkDetails;
use crate::port::subscriber::SubscriberSharedState;
use crate::raw_sample::RawSample;
use crate::service::header::publish_subscribe::Header;
use crate::service::service_name::ServiceName;

/// Errors produced by [`Sample::forward_to`] and
/// [`Sample::drop_and_forward_to`].
///
/// See `doc/design-documents/publish-subscribe-forwarding.md` for the
/// full semantics.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum ForwardError {
    /// The named target service is not present in the source service's
    /// declared `forwards_into` list — this forwarding edge was never
    /// authorized at service-creation time.
    TargetNotDeclared,
    /// This `Sample` handle has already been forwarded to the named
    /// target. R9 forbids forwarding the same Sample to the same target
    /// more than once.
    AlreadyForwarded,
    /// The publisher's completion queue (the channel through which the
    /// subscriber signals Forward / DropAndForward to the publisher) is
    /// full. Retry later — the publisher will eventually drain it.
    CompletionQueueFull,
    /// The connection back to the publisher is no longer valid (for
    /// instance, the publisher dropped). The forward request cannot be
    /// delivered.
    PublisherUnavailable,
}

impl core::fmt::Display for ForwardError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "ForwardError::{self:?}")
    }
}

impl core::error::Error for ForwardError {}

/// It stores the payload and is acquired by the [`Subscriber`](crate::port::subscriber::Subscriber) whenever
/// it receives new data from a [`Publisher`](crate::port::publisher::Publisher) via
/// [`Subscriber::receive()`](crate::port::subscriber::Subscriber::receive()).
pub struct Sample<
    Service: crate::service::Service,
    Payload: Debug + ?Sized + ZeroCopySend,
    UserHeader: ZeroCopySend,
> {
    pub(crate) ptr: RawSample<Header, UserHeader, Payload>,
    pub(crate) subscriber_shared_state:
        Service::ArcThreadSafetyPolicy<SubscriberSharedState<Service>>,
    pub(crate) details: ChunkDetails,
    /// R9 per-Sample-handle forwarding history. Bit `t` is set iff this
    /// particular `Sample` handle has already been forwarded to the
    /// target with index `t` in the source service's declared
    /// `forwards_into` list. Subscriber-process-local; never shared.
    ///
    /// Uses `Cell` for interior mutability so that `forward_to(&self)` can
    /// observe and update the bitmap. `Sample` is `Send` but not `Sync`,
    /// so single-threaded interior mutability is sound.
    pub(crate) forwarding_history: Cell<u64>,
}

unsafe impl<
    Service: crate::service::Service,
    Payload: Debug + ZeroCopySend + ?Sized,
    UserHeader: ZeroCopySend,
> Send for Sample<Service, Payload, UserHeader>
where
    Service::ArcThreadSafetyPolicy<SubscriberSharedState<Service>>: Send + Sync,
{
}

impl<
    Service: crate::service::Service,
    Payload: Debug + ZeroCopySend + ?Sized,
    UserHeader: ZeroCopySend,
> Debug for Sample<Service, Payload, UserHeader>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "Sample<{}, {}, {}> {{ ptr: {:?}, details: {:?} }}",
            core::any::type_name::<Payload>(),
            core::any::type_name::<UserHeader>(),
            core::any::type_name::<Service>(),
            self.ptr,
            self.details,
        )
    }
}

impl<
    Service: crate::service::Service,
    Payload: Debug + ZeroCopySend + ?Sized,
    UserHeader: ZeroCopySend,
> Deref for Sample<Service, Payload, UserHeader>
{
    type Target = Payload;
    fn deref(&self) -> &Self::Target {
        self.ptr.as_payload_ref()
    }
}

impl<
    Service: crate::service::Service,
    Payload: Debug + ZeroCopySend + ?Sized,
    UserHeader: ZeroCopySend,
> Drop for Sample<Service, Payload, UserHeader>
{
    fn drop(&mut self) {
        self.subscriber_shared_state
            .lock()
            .receiver
            .release_offset(&self.details, ChannelId::new(0));
    }
}

impl<
    Service: crate::service::Service,
    Payload: Debug + ZeroCopySend + ?Sized,
    UserHeader: ZeroCopySend,
> Sample<Service, Payload, UserHeader>
{
    /// Returns a reference to the payload of the [`Sample`]
    pub fn payload(&self) -> &Payload {
        self.ptr.as_payload_ref()
    }

    /// Returns a reference to the user_header of the [`Sample`]
    pub fn user_header(&self) -> &UserHeader {
        self.ptr.as_user_header_ref()
    }

    /// Returns a reference to the [`Header`] of the [`Sample`].
    pub fn header(&self) -> &Header {
        self.ptr.as_header_ref()
    }

    /// Returns the [`UniquePublisherId`] of the [`Publisher`](crate::port::publisher::Publisher)
    pub fn origin(&self) -> UniquePublisherId {
        UniquePublisherId(UniqueSystemId::from(self.details.origin))
    }

    /// Requests that this [`Sample`] be forwarded onto the publish-subscribe
    /// service identified by `target`, without releasing this subscriber's
    /// own borrow on the underlying bucket.
    ///
    /// `target` must appear in the source service's declared
    /// `forwards_into` list. The R9 invariant (at-most-once per Sample
    /// handle per target) is enforced via a subscriber-process-local
    /// bitmap on this handle: a second `forward_to` to the same target
    /// returns [`ForwardError::AlreadyForwarded`].
    ///
    /// On success, a `Forward(target_index)` entry is pushed onto the
    /// publisher's completion queue via the wide-entry sidetable. The
    /// publisher will, on its next sweep, consult the per-bucket
    /// forwarding-history bitmap (R10) and — if this is the first
    /// `Forward(target)` for the bucket — fan the offset out to each
    /// subscriber of the target service.
    pub fn forward_to(&self, target: &ServiceName) -> Result<(), ForwardError> {
        use iceoryx2_cal::zero_copy_connection::completion_entry::{
            CompletionEntry, CompletionEntryTag,
        };

        let target_index = self.resolve_target_index(target)?;
        let mask = 1u64 << target_index;
        let current = self.forwarding_history.get();
        if current & mask != 0 {
            return Err(ForwardError::AlreadyForwarded);
        }

        let entry = CompletionEntry {
            tag: CompletionEntryTag::Forward,
            target_index,
            offset: self.details.offset.as_value(),
        };
        let pushed = self.subscriber_shared_state.lock().receiver.
            release_offset_with_entry(
                &self.details,
                ChannelId::new(0),
                entry,
            );
        if !pushed {
            return Err(ForwardError::CompletionQueueFull);
        }
        // R9 bit is set only after successful push, so that a failed
        // push can be retried by the caller.
        self.forwarding_history.set(current | mask);
        Ok(())
    }

    /// Consumes this [`Sample`], releasing the subscriber's borrow on the
    /// underlying bucket *and* requesting that the bucket be forwarded to
    /// the named target service in a single fused operation.
    ///
    /// The drop portion runs unconditionally — the `Sample` is consumed
    /// even when the forward portion fails. The returned `Result` reports
    /// only whether the forward portion went through; in the failure
    /// case the Sample's normal `Drop` path runs (releasing as a plain
    /// `Drop` entry).
    ///
    /// On success, a single fused `DropAndForward(target_index)` entry is
    /// pushed onto the publisher's completion queue, and the Sample's
    /// natural `Drop` is suppressed (so the publisher sees one entry, not
    /// two).
    pub fn drop_and_forward_to(self, target: &ServiceName) -> Result<(), ForwardError> {
        use iceoryx2_cal::zero_copy_connection::completion_entry::{
            CompletionEntry, CompletionEntryTag,
        };

        let target_index = match self.resolve_target_index(target) {
            Ok(i) => i,
            Err(e) => {
                // Sample is consumed; Drop runs on function exit, which
                // pushes a plain Drop entry (releases the borrow).
                return Err(e);
            }
        };
        let mask = 1u64 << target_index;
        let current = self.forwarding_history.get();
        if current & mask != 0 {
            // R9 forbids a second forward to the same target. Sample is
            // still consumed — the natural Drop releases the borrow.
            return Err(ForwardError::AlreadyForwarded);
        }

        let entry = CompletionEntry {
            tag: CompletionEntryTag::DropAndForward,
            target_index,
            offset: self.details.offset.as_value(),
        };
        let pushed = self.subscriber_shared_state.lock().receiver.
            release_offset_with_entry(
                &self.details,
                ChannelId::new(0),
                entry,
            );
        if !pushed {
            // Drop will run naturally and emit a plain Drop entry.
            return Err(ForwardError::CompletionQueueFull);
        }
        // Suppress the natural Drop so the publisher sees exactly one
        // entry for this Sample handle — the DropAndForward we just
        // pushed.
        core::mem::forget(self);
        Ok(())
    }

    /// Looks up `target` in the source service's declared `forwards_into`
    /// list and returns its stable target index (its position in the
    /// list). Returns [`ForwardError::TargetNotDeclared`] if the target
    /// is not declared.
    fn resolve_target_index(&self, target: &ServiceName) -> Result<u8, ForwardError> {
        let shared = self.subscriber_shared_state.lock();
        let static_config = shared
            .receiver
            .service_state
            .static_config()
            .publish_subscribe();
        match static_config.forwards_into().position(target) {
            Some(idx) => Ok(idx as u8),
            None => Err(ForwardError::TargetNotDeclared),
        }
    }
}
