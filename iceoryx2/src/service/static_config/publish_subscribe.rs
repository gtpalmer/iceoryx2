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
//!
//! # fn main() -> Result<(), Box<dyn core::error::Error>> {
//! let node = NodeBuilder::new().create::<ipc::Service>()?;
//! let pubsub = node.service_builder(&"My/Funk/ServiceName".try_into()?)
//!     .publish_subscribe::<u64>()
//!     .open_or_create()?;
//!
//! println!("type details:                     {:?}", pubsub.static_config().message_type_details());
//! println!("max publishers:                   {:?}", pubsub.static_config().max_publishers());
//! println!("max subscribers:                  {:?}", pubsub.static_config().max_subscribers());
//! println!("subscriber buffer size:           {:?}", pubsub.static_config().subscriber_max_buffer_size());
//! println!("history size:                     {:?}", pubsub.static_config().history_size());
//! println!("subscriber max borrowed samples:  {:?}", pubsub.static_config().subscriber_max_borrowed_samples());
//! println!("safe overflow:                    {:?}", pubsub.static_config().has_safe_overflow());
//! println!("publisher mode:                   {:?}", pubsub.static_config().publisher_mode());
//! println!("forwards into:                    {:?}", pubsub.static_config().forwards_into());
//! println!("accepts forwarders from:          {:?}", pubsub.static_config().accepts_forwarders_from());
//!
//! # Ok(())
//! # }
//! ```

use super::message_type_details::MessageTypeDetails;
use crate::config;
use crate::port::publisher_mode::PublisherMode;
use crate::service::service_name::ServiceName;
use alloc::vec::Vec;
use iceoryx2_bb_container::relocatable_option::RelocatableOption;
use iceoryx2_bb_derive_macros::ZeroCopySend;
use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;
use serde::{Deserialize, Serialize, de::SeqAccess, ser::SerializeSeq};

/// Maximum number of forwarding targets that may be declared on a single
/// [`MessagingPattern::PublishSubscribe`](crate::service::messaging_pattern::MessagingPattern::PublishSubscribe)
/// source service via
/// [`forwards_into`](crate::service::builder::publish_subscribe::Builder::forwards_into),
/// and the maximum number of source services that may be declared via
/// [`accepts_forwarders_from`](crate::service::builder::publish_subscribe::Builder::accepts_forwarders_from).
pub const MAX_FORWARDING_TARGETS_PER_SERVICE: usize = 8;

/// A fixed-capacity, shared-memory-compatible list of [`ServiceName`]s used to
/// declare publish-subscribe forwarding topology in the static service config.
///
/// The list preserves insertion order, so the position of each entry is the
/// stable target index used by later runtime bookkeeping (see R9 / R10 in
/// `doc/design-documents/publish-subscribe-forwarding.md`).
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, ZeroCopySend)]
#[repr(C)]
pub struct ForwardingTargets {
    entries: [RelocatableOption<ServiceName>; MAX_FORWARDING_TARGETS_PER_SERVICE],
    len: u8,
}

impl Default for ForwardingTargets {
    fn default() -> Self {
        Self {
            entries: [RelocatableOption::None; MAX_FORWARDING_TARGETS_PER_SERVICE],
            len: 0,
        }
    }
}

/// Errors that can occur when constructing a [`ForwardingTargets`] from a slice.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum ForwardingTargetsError {
    /// The provided slice contained more entries than
    /// [`MAX_FORWARDING_TARGETS_PER_SERVICE`].
    ExceedsCapacity,
    /// The provided slice contained the same [`ServiceName`] more than once.
    DuplicateEntry,
}

impl core::fmt::Display for ForwardingTargetsError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "ForwardingTargetsError::{self:?}")
    }
}

impl core::error::Error for ForwardingTargetsError {}

impl ForwardingTargets {
    /// Constructs a new empty [`ForwardingTargets`] list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Constructs a [`ForwardingTargets`] list from a slice of [`ServiceName`]s.
    ///
    /// Returns [`ForwardingTargetsError::ExceedsCapacity`] if the slice is longer
    /// than [`MAX_FORWARDING_TARGETS_PER_SERVICE`], or
    /// [`ForwardingTargetsError::DuplicateEntry`] if the slice contains any
    /// duplicate name.
    pub fn from_slice(targets: &[ServiceName]) -> Result<Self, ForwardingTargetsError> {
        if targets.len() > MAX_FORWARDING_TARGETS_PER_SERVICE {
            return Err(ForwardingTargetsError::ExceedsCapacity);
        }
        for i in 0..targets.len() {
            for j in (i + 1)..targets.len() {
                if targets[i] == targets[j] {
                    return Err(ForwardingTargetsError::DuplicateEntry);
                }
            }
        }
        let mut result = Self::default();
        for (i, name) in targets.iter().enumerate() {
            result.entries[i] = RelocatableOption::Some(*name);
        }
        result.len = targets.len() as u8;
        Ok(result)
    }

    /// Returns the number of [`ServiceName`]s in the list.
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// Returns `true` if the list contains no entries.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the [`ServiceName`] stored at the given index, or `None` if the
    /// index is out of range.
    pub fn get(&self, index: usize) -> Option<&ServiceName> {
        if index >= self.len() {
            return None;
        }
        match &self.entries[index] {
            RelocatableOption::Some(name) => Some(name),
            RelocatableOption::None => None,
        }
    }

    /// Returns the index of the first occurrence of `name` in the list, or
    /// `None` if not present.
    pub fn position(&self, name: &ServiceName) -> Option<usize> {
        for i in 0..self.len() {
            if let Some(entry) = self.get(i) {
                if entry == name {
                    return Some(i);
                }
            }
        }
        None
    }

    /// Returns `true` if the list contains the provided [`ServiceName`].
    pub fn contains(&self, name: &ServiceName) -> bool {
        self.position(name).is_some()
    }

    /// Returns an iterator over the [`ServiceName`]s in the list, in insertion
    /// order.
    pub fn iter(&self) -> ForwardingTargetsIter<'_> {
        ForwardingTargetsIter {
            list: self,
            cursor: 0,
        }
    }
}

/// Iterator over the [`ServiceName`]s of a [`ForwardingTargets`] list.
pub struct ForwardingTargetsIter<'a> {
    list: &'a ForwardingTargets,
    cursor: usize,
}

impl<'a> Iterator for ForwardingTargetsIter<'a> {
    type Item = &'a ServiceName;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.list.get(self.cursor)?;
        self.cursor += 1;
        Some(entry)
    }
}

impl Serialize for ForwardingTargets {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for name in self.iter() {
            seq.serialize_element(name)?;
        }
        seq.end()
    }
}

struct ForwardingTargetsVisitor;

impl<'de> serde::de::Visitor<'de> for ForwardingTargetsVisitor {
    type Value = ForwardingTargets;

    fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
        formatter.write_str("a sequence of at most ")?;
        write!(formatter, "{MAX_FORWARDING_TARGETS_PER_SERVICE}")?;
        formatter.write_str(" service names")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut buffer: Vec<ServiceName> = Vec::new();
        while let Some(value) = seq.next_element::<ServiceName>()? {
            if buffer.len() >= MAX_FORWARDING_TARGETS_PER_SERVICE {
                return Err(serde::de::Error::custom(alloc::format!(
                    "ForwardingTargets exceeds maximum capacity of {MAX_FORWARDING_TARGETS_PER_SERVICE}"
                )));
            }
            buffer.push(value);
        }
        ForwardingTargets::from_slice(&buffer)
            .map_err(|e| serde::de::Error::custom(alloc::format!("{e:?}")))
    }
}

impl<'de> Deserialize<'de> for ForwardingTargets {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(ForwardingTargetsVisitor)
    }
}

/// The static configuration of an
/// [`MessagingPattern::PublishSubscribe`](crate::service::messaging_pattern::MessagingPattern::PublishSubscribe)
/// based service. Contains all parameters that do not change during the lifetime of a
/// [`Service`](crate::service::Service).
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, ZeroCopySend, Serialize, Deserialize)]
#[repr(C)]
pub struct StaticConfig {
    pub(crate) max_subscribers: usize,
    pub(crate) max_publishers: usize,
    pub(crate) max_nodes: usize,
    pub(crate) history_size: usize,
    pub(crate) subscriber_max_buffer_size: usize,
    pub(crate) subscriber_max_borrowed_samples: usize,
    pub(crate) enable_safe_overflow: bool,
    pub(crate) publisher_mode: PublisherMode,
    pub(crate) forwards_into: ForwardingTargets,
    pub(crate) accepts_forwarders_from: ForwardingTargets,
    pub(crate) message_type_details: MessageTypeDetails,
}

impl StaticConfig {
    pub(crate) fn new(config: &config::Config) -> Self {
        Self {
            max_subscribers: config.defaults.publish_subscribe.max_subscribers,
            max_publishers: config.defaults.publish_subscribe.max_publishers,
            max_nodes: config.defaults.publish_subscribe.max_nodes,
            history_size: config.defaults.publish_subscribe.publisher_history_size,
            subscriber_max_buffer_size: config
                .defaults
                .publish_subscribe
                .subscriber_max_buffer_size,
            subscriber_max_borrowed_samples: config
                .defaults
                .publish_subscribe
                .subscriber_max_borrowed_samples,
            enable_safe_overflow: config.defaults.publish_subscribe.enable_safe_overflow,
            publisher_mode: PublisherMode::default(),
            forwards_into: ForwardingTargets::default(),
            accepts_forwarders_from: ForwardingTargets::default(),
            message_type_details: MessageTypeDetails::default(),
        }
    }

    pub(crate) fn required_amount_of_samples_per_data_segment(
        &self,
        publisher_max_loaned_data: usize,
    ) -> usize {
        self.max_subscribers
            * (self.subscriber_max_buffer_size + self.subscriber_max_borrowed_samples)
            + self.history_size
            + publisher_max_loaned_data
    }

    /// Returns the maximum supported amount of [`Node`](crate::node::Node)s that can open the
    /// [`Service`](crate::service::Service) in parallel.
    pub fn max_nodes(&self) -> usize {
        self.max_nodes
    }

    /// Returns the maximum supported amount of [`crate::port::publisher::Publisher`] ports
    pub fn max_publishers(&self) -> usize {
        self.max_publishers
    }

    /// Returns the maximum supported amount of [`crate::port::subscriber::Subscriber`] ports
    pub fn max_subscribers(&self) -> usize {
        self.max_subscribers
    }

    /// Returns the maximum history size that can be requested on connect.
    pub fn history_size(&self) -> usize {
        self.history_size
    }

    /// Returns the maximum supported buffer size for [`crate::port::subscriber::Subscriber`] port
    pub fn subscriber_max_buffer_size(&self) -> usize {
        self.subscriber_max_buffer_size
    }

    /// Returns how many [`crate::sample::Sample`] a [`crate::port::subscriber::Subscriber`] port
    /// can borrow in parallel at most.
    pub fn subscriber_max_borrowed_samples(&self) -> usize {
        self.subscriber_max_borrowed_samples
    }

    /// Returns true if the [`crate::service::Service`] safely overflows, otherwise false. Safe
    /// overflow means that the [`crate::port::publisher::Publisher`] will recycle the oldest
    /// [`crate::sample::Sample`] from the [`crate::port::subscriber::Subscriber`] when its buffer
    /// is full.
    pub fn has_safe_overflow(&self) -> bool {
        self.enable_safe_overflow
    }

    /// Returns the type details of the [`crate::service::Service`].
    pub fn message_type_details(&self) -> &MessageTypeDetails {
        &self.message_type_details
    }

    /// Returns the [`PublisherMode`] that controls which kinds of publishers may
    /// attach to the [`crate::service::Service`]. See the publish-subscribe
    /// forwarding design document for semantics.
    pub fn publisher_mode(&self) -> PublisherMode {
        self.publisher_mode
    }

    /// Returns the list of target services this service's publishers may forward
    /// into. Together with each target's
    /// [`accepts_forwarders_from`](Self::accepts_forwarders_from), this defines
    /// the agreed forwarding graph.
    pub fn forwards_into(&self) -> &ForwardingTargets {
        &self.forwards_into
    }

    /// Returns the list of source services whose buckets this service accepts as
    /// forwarded samples.
    pub fn accepts_forwarders_from(&self) -> &ForwardingTargets {
        &self.accepts_forwarders_from
    }
}
