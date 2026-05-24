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

use iceoryx2_bb_derive_macros::ZeroCopySend;
use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;
use serde::{Deserialize, Serialize, de::Visitor};

/// Controls which kinds of publishers may attach to a
/// [`MessagingPattern::PublishSubscribe`](crate::service::messaging_pattern::MessagingPattern::PublishSubscribe)
/// service. Used in conjunction with the
/// [`forwards_into`](crate::service::builder::publish_subscribe::Builder::forwards_into)
/// and
/// [`accepts_forwarders_from`](crate::service::builder::publish_subscribe::Builder::accepts_forwarders_from)
/// declarations to model the publish-subscribe forwarding feature described in
/// `doc/design-documents/publish-subscribe-forwarding.md`.
#[repr(C)]
#[derive(Debug, Default, Eq, PartialEq, Hash, Clone, Copy, ZeroCopySend)]
pub enum PublisherMode {
    /// The service may have both native publishers and publishers participating as
    /// forwarders from other services. This is the default and preserves the
    /// behavior of pre-forwarding pub/sub.
    #[default]
    Mixed,
    /// Only native publishers are permitted. Any attempt to attach a forwarding
    /// participation from another service is rejected at service open time.
    NativeOnly,
    /// Only forwarding participations from other services are permitted. Any
    /// attempt to create a native publisher on this service is rejected at
    /// publisher creation time.
    ForwarderOnly,
}

impl Serialize for PublisherMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&alloc::format!("{self:?}"))
    }
}

struct PublisherModeVisitor;

impl Visitor<'_> for PublisherModeVisitor {
    type Value = PublisherMode;

    fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
        formatter
            .write_str("a string containing 'Mixed', 'NativeOnly', or 'ForwarderOnly'")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match v {
            "Mixed" => Ok(PublisherMode::Mixed),
            "NativeOnly" => Ok(PublisherMode::NativeOnly),
            "ForwarderOnly" => Ok(PublisherMode::ForwarderOnly),
            v => Err(E::custom(alloc::format!(
                "Invalid PublisherMode provided: \"{v:?}\"."
            ))),
        }
    }
}

impl<'de> Deserialize<'de> for PublisherMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(PublisherModeVisitor)
    }
}
