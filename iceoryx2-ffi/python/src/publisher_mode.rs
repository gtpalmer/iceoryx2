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

use pyo3::prelude::*;

#[pyclass(eq, eq_int)]
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
/// Controls which kinds of publishers may attach to a
/// publish-subscribe service. Used with the
/// `ServiceBuilderPublishSubscribe.publisher_mode`,
/// `forwards_into`, and `accepts_forwarders_from` builder methods.
pub enum PublisherMode {
    /// Both native publishers and publishers participating as
    /// forwarders from other services are permitted. Default.
    Mixed,
    /// Only native publishers are permitted; forwarder attachment from
    /// other services is rejected at publisher creation time.
    NativeOnly,
    /// Only forwarding participations from other services are
    /// permitted; native publisher creation is rejected.
    ForwarderOnly,
}

impl From<PublisherMode> for iceoryx2::port::publisher_mode::PublisherMode {
    fn from(value: PublisherMode) -> Self {
        match value {
            PublisherMode::Mixed => iceoryx2::port::publisher_mode::PublisherMode::Mixed,
            PublisherMode::NativeOnly => iceoryx2::port::publisher_mode::PublisherMode::NativeOnly,
            PublisherMode::ForwarderOnly => {
                iceoryx2::port::publisher_mode::PublisherMode::ForwarderOnly
            }
        }
    }
}

impl From<iceoryx2::port::publisher_mode::PublisherMode> for PublisherMode {
    fn from(value: iceoryx2::port::publisher_mode::PublisherMode) -> Self {
        match value {
            iceoryx2::port::publisher_mode::PublisherMode::Mixed => PublisherMode::Mixed,
            iceoryx2::port::publisher_mode::PublisherMode::NativeOnly => PublisherMode::NativeOnly,
            iceoryx2::port::publisher_mode::PublisherMode::ForwarderOnly => {
                PublisherMode::ForwarderOnly
            }
        }
    }
}
