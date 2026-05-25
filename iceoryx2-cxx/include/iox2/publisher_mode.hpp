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

#ifndef IOX2_PUBLISHER_MODE_HPP
#define IOX2_PUBLISHER_MODE_HPP

#include <cstdint>

namespace iox2 {
/// Controls which kinds of publishers may attach to a
/// publish-subscribe service. Used with the
/// [`ServiceBuilderPublishSubscribe::publisher_mode`],
/// [`ServiceBuilderPublishSubscribe::forwards_into`], and
/// [`ServiceBuilderPublishSubscribe::accepts_forwarders_from`]
/// builder methods to model the publish-subscribe forwarding feature.
/// See `doc/design-documents/publish-subscribe-forwarding.md`.
enum class PublisherMode : uint8_t {
    /// The service may have both native publishers and publishers
    /// participating as forwarders from other services. Default.
    Mixed,
    /// Only native publishers are permitted. Forwarder attachment from
    /// other services is rejected at publisher creation time.
    NativeOnly,
    /// Only forwarding participations from other services are
    /// permitted. Native publisher creation is rejected.
    ForwarderOnly,
};
} // namespace iox2

#endif
