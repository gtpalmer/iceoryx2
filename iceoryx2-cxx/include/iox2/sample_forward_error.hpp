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

#ifndef IOX2_SAMPLE_FORWARD_ERROR_HPP
#define IOX2_SAMPLE_FORWARD_ERROR_HPP

#include <cstdint>

namespace iox2 {
/// Errors that can occur when invoking
/// [`Sample::forward_to`] or [`Sample::drop_and_forward_to`].
enum class ForwardError : uint8_t {
    /// The named target service is not present in the source service's
    /// `forwards_into` list — this forwarding edge was never authorized
    /// at service-creation time.
    TargetNotDeclared,
    /// This [`Sample`] handle has already been forwarded to the named
    /// target. R9 forbids forwarding the same Sample to the same target
    /// more than once.
    AlreadyForwarded,
    /// The publisher's completion queue (where Forward / DropAndForward
    /// signals flow) is full. Transient; retry once the publisher has
    /// drained.
    CompletionQueueFull,
    /// The connection back to the publisher is no longer valid (the
    /// publisher dropped).
    PublisherUnavailable,
};
} // namespace iox2

#endif
