# Copyright (c) 2026 Contributors to the Eclipse Foundation
#
# See the NOTICE file(s) distributed with this work for additional
# information regarding copyright ownership.
#
# This program and the accompanying materials are made available under the
# terms of the Apache Software License 2.0 which is available at
# https://www.apache.org/licenses/LICENSE-2.0, or the MIT license
# which is available at https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: Apache-2.0 OR MIT

"""LidarScan payload type used by the forwarding example."""

import ctypes


class LidarScan(ctypes.Structure):
    """Fixed-size LidarScan record passed via shared memory."""

    _fields_ = [
        ("timestamp_ns", ctypes.c_uint64),
        ("angle", ctypes.c_float),
        ("range", ctypes.c_float),
        ("intensity", ctypes.c_uint32),
    ]
