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

"""Triage subscriber for the publish-subscribe forwarding example.

Subscribes to `raw_lidar_scans` and forwards near-range scans onto
`obstacle_scans` via `Sample.forward_to`. The bucket is reused on the
target service's side — no payload copy is performed.
"""

import iceoryx2 as iox2
from lidar_scan import LidarScan

iox2.set_log_level_from_env_or(iox2.LogLevel.Info)

cycle_time = iox2.Duration.from_millis(100)
node = iox2.NodeBuilder.new().create(iox2.ServiceType.Ipc)

raw_scans_name = iox2.ServiceName.new("raw_lidar_scans")
obstacle_scans_name = iox2.ServiceName.new("obstacle_scans")

raw_scans = (
    node.service_builder(raw_scans_name)
    .publish_subscribe(LidarScan)
    .forwards_into([obstacle_scans_name])
    .open_or_create()
)

subscriber = raw_scans.subscriber_builder().create()

OBSTACLE_RANGE_THRESHOLD = 2.0

try:
    while True:
        node.wait(cycle_time)
        sample = subscriber.receive()
        while sample is not None:
            scan = sample.payload().contents
            if scan.range < OBSTACLE_RANGE_THRESHOLD:
                try:
                    sample.forward_to(obstacle_scans_name)
                    print(
                        f"[triage] forward to obstacle_scans: range={scan.range:.1f}m"
                    )
                except iox2.ForwardError as exc:
                    print(f"[triage] forward failed: {exc}")
            else:
                print(f"[triage] discard non-obstacle: range={scan.range:.1f}m")
            sample = subscriber.receive()
except iox2.NodeWaitFailure:
    print("exit")
