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

"""Source publisher for the publish-subscribe forwarding example.

Publishes raw lidar scans on the `raw_lidar_scans` service. The service
declares `forwards_into = [obstacle_scans]`, so any subscriber may
re-emit a received `Sample` onto `obstacle_scans` via
`Sample.forward_to`. See the triage_subscriber.py companion.
"""

import iceoryx2 as iox2
from lidar_scan import LidarScan

iox2.set_log_level_from_env_or(iox2.LogLevel.Info)

cycle_time = iox2.Duration.from_secs(1)
node = iox2.NodeBuilder.new().create(iox2.ServiceType.Ipc)

raw_scans_name = iox2.ServiceName.new("raw_lidar_scans")
obstacle_scans_name = iox2.ServiceName.new("obstacle_scans")

raw_scans = (
    node.service_builder(raw_scans_name)
    .publish_subscribe(LidarScan)
    .forwards_into([obstacle_scans_name])
    .open_or_create()
)

publisher = raw_scans.publisher_builder().create()

counter = 0
try:
    while True:
        counter += 1
        node.wait(cycle_time)
        scan = LidarScan(
            timestamp_ns=counter * 1_000_000_000,
            angle=counter * 0.5,
            range=1.2 if counter % 3 == 0 else 15.0,
            intensity=counter * 10,
        )
        publisher.send_copy(scan)
        print(f"[raw_lidar_scans] scan {counter}: range={scan.range:.1f}m")
except iox2.NodeWaitFailure:
    print("exit")
