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

"""Target-side subscriber for the publish-subscribe forwarding example.

Subscribes to `obstacle_scans`, which is declared with
`accepts_forwarders_from = [raw_lidar_scans]` and
`publisher_mode = ForwarderOnly`. The only way data flows here is via
the triage subscriber's `Sample.forward_to` calls; the samples
received read from the source publisher's data segment directly — no
payload copy occurs.
"""

import iceoryx2 as iox2
from lidar_scan import LidarScan

iox2.set_log_level_from_env_or(iox2.LogLevel.Info)

cycle_time = iox2.Duration.from_millis(100)
node = iox2.NodeBuilder.new().create(iox2.ServiceType.Ipc)

raw_scans_name = iox2.ServiceName.new("raw_lidar_scans")
obstacle_scans_name = iox2.ServiceName.new("obstacle_scans")

obstacle_scans = (
    node.service_builder(obstacle_scans_name)
    .publish_subscribe(LidarScan)
    .accepts_forwarders_from([raw_scans_name])
    .publisher_mode(iox2.PublisherMode.ForwarderOnly)
    .open_or_create()
)

subscriber = obstacle_scans.subscriber_builder().create()

try:
    while True:
        node.wait(cycle_time)
        sample = subscriber.receive()
        while sample is not None:
            scan = sample.payload().contents
            print(
                "[obstacle_scans] received forwarded scan: "
                f"t={scan.timestamp_ns} ns, range={scan.range:.1f}m, "
                f"intensity={scan.intensity}"
            )
            sample = subscriber.receive()
except iox2.NodeWaitFailure:
    print("exit")
