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

// Target-side subscriber on `obstacle_scans`. Only forwarded samples
// from the source publisher's `raw_lidar_scans` service arrive here.

#include "iox2/iceoryx2.hpp"
#include "lidar_scan.hpp"

#include <iostream>
#include <vector>

constexpr iox2::bb::Duration CYCLE_TIME = iox2::bb::Duration::from_millis(100);

auto main() -> int {
    using namespace iox2;
    set_log_level_from_env_or(LogLevel::Info);
    auto node = NodeBuilder().create<ServiceType::Ipc>().value();

    auto raw_scans_name = ServiceName::create("raw_lidar_scans").value();
    auto obstacle_scans_name = ServiceName::create("obstacle_scans").value();

    std::vector<ServiceName> sources;
    sources.push_back(raw_scans_name);

    auto obstacle_scans = node.service_builder(obstacle_scans_name)
                              .publish_subscribe<LidarScan>()
                              .accepts_forwarders_from(std::move(sources))
                              .publisher_mode(PublisherMode::ForwarderOnly)
                              .open_or_create()
                              .value();

    auto subscriber = obstacle_scans.subscriber_builder().create().value();

    while (node.wait(CYCLE_TIME).has_value()) {
        auto sample = subscriber.receive();
        while (sample.has_value() && sample.value().has_value()) {
            const auto& s = sample.value().value();
            std::cout << "[obstacle_scans] received forwarded scan: t="
                      << s.payload().timestamp_ns << " ns, range=" << s.payload().range
                      << "m, intensity=" << s.payload().intensity << "\n";
            sample = subscriber.receive();
        }
    }

    return 0;
}
