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

// Source publisher for the publish-subscribe forwarding example.
//
// Declares `raw_lidar_scans` with `forwards_into = [obstacle_scans]`.
// See the README and the design doc.

#include "iox2/iceoryx2.hpp"
#include "lidar_scan.hpp"

#include <iostream>
#include <vector>

constexpr iox2::bb::Duration CYCLE_TIME = iox2::bb::Duration::from_secs(1);

auto main() -> int {
    using namespace iox2;
    set_log_level_from_env_or(LogLevel::Info);
    auto node = NodeBuilder().create<ServiceType::Ipc>().value();

    auto raw_scans_name = ServiceName::create("raw_lidar_scans").value();
    auto obstacle_scans_name = ServiceName::create("obstacle_scans").value();

    std::vector<ServiceName> targets;
    targets.push_back(obstacle_scans_name);

    auto raw_scans = node.service_builder(raw_scans_name)
                         .publish_subscribe<LidarScan>()
                         .forwards_into(std::move(targets))
                         .open_or_create()
                         .value();

    auto publisher = raw_scans.publisher_builder().create().value();

    std::uint64_t counter = 0;
    while (node.wait(CYCLE_TIME).has_value()) {
        counter += 1;
        LidarScan scan { counter * 1'000'000'000ULL,
                         static_cast<float>(counter) * 0.5F,
                         (counter % 3 == 0) ? 1.2F : 15.0F,
                         static_cast<std::uint32_t>(counter * 10) };
        const auto result = publisher.send_copy(scan);
        if (!result.has_value()) {
            std::cerr << "send_copy failed\n";
            break;
        }
        std::cout << "[raw_lidar_scans] scan " << counter << ": range=" << scan.range << "m\n";
    }

    return 0;
}
