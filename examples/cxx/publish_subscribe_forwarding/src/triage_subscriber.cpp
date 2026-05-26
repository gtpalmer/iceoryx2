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

// Triage subscriber: forwards near-range scans onto `obstacle_scans`.

#include "iox2/iceoryx2.hpp"
#include "lidar_scan.hpp"

#include <iostream>
#include <vector>

constexpr iox2::bb::Duration CYCLE_TIME = iox2::bb::Duration::from_millis(100);
constexpr float OBSTACLE_RANGE_THRESHOLD = 2.0F;

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

    auto subscriber = raw_scans.subscriber_builder().create().value();

    while (node.wait(CYCLE_TIME).has_value()) {
        auto sample = subscriber.receive();
        while (sample.has_value() && sample.value().has_value()) {
            const auto& s = sample.value().value();
            if (s.payload().range < OBSTACLE_RANGE_THRESHOLD) {
                const auto forward_result = s.forward_to(obstacle_scans_name.as_view());
                if (forward_result.has_value()) {
                    std::cout << "[triage] forward to obstacle_scans: range="
                              << s.payload().range << "m\n";
                } else {
                    std::cerr << "[triage] forward failed (error)\n";
                }
            } else {
                std::cout << "[triage] discard non-obstacle: range=" << s.payload().range << "m\n";
            }
            sample = subscriber.receive();
        }
    }

    return 0;
}
