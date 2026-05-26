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

//! Target-side subscriber for the publish-subscribe forwarding example.
//!
//! Subscribes to `obstacle_scans`, which is declared with
//! `accepts_forwarders_from(["raw_lidar_scans"])` and
//! `publisher_mode(PublisherMode::ForwarderOnly)` — meaning the only
//! way data flows into this service is via the triage subscriber's
//! `Sample::forward_to(&obstacle_scans)` calls. The samples received
//! here read from the source publisher's data segment directly; no
//! payload copy was made anywhere along the chain.

use core::time::Duration;

extern crate alloc;
use alloc::boxed::Box;

use iceoryx2::prelude::*;

const CYCLE_TIME: Duration = Duration::from_millis(100);

#[repr(C)]
#[derive(Debug, Default, Clone, Copy, ZeroCopySend)]
struct LidarScan {
    timestamp_ns: u64,
    angle: f32,
    range: f32,
    intensity: u32,
}

fn main() -> Result<(), Box<dyn core::error::Error>> {
    set_log_level_from_env_or(LogLevel::Info);

    let node = NodeBuilder::new().create::<ipc::Service>()?;

    let raw_scans_name: ServiceName = "raw_lidar_scans".try_into()?;
    let obstacle_scans_name: ServiceName = "obstacle_scans".try_into()?;

    let obstacle_scans = node
        .service_builder(&obstacle_scans_name)
        .publish_subscribe::<LidarScan>()
        .accepts_forwarders_from(vec![raw_scans_name])
        .publisher_mode(PublisherMode::ForwarderOnly)
        .open_or_create()?;

    let subscriber = obstacle_scans.subscriber_builder().create()?;

    while node.wait(CYCLE_TIME).is_ok() {
        while let Some(sample) = subscriber.receive()? {
            coutln!(
                "[obstacle_scans] received forwarded scan: t={} ns, range={:.1}m, intensity={}",
                sample.timestamp_ns,
                sample.range,
                sample.intensity
            );
        }
    }

    Ok(())
}
