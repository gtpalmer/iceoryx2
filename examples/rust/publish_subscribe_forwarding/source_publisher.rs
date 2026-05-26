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

//! Source-side publisher for the publish-subscribe forwarding example.
//!
//! Declares `raw_lidar_scans` as a publish-subscribe service whose
//! `forwards_into` list contains `obstacle_scans`. Subscribers of
//! `raw_lidar_scans` (the "triage" subscriber binary) decide which
//! scans should be re-emitted onto `obstacle_scans` and call
//! `Sample::forward_to(&obstacle_scans)`. The bucket is reused on the
//! receiving side — no payload copy.

use core::time::Duration;

extern crate alloc;
use alloc::boxed::Box;

use iceoryx2::prelude::*;

const CYCLE_TIME: Duration = Duration::from_secs(1);

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

    let raw_scans = node
        .service_builder(&raw_scans_name)
        .publish_subscribe::<LidarScan>()
        .forwards_into(vec![obstacle_scans_name])
        .open_or_create()?;

    let publisher = raw_scans.publisher_builder().create()?;

    let mut counter: u64 = 0;

    while node.wait(CYCLE_TIME).is_ok() {
        counter += 1;
        let scan = LidarScan {
            timestamp_ns: counter * 1_000_000_000,
            angle: (counter as f32) * 0.5,
            range: if counter % 3 == 0 { 1.2 } else { 15.0 },
            intensity: counter as u32 * 10,
        };

        publisher.send_copy(scan)?;
        coutln!("[raw_lidar_scans] scan {counter}: range={:.1}m", scan.range);
    }

    Ok(())
}
