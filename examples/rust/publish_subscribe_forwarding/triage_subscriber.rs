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

//! Triage subscriber for the publish-subscribe forwarding example.
//!
//! Subscribes to `raw_lidar_scans` and, for each received sample,
//! decides whether it represents an obstacle (range below threshold).
//! If so, it calls `Sample::forward_to(&obstacle_scans)` — the bucket
//! is re-emitted onto the `obstacle_scans` service's subscribers
//! without a payload copy. Drop-only samples are returned as usual.

use core::time::Duration;

extern crate alloc;
use alloc::boxed::Box;

use iceoryx2::prelude::*;

const CYCLE_TIME: Duration = Duration::from_millis(100);
const OBSTACLE_RANGE_THRESHOLD: f32 = 2.0;

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

    let subscriber = raw_scans.subscriber_builder().create()?;

    while node.wait(CYCLE_TIME).is_ok() {
        while let Some(sample) = subscriber.receive()? {
            if sample.range < OBSTACLE_RANGE_THRESHOLD {
                // Re-emit this bucket onto `obstacle_scans` — no copy.
                match sample.forward_to(&obstacle_scans_name) {
                    Ok(()) => coutln!(
                        "[triage] forward to obstacle_scans: range={:.1}m",
                        sample.range
                    ),
                    Err(e) => coutln!("[triage] forward failed: {:?}", e),
                }
            } else {
                coutln!(
                    "[triage] discard non-obstacle: range={:.1}m",
                    sample.range
                );
            }
            // Sample drops naturally at end of scope, returning the
            // borrow to the source publisher.
        }
    }

    Ok(())
}
