# Publish-Subscribe Forwarding

A small triage example demonstrating the publish-subscribe forwarding
extension. A source publisher emits raw lidar scans; a triage subscriber
inspects each scan and re-emits the obstacle-range ones onto a second
service (`obstacle_scans`) without copying. A target subscriber on
`obstacle_scans` receives only the forwarded scans, reading directly
from the source publisher's data segment.

See `doc/design-documents/publish-subscribe-forwarding.md` for the full
design.

## Run

**Start order matters.** A publisher on the source service attaches
as a forwarder participant on each target service named in
`forwards_into` at publisher-creation time; if the target service
does not yet exist, publisher creation fails with
`ForwardingTargetServiceUnavailable`. Run in this order — three
terminals:

### Terminal 1 — target subscriber (creates the target service first)

```sh
cargo run --example publish_subscribe_forwarding_target_subscriber
```

### Terminal 2 — triage subscriber (forwarder)

```sh
cargo run --example publish_subscribe_forwarding_triage_subscriber
```

### Terminal 3 — source publisher (last; needs the target service alive)

```sh
cargo run --example publish_subscribe_forwarding_source_publisher
```

## Topology

```text
                              forwards_into = [obstacle_scans]
                                          │
        publisher_mode = Mixed (default)  │
                                          ▼
   ┌────────────────────────────┐    Sample::forward_to()    ┌──────────────────────────────┐
   │ source publisher (this)    │ ────────────────────────▶  │ target subscriber            │
   │ service: raw_lidar_scans   │   ╲                        │ service: obstacle_scans      │
   │                            │    ╲(zero-copy fanout)     │                              │
   │      LidarScan buckets     │     ╲                      │      LidarScan buckets       │
   └────────────────────────────┘      ╲                     │      (read from source seg)  │
              │                         ╲                    └──────────────────────────────┘
              │ native delivery          ╲                                ▲
              ▼                           ╲                               │ accepts_forwarders_from
   ┌────────────────────────────┐         ╲                               │     = [raw_lidar_scans]
   │ triage subscriber          │  ────────╲──────  decides:              │     publisher_mode = ForwarderOnly
   │ (subscriber on             │                   range < threshold?    │
   │  raw_lidar_scans)          │                                         │
   └────────────────────────────┘  ◀──────────────  forward / drop ───────┘
```

The triage subscriber is a normal subscriber of `raw_lidar_scans`.
Calling `sample.forward_to(&obstacle_scans)` on a received `Sample`
pushes a `Forward` entry onto the source publisher's completion queue;
the source publisher (which has set up forwarding connections to
`obstacle_scans` subscribers via M3d) fans out the offset on its next
sweep. The target subscriber's `Sample` reads from the same shared
memory bucket the source publisher originally allocated — no copy ever
happens.

Per-bucket R10 (publisher-side, per-target) and per-Sample R9
(subscriber-side, per-target) make the operation safe under any number
of triage subscribers and any combination of `forward_to` /
`drop_and_forward_to` calls. See the design doc for details.
