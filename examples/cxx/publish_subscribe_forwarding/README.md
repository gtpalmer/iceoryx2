# Publish-Subscribe Forwarding (C++)

C++ port of the triage example demonstrating the publish-subscribe
forwarding extension. See the Rust example at
`examples/rust/publish_subscribe_forwarding/` and the design doc at
`doc/design-documents/publish-subscribe-forwarding.md`.

## Build

From the repository root:

```sh
mkdir -p build
cd build
cmake -DBUILD_CXX=ON -DBUILD_EXAMPLES=ON ..
cmake --build . -j4
```

The three binaries land at:

```text
build/examples/cxx/publish_subscribe_forwarding/example_cxx_publish_subscribe_forwarding_source_publisher
build/examples/cxx/publish_subscribe_forwarding/example_cxx_publish_subscribe_forwarding_triage_subscriber
build/examples/cxx/publish_subscribe_forwarding/example_cxx_publish_subscribe_forwarding_target_subscriber
```

## Run

**Start order matters.** A publisher on the source service attaches as
a forwarder participant on each target service named in
`forwards_into` at publisher-creation time; if the target service does
not yet exist, publisher creation fails with
`ForwardingTargetServiceUnavailable`. Run in this order — three
terminals:

### Terminal 1 — target subscriber (creates the target service first)

```sh
./build/examples/cxx/publish_subscribe_forwarding/example_cxx_publish_subscribe_forwarding_target_subscriber
```

### Terminal 2 — triage subscriber (forwarder)

```sh
./build/examples/cxx/publish_subscribe_forwarding/example_cxx_publish_subscribe_forwarding_triage_subscriber
```

### Terminal 3 — source publisher (last; needs the target service alive)

```sh
./build/examples/cxx/publish_subscribe_forwarding/example_cxx_publish_subscribe_forwarding_source_publisher
```

If you run `source_publisher` before `target_subscriber`, you'll see a
crash at the `publisher_builder().create().value()` call — that's the
target-service-missing path. Bring up the target first and the chain
flows cleanly:

```text
[raw_lidar_scans] scan 3: range=1.2m                                   # source emits
[triage] forward to obstacle_scans: range=1.2m                         # triage forwards
[obstacle_scans] received forwarded scan: t=3000000000 ns, range=1.2m  # target receives
```

## Removing the start-order constraint

The example above hard-codes the dependency: the target subscriber is
the canonical creator of `obstacle_scans`. If you'd rather have the
three processes start in any order, have **every process that touches
the target service call `open_or_create` on it with the same
parameters** — including the source publisher process. Whichever
process runs first wins the create race; the others open the existing
SHM region:

```cpp
// Source publisher process: bootstrap the target service before
// constructing the publisher.
std::vector<ServiceName> sources;
sources.push_back(raw_scans_name);
auto _obstacle_scans = node.service_builder(obstacle_scans_name)
                           .publish_subscribe<LidarScan>()
                           .accepts_forwarders_from(std::move(sources))
                           .publisher_mode(PublisherMode::ForwarderOnly)
                           .open_or_create()
                           .value();

// Now the publisher attaches to a target service that's guaranteed
// to exist:
std::vector<ServiceName> targets;
targets.push_back(obstacle_scans_name);
auto raw_scans = node.service_builder(raw_scans_name)
                     .publish_subscribe<LidarScan>()
                     .forwards_into(std::move(targets))
                     .open_or_create()
                     .value();
auto publisher = raw_scans.publisher_builder().create().value();
```

Caveats:

* **Parameter consistency is on you.** `accepts_forwarders_from`,
  `publisher_mode`, `max_publishers`, etc. must match exactly across
  every process that calls `open_or_create` on a given service;
  mismatch surfaces as `IncompatibleAcceptsForwardersFrom` /
  `IncompatibleMode` / etc. errors at the second open. A small
  shared helper that returns the configured builder is the usual fix.
* **Service lifetime is reference-counted across nodes.** If the
  source publisher creates `obstacle_scans` and exits before any
  target subscriber has opened it, the SHM region is reaped. A
  subsequent run recreates it — generally fine, but means the
  "boot, create, exit" pattern does not pre-stage services for
  arbitrarily-delayed consumers.

This pattern is the one production iceoryx2 deployments tend to use,
specifically to remove start-order assumptions.
