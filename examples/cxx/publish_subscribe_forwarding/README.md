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
