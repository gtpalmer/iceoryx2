# Publish-Subscribe Forwarding (C++)

C++ port of the triage example demonstrating the publish-subscribe
forwarding extension. See the Rust example at
`examples/rust/publish_subscribe_forwarding/` and the design doc at
`doc/design-documents/publish-subscribe-forwarding.md`.

## Build

The example is wired into the top-level `examples/cxx/CMakeLists.txt`.
Build the full C++ examples and you will get three binaries:

- `example_cxx_publish_subscribe_forwarding_source_publisher`
- `example_cxx_publish_subscribe_forwarding_triage_subscriber`
- `example_cxx_publish_subscribe_forwarding_target_subscriber`

## Run

Three terminals, in this order:

```sh
./example_cxx_publish_subscribe_forwarding_target_subscriber
./example_cxx_publish_subscribe_forwarding_triage_subscriber
./example_cxx_publish_subscribe_forwarding_source_publisher
```
