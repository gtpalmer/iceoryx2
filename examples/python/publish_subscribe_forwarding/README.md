# Publish-Subscribe Forwarding (Python)

Python port of the publish-subscribe forwarding triage example. See
the Rust example at `examples/rust/publish_subscribe_forwarding/` and
the design doc at
`doc/design-documents/publish-subscribe-forwarding.md` for full
details on the pattern.

## Run

Three terminals. From the repository root with the Python bindings
installed:

```sh
python3 examples/python/publish_subscribe_forwarding/target_subscriber.py
```

```sh
python3 examples/python/publish_subscribe_forwarding/triage_subscriber.py
```

```sh
python3 examples/python/publish_subscribe_forwarding/source_publisher.py
```

The triage subscriber inspects each scan; those whose `range` falls
below the obstacle threshold are forwarded onto `obstacle_scans`
without copying — the target subscriber reads from the same shared
memory bucket the source publisher originally allocated.
