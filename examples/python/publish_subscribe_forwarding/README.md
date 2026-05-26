# Publish-Subscribe Forwarding (Python)

Python port of the publish-subscribe forwarding triage example. See
the Rust example at `examples/rust/publish_subscribe_forwarding/` and
the design doc at
`doc/design-documents/publish-subscribe-forwarding.md` for full
details on the pattern.

## Run

**Start order matters.** A publisher on the source service attaches
as a forwarder participant on each target service named in
`forwards_into` at publisher-creation time; if the target service
does not yet exist, publisher creation fails. Run in this order —
three terminals, with the Python bindings installed:

### Terminal 1 — target subscriber (creates the target service first)

```sh
python3 examples/python/publish_subscribe_forwarding/target_subscriber.py
```

### Terminal 2 — triage subscriber (forwarder)

```sh
python3 examples/python/publish_subscribe_forwarding/triage_subscriber.py
```

### Terminal 3 — source publisher (last; needs the target service alive)

```sh
python3 examples/python/publish_subscribe_forwarding/source_publisher.py
```

The triage subscriber inspects each scan; those whose `range` falls
below the obstacle threshold are forwarded onto `obstacle_scans`
without copying — the target subscriber reads from the same shared
memory bucket the source publisher originally allocated.
