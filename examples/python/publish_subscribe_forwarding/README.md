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

## Removing the start-order constraint

The example above hard-codes the dependency: `target_subscriber.py` is
the canonical creator of `obstacle_scans`. If you'd rather have the
three processes start in any order, have **every process that touches
the target service call `open_or_create` on it with the same
parameters** — including the source publisher process. Whichever
process runs first wins the create race; the others open the existing
SHM region:

```python
# Source publisher process: bootstrap the target service before
# constructing the publisher.
_obstacle_scans = (
    node.service_builder(obstacle_scans_name)
    .publish_subscribe(LidarScan)
    .accepts_forwarders_from([raw_scans_name])
    .publisher_mode(iox2.PublisherMode.ForwarderOnly)
    .open_or_create()
)

# Now the publisher attaches to a target service that's guaranteed to
# exist:
raw_scans = (
    node.service_builder(raw_scans_name)
    .publish_subscribe(LidarScan)
    .forwards_into([obstacle_scans_name])
    .open_or_create()
)
publisher = raw_scans.publisher_builder().create()
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
