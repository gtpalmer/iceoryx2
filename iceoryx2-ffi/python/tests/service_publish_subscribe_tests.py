# Copyright (c) 2025 Contributors to the Eclipse Foundation
#
# See the NOTICE file(s) distributed with this work for additional
# information regarding copyright ownership.
#
# This program and the accompanying materials are made available under the
# terms of the Apache Software License 2.0 which is available at
# https://www.apache.org/licenses/LICENSE-2.0, or the MIT license
# which is available at https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: Apache-2.0 OR MIT

import ctypes

import iceoryx2 as iox2
import pytest

service_types = [iox2.ServiceType.Ipc, iox2.ServiceType.Local]


class Payload(ctypes.Structure):
    _fields_ = [("data", ctypes.c_ubyte)]


class LargePayload(ctypes.Structure):
    _fields_ = [("data", ctypes.c_ulonglong)]


@pytest.mark.parametrize("service_type", service_types)
def test_send_and_receive_with_memmove_works(
    service_type: iox2.ServiceType,
) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)
    number_of_samples = 5

    service_name = iox2.testing.generate_service_name()
    service = (
        node.service_builder(service_name)
        .publish_subscribe(Payload)
        .subscriber_max_buffer_size(number_of_samples)
        .create()
    )

    publisher = service.publisher_builder().create()
    subscriber = service.subscriber_builder().create()
    assert not subscriber.has_samples()

    for i in range(0, number_of_samples):
        send_payload = Payload(data=82 + i)
        sample_uninit = publisher.loan_uninit()
        ctypes.memmove(
            sample_uninit.payload_ptr,
            ctypes.byref(send_payload),
            ctypes.sizeof(Payload),
        )
        sample = sample_uninit.assume_init()
        sample.send()

    assert subscriber.has_samples()

    for i in range(0, number_of_samples):
        assert subscriber.has_samples()
        received_sample = subscriber.receive()
        assert received_sample is not None
        received_payload = Payload(data=0)
        ctypes.memmove(
            ctypes.byref(received_payload),
            received_sample.payload_ptr,
            ctypes.sizeof(Payload),
        )
        assert received_payload.data == 82 + i

    assert not subscriber.has_samples()


@pytest.mark.parametrize("service_type", service_types)
def test_send_copy_and_receive_works(
    service_type: iox2.ServiceType,
) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)
    number_of_samples = 6

    service_name = iox2.testing.generate_service_name()
    service = (
        node.service_builder(service_name)
        .publish_subscribe(Payload)
        .subscriber_max_buffer_size(number_of_samples)
        .create()
    )

    publisher = service.publisher_builder().create()
    subscriber = service.subscriber_builder().create()
    assert not subscriber.has_samples()

    for i in range(0, number_of_samples):
        publisher.send_copy(Payload(data=85 + i))

    assert subscriber.has_samples()

    for i in range(0, number_of_samples):
        received_sample = subscriber.receive()
        assert received_sample.payload().contents.data == 85 + i

    assert not subscriber.has_samples()


@pytest.mark.parametrize("service_type", service_types)
def test_send_with_write_payload_and_receive_works(
    service_type: iox2.ServiceType,
) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)
    number_of_samples = 6

    service_name = iox2.testing.generate_service_name()
    service = (
        node.service_builder(service_name)
        .publish_subscribe(Payload)
        .subscriber_max_buffer_size(number_of_samples)
        .create()
    )

    publisher = service.publisher_builder().create()
    subscriber = service.subscriber_builder().create()
    assert not subscriber.has_samples()

    for i in range(0, number_of_samples):
        sample_uninit = publisher.loan_uninit()
        sample = sample_uninit.write_payload(Payload(data=89 + i))
        sample.send()

    assert subscriber.has_samples()

    for i in range(0, number_of_samples):
        received_sample = subscriber.receive()
        assert received_sample.payload().contents.data == 89 + i

    assert not subscriber.has_samples()


@pytest.mark.parametrize("service_type", service_types)
def test_send_large_payload_works(
    service_type: iox2.ServiceType,
) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)

    service_name = iox2.testing.generate_service_name()
    service = (
        node.service_builder(service_name).publish_subscribe(LargePayload).create()
    )

    publisher = service.publisher_builder().create()
    subscriber = service.subscriber_builder().create()

    send_payload = LargePayload(data=19203182930990147)
    sample_uninit = publisher.loan_uninit()
    ctypes.memmove(sample_uninit.payload_ptr, ctypes.byref(send_payload), 8)
    sample = sample_uninit.assume_init()
    sample.send()

    received_sample = subscriber.receive()
    assert received_sample is not None
    received_payload = LargePayload(data=0)
    ctypes.memmove(ctypes.byref(received_payload), received_sample.payload_ptr, 8)
    assert received_payload.data == send_payload.data


@pytest.mark.parametrize("service_type", service_types)
def test_override_sample_preallocation_to_one_works(
    service_type: iox2.ServiceType,
) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)
    number_of_samples = 6

    service_name = iox2.testing.generate_service_name()
    service = (
        node.service_builder(service_name)
        .publish_subscribe(Payload)
        .subscriber_max_buffer_size(number_of_samples)
        .create()
    )

    publisher = (
        service.publisher_builder()
        .max_loaned_samples(2)
        .override_sample_preallocation(1)
        .create()
    )

    _sample = publisher.loan_uninit()
    with pytest.raises(iox2.LoanError):
        publisher.loan_uninit()


@pytest.mark.parametrize("service_type", service_types)
def test_published_header_is_the_same_as_received_header(
    service_type: iox2.ServiceType,
) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)

    service_name = iox2.testing.generate_service_name()
    service = node.service_builder(service_name).publish_subscribe(Payload).create()

    publisher = service.publisher_builder().create()
    subscriber = service.subscriber_builder().create()

    sample_uninit = publisher.loan_uninit()
    sample = sample_uninit.assume_init()
    send_header = sample.header
    assert send_header.node_id == node.id
    assert send_header.publisher_id == publisher.id
    assert send_header.number_of_elements == 1

    sample.send()

    received_sample = subscriber.receive()
    assert received_sample is not None
    assert received_sample.header == send_header


@pytest.mark.parametrize("service_type", service_types)
def test_custom_user_header_can_be_used(
    service_type: iox2.ServiceType,
) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)

    service_name = iox2.testing.generate_service_name()
    service = (
        node.service_builder(service_name)
        .publish_subscribe(Payload)
        .user_header(Payload)
        .create()
    )

    publisher = service.publisher_builder().create()
    subscriber = service.subscriber_builder().create()

    sample_uninit = publisher.loan_uninit()
    send_user_header_payload = Payload(data=37)
    ctypes.memmove(
        sample_uninit.user_header_ptr, ctypes.byref(send_user_header_payload), 1
    )
    sample = sample_uninit.assume_init()
    sample.send()

    received_sample = subscriber.receive()
    assert received_sample is not None
    assert received_sample.user_header().contents.data == send_user_header_payload.data


@pytest.mark.parametrize("service_type", service_types)
def test_reallocation_fails_when_allocation_strategy_is_static(
    service_type: iox2.ServiceType,
) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)

    service_name = iox2.testing.generate_service_name()
    service = (
        node.service_builder(service_name)
        .publish_subscribe(iox2.Slice[ctypes.c_uint8])
        .create()
    )

    publisher = (
        service.publisher_builder()
        .initial_max_slice_len(8)
        .allocation_strategy(iox2.AllocationStrategy.Static)
        .create()
    )

    try:
        publisher.loan_slice_uninit(8)
    except iox2.LoanError:
        assert False

    with pytest.raises(iox2.LoanError):
        publisher.loan_slice_uninit(9)


@pytest.mark.parametrize("service_type", service_types)
def test_reallocation_works_when_allocation_strategy_is_not_static(
    service_type: iox2.ServiceType,
) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)

    service_name = iox2.testing.generate_service_name()
    service = (
        node.service_builder(service_name)
        .publish_subscribe(iox2.Slice[ctypes.c_uint8])
        .create()
    )

    publisher = (
        service.publisher_builder()
        .initial_max_slice_len(8)
        .allocation_strategy(iox2.AllocationStrategy.PowerOfTwo)
        .create()
    )

    try:
        publisher.loan_slice_uninit(12)
    except iox2.LoanError:
        assert False


@pytest.mark.parametrize("service_type", service_types)
def test_slice_type_forbids_use_of_non_slice_api(
    service_type: iox2.ServiceType,
) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)

    service_name = iox2.testing.generate_service_name()
    service = (
        node.service_builder(service_name)
        .publish_subscribe(iox2.Slice[ctypes.c_uint8])
        .create()
    )

    publisher = service.publisher_builder().create()

    with pytest.raises(AssertionError):
        publisher.loan_uninit()


@pytest.mark.parametrize("service_type", service_types)
def test_non_slice_type_forbids_use_of_slice_api(
    service_type: iox2.ServiceType,
) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)

    service_name = iox2.testing.generate_service_name()
    service = node.service_builder(service_name).publish_subscribe(Payload).create()

    with pytest.raises(AssertionError):
        publisher = service.publisher_builder().initial_max_slice_len(8).create()

    with pytest.raises(AssertionError):
        publisher = (
            service.publisher_builder()
            .allocation_strategy(iox2.AllocationStrategy.PowerOfTwo)
            .create()
        )

    publisher = service.publisher_builder().create()

    with pytest.raises(AssertionError):
        publisher.loan_slice_uninit(1)


@pytest.mark.parametrize("service_type", service_types)
def test_history_is_delivered_with_update_connections(
    service_type: iox2.ServiceType,
) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)
    number_of_samples = 4

    service_name = iox2.testing.generate_service_name()
    service = (
        node.service_builder(service_name)
        .publish_subscribe(Payload)
        .history_size(number_of_samples)
        .subscriber_max_buffer_size(number_of_samples)
        .create()
    )

    publisher = service.publisher_builder().create()

    for i in range(0, number_of_samples):
        publisher.send_copy(Payload(data=85 + i))

    subscriber = service.subscriber_builder().create()
    assert not subscriber.has_samples()

    publisher.update_connections()

    assert subscriber.has_samples()

    for i in range(0, number_of_samples):
        received_sample = subscriber.receive()
        assert received_sample.payload().contents.data == 85 + i

    assert not subscriber.has_samples()


# ----------------------------------------------------------------------
# Publish-subscribe forwarding (Milestone 5c – Python wrapper coverage)
# ----------------------------------------------------------------------


@pytest.mark.parametrize("service_type", service_types)
def test_publisher_mode_native_only_persists(
    service_type: iox2.ServiceType,
) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)

    service_name = iox2.testing.generate_service_name()
    factory = (
        node.service_builder(service_name)
        .publish_subscribe(Payload)
        .publisher_mode(iox2.PublisherMode.NativeOnly)
        .create()
    )
    assert factory is not None

    # Re-opening the same service requesting Mixed should fail because
    # the stored mode is NativeOnly.
    with pytest.raises(Exception):
        node.service_builder(service_name).publish_subscribe(Payload).publisher_mode(
            iox2.PublisherMode.Mixed
        ).open()


@pytest.mark.parametrize("service_type", service_types)
def test_forwards_into_round_trip_via_python_wrapper(
    service_type: iox2.ServiceType,
) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)

    source_name = iox2.testing.generate_service_name()
    target_name = iox2.testing.generate_service_name()

    # Target service accepts forwarders from the source, ForwarderOnly.
    target = (
        node.service_builder(target_name)
        .publish_subscribe(Payload)
        .accepts_forwarders_from([source_name])
        .publisher_mode(iox2.PublisherMode.ForwarderOnly)
        .create()
    )
    # Source declares forwards_into = [target].
    source = (
        node.service_builder(source_name)
        .publish_subscribe(Payload)
        .forwards_into([target_name])
        .create()
    )

    publisher = source.publisher_builder().create()
    source_subscriber = source.subscriber_builder().create()
    target_subscriber = target.subscriber_builder().create()

    publisher.send_copy(Payload(data=42))

    received_source = source_subscriber.receive()
    assert received_source is not None
    received_source.forward_to(target_name)

    # Drive the publisher's retrieve-and-dispatch by another loan.
    _trigger = publisher.loan_uninit()

    received_target = target_subscriber.receive()
    assert received_target is not None
    assert received_target.payload().contents.data == 42


@pytest.mark.parametrize("service_type", service_types)
def test_forward_to_undeclared_target_raises(
    service_type: iox2.ServiceType,
) -> None:
    config = iox2.testing.generate_isolated_config()
    node = iox2.NodeBuilder.new().config(config).create(service_type)

    service_name = iox2.testing.generate_service_name()
    service = (
        node.service_builder(service_name).publish_subscribe(Payload).create()
    )

    publisher = service.publisher_builder().create()
    subscriber = service.subscriber_builder().create()

    publisher.send_copy(Payload(data=7))
    received = subscriber.receive()
    assert received is not None

    # No forwards_into declared; any target is unauthorized.
    unrelated_target = iox2.testing.generate_service_name()
    with pytest.raises(iox2.ForwardError):
        received.forward_to(unrelated_target)
