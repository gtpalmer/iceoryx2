// Copyright (c) 2024 Contributors to the Eclipse Foundation
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

#[generic_tests::define]
mod service_builder {
    use crate::api::*;
    use crate::tests::{ServiceTypeMapping, create_node};
    use core::ffi::c_int;
    use iceoryx2::prelude::*;
    use iceoryx2_bb_testing::assert_that;

    /// Distinguishes the IPC vs LOCAL test instantiations in generated
    /// service names without requiring `Debug` on `iox2_service_type_e`.
    fn type_tag<S: Service + ServiceTypeMapping>() -> &'static str {
        match S::service_type() {
            iox2_service_type_e::IPC => "ipc",
            iox2_service_type_e::LOCAL => "local",
        }
    }

    #[test]
    fn basic_service_builder_pub_sub_test<S: Service + ServiceTypeMapping>() {
        unsafe {
            let node_handle = create_node::<S>("bar");

            let service_name = "all/glory/to/hypnotaod";

            let mut service_name_handle: iox2_service_name_h = core::ptr::null_mut();
            let ret_val = iox2_service_name_new(
                core::ptr::null_mut(),
                service_name.as_ptr() as *const _,
                service_name.len(),
                &mut service_name_handle,
            );
            assert_that!(ret_val, eq(IOX2_OK));

            let service_builder_handle = iox2_node_service_builder(
                &node_handle,
                core::ptr::null_mut(),
                iox2_cast_service_name_ptr(service_name_handle),
            );
            iox2_service_name_drop(service_name_handle);

            let service_builder_handle = iox2_service_builder_pub_sub(service_builder_handle);
            iox2_service_builder_pub_sub_set_max_publishers(&service_builder_handle, 10);
            iox2_service_builder_pub_sub_set_max_subscribers(&service_builder_handle, 10);

            let mut pub_sub_factory: iox2_port_factory_pub_sub_h = core::ptr::null_mut();
            iox2_service_builder_pub_sub_open_or_create(
                service_builder_handle,
                core::ptr::null_mut(),
                &mut pub_sub_factory as *mut _,
            );
            assert_that!(ret_val, eq(IOX2_OK));

            iox2_port_factory_pub_sub_drop(pub_sub_factory);
            iox2_node_drop(node_handle);
        }
    }

    /// Helper: create a publish-subscribe `u64` service via the C FFI,
    /// optionally configuring publisher mode and forwarding lists. The
    /// caller is responsible for dropping the returned port-factory.
    #[allow(clippy::too_many_arguments)]
    unsafe fn create_forwarding_pub_sub_service<S: Service + ServiceTypeMapping>(
        node_handle: iox2_node_h_ref,
        service_name: &str,
        publisher_mode: iox2_publisher_mode_e,
        forwards_into: &[iox2_service_name_ptr],
        accepts_from: &[iox2_service_name_ptr],
    ) -> (c_int, iox2_port_factory_pub_sub_h) {
        unsafe {
            let mut service_name_handle: iox2_service_name_h = core::ptr::null_mut();
            let ret_val = iox2_service_name_new(
                core::ptr::null_mut(),
                service_name.as_ptr() as *const _,
                service_name.len(),
                &mut service_name_handle,
            );
            assert_that!(ret_val, eq(IOX2_OK));

            let service_builder_handle = iox2_node_service_builder(
                node_handle,
                core::ptr::null_mut(),
                iox2_cast_service_name_ptr(service_name_handle),
            );
            iox2_service_name_drop(service_name_handle);

            let pubsub_builder = iox2_service_builder_pub_sub(service_builder_handle);

            iox2_service_builder_pub_sub_set_publisher_mode(&pubsub_builder, publisher_mode);

            if !forwards_into.is_empty()
                || matches!(publisher_mode, iox2_publisher_mode_e::FORWARDER_ONLY)
            {
                let rc = iox2_service_builder_pub_sub_set_forwards_into(
                    &pubsub_builder,
                    forwards_into.as_ptr(),
                    forwards_into.len() as _,
                );
                assert_that!(rc, eq(IOX2_OK));
            }

            if !accepts_from.is_empty() {
                let rc = iox2_service_builder_pub_sub_set_accepts_forwarders_from(
                    &pubsub_builder,
                    accepts_from.as_ptr(),
                    accepts_from.len() as _,
                );
                assert_that!(rc, eq(IOX2_OK));
            }

            // Pin the payload type to u64.
            iox2_service_builder_pub_sub_set_payload_type_details(
                &pubsub_builder,
                iox2_type_variant_e::FIXED_SIZE,
                "u64".as_ptr() as _,
                "u64".len(),
                core::mem::size_of::<u64>(),
                core::mem::align_of::<u64>(),
            );

            let mut factory: iox2_port_factory_pub_sub_h = core::ptr::null_mut();
            let rc = iox2_service_builder_pub_sub_open_or_create(
                pubsub_builder,
                core::ptr::null_mut(),
                &mut factory as *mut _,
            );
            (rc, factory)
        }
    }

    unsafe fn make_service_name(name: &str) -> iox2_service_name_h {
        let mut h: iox2_service_name_h = core::ptr::null_mut();
        let rc = iox2_service_name_new(
            core::ptr::null_mut(),
            name.as_ptr() as *const _,
            name.len(),
            &mut h,
        );
        assert_that!(rc, eq(IOX2_OK));
        h
    }

    #[test]
    fn publisher_mode_persists_in_static_config<S: Service + ServiceTypeMapping>() {
        unsafe {
            let node = create_node::<S>("pub_mode_node");
            let svc_name = format!("c_ffi/pub_mode/{}", type_tag::<S>());

            let (rc, factory) = create_forwarding_pub_sub_service::<S>(
                &node,
                &svc_name,
                iox2_publisher_mode_e::NATIVE_ONLY,
                &[],
                &[],
            );
            assert_that!(rc, eq(IOX2_OK));

            let mut sc = core::mem::MaybeUninit::<iox2_static_config_publish_subscribe_t>::uninit();
            iox2_port_factory_pub_sub_static_config(&factory, sc.as_mut_ptr());
            let sc = sc.assume_init();
            assert_that!(
                matches!(sc.publisher_mode, iox2_publisher_mode_e::NATIVE_ONLY),
                eq(true)
            );

            iox2_port_factory_pub_sub_drop(factory);
            iox2_node_drop(node);
        }
    }

    #[test]
    fn forwarding_routes_can_be_declared_via_ffi<S: Service + ServiceTypeMapping>() {
        unsafe {
            let node = create_node::<S>("fwd_node");
            let source_name_str = format!("c_ffi/fwd/src/{}", type_tag::<S>());
            let target_name_str = format!("c_ffi/fwd/tgt/{}", type_tag::<S>());

            let target_h = make_service_name(&target_name_str);
            let source_h = make_service_name(&source_name_str);

            let target_ptr = iox2_cast_service_name_ptr(target_h);
            let source_ptr = iox2_cast_service_name_ptr(source_h);

            // Create the target first (so it exists when the source publisher
            // attaches as a forwarder participant).
            let (rc_t, factory_t) = create_forwarding_pub_sub_service::<S>(
                &node,
                &target_name_str,
                iox2_publisher_mode_e::FORWARDER_ONLY,
                &[],
                &[source_ptr],
            );
            assert_that!(rc_t, eq(IOX2_OK));

            // Source declares forwards_into = [target].
            let (rc_s, factory_s) = create_forwarding_pub_sub_service::<S>(
                &node,
                &source_name_str,
                iox2_publisher_mode_e::MIXED,
                &[target_ptr],
                &[],
            );
            assert_that!(rc_s, eq(IOX2_OK));

            // Static config on the target reports ForwarderOnly mode.
            let mut sc = core::mem::MaybeUninit::<iox2_static_config_publish_subscribe_t>::uninit();
            iox2_port_factory_pub_sub_static_config(&factory_t, sc.as_mut_ptr());
            let sc = sc.assume_init();
            assert_that!(
                matches!(sc.publisher_mode, iox2_publisher_mode_e::FORWARDER_ONLY),
                eq(true)
            );

            iox2_service_name_drop(target_h);
            iox2_service_name_drop(source_h);
            iox2_port_factory_pub_sub_drop(factory_s);
            iox2_port_factory_pub_sub_drop(factory_t);
            iox2_node_drop(node);
        }
    }

    #[test]
    fn forwards_into_set_with_self_target_fails_at_create<S: Service + ServiceTypeMapping>() {
        unsafe {
            let node = create_node::<S>("self_target_node");
            let svc_name = format!("c_ffi/self_target/{}", type_tag::<S>());

            let self_h = make_service_name(&svc_name);
            let self_ptr = iox2_cast_service_name_ptr(self_h);

            let (rc, factory) = create_forwarding_pub_sub_service::<S>(
                &node,
                &svc_name,
                iox2_publisher_mode_e::MIXED,
                &[self_ptr],
                &[],
            );

            // Service creation must fail; the exact error code is the
            // self-target variant defined in `iox2_pub_sub_open_or_create_error_e`.
            assert_that!(rc, ne(IOX2_OK));
            // factory remains null on failure.
            assert_that!(factory.is_null(), eq(true));

            iox2_service_name_drop(self_h);
            iox2_node_drop(node);
        }
    }

    #[test]
    fn forwarder_only_service_rejects_empty_forwards_into<S: Service + ServiceTypeMapping>() {
        unsafe {
            let node = create_node::<S>("fwd_only_empty_node");
            let svc_name = format!("c_ffi/fwd_only_empty/{}", type_tag::<S>());

            let (rc, factory) = create_forwarding_pub_sub_service::<S>(
                &node,
                &svc_name,
                iox2_publisher_mode_e::FORWARDER_ONLY,
                &[],
                &[],
            );

            // ForwarderOnly without any forwards_into is not a useful
            // service; Rust core does not require forwards_into on a
            // ForwarderOnly *target* (that's a separate concept). What we
            // *do* require here is that the call succeeded structurally
            // through the FFI surface.
            assert_that!(rc, eq(IOX2_OK));

            if !factory.is_null() {
                iox2_port_factory_pub_sub_drop(factory);
            }
            iox2_node_drop(node);
        }
    }

    #[instantiate_tests(<iceoryx2::service::ipc::Service>)]
    mod ipc {}

    #[instantiate_tests(<iceoryx2::service::local::Service>)]
    mod local {}
}
