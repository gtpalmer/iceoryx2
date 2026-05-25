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

#![allow(non_camel_case_types)]

use crate::api::{
    AssertNonNullHandle, HandleToType, IOX2_OK, IntoCInt, PayloadFfi, PortFactoryPubSubUnion,
    ServiceBuilderUnion, UserHeaderFfi, c_size_t, iox2_port_factory_pub_sub_h,
    iox2_port_factory_pub_sub_t, iox2_service_builder_pub_sub_h,
    iox2_service_builder_pub_sub_h_ref, iox2_service_name_ptr, iox2_service_type_e,
};
use crate::create_type_details;

use iceoryx2::port::publisher_mode::PublisherMode;
use iceoryx2::prelude::*;
use iceoryx2::service::builder::publish_subscribe::{
    Builder, PublishSubscribeCreateError, PublishSubscribeOpenError,
    PublishSubscribeOpenOrCreateError,
};
use iceoryx2::service::port_factory::publish_subscribe::PortFactory;
use iceoryx2::service::static_config::message_type_details::TypeVariant;
use iceoryx2_bb_elementary_traits::AsCStr;
use iceoryx2_ffi_macros::CStrRepr;
use iceoryx2_log::fatal_panic;

use core::ffi::{c_char, c_int};
use core::mem::ManuallyDrop;
use core::slice;

use super::{iox2_attribute_specifier_h_ref, iox2_attribute_verifier_h_ref};

// BEGIN types definition

#[repr(C)]
#[derive(Copy, Clone, CStrRepr)]
pub enum iox2_pub_sub_open_or_create_error_e {
    #[CStr = "does not exist"]
    O_DOES_NOT_EXIST = IOX2_OK as isize + 1,
    #[CStr = "internal failure"]
    O_INTERNAL_FAILURE,
    #[CStr = "incompatible types"]
    O_INCOMPATIBLE_TYPES,
    #[CStr = "incompatible messaging pattern"]
    O_INCOMPATIBLE_MESSAGING_PATTERN,
    #[CStr = "incompatible attributes"]
    O_INCOMPATIBLE_ATTRIBUTES,
    #[CStr = "does not support requested min buffer size"]
    O_DOES_NOT_SUPPORT_REQUESTED_MIN_BUFFER_SIZE,
    #[CStr = "does not support requested min history size"]
    O_DOES_NOT_SUPPORT_REQUESTED_MIN_HISTORY_SIZE,
    #[CStr = "does not support requested min subscriber borrowed samples"]
    O_DOES_NOT_SUPPORT_REQUESTED_MIN_SUBSCRIBER_BORROWED_SAMPLES,
    #[CStr = "does not support requested amount of publishers"]
    O_DOES_NOT_SUPPORT_REQUESTED_AMOUNT_OF_PUBLISHERS,
    #[CStr = "does not support requested amount of subscribers"]
    O_DOES_NOT_SUPPORT_REQUESTED_AMOUNT_OF_SUBSCRIBERS,
    #[CStr = "does not support requested amount of nodes"]
    O_DOES_NOT_SUPPORT_REQUESTED_AMOUNT_OF_NODES,
    #[CStr = "incompatible overflow behavior"]
    O_INCOMPATIBLE_OVERFLOW_BEHAVIOR,
    #[CStr = "insufficient permissions"]
    O_INSUFFICIENT_PERMISSIONS,
    #[CStr = "service in corrupted state"]
    O_SERVICE_IN_CORRUPTED_STATE,
    #[CStr = "hangs in creation"]
    O_HANGS_IN_CREATION,
    #[CStr = "exceeds max number of nodes"]
    O_EXCEEDS_MAX_NUMBER_OF_NODES,
    #[CStr = "is marked for destruction"]
    O_IS_MARKED_FOR_DESTRUCTION,
    #[CStr = "service in corrupted state"]
    C_SERVICE_IN_CORRUPTED_STATE,
    #[CStr = "subscriber buffer must be larger than history size"]
    C_SUBSCRIBER_BUFFER_MUST_BE_LARGER_THAN_HISTORY_SIZE,
    #[CStr = "already exists"]
    C_ALREADY_EXISTS,
    #[CStr = "insufficient permissions"]
    C_INSUFFICIENT_PERMISSIONS,
    #[CStr = "internal failure"]
    C_INTERNAL_FAILURE,
    #[CStr = "is being created by another instance"]
    C_IS_BEING_CREATED_BY_ANOTHER_INSTANCE,
    #[CStr = "old connection still active"]
    C_OLD_CONNECTION_STILL_ACTIVE,
    #[CStr = "hangs in creation"]
    C_HANGS_IN_CREATION,
    #[CStr = "incompatible publisher mode"]
    O_INCOMPATIBLE_PUBLISHER_MODE,
    #[CStr = "incompatible forwards_into"]
    O_INCOMPATIBLE_FORWARDS_INTO,
    #[CStr = "incompatible accepts_forwarders_from"]
    O_INCOMPATIBLE_ACCEPTS_FORWARDERS_FROM,
    #[CStr = "forwarding targets exceeds capacity"]
    O_FORWARDING_TARGETS_EXCEEDS_CAPACITY,
    #[CStr = "forwarding targets contains duplicate"]
    O_FORWARDING_TARGETS_CONTAINS_DUPLICATE,
    #[CStr = "forwards_into contains self"]
    O_FORWARDING_TARGETS_CONTAINS_SELF,
    #[CStr = "accepts_forwarders_from contains self"]
    O_ACCEPTS_FORWARDERS_FROM_CONTAINS_SELF,
    #[CStr = "forwarding targets exceeds capacity"]
    C_FORWARDING_TARGETS_EXCEEDS_CAPACITY,
    #[CStr = "forwarding targets contains duplicate"]
    C_FORWARDING_TARGETS_CONTAINS_DUPLICATE,
    #[CStr = "forwards_into contains self"]
    C_FORWARDING_TARGETS_CONTAINS_SELF,
    #[CStr = "accepts_forwarders_from contains self"]
    C_ACCEPTS_FORWARDERS_FROM_CONTAINS_SELF,
    #[CStr = "NativeOnly publisher mode cannot accept forwarders"]
    O_NATIVE_ONLY_CANNOT_ACCEPT_FORWARDERS,
    #[CStr = "NativeOnly publisher mode cannot accept forwarders"]
    C_NATIVE_ONLY_CANNOT_ACCEPT_FORWARDERS,
    #[CStr = "ForwarderOnly publisher mode cannot have forwards_into"]
    O_FORWARDER_ONLY_CANNOT_HAVE_FORWARDS_INTO,
    #[CStr = "ForwarderOnly publisher mode cannot have forwards_into"]
    C_FORWARDER_ONLY_CANNOT_HAVE_FORWARDS_INTO,
    #[CStr = "same service is created and removed repeatedly"]
    SYSTEM_IN_FLUX,
}

impl IntoCInt for PublishSubscribeOpenError {
    fn into_c_int(self) -> c_int {
        (match self {
            PublishSubscribeOpenError::DoesNotExist => iox2_pub_sub_open_or_create_error_e::O_DOES_NOT_EXIST,
         PublishSubscribeOpenError::InternalFailure => {
             iox2_pub_sub_open_or_create_error_e::O_INTERNAL_FAILURE
         }
         PublishSubscribeOpenError::IncompatibleTypes => {
             iox2_pub_sub_open_or_create_error_e::O_INCOMPATIBLE_TYPES
         }
         PublishSubscribeOpenError::IncompatibleMessagingPattern => {
             iox2_pub_sub_open_or_create_error_e::O_INCOMPATIBLE_MESSAGING_PATTERN
         }
         PublishSubscribeOpenError::IncompatibleAttributes => {
             iox2_pub_sub_open_or_create_error_e::O_INCOMPATIBLE_ATTRIBUTES
         }
         PublishSubscribeOpenError::DoesNotSupportRequestedMinBufferSize => {
             iox2_pub_sub_open_or_create_error_e::O_DOES_NOT_SUPPORT_REQUESTED_MIN_BUFFER_SIZE
         }
         PublishSubscribeOpenError::DoesNotSupportRequestedMinHistorySize => {
             iox2_pub_sub_open_or_create_error_e::O_DOES_NOT_SUPPORT_REQUESTED_MIN_HISTORY_SIZE
         }
         PublishSubscribeOpenError::DoesNotSupportRequestedMinSubscriberBorrowedSamples => {
             iox2_pub_sub_open_or_create_error_e::O_DOES_NOT_SUPPORT_REQUESTED_MIN_SUBSCRIBER_BORROWED_SAMPLES
         }
         PublishSubscribeOpenError::DoesNotSupportRequestedAmountOfPublishers => {
             iox2_pub_sub_open_or_create_error_e::O_DOES_NOT_SUPPORT_REQUESTED_AMOUNT_OF_PUBLISHERS
         }
         PublishSubscribeOpenError::DoesNotSupportRequestedAmountOfSubscribers => {
             iox2_pub_sub_open_or_create_error_e::O_DOES_NOT_SUPPORT_REQUESTED_AMOUNT_OF_SUBSCRIBERS
         }
         PublishSubscribeOpenError::DoesNotSupportRequestedAmountOfNodes => {
             iox2_pub_sub_open_or_create_error_e::O_DOES_NOT_SUPPORT_REQUESTED_AMOUNT_OF_NODES
         }
         PublishSubscribeOpenError::IncompatibleOverflowBehavior => {
             iox2_pub_sub_open_or_create_error_e::O_INCOMPATIBLE_OVERFLOW_BEHAVIOR
         }
         PublishSubscribeOpenError::InsufficientPermissions => {
             iox2_pub_sub_open_or_create_error_e::O_INSUFFICIENT_PERMISSIONS
         }
         PublishSubscribeOpenError::ServiceInCorruptedState => {
             iox2_pub_sub_open_or_create_error_e::O_SERVICE_IN_CORRUPTED_STATE
         }
         PublishSubscribeOpenError::HangsInCreation => {
             iox2_pub_sub_open_or_create_error_e::O_HANGS_IN_CREATION
         }
         PublishSubscribeOpenError::ExceedsMaxNumberOfNodes => {
             iox2_pub_sub_open_or_create_error_e::O_EXCEEDS_MAX_NUMBER_OF_NODES
         }
         PublishSubscribeOpenError::IsMarkedForDestruction => {
             iox2_pub_sub_open_or_create_error_e::O_IS_MARKED_FOR_DESTRUCTION
         }
         PublishSubscribeOpenError::IncompatiblePublisherMode => {
             iox2_pub_sub_open_or_create_error_e::O_INCOMPATIBLE_PUBLISHER_MODE
         }
         PublishSubscribeOpenError::IncompatibleForwardsInto => {
             iox2_pub_sub_open_or_create_error_e::O_INCOMPATIBLE_FORWARDS_INTO
         }
         PublishSubscribeOpenError::IncompatibleAcceptsForwardersFrom => {
             iox2_pub_sub_open_or_create_error_e::O_INCOMPATIBLE_ACCEPTS_FORWARDERS_FROM
         }
         PublishSubscribeOpenError::ForwardingTargetsExceedsCapacity => {
             iox2_pub_sub_open_or_create_error_e::O_FORWARDING_TARGETS_EXCEEDS_CAPACITY
         }
         PublishSubscribeOpenError::ForwardingTargetsContainsDuplicate => {
             iox2_pub_sub_open_or_create_error_e::O_FORWARDING_TARGETS_CONTAINS_DUPLICATE
         }
         PublishSubscribeOpenError::ForwardingTargetsContainsSelf => {
             iox2_pub_sub_open_or_create_error_e::O_FORWARDING_TARGETS_CONTAINS_SELF
         }
         PublishSubscribeOpenError::AcceptsForwardersFromContainsSelf => {
             iox2_pub_sub_open_or_create_error_e::O_ACCEPTS_FORWARDERS_FROM_CONTAINS_SELF
         }
         PublishSubscribeOpenError::NativeOnlyCannotAcceptForwarders => {
             iox2_pub_sub_open_or_create_error_e::O_NATIVE_ONLY_CANNOT_ACCEPT_FORWARDERS
         }
         PublishSubscribeOpenError::ForwarderOnlyCannotHaveForwardsInto => {
             iox2_pub_sub_open_or_create_error_e::O_FORWARDER_ONLY_CANNOT_HAVE_FORWARDS_INTO
         }
        }) as c_int
    }
}

impl IntoCInt for PublishSubscribeCreateError {
    fn into_c_int(self) -> c_int {
        (match self {
            PublishSubscribeCreateError::ServiceInCorruptedState => {
                iox2_pub_sub_open_or_create_error_e::C_SERVICE_IN_CORRUPTED_STATE
            }
            PublishSubscribeCreateError::SubscriberBufferMustBeLargerThanHistorySize => {
                iox2_pub_sub_open_or_create_error_e::C_SUBSCRIBER_BUFFER_MUST_BE_LARGER_THAN_HISTORY_SIZE
            }
            PublishSubscribeCreateError::AlreadyExists => iox2_pub_sub_open_or_create_error_e::C_ALREADY_EXISTS,
          PublishSubscribeCreateError::InsufficientPermissions => {
             iox2_pub_sub_open_or_create_error_e::C_INSUFFICIENT_PERMISSIONS
         }
            PublishSubscribeCreateError::InternalFailure => {
                iox2_pub_sub_open_or_create_error_e::C_INTERNAL_FAILURE
            }
            PublishSubscribeCreateError::IsBeingCreatedByAnotherInstance => {
                iox2_pub_sub_open_or_create_error_e::C_IS_BEING_CREATED_BY_ANOTHER_INSTANCE
            }
         PublishSubscribeCreateError::HangsInCreation => {
             iox2_pub_sub_open_or_create_error_e::C_HANGS_IN_CREATION
         }
            PublishSubscribeCreateError::ForwardingTargetsExceedsCapacity => {
                iox2_pub_sub_open_or_create_error_e::C_FORWARDING_TARGETS_EXCEEDS_CAPACITY
            }
            PublishSubscribeCreateError::ForwardingTargetsContainsDuplicate => {
                iox2_pub_sub_open_or_create_error_e::C_FORWARDING_TARGETS_CONTAINS_DUPLICATE
            }
            PublishSubscribeCreateError::ForwardingTargetsContainsSelf => {
                iox2_pub_sub_open_or_create_error_e::C_FORWARDING_TARGETS_CONTAINS_SELF
            }
            PublishSubscribeCreateError::AcceptsForwardersFromContainsSelf => {
                iox2_pub_sub_open_or_create_error_e::C_ACCEPTS_FORWARDERS_FROM_CONTAINS_SELF
            }
            PublishSubscribeCreateError::NativeOnlyCannotAcceptForwarders => {
                iox2_pub_sub_open_or_create_error_e::C_NATIVE_ONLY_CANNOT_ACCEPT_FORWARDERS
            }
            PublishSubscribeCreateError::ForwarderOnlyCannotHaveForwardsInto => {
                iox2_pub_sub_open_or_create_error_e::C_FORWARDER_ONLY_CANNOT_HAVE_FORWARDS_INTO
            }
        }) as c_int
    }
}

impl IntoCInt for PublishSubscribeOpenOrCreateError {
    fn into_c_int(self) -> c_int {
        match self {
            PublishSubscribeOpenOrCreateError::PublishSubscribeOpenError(error) => {
                error.into_c_int()
            }
            PublishSubscribeOpenOrCreateError::PublishSubscribeCreateError(error) => {
                error.into_c_int()
            }
            e => e.into_c_int(),
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub enum iox2_type_variant_e {
    FIXED_SIZE,
    DYNAMIC,
}

impl From<&TypeVariant> for iox2_type_variant_e {
    fn from(value: &TypeVariant) -> Self {
        match value {
            TypeVariant::Dynamic => iox2_type_variant_e::DYNAMIC,
            TypeVariant::FixedSize => iox2_type_variant_e::FIXED_SIZE,
        }
    }
}

impl From<iox2_type_variant_e> for TypeVariant {
    fn from(value: iox2_type_variant_e) -> Self {
        const DYNAMIC: usize = iox2_type_variant_e::DYNAMIC as usize;
        const FIXED_SIZE: usize = iox2_type_variant_e::FIXED_SIZE as usize;

        match value as usize {
            DYNAMIC => TypeVariant::Dynamic,
            FIXED_SIZE => TypeVariant::FixedSize,
            e => fatal_panic!("Invalid iox2_type_variant_e value {}", e),
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub enum iox2_type_detail_error_e {
    INVALID_TYPE_NAME = IOX2_OK as isize + 1,
    INVALID_SIZE_OR_ALIGNMENT_VALUE,
}

// END type definition

// BEGIN C API

/// Returns a string literal describing the provided [`iox2_pub_sub_open_or_create_error_e`].
///
/// # Arguments
///
/// * `error` - The error value for which a description should be returned
///
/// # Returns
///
/// A pointer to a null-terminated string containing the error message.
/// The string is stored in the .rodata section of the binary.
///
/// # Safety
///
/// The returned pointer must not be modified or freed and is valid as long as the program runs.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_pub_sub_open_or_create_error_string(
    error: iox2_pub_sub_open_or_create_error_e,
) -> *const c_char {
    error.as_const_cstr().as_ptr() as *const c_char
}

/// Sets the user header type details for the builder
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid [`iox2_service_builder_pub_sub_h_ref`]
///   obtained by [`iox2_service_builder_pub_sub`](crate::iox2_service_builder_pub_sub).
/// * `type_variant` - The [`iox2_type_variant_e`] for the payload
/// * `type_name_str` - Must string for the type name.
/// * `type_name_len` - The length of the type name string, not including a null
/// * `size` - The size of the payload
/// * `alignment` - The alignment of the payload
///
/// Returns IOX2_OK on success, an [`iox2_type_detail_error_e`] otherwise.
///
/// # Safety
///
/// * `service_builder_handle` must be valid handles
/// * `type_name_str` must be a valid pointer to an utf8 string
/// * `size` and `alignment` must satisfy the Rust `Layout` type requirements
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_set_user_header_type_details(
    service_builder_handle: iox2_service_builder_pub_sub_h_ref,
    type_variant: iox2_type_variant_e,
    type_name_str: *const c_char,
    type_name_len: c_size_t,
    size: c_size_t,
    alignment: c_size_t,
) -> c_int {
    service_builder_handle.assert_non_null();
    unsafe {
        let value = match create_type_details(
            type_variant,
            type_name_str,
            type_name_len,
            size,
            alignment,
        ) {
            Ok(v) => v,
            Err(e) => return e,
        };

        let service_builder_struct = &mut *service_builder_handle.as_type();

        match service_builder_struct.service_type {
            iox2_service_type_e::IPC => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_ipc_pub_sub(
                    service_builder.__internal_set_user_header_type_details(&value),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_local_pub_sub(
                    service_builder.__internal_set_user_header_type_details(&value),
                ));
            }
        }
    }
    IOX2_OK
}

/// Sets the payload type details for the builder
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid [`iox2_service_builder_pub_sub_h_ref`]
///   obtained by [`iox2_service_builder_pub_sub`](crate::iox2_service_builder_pub_sub).
/// * `type_variant` - The [`iox2_type_variant_e`] for the payload
/// * `type_name_str` - Must string for the type name.
/// * `type_name_len` - The length of the type name string, not including a null
/// * `size` - The size of the payload
/// * `alignment` - The alignment of the payload
///
/// Returns IOX2_OK on success, an [`iox2_type_detail_error_e`] otherwise.
///
/// # Safety
///
/// * `service_builder_handle` must be valid handles
/// * `type_name_str` must be a valid pointer to an utf8 string
/// * `size` and `alignment` must satisfy the Rust `Layout` type requirements
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_set_payload_type_details(
    service_builder_handle: iox2_service_builder_pub_sub_h_ref,
    type_variant: iox2_type_variant_e,
    type_name_str: *const c_char,
    type_name_len: c_size_t,
    size: c_size_t,
    alignment: c_size_t,
) -> c_int {
    service_builder_handle.assert_non_null();
    unsafe {
        let value = match create_type_details(
            type_variant,
            type_name_str,
            type_name_len,
            size,
            alignment,
        ) {
            Ok(v) => v,
            Err(e) => return e,
        };

        let service_builder_struct = &mut *service_builder_handle.as_type();

        match service_builder_struct.service_type {
            iox2_service_type_e::IPC => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_ipc_pub_sub(
                    service_builder.__internal_set_payload_type_details(&value),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_local_pub_sub(
                    service_builder.__internal_set_payload_type_details(&value),
                ));
            }
        }
    }
    IOX2_OK
}

/// Sets the max nodes for the builder
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid [`iox2_service_builder_pub_sub_h_ref`]
///   obtained by [`iox2_service_builder_pub_sub`](crate::iox2_service_builder_pub_sub).
/// * `value` - The value to set the max nodes to
///
/// # Safety
///
/// * `service_builder_handle` must be valid handles
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_set_max_nodes(
    service_builder_handle: iox2_service_builder_pub_sub_h_ref,
    value: c_size_t,
) {
    service_builder_handle.assert_non_null();
    unsafe {
        let service_builder_struct = &mut *service_builder_handle.as_type();

        match service_builder_struct.service_type {
            iox2_service_type_e::IPC => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_ipc_pub_sub(
                    service_builder.max_nodes(value),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_local_pub_sub(
                    service_builder.max_nodes(value),
                ));
            }
        }
    }
}

/// Sets the max publishers for the builder
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid [`iox2_service_builder_pub_sub_h_ref`]
///   obtained by [`iox2_service_builder_pub_sub`](crate::iox2_service_builder_pub_sub).
/// * `value` - The value to set the max publishers to
///
/// # Safety
///
/// * `service_builder_handle` must be valid handles
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_set_max_publishers(
    service_builder_handle: iox2_service_builder_pub_sub_h_ref,
    value: c_size_t,
) {
    service_builder_handle.assert_non_null();
    unsafe {
        let service_builder_struct = &mut *service_builder_handle.as_type();

        match service_builder_struct.service_type {
            iox2_service_type_e::IPC => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_ipc_pub_sub(
                    service_builder.max_publishers(value),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_local_pub_sub(
                    service_builder.max_publishers(value),
                ));
            }
        }
    }
}

/// Sets the max subscribers for the builder
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid [`iox2_service_builder_pub_sub_h_ref`]
///   obtained by [`iox2_service_builder_pub_sub`](crate::iox2_service_builder_pub_sub).
/// * `value` - The value to set the max subscribers to
///
/// # Safety
///
/// * `service_builder_handle` must be valid handles
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_set_max_subscribers(
    service_builder_handle: iox2_service_builder_pub_sub_h_ref,
    value: c_size_t,
) {
    service_builder_handle.assert_non_null();
    unsafe {
        let service_builder_struct = &mut *service_builder_handle.as_type();

        match service_builder_struct.service_type {
            iox2_service_type_e::IPC => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_ipc_pub_sub(
                    service_builder.max_subscribers(value),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_local_pub_sub(
                    service_builder.max_subscribers(value),
                ));
            }
        }
    }
}

/// Sets the payload alignment for the builder
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid [`iox2_service_builder_pub_sub_h_ref`]
///   obtained by [`iox2_service_builder_pub_sub`](crate::iox2_service_builder_pub_sub).
/// * `value` - The value to set the payload alignment to
///
/// # Safety
///
/// * `service_builder_handle` must be valid handles
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_set_payload_alignment(
    service_builder_handle: iox2_service_builder_pub_sub_h_ref,
    value: c_size_t,
) {
    service_builder_handle.assert_non_null();
    unsafe {
        let service_builder_struct = &mut *service_builder_handle.as_type();

        match service_builder_struct.service_type {
            iox2_service_type_e::IPC => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_ipc_pub_sub(
                    service_builder.payload_alignment(
                        Alignment::new(value).unwrap_or(Alignment::new_unchecked(8)),
                    ),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_local_pub_sub(
                    service_builder.payload_alignment(
                        Alignment::new(value).unwrap_or(Alignment::new_unchecked(8)),
                    ),
                ));
            }
        }
    }
}

/// Sets the history size
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid [`iox2_service_builder_pub_sub_h_ref`]
///   obtained by [`iox2_service_builder_pub_sub`](crate::iox2_service_builder_pub_sub).
/// * `value` - The value to set the history size to
///
/// # Safety
///
/// * `service_builder_handle` must be valid handles
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_set_history_size(
    service_builder_handle: iox2_service_builder_pub_sub_h_ref,
    value: c_size_t,
) {
    service_builder_handle.assert_non_null();
    unsafe {
        let service_builder_struct = &mut *service_builder_handle.as_type();

        match service_builder_struct.service_type {
            iox2_service_type_e::IPC => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_ipc_pub_sub(
                    service_builder.history_size(value),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_local_pub_sub(
                    service_builder.history_size(value),
                ));
            }
        }
    }
}

/// Sets the subscriber max buffer size
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid [`iox2_service_builder_pub_sub_h_ref`]
///   obtained by [`iox2_service_builder_pub_sub`](crate::iox2_service_builder_pub_sub).
/// * `value` - The value to set the subscriber max buffer size to
///
/// # Safety
///
/// * `service_builder_handle` must be valid handles
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_set_subscriber_max_buffer_size(
    service_builder_handle: iox2_service_builder_pub_sub_h_ref,
    value: c_size_t,
) {
    service_builder_handle.assert_non_null();
    unsafe {
        let service_builder_struct = &mut *service_builder_handle.as_type();

        match service_builder_struct.service_type {
            iox2_service_type_e::IPC => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_ipc_pub_sub(
                    service_builder.subscriber_max_buffer_size(value),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_local_pub_sub(
                    service_builder.subscriber_max_buffer_size(value),
                ));
            }
        }
    }
}

/// Sets the subscriber max borrowed samples
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid [`iox2_service_builder_pub_sub_h_ref`]
///   obtained by [`iox2_service_builder_pub_sub`](crate::iox2_service_builder_pub_sub).
/// * `value` - The value to set the subscriber max borrowed samples to
///
/// # Safety
///
/// * `service_builder_handle` must be valid handles
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_set_subscriber_max_borrowed_samples(
    service_builder_handle: iox2_service_builder_pub_sub_h_ref,
    value: c_size_t,
) {
    service_builder_handle.assert_non_null();
    unsafe {
        let service_builder_struct = &mut *service_builder_handle.as_type();

        match service_builder_struct.service_type {
            iox2_service_type_e::IPC => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_ipc_pub_sub(
                    service_builder.subscriber_max_borrowed_samples(value),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_local_pub_sub(
                    service_builder.subscriber_max_borrowed_samples(value),
                ));
            }
        }
    }
}

/// Enables/disables safe overflow for the service
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid [`iox2_service_builder_pub_sub_h_ref`]
///   obtained by [`iox2_service_builder_pub_sub`](crate::iox2_service_builder_pub_sub).
/// * `value` - defines if safe overflow shall be enabled (true) or not (false)
///
/// # Safety
///
/// * `service_builder_handle` must be valid handles
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_set_enable_safe_overflow(
    service_builder_handle: iox2_service_builder_pub_sub_h_ref,
    value: bool,
) {
    service_builder_handle.assert_non_null();
    unsafe {
        let service_builder_struct = &mut *service_builder_handle.as_type();

        match service_builder_struct.service_type {
            iox2_service_type_e::IPC => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_ipc_pub_sub(
                    service_builder.enable_safe_overflow(value),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_local_pub_sub(
                    service_builder.enable_safe_overflow(value),
                ));
            }
        }
    }
}

/// Controls which kinds of publishers may attach to a publish-subscribe
/// service. See [`PublisherMode`](iceoryx2::port::publisher_mode::PublisherMode)
/// for the full semantics. C mirror of the Rust enum.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub enum iox2_publisher_mode_e {
    /// Both native publishers and publishers participating as
    /// forwarders from other services are permitted. Default.
    MIXED = 0,
    /// Only native publishers are permitted.
    NATIVE_ONLY = 1,
    /// Only forwarding participations from other services are
    /// permitted.
    FORWARDER_ONLY = 2,
}

impl From<iox2_publisher_mode_e> for PublisherMode {
    fn from(value: iox2_publisher_mode_e) -> Self {
        match value {
            iox2_publisher_mode_e::MIXED => PublisherMode::Mixed,
            iox2_publisher_mode_e::NATIVE_ONLY => PublisherMode::NativeOnly,
            iox2_publisher_mode_e::FORWARDER_ONLY => PublisherMode::ForwarderOnly,
        }
    }
}

/// Sets the publisher mode for the publish-subscribe service builder.
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid [`iox2_service_builder_pub_sub_h_ref`]
///   obtained by [`iox2_service_builder_pub_sub`](crate::iox2_service_builder_pub_sub).
/// * `value` - The publisher mode to set (see [`iox2_publisher_mode_e`]).
///
/// # Safety
///
/// * `service_builder_handle` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_set_publisher_mode(
    service_builder_handle: iox2_service_builder_pub_sub_h_ref,
    value: iox2_publisher_mode_e,
) {
    service_builder_handle.assert_non_null();
    unsafe {
        let service_builder_struct = &mut *service_builder_handle.as_type();
        let mode: PublisherMode = value.into();

        match service_builder_struct.service_type {
            iox2_service_type_e::IPC => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_ipc_pub_sub(
                    service_builder.publisher_mode(mode),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_local_pub_sub(
                    service_builder.publisher_mode(mode),
                ));
            }
        }
    }
}

/// Collects an array of `iox2_service_name_ptr` into a `Vec<ServiceName>`,
/// returning `None` if any entry is null.
unsafe fn collect_service_names(
    names_ptr: *const iox2_service_name_ptr,
    count: c_size_t,
) -> Option<alloc::vec::Vec<ServiceName>> {
    if count == 0 {
        return Some(alloc::vec::Vec::new());
    }
    debug_assert!(!names_ptr.is_null());
    let slice = unsafe { slice::from_raw_parts(names_ptr, count as usize) };
    let mut out: alloc::vec::Vec<ServiceName> = alloc::vec::Vec::with_capacity(count as usize);
    for &name_ptr in slice {
        if name_ptr.is_null() {
            return None;
        }
        out.push(*unsafe { &*name_ptr });
    }
    Some(out)
}

/// Declares the list of target services this publish-subscribe service may forward into.
///
/// The list is validated at service creation time (open / create call):
/// duplicates, self-target, oversize lists, and conflicts with
/// `publisher_mode` are reported as structured errors via the
/// open-or-create error path.
///
/// Calling this method with `count = 0` clears any previously-declared
/// targets. Calling it repeatedly replaces the previous list (not
/// additive).
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid
///   [`iox2_service_builder_pub_sub_h_ref`] obtained by
///   [`iox2_service_builder_pub_sub`](crate::iox2_service_builder_pub_sub).
/// * `targets` - Pointer to an array of `count` non-null
///   [`iox2_service_name_ptr`] handles. May be `NULL` if `count == 0`.
/// * `count` - Number of entries in `targets`.
///
/// Returns `IOX2_OK` on success, or `-1` if any entry of `targets` is
/// `NULL`. (Validity of the target service names themselves — existence,
/// type compatibility, etc. — is checked at open / create time.)
///
/// # Safety
///
/// * `service_builder_handle` must be a valid handle.
/// * `targets` must point to `count` valid, non-null
///   `iox2_service_name_ptr` entries (each owned externally by the
///   caller).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_set_forwards_into(
    service_builder_handle: iox2_service_builder_pub_sub_h_ref,
    targets: *const iox2_service_name_ptr,
    count: c_size_t,
) -> c_int {
    service_builder_handle.assert_non_null();

    let names = match unsafe { collect_service_names(targets, count) } {
        Some(v) => v,
        None => return -1,
    };

    unsafe {
        let service_builder_struct = &mut *service_builder_handle.as_type();

        match service_builder_struct.service_type {
            iox2_service_type_e::IPC => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_ipc_pub_sub(
                    service_builder.forwards_into(names),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_local_pub_sub(
                    service_builder.forwards_into(names),
                ));
            }
        }
    }
    IOX2_OK
}

/// Declares the list of source services this publish-subscribe service
/// will accept forwarded buckets from. Symmetric to
/// [`iox2_service_builder_pub_sub_set_forwards_into`].
///
/// Validation (duplicates, self-source, oversize lists, conflicts with
/// `publisher_mode`) happens at open / create time.
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid
///   [`iox2_service_builder_pub_sub_h_ref`].
/// * `sources` - Pointer to an array of `count` non-null
///   [`iox2_service_name_ptr`] handles. May be `NULL` if `count == 0`.
/// * `count` - Number of entries in `sources`.
///
/// Returns `IOX2_OK` on success, or `-1` if any entry of `sources` is
/// `NULL`.
///
/// # Safety
///
/// * `service_builder_handle` must be a valid handle.
/// * `sources` must point to `count` valid, non-null
///   `iox2_service_name_ptr` entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_set_accepts_forwarders_from(
    service_builder_handle: iox2_service_builder_pub_sub_h_ref,
    sources: *const iox2_service_name_ptr,
    count: c_size_t,
) -> c_int {
    service_builder_handle.assert_non_null();

    let names = match unsafe { collect_service_names(sources, count) } {
        Some(v) => v,
        None => return -1,
    };

    unsafe {
        let service_builder_struct = &mut *service_builder_handle.as_type();

        match service_builder_struct.service_type {
            iox2_service_type_e::IPC => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().ipc);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_ipc_pub_sub(
                    service_builder.accepts_forwarders_from(names),
                ));
            }
            iox2_service_type_e::LOCAL => {
                let service_builder =
                    ManuallyDrop::take(&mut service_builder_struct.value.as_mut().local);

                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);
                service_builder_struct.set(ServiceBuilderUnion::new_local_pub_sub(
                    service_builder.accepts_forwarders_from(names),
                ));
            }
        }
    }
    IOX2_OK
}

/// Opens a publish-subscribe service or creates the service if it does not exist and returns a port factory to create publishers and subscribers.
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid [`iox2_service_builder_pub_sub_h`]
///   obtained by [`iox2_service_builder_pub_sub`](crate::iox2_service_builder_pub_sub)
/// * `port_factory_struct_ptr` - Must be either a NULL pointer or a pointer to a valid
///   [`iox2_port_factory_pub_sub_t`]. If it is a NULL pointer, the storage will be allocated on the heap.
/// * `port_factory_handle_ptr` - An uninitialized or dangling [`iox2_port_factory_pub_sub_h`] handle which will be initialized by this function call.
///
/// Returns IOX2_OK on success, an [`iox2_pub_sub_open_or_create_error_e`] otherwise.
///
/// # Safety
///
/// * The `service_builder_handle` is invalid after the return of this function and leads to undefined behavior if used in another function call!
/// * The corresponding [`iox2_service_builder_t`](crate::iox2_service_builder_t) can be re-used with
///   a call to [`iox2_node_service_builder`](crate::iox2_node_service_builder)!
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_open_or_create(
    service_builder_handle: iox2_service_builder_pub_sub_h,
    port_factory_struct_ptr: *mut iox2_port_factory_pub_sub_t,
    port_factory_handle_ptr: *mut iox2_port_factory_pub_sub_h,
) -> c_int {
    unsafe {
        iox2_service_builder_pub_sub_open_create_impl(
            service_builder_handle,
            port_factory_struct_ptr,
            port_factory_handle_ptr,
            |service_builder| service_builder.open_or_create(),
            |service_builder| service_builder.open_or_create(),
        )
    }
}

/// Opens a publish-subscribe service or creates the service if it does not exist and returns a port factory to create publishers and subscribers.
/// If the service does not exist, the provided arguments are stored inside the services, if the
/// service already exists, the provided attributes are considered as requirements.
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid [`iox2_service_builder_pub_sub_h`]
///   obtained by [`iox2_service_builder_pub_sub`](crate::iox2_service_builder_pub_sub)
/// * `port_factory_struct_ptr` - Must be either a NULL pointer or a pointer to a valid
///   [`iox2_port_factory_pub_sub_t`]. If it is a NULL pointer, the storage will be allocated on the heap.
/// * `port_factory_handle_ptr` - An uninitialized or dangling [`iox2_port_factory_pub_sub_h`] handle which will be initialized by this function call.
///
/// Returns IOX2_OK on success, an [`iox2_pub_sub_open_or_create_error_e`] otherwise.
///
/// # Safety
///
/// * The `service_builder_handle` is invalid after the return of this function and leads to undefined behavior if used in another function call!
/// * The corresponding [`iox2_service_builder_t`](crate::iox2_service_builder_t) can be re-used with
///   a call to [`iox2_node_service_builder`](crate::iox2_node_service_builder)!
/// * The `attribute_verifier_handle` must be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_open_or_create_with_attributes(
    service_builder_handle: iox2_service_builder_pub_sub_h,
    attribute_verifier_handle: iox2_attribute_verifier_h_ref,
    port_factory_struct_ptr: *mut iox2_port_factory_pub_sub_t,
    port_factory_handle_ptr: *mut iox2_port_factory_pub_sub_h,
) -> c_int {
    unsafe {
        let attribute_verifier_struct = &mut *attribute_verifier_handle.as_type();
        let attribute_verifier = &attribute_verifier_struct.value.as_ref().0;

        iox2_service_builder_pub_sub_open_create_impl(
            service_builder_handle,
            port_factory_struct_ptr,
            port_factory_handle_ptr,
            |service_builder| service_builder.open_or_create_with_attributes(attribute_verifier),
            |service_builder| service_builder.open_or_create_with_attributes(attribute_verifier),
        )
    }
}

/// Opens a publish-subscribe service and returns a port factory to create publishers and subscribers.
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid [`iox2_service_builder_pub_sub_h`]
///   obtained by [`iox2_service_builder_pub_sub`](crate::iox2_service_builder_pub_sub)
/// * `port_factory_struct_ptr` - Must be either a NULL pointer or a pointer to a valid
///   [`iox2_port_factory_pub_sub_t`]. If it is a NULL pointer, the storage will be allocated on the heap.
/// * `port_factory_handle_ptr` - An uninitialized or dangling [`iox2_port_factory_pub_sub_h`] handle which will be initialized by this function call.
///
/// Returns IOX2_OK on success, an [`iox2_pub_sub_open_or_create_error_e`] otherwise. Note, only the errors annotated with `O_` are relevant.
///
/// # Safety
///
/// * The `service_builder_handle` is invalid after the return of this function and leads to undefined behavior if used in another function call!
/// * The corresponding [`iox2_service_builder_t`](crate::iox2_service_builder_t) can be re-used with
///   a call to [`iox2_node_service_builder`](crate::iox2_node_service_builder)!
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_open(
    service_builder_handle: iox2_service_builder_pub_sub_h,
    port_factory_struct_ptr: *mut iox2_port_factory_pub_sub_t,
    port_factory_handle_ptr: *mut iox2_port_factory_pub_sub_h,
) -> c_int {
    unsafe {
        iox2_service_builder_pub_sub_open_create_impl(
            service_builder_handle,
            port_factory_struct_ptr,
            port_factory_handle_ptr,
            |service_builder| service_builder.open(),
            |service_builder| service_builder.open(),
        )
    }
}

/// Opens a publish-subscribe service and returns a port factory to create publishers and subscribers.
/// The provided attributes are considered as requirements.
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid [`iox2_service_builder_pub_sub_h`]
///   obtained by [`iox2_service_builder_pub_sub`](crate::iox2_service_builder_pub_sub)
/// * `port_factory_struct_ptr` - Must be either a NULL pointer or a pointer to a valid
///   [`iox2_port_factory_pub_sub_t`]. If it is a NULL pointer, the storage will be allocated on the heap.
/// * `port_factory_handle_ptr` - An uninitialized or dangling [`iox2_port_factory_pub_sub_h`] handle which will be initialized by this function call.
///
/// Returns IOX2_OK on success, an [`iox2_pub_sub_open_or_create_error_e`] otherwise. Note, only the errors annotated with `O_` are relevant.
///
/// # Safety
///
/// * The `service_builder_handle` is invalid after the return of this function and leads to undefined behavior if used in another function call!
/// * The corresponding [`iox2_service_builder_t`](crate::iox2_service_builder_t) can be re-used with
///   a call to [`iox2_node_service_builder`](crate::iox2_node_service_builder)!
/// * The `attribute_verifier_handle` must be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_open_with_attributes(
    service_builder_handle: iox2_service_builder_pub_sub_h,
    attribute_verifier_handle: iox2_attribute_verifier_h_ref,
    port_factory_struct_ptr: *mut iox2_port_factory_pub_sub_t,
    port_factory_handle_ptr: *mut iox2_port_factory_pub_sub_h,
) -> c_int {
    unsafe {
        let attribute_verifier_struct = &mut *attribute_verifier_handle.as_type();
        let attribute_verifier = &attribute_verifier_struct.value.as_ref().0;

        iox2_service_builder_pub_sub_open_create_impl(
            service_builder_handle,
            port_factory_struct_ptr,
            port_factory_handle_ptr,
            |service_builder| service_builder.open_with_attributes(attribute_verifier),
            |service_builder| service_builder.open_with_attributes(attribute_verifier),
        )
    }
}

/// Creates a publish-subscribe service and returns a port factory to create publishers and subscribers.
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid [`iox2_service_builder_pub_sub_h`]
///   obtained by [`iox2_service_builder_pub_sub`](crate::iox2_service_builder_pub_sub)
/// * `port_factory_struct_ptr` - Must be either a NULL pointer or a pointer to a valid
///   [`iox2_port_factory_pub_sub_t`]. If it is a NULL pointer, the storage will be allocated on the heap.
/// * `port_factory_handle_ptr` - An uninitialized or dangling [`iox2_port_factory_pub_sub_h`] handle which will be initialized by this function call.
///
/// Returns IOX2_OK on success, an [`iox2_pub_sub_open_or_create_error_e`] otherwise. Note, only the errors annotated with `C_` are relevant.
///
/// # Safety
///
/// * The `service_builder_handle` is invalid after the return of this function and leads to undefined behavior if used in another function call!
/// * The corresponding [`iox2_service_builder_t`](crate::iox2_service_builder_t) can be re-used with
///   a call to [`iox2_node_service_builder`](crate::iox2_node_service_builder)!
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_create(
    service_builder_handle: iox2_service_builder_pub_sub_h,
    port_factory_struct_ptr: *mut iox2_port_factory_pub_sub_t,
    port_factory_handle_ptr: *mut iox2_port_factory_pub_sub_h,
) -> c_int {
    unsafe {
        iox2_service_builder_pub_sub_open_create_impl(
            service_builder_handle,
            port_factory_struct_ptr,
            port_factory_handle_ptr,
            |service_builder| service_builder.create(),
            |service_builder| service_builder.create(),
        )
    }
}

/// Creates a publish-subscribe service and returns a port factory to create publishers and subscribers.
/// The provided arguments are stored inside the services.
///
/// # Arguments
///
/// * `service_builder_handle` - Must be a valid [`iox2_service_builder_pub_sub_h`]
///   obtained by [`iox2_service_builder_pub_sub`](crate::iox2_service_builder_pub_sub)
/// * `port_factory_struct_ptr` - Must be either a NULL pointer or a pointer to a valid
///   [`iox2_port_factory_pub_sub_t`]. If it is a NULL pointer, the storage will be allocated on the heap.
/// * `port_factory_handle_ptr` - An uninitialized or dangling [`iox2_port_factory_pub_sub_h`] handle which will be initialized by this function call.
///
/// Returns IOX2_OK on success, an [`iox2_pub_sub_open_or_create_error_e`] otherwise. Note, only the errors annotated with `C_` are relevant.
///
/// # Safety
///
/// * The `service_builder_handle` is invalid after the return of this function and leads to undefined behavior if used in another function call!
/// * The corresponding [`iox2_service_builder_t`](crate::iox2_service_builder_t) can be re-used with
///   a call to [`iox2_node_service_builder`](crate::iox2_node_service_builder)!
/// * The `attribute_verifier_handle` must be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_service_builder_pub_sub_create_with_attributes(
    service_builder_handle: iox2_service_builder_pub_sub_h,
    attribute_specifier_handle: iox2_attribute_specifier_h_ref,
    port_factory_struct_ptr: *mut iox2_port_factory_pub_sub_t,
    port_factory_handle_ptr: *mut iox2_port_factory_pub_sub_h,
) -> c_int {
    unsafe {
        let attribute_specifier_struct = &mut *attribute_specifier_handle.as_type();
        let attribute_specifier = &attribute_specifier_struct.value.as_ref().0;

        iox2_service_builder_pub_sub_open_create_impl(
            service_builder_handle,
            port_factory_struct_ptr,
            port_factory_handle_ptr,
            |service_builder| service_builder.create_with_attributes(attribute_specifier),
            |service_builder| service_builder.create_with_attributes(attribute_specifier),
        )
    }
}

unsafe fn iox2_service_builder_pub_sub_open_create_impl<E: IntoCInt>(
    service_builder_handle: iox2_service_builder_pub_sub_h,
    port_factory_struct_ptr: *mut iox2_port_factory_pub_sub_t,
    port_factory_handle_ptr: *mut iox2_port_factory_pub_sub_h,
    func_ipc: impl FnOnce(
        Builder<PayloadFfi, UserHeaderFfi, crate::IpcService>,
    ) -> Result<PortFactory<crate::IpcService, PayloadFfi, UserHeaderFfi>, E>,
    func_local: impl FnOnce(
        Builder<PayloadFfi, UserHeaderFfi, crate::LocalService>,
    )
        -> Result<PortFactory<crate::LocalService, PayloadFfi, UserHeaderFfi>, E>,
) -> c_int {
    service_builder_handle.assert_non_null();
    debug_assert!(!port_factory_handle_ptr.is_null());

    let init_port_factory_struct_ptr =
        |port_factory_struct_ptr: *mut iox2_port_factory_pub_sub_t| {
            let mut port_factory_struct_ptr = port_factory_struct_ptr;
            fn no_op(_: *mut iox2_port_factory_pub_sub_t) {}
            let mut deleter: fn(*mut iox2_port_factory_pub_sub_t) = no_op;
            if port_factory_struct_ptr.is_null() {
                port_factory_struct_ptr = iox2_port_factory_pub_sub_t::alloc();
                deleter = iox2_port_factory_pub_sub_t::dealloc;
            }
            debug_assert!(!port_factory_struct_ptr.is_null());

            (port_factory_struct_ptr, deleter)
        };

    unsafe {
        let service_builder_struct = &mut *service_builder_handle.as_type();
        let service_type = service_builder_struct.service_type;
        let service_builder = service_builder_struct
            .value
            .as_option_mut()
            .take()
            .unwrap_or_else(|| {
                panic!("Trying to use an invalid 'iox2_service_builder_pub_sub_h'!");
            });
        (service_builder_struct.deleter)(service_builder_struct);

        match service_type {
            iox2_service_type_e::IPC => {
                let service_builder = ManuallyDrop::into_inner(service_builder.ipc);
                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);

                match func_ipc(service_builder) {
                    Ok(port_factory) => {
                        let (port_factory_struct_ptr, deleter) =
                            init_port_factory_struct_ptr(port_factory_struct_ptr);
                        (*port_factory_struct_ptr).init(
                            service_type,
                            PortFactoryPubSubUnion::new_ipc(port_factory),
                            deleter,
                        );
                        *port_factory_handle_ptr = (*port_factory_struct_ptr).as_handle();
                    }
                    Err(error) => {
                        return error.into_c_int();
                    }
                }
            }
            iox2_service_type_e::LOCAL => {
                let service_builder = ManuallyDrop::into_inner(service_builder.local);
                let service_builder = ManuallyDrop::into_inner(service_builder.pub_sub);

                match func_local(service_builder) {
                    Ok(port_factory) => {
                        let (port_factory_struct_ptr, deleter) =
                            init_port_factory_struct_ptr(port_factory_struct_ptr);
                        (*port_factory_struct_ptr).init(
                            service_type,
                            PortFactoryPubSubUnion::new_local(port_factory),
                            deleter,
                        );
                        *port_factory_handle_ptr = (*port_factory_struct_ptr).as_handle();
                    }
                    Err(error) => {
                        return error.into_c_int();
                    }
                }
            }
        }
    }
    IOX2_OK
}

// END C API
