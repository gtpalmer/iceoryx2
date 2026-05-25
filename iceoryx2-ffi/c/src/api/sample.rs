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
    AssertNonNullHandle, HandleToType, IOX2_OK, PayloadFfi, UserHeaderFfi, c_size_t,
    iox2_publish_subscribe_header_h, iox2_publish_subscribe_header_t, iox2_service_name_ptr,
    iox2_service_type_e,
};

use iceoryx2::sample::{ForwardError, Sample};
use iceoryx2_bb_elementary::static_assert::*;
use iceoryx2_bb_elementary_traits::AsCStr;
use iceoryx2_ffi_macros::{CStrRepr, iceoryx2_ffi};

use core::ffi::{c_char, c_int, c_void};
use core::mem::ManuallyDrop;

/// C mirror of [`ForwardError`] for the sample-forwarding APIs.
#[repr(C)]
#[derive(Copy, Clone, CStrRepr)]
pub enum iox2_forward_error_e {
    /// The named target service is not present in the source service's
    /// `forwards_into` list.
    #[CStr = "target not declared"]
    TARGET_NOT_DECLARED = IOX2_OK as isize + 1,
    /// The sample has already been forwarded to that target (R9
    /// at-most-once invariant).
    #[CStr = "already forwarded"]
    ALREADY_FORWARDED,
    /// The publisher's completion queue (where Forward / DropAndForward
    /// signals flow) is full. Retry later.
    #[CStr = "completion queue full"]
    COMPLETION_QUEUE_FULL,
    /// The connection back to the publisher is no longer valid.
    #[CStr = "publisher unavailable"]
    PUBLISHER_UNAVAILABLE,
}

impl From<ForwardError> for iox2_forward_error_e {
    fn from(value: ForwardError) -> Self {
        match value {
            ForwardError::TargetNotDeclared => Self::TARGET_NOT_DECLARED,
            ForwardError::AlreadyForwarded => Self::ALREADY_FORWARDED,
            ForwardError::CompletionQueueFull => Self::COMPLETION_QUEUE_FULL,
            ForwardError::PublisherUnavailable => Self::PUBLISHER_UNAVAILABLE,
        }
    }
}

/// Returns the static C-string representation of an
/// [`iox2_forward_error_e`].
///
/// # Safety
///
/// * `value` must be a valid variant of [`iox2_forward_error_e`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_forward_error_string(
    value: iox2_forward_error_e,
) -> *const c_char {
    value.as_const_cstr().as_ptr() as *const c_char
}

// BEGIN types definition

pub(super) union SampleUnion {
    ipc: ManuallyDrop<Sample<crate::IpcService, PayloadFfi, UserHeaderFfi>>,
    local: ManuallyDrop<Sample<crate::LocalService, PayloadFfi, UserHeaderFfi>>,
}

impl SampleUnion {
    pub(super) fn new_ipc(sample: Sample<crate::IpcService, PayloadFfi, UserHeaderFfi>) -> Self {
        Self {
            ipc: ManuallyDrop::new(sample),
        }
    }
    pub(super) fn new_local(
        sample: Sample<crate::LocalService, PayloadFfi, UserHeaderFfi>,
    ) -> Self {
        Self {
            local: ManuallyDrop::new(sample),
        }
    }
}

#[repr(C)]
#[repr(align(16))] // alignment of Option<SampleUnion>
pub struct iox2_sample_storage_t {
    internal: [u8; 96], // magic number obtained with size_of::<Option<SampleUnion>>()
}

#[repr(C)]
#[iceoryx2_ffi(SampleUnion)]
pub struct iox2_sample_t {
    service_type: iox2_service_type_e,
    value: iox2_sample_storage_t,
    deleter: fn(*mut iox2_sample_t),
}

impl iox2_sample_t {
    pub(super) fn init(
        &mut self,
        service_type: iox2_service_type_e,
        value: SampleUnion,
        deleter: fn(*mut iox2_sample_t),
    ) {
        self.service_type = service_type;
        self.value.init(value);
        self.deleter = deleter;
    }
}

pub struct iox2_sample_h_t;
/// The owning handle for `iox2_sample_t`. Passing the handle to an function transfers the ownership.
pub type iox2_sample_h = *mut iox2_sample_h_t;
/// The non-owning handle for `iox2_sample_t`. Passing the handle to an function does not transfers the ownership.
pub type iox2_sample_h_ref = *const iox2_sample_h;

impl AssertNonNullHandle for iox2_sample_h {
    fn assert_non_null(self) {
        debug_assert!(!self.is_null());
    }
}

impl AssertNonNullHandle for iox2_sample_h_ref {
    fn assert_non_null(self) {
        debug_assert!(!self.is_null());
        unsafe {
            debug_assert!(!(*self).is_null());
        }
    }
}

impl HandleToType for iox2_sample_h {
    type Target = *mut iox2_sample_t;

    fn as_type(self) -> Self::Target {
        self as *mut _ as _
    }
}

impl HandleToType for iox2_sample_h_ref {
    type Target = *mut iox2_sample_t;

    fn as_type(self) -> Self::Target {
        unsafe { *self as *mut _ as _ }
    }
}

// END type definition

// BEGIN C API

/// cbindgen:ignore
/// Internal API - do not use
/// # Safety
///
/// * `source_struct_ptr` must not be `null` and the struct it is pointing to must be initialized and valid, i.e. not moved or dropped.
/// * `dest_struct_ptr` must not be `null` and the struct it is pointing to must not contain valid data, i.e. initialized. It can be moved or dropped, though.
/// * `dest_handle_ptr` must not be `null`
#[doc(hidden)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_sample_move(
    source_struct_ptr: *mut iox2_sample_t,
    dest_struct_ptr: *mut iox2_sample_t,
    dest_handle_ptr: *mut iox2_sample_h,
) {
    debug_assert!(!source_struct_ptr.is_null());
    debug_assert!(!dest_struct_ptr.is_null());
    debug_assert!(!dest_handle_ptr.is_null());
    unsafe {
        let source = &mut *source_struct_ptr;
        let dest = &mut *dest_struct_ptr;

        dest.service_type = source.service_type;
        dest.value.init(
            source
                .value
                .as_option_mut()
                .take()
                .expect("Source must have a valid sample"),
        );
        dest.deleter = source.deleter;

        *dest_handle_ptr = (*dest_struct_ptr).as_handle();
    }
}

/// Acquires the samples header.
///
/// # Safety
///
/// * `handle` obtained by [`iox2_subscriber_receive()`](crate::iox2_subscriber_receive())
/// * `header_struct_ptr` - Must be either a NULL pointer or a pointer to a valid
///   [`iox2_publish_subscribe_header_t`]. If it is a NULL pointer, the storage will be allocated on the heap.
/// * `header_handle_ptr` valid pointer to a [`iox2_publish_subscribe_header_h`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_sample_header(
    handle: iox2_sample_h_ref,
    header_struct_ptr: *mut iox2_publish_subscribe_header_t,
    header_handle_ptr: *mut iox2_publish_subscribe_header_h,
) {
    handle.assert_non_null();
    debug_assert!(!header_handle_ptr.is_null());

    fn no_op(_: *mut iox2_publish_subscribe_header_t) {}
    let mut deleter: fn(*mut iox2_publish_subscribe_header_t) = no_op;
    let mut storage_ptr = header_struct_ptr;
    if header_struct_ptr.is_null() {
        deleter = iox2_publish_subscribe_header_t::dealloc;
        storage_ptr = iox2_publish_subscribe_header_t::alloc();
    }
    debug_assert!(!storage_ptr.is_null());
    unsafe {
        let sample = &mut *handle.as_type();

        let header = *match sample.service_type {
            iox2_service_type_e::IPC => sample.value.as_mut().ipc.header(),
            iox2_service_type_e::LOCAL => sample.value.as_mut().local.header(),
        };

        (*storage_ptr).init(header, deleter);
        *header_handle_ptr = (*storage_ptr).as_handle();
    }
}

/// Acquires the samples user header.
///
/// # Safety
///
/// * `handle` obtained by [`iox2_subscriber_receive()`](crate::iox2_subscriber_receive())
/// * `header_ptr` a valid, non-null pointer pointing to a [`*const c_void`] pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_sample_user_header(
    handle: iox2_sample_h_ref,
    header_ptr: *mut *const c_void,
) {
    handle.assert_non_null();
    debug_assert!(!header_ptr.is_null());
    unsafe {
        let sample = &mut *handle.as_type();

        let header = match sample.service_type {
            iox2_service_type_e::IPC => sample.value.as_mut().ipc.user_header(),
            iox2_service_type_e::LOCAL => sample.value.as_mut().local.user_header(),
        };

        *header_ptr = (header as *const UserHeaderFfi).cast();
    }
}

/// Acquires the samples payload.
///
/// # Safety
///
/// * `handle` obtained by [`iox2_subscriber_receive()`](crate::iox2_subscriber_receive())
/// * `payload_ptr` a valid, non-null pointer pointing to a [`*const c_void`] pointer.
/// * `number_of_elements` (optional) either a null poitner or a valid pointer pointing to a [`c_size_t`] with
///   the number of elements of the underlying type
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_sample_payload(
    handle: iox2_sample_h_ref,
    payload_ptr: *mut *const c_void,
    number_of_elements: *mut c_size_t,
) {
    handle.assert_non_null();
    debug_assert!(!payload_ptr.is_null());
    unsafe {
        let sample = &mut *handle.as_type();
        let payload = sample.value.as_mut().local.payload();

        match sample.service_type {
            iox2_service_type_e::IPC => {
                *payload_ptr = payload.as_ptr().cast();
            }
            iox2_service_type_e::LOCAL => {
                *payload_ptr = payload.as_ptr().cast();
            }
        };

        if !number_of_elements.is_null() {
            *number_of_elements =
                sample.value.as_mut().local.header().number_of_elements() as c_size_t;
        }
    }
}

/// Forward this sample onto the publish-subscribe service identified by
/// `target_name`, without releasing the subscriber's borrow.
///
/// `target_name` must appear in the source service's declared
/// `forwards_into` list. R9 (at-most-once per Sample handle per target)
/// is enforced via a subscriber-process-local bitmap on this handle.
///
/// # Arguments
///
/// * `sample_handle` - A valid non-owning sample handle. The sample is
///   *not* consumed.
/// * `target_name` - The target service name as a
///   [`iox2_service_name_ptr`].
///
/// Returns `IOX2_OK` on success, or an [`iox2_forward_error_e`] value
/// (cast to `c_int`) on failure.
///
/// # Safety
///
/// * `sample_handle` must be a valid handle.
/// * `target_name` must be a valid pointer to an existing service name.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_sample_forward_to(
    sample_handle: iox2_sample_h_ref,
    target_name: iox2_service_name_ptr,
) -> c_int {
    sample_handle.assert_non_null();
    debug_assert!(!target_name.is_null());

    let target = unsafe { &*target_name };

    unsafe {
        let sample = &mut *sample_handle.as_type();
        let result = match sample.service_type {
            iox2_service_type_e::IPC => sample.value.as_mut().ipc.forward_to(target),
            iox2_service_type_e::LOCAL => sample.value.as_mut().local.forward_to(target),
        };
        match result {
            Ok(()) => IOX2_OK,
            Err(e) => iox2_forward_error_e::from(e) as c_int,
        }
    }
}

/// Consume this sample, releasing the subscriber's borrow AND
/// forwarding the bucket onto the named target service in a single
/// fused operation. The sample handle is invalidated regardless of
/// whether the forward portion succeeds; on failure, the natural Drop
/// path runs and the subscriber's borrow is released as a plain Drop
/// (no fanout to the target).
///
/// # Arguments
///
/// * `sample_handle` - An owning sample handle. After this call the
///   handle is invalid.
/// * `target_name` - The target service name.
///
/// Returns `IOX2_OK` on success, or an [`iox2_forward_error_e`] value
/// (cast to `c_int`) on failure (sample is still consumed).
///
/// # Safety
///
/// * `sample_handle` must be a valid owning handle and is consumed by
///   this call; it must not be used in any further function call.
/// * `target_name` must be a valid pointer to an existing service name.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_sample_drop_and_forward_to(
    sample_handle: iox2_sample_h,
    target_name: iox2_service_name_ptr,
) -> c_int {
    debug_assert!(!sample_handle.is_null());
    debug_assert!(!target_name.is_null());

    let target = unsafe { &*target_name };

    unsafe {
        let sample = &mut *sample_handle.as_type();
        let result_code = match sample.service_type {
            iox2_service_type_e::IPC => {
                let typed = ManuallyDrop::take(&mut sample.value.as_mut().ipc);
                match typed.drop_and_forward_to(target) {
                    Ok(()) => IOX2_OK,
                    Err(e) => iox2_forward_error_e::from(e) as c_int,
                }
            }
            iox2_service_type_e::LOCAL => {
                let typed = ManuallyDrop::take(&mut sample.value.as_mut().local);
                match typed.drop_and_forward_to(target) {
                    Ok(()) => IOX2_OK,
                    Err(e) => iox2_forward_error_e::from(e) as c_int,
                }
            }
        };
        (sample.deleter)(sample);
        result_code
    }
}

/// This function needs to be called to destroy the sample!
///
/// # Arguments
///
/// * `sample_handle` - A valid [`iox2_sample_h`]
///
/// # Safety
///
/// * The `sample_handle` is invalid after the return of this function and leads to undefined behavior if used in another function call!
/// * The corresponding [`iox2_sample_t`] can be re-used with a call to
///   [`iox2_subscriber_receive`](crate::iox2_subscriber_receive)!
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iox2_sample_drop(sample_handle: iox2_sample_h) {
    debug_assert!(!sample_handle.is_null());
    unsafe {
        let sample = &mut *sample_handle.as_type();

        match sample.service_type {
            iox2_service_type_e::IPC => {
                ManuallyDrop::drop(&mut sample.value.as_mut().ipc);
            }
            iox2_service_type_e::LOCAL => {
                ManuallyDrop::drop(&mut sample.value.as_mut().local);
            }
        }
        (sample.deleter)(sample);
    }
}

// END C API
