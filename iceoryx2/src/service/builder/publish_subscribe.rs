// Copyright (c) 2023 Contributors to the Eclipse Foundation
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

//! # Example
//!
//! See [`crate::service`]
//!
use core::marker::PhantomData;

use alloc::format;

use iceoryx2_bb_elementary::alignment::Alignment;
use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;
use iceoryx2_cal::dynamic_storage::DynamicStorageCreateError;
use iceoryx2_cal::serialize::Serialize;
use iceoryx2_cal::static_storage::StaticStorageLocked;
use iceoryx2_log::{fail, fatal_panic, warn};

use crate::port::publisher_mode::PublisherMode;
use crate::service::dynamic_config::publish_subscribe::DynamicConfigSettings;
use crate::service::header::publish_subscribe::Header;
use crate::service::port_factory::publish_subscribe;
use crate::service::service_name::ServiceName;
use crate::service::static_config::messaging_pattern::MessagingPattern;
use crate::service::static_config::publish_subscribe::{
    ForwardingTargets, ForwardingTargetsError,
};
use crate::service::*;
use crate::service::{self, dynamic_config::MessagingPatternSettings};

use super::{CustomHeaderMarker, CustomPayloadMarker, OpenDynamicStorageFailure, ServiceState};

use self::{
    attribute::{AttributeSpecifier, AttributeVerifier},
    message_type_details::{MessageTypeDetails, TypeDetail, TypeVariant},
};
use builder::RETRY_LIMIT;

/// Errors that can occur when an existing [`MessagingPattern::PublishSubscribe`] [`Service`] shall be opened.
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub enum PublishSubscribeOpenError {
    /// Service could not be openen since it does not exist
    DoesNotExist,
    /// Errors that indicate either an implementation issue or a wrongly configured system.
    InternalFailure,
    /// The [`Service`] has the wrong payload type.
    IncompatibleTypes,
    /// The [`Service`] has the wrong messaging pattern.
    IncompatibleMessagingPattern,
    /// The [`AttributeVerifier`] required attributes that the [`Service`] does not satisfy.
    IncompatibleAttributes,
    /// The [`Service`] has a lower minimum buffer size than requested.
    DoesNotSupportRequestedMinBufferSize,
    /// The [`Service`] has a lower minimum history size than requested.
    DoesNotSupportRequestedMinHistorySize,
    /// The [`Service`] has a lower minimum subscriber borrow size than requested.
    DoesNotSupportRequestedMinSubscriberBorrowedSamples,
    /// The [`Service`] supports less [`Publisher`](crate::port::publisher::Publisher)s than requested.
    DoesNotSupportRequestedAmountOfPublishers,
    /// The [`Service`] supports less [`Subscriber`](crate::port::subscriber::Subscriber)s than requested.
    DoesNotSupportRequestedAmountOfSubscribers,
    /// The [`Service`] supports less [`Node`](crate::node::Node)s than requested.
    DoesNotSupportRequestedAmountOfNodes,
    /// The [`Service`] required overflow behavior is not compatible.
    IncompatibleOverflowBehavior,
    /// The process has not enough permissions to open the [`Service`]
    InsufficientPermissions,
    /// Some underlying resources of the [`Service`] are either missing, corrupted or unaccessible.
    ServiceInCorruptedState,
    /// The [`Service`]s creation timeout has passed and it is still not initialized. Can be caused
    /// by a process that crashed during [`Service`] creation.
    HangsInCreation,
    /// The maximum number of [`Node`](crate::node::Node)s have already opened the [`Service`].
    ExceedsMaxNumberOfNodes,
    /// The [`Service`] is marked for destruction and currently cleaning up since no one is using it anymore.
    /// When the call creation call is repeated with a little delay the [`Service`] should be
    /// recreatable.
    IsMarkedForDestruction,
    /// The [`Service`] has a [`PublisherMode`] that is not compatible with the
    /// one requested by the caller.
    IncompatiblePublisherMode,
    /// The [`Service`] declares a different `forwards_into` set than the caller
    /// requested.
    IncompatibleForwardsInto,
    /// The [`Service`] declares a different `accepts_forwarders_from` set than
    /// the caller requested.
    IncompatibleAcceptsForwardersFrom,
    /// The provided list of forwarding targets exceeds
    /// [`MAX_FORWARDING_TARGETS_PER_SERVICE`](crate::service::static_config::publish_subscribe::MAX_FORWARDING_TARGETS_PER_SERVICE).
    ForwardingTargetsExceedsCapacity,
    /// The provided list of forwarding targets contains a duplicate entry.
    ForwardingTargetsContainsDuplicate,
    /// The `forwards_into` list contains the service's own name.
    ForwardingTargetsContainsSelf,
    /// The `accepts_forwarders_from` list contains the service's own name.
    AcceptsForwardersFromContainsSelf,
    /// The requested [`PublisherMode::NativeOnly`](crate::port::publisher_mode::PublisherMode::NativeOnly)
    /// rejects forwarder participations, but a non-empty
    /// `accepts_forwarders_from` list was also declared. These two settings
    /// are contradictory.
    NativeOnlyCannotAcceptForwarders,
    /// The requested [`PublisherMode::ForwarderOnly`](crate::port::publisher_mode::PublisherMode::ForwarderOnly)
    /// forbids native publishers, so a non-empty `forwards_into` list could
    /// never take effect.
    ForwarderOnlyCannotHaveForwardsInto,
}

impl core::fmt::Display for PublishSubscribeOpenError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "PublishSubscribeOpenError::{self:?}")
    }
}

impl core::error::Error for PublishSubscribeOpenError {}

impl From<ServiceAvailabilityState> for PublishSubscribeOpenError {
    fn from(value: ServiceAvailabilityState) -> Self {
        match value {
            ServiceAvailabilityState::IncompatibleTypes => {
                PublishSubscribeOpenError::IncompatibleTypes
            }
            ServiceAvailabilityState::ServiceState(ServiceState::IncompatibleMessagingPattern) => {
                PublishSubscribeOpenError::IncompatibleMessagingPattern
            }
            ServiceAvailabilityState::ServiceState(ServiceState::InsufficientPermissions) => {
                PublishSubscribeOpenError::InsufficientPermissions
            }
            ServiceAvailabilityState::ServiceState(ServiceState::HangsInCreation) => {
                PublishSubscribeOpenError::HangsInCreation
            }
            ServiceAvailabilityState::ServiceState(ServiceState::Corrupted) => {
                PublishSubscribeOpenError::ServiceInCorruptedState
            }
        }
    }
}

/// Errors that can occur when a new [`MessagingPattern::PublishSubscribe`] [`Service`] shall be created.
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub enum PublishSubscribeCreateError {
    /// Some underlying resources of the [`Service`] are either missing, corrupted or unaccessible.
    ServiceInCorruptedState,
    /// Invalid [`Service`] configuration provided. The
    /// [`Subscriber`](crate::port::subscriber::Subscriber)s buffer size must be at least the size
    /// of the history. Otherwise, how could it hold the whole history?
    SubscriberBufferMustBeLargerThanHistorySize,
    /// The [`Service`] already exists.
    AlreadyExists,
    /// The process has insufficient permissions to create the [`Service`].
    InsufficientPermissions,
    /// Errors that indicate either an implementation issue or a wrongly configured system.
    InternalFailure,
    /// Multiple processes are trying to create the same [`Service`].
    IsBeingCreatedByAnotherInstance,
    /// The [`Service`]s creation timeout has passed and it is still not initialized. Can be caused
    /// by a process that crashed during [`Service`] creation.
    HangsInCreation,
    /// The provided list of forwarding targets exceeds
    /// [`MAX_FORWARDING_TARGETS_PER_SERVICE`](crate::service::static_config::publish_subscribe::MAX_FORWARDING_TARGETS_PER_SERVICE).
    ForwardingTargetsExceedsCapacity,
    /// The provided list of forwarding targets contains a duplicate entry.
    ForwardingTargetsContainsDuplicate,
    /// The `forwards_into` list contains the service's own name, which would
    /// produce a self-loop.
    ForwardingTargetsContainsSelf,
    /// The `accepts_forwarders_from` list contains the service's own name,
    /// which would only be meaningful if the service forwarded to itself.
    AcceptsForwardersFromContainsSelf,
    /// The requested [`PublisherMode::NativeOnly`](crate::port::publisher_mode::PublisherMode::NativeOnly)
    /// rejects forwarder participations, but a non-empty
    /// `accepts_forwarders_from` list was also declared. These two settings
    /// are contradictory — a service either accepts forwarders from declared
    /// sources or rejects all forwarders.
    NativeOnlyCannotAcceptForwarders,
    /// The requested [`PublisherMode::ForwarderOnly`](crate::port::publisher_mode::PublisherMode::ForwarderOnly)
    /// forbids native publishers, so a non-empty `forwards_into` list could
    /// never have any runtime effect — `forwards_into` declares the outbound
    /// routes of the service's native publishers, which do not exist under
    /// `ForwarderOnly`. Rejecting at builder time surfaces the dead
    /// declaration rather than silently honoring it.
    ForwarderOnlyCannotHaveForwardsInto,
}

impl core::fmt::Display for PublishSubscribeCreateError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "PublishSubscribeCreateError::{self:?}")
    }
}

impl core::error::Error for PublishSubscribeCreateError {}

impl From<ServiceAvailabilityState> for PublishSubscribeCreateError {
    fn from(value: ServiceAvailabilityState) -> Self {
        match value {
            ServiceAvailabilityState::IncompatibleTypes
            | ServiceAvailabilityState::ServiceState(ServiceState::IncompatibleMessagingPattern) => {
                PublishSubscribeCreateError::AlreadyExists
            }
            ServiceAvailabilityState::ServiceState(ServiceState::InsufficientPermissions) => {
                PublishSubscribeCreateError::InsufficientPermissions
            }
            ServiceAvailabilityState::ServiceState(ServiceState::HangsInCreation) => {
                PublishSubscribeCreateError::HangsInCreation
            }
            ServiceAvailabilityState::ServiceState(ServiceState::Corrupted) => {
                PublishSubscribeCreateError::ServiceInCorruptedState
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
enum ServiceAvailabilityState {
    ServiceState(ServiceState),
    IncompatibleTypes,
}

/// Internal error from [`Builder::resolve_forwarding_lists`], mapped onto
/// either [`PublishSubscribeCreateError`] or [`PublishSubscribeOpenError`]
/// depending on the calling path.
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
enum ForwardingValidationError {
    ExceedsCapacity,
    DuplicateEntry,
    ForwardsIntoContainsSelf,
    AcceptsForwardersFromContainsSelf,
    NativeOnlyCannotAcceptForwarders,
    ForwarderOnlyCannotHaveForwardsInto,
}

impl ForwardingValidationError {
    fn into_create_error(self) -> PublishSubscribeCreateError {
        match self {
            Self::ExceedsCapacity => {
                PublishSubscribeCreateError::ForwardingTargetsExceedsCapacity
            }
            Self::DuplicateEntry => {
                PublishSubscribeCreateError::ForwardingTargetsContainsDuplicate
            }
            Self::ForwardsIntoContainsSelf => {
                PublishSubscribeCreateError::ForwardingTargetsContainsSelf
            }
            Self::AcceptsForwardersFromContainsSelf => {
                PublishSubscribeCreateError::AcceptsForwardersFromContainsSelf
            }
            Self::NativeOnlyCannotAcceptForwarders => {
                PublishSubscribeCreateError::NativeOnlyCannotAcceptForwarders
            }
            Self::ForwarderOnlyCannotHaveForwardsInto => {
                PublishSubscribeCreateError::ForwarderOnlyCannotHaveForwardsInto
            }
        }
    }

    fn into_open_error(self) -> PublishSubscribeOpenError {
        match self {
            Self::ExceedsCapacity => {
                PublishSubscribeOpenError::ForwardingTargetsExceedsCapacity
            }
            Self::DuplicateEntry => {
                PublishSubscribeOpenError::ForwardingTargetsContainsDuplicate
            }
            Self::ForwardsIntoContainsSelf => {
                PublishSubscribeOpenError::ForwardingTargetsContainsSelf
            }
            Self::AcceptsForwardersFromContainsSelf => {
                PublishSubscribeOpenError::AcceptsForwardersFromContainsSelf
            }
            Self::NativeOnlyCannotAcceptForwarders => {
                PublishSubscribeOpenError::NativeOnlyCannotAcceptForwarders
            }
            Self::ForwarderOnlyCannotHaveForwardsInto => {
                PublishSubscribeOpenError::ForwarderOnlyCannotHaveForwardsInto
            }
        }
    }
}

/// Errors that can occur when a [`MessagingPattern::PublishSubscribe`] [`Service`] shall be
/// created or opened.
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub enum PublishSubscribeOpenOrCreateError {
    /// Failures that can occur when an existing [`Service`] could not be opened.
    PublishSubscribeOpenError(PublishSubscribeOpenError),
    /// Failures that can occur when a [`Service`] could not be created.
    PublishSubscribeCreateError(PublishSubscribeCreateError),
    /// Can occur when another process creates and removes the same [`Service`] repeatedly with a
    /// high frequency.
    SystemInFlux,
}

impl From<ServiceAvailabilityState> for PublishSubscribeOpenOrCreateError {
    fn from(value: ServiceAvailabilityState) -> Self {
        PublishSubscribeOpenOrCreateError::PublishSubscribeOpenError(value.into())
    }
}

impl From<PublishSubscribeOpenError> for PublishSubscribeOpenOrCreateError {
    fn from(value: PublishSubscribeOpenError) -> Self {
        Self::PublishSubscribeOpenError(value)
    }
}

impl From<PublishSubscribeCreateError> for PublishSubscribeOpenOrCreateError {
    fn from(value: PublishSubscribeCreateError) -> Self {
        Self::PublishSubscribeCreateError(value)
    }
}

impl core::fmt::Display for PublishSubscribeOpenOrCreateError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "PublishSubscribeOpenOrCreateError::{self:?}")
    }
}

impl core::error::Error for PublishSubscribeOpenOrCreateError {}

/// Builder to create new [`MessagingPattern::PublishSubscribe`] based [`Service`]s
///
/// # Example
///
/// See [`crate::service`]
#[derive(Debug)]
pub struct Builder<
    Payload: Debug + ?Sized + ZeroCopySend,
    UserHeader: Debug + ZeroCopySend,
    ServiceType: service::Service,
> {
    base: builder::BuilderWithServiceType<ServiceType>,
    override_alignment: Option<usize>,
    override_payload_type: Option<TypeDetail>,
    override_user_header_type: Option<TypeDetail>,
    verify_number_of_subscribers: bool,
    verify_number_of_publishers: bool,
    verify_subscriber_max_buffer_size: bool,
    verify_subscriber_max_borrowed_samples: bool,
    verify_publisher_history_size: bool,
    verify_enable_safe_overflow: bool,
    verify_max_nodes: bool,
    verify_publisher_mode: bool,
    verify_forwards_into: bool,
    verify_accepts_forwarders_from: bool,
    requested_forwards_into: Option<alloc::vec::Vec<ServiceName>>,
    requested_accepts_forwarders_from: Option<alloc::vec::Vec<ServiceName>>,
    _data: PhantomData<Payload>,
    _user_header: PhantomData<UserHeader>,
}

impl<
    Payload: Debug + ?Sized + ZeroCopySend,
    UserHeader: Debug + ZeroCopySend,
    ServiceType: service::Service,
> Clone for Builder<Payload, UserHeader, ServiceType>
{
    fn clone(&self) -> Self {
        Self {
            base: self.base.clone(),
            override_alignment: self.override_alignment,
            override_payload_type: self.override_payload_type,
            override_user_header_type: self.override_user_header_type,
            verify_number_of_subscribers: self.verify_number_of_subscribers,
            verify_number_of_publishers: self.verify_number_of_publishers,
            verify_subscriber_max_buffer_size: self.verify_subscriber_max_buffer_size,
            verify_subscriber_max_borrowed_samples: self.verify_subscriber_max_borrowed_samples,
            verify_publisher_history_size: self.verify_publisher_history_size,
            verify_enable_safe_overflow: self.verify_enable_safe_overflow,
            verify_max_nodes: self.verify_max_nodes,
            verify_publisher_mode: self.verify_publisher_mode,
            verify_forwards_into: self.verify_forwards_into,
            verify_accepts_forwarders_from: self.verify_accepts_forwarders_from,
            requested_forwards_into: self.requested_forwards_into.clone(),
            requested_accepts_forwarders_from: self.requested_accepts_forwarders_from.clone(),
            _data: PhantomData,
            _user_header: PhantomData,
        }
    }
}

impl<
    Payload: Debug + ?Sized + ZeroCopySend,
    UserHeader: Debug + ZeroCopySend,
    ServiceType: service::Service,
> Builder<Payload, UserHeader, ServiceType>
{
    pub(crate) fn new(base: builder::BuilderWithServiceType<ServiceType>) -> Self {
        let mut new_self = Self {
            base,
            verify_number_of_publishers: false,
            verify_number_of_subscribers: false,
            verify_subscriber_max_buffer_size: false,
            verify_publisher_history_size: false,
            verify_subscriber_max_borrowed_samples: false,
            verify_enable_safe_overflow: false,
            verify_max_nodes: false,
            verify_publisher_mode: false,
            verify_forwards_into: false,
            verify_accepts_forwarders_from: false,
            requested_forwards_into: None,
            requested_accepts_forwarders_from: None,
            override_alignment: None,
            override_payload_type: None,
            override_user_header_type: None,
            _data: PhantomData,
            _user_header: PhantomData,
        };

        new_self.base.service_config.messaging_pattern = MessagingPattern::PublishSubscribe(
            static_config::publish_subscribe::StaticConfig::new(new_self.base.shared_node.config()),
        );

        new_self
    }

    fn config_details_mut(&mut self) -> &mut static_config::publish_subscribe::StaticConfig {
        match self.base.service_config.messaging_pattern {
            MessagingPattern::PublishSubscribe(ref mut v) => v,
            _ => {
                fatal_panic!(from self, "This should never happen! Accessing wrong messaging pattern in PublishSubscribe builder!");
            }
        }
    }

    fn config_details(&self) -> &static_config::publish_subscribe::StaticConfig {
        match self.base.service_config.messaging_pattern {
            MessagingPattern::PublishSubscribe(ref v) => v,
            _ => {
                fatal_panic!(from self, "This should never happen! Accessing wrong messaging pattern in PublishSubscribe builder!");
            }
        }
    }

    // triggers the underlying is_service_available method to check whether the service described in base is available.
    fn is_service_available(
        &mut self,
        error_msg: &str,
    ) -> Result<Option<(StaticConfig, ServiceType::StaticStorage)>, ServiceAvailabilityState> {
        match self.base.is_service_available(error_msg) {
            Ok(Some((config, storage))) => {
                if !self
                    .config_details()
                    .message_type_details
                    .is_compatible_to(&config.publish_subscribe().message_type_details)
                {
                    fail!(from self, with ServiceAvailabilityState::IncompatibleTypes,
                        "{} since the service offers the type \"{:?}\" which is not compatible to the requested type \"{:?}\".",
                        error_msg, &config.publish_subscribe().message_type_details , self.config_details().message_type_details);
                }

                Ok(Some((config, storage)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(ServiceAvailabilityState::ServiceState(e)),
        }
    }

    /// Sets the user header type of the [`Service`].
    pub fn user_header<M: Debug + ZeroCopySend>(self) -> Builder<Payload, M, ServiceType> {
        unsafe { core::mem::transmute::<Self, Builder<Payload, M, ServiceType>>(self) }
    }

    /// If the [`Service`] is created, it defines the [`Alignment`] of the payload for the service. If
    /// an existing [`Service`] is opened it requires the service to have at least the defined
    /// [`Alignment`]. If the Payload [`Alignment`] is greater than the provided [`Alignment`]
    /// then the Payload [`Alignment`] is used.
    pub fn payload_alignment(mut self, alignment: Alignment) -> Self {
        self.override_alignment = Some(alignment.value());
        self
    }

    /// If the [`Service`] is created, defines the overflow behavior of the service. If an existing
    /// [`Service`] is opened it requires the service to have the defined overflow behavior.
    pub fn enable_safe_overflow(mut self, value: bool) -> Self {
        self.config_details_mut().enable_safe_overflow = value;
        self.verify_enable_safe_overflow = true;
        self
    }

    /// If the [`Service`] is created it defines how many [`crate::sample::Sample`] a
    /// [`crate::port::subscriber::Subscriber`] can borrow at most in parallel. If an existing
    /// [`Service`] is opened it defines the minimum required.
    pub fn subscriber_max_borrowed_samples(mut self, value: usize) -> Self {
        self.config_details_mut().subscriber_max_borrowed_samples = value;
        self.verify_subscriber_max_borrowed_samples = true;
        self
    }

    /// If the [`Service`] is created it defines the maximum history size a
    /// [`crate::port::subscriber::Subscriber`] can request on connection. If an existing
    /// [`Service`] is opened it defines the minimum required.
    pub fn history_size(mut self, value: usize) -> Self {
        self.config_details_mut().history_size = value;
        self.verify_publisher_history_size = true;
        self
    }

    /// If the [`Service`] is created it defines how many [`crate::sample::Sample`] a
    /// [`crate::port::subscriber::Subscriber`] can store in its internal buffer. If an existing
    /// [`Service`] is opened it defines the minimum required.
    pub fn subscriber_max_buffer_size(mut self, value: usize) -> Self {
        self.config_details_mut().subscriber_max_buffer_size = value;
        self.verify_subscriber_max_buffer_size = true;
        self
    }

    /// If the [`Service`] is created it defines how many [`crate::port::subscriber::Subscriber`] shall
    /// be supported at most. If an existing [`Service`] is opened it defines how many
    /// [`crate::port::subscriber::Subscriber`] must be at least supported.
    pub fn max_subscribers(mut self, value: usize) -> Self {
        self.config_details_mut().max_subscribers = value;
        self.verify_number_of_subscribers = true;
        self
    }

    /// If the [`Service`] is created it defines how many [`crate::port::publisher::Publisher`] shall
    /// be supported at most. If an existing [`Service`] is opened it defines how many
    /// [`crate::port::publisher::Publisher`] must be at least supported.
    pub fn max_publishers(mut self, value: usize) -> Self {
        self.config_details_mut().max_publishers = value;
        self.verify_number_of_publishers = true;
        self
    }

    /// If the [`Service`] is created it defines how many [`Node`](crate::node::Node)s shall
    /// be able to open it in parallel. If an existing [`Service`] is opened it defines how many
    /// [`Node`](crate::node::Node)s must be at least supported.
    pub fn max_nodes(mut self, value: usize) -> Self {
        self.config_details_mut().max_nodes = value;
        self.verify_max_nodes = true;
        self
    }

    /// Sets the [`PublisherMode`] for this [`Service`]. When the [`Service`] is
    /// created, this controls whether native publishers, forwarder
    /// participations, or both may attach. When opening an existing
    /// [`Service`], the requested mode must match exactly. Defaults to
    /// [`PublisherMode::Mixed`].
    pub fn publisher_mode(mut self, mode: PublisherMode) -> Self {
        self.config_details_mut().publisher_mode = mode;
        self.verify_publisher_mode = true;
        self
    }

    /// Declares the target services that this [`Service`]'s publishers may
    /// forward into. When the [`Service`] is created, this list defines the
    /// authorized outbound forwarding routes for buckets allocated by this
    /// service's native publishers. When opening an existing [`Service`], the
    /// requested list must match the stored list exactly (order included, since
    /// the position of each entry is the stable target index).
    ///
    /// The list may contain at most
    /// [`MAX_FORWARDING_TARGETS_PER_SERVICE`](crate::service::static_config::publish_subscribe::MAX_FORWARDING_TARGETS_PER_SERVICE)
    /// entries, must not contain duplicates, and must not contain this
    /// service's own name. Violations are surfaced as structured errors when
    /// [`open_or_create`](Self::open_or_create) / [`create`](Self::create) is
    /// called.
    pub fn forwards_into<I>(mut self, targets: I) -> Self
    where
        I: IntoIterator<Item = ServiceName>,
    {
        let collected: alloc::vec::Vec<ServiceName> = targets.into_iter().collect();
        self.requested_forwards_into = Some(collected);
        self.verify_forwards_into = true;
        self
    }

    /// Declares the source services whose forwarded buckets this [`Service`]
    /// will accept. When the [`Service`] is created, this list defines the
    /// authorized inbound forwarding sources. When opening an existing
    /// [`Service`], the requested list must match the stored list exactly
    /// (order included).
    ///
    /// The list may contain at most
    /// [`MAX_FORWARDING_TARGETS_PER_SERVICE`](crate::service::static_config::publish_subscribe::MAX_FORWARDING_TARGETS_PER_SERVICE)
    /// entries, must not contain duplicates, and must not contain this
    /// service's own name. Violations are surfaced as structured errors when
    /// [`open_or_create`](Self::open_or_create) / [`create`](Self::create) is
    /// called.
    pub fn accepts_forwarders_from<I>(mut self, sources: I) -> Self
    where
        I: IntoIterator<Item = ServiceName>,
    {
        let collected: alloc::vec::Vec<ServiceName> = sources.into_iter().collect();
        self.requested_accepts_forwarders_from = Some(collected);
        self.verify_accepts_forwarders_from = true;
        self
    }

    /// Validates configuration and overrides the invalid setting with meaningful values.
    fn adjust_configuration_to_meaningful_values(&mut self) {
        let origin = format!("{self:?}");
        let settings = self.base.service_config.publish_subscribe_mut();

        if settings.subscriber_max_borrowed_samples == 0 {
            warn!(from origin,
                "Setting the subscribers max borrowed samples to 0 is not supported. Adjust it to 1, the smallest supported value.");
            settings.subscriber_max_borrowed_samples = 1;
        }

        if settings.subscriber_max_buffer_size == 0 {
            warn!(from origin,
                "Setting the subscribers buffer size to 0 is not supported. Adjust it to 1, the smallest supported value.");
            settings.subscriber_max_buffer_size = 1;
        }

        if settings.max_subscribers == 0 {
            warn!(from origin,
                "Setting the maximum amount of subscribers to 0 is not supported. Adjust it to 1, the smallest supported value.");
            settings.max_subscribers = 1;
        }

        if settings.max_publishers == 0 {
            warn!(from origin,
                "Setting the maximum amount of publishers to 0 is not supported. Adjust it to 1, the smallest supported value.");
            settings.max_publishers = 1;
        }

        if settings.max_nodes == 0 {
            warn!(from origin,
                "Setting the maximum amount of nodes to 0 is not supported. Adjust it to 1, the smallest supported value.");
            settings.max_nodes = 1;
        }
    }

    /// Materializes any `requested_forwards_into` /
    /// `requested_accepts_forwarders_from` lists into validated
    /// [`ForwardingTargets`] values stored in the static config. Surfaces
    /// structured errors for over-capacity, duplicate, or self-referential
    /// lists.
    fn resolve_forwarding_lists(
        &mut self,
    ) -> Result<(), ForwardingValidationError> {
        let own_name = *self.base.service_config.name();
        let targets_opt = self.requested_forwards_into.clone();
        let sources_opt = self.requested_accepts_forwarders_from.clone();

        if let Some(targets) = targets_opt {
            if targets.iter().any(|n| n == &own_name) {
                return Err(ForwardingValidationError::ForwardsIntoContainsSelf);
            }
            let list = ForwardingTargets::from_slice(&targets).map_err(|e| match e {
                ForwardingTargetsError::ExceedsCapacity => {
                    ForwardingValidationError::ExceedsCapacity
                }
                ForwardingTargetsError::DuplicateEntry => {
                    ForwardingValidationError::DuplicateEntry
                }
            })?;
            self.config_details_mut().forwards_into = list;
        }
        if let Some(sources) = sources_opt {
            if sources.iter().any(|n| n == &own_name) {
                return Err(ForwardingValidationError::AcceptsForwardersFromContainsSelf);
            }
            let list = ForwardingTargets::from_slice(&sources).map_err(|e| match e {
                ForwardingTargetsError::ExceedsCapacity => {
                    ForwardingValidationError::ExceedsCapacity
                }
                ForwardingTargetsError::DuplicateEntry => {
                    ForwardingValidationError::DuplicateEntry
                }
            })?;
            self.config_details_mut().accepts_forwarders_from = list;
        }

        // Cross-field consistency: NativeOnly rejects all forwarders, so it
        // cannot be paired with a non-empty accepts_forwarders_from list.
        if self.config_details().publisher_mode == PublisherMode::NativeOnly
            && !self.config_details().accepts_forwarders_from.is_empty()
        {
            return Err(ForwardingValidationError::NativeOnlyCannotAcceptForwarders);
        }

        // ForwarderOnly forbids native publishers, so a non-empty
        // forwards_into list (which only takes effect through native
        // publishers) would be a dead declaration. Reject at builder time so
        // the user sees their mistake instead of being puzzled at runtime.
        if self.config_details().publisher_mode == PublisherMode::ForwarderOnly
            && !self.config_details().forwards_into.is_empty()
        {
            return Err(ForwardingValidationError::ForwarderOnlyCannotHaveForwardsInto);
        }

        Ok(())
    }

    fn verify_service_configuration(
        &self,
        existing_settings: &static_config::StaticConfig,
        verifier: &AttributeVerifier,
    ) -> Result<static_config::publish_subscribe::StaticConfig, PublishSubscribeOpenError> {
        let msg = "Unable to open publish subscribe service";

        let existing_attributes = existing_settings.attributes();
        if let Err(incompatible_key) = verifier.verify_requirements(existing_attributes) {
            fail!(from self, with PublishSubscribeOpenError::IncompatibleAttributes,
                "{} due to incompatible service attribute key \"{}\". The following attributes {:?} are required but the service has the attributes {:?}.",
                msg, incompatible_key, verifier, existing_attributes);
        }

        let required_settings = self.base.service_config.publish_subscribe();
        let existing_settings = match &existing_settings.messaging_pattern {
            MessagingPattern::PublishSubscribe(v) => v,
            p => {
                fail!(from self, with PublishSubscribeOpenError::IncompatibleMessagingPattern,
                "{} since a service with the messaging pattern {:?} exists but MessagingPattern::PublishSubscribe is required.", msg, p);
            }
        };

        if self.verify_number_of_publishers
            && existing_settings.max_publishers < required_settings.max_publishers
        {
            fail!(from self, with PublishSubscribeOpenError::DoesNotSupportRequestedAmountOfPublishers,
                                "{} since the service supports only {} publishers but a support of {} publishers was requested.",
                                msg, existing_settings.max_publishers, required_settings.max_publishers);
        }

        if self.verify_number_of_subscribers
            && existing_settings.max_subscribers < required_settings.max_subscribers
        {
            fail!(from self, with PublishSubscribeOpenError::DoesNotSupportRequestedAmountOfSubscribers,
                                "{} since the service supports only {} subscribers but a support of {} subscribers was requested.",
                                msg, existing_settings.max_subscribers, required_settings.max_subscribers);
        }

        if self.verify_subscriber_max_buffer_size
            && existing_settings.subscriber_max_buffer_size
                < required_settings.subscriber_max_buffer_size
        {
            fail!(from self, with PublishSubscribeOpenError::DoesNotSupportRequestedMinBufferSize,
                                "{} since the service supports only a subscriber buffer size of {} but a buffer size of {} was requested.",
                                msg, existing_settings.subscriber_max_buffer_size, required_settings.subscriber_max_buffer_size);
        }

        if self.verify_publisher_history_size
            && existing_settings.history_size < required_settings.history_size
        {
            fail!(from self, with PublishSubscribeOpenError::DoesNotSupportRequestedMinHistorySize,
                                "{} since the service supports only a history size of {} but a history size of {} was requested.",
                                msg, existing_settings.history_size, required_settings.history_size);
        }

        if self.verify_subscriber_max_borrowed_samples
            && existing_settings.subscriber_max_borrowed_samples
                < required_settings.subscriber_max_borrowed_samples
        {
            fail!(from self, with PublishSubscribeOpenError::DoesNotSupportRequestedMinSubscriberBorrowedSamples,
                                "{} since the service supports only {} borrowed subscriber samples but a {} borrowed subscriber samples were requested.",
                                msg, existing_settings.subscriber_max_borrowed_samples, required_settings.subscriber_max_borrowed_samples);
        }

        if self.verify_enable_safe_overflow
            && existing_settings.enable_safe_overflow != required_settings.enable_safe_overflow
        {
            fail!(from self, with PublishSubscribeOpenError::IncompatibleOverflowBehavior,
                                "{} since the service has an incompatible safe overflow behavior.",
                                msg);
        }

        if self.verify_max_nodes && existing_settings.max_nodes < required_settings.max_nodes {
            fail!(from self, with PublishSubscribeOpenError::DoesNotSupportRequestedAmountOfNodes,
                                "{} since the service supports only {} nodes but {} are required.",
                                msg, existing_settings.max_nodes, required_settings.max_nodes);
        }

        if self.verify_publisher_mode
            && existing_settings.publisher_mode != required_settings.publisher_mode
        {
            fail!(from self, with PublishSubscribeOpenError::IncompatiblePublisherMode,
                "{} since the service publisher_mode is {:?} but {:?} was requested.",
                msg, existing_settings.publisher_mode, required_settings.publisher_mode);
        }

        if self.verify_forwards_into
            && existing_settings.forwards_into != required_settings.forwards_into
        {
            fail!(from self, with PublishSubscribeOpenError::IncompatibleForwardsInto,
                "{} since the service forwards_into list is {:?} but {:?} was requested.",
                msg, existing_settings.forwards_into, required_settings.forwards_into);
        }

        if self.verify_accepts_forwarders_from
            && existing_settings.accepts_forwarders_from
                != required_settings.accepts_forwarders_from
        {
            fail!(from self, with PublishSubscribeOpenError::IncompatibleAcceptsForwardersFrom,
                "{} since the service accepts_forwarders_from list is {:?} but {:?} was requested.",
                msg, existing_settings.accepts_forwarders_from, required_settings.accepts_forwarders_from);
        }

        Ok(*existing_settings)
    }

    fn create_impl(
        &mut self,
        attributes: &AttributeSpecifier,
    ) -> Result<
        publish_subscribe::PortFactory<ServiceType, Payload, UserHeader>,
        PublishSubscribeCreateError,
    > {
        self.adjust_configuration_to_meaningful_values();

        if let Err(e) = self.resolve_forwarding_lists() {
            let msg = "Unable to create publish subscribe service";
            let create_error = e.into_create_error();
            fail!(from self, with create_error,
                "{} since the requested forwarding declarations are invalid ({:?}).",
                msg, e);
        }

        let msg = "Unable to create publish subscribe service";

        if !self.config_details().enable_safe_overflow
            && (self.config_details().subscriber_max_buffer_size
                < self.config_details().history_size)
        {
            fail!(from self, with PublishSubscribeCreateError::SubscriberBufferMustBeLargerThanHistorySize,
                "{} since the history size is greater than the subscriber buffer size. The subscriber buffer size must be always greater or equal to the history size in the non-overflowing setup.", msg);
        }

        match self.is_service_available(msg)? {
            None => {
                let service_tag = self
                    .base
                    .create_node_service_tag(msg, PublishSubscribeCreateError::InternalFailure)?;

                // create static config
                let static_config = match self.base.create_static_config_storage() {
                    Ok(c) => c,
                    Err(StaticStorageCreateError::AlreadyExists) => {
                        fail!(from self, with PublishSubscribeCreateError::AlreadyExists,
                           "{} since the service already exists.", msg);
                    }
                    Err(StaticStorageCreateError::Creation) => {
                        fail!(from self, with PublishSubscribeCreateError::IsBeingCreatedByAnotherInstance,
                            "{} since the service is being created by another instance.", msg);
                    }
                    Err(StaticStorageCreateError::InsufficientPermissions) => {
                        fail!(from self, with PublishSubscribeCreateError::InsufficientPermissions,
                            "{} since the static service information could not be created due to insufficient permissions.", msg);
                    }
                    Err(e) => {
                        fail!(from self, with PublishSubscribeCreateError::InternalFailure,
                            "{} since the static service information could not be created due to an internal failure ({:?}).", msg, e);
                    }
                };

                let pubsub_config = self.base.service_config.publish_subscribe();

                // create dynamic config
                let dynamic_config_setting = DynamicConfigSettings {
                    number_of_publishers: pubsub_config.max_publishers,
                    number_of_subscribers: pubsub_config.max_subscribers,
                };

                let dynamic_config = match self.base.create_dynamic_config_storage(
                    &MessagingPatternSettings::PublishSubscribe(dynamic_config_setting),
                    dynamic_config::publish_subscribe::DynamicConfig::memory_size(
                        &dynamic_config_setting,
                    ),
                    pubsub_config.max_nodes,
                ) {
                    Ok(dynamic_config) => dynamic_config,
                    Err(DynamicStorageCreateError::AlreadyExists) => {
                        fail!(from self, with PublishSubscribeCreateError::ServiceInCorruptedState,
                            "{} since the dynamic config of a previous instance of the service still exists.", msg);
                    }
                    Err(e) => {
                        fail!(from self, with PublishSubscribeCreateError::InternalFailure,
                            "{} since the dynamic service segment could not be created ({:?}).", msg, e);
                    }
                };

                self.base.service_config.attributes = attributes.0.clone();
                let service_config = fail!(from self,
                            when ServiceType::ConfigSerializer::serialize(&self.base.service_config),
                            with PublishSubscribeCreateError::ServiceInCorruptedState,
                            "{} since the configuration could not be serialized.", msg);

                // only unlock the static details when the service is successfully created
                let unlocked_static_details = fail!(from self, when static_config.unlock(service_config.as_slice()),
                            with PublishSubscribeCreateError::ServiceInCorruptedState,
                            "{} since the configuration could not be written to the static storage.", msg);

                unlocked_static_details.release_ownership();
                if let Some(service_tag) = service_tag {
                    service_tag.release_ownership();
                }

                Ok(publish_subscribe::PortFactory::new(
                    service::ServiceState::new(
                        self.base.service_config.clone(),
                        self.base.shared_node.clone(),
                        dynamic_config,
                        unlocked_static_details,
                        NoResource,
                    ),
                ))
            }
            Some(_) => {
                fail!(from self, with PublishSubscribeCreateError::AlreadyExists,
                    "{} since the service already exists.", msg);
            }
        }
    }

    fn open_impl(
        &mut self,
        attributes: &AttributeVerifier,
    ) -> Result<
        publish_subscribe::PortFactory<ServiceType, Payload, UserHeader>,
        PublishSubscribeOpenError,
    > {
        let msg = "Unable to open publish subscribe service";

        if let Err(e) = self.resolve_forwarding_lists() {
            let open_error = e.into_open_error();
            fail!(from self, with open_error,
                "{} since the requested forwarding declarations are invalid ({:?}).",
                msg, e);
        }

        let mut service_open_retry_count = 0;
        loop {
            match self.is_service_available(msg)? {
                None => {
                    fail!(from self, with PublishSubscribeOpenError::DoesNotExist,
                        "{} since the service does not exist.", msg);
                }
                Some((static_config, static_storage)) => {
                    let pub_sub_static_config =
                        self.verify_service_configuration(&static_config, attributes)?;

                    let service_tag = self
                        .base
                        .create_node_service_tag(msg, PublishSubscribeOpenError::InternalFailure)?;

                    let dynamic_config = match self.base.open_dynamic_config_storage() {
                        Ok(v) => v,
                        Err(OpenDynamicStorageFailure::IsMarkedForDestruction) => {
                            fail!(from self, with PublishSubscribeOpenError::IsMarkedForDestruction,
                                "{} since the service is marked for destruction.", msg);
                        }
                        Err(OpenDynamicStorageFailure::ExceedsMaxNumberOfNodes) => {
                            fail!(from self, with PublishSubscribeOpenError::ExceedsMaxNumberOfNodes,
                                "{} since it would exceed the maximum number of supported nodes.", msg);
                        }
                        Err(OpenDynamicStorageFailure::DynamicStorageOpenError(
                            DynamicStorageOpenError::DoesNotExist,
                        )) => {
                            fail!(from self, with PublishSubscribeOpenError::ServiceInCorruptedState,
                                "{} since the dynamic segment of the service is missing.", msg);
                        }
                        Err(e) => {
                            if self.is_service_available(msg)?.is_none() {
                                fail!(from self, with PublishSubscribeOpenError::DoesNotExist,
                                    "{} since the service does not exist.", msg);
                            }

                            service_open_retry_count += 1;

                            if RETRY_LIMIT < service_open_retry_count {
                                fail!(from self, with PublishSubscribeOpenError::ServiceInCorruptedState,
                                "{} since the dynamic service information could not be opened ({:?}). This could indicate a corrupted system or a misconfigured system where services are created/removed with a high frequency.",
                                msg, e);
                            }

                            continue;
                        }
                    };

                    self.base.service_config.messaging_pattern =
                        MessagingPattern::PublishSubscribe(pub_sub_static_config);

                    if let Some(service_tag) = service_tag {
                        service_tag.release_ownership();
                    }

                    return Ok(publish_subscribe::PortFactory::new(
                        service::ServiceState::new(
                            static_config,
                            self.base.shared_node.clone(),
                            dynamic_config,
                            static_storage,
                            NoResource,
                        ),
                    ));
                }
            }
        }
    }

    fn open_or_create_impl(
        mut self,
        verifier: &AttributeVerifier,
    ) -> Result<
        publish_subscribe::PortFactory<ServiceType, Payload, UserHeader>,
        PublishSubscribeOpenOrCreateError,
    > {
        let msg = "Unable to open or create publish subscribe service";

        let mut retry_count = 0;
        loop {
            if RETRY_LIMIT < retry_count {
                fail!(from self,
                      with PublishSubscribeOpenOrCreateError::SystemInFlux,
                      "{} since an instance is creating and removing the same service repeatedly.",
                      msg);
            }
            retry_count += 1;

            match self.is_service_available(msg)? {
                Some(_) => match self.open_impl(verifier) {
                    Ok(factory) => return Ok(factory),
                    Err(PublishSubscribeOpenError::DoesNotExist)
                    // If the service is currently being cleaned up then this process might identify
                    // the service like this. Therefore, it makes sense to retry it multiple times until
                    // the external cleanup process if finished.
                    | Err(PublishSubscribeOpenError::ServiceInCorruptedState)
                    | Err(PublishSubscribeOpenError::IsMarkedForDestruction) => continue,
                    Err(e) => return Err(e.into()),
                },
                None => {
                    match self
                        .create_impl(&AttributeSpecifier(verifier.required_attributes().clone()))
                    {
                        Ok(factory) => return Ok(factory),
                        Err(PublishSubscribeCreateError::AlreadyExists)
                        // If the service is currently being cleaned up then this process might identify
                        // the service like this. Therefore, it makes sense to retry it multiple times until
                        // the external cleanup process if finished.
                        | Err(PublishSubscribeCreateError::ServiceInCorruptedState)
                        | Err(PublishSubscribeCreateError::IsBeingCreatedByAnotherInstance) => {
                            continue;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
    }

    fn adjust_payload_alignment(&mut self) {
        if let Some(alignment) = self.override_alignment {
            self.config_details_mut()
                .message_type_details
                .payload
                .alignment = self
                .config_details()
                .message_type_details
                .payload
                .alignment
                .max(alignment);
        }
    }
}

impl<UserHeader: Debug + ZeroCopySend, ServiceType: service::Service>
    Builder<[CustomPayloadMarker], UserHeader, ServiceType>
{
    #[doc(hidden)]
    pub unsafe fn __internal_set_payload_type_details(mut self, value: &TypeDetail) -> Self {
        self.override_payload_type = Some(*value);
        self
    }
}

impl<Payload: Debug + ?Sized + ZeroCopySend, ServiceType: service::Service>
    Builder<Payload, CustomHeaderMarker, ServiceType>
{
    #[doc(hidden)]
    pub unsafe fn __internal_set_user_header_type_details(mut self, value: &TypeDetail) -> Self {
        self.override_user_header_type = Some(*value);
        self
    }
}

impl<Payload: Debug + ZeroCopySend, UserHeader: Debug + ZeroCopySend, ServiceType: service::Service>
    Builder<Payload, UserHeader, ServiceType>
{
    fn prepare_config_details(&mut self) {
        self.config_details_mut().message_type_details =
            MessageTypeDetails::from::<Header, UserHeader, Payload>(TypeVariant::FixedSize);

        if let Some(details) = &self.override_payload_type {
            self.config_details_mut().message_type_details.payload = *details;
        }

        if let Some(details) = &self.override_user_header_type {
            self.config_details_mut().message_type_details.user_header = *details;
        }

        self.adjust_payload_alignment();
    }

    /// If the [`Service`] exists, it will be opened otherwise a new [`Service`] will be
    /// created.
    pub fn open_or_create(
        self,
    ) -> Result<
        publish_subscribe::PortFactory<ServiceType, Payload, UserHeader>,
        PublishSubscribeOpenOrCreateError,
    > {
        self.open_or_create_with_attributes(&AttributeVerifier::new())
    }

    /// If the [`Service`] exists, it will be opened otherwise a new [`Service`] will be
    /// created. It defines a set of attributes.
    ///
    /// If the [`Service`] already exists all attribute requirements must be satisfied,
    /// and service payload type must be the same, otherwise the open process will fail.
    /// If the [`Service`] does not exist the required attributes will be defined in the [`Service`].
    pub fn open_or_create_with_attributes(
        mut self,
        verifier: &AttributeVerifier,
    ) -> Result<
        publish_subscribe::PortFactory<ServiceType, Payload, UserHeader>,
        PublishSubscribeOpenOrCreateError,
    > {
        self.prepare_config_details();
        self.open_or_create_impl(verifier)
    }

    /// Opens an existing [`Service`].
    pub fn open(
        self,
    ) -> Result<
        publish_subscribe::PortFactory<ServiceType, Payload, UserHeader>,
        PublishSubscribeOpenError,
    > {
        self.open_with_attributes(&AttributeVerifier::new())
    }

    /// Opens an existing [`Service`] with attribute requirements. If the defined attribute
    /// requirements are not satisfied the open process will fail.
    pub fn open_with_attributes(
        mut self,
        verifier: &AttributeVerifier,
    ) -> Result<
        publish_subscribe::PortFactory<ServiceType, Payload, UserHeader>,
        PublishSubscribeOpenError,
    > {
        self.prepare_config_details();
        self.open_impl(verifier)
    }

    /// Creates a new [`Service`].
    pub fn create(
        self,
    ) -> Result<
        publish_subscribe::PortFactory<ServiceType, Payload, UserHeader>,
        PublishSubscribeCreateError,
    > {
        self.create_with_attributes(&AttributeSpecifier::new())
    }

    /// Creates a new [`Service`] with a set of attributes.
    pub fn create_with_attributes(
        mut self,
        attributes: &AttributeSpecifier,
    ) -> Result<
        publish_subscribe::PortFactory<ServiceType, Payload, UserHeader>,
        PublishSubscribeCreateError,
    > {
        self.prepare_config_details();
        self.create_impl(attributes)
    }
}

impl<Payload: Debug + ZeroCopySend, UserHeader: Debug + ZeroCopySend, ServiceType: service::Service>
    Builder<[Payload], UserHeader, ServiceType>
{
    fn prepare_config_details(&mut self) {
        self.config_details_mut().message_type_details =
            MessageTypeDetails::from::<Header, UserHeader, Payload>(TypeVariant::Dynamic);

        if let Some(details) = &self.override_payload_type {
            self.config_details_mut().message_type_details.payload = *details;
        }

        if let Some(details) = &self.override_user_header_type {
            self.config_details_mut().message_type_details.user_header = *details;
        }

        self.adjust_payload_alignment();
    }

    /// If the [`Service`] exists, it will be opened otherwise a new [`Service`] will be
    /// created.
    pub fn open_or_create(
        self,
    ) -> Result<
        publish_subscribe::PortFactory<ServiceType, [Payload], UserHeader>,
        PublishSubscribeOpenOrCreateError,
    > {
        self.open_or_create_with_attributes(&AttributeVerifier::new())
    }

    /// If the [`Service`] exists, it will be opened otherwise a new [`Service`] will be
    /// created. It defines a set of attributes. If the [`Service`] already exists all attribute
    /// requirements must be satisfied otherwise the open process will fail. If the [`Service`]
    /// does not exist the required attributes will be defined in the [`Service`].
    pub fn open_or_create_with_attributes(
        mut self,
        attributes: &AttributeVerifier,
    ) -> Result<
        publish_subscribe::PortFactory<ServiceType, [Payload], UserHeader>,
        PublishSubscribeOpenOrCreateError,
    > {
        self.prepare_config_details();
        self.open_or_create_impl(attributes)
    }

    /// Opens an existing [`Service`].
    pub fn open(
        self,
    ) -> Result<
        publish_subscribe::PortFactory<ServiceType, [Payload], UserHeader>,
        PublishSubscribeOpenError,
    > {
        self.open_with_attributes(&AttributeVerifier::new())
    }

    /// Opens an existing [`Service`] with attribute requirements. If the defined attribute
    /// requirements are not satisfied the open process will fail.
    pub fn open_with_attributes(
        mut self,
        attributes: &AttributeVerifier,
    ) -> Result<
        publish_subscribe::PortFactory<ServiceType, [Payload], UserHeader>,
        PublishSubscribeOpenError,
    > {
        self.prepare_config_details();
        self.open_impl(attributes)
    }

    /// Creates a new [`Service`].
    pub fn create(
        self,
    ) -> Result<
        publish_subscribe::PortFactory<ServiceType, [Payload], UserHeader>,
        PublishSubscribeCreateError,
    > {
        self.create_with_attributes(&AttributeSpecifier::new())
    }

    /// Creates a new [`Service`] with a set of attributes.
    pub fn create_with_attributes(
        mut self,
        attributes: &AttributeSpecifier,
    ) -> Result<
        publish_subscribe::PortFactory<ServiceType, [Payload], UserHeader>,
        PublishSubscribeCreateError,
    > {
        self.prepare_config_details();
        self.create_impl(attributes)
    }
}
