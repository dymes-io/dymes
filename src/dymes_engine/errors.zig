//! Dymes Engine errors.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const common_errors = @import("dymes_common").errors;

pub const UsageError = common_errors;
pub const AllocationError = common_errors;
pub const CreationError = common_errors;
pub const AccessError = common_errors;
pub const TimeoutError = common_errors;
pub const StateError = common_errors;
pub const SocketError = common_errors;
pub const NetworkError = common_errors;
pub const ServerError = common_errors;
pub const ClientError = common_errors;
pub const PeerError = common_errors;

const msg_store_errors = @import("dymes_msg_store").errors;

pub const SyncError = msg_store_errors.SyncError;
pub const DatasetError = msg_store_errors.DatasetError;
pub const DatasetScannerError = msg_store_errors.DatasetScannerError;
