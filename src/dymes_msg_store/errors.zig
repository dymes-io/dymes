//! Message store errors.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0
const common_errors = @import("dymes_common").errors;

pub const AllocationError = common_errors.AllocationError;
pub const CreationError = common_errors.CreationError;
pub const AccessError = common_errors.AccessError;
pub const StateError = common_errors.StateError;
pub const UsageError = common_errors.UsageError;
pub const IoError = common_errors.IoError;

pub const SyncError = AccessError || CreationError;

pub const DatasetError = error{
    FailedToStore,
    PartiallyStored,
};

pub const DatasetScannerError = AccessError || AllocationError;
