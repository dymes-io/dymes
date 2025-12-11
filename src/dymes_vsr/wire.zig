//! Dymes wire format types and helpers.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const dymes_common = @import("dymes_common");
const errors = dymes_common.errors;
const dymes_msg = @import("dymes_msg");

pub const AllocationError = errors.AllocationError;
pub const MarshallingError = errors.IoError || errors.AllocationError || errors.StateError;
pub const UnmarshallingError = errors.IoError || errors.AllocationError || errors.StateError;

/// Allocators for use during marshalling and unmarshalling.
pub const Allocators = struct {
    /// Used for allocations that outlive the marshal/unmarshal invocation.
    gpa: std.mem.Allocator,

    /// Used for message frame allocations that outlive the marshal/unmarshal invocation.
    msg_frame_allocator: std.mem.Allocator,
};

/// Log sequence number (lsn).
///
/// The `lsn` is a monotonically increasing log sequence number, starting at 0 (zero), with no gaps allowed.
///
/// Dymes-specific: I considered using the bit-casted ULID of the message to append (u128), but since the VSR protocol
/// requires replicas to only send back an acknowledgment of the operation if-and-only-if all the previous `op-num`s
/// have been received as well (no gaps), we have to use a separate monotonic counter.
pub const LogSequenceNumber = dymes_msg.LogSequenceNumber;

/// Operation number (op-num)
///
/// The `op-number` is a monotonically increasing log sequence number, starting at 1 (one-based), with no gaps allowed.
///
/// Dymes-specific: Essentially, `op-num` = `lsn` + 1.
pub const OpNumber = LogSequenceNumber;

/// Commit number (commit-num).
///
/// The `commit-num` reflects the last committed `op-num`.
pub const CommitNumber = OpNumber;

/// Replica number (replica-num).
///
/// Dymes-specific: We won't support more than 256 replica nodes, so a `u8` is sufficient.
pub const ReplicaNumber = u8;

/// Client identifier (client-id).
pub const ClientId = u64;

/// Client request number (request-num).
pub const RequestNumber = u64;

/// View number (view-num).
pub const ViewNumber = u64;
