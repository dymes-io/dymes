//! Dymes Message Constants.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const headers = @import("headers.zig");

pub const message_frame_header_size = @sizeOf(headers.FrameHeader);
pub const message_kv_header_size = @sizeOf(headers.KvHeader);

/// Universally Unique Lexicographically Sortable Identifier (ULID).
///
/// This is a globally unique identifier that combine timestamp with
/// randomness to achieve high performance and low collision while
/// preserving the ability to be sorted lexicographically.
pub const Ulid = @import("dymes_common").ulid.Ulid;

/// Sentinel value representing an unset (nil) correlation identifier.
pub const NIL_CORRELATION_ID: Ulid = .{ .time = 0x0, .rand = 0x0 };

/// Log sequence number (LSN).
///
/// This is a monotonically increasing log sequence number, starting at 0 (zero), with no gaps allowed.
pub const LogSequenceNumber = headers.LogSequenceNumber;
