//! Dymes Message Store Limits.
//!
//! These values aren't set in stone, but _should_ be subject to *careful* tuning.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const dymes_msg = @import("dymes_msg");

/// Maximum message size (2MiB)
pub const max_message_size = dymes_msg.limits.max_frame_size;

/// Maximum message file size (2GiB)
pub const max_message_file_size = 2 * 1024 * 1024 * 1024;

/// Maximum number of entries in a message file
///
/// You may fit few entries per file if many large messages are sent.
/// Conversely, you may fill up many mostly empty files with lots of tiny entries.
/// Massaged to factor in size of message index entries yielding ≈ 16MiB in-memory message index buffer
pub const max_message_file_entries = 262_144;

/// Maximum number of active read dataset segments.
///
/// Yields 512MiB cap for segment index buffer, given 8MiB segment index buffer per segment
/// Yields 1GiB cap for in-memory message index buffers, given 16MiB per segment
pub const max_active_read_data_segments = 64;

/// Message file direct read buffer size (4KiB)
pub const message_file_direct_read_buffer_size: usize = 4 * 1024;

/// Message file iterator read buffer size (8KiB)
pub const message_file_iterator_read_buffer_size: usize = 8 * 1024;

// Sanity check knobs
const std = @import("std");
comptime {
    std.debug.assert(max_message_size < max_message_file_size - 128);
}
