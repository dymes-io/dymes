//! Dymes client limits.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

// Default forward cursor batch size - translates to (max) batch buffer of 32MiB per cursor
pub const default_forward_cursor_batch_size = 16;

// Maximum forward cursor batch size - translates to (max) batch buffer of 64MiB per cursor
pub const max_forward_cursor_batch_size = 32;

/// Maximum number of digits in cursor identifier
pub const max_cursor_id_txt_digits: usize = 23;

/// Short (non-message) HTTP response buffer size
pub const short_result_buffer_size = 512;

const max_msg_frame_size = @import("dymes_msg").limits.max_frame_size;

/// Message (long) HTTP response buffer size
pub const msg_result_buffer_size = max_msg_frame_size * max_forward_cursor_batch_size;

/// JSON write buffer size
pub const json_write_buffer_size = max_msg_frame_size * 1.5;
