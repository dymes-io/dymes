//! Dymes Message Limits.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const assert = std.debug.assert;

/// Upper bound on size of a Dymes message frame.
pub const frame_size_max_upper_bound = 8 * 1024 * 1024; // 8MiB

/// Lower bound on size of a Dymes message frame.
pub const frame_size_max_lower_bound = 1 * 1024 * 1024; // 1MiB

/// Maximum size of a Dymes message frame.
pub const max_frame_size = 2 * 1024 * 1024; // 2MiB

comptime {
    assert(max_frame_size <= frame_size_max_upper_bound and max_frame_size >= frame_size_max_lower_bound);
}
