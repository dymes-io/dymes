// Location of Dymes Message in data store.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const common = @import("dymes_common");
const logging = common.logging;
const Ulid = common.ulid.Ulid;

/// Id of the stored entry
id: Ulid,

/// Segment number
segment_no: u64,

/// Offset in segment message file message file
file_offset: u64,

/// The size of the message frame: frame header + payload (KV headers, KV data + message body) + padding
frame_size: u32,

pub fn format(self: @This(), comptime fmt: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
    if (fmt.len != 0) {
        std.fmt.invalidFmtError(fmt, self);
    }
    try writer.print("{{id={}, segment_no=0x{x}, file_offset=0x{x}, frame_size=0x{x},}}", .{
        self.id, self.segment_no, self.file_offset, self.frame_size,
    });
}
