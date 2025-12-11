//! Dymes Message Index Entry.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const common = @import("dymes_common");
const AllocationError = common.errors.AllocationError;
const logging = common.logging;
const dymes_msg = @import("dymes_msg");
pub const FrameHeader = dymes_msg.FrameHeader;
const KvHeader = dymes_msg.KvHeader;
const Message = dymes_msg.Message;
const Self = @This();

const errors = @import("errors.zig");

pub const idx_entry_size = @sizeOf(FrameHeader) + @sizeOf(u64) + @sizeOf(u64);

/// Index entry buffer
idx_entry: []u8,

/// Frame header pointer into entry buffer
frame_header: *FrameHeader,

/// Offset of related message in its message file
msg_file_offset: u64,

/// Crc64Redis of header and message file offset
entry_crc: u64,

/// Allocates and initializes a message index entry.
///
/// Caller must call `deinit()` to release resources.
pub fn init(allocator: std.mem.Allocator, message_frame: []const u8, offset: u64) AllocationError!Self {
    assert(message_frame.len >= @sizeOf(FrameHeader));
    var idx_entry = allocator.alloc(u8, idx_entry_size) catch return AllocationError.OutOfMemory;
    _ = &idx_entry;
    errdefer allocator.free(idx_entry);
    return initOverlay(idx_entry, message_frame, offset);
}

/// De-initializes a message index entry.
pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    defer allocator.free(self.idx_entry);
}

/// Initialize an index entry in an overlayed buffer, this destroys existing contents of the buffer, use `overlay()`
/// to overlay a buffer non-destructively.
///
/// Do *not* call `deinit()`, this is a _view_ of real index entry
pub fn initOverlay(idx_entry: []u8, message_frame: []const u8, offset: u64) AllocationError!Self {
    assert(message_frame.len >= @sizeOf(FrameHeader));
    @memcpy(idx_entry[0..@sizeOf(FrameHeader)], message_frame[0..@sizeOf(FrameHeader)]);
    const frame_header: *FrameHeader = @ptrCast(@alignCast(idx_entry.ptr));

    // Update message file offset
    const msg_file_offset: *u64 = @ptrCast(@alignCast(idx_entry.ptr + @sizeOf(FrameHeader)));
    msg_file_offset.* = offset;

    // Calculate index entry CRC
    const entry_crc: *u64 = @ptrCast(@alignCast(idx_entry.ptr + idx_entry_size - @sizeOf(u64)));
    entry_crc.* = std.hash.crc.Crc64Redis.hash(idx_entry[0 .. idx_entry_size - @sizeOf(u64)]);

    return .{
        .idx_entry = idx_entry,
        .frame_header = frame_header,
        .msg_file_offset = msg_file_offset.*,
        .entry_crc = entry_crc.*,
    };
}

/// Overlays an existing index entry in a buffer non-destructively.
///
/// Do *not* call `deinit()`, this is a _view_ of real index entry
pub fn overlay(idx_entry: []u8) Self {
    const frame_header: *FrameHeader = @ptrCast(@alignCast(idx_entry.ptr));
    const msg_file_offset: *u64 = @ptrCast(@alignCast(idx_entry.ptr + @sizeOf(FrameHeader)));
    const entry_crc: *u64 = @ptrCast(@alignCast(idx_entry.ptr + idx_entry_size - @sizeOf(u64)));
    return .{
        .idx_entry = idx_entry,
        .frame_header = frame_header,
        .msg_file_offset = msg_file_offset.*,
        .entry_crc = entry_crc.*,
    };
}

/// Checksum failure errors.
pub const ChecksumError = errors.StateError;

/// Verifies the index entry checksum.
pub fn verifyChecksum(self: *const Self) bool {
    const entry_crc = std.hash.crc.Crc64Redis.hash(self.idx_entry[0 .. idx_entry_size - @sizeOf(u64)]);
    return entry_crc == self.entry_crc;
}

const IndexEntry = @This();

/// Formats the message prints to the given writer.
pub fn format(self: *const Self, writer: *std.Io.Writer) !void {
    try writer.print("{{frame_header={any},msg_file_offset={d},entry_crc=0x{x}}}", .{
        self.frame_header,
        self.msg_file_offset,
        self.entry_crc,
    });
}

test "IndexEntry" {
    std.debug.print("test.IndexEntry.smoke\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var ulid_generator = common.ulid.generator();

    var map = std.StringArrayHashMap([]const u8).init(allocator);
    defer map.deinit();

    try map.put("0", "zero");
    try map.put("1", "one");
    try map.put("2", "two");
    try map.put("3", "three");
    try map.put("4", "four");

    const message_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    var allocated_msg =
        try Message.init(
            allocator,
            0,
            try ulid_generator.next(),
            702,
            101,
            message_body,
            .{ .transient_kv_headers = map },
        );
    defer allocated_msg.deinit(allocator);

    // Init index entry

    var allocated_idx_entry = try init(allocator, allocated_msg.frame, 0);
    defer allocated_idx_entry.deinit(allocator);

    try testing.expectEqual(allocated_msg.frame_header.*, allocated_idx_entry.frame_header.*);

    // Checksums

    const overlayed_idx_entry = overlay(allocated_idx_entry.idx_entry);

    allocated_idx_entry.frame_header.channel = 666;

    try testing.expectEqual(false, overlayed_idx_entry.verifyChecksum());
}
