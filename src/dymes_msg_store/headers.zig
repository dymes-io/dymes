//! Dymes file headers.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const common = @import("dymes_common");

const errors = @import("errors.zig");

const AllocationError = errors.AllocationError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;
const SyncError = errors.SyncError;

const logging = common.logging;
const Ulid = common.ulid.Ulid;
const isBeforeUlid = common.util.isBeforeUlid;

const constants = @import("constants.zig");
const limits = @import("limits.zig");
const dymes_msg = @import("dymes_msg");
const FrameHeader = dymes_msg.Message.FrameHeader;
const Message = dymes_msg.Message;

pub const idx_file_name_ext: []const u8 = "idx";
pub const idx_file_name_fmt: []const u8 = "dymes_{d}.{d}.idx";

/// Dymes Index File Header.
pub const IndexFileHeader = extern struct {
    const default_magic: [16]u8 = constants.index_file_header_magic;

    /// Header checksum
    header_checksum: u128,

    /// Dymes Index File Magic
    magic: [16]u8,

    /// Nanosecond timestamp
    timestamp: i128,

    /// Segment in message store
    segment_no: u64,

    reserved: [72]u8,

    pub fn initOverlay(header_buf: []u8, segment_no: u64, timestamp: i128) *IndexFileHeader {
        var hdr_ptr: *IndexFileHeader = @ptrCast(@alignCast(header_buf[0..@sizeOf(IndexFileHeader)]));
        hdr_ptr.* = .{
            .header_checksum = 0,
            .magic = default_magic,
            .segment_no = segment_no,
            .timestamp = timestamp,
            .reserved = std.mem.zeroes([72]u8),
        };
        hdr_ptr.header_checksum = common.checksum.checksum(header_buf[@sizeOf(u128)..]);

        return hdr_ptr;
    }

    pub fn overlay(header_buf: []u8) *IndexFileHeader {
        const hdr_ptr: *IndexFileHeader = @ptrCast(@alignCast(header_buf[0..@sizeOf(IndexFileHeader)]));
        assert(std.mem.eql(u8, &default_magic, &hdr_ptr.magic));
        return hdr_ptr;
    }

    /// Verifies the index file header checksum
    pub fn verifyChecksum(self: *const IndexFileHeader) StateError!void {
        const hdr_buf = std.mem.asBytes(self);
        const header_checksum = common.checksum.checksum(hdr_buf[@sizeOf(u128)..]);
        if (header_checksum != self.header_checksum) {
            return StateError.ChecksumFailure;
        }
    }

    /// Formats the index file header and prints to the given writer.
    pub fn format(self: *const IndexFileHeader, writer: *std.Io.Writer) !void {
        try writer.print("{{header_checksum=0x{x},segment_no={d},timestamp={d}}}", .{
            self.header_checksum,
            self.segment_no,
            self.timestamp,
        });
    }
};

comptime {
    assert(@sizeOf(IndexFileHeader) == 128);
    assert(common.util.checkNoPadding(IndexFileHeader));
}

test "headers.IndexFileHeader" {
    std.debug.print("test.headers.IndexFileHeader.smoke\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var logger = logging.logger("test.headers.IndexFileHeader.smoke");

    logger.debug().msg("Header sizing").int("sizeOf", @sizeOf(IndexFileHeader)).log();

    const header_0_0_ts = std.time.nanoTimestamp();
    const header_buf_0_0 = try allocator.alloc(u8, @sizeOf(IndexFileHeader));
    defer allocator.free(header_buf_0_0);
    const header_0_0 = IndexFileHeader.initOverlay(header_buf_0_0, 0, header_0_0_ts);
    try header_0_0.verifyChecksum();
    logger.debug().msg("Initialized by overlay").any("header_0_0", header_0_0).log();

    const header_0_0_o = IndexFileHeader.overlay(header_buf_0_0);
    try header_0_0_o.verifyChecksum();
    logger.debug().msg("Overlayed").any("header_0_0_o", header_0_0_o).log();
    try testing.expectEqual(header_0_0, header_0_0_o);

    var hdr_ptr = allocator.create(IndexFileHeader) catch return AllocationError.OutOfMemory;
    _ = &hdr_ptr;
    defer allocator.destroy(hdr_ptr);

    const header_0_1_ts = std.time.nanoTimestamp();
    const header_0_1 = IndexFileHeader.initOverlay(std.mem.asBytes(hdr_ptr), 1, header_0_1_ts);
    try header_0_1.verifyChecksum();
    logger.debug().msg("Initialized by overlay").any("header_0_1", header_0_1).log();

    const header_0_1_o = IndexFileHeader.overlay(std.mem.asBytes(hdr_ptr));
    try header_0_1_o.verifyChecksum();
    logger.debug().msg("Overlayed").any("header_0_1_o", header_0_1).log();

    try testing.expectEqual(header_0_1, header_0_1_o);
}

/// Dymes Message File Header.
pub const MessageFileHeader = extern struct {
    const default_magic: [16]u8 = constants.message_file_header_magic;
    const default_message_frame_header_size: u32 = dymes_msg.constants.message_frame_header_size;
    const default_message_kv_header_size: u32 = dymes_msg.constants.message_kv_header_size;

    /// Header checksum
    header_checksum: u128,

    /// Dymes Message File Magic
    magic: [16]u8,

    /// Nanosecond timestamp
    timestamp: i128,

    /// Segment number
    segment_no: u64,

    /// Size of message frame headers
    msg_frame_header_size: u32,

    /// Size of message kv headers
    msg_kv_header_size: u32,

    reserved: [64]u8,

    pub fn initOverlay(header_buf: []u8, segment_no: u64, timestamp: i128) *MessageFileHeader {
        var hdr_ptr: *MessageFileHeader = @ptrCast(@alignCast(header_buf[0..@sizeOf(MessageFileHeader)]));
        hdr_ptr.* = .{
            .header_checksum = 0,
            .magic = default_magic,
            .segment_no = segment_no,
            .timestamp = timestamp,
            .msg_frame_header_size = default_message_frame_header_size,
            .msg_kv_header_size = default_message_kv_header_size,
            .reserved = std.mem.zeroes([64]u8),
        };
        hdr_ptr.header_checksum = common.checksum.checksum(header_buf[@sizeOf(u128)..]);

        return hdr_ptr;
    }

    pub fn overlay(header_buf: []u8) *MessageFileHeader {
        const hdr_ptr: *MessageFileHeader = @ptrCast(@alignCast(header_buf[0..@sizeOf(MessageFileHeader)]));
        assert(std.mem.eql(u8, &default_magic, &hdr_ptr.magic));
        return hdr_ptr;
    }

    /// Verifies the message file header checksum
    pub fn verifyChecksum(self: *const MessageFileHeader) StateError!void {
        const hdr_buf = std.mem.asBytes(self);
        const header_checksum = common.checksum.checksum(hdr_buf[@sizeOf(u128)..]);
        if (header_checksum != self.header_checksum) {
            return StateError.ChecksumFailure;
        }
    }

    /// Formats the message file header and prints to the given writer.
    pub fn format(self: *const MessageFileHeader, writer: *std.Io.Writer) !void {
        try writer.print("{{header_checksum=0x{x},segment_no={d},timestamp={d},msg_frame_header_size={d},msg_kv_header_size={d}}}", .{
            self.header_checksum,
            self.segment_no,
            self.timestamp,
            self.msg_frame_header_size,
            self.msg_kv_header_size,
        });
    }
};

comptime {
    assert(@sizeOf(MessageFileHeader) == 128);
    assert(common.util.checkNoPadding(MessageFileHeader));
}

test "headers.MessageFileHeader" {
    std.debug.print("test.headers.MessageFileHeader.smoke\n", .{});
    const allocator = testing.allocator;

    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var logger = logging.logger("test.headers.MessageFileHeader.smoke");

    logger.debug().msg("Header sizing").int("sizeOf", @sizeOf(MessageFileHeader)).log();

    const header_0_0_ts = std.time.nanoTimestamp();
    const header_buf_0_0 = try allocator.alloc(u8, @sizeOf(MessageFileHeader));
    defer allocator.free(header_buf_0_0);
    const header_0_0 = MessageFileHeader.initOverlay(header_buf_0_0, 0, header_0_0_ts);
    try header_0_0.verifyChecksum();
    logger.debug().msg("Initialized by overlay").any("header_0_0", header_0_0).log();

    const header_0_0_o = MessageFileHeader.overlay(header_buf_0_0);
    try header_0_0_o.verifyChecksum();
    logger.debug().msg("Overlayed").any("header_0_0_o", header_0_0_o).log();
    try testing.expectEqual(header_0_0, header_0_0_o);

    var hdr_ptr = allocator.create(MessageFileHeader) catch return AllocationError.OutOfMemory;
    _ = &hdr_ptr;
    defer allocator.destroy(hdr_ptr);

    const header_0_1_ts = std.time.nanoTimestamp();
    const header_0_1 = MessageFileHeader.initOverlay(std.mem.asBytes(hdr_ptr), 1, header_0_1_ts);
    try header_0_1.verifyChecksum();
    logger.debug().msg("Initialized by overlay").any("header_0_1", header_0_1).log();

    const header_0_1_o = MessageFileHeader.overlay(std.mem.asBytes(hdr_ptr));
    try header_0_1_o.verifyChecksum();
    logger.debug().msg("Overlayed").any("header_0_1_o", header_0_1).log();

    try testing.expectEqual(header_0_1, header_0_1_o);
}
