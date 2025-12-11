//! Dymes Message Frame Header.
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

/// Log sequence number (LSN).
///
/// This is a monotonically increasing log sequence number, starting at 0 (zero), with no gaps allowed.
pub const LogSequenceNumber = u64;

/// Message Frame Header.
///
/// This header resides at the start of a message frame.
pub const FrameHeader = extern struct {
    const Self = @This();

    /// Header checksum
    /// (bits 0-127)
    checksum: u128,

    /// Log sequence number
    /// (bits 128-192)
    log_sequence: LogSequenceNumber,

    /// Reserved
    /// (bits 192-255)
    reserved_0: u64 = 0,

    /// ULID Message Identifier
    /// (bits 256-383)
    id: Ulid,

    /// Correlation ULID
    /// (bits 384-511)
    correlation_id: Ulid,

    /// Channel identifier
    /// (bits 512-639)
    channel: u128,

    /// Routing data
    /// (bits 640-767)
    routing: u128,

    /// The size of the message frame: frame header + payload (KV headers, KV data + message body) + padding
    /// (bits 768-799)
    frame_size: u32,

    /// The number of KV headers following the frame header
    /// (bits 800-831)
    kv_headers_count: u32,

    /// The size of KV data following the KV headers.
    /// (bits 832-863)
    kv_headers_data_size: u32,

    /// The size of message content data following the KV data.
    /// (bits 864-895)
    content_data_size: u32,

    /// Payload checksum (KV headers, KV data + message body)
    /// (bits 896-1023)
    payload_checksum: u128,

    /// Returns true if the calculated header checksum matches our checkum in the struct
    pub fn verifyChecksum(self: *const Self) bool {
        const partial_frame: [*]const u8 = @ptrCast(self);
        const calculated: u128 = common.checksum.checksum(partial_frame[@sizeOf(u128)..@sizeOf(FrameHeader)]);
        return self.checksum == calculated;
    }

    /// Formats the frame header and prints to the given writer.
    pub fn format(self: *const Self, writer: *std.Io.Writer) !void {
        try writer.print("{{checksum=0x{x},log_sequence={d},id={any},correlation_id={any},channel=0x{x},routing=0x{x},frame_size={d},kv_headers_count={d},kv_headers_data_size={d},content_data_size={d},payload_checksum=0x{x}}}", .{
            self.checksum,
            self.log_sequence,
            self.id,
            self.correlation_id,
            self.channel,
            self.routing,
            self.frame_size,
            self.kv_headers_count,
            self.kv_headers_data_size,
            self.content_data_size,
            self.payload_checksum,
        });
    }
};

comptime {
    assert(@sizeOf(FrameHeader) == 128);
    assert(common.util.checkNoPadding(FrameHeader));
}

test "headers.FrameHeader" {
    std.debug.print("test.headers.FrameHeader\n", .{});
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

    var logger = logging.logger("test.dymes_msg.headers.FrameHeader");

    const header: FrameHeader = .{
        .checksum = 0,
        .log_sequence = 0,
        .id = try ulid_generator.next(),
        .correlation_id = .{ .rand = 0, .time = 0 },
        .channel = 0,
        .routing = 0,
        .frame_size = @sizeOf(FrameHeader),
        .kv_headers_count = 0,
        .kv_headers_data_size = 0,
        .content_data_size = 0,
        .payload_checksum = 0,
    };
    logger.debug().obj("header", header).int("header.frame_size", header.frame_size).log();
}

/// KV Header.
///
/// These headers reference key and value data in the frame following all KV Headers.
pub const KvHeader = extern struct {
    const Self = @This();

    /// Key offset
    key_off: u32,

    /// Key length
    key_len: u32,

    /// Value offset
    val_off: u32,

    /// Value length
    val_len: u32,

    /// Queries the key as a const slice
    pub fn key(self: *const Self, buffer_ptr: [*]const u8) []const u8 {
        const content_ptr: [*]const u8 = buffer_ptr + self.key_off;
        return content_ptr[0..self.key_len];
    }

    /// Queries the value as a const slice
    pub fn value(self: *const Self, buffer_ptr: [*]const u8) []const u8 {
        const content_ptr: [*]const u8 = buffer_ptr + self.val_off;
        return content_ptr[0..self.val_len];
    }

    pub fn initOverlay(buffer_ptr: [*]u8, key_off: u32, key_len: u32, val_off: u32, val_len: u32) void {
        var self: *Self = overlay(buffer_ptr);
        self.key_off = key_off;
        self.key_len = key_len;
        self.val_off = val_off;
        self.val_len = val_len;
    }

    pub fn calcSizeFromTransientMap(transient_map: std.StringArrayHashMap([]const u8)) usize {
        var buffer_size: usize = 0;
        var mapi = transient_map.iterator();
        while (mapi.next()) |entry| {
            buffer_size += entry.key_ptr.len + entry.value_ptr.len;
        }
        return buffer_size;
    }

    pub inline fn overlay(buffer_ptr: [*]u8) *Self {
        var self_overlayed: *Self = @ptrCast(@alignCast(buffer_ptr));
        _ = &self_overlayed;
        return self_overlayed;
    }

    /// Formats the KV header and prints to the given writer.
    pub fn format(self: *const Self, writer: *std.Io.Writer) !void {
        try writer.print("{{key_off={d},key_len={d},val_off={d},val_len={d}}}", .{
            self.key_off,
            self.key_len,
            self.val_off,
            self.val_len,
        });
    }
};
comptime {
    assert(@sizeOf(KvHeader) == 16);
    assert(common.util.checkNoPadding(KvHeader));
}

test "test msg.headers.KvHeader" {
    std.debug.print("test.headers.KvHeader\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var logger = logging.logger("test.msg.headers.KvHeader");

    {
        const kv_header: KvHeader = .{
            .key_off = 0,
            .key_len = 0,
            .val_off = 0,
            .val_len = 0,
        };
        logger.debug().obj("kv_header", kv_header).int("sizeof kv_header", @sizeOf(KvHeader)).log();
    }

    {
        var map = std.StringArrayHashMap([]const u8).init(allocator);
        defer map.deinit();

        try map.put("0", "zero");
        try map.put("1", "one");
        try map.put("2", "two");
        try map.put("3", "three");
        try map.put("4", "four");

        const kv_data_size = KvHeader.calcSizeFromTransientMap(map);
        const kv_headers_size = map.count() * @sizeOf(KvHeader);
        logger.debug().int("kv_data_size", kv_data_size).int("kv_headers_size", kv_headers_size).log();

        var kv_buf = try allocator.alloc(u8, kv_headers_size + kv_data_size);
        _ = &kv_buf;
        defer allocator.free(kv_buf);

        var mapi = map.iterator();
        var kv_header_ptr: [*]KvHeader = @ptrCast(@alignCast(kv_buf));
        var offset = kv_headers_size;
        var kv_data_ptr: [*]u8 = kv_buf.ptr;
        kv_data_ptr += offset;
        var header_idx: u32 = 0;
        while (mapi.next()) |entry| {
            logger.debug()
                .msg("Adding map entry")
                .str("key", entry.key_ptr.*)
                .str("value", entry.value_ptr.*)
                .log();
            kv_header_ptr[header_idx] = .{
                .key_off = @as(u32, @intCast(offset)),
                .key_len = @as(u32, @intCast(entry.key_ptr.len)),
                .val_off = @as(u32, @intCast(offset + entry.key_ptr.len)),
                .val_len = @as(u32, @intCast(entry.value_ptr.len)),
            };
            @memcpy(kv_data_ptr, entry.key_ptr.*);
            offset += entry.key_ptr.len;
            kv_data_ptr += entry.key_ptr.len;
            @memcpy(kv_data_ptr, entry.value_ptr.*);
            offset += entry.value_ptr.len;
            kv_data_ptr += entry.value_ptr.len;

            const kvh: *KvHeader = &kv_header_ptr[header_idx];
            logger.debug()
                .msg("KV entry")
                .int("key_off", kvh.key_off)
                .int("key_len", kvh.key_len)
                .int("val_off", kvh.val_off)
                .int("val_len", kvh.val_len)
                .log();
            logger.debug()
                .msg("KV entry")
                .str("key", kvh.key(kv_buf.ptr))
                .str("value", kvh.value(kv_buf.ptr))
                .log();
            header_idx += 1;
        }
    }
}
