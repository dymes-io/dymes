//! Dymes Message.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const common = @import("dymes_common");
const AllocationError = common.errors.AllocationError;
const rfc3339 = common.rfc3339;
const logging = common.logging;
const Ulid = common.ulid.Ulid;

const headers = @import("headers.zig");
pub const FrameHeader = headers.FrameHeader;
pub const KvHeader = headers.KvHeader;
pub const LogSequenceNumber = headers.LogSequenceNumber;

pub const FrameAllocator = @import("FrameAllocator.zig");

const Self = @This();

/// Message frame buffer
frame: []u8,

/// Frame header pointer into frame buffer
frame_header: *FrameHeader,

/// KV header pointer into frame buffer
kv_headers: ?[*]KvHeader,

/// Message body slice into the frame buffer
msg_body: []const u8,

pub const Options = struct {
    correlation_id: Ulid = .{ .rand = 0, .time = 0 },
    transient_kv_headers: ?std.StringArrayHashMap([]const u8) = null,
};

/// Initializes a message, allocating the message frame.
///
/// Caller must call `deinit()` to release resources.
pub fn init(allocator: std.mem.Allocator, log_sequence: LogSequenceNumber, msg_id: Ulid, msg_channel: u128, msg_routing: u128, message_body: []const u8, options: Options) AllocationError!Self {
    const frame_sizing = FrameSizing.calculate(message_body, options.transient_kv_headers);
    var message_frame = allocator.alloc(u8, frame_sizing.frame_size) catch return AllocationError.OutOfMemory;
    _ = &message_frame;
    errdefer allocator.free(message_frame);
    return initOverlay(message_frame, log_sequence, msg_id, msg_channel, msg_routing, message_body, options);
}

/// De-initializes a message, freeing the message frame.
pub fn deinit(self: *const Self, allocator: std.mem.Allocator) void {
    defer allocator.free(self.frame);
}

/// Initialize a message in an overlayed buffer, this destroys existing contents of the buffer, use `overlay()`
/// to overlay a buffer non-destructively.
pub fn initOverlay(message_frame: []u8, log_sequence: LogSequenceNumber, msg_id: Ulid, msg_channel: u128, msg_routing: u128, message_body: []const u8, options: Options) AllocationError!Self {
    const frame_sizing = FrameSizing.calculate(message_body, options.transient_kv_headers);
    assert(frame_sizing.frame_size <= message_frame.len);
    // @memset(message_frame, 0x0);
    const frame_header: *FrameHeader = @ptrCast(@alignCast(message_frame.ptr));
    frame_header.* = .{
        .checksum = 0,
        .log_sequence = log_sequence,
        .id = msg_id,
        .correlation_id = options.correlation_id,
        .channel = msg_channel,
        .routing = msg_routing,
        .frame_size = frame_sizing.frame_size,
        .kv_headers_count = frame_sizing.kv_headers_count,
        .kv_headers_data_size = frame_sizing.kv_headers_data_size,
        .content_data_size = frame_sizing.content_data_size,
        .payload_checksum = 0,
    }; // Note: Checksums still need calculating
    var offset = @sizeOf(FrameHeader) + frame_sizing.kv_headers_count * @sizeOf(KvHeader);

    var opt_kv_headers: ?[*]KvHeader = null;
    if (options.transient_kv_headers) |transient_kv_headers| {
        var kv_headers: [*]KvHeader = @ptrCast(@alignCast(message_frame.ptr + @sizeOf(FrameHeader)));
        opt_kv_headers = kv_headers;
        var idx: u32 = 0;
        var kv_data_ptr: [*]u8 = message_frame.ptr;
        kv_data_ptr += offset;
        var mapi = transient_kv_headers.iterator();
        while (mapi.next()) |entry| : (idx += 1) {
            kv_headers[idx] = .{
                .key_off = @as(u32, @intCast(offset)),
                .key_len = @as(u32, @intCast(entry.key_ptr.len)),
                .val_off = @as(u32, @intCast(offset + entry.key_ptr.len)),
                .val_len = @as(u32, @intCast(entry.value_ptr.len)),
            };
            @memcpy(kv_data_ptr, entry.key_ptr.*);
            offset += @as(u32, @intCast(entry.key_ptr.len));
            kv_data_ptr += entry.key_ptr.len;
            @memcpy(kv_data_ptr, entry.value_ptr.*);
            offset += @as(u32, @intCast(entry.value_ptr.len));
            kv_data_ptr += entry.value_ptr.len;
        }
        @memcpy(kv_data_ptr, message_body);
    } else {
        @memcpy(message_frame.ptr + offset, message_body);
    }

    // Calculate payload checksum
    frame_header.*.payload_checksum =
        common.checksum.checksum(message_frame[@sizeOf(FrameHeader)..frame_sizing.frame_size]);

    // Calculate frame header checksum
    frame_header.*.checksum =
        common.checksum.checksum(message_frame[@sizeOf(u128)..@sizeOf(FrameHeader)]);

    return .{
        .frame = message_frame[0..frame_header.frame_size],
        .frame_header = frame_header,
        .kv_headers = opt_kv_headers,
        .msg_body = message_frame[offset .. offset + frame_header.content_data_size],
    };
}

/// Overlays an existing message in a buffer non-destructively.
///
/// Do *not* call `deinit()`, this is a _view_ of real message
pub fn overlay(message_frame: []u8) Self {
    const frame_header: *FrameHeader = @ptrCast(@alignCast(message_frame.ptr));
    assert(frame_header.frame_size <= message_frame.len);
    const offset = @sizeOf(FrameHeader) + @sizeOf(KvHeader) * frame_header.kv_headers_count + frame_header.kv_headers_data_size;
    const end_offset = offset + frame_header.content_data_size;
    const opt_kv_headers: ?[*]KvHeader = if (frame_header.kv_headers_data_size > 0) @ptrCast(@alignCast(message_frame.ptr + @sizeOf(FrameHeader))) else null;
    return .{
        .frame = message_frame,
        .frame_header = @ptrCast(@alignCast(message_frame.ptr)),
        .kv_headers = opt_kv_headers,
        .msg_body = message_frame[offset..end_offset],
    };
}

/// Duplicates this message.
///
/// Note that calling this function is contra-indicated on hot paths, rather pass overlays (views) of messages around.
///
/// Caller must call `deinit()` on the duplicated message to release resources.
pub fn dupe(self: *const Self, allocator: std.mem.Allocator) AllocationError!Self {
    var message_frame = allocator.dupe(u8, self.frame[0..self.frame_header.frame_size]) catch return AllocationError.OutOfMemory;
    errdefer allocator.free(message_frame);
    const frame_header: *FrameHeader = @ptrCast(@alignCast(message_frame.ptr));
    const offset = @sizeOf(FrameHeader) + @sizeOf(KvHeader) * frame_header.kv_headers_count + frame_header.kv_headers_data_size;
    const end_offset = offset + frame_header.content_data_size;
    const opt_kv_headers: ?[*]KvHeader = if (frame_header.kv_headers_data_size > 0) @ptrCast(@alignCast(message_frame.ptr + @sizeOf(FrameHeader))) else null;
    return .{
        .frame = message_frame,
        .frame_header = frame_header,
        .kv_headers = opt_kv_headers,
        .msg_body = message_frame[offset..end_offset],
    };
}

/// Duplicates the frame buffer of this message.
///
/// Caller must call `deinit()` on the duplicated frame buffer to release resources.
pub inline fn dupeFrame(self: *const Self, allocator: std.mem.Allocator) AllocationError![]u8 {
    return allocator.dupe(u8, self.frame[0..self.frame_header.frame_size]) catch return AllocationError.OutOfMemory;
}

/// Queries the 'used slice' of the frame buffer.
///
/// Ownership is _NOT_ passed to caller.
pub inline fn usedFrame(self: *const Self) []const u8 {
    return self.frame[0..self.frame_header.frame_size];
}

/// Checksum failure errors.
pub const ChecksumFailure = error{
    HeaderChecksumMismatch,
    PayloadChecksumMismatch,
};

/// Verifies the message checksums.
pub fn verifyChecksums(self: *const Self) ChecksumFailure!void {

    // Calculate frame header checksum
    const frame_header_checksum = common.checksum.checksum(self.frame[@sizeOf(u128)..@sizeOf(FrameHeader)]);
    if (frame_header_checksum != self.frame_header.checksum) {
        return ChecksumFailure.HeaderChecksumMismatch;
    }

    // Calculate payload checksum
    const payload_checksum = common.checksum.checksum(self.frame[@sizeOf(FrameHeader)..self.frame_header.frame_size]);
    if (payload_checksum != self.frame_header.payload_checksum) {
        return ChecksumFailure.PayloadChecksumMismatch;
    }
}

/// Queries the message LSN (log sequence number)
pub inline fn logSequence(self: *const Self) LogSequenceNumber {
    return self.frame_header.log_sequence;
}

/// Queries the message id
pub inline fn id(self: *const Self) Ulid {
    return self.frame_header.id;
}

/// Queries the message correlation id
pub inline fn correlationId(self: *const Self) Ulid {
    return self.frame_header.correlation_id;
}

/// Queries the message channel mask
pub inline fn channel(self: *const Self) u128 {
    return self.frame_header.channel;
}

/// Queries the message routing data
pub inline fn routing(self: *const Self) u128 {
    return self.frame_header.routing;
}

const Message = @This();

/// An iterator to traverse KV headers in the message.
pub const KvHeaderIterator = struct {
    kv_header_idx: u32,
    msg: *const Message,

    pub fn init(msg: *const Message) KvHeaderIterator {
        return .{ .msg = msg, .kv_header_idx = 0 };
    }

    pub fn next(self: *KvHeaderIterator) ?*const KvHeader {
        if (self.kv_header_idx >= self.msg.frame_header.kv_headers_count) {
            return null;
        }
        self.kv_header_idx += 1;
        if (self.msg.kv_headers) |kvh| {
            return &kvh[self.kv_header_idx - 1];
        }
        unreachable;
    }
};

/// Returns a KV header iterator.
pub fn kvHeaderIterator(self: *const Self) KvHeaderIterator {
    return KvHeaderIterator.init(self);
}

/// Returns a map containing keys and values from KV Headers.
///
/// Note that the keys and values are slices of the message frame, not additional allocations.
///
/// Caller owns the returned map, so needs to call deinit() to free resources.
pub fn transientKvMap(self: *const Self, allocator: std.mem.Allocator) AllocationError!std.array_hash_map.StringArrayHashMap([]const u8) {
    var map = std.array_hash_map.StringArrayHashMap([]const u8).init(allocator);
    errdefer map.deinit();
    var kv_hdr_it = self.kvHeaderIterator();
    while (kv_hdr_it.next()) |kv_hdr| {
        map.put(kv_hdr.key(self.frame.ptr), kv_hdr.value(self.frame.ptr)) catch return AllocationError.OutOfMemory;
    }
    return map;
}

/// Formats the message prints to the given writer.
pub fn format(self: *const Self, writer: *std.Io.Writer) !void {
    try writer.print("{{frame_header={any}", .{self.frame_header});
    if (self.kv_headers) |kvh| {
        try writer.print(",kv_headers={{", .{});
        for (0..self.frame_header.kv_headers_count) |idx| {
            try writer.print("kv_header={any},", .{kvh[idx]});
        }
    }
    try writer.print("}},msg_body={any}}}", .{self.msg_body});
}

pub fn containsKey(self: *const Self, key: []const u8) bool {
    var kv_it = self.kvHeaderIterator();
    while (kv_it.next()) |kvh| {
        const kv_key = self.frame[kvh.key_off .. kvh.key_off + kvh.key_len];
        if (std.mem.eql(u8, key, kv_key)) {
            return true;
        }
    }
    return false;
}

pub fn containsKeyValue(self: *const Self, key: []const u8, value: []const u8) bool {
    var kv_it = self.kvHeaderIterator();
    while (kv_it.next()) |kvh| {
        const kv_key = self.frame[kvh.key_off .. kvh.key_off + kvh.key_len];
        if (std.mem.eql(u8, key, kv_key)) {
            const kv_value = self.frame[kvh.val_off .. kvh.val_off + kvh.val_len];
            return std.mem.eql(u8, value, kv_value);
        }
    }
    return false;
}

pub fn renderAsCSV(self: *const Self, writer: *std.Io.Writer) !void {
    try writer.print("{s}, {f}, {f}, {x}, {d}", .{
        rfc3339.epochToRFC3339(self.frame_header.id.time),
        self.id(),
        self.correlationId(),
        self.channel(),
        self.routing(),
    });
    var it = self.kvHeaderIterator();
    while (it.next()) |kvh| {
        const key = kvh.key(self.frame.ptr);
        const value = kvh.value(self.frame.ptr);
        try writer.print(", {s}={s}", .{ key, value });
    }
    const payload = encodePayload(self.msg_body);
    try writer.print(", {s}\n", .{payload});
}

pub fn renderAsJSON(self: *const Self, writer: *std.Io.Writer) !void {
    try writer.writeAll("{");

    var id_buf = self.id().encode();
    try writer.print("\"id\": \"{s}\", ", .{id_buf[0..]});

    var corr_buf = self.correlationId().encode();
    try writer.print("\"correlation_id\": \"{s}\", ", .{corr_buf[0..]});

    try writer.print("\"channel\": {d}, ", .{self.channel()});
    try writer.print("\"routing\": {d}, ", .{self.routing()});

    try writer.writeAll("\"headers\": [");
    var it = self.kvHeaderIterator();
    var first = true;
    while (it.next()) |kvh| {
        const key = kvh.key(self.frame.ptr);
        const value = kvh.value(self.frame.ptr);
        if (!first) try writer.writeAll(", ");
        first = false;
        try writer.writeAll("{ ");
        try std.json.Stringify.encodeJsonString("key", .{}, writer);
        try writer.writeAll(": ");
        try std.json.Stringify.encodeJsonString(key, .{}, writer);
        try writer.writeAll(", ");
        try std.json.Stringify.encodeJsonString("value", .{}, writer);
        try writer.writeAll(": ");
        try std.json.Stringify.encodeJsonString(value, .{}, writer);
        try writer.writeAll(" }");
    }
    try writer.writeAll("], ");

    const payload = encodePayload(self.msg_body);
    if (payload.len > 0) {
        try writer.print("\"content\": \"{s}\"", .{payload});
    } else {
        try writer.writeAll("\"content\": null");
    }

    try writer.writeAll("}\n");
}

fn encodePayload(payload: []const u8) []const u8 {
    if (payload.len == 0) return "<none>";

    const needed_len = ((payload.len + 2) / 3) * 4;
    const max_len: usize = 4096;

    if (needed_len > max_len) return "<payload too large>";

    var buf: [max_len]u8 = undefined;

    const encoded = std.base64.standard.Encoder.encode(&buf, payload);
    return encoded;
}

/// Frame sizing calculator
const FrameSizing = struct {
    frame_size: u32,
    kv_headers_count: u32,
    kv_headers_data_size: u32,
    content_data_size: u32,
    padding: u32,

    fn calculate(content: []const u8, opt_kv_headers_map: ?std.StringArrayHashMap([]const u8)) FrameSizing {

        // Start with frame header and message content len
        var frame_size = @as(u32, @intCast(@sizeOf(FrameHeader) + content.len));
        var kv_headers_count: u32 = 0;
        var kv_headers_data_size: u32 = 0;
        if (opt_kv_headers_map) |kv_headers_map| {
            // Add size of KV headers
            kv_headers_count = @as(u32, @intCast(kv_headers_map.count()));
            frame_size += kv_headers_count * @sizeOf(KvHeader);

            // Add size of kv header data
            var mapi = kv_headers_map.iterator();
            while (mapi.next()) |map_entry| {
                kv_headers_data_size += @as(u32, @intCast(map_entry.key_ptr.len + map_entry.value_ptr.len));
            }
            frame_size += kv_headers_data_size;
        }

        // Add alignment padding
        var padding: u32 = 0;
        const amod = frame_size % 16;
        if (amod > 0) {
            padding = 16 - amod;
            frame_size += padding;
        }

        return .{
            .frame_size = frame_size,
            .kv_headers_count = kv_headers_count,
            .kv_headers_data_size = kv_headers_data_size,
            .content_data_size = @as(u32, @intCast(content.len)),
            .padding = padding,
        };
    }
};

test "Message.FrameSizing.calculate" {
    std.debug.print("test.Message.FrameSizing.calculate\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var logger = logging.logger("test.Message.FrameSizing.calculate");

    {
        const frame_sizing = FrameSizing.calculate("", null);
        logger.debug()
            .msg("Without content or KV headers")
            .int("calculated", frame_sizing.frame_size)
            .int("frame_size % 16", frame_sizing.frame_size % 16)
            .log();
        try testing.expect(frame_sizing.frame_size % 16 == 0);
        try testing.expectEqual(@sizeOf(FrameHeader), frame_sizing.frame_size);
    }

    {
        const frame_sizing = FrameSizing.calculate("12345678", null);
        logger.debug()
            .msg("With content without KV headers")
            .int("calculated", frame_sizing.frame_size)
            .int("frame_size % 16", frame_sizing.frame_size % 16)
            .log();
        try testing.expect(frame_sizing.frame_size % 16 == 0);
        try testing.expectEqual(@sizeOf(FrameHeader) + 8 + 8, frame_sizing.frame_size);
    }

    const kv_header_data_size = 2 * 8 + 11 + 10;

    var map = std.StringArrayHashMap([]const u8).init(allocator);
    defer map.deinit();
    try map.put("header-0", "Value zero!");
    try map.put("header-1", "Value one!");

    {
        const frame_sizing = FrameSizing.calculate("12345678", map);
        logger.debug()
            .msg("With content and KV headers")
            .int("calculated", frame_sizing.frame_size)
            .int("frame_size % 16", frame_sizing.frame_size % 16)
            .log();
        try testing.expect(frame_sizing.frame_size % 16 == 0);
        try testing.expectEqual(@sizeOf(FrameHeader) + 8 + 2 * @sizeOf(KvHeader) + kv_header_data_size + 3, frame_sizing.frame_size);
    }
}

test "Message" {
    std.debug.print("test.Message.smoke\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var frame_allocator_holder = FrameAllocator.init(allocator);
    const frame_allocator = frame_allocator_holder.allocator();

    var ulid_generator = common.ulid.generator();

    var logger = logging.logger("test.Message.smoke");

    var map = std.StringArrayHashMap([]const u8).init(allocator);
    defer map.deinit();

    try map.put("0", "zero");
    try map.put("1", "one");
    try map.put("2", "two");
    try map.put("3", "three");
    try map.put("4", "four");

    const message_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    var allocated_msg =
        try init(
            frame_allocator,
            0,
            try ulid_generator.next(),
            702,
            101,
            message_body,
            .{ .transient_kv_headers = map },
        );
    defer allocated_msg.deinit(frame_allocator);
    logger.debug()
        .any("allocated_msg", allocated_msg)
        .log();
    const overlayed_msg = overlay(allocated_msg.frame);
    logger.debug()
        .any("overlayed_msg", overlayed_msg)
        .log();
    try testing.expectEqualStrings(allocated_msg.msg_body, overlayed_msg.msg_body);
    try testing.expectEqualStrings(message_body, overlayed_msg.msg_body);

    try testing.expect(!overlayed_msg.containsKey("garbage"));
    try testing.expect(overlayed_msg.containsKey("0"));
    try testing.expect(overlayed_msg.containsKeyValue("3", "three"));
    try testing.expect(!overlayed_msg.containsKeyValue("2", "three"));

    logger.debug()
        .msg("Iterating KV Headers")
        .int("count", overlayed_msg.frame_header.kv_headers_count)
        .log();
    var kv_hdr_it = kvHeaderIterator(&overlayed_msg);
    while (kv_hdr_it.next()) |kv_hdr| {
        logger.debug()
            .msg("kv_hdr")
            .str("key", kv_hdr.key(overlayed_msg.frame.ptr))
            .str("value", kv_hdr.value(overlayed_msg.frame.ptr))
            .log();
    }

    var kv_map = try overlayed_msg.transientKvMap(allocator);
    defer kv_map.deinit();
    logger.debug()
        .msg("Iterating extracted KV Map")
        .int("count", overlayed_msg.frame_header.kv_headers_count)
        .log();
    var kv_map_it = kv_map.iterator();
    while (kv_map_it.next()) |entry| {
        logger.debug()
            .msg("kv")
            .str("key", entry.key_ptr.*)
            .str("value", entry.value_ptr.*)
            .log();
    }

    var dup_message = try overlayed_msg.dupe(frame_allocator);
    defer dup_message.deinit(frame_allocator);

    // Checksums

    try overlayed_msg.verifyChecksums();
    try dup_message.verifyChecksums();

    try testing.expectEqualStrings(overlayed_msg.frame, dup_message.frame);

    const old_channel = allocated_msg.frame_header.channel;
    allocated_msg.frame_header.channel = 1066;
    try testing.expectError(ChecksumFailure.HeaderChecksumMismatch, overlayed_msg.verifyChecksums());
    allocated_msg.frame_header.channel = old_channel;
    try overlayed_msg.verifyChecksums();

    allocated_msg.frame[@sizeOf(FrameHeader)] = 'X';
    try testing.expectError(ChecksumFailure.PayloadChecksumMismatch, overlayed_msg.verifyChecksums());
}
