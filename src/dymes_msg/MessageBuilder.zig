//! Dymes Message builder.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const common = @import("dymes_common");
const errors = common.errors;
const StateError = errors.StateError;
const AllocationError = errors.AllocationError;
const BuildError = AllocationError || StateError;

const logging = common.logging;
const Ulid = common.ulid.Ulid;

const Message = @import("Message.zig");

const Self = @This();

const nil_correlation_id: Ulid = .{ .rand = 0, .time = 0 };

/// Allocator
allocator: std.mem.Allocator,

/// Log sequence
log_sequence: ?u64,

/// ULID Message Identifier
id: ?Ulid,

/// Correlation ULID
correlation_id: ?Ulid,

/// Channel identifier
channel: ?u128,

/// Routing data
routing: ?u128,

/// KV header map
transient_kv_map: std.StringArrayHashMap([]const u8),

/// Message body
msg_body: ?[]const u8,

/// Initializes the message builder.
///
/// Caller must call `deinit()` to release resources.
pub fn init(allocator: std.mem.Allocator) AllocationError!Self {
    return .{
        .allocator = allocator,
        .transient_kv_map = std.StringArrayHashMap([]const u8).init(allocator),
        .log_sequence = null,
        .id = null,
        .correlation_id = null,
        .channel = null,
        .routing = null,
        .msg_body = null,
    };
}

/// De-initializes the message builder, releasing resources.
pub fn deinit(self: *Self) void {
    defer self.transient_kv_map.deinit();
}

pub fn reset(self: *Self) *Self {
    self.log_sequence = null;
    self.id = null;
    self.correlation_id = null;
    self.channel = null;
    self.routing = null;
    self.msg_body = null;
    self.transient_kv_map.clearRetainingCapacity();
    return self;
}

pub fn withLogSequence(self: *Self, log_sequence: u64) *Self {
    self.log_sequence = log_sequence;
    return self;
}

pub fn withId(self: *Self, id: Ulid) *Self {
    self.id = id;
    return self;
}

pub fn withCorrelationId(self: *Self, correlation_id: ?Ulid) *Self {
    self.correlation_id = correlation_id;
    return self;
}

pub fn withChannel(self: *Self, channel: u128) *Self {
    self.channel = channel;
    return self;
}

pub fn withRouting(self: *Self, routing: u128) *Self {
    self.routing = routing;
    return self;
}

pub fn withBody(self: *Self, msg_body: []const u8) *Self {
    self.msg_body = msg_body;
    return self;
}

pub fn withKvPair(self: *Self, key: []const u8, value: []const u8) AllocationError!*Self {
    self.transient_kv_map.put(key, value) catch return AllocationError.OutOfMemory;
    return self;
}

pub fn build(self: *Self) BuildError!Message {
    if (!self.checkRequiredValues()) {
        return BuildError.NotReady;
    }
    return try Message.init(self.allocator, self.log_sequence.?, self.id.?, self.channel.?, self.routing.?, self.msg_body.?, .{
        .correlation_id = self.correlation_id orelse nil_correlation_id,
        .transient_kv_headers = self.transient_kv_map,
    });
}

pub fn buildOverlay(self: *Self, message_frame: []u8) BuildError!Message {
    if (!self.checkRequiredValues()) {
        return BuildError.NotReady;
    }
    return try Message.initOverlay(message_frame, self.log_sequence.?, self.id.?, self.channel.?, self.routing.?, self.msg_body.?, .{
        .correlation_id = self.correlation_id orelse nil_correlation_id,
        .transient_kv_headers = self.transient_kv_map,
    });
}

fn checkRequiredValues(self: *Self) bool {
    if (self.log_sequence) |_| {
        if (self.id) |_| {
            if (self.channel) |_| {
                if (self.routing) |_| {
                    if (self.msg_body) |_| {
                        return true;
                    }
                }
            }
        }
    }
    return false;
}

const MessageBuilder = @This();

test "MessageBuilder.build" {
    std.debug.print("test.MessageBuilder.build\n", .{});
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

    const message_id = try ulid_generator.next();

    var manual_msg =
        try Message.init(
            allocator,
            0,
            message_id,
            702,
            101,
            message_body,
            .{ .transient_kv_headers = map },
        );
    defer manual_msg.deinit(allocator);

    var builder = try MessageBuilder.init(allocator);
    defer builder.deinit();

    _ = builder.withLogSequence(0)
        .withId(message_id)
        .withChannel(702)
        .withRouting(101)
        .withBody(message_body);
    var kvit = map.iterator();
    while (kvit.next()) |kv_| {
        _ = try builder.withKvPair(kv_.key_ptr.*, kv_.value_ptr.*);
    }
    var built_message = try builder.build();
    defer built_message.deinit(allocator);

    try testing.expectEqualStrings(manual_msg.msg_body, built_message.msg_body);
    try testing.expectEqualStrings(message_body, built_message.msg_body);

    try testing.expect(!built_message.containsKey("garbage"));
    try testing.expect(built_message.containsKey("0"));
    try testing.expect(built_message.containsKeyValue("3", "three"));
    try testing.expect(!built_message.containsKeyValue("2", "three"));
}

test "MessageBuilder.buildOverlay" {
    std.debug.print("test.MessageBuilder.buildOverlay\n", .{});
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

    const message_id = try ulid_generator.next();

    var manual_msg =
        try Message.init(
            allocator,
            0,
            message_id,
            702,
            101,
            message_body,
            .{ .transient_kv_headers = map },
        );
    defer manual_msg.deinit(allocator);

    const msg_buffer = try allocator.alloc(u8, 1024);
    defer allocator.free(msg_buffer);
    @memset(msg_buffer, 0x0);

    var builder = try MessageBuilder.init(allocator);
    defer builder.deinit();

    _ = builder.withLogSequence(0)
        .withId(message_id)
        .withChannel(702)
        .withRouting(101)
        .withBody(message_body);
    var kvit = map.iterator();
    while (kvit.next()) |kv_| {
        _ = try builder.withKvPair(kv_.key_ptr.*, kv_.value_ptr.*);
    }
    var built_message = try builder.buildOverlay(msg_buffer);

    try testing.expectEqualStrings(manual_msg.msg_body, built_message.msg_body);
    try testing.expectEqualStrings(message_body, built_message.msg_body);

    try testing.expect(!built_message.containsKey("garbage"));
    try testing.expect(built_message.containsKey("0"));
    try testing.expect(built_message.containsKeyValue("3", "three"));
    try testing.expect(!built_message.containsKeyValue("2", "three"));
}

test "Incomplete MessageBuilder" {
    std.debug.print("test.MessageBuilder.incomplete\n", .{});
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

    const message_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

    const message_id = try ulid_generator.next();

    var incomplete_builder = try MessageBuilder.init(allocator);
    defer incomplete_builder.deinit();

    try testing.expectError(StateError.NotReady, incomplete_builder.build());
    _ = incomplete_builder.withLogSequence(0);
    try testing.expectError(StateError.NotReady, incomplete_builder.build());
    _ = incomplete_builder.withId(message_id);
    try testing.expectError(StateError.NotReady, incomplete_builder.build());
    _ = incomplete_builder.withChannel(702);
    try testing.expectError(StateError.NotReady, incomplete_builder.build());
    _ = incomplete_builder.withRouting(101);
    try testing.expectError(StateError.NotReady, incomplete_builder.build());
    _ = incomplete_builder.withBody(message_body);
    var built_message = try incomplete_builder.build();
    defer built_message.deinit(allocator);
    _ = incomplete_builder.reset();
    try testing.expectError(StateError.NotReady, incomplete_builder.build());
}
