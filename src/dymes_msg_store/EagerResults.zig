//! Dymes Eagerly fetched query results.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const debug = std.debug;
const assert = debug.assert;

const common = @import("dymes_common");
const msg_store_limits = @import("limits.zig");

const Config = common.config.Config;

const errors = common.errors;
const AllocationError = errors.AllocationError;
const UsageError = errors.UsageError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;

const logging = common.logging;
const Logger = logging.Logger;

const Ulid = common.ulid.Ulid;

const Message = @import("dymes_msg").Message;

const component_name: []const u8 = "msg_store.EagerResults";

pub const MessageRef = packed struct {
    msg_off: usize,
    msg_len: usize,
};

const Self = @This();

allocator: std.mem.Allocator,
results_buffer: []u8,
messages: []Message,

pub fn init(allocator: std.mem.Allocator, results_buffer: []const u8, results_size: usize, message_refs: []const MessageRef) AllocationError!*Self {
    const new_self = allocator.create(EagerResults) catch return AllocationError.OutOfMemory;
    errdefer allocator.destroy(new_self);

    const results_buffer_copy = allocator.dupe(u8, results_buffer[0..results_size]) catch return AllocationError.OutOfMemory;
    errdefer allocator.free(results_buffer_copy);
    var messages = allocator.alloc(Message, message_refs.len) catch return AllocationError.OutOfMemory;
    errdefer allocator.free(messages);
    for (message_refs, 0..) |msg_ref, idx| {
        messages[idx] = Message.overlay(results_buffer_copy[msg_ref.msg_off .. msg_ref.msg_off + msg_ref.msg_len]);
    }
    new_self.* = .{
        .allocator = allocator,
        .results_buffer = results_buffer_copy,
        .messages = messages,
    };
    return new_self;
}

pub fn deinit(self: *Self) void {
    defer self.allocator.destroy(self);
    defer self.allocator.free(self.messages);
    self.allocator.free(self.results_buffer);
}

pub fn iterator(self: *Self) Iterator {
    return Iterator.open(self);
}

test "EagerResults" {
    std.debug.print("test.EagerResults.smoke\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var ulid_gen = common.ulid.generator();

    const num_test_messages: usize = 20;

    var results_buffer: []u8 = try allocator.alloc(u8, num_test_messages * 1024);
    defer allocator.free(results_buffer);

    var msg_refs: []MessageRef = try allocator.alloc(MessageRef, num_test_messages);
    defer allocator.free(msg_refs);

    var cb_results: usize = 0;
    for (0..num_test_messages, 0..) |_, idx| {
        const msg_body = try std.fmt.allocPrint(allocator, "Message number {d}", .{idx + 1});
        defer allocator.free(msg_body);
        const test_msg = try Message.initOverlay(results_buffer[cb_results..], idx, try ulid_gen.next(), 702, 101, msg_body, .{});
        msg_refs[idx] = .{ .msg_off = cb_results, .msg_len = test_msg.frame_header.frame_size };
        cb_results += test_msg.frame_header.frame_size;
    }
    var query_results = try init(allocator, results_buffer, cb_results, msg_refs);
    defer query_results.deinit();
    for (query_results.messages, 0..) |message, idx| {
        const msg_body = try std.fmt.allocPrint(allocator, "Message number {d}", .{idx + 1});
        defer allocator.free(msg_body);
        try testing.expectEqualStrings(msg_body, message.msg_body);
    }
}

const EagerResults = @This();

pub const Iterator = struct {
    query_results: *EagerResults,
    current_idx: usize,

    pub inline fn open(query_results: *EagerResults) Iterator {
        return .{
            .query_results = query_results,
            .current_idx = 0,
        };
    }

    pub inline fn close(self: *Iterator) void {
        _ = &self;
    }

    pub inline fn next(self: *Iterator) ?Message {
        if (self.current_idx >= self.query_results.messages.len) {
            return null;
        }
        self.current_idx += 1;
        return self.query_results.messages[self.current_idx - 1];
    }

    pub inline fn prev(self: *Iterator) ?Message {
        if (self.current_idx == 0) {
            return null;
        }
        self.current_idx -= 1;
        return self.query_results.messages[self.current_idx];
    }

    pub inline fn rewind(self: *Iterator) void {
        self.current_idx = 0;
    }
};

test "EagerResults.Iterator" {
    std.debug.print("test.EagerResults.Iterator\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var ulid_gen = common.ulid.generator();

    const num_test_messages: usize = 20;

    var results_buffer: []u8 = try allocator.alloc(u8, num_test_messages * 1024);
    defer allocator.free(results_buffer);

    var msg_refs: []MessageRef = try allocator.alloc(MessageRef, num_test_messages);
    defer allocator.free(msg_refs);

    var cb_results: usize = 0;
    for (0..num_test_messages, 0..) |_, idx| {
        const msg_body = try std.fmt.allocPrint(allocator, "Message number {d}", .{idx + 1});
        defer allocator.free(msg_body);
        const test_msg = try Message.initOverlay(results_buffer[cb_results..], idx, try ulid_gen.next(), 702, 101, msg_body, .{});
        msg_refs[idx] = .{ .msg_off = cb_results, .msg_len = test_msg.frame_header.frame_size };
        cb_results += test_msg.frame_header.frame_size;
    }
    var query_results = try init(allocator, results_buffer, cb_results, msg_refs);
    defer query_results.deinit();

    var results_it = query_results.iterator();
    defer results_it.close();
    var idx: usize = 0;
    while (results_it.next()) |msg| : (idx += 1) {
        const msg_body = try std.fmt.allocPrint(allocator, "Message number {d}", .{idx + 1});
        defer allocator.free(msg_body);
        try testing.expectEqualStrings(msg_body, msg.msg_body);
    }
    results_it.rewind();
    try testing.expectEqualStrings("Message number 1", results_it.next().?.msg_body);
}

test "EagerResults.Iterator.minibench" {
    std.debug.print("test.EagerResults.Iterator.minibench\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var ulid_gen = common.ulid.generator();

    const num_test_messages: usize = 20000;

    var results_buffer: []u8 = try allocator.alloc(u8, num_test_messages * 1024);
    defer allocator.free(results_buffer);

    var msg_refs: []MessageRef = try allocator.alloc(MessageRef, num_test_messages);
    defer allocator.free(msg_refs);

    var cb_results: usize = 0;
    for (0..num_test_messages, 0..) |_, idx| {
        const msg_body = try std.fmt.allocPrint(allocator, "Message number {d}", .{idx + 1});
        defer allocator.free(msg_body);
        const test_msg = try Message.initOverlay(results_buffer[cb_results..], idx, try ulid_gen.next(), 702, 101, msg_body, .{});
        msg_refs[idx] = .{ .msg_off = cb_results, .msg_len = test_msg.frame_header.frame_size };
        cb_results += test_msg.frame_header.frame_size;
    }
    const query_results = try init(allocator, results_buffer, cb_results, msg_refs);
    defer query_results.deinit();

    var logger = logging.logger("test.EagerResults.Iterator.minibench");

    logger.debug()
        .msg("Results populated")
        .int("num_test_messages", num_test_messages)
        .log();

    var timer = std.time.Timer.start() catch unreachable;

    {
        timer.reset();
        for (0..10) |_| {
            for (query_results.messages) |*msg| {
                assert(msg.frame_header.frame_size != 0);
            }
        }

        const avg_ns = timer.read() / num_test_messages;
        logger.debug()
            .msg("Results traversed by pointer")
            .int("num_test_messages", num_test_messages)
            .int("avg_fetch_ns", avg_ns)
            .log();
    }

    {
        timer.reset();
        for (0..10) |_| {
            for (query_results.messages) |msg| {
                assert(msg.frame_header.frame_size != 0);
            }
        }

        const avg_ns = timer.lap() / num_test_messages;
        logger.debug()
            .msg("Results traversed by copy")
            .int("num_test_messages", num_test_messages)
            .int("avg_fetch_ns", avg_ns)
            .log();
    }

    {
        var results_it = query_results.iterator();
        defer results_it.close();
        timer.reset();

        for (0..10) |_| {
            while (results_it.next()) |*msg| {
                assert(msg.*.frame_header.frame_size != 0);
            }
            results_it.rewind();
        }

        const avg_ns = timer.read() / num_test_messages;
        logger.debug()
            .msg("Results traversed by iterator-pointer")
            .int("num_test_messages", num_test_messages)
            .int("avg_fetch_ns", avg_ns)
            .log();
    }

    {
        var results_it = query_results.iterator();
        defer results_it.close();
        timer.reset();

        for (0..10) |_| {
            while (results_it.next()) |msg| {
                assert(msg.frame_header.frame_size != 0);
            }
            results_it.rewind();
        }

        const avg_ns = timer.read() / num_test_messages;
        logger.debug()
            .msg("Results traversed by iterator-copy")
            .int("num_test_messages", num_test_messages)
            .int("avg_fetch_ns", avg_ns)
            .log();
    }
}
