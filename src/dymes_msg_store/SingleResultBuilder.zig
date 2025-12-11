//! Dymes Single result builder.
//!
//! Single results are an oddity - the builder holds a single message buffer and single result,
//! with the "build" returning a pointer to the single result
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
const SingleResult = @import("SingleResult.zig");

const component_name: []const u8 = "msg_store.SingleResultBuilder";

const Self = @This();

allocator: std.mem.Allocator,
result_buffer: []u8,
single_result: SingleResult,

pub fn init(allocator: std.mem.Allocator) AllocationError!Self {
    return .{
        .allocator = allocator,
        .single_result = SingleResult.init(),
        .result_buffer = allocator.alloc(u8, msg_store_limits.max_message_size) catch return AllocationError.OutOfMemory,
    };
}

pub fn deinit(self: *Self) void {
    defer self.allocator.free(self.result_buffer);
}

pub inline fn withMessage(self: *Self, msg: *const Message) AllocationError!void {
    @memcpy(self.result_buffer[0..msg.frame.len], msg.frame[0..msg.frame.len]);
    self.single_result.withMessage(Message.overlay(self.result_buffer));
}

pub inline fn reset(self: *Self) void {
    self.single_result.withMessage(null);
}

pub inline fn build(self: *Self) AllocationError!?*SingleResult {
    return &self.single_result;
}

const SingleResultBuilder = @This();

test "SingleResultBuilder" {
    std.debug.print("test.SingleResultBuilder.smoke\n", .{});
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

    var results_buffer: []u8 = try allocator.alloc(u8, 1024);
    defer allocator.free(results_buffer);

    const msg_body = try allocator.dupe(u8, "A single test message");
    defer allocator.free(msg_body);
    const test_msg = try Message.initOverlay(results_buffer[0..], 0, try ulid_gen.next(), 702, 101, msg_body, .{});

    var bld = try SingleResultBuilder.init(allocator);
    defer bld.deinit();
    try bld.withMessage(&test_msg);
    const query_results = try bld.build();
    if (query_results) |query_results_| {
        defer query_results_.deinit();
        var it = query_results_.iterator();
        defer it.close();
        try testing.expectEqualStrings("A single test message", it.next().?.msg_body);
        const reachedEnd = if (it.next()) |_| false else true;
        try testing.expect(reachedEnd);
    }
}
