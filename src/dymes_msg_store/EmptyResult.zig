//! Dymes Empty result.
//!
//! Empty results represent a not found condition.
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

const component_name: []const u8 = "msg_store.EmptyResult";

const Self = @This();
shared_empty_iterator: Iterator,

/// Initializes the empty result.
pub inline fn init() Self {
    return .{ .shared_empty_iterator = Iterator.open() };
}

pub inline fn deinit(self: *Self) void {
    self.shared_empty_iterator.close();
}

pub inline fn iterator(self: *Self) Iterator {
    return self.shared_empty_iterator;
}

const EmptyResult = @This();

pub const Iterator = struct {
    pub inline fn open() Iterator {
        return .{};
    }

    pub inline fn close(_: *Iterator) void {}

    pub inline fn next(_: *Iterator) ?Message {
        return null;
    }

    pub inline fn rewind(_: *Iterator) void {}
};

test "EmptyResult" {
    std.debug.print("test.EmptyResult.smoke\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var query_results = EmptyResult.init();
    defer query_results.deinit();
    var it = query_results.iterator();
    defer it.close();

    const reachedEnd = if (it.next()) |_| false else true;
    try testing.expect(reachedEnd);
}
