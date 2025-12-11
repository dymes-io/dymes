//! Dymes Eagerly fetched single result.
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

const component_name: []const u8 = "msg_store.SingleResult";

const Self = @This();

result: ?Message,

/// Initializes the single result.
pub inline fn init() Self {
    return .{ .result = null };
}

pub inline fn withMessage(self: *Self, message: ?Message) void {
    self.result = message;
}

pub inline fn deinit(_: *Self) void {}

pub inline fn iterator(self: *Self) Iterator {
    return Iterator.open(self);
}

const SingleResult = @This();

pub const Iterator = struct {
    query_results: *SingleResult,
    rewound: bool,

    pub inline fn open(query_results: *SingleResult) Iterator {
        return .{
            .query_results = query_results,
            .rewound = true,
        };
    }

    pub inline fn close(_: *Iterator) void {}

    pub inline fn next(self: *Iterator) ?Message {
        if (self.rewound) {
            self.rewound = false;
            return if (self.query_results.result) |_msg| _msg else null;
        }
        return null;
    }

    pub inline fn rewind(self: *Iterator) void {
        self.rewound = true;
    }
};

test "SingleResult" {
    std.debug.print("test.SingleResult.smoke\n", .{});
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

    const msg_body = try allocator.dupe(u8, "A single test message");
    defer allocator.free(msg_body);
    var test_msg = try Message.init(allocator, 0, try ulid_gen.next(), 702, 101, msg_body, .{});
    defer test_msg.deinit(allocator);

    var query_results = SingleResult.init();
    query_results.withMessage(test_msg);
    defer query_results.deinit();
    var it = query_results.iterator();
    defer it.close();

    try testing.expectEqualStrings("A single test message", it.next().?.msg_body);
    const reachedEnd = if (it.next()) |_| false else true;
    try testing.expect(reachedEnd);
}
