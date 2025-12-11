//! Dymes Engine Cursor Traversal Request.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const debug = std.debug;
const assert = debug.assert;

const common = @import("dymes_common");

const errors = common.errors;
const AllocationError = errors.AllocationError;
const UsageError = errors.UsageError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;

const logging = common.logging;
const Logger = logging.Logger;

const Ulid = common.ulid.Ulid;

const dymes_msg = @import("dymes_msg");
pub const Query = dymes_msg.Query;
const Message = dymes_msg.Message;

const QueryCursor = @import("QueryCursor.zig");

const component_name: []const u8 = "msg_store.CursorTraversalRequest";

const Self = @This();

cursor: *QueryCursor,

fn init(cursor: *QueryCursor) Self {
    return .{
        .cursor = cursor,
    };
}

pub fn deinit(_: *const Self) void {}

pub fn dupe(self: *const Self) Self {
    return init(self.cursor);
}

const CursorTraversalRequest = @This();

/// Query Traversal Request Builder
pub const Builder = struct {
    cursor: ?*QueryCursor,

    pub fn init() Builder {
        return .{
            .cursor = null,
        };
    }

    pub fn withQueryCursor(self: *Builder, query_cursor: *QueryCursor) void {
        self.cursor = query_cursor;
    }

    pub fn deinit(_: *const Builder) void {}

    fn reset(self: *Builder) void {
        self.cursor = null;
    }

    const BuildError = AllocationError || UsageError;
    pub fn build(self: *Builder) BuildError!CursorTraversalRequest {
        if (self.cursor) |cursor_| {
            const request = CursorTraversalRequest.init(cursor_);
            self.reset();
            return request;
        }
        return UsageError.OtherUsageFailure; // withQueryCursor() wasn't called
    }
};

test "CursorTraversalRequest" {
    std.debug.print("test.CursorTraversalRequest.smoke\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var my_cursor: QueryCursor = undefined;

    var ctrb = Builder.init();
    defer ctrb.deinit();

    ctrb.withQueryCursor(&my_cursor);

    const request = try ctrb.build();

    try testing.expectEqual(request.cursor, &my_cursor);
}
