//! Dymes Engine Import Request.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
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

const Message = @import("dymes_msg").Message;

const component_name: []const u8 = "msg_store.ImportRequest";

const Self = @This();

message: *const Message,

inline fn init(message: *const Message) Self {
    return .{
        .message = message,
    };
}

const ImportRequest = @This();

/// Import Request Builder
pub const Builder = struct {
    message: ?*const Message,

    pub inline fn init() Builder {
        return .{
            .message = null,
        };
    }

    pub inline fn withMessage(self: *Builder, message: ?*const Message) void {
        self.message = message;
    }

    pub inline fn deinit(_: Builder) void {}

    inline fn reset(self: *Builder) void {
        self.message = null;
    }

    const BuildError = UsageError;
    pub fn build(self: *Builder) BuildError!ImportRequest {
        if (self.message) |message_| {
            const query_result = ImportRequest.init(message_);
            self.reset();
            return query_result;
        }
        return UsageError.OtherUsageFailure;
    }
};

test "ImportRequest" {
    std.debug.print("test.ImportRequest.smoke\n", .{});
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

    const msg_buf: []u8 = try allocator.alloc(u8, 1024);
    defer allocator.free(msg_buf);
    const msg_body: []const u8 = "A test message";
    const test_msg = try Message.initOverlay(msg_buf, 0, try ulid_gen.next(), 702, 101, msg_body, .{});

    var irb = Builder.init();
    defer irb.deinit();
    irb.withMessage(&test_msg);
    const import_request = try irb.build();

    try testing.expectEqualDeep(import_request.message, &test_msg);
}
