//! Dymes Eagerly fetched results builder
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

const component_name: []const u8 = "msg_store.EagerResultsBuilder";

/// Maximum message size (2MiB)
pub const max_message_size = msg_store_limits.max_message_size;

/// Initial results buffer size (8MiB)
pub const init_results_buffer_size: usize = 4 * max_message_size;

/// Maximum results buffer size (64MiB)
pub const max_results_buffer_size: usize = 8 * init_results_buffer_size;

const Self = @This();

const EagerResults = @import("EagerResults.zig");

const MessageRef = EagerResults.MessageRef;

allocator: std.mem.Allocator,
logger: *Logger,
results_buffer: []u8,
message_refs: std.array_list.Managed(MessageRef),
append_offset: usize,

pub fn init(allocator: std.mem.Allocator) AllocationError!Self {
    const results_buffer = allocator.alloc(u8, init_results_buffer_size) catch return AllocationError.OutOfMemory;
    errdefer allocator.free(results_buffer);
    return .{
        .allocator = allocator,
        .logger = logging.logger(component_name),
        .append_offset = 0,
        .results_buffer = results_buffer,
        .message_refs = std.array_list.Managed(MessageRef).initCapacity(allocator, 32) catch return AllocationError.OutOfMemory,
    };
}

pub fn deinit(self: *Self) void {
    defer self.allocator.free(self.results_buffer);
    defer self.message_refs.deinit();
}

pub fn withMessage(self: *Self, msg: *const Message) AllocationError!void {
    const msg_len: usize = msg.frame_header.frame_size;
    const end_offset = self.append_offset + msg_len;
    if (end_offset >= self.results_buffer.len) {
        if (self.results_buffer.len + msg_len >= max_results_buffer_size or !self.allocator.resize(self.results_buffer, end_offset)) {
            self.logger.err()
                .msg("Maximum result buffer length reached")
                .err(AllocationError.LimitReached)
                .int("results_buffer_len", self.results_buffer.len)
                .int("msg_len", msg_len)
                .int("max_results_buffer_size", max_results_buffer_size)
                .log();
            return AllocationError.LimitReached;
        } else if (!self.allocator.resize(self.results_buffer, end_offset)) {
            self.logger.err()
                .msg("Unable to resize result buffer")
                .err(AllocationError.OutOfMemory)
                .int("results_buffer_len", self.results_buffer.len)
                .int("msg_len", msg_len)
                .log();
            return AllocationError.OutOfMemory;
        }
    }
    @memcpy(self.results_buffer[self.append_offset..end_offset], msg.frame[0..msg_len]);
    self.message_refs.append(.{ .msg_off = self.append_offset, .msg_len = msg_len }) catch return AllocationError.OutOfMemory;
    self.append_offset += msg_len;
}

fn reset(self: *Self) void {
    self.append_offset = 0;
    self.message_refs.clearRetainingCapacity();
}

pub fn build(self: *Self) AllocationError!?*EagerResults {
    if (self.append_offset == 0) {
        return null;
    }
    const query_results = try EagerResults.init(self.allocator, self.results_buffer, self.append_offset, self.message_refs.items);
    errdefer query_results.deinit();

    self.reset();
    return query_results;
}

test "EagerResultsBuilder" {
    std.debug.print("test.EagerResultsBuilder.smoke\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    const msg_buf: []u8 = try allocator.alloc(u8, 1024);
    defer allocator.free(msg_buf);

    var ulid_gen = common.ulid.generator();

    const num_test_messages: usize = 20;

    var qrb = try init(allocator);
    defer qrb.deinit();

    try testing.expect(try qrb.build() == null);

    for (0..num_test_messages, 0..) |_, idx| {
        const msg_body = try std.fmt.allocPrint(allocator, "Message number {d}", .{idx + 1});
        defer allocator.free(msg_body);
        const test_msg = try Message.initOverlay(msg_buf, idx, try ulid_gen.next(), 702, 101, msg_body, .{});
        try qrb.withMessage(&test_msg);
    }
    if (try qrb.build()) |query_results| {
        defer query_results.deinit();
        for (query_results.messages, 0..) |message, idx| {
            const msg_body = try std.fmt.allocPrint(allocator, "Message number {d}", .{idx + 1});
            defer allocator.free(msg_body);
            try testing.expectEqualStrings(msg_body, message.msg_body);
        }
    }
}
