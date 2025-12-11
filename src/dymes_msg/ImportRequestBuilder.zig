//! Message import request builder.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const debug = std.debug;
const assert = debug.assert;
const io = std.io;

const common = @import("dymes_common");
const ulid = common.ulid;
const Ulid = common.ulid.Ulid;
const config = common.config;
const Config = common.config.Config;

const errors = common.errors;
const UsageError = errors.UsageError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;
const AllocationError = errors.AllocationError;
const BuildError = AllocationError || UsageError;

const logging = common.logging;
const LogLevel = logging.LogLevel;
const LogFormat = logging.LogFormat;
const Logger = logging.Logger;

const KvPair = @import("filters.zig").KvPair;

const Message = @import("Message.zig");

const http = std.http;

const constants = @import("constants.zig");

const ImportRequest = @import("ImportRequest.zig");

const component_name = "msg.ImportRequestBuilder";

const Self = @This();

/// Allocator
allocator: std.mem.Allocator,

/// Logger
logger: *Logger,

/// ID ULID
id: ?Ulid,

/// Correlation ULID
correlation_id: ?Ulid,

/// Channel identifier
channel: ?u128,

/// Routing data
routing: ?u128,

/// Message body
encoded_msg_body: ?[]const u8,

/// KV map
kv_map: std.StringArrayHashMap([]const u8),

pub fn init(allocator: std.mem.Allocator) AllocationError!Self {
    return .{
        .allocator = allocator,
        .logger = logging.logger(component_name),
        .id = null,
        .correlation_id = null,
        .channel = null,
        .routing = null,
        .encoded_msg_body = null,
        .kv_map = std.StringArrayHashMap([]const u8).init(allocator),
    };
}

pub fn deinit(self: *Self) void {
    defer self.kv_map.deinit();
    _ = self.reset();
}

pub fn reset(self: *Self) *Self {
    var kv_it = self.kv_map.iterator();
    while (kv_it.next()) |_kv| {
        self.allocator.free(_kv.key_ptr.*);
        self.allocator.free(_kv.value_ptr.*);
    }
    self.kv_map.clearRetainingCapacity();

    if (self.id) |_| {
        self.id = null;
    }

    if (self.correlation_id) |_| {
        self.correlation_id = null;
    }

    if (self.channel) |_| {
        self.channel = null;
    }

    if (self.routing) |_| {
        self.routing = null;
    }

    if (self.encoded_msg_body) |_msg_body| {
        self.allocator.free(_msg_body);
        self.encoded_msg_body = null;
    }

    return self;
}

pub fn withId(self: *Self, id: Ulid) AllocationError!void {
    self.id = id;
}

pub fn withCorrelationId(self: *Self, correlation_id: ?Ulid) AllocationError!void {
    self.correlation_id = correlation_id;
}

pub fn withChannel(self: *Self, channel: u128) AllocationError!void {
    self.channel = channel;
}

pub fn withRouting(self: *Self, routing: u128) AllocationError!void {
    self.routing = routing;
}

pub fn withBody(self: *Self, msg_body: []const u8) AllocationError!void {
    if (self.encoded_msg_body) |_body| {
        self.allocator.free(_body);
    }

    self.encoded_msg_body = common.base64.encode(self.allocator, msg_body) catch return AllocationError.OutOfMemory;
}

pub fn withKvPair(self: *Self, key: []const u8, value: []const u8) AllocationError!void {
    const kv_key = self.allocator.dupe(u8, key) catch return AllocationError.OutOfMemory;
    errdefer self.allocator.free(kv_key);
    const gpr = self.kv_map.getOrPut(kv_key) catch return AllocationError.OutOfMemory;
    if (gpr.found_existing) {
        self.allocator.free(kv_key);
        self.allocator.free(gpr.value_ptr.*);
    }
    gpr.value_ptr.* = self.allocator.dupe(u8, value) catch return AllocationError.OutOfMemory;
}

pub fn build(self: *Self) BuildError!ImportRequest {
    try self.checkRequiredValues();

    const encoded_id = if (self.id) |_id| encoded_val: {
        break :encoded_val self.allocator.dupe(u8, &_id.encode()) catch return AllocationError.OutOfMemory;
    } else null;
    const encoded_correlation_id = if (self.correlation_id) |_corr_id| encoded_val: {
        break :encoded_val self.allocator.dupe(u8, &_corr_id.encode()) catch return AllocationError.OutOfMemory;
    } else null;
    const kv_entries = self.allocator.alloc(KvPair, self.kv_map.count()) catch return AllocationError.OutOfMemory;
    errdefer self.allocator.free(kv_entries);
    var kvmap_it = self.kv_map.iterator();
    var idx: usize = 0;
    while (kvmap_it.next()) |_kve| : (idx = idx + 1) {
        kv_entries[idx].key = self.allocator.dupe(u8, _kve.key_ptr.*) catch return AllocationError.OutOfMemory;
        errdefer self.allocator.free(kv_entries[idx].key);
        errdefer for (0..idx) |_subidx| {
            self.allocator.free(kv_entries[_subidx].key);
            self.allocator.free(kv_entries[_subidx].value);
        };
        kv_entries[idx].value = self.allocator.dupe(u8, _kve.value_ptr.*) catch return AllocationError.OutOfMemory;
    }

    defer self.kv_map.clearRetainingCapacity();
    defer {
        var kv_it = self.kv_map.iterator();
        while (kv_it.next()) |_kv| {
            self.allocator.free(_kv.key_ptr.*);
            self.allocator.free(_kv.value_ptr.*);
        }
    }

    defer self.id = null;
    defer self.correlation_id = null;
    defer self.channel = null;
    defer self.routing = null;
    defer self.encoded_msg_body = null;

    return ImportRequest.init(encoded_id.?, encoded_correlation_id, self.channel.?, self.routing.?, self.encoded_msg_body.?, kv_entries);
}

fn checkRequiredValues(self: *Self) UsageError!void {
    if (self.id == null) {
        self.logger.warn()
            .msg("Missing message ID")
            .log();
        return UsageError.MissingArgument;
    }
    if (self.channel) |_| {
        if (self.routing) |_| {
            if (self.encoded_msg_body) |_| {
                return;
            } else {
                self.logger.warn()
                    .msg("Missing message body")
                    .log();
                return UsageError.MissingArgument;
            }
        } else {
            self.logger.warn()
                .msg("Missing routing value")
                .log();
            return UsageError.MissingArgument;
        }
    } else {
        self.logger.warn()
            .msg("Missing channel value")
            .log();
        return UsageError.MissingArgument;
    }
}

const ImportRequestBuilder = @This();

test "ImportRequestBuilder" {
    std.testing.refAllDeclsRecursive(@This());

    std.debug.print("test.ImportRequestBuilder\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var req_bld = try ImportRequestBuilder.init(allocator);
    defer req_bld.deinit();

    try testing.expectError(UsageError.MissingArgument, req_bld.build());

    try req_bld.withChannel(0x02be);
    try testing.expectError(UsageError.MissingArgument, req_bld.build());

    try req_bld.withChannel(0x02be);
    try req_bld.withRouting(66);
    try testing.expectError(UsageError.MissingArgument, req_bld.build());

    const msg_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

    try req_bld.withChannel(0x02be);
    try req_bld.withRouting(66);
    try req_bld.withBody(msg_body);

    try testing.expectError(UsageError.MissingArgument, req_bld.build());

    try req_bld.withKvPair("rat", "rattus rattus");
    try req_bld.withKvPair("cat", "cats rule!");
    try req_bld.withKvPair("cat", "felis domesticus");

    var ulid_generator = common.ulid.generator();
    const id = try ulid_generator.next();
    const correlation_id = try ulid_generator.next();

    try req_bld.withId(id);
    try req_bld.withCorrelationId(correlation_id);

    {
        var req = try req_bld.build();
        defer req.deinit(allocator);

        const encoded_body = try common.base64.encode(allocator, msg_body);
        defer allocator.free(encoded_body);

        try testing.expectEqual(0x2be, req.channel);
        try testing.expectEqual(0x42, req.routing);
        try testing.expectEqual(correlation_id, try ulid.decode(req.encoded_correlation_id.?));
        try testing.expectEqual(id, try ulid.decode(req.encoded_id));
        try testing.expectEqual(req.kv_entries.len, 2);
        try testing.expectEqualStrings(encoded_body, req.encoded_msg_body);
        try testing.expectEqualStrings("rat", req.kv_entries[0].key);
        try testing.expectEqualStrings("rattus rattus", req.kv_entries[0].value);
        try testing.expectEqualStrings("cat", req.kv_entries[1].key);
        try testing.expectEqualStrings("felis domesticus", req.kv_entries[1].value);
    }
}
