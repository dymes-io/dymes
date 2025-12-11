//! Message import request.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const debug = std.debug;
const assert = debug.assert;
const io = std.io;

const common = @import("dymes_common");
const rfc3339 = common.rfc3339;
const ulid = common.ulid;
const Ulid = common.ulid.Ulid;
const config = common.config;
const Config = common.config.Config;
const logging = common.logging;

const errors = common.errors;
const AllocationError = errors.AllocationError;

const constants = @import("constants.zig");

const component_name = "msg.ImportRequest";

const filters = @import("filters.zig");

pub const Filter = filters.Filter;
pub const FilterDto = filters.FilterDto;
pub const KvFilter = filters.KvFilter;
pub const KvPair = filters.KvPair;

const EncodedUlid = @import("queries.zig").EncodedUlid;

const Self = @This();

/// Pre-assigned message ULID
encoded_id: EncodedUlid,

/// Correlation ULID
encoded_correlation_id: ?EncodedUlid,

/// Channel identifier
channel: u128,

/// Routing data
routing: u128,

/// KV entries
kv_entries: []const KvPair,

/// Message body
encoded_msg_body: []const u8,

pub fn init(encoded_id: EncodedUlid, encoded_correlation_id: ?EncodedUlid, channel: u128, routing: u128, encoded_msg_body: []const u8, kv_entries: []const KvPair) AllocationError!Self {
    return .{
        .encoded_id = encoded_id,
        .encoded_correlation_id = encoded_correlation_id,
        .channel = channel,
        .routing = routing,
        .kv_entries = kv_entries,
        .encoded_msg_body = encoded_msg_body,
    };
}

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    defer allocator.free(self.encoded_id);
    defer allocator.free(self.encoded_msg_body);
    defer allocator.free(self.kv_entries);
    if (self.encoded_correlation_id) |_encoded_correlation_id| {
        defer allocator.free(_encoded_correlation_id);
    }
    for (self.kv_entries) |_kv| {
        allocator.free(_kv.key);
        allocator.free(_kv.value);
    }
}

const ImportRequest = @This();

test "ImportRequest" {
    std.testing.refAllDeclsRecursive(@This());

    std.debug.print("test.ImportRequest\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    var logger = logging.logger("test.ImportRequest");

    var ulid_generator = common.ulid.generator();

    const kv_entries = [_]KvPair{
        .{
            .key = "black rat",
            .value = "Rattus Rattus",
        },
        .{
            .key = "black cat",
            .value = "Felis Domesticus",
        },
    };
    const corr_ulid = try ulid_generator.next();
    const msg_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

    const encoded_correlation = try corr_ulid.encodeAlloc(allocator);
    defer allocator.free(encoded_correlation);

    const encoded_body = try common.base64.encode(allocator, msg_body);
    defer allocator.free(encoded_body);

    const id_ulid = try ulid_generator.next();
    const encoded_id = try id_ulid.encodeAlloc(allocator);
    defer allocator.free(encoded_id);

    const import_request = try ImportRequest.init(encoded_id, encoded_correlation, 702, 101, encoded_body, &kv_entries);
    {
        var arr_json = std.array_list.Managed(u8).init(allocator);
        defer arr_json.deinit();
        var adapter = arr_json.writer().adaptToNewApi(&.{});
        const writer: *std.Io.Writer = &adapter.new_interface;
        try std.json.Stringify.value(import_request, .{ .whitespace = .minified }, writer);

        logger.debug()
            .msg("JSON encoding")
            .str("import_request_json", arr_json.items)
            .log();

        const parsed = try std.json.parseFromSlice(ImportRequest, allocator, arr_json.items, .{});
        defer parsed.deinit();

        logger.debug()
            .msg("JSON decoding")
            .any("import_request", parsed.value)
            .log();
        try testing.expectEqual(import_request.channel, parsed.value.channel);
        try testing.expectEqual(import_request.routing, parsed.value.routing);
        try testing.expectEqualStrings(encoded_correlation, parsed.value.encoded_correlation_id.?);
        try testing.expectEqualStrings(encoded_body, parsed.value.encoded_msg_body);
        try testing.expectEqualStrings(kv_entries[0].key, import_request.kv_entries[0].key);
        try testing.expectEqualStrings(kv_entries[0].value, import_request.kv_entries[0].value);
        try testing.expectEqualStrings(kv_entries[1].key, import_request.kv_entries[1].key);
        try testing.expectEqualStrings(kv_entries[1].value, import_request.kv_entries[1].value);
        try testing.expectEqualStrings(encoded_id, parsed.value.encoded_id);
    }
}
