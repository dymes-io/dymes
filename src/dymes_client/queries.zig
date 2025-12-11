//! Dymes Client Query.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const debug = std.debug;
const assert = debug.assert;
const Uri = std.Uri;
const http = std.http;

const common = @import("dymes_common");
const ulid = common.ulid;
const Ulid = ulid.Ulid;
const config = common.config;
const Config = common.config.Config;

const errors = common.errors;
const UsageError = errors.UsageError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const AllocationError = errors.AllocationError;

const logging = common.logging;
const Logger = logging.Logger;

const dymes_msg = @import("dymes_msg");

const constants = @import("constants.zig");

const component_name = "client.queries";

pub const Filter = dymes_msg.Filter;
pub const FilterDto = dymes_msg.FilterDto;
pub const QueryTag = dymes_msg.QueryTag;
pub const QueryDto = dymes_msg.QueryDto;
pub const EncodedUlid = dymes_msg.EncodedUlid;
pub const RangeQueryDto = dymes_msg.RangeQueryDto;
pub const ChanneQueryDto = dymes_msg.ChannelQueryDto;
pub const MultiMsgQueryDto = dymes_msg.MultiMsgQueryDto;

pub const ClientQueryDto = struct {
    query_dto: QueryDto,
    filters: []const FilterDto = &.{},
};

test "dymes_client.queries.smoke" {
    std.testing.refAllDeclsRecursive(@This());

    std.debug.print("dymes_client.queries.smoke\n", .{});
    const allocator = testing.allocator;

    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var logger = logging.logger("dymes_client.queries.smoke");

    var ulid_generator = common.ulid.generator();

    const range_query_filters = [_]FilterDto{
        .{ .channel_filter = 0x702 },
        .{
            .kv_filter = .{
                .eql = .{
                    .key = "black rat",
                    .value = "Rattus Rattus",
                },
            },
        },
        .{
            .kv_filter = .{
                .has = "abcxyz",
            },
        },
    };
    const start_ulid = try ulid_generator.next();
    const end_ulid = try ulid_generator.next();
    const query_dto: ClientQueryDto = .{
        .query_dto = .{ .query_range = .{
            .range_start = &start_ulid.encode(),
            .range_end = &end_ulid.encode(),
        } },
        .filters = &range_query_filters,
    };

    {
        var arr_json = std.array_list.Managed(u8).init(allocator);
        defer arr_json.deinit();
        try std.json.stringify(query_dto, .{ .whitespace = .minified }, arr_json.writer());

        logger.debug()
            .msg("JSON encoding")
            .str("range_query_json", arr_json.items)
            .log();

        const parsed = try std.json.parseFromSlice(ClientQueryDto, allocator, arr_json.items, .{});
        defer parsed.deinit();

        logger.debug()
            .msg("JSON decoding")
            .any("range_query", parsed.value)
            .log();
        try testing.expectEqualStrings(query_dto.query_dto.query_range.range_start, parsed.value.query_dto.query_range.range_start);
        try testing.expectEqualStrings(query_dto.query_dto.query_range.range_end, parsed.value.query_dto.query_range.range_end);
    }

    const msg_ids = [_][]const u8{ &start_ulid.encode(), &end_ulid.encode() };
    const multi_msg_query: ClientQueryDto = .{
        .query_dto = .{ .query_multiple = .{
            .msg_ids = &msg_ids,
        } },
        .filters = &.{},
    };

    {
        var arr_json = std.array_list.Managed(u8).init(allocator);
        defer arr_json.deinit();
        try std.json.stringify(multi_msg_query, .{ .whitespace = .minified }, arr_json.writer());

        logger.debug()
            .msg("JSON encoding")
            .str("multi_msg_json", arr_json.items)
            .log();

        const parsed = try std.json.parseFromSlice(ClientQueryDto, allocator, arr_json.items, .{});
        defer parsed.deinit();

        logger.debug()
            .msg("JSON decoding")
            .any("multi_msg_query", parsed.value)
            .log();

        try testing.expectEqualStrings(multi_msg_query.query_dto.query_multiple.msg_ids[0], parsed.value.query_dto.query_multiple.msg_ids[0]);
    }
}
