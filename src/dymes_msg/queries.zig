//! Dymes Message Queries.
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

const Config = common.config.Config;

const errors = common.errors;
const AllocationError = errors.AllocationError;
const UsageError = errors.UsageError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;

const logging = common.logging;
const Logger = logging.Logger;

const ulid = common.ulid;
const Ulid = ulid.Ulid;

const Message = @import("Message.zig");

/// Query tag.
pub const QueryTag = enum {
    /// A single message is being queried
    query_single,
    /// A range of messages are being queried
    /// Note that a simple transform allows this to be used as a timestamp range query
    query_range,
    /// Multiple messages are being queried
    query_multiple,
    /// Query by correlation id
    query_correlation,
    /// Query by channel
    query_channel,
};

pub const EncodedUlid = []const u8;

pub const RangeQueryDto = struct {
    range_start: EncodedUlid,
    range_end: EncodedUlid,
};

pub const ChannelQueryDto = struct {
    channel_id: u128,
    range_start: EncodedUlid,
    range_end: EncodedUlid,
};

pub const MultiMsgQueryDto = struct {
    msg_ids: []const EncodedUlid,
};

pub const QueryDto = union(QueryTag) {
    const Self = @This();
    query_single: EncodedUlid,
    query_range: RangeQueryDto,
    query_multiple: MultiMsgQueryDto,
    query_correlation: EncodedUlid,
    query_channel: ChannelQueryDto,
};

pub const Query = union(QueryTag) {
    const Self = @This();

    query_single: Ulid,
    query_range: struct {
        range_start: Ulid,
        range_end: Ulid,
    },
    query_multiple: []Ulid,
    query_correlation: Ulid,
    query_channel: struct {
        channel_id: u128,
        range_start: Ulid,
        range_end: Ulid,
    },

    pub const FromDtoError = AllocationError || UsageError;
    pub fn fromDTO(allocator: Allocator, query_dto: QueryDto) FromDtoError!Self {
        return switch (query_dto) {
            .query_range => |_range_dto| {
                return rangeFrom(_range_dto);
            },
            .query_multiple => |_multi_dto| {
                return multipleFrom(allocator, _multi_dto);
            },
            .query_single => |_single_dto| {
                return .{
                    .query_single = ulid.decode(_single_dto) catch return UsageError.IllegalArgument,
                };
            },
            .query_correlation => |_correlation_dto| {
                return .{
                    .query_correlation = ulid.decode(_correlation_dto) catch return UsageError.IllegalArgument,
                };
            },
            .query_channel => |_channel_dto| {
                return channelFrom(_channel_dto);
            },
        };
    }

    /// Query single message with the given identifier.
    pub fn single(msg_id: Ulid) Self {
        return .{
            .query_single = msg_id,
        };
    }

    /// Query messages with id range starting at `from_msg_id` inclusive, to `to_msg_id` inclusive.
    pub fn range(from_msg_id: Ulid, to_msg_id: Ulid) Self {
        return .{
            .query_range = .{
                .range_start = from_msg_id,
                .range_end = to_msg_id,
            },
        };
    }

    /// Query messages with channel_id starting at `from_msg_id` inclusive, to `to_msg_id` inclusive.
    pub fn channel(channel_id: u128, from_msg_id: Ulid, to_msg_id: Ulid) Self {
        return .{
            .query_channel = .{
                .channel_id = channel_id,
                .range_start = from_msg_id,
                .range_end = to_msg_id,
            },
        };
    }

    /// Query messages using the given range query dto.
    pub fn rangeFrom(range_query_dto: RangeQueryDto) UsageError!Self {
        return .{
            .query_range = .{
                .range_start = ulid.decode(range_query_dto.range_start) catch return UsageError.IllegalArgument,
                .range_end = ulid.decode(range_query_dto.range_end) catch return UsageError.IllegalArgument,
            },
        };
    }

    /// Query messages using the given channel query dto.
    pub fn channelFrom(channel_query_dto: ChannelQueryDto) UsageError!Self {
        return .{
            .query_channel = .{
                .channel_id = channel_query_dto.channel_id,
                .range_start = ulid.decode(channel_query_dto.range_start) catch return UsageError.IllegalArgument,
                .range_end = ulid.decode(channel_query_dto.range_end) catch return UsageError.IllegalArgument,
            },
        };
    }

    /// Query time range starting at `from_timestamp_ms` inclusive, to `to_timestamp_ms` exclusive.
    pub fn timeRange(from_timestamp_ms: u48, to_timestamp_ms: u48) UsageError!Self {
        if (to_timestamp_ms <= from_timestamp_ms) {
            return UsageError.InvalidRequest;
        }
        return .{
            .query_range = .{
                .range_start = .{ .time = from_timestamp_ms, .rand = 0x0 },
                .range_end = .{ .time = to_timestamp_ms, .rand = 0x0 },
            },
        };
    }

    /// Query messages with the given identifiers.
    pub fn multiple(msg_ids: []Ulid) Self {
        return .{
            .query_multiple = msg_ids,
        };
    }

    /// Query messages using the given multi-message query DTO.
    ///
    /// Warning: This will most likely leak, if you're not careful. OTOH, it's meant for use in arenas...
    pub const MultiFromError = AllocationError || UsageError;

    pub fn multipleFrom(arena: Allocator, multiple_msg_query_dto: MultiMsgQueryDto) MultiFromError!Self {
        var msg_ids: []Ulid = arena.alloc(Ulid, multiple_msg_query_dto.msg_ids.len) catch return AllocationError.OutOfMemory;
        errdefer arena.free(msg_ids);

        for (multiple_msg_query_dto.msg_ids, 0..) |_encoded_msg_id, _idx| {
            msg_ids[_idx] = ulid.decode(_encoded_msg_id) catch return UsageError.IllegalArgument;
        }

        return .{
            .query_multiple = msg_ids,
        };
    }

    /// Query correlation chain, starting at the message with the given identifier.
    pub fn correlation(correlation_id: Ulid) Self {
        return .{
            .query_correlation = correlation_id,
        };
    }
};

test "queries smoke test" {
    std.debug.print("test.queries.smoke\n", .{});
    std.testing.refAllDeclsRecursive(@This());

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

    const before_id = try ulid_gen.next();
    const msg_id = try ulid_gen.next();
    const after_id = try ulid_gen.next();

    const query_single = Query.single(msg_id);
    try testing.expectEqual(@as(u128, @bitCast(query_single.query_single)), @as(u128, @bitCast(msg_id)));

    var msg_ids: [3]Ulid = .{ before_id, msg_id, after_id };
    const query_multiple = Query.multiple(&msg_ids);
    try testing.expectEqualSlices(Ulid, query_multiple.query_multiple, &msg_ids);

    const query_corr = Query.correlation(msg_id);
    try testing.expectEqual(@as(u128, @bitCast(query_corr.query_correlation)), @as(u128, @bitCast(msg_id)));

    const query_chan = Query.channel(1234, before_id, after_id);
    try testing.expectEqual(1234, query_chan.query_channel.channel_id);
    try testing.expectEqual(before_id, query_chan.query_channel.range_start);
    try testing.expectEqual(after_id, query_chan.query_channel.range_end);
}
