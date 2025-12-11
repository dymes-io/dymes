//! Message filters.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const debug = std.debug;
const assert = debug.assert;

const common = @import("dymes_common");
const Ulid = common.ulid.Ulid;
const logging = common.logging;
const AllocationError = common.errors.AllocationError;

const Message = @import("Message.zig");

/// Filter to apply to query results.
///
/// Filters are applied _during_ the query operation, and results are filtered in a short-cicuit fashion,
/// i.e. The filters are NOT applied afterwards, once a result set has been created.
pub const FilterTag = enum {
    channel_filter,
    routing_filter,
    kv_filter,
};

pub const KvFilterOp = enum {
    has,
    eql,
    neq,
};

pub const KvPair = struct {
    key: []const u8,
    value: []const u8,
    pub inline fn kv(key: []const u8, value: []const u8) KvPair {
        return .{
            .key = key,
            .value = value,
        };
    }
};

pub const KvFilter = union(KvFilterOp) {
    has: []const u8,
    eql: KvPair,
    neq: KvPair,

    /// Allow messages containing the given KV header.
    pub inline fn having(kv_present: []const u8) KvFilter {
        return .{
            .has = kv_present,
        };
    }

    /// Allow messages which have a matching KV header entry
    pub inline fn equals(kv_pair: KvPair) KvFilter {
        return .{
            .eql = kv_pair,
        };
    }

    /// Allow messages which do not have a matching KV header entry
    pub inline fn notEquals(kv_pair: KvPair) KvFilter {
        return .{
            .neq = kv_pair,
        };
    }

    pub inline fn apply(self: *const KvFilter, message: Message) bool {
        return switch (self.*) {
            .has => |kv_present| message.containsKey(kv_present),
            .eql => |kv_eql| message.containsKeyValue(kv_eql.key, kv_eql.value),
            .neq => |kv_neq| !message.containsKeyValue(kv_neq.key, kv_neq.value),
        };
    }
};

pub const RoutingFilter = struct {
    pub const Variant = enum {
        widening, // traditional
        narrowing, // constrictive
    };
    routing_mask: u128,
    routing_cmp: u128 = 0x0,
    variant: Variant,

    inline fn narrowing(routing_mask: u128, routing_cmp: u128) RoutingFilter {
        return .{ .variant = .narrowing, .routing_mask = routing_mask, .routing_cmp = routing_cmp };
    }

    inline fn widening(routing_mask: u128) RoutingFilter {
        return .{ .variant = .widening, .routing_mask = routing_mask, .routing_cmp = 0x0 };
    }
};

pub const Filter = union(FilterTag) {
    const Self = @This();

    channel_filter: u128,
    routing_filter: RoutingFilter,
    kv_filter: KvFilter,

    pub inline fn channelFilter(channel_id: u128) Filter {
        return .{
            .channel_filter = channel_id,
        };
    }

    pub inline fn routingFilter(routing_filter: RoutingFilter) Filter {
        return .{
            .routing_filter = routing_filter,
        };
    }

    pub inline fn kvFilter(kv_filter: KvFilter) AllocationError!Filter {
        return .{
            .kv_filter = kv_filter,
        };
    }

    pub inline fn apply(self: *const Filter, message: Message) bool {
        return switch (self.*) {
            .channel_filter => |channel_mask| message.frame_header.channel == channel_mask,
            .routing_filter => |routing_filter| {
                return switch (routing_filter.variant) {
                    .widening => 0x0 != (message.frame_header.routing & routing_filter.routing_mask),
                    .narrowing => routing_filter.routing_cmp == (message.frame_header.routing & routing_filter.routing_mask),
                };
            },
            .kv_filter => |kv_filter| kv_filter.apply(message),
        };
    }
};

pub const FilterDto = Filter;

test "filters" {
    std.debug.print("test.filters\n", .{});
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

    const msg_id = try ulid_gen.next();

    var map = std.StringArrayHashMap([]const u8).init(allocator);
    defer map.deinit();
    try map.put("key_0", "zero");
    try map.put("key_1", "one");
    try map.put("key_2", "two");
    try map.put("internal_use", "true");
    const message_body: [4]u8 = .{ 'r', 'a', 't', 's' };

    const msg_buf: []u8 = try allocator.alloc(u8, 1024);
    defer allocator.free(msg_buf);

    const test_msg = try Message.initOverlay(
        msg_buf,
        0,
        msg_id,
        128,
        0x0101,
        &message_body,
        .{
            .transient_kv_headers = map,
        },
    );

    const filter_ch64 = Filter.channelFilter(64);
    try testing.expect(!filter_ch64.apply(test_msg));
    const filter_ch128 = Filter.channelFilter(128);
    try testing.expect(filter_ch128.apply(test_msg));

    const filter_r0001 = Filter.routingFilter(RoutingFilter.widening(0x0001));
    try testing.expect(filter_r0001.apply(test_msg));
    const filter_r0101 = Filter.routingFilter(RoutingFilter.widening(0x0101));
    try testing.expect(filter_r0101.apply(test_msg));
    const filter_r0100 = Filter.routingFilter(RoutingFilter.widening(0x0100));
    try testing.expect(filter_r0100.apply(test_msg));
    const filter_r0110 = Filter.routingFilter(RoutingFilter.widening(0x0110));
    try testing.expect(filter_r0110.apply(test_msg));
    const filter_r1111 = Filter.routingFilter(RoutingFilter.widening(0x1111));
    try testing.expect(filter_r1111.apply(test_msg));
    const filter_r1000 = Filter.routingFilter(RoutingFilter.widening(0x1000));
    try testing.expect(!filter_r1000.apply(test_msg));

    const da_inbound = 0b0000001;
    const da_outbound = 0b0000010;
    const dc_inbound = 0b0000100;
    const dc_outbound = 0b0001000;

    try testing.expect(0x0 != (da_inbound & da_inbound));
    try testing.expect(0x0 != (da_outbound & da_outbound));
    try testing.expect(0x0 != (dc_inbound & dc_inbound));
    try testing.expect(0x0 != (dc_outbound & dc_outbound));

    const mask_da_inout = 0b0000011;
    try testing.expect(0x0 != (mask_da_inout & da_inbound));
    try testing.expect(0x0 != (mask_da_inout & da_outbound));

    const mask_dc_inout = 0b0001100;
    try testing.expect(0x0 != (mask_dc_inout & dc_inbound));
    try testing.expect(0x0 != (mask_dc_inout & dc_outbound));

    const mask_all_outbound = 0b0001010;
    try testing.expect(0x0 != (mask_all_outbound & da_outbound));
    try testing.expect(0x0 != (mask_all_outbound & dc_outbound));

    const mask_all_inbound = 0b0000101;
    try testing.expect(0x0 != (mask_all_inbound & da_inbound));
    try testing.expect(0x0 != (mask_all_inbound & dc_inbound));

    const filter_internal_only = try Filter.kvFilter(KvFilter.equals(KvPair.kv("internal_use", "true")));
    try testing.expect(filter_internal_only.apply(test_msg));
    const filter_external_only = try Filter.kvFilter(KvFilter.notEquals(KvPair.kv("internal_use", "true")));
    try testing.expect(!filter_external_only.apply(test_msg));
}
