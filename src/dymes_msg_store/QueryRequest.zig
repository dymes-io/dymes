//! Dymes Engine Query Request.
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
const Message = dymes_msg.Message;
const Query = dymes_msg.Query;
const Filter = dymes_msg.Filter;
const KvFilter = dymes_msg.KvFilter;
const KvPair = dymes_msg.KvPair;

const component_name: []const u8 = "msg_store.QueryRequest";

const Self = @This();

allocator: Allocator,
query: Query,
filters: []Filter,

fn init(allocator: Allocator, query: Query, filters: []Filter) AllocationError!Self {
    return .{
        .allocator = allocator,
        .query = query,
        .filters = allocator.dupe(Filter, filters) catch return AllocationError.OutOfMemory,
    };
}

pub fn deinit(self: *const Self) void {
    self.allocator.free(self.filters);
}

pub fn dupe(self: *const Self) AllocationError!Self {
    return init(self.allocator, self.query, self.filters);
}

const QueryRequest = @This();

/// Query Request Builder
pub const Builder = struct {
    const FiltersList = std.array_list.AlignedManaged(Filter, null);

    allocator: std.mem.Allocator,
    query_: ?Query,
    filters_lst: FiltersList,

    pub fn init(allocator: std.mem.Allocator) AllocationError!Builder {
        const filters_lst = FiltersList.init(allocator);
        errdefer filters_lst.deinit();
        return .{
            .allocator = allocator,
            .query_ = null,
            .filters_lst = filters_lst,
        };
    }

    pub fn withQuery(self: *Builder, query: Query) void {
        self.query_ = query;
    }

    pub fn withFilter(self: *Builder, filter: Filter) AllocationError!void {
        self.filters_lst.append(filter) catch return AllocationError.OutOfMemory;
    }

    pub fn deinit(self: *const Builder) void {
        self.filters_lst.deinit();
    }

    fn reset(self: *Builder) void {
        self.query_ = null;
        self.filters_lst.clearRetainingCapacity();
    }

    const BuildError = AllocationError || UsageError;
    pub fn build(self: *Builder) BuildError!QueryRequest {
        if (self.query_) |query| {
            const query_result = try QueryRequest.init(self.allocator, query, self.filters_lst.items);
            self.reset();
            return query_result;
        }
        return UsageError.OtherUsageFailure; // withQuery() wasn't called
    }
};

test "QueryRequest" {
    std.debug.print("test.QueryRequest.smoke\n", .{});
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

    var qrb = try Builder.init(allocator);
    defer qrb.deinit();
    const single_query = Query.single(try ulid_gen.next());
    qrb.withQuery(single_query);
    try qrb.withFilter(try Filter.kvFilter(KvFilter.having("key-present")));
    try qrb.withFilter(try Filter.kvFilter(KvFilter.equals(KvPair.kv("expected-key", "expected-value"))));
    const query_request = try qrb.build();
    defer query_request.deinit();

    try testing.expectEqualDeep(query_request.query.query_single, single_query.query_single);
}
