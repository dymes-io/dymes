//! Dymes Engine query subsystem.
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

const config = common.config;
const Config = common.config.Config;

const errors = common.errors;
const UsageError = errors.UsageError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;

const logging = common.logging;
const Logger = logging.Logger;

const Ulid = common.ulid.Ulid;

const msg_store = @import("dymes_msg_store");
const ImmutableStore = msg_store.ImmutableStore;
const MessageStoreOptions = msg_store.MessageStoreOptions;

const engine_api = @import("api.zig");
const EmptyResult = engine_api.EmptyResult;
const SingleResult = engine_api.SingleResult;
const SingleResultBuilder = engine_api.SingleResultBuilder;
const EagerResults = engine_api.EagerResults;
const EagerResultsBuilder = engine_api.EagerResultsBuilder;
const RangeResults = engine_api.RangeResults;
const RangeResultsBuilder = engine_api.RangeResultsBuilder;
const CorrelationResults = engine_api.CorrelationResults;
const CorrelationResultsBuilder = engine_api.CorrelationResultsBuilder;
const ChannelResults = engine_api.ChannelResults;
const ChannelResultsBuilder = engine_api.ChannelResultsBuilder;
const QueryRequest = engine_api.QueryRequest;
const QueryCursor = engine_api.QueryCursor;
const Filter = engine_api.Filter;
const MessageLocation = engine_api.MessageLocation;

const constants = @import("constants.zig");

const component_name: []const u8 = "engine.QuerySubsystem";
const Self = @This();

var empty_result = EmptyResult.init();

allocator: std.mem.Allocator,
logger: *Logger,
immutable_store: *ImmutableStore,
single_result_bld: SingleResultBuilder,
eager_results_bld: EagerResultsBuilder,
range_results_bld: RangeResultsBuilder,
correlation_results_bld: CorrelationResultsBuilder,
channel_results_bld: ChannelResultsBuilder,
// TODO - re-use this cursor for empty results
empty_cursor: *QueryCursor,

const SubsystemOpenError = AccessError || CreationError || StateError || UsageError;
pub fn init(allocator: std.mem.Allocator, frame_allocator: std.mem.Allocator, store_options: MessageStoreOptions) SubsystemOpenError!*Self {
    var logger = logging.logger(component_name);

    logger.fine()
        .msg("Starting query subsystem")
        .log();

    const empty_cursor = try QueryCursor.open(allocator, .{ .empty_result = &empty_result });
    errdefer empty_cursor.close();

    const immutable_store = try openImmutableStore(logger, allocator, frame_allocator, store_options);
    errdefer immutable_store.close();
    assert(immutable_store.ready());

    const new_self = allocator.create(Self) catch return CreationError.OutOfMemory;
    errdefer allocator.destroy(new_self);

    var single_result_bld = try SingleResultBuilder.init(allocator);
    errdefer single_result_bld.deinit();

    var eager_results_bld = try EagerResultsBuilder.init(allocator);
    errdefer eager_results_bld.deinit();

    var range_results_bld = RangeResultsBuilder.init(allocator, &immutable_store.read_dataset, &immutable_store.segments_ulid_idx);
    errdefer range_results_bld.deinit();

    var corr_results_bld = CorrelationResultsBuilder.init(allocator, &immutable_store.read_dataset, &immutable_store.segments_ulid_idx);
    errdefer corr_results_bld.deinit();

    var channel_results_bld = ChannelResultsBuilder.init(allocator, frame_allocator, &immutable_store.read_dataset, &immutable_store.segments_ulid_idx);
    errdefer channel_results_bld.deinit();

    new_self.* = .{
        .allocator = allocator,
        .logger = logger,
        .immutable_store = immutable_store,
        .single_result_bld = single_result_bld,
        .eager_results_bld = eager_results_bld,
        .range_results_bld = range_results_bld,
        .correlation_results_bld = corr_results_bld,
        .channel_results_bld = channel_results_bld,
        .empty_cursor = empty_cursor,
    };
    errdefer allocator.destroy(new_self);

    logger.debug()
        .msg("Query subsystem started")
        .log();

    return new_self;
}

pub fn deinit(self: *Self) void {
    self.logger.fine()
        .msg("Stopping query subsystem")
        .log();
    defer self.allocator.destroy(self);
    defer self.single_result_bld.deinit();
    defer self.eager_results_bld.deinit();
    defer self.range_results_bld.deinit();
    defer self.correlation_results_bld.deinit();
    defer self.channel_results_bld.deinit();
    defer self.empty_cursor.close();
    self.immutable_store.close();
    defer self.logger.debug()
        .msg("Query subsystem stopped")
        .log();
}

pub fn ready(self: *const Self) bool {
    return self.immutable_store.ready();
}

pub const FetchError = ImmutableStore.FetchError;
const Message = @import("dymes_msg").Message;

/// Handles notification that a new message was appended.
pub inline fn appended(self: *Self, channel_id: u128, msg_location: MessageLocation, corr_location: ?MessageLocation) CreationError!void {
    try self.immutable_store.appended(channel_id, msg_location, corr_location);
}

const StoreOpenError = ImmutableStore.OpenError;
fn openImmutableStore(_: *logging.Logger, allocator: std.mem.Allocator, frame_allocator: std.mem.Allocator, store_options: MessageStoreOptions) StoreOpenError!*ImmutableStore {
    return ImmutableStore.open(allocator, frame_allocator, store_options);
}

pub const QueryExecutionError = AccessError || CreationError || StateError || UsageError;

/// Executes the given query request and returns a query cursor, passing on ownership.
///
/// Caller must call `deinit()` on the returned query cursor to release resources.
pub fn execute(self: *Self, request: QueryRequest) QueryExecutionError!*QueryCursor {
    var execution_timer = std.time.Timer.start() catch unreachable;

    var execution_time_ns: u64 = 0;
    var results_build_time_ns: u64 = 0;
    const cursor: *QueryCursor = switch (request.query) {
        .query_single => |ulid| cursor_single: {
            self.logger.debug()
                .msg("Eagerly executing single-message query")
                .log();
            self.single_result_bld.reset();
            try self.immutable_store.querySingle(ulid, request.filters, &self.single_result_bld);
            execution_time_ns = execution_timer.lap();
            const result = try self.single_result_bld.build();
            results_build_time_ns = execution_timer.lap();
            if (result) |result_| {
                break :cursor_single try QueryCursor.open(self.allocator, .{ .single_result = result_ });
            } else {
                break :cursor_single try QueryCursor.open(self.allocator, .{ .empty_result = &empty_result });
                // TODO - re-use shared empty cursor for empty results
                //break :cursor_single self.empty_cursor;
            }
        },
        .query_multiple => |ulids| cursor_eager_multiple: {
            self.logger.debug()
                .msg("Eagerly executing multi-message query")
                .log();
            try self.immutable_store.queryMultiple(ulids, request.filters, &self.eager_results_bld);
            execution_time_ns = execution_timer.lap();
            const results = try self.eager_results_bld.build();
            results_build_time_ns = execution_timer.lap();
            if (results) |results_| {
                break :cursor_eager_multiple try QueryCursor.open(self.allocator, .{ .eager_results = results_ });
            } else {
                break :cursor_eager_multiple try QueryCursor.open(self.allocator, .{ .empty_result = &empty_result });
                // TODO - re-use shared empty cursor for empty results
                //break :cursor_eager_multiple self.empty_cursor;
            }
        },
        .query_correlation => |corr_id| cursor_correlation: {
            self.logger.debug()
                .msg("Lazily executing correlation chain query")
                .any("query", request.query)
                .any("filters", request.filters)
                .log();
            const results = try self.correlation_results_bld.buildCorrelationChain(request.filters, corr_id, null);
            execution_time_ns = execution_timer.lap();
            defer results_build_time_ns = execution_timer.lap();
            break :cursor_correlation try QueryCursor.open(self.allocator, .{ .correlation_results = results });
        },
        .query_range => |start_end| cursor_lazy_range: {
            self.logger.debug()
                .msg("Lazily executing range query")
                .any("query", request.query)
                .any("filters", request.filters)
                .log();
            const results = try self.range_results_bld.buildRanged(request.filters, start_end.range_start, start_end.range_end);
            execution_time_ns = execution_timer.lap();
            defer results_build_time_ns = execution_timer.lap();
            break :cursor_lazy_range try QueryCursor.open(self.allocator, .{ .lazy_results = results });
        },

        .query_channel => |channel_start_end| cursor_lazy_range: {
            self.logger.debug()
                .msg("Lazily executing channel query")
                .any("query", request.query)
                .any("filters", request.filters)
                .log();
            const results = try self.channel_results_bld.buildChannel(channel_start_end.channel_id, request.filters, channel_start_end.range_start, channel_start_end.range_end);
            execution_time_ns = execution_timer.lap();
            defer results_build_time_ns = execution_timer.lap();
            break :cursor_lazy_range try QueryCursor.open(self.allocator, .{ .channel_results = results });
        },

        // .query_range => |start_end| cursor_eager_range: {
        //     self.logger.fine()
        //         .msg("Eagerly executing range query")
        //         .log();
        //     try self.immutable_store.queryRangeEager(start_end.range_start, start_end.range_end, request.filters, &self.eager_results_bld);
        //     execution_time_ns = execution_timer.lap();
        //     const results = try self.eager_results_bld.build();
        //     results_build_time_ns = execution_timer.lap();
        //     if (results) |results_| {
        //         break :cursor_eager_range QueryCursor.open(.{ .eager_results = results_ });
        //     } else {
        //         break :cursor_eager_range null;
        //     }
        // },
    };

    const cursor_build_time_ns = execution_timer.read();
    self.logger.debug()
        .msg("Query executed")
        .int("query_execution_µs", execution_time_ns / std.time.ns_per_us)
        .int("results_build_µs", results_build_time_ns / std.time.ns_per_us)
        .int("cursor_build_µs", cursor_build_time_ns / std.time.ns_per_us)
        .log();

    return cursor;
}

test "QuerySubsystem" {
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

    var builder = try EagerResultsBuilder.init(allocator);
    defer builder.deinit();
}
