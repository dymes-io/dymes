//! Dymes Query Cursor.
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

const IteratorError = CreationError || AccessError || StateError;

const logging = common.logging;
const Logger = logging.Logger;

const Ulid = common.ulid.Ulid;

const Message = @import("dymes_msg").Message;

const EmptyResult = @import("EmptyResult.zig");
const SingleResult = @import("SingleResult.zig");
const EagerResults = @import("EagerResults.zig");
const RangeResults = @import("RangeResults.zig");
const CorrelationResults = @import("CorrelationResults.zig");
const ChannelResults = @import("ChannelResults.zig");

const component_name: []const u8 = "msg_store.QueryCursor";

/// Results tag.
pub const ResultsTag = enum {
    /// Empty result
    empty_result,
    /// Single result
    single_result,
    /// Prefetched results
    eager_results,
    /// Lazy results
    lazy_results,
    /// Correlation chain results
    correlation_results,
    /// Channel query results
    channel_results,
};

/// Results holder
pub const Results = union(ResultsTag) {
    empty_result: *EmptyResult,
    single_result: *SingleResult,
    eager_results: *EagerResults,
    lazy_results: *RangeResults,
    correlation_results: *CorrelationResults,
    channel_results: *ChannelResults,
};

pub const ResultsIterator = union(ResultsTag) {
    empty_result: EmptyResult.Iterator,
    single_result: SingleResult.Iterator,
    eager_results: EagerResults.Iterator,
    lazy_results: RangeResults.Iterator,
    correlation_results: CorrelationResults.Iterator,
    channel_results: ChannelResults.Iterator,
};

const Self = @This();

allocator: Allocator,
results: Results,
results_iterator: ResultsIterator,

/// Initializes the query cursor, with the cursor assuming ownership of the given results.
///
/// Caller must call `close()` to release resources.
pub fn open(allocator: Allocator, results: Results) IteratorError!*Self {
    const new_self = allocator.create(Self) catch return AllocationError.OutOfMemory;
    errdefer allocator.destroy(new_self);
    new_self.* = .{
        .allocator = allocator,
        .results_iterator = switch (results) {
            .empty_result => |empty_result| .{ .empty_result = empty_result.iterator() },
            .single_result => |single_result| .{ .single_result = single_result.iterator() },
            .eager_results => |eager_results| .{ .eager_results = eager_results.iterator() },
            .lazy_results => |lazy_results| .{ .lazy_results = lazy_results.iterator() },
            .correlation_results => |correlation_results| .{ .correlation_results = try correlation_results.iterator() },
            .channel_results => |channel_results| .{ .channel_results = try channel_results.iterator() },
        },
        .results = results,
    };
    return new_self;
}

/// De-initializes the query cursor, releasing resources.
pub fn close(self: *Self) void {
    defer self.allocator.destroy(self);
    switch (self.results_iterator) {
        .empty_result => |*empty_result_it| empty_result_it.close(),
        .single_result => |*single_result_it| single_result_it.close(),
        .eager_results => |*eager_results_it| eager_results_it.close(),
        .lazy_results => |*lazy_results_it| lazy_results_it.close(),
        .correlation_results => |*correlation_results_it| correlation_results_it.close(),
        .channel_results => |*channel_results_it| channel_results_it.close(),
    }
    switch (self.results) {
        .empty_result => |empty_result| empty_result.deinit(),
        .single_result => |single_result| single_result.deinit(),
        .eager_results => |eager_results| eager_results.deinit(),
        .lazy_results => |lazy_results| lazy_results.deinit(),
        .correlation_results => |correlation_results| correlation_results.deinit(),
        .channel_results => |channel_results| channel_results.deinit(),
    }
}

/// Retrieves the next result message (if any).
pub inline fn next(self: *Self) IteratorError!?Message {
    return switch (self.results_iterator) {
        .empty_result => |*empty_result_it| empty_result_it.next(),
        .single_result => |*single_result_it| single_result_it.next(),
        .eager_results => |*eager_results_it| eager_results_it.next(),
        .lazy_results => |*lazy_results_it| lazy_results_it.next(),
        .correlation_results => |*correlation_results_it| correlation_results_it.next(),
        .channel_results => |*channel_results_it| channel_results_it.next(),
    };
}

/// Rewinds the cursor to the first result.
pub inline fn rewind(self: *Self) IteratorError!void {
    switch (self.results_iterator) {
        .empty_result => |*empty_result_it| empty_result_it.rewind(),
        .single_result => |*single_result_it| single_result_it.rewind(),
        .eager_results => |*eager_results_it| eager_results_it.rewind(),
        .lazy_results => |*lazy_results_it| lazy_results_it.rewind(),
        .correlation_results => |*correlation_results_it| correlation_results_it.rewind(),
        .channel_results => |*channel_results_it| channel_results_it.rewind(),
    }
}

test "QueryCursor" {
    std.testing.refAllDeclsRecursive(@This());
}
