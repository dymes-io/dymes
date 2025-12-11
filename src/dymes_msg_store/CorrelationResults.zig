//! Dymes Correlation Query Results.
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
const IteratorError = CreationError || AccessError || StateError;

const logging = common.logging;
const Logger = logging.Logger;

const Ulid = common.ulid.Ulid;
const isBeforeUlid = common.util.isBeforeUlid;

const dymes_msg = @import("dymes_msg");
const Message = dymes_msg.Message;
const MessageLocation = @import("MessageLocation.zig");
const Filter = dymes_msg.Filter;
const Query = dymes_msg.Query;
const QueryRequest = @import("QueryRequest.zig");
const FrameAllocator = dymes_msg.FrameAllocator;

const ReadDataset = @import("ReadDataset.zig");

const SegmentsUlidIndex = @import("SegmentsUlidIndex.zig");

const component_name: []const u8 = "msg_store.CorrelationResults";

const Self = @This();

allocator: std.mem.Allocator,
frame_allocator: std.mem.Allocator,
correlation_tail: Ulid,
tail_segment_no: u64,
correlation_head: ?Ulid,
read_dataset: *ReadDataset,
segments_ulid_idx: *SegmentsUlidIndex,
filters: []Filter,

frame_alloc_impl: *FrameAllocator,

pub fn init(
    allocator: std.mem.Allocator,
    read_dataset: *ReadDataset,
    segments_ulid_idx: *SegmentsUlidIndex,
    filters: []Filter,
    correlation_tail: Ulid,
    tail_segment_no: u64,
    correlation_short_circuit: ?Ulid,
) AllocationError!*Self {
    var frame_alloc_impl = allocator.create(FrameAllocator) catch return AllocationError.OutOfMemory;
    frame_alloc_impl.* = FrameAllocator.init(allocator);
    const frame_allocator = frame_alloc_impl.allocator();

    const new_self = allocator.create(CorrelationResults) catch return AllocationError.OutOfMemory;
    errdefer allocator.destroy(new_self);
    const filters_copy = allocator.dupe(Filter, filters) catch return AllocationError.OutOfMemory;
    errdefer allocator.free(filters_copy);
    new_self.* = .{
        .allocator = allocator,
        .frame_allocator = frame_allocator,
        .filters = filters_copy,
        .read_dataset = read_dataset,
        .correlation_tail = correlation_tail,
        .tail_segment_no = tail_segment_no,
        .correlation_head = correlation_short_circuit,
        .frame_alloc_impl = frame_alloc_impl,
        .segments_ulid_idx = segments_ulid_idx,
    };
    return new_self;
}

pub fn deinit(self: *const Self) void {
    defer self.allocator.destroy(self);
    defer self.allocator.free(self.filters);
    defer self.allocator.destroy(self.frame_alloc_impl);
}

/// Opens a correlation results iterator
///
/// The caller gains ownership of the iterator, and must call `close()` to release resources.
pub fn iterator(self: *Self) IteratorError!Iterator {
    return Iterator.open(self);
}

const CorrelationResults = @This();

pub const Iterator = struct {
    next_msg_loc: ?MessageLocation,
    correlation_results: *CorrelationResults,
    msg_frame_buffer: []u8,

    /// Opens a correlation results iterator
    ///
    /// The caller gains ownership of the iterator, and must call `close()` to release resources.
    fn open(correlation_results: *CorrelationResults) IteratorError!Iterator {
        const msg_frame_buffer = correlation_results.frame_allocator.alloc(u8, dymes_msg.limits.max_frame_size) catch return AllocationError.OutOfMemory;
        const tail_location = try correlation_results.read_dataset.lookupLocation(correlation_results.tail_segment_no, correlation_results.correlation_tail) orelse return AccessError.EntryNotFound;
        return .{
            .next_msg_loc = tail_location,
            .correlation_results = correlation_results,
            .msg_frame_buffer = msg_frame_buffer,
        };
    }

    /// Closes the iterator, releasing resources.
    pub fn close(self: *const Iterator) void {
        defer self.correlation_results.frame_allocator.free(self.msg_frame_buffer);
    }

    /// Retrieves the next result message (if any).
    pub fn next(self: *Iterator) IteratorError!?Message {
        while (self.next_msg_loc) |_msg_loc| {
            const msg = try self.correlation_results.read_dataset.fetchAt(_msg_loc, self.msg_frame_buffer);
            if (msg) |_msg| {
                // Lookup next link in chain
                const corr_id = _msg.correlationId();
                if (self.correlation_results.segments_ulid_idx.lookup(corr_id)) |_corr_segment_no| {
                    self.next_msg_loc = try self.correlation_results.read_dataset.lookupLocation(_corr_segment_no, corr_id);
                } else {
                    self.next_msg_loc = null;
                }

                // Apply filters
                for (self.correlation_results.filters) |_filter| {
                    if (!_filter.apply(_msg)) {
                        continue;
                    }
                }

                // The backing frame is held in this iterator's buffer, so while it's around we can safely pass out
                // a pointer to the message
                return msg;
            } else {
                self.next_msg_loc = null;
            }
        }
        return null;
    }

    /// Rewinds the cursor to the first result.
    pub inline fn rewind(self: *Iterator) IteratorError!void {
        self.next_msg_loc = try self.correlation_results.read_dataset.lookupLocation(self.correlation_results.tail_segment_no, self.correlation_results.correlation_tail) orelse return AccessError.EntryNotFound;
    }
};

test "CorrelationResults.Iterator" {
    std.debug.print("test.CorrelationResults.Iterator\n", .{});
    const allocator = testing.allocator;
    logging.default_logging_filter_level = .debug;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    var fah = FrameAllocator.init(allocator);
    const frame_allocator = fah.allocator();

    var logger = logging.logger("test.CorrelationResults.Iterator");

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    try tmp_dir.dir.makeDir("test-corr-iterator");

    const abs_dir_name = try tmp_dir.dir.realpathAlloc(allocator, "test-corr-iterator");
    defer allocator.free(abs_dir_name);

    const timestamp = std.time.nanoTimestamp();

    // POPULATE
    var ulid_generator = common.ulid.generator();

    const number_of_messages: usize = 1000;
    logger.debug()
        .msg("Will store and lazily range query messages")
        .int("number_of_messages", number_of_messages)
        .log();
    var message_ids: []Ulid = try allocator.alloc(Ulid, number_of_messages);
    defer allocator.free(message_ids);
    var message_id_first: Ulid = undefined;
    var message_id_last: Ulid = undefined;
    {
        // Open the dataset
        const Dataset = @import("Dataset.zig");
        var dataset = try Dataset.open(allocator, frame_allocator, timestamp, .{
            .dir = tmp_dir.dir,
        });
        defer dataset.close();

        // Store some messages

        const message_frame: []u8 = try allocator.alloc(u8, 1024);
        defer allocator.free(message_frame);
        // const message_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

        const message_body = "A" ** 896;

        logger.debug().msg("Storing block of test messages").log();
        var prev_msg_id = dymes_msg.constants.NIL_CORRELATION_ID;
        for (0..number_of_messages) |idx| {
            var test_msg =
                try Message.initOverlay(message_frame, idx, try ulid_generator.next(), idx, 101, message_body, .{ .correlation_id = prev_msg_id });
            _ = try dataset.store(&test_msg);
            try testing.expectEqual(test_msg.frame_header.frame_size, 1024);
            message_ids[idx] = test_msg.frame_header.id;
            prev_msg_id = test_msg.id();
        }
        logger.debug().msg("Test messages stored").int("number_of_messages", number_of_messages).log();
        message_id_first = message_ids[0];
        message_id_last = message_ids[message_ids.len - 1];

        try dataset.sync();
    }

    var segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer segments_ulid_idx.deinit(allocator);

    var read_dataset = try ReadDataset.open(allocator, frame_allocator, &segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    });
    defer read_dataset.close();

    try read_dataset.populateSegmentsUlidIndex(&segments_ulid_idx);

    logger.debug().msg("Performing correlation chain query").log();
    {
        var found_ids: []Ulid = try allocator.alloc(Ulid, number_of_messages);
        defer allocator.free(found_ids);
        var found_idx: usize = 0;

        const tail_seg = segments_ulid_idx.lookup(message_id_last) orelse @panic("Oh dear");
        const filters: []Filter = &.{};

        var results = try CorrelationResults.init(allocator, &read_dataset, &segments_ulid_idx, filters, message_id_last, tail_seg, message_id_first);
        defer results.deinit();

        var it = try results.iterator();
        defer it.close();

        while (try it.next()) |found_msg| : (found_idx += 1) {
            found_ids[found_idx] = found_msg.frame_header.id;
        }
        try testing.expectEqual(message_id_last, found_ids[0]);
        try testing.expectEqual(message_id_first, found_ids[found_ids.len - 1]);
        logger.debug().msg("Traversed correlation chain results").int("number_of_messages", number_of_messages).log();
    }
}
