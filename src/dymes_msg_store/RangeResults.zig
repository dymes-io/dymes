//! Dymes Range Query Results.
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
const Filter = dymes_msg.Filter;
const Query = dymes_msg.Query;
const QueryRequest = @import("QueryRequest.zig");
const FrameAllocator = dymes_msg.FrameAllocator;

const ReadDataset = @import("ReadDataset.zig");

const component_name: []const u8 = "msg_store.RangeResults";

const Self = @This();

allocator: std.mem.Allocator,

first_ulid: Ulid,
final_ulid: Ulid,
first_segment: u64,
last_segment: u64,
read_dataset: *ReadDataset,
filters: []Filter,

pub fn init(
    allocator: std.mem.Allocator,
    read_dataset: *ReadDataset,
    filters: []Filter,
    first_ulid: Ulid,
    final_ulid: Ulid,
    first_segment: u64,
    last_segment: u64,
) AllocationError!*Self {
    const new_self = allocator.create(RangeResults) catch return AllocationError.OutOfMemory;
    errdefer allocator.destroy(new_self);
    const filters_copy = allocator.dupe(Filter, filters) catch return AllocationError.OutOfMemory;
    errdefer allocator.free(filters_copy);
    new_self.* = .{
        .allocator = allocator,
        .filters = filters_copy,
        .read_dataset = read_dataset,
        .first_ulid = first_ulid,
        .first_segment = first_segment,
        .final_ulid = final_ulid,
        .last_segment = last_segment,
    };
    return new_self;
}

pub fn deinit(self: *Self) void {
    defer self.allocator.destroy(self);
    defer self.allocator.free(self.filters);
}

/// Opens a range results iterator
///
/// The caller gains ownership of the iterator, and must call `close()` to release resources.
pub fn iterator(self: *Self) Iterator {
    return Iterator.open(self);
}

const RangeResults = @This();

pub const Iterator = struct {
    range_it: ReadDataset.RangeIterator,
    filters: []Filter,

    /// Opens a range results iterator
    ///
    /// The caller gains ownership of the iterator, and must call `close()` to release resources.
    fn open(range_results: *RangeResults) Iterator {
        return .{
            .filters = range_results.filters,
            .range_it = range_results.read_dataset.rangeIterator(
                range_results.first_ulid,
                range_results.final_ulid,
                range_results.first_segment,
                range_results.last_segment,
            ),
        };
    }

    /// Closes the iterator, releasing resources.
    pub fn close(self: *Iterator) void {
        self.range_it.close();
    }

    /// Retrieves the next result message (if any).
    pub fn next(self: *Iterator) IteratorError!?Message {
        next_msg: while (try self.range_it.next()) |_msg| {
            for (self.filters) |filter| {
                if (!filter.apply(_msg)) {
                    continue :next_msg;
                }
            }
            // The backing frame is held in the range iterator's buffer, so while it's around we can safely pass out
            // a pointer to the message
            return _msg;
        }
        return null;
    }

    /// Rewinds the cursor to the first result.
    pub inline fn rewind(self: *Iterator) void {
        self.range_it.rewind();
    }
};

test "RangeResults.Iterator" {
    std.debug.print("test.RangeResults.Iterator\n", .{});
    const allocator = testing.allocator;
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

    var logger = logging.logger("test.RangeResults.Iterator");

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    try tmp_dir.dir.makeDir("test-range-iterator");

    const abs_dir_name = try tmp_dir.dir.realpathAlloc(allocator, "test-range-iterator");
    defer allocator.free(abs_dir_name);

    const timestamp = std.time.nanoTimestamp();

    // POPULATE
    var ulid_generator = common.ulid.generator();

    const number_of_messages: usize = @min((msg_store_limits.max_message_file_size - 128) / 1024 + 10, msg_store_limits.max_message_file_entries + 10);
    logger.debug()
        .msg("Will store and lazily range query messages")
        .int("number_of_messages", number_of_messages)
        .log();
    var message_ids: []Ulid = try allocator.alloc(Ulid, number_of_messages);
    defer allocator.free(message_ids);
    var message_id_first: Ulid = undefined;
    var message_id_last_in_segment: Ulid = undefined;
    var message_id_first_in_next_segment: Ulid = undefined;
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
        for (0..number_of_messages) |idx| {
            var test_msg =
                try Message.initOverlay(message_frame, idx, try ulid_generator.next(), idx, 101, message_body, .{});
            _ = try dataset.store(&test_msg);
            try testing.expectEqual(test_msg.frame_header.frame_size, 1024);
            message_ids[idx] = test_msg.frame_header.id;
        }
        logger.debug().msg("Test messages stored").int("number_of_messages", number_of_messages).log();
        message_id_first = message_ids[0];
        message_id_last = message_ids[message_ids.len - 1];
        message_id_last_in_segment = message_ids[msg_store_limits.max_message_file_entries];
        message_id_first_in_next_segment = message_ids[msg_store_limits.max_message_file_entries + 1];

        try dataset.sync();
    }

    const SegmentsUlidIndex = @import("SegmentsUlidIndex.zig");

    var segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer segments_ulid_idx.deinit(allocator);

    var read_dataset = try ReadDataset.open(allocator, frame_allocator, &segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    });
    defer read_dataset.close();

    logger.debug().msg("Performing range query").log();
    {
        var found_ids: []Ulid = try allocator.alloc(Ulid, number_of_messages);
        defer allocator.free(found_ids);
        var found_idx: usize = 0;

        const first_segment: u64 = 0;
        const last_segment: u64 = 1;
        const filters: []Filter = &.{};

        var results = try RangeResults.init(
            allocator,
            &read_dataset,
            filters,
            message_id_first,
            message_id_last,
            first_segment,
            last_segment,
        );
        defer results.deinit();

        var it = results.iterator();
        defer it.close();

        while (try it.next()) |found_msg| : (found_idx += 1) {
            found_ids[found_idx] = found_msg.frame_header.id;
        }
        try testing.expectEqualSlices(Ulid, message_ids[0..], found_ids[0..]);
        logger.debug().msg("Traversed range results").int("number_of_messages", number_of_messages).log();
    }
}
