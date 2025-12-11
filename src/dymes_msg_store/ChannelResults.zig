//! Dymes Channel Query Results.
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

const component_name: []const u8 = "msg_store.ChannelResults";

const Self = @This();

gpa: std.mem.Allocator,
frame_allocator: std.mem.Allocator,
channel_id: u128,
first_ulid: Ulid,
final_ulid: Ulid,
first_segment: u64,
last_segment: u64,
read_dataset: *ReadDataset,
filters: []Filter,

pub fn init(
    gpa: std.mem.Allocator,
    frame_allocator: std.mem.Allocator,
    read_dataset: *ReadDataset,
    channel_id: u128,
    filters: []Filter,
    first_ulid: Ulid,
    final_ulid: Ulid,
    first_segment: u64,
    last_segment: u64,
) AllocationError!*Self {
    const new_self = gpa.create(ChannelResults) catch return AllocationError.OutOfMemory;
    errdefer gpa.destroy(new_self);
    const filters_copy = gpa.dupe(Filter, filters) catch return AllocationError.OutOfMemory;
    errdefer gpa.free(filters_copy);

    new_self.* = .{
        .gpa = gpa,
        .frame_allocator = frame_allocator,
        .channel_id = channel_id,
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
    defer self.gpa.destroy(self);
    defer self.gpa.free(self.filters);
}

/// Opens a range results iterator
///
/// The caller gains ownership of the iterator, and must call `close()` to release resources.
pub fn iterator(self: *Self) AllocationError!Iterator {
    return Iterator.open(self);
}

const ChannelResults = @This();

pub const Iterator = struct {
    channel_it: ReadDataset.ChannelIterator,
    filters: []Filter,
    frame_allocator: std.mem.Allocator,
    msg_buffer: []u8,

    /// Opens a channel results iterator
    ///
    /// The caller gains ownership of the iterator, and must call `close()` to release resources.
    fn open(channel_results: *ChannelResults) AllocationError!Iterator {
        const msg_buffer = channel_results.frame_allocator.alloc(u8, msg_store_limits.max_message_size) catch return AllocationError.OutOfMemory;
        errdefer channel_results.frame_allocator.free(msg_buffer);

        return .{
            .filters = channel_results.filters,
            .channel_it = try channel_results.read_dataset.channelIterator(
                channel_results.channel_id,
                channel_results.first_ulid,
                channel_results.final_ulid,
                channel_results.first_segment,
                channel_results.last_segment,
            ),
            .frame_allocator = channel_results.frame_allocator,
            .msg_buffer = msg_buffer,
        };
    }

    /// Closes the iterator, releasing resources.
    pub fn close(self: *Iterator) void {
        defer self.frame_allocator.free(self.msg_buffer);
        self.channel_it.close(self.frame_allocator);
    }

    /// Retrieves the next result message (if any).
    pub fn next(self: *Iterator) IteratorError!?Message {
        next_msg: while (try self.channel_it.next()) |_msg| {
            for (self.filters) |filter| {
                if (!filter.apply(_msg)) {
                    continue :next_msg;
                }
            }
            return _msg;
        }
        return null;
    }

    /// Rewinds the cursor to the first result.
    pub inline fn rewind(self: *Iterator) void {
        self.channel_it.rewind();
    }
};

test "ChannelResults.Iterator" {
    std.debug.print("test.ChannelResults.Iterator\n", .{});
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

    var logger = logging.logger("test.ChannelResults.Iterator");

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    try tmp_dir.dir.makeDir("test-channel-iterator");

    const abs_dir_name = try tmp_dir.dir.realpathAlloc(allocator, "test-channel-iterator");
    defer allocator.free(abs_dir_name);

    const timestamp = std.time.nanoTimestamp();

    // POPULATE
    var ulid_generator = common.ulid.generator();

    const test_channel_id: u128 = 4321;

    const message_ids_size: usize = @min((msg_store_limits.max_message_file_size - 128) / 1024 + 10, msg_store_limits.max_message_file_entries + 10);
    // const number_of_messages: usize = 10;
    const number_of_messages: usize = @min((msg_store_limits.max_message_file_size - 128) / 1024 + 10, msg_store_limits.max_message_file_entries + 10);
    logger.debug()
        .msg("Will store and lazily channel query messages")
        .int("number_of_messages", number_of_messages)
        .log();
    var message_ids: []Ulid = try allocator.alloc(Ulid, message_ids_size);
    defer allocator.free(message_ids);
    var message_id_first: Ulid = undefined;
    // var message_id_last_in_segment: Ulid = undefined;
    // var message_id_first_in_next_segment: Ulid = undefined;
    var message_id_last: Ulid = undefined;
    {
        // Open the dataset
        const Dataset = @import("Dataset.zig");
        var dataset = try Dataset.open(allocator, frame_allocator, timestamp, .{
            .dir = tmp_dir.dir,
        });
        defer dataset.close();

        // Store some messages

        const message_frame: []u8 = try frame_allocator.alloc(u8, 1024);
        defer frame_allocator.free(message_frame);

        const message_body = "A" ** 896;

        logger.debug().msg("Storing block of test messages").log();
        for (0..number_of_messages) |idx| {
            var test_msg =
                try Message.initOverlay(message_frame, idx, try ulid_generator.next(), test_channel_id, idx, message_body, .{});
            _ = try dataset.store(&test_msg);
            try testing.expectEqual(test_msg.frame_header.frame_size, 1024);
            message_ids[idx] = test_msg.frame_header.id;
            message_id_last = test_msg.frame_header.id;
        }
        logger.debug().msg("Test messages stored").int("number_of_messages", number_of_messages).log();
        message_id_first = message_ids[0];
        // message_id_last_in_segment = message_ids[msg_store_limits.max_message_file_entries];
        // message_id_first_in_next_segment = message_ids[msg_store_limits.max_message_file_entries + 1];

        try dataset.sync();
    }

    const SegmentsUlidIndex = @import("SegmentsUlidIndex.zig");

    var segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer segments_ulid_idx.deinit(allocator);

    var read_dataset = try ReadDataset.open(allocator, frame_allocator, &segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    });
    defer read_dataset.close();

    logger.debug().msg("Performing channel query").log();
    {
        var found_ids: []Ulid = try allocator.alloc(Ulid, number_of_messages);
        defer allocator.free(found_ids);
        var found_idx: usize = 0;

        const first_segment: u64 = 0;
        const last_segment: u64 = 1;
        const filters: []Filter = &.{};

        var results = try ChannelResults.init(
            allocator,
            frame_allocator,
            &read_dataset,
            test_channel_id,
            filters,
            message_id_first,
            message_id_last,
            first_segment,
            last_segment,
        );
        defer results.deinit();

        var it = try results.iterator();
        defer it.close();

        while (try it.next()) |found_msg| : (found_idx += 1) {
            found_ids[found_idx] = found_msg.frame_header.id;
        }
        try testing.expectEqualSlices(Ulid, message_ids[0..found_ids.len], found_ids[0..]);
        logger.debug().msg("Traversed channel results").int("number_of_messages", number_of_messages).log();
    }
}
