//! Dymes Lazy Correlation Results Builder.
//!
//! Currently, we only support lazy correlation queries, others may follow as required
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

const CorrelationResults = @import("CorrelationResults.zig");

const ReadDataset = @import("ReadDataset.zig");

const SegmentsUlidIndex = @import("SegmentsUlidIndex.zig");

const component_name: []const u8 = "msg_store.CorrelationResultsBuilder";

const Self = @This();

allocator: std.mem.Allocator,

read_dataset: *ReadDataset,
segments_ulid_idx: *SegmentsUlidIndex,

pub fn init(allocator: std.mem.Allocator, read_dataset: *ReadDataset, segments_ulid_idx: *SegmentsUlidIndex) Self {
    return .{
        .allocator = allocator,
        .read_dataset = read_dataset,
        .segments_ulid_idx = segments_ulid_idx,
    };
}

pub fn deinit(_: *Self) void {}

pub const BuildError = AllocationError || UsageError;

pub fn buildCorrelationChain(
    self: *Self,
    filters: []Filter,
    correlation_tail: Ulid,
    correlation_short_circuit: ?Ulid,
) BuildError!*CorrelationResults {
    if (correlation_short_circuit) |_abs_head| {
        assert(common.util.isBeforeUlid(_abs_head, correlation_tail));
    }
    const tail_segment_no = self.segments_ulid_idx.lookup(correlation_tail) orelse return UsageError.IllegalArgument;
    return try CorrelationResults.init(self.allocator, self.read_dataset, self.segments_ulid_idx, filters, correlation_tail, tail_segment_no, correlation_short_circuit);
}

const CorrelationResultsBuilder = @This();

test "CorrelationResultsBuilder" {
    std.debug.print("test.CorrelationResultsBuilder\n", .{});
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

    var logger = logging.logger("test.CorrelationResultsBuilder");

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    try tmp_dir.dir.makeDir("test-corr-builder");

    const abs_dir_name = try tmp_dir.dir.realpathAlloc(allocator, "test-corr-builder");
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

    logger.debug().msg("Building lazy correlation query results").log();
    {
        var found_ids: []Ulid = try allocator.alloc(Ulid, number_of_messages);
        defer allocator.free(found_ids);
        var found_idx: usize = 0;

        try read_dataset.populateSegmentsUlidIndex(&segments_ulid_idx);

        var builder = CorrelationResultsBuilder.init(allocator, &read_dataset, &segments_ulid_idx);
        defer builder.deinit();

        const filters: []Filter = &.{};
        var results = try builder.buildCorrelationChain(filters, message_id_last, message_id_first);
        defer results.deinit();

        var it = try results.iterator();
        defer it.close();

        while (try it.next()) |found_msg| : (found_idx += 1) {
            found_ids[found_idx] = found_msg.frame_header.id;
        }

        std.mem.sort(Ulid, found_ids, {}, struct {
            pub fn inner(_: void, lhs: Ulid, rhs: Ulid) bool {
                return common.util.isBeforeUlid(lhs, rhs);
            }
        }.inner);

        try testing.expectEqualSlices(Ulid, message_ids[0..], found_ids[0..]);
        logger.debug().msg("Traversed correlation chain results").int("number_of_messages", number_of_messages).log();
    }
}
