//! Dymes Message Store - Immutable access.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const common = @import("dymes_common");
const logging = common.logging;
const Ulid = common.ulid.Ulid;

const limits = @import("limits.zig");
const constants = @import("constants.zig");

const AllocationError = common.errors.AllocationError;
const CreationError = common.errors.CreationError;
const AccessError = common.errors.AccessError;
const StateError = common.errors.StateError;
const UsageError = common.errors.UsageError;

const Health = common.health.Health;
const ComponentHealthProvider = common.health.ComponentHealthProvider;

const dymes_msg = @import("dymes_msg");
const FrameAllocator = dymes_msg.FrameAllocator;

const Filter = dymes_msg.Filter;
const EagerResults = @import("EagerResults.zig");
const EagerResultsBuilder = @import("EagerResultsBuilder.zig");
const SingleResult = @import("SingleResult.zig");
const SingleResultBuilder = @import("SingleResultBuilder.zig");

const ReadDataset = @import("ReadDataset.zig");

const SegmentsUlidIndex = @import("SegmentsUlidIndex.zig");

const MessageStoreOptions = @import("MessageStoreOptions.zig");

pub const OpenError = CreationError || AccessError || StateError || UsageError;

const component_name: []const u8 = "msg_store.ImmutableStore";
const Self = @This();

pub var sync_all_datasets: bool = false;

/// Message store logger
logger: *logging.Logger,

/// Message store memory allocator
gpa: std.mem.Allocator,

/// Read dataset
read_dataset: ReadDataset,

/// Segment index (Allows fast checking if ULID may be present)
segments_ulid_idx: SegmentsUlidIndex,

/// Data directory path
data_path: []const u8,

/// Open state
opened: bool,

/// Health monitor
health: *Health,

/// Opens a message store.
///
/// Caller must call `close()` to release resources.
pub fn open(gpa: std.mem.Allocator, frame_allocator: std.mem.Allocator, options: MessageStoreOptions) OpenError!*Self {
    var logger = logging.logger(component_name);
    logger.fine()
        .msg("Opening immutable message store")
        .str("data_path", options.data_path)
        .log();

    const data_path_copy = gpa.dupe(u8, options.data_path) catch return OpenError.OutOfMemory;
    errdefer gpa.free(data_path_copy);

    // Prepare segment index
    var segments_ulid_idx = try SegmentsUlidIndex.init(gpa, 10); // FIXME add constant for init seg count
    errdefer segments_ulid_idx.deinit(gpa);

    // Open read dataset
    // TODO: (MSG-28) Improve selection of read dataset, for now, pick the last dataset as victim
    var read_dataset = try ReadDataset.open(gpa, frame_allocator, &segments_ulid_idx, .{
        .dir = options.data_dir,
    });
    errdefer read_dataset.close();

    try read_dataset.populateSegmentsUlidIndex(&segments_ulid_idx);

    assert(@as(u128, @bitCast(read_dataset.first_ulid)) == @as(u128, @bitCast(segments_ulid_idx.entries.items[0].first_id)));
    assert(@as(u128, @bitCast(read_dataset.last_ulid)) == @as(u128, @bitCast(segments_ulid_idx.entries.items[segments_ulid_idx.entries.items.len - 1].last_id)));

    var new_self = gpa.create(Self) catch return OpenError.OutOfMemory;
    _ = &new_self;
    new_self.* = .{
        .logger = logger,
        .gpa = gpa,
        .read_dataset = read_dataset,
        .segments_ulid_idx = segments_ulid_idx,
        .data_path = data_path_copy,
        .opened = true,
        .health = options.health,
    };
    errdefer gpa.destroy(new_self);

    const health_provider = ComponentHealthProvider.init(component_name, new_self, healthProbe);

    try options.health.addProvider(health_provider);

    logger.debug()
        .msg("Immutable message store opened")
        .str("data_path", options.data_path)
        .log();

    return new_self;
}

/// Closes (de-initializes) a message store.
pub fn close(self: *Self) void {
    if (!self.opened) {
        self.logger.fine()
            .msg("Immutable message store already closed")
            .str("data_path", self.data_path)
            .log();
        return;
    }
    defer self.gpa.destroy(self);
    defer self.logger.debug()
        .msg("Immutable message store closed")
        .log();
    self.logger.fine()
        .msg("Closing immutable message store")
        .str("data_path", self.data_path)
        .log();
    defer self.opened = false;
    defer self.gpa.free(self.data_path);
    defer self.segments_ulid_idx.deinit(self.gpa);
    self.health.removeProvider(component_name);
    self.read_dataset.close();
}

/// Queries the message store readiness.
///
/// This is used as one of the component readiness checks during k8s health probes.
///
/// This is *not* the same as "totally healthy", if the minimum number of datasets are ready,
/// even with some needing repair, this will return true.
pub fn ready(self: *const Self) bool {
    if (!self.opened) {
        return false;
    }
    return self.read_dataset.ready();
}

/// Checks if the message store needs repair
pub fn needsRepair(self: *const Self) bool {
    return !self.ready();
}

/// Performs readiness check
fn healthProbe(context: *anyopaque) bool {
    var self: *Self = @ptrCast(@alignCast(context));
    return self.ready();
}

pub const FetchError = AccessError || CreationError || StateError;

/// Queries the location of a message by id
pub fn lookupLocation(self: *Self, msg_id: Ulid) FetchError!?MessageLocation {
    if (self.segments_ulid_idx.lookup(msg_id)) |msg_segment_no| {
        return try self.read_dataset.lookupLocation(msg_segment_no, msg_id);
    }
    return null;
}

/// Queries a single message by id, and adds it to the result builder.
pub fn querySingle(self: *Self, ulid: Ulid, filters: []Filter, result_builder: *SingleResultBuilder) FetchError!void {
    if (self.segments_ulid_idx.lookup(ulid)) |message_segment_no| {
        if (try self.read_dataset.fetch(message_segment_no, ulid, filters)) |message| {
            try result_builder.withMessage(&message);
        }
    }
}

/// Queries multiple messages by id, and adds them to the result builder.
pub fn queryMultiple(self: *Self, ulids: []Ulid, filters: []Filter, result_builder: *EagerResultsBuilder) FetchError!void {
    for (ulids) |ulid| {
        if (self.segments_ulid_idx.lookup(ulid)) |message_segment_no| {
            if (try self.read_dataset.fetch(message_segment_no, ulid, filters)) |message| {
                try result_builder.withMessage(&message);
            }
        }
    }
}

const isBeforeUlid = common.util.isBeforeUlid;

/// Queries multiple messages by range, and eagerly adds them to the result builder.
pub fn queryRangeEager(self: *Self, start_inclusive: Ulid, end_inclusive: Ulid, filters: []Filter, result_builder: *EagerResultsBuilder) FetchError!void {
    assert(isBeforeUlid(start_inclusive, end_inclusive));
    const total_segments = self.segments_ulid_idx.entries.items.len;
    assert(self.read_dataset.num_segments == total_segments);
    const first_ulid = if (self.segments_ulid_idx.mayContain(start_inclusive)) start_inclusive else self.segments_ulid_idx.entries.items[0].first_id;
    const final_ulid = if (self.segments_ulid_idx.mayContain(end_inclusive)) end_inclusive else self.segments_ulid_idx.entries.items[total_segments - 1].last_id;

    self.logger.fine()
        .msg("Querying range of messages")
        .ulid("first_ulid", first_ulid)
        .ulid("final_ulid", final_ulid)
        .str("data_path", self.data_path)
        .log();

    const first_segment: u64 = if (self.segments_ulid_idx.lookup(first_ulid)) |segment_no| segment_no else 0;
    const last_segment = if (self.segments_ulid_idx.lookup(final_ulid)) |segment_no| segment_no else total_segments - 1;
    var it = self.read_dataset.rangeIterator(first_ulid, final_ulid, first_segment, last_segment);
    defer it.close();
    next_msg: while (try it.next()) |msg| {
        for (filters) |filter| {
            if (!filter.apply(msg)) {
                continue :next_msg;
            }
        }
        try result_builder.withMessage(&msg);
    }
}

pub const MessageLocation = @import("MessageLocation.zig");

/// Handles notification that a new message was appended.
///
/// The notification doesn't cause the latest segment to be accessed if it's not already cached.
pub fn appended(self: *Self, channel_id: u128, msg_location: MessageLocation, corr_location: ?MessageLocation) CreationError!void {
    if (!self.segments_ulid_idx.updateExisting(msg_location.segment_no, msg_location.id)) {
        assert(msg_location.segment_no == self.read_dataset.num_segments);
        const dse = self.read_dataset.acquireSegment(msg_location.segment_no) catch |e| {
            self.logger.err()
                .msg("Failed to handle append at segment boundary")
                .err(e)
                .any("msg_location", msg_location)
                .any("corr_location", corr_location)
                .str("data_path", self.data_path)
                .log();
            return CreationError.OtherCreationFailure;
        };
        defer self.read_dataset.releaseSegment(dse);
        const first_id = dse.message_index.entries[0].id;
        try self.segments_ulid_idx.update(msg_location.segment_no, first_id, msg_location.id);
    } else {
        try self.read_dataset.appended(channel_id, msg_location, corr_location);
    }
}

const Message = @import("dymes_msg").Message;

const ImmutableStore = @This();
const AppendStore = @import("AppendStore.zig");

test "ImmutableStore" {
    std.debug.print("test.ImmutableStore.smoke\n", .{});
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

    var logger = logging.logger("test.ImmutableStore.smoke");

    // Prepare config

    logger.fine().msg("Preparing message store options").log();
    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    var health_monitor = Health.init(allocator);
    defer health_monitor.deinit();

    var options = try MessageStoreOptions.init(allocator, tmp_dir.dir, "dummy", &health_monitor);
    defer options.deinit(allocator);

    // Open the append-only message store
    var append_only_store = try AppendStore.open(allocator, frame_allocator, options);
    defer append_only_store.close();

    // Open the immutable message store
    var immutable_store = try ImmutableStore.open(allocator, frame_allocator, options);
    defer immutable_store.close();

    // Populate message store
    const number_of_messages = 10;
    var message_ids: [number_of_messages]Ulid = undefined;
    {
        // Store some messages
        var ulid_generator = common.ulid.generator();

        var map = std.StringArrayHashMap([]const u8).init(allocator);
        defer map.deinit();
        try map.put("0", "zero");
        try map.put("1", "one");
        try map.put("2", "two");
        try map.put("3", "three");
        try map.put("4", "four");
        const message_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

        logger.debug().msg("Storing test messages").int("number_of_messages", number_of_messages).log();
        const message_frame: []u8 = try allocator.alloc(u8, 1024);
        defer allocator.free(message_frame);

        for (0..1) |idx| {
            var test_msg =
                try Message.initOverlay(message_frame, idx, try ulid_generator.next(), idx, 101, message_body, .{ .transient_kv_headers = map });
            const msg_loc = try append_only_store.append(&test_msg);
            try immutable_store.appended(test_msg.channel(), msg_loc, null);
        }

        for (0..number_of_messages) |idx| {
            message_ids[idx] = try ulid_generator.next();
            var test_msg =
                try Message.initOverlay(message_frame, idx, message_ids[idx], idx, 101, message_body, .{ .transient_kv_headers = map });
            const msg_loc = try append_only_store.append(&test_msg);
            message_ids[idx] = msg_loc.id;
            try immutable_store.appended(test_msg.channel(), msg_loc, null);
        }
        logger.debug().msg("Test messages stored").int("number_of_messages", number_of_messages).log();
    }

    logger.debug().msg("Fetching test messages").int("number_of_messages", number_of_messages).log();
    var srb = try SingleResultBuilder.init(allocator);
    defer srb.deinit();
    const noop_filter = [_]Filter{};

    for (0..number_of_messages) |idx| {
        try immutable_store.querySingle(message_ids[idx], &noop_filter, &srb);
        var results = try srb.build();
        if (results) |*results_| {
            defer results_.*.deinit();
            const actual = results_.*.result.?;
            try testing.expectEqual(message_ids[idx], actual.frame_header.id);
        } else {
            @panic("Stored message not found");
        }
    }
    logger.debug().msg("Test messages fetched").int("number_of_messages", number_of_messages).log();
}
