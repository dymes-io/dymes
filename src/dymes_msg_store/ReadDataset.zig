//! Dymes Message Store Read Dataset.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const common = @import("dymes_common");
const Ulid = common.ulid.Ulid;
const isBeforeUlid = common.util.isBeforeUlid;
const logging = common.logging;
const limits = @import("limits.zig");
const constants = @import("constants.zig");

const dymes_msg = @import("dymes_msg");
const msg_limits = dymes_msg.limits;
const FrameAllocator = dymes_msg.FrameAllocator;

const Filter = dymes_msg.Filter;

const errors = @import("errors.zig");

const AllocationError = errors.AllocationError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;
const SyncError = errors.SyncError;

pub const MessageLocation = @import("MessageLocation.zig");
pub const SegmentLocation = @import("SegmentLocation.zig");

const DatasetScanner = @import("DatasetScanner.zig");

const DataSegment = @import("DataSegment.zig");
const SegmentsUlidIndex = @import("SegmentsUlidIndex.zig");
const MessageIndex = @import("MessageIndex.zig");
const MessageFile = @import("MessageFile.zig");
const ChannelIndex = @import("ChannelIndex.zig");

/// Message store dataset options
pub const Options = struct {
    /// Data store directory
    dir: std.fs.Dir,

    /// Segment number
    segment_no: ?u64 = null,
};

/// State of repair
pub const RepairState = enum {
    healthy,
    needs_repair,
    unrepairable,
};

/// Dataset status
pub const Status = struct {
    /// Open state
    opened: bool,

    /// State of repair
    repair_state: RepairState,
};

const Self = @This();

/// Cache of active data segments by segment no
const DataSegmentsCache = @import("DataSegmentsCache.zig");

pub const OpenError = CreationError || AccessError || StateError;

/// Logger
logger: *logging.Logger,

/// Memory allocator
gpa: std.mem.Allocator,

/// Message frame allocator
frame_allocator: std.mem.Allocator,

/// Data store directory
dir: std.fs.Dir,

/// Segment cache
segments_cache: DataSegmentsCache,

/// Segments in use
num_segments: u64,

/// Dataset status
status: Status,

/// First ULID in dataset across segments
first_ulid: Ulid,

/// Last ULID in dataset across segments
last_ulid: Ulid,

/// Segment index (owned by creator)
/// Used for correlation lookup
segments_ulid_idx: *SegmentsUlidIndex,

/// Dataset lock
mtx_dataset: std.Thread.Mutex.Recursive = .init,

/// Opens a read dataset.
///
/// Caller must call `close()` to release resources.
pub fn open(gpa: std.mem.Allocator, frame_allocator: std.mem.Allocator, segments_ulid_idx: *SegmentsUlidIndex, options: Options) OpenError!Self {
    var logger = logging.logger("msg_store.ReadDataset");

    logger
        .fine()
        .msg("Opening read dataset")
        .log();

    // Scan directory for dataset files
    var scanner = try DatasetScanner.init(gpa, options.dir);
    defer scanner.deinit();
    if (scanner.mismatched) {
        return OpenError.InconsistentState;
    }

    // Determine initial data segment
    const initial_segment_no: u64 = if (options.segment_no) |seq| seq else 0;
    var first_ulid: Ulid = .{ .rand = 0x0, .time = 0x0 };
    var last_ulid: Ulid = .{ .rand = 0x0, .time = 0x0 };
    if (initial_segment_no == 0 and scanner.results.len == 0) {
        // We're a new dataset - this cannot be!
        logger
            .err()
            .msg("Read dataset directory contains no message or index files")
            .log();
        return OpenError.FileNotFound;
    }

    // We take advantage of the scanner results being sorted
    assert(scanner.results.len % 2 == 0);
    const msg_file = scanner.results[scanner.results.len - 2];
    const idx_file = scanner.results[scanner.results.len - 1];
    if (msg_file.segment_no != idx_file.segment_no) {
        return OpenError.InconsistentState;
    }
    const last_segment_no = msg_file.segment_no;

    // Open first segment
    var first_segment = try DataSegment.open(gpa, frame_allocator, options.dir, .{
        .initialize = false,
        .segment_no = 0,
        .timestamp = 0x0,
    });
    first_ulid = first_segment.index_file.first_ulid;
    last_ulid = first_segment.index_file.last_ulid;
    first_segment.close();

    if (last_segment_no != 0) {
        // Open last segment
        var last_segment = try DataSegment.open(gpa, frame_allocator, options.dir, .{
            .initialize = false,
            .segment_no = last_segment_no,
            .timestamp = 0x0,
        });
        defer last_segment.close();
        last_ulid = last_segment.index_file.last_ulid;
    }

    var segments_cache = DataSegmentsCache.init(gpa) catch return OpenError.OutOfMemory;
    errdefer segments_cache.deinit();

    logger
        .debug()
        .msg("Read dataset opened")
        .log();
    return .{
        .logger = logger,
        .gpa = gpa,
        .frame_allocator = frame_allocator,
        .dir = options.dir,
        .num_segments = scanner.results.len / 2,
        .segments_cache = segments_cache,
        .status = .{
            .opened = true,
            .repair_state = .healthy,
        },
        .first_ulid = first_ulid,
        .last_ulid = last_ulid,
        .segments_ulid_idx = segments_ulid_idx,
    };
}

/// Closes (de-initializes) a dataset.
pub fn close(self: *Self) void {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();
    if (!self.status.opened) {
        self.logger
            .fine()
            .msg("Read dataset already closed")
            .log();
        return;
    }
    self.logger
        .fine()
        .msg("Closing read dataset")
        .log();
    defer self.segments_cache.deinit();

    self.status.opened = false;
    self.logger
        .debug()
        .msg("Read dataset closed")
        .log();
}

/// Queries whether or not the read dataset may contain the given ULID.
///
/// False positives are possible, but not false negatives.
pub fn mayContain(self: *const Self, id: Ulid) bool {
    return self.first_ulid.equals(id) or self.last_ulid.equals(id) or
        isBeforeUlid(self.first_ulid, id) and isBeforeUlid(id, self.last_ulid);
}

/// Queries the read dataset readiness
pub inline fn ready(self: *const Self) bool {
    return self.status.opened and self.status.repair_state == .healthy;
}

/// Opens a range iterator across segments
///
/// The caller gains ownership of the iterator, and must call `close()` to release resources.
pub fn rangeIterator(self: *Self, first_ulid: Ulid, final_ulid: Ulid, first_segment: u64, final_segment: u64) RangeIterator {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();
    assert(first_segment >= 0);
    assert(final_segment < self.num_segments);
    return RangeIterator.init(self, first_ulid, final_ulid, first_segment, final_segment);
}

/// Opens a channel iterator across segments
///
/// The caller gains ownership of the iterator, and must call `close()` to release resources.
pub fn channelIterator(self: *Self, channel_id: u128, first_ulid: Ulid, final_ulid: Ulid, first_segment: u64, final_segment: u64) AllocationError!ChannelIterator {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();
    assert(first_segment >= 0);
    assert(final_segment < self.num_segments);
    return ChannelIterator.init(self, channel_id, first_ulid, final_ulid, first_segment, final_segment);
}

const Message = @import("dymes_msg").Message;

pub const FetchError = ActiveSegmentError;

/// Returns the location of the message with the given identifier
pub fn lookupLocation(self: *Self, segment_no: u64, msg_id: Ulid) FetchError!?MessageLocation {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();

    const dse = try self.acquireSegment(segment_no);
    defer self.releaseSegment(dse);
    if (dse.message_index.lookup(msg_id)) |index_entry| {
        return .{
            .id = index_entry.id,
            .segment_no = segment_no,
            .file_offset = index_entry.msg_file_offset,
            .frame_size = index_entry.msg_frame_size,
        };
    }
    return null;
}

/// Returns a view of the message with the given id if present.
///
/// This is a *view* of the message, the caller _does not_ gain ownership.
/// Future calls to `fetch` may invalidate the message view.
pub fn fetch(self: *Self, segment_no: u64, ulid: Ulid, filters: []Filter) FetchError!?Message {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();

    const dse = try self.acquireSegment(segment_no);
    defer self.releaseSegment(dse);
    if (dse.message_index.lookup(ulid)) |index_entry| {
        const message_ = dse.data_segment.fetchAt(index_entry.msg_file_offset) catch |e| {
            self.status.repair_state = .needs_repair;
            return e;
        };
        if (message_) |message| {
            for (filters) |_filter| {
                if (!_filter.apply(message)) {
                    return null;
                }
            }
            return message;
        }
    }
    return null;
}

/// Returns a view of the message at the given message location, if present.
///
/// This is a *view* of the message, the caller _does not_ gain ownership.
/// Future calls to `fetch` may invalidate the message view.
pub inline fn fetchAt(self: *Self, msg_location: MessageLocation, msg_buffer: []u8) FetchError!?Message {
    return self.fetchFromSegment(.{
        .segment_no = msg_location.segment_no,
        .file_offset = msg_location.file_offset,
        .frame_size = msg_location.frame_size,
    }, msg_buffer);
}

/// Returns a view of the message at the given segment location, if present.
///
/// This is a *view* of the message, the caller _does not_ gain ownership.
/// Future calls to `fetch` may invalidate the message view.
pub fn fetchFromSegment(self: *Self, seg_location: SegmentLocation, msg_buffer: []u8) FetchError!?Message {
    assert(msg_buffer.len >= seg_location.frame_size);
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();

    const dse = try self.acquireSegment(seg_location.segment_no);
    defer self.releaseSegment(dse);
    const msg = dse.data_segment.fetchAt(seg_location.file_offset) catch |e| {
        self.status.repair_state = .needs_repair;
        return e;
    };
    if (msg) |_msg| {
        const used_frame = _msg.usedFrame();
        if (msg_buffer.len < used_frame.len) {
            self.logger.err()
                .msg("Result message buffer too small")
                .intx("msg_frame_size", _msg.frame_header.frame_size)
                .intx("msg_buffer_len", msg_buffer.len)
                .log();
            return FetchError.OtherAccessFailure;
        }
        @memcpy(msg_buffer[0.._msg.frame_header.frame_size], _msg.usedFrame());
        return Message.overlay(msg_buffer);
    }
    return null;
}

/// Handles notification that a new message was appended.
///
/// We 'update' the segment's index and message file contexts (no actual I/O) and the segment's message index
pub fn appended(self: *Self, channel_id: u128, msg_location: MessageLocation, corr_location: ?MessageLocation) CreationError!void {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();
    // Dataset housekeeping
    if (msg_location.segment_no == 0 and 0x0 == @as(u128, @bitCast(self.first_ulid))) {
        self.first_ulid = msg_location.id;
    }
    self.last_ulid = msg_location.id;

    // Update segment cache entry
    if (self.segments_cache.acquire(msg_location.segment_no)) |*entry| {
        defer self.segments_cache.release(entry.*);
        // Update cache entry
        try entry.message_index.store(msg_location, corr_location);
        try entry.channel_index.store(msg_location, channel_id);
        entry.data_segment.index_file.append_entry_no += 1;
        entry.data_segment.index_file.last_ulid = msg_location.id;
        entry.data_segment.message_file.append_offset += msg_location.frame_size;
    }
}

const ActiveSegmentError = CreationError || AccessError || StateError;

/// Acquires the segment cache entry for the given segment_no.
pub fn acquireSegment(self: *Self, segment_no: u64) ActiveSegmentError!DataSegmentsCache.DataSegmentEntry {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();

    if (self.segments_cache.acquire(segment_no)) |_dse| {
        return _dse;
    }

    // Update number of segments _in use_ if required (there may be more segments in use than are active)
    if (segment_no >= self.num_segments) {
        self.num_segments += 1;
    }

    self.logger
        .fine()
        .msg("Udating data segment cache")
        .int("segment_no", segment_no)
        .int("num_segments", self.num_segments)
        .log();

    // Open desired segment
    const data_segment_ptr = try DataSegment.open(self.gpa, self.frame_allocator, self.dir, .{
        .initialize = false,
        .segment_no = segment_no,
        .timestamp = 0x0,
    });
    errdefer data_segment_ptr.close();

    // Prepare message index
    const msg_index_ptr = try MessageIndex.init(self.gpa);
    errdefer msg_index_ptr.deinit();

    // Prepare channel index
    const channel_index_ptr = try ChannelIndex.init(self.gpa);
    errdefer channel_index_ptr.deinit();

    // Update cache entry
    const dse: DataSegmentsCache.DataSegmentEntry = dse_val: {
        self.segments_cache.put(.{
            .channel_index = channel_index_ptr,
            .message_index = msg_index_ptr,
            .data_segment = data_segment_ptr,
        }) catch |e| return switch (e) {
            DataSegmentsCache.PutError.OutOfMemory => ActiveSegmentError.OutOfMemory,
            else => ActiveSegmentError.OtherCreationFailure,
        };
        break :dse_val self.segments_cache.acquire(segment_no) orelse return ActiveSegmentError.AccessFailure;
    };
    errdefer self.releaseSegment(dse);

    // Populate indexes

    var it = try data_segment_ptr.index_file.iterator();
    defer it.deinit();
    while (try it.next()) |idx_entry| {

        // Lookup correlation location
        const msg_id = idx_entry.frame_header.id;
        const corr_id = idx_entry.frame_header.correlation_id;

        const corr_location: ?MessageLocation = if (corr_id != dymes_msg.constants.NIL_CORRELATION_ID) loc_val: {
            // self.logger.fine()
            //     .msg("Performing correlation lookup")
            //     .ulid("msg_id", msg_id)
            //     .ulid("corr_id", corr_id)
            //     .log();

            // Check earlier in current segment
            if (msg_index_ptr.lookup(corr_id)) |_corr_idx_entry| {
                break :loc_val .{
                    .id = _corr_idx_entry.id,
                    .file_offset = _corr_idx_entry.msg_file_offset,
                    .frame_size = _corr_idx_entry.msg_frame_size,
                    .segment_no = segment_no,
                };
            }

            // Lookup in earlier segments
            if (self.segments_ulid_idx.lookup(corr_id)) |_corr_segment_no| {
                self.logger.debug()
                    .msg("Lookup in earlier segments")
                    .int("segment_no", segment_no)
                    .ulid("corr_id", corr_id)
                    .int("corr_segment_no", _corr_segment_no)
                    .log();

                if (try self.lookupLocation(_corr_segment_no, corr_id)) |_corr_loc| {
                    break :loc_val _corr_loc;
                }
            }

            // Invalid correlation id
            self.logger.err()
                .msg("Unable to index message with invalid correlation reference")
                .ulid("msg_id", msg_id)
                .ulid("corr_id", corr_id)
                .log();
            return ActiveSegmentError.IllegalState;
        } else null;

        const msg_location: MessageLocation = .{
            .id = msg_id,
            .segment_no = segment_no,
            .file_offset = idx_entry.msg_file_offset,
            .frame_size = idx_entry.frame_header.frame_size,
        };

        try msg_index_ptr.store(msg_location, corr_location);
        try channel_index_ptr.store(msg_location, idx_entry.frame_header.channel);
    }

    self.logger
        .debug()
        .msg("Data segment cache updated")
        .int("segment_no", segment_no)
        .int("num_segments", self.num_segments)
        .log();

    return dse;
}

pub fn releaseSegment(self: *Self, dse: DataSegmentsCache.DataSegmentEntry) void {
    self.segments_cache.release(dse);
}

const IndexPopulationError = DataSegment.OpenError;
pub fn populateSegmentsUlidIndex(self: *Self, segments_ulid_idx: *SegmentsUlidIndex) IndexPopulationError!void {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();
    self.logger
        .fine()
        .msg("Populating segments ULID index")
        .int("num_segments", self.num_segments)
        .log();
    for (0..self.num_segments) |segment_no| {
        if (self.segments_cache.acquire(segment_no)) |entry| {
            defer self.segments_cache.release(entry);
            try segments_ulid_idx.update(segment_no, entry.data_segment.index_file.first_ulid, entry.data_segment.index_file.last_ulid);
        } else {
            var segment = try DataSegment.open(self.gpa, self.frame_allocator, self.dir, .{
                .initialize = false,
                .segment_no = segment_no,
                .timestamp = 0x0,
            });
            defer segment.close();
            try segments_ulid_idx.update(segment_no, segment.index_file.first_ulid, segment.index_file.last_ulid);
        }
    }
    self.logger
        .debug()
        .msg("Segments ULID index populated")
        .int("num_segments", self.num_segments)
        .log();
}

const ReadDataset = @This();

test "ReadDataset" {
    std.debug.print("test.ReadDataset.smoke\n", .{});
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

    var logger = logging.logger("test.ReadDataset.smoke");

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    try tmp_dir.dir.makeDir("test-read-dataset");

    const timestamp = std.time.nanoTimestamp();

    var dummy_segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer dummy_segments_ulid_idx.deinit(allocator);

    try testing.expectError(OpenError.FileNotFound, ReadDataset.open(allocator, frame_allocator, &dummy_segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    }));

    var ulid_generator = common.ulid.generator();

    var map = std.StringArrayHashMap([]const u8).init(allocator);
    defer map.deinit();

    try map.put("0", "zero");
    try map.put("1", "one");
    try map.put("2", "two");
    try map.put("3", "three");
    try map.put("4", "four");

    const message_frame: []u8 = try frame_allocator.alloc(u8, 1024);
    defer frame_allocator.free(message_frame);
    const message_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

    const older_ulid = try ulid_generator.next();

    // POPULATE

    const number_of_messages = 3;
    var message_ids: [number_of_messages]common.ulid.Ulid = undefined;

    {
        // Open the dataset
        const Dataset = @import("Dataset.zig");
        var dataset = try Dataset.open(allocator, frame_allocator, timestamp, .{
            .dir = tmp_dir.dir,
        });
        defer dataset.close();

        logger.debug().msg("Storing test messages").int("number_of_messages", number_of_messages).log();
        for (0..number_of_messages) |idx| {
            var test_msg =
                try Message.initOverlay(message_frame, idx, try ulid_generator.next(), idx, 101, message_body, .{ .transient_kv_headers = map });
            logger.debug()
                .msg("Storing test message")
                .int("idx", idx)
                .any("test_msg", test_msg)
                .log();
            _ = try dataset.store(&test_msg);
            message_ids[idx] = test_msg.frame_header.id;
        }
        logger.debug().msg("Test messages stored").int("number_of_messages", number_of_messages).log();
        try dataset.sync();
    }

    var segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer segments_ulid_idx.deinit(allocator);

    var read_dataset = try ReadDataset.open(allocator, frame_allocator, &segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    });
    defer read_dataset.close();

    const newer_ulid = try ulid_generator.next();
    try testing.expect(!read_dataset.mayContain(older_ulid));
    try testing.expect(!read_dataset.mayContain(newer_ulid));
    for (0..number_of_messages) |idx| {
        const expected_ulid = message_ids[idx];
        try testing.expect(read_dataset.mayContain(expected_ulid));
    }

    const noop_filter = [_]Filter{};

    logger.debug().msg("Fetching test messages").int("number_of_messages", number_of_messages).log();
    _ = try read_dataset.fetch(0x0, newer_ulid, &noop_filter);
    var timer = try std.time.Timer.start();
    var lookup_time: u64 = 0;
    for (0..number_of_messages) |idx| {
        const expected_ulid = message_ids[idx];
        timer.reset();
        const msg = try read_dataset.fetch(0x0, expected_ulid, &noop_filter) orelse @panic("Oh dear");
        lookup_time += timer.lap();
        try testing.expect(msg.frame_header.id.equals(expected_ulid));
    }
    const avg_ns = lookup_time / number_of_messages;
    logger.debug()
        .msg("Test messages fetched")
        .int("number_of_messages", number_of_messages)
        .int("avg_fetch_µs", avg_ns / std.time.ns_per_us)
        .log();
}

test "ReadDataset.appended" {
    std.debug.print("test.ReadDataset.appended\n", .{});
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

    var logger = logging.logger("test.ReadDataset.appended");

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    try tmp_dir.dir.makeDir("test-read-dataset-appended");

    const abs_dir_name = try tmp_dir.dir.realpathAlloc(allocator, "test-read-dataset-appended");
    defer allocator.free(abs_dir_name);

    const timestamp = std.time.nanoTimestamp();

    var segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer segments_ulid_idx.deinit(allocator);

    try testing.expectError(OpenError.FileNotFound, ReadDataset.open(allocator, frame_allocator, &segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    }));

    // Open the dataset
    const Dataset = @import("Dataset.zig");
    var dataset = try Dataset.open(allocator, frame_allocator, timestamp, .{
        .dir = tmp_dir.dir,
    });
    defer dataset.close();

    // Open read dataset
    var read_dataset = try ReadDataset.open(allocator, frame_allocator, &segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    });
    defer read_dataset.close();

    // Store some messages

    var ulid_generator = common.ulid.generator();

    var map = std.StringArrayHashMap([]const u8).init(allocator);
    defer map.deinit();

    try map.put("0", "zero");
    try map.put("1", "one");
    try map.put("2", "two");
    try map.put("3", "three");
    try map.put("4", "four");

    const message_frame: []u8 = try frame_allocator.alloc(u8, 1024);
    defer frame_allocator.free(message_frame);
    const message_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

    const older_ulid = try ulid_generator.next();
    const noop_filter = [_]Filter{};

    _ = try read_dataset.fetch(0x0, older_ulid, &noop_filter);

    const number_of_messages = 3;
    var message_ids: [number_of_messages]common.ulid.Ulid = undefined;
    logger.debug().msg("Storing test messages with append notification").int("number_of_messages", number_of_messages).log();
    for (0..number_of_messages) |idx| {
        var test_msg =
            try Message.initOverlay(message_frame, idx, try ulid_generator.next(), idx, 101, message_body, .{ .transient_kv_headers = map });
        const msg_loc = try dataset.store(&test_msg);
        try read_dataset.appended(test_msg.channel(), msg_loc, null);
        message_ids[idx] = test_msg.frame_header.id;
    }
    logger.debug().msg("Test messages stored with append notification").int("number_of_messages", number_of_messages).log();
    try dataset.sync();

    const newer_ulid = try ulid_generator.next();
    try testing.expect(!read_dataset.mayContain(older_ulid));
    try testing.expect(!read_dataset.mayContain(newer_ulid));
    for (0..number_of_messages) |idx| {
        const expected_ulid = message_ids[idx];
        try testing.expect(read_dataset.mayContain(expected_ulid));
    }

    logger.debug().msg("Fetching test messages").int("number_of_messages", number_of_messages).log();
    var timer = try std.time.Timer.start();
    var lookup_time: u64 = 0;
    for (0..number_of_messages) |idx| {
        const expected_ulid = message_ids[idx];
        timer.reset();
        const msg = try read_dataset.fetch(0x0, expected_ulid, &noop_filter) orelse @panic("Oh dear");
        lookup_time += timer.lap();
        try testing.expect(msg.frame_header.id.equals(expected_ulid));
    }
    const avg_ns = lookup_time / number_of_messages;
    logger.debug()
        .msg("Test messages fetched")
        .int("number_of_messages", number_of_messages)
        .int("avg_fetch_µs", avg_ns / std.time.ns_per_us)
        .log();
}

const IteratorError = CreationError || AccessError || StateError;

pub const RangeIterator = struct {
    logger: *logging.Logger,
    current_segment: u64,
    first_ulid: Ulid,
    final_ulid: Ulid,
    first_segment: u64,
    final_segment: u64,
    read_dataset: *ReadDataset,
    msg_file_it_: ?MessageFile.Iterator,
    reached_end: bool = false,

    fn init(read_dataset: *ReadDataset, first_ulid: Ulid, final_ulid: Ulid, first_segment: u64, final_segment: u64) RangeIterator {
        assert(first_segment <= final_segment);

        return .{
            .logger = logging.logger("msg_store.ReadDataset.RangeIterator"),
            .read_dataset = read_dataset,
            .first_ulid = first_ulid,
            .final_ulid = final_ulid,
            .first_segment = first_segment,
            .final_segment = final_segment,
            .current_segment = first_segment,
            .msg_file_it_ = null,
        };
    }

    pub fn close(self: *RangeIterator) void {
        // self.logger.fine()
        //     .msg("Closing range iterator")
        //     .int("current_segment", self.current_segment)
        //     .int("first_segment", self.first_segment)
        //     .int("final_segment", self.final_segment)
        //     .ulid("first_ulid", self.first_ulid)
        //     .ulid("final_ulid", self.final_ulid)
        //     .boolean("reached_end", self.reached_end)
        //     .log();

        if (self.msg_file_it_) |*msg_file_it| {
            // self.logger.fine()
            //     .msg("Closing message file iterator")
            //     .int("current_segment", self.current_segment)
            //     .int("first_segment", self.first_segment)
            //     .int("final_segment", self.final_segment)
            //     .ulid("first_ulid", self.first_ulid)
            //     .ulid("final_ulid", self.final_ulid)
            //     .boolean("reached_end", self.reached_end)
            //     .log();
            msg_file_it.close();
        }
    }

    pub fn next(self: *RangeIterator) IteratorError!?Message {
        // self.logger.fine()
        //     .msg("Start of next range iteration")
        //     .int("current_segment", self.current_segment)
        //     .int("first_segment", self.first_segment)
        //     .int("final_segment", self.final_segment)
        //     .ulid("first_ulid", self.first_ulid)
        //     .ulid("final_ulid", self.final_ulid)
        //     .boolean("reached_end", self.reached_end)
        //     .log();

        if (self.reached_end) {
            // self.logger.fine()
            //     .msg("Short-circuiting next()")
            //     .int("current_segment", self.current_segment)
            //     .int("first_segment", self.first_segment)
            //     .int("final_segment", self.final_segment)
            //     .ulid("first_ulid", self.first_ulid)
            //     .ulid("final_ulid", self.final_ulid)
            //     .boolean("reached_end", self.reached_end)
            //     .log();

            // Short-circuit
            return null;
        }

        // Fetch next from current segment's message file
        if (self.msg_file_it_) |*msg_file_it| {
            const fetched = try msg_file_it.next();
            if (fetched) |val| {
                // self.logger.fine()
                //     .msg("Found message")
                //     .ulid("message_id", val.frame_header.id)
                //     .int("current_segment", self.current_segment)
                //     .int("first_segment", self.first_segment)
                //     .int("final_segment", self.final_segment)
                //     .ulid("first_ulid", self.first_ulid)
                //     .ulid("final_ulid", self.final_ulid)
                //     .boolean("reached_end", self.reached_end)
                //     .log();
                if (@as(u128, @bitCast(val.frame_header.id)) == @as(u128, @bitCast(self.final_ulid))) {
                    self.reached_end = true; // Actually found final desired message
                    // self.logger.fine()
                    //     .msg("Reached final message")
                    //     .ulid("message_id", val.frame_header.id)
                    //     .int("current_segment", self.current_segment)
                    //     .int("first_segment", self.first_segment)
                    //     .int("final_segment", self.final_segment)
                    //     .ulid("first_ulid", self.first_ulid)
                    //     .ulid("final_ulid", self.final_ulid)
                    //     .boolean("reached_end", self.reached_end)
                    //     .log();
                } else if (isBeforeUlid(self.final_ulid, val.frame_header.id)) {
                    self.reached_end = true; // We overshot the final desired message
                    // self.logger.fine()
                    //     .msg("Overshot final message")
                    //     .ulid("message_id", val.frame_header.id)
                    //     .int("current_segment", self.current_segment)
                    //     .int("first_segment", self.first_segment)
                    //     .int("final_segment", self.final_segment)
                    //     .ulid("first_ulid", self.first_ulid)
                    //     .ulid("final_ulid", self.final_ulid)
                    //     .boolean("reached_end", self.reached_end)
                    //     .log();
                    return null;
                }
                return val;
            }
            if (self.current_segment == self.final_segment) {
                self.reached_end = true; // Ran out of messages before reaching final message
                // self.logger.fine()
                //     .msg("Ran out of messages before reaching final message")
                //     .int("current_segment", self.current_segment)
                //     .int("first_segment", self.first_segment)
                //     .int("final_segment", self.final_segment)
                //     .ulid("first_ulid", self.first_ulid)
                //     .ulid("final_ulid", self.final_ulid)
                //     .boolean("reached_end", self.reached_end)
                //     .log();
                return null;
            } else {
                // self.logger.fine()
                //     .msg("Ran out of messages in current segment")
                //     .int("current_segment", self.current_segment)
                //     .int("first_segment", self.first_segment)
                //     .int("final_segment", self.final_segment)
                //     .ulid("first_ulid", self.first_ulid)
                //     .ulid("final_ulid", self.final_ulid)
                //     .boolean("reached_end", self.reached_end)
                //     .log();

                // We've run out messages in the current segment, more remains
                msg_file_it.close();
                self.msg_file_it_ = null;
                self.current_segment += 1;
            }
        }

        // No current segment, or non-final segment fully traversed
        const dse = try self.read_dataset.acquireSegment(self.current_segment);
        defer self.read_dataset.releaseSegment(dse);

        const iterator_offset: u64 = if (self.current_segment == self.first_segment) offset: {
            break :offset if (dse.message_index.lookupClosest(self.first_ulid)) |mie| mie.msg_file_offset else @sizeOf(MessageFile.Header);
        } else @sizeOf(MessageFile.Header);

        // self.logger.fine()
        //     .msg("Opening segment message file iterator")
        //     .int("iterator_offset", iterator_offset)
        //     .int("current_segment", self.current_segment)
        //     .int("first_segment", self.first_segment)
        //     .int("final_segment", self.final_segment)
        //     .ulid("first_ulid", self.first_ulid)
        //     .ulid("final_ulid", self.final_ulid)
        //     .boolean("reached_end", self.reached_end)
        //     .log();

        if (self.msg_file_it_) |*msg_file_it| {
            msg_file_it.close();
            self.msg_file_it_ = null;
        }

        self.msg_file_it_ = try dse.data_segment.message_file.offsetIterator(iterator_offset);
        errdefer self.msg_file_it_.?.close();
        return self.next(); // Now attempt fetch again
    }

    pub fn rewind(self: *RangeIterator) void {
        if (self.msg_file_it_) |*msg_file_it| {
            msg_file_it.close();
            self.msg_file_it_ = null;
        }
        self.current_segment = self.first_segment;
        self.reached_end = false;
    }
};

test "ReadDataset.RangeIterator" {
    std.debug.print("test.ReadDataset.RangeIterator\n", .{});
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

    var logger = logging.logger("test.ReadDataset.RangeIterator");

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    try tmp_dir.dir.makeDir("test-range-iterator");

    const abs_dir_name = try tmp_dir.dir.realpathAlloc(allocator, "test-range-iterator");
    defer allocator.free(abs_dir_name);

    const timestamp = std.time.nanoTimestamp();

    var dummy_segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer dummy_segments_ulid_idx.deinit(allocator);

    try testing.expectError(OpenError.FileNotFound, ReadDataset.open(allocator, frame_allocator, &dummy_segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    }));

    // POPULATE
    var ulid_generator = common.ulid.generator();

    const number_of_messages: usize = @min((limits.max_message_file_size - 128) / 1024 + 10, limits.max_message_file_entries + 10);
    logger.debug()
        .msg("Will store and range query messages")
        .int("number_of_messages", number_of_messages)
        .log();
    var message_ids: []Ulid = try allocator.alloc(Ulid, number_of_messages);
    defer allocator.free(message_ids);
    const message_id_before_first: Ulid = try ulid_generator.next();
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
        message_id_last_in_segment = message_ids[limits.max_message_file_entries];
        message_id_first_in_next_segment = message_ids[limits.max_message_file_entries + 1];

        try dataset.sync();
    }

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
        var it = read_dataset.rangeIterator(message_id_before_first, message_id_last, 0, 1);
        defer it.close();

        while (try it.next()) |found_msg| : (found_idx += 1) {
            found_ids[found_idx] = found_msg.frame_header.id;
        }
        try testing.expectEqualSlices(Ulid, message_ids[0..], found_ids[0..]);
    }

    logger.debug().msg("Fetching early range of test messages").log();

    {
        var found_ids: []Ulid = try allocator.alloc(Ulid, number_of_messages);
        defer allocator.free(found_ids);
        var found_idx: usize = 0;
        var it = read_dataset.rangeIterator(message_ids[0], message_ids[99], 0, 0);
        defer it.close();

        while (try it.next()) |found_msg| : (found_idx += 1) {
            found_ids[found_idx] = found_msg.frame_header.id;
        }
        try testing.expectEqualSlices(Ulid, message_ids[0..100], found_ids[0..100]);
    }

    logger.debug().msg("Fetching late range of test messages").log();

    {
        var found_ids: []Ulid = try allocator.alloc(Ulid, number_of_messages);
        defer allocator.free(found_ids);
        var found_idx: usize = 0;
        var it = read_dataset.rangeIterator(message_ids[900], message_ids[999], 0, 0);
        defer it.close();

        while (try it.next()) |found_msg| : (found_idx += 1) {
            found_ids[found_idx] = found_msg.frame_header.id;
        }
        try testing.expectEqualSlices(Ulid, message_ids[900..1000], found_ids[0..100]);
    }
}

pub const ChannelIterator = struct {
    logger: *logging.Logger,
    channel_id: u128,
    current_segment: u64,
    first_ulid: Ulid,
    final_ulid: Ulid,
    first_segment: u64,
    final_segment: u64,
    read_dataset: *ReadDataset,
    reached_end: bool = false,
    channel_run_idx: ?usize,
    msg_buffer: []u8,
    reached_first: bool = false,

    fn init(read_dataset: *ReadDataset, channel_id: u128, first_ulid: Ulid, final_ulid: Ulid, first_segment: u64, final_segment: u64) AllocationError!ChannelIterator {
        assert(isBeforeUlid(first_ulid, final_ulid));
        assert(first_segment <= final_segment);

        const msg_buffer = read_dataset.frame_allocator.alloc(u8, msg_limits.max_frame_size) catch return AllocationError.OutOfMemory;
        errdefer read_dataset.frame_allocator.free(msg_buffer);

        return .{
            .logger = logging.logger("msg_store.ReadDataset.ChannelIterator"),
            .channel_id = channel_id,
            .read_dataset = read_dataset,
            .first_ulid = first_ulid,
            .final_ulid = final_ulid,
            .first_segment = first_segment,
            .final_segment = final_segment,
            .current_segment = first_segment,
            .channel_run_idx = null,
            .msg_buffer = msg_buffer,
        };
    }

    pub fn close(self: *ChannelIterator, frame_allocator: std.mem.Allocator) void {
        defer frame_allocator.free(self.msg_buffer);
        // FIXME - remove dev tracing
        // self.logger.fine()
        //     .msg("Closing channel iterator")
        //     .intx("channel_id", self.channel_id)
        //     .int("current_segment", self.current_segment)
        //     .int("first_segment", self.first_segment)
        //     .int("final_segment", self.final_segment)
        //     .ulid("first_ulid", self.first_ulid)
        //     .ulid("final_ulid", self.final_ulid)
        //     .boolean("reached_end", self.reached_end)
        //     .log();
    }

    pub fn next(self: *ChannelIterator) IteratorError!?Message {
        // FIXME - remove dev tracing
        // self.logger.fine()
        //     .msg("Start of next iteration")
        //     .intx("channel_id", self.channel_id)
        //     .int("current_segment", self.current_segment)
        //     .int("first_segment", self.first_segment)
        //     .int("final_segment", self.final_segment)
        //     .ulid("first_ulid", self.first_ulid)
        //     .ulid("final_ulid", self.final_ulid)
        //     .boolean("reached_end", self.reached_end)
        //     .log();

        if (self.reached_end) {
            // FIXME - remove dev tracing
            // self.logger.fine()
            //     .msg("Short-circuiting next()")
            //     .intx("channel_id", self.channel_id)
            //     .int("current_segment", self.current_segment)
            //     .int("first_segment", self.first_segment)
            //     .int("final_segment", self.final_segment)
            //     .ulid("first_ulid", self.first_ulid)
            //     .ulid("final_ulid", self.final_ulid)
            //     .boolean("reached_end", self.reached_end)
            //     .log();

            // Short-circuit
            return null;
        }

        // Sets channel_run to first segment containing a channel run for channel_id, scanning current segment up to final segment for channel matches
        const channel_run: []const ChannelIndex.ChannelRunEntry = run_val: {
            for (self.current_segment..self.final_segment + 1) |active_segment| {
                const dse = try self.read_dataset.acquireSegment(active_segment);
                defer self.read_dataset.releaseSegment(dse);
                if (dse.channel_index.lookup(self.channel_id)) |channel_run| {
                    assert(channel_run.len != 0);
                    if (self.channel_run_idx == null) {
                        self.channel_run_idx = 0;
                    }
                    self.current_segment = active_segment;
                    break :run_val channel_run;
                } else {
                    // FIXME - remove dev tracing
                    // self.logger.fine()
                    //     .msg("No channel matches in current segment")
                    //     .intx("channel_id", self.channel_id)
                    //     .int("current_segment", self.current_segment)
                    //     .int("first_segment", self.first_segment)
                    //     .int("final_segment", self.final_segment)
                    //     .ulid("first_ulid", self.first_ulid)
                    //     .ulid("final_ulid", self.final_ulid)
                    //     .boolean("reached_end", self.reached_end)
                    //     .log();
                    self.channel_run_idx = null;
                    if (self.reached_first) {
                        self.reached_end = true;
                        // FIXME - remove dev tracing
                        // self.logger.fine()
                        //     .msg("Reached end with no channel matches in current segment after reaching first in prior")
                        //     .intx("channel_id", self.channel_id)
                        //     .int("current_segment", self.current_segment)
                        //     .int("first_segment", self.first_segment)
                        //     .int("final_segment", self.final_segment)
                        //     .ulid("first_ulid", self.first_ulid)
                        //     .ulid("final_ulid", self.final_ulid)
                        //     .boolean("reached_end", self.reached_end)
                        //     .log();
                    }
                    continue;
                }
            }
            // FIXME - remove dev tracing
            // self.logger.fine()
            //     .msg("No segment has messages for channel")
            //     .intx("channel_id", self.channel_id)
            //     .int("current_segment", self.current_segment)
            //     .int("first_segment", self.first_segment)
            //     .int("final_segment", self.final_segment)
            //     .ulid("first_ulid", self.first_ulid)
            //     .ulid("final_ulid", self.final_ulid)
            //     .boolean("reached_end", self.reached_end)
            //     .log();

            // No segment has messages for channel
            self.channel_run_idx = null;
            self.reached_end = true;
            return null;
        };

        // Fetch next from current segment's channel run
        if (self.channel_run_idx) |_channel_run_idx| {
            if (_channel_run_idx < channel_run.len) {
                if (self.reached_first) {
                    const run_entry = channel_run[_channel_run_idx];
                    const seg_loc: SegmentLocation = .{ .segment_no = self.current_segment, .file_offset = run_entry.file_offset, .frame_size = run_entry.frame_size };
                    const msg = try self.read_dataset.fetchFromSegment(seg_loc, self.msg_buffer);
                    if (msg) |_msg| {
                        if (isSameOrBeforeUlid(_msg.id(), self.final_ulid)) {
                            defer self.channel_run_idx = _channel_run_idx + 1;
                            return _msg;
                        }
                        // Overshot final message
                        self.channel_run_idx = null;
                        self.reached_end = true;
                        return null;
                    } else {
                        return IteratorError.EntryNotFound;
                    }
                } else {
                    for (_channel_run_idx..channel_run.len) |_cridx| {
                        const run_entry = channel_run[_cridx];
                        const seg_loc: SegmentLocation = .{ .segment_no = self.current_segment, .file_offset = run_entry.file_offset, .frame_size = run_entry.frame_size };
                        const msg = try self.read_dataset.fetchFromSegment(seg_loc, self.msg_buffer);
                        if (msg) |_msg| {
                            if (isSameOrAfterUlid(_msg.id(), self.first_ulid)) {
                                self.reached_first = true;
                                defer self.channel_run_idx = _cridx + 1;
                                return _msg;
                            }
                        } else {
                            return IteratorError.EntryNotFound;
                        }
                    }
                    // FIXME - remove dev tracing
                    // self.logger.fine()
                    //     .msg("First not reached in current segment")
                    //     .intx("channel_id", self.channel_id)
                    //     .int("current_segment", self.current_segment)
                    //     .int("first_segment", self.first_segment)
                    //     .int("final_segment", self.final_segment)
                    //     .ulid("first_ulid", self.first_ulid)
                    //     .ulid("final_ulid", self.final_ulid)
                    //     .boolean("reached_end", self.reached_end)
                    //     .log();
                    // Reached end of channel run in current segment before reaching first ULID
                    self.channel_run_idx = null;
                    self.current_segment += 1;
                    return self.next();
                }
            } else if (self.current_segment == self.final_segment) {
                // FIXME - remove dev tracing
                // self.logger.fine()
                //     .msg("Reached end of channel run in final segment")
                //     .intx("channel_id", self.channel_id)
                //     .int("current_segment", self.current_segment)
                //     .int("first_segment", self.first_segment)
                //     .int("final_segment", self.final_segment)
                //     .ulid("first_ulid", self.first_ulid)
                //     .ulid("final_ulid", self.final_ulid)
                //     .boolean("reached_end", self.reached_end)
                //     .log();
                // Reached end of channel run in final segment
                self.channel_run_idx = null;
                self.reached_end = true;
                return null;
            } else {
                // FIXME - remove dev tracing
                // self.logger.fine()
                //     .msg("Reached end of channel run in current segment")
                //     .intx("channel_id", self.channel_id)
                //     .int("current_segment", self.current_segment)
                //     .int("first_segment", self.first_segment)
                //     .int("final_segment", self.final_segment)
                //     .ulid("first_ulid", self.first_ulid)
                //     .ulid("final_ulid", self.final_ulid)
                //     .boolean("reached_end", self.reached_end)
                //     .log();
                // Reached end of channel run in current segment
                self.channel_run_idx = null;
                self.current_segment += 1;
                return self.next();
            }
        } else {
            self.logger.err()
                .msg("Uninitialized channel run index")
                .intx("channel_id", self.channel_id)
                .int("current_segment", self.current_segment)
                .int("first_segment", self.first_segment)
                .int("final_segment", self.final_segment)
                .ulid("first_ulid", self.first_ulid)
                .ulid("final_ulid", self.final_ulid)
                .boolean("reached_end", self.reached_end)
                .log();
            return IteratorError.IllegalState;
        }
    }

    pub fn rewind(self: *ChannelIterator) void {
        self.current_segment = self.first_segment;
        self.reached_first = false;
        self.reached_end = false;
        self.channel_run_idx = null;
    }
};

pub inline fn isSameOrBeforeUlid(ulid_l: Ulid, ulid_r: Ulid) bool {
    if (ulid_l.time < ulid_r.time) {
        return true;
    } else if (ulid_l.time == ulid_r.time and ulid_l.rand <= ulid_r.rand) { // DESIGN-DECISION: end-of-range: [] or [)
        return true;
    }
    return false;
}

pub inline fn isSameOrAfterUlid(ulid_l: Ulid, ulid_r: Ulid) bool {
    if (ulid_l.time > ulid_r.time) {
        return true;
    } else if (ulid_l.time == ulid_r.time and ulid_l.rand >= ulid_r.rand) {
        return true;
    }
    return false;
}

test "ReadDataset.ChannelIterator.smoke" {
    std.debug.print("test.ReadDataset.ChannelIterator.smoke\n", .{});
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

    var logger = logging.logger("test.ReadDataset.ChannelIterator.smoke");

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    try tmp_dir.dir.makeDir("test-channel-iterator-smoke");

    const abs_dir_name = try tmp_dir.dir.realpathAlloc(allocator, "test-channel-iterator-smoke");
    defer allocator.free(abs_dir_name);

    const timestamp = std.time.nanoTimestamp();

    var dummy_segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer dummy_segments_ulid_idx.deinit(allocator);

    try testing.expectError(OpenError.FileNotFound, ReadDataset.open(allocator, frame_allocator, &dummy_segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    }));

    // POPULATE
    var ulid_generator = common.ulid.generator();
    const first_ulid: Ulid = try ulid_generator.next();

    // const number_of_messages: usize = @min((limits.max_message_file_size - 128) / 1024 + 10, limits.max_message_file_entries + 10);
    const number_of_channels: usize = 100;
    const messages_per_channel: usize = 100;
    const number_of_messages: usize = number_of_channels * messages_per_channel;
    logger.debug()
        .msg("Will store and channel query messages")
        .int("number_of_messages", number_of_messages)
        .log();

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
        // const message_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

        const message_body = "A" ** 896;

        logger.debug().msg("Storing block of test messages").log();
        for (0..number_of_channels) |channel_id| {
            for (0..messages_per_channel) |_idx| {
                var test_msg =
                    try Message.initOverlay(message_frame, _idx, try ulid_generator.next(), channel_id, 101, message_body, .{});
                _ = try dataset.store(&test_msg);
            }
        }
        logger.debug().msg("Test messages stored").int("number_of_messages", number_of_messages).log();

        try dataset.sync();
    }
    const final_ulid: Ulid = try ulid_generator.next();

    var segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer segments_ulid_idx.deinit(allocator);

    var read_dataset = try ReadDataset.open(allocator, frame_allocator, &segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    });
    defer read_dataset.close();

    logger.debug().msg("Hackery").log();
    _ = &first_ulid;
    _ = &final_ulid;

    logger.debug().msg("Performing channel queries").log();
    for (0..number_of_channels) |channel_id| {
        for (0..messages_per_channel) |_| {
            var it = try read_dataset.channelIterator(channel_id, first_ulid, final_ulid, 0, read_dataset.num_segments - 1);
            defer it.close(frame_allocator);

            var actual_count: usize = 0;
            while (try it.next()) |_| {
                actual_count += 1;
            }
            try testing.expectEqual(messages_per_channel, actual_count);
        }
    }
}

test "ReadDataset.ChannelIterator.bounds" {
    std.debug.print("test.ReadDataset.ChannelIterator.bounds\n", .{});
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

    var logger = logging.logger("test.ReadDataset.ChannelIterator.bounds");

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    try tmp_dir.dir.makeDir("test-channel-iterator-bounds");

    const abs_dir_name = try tmp_dir.dir.realpathAlloc(allocator, "test-channel-iterator-bounds");
    defer allocator.free(abs_dir_name);

    const timestamp = std.time.nanoTimestamp();

    var dummy_segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer dummy_segments_ulid_idx.deinit(allocator);

    try testing.expectError(OpenError.FileNotFound, ReadDataset.open(allocator, frame_allocator, &dummy_segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    }));

    // POPULATE
    var ulid_generator = common.ulid.generator();
    const before_first_ulid: Ulid = try ulid_generator.next();
    const first_ulid: Ulid = try ulid_generator.next();
    var final_ulid: Ulid = undefined;
    const channel_id: u128 = 123456789;

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

        logger.debug().msg("Test messages").log();

        var before_first_msg =
            try Message.initOverlay(message_frame, 0, before_first_ulid, channel_id, 101, message_body, .{});
        _ = try dataset.store(&before_first_msg);

        var first_msg =
            try Message.initOverlay(message_frame, 1, first_ulid, channel_id, 101, message_body, .{});
        _ = try dataset.store(&first_msg);
        for (0..3) |idx| {
            var test_msg =
                try Message.initOverlay(message_frame, idx + 2, try ulid_generator.next(), channel_id, 101, message_body, .{});
            _ = try dataset.store(&test_msg);
        }
        final_ulid = try ulid_generator.next();
        var final_msg =
            try Message.initOverlay(message_frame, 5, final_ulid, channel_id, 101, message_body, .{});
        _ = try dataset.store(&final_msg);

        var after_final_msg =
            try Message.initOverlay(message_frame, 6, try ulid_generator.next(), channel_id, 101, message_body, .{});
        _ = try dataset.store(&after_final_msg);

        try dataset.sync();
    }

    var segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer segments_ulid_idx.deinit(allocator);

    var read_dataset = try ReadDataset.open(allocator, frame_allocator, &segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    });
    defer read_dataset.close();

    logger.debug().msg("Performing channel query").log();
    {
        var it = try read_dataset.channelIterator(channel_id, first_ulid, final_ulid, 0, 0);
        defer it.close(frame_allocator);

        var actual_count: usize = 0;
        while (try it.next()) |_| {
            actual_count += 1;
        }
        logger.debug()
            .msg("Expecting 5 results")
            .int("actual_count", actual_count)
            .log();
        try testing.expectEqual(5, actual_count);
    }
}
