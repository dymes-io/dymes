//! Dymes Message Store In-Memory Message Index.
//!
//! Each message index is specific to a particular store and segment
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const assert = std.debug.assert;

const common = @import("dymes_common");
const errors = common.errors;

const AllocationError = common.errors.AllocationError;
const CreationError = common.errors.CreationError;
const AccessError = common.errors.AccessError;
const StateError = common.errors.StateError;

const logging = common.logging;
const Ulid = common.ulid.Ulid;
const isBeforeUlid = common.util.isBeforeUlid;
const constants = @import("constants.zig");
const limits = @import("limits.zig");
const dymes_msg = @import("dymes_msg");
const FrameHeader = dymes_msg.Message.FrameHeader;
const Message = dymes_msg.Message;

const MessageLocation = @import("MessageLocation.zig");

/// In-memory message index entry
const MessageIndexEntry = extern struct {
    /// Message identifier
    id: Ulid,

    /// Correlation identifier
    corr_id: Ulid,

    /// Offset of related message in its message file
    msg_file_offset: u64,

    /// Offset of correlated message in its message file
    corr_file_offset: u64,

    /// Segment number of correlated message
    corr_segment_no: u64,

    /// The size of the message frame: frame header + payload (KV headers, KV data + message body) + padding
    msg_frame_size: u32,

    /// The size of the correlated message frame: frame header + payload (KV headers, KV data + message body) + padding
    corr_frame_size: u32,
};

comptime {
    assert(@sizeOf(MessageIndexEntry) == 64);
    assert(common.util.checkNoPadding(MessageIndexEntry));
}

/// Index buffer size (in bytes)
/// `limits.max_message_file_entries` was massaged to factor in size of index entries yielding ≈ 16MiB in-memory index buffer
pub const index_buffer_size = limits.max_message_file_entries * @sizeOf(MessageIndexEntry);

const Self = @This();

/// Logger
logger: *logging.Logger,

/// General purpose allocator
gpa: Allocator,

/// Index buffer
entries: []MessageIndexEntry,

/// Append index
append_idx: usize,

///
/// Allocates and initializes the in-memory index.
///
/// Caller must call `deinit()` to release resources.
pub fn init(gpa: std.mem.Allocator) AllocationError!*Self {
    const logger = logging.logger("msg_store.MessageIndex");

    const new_self = gpa.create(Self) catch return CreationError.OutOfMemory;
    errdefer gpa.destroy(new_self);

    const entries = gpa.alignedAlloc(MessageIndexEntry, null, limits.max_message_file_entries) catch return AllocationError.OutOfMemory;
    errdefer gpa.free(entries);

    new_self.* = .{
        .logger = logger,
        .gpa = gpa,
        .entries = entries,
        .append_idx = 0,
    };
    return new_self;
}

/// De-initializes a message index entry.
pub fn deinit(self: *Self) void {
    defer self.gpa.destroy(self);
    defer self.gpa.free(self.entries);
}

/// Appends an entry to the in-memory index
pub fn store(self: *Self, msg_loc: MessageLocation, corr_loc: ?MessageLocation) CreationError!void {
    if (self.append_idx >= self.entries.len) {
        self.logger
            .err()
            .msg("In-memory message file index full")
            .any("msg_loc", msg_loc)
            .any("corr_loc", corr_loc)
            .log();
        return CreationError.OutOfSpace;
    }

    const idx_entry: MessageIndexEntry = if (corr_loc) |_corr_loc| .{
        .id = msg_loc.id,
        .msg_file_offset = msg_loc.file_offset,
        .msg_frame_size = msg_loc.frame_size,
        .corr_id = _corr_loc.id,
        .corr_segment_no = _corr_loc.segment_no,
        .corr_file_offset = _corr_loc.file_offset,
        .corr_frame_size = _corr_loc.frame_size,
    } else .{
        .id = msg_loc.id,
        .msg_file_offset = msg_loc.file_offset,
        .msg_frame_size = msg_loc.frame_size,
        .corr_id = dymes_msg.constants.NIL_CORRELATION_ID,
        .corr_segment_no = 0x0,
        .corr_file_offset = 0x0,
        .corr_frame_size = 0x0,
    };

    self.entries[self.append_idx] = idx_entry;
    self.append_idx += 1;
}

/// Looks up the index entry for the given `ulid`.
///
/// Performs a binary search of the in-memory index buffer.
pub fn lookup(self: *Self, ulid: Ulid) ?MessageIndexEntry {
    if (!self.mayContain(ulid)) {
        return null;
    }
    var low_idx: usize = 0;
    var high_idx: usize = self.append_idx;
    while (low_idx < high_idx) {
        const mid_idx: usize = (low_idx + high_idx) / 2;
        const mid_ulid = self.entries[mid_idx].id;
        if (mid_ulid.equals(ulid)) {
            return self.entries[mid_idx];
        }
        if (isBeforeUlid(mid_ulid, ulid)) {
            low_idx = mid_idx + 1;
        } else {
            high_idx = mid_idx;
        }
    }
    return null;
}

/// Looks up the _closest_ index entry for the given `ulid`.
///
/// Performs a binary search of the in-memory index buffer.
pub fn lookupClosest(self: *Self, ulid: Ulid) ?MessageIndexEntry {
    if (!self.mayContain(ulid)) {
        return null;
    }
    var low_idx: usize = 0;
    var high_idx: usize = self.append_idx;
    while (low_idx < high_idx) {
        const mid_idx: usize = (low_idx + high_idx) / 2;
        const mid_ulid = self.entries[mid_idx].id;
        if (mid_ulid.equals(ulid)) {
            return self.entries[mid_idx];
        }
        if (isBeforeUlid(mid_ulid, ulid)) {
            low_idx = mid_idx + 1;
        } else {
            high_idx = mid_idx;
        }
    }

    return if (low_idx > 0) self.entries[low_idx - 1] else self.entries[0];
}

/// Queries whether or not the index may contain the given ULID.
///
/// False positives are possible, but not false negatives.
pub fn mayContain(self: *const Self, id: Ulid) bool {
    if (self.append_idx == 0) {
        return false;
    }
    const lower = self.entries[0].id;
    const upper = self.entries[self.append_idx - 1].id;
    return lower.equals(id) or upper.equals(id) or
        isBeforeUlid(lower, id) and isBeforeUlid(id, upper);
}

test "MessageIndex" {
    std.debug.print("test.MessageIndex.smoke\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    var ulid_generator = common.ulid.generator();
    _ = try ulid_generator.next();

    const ancient_ulid = try ulid_generator.next();

    var logger = logging.logger("test.MessageIndex");

    logger.debug()
        .msg("Size of index entry")
        .int("expected", 64)
        .int("actual", @sizeOf(MessageIndexEntry))
        .log();

    // Create in-memory index
    logger.fine().msg("Creating in-memory index").log();
    var in_mem = try init(allocator);
    defer in_mem.deinit();
    logger.debug().msg("In-memory index created").log();

    const num_test_entries: usize = limits.max_message_file_entries;

    // Prepare test 'ULIDs'
    const ulids = try allocator.alloc(Ulid, num_test_entries);
    defer allocator.free(ulids);
    for (0..num_test_entries) |idx| {
        ulids[idx] = try ulid_generator.next();
    }

    // Populate index with dummy entries
    for (0..num_test_entries) |idx| {
        const msg_loc: MessageLocation = .{
            .id = ulids[idx],
            .file_offset = 0,
            .frame_size = 0,
            .segment_no = 0,
        };
        try in_mem.store(msg_loc, null);
    }

    // Containment check
    try testing.expect(!in_mem.mayContain(ancient_ulid));
    try testing.expect(in_mem.mayContain(ulids[0]));
    try testing.expect(in_mem.mayContain(ulids[num_test_entries - 1]));
    try testing.expect(in_mem.mayContain(ulids[num_test_entries / 2]));

    // Lookup
    const off_qtr = num_test_entries / 4;
    const off_third = num_test_entries / 3;

    var timer = try std.time.Timer.start();

    try testing.expect(ulids[0].equals(in_mem.lookup(ulids[0]).?.id));
    try testing.expect(ulids[off_qtr].equals(in_mem.lookup(ulids[off_qtr]).?.id));
    try testing.expect(ulids[off_qtr * 2].equals(in_mem.lookup(ulids[off_qtr * 2]).?.id));
    try testing.expect(ulids[off_qtr * 3].equals(in_mem.lookup(ulids[off_qtr * 3]).?.id));
    try testing.expect(ulids[off_third].equals(in_mem.lookup(ulids[off_third]).?.id));
    try testing.expect(ulids[off_third * 2].equals(in_mem.lookup(ulids[off_third * 2]).?.id));

    const lookup_time = timer.read();

    logger.debug()
        .msg("In-memory message index lookup times")
        .int("avg lookup (ns)", lookup_time / 6)
        .log();

    const ulid_before_qtr: Ulid = .{ .time = ulids[off_qtr].time, .rand = ulids[off_qtr].rand - 1 };
    if (in_mem.lookupClosest(ulid_before_qtr)) |mei| {
        logger.debug()
            .msg("Lookup closest")
            .ulid("closest_ulid", mei.id)
            .ulid("ulid_qtr-2", ulids[off_qtr - 2])
            .ulid("ulid_qtr-1", ulids[off_qtr - 1])
            .ulid("ulid_qtr", ulids[off_qtr])
            .ulid("ulid_qtr+1", ulids[off_qtr + 1])
            .ulid("ulid_before_qtr", ulid_before_qtr)
            .ulid("closest_before_qtr", ulid_before_qtr)
            .any("mei", mei)
            .log();
    }
}
