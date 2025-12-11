//! Dymes Message Store In-Memory Index of Segments.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
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

/// Segment index entry
const SegmentsUlidIndexEntry = extern struct {
    /// Id of first message in the segment
    first_id: Ulid,

    /// Id of last message in the segment
    last_id: Ulid,
};

comptime {
    assert(@sizeOf(SegmentsUlidIndexEntry) == 32);
    assert(common.util.checkNoPadding(SegmentsUlidIndexEntry));
}

const Self = @This();
const SegmentsUlidIndexEntryList = std.array_list.AlignedManaged(SegmentsUlidIndexEntry, null);

/// Index list
entries: *SegmentsUlidIndexEntryList,

/// Append index
append_idx: usize,

/// Allocates and initializes the dataset segments in-memory index.
///
/// Caller must call `deinit()` to release resources.
pub fn init(gpa: std.mem.Allocator, active_segments: u64) AllocationError!Self {
    const entries_ptr = gpa.create(SegmentsUlidIndexEntryList) catch return AllocationError.OutOfMemory;
    errdefer gpa.destroy(entries_ptr);
    entries_ptr.* = SegmentsUlidIndexEntryList.initCapacity(gpa, active_segments) catch return AllocationError.OutOfMemory;
    errdefer entries_ptr.*.deinit();
    return .{
        .entries = entries_ptr,
        .append_idx = 0,
    };
}

/// De-initializes the dataset segments in-memory index.
pub fn deinit(self: *Self, gpa: std.mem.Allocator) void {
    defer gpa.destroy(self.entries);
    defer self.entries.deinit();
}

/// Looks up the segment number probably containing the given `id`.
///
/// Performs a binary search of the segment index buffer.
pub fn lookup(self: *const Self, id: Ulid) ?u64 {
    if (!self.mayContain(id)) {
        return null;
    }
    var low_idx: usize = 0;
    var high_idx: usize = self.entries.items.len;
    while (low_idx < high_idx) {
        const mid_idx: usize = (low_idx + high_idx) / 2;
        const mid_entry = self.entries.items[mid_idx];
        const lower_id = mid_entry.first_id;
        const upper_id = mid_entry.last_id;
        if (lower_id.equals(id) or upper_id.equals(id) or
            isBeforeUlid(lower_id, id) and isBeforeUlid(id, upper_id))
        {
            return mid_idx;
        }
        if (isBeforeUlid(lower_id, id)) {
            low_idx = mid_idx + 1;
        } else {
            high_idx = mid_idx;
        }
    }
    return null;
}

/// Updates an index entry
///
/// Appends an entry for a new segment if required, but no gaps are allowed.
pub fn update(self: *Self, segment_no: u64, first_id: Ulid, last_id: Ulid) CreationError!void {
    const updated_entry: SegmentsUlidIndexEntry = .{ .first_id = first_id, .last_id = last_id };
    if (segment_no >= self.entries.items.len) {
        assert(segment_no == self.entries.items.len);
        self.entries.append(updated_entry) catch return CreationError.OutOfMemory;
    } else {
        self.entries.items[segment_no] = updated_entry;
    }
}

/// Updates an existing index entry
///
/// Returns `false` if segment boundary would be crossed
pub fn updateExisting(self: *Self, segment_no: u64, last_id: Ulid) bool {
    if (segment_no >= self.entries.items.len) {
        return false; // Segment boundary crossed
    }
    self.entries.items[segment_no].last_id = last_id;
    return true;
}

/// Queries whether or not the segment may contain the given ULID.
///
/// False positives are possible, but not false negatives.
pub fn mayContain(self: *const Self, id: Ulid) bool {
    assert(self.entries.items.len != 0);
    const lower = self.entries.items[0].first_id;
    const upper = self.entries.items[self.entries.items.len - 1].last_id;
    return lower.equals(id) or upper.equals(id) or
        isBeforeUlid(lower, id) and isBeforeUlid(id, upper);
}

/// Queries whether or not a message with a given ID may be imported.
///
/// Messages with an ID after the current last entry are allowed.
pub fn mayImport(self: *const Self, id: Ulid) bool {
    if (self.append_idx == 0) return true;
    const upper = self.entries.items[self.entries.items.len - 1].last_id;
    return !isBeforeUlid(id, upper);
}

/// Formats the value and prints to the given writer.
pub fn format(self: *const Self, writer: *std.Io.Writer) !void {
    try writer.print("{{", .{});
    for (self.entries.items, 0..) |entry, segment_no| {
        try writer.print("{{segment_no={d}, first_id={f}, last_id={f}}},", .{
            segment_no,
            entry.first_id,
            entry.last_id,
        });
    }
    try writer.print("}}", .{});
}

test "SegmentsUlidIndex" {
    std.debug.print("test.SegmentsUlidIndex.smoke\n", .{});
    const gpa = testing.allocator;
    const out_buffer = try gpa.alloc(u8, 2048);
    defer gpa.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(gpa, &stderr_writer);
    defer logging.deinit();

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    var ulid_generator = common.ulid.generator();
    _ = try ulid_generator.next();

    var logger = logging.logger("test.SegmentsUlidIndex.smoke");

    logger.debug()
        .msg("Size of index entry")
        .int("expected", 32)
        .int("actual", @sizeOf(SegmentsUlidIndexEntry))
        .log();

    // Create in-memory index
    logger.fine().msg("Creating segment index").log();
    var segment_index = try init(gpa, 10);
    defer segment_index.deinit(gpa);
    logger.debug().msg("Segment index created").log();

    // Populate the index

    const ulid_before = try ulid_generator.next();
    const ulid_start_seg_0 = try ulid_generator.next();
    const ulid_inside_seg_0 = try ulid_generator.next();
    const ulid_end_seg_0 = try ulid_generator.next();
    const ulid_between = try ulid_generator.next();
    const ulid_start_seg_1 = try ulid_generator.next();
    const ulid_inside_seg_1 = try ulid_generator.next();
    const ulid_end_seg_1 = try ulid_generator.next();
    const ulid_after = try ulid_generator.next();

    try segment_index.update(0, ulid_start_seg_0, ulid_end_seg_0);
    try segment_index.update(1, ulid_start_seg_1, ulid_end_seg_1);

    // Containment check
    try testing.expect(!segment_index.mayContain(ulid_before));
    try testing.expect(!segment_index.mayContain(ulid_after));
    try testing.expect(segment_index.mayContain(ulid_between)); // false positive

    try testing.expect(segment_index.mayContain(ulid_inside_seg_0));
    try testing.expect(segment_index.mayContain(ulid_start_seg_0));
    try testing.expect(segment_index.mayContain(ulid_end_seg_0));

    try testing.expect(segment_index.mayContain(ulid_inside_seg_1));
    try testing.expect(segment_index.mayContain(ulid_start_seg_1));
    try testing.expect(segment_index.mayContain(ulid_end_seg_1));

    // Lookup check
    try testing.expectEqual(999, segment_index.lookup(ulid_before) orelse 999);
    try testing.expectEqual(999, segment_index.lookup(ulid_after) orelse 999);
    try testing.expectEqual(999, segment_index.lookup(ulid_between) orelse 999);
    try testing.expectEqual(0, segment_index.lookup(ulid_inside_seg_0));
    try testing.expectEqual(1, segment_index.lookup(ulid_inside_seg_1));
}
