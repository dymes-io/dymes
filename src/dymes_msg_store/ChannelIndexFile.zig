//! Dymes Segment Channel Index File.
//!
//! One per closed segment, beside its message and index files: which channels the segment holds,
//! each channel's id range, and each channel's messages in order with their ids, offsets and sizes.
//! A channel read uses it to skip segments that hold nothing of the channel, to start at the right
//! message of a segment, and to read frames without loading the segment's whole index.
//!
//! The file is derived from the segment's index file and holds nothing the index file doesn't: one
//! that is missing, of another version or damaged is rebuilt.
//!
//! Layout: a header, then a directory of channels sorted by channel id, then the run entries of
//! each channel, in the directory's order.
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

const constants = @import("constants.zig");
const limits = @import("limits.zig");
const errors = @import("errors.zig");
const AllocationError = errors.AllocationError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;

const IndexFile = @import("IndexFile.zig");
const IndexEntry = @import("IndexEntry.zig");

pub const file_name_ext: []const u8 = "chx";
pub const file_name_fmt: []const u8 = "dymes_{d}.chx";
const tmp_file_name_fmt: []const u8 = "dymes_{d}.chx.tmp";

const magic: [16]u8 = "DYMES-CHX-000001".*;

pub const Header = extern struct {
    /// Checksum of the rest of the header
    header_checksum: u128,
    magic: [16]u8,
    /// First and last message ids of the segment
    first_id: Ulid,
    last_id: Ulid,
    segment_no: u64,
    /// Messages in the segment: the number of run entries
    num_entries: u64,
    /// Channels in the segment: the number of directory entries
    num_channels: u64,
    /// Size of the index file this was derived from
    idx_file_size: u64,
    /// CRC of the directory
    dir_checksum: u64,
    reserved: [24]u8 = std.mem.zeroes([24]u8),
};

/// A channel's messages in one segment.
pub const DirEntry = extern struct {
    channel: u128,
    /// First and last ids of the channel's messages in the segment
    first_id: Ulid,
    last_id: Ulid,
    /// Where the channel's run starts, counted in run entries from the first
    run_start: u64,
    run_len: u64,
};

/// One message of a channel's run.
pub const RunEntry = extern struct {
    id: Ulid,
    file_offset: u64,
    frame_size: u32,
    reserved: u32 = 0,
};

comptime {
    assert(@sizeOf(Header) == 128);
    assert(@sizeOf(DirEntry) == 64);
    assert(@sizeOf(RunEntry) == 32);
    assert(common.util.checkNoPadding(DirEntry));
    assert(common.util.checkNoPadding(RunEntry));
}

/// A segment's channel directory, held in memory.
pub const Directory = struct {
    first_id: Ulid,
    last_id: Ulid,
    num_entries: u64,
    /// Sorted by channel id
    entries: []DirEntry,

    pub fn deinit(self: *Directory, gpa: std.mem.Allocator) void {
        gpa.free(self.entries);
    }

    pub fn find(self: *const Directory, channel: u128) ?DirEntry {
        return self.entries[self.findIndex(channel) orelse return null];
    }

    /// Where in the directory a channel's entry is.
    pub fn findIndex(self: *const Directory, channel: u128) ?usize {
        var low: usize = 0;
        var high: usize = self.entries.len;
        while (low < high) {
            const mid = (low + high) / 2;
            const mid_channel = self.entries[mid].channel;
            if (mid_channel == channel) return mid;
            if (mid_channel < channel) low = mid + 1 else high = mid;
        }
        return null;
    }

    /// Offset in the file of a run entry.
    pub fn runOffset(self: *const Directory, dir_entry: DirEntry, run_pos: u64) u64 {
        return @sizeOf(Header) + self.entries.len * @sizeOf(DirEntry) + (dir_entry.run_start + run_pos) * @sizeOf(RunEntry);
    }
};

pub const LoadError = AllocationError || AccessError;

/// Loads a segment's channel directory, or returns null when the file is missing or can't be trusted (the caller
/// rebuilds it).
pub fn load(gpa: std.mem.Allocator, dir: std.fs.Dir, segment_no: u64) LoadError!?Directory {
    var name_buf: [64]u8 = undefined;
    const name = std.fmt.bufPrint(&name_buf, file_name_fmt, .{segment_no}) catch unreachable;
    var file = dir.openFile(name, .{ .mode = .read_only }) catch |e| return switch (e) {
        error.FileNotFound => null,
        else => AccessError.AccessFailure,
    };
    defer file.close();

    var header: Header = undefined;
    const header_bytes = std.mem.asBytes(&header);
    if ((file.preadAll(header_bytes, 0) catch return AccessError.AccessFailure) != header_bytes.len) return null;
    if (header.header_checksum != common.checksum.checksum(header_bytes[@sizeOf(u128)..])) return null;
    if (!std.mem.eql(u8, &header.magic, &magic) or header.segment_no != segment_no) return null;
    if (header.num_channels > header.num_entries or header.num_entries > limits.max_message_file_entries) return null;

    const expected_size = @sizeOf(Header) + header.num_channels * @sizeOf(DirEntry) + header.num_entries * @sizeOf(RunEntry);
    const stat = file.stat() catch return AccessError.AccessFailure;
    if (stat.size != expected_size) return null;
    if (header.idx_file_size != (idxFileSize(dir, segment_no) catch return null)) return null;

    const entries = gpa.alloc(DirEntry, header.num_channels) catch return AllocationError.OutOfMemory;
    errdefer gpa.free(entries);
    const dir_bytes = std.mem.sliceAsBytes(entries);
    if ((file.preadAll(dir_bytes, @sizeOf(Header)) catch return AccessError.AccessFailure) != dir_bytes.len or
        header.dir_checksum != std.hash.crc.Crc64Redis.hash(dir_bytes))
    {
        gpa.free(entries);
        return null;
    }

    return .{
        .first_id = header.first_id,
        .last_id = header.last_id,
        .num_entries = header.num_entries,
        .entries = entries,
    };
}

fn idxFileSize(dir: std.fs.Dir, segment_no: u64) !u64 {
    var name_buf: [64]u8 = undefined;
    const name = std.fmt.bufPrint(&name_buf, IndexFile.idx_file_name_fmt, .{segment_no}) catch unreachable;
    return (try dir.statFile(name)).size;
}

pub const BuildError = AllocationError || AccessError || StateError || CreationError;

/// Builds a segment's channel index file from its index file and returns its directory. The file is written under
/// another name, synchronized and renamed, so a file under the final name is always whole.
pub fn build(gpa: std.mem.Allocator, dir: std.fs.Dir, segment_no: u64) BuildError!Directory {
    var name_buf: [64]u8 = undefined;
    const idx_name = std.fmt.bufPrint(&name_buf, IndexFile.idx_file_name_fmt, .{segment_no}) catch unreachable;
    var idx_file = dir.openFile(idx_name, .{ .mode = .read_only }) catch |e| return switch (e) {
        error.FileNotFound => AccessError.FileNotFound,
        else => AccessError.AccessFailure,
    };
    defer idx_file.close();
    const idx_size = (idx_file.stat() catch return AccessError.AccessFailure).size;
    if (idx_size < @sizeOf(IndexFile.Header) or (idx_size - @sizeOf(IndexFile.Header)) % IndexEntry.idx_entry_size != 0) {
        return StateError.DataTruncated;
    }
    const num_entries: usize = @intCast((idx_size - @sizeOf(IndexFile.Header)) / IndexEntry.idx_entry_size);
    if (num_entries == 0 or num_entries > limits.max_message_file_entries) return StateError.IllegalState;

    const idx_bytes = gpa.alignedAlloc(u8, .@"16", num_entries * IndexEntry.idx_entry_size) catch return AllocationError.OutOfMemory;
    defer gpa.free(idx_bytes);
    if ((idx_file.preadAll(idx_bytes, @sizeOf(IndexFile.Header)) catch return AccessError.AccessFailure) != idx_bytes.len) {
        return StateError.DataTruncated;
    }

    // Count each channel's messages, then lay the runs out in channel order
    var channel_slots = std.AutoArrayHashMap(u128, DirEntry).init(gpa);
    defer channel_slots.deinit();
    var first_id: Ulid = undefined;
    var last_id: Ulid = undefined;
    for (0..num_entries) |entry_no| {
        const idx_entry = IndexEntry.overlay(@alignCast(idx_bytes[entry_no * IndexEntry.idx_entry_size ..][0..IndexEntry.idx_entry_size]));
        if (!idx_entry.verifyChecksum()) return StateError.ChecksumFailure;
        const id = idx_entry.frame_header.id;
        if (entry_no == 0) first_id = id;
        last_id = id;
        const slot = channel_slots.getOrPut(idx_entry.frame_header.channel) catch return AllocationError.OutOfMemory;
        if (!slot.found_existing) {
            slot.value_ptr.* = .{ .channel = idx_entry.frame_header.channel, .first_id = id, .last_id = id, .run_start = 0, .run_len = 0 };
        }
        slot.value_ptr.last_id = id;
        slot.value_ptr.run_len += 1;
    }

    const dir_entries = gpa.alloc(DirEntry, channel_slots.count()) catch return AllocationError.OutOfMemory;
    errdefer gpa.free(dir_entries);
    @memcpy(dir_entries, channel_slots.values());
    std.mem.sort(DirEntry, dir_entries, {}, struct {
        fn lessThan(_: void, a: DirEntry, b: DirEntry) bool {
            return a.channel < b.channel;
        }
    }.lessThan);
    var run_start: u64 = 0;
    for (dir_entries) |*dir_entry| {
        dir_entry.run_start = run_start;
        run_start += dir_entry.run_len;
        // The slot now counts what has been placed
        channel_slots.getPtr(dir_entry.channel).?.* = .{ .channel = dir_entry.channel, .first_id = dir_entry.first_id, .last_id = dir_entry.last_id, .run_start = dir_entry.run_start, .run_len = 0 };
    }

    const run_entries = gpa.alloc(RunEntry, num_entries) catch return AllocationError.OutOfMemory;
    defer gpa.free(run_entries);
    for (0..num_entries) |entry_no| {
        const idx_entry = IndexEntry.overlay(@alignCast(idx_bytes[entry_no * IndexEntry.idx_entry_size ..][0..IndexEntry.idx_entry_size]));
        const slot = channel_slots.getPtr(idx_entry.frame_header.channel).?;
        run_entries[slot.run_start + slot.run_len] = .{
            .id = idx_entry.frame_header.id,
            .file_offset = idx_entry.msg_file_offset,
            .frame_size = idx_entry.frame_header.frame_size,
        };
        slot.run_len += 1;
    }

    var header: Header = .{
        .header_checksum = 0,
        .magic = magic,
        .first_id = first_id,
        .last_id = last_id,
        .segment_no = segment_no,
        .num_entries = num_entries,
        .num_channels = dir_entries.len,
        .idx_file_size = idx_size,
        .dir_checksum = std.hash.crc.Crc64Redis.hash(std.mem.sliceAsBytes(dir_entries)),
    };
    header.header_checksum = common.checksum.checksum(std.mem.asBytes(&header)[@sizeOf(u128)..]);

    var tmp_name_buf: [64]u8 = undefined;
    const tmp_name = std.fmt.bufPrint(&tmp_name_buf, tmp_file_name_fmt, .{segment_no}) catch unreachable;
    var chx_name_buf: [64]u8 = undefined;
    const chx_name = std.fmt.bufPrint(&chx_name_buf, file_name_fmt, .{segment_no}) catch unreachable;
    {
        var tmp_file = dir.createFile(tmp_name, .{ .truncate = true }) catch |e| return switch (e) {
            error.NoSpaceLeft => CreationError.OutOfSpace,
            else => CreationError.OtherCreationFailure,
        };
        defer tmp_file.close();
        errdefer dir.deleteFile(tmp_name) catch {};
        tmp_file.writeAll(std.mem.asBytes(&header)) catch return CreationError.OtherCreationFailure;
        tmp_file.writeAll(std.mem.sliceAsBytes(dir_entries)) catch return CreationError.OtherCreationFailure;
        tmp_file.writeAll(std.mem.sliceAsBytes(run_entries)) catch return CreationError.OtherCreationFailure;
        tmp_file.sync() catch return CreationError.OtherCreationFailure;
    }
    dir.rename(tmp_name, chx_name) catch return CreationError.OtherCreationFailure;

    return .{
        .first_id = first_id,
        .last_id = last_id,
        .num_entries = num_entries,
        .entries = dir_entries,
    };
}

/// Removes a segment's channel index file (one that turned out not to match its segment).
pub fn remove(dir: std.fs.Dir, segment_no: u64) void {
    var name_buf: [64]u8 = undefined;
    const name = std.fmt.bufPrint(&name_buf, file_name_fmt, .{segment_no}) catch unreachable;
    dir.deleteFile(name) catch {};
}

pub fn openForRead(dir: std.fs.Dir, segment_no: u64) AccessError!std.fs.File {
    var name_buf: [64]u8 = undefined;
    const name = std.fmt.bufPrint(&name_buf, file_name_fmt, .{segment_no}) catch unreachable;
    return dir.openFile(name, .{ .mode = .read_only }) catch |e| return switch (e) {
        error.FileNotFound => AccessError.FileNotFound,
        else => AccessError.AccessFailure,
    };
}

/// Reads run entries of a channel, from `run_pos`, into `out`; returns how many were read.
pub fn readRun(file: std.fs.File, directory: *const Directory, dir_entry: DirEntry, run_pos: u64, out: []RunEntry) AccessError!usize {
    assert(run_pos <= dir_entry.run_len);
    const count: usize = @intCast(@min(out.len, dir_entry.run_len - run_pos));
    const bytes = std.mem.sliceAsBytes(out[0..count]);
    if ((file.preadAll(bytes, directory.runOffset(dir_entry, run_pos)) catch return AccessError.AccessFailure) != bytes.len) {
        return AccessError.AccessFailure;
    }
    return count;
}

/// Position in a channel's run of its first message whose id is `id` or later (the run's length if none is).
pub fn lowerBound(file: std.fs.File, directory: *const Directory, dir_entry: DirEntry, id: Ulid) AccessError!u64 {
    var low: u64 = 0;
    var high: u64 = dir_entry.run_len;
    while (low < high) {
        const mid = (low + high) / 2;
        var one: [1]RunEntry = undefined;
        _ = try readRun(file, directory, dir_entry, mid, &one);
        if (isBeforeUlid(one[0].id, id)) low = mid + 1 else high = mid;
    }
    return low;
}
