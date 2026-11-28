//! Dymes Segment Header Index File.
//!
//! One per closed segment, beside its channel index file: for every message, a hash of the value of one
//! header, the store's indexed header. The hashes are in the order of the channel index file's run entries, so
//! a channel read that asks for one value of that header reads a channel's hashes in one pass and fetches only
//! the frames whose hash matches. Without it the read fetches every frame of the channel in its range.
//!
//! The file is derived from the segment's message file: one that is missing, damaged, built for another header
//! or for another index file is rebuilt, in the background. A read that finds none reads the frames.
//!
//! Layout: a header, then one hash per message.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const common = @import("dymes_common");

const dymes_msg = @import("dymes_msg");
const Message = dymes_msg.Message;

const limits = @import("limits.zig");
const errors = @import("errors.zig");
const AllocationError = errors.AllocationError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;

const IndexFile = @import("IndexFile.zig");
const MessageFile = @import("MessageFile.zig");
const ChannelIndexFile = @import("ChannelIndexFile.zig");

pub const file_name_ext: []const u8 = "hdx";
pub const file_name_fmt: []const u8 = "dymes_{d}.hdx";
const tmp_file_name_fmt: []const u8 = "dymes_{d}.hdx.tmp";

const magic: [16]u8 = "DYMES-HDX-000001".*;

/// The hash of a message that does not carry the indexed header
pub const absent: u32 = 0;

pub const Header = extern struct {
    /// Checksum of the rest of the header
    header_checksum: u128,
    magic: [16]u8,
    segment_no: u64,
    /// Messages in the segment: the number of hashes
    num_entries: u64,
    /// Hash of the name of the indexed header
    key_hash: u64,
    /// Size of the index file the channel index file this follows was derived from
    idx_file_size: u64,
    /// CRC of the hashes
    data_checksum: u64,
    reserved: u64 = 0,
};

comptime {
    assert(@sizeOf(Header) == 80);
}

pub fn keyHash(key: []const u8) u64 {
    return std.hash.Wyhash.hash(0x64796d6573686478, key);
}

/// The hash of a value of the indexed header; never `absent`.
pub fn valueHash(value: []const u8) u32 {
    const hash: u32 = @truncate(std.hash.Wyhash.hash(0x64796d6573686478, value));
    return if (hash == absent) 1 else hash;
}

/// The hash a message is indexed under.
pub fn messageHash(message: Message, key: []const u8) u32 {
    var kv_it = message.kvHeaderIterator();
    while (kv_it.next()) |kvh| {
        if (std.mem.eql(u8, key, kvh.key(message.frame.ptr))) return valueHash(kvh.value(message.frame.ptr));
    }
    return absent;
}

fn fileName(buf: []u8, comptime fmt: []const u8, segment_no: u64) []const u8 {
    return std.fmt.bufPrint(buf, fmt, .{segment_no}) catch unreachable;
}

fn idxFileSize(dir: std.fs.Dir, segment_no: u64) !u64 {
    var name_buf: [64]u8 = undefined;
    return (try dir.statFile(fileName(&name_buf, IndexFile.idx_file_name_fmt, segment_no))).size;
}

/// Whether a segment has a header index file for `key` that can be trusted. Reads the whole file.
pub fn valid(gpa: std.mem.Allocator, dir: std.fs.Dir, segment_no: u64, key: []const u8) bool {
    var name_buf: [64]u8 = undefined;
    var file = dir.openFile(fileName(&name_buf, file_name_fmt, segment_no), .{ .mode = .read_only }) catch return false;
    defer file.close();

    var header: Header = undefined;
    const header_bytes = std.mem.asBytes(&header);
    if ((file.preadAll(header_bytes, 0) catch return false) != header_bytes.len) return false;
    if (header.header_checksum != common.checksum.checksum(header_bytes[@sizeOf(u128)..])) return false;
    if (!std.mem.eql(u8, &header.magic, &magic) or header.segment_no != segment_no) return false;
    if (header.key_hash != keyHash(key)) return false;
    if (header.num_entries == 0 or header.num_entries > limits.max_message_file_entries) return false;
    if (header.idx_file_size != (idxFileSize(dir, segment_no) catch return false)) return false;

    const hashes = gpa.alloc(u32, @intCast(header.num_entries)) catch return false;
    defer gpa.free(hashes);
    const bytes = std.mem.sliceAsBytes(hashes);
    const stat = file.stat() catch return false;
    if (stat.size != @sizeOf(Header) + bytes.len) return false;
    if ((file.preadAll(bytes, @sizeOf(Header)) catch return false) != bytes.len) return false;
    return header.data_checksum == std.hash.crc.Crc64Redis.hash(bytes);
}

pub const BuildError = AllocationError || AccessError || StateError || CreationError;

/// Builds a segment's header index file from its message file, replacing any there is. `directory` is the
/// segment's channel directory: a message's hash goes where the channel index file has its run entry.
pub fn build(gpa: std.mem.Allocator, frame_allocator: std.mem.Allocator, dir: std.fs.Dir, segment_no: u64, directory: *const ChannelIndexFile.Directory, key: []const u8) BuildError!void {
    const num_entries: usize = @intCast(directory.num_entries);
    if (num_entries == 0 or num_entries > limits.max_message_file_entries) return StateError.IllegalState;
    const idx_size = idxFileSize(dir, segment_no) catch return AccessError.AccessFailure;

    const hashes = gpa.alloc(u32, num_entries) catch return AllocationError.OutOfMemory;
    defer gpa.free(hashes);
    // How many of each channel's messages have been seen: the next one's place in the channel's run
    const seen = gpa.alloc(u64, directory.entries.len) catch return AllocationError.OutOfMemory;
    defer gpa.free(seen);
    @memset(seen, 0);

    var it = try MessageFile.Iterator.openInDir(frame_allocator, dir, segment_no, @sizeOf(MessageFile.Header));
    defer it.close();
    var total: usize = 0;
    while (try it.next()) |message| {
        const slot = directory.findIndex(message.channel()) orelse return StateError.InconsistentState;
        const dir_entry = directory.entries[slot];
        if (seen[slot] >= dir_entry.run_len) return StateError.InconsistentState;
        hashes[@intCast(dir_entry.run_start + seen[slot])] = messageHash(message, key);
        seen[slot] += 1;
        total += 1;
    }
    if (total != num_entries) return StateError.InconsistentState;

    const bytes = std.mem.sliceAsBytes(hashes);
    var header: Header = .{
        .header_checksum = 0,
        .magic = magic,
        .segment_no = segment_no,
        .num_entries = num_entries,
        .key_hash = keyHash(key),
        .idx_file_size = idx_size,
        .data_checksum = std.hash.crc.Crc64Redis.hash(bytes),
    };
    header.header_checksum = common.checksum.checksum(std.mem.asBytes(&header)[@sizeOf(u128)..]);

    var tmp_name_buf: [64]u8 = undefined;
    const tmp_name = fileName(&tmp_name_buf, tmp_file_name_fmt, segment_no);
    var name_buf: [64]u8 = undefined;
    const name = fileName(&name_buf, file_name_fmt, segment_no);
    {
        var tmp_file = dir.createFile(tmp_name, .{ .truncate = true }) catch |e| return switch (e) {
            error.NoSpaceLeft => CreationError.OutOfSpace,
            else => CreationError.OtherCreationFailure,
        };
        defer tmp_file.close();
        errdefer dir.deleteFile(tmp_name) catch {};
        tmp_file.writeAll(std.mem.asBytes(&header)) catch return CreationError.OtherCreationFailure;
        tmp_file.writeAll(bytes) catch return CreationError.OtherCreationFailure;
        tmp_file.sync() catch return CreationError.OtherCreationFailure;
    }
    dir.rename(tmp_name, name) catch return CreationError.OtherCreationFailure;
}

pub fn remove(dir: std.fs.Dir, segment_no: u64) void {
    var name_buf: [64]u8 = undefined;
    dir.deleteFile(fileName(&name_buf, file_name_fmt, segment_no)) catch {};
}

pub fn openForRead(dir: std.fs.Dir, segment_no: u64) AccessError!std.fs.File {
    var name_buf: [64]u8 = undefined;
    return dir.openFile(fileName(&name_buf, file_name_fmt, segment_no), .{ .mode = .read_only }) catch |e| return switch (e) {
        error.FileNotFound => AccessError.FileNotFound,
        else => AccessError.AccessFailure,
    };
}

/// Reads the hashes of a channel's run from `run_pos`, as many as fit in `out` or the run has left.
pub fn readHashes(file: std.fs.File, dir_entry: ChannelIndexFile.DirEntry, run_pos: u64, out: []u32) AccessError!usize {
    assert(run_pos <= dir_entry.run_len);
    const count: usize = @intCast(@min(out.len, dir_entry.run_len - run_pos));
    const bytes = std.mem.sliceAsBytes(out[0..count]);
    const offset = @sizeOf(Header) + (dir_entry.run_start + run_pos) * @sizeOf(u32);
    if ((file.preadAll(bytes, offset) catch return AccessError.AccessFailure) != bytes.len) return AccessError.AccessFailure;
    return count;
}

test "HeaderIndexFile.hashes" {
    try testing.expect(valueHash("a") != absent);
    try testing.expect(valueHash("a") != valueHash("b"));
    try testing.expect(keyHash("tracking_id") != keyHash("dataset"));
}
