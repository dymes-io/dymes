//! Dymes Index File.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const assert = std.debug.assert;

const common = @import("dymes_common");

const errors = @import("errors.zig");

const AllocationError = errors.AllocationError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;
const SyncError = errors.SyncError;

const logging = common.logging;
const Ulid = common.ulid.Ulid;
const isBeforeUlid = common.util.isBeforeUlid;

const constants = @import("constants.zig");
const limits = @import("limits.zig");
const dymes_msg = @import("dymes_msg");
const FrameAllocator = dymes_msg.FrameAllocator;
const FrameHeader = dymes_msg.Message.FrameHeader;
const Message = dymes_msg.Message;
pub const IndexEntry = @import("IndexEntry.zig");

pub const idx_file_name_ext = constants.idx_file_name_ext;
pub const idx_file_name_fmt = constants.idx_file_name_fmt;

pub const Header = @import("headers.zig").IndexFileHeader;

/// Index file status
pub const Status = struct {
    /// Open state
    opened: bool,

    /// Readiness
    ready: bool,
};

const Self = @This();

/// Logger
logger: *logging.Logger,

/// Index file
file: std.fs.File,

/// Allocator
allocator: std.mem.Allocator,

/// Message frame allocator
frame_allocator: std.mem.Allocator,

/// Index file header
header: Header,

/// Absolute pathname of index file
pathname: []const u8,

/// Status
status: Status,

/// Segment in dataset
segment_no: u64,

/// First ULID in the file
first_ulid: Ulid,

/// Last ULID in the file
last_ulid: Ulid,

/// Last LSN in the file
last_lsn: u64,

/// Offset of last message in message file
last_msg_offset: u64,

/// Index entry write buffer
entry_write_buffer: []u8,

/// Index entry read buffer
entry_read_buffer: []u8,

/// Entry number to append next
append_entry_no: usize,

/// Dirty seek flag
dirty_seek: bool,

/// File read buffer
file_read_buffer: []u8,

/// Message store options
pub const StoreOptions = struct {
    /// Segment in message store
    segment_no: u64,

    /// Time of file creation (as per mesage store)
    timestamp: i128,
};

/// File options
pub const FileOptions = struct {
    /// Relative pathname
    rel_path: []const u8,

    /// Should file be created?
    create: bool = false,
};

pub const OpenError = AllocationError || CreationError || AccessError || StateError;

/// Opens a index file.
///
/// Caller must call `close()` to release resources.
pub fn open(allocator: Allocator, frame_allocator: Allocator, dir: std.fs.Dir, file_options: FileOptions, store_options: StoreOptions) OpenError!Self {
    var logger = logging.logger("msg_store.IndexFile");

    logger.fine()
        .msg("Opening index file")
        .int("segment", store_options.segment_no)
        .log();

    const entry_write_buffer = frame_allocator.alloc(u8, IndexEntry.idx_entry_size) catch return OpenError.OutOfMemory;
    errdefer frame_allocator.free(entry_write_buffer);

    const entry_read_buffer = frame_allocator.alloc(u8, IndexEntry.idx_entry_size) catch return OpenError.OutOfMemory;
    errdefer frame_allocator.free(entry_read_buffer);

    const file_read_buffer = frame_allocator.alloc(u8, 4 * 1024) catch return OpenError.OutOfMemory;
    errdefer frame_allocator.free(file_read_buffer);

    const file_create_flags: std.fs.File.CreateFlags = .{
        .lock = .shared,
        .exclusive = false,
        .read = true,
        .truncate = file_options.create,
    };
    var file = dir.createFile(file_options.rel_path, file_create_flags) catch |e| {
        logger.err()
            .msg("Failed to open index file")
            .err(e)
            .int("segment", store_options.segment_no)
            .str("rel_path", file_options.rel_path)
            .log();
        return switch (e) {
            error.SharingViolation, error.AccessDenied => OpenError.AccessFailure,
            error.FileNotFound => OpenError.FileNotFound,
            error.PathAlreadyExists => OpenError.FileAlreadyExists,
            else => OpenError.OtherCreationFailure,
        };
    };
    errdefer file.close();

    const pathname = dir.realpathAlloc(allocator, file_options.rel_path) catch |e| {
        logger.err()
            .msg("Failed to determine index file path")
            .err(e)
            .int("segment", store_options.segment_no)
            .str("rel_path", file_options.rel_path)
            .log();
        return switch (e) {
            error.AccessDenied => OpenError.AccessFailure,
            error.NameTooLong, error.BadPathName => OpenError.OtherCreationFailure,
            error.FileNotFound => OpenError.FileNotFound,
            error.SharingViolation, error.NoSpaceLeft => OpenError.OutOfSpace,
            error.OutOfMemory => OpenError.OutOfMemory,
            else => OpenError.OtherAccessFailure,
        };
    };
    errdefer allocator.free(pathname);

    var new_self: Self = .{
        .allocator = allocator,
        .frame_allocator = frame_allocator,
        .logger = logger,
        .file = file,
        .pathname = pathname,
        .header = undefined,
        .status = .{ .opened = true, .ready = true },
        .segment_no = store_options.segment_no,
        .entry_write_buffer = entry_write_buffer,
        .entry_read_buffer = entry_read_buffer,
        .first_ulid = .{ .time = 0, .rand = 0 },
        .last_ulid = .{ .time = 0, .rand = 0 },
        .last_lsn = 0,
        .last_msg_offset = 0,
        .append_entry_no = 0,
        .dirty_seek = true,
        .file_read_buffer = file_read_buffer,
    };

    if (file_options.create) {
        // Write initial header upon creation
        const hdr_buf = std.mem.asBytes(&new_self.header);
        _ = Header.initOverlay(hdr_buf, store_options.segment_no, store_options.timestamp);
        file.writeAll(hdr_buf) catch |e| {
            logger.err()
                .msg("Failed to initialize index file")
                .err(e)
                .int("segment", store_options.segment_no)
                .str("pathname", pathname)
                .log();
            return switch (e) {
                error.NoSpaceLeft => OpenError.OutOfSpace,
                error.AccessDenied => OpenError.AccessFailure,
                else => OpenError.OtherAccessFailure,
            };
        };
        logger.fine()
            .msg("Initialized index file")
            .int("segment", store_options.segment_no)
            .str("pathname", pathname)
            .log();
    } else {
        // Read initial header upon opening
        const hdr_buf = std.mem.asBytes(&new_self.header);
        var rdr = file.reader(file_read_buffer);
        var rdr_itf = &rdr.interface;
        _ = rdr_itf.readSliceAll(hdr_buf) catch |e| {
            logger.err()
                .msg("Failed to read index file header")
                .err(e)
                .int("segment", store_options.segment_no)
                .str("pathname", pathname)
                .log();
            return switch (e) {
                error.ReadFailed => OpenError.AccessFailure,
                error.EndOfStream => StateError.DataTruncated,
            };
        };
        try new_self.header.verifyChecksum();
    }

    logger.fine()
        .msg("Verifying index file")
        .int("segment", store_options.segment_no)
        .str("pathname", pathname)
        .log();

    const fnl = try verify(&new_self);
    new_self.first_ulid = fnl.first_ulid;
    new_self.last_ulid = fnl.last_ulid;
    new_self.last_lsn = fnl.last_lsn;
    new_self.last_msg_offset = fnl.last_msg_offset;
    new_self.append_entry_no = fnl.num_entries;

    logger.fine()
        .msg("Index file opened")
        .int("segment", store_options.segment_no)
        .str("pathname", pathname)
        .ulid("first_ulid", fnl.first_ulid)
        .ulid("last_ulid", fnl.last_ulid)
        .int("last_lsn", fnl.last_lsn)
        .int("last_msg_offset", fnl.last_msg_offset)
        .int("append_entry_no", fnl.num_entries)
        .log();

    return new_self;
}

/// Closes (de-initializes) an index file.
pub fn close(self: *Self) void {
    if (!self.status.opened) {
        self.logger.fine()
            .msg("Index file already closed")
            .int("segment", self.segment_no)
            .str("pathname", self.pathname)
            .ulid("first_ulid", self.first_ulid)
            .ulid("last_ulid", self.last_ulid)
            .int("last_lsn", self.last_lsn)
            .int("last_msg_offset", self.last_msg_offset)
            .int("append_entry_no", self.append_entry_no)
            .log();
        return;
    }
    defer self.status.opened = false;
    defer self.allocator.free(self.pathname);
    defer self.frame_allocator.free(self.entry_write_buffer);
    defer self.frame_allocator.free(self.entry_read_buffer);
    defer self.frame_allocator.free(self.file_read_buffer);
    self.logger.fine()
        .msg("Closing index file")
        .int("segment", self.segment_no)
        .str("pathname", self.pathname)
        .ulid("first_ulid", self.first_ulid)
        .ulid("last_ulid", self.last_ulid)
        .int("last_lsn", self.last_lsn)
        .int("last_msg_offset", self.last_msg_offset)
        .int("append_entry_no", self.append_entry_no)
        .log();
    {
        defer self.file.close();
        self.sync() catch {};
    }
    self.logger.fine()
        .msg("Index file closed")
        .int("segment", self.segment_no)
        .str("pathname", self.pathname)
        .ulid("first_ulid", self.first_ulid)
        .ulid("last_ulid", self.last_ulid)
        .int("last_lsn", self.last_lsn)
        .int("last_msg_offset", self.last_msg_offset)
        .int("append_entry_no", self.append_entry_no)
        .log();
}

/// Synchronizes pending changes with the filesystem.
pub fn sync(self: *Self) SyncError!void {
    self.file.sync() catch |e| {
        self.logger.err()
            .msg("Failed to sync index file")
            .err(e)
            .int("segment", self.segment_no)
            .str("pathname", self.pathname)
            .ulid("first_ulid", self.first_ulid)
            .ulid("last_ulid", self.last_ulid)
            .int("last_lsn", self.last_lsn)
            .int("last_msg_offset", self.last_msg_offset)
            .int("append_entry_no", self.append_entry_no)
            .log();
        self.status.ready = false;
        return switch (e) {
            std.posix.SyncError.AccessDenied, std.posix.SyncError.InputOutput => SyncError.AccessFailure,
            std.posix.SyncError.DiskQuota, std.posix.SyncError.NoSpaceLeft => SyncError.OutOfSpace,
            std.posix.SyncError.Unexpected => SyncError.OtherAccessFailure,
        };
    };
}

/// Looks up the index entry for the given `ulid`.
///
/// Performs a binary search of the index file.
pub fn lookup(self: *Self, ulid: Ulid) FetchError!?IndexEntry {
    if (!self.mayContain(ulid)) {
        return null;
    }

    var rdr = self.file.reader(self.file_read_buffer);

    var low_idx: usize = 0;
    var high_idx: usize = self.append_entry_no;
    while (low_idx < high_idx) {
        const mid_idx: usize = (low_idx + high_idx) / 2;
        const mid_entry = try self.fetch(&rdr, mid_idx);
        const mid_ulid = mid_entry.frame_header.id;
        if (mid_ulid.equals(ulid)) {
            return mid_entry;
        }
        if (isBeforeUlid(mid_ulid, ulid)) {
            low_idx = mid_idx + 1;
        } else {
            high_idx = mid_idx;
        }
    }
    return null;
}

/// Queries whether or not the index file may contain the given ULID.
///
/// False positives are possible, but not false negatives.
pub fn mayContain(self: *const Self, id: Ulid) bool {
    return self.first_ulid.equals(id) or self.last_ulid.equals(id) or
        isBeforeUlid(self.first_ulid, id) and isBeforeUlid(id, self.last_ulid);
}

const FirstAndLast = struct {
    /// First ULID in the file
    first_ulid: Ulid,

    /// Last ULID in the file
    last_ulid: Ulid,

    /// Last LSN in the file
    last_lsn: u64,

    /// Offset of last message in message file
    last_msg_offset: u64,

    /// Number of entries verified
    num_entries: usize,
};

/// Walks the index and fails on out-of-order entries.
///
/// Returns the first and last message ids
fn verify(self: *Self) IteratorError!FirstAndLast {
    var it = try Iterator.init(self);
    defer it.deinit();

    var num_entries: usize = 0;
    var first_ulid: Ulid = .{ .time = 0, .rand = 0 };
    var last_ulid: Ulid = .{ .time = 0, .rand = 0 };
    var last_lsn: u64 = 0;
    var last_msg_offset: u64 = 0;

    if (try it.next()) |first| {
        first_ulid = first.frame_header.id;
        last_ulid = first.frame_header.id;
        last_lsn = first.frame_header.log_sequence;
        last_msg_offset = first.msg_file_offset;
        num_entries += 1;
    }

    var prev_ulid = last_ulid;
    while (try it.next()) |entry| {
        last_ulid = entry.frame_header.id;
        last_lsn = entry.frame_header.log_sequence;
        last_msg_offset = entry.msg_file_offset;
        if (!isBeforeUlid(prev_ulid, last_ulid)) {
            self.logger.err()
                .msg("Index file has out-of-order entries")
                .ulid("prev_ulid", prev_ulid)
                .ulid("last_ulid", last_ulid)
                .ulid("first_ulid", first_ulid)
                .int("last_lsn", last_lsn)
                .int("segment_no", self.segment_no)
                .str("pathname", self.pathname)
                .log();
            return StateError.IllegalState;
        }
        prev_ulid = last_ulid;
        num_entries += 1;
    }

    return .{ .first_ulid = first_ulid, .last_ulid = last_ulid, .last_lsn = last_lsn, .last_msg_offset = last_msg_offset, .num_entries = num_entries };
}

/// Queries the index file readiness
pub inline fn ready(self: *const Self) bool {
    return self.status.ready;
}

/// Formats the index file context and prints to the given writer.
pub fn format(self: *const Self, writer: *std.Io.Writer) !void {
    try writer.print("{{pathname=\\\"{s}\\\",header={any}}}", .{ self.pathname, self.header });
}

pub const StoreError = AccessError || CreationError || StateError;

/// Stores index entry for the given message.
pub fn store(self: *Self, message: *const Message, message_offset: u64) StoreError!void {
    if (!self.ready()) {
        return StoreError.NotReady;
    }
    const entry = try IndexEntry.initOverlay(self.entry_write_buffer, message.frame, message_offset);
    try self.storeIndexEntry(entry);
}

/// Checks if we have capacity to store the given message
pub fn haveCapacity(self: *const Self, _: *const Message) bool {
    if (!self.ready() or self.append_entry_no + 1 >= limits.max_message_file_entries) {
        return false;
    }
    return true;
}

/// Stores index entry for the given message.
fn storeIndexEntry(self: *Self, entry: IndexEntry) StoreError!void {
    if (!self.ready()) {
        return StoreError.NotReady;
    } else if (self.append_entry_no + 1 >= limits.max_message_file_entries) {
        self.logger
            .warn()
            .msg("Failed to store index entry - limit reached")
            .ulid("entry.id", entry.frame_header.id)
            .int("limit", limits.max_message_file_entries)
            .int("segment", self.segment_no)
            .str("pathname", self.pathname)
            .log();
        return StoreError.OutOfSpace;
    }
    const message_id = entry.frame_header.id;
    const message_channel = entry.frame_header.channel;

    if (self.dirty_seek) {
        // Seek to end
        self.file.seekFromEnd(0) catch |e| {
            self.logger.err()
                .msg("Failed to seek to index entry storage position")
                .err(e)
                .ulid("message_id", message_id)
                .int("message_channel", message_channel)
                .int("segment", self.segment_no)
                .str("pathname", self.pathname)
                .log();
            self.status.ready = false;
            return StoreError.AccessFailure;
        };
        self.dirty_seek = false;
    }

    // Write index entry
    const WriteError = std.fs.File.WriteError;
    self.file.writeAll(entry.idx_entry) catch |e| {
        self.logger.err()
            .msg("Failed to store index entry")
            .err(e)
            .ulid("message_id", message_id)
            .int("message_channel", message_channel)
            .int("segment", self.segment_no)
            .str("pathname", self.pathname)
            .log();
        self.status.ready = false;
        return switch (e) {
            WriteError.DiskQuota, WriteError.FileTooBig, WriteError.NoSpaceLeft => StoreError.OutOfSpace,
            WriteError.SystemResources => StoreError.OutOfMemory,
            else => StoreError.AccessFailure,
        };
    };

    // Keep track of progress
    if (self.first_ulid.time == 0) {
        self.first_ulid = entry.frame_header.id;
    }
    self.last_ulid = entry.frame_header.id;
    self.last_lsn = entry.frame_header.log_sequence;
    self.last_msg_offset = entry.msg_file_offset;
    self.append_entry_no += 1;
}

const FetchError = AccessError || StateError;
fn fetch(self: *Self, file_reader: *std.fs.File.Reader, entry_no: usize) FetchError!IndexEntry {
    const read_offset = @sizeOf(Header) + entry_no * IndexEntry.idx_entry_size;
    file_reader.seekTo(read_offset) catch |e| {
        self.logger.err()
            .msg("Failed to seek to index entry position")
            .err(e)
            .int("entry_no", entry_no)
            .int("read_offset", read_offset)
            .int("segment", self.segment_no)
            .str("pathname", self.pathname)
            .log();
        self.status.ready = false;
        return AccessError.AccessFailure;
    };
    self.dirty_seek = true;

    // Read index entry
    var rdr_itf = &file_reader.interface;
    rdr_itf.readSliceAll(self.entry_read_buffer) catch |e| {
        self.logger.err()
            .msg("Failed to read index entry")
            .err(e)
            .int("entry_no", entry_no)
            .int("read_offset", read_offset)
            .int("segment", self.segment_no)
            .str("pathname", self.pathname)
            .log();
        self.status.ready = false;
        return AccessError.AccessFailure;
    };
    const idx_entry = IndexEntry.overlay(self.entry_read_buffer);
    if (!idx_entry.verifyChecksum()) {
        self.logger.err()
            .msg("Index entry failed checksum validation")
            .ulid("ulid", idx_entry.frame_header.id)
            .int("entry_no", entry_no)
            .int("read_offset", read_offset)
            .int("segment", self.segment_no)
            .str("pathname", self.pathname)
            .obj("idx_entry", idx_entry)
            .log();
        return StateError.ChecksumFailure;
    }
    return idx_entry;
}

test "IndexFile.smoke" {
    std.debug.print("test.IndexFile.smoke\n", .{});
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

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    var logger = logging.logger("test.IndexFile.smoke");

    // Create index files
    {
        const store_ts = std.time.nanoTimestamp();

        // Create store index files
        logger.fine().msg("Creating index files for store").log();
        var idx_file_primary = try open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = true, .rel_path = "primary_0.idx" },
            .{ .segment_no = 0, .timestamp = store_ts },
        );
        defer idx_file_primary.close();
        logger.fine().msg("Created primary index file for store").log();
        var idx_file_secondary = try open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = true, .rel_path = "secondary_0.idx" },
            .{ .segment_no = 0, .timestamp = store_ts },
        );
        defer idx_file_secondary.close();
        logger.fine().msg("Created secondary index file").log();
        logger.debug().msg("Created index files").log();
    }

    // Open index files
    {
        // Open store index files
        logger.fine().msg("Opening index files").log();
        var idx_file_primary = try open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = false, .rel_path = "primary_0.idx" },
            .{ .segment_no = 0, .timestamp = 0 },
        );
        defer idx_file_primary.close();
        logger.fine().msg("Opened primary index file").log();

        var idx_file_secondary = try open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = false, .rel_path = "secondary_0.idx" },
            .{ .segment_no = 0, .timestamp = 0 },
        );
        defer idx_file_secondary.close();
        logger.fine().msg("Opened secondary index file").log();

        logger.debug().msg("Opened index files").log();
        try testing.expectEqualStrings(std.mem.asBytes(&idx_file_primary.header), std.mem.asBytes(&idx_file_secondary.header));
    }

    // Corrupt index files
    {
        logger.fine().msg("Corrupting index files").log();
        var idx_file_primary = try tmp_dir.dir.createFile("primary_0.idx", .{ .truncate = false, .read = true });
        defer idx_file_primary.close();
        try idx_file_primary.writeAll("Achoo!");

        var idx_file_secondary = try tmp_dir.dir.createFile("secondary_0.idx", .{ .truncate = true, .read = false });
        defer idx_file_secondary.close();
        logger.debug().msg("Corrupted index files").log();
    }

    // Attempt (and hopefully fail) to open corrupted index files
    {
        logger.fine().msg("Re-opening corrupted index files for store").log();
        try testing.expectError(StateError.ChecksumFailure, open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = false, .rel_path = "primary_0.idx" },
            .{ .segment_no = 0, .timestamp = 0 },
        ));
        try testing.expectError(StateError.DataTruncated, open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = false, .rel_path = "secondary_0.idx" },
            .{ .segment_no = 0, .timestamp = 0 },
        ));
        logger.debug().msg("Did NOT open corrupted index files for store").log();
    }
}

/// Dymes Index Entry Iterator
const IndexFile = @This();

pub const IteratorError = AllocationError || AccessError || StateError;

pub const Iterator = struct {
    const idx_entry_buffer_size: u64 = IndexEntry.idx_entry_size;
    const read_buffer_size: usize = 4 * 1024; // 4KiB

    logger: *logging.Logger,
    pathname: []const u8,
    file: std.fs.File,
    allocator: std.mem.Allocator,
    frame_allocator: std.mem.Allocator,
    read_offset: u64,
    idx_entry_buffer: []u8,
    reader: std.fs.File.Reader,
    read_buffer: []u8,

    pub fn init(index_file: *IndexFile) IteratorError!Iterator {
        const logger = logging.logger("msg_store.IndexFile.Iterator");
        const idx_entry_buffer: []u8 = index_file.frame_allocator.alloc(u8, idx_entry_buffer_size) catch return AllocationError.OutOfMemory;
        errdefer index_file.frame_allocator.free(idx_entry_buffer);
        const read_buffer = index_file.frame_allocator.alloc(u8, read_buffer_size) catch return AllocationError.OutOfMemory;
        errdefer index_file.frame_allocator.free(read_buffer);

        const iterator_file = std.fs.openFileAbsolute(index_file.pathname, .{ .mode = .read_only }) catch return AccessError.AccessFailure;
        errdefer iterator_file.close();

        var reader = iterator_file.reader(read_buffer);

        reader.seekTo(@sizeOf(Header)) catch |e| {
            logger.err()
                .msg("Failed to find start of index entries")
                .err(e)
                .str("pathname", index_file.pathname)
                .int("offset", @sizeOf(Header))
                .log();
            return switch (e) {
                error.EndOfStream => StateError.DataTruncated,
                else => return AccessError.AccessFailure,
            };
        };

        return .{
            .logger = logger,
            .pathname = index_file.pathname,
            .file = iterator_file,
            .allocator = index_file.allocator,
            .frame_allocator = index_file.frame_allocator,
            .read_offset = @sizeOf(Header),
            .idx_entry_buffer = idx_entry_buffer,
            .read_buffer = read_buffer,
            .reader = reader,
        };
    }

    pub fn deinit(self: *Iterator) void {
        defer self.frame_allocator.free(self.idx_entry_buffer);
        defer self.frame_allocator.free(self.read_buffer);
        self.file.close();
    }

    pub fn next(self: *Iterator) IteratorError!?IndexEntry {
        // Read index entry

        // self.reader.seekTo(self.read_offset) catch |e| {
        //     self.logger.err()
        //         .msg("Failed to seek to index entry")
        //         .err(e)
        //         .str("pathname", self.pathname)
        //         .int("read_offset", self.read_offset)
        //         .log();
        //     return switch (e) {
        //         error.EndOfStream => StateError.DataTruncated,
        //         else => return AccessError.AccessFailure,
        //     };
        // };

        var rdr_itf = &self.reader.interface;
        const cb_read = rdr_itf.readSliceShort(self.idx_entry_buffer[0..IndexEntry.idx_entry_size]) catch |e| {
            self.logger.err()
                .msg("Failed to read index entry")
                .err(e)
                .str("pathname", self.pathname)
                .int("read_offset", self.read_offset)
                .log();
            return StateError.DataTruncated;
        };
        if (cb_read == 0) {
            return null;
        }

        self.read_offset += IndexEntry.idx_entry_size;

        const idx_entry = IndexEntry.overlay(self.idx_entry_buffer);
        if (!idx_entry.verifyChecksum()) {
            self.logger.err()
                .msg("Index entry failed checksum validation")
                .ulid("ulid", idx_entry.frame_header.id)
                .str("pathname", self.pathname)
                .int("read_offset", self.read_offset)
                .log();
            return IteratorError.ChecksumFailure;
        }

        return idx_entry;
    }
};

pub fn iterator(self: *Self) IteratorError!Iterator {
    return Iterator.init(self);
}

test "IndexFile.Iterator.smoke" {
    std.debug.print("test.IndexFile.Iterator.smoke\n", .{});
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

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    var ulid_generator = common.ulid.generator();

    var logger = logging.logger("test.IndexFile.Iterator.smoke");

    const num_test_entries = 1000;

    const ancient_ulid = try ulid_generator.next();

    var ulids: [num_test_entries]Ulid = undefined;

    // Create index files
    {
        const store_ts = std.time.nanoTimestamp();

        // Create store index files
        logger.fine().msg("Creating index files").log();
        var idx_file_primary = try open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = true, .rel_path = "primary_0.idx" },
            .{ .segment_no = 0, .timestamp = store_ts },
        );
        defer idx_file_primary.close();
        logger.fine().msg("Created primary index file").log();
        var idx_file_secondary = try open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = true, .rel_path = "secondary_0.idx" },
            .{ .segment_no = 0, .timestamp = store_ts },
        );
        defer idx_file_secondary.close();
        logger.fine().msg("Created secondary index file").log();
        logger.debug().msg("Created index files").log();

        // Populate index files
        try idx_file_primary.file.seekTo(@sizeOf(Header));
        try idx_file_secondary.file.seekTo(@sizeOf(Header));
        var msg_body_buf: [100]u8 = undefined;
        var msg_file_offset: u64 = 0; // not correct offset, testing only
        for (0..num_test_entries) |idx| {
            const ulid = try ulid_generator.next();
            ulids[idx] = ulid;

            var body = std.fmt.bufPrint(&msg_body_buf, "Test message #{d}", .{idx}) catch return AccessError.AccessFailure;
            _ = &body;

            var msg = try Message.init(allocator, idx, ulid, 702, 0x0, body, .{});
            defer msg.deinit(allocator);

            var idx_entry = try IndexEntry.init(allocator, msg.frame, msg_file_offset);
            defer idx_entry.deinit(allocator);

            try idx_file_primary.storeIndexEntry(idx_entry);
            try idx_file_secondary.storeIndexEntry(idx_entry);
            msg_file_offset += msg.frame.len;
        }
    }

    {
        // Open store index files
        logger.fine().msg("Opening index files").log();
        var idx_file_primary = try open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = false, .rel_path = "primary_0.idx" },
            .{ .segment_no = 0, .timestamp = 0 },
        );
        defer idx_file_primary.close();
        logger.fine().msg("Opened primary index file").log();

        var idx_file_secondary = try open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = false, .rel_path = "secondary_0.idx" },
            .{ .segment_no = 0, .timestamp = 0 },
        );
        defer idx_file_secondary.close();
        logger.fine().msg("Opened secondary index file").log();

        logger.debug().msg("Opened index files").log();

        logger.fine().msg("Iterating over messages").log();
        var primary_it = try idx_file_primary.iterator();
        defer primary_it.deinit();
        var secondary_it = try idx_file_secondary.iterator();
        defer secondary_it.deinit();

        var idx: u64 = 0;
        while (try primary_it.next()) |entry| : (idx += 1) {
            const secondary_entry = try secondary_it.next() orelse return StateError.DataTruncated;
            try testing.expectEqualStrings(entry.idx_entry, secondary_entry.idx_entry);
        }
        logger.debug().msg("Iterated over messages in store").log();

        // Containment check
        try testing.expect(!idx_file_primary.mayContain(ancient_ulid));
        try testing.expect(idx_file_primary.mayContain(ulids[0]));
        try testing.expect(idx_file_primary.mayContain(ulids[num_test_entries - 1]));
        try testing.expect(idx_file_primary.mayContain(ulids[num_test_entries / 2]));

        // Lookup
        const off_qtr = num_test_entries / 4;
        const off_third = num_test_entries / 3;

        var timer = try std.time.Timer.start();

        const lookup_ancient = try idx_file_primary.lookup(ancient_ulid);
        try testing.expect(if (lookup_ancient) |_| false else true);

        const lookup_0 = try idx_file_primary.lookup(ulids[0]);
        try testing.expect(ulids[0].equals(lookup_0.?.frame_header.id));
        const lookup_qtr = try idx_file_primary.lookup(ulids[off_qtr]);
        try testing.expect(ulids[off_qtr].equals(lookup_qtr.?.frame_header.id));
        const lookup_2_3rds = try idx_file_primary.lookup(ulids[off_third * 2]);
        try testing.expect(ulids[off_third * 2].equals(lookup_2_3rds.?.frame_header.id));

        const lookup_time = timer.read();

        logger
            .debug()
            .msg("Lookup times")
            .int("avg lookup (ns)", lookup_time / 6)
            .log();
    }
}
