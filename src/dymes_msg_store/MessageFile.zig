//! Dymes Message File.
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

const constants = @import("constants.zig");
const limits = @import("limits.zig");
const dymes_msg = @import("dymes_msg");
const FrameHeader = dymes_msg.Message.FrameHeader;
const Message = dymes_msg.Message;
const FrameAllocator = dymes_msg.FrameAllocator;

pub const Header = @import("headers.zig").MessageFileHeader;

pub const msg_file_name_ext = constants.msg_file_name_ext;
pub const msg_file_name_fmt = constants.msg_file_name_fmt;

/// Message file status
pub const Status = struct {
    /// Open state
    opened: bool,

    /// Readiness
    ready: bool,
};

const Self = @This();

/// Logger
logger: *logging.Logger,

/// Message file
file: std.fs.File,

/// Allocator
allocator: std.mem.Allocator,

/// Frame Allocator
frame_allocator: std.mem.Allocator,

/// Message file header
header: Header,

/// Absolute pathname of message file
pathname: []const u8,

/// Segment number
segment_no: u64,

/// Status
status: Status,

/// Offset from start of file to next message write position
append_offset: u64,

/// Message frame buffer (used for fetching)
frame_buffer: []u8,

/// Dirty seek flag
dirty_seek: bool,

/// Read buffer
read_buffer: []u8,

/// Message store options
pub const StoreOptions = struct {
    /// Time of file creation (as per message store)
    timestamp: i128,

    /// Segment number
    segment_no: u64,

    /// Verification required?
    /// - Typically set to `true` when resuming with an open hot-journal
    needs_verification: bool = false,
};

/// File options
pub const FileOptions = struct {
    /// Relative pathname
    rel_path: []const u8,

    /// Should file be created?
    create: bool = false,
};

pub const OpenError = AllocationError || CreationError || AccessError || StateError;

/// Opens a message file.
///
/// Caller must call `close()` to release resources.
pub fn open(allocator: std.mem.Allocator, frame_allocator: std.mem.Allocator, dir: std.fs.Dir, file_options: FileOptions, store_options: StoreOptions) OpenError!Self {
    var logger = logging.logger("msg_store.MessageFile");

    logger.fine()
        .msg("Opening message file")
        .int("segment_no", store_options.segment_no)
        .log();
    const file_create_flags: std.fs.File.CreateFlags = .{
        .lock = .shared,
        .exclusive = false,
        .read = true,
        .truncate = file_options.create,
    };
    var file = dir.createFile(file_options.rel_path, file_create_flags) catch |e| {
        logger.err()
            .msg("Failed to open message file")
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
            .msg("Failed to determine message file path")
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

    const frame_buffer = frame_allocator.alloc(u8, limits.max_message_size) catch return AllocationError.OutOfMemory;
    errdefer frame_allocator.free(frame_buffer);

    const read_buffer = frame_allocator.alloc(u8, limits.message_file_direct_read_buffer_size) catch return AllocationError.OutOfMemory;
    errdefer frame_allocator.free(read_buffer);

    var new_self: Self = .{
        .allocator = allocator,
        .frame_allocator = frame_allocator,
        .logger = logger,
        .file = file,
        .pathname = pathname,
        .header = undefined,
        .segment_no = store_options.segment_no,
        .status = .{ .opened = true, .ready = false },
        .append_offset = 0,
        .frame_buffer = frame_buffer,
        .dirty_seek = true,
        .read_buffer = read_buffer,
    };

    if (file_options.create) {
        // Write initial header upon creation
        const hdr_buf = std.mem.asBytes(&new_self.header);
        _ = Header.initOverlay(hdr_buf, store_options.segment_no, store_options.timestamp);
        file.writeAll(hdr_buf) catch |e| {
            logger.err()
                .msg("Failed to initialize message file")
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
            .msg("Initialized message file")
            .int("segment", store_options.segment_no)
            .str("pathname", pathname)
            .log();
    } else {
        // Read initial header upon opening
        const hdr_buf = std.mem.asBytes(&new_self.header);
        var rdr = file.reader(new_self.read_buffer);
        var rdr_itf = &rdr.interface;
        _ = rdr_itf.readSliceAll(hdr_buf) catch |e| {
            logger.err()
                .msg("Failed to read message file header")
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

    new_self.append_offset = file.getEndPos() catch |e| {
        logger
            .err()
            .msg("Failed to determine message storage position")
            .err(e)
            .int("segment", new_self.segment_no)
            .str("pathname", new_self.pathname)
            .log();
        return StoreError.AccessFailure;
    };

    if (store_options.needs_verification) {
        if (!new_self.verifyHealth()) {
            logger
                .err()
                .msg("Message file failed health check")
                .int("append_offset", new_self.append_offset)
                .int("segment", new_self.segment_no)
                .str("pathname", new_self.pathname)
                .log();
            return StateError.IllegalState;
        }
    }

    new_self.status.ready = true;

    logger.fine()
        .msg("Message file opened")
        .int("segment", store_options.segment_no)
        .str("pathname", pathname)
        .log();

    return new_self;
}

/// Closes (de-initializes) a message file.
pub fn close(self: *Self) void {
    if (!self.status.opened) {
        self.logger.fine()
            .msg("Message file already closed")
            .int("segment", self.segment_no)
            .log();
        return;
    }
    defer self.status.opened = false;
    defer self.frame_allocator.free(self.frame_buffer);
    defer self.frame_allocator.free(self.read_buffer);
    defer self.allocator.free(self.pathname);
    {
        defer self.file.close();
        self.sync() catch {};
    }
    self.logger.fine()
        .msg("Message file closed")
        .int("segment", self.segment_no)
        .str("pathname", self.pathname)
        .log();
}

/// Synchronizes pending changes with the filesystem.
pub fn sync(self: *Self) SyncError!void {
    if (!self.ready()) {
        return;
    }
    self.file.sync() catch |e| {
        self.logger.err()
            .msg("Failed to sync message file")
            .err(e)
            .int("segment", self.segment_no)
            .str("pathname", self.pathname)
            .int("append_offset", self.append_offset)
            .log();
        self.status.ready = false;
        return switch (e) {
            std.posix.SyncError.AccessDenied, std.posix.SyncError.InputOutput => SyncError.AccessFailure,
            std.posix.SyncError.DiskQuota, std.posix.SyncError.NoSpaceLeft => SyncError.OutOfSpace,
            std.posix.SyncError.Unexpected => SyncError.OtherAccessFailure,
        };
    };
}

/// Queries the message file readiness
pub inline fn ready(self: *const Self) bool {
    return self.status.ready;
}

/// Formats the message file context and prints to the given writer.
pub fn format(self: *const Self, writer: *std.Io.Writer) !void {
    try writer.print("{{pathname=\\\"{s}\\\",header={any}}}", .{ self.pathname, self.header });
}

pub const StoreError = StateError || AccessError || CreationError;

/// Checks if we have capacity to store the given message
pub inline fn haveCapacity(self: *const Self, message: *const Message) bool {
    if (!self.ready() or self.append_offset + message.frame_header.frame_size > limits.max_message_file_size) {
        return false;
    }
    return true;
}

/// Stores the given message and returns its offset in the message file.
pub fn store(self: *Self, message: *const Message) StoreError!u64 {
    assert(message.frame_header.frame_size <= limits.max_message_size);
    if (!self.ready()) {
        return StoreError.NotReady;
    } else if (self.append_offset + message.frame_header.frame_size > limits.max_message_file_size) {
        self.logger.warn()
            .msg("Failed to store message - file size limit reached")
            .ulid("message.id", message.frame_header.id)
            .int("limit", limits.max_message_file_size)
            .int("segment", self.segment_no)
            .str("pathname", self.pathname)
            .log();
        return StoreError.OutOfSpace;
    }
    const message_id = message.frame_header.id;

    if (self.dirty_seek) {
        self.file.seekFromEnd(0) catch |e| {
            self.logger.err()
                .msg("Failed to seek to message storage position")
                .err(e)
                .ulid("message_id", message_id)
                .int("message_channel", message.frame_header.channel)
                .int("segment", self.segment_no)
                .str("pathname", self.pathname)
                .log();
            self.status.ready = false;
            return StoreError.AccessFailure;
        };
        self.dirty_seek = false;
    }

    const WriteError = std.fs.File.WriteError;
    self.file.writeAll(message.frame) catch |e| {
        self.logger.err()
            .msg("Failed to store message")
            .err(e)
            .ulid("message_id", message_id)
            .int("message_channel", message.frame_header.channel)
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
    const msg_write_pos = self.append_offset;
    self.append_offset += message.frame.len;
    return msg_write_pos;
}

pub const FetchError = AccessError || StateError;

pub fn fetch(self: *Self, msg_offset: usize) FetchError!Message {
    var rdr = self.file.reader(self.read_buffer);
    var rdr_itf = &rdr.interface;

    rdr.seekTo(msg_offset) catch |e| {
        self.logger.err()
            .msg("Failed to seek to message position")
            .err(e)
            .int("msg_offset", msg_offset)
            .int("segment", self.segment_no)
            .str("pathname", self.pathname)
            .log();
        self.status.ready = false;
        return FetchError.AccessFailure;
    };
    self.dirty_seek = true;

    // Read message header
    rdr_itf.readSliceAll(self.frame_buffer[0..@sizeOf(Message.FrameHeader)]) catch |e| {
        self.logger.err()
            .msg("Failed to read message header")
            .err(e)
            .int("msg_offset", msg_offset)
            .int("segment", self.segment_no)
            .str("pathname", self.pathname)
            .log();
        switch (e) {
            error.ReadFailed => return FetchError.AccessFailure,
            error.EndOfStream => return FetchError.DataTruncated,
        }
        self.status.ready = false;
    };

    // Ensure the header is sane, don't want to go fetching random bits further below
    const msg_hdr: *Message.FrameHeader = @ptrCast(@alignCast(self.frame_buffer));
    if (!msg_hdr.verifyChecksum()) {
        self.logger.err()
            .msg("Message header failed checksum verification")
            .int("msg_offset", msg_offset)
            .int("segment", self.segment_no)
            .str("pathname", self.pathname)
            .log();
        self.status.ready = false;
        return FetchError.ChecksumFailure;
    }

    // Read the complete message frame
    const remaining: usize = msg_hdr.frame_size - @sizeOf(Message.FrameHeader);
    if (remaining > 0) {
        _ = rdr_itf.readSliceAll(self.frame_buffer[@sizeOf(Message.FrameHeader)..msg_hdr.frame_size]) catch |e| {
            self.logger.err()
                .msg("Failed to read message remainder")
                .err(e)
                .int("remaining", remaining)
                .int("msg_offset", msg_offset)
                .int("segment", self.segment_no)
                .str("pathname", self.pathname)
                .log();
            self.status.ready = false;
            return FetchError.DataTruncated;
        };
    }

    // Overlay a message over the buffer
    const message = Message.overlay(self.frame_buffer[0..msg_hdr.frame_size]);
    message.verifyChecksums() catch |e| {
        self.logger.err()
            .msg("Message failed checksum validation")
            .err(e)
            .ulid("ulid", message.frame_header.id)
            .int("msg_offset", msg_offset)
            .int("segment", self.segment_no)
            .str("pathname", self.pathname)
            .log();
        return FetchError.ChecksumFailure;
    };
    return message;
}

/// Verify healthy status of this message file
fn verifyHealth(self: *Self) bool {
    self.logger.fine()
        .msg("Verifying message file")
        .int("segment", self.segment_no)
        .str("pathname", self.pathname)
        .log();
    var it = Iterator.open(self, @sizeOf(Header)) catch |e| {
        self.logger.warn()
            .msg("Failed to verify message file health - cannot open iterator")
            .err(e)
            .int("segment", self.segment_no)
            .str("pathname", self.pathname)
            .log();
        return false;
    };

    defer it.close();
    while (it.next() catch |e| {
        self.logger.warn()
            .msg("Failed to verify message file health while iterating messages")
            .err(e)
            .int("segment", self.segment_no)
            .str("pathname", self.pathname)
            .log();
        return false;
    }) |msg| {
        _ = &msg;
    }
    self.logger.fine()
        .msg("Message file verified")
        .int("segment", self.segment_no)
        .str("pathname", self.pathname)
        .log();
    return true;
}

test "MessageFile.smoke" {
    std.debug.print("test.MessageFile.smoke\n", .{});
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

    var logger = logging.logger("test.MessageFile");

    // Create message files
    {
        const store_ts = std.time.nanoTimestamp();

        // Create store message files
        logger.fine().msg("Creating message files").log();
        var msg_file_primary = try open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = true, .rel_path = "primary_0.msg" },
            .{ .segment_no = 0, .timestamp = store_ts },
        );
        defer msg_file_primary.close();
        logger.fine().msg("Created primary message file").log();
        var msg_file_secondary = try open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = true, .rel_path = "secondary_0.msg" },
            .{ .segment_no = 0, .timestamp = store_ts },
        );
        defer msg_file_secondary.close();
        logger.fine().msg("Created secondary message file").log();
        logger.debug().msg("Created message files").log();
    }

    // Open message files
    {
        // Open store message files
        logger.fine().msg("Opening message files").log();
        var msg_file_primary = try open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = false, .rel_path = "primary_0.msg" },
            .{ .segment_no = 0, .timestamp = 0 },
        );
        defer msg_file_primary.close();
        logger.fine().msg("Opened primary message file").log();

        var msg_file_secondary = try open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = false, .rel_path = "secondary_0.msg" },
            .{ .segment_no = 0, .timestamp = 0 },
        );
        defer msg_file_secondary.close();
        logger.fine().msg("Opened secondary message file").log();

        logger.debug().msg("Opened message files").log();
        try testing.expectEqualStrings(std.mem.asBytes(&msg_file_primary.header), std.mem.asBytes(&msg_file_secondary.header));
    }

    // Corrupt message files
    {
        logger.fine().msg("Corrupting message files").log();
        var msg_file_primary = try tmp_dir.dir.createFile("primary_0.msg", .{ .truncate = false, .read = true });
        defer msg_file_primary.close();
        try msg_file_primary.writeAll("Achoo!");

        var msg_file_secondary = try tmp_dir.dir.createFile("secondary_0.msg", .{ .truncate = true, .read = false });
        defer msg_file_secondary.close();
        logger.debug().msg("Corrupted message files").log();
    }

    // Attempt (and hopefully fail) to open corrupted message files
    {
        logger.fine().msg("Re-opening corrupted message files").log();
        try testing.expectError(StateError.ChecksumFailure, open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = false, .rel_path = "primary_0.msg" },
            .{ .segment_no = 0, .timestamp = 0 },
        ));
        try testing.expectError(StateError.DataTruncated, open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = false, .rel_path = "secondary_0.msg" },
            .{ .segment_no = 0, .timestamp = 0 },
        ));
        logger.debug().msg("Did NOT open corrupted message files").log();
    }
}

test "MessageFile.fetch" {
    std.debug.print("test.MessageFile.fetch\n", .{});
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

    var logger = logging.logger("test.MessageFile.fetch");

    // Create message files
    {
        const store_ts = std.time.nanoTimestamp();

        // Create store message files
        logger.fine().msg("Creating message files").log();
        var msg_file_primary = try open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = true, .rel_path = "primary_10_seq_0.msg" },
            .{ .segment_no = 0, .timestamp = store_ts },
        );
        defer msg_file_primary.close();
        logger.fine().msg("Created primary message file").log();
        var msg_file_secondary = try open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = true, .rel_path = "secondary_10_seq_0.msg" },
            .{ .segment_no = 0, .timestamp = store_ts },
        );
        defer msg_file_secondary.close();
        logger.fine().msg("Created secondary message file").log();
        logger.debug().msg("Created message files").log();

        // Populate message files
        var msg_body_buf: [100]u8 = undefined;
        for (0..10) |idx| {
            const ulid = try ulid_generator.next();

            var body = std.fmt.bufPrint(&msg_body_buf, "Test message #{d}", .{idx}) catch return AccessError.AccessFailure;
            _ = &body;

            var msg = try Message.init(allocator, idx, ulid, 702, 0x0, body, .{});
            defer msg.deinit(allocator);

            _ = try msg_file_primary.store(&msg);
            _ = try msg_file_secondary.store(&msg);
            logger.debug().msg("Stored message").ulid("id", msg.frame_header.id).int("size", msg.frame_header.frame_size).log();
        }
        logger.debug().msg("Populated message files").log();
        var msg_offset: usize = @sizeOf(Header);
        for (0..10) |_| {
            const msg_p = try msg_file_primary.fetch(msg_offset);
            const msg_s = try msg_file_secondary.fetch(msg_offset);
            try testing.expectEqualSlices(u8, msg_p.frame, msg_s.frame);
            msg_offset += msg_p.frame_header.frame_size;
            logger.debug().msg("Fetched message").ulid("id", msg_p.frame_header.id).int("size", msg_p.frame_header.frame_size).log();
        }
        logger.debug().msg("Read message files").log();
    }
}

/// Dymes Message Iterator
const MessageFile = @This();

pub const IteratorError = AllocationError || AccessError || StateError;

pub const Iterator = struct {
    const message_buffer_size: u64 = dymes_msg.limits.max_frame_size;
    const iterator_read_buffer_size = limits.message_file_iterator_read_buffer_size;

    logger: *logging.Logger,
    pathname: []const u8,
    file: std.fs.File,
    frame_allocator: std.mem.Allocator,
    read_offset: u64,
    frame_buffer: []u8,
    reader_buffer: []u8,
    reader: std.fs.File.Reader,
    message_file: *MessageFile,

    pub fn open(message_file: *MessageFile, start_offset: u64) IteratorError!Iterator {
        const logger = logging.logger("msg_store.MessageFile.Iterator");
        const frame_buffer: []u8 = message_file.frame_allocator.alloc(u8, message_buffer_size) catch return AllocationError.OutOfMemory;
        errdefer message_file.frame_allocator.free(frame_buffer);

        const reader_buffer = message_file.frame_allocator.alloc(u8, iterator_read_buffer_size) catch return AllocationError.OutOfMemory;
        errdefer message_file.frame_allocator.free(reader_buffer);

        const iterator_file = std.fs.openFileAbsolute(message_file.pathname, .{ .mode = .read_only }) catch return AccessError.AccessFailure;
        errdefer iterator_file.close();

        var reader = iterator_file.reader(reader_buffer);
        reader.seekTo(start_offset) catch |e| {
            logger.err()
                .msg("Failed to seek to start of message file")
                .err(e)
                .str("pathname", message_file.pathname)
                .int("start_offset", start_offset)
                .log();
            return AccessError.AccessFailure;
        };

        return .{
            .logger = logger,
            .pathname = message_file.pathname,
            .file = iterator_file,
            .frame_allocator = message_file.frame_allocator,
            .read_offset = start_offset,
            .frame_buffer = frame_buffer,
            .reader_buffer = reader_buffer,
            .reader = reader,
            .message_file = message_file,
        };
    }

    pub fn close(self: *Iterator) void {
        defer self.frame_allocator.free(self.frame_buffer);
        defer self.frame_allocator.free(self.reader_buffer);
        self.file.close();
    }

    pub fn next(self: *Iterator) IteratorError!?Message {
        // Read message header
        var rdr = &self.reader.interface;
        const cb_read = rdr.readSliceShort(self.frame_buffer[0..@sizeOf(Message.FrameHeader)]) catch |e| {
            self.logger.err()
                .msg("Failed to read message header")
                .err(e)
                .str("pathname", self.pathname)
                .int("read_offset", self.read_offset)
                .log();
            return StateError.DataTruncated;
        };

        if (cb_read == 0) {
            return null;
        }

        const msg_hdr: *Message.FrameHeader = @ptrCast(@alignCast(self.frame_buffer));

        if (msg_hdr.frame_size == 0 or msg_hdr.frame_size > limits.max_message_size) {
            self.logger.err()
                .msg("Message frame header contains invalid frame size")
                .int("frame_size", msg_hdr.frame_size)
                .str("pathname", self.pathname)
                .int("read_offset", self.read_offset)
                .log();
            return StateError.IllegalState;
        }

        _ = rdr.readSliceAll(self.frame_buffer[@sizeOf(Message.FrameHeader)..msg_hdr.frame_size]) catch |e| {
            self.logger.err()
                .msg("Failed to read message remainder")
                .err(e)
                .str("pathname", self.pathname)
                .int("read_offset", self.read_offset)
                .log();
            return StateError.DataTruncated;
        };

        if (!msg_hdr.verifyChecksum()) {
            self.logger.err()
                .msg("Message header failed checksum validation")
                .ulid("ulid", msg_hdr.id)
                .int("read_offset", self.read_offset)
                .str("pathname", self.pathname)
                .any("msg_header", msg_hdr)
                .log();
            return FetchError.ChecksumFailure;
        }

        self.read_offset += msg_hdr.frame_size;

        const message = Message.overlay(self.frame_buffer[0..msg_hdr.frame_size]);
        message.verifyChecksums() catch |e| {
            self.logger.err()
                .msg("Message failed checksum validation")
                .err(e)
                .ulid("ulid", message.frame_header.id)
                .int("read_offset", self.read_offset)
                .str("pathname", self.pathname)
                .log();
            return FetchError.ChecksumFailure;
        };
        return message;
    }
};

pub inline fn iterator(self: *Self) IteratorError!Iterator {
    return offsetIterator(self, @sizeOf(Header));
}

pub fn offsetIterator(self: *Self, start_offset: u64) IteratorError!Iterator {
    if (!self.ready()) {
        return IteratorError.NotReady;
    }
    return Iterator.open(self, start_offset);
}

test "MessageFile.Iterator.smoke" {
    std.debug.print("test.MessageFile.Iterator.smoke\n", .{});
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

    var logger = logging.logger("test.MessageFile.Iterator.smoke");

    const num_test_messages: usize = 10;

    // Create message files
    {
        const store_ts = std.time.nanoTimestamp();

        // Create store message files
        logger.fine().msg("Creating message files").log();
        var msg_file_primary = try open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = true, .rel_path = "primary_0.msg" },
            .{ .segment_no = 0, .timestamp = store_ts },
        );
        defer msg_file_primary.close();
        logger.fine().msg("Created primary message file").log();
        var msg_file_secondary = try open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = true, .rel_path = "secondary_0.msg" },
            .{ .segment_no = 0, .timestamp = store_ts },
        );
        defer msg_file_secondary.close();
        logger.fine().msg("Created secondary message file").log();
        logger.debug().msg("Created message files").log();

        // Populate message files
        var msg_body_buf: [100]u8 = undefined;
        for (0..num_test_messages) |idx| {
            const ulid = try ulid_generator.next();

            var body = std.fmt.bufPrint(&msg_body_buf, "Test message #{d}", .{idx}) catch return AccessError.AccessFailure;
            _ = &body;

            var msg = try Message.init(allocator, idx, ulid, 702, 0x0, body, .{});
            defer msg.deinit(allocator);

            _ = try msg_file_primary.store(&msg);
            _ = try msg_file_secondary.store(&msg);
        }
    }

    {
        // Open store message files
        logger.fine().msg("Opening message files").log();
        var msg_file_primary = try open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = false, .rel_path = "primary_0.msg" },
            .{ .segment_no = 0, .timestamp = 0 },
        );
        defer msg_file_primary.close();
        logger.fine().msg("Opened primary message file").log();

        var msg_file_secondary = try open(
            allocator,
            frame_allocator,
            tmp_dir.dir,
            .{ .create = false, .rel_path = "secondary_0.msg" },
            .{ .segment_no = 0, .timestamp = 0 },
        );
        defer msg_file_secondary.close();
        logger.fine().msg("Opened secondary message file").log();

        logger.debug().msg("Opened message files").log();

        logger.fine().msg("Iterating over messages").log();
        var msg_body_buf: [100]u8 = undefined;
        var primary_it = try msg_file_primary.iterator();
        defer primary_it.close();
        var secondary_it = try msg_file_secondary.offsetIterator(@sizeOf(Header));
        defer secondary_it.close();

        var idx: u64 = 0;
        while (try primary_it.next()) |pmsg| : (idx += 1) {
            const body = std.fmt.bufPrint(&msg_body_buf, "Test message #{d}", .{idx}) catch return AccessError.AccessFailure;
            const smsg = try secondary_it.next() orelse return StateError.DataTruncated;
            logger.fine().msg("Next encountered message").str("body", smsg.msg_body).log();
            try testing.expectEqualStrings(pmsg.frame, smsg.frame);
            try testing.expectEqualStrings(body, pmsg.msg_body);
        }
        try testing.expectEqual(num_test_messages, idx);
        logger.debug().msg("Iterated over messages").log();
    }
}
