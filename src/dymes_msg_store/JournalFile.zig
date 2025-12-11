//! Dymes dataset journal file.
//!
//! This is NOT a _log journal_, it's an overall control block journal.
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
const IoError = errors.IoError;

const logging = common.logging;
const Ulid = common.ulid.Ulid;
const isBeforeUlid = common.util.isBeforeUlid;

// Log level for embedded tests
const module_tests_log_level = logging.LogLevel.none;

const constants = @import("constants.zig");
const limits = @import("limits.zig");
const dymes_msg = @import("dymes_msg");
const FrameAllocator = dymes_msg.FrameAllocator;
const FrameHeader = dymes_msg.Message.FrameHeader;
const Message = dymes_msg.Message;

pub const hot_journal_file_name = constants.hot_journal_file_name;

pub const dymes_journal_magic: [8]u8 = .{ 'D', 'I', 'M', 'E', 'S', 'J', 'N', 'L' };

/// Dataset journal information stored in journal file.
pub const DatasetJournal = extern struct {
    /// Magic file identifier
    journal_magic: [8]u8,

    /// Journal CRC.
    ///
    /// Calculation includes all fields following the `crc`.
    crc: u64,

    /// Latest data segment number
    latest_segment_no: u64,

    /// `op-no` of last committed VSR operation
    log_sequence_no: u64,

    /// First ULID in entire dataset
    dataset_first_ulid: Ulid,

    /// Last ULID in entire dataset
    dataset_last_ulid: Ulid,

    /// First ULID in latest data segment
    latest_segment_first_ulid: Ulid,

    /// Offset of latest message in data segment message file
    latest_msg_offset: u64,

    /// EOF offset in latest data segment message file
    latest_msg_file_eof: u64,

    /// EOF offset in latest data segment index file
    latest_idx_file_eof: u64,

    /// Initializes a dataset journal
    pub fn init(
        latest_segment_no: u64,
        log_sequence_no: u64,
        dataset_first_ulid: Ulid,
        dataset_last_ulid: Ulid,
        latest_segment_first_ulid: Ulid,
        latest_msg_offset: u64,
        latest_msg_file_eof: u64,
        latest_idx_file_eof: u64,
    ) DatasetJournal {
        var new_journal: DatasetJournal = undefined;
        @memcpy(&new_journal.journal_magic, &dymes_journal_magic);
        new_journal.latest_segment_no = latest_segment_no;
        new_journal.log_sequence_no = log_sequence_no;
        new_journal.dataset_first_ulid = dataset_first_ulid;
        new_journal.dataset_last_ulid = dataset_last_ulid;
        new_journal.latest_segment_first_ulid = latest_segment_first_ulid;
        new_journal.latest_msg_offset = latest_msg_offset;
        new_journal.latest_msg_file_eof = latest_msg_file_eof;
        new_journal.latest_idx_file_eof = latest_idx_file_eof;
        new_journal.crc = calculateCrc(&new_journal);
        return new_journal;
    }

    /// Updates the dataset journal.
    pub inline fn update(
        self: *DatasetJournal,
        latest_segment_no: u64,
        log_sequence_no: u64,
        dataset_first_ulid: Ulid,
        dataset_last_ulid: Ulid,
        latest_segment_first_ulid: Ulid,
        latest_msg_offset: u64,
        latest_msg_file_eof: u64,
        latest_idx_file_eof: u64,
    ) void {
        self.latest_segment_no = latest_segment_no;
        self.log_sequence_no = log_sequence_no;
        self.dataset_first_ulid = dataset_first_ulid;
        self.dataset_last_ulid = dataset_last_ulid;
        self.latest_segment_first_ulid = latest_segment_first_ulid;
        self.latest_msg_offset = latest_msg_offset;
        self.latest_msg_file_eof = latest_msg_file_eof;
        self.latest_idx_file_eof = latest_idx_file_eof;
        self.crc = self.calculateCrc();
    }

    pub const JournalReadError = IoError || StateError;

    /// Reads a dataset journal and verifies its checksum.
    pub fn read(reader: *std.Io.Reader) JournalReadError!DatasetJournal {
        var new_self = reader.takeStruct(DatasetJournal, .little) catch return JournalReadError.ReadFailed;
        if (!std.mem.eql(u8, &dymes_journal_magic, &new_self.journal_magic)) {
            return JournalReadError.IllegalState;
        }
        if (new_self.crc != calculateCrc(&new_self)) {
            return JournalReadError.ChecksumFailure;
        }
        return new_self;
    }

    pub const JournalWriteError = IoError;

    /// Writes the dataset journal.
    pub fn write(self: *const DatasetJournal, writer: *std.Io.Writer) JournalWriteError!void {
        writer.writeStruct(self.*, .little) catch return JournalWriteError.WriteFailed;
    }

    /// Formats the dataset journal and prints to the given writer.
    pub fn format(self: *const DatasetJournal, writer: *std.Io.Writer) !void {
        try writer.print("{{latest_segment_no={d},log_sequence_no={d},dataset_first_ulid={f},dataset_last_ulid={f},latest_segment_first_ulid={f},latest_msg_offset={d},latest_msg_file_eof={d},latest_idx_file_eof={d},crc={d}}}", .{
            self.latest_segment_no, self.log_sequence_no, self.dataset_first_ulid, self.dataset_last_ulid, self.latest_segment_first_ulid, self.latest_msg_offset, self.latest_msg_file_eof, self.latest_idx_file_eof, self.crc,
        });
    }

    /// Calculates the journal CRC.
    pub inline fn calculateCrc(self: *const DatasetJournal) u64 {
        const crc_slice_offset: usize = dymes_journal_magic.len + @sizeOf(u64);
        const journal_slice: []const u8 = @ptrCast(@alignCast(self));
        return std.hash.crc.Crc64Redis.hash(journal_slice[crc_slice_offset..]);
    }
};

const Self = @This();

/// Logger
logger: *logging.Logger,

/// Journal file
file: std.fs.File,

/// Journal file directory
dir: std.fs.Dir,

/// General purpose allocator
gpa: std.mem.Allocator,

/// Absolute pathname of journal file
pathname: []const u8,

/// Write buffer
write_buffer: []u8,

/// File writer
file_writer: std.fs.File.Writer,

/// Message store options
pub const StoreOptions = struct {
    /// Segment in message store
    segment_no: u64,
};

/// File options
pub const FileOptions = struct {
    /// Relative pathname
    rel_path: []const u8,
};

pub const OpenError = AllocationError || CreationError || AccessError || StateError;
pub const WriteError = DatasetJournal.JournalWriteError || AccessError;
pub const VerifyError = DatasetJournal.JournalReadError || AccessError || AllocationError;

/// Creates a journal file.
///
/// Caller must call `close()` to release resources.
pub fn create(gpa: Allocator, dir: std.fs.Dir, rel_path: []const u8) OpenError!Self {
    var logger = logging.logger("msg_store.JournalFile");

    logger.fine()
        .msg("Creating journal file")
        .str("rel_path", rel_path)
        .log();

    // We clobber the old journal file, since contrary to expectations and API docs,
    // dir.createFile(), even with truncate flag, is unhappy if the file already exists. Sigh.
    dir.deleteFile(rel_path) catch |e| {
        if (e != error.FileNotFound) {
            logger.err()
                .msg("Failed to delete old journal file")
                .err(e)
                .str("rel_path", rel_path)
                .log();
            return switch (e) {
                error.PermissionDenied, error.AccessDenied => OpenError.AccessFailure,
                else => OpenError.OtherAccessFailure,
            };
        }
    };

    const file_create_flags: std.fs.File.CreateFlags = .{
        .lock = .shared,
        .exclusive = true,
        .read = true,
        .truncate = true,
    };
    var hot_journal_file = dir.createFile(rel_path, file_create_flags) catch |e| {
        logger.err()
            .msg("Failed to open journal file")
            .err(e)
            .str("rel_path", rel_path)
            .log();
        return switch (e) {
            error.SharingViolation, error.AccessDenied => OpenError.AccessFailure,
            error.FileNotFound => OpenError.FileNotFound,
            error.PathAlreadyExists => OpenError.FileAlreadyExists,
            else => OpenError.OtherCreationFailure,
        };
    };
    errdefer hot_journal_file.close();

    const pathname = dir.realpathAlloc(gpa, rel_path) catch |e| {
        logger.err()
            .msg("Failed to determine real journal file path")
            .err(e)
            .str("rel_path", rel_path)
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
    errdefer gpa.free(pathname);

    const write_buffer = gpa.alloc(u8, @max(512, @sizeOf(DatasetJournal))) catch return VerifyError.OutOfMemory;
    errdefer gpa.free(write_buffer);

    // NOTE: We *do not* write a journal entry upon creation

    defer logger.debug()
        .msg("Journal file opened")
        .str("rel_path", rel_path)
        .log();

    return .{
        .logger = logger,
        .file = hot_journal_file,
        .dir = dir,
        .gpa = gpa,
        .pathname = pathname,
        .write_buffer = write_buffer,
        .file_writer = hot_journal_file.writer(write_buffer),
    };
}

/// Closes a journal file, releasing resources.
pub fn close(self: *Self) void {
    defer self.gpa.free(self.pathname);
    defer self.gpa.free(self.write_buffer);
    self.logger.fine()
        .msg("Closing journal file")
        .str("pathname", self.pathname)
        .log();
    {
        defer self.file.close();
        self.sync() catch {};
    }
    defer self.logger.debug()
        .msg("Journal file closed")
        .log();
}

/// Synchronizes pending changes with the filesystem.
pub fn sync(self: *const Self) SyncError!void {
    self.file.sync() catch |e| {
        self.logger.err()
            .msg("Failed to synchronize journal state with filesystem")
            .err(e)
            .str("pathname", self.pathname)
            .log();
        return switch (e) {
            std.posix.SyncError.AccessDenied, std.posix.SyncError.InputOutput => SyncError.AccessFailure,
            std.posix.SyncError.DiskQuota, std.posix.SyncError.NoSpaceLeft => SyncError.OutOfSpace,
            std.posix.SyncError.Unexpected => SyncError.OtherAccessFailure,
        };
    };
}

/// Writes the given dataset journal data to the hot journal file.
pub fn write(self: *Self, journal: *const DatasetJournal) WriteError!void {
    self.file_writer.seekTo(0) catch |_e| {
        self.logger.err()
            .msg("Failed to reposition in journal file")
            .err(_e)
            .str("pathname", self.pathname)
            .log();
        return WriteError.AccessFailure;
    };
    journal.write(&self.file_writer.interface) catch |_e| {
        self.logger.err()
            .msg("Failed to write to journal file")
            .err(_e)
            .str("pathname", self.pathname)
            .log();
        return WriteError.WriteFailed;
    };
    self.file_writer.interface.flush() catch |_e| {
        self.logger.err()
            .msg("Failed to flush to journal file")
            .err(_e)
            .str("pathname", self.pathname)
            .log();
        return WriteError.WriteFailed;
    };
}

/// Verifies a hot journal file during recovery.
///
/// If the journal is corrupt, we log a warning and return null.
pub fn verify(logger: *logging.Logger, gpa: Allocator, dir: std.fs.Dir, rel_path: []const u8) ?DatasetJournal {
    var hot_journal_file = dir.openFile(rel_path, .{ .lock = .exclusive, .mode = .read_only }) catch |_e| {
        if (_e == error.FileNotFound) {
            // No hot journal found, we should be good to go
            logger.fine()
                .msg("No hot journal detected")
                .log();
        } else {
            logger.warn()
                .msg("Ignoring corrupt hot journal file")
                .err(_e)
                .log();
        }
        return null;
    };
    defer hot_journal_file.close();

    const eof_offset = hot_journal_file.getEndPos() catch |_e| {
        logger.warn()
            .msg("Failed to determine end of journal")
            .err(_e)
            .log();
        return null;
    };
    if (eof_offset < @sizeOf(DatasetJournal)) {
        // NOTE: This can happen when journal is created, but not written to afterwards
        logger.warn()
            .msg("Hot journal file truncated")
            .log();
        return null;
    }

    const read_buffer = gpa.alignedAlloc(u8, std.mem.Alignment.of(DatasetJournal), @max(512, @sizeOf(DatasetJournal))) catch {
        logger.warn()
            .msg("Out of memory while allocating hot journal read buffer")
            .log();
        return null;
    };
    defer gpa.free(read_buffer);

    var hot_journal_reader = hot_journal_file.reader(read_buffer);
    return DatasetJournal.read(&hot_journal_reader.interface) catch |_e| {
        logger.warn()
            .msg("Failed to read hot journal file")
            .err(_e)
            .log();
        return null;
    };
}

const JournalFile = @This();

test "JournalFile.smoke" {
    std.debug.print("test.JournalFile.smoke\n", .{});
    const allocator = testing.allocator;

    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = module_tests_log_level;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    var logger = logging.logger("test.JournalFile.smoke");

    // No pre-existing journal file

    {
        logger.debug()
            .msg("Testing handling of no pre-existing journal file")
            .log();

        if (JournalFile.verify(logger, allocator, tmp_dir.dir, "nonexistent.journal")) |_| {
            try testing.expect(false);
        } else {
            try testing.expect(true);
        }
    }

    // Empty journal file

    {
        logger.debug()
            .msg("Testing handling of empty journal file")
            .log();

        {
            var jf = try JournalFile.create(allocator, tmp_dir.dir, "empty.journal");
            defer jf.close();
        }

        try testing.expectEqual(null, JournalFile.verify(logger, allocator, tmp_dir.dir, "empty.journal"));
    }

    // Not a journal file

    {
        logger.debug()
            .msg("Testing handling of a non-journal file")
            .log();
        {
            var jf = try JournalFile.create(allocator, tmp_dir.dir, "not_a_journal.txt");
            var writer: *std.Io.Writer = &jf.file_writer.interface;
            try writer.writeAll("A" ** 1024);
            try writer.flush();
            defer jf.close();
        }

        try testing.expectEqual(null, JournalFile.verify(logger, allocator, tmp_dir.dir, "not_a_journal.txt"));
    }

    // Corrupt journal file

    {
        logger.debug()
            .msg("Testing handling of corrupt journal file")
            .log();
        {
            var jf = try JournalFile.create(allocator, tmp_dir.dir, "corrupt.journal");

            var journal: DatasetJournal = undefined;
            journal.latest_idx_file_eof = 128; // modified without recalculating CRC
            try jf.write(&journal);

            const end_pos = try jf.file.getEndPos();
            logger.fine()
                .msg("Written to corrupt journal file")
                .int("end_pos", end_pos)
                .int("expected", @sizeOf(DatasetJournal))
                .log();

            defer jf.close();
        }
        try testing.expectEqual(null, JournalFile.verify(logger, allocator, tmp_dir.dir, "corrupt.journal"));
    }

    // Sane journal file

    {
        logger.debug()
            .msg("Testing handling of sane journal file")
            .log();
        {
            var jf = try JournalFile.create(allocator, tmp_dir.dir, "sane.journal");

            const dummy_ulid: Ulid = .{ .time = 0, .rand = 0 };
            var journal = DatasetJournal.init(5, 50_000, dummy_ulid, dummy_ulid, dummy_ulid, 499_900, 500_000, 5_000);
            try jf.write(&journal);

            const end_pos = try jf.file.getEndPos();
            logger.fine()
                .msg("Written to sane journal file")
                .int("end_pos", end_pos)
                .int("expected", @sizeOf(DatasetJournal))
                .log();

            defer jf.close();
        }
        if (JournalFile.verify(logger, allocator, tmp_dir.dir, "sane.journal")) |_journal| {
            try testing.expectEqual(_journal.log_sequence_no, 50_000);
            try testing.expectEqual(_journal.latest_msg_file_eof, 500_000);
            try testing.expectEqual(_journal.latest_idx_file_eof, 5_000);
        } else {
            try testing.expect(false);
        }
    }
}
