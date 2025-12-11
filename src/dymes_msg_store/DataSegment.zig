//! Dymes Message Store Data Segment.
//!
//! This is the combination of message file and index file for a particular sequence no
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const assert = std.debug.assert;

const common = @import("dymes_common");
const Ulid = common.ulid.Ulid;
const logging = common.logging;
const Logger = logging.Logger;
const limits = @import("limits.zig");
const constants = @import("constants.zig");

const dymes_msg = @import("dymes_msg");

const FrameAllocator = dymes_msg.FrameAllocator;

const errors = @import("errors.zig");

const AllocationError = errors.AllocationError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;
const SyncError = errors.SyncError;

const MessageFile = @import("MessageFile.zig");
const IndexFile = @import("IndexFile.zig");
const MessageIndex = @import("MessageIndex.zig");
const JournalFile = @import("JournalFile.zig");

const msg_file_name_ext = MessageFile.msg_file_name_ext;
const msg_file_name_fmt = MessageFile.msg_file_name_fmt;
const idx_file_name_ext = IndexFile.idx_file_name_ext;
const idx_file_name_fmt = IndexFile.idx_file_name_fmt;

pub const SyncLevel = constants.SyncLevel;

/// Data segment options
pub const Options = struct {
    /// Segment number
    segment_no: u64,

    /// Segment timestamp
    timestamp: i128,

    /// Verification required?
    /// - Typically set to `true` when resuming with an open hot-journal
    needs_verification: bool = false,

    /// Initialize data files flag
    ///
    /// If this flag is set, then the message and index files are either created (if missing) or truncated (if present)
    /// If this flag is _not_ set, then the message and index files must already exist
    initialize: bool,

    /// Sync level
    sync_level: SyncLevel = constants.default_sync_level,
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

pub const OpenError = CreationError || AccessError || StateError;

/// Logger
logger: *Logger,

/// Memory allocator
gpa: std.mem.Allocator,

/// Message frame allocator
frame_allocator: std.mem.Allocator,

/// Segment number
segment_no: u64,

/// Message file
message_file: MessageFile,

/// Index file
index_file: IndexFile,

/// Sync level
sync_level: SyncLevel,

/// Dataset status
status: Status,

/// Sync level
/// Opens a dataset segment.
///
/// Caller must call `close()` to release resources.
pub fn open(gpa: std.mem.Allocator, frame_allocator: std.mem.Allocator, dir: std.fs.Dir, options: Options) OpenError!*Self {
    var logger = logging.logger("msg_store.DataSegment");

    logger.fine()
        .msg("Opening data segment")
        .int("segment", options.segment_no)
        .log();

    // Prepare file names

    const seg_idx_file_name = try std.fmt.allocPrint(gpa, idx_file_name_fmt, .{options.segment_no});
    defer gpa.free(seg_idx_file_name);
    const index_file_options: IndexFile.FileOptions = .{
        .rel_path = seg_idx_file_name,
        .create = options.initialize,
    };
    var index_file: IndexFile = try IndexFile.open(gpa, frame_allocator, dir, index_file_options, .{
        .segment_no = options.segment_no,
        .timestamp = options.timestamp,
    });
    errdefer index_file.close();

    const seg_msg_file_name = try std.fmt.allocPrint(gpa, msg_file_name_fmt, .{options.segment_no});
    defer gpa.free(seg_msg_file_name);
    const message_file_options: MessageFile.FileOptions = .{
        .create = options.initialize,
        .rel_path = seg_msg_file_name,
    };
    var message_file: MessageFile = try MessageFile.open(gpa, frame_allocator, dir, message_file_options, .{
        .segment_no = options.segment_no,
        .timestamp = options.timestamp,
        .needs_verification = options.needs_verification,
    });
    errdefer message_file.close();

    const new_self = gpa.create(Self) catch return OpenError.OutOfMemory;
    errdefer gpa.destroy(new_self);

    new_self.* = .{
        .logger = logger,
        .gpa = gpa,
        .frame_allocator = frame_allocator,
        .segment_no = options.segment_no,
        .message_file = message_file,
        .index_file = index_file,
        .status = .{
            .opened = true,
            .repair_state = .healthy,
        },
        .sync_level = options.sync_level,
    };

    logger.debug()
        .msg("Data segment opened")
        .int("segment", options.segment_no)
        .str("message_file_name", seg_msg_file_name)
        .str("index_file_name", seg_idx_file_name)
        .log();

    return new_self;
}

/// Closes (de-initializes) a dataset.
pub fn close(self: *Self) void {
    defer self.gpa.destroy(self);
    self.logger.fine()
        .msg("Closing data segment")
        .int("segment", self.segment_no)
        .log();
    self.message_file.close();
    self.index_file.close();
    self.status.opened = false;
    self.logger.fine()
        .msg("Data segment closed")
        .int("segment", self.segment_no)
        .log();
}

/// Queries whether or not the dataset may contain the given ULID.
///
/// False positives are possible, but not false negatives.
pub fn mayContain(self: *const Self, id: Ulid) bool {
    return self.index_file.mayContain(id);
}

const repair_mod = @import("repair.zig");

pub const VerificationError = error{
    OutOfResources,
    Unrecoverable,
};

pub const VerificationDiagnosis = enum {
    Verified,
    IndexFileNeedsRepair,
    MessageFileNeedsRepair,
};

pub fn verify(
    allocator: Allocator,
    frame_allocator: Allocator,
    hot_journal: ?JournalFile.DatasetJournal,
    dataset_dir: std.fs.Dir,
    options: Options,
) VerificationError!VerificationDiagnosis {
    var logger = logging.logger("msg_store.DataSegment.verify");

    const verify_results =
        repair_mod.verifySegment(logger, allocator, frame_allocator, hot_journal, .{
            .dir = dataset_dir,
            .segment_no = options.segment_no,
        }) catch |e| {
            switch (e) {
                error.NonIndexFile, error.IndexFileCorrupted, error.IndexFileTruncated => {
                    logger.warn()
                        .msg("Attempting auto-repair of data segment index file")
                        .int("segment_no", options.segment_no)
                        .log();

                    const idx_repair_opts: DataSegment.Options = .{
                        .initialize = true,
                        .timestamp = 0,
                        .segment_no = options.segment_no,
                    };

                    _ = DataSegment.repairIndexFile(allocator, frame_allocator, dataset_dir, idx_repair_opts) catch |er| {
                        logger.err()
                            .msg("Failed to auto-repair index file from message file")
                            .err(er)
                            .int("segment_no", options.segment_no)
                            .log();
                        return VerificationDiagnosis.IndexFileNeedsRepair;
                    };

                    logger.info()
                        .msg("Data segment index file repaired successfully")
                        .log();

                    return VerificationDiagnosis.Verified;
                },
                error.NonMessageFile, error.MessageFileCorrupted, error.MessageFileTruncated, error.MessageFileTorn => {
                    logger.warn()
                        .msg("Message file needs repair")
                        .err(e)
                        .int("segment_no", options.segment_no)
                        .log();
                    return VerificationDiagnosis.MessageFileNeedsRepair;
                },
                error.ResourcesExhausted => {
                    logger.err()
                        .msg("Resources exhausted")
                        .err(e)
                        .int("segment_no", options.segment_no)
                        .log();
                    return VerificationError.OutOfResources;
                },
                else => {
                    logger.err()
                        .msg("Segment is very unhappy")
                        .err(e)
                        .int("segment_no", options.segment_no)
                        .log();
                    return VerificationError.Unrecoverable;
                },
            }
        };

    if (verify_results) |_results| {
        logger.fine()
            .msg("Last valid ULID in segment")
            .int("segment_no", options.segment_no)
            .ulid("ulid", _results.ulid)
            .int("lsn", _results.lsn)
            .log();
    }

    return VerificationDiagnosis.Verified;
}

pub const IndexRepairError = error{
    OutOfResources,
    Unrecoverable,
};

pub fn repairIndexFile(allocator: Allocator, frame_allocator: Allocator, dataset_dir: std.fs.Dir, options: Options) IndexRepairError!Ulid {
    const seg_msg_file_name = std.fmt.allocPrint(allocator, msg_file_name_fmt, .{options.segment_no}) catch return IndexRepairError.OutOfResources;
    defer allocator.free(seg_msg_file_name);
    const message_file_options: MessageFile.FileOptions = .{
        .rel_path = seg_msg_file_name,
        .create = false,
    };

    var message_file: MessageFile = MessageFile.open(allocator, frame_allocator, dataset_dir, message_file_options, .{
        .segment_no = options.segment_no,
        .timestamp = options.timestamp,
    }) catch return IndexRepairError.Unrecoverable;
    defer message_file.close();

    const seg_idx_file_name = std.fmt.allocPrint(allocator, idx_file_name_fmt, .{options.segment_no}) catch return IndexRepairError.OutOfResources;
    defer allocator.free(seg_idx_file_name);
    const index_file_options: IndexFile.FileOptions = .{
        .rel_path = seg_idx_file_name,
        .create = true,
    };

    var index_file: IndexFile = IndexFile.open(allocator, frame_allocator, dataset_dir, index_file_options, .{
        .segment_no = options.segment_no,
        .timestamp = options.timestamp,
    }) catch return IndexRepairError.Unrecoverable;
    defer index_file.close();

    var msg_it = message_file.iterator() catch |e| return switch (e) {
        error.OutOfMemory, error.LimitReached => IndexRepairError.OutOfResources,
        else => IndexRepairError.Unrecoverable,
    };
    defer msg_it.close();

    var last_ulid: Ulid = .{ .rand = 0, .time = 0 };
    var message_offset: u64 = @sizeOf(MessageFile.Header);
    while (msg_it.next() catch return IndexRepairError.Unrecoverable) |_msg| {
        index_file.store(&_msg, message_offset) catch |e| return switch (e) {
            error.OutOfMemory, error.LimitReached => IndexRepairError.OutOfResources,
            else => IndexRepairError.Unrecoverable,
        };
        message_offset += _msg.usedFrame().len;
        last_ulid = _msg.id();
    }
    return last_ulid;
}

/// Synchronizes pending changes with the filesystem.
pub fn sync(self: *Self) StateError!void {
    if (self.sync_level == .all or self.sync_level == .msg_and_journal) {
        self.message_file.sync() catch |e| {
            self.logger.warn()
                .msg("Data segment failed to sync message file")
                .err(e)
                .int("segment", self.segment_no)
                .log();
            self.status.repair_state = .needs_repair;
            // At least try to sync the index, so _don't_ return here
        };
    }

    if (self.sync_level == .all) {
        self.index_file.sync() catch |e| {
            self.logger.warn()
                .msg("Data segment failed to sync index file")
                .err(e)
                .int("segment", self.segment_no)
                .log();
            self.status.repair_state = .needs_repair;
        };
    }

    if (self.status.repair_state == .needs_repair) {
        return StateError.InconsistentState;
    }
}

/// Queries the dataset segment readiness
pub inline fn ready(self: *const Self) bool {
    return self.status.opened and self.status.repair_state == .healthy;
}

/// Checks if we have capacity to store the given message
pub fn haveCapacity(self: *const Self, message: *const Message) bool {
    return self.message_file.haveCapacity(message) and self.index_file.haveCapacity(message);
}

pub const StoreError = AccessError || CreationError || StateError;
const Message = @import("dymes_msg").Message;

/// Stores the given message in message file and header in index file.
pub fn store(self: *Self, message: *const Message) StoreError!u64 {
    if (!self.ready()) {
        return StoreError.NotReady;
    }
    if (!self.haveCapacity(message)) {
        // Dataset needs to roll over to next segment
        return StoreError.OutOfSpace;
    }
    const message_offset = self.message_file.store(message) catch |e| {
        self.status.repair_state = .needs_repair;
        return e;
    };
    self.index_file.store(message, message_offset) catch |e| {
        self.status.repair_state = .needs_repair;
        return e;
    };

    return message_offset;
}

pub const FetchError = AccessError || AllocationError || StateError;

/// Returns a view of the message with the given id, if present.
///
/// This is a *view* of the message, the caller _does not_ gain ownership.
/// Future calls to `fetch` may invalidate the message view.
pub fn fetch(self: *Self, ulid: Ulid) FetchError!?Message {
    if (try self.index_file.lookup(ulid)) |idx_entry| {
        return self.fetchAt(idx_entry.msg_file_offset);
    }
    return null;
}

/// Returns a view of the message at the given offset, if present.
///
/// This is an alternative to `fetch` when the message offset is already known (in-mem indexed, etc).
///
/// This is a *view* of the message, the caller _does not_ gain ownership.
/// Future calls to `fetch` may invalidate the message view.
pub fn fetchAt(self: *Self, msg_file_offset: u64) FetchError!?Message {
    return self.message_file.fetch(msg_file_offset) catch |e| {
        self.status.repair_state = .needs_repair;
        return e;
    };
}

const DataSegment = @This();

test "DataSegment" {
    std.debug.print("test.DataSegment.smoke\n", .{});
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

    var logger = logging.logger("test.DataSegment");

    // Prepare config

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    try tmp_dir.dir.makeDir("test-data-seg");

    const abs_dir_name = try tmp_dir.dir.realpathAlloc(allocator, "test-data-seg");
    defer allocator.free(abs_dir_name);

    const timestamp = std.time.nanoTimestamp();

    var dataset_dir = try std.fs.openDirAbsolute(abs_dir_name, .{ .iterate = true });
    defer dataset_dir.close();

    // Open the dataset segment
    var data_seg = try DataSegment.open(allocator, frame_allocator, dataset_dir, .{
        .segment_no = 66,
        .initialize = true,
        .timestamp = timestamp,
    });
    defer data_seg.close();

    // Store some messages

    var ulid_generator = common.ulid.generator();

    var map = std.StringArrayHashMap([]const u8).init(allocator);
    defer map.deinit();

    try map.put("0", "zero");
    try map.put("1", "one");
    try map.put("2", "two");
    try map.put("3", "three");
    try map.put("4", "four");

    const message_frame: []u8 = try allocator.alloc(u8, 1024);
    defer allocator.free(message_frame);
    @memset(message_frame, 0x0);
    const message_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

    const older_ulid = try ulid_generator.next();

    const number_of_messages = 100;
    var message_ids: [number_of_messages]common.ulid.Ulid = undefined;
    logger.debug().msg("Storing test messages").int("number_of_messages", number_of_messages).log();
    for (0..number_of_messages) |idx| {
        var test_msg =
            try Message.initOverlay(message_frame, idx, try ulid_generator.next(), idx, 101, message_body, .{ .transient_kv_headers = map });
        _ = try data_seg.store(&test_msg);
        message_ids[idx] = test_msg.frame_header.id;
    }
    logger.debug().msg("Test messages stored").int("number_of_messages", number_of_messages).log();

    // Fetch some messages

    const newer_ulid = try ulid_generator.next();
    try testing.expect(!data_seg.mayContain(older_ulid));
    try testing.expect(!data_seg.mayContain(newer_ulid));

    logger.debug().msg("Fetching test messages").int("number_of_messages", number_of_messages).log();
    for (0..number_of_messages) |idx| {
        const expected_ulid = message_ids[idx];
        try testing.expect(data_seg.mayContain(expected_ulid));
        const msg = try data_seg.fetch(expected_ulid) orelse @panic("Oh dear");
        try testing.expect(msg.frame_header.id.equals(expected_ulid));
    }
    logger.debug().msg("Test messages fetched").int("number_of_messages", number_of_messages).log();

    try data_seg.sync();
}
