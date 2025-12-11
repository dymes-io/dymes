//! Dymes Message Store Dataset.
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
const FrameAllocator = dymes_msg.FrameAllocator;

const errors = @import("errors.zig");

const AllocationError = errors.AllocationError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;
const SyncError = errors.SyncError;

const MessageFile = @import("MessageFile.zig");
const IndexFile = @import("IndexFile.zig");

const MessageLocation = @import("MessageLocation.zig");

const DatasetScanner = @import("DatasetScanner.zig");
const DataSegment = @import("DataSegment.zig");

const JournalFile = @import("JournalFile.zig");

pub const msg_file_name_ext = MessageFile.msg_file_name_ext;
pub const msg_file_name_fmt = MessageFile.msg_file_name_fmt;
pub const idx_file_name_ext = IndexFile.idx_file_name_ext;
pub const idx_file_name_fmt = IndexFile.idx_file_name_fmt;

pub const SyncLevel = constants.SyncLevel;

/// Message store dataset options
pub const Options = struct {
    /// Data store directory
    dir: std.fs.Dir,

    /// Repairing flag
    repairing: bool = false,

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
logger: *logging.Logger,

/// Memory allocator
allocator: std.mem.Allocator,

/// Message frame allocator
frame_allocator: std.mem.Allocator,

/// Active segment
active_segment: *DataSegment,

/// Segments in use
num_segments: u64,

/// Dataset status
status: Status,

/// Data store directory
dir: std.fs.Dir,

/// Hot journal
hot_journal: JournalFile.DatasetJournal,

/// Hot journal file
journal_file: JournalFile,

/// Sync level
sync_level: SyncLevel,

/// Opens a dataset.
///
/// Caller must call `close()` to release resources.
pub fn open(gpa: std.mem.Allocator, frame_allocator: std.mem.Allocator, timestamp: i128, options: Options) OpenError!Self {
    var logger = logging.logger("msg_store.Dataset");

    logger
        .fine()
        .msg("Opening dataset")
        .log();

    const old_journal = JournalFile.verify(logger, gpa, options.dir, constants.hot_journal_file_name);
    const must_verify_ds = if (old_journal) |_| true else false;

    verifyAndRepair(gpa, frame_allocator, old_journal, options) catch return OpenError.InconsistentState;

    // Scan directory for dataset files
    var scanner = try DatasetScanner.init(gpa, options.dir);
    defer scanner.deinit();
    if (scanner.mismatched) {
        return OpenError.InconsistentState;
    }

    // Determine latest data segment
    var segment_no: u64 = 0;
    var active_segment: *DataSegment = undefined;
    var first_ulid: Ulid = .{ .rand = 0x0, .time = 0x0 };
    var last_ulid: Ulid = .{ .rand = 0x0, .time = 0x0 };

    if (scanner.results.len > 1) {
        // We take advantage of the scanner results being sorted
        assert(scanner.results.len % 2 == 0);
        const msg_file = scanner.results[scanner.results.len - 2];
        const idx_file = scanner.results[scanner.results.len - 1];
        if (msg_file.segment_no != idx_file.segment_no) {
            return OpenError.InconsistentState;
        }
        segment_no = msg_file.segment_no;

        if (!options.repairing) {
            // Open first segment
            var first_segment = try DataSegment.open(gpa, frame_allocator, options.dir, .{
                .initialize = false,
                .segment_no = 0,
                .timestamp = timestamp,
                .needs_verification = must_verify_ds,
            });
            first_ulid = first_segment.index_file.first_ulid;
            first_segment.close();
        }

        // Open latest segment
        active_segment = try DataSegment.open(gpa, frame_allocator, options.dir, .{
            .initialize = false,
            .timestamp = timestamp,
            .segment_no = segment_no,
            .needs_verification = must_verify_ds,
            .sync_level = options.sync_level,
        });
        last_ulid = active_segment.index_file.last_ulid;
    } else {
        // We're a new dataset
        const message_file_name = std.fmt.allocPrint(gpa, msg_file_name_fmt, .{segment_no}) catch return OpenError.OutOfMemory;
        defer gpa.free(message_file_name);
        const index_file_name = std.fmt.allocPrint(gpa, idx_file_name_fmt, .{segment_no}) catch return OpenError.OutOfMemory;
        defer gpa.free(index_file_name);

        // Create new dataset files
        active_segment = try DataSegment.open(gpa, frame_allocator, options.dir, .{
            .initialize = true,
            .timestamp = timestamp,
            .segment_no = segment_no,
            .needs_verification = false,
        });
    }
    errdefer active_segment.close();

    const journal = JournalFile.DatasetJournal.init(
        segment_no,
        active_segment.index_file.last_lsn,
        first_ulid,
        last_ulid,
        active_segment.index_file.first_ulid,
        active_segment.index_file.last_msg_offset,
        active_segment.message_file.append_offset,
        @sizeOf(IndexFile.Header) + active_segment.index_file.append_entry_no * IndexFile.IndexEntry.idx_entry_size,
    );

    const journal_file = JournalFile.create(gpa, options.dir, JournalFile.hot_journal_file_name) catch |_e| {
        logger.warn()
            .msg("Failed to create hot journal file")
            .err(_e)
            .str("rel_path", JournalFile.hot_journal_file_name)
            .log();
        return OpenError.InconsistentState;
    };

    logger
        .debug()
        .msg("Dataset opened")
        .log();
    return .{
        .logger = logger,
        .allocator = gpa,
        .frame_allocator = frame_allocator,
        .num_segments = segment_no + 1,
        .active_segment = active_segment,
        .status = .{
            .opened = true,
            .repair_state = .healthy,
        },
        .hot_journal = journal,
        .journal_file = journal_file,
        .sync_level = options.sync_level,
        .dir = options.dir,
    };
}

/// Closes (de-initializes) a dataset.
pub fn close(self: *Self) void {
    if (!self.status.opened) {
        self.logger
            .fine()
            .msg("Dataset already closed")
            .log();
        return;
    }
    self.logger
        .fine()
        .msg("Closing dataset")
        .log();
    defer self.active_segment.close();
    defer self.journal_file.close();
    defer self.status.opened = false;
    defer self.logger.debug()
        .msg("Dataset closed")
        .log();
}

/// Cleans up after `close()`
pub fn cleanup(logger: *logging.Logger, dir: std.fs.Dir) void {
    dir.deleteFile(JournalFile.hot_journal_file_name) catch |_e| {
        logger.warn()
            .msg("Failed to delete closed journal file")
            .err(_e)
            .str("rel_path", JournalFile.hot_journal_file_name)
            .log();
    };
}

/// Queries whether or not the dataset may contain the given ULID.
///
/// False positives are possible, but not false negatives.
pub fn mayContain(self: *const Self, id: Ulid) bool {
    return self.hot_journal.dataset_first_ulid.equals(id) or self.hot_journal.dataset_last_ulid.equals(id) or
        isBeforeUlid(self.hot_journal.dataset_first_ulid, id) and isBeforeUlid(id, self.hot_journal.dataset_last_ulid);
}

/// Verifies a dataset, auto-repairing whenever possible.
pub fn verifyAndRepair(allocator: std.mem.Allocator, frame_allocator: std.mem.Allocator, hot_journal: ?JournalFile.DatasetJournal, options: Options) OpenError!void {
    var logger = logging.logger("msg_store.Dataset.verify");

    logger.fine()
        .msg("Verifying dataset")
        .log();

    // Scan directory for dataset files
    var scanner = try DatasetScanner.init(allocator, options.dir);
    defer scanner.deinit();
    if (scanner.mismatched) {
        return OpenError.InconsistentState;
    }

    const num_segments: usize = blk_num_segments: {
        var seg_count: usize = 0;
        for (scanner.results) |_rf| {
            if (_rf.file_type == .message_file) {
                seg_count += 1;
            }
        }
        break :blk_num_segments seg_count;
    };

    // Verify segments
    for (0..num_segments) |_segment_no| {
        const diagnosis = DataSegment.verify(allocator, frame_allocator, hot_journal, options.dir, .{
            .initialize = false,
            .segment_no = _segment_no,
            .timestamp = std.time.nanoTimestamp(),
        }) catch |e| {
            logger.err()
                .msg("Failed to verify data segment")
                .err(e)
                .int("segment_no", _segment_no)
                .log();

            return OpenError.NotReady;
        };

        switch (diagnosis) {
            DataSegment.VerificationDiagnosis.Verified => {},
            DataSegment.VerificationDiagnosis.IndexFileNeedsRepair => {
                logger.err()
                    .msg("Data segment requires manual index file repair")
                    .int("segment_no", _segment_no)
                    .log();
                return OpenError.InconsistentState;
            },
            DataSegment.VerificationDiagnosis.MessageFileNeedsRepair => {
                logger.err()
                    .msg("Data segment requires manual message file repair")
                    .int("segment_no", _segment_no)
                    .log();
                return OpenError.InconsistentState;
            },
        }
    }

    logger.debug()
        .msg("Dataset verified")
        .log();
}

/// Synchronizes pending changes with the filesystem.
pub fn sync(self: *Self) StateError!void {
    if (self.sync_level == .all or self.sync_level == .msg_and_journal) {
        self.active_segment.sync() catch |e| {
            self.logger.warn()
                .msg("Dataset failed to sync segment")
                .err(e)
                .log();
            self.status.repair_state = .needs_repair;
            return StateError.InconsistentState;
        };
    }

    if (self.sync_level != .none) {
        self.journal_file.sync() catch |e| {
            self.logger.warn()
                .msg("Dataset failed to sync hot journal")
                .err(e)
                .log();
            self.status.repair_state = .needs_repair;
            return StateError.InconsistentState;
        };
    }
}

/// Queries the dataset readiness
pub inline fn ready(self: *const Self) bool {
    return self.status.opened and self.status.repair_state == .healthy;
}

/// Checks if we have capacity to store the given message
pub fn haveCapacity(self: *const Self, message: *const Message) bool {
    return self.active_segment.haveCapacity(message);
}

pub const StoreError = AccessError || CreationError || StateError;
const Message = @import("dymes_msg").Message;

/// Stores the given message in message file and header in index file.
pub fn store(self: *Self, message: *const Message) StoreError!MessageLocation {
    if (!self.ready()) {
        return StoreError.NotReady;
    }

    // Handle rollover
    if (!self.active_segment.haveCapacity(message)) {
        const rollover_timestamp = std.time.nanoTimestamp();
        // Roll over to next sequence
        self.status.repair_state = .needs_repair;
        const old_segment = self.active_segment.segment_no;
        const new_segment = old_segment + 1;
        self.logger.fine()
            .msg("Rolling over to new data segment")
            .int("old_segment", old_segment)
            .int("new_segment", new_segment)
            .log();
        self.active_segment.close();
        self.active_segment = DataSegment.open(self.allocator, self.frame_allocator, self.dir, .{
            .initialize = true,
            .timestamp = rollover_timestamp,
            .segment_no = new_segment,
            .needs_verification = false,
        }) catch |e| {
            self.logger.err()
                .msg("Failed to roll over to new data segment")
                .err(e)
                .int("old_segment", old_segment)
                .int("new_segment", new_segment)
                .log();
            return StateError.NotReady;
        };
        self.logger.fine()
            .msg("Rolled over to new data segment")
            .int("old_segment", old_segment)
            .int("new_segment", new_segment)
            .log();
        self.status.repair_state = .healthy;
        self.num_segments += 1;
    }

    // Store message
    const offset = self.active_segment.store(message) catch |e| {
        self.status.repair_state = .needs_repair;
        return e;
    };

    // Update journal
    const dataset_first_ulid: Ulid = first_ulid_val: {
        if (@as(u128, @bitCast(self.hot_journal.dataset_first_ulid)) == 0x0) {
            break :first_ulid_val message.frame_header.id;
        } else {
            break :first_ulid_val self.hot_journal.dataset_first_ulid;
        }
    };

    self.hot_journal.update(
        self.active_segment.segment_no,
        message.frame_header.log_sequence,
        dataset_first_ulid,
        message.frame_header.id,
        self.active_segment.index_file.first_ulid,
        offset,
        offset + message.frame_header.frame_size,
        @sizeOf(IndexFile.Header) + self.active_segment.index_file.append_entry_no * IndexFile.IndexEntry.idx_entry_size,
    );

    self.journal_file.write(&self.hot_journal) catch |_e| {
        self.logger.err()
            .msg("Failed to update hot journal")
            .err(_e)
            .log();
        self.status.repair_state = .needs_repair;
        return StateError.InconsistentState;
    };

    return .{
        .id = self.hot_journal.dataset_last_ulid,
        .segment_no = self.active_segment.segment_no,
        .file_offset = offset,
        .frame_size = message.frame_header.frame_size,
    };
}

pub const FetchError = AccessError || AllocationError || StateError;

/// Returns a view of the message with the given id if present.
///
/// This is a *view* of the message, the caller _does not_ gain ownership.
/// Future calls to `fetch` may invalidate the message view.
pub fn fetch(self: *Self, ulid: Ulid) FetchError!?Message {
    return self.active_segment.fetch(ulid) catch |e| {
        self.status.repair_state = .needs_repair;
        return e;
    };
}

const Dataset = @This();

test "Dataset" {
    std.debug.print("test.Dataset.smoke\n", .{});
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

    var logger = logging.logger("test.Dataset");

    // Prepare config

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    const timestamp = std.time.nanoTimestamp();

    // Open the dataset
    var dataset = try Dataset.open(allocator, frame_allocator, timestamp, .{
        .dir = tmp_dir.dir,
    });
    defer dataset.close();
    defer cleanup(logger, tmp_dir.dir);

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
    const message_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

    const older_ulid = try ulid_generator.next();

    const number_of_messages = 100;
    var message_ids: [number_of_messages]common.ulid.Ulid = undefined;
    logger.debug().msg("Storing test messages").int("number_of_messages", number_of_messages).log();
    for (0..number_of_messages) |idx| {
        var test_msg =
            try Message.initOverlay(message_frame, idx, try ulid_generator.next(), idx, 101, message_body, .{ .transient_kv_headers = map });
        _ = try dataset.store(&test_msg);
        message_ids[idx] = test_msg.frame_header.id;
    }
    logger.debug().msg("Test messages stored").int("number_of_messages", number_of_messages).log();

    const newer_ulid = try ulid_generator.next();
    try testing.expect(!dataset.mayContain(older_ulid));
    try testing.expect(!dataset.mayContain(newer_ulid));

    logger.debug().msg("Fetching test messages").int("number_of_messages", number_of_messages).log();
    for (0..number_of_messages) |idx| {
        const expected_ulid = message_ids[idx];
        try testing.expect(dataset.mayContain(expected_ulid));
        const msg = try dataset.fetch(expected_ulid) orelse @panic("Oh dear");
        try testing.expect(msg.frame_header.id.equals(expected_ulid));
    }
    logger.debug().msg("Test messages fetched").int("number_of_messages", number_of_messages).log();

    try dataset.sync();
}

test "Dataset.rollover" {
    std.debug.print("test.Dataset.rollover\n", .{});
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

    var logger = logging.logger("test.Dataset.rollover");

    // Prepare config

    logger.fine().msg("Preparing dataset config").log();
    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    try tmp_dir.dir.makeDir("rolling-dataset");

    const abs_dir_name = try tmp_dir.dir.realpathAlloc(allocator, "rolling-dataset");
    defer allocator.free(abs_dir_name);

    const timestamp = std.time.nanoTimestamp();

    // Open the dataset
    var dataset = try Dataset.open(allocator, frame_allocator, timestamp, .{
        .dir = tmp_dir.dir,
    });
    defer dataset.close();

    // Store some messages

    var ulid_generator = common.ulid.generator();

    var message_frame: []u8 = try allocator.alloc(u8, 1024);
    defer allocator.free(message_frame);
    const message_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut e";

    const number_of_messages = @min(3 * @divFloor(limits.max_message_file_size, 256), limits.max_message_file_entries * 3);
    logger.info().msg("Storing test messages").int("number_of_messages", number_of_messages).log();
    for (0..number_of_messages) |idx| {
        var test_msg =
            try Message.initOverlay(message_frame, idx, try ulid_generator.next(), idx, 101, message_body, .{});
        _ = try dataset.store(&test_msg);
    }
    logger.info().msg("Test messages stored").int("number_of_messages", number_of_messages).log();
    _ = &ulid_generator;
    _ = &message_frame;
    _ = &message_body;
}
