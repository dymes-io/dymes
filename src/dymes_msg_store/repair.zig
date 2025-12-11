//! Utility functions for performing repairs to datasets.
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

const errors = @import("errors.zig");

const AllocationError = errors.AllocationError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;
const SyncError = errors.SyncError;

const constants = @import("constants.zig");

const dymes_msg = @import("dymes_msg");
const msg_limits = dymes_msg.limits;
const FrameHeader = dymes_msg.Message.FrameHeader;
const Message = dymes_msg.Message;
pub const IndexEntry = @import("IndexEntry.zig");

const msg_store_headers = @import("headers.zig");

const IndexFileHeader = msg_store_headers.IndexFileHeader;
const MessageFileHeader = msg_store_headers.MessageFileHeader;

const Dataset = @import("Dataset.zig");

const JournalFile = @import("JournalFile.zig");

pub const broken_file_ext: []const u8 = ".broken";

pub const SegmentConsistencyFailure = error{
    ResourcesExhausted,
    MissingIndexFile,
    MissingMessageFile,
    NonIndexFile,
    IndexFileTruncated,
    IndexFileCorrupted,
    NonMessageFile,
    MessageFileTruncated,
    MessageFileCorrupted,
    MessageFileTorn,
};

/// Data segment options
pub const SegmentOptions = struct {
    /// Datastore directory
    dir: std.fs.Dir,

    /// Segment number
    segment_no: u64,
};

pub const VerifyResults = struct {
    /// Final ULID
    ulid: Ulid,

    /// Final log sequence number
    lsn: u64,
};

/// Performs a consistency check on a datastore (message file + index file combo).
/// Returns the Ulid of the last valid message in the store, or a consistency failure.
pub fn verifySegment(logger: *Logger, allocator: Allocator, frame_allocator: Allocator, hot_journal: ?JournalFile.DatasetJournal, options: SegmentOptions) SegmentConsistencyFailure!?VerifyResults {
    logger.fine()
        .msg("Verifying integrity of data segment")
        .int("segment", options.segment_no)
        .log();

    const seg_idx_file_name = std.fmt.allocPrint(allocator, constants.idx_file_name_fmt, .{options.segment_no}) catch return SegmentConsistencyFailure.ResourcesExhausted;
    defer allocator.free(seg_idx_file_name);

    const open_flags: std.fs.File.OpenFlags = .{
        .lock = .shared,
        .mode = .read_only,
    };

    const idx_file = options.dir.openFile(seg_idx_file_name, open_flags) catch |e| {
        logger.err()
            .msg("Failed to open index file")
            .err(e)
            .int("segment", options.segment_no)
            .str("idx_path", seg_idx_file_name)
            .log();
        return SegmentConsistencyFailure.MissingIndexFile;
    };
    defer idx_file.close();

    const seg_msg_file_name = std.fmt.allocPrint(allocator, constants.msg_file_name_fmt, .{options.segment_no}) catch return SegmentConsistencyFailure.ResourcesExhausted;
    defer allocator.free(seg_msg_file_name);

    const msg_file = options.dir.openFile(seg_msg_file_name, open_flags) catch |e| {
        logger.err()
            .msg("Failed to open message file")
            .err(e)
            .int("segment", options.segment_no)
            .str("msg_path", seg_msg_file_name)
            .log();
        return SegmentConsistencyFailure.MissingIndexFile;
    };
    defer msg_file.close();

    const verify_results = try verifySegmentConsistency(logger, frame_allocator, hot_journal, options.segment_no, idx_file, msg_file);

    if (verify_results) |_verified| {
        logger.debug()
            .msg("Verified integrity of data segment")
            .int("segment", options.segment_no)
            .str("index_path", seg_idx_file_name)
            .str("msg_path", seg_msg_file_name)
            .ulid("final_ulid", _verified.ulid)
            .int("final_lsn", _verified.lsn)
            .log();
    }

    return verify_results;
}

/// Performs a consistency check on a datastore (message file + index file combo).
/// Returns the Ulid of the last valid message in the store, or a consistency failure.
pub fn verifySegmentConsistency(
    logger: *Logger,
    frame_allocator: Allocator,
    hot_journal: ?JournalFile.DatasetJournal,
    segment_no: u64,
    idx_file: std.fs.File,
    msg_file: std.fs.File,
) SegmentConsistencyFailure!?VerifyResults {
    const last_msg_off = try verifySegmentIndexFile(logger, frame_allocator, hot_journal, segment_no, idx_file);

    return try verifySegmentMessageFile(logger, frame_allocator, hot_journal, segment_no, msg_file, last_msg_off);
}

/// Performs a consistency check on a datastore's index file.
/// Returns the offset of the last valid message in the store, or a consistency failure.
pub fn verifySegmentIndexFile(
    logger: *Logger,
    frame_allocator: Allocator,
    hot_journal: ?JournalFile.DatasetJournal,
    segment_no: u64,
    idx_file: std.fs.File,
) SegmentConsistencyFailure!?u64 {
    const header_buffer: []u8 = frame_allocator.alloc(u8, @sizeOf(IndexFileHeader)) catch return SegmentConsistencyFailure.ResourcesExhausted;
    defer frame_allocator.free(header_buffer);

    const read_buffer: []u8 = frame_allocator.alloc(u8, 4096) catch return SegmentConsistencyFailure.ResourcesExhausted;
    defer frame_allocator.free(read_buffer);

    // Check file bounds
    const eof_off = idx_file.getEndPos() catch |e| {
        logger.err()
            .msg("Failed to determine future index entry position")
            .err(e)
            .log();
        return SegmentConsistencyFailure.IndexFileCorrupted;
    };

    const adjusted_eof: u64 = adjusted_eof_val: {
        if (hot_journal) |_journal| {
            if (_journal.latest_segment_no == segment_no) {
                if (_journal.latest_idx_file_eof > eof_off) {
                    return SegmentConsistencyFailure.IndexFileTruncated;
                }
                break :adjusted_eof_val _journal.latest_msg_file_eof;
            }
        }
        break :adjusted_eof_val eof_off;
    };

    if (adjusted_eof < eof_off) {
        logger.warn()
            .msg("Truncating torn index file")
            .int("segment_no", segment_no)
            .int("expected_eof", adjusted_eof)
            .int("actual_eof", eof_off)
            .log();
        idx_file.setEndPos(adjusted_eof) catch |_e| {
            logger.err()
                .msg("Failed to truncate torn index file")
                .err(_e)
                .int("segment_no", segment_no)
                .log();
            return SegmentConsistencyFailure.IndexFileCorrupted;
        };
    }

    if (adjusted_eof >= @sizeOf(IndexFileHeader)) {
        if (adjusted_eof == @sizeOf(IndexFileHeader)) {
            logger.fine()
                .msg("Index file has no entries yet")
                .log();
            return null; // No mesages yet
        }
        if (adjusted_eof - @sizeOf(IndexFileHeader) < IndexEntry.idx_entry_size) {
            logger.err()
                .msg("Last index entry truncated")
                .int("eof_pos", adjusted_eof)
                .int("header_size", @sizeOf(IndexFileHeader))
                .int("idx_entry_size", IndexEntry.idx_entry_size)
                .log();
            return SegmentConsistencyFailure.IndexFileTruncated;
        }
    }

    // Check index file header
    var idx_reader = idx_file.reader(read_buffer);
    var idx_reader_itf = &idx_reader.interface;

    idx_reader.seekTo(0) catch |e| {
        logger.err()
            .msg("Failed to seek to start of index file")
            .err(e)
            .log();
        return SegmentConsistencyFailure.NonIndexFile;
    };

    _ = idx_reader_itf.readSliceAll(header_buffer) catch |e| {
        logger.err()
            .msg("Incomplete index file header")
            .err(e)
            .log();
        return SegmentConsistencyFailure.NonIndexFile;
    };

    const off_magic = @offsetOf(IndexFileHeader, "magic");
    if (!std.mem.eql(u8, &constants.index_file_header_magic, header_buffer[off_magic .. off_magic + constants.index_file_header_magic.len])) {
        logger.err()
            .msg("Index file header magic mismatch")
            .log();
        return SegmentConsistencyFailure.NonIndexFile;
    }

    // Check last message index entry
    idx_reader.seekTo(adjusted_eof - IndexEntry.idx_entry_size) catch |e| {
        logger.err()
            .msg("Failed to seek to end of index entries")
            .err(e)
            .log();
        return SegmentConsistencyFailure.IndexFileCorrupted;
    };

    const last_idx_entry_pos = idx_reader.logicalPos();
    if (last_idx_entry_pos < @sizeOf(IndexFileHeader)) {
        logger.err()
            .msg("Last index entry truncated")
            .log();
        return SegmentConsistencyFailure.IndexFileCorrupted;
    }

    const entry_buffer: []u8 = frame_allocator.alloc(u8, IndexEntry.idx_entry_size) catch return SegmentConsistencyFailure.ResourcesExhausted;
    defer frame_allocator.free(entry_buffer);

    _ = idx_reader_itf.readSliceAll(entry_buffer) catch |e| {
        logger.err()
            .msg("Failed to read last index entry")
            .err(e)
            .log();
        return SegmentConsistencyFailure.IndexFileCorrupted;
    };

    const idx_entry = IndexEntry.overlay(entry_buffer);
    if (!idx_entry.verifyChecksum()) {
        logger.err()
            .msg("Checksum mismatch in last index entry")
            .log();
        return SegmentConsistencyFailure.IndexFileCorrupted;
    }

    return idx_entry.msg_file_offset;
}

/// Performs a consistency check on a datastore's message file.
/// Returns the Ulid of the last valid message in the store, or a consistency failure.
pub fn verifySegmentMessageFile(
    logger: *Logger,
    frame_allocator: Allocator,
    hot_journal: ?JournalFile.DatasetJournal,
    segment_no: u64,
    msg_file: std.fs.File,
    last_msg_off: ?u64,
) SegmentConsistencyFailure!?VerifyResults {
    const read_buffer: []u8 = frame_allocator.alloc(u8, msg_limits.max_frame_size) catch return SegmentConsistencyFailure.ResourcesExhausted;
    defer frame_allocator.free(read_buffer);

    // Check message file header
    var msg_reader = msg_file.reader(read_buffer);
    var msg_reader_itf = &msg_reader.interface;

    msg_reader.seekTo(0) catch |e| {
        logger.err()
            .msg("Failed to seek to start of message file")
            .int("segment_no", segment_no)
            .err(e)
            .log();
        return SegmentConsistencyFailure.NonMessageFile;
    };

    _ = msg_reader_itf.readSliceAll(read_buffer[0..@sizeOf(MessageFileHeader)]) catch |e| {
        logger.err()
            .msg("Incomplete message file header")
            .int("segment_no", segment_no)
            .err(e)
            .log();
        return SegmentConsistencyFailure.NonMessageFile;
    };

    const off_magic = @offsetOf(MessageFileHeader, "magic");
    if (!std.mem.eql(u8, &constants.message_file_header_magic, read_buffer[off_magic .. off_magic + constants.message_file_header_magic.len])) {
        logger.err()
            .msg("Message file header magic mismatch")
            .int("segment_no", segment_no)
            .log();
        return SegmentConsistencyFailure.NonMessageFile;
    }

    const eof_off = msg_file.getEndPos() catch |e| {
        logger.err()
            .msg("Failed to determine end of message file")
            .err(e)
            .int("segment_no", segment_no)
            .log();
        return SegmentConsistencyFailure.MessageFileCorrupted;
    };

    const adjusted_eof: u64 = adjusted_eof_val: {
        if (hot_journal) |_journal| {
            if (_journal.latest_segment_no == segment_no) {
                if (_journal.latest_msg_file_eof > eof_off) {
                    return SegmentConsistencyFailure.MessageFileTruncated;
                }
                break :adjusted_eof_val _journal.latest_msg_file_eof;
            }
        }
        break :adjusted_eof_val eof_off;
    };

    if (adjusted_eof < eof_off) {
        logger.warn()
            .msg("Truncating torn message file")
            .int("segment_no", segment_no)
            .int("expected_eof", adjusted_eof)
            .int("actual_eof", eof_off)
            .log();
        msg_file.setEndPos(adjusted_eof) catch |_e| {
            logger.err()
                .msg("Failed to truncate torn message file")
                .err(_e)
                .int("segment_no", segment_no)
                .log();
            return SegmentConsistencyFailure.MessageFileTorn;
        };
    }

    const adjusted_last_msg_off: ?u64 = adjusted_last_msg_off: {
        if (hot_journal) |_journal| {
            if (_journal.latest_segment_no == segment_no) {
                break :adjusted_last_msg_off _journal.latest_msg_offset;
            }
        } else if (last_msg_off) |_last_msg_off| {
            break :adjusted_last_msg_off _last_msg_off;
        }
        break :adjusted_last_msg_off null;
    };

    if (adjusted_last_msg_off) |_adjusted_last_msg_off| {
        if (_adjusted_last_msg_off >= adjusted_eof) {
            logger.err()
                .msg("Index refers to offset beyond end of message file")
                .int("segment_no", segment_no)
                .int("msg_offset", _adjusted_last_msg_off)
                .int("eof_offset", adjusted_eof)
                .log();
            return SegmentConsistencyFailure.MessageFileTruncated;
        }

        // Check last message offset
        msg_reader.seekTo(_adjusted_last_msg_off) catch |e| {
            logger.err()
                .msg("Failed to seek to last message")
                .err(e)
                .int("msg_offset", _adjusted_last_msg_off)
                .log();
            return SegmentConsistencyFailure.MessageFileCorrupted;
        };

        _ = msg_reader_itf.readSliceAll(read_buffer[0..@sizeOf(Message.FrameHeader)]) catch |e| {
            logger.err()
                .msg("Read partial message header")
                .err(e)
                .int("segment_no", segment_no)
                .int("msg_offset", _adjusted_last_msg_off)
                .log();
            return SegmentConsistencyFailure.MessageFileTruncated;
        };

        const msg_hdr: *Message.FrameHeader = @ptrCast(@alignCast(read_buffer[0..@sizeOf(Message.FrameHeader)]));

        const remaining: usize = msg_hdr.frame_size - @sizeOf(Message.FrameHeader);
        if (remaining > 0) {
            _ = msg_reader_itf.readSliceAll(read_buffer[@sizeOf(Message.FrameHeader)..msg_hdr.frame_size]) catch |e| {
                logger.err()
                    .msg("Read partial message")
                    .err(e)
                    .int("segment_no", segment_no)
                    .int("msg_offset", _adjusted_last_msg_off)
                    .log();
                return SegmentConsistencyFailure.MessageFileTruncated;
            };
        }

        if (!msg_hdr.verifyChecksum()) {
            logger.err()
                .msg("Message header failed checksum validation")
                .int("segment_no", segment_no)
                .int("msg_offset", _adjusted_last_msg_off)
                .ulid("ulid", msg_hdr.id)
                .log();
            return SegmentConsistencyFailure.MessageFileCorrupted;
        }

        const message = Message.overlay(read_buffer[0..msg_hdr.frame_size]);
        message.verifyChecksums() catch |e| {
            logger.err()
                .msg("Message failed checksum validation")
                .err(e)
                .int("segment_no", segment_no)
                .int("msg_offset", _adjusted_last_msg_off)
                .ulid("ulid", message.frame_header.id)
                .log();
            return SegmentConsistencyFailure.MessageFileCorrupted;
        };

        if (adjusted_eof > _adjusted_last_msg_off + message.usedFrame().len) {
            logger.err()
                .msg("Unindexed message data present")
                .int("segment_no", segment_no)
                .log();
            return SegmentConsistencyFailure.MessageFileTorn;
        }

        const final_ulid_in_segment = message.id();

        if (hot_journal) |_journal| {
            if (_journal.latest_segment_no == segment_no) {
                if (final_ulid_in_segment != _journal.dataset_last_ulid) {
                    logger.err()
                        .msg("Inconsistent final ULID")
                        .int("segment_no", segment_no)
                        .ulid("expected_ulid", _journal.dataset_last_ulid)
                        .ulid("actual_ulid", final_ulid_in_segment)
                        .log();
                    return SegmentConsistencyFailure.MessageFileCorrupted;
                } else if (_journal.log_sequence_no != message.frame_header.log_sequence) {
                    logger.err()
                        .msg("Inconsistent log sequence number")
                        .int("segment_no", segment_no)
                        .int("expected_lsn", _journal.log_sequence_no)
                        .int("actual_lsn", message.frame_header.log_sequence)
                        .log();
                    return SegmentConsistencyFailure.MessageFileCorrupted;
                }
            }
        }

        return .{ .ulid = final_ulid_in_segment, .lsn = message.frame_header.log_sequence };
    }

    return null;
}
