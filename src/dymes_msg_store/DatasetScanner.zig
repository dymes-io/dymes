//! Dymes Message Store Dataset Scanner.
//!
//! This scans files in the dataset directory, allowing mismatch queries, etc.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const common = @import("dymes_common");
const logging = common.logging;
const limits = @import("limits.zig");
const constants = @import("constants.zig");

const Dataset = @import("Dataset.zig");
const repair = @import("repair.zig");

const dymes_msg = @import("dymes_msg");

const errors = @import("errors.zig");

const AllocationError = errors.AllocationError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;

const Self = @This();

pub const DatasetScannerError = AllocationError || AccessError || StateError;

pub const DatasetFile = struct {
    pub const FileType = enum {
        message_file,
        index_file,
    };

    segment_no: u64,
    file_type: FileType,
    file_name: []const u8,
};

/// Logger
logger: *logging.Logger,

/// Memory allocator
allocator: std.mem.Allocator,

/// Scan results
results: []DatasetFile,

/// Mismatched flag, set when there's a mismatch between message and index files
mismatched: bool,

/// Initializes the dataset scanner.
pub fn init(allocator: std.mem.Allocator, dir: std.fs.Dir) DatasetScannerError!Self {
    const logger = logging.logger("msg_store.DatasetScanner");

    logger.fine()
        .msg("Scanning dataset files")
        .log();

    var result_list = std.array_list.Managed(DatasetFile).init(allocator);
    defer result_list.deinit();
    errdefer for (result_list.items) |result| {
        allocator.free(result.file_name);
    };

    var num_message_files: usize = 0;
    var num_index_files: usize = 0;
    var last_seg_no: u128 = 0;
    var dir_it = dir.iterate();
    while (dir_it.next() catch return DatasetScannerError.AccessFailure) |_entry| {
        if (_entry.kind != .file) {
            continue; // skip non-file
        } else if (!(std.mem.endsWith(u8, _entry.name, Dataset.idx_file_name_ext) or std.mem.endsWith(u8, _entry.name, Dataset.msg_file_name_ext))) {
            continue; // skip non message or index file
        }

        const idx_underscore = std.mem.indexOf(u8, _entry.name, "_") orelse return DatasetScannerError.EntryNotFound;
        const idx_ext_period = std.mem.indexOfPos(u8, _entry.name, idx_underscore + 1, ".") orelse return DatasetScannerError.EntryNotFound;
        const segment_no = std.fmt.parseInt(u64, _entry.name[idx_underscore + 1 .. idx_ext_period], 10) catch return DatasetScannerError.EntryNotFound;
        const suffix = _entry.name[idx_ext_period + 1 ..];
        const file_type: DatasetFile.FileType = file_type_val: {
            if (std.mem.eql(u8, suffix, Dataset.msg_file_name_ext)) {
                break :file_type_val DatasetFile.FileType.message_file;
            } else if (std.mem.eql(u8, suffix, Dataset.idx_file_name_ext)) {
                break :file_type_val DatasetFile.FileType.index_file;
            } else {
                @panic("Dataset scanner internal error: Unknown file type");
            }
        };
        switch (file_type) {
            .message_file => num_message_files += 1,
            .index_file => num_index_files += 1,
        }
        const file_name = allocator.dupe(u8, _entry.name) catch return DatasetScannerError.OutOfMemory;
        errdefer allocator.free(file_name);
        const result: DatasetFile = .{
            .segment_no = segment_no,
            .file_name = file_name,
            .file_type = file_type,
        };

        if (segment_no != last_seg_no) {
            last_seg_no = segment_no;
        }

        result_list.append(result) catch return AllocationError.OutOfMemory;
    }

    if (num_index_files != num_message_files) {
        logger.warn()
            .msg("Mismatching number of message and index files")
            .int("num_index_files", num_index_files)
            .int("num_message_files", num_message_files)
            .log();
    }

    var results = allocator.alloc(DatasetFile, result_list.items.len) catch return AllocationError.OutOfMemory;
    for (result_list.items, 0..) |_entry, _idx| {
        results[_idx] = _entry;
    }

    // Sort the scan results (simplifies mismatch checking)
    const srlt = comptime struct {
        pub fn inner(_: void, a: DatasetFile, b: DatasetFile) bool {
            if (a.segment_no == b.segment_no and a.file_type == .message_file) {
                return true;
            }
            return a.segment_no < b.segment_no;
        }
    }.inner;
    std.mem.sort(DatasetFile, results, {}, srlt);

    logger.fine()
        .msg("Scanned dataset files")
        .log();
    return .{
        .allocator = allocator,
        .logger = logger,
        .results = results,
        .mismatched = (num_index_files != num_message_files),
    };
}

/// De-initializes the dataset scanner, releasing resources.
pub fn deinit(self: *Self) void {
    defer self.allocator.free(self.results);
    for (self.results) |result| {
        _ = &result;
        self.allocator.free(result.file_name);
    }
}

/// Detects missing files.
///
/// It's the caller's responsibility to free the results slice.
pub fn detectMissing(self: *Self, reference_results: []const DatasetFile) AllocationError![]DatasetFile {
    self.logger.fine()
        .msg("Checking if any files are missing from the dataset")
        .log();

    var missing_lst = std.array_list.Managed(DatasetFile).init(self.allocator);
    defer missing_lst.deinit();

    if (reference_results.len == 0) {
        return missing_lst.toOwnedSlice() catch return AllocationError.OutOfMemory;
    }

    var idx_results: usize = 0;
    for (reference_results, 0..) |ref_dataset_file, idx_ref| {
        // Short-circuit for gap at end
        if (idx_results >= self.results.len) {
            for (reference_results[idx_ref..]) |missing_file| {
                // self.logger.fine()
                //     .msg("Missing file detected")
                //     .obj("missing_file", missing_file)
                //     .log();
                missing_lst.append(missing_file) catch return AllocationError.OutOfMemory;
            }
            break;
        }
        // Check for missing files.
        const expected_seg = ref_dataset_file.segment_no;
        const actual_seg = self.results[idx_results].segment_no;
        if (actual_seg == expected_seg and ref_dataset_file.file_type == self.results[idx_results].file_type) {
            // Matching sequence and type, all good
            idx_results += 1;
        } else {
            // self.logger.fine()
            //     .msg("Missing file detected")
            //     .obj("missing_file", ref_dataset_file.file_name)
            //     .int("idx_ref", idx_ref)
            //     .int("idx_results", idx_results)
            //     .int("expected_seg", expected_seg)
            //     .int("actual_seg", actual_seg)
            //     .log();
            missing_lst.append(ref_dataset_file) catch return AllocationError.OutOfMemory;
        }
    }

    self.logger.fine()
        .msg("Missing files detected")
        .obj("missing_files", missing_lst.items)
        .log();

    return missing_lst.toOwnedSlice() catch return AllocationError.OutOfMemory;
}

test "DatasetScanner" {
    std.debug.print("test.DatasetScanner.smoke\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var logger = logging.logger("test.DatasetScanner.smoke");

    logger.debug().msg("Preparing test files for dataset scanner").log();
    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();
    var dataset_dir = tmp_dir.dir;

    const msg_file_0 = try dataset_dir.createFile("dymes_0.msg", .{ .truncate = false });
    msg_file_0.close();
    const idx_file_0 = try dataset_dir.createFile("dymes_0.idx", .{ .truncate = false });
    idx_file_0.close();
    const msg_file_2 = try dataset_dir.createFile("dymes_2.msg", .{ .truncate = false });
    msg_file_2.close();
    const idx_file_2 = try dataset_dir.createFile("dymes_2.idx", .{ .truncate = false });
    idx_file_2.close();
    const msg_file_3 = try dataset_dir.createFile("dymes_3.msg", .{ .truncate = false });
    msg_file_3.close();
    const idx_file_4 = try dataset_dir.createFile("dymes_4.idx", .{ .truncate = false });
    idx_file_4.close();

    {
        var dataset_scanner = try init(allocator, dataset_dir);
        defer dataset_scanner.deinit();

        const results = dataset_scanner.results;
        logger.debug()
            .msg("Dataset scanner results")
            .obj("results", results)
            .log();

        const expected = [_]DatasetFile{
            .{ .file_type = .message_file, .segment_no = 0, .file_name = "0.msg" },
            .{ .file_type = .index_file, .segment_no = 0, .file_name = "0.idx" },
            .{ .file_type = .message_file, .segment_no = 1, .file_name = "1.msg" },
            .{ .file_type = .index_file, .segment_no = 1, .file_name = "1.idx" },
            .{ .file_type = .message_file, .segment_no = 2, .file_name = "2.msg" },
            .{ .file_type = .index_file, .segment_no = 2, .file_name = "2.idx" },
            .{ .file_type = .message_file, .segment_no = 3, .file_name = "3.msg" }, //6
            .{ .file_type = .index_file, .segment_no = 3, .file_name = "3.idx" },
            .{ .file_type = .message_file, .segment_no = 4, .file_name = "4.msg" },
            .{ .file_type = .index_file, .segment_no = 4, .file_name = "4.idx" },
        };

        const missing_files = try dataset_scanner.detectMissing(&expected);
        defer allocator.free(missing_files);
        logger.debug()
            .msg("Dataset scanner detected missing files")
            .obj("missing", missing_files)
            .log();

        try testing.expectEqualSlices(u8, expected[2].file_name, missing_files[0].file_name);
        try testing.expectEqualSlices(u8, expected[3].file_name, missing_files[1].file_name);
        try testing.expectEqualSlices(u8, expected[7].file_name, missing_files[2].file_name);
        try testing.expectEqualSlices(u8, expected[8].file_name, missing_files[3].file_name);
    }
}
