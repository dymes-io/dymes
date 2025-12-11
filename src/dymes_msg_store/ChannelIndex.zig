//! Dymes Message Store In-Memory Channel Index.
//!
//! Each channel index is specific to a particular store and segment
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
const FrameHeader = dymes_msg.Message.FrameHeader;
const Message = dymes_msg.Message;

const MessageLocation = @import("MessageLocation.zig");

/// In-memory channel run entry
pub const ChannelRunEntry = extern struct {
    /// Offset in message in message file
    file_offset: u64,

    /// The size of the message frame: frame header + payload (KV headers, KV data + message body) + padding
    frame_size: u32,

    /// Reserved for future use
    reserved: u32,
};

comptime {
    assert(@sizeOf(ChannelRunEntry) == 16);
    assert(common.util.checkNoPadding(ChannelRunEntry));
}

const ChannelRun = std.array_list.Managed(ChannelRunEntry);

/// Initial channel run capacity, yielding ≈ 4KiB
pub const channel_run_init_capacity: usize = 256;

const ChannelRunMap = std.array_hash_map.AutoArrayHashMap(u128, ChannelRun);

const Self = @This();

/// Logger
logger: *logging.Logger,

/// General Purpose Allocator
gpa: std.mem.Allocator,

/// Channel runs
run_map: ChannelRunMap,

///
/// Allocates and initializes the in-memory channel index.
///
/// Caller must call `deinit()` to release resources.
pub fn init(gpa: std.mem.Allocator) AllocationError!*Self {
    const logger = logging.logger("msg_store.ChannelIndex");

    const new_self = gpa.create(Self) catch return AllocationError.OutOfMemory;
    errdefer gpa.destroy(new_self);

    const run_map = ChannelRunMap.init(gpa);
    errdefer run_map.deinit();

    new_self.* = .{
        .logger = logger,
        .gpa = gpa,
        .run_map = run_map,
    };

    return new_self;
}

/// De-initializes the in-memory channel index.
pub fn deinit(self: *Self) void {
    defer self.gpa.destroy(self);
    defer self.run_map.deinit();
    var rmi = self.run_map.iterator();
    while (rmi.next()) |*_rme| {
        _rme.value_ptr.deinit();
    }
}

/// Appends an entry to the in-memory channel index.
pub fn store(self: *Self, msg_loc: MessageLocation, channel_id: u128) CreationError!void {
    const gpr = self.run_map.getOrPut(channel_id) catch |e| {
        self.logger.err()
            .msg("Failed to append in-memory channel index run")
            .err(e)
            .log();
        return CreationError.OutOfMemory;
    };

    if (!gpr.found_existing) {
        // Initialize new channel run
        const new_run = ChannelRun.init(self.gpa);
        errdefer new_run.deinit();

        gpr.value_ptr.* = new_run;
    }

    gpr.value_ptr.append(.{
        .file_offset = msg_loc.file_offset,
        .frame_size = msg_loc.frame_size,
        .reserved = 0x0,
    }) catch |e| {
        self.logger.err()
            .msg("Failed to append in-memory channel index run entry")
            .err(e)
            .log();
        return CreationError.OutOfMemory;
    };
}

/// Looks up the index entry for the given `ulid`.
///
/// Performs a binary search of the in-memory index buffer.
pub fn lookup(self: *Self, channel_id: u128) ?[]const ChannelRunEntry {
    if (self.run_map.get(channel_id)) |_cr| {
        return _cr.items;
    }
    return null;
}

test "ChannelIndex" {
    std.debug.print("test.ChannelIndex.smoke\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    var ulid_generator = common.ulid.generator();
    _ = try ulid_generator.next();

    const ancient_ulid = try ulid_generator.next();

    var logger = logging.logger("test.ChannelIndex.smoke");

    logger.debug()
        .msg("Size of channel run entry")
        .int("expected", 16)
        .int("actual", @sizeOf(ChannelRunEntry))
        .log();

    // Create in-memory channel index
    logger.fine().msg("Creating in-memory channel index").log();
    var in_mem = try init(allocator);
    defer in_mem.deinit();
    logger.debug().msg("In-memory channel index created").log();

    for (1..1000) |_idx_channel| {
        for (0.._idx_channel) |_idx_run| {
            const dummy_loc: MessageLocation = .{
                .id = ancient_ulid,
                .file_offset = _idx_run,
                .frame_size = 0,
                .segment_no = _idx_channel,
            };

            try in_mem.store(dummy_loc, _idx_channel);
        }
    }

    for (1..1000) |_idx_channel| {
        const run_entries = in_mem.lookup(_idx_channel);
        if (run_entries) |_run_entry| {
            try testing.expectEqual(_idx_channel, _run_entry.len);
        }
    }
}
