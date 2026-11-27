//! Dymes Engine storage subsystem.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const debug = std.debug;
const assert = debug.assert;
const io = std.io;

const common = @import("dymes_common");

const config = common.config;
const Config = common.config.Config;

const errors = common.errors;
const UsageError = errors.UsageError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;
const DatasetError = AppendStore.DatasetError;

const logging = common.logging;
const Logger = logging.Logger;

const msg_store = @import("dymes_msg_store");
const AppendStore = msg_store.AppendStore;
const MessageStoreOptions = msg_store.MessageStoreOptions;
const MessageLocation = msg_store.MessageLocation;

const Message = @import("dymes_msg").Message;

const constants = @import("constants.zig");

const component_name: []const u8 = "engine.StorageSubsystem";
const Self = @This();

allocator: std.mem.Allocator,
logger: *Logger,
append_store: *AppendStore,
dir: std.fs.Dir,

const SubsystemOpenError = AccessError || CreationError || StateError || UsageError;
pub fn init(allocator: std.mem.Allocator, frame_allocator: std.mem.Allocator, store_options: MessageStoreOptions, allow_init: bool) SubsystemOpenError!*Self {
    var logger = logging.logger(component_name);
    logger.fine()
        .msg("Starting storage subsystem")
        .log();

    var message_store = try openAppendStore(logger, allocator, frame_allocator, allow_init, store_options);
    errdefer message_store.close();
    assert(message_store.ready());

    var new_self = allocator.create(Self) catch return CreationError.OutOfMemory;
    _ = &new_self;
    new_self.* = .{
        .allocator = allocator,
        .logger = logger,
        .append_store = message_store,
        .dir = store_options.data_dir,
    };
    errdefer allocator.destroy(new_self);

    logger.debug()
        .msg("Storage subsystem started")
        .log();

    return new_self;
}

pub fn deinit(self: *Self) void {
    self.logger.fine()
        .msg("Stopping storage subsystem")
        .log();
    defer self.allocator.destroy(self);
    // A write that failed (a full volume) can leave bytes past the journal's ends. The journal is what the next start
    // trims them by, as after a crash: without it the store needs manual repair.
    const keep_journal = self.append_store.needsRepair();
    self.append_store.close();
    if (keep_journal) {
        self.logger.warn()
            .msg("Message store stopped after a failed write; its journal is kept for the next start to repair by")
            .log();
    } else {
        AppendStore.cleanup(self.logger, self.dir);
    }
    defer self.logger.debug()
        .msg("Storage subsystem stopped")
        .log();
}

pub fn ready(self: *const Self) bool {
    return self.append_store.ready();
}

pub const StoreError = AppendStore.StoreError;

/// Appends the given message to the store.
///
/// If this call fails, this node should go into full cluster recovery.
pub inline fn append(self: *Self, message: *const Message) StoreError!MessageLocation {
    return try self.append_store.append(message);
}

/// Stores the given message without synchronizing it with the filesystem (see `sync`).
pub inline fn store(self: *Self, message: *const Message) StoreError!MessageLocation {
    return try self.append_store.store(message);
}

/// Synchronizes stored messages with the filesystem.
pub inline fn sync(self: *Self) StoreError!void {
    return try self.append_store.sync();
}

const StoreOpenError = AppendStore.OpenError;
fn openAppendStore(logger: *logging.Logger, allocator: std.mem.Allocator, frame_allocator: std.mem.Allocator, allow_init: bool, store_options: MessageStoreOptions) StoreOpenError!*AppendStore {

    // Repair message store if required
    const store_healthy = AppendStore.coldRepair(allocator, store_options) catch |e| {
        logger.err()
            .msg("Failed to repair message store")
            .err(e)
            .log();
        return e;
    };
    if (!store_healthy) {
        if (!allow_init) {
            logger.err()
                .msg("Message store is uninitialized, start with '--initialize' to allow initialization")
                .log();
            return StoreOpenError.NotReady;
        }
        logger.info()
            .msg("Message store will be initialized")
            .log();
    }

    return AppendStore.open(allocator, frame_allocator, store_options);
}

const testing = std.testing;

test "StorageSubsystem.stop after a failed write keeps the journal" {
    std.debug.print("test.StorageSubsystem.failed_write_stop\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);
    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;
    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    var fah = @import("dymes_msg").FrameAllocator.init(allocator);
    const frame_allocator = fah.allocator();

    var tmp_dir = testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();
    var health_monitor = common.health.Health.init(allocator);
    defer health_monitor.deinit();
    var options = try MessageStoreOptions.init(allocator, tmp_dir.dir, "dummy", &health_monitor);
    defer options.deinit(allocator);

    var ulid_generator = common.ulid.generator();
    const message_frame: []u8 = try allocator.alloc(u8, 1024);
    defer allocator.free(message_frame);

    var msg_eof: u64 = 0;
    {
        var storage = try init(allocator, frame_allocator, options, true);
        for (0..20) |idx| {
            var test_msg = try Message.initOverlay(message_frame, idx, try ulid_generator.next(), idx, 101, "a body long enough that the message file outgrows the index file, as it does in use", .{});
            _ = try storage.store(&test_msg);
        }
        try storage.sync();
        const msg_file = try tmp_dir.dir.openFile("dymes_0.msg", .{ .mode = .read_write });
        defer msg_file.close();
        msg_eof = try msg_file.getEndPos();
        // The volume fills: a message reaches the message file, its index entry does not, and the store stops taking writes
        try msg_file.seekFromEnd(0);
        try msg_file.writeAll(&([_]u8{0xA5} ** 656));
        storage.append_store.dataset.status.repair_state = .needs_repair;
        try testing.expect(!storage.ready());
        storage.deinit();
    }

    // The next start trims the message file to the journal and takes writes again
    var storage = try init(allocator, frame_allocator, options, false);
    {
        const msg_file = try tmp_dir.dir.openFile("dymes_0.msg", .{});
        defer msg_file.close();
        try testing.expectEqual(msg_eof, try msg_file.getEndPos());
    }
    try testing.expect(storage.ready());
    var test_msg = try Message.initOverlay(message_frame, 20, try ulid_generator.next(), 20, 101, "after the repair", .{});
    _ = try storage.store(&test_msg);
    storage.deinit();

    // A store that stopped healthy leaves no journal behind
    try testing.expectError(error.FileNotFound, tmp_dir.dir.access(msg_store.constants.hot_journal_file_name, .{}));
}
