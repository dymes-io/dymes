//! Dymes Message Store - Append-only access.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const common = @import("dymes_common");
const logging = common.logging;
const Ulid = common.ulid.Ulid;

// Log level for embedded tests
const module_tests_log_level = logging.LogLevel.none;

const limits = @import("limits.zig");
const constants = @import("constants.zig");

const errors = @import("errors.zig");

const AllocationError = errors.AllocationError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;
const UsageError = errors.UsageError;
const DatasetError = errors.DatasetError;

const repair_mod = @import("repair.zig");

const Health = common.health.Health;
const ComponentHealthProvider = common.health.ComponentHealthProvider;

const dymes_msg = @import("dymes_msg");
const FrameAllocator = dymes_msg.FrameAllocator;

const Dataset = @import("Dataset.zig");
const DatasetScanner = @import("DatasetScanner.zig");

const MessageStoreOptions = @import("MessageStoreOptions.zig");

pub const OpenError = CreationError || AccessError || StateError || UsageError;

pub const MessageLocation = @import("MessageLocation.zig");

const component_name: []const u8 = "msg_store.AppendStore";
const Self = @This();

/// Message store logger
logger: *logging.Logger,

/// Message store memory allocator
allocator: std.mem.Allocator,

/// Dataset
dataset: Dataset,

/// Data directory path
data_path: []const u8,

/// Open state
opened: bool,

/// Health monitor
health: *Health,

/// Opens a message store.
///
/// Caller must call `close()` to release resources.
pub fn open(allocator: std.mem.Allocator, frame_allocator: std.mem.Allocator, options: MessageStoreOptions) OpenError!*Self {
    var logger = logging.logger(component_name);
    logger.fine()
        .msg("Opening append-only message store")
        .str("data_path", options.data_path)
        .log();

    const data_path_copy = allocator.dupe(u8, options.data_path) catch return OpenError.OutOfMemory;
    errdefer allocator.free(data_path_copy);

    const open_timestamp = std.time.nanoTimestamp();

    var dataset = try Dataset.open(allocator, frame_allocator, open_timestamp, .{ .dir = options.data_dir });
    errdefer dataset.close();

    var new_self = allocator.create(Self) catch return OpenError.OutOfMemory;
    _ = &new_self;
    new_self.* = .{
        .logger = logger,
        .allocator = allocator,
        .dataset = dataset,
        .data_path = data_path_copy,
        .opened = true,
        .health = options.health,
    };
    errdefer allocator.destroy(new_self);

    const health_provider = ComponentHealthProvider.init(component_name, new_self, healthProbe);
    try options.health.addProvider(health_provider);

    logger.debug()
        .msg("Append-only message store opened")
        .str("data_path", options.data_path)
        .log();

    return new_self;
}

/// Closes (de-initializes) a message store.
pub fn close(self: *Self) void {
    if (!self.opened) {
        self.logger.fine()
            .msg("Append-only message store already closed")
            .str("data_path", self.data_path)
            .log();
        return;
    }
    defer self.allocator.destroy(self);
    defer self.logger.debug()
        .msg("Append-only message store closed")
        .log();
    self.logger.fine()
        .msg("Closing append-only message store")
        .str("data_path", self.data_path)
        .log();
    defer self.opened = false;
    defer self.allocator.free(self.data_path);
    defer self.health.removeProvider(component_name);
    defer self.dataset.close();
}

pub fn cleanup(logger: *logging.Logger, dir: std.fs.Dir) void {
    Dataset.cleanup(logger, dir);
}

/// Queries the message store readiness.
///
/// This is used as one of the component readiness checks during k8s health probes.
pub fn ready(self: *const Self) bool {
    if (!self.opened) {
        return false;
    }
    return self.dataset.ready();
}

/// Checks if the message store needs repair
pub fn needsRepair(self: *const Self) bool {
    return !self.dataset.ready();
}

pub const StoreError = AccessError || CreationError || DatasetError || StateError;
const Message = @import("dymes_msg").Message;

/// Appends the given message in the dataset.
///
/// If this call fails with `DatasetError.FailedToStore`, then the store is very unhealthy,
/// most likely to the point where even a cold repair won't work, this node should go into full cluster recovery.
pub fn append(self: *Self, message: *const Message) StoreError!MessageLocation {
    const last_dataset_loc = self.dataset.store(message) catch |e| {
        self.logger.err()
            .msg("Failed to store message in dataset")
            .err(e)
            .ulid("message_id", message.frame_header.id)
            .int("message_channel", message.frame_header.channel)
            .str("data_path", self.data_path)
            .log();
        return StoreError.FailedToStore;
    };

    self.dataset.sync() catch |e| {
        self.logger.err()
            .msg("Failed to synchronize message store with filesystem")
            .err(e)
            .ulid("message_id", message.frame_header.id)
            .int("message_channel", message.frame_header.channel)
            .str("data_path", self.data_path)
            .log();
        return StoreError.FailedToStore;
    };

    self.logger.fine()
        .msg("Message stored")
        .ulid("message_id", message.frame_header.id)
        .int("message_channel", message.frame_header.channel)
        .str("data_path", self.data_path)
        .log();

    return last_dataset_loc;
}

fn healthProbe(context: *anyopaque) bool {
    var self: *Self = @ptrCast(@alignCast(context));
    return self.ready();
}

const AppendStore = @This();

test "AppendStore smoke test" {
    std.debug.print("test.AppendStore.smoke\n", .{});
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

    var logger = logging.logger("test.AppendStore");

    // Prepare config

    logger.fine().msg("Preparing message store options").log();
    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    var health_monitor = Health.init(allocator);
    defer health_monitor.deinit();

    var options = try MessageStoreOptions.init(allocator, tmp_dir.dir, "dummy", &health_monitor);
    defer options.deinit(allocator);

    // Open the message store
    var message_store = try AppendStore.open(allocator, frame_allocator, options);
    defer message_store.close();

    // Store some messages

    var ulid_generator = common.ulid.generator();

    var map = std.StringArrayHashMap([]const u8).init(allocator);
    defer map.deinit();

    try map.put("0", "zero");
    try map.put("1", "one");
    try map.put("2", "two");
    try map.put("3", "three");
    try map.put("4", "four");

    const message_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    var test_msg =
        try Message.init(
            allocator,
            0,
            try ulid_generator.next(),
            0,
            101,
            message_body,
            .{ .transient_kv_headers = map },
        );
    defer test_msg.deinit(allocator);

    const number_of_messages = 10;
    logger.debug().msg("Storing test messages").int("number_of_messages", number_of_messages).log();
    for (0..number_of_messages) |idx| {
        test_msg.frame_header.log_sequence = idx;
        test_msg.frame_header.id = try ulid_generator.next();
        test_msg.frame_header.channel = idx;
        _ = try message_store.append(&test_msg);
    }
    logger.debug().msg("Test messages stored").int("number_of_messages", number_of_messages).log();
}

/// Cold repairs the message store, if possible. Returns a state error if the repair fails, true if the
/// store is healthy, false if the store is empty.
pub fn coldRepair(allocator: std.mem.Allocator, options: MessageStoreOptions) StateError!bool {
    var logger = logging.logger("msg_store.MessageStore.coldRepair");

    logger.fine()
        .msg("Checking message store health")
        .str("data_path", options.data_path)
        .log();

    var scanner = DatasetScanner.init(allocator, options.data_dir) catch |e| {
        logger.err()
            .msg("Failed to cold repair message store, unable to scan dataset")
            .err(e)
            .str("data_path", options.data_path)
            .log();
        return StateError.NotReady;
    };
    defer scanner.deinit();

    if (!scanner.mismatched) {
        logger.fine()
            .msg("Dataset appears healthy")
            .str("data_path", options.data_path)
            .log();
        return scanner.results.len != 0;
    }

    return StateError.NotReady;
}

test "AppendStore.coldRepair" {
    std.debug.print("test.AppendStore.coldRepair\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = module_tests_log_level;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    var fah = FrameAllocator.init(allocator);
    const frame_allocator = fah.allocator();

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    var ulid_generator = common.ulid.generator();

    var logger = logging.logger("test.AppendStore.coldRepair");

    // Create store
    logger.fine().msg("Creating store").log();

    var health_monitor = Health.init(allocator);
    defer health_monitor.deinit();

    var options = try MessageStoreOptions.init(allocator, tmp_dir.dir, "dummy", &health_monitor);
    defer options.deinit(allocator);

    {
        // Open the message store
        var message_store = try AppendStore.open(allocator, frame_allocator, options);
        defer message_store.close();

        // Store messages
        var msg_body_buf: [100]u8 = undefined;
        for (0..10) |idx| {
            const ulid = try ulid_generator.next();

            var body = std.fmt.bufPrint(&msg_body_buf, "Test message #{d}", .{idx}) catch return AccessError.AccessFailure;
            _ = &body;

            var msg = try Message.init(allocator, idx, ulid, 702, 0x0, body, .{});
            defer msg.deinit(allocator);

            _ = try message_store.append(&msg);
            logger.debug().msg("Stored message").ulid("id", msg.frame_header.id).int("size", msg.frame_header.frame_size).log();
        }
        logger.debug().msg("Populated store").log();
    }

    // Cold repair GOOD message store
    {
        const repaired = try coldRepair(allocator, options);
        try testing.expectEqual(true, repaired);

        var message_store_repaired = try AppendStore.open(allocator, frame_allocator, options);
        defer message_store_repaired.close();

        try testing.expect(message_store_repaired.ready());
        try testing.expect(!message_store_repaired.needsRepair());
    }

    // "Break" the dataset
    try tmp_dir.dir.rename("dymes_0.msg", "dymes_0.msg.broken");

    // Cold repair BROKEN message store
    {
        try testing.expectError(StateError.NotReady, coldRepair(allocator, options));
    }
}
