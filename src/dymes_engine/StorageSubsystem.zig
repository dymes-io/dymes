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
    self.append_store.close();
    AppendStore.cleanup(self.logger, self.dir);
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
