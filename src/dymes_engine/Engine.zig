//! Dymes Engine.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const debug = std.debug;
const assert = debug.assert;

const common = @import("dymes_common");
const Ulid = common.ulid.Ulid;
const msg_store = @import("dymes_msg_store");

const constants = @import("constants.zig");

const Config = common.config.Config;

const errors = common.errors;
const AllocationError = errors.AllocationError;
const UsageError = errors.UsageError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;
const TimeoutError = errors.TimeoutError;
const DatasetError = msg_store.errors.DatasetError;

const logging = common.logging;
const Logger = logging.Logger;

const dymes_msg = @import("dymes_msg");
const Message = dymes_msg.Message;
const NIL_CORRELATION_ID = dymes_msg.constants.NIL_CORRELATION_ID;

const Health = common.health.Health;
const ComponentHealthProvider = common.health.ComponentHealthProvider;

const StorageSubsystem = @import("StorageSubsystem.zig");
const QuerySubsystem = @import("QuerySubsystem.zig");

const engine_api = @import("api.zig");
const QueryRequest = engine_api.QueryRequest;
const EagerResults = engine_api.EagerResults;
const AppendRequest = engine_api.AppendRequest;
const QueryCursor = engine_api.QueryCursor;
const CursorTraversalRequest = engine_api.CursorTraversalRequest;
const UlidGenerator = @import("UlidGenerator.zig");

pub const EngineMetrics = @import("metrics.zig").EngineMetrics;

const MessageLocation = engine_api.MessageLocation;

const component_name: []const u8 = "engine.Engine";

const Self = @This();

allocator: std.mem.Allocator,
frame_allocator: std.mem.Allocator,
logger: *Logger,
storage_subsystem: *StorageSubsystem,
query_subsystem: *QuerySubsystem,
mtx_engine: std.Thread.Mutex.Recursive = .init,
ulid_gen: UlidGenerator,
metrics: *EngineMetrics,

const EngineInitError = AccessError || CreationError || UsageError || StateError;

pub fn init(allocator: std.mem.Allocator, frame_allocator: std.mem.Allocator, health: *Health, metrics: *EngineMetrics, engine_config: Config) EngineInitError!*Self {
    const logger = logging.logger(component_name);
    const store_config = try engine_config.asConfig("store") orelse {
        logger.err()
            .msg("Unable to determine store options from configuration")
            .log();
        return StateError.IllegalState;
    };
    var store_options = try buildStoreOptions(logger, allocator, store_config, health);
    defer store_options.deinit(allocator);

    const allow_init: bool = try store_config.asBool("initialize") orelse true;

    logger.info()
        .msg("Starting engine")
        .log();

    var storage_subsystem = try StorageSubsystem.init(allocator, frame_allocator, store_options, allow_init);
    errdefer storage_subsystem.deinit();

    var query_subsystem = try QuerySubsystem.init(allocator, frame_allocator, store_options);
    errdefer query_subsystem.deinit();

    const new_self = allocator.create(Self) catch return EngineInitError.OutOfMemory;
    errdefer allocator.destroy(new_self);

    new_self.* = .{
        .logger = logger,
        .allocator = allocator,
        .frame_allocator = frame_allocator,
        .storage_subsystem = storage_subsystem,
        .query_subsystem = query_subsystem,
        .metrics = metrics,
        .ulid_gen = .{
            .supplyMilliTimestamp = std.time.milliTimestamp,
        },
    };
    errdefer new_self.deinit();

    logger.info()
        .msg("Engine started")
        .log();

    return new_self;
}

pub fn deinit(self: *Self) void {
    self.logger.info()
        .msg("Stopping engine")
        .log();
    defer self.allocator.destroy(self);
    defer self.logger.info()
        .msg("Engine stopped")
        .log();
    defer self.storage_subsystem.deinit();
    defer self.query_subsystem.deinit();
}

pub const AppendError = AccessError || CreationError || DatasetError || StateError || TimeoutError || AllocationError;
pub const AppendOptions = Message.Options;

/// Stores the given message.
///
/// If this call fails, this node should go into full cluster recovery.
pub fn append(self: *Self, msg_channel: u128, msg_routing: u128, msg_body: []const u8, options: AppendOptions) AppendError!Ulid {
    self.mtx_engine.lock();
    defer self.mtx_engine.unlock();

    const msg_id = try self.nextMessageId();

    return try self.import(msg_id, msg_channel, msg_routing, msg_body, options);
}

pub fn import(self: *Self, msg_id: Ulid, msg_channel: u128, msg_routing: u128, msg_body: []const u8, options: Message.Options) AppendError!Ulid {
    self.metrics.engine_appends_received.increment();
    self.mtx_engine.lock();
    defer self.mtx_engine.unlock();

    const final_ulid = self.storage_subsystem.append_store.dataset.hot_journal.dataset_last_ulid;
    if (common.util.isBeforeUlid(msg_id, final_ulid) or msg_id == final_ulid) {
        return AppendError.OutOfOrderCreation;
    }

    var msg = try Message.init(
        self.frame_allocator,
        0,
        msg_id,
        msg_channel,
        msg_routing,
        msg_body,
        options,
    );
    defer msg.deinit(self.frame_allocator);

    self.logger.fine()
        .msg("Appending message")
        .ulid("msg_id", msg.id())
        .log();

    const corr_location: ?MessageLocation = if (options.correlation_id != NIL_CORRELATION_ID) loc_val: {
        if (try self.query_subsystem.immutable_store.lookupLocation(options.correlation_id)) |_corr_loc| {
            break :loc_val _corr_loc;
        } else {
            self.logger.err()
                .msg("Rejected attempt to append message with invalid correlation reference")
                .ulid("msg_id", msg.id())
                .ulid("corr_id", msg.correlationId())
                .log();
            return AppendError.OtherCreationFailure;
        }
    } else null;

    const msg_location = try self.storage_subsystem.append(&msg);
    try self.query_subsystem.appended(msg.channel(), msg_location, corr_location);
    self.metrics.engine_appends_ok.increment();
    return msg.id();
}

pub inline fn nextMessageId(self: *Self) CreationError!Ulid {
    return self.ulid_gen.next() catch |e| {
        self.logger.err()
            .msg("Failed to generate message id (ULID)")
            .err(e)
            .log();
        return CreationError.GenerationFailure;
    };
}

pub const QueryExecutionError = AccessError || CreationError || StateError || UsageError || TimeoutError || AllocationError;
pub fn query(self: *Self, request: QueryRequest) QueryExecutionError!*QueryCursor {
    self.metrics.engine_queries_received.increment();
    self.mtx_engine.lock();
    defer self.mtx_engine.unlock();

    const cursor = try self.query_subsystem.execute(request);
    self.metrics.engine_queries_ok.increment();
    return cursor;
}

pub const CursorTraversalError = AccessError || TimeoutError || AllocationError;
pub fn traverseCursor(self: *Self, traversal_request: CursorTraversalRequest) CursorTraversalError!?Message {
    self.mtx_engine.lock();
    defer self.mtx_engine.unlock();

    return traversal_request.cursor.next() catch |e| {
        self.logger.err()
            .msg("Failed to traverse cursor")
            .err(e)
            .log();
        return AccessError.OtherAccessFailure;
    };
}

const MessageStoreOptions = msg_store.MessageStoreOptions;
const OptionsError = MessageStoreOptions.OptionsError;
const StoreBuildError = AccessError || CreationError || UsageError || OptionsError;
/// Builds engine message store options
pub fn buildStoreOptions(logger: *logging.Logger, allocator: std.mem.Allocator, store_config: Config, health: *Health) StoreBuildError!MessageStoreOptions {
    const data_path: []const u8 = try store_config.asString("data_path") orelse return StoreBuildError.IllegalArgument;

    const engine_store_dir = std.fs.cwd().makeOpenPath(data_path, .{ .iterate = true }) catch |e| {
        logger.err()
            .msg("Failed to create data directory")
            .err(e)
            .str("data_path", data_path)
            .log();
        return switch (e) {
            error.PathAlreadyExists => StoreBuildError.FileAlreadyExists,
            error.NoSpaceLeft => StoreBuildError.OutOfSpace,
            error.SystemFdQuotaExceeded, error.ProcessFdQuotaExceeded => StoreBuildError.OtherCreationFailure,
            else => StoreBuildError.AccessFailure,
        };
    };

    return msg_store.MessageStoreOptions.init(allocator, engine_store_dir, data_path, health) catch |e| {
        logger.err()
            .msg("Failed to prepare message store options")
            .err(e)
            .str("data_path", data_path)
            .log();
        return e;
    };
}
