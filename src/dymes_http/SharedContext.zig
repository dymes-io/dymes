//! Dymes HTTP Server Shared Context.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const debug = std.debug;
const assert = debug.assert;

const httpz = @import("httpz");

const common = @import("dymes_common");
const Health = common.health.Health;

const HttpMetrics = @import("metrics.zig").HttpMetrics;

const ulid = common.ulid;
const Ulid = ulid.Ulid;

const Logger = common.logging.Logger;

const errors = common.errors;
const CreationError = errors.CreationError;
const AllocationError = errors.AllocationError;

const dymes_msg_store = @import("dymes_msg_store");
const dymes_engine = @import("dymes_engine");
const Engine = dymes_engine.Engine;

const QueryCursor = dymes_msg_store.QueryCursor;

const component_name = "http.SharedContext";

const RequestContext = @import("RequestContext.zig");

const Ingester = @import("ingest.zig").Ingester;

const Self = @This();

pub const Options = struct {
    ns_ts_supplier: *const fn () i128 = std.time.nanoTimestamp,
    ms_ts_supplier: *const fn () i64 = std.time.milliTimestamp,
};

allocator: Allocator,
logger: *Logger,
nanoTimestamp: *const fn () i128,
health: *Health,
metrics: *HttpMetrics,
engine: *Engine,
ingester: *Ingester,
prng: std.Random,
stopping_flag: bool = false,

/// Active cursor map
query_cursors: std.AutoHashMap(u64, *QueryCursor),
/// Active cursor map lock
query_cursors_mtx: std.Thread.Mutex = .{},

pub fn init(allocator: Allocator, metrics: *HttpMetrics, health: *Health, engine: *Engine, ingester: *Ingester, options: Options) CreationError!*Self {
    const logger = common.logging.logger(component_name);
    const new_self = allocator.create(Self) catch return AllocationError.OutOfMemory;
    errdefer allocator.destroy(new_self);

    var query_cursors = std.AutoHashMap(u64, *QueryCursor).init(allocator);
    errdefer query_cursors.deinit();

    var prng_fast = std.Random.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        std.posix.getrandom(std.mem.asBytes(&seed)) catch |e| {
            logger.err()
                .msg("Failed to initialize PRNG")
                .err(e)
                .log();
            return CreationError.OtherCreationFailure;
        };
        break :blk seed;
    });

    new_self.* = .{
        .allocator = allocator,
        .logger = logger,
        .metrics = metrics,
        .health = health,
        .engine = engine,
        .ingester = ingester,
        .query_cursors = query_cursors,
        .nanoTimestamp = options.ns_ts_supplier,
        .prng = prng_fast.random(),
    };
    return new_self;
}

pub fn deinit(self: *Self) void {
    defer self.allocator.destroy(self);
    {
        self.query_cursors_mtx.lock();
        defer self.query_cursors_mtx.unlock();
        defer self.query_cursors.deinit();
        var it = self.query_cursors.iterator();
        while (it.next()) |_query_cursor| {
            self.logger.warn()
                .msg("Force-closing query cursor")
                .intx("cursor_id", _query_cursor.key_ptr.*)
                .log();
            _query_cursor.value_ptr.*.close();
        }
    }
}

pub inline fn stopping(self: *const Self) bool {
    return @atomicLoad(bool, &self.stopping_flag, .monotonic);
}

pub fn dispatch(self: *Self, action: httpz.Action(*RequestContext), req: *httpz.Request, res: *httpz.Response) !void {
    if (self.stopping()) {
        self.logger.warn()
            .msg("Refusing HTTP dispatch during shutdown")
            .str("path", req.url.path)
            .str("query", req.url.query)
            .str("method", @tagName(req.method))
            .log();
        return;
    }

    var ctx = try RequestContext.init(req.arena, self);
    defer ctx.deinit();

    // defer self.logger.fine()
    //     .msg("HTTP request dispatched")
    //     .str("path", req.url.path)
    //     .str("query", req.url.query)
    //     .str("method", @tagName(req.method))
    //     .log();

    return action(&ctx, req, res);
}

pub fn acquiredCursor(self: *Self, query_cursor: *QueryCursor) AllocationError!u64 {
    self.query_cursors_mtx.lock();
    defer self.query_cursors_mtx.unlock();

    const cursor_id = self.prng.int(u64);
    assert(!self.query_cursors.contains(cursor_id));

    self.query_cursors.put(cursor_id, query_cursor) catch return AllocationError.OutOfMemory;
    self.logger.fine()
        .msg("Query cursor acquired")
        .intx("cursor_id", cursor_id)
        .intx("*cursor", @as(u64, @intFromPtr(query_cursor)))
        .any("cursor", query_cursor)
        .log();
    return cursor_id;
}

pub fn releaseCursor(self: *Self, cursor_id: u64) void {
    self.query_cursors_mtx.lock();
    defer self.query_cursors_mtx.unlock();

    if (self.query_cursors.fetchRemove(cursor_id)) |_entry| {
        _entry.value.close();
        self.logger.fine()
            .msg("Query cursor released")
            .intx("cursor_id", cursor_id)
            .intx("*cursor", @as(u64, @intFromPtr(_entry.value)))
            .log();
    } else {
        self.logger.warn()
            .msg("Ignored attempt to release unknown cursor")
            .intx("cursor_id", cursor_id)
            .log();
    }
}

pub fn cursor(self: *Self, cursor_id: u64) ?*QueryCursor {
    self.query_cursors_mtx.lock();
    defer self.query_cursors_mtx.unlock();

    return self.query_cursors.get(cursor_id);
}
