//! Dymes Caching support.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const assert = std.debug.assert;

pub const logging = @import("logging.zig");
const Logger = logging.Logger;
const limits = @import("limits.zig");
const constants = @import("constants.zig");

const errors = @import("errors.zig");

const AllocationError = errors.AllocationError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;
const SyncError = errors.SyncError;

pub const PutError = errors.UsageError || AllocationError;

/// "Managed" LRU Cache
///
/// When evicting, avoids reserved entries and invokes user-supplied resource release function
///
pub fn ManagedLruCache(comptime K: type, comptime V: type) type {
    const component_name = "ManagedLruCache";
    return struct {
        const RcvNode = struct {
            node: std.DoublyLinkedList.Node = .{},
            val: V,
            refs: usize = 0,
        };
        const RcvNodeList = std.DoublyLinkedList;
        const RcvNodeMap = std.hash_map.AutoHashMap(K, *RcvNode);

        const Self = @This();

        /// General Purpose Allocator
        gpa: Allocator,

        /// Logger
        logger: *Logger,

        /// Node map
        node_map: RcvNodeMap,

        /// Node access list, used for LRU eviction
        node_list: RcvNodeList,

        /// Limits
        max_entries: usize,

        /// Cache lock
        mtx_dataset: std.Thread.Mutex.Recursive = .init,

        /// Resource management
        extractKey: *const fn (v: *const V) K,
        releaseResources: *const fn (v: *V) anyerror!void,

        pub fn init(
            gpa: Allocator,
            max_entries: usize,
            extractKey: *const fn (v: *const V) K,
            releaseResources: *const fn (v: *V) anyerror!void,
        ) Self {
            var logger = logging.logger(component_name);
            logger.fine()
                .msg("Initializing cache")
                .int("max_entries", max_entries)
                .log();
            var node_map = RcvNodeMap.init(gpa);
            errdefer node_map.deinit();

            const node_list: RcvNodeList = .{};

            defer logger.fine()
                .msg("Cache initialized")
                .int("max_entries", max_entries)
                .log();
            return .{
                .gpa = gpa,
                .logger = logger,
                .node_map = node_map,
                .node_list = node_list,
                .max_entries = max_entries,
                .releaseResources = releaseResources,
                .extractKey = extractKey,
            };
        }

        pub fn deinit(self: *Self) void {
            self.logger.fine()
                .msg("De-initializing cache")
                .int("num_entries", self.node_map.count())
                .int("max_entries", self.max_entries)
                .log();
            {
                self.mtx_dataset.lock();
                defer self.mtx_dataset.unlock();

                defer self.node_map.deinit();

                while (self.node_list.pop()) |_node| {
                    const rcv_node: *RcvNode = @alignCast(@fieldParentPtr("node", _node));
                    defer self.gpa.destroy(rcv_node);
                    var value = rcv_node.val;
                    defer self.releaseResources(&value) catch {};
                }
            }
            self.logger.fine()
                .msg("Cache de-initialized")
                .log();
        }

        pub fn put(self: *Self, k: K, v: V) PutError!void {
            self.mtx_dataset.lock();
            defer self.mtx_dataset.unlock();

            const gpr = self.node_map.getOrPut(k) catch return AllocationError.OutOfMemory;
            if (gpr.found_existing) {
                return PutError.InvalidRequest;
            }

            if (self.node_map.count() > self.max_entries) {
                if (!self.evictLRU()) {
                    _ = self.node_map.remove(k);
                    return PutError.LimitReached;
                }
            }

            const rcv_node: *RcvNode = self.gpa.create(RcvNode) catch return AllocationError.OutOfMemory;
            errdefer self.gpa.destroy(rcv_node);
            rcv_node.* = .{ .val = v };

            gpr.value_ptr.* = rcv_node;

            self.node_list.append(&rcv_node.node);
        }

        pub fn acquire(self: *Self, k: K) ?V {
            self.mtx_dataset.lock();
            defer self.mtx_dataset.unlock();

            if (self.node_map.get(k)) |_rcvnode| {
                // Move node to tail for LRU
                self.node_list.remove(&_rcvnode.node);
                self.node_list.append(&_rcvnode.node);
                _rcvnode.refs += 1;
                return _rcvnode.val;
            }

            return null;
        }

        pub fn release(self: *Self, k: K) void {
            self.mtx_dataset.lock();
            defer self.mtx_dataset.unlock();

            if (self.node_map.get(k)) |_rcvnode| {
                assert(_rcvnode.refs > 0);
                _rcvnode.refs -= 1;
            }
        }

        fn evictLRU(self: *Self) bool {
            self.mtx_dataset.lock();
            defer self.mtx_dataset.unlock();

            var it: ?*std.DoublyLinkedList.Node = self.node_list.first;
            while (it) |_node| : (it = _node.next) {
                const rcv_node: *RcvNode = @alignCast(@fieldParentPtr("node", _node));
                if (rcv_node.refs == 0) {
                    defer self.gpa.destroy(rcv_node);
                    var v = rcv_node.val;
                    const k = self.extractKey(&v);
                    _ = self.node_map.remove(k);
                    self.node_list.remove(_node);
                    self.releaseResources(&v) catch |e| {
                        self.logger.warn()
                            .msg("Failed to release cache resources")
                            .any("key", k)
                            .err(e)
                            .log();
                    };
                    return true;
                }
            }
            self.logger.warn()
                .msg("No cache entry could be evicted")
                .log();
            return false;
        }
    };
}

const RatsNest = struct {
    nest_no: usize,
    rat_resources: *bool,
};

fn dummyCleanupRatsNest(nest: *RatsNest) !void {
    nest.rat_resources.* = false;
}

fn dummyExtractKey(nest: *const RatsNest) usize {
    return nest.nest_no;
}

test "ManagedLruCache.smoke" {
    std.debug.print("test.ManagedLruCache.smoke\n", .{});
    const allocator = testing.allocator;

    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var logger = logging.logger("test.ManagedLruCache.smoke");

    logger.debug()
        .msg("Preparing test cache")
        .log();

    var nest_resources: [6]bool = .{
        true,
        true,
        true,
        true,
        true,
        true,
    };

    const nests: [6]RatsNest = .{
        .{ .nest_no = 0, .rat_resources = &nest_resources[0] },
        .{ .nest_no = 1, .rat_resources = &nest_resources[1] },
        .{ .nest_no = 2, .rat_resources = &nest_resources[2] },
        .{ .nest_no = 3, .rat_resources = &nest_resources[3] },
        .{ .nest_no = 4, .rat_resources = &nest_resources[4] },
        .{ .nest_no = 5, .rat_resources = &nest_resources[5] },
    };

    var cache = ManagedLruCache(usize, RatsNest).init(allocator, 3, dummyExtractKey, dummyCleanupRatsNest);
    defer cache.deinit();

    try cache.put(0, nests[0]);
    try cache.put(1, nests[1]);
    try cache.put(2, nests[2]);
    _ = cache.acquire(0);
    try cache.put(3, nests[3]);
    _ = cache.release(0);

    try testing.expect(nests[0].rat_resources.*);
    try testing.expect(!nests[1].rat_resources.*);
    try testing.expect(nests[2].rat_resources.*);

    try cache.put(4, nests[4]);

    try testing.expect(nests[0].rat_resources.*);
    try testing.expect(!nests[1].rat_resources.*);
    try testing.expect(!nests[2].rat_resources.*);

    try cache.put(5, nests[5]);

    try testing.expect(!nests[0].rat_resources.*);
    try testing.expect(!nests[1].rat_resources.*);
    try testing.expect(!nests[2].rat_resources.*);

    try testing.expect(!nests[0].rat_resources.*);
    try testing.expect(!nests[1].rat_resources.*);
    try testing.expect(!nests[2].rat_resources.*);
    try testing.expect(nests[3].rat_resources.*);
    try testing.expect(nests[4].rat_resources.*);
    try testing.expect(nests[5].rat_resources.*);

    _ = cache.acquire(3);
    defer cache.release(3);
    _ = cache.acquire(4);
    defer cache.release(4);
    _ = cache.acquire(5);
    defer cache.release(5);

    var sadly_not: bool = true;
    try testing.expectError(PutError.LimitReached, cache.put(6, .{ .nest_no = 6, .rat_resources = &sadly_not }));
}
