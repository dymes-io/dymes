//! Dymes Message Store Data Segments Cache.
//!
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

pub const PutError = common.caching.PutError;

const MessageFile = @import("MessageFile.zig");
const IndexFile = @import("IndexFile.zig");

const DataSegment = @import("DataSegment.zig");
const MessageIndex = @import("MessageIndex.zig");
const ChannelIndex = @import("ChannelIndex.zig");

const component_name = "msg_store.DataSegmentsCache";

pub const DataSegmentEntry = struct {
    data_segment: *DataSegment,
    message_index: *MessageIndex,
    channel_index: *ChannelIndex,
};

const LruCache = common.caching.ManagedLruCache(u64, DataSegmentEntry);

const Self = @This();

/// General Purpose Allocator
gpa: Allocator,

/// Logger
logger: *Logger,

/// Data segment entry LRU cache
dse_cache: *LruCache,

pub fn init(gpa: Allocator) AllocationError!Self {
    var logger = logging.logger(component_name);
    logger.fine()
        .msg("Initializing data segments cache")
        .int("max_active_segments", limits.max_active_read_data_segments)
        .log();

    const dse_cache = gpa.create(LruCache) catch return AllocationError.OutOfMemory;
    errdefer gpa.destroy(dse_cache);
    dse_cache.* = LruCache.init(gpa, limits.max_active_read_data_segments, extractDseKey, releaseDseResources);
    errdefer dse_cache.deinit();

    defer logger.debug()
        .msg("Data segments cache initialized")
        .int("max_active_segments", limits.max_active_read_data_segments)
        .log();
    return .{
        .gpa = gpa,
        .logger = logger,
        .dse_cache = dse_cache,
    };
}

pub fn deinit(self: *Self) void {
    defer self.gpa.destroy(self.dse_cache);
    defer self.dse_cache.deinit();
    self.logger.debug()
        .msg("Data segments cache de-initialized")
        .log();
}

pub fn put(self: *Self, dse: DataSegmentEntry) PutError!void {
    try self.dse_cache.put(dse.data_segment.segment_no, dse);
}

pub fn acquire(self: *Self, segment_no: u64) ?DataSegmentEntry {
    return self.dse_cache.acquire(segment_no);
}

pub fn release(self: *Self, dse: DataSegmentEntry) void {
    return self.dse_cache.release(dse.data_segment.segment_no);
}

fn extractDseKey(dse: *const DataSegmentEntry) u64 {
    return dse.data_segment.segment_no;
}

fn releaseDseResources(dse: *DataSegmentEntry) !void {
    defer dse.channel_index.deinit();
    defer dse.message_index.deinit();
    defer dse.data_segment.close();
}

test "msg_store.DataSegmentsCache.smoke" {}
