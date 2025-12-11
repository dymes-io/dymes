//! Dymes Channel Query Results Builder.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const debug = std.debug;
const assert = debug.assert;

const common = @import("dymes_common");
const msg_store_limits = @import("limits.zig");

const Config = common.config.Config;

const errors = common.errors;
const AllocationError = errors.AllocationError;
const UsageError = errors.UsageError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;
const IteratorError = CreationError || AccessError || StateError;

const logging = common.logging;
const Logger = logging.Logger;

const Ulid = common.ulid.Ulid;
const isBeforeUlid = common.util.isBeforeUlid;

const dymes_msg = @import("dymes_msg");
const Message = dymes_msg.Message;
const Filter = dymes_msg.Filter;
const Query = dymes_msg.Query;
const QueryRequest = @import("QueryRequest.zig");
const FrameAllocator = dymes_msg.FrameAllocator;

const ChannelResults = @import("ChannelResults.zig");

const ReadDataset = @import("ReadDataset.zig");

const SegmentsUlidIndex = @import("SegmentsUlidIndex.zig");

const component_name: []const u8 = "msg_store.ChannelResultsBuilder";

pub const MessageRef = packed struct {
    msg_off: usize,
    msg_len: usize,
};

const Self = @This();

gpa: std.mem.Allocator,
frame_allocator: std.mem.Allocator,

read_dataset: *ReadDataset,
segments_ulid_idx: *SegmentsUlidIndex,

pub fn init(gpa: std.mem.Allocator, frame_allocator: std.mem.Allocator, read_dataset: *ReadDataset, segments_ulid_idx: *SegmentsUlidIndex) Self {
    return .{
        .gpa = gpa,
        .frame_allocator = frame_allocator,
        .read_dataset = read_dataset,
        .segments_ulid_idx = segments_ulid_idx,
    };
}

pub fn deinit(_: *Self) void {}

pub const BuildError = AllocationError || UsageError;

pub fn buildChannel(
    self: *Self,
    channel_id: u128,
    filters: []Filter,
    range_start: Ulid,
    range_end: Ulid,
) BuildError!*ChannelResults {
    const total_segments = self.read_dataset.num_segments;

    const first_ulid = if (self.segments_ulid_idx.mayContain(range_start)) range_start else self.segments_ulid_idx.entries.items[0].first_id;
    const final_ulid = if (self.segments_ulid_idx.mayContain(range_end)) range_end else self.segments_ulid_idx.entries.items[total_segments - 1].last_id;
    const first_segment: u64 = if (self.segments_ulid_idx.lookup(first_ulid)) |segment_no| segment_no else 0;
    const last_segment = if (self.segments_ulid_idx.lookup(final_ulid)) |segment_no| segment_no else total_segments - 1;
    return try ChannelResults.init(self.gpa, self.frame_allocator, self.read_dataset, channel_id, filters, first_ulid, final_ulid, first_segment, last_segment);
}

const ChannelResultsBuilder = @This();
