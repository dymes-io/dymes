//! Batch message creation request: many messages appended in order, synchronized once
//! (`POST /api/v1/messages/batch`, doc/dymes-batch-append-proposal.md).
//!
// SPDX-FileCopyrightText: Copyright © 2026 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;

const filters = @import("filters.zig");
pub const KvPair = filters.KvPair;

const EncodedUlid = @import("queries.zig").EncodedUlid;

/// Most messages in one batch.
pub const max_messages: usize = 1_000;

/// One message of a batch: a `CreationRequest`, plus an optional `correlation_index`.
pub const Entry = struct {
    /// Correlation ULID of a stored message.
    encoded_correlation_id: ?EncodedUlid = null,

    /// Correlates to an earlier entry of the same batch instead: ids are assigned by the node, so a client can't
    /// give an earlier entry's id. At most one of the two.
    correlation_index: ?usize = null,

    /// Channel identifier
    channel: u128,

    /// Routing data
    routing: u128,

    /// KV entries
    kv_entries: []const KvPair = &.{},

    /// Message body (base64, url-safe)
    encoded_msg_body: []const u8,
};

/// The messages, in the order they are appended.
messages: []const Entry,

/// The response: one id per message, in request order.
pub const Response = struct {
    ids: []const EncodedUlid,
};

/// The response to a storage failure part-way through: the messages appended before it stay.
pub const FailureResponse = struct {
    @"error": []const u8,
    ids: []const EncodedUlid,
};

const BatchCreationRequest = @This();

test "BatchCreationRequest" {
    std.testing.refAllDeclsRecursive(@This());
    const json =
        \\{"messages":[
        \\  {"channel":5,"routing":7,"encoded_msg_body":"aGk"},
        \\  {"channel":5,"routing":8,"kv_entries":[{"key":"k","value":"v"}],"encoded_msg_body":"aG8","correlation_index":0}
        \\]}
    ;
    const parsed = try std.json.parseFromSlice(BatchCreationRequest, testing.allocator, json, .{});
    defer parsed.deinit();
    try testing.expectEqual(@as(usize, 2), parsed.value.messages.len);
    try testing.expectEqual(@as(?usize, null), parsed.value.messages[0].correlation_index);
    try testing.expectEqual(@as(?EncodedUlid, null), parsed.value.messages[0].encoded_correlation_id);
    try testing.expectEqual(@as(?usize, 0), parsed.value.messages[1].correlation_index);
    try testing.expectEqualStrings("v", parsed.value.messages[1].kv_entries[0].value);
}
