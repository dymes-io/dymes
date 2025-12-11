//! Dymes Engine API.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const debug = std.debug;
const assert = debug.assert;

const dymes_msg = @import("dymes_msg");
pub const Filter = dymes_msg.Filter;
const dymes_msg_store = @import("dymes_msg_store");
pub const Message = dymes_msg.Message;
pub const FrameAllocator = dymes_msg.FrameAllocator;
pub const Query = dymes_msg_store.Query;
pub const QueryDto = dymes_msg_store.QueryDto;
pub const EmptyResult = dymes_msg_store.EmptyResult;
pub const SingleResult = dymes_msg_store.SingleResult;
pub const SingleResultBuilder = dymes_msg_store.SingleResultBuilder;
pub const EagerResults = dymes_msg_store.EagerResults;
pub const EagerResultsBuilder = dymes_msg_store.EagerResultsBuilder;
pub const RangeResults = dymes_msg_store.RangeResults;
pub const RangeResultsBuilder = dymes_msg_store.RangeResultsBuilder;
pub const CorrelationResults = dymes_msg_store.CorrelationResults;
pub const CorrelationResultsBuilder = dymes_msg_store.CorrelationResultsBuilder;
pub const ChannelResults = dymes_msg_store.ChannelResults;
pub const ChannelResultsBuilder = dymes_msg_store.ChannelResultsBuilder;
pub const QueryRequest = dymes_msg_store.QueryRequest;
pub const QueryCursor = dymes_msg_store.QueryCursor;
pub const AppendRequest = dymes_msg_store.AppendRequest;
pub const ImportRequest = dymes_msg_store.ImportRequest;
pub const CursorTraversalRequest = dymes_msg_store.CursorTraversalRequest;
pub const MessageLocation = dymes_msg_store.MessageLocation;
pub const EngineMetrics = @import("metrics.zig").EngineMetrics;

test "api" {
    std.testing.refAllDeclsRecursive(@This());
}
