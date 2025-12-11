//! This module exposes Dymes MSGSTORE functionality.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

const testing = std.testing;

pub const constants = @import("dymes_msg_store/constants.zig");
pub const limits = @import("dymes_msg_store/limits.zig");
pub const errors = @import("dymes_msg_store/errors.zig");
pub const MessageStoreOptions = @import("dymes_msg_store/MessageStoreOptions.zig");
pub const AppendStore = @import("dymes_msg_store/AppendStore.zig");
pub const ImmutableStore = @import("dymes_msg_store/ImmutableStore.zig");
pub const MessageFile = @import("dymes_msg_store/MessageFile.zig");
pub const IndexEntry = @import("dymes_msg_store/IndexEntry.zig");
pub const IndexFile = @import("dymes_msg_store/IndexFile.zig");
pub const Dataset = @import("dymes_msg_store/Dataset.zig");
pub const ReadDataset = @import("dymes_msg_store/ReadDataset.zig");
pub const DataSegment = @import("dymes_msg_store/DataSegment.zig");
pub const MessageIndex = @import("dymes_msg_store/MessageIndex.zig");
pub const SegmentsUlidIndex = @import("dymes_msg_store/SegmentsUlidIndex.zig");
pub const MessageLocation = @import("dymes_msg_store/MessageLocation.zig");
pub const ChannelIndex = @import("dymes_msg_store/ChannelIndex.zig");
pub const JournalFile = @import("dymes_msg_store/JournalFile.zig");

pub const DataSegmentsCache = @import("dymes_msg_store/DataSegmentsCache.zig");

const dymes_msg = @import("dymes_msg");
pub const Query = dymes_msg.Query;
pub const QueryDto = dymes_msg.QueryDto;
pub const EmptyResult = @import("dymes_msg_store/EmptyResult.zig");
pub const SingleResult = @import("dymes_msg_store/SingleResult.zig");
pub const SingleResultBuilder = @import("dymes_msg_store/SingleResultBuilder.zig");
pub const EagerResults = @import("dymes_msg_store/EagerResults.zig");
pub const EagerResultsBuilder = @import("dymes_msg_store/EagerResultsBuilder.zig");
pub const RangeResults = @import("dymes_msg_store/RangeResults.zig");
pub const RangeResultsBuilder = @import("dymes_msg_store/RangeResultsBuilder.zig");
pub const CorrelationResults = @import("dymes_msg_store/CorrelationResults.zig");
pub const CorrelationResultsBuilder = @import("dymes_msg_store/CorrelationResultsBuilder.zig");
pub const ChannelResults = @import("dymes_msg_store/ChannelResults.zig");
pub const ChannelResultsBuilder = @import("dymes_msg_store/ChannelResultsBuilder.zig");
pub const QueryRequest = @import("dymes_msg_store/QueryRequest.zig");
pub const AppendRequest = @import("dymes_msg_store/AppendRequest.zig");
pub const ImportRequest = @import("dymes_msg_store/ImportRequest.zig");
pub const QueryCursor = @import("dymes_msg_store/QueryCursor.zig");
pub const CursorTraversalRequest = @import("dymes_msg_store/CursorTraversalRequest.zig");

test "dymes_msg_store dependencies" {
    std.testing.refAllDeclsRecursive(@This());
}
