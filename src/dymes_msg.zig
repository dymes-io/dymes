//! This module exposes Dymes MSG functionality.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

const testing = std.testing;

pub const common = @import("dymes_common");
pub const constants = @import("dymes_msg/constants.zig");

pub const LogSequenceNumber = constants.LogSequenceNumber;
pub const Ulid = constants.Ulid;

pub const limits = @import("dymes_msg/limits.zig");
pub const Message = @import("dymes_msg/Message.zig");
pub const MessageBuilder = @import("dymes_msg/MessageBuilder.zig");
const headers = @import("dymes_msg/headers.zig");
pub const FrameHeader = headers.FrameHeader;
pub const KvHeader = headers.KvHeader;

const filters = @import("dymes_msg/filters.zig");
pub const Filter = filters.Filter;
pub const FilterDto = filters.FilterDto;
pub const KvFilter = filters.KvFilter;
pub const KvPair = filters.KvPair;

const queries = @import("dymes_msg/queries.zig");

pub const QueryTag = queries.QueryTag;
pub const Query = queries.Query;
pub const EncodedUlid = queries.EncodedUlid;
pub const RangeQueryDto = queries.RangeQueryDto;
pub const ChannelQueryDto = queries.ChannelQueryDto;
pub const MultiMsgQueryDto = queries.MultiMsgQueryDto;
pub const QueryDto = queries.QueryDto;

pub const CreationRequest = @import("dymes_msg/CreationRequest.zig");
pub const CreationRequestBuider = @import("dymes_msg/CreationRequestBuilder.zig");
pub const ImportRequest = @import("dymes_msg/ImportRequest.zig");
pub const ImportRequestBuilder = @import("dymes_msg/ImportRequestBuilder.zig");

pub const FrameAllocator = @import("dymes_msg/FrameAllocator.zig");

pub const marshalling = @import("dymes_msg/marshalling.zig");

test "dymes_msg dependencies" {
    std.testing.refAllDeclsRecursive(@This());
    common.util.maybe(true);
}
