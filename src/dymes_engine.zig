//! Dymes Engine.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const debug = std.debug;
const assert = debug.assert;
const io = std.io;

const clap = @import("clap");

pub const Engine = @import("dymes_engine/Engine.zig");
pub const EngineMetrics = Engine.EngineMetrics;
const engine_api = @import("dymes_engine/api.zig");
pub const Query = engine_api.Query;
pub const QueryDto = engine_api.QueryDto;
pub const EagerResults = engine_api.EagerResults;
pub const QueryRequest = engine_api.QueryRequest;
pub const AppendRequest = engine_api.AppendRequest;
pub const ImportRequest = engine_api.ImportRequest;
pub const CursorTraversalRequest = engine_api.CursorTraversalRequest;

const msg = @import("dymes_msg");
pub const Message = msg.Message;
pub const FrameAllocator = msg.FrameAllocator;
const msg_store = @import("dymes_msg_store");
pub const AppendStore = msg_store.AppendStore;
pub const StorageSubsystem = @import("dymes_engine/StorageSubsystem.zig");
pub const QuerySubsystem = @import("dymes_engine/QuerySubsystem.zig");

pub const UlidGenerator = @import("dymes_engine/UlidGenerator.zig");

pub const constants = @import("dymes_engine/constants.zig");

test "dymes_engine dependencies" {
    std.testing.refAllDeclsRecursive(@This());
}
