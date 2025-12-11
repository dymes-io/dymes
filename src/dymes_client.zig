//! Dymes Client
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const debug = std.debug;
const assert = debug.assert;
const io = std.io;

const common = @import("dymes_common");
const Ulid = common.ulid.Ulid;
const config = common.config;
const Config = common.config.Config;

const errors = common.errors;
const UsageError = errors.UsageError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;

const logging = common.logging;
const LogLevel = logging.LogLevel;
const LogFormat = logging.LogFormat;

pub const constants = @import("dymes_client/constants.zig");
pub const Client = @import("dymes_client/Client.zig");
const queries = @import("dymes_client/queries.zig");

pub const Filter = queries.Filter;
pub const FilterDto = queries.FilterDto;
pub const QueryDto = queries.QueryDto;
pub const EncodedUlid = queries.EncodedUlid;
pub const ClientQueryDto = queries.ClientQueryDto;

// test "dymes_client dependencies" {
//     std.testing.refAllDeclsRecursive(@This());
// }
