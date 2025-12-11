//! This module exposes common Dymes functionality.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const lmdb = @import("lmdb");

pub const constants = @import("dymes_common/constants.zig");
pub const limits = @import("dymes_common/limits.zig");
pub const net = @import("dymes_common/net.zig");
// pub const kv = @import("dymes_common/kv.zig"); // Moved to EXT
pub const util = @import("dymes_common/util.zig");
pub const base64 = @import("dymes_common/base64.zig");
pub const config = @import("dymes_common/config.zig");
pub const errors = @import("dymes_common/errors.zig");
pub const logging = @import("dymes_common/logging.zig");
pub const health = @import("dymes_common/health.zig");
pub const checksum = @import("dymes_common/checksum.zig");
pub const queues = @import("dymes_common/queues.zig");
pub const rfc3339 = @import("dymes_common/rfc3339.zig");
const hissylogz = @import("hissylogz");
pub const ulid = hissylogz.ulid;

pub const caching = @import("dymes_common/caching.zig");

pub const metrics = @import("dymes_common/metrics.zig");

const testing = std.testing;

const base64Encoder = util.encodeBase64;

test "dymes_common dependencies" {
    std.testing.refAllDeclsRecursive(@This());
}
