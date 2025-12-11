//! Dymes HTTP support.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const debug = std.debug;
const assert = debug.assert;
const io = std.io;

pub const constants = @import("dymes_http/constants.zig");
pub const Server = @import("dymes_http/Server.zig");
pub const ingest = @import("dymes_http/ingest.zig");
pub const Ingester = ingest.Ingester;
pub const StandaloneIngester = ingest.Standalone;
pub const ClusteredIngester = ingest.Clustered;
pub const Allocators = ingest.Allocators;
pub const HttpMetrics = @import("dymes_http/metrics.zig").HttpMetrics;

test "dymes_http dependencies" {
    std.testing.refAllDeclsRecursive(@This());
}
