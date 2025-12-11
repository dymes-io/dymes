//! Networking convenience functionality.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

pub const k8s = @import("net/k8s.zig");
pub const resolver = @import("net/resolver.zig");

test "net" {
    std.testing.refAllDeclsRecursive(@This());
}
