//! Dymes Metrics.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

pub fn DymesMetrics(comptime EngineMetrics: type, comptime HttpMetrics: type) type {
    return extern struct {
        const Self = @This();
        engine: EngineMetrics,
        http: HttpMetrics,

        pub fn init() Self {
            return .{
                .engine = .init(),
                .http = .init(),
            };
        }

        pub fn format(self: *const Self, w: *std.Io.Writer) !void {
            try w.print("{f}\n{f}\n", .{ self.engine, self.http });
        }
    };
}
