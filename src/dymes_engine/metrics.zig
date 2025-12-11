//! Dymes engine metrics.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const metrics = @import("dymes_common").metrics;

/// Dymes engine metrics.
pub const EngineMetrics = extern struct {
    const Self = @This();
    engine_queries_received: metrics.CounterU64,
    engine_queries_ok: metrics.CounterU64,

    engine_appends_received: metrics.CounterU64,
    engine_appends_ok: metrics.CounterU64,

    pub fn init() Self {
        return .{
            .engine_queries_received = .init(),
            .engine_queries_ok = .init(),
            .engine_appends_received = .init(),
            .engine_appends_ok = .init(),
        };
    }

    pub fn format(self: *const Self, w: *std.Io.Writer) !void {
        try w.print("engine_queries_received {d}\n", .{self.engine_queries_received.get()});
        try w.print("engine_queries_ok {d}\n", .{self.engine_queries_ok.get()});
        try w.print("engine_appends_received {d}\n", .{self.engine_appends_received.get()});
        try w.print("engine_appends_ok {d}\n", .{self.engine_appends_ok.get()});
    }
};
