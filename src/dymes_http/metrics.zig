//! Dymes HTTP metrics.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const metrics = @import("dymes_common").metrics;

/// Dymes HTTP metrics.
pub const HttpMetrics = extern struct {
    const Self = @This();
    http_liveness_probes: metrics.CounterU64,
    http_readiness_probes: metrics.CounterU64,
    http_cursors_open: metrics.GaugeU64,

    pub fn init() Self {
        return .{
            .http_liveness_probes = .init(),
            .http_readiness_probes = .init(),
            .http_cursors_open = .init(),
        };
    }

    pub fn format(self: *const Self, w: *std.Io.Writer) !void {
        try w.print("http_liveness_probes {d}\n", .{self.http_liveness_probes.get()});
        try w.print("http_readiness_probes {d}\n", .{self.http_readiness_probes.get()});
        try w.print("http_cursors_open {d}\n", .{self.http_cursors_open.get()});
    }
};
