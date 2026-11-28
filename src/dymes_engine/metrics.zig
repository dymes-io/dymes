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

    engine_batch_appends: metrics.CounterU64,
    engine_batch_messages: metrics.CounterU64,

    /// Times the engine lock was taken and released, how many of those held it for over 10 ms, the microseconds it
    /// was held in all, and the longest it was held
    engine_lock_holds: metrics.CounterU64,
    engine_lock_holds_over_10ms: metrics.CounterU64,
    engine_lock_held_us: metrics.CounterU64,
    engine_lock_hold_max_us: metrics.MaxGaugeU64,

    pub fn init() Self {
        return .{
            .engine_queries_received = .init(),
            .engine_queries_ok = .init(),
            .engine_appends_received = .init(),
            .engine_appends_ok = .init(),
            .engine_batch_appends = .init(),
            .engine_batch_messages = .init(),
            .engine_lock_holds = .init(),
            .engine_lock_holds_over_10ms = .init(),
            .engine_lock_held_us = .init(),
            .engine_lock_hold_max_us = .init(),
        };
    }

    pub fn format(self: *const Self, w: *std.Io.Writer) !void {
        try w.print("engine_queries_received {d}\n", .{self.engine_queries_received.get()});
        try w.print("engine_queries_ok {d}\n", .{self.engine_queries_ok.get()});
        try w.print("engine_appends_received {d}\n", .{self.engine_appends_received.get()});
        try w.print("engine_appends_ok {d}\n", .{self.engine_appends_ok.get()});
        try w.print("engine_batch_appends {d}\n", .{self.engine_batch_appends.get()});
        try w.print("engine_batch_messages {d}\n", .{self.engine_batch_messages.get()});
        try w.print("engine_lock_holds {d}\n", .{self.engine_lock_holds.get()});
        try w.print("engine_lock_holds_over_10ms {d}\n", .{self.engine_lock_holds_over_10ms.get()});
        try w.print("engine_lock_held_us {d}\n", .{self.engine_lock_held_us.get()});
        try w.print("engine_lock_hold_max_us {d}\n", .{self.engine_lock_hold_max_us.get()});
    }
};
