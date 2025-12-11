//! Daemon thread (or subsystem, etc) lifetime control structure.
//!
//! SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//!
//! SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const net = std.net;
const http = std.http;

pub const default_cond_timeout_ns = std.time.ns_per_ms * 100;

const Self = @This();
mtx: std.Thread.Mutex = .{},
cond: std.Thread.Condition = .{},
shutdown_trigger: bool = false,
timeout_ns: u64 = default_cond_timeout_ns,

pub fn mustShutdown(self: *Self) bool {
    self.mtx.lock();
    defer self.mtx.unlock();
    self.cond.timedWait(&self.mtx, self.timeout_ns) catch {};
    return @atomicLoad(bool, &self.shutdown_trigger, .monotonic);
}

pub fn shutdown(self: *Self) void {
    {
        self.mtx.lock();
        defer self.mtx.unlock();
        @atomicStore(bool, &self.shutdown_trigger, true, .monotonic);
    }
    self.cond.broadcast();
}

const LifecycleControl = @This();

test "test.LifeCycleControl" {
    var lc: LifecycleControl = .{};

    try testing.expectEqual(lc.mustShutdown(), false);
    lc.shutdown();
    try testing.expectEqual(lc.mustShutdown(), true);
}
