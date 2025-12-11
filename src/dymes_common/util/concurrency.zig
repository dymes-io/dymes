//! Concurrency support and control structures.
//!
//! SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//!
//! SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const assert = std.debug.assert;
const net = std.net;
const http = std.http;

const errors = @import("../errors.zig");
const AllocationError = errors.AllocationError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;
const TimeoutError = errors.TimeoutError;

pub const SupplyError = AccessError || CreationError || StateError || TimeoutError;

/// Future to hold values supplied later.
///
pub fn Future(ValueType: anytype) type {
    return struct {
        const Self = @This();
        pub const empty: Self = .{};
        mtx: std.Thread.Mutex = .{},
        cond: std.Thread.Condition = .{},
        supplied: bool = false,
        value: ?ValueType = null,
        failed: ?SupplyError = null,

        pub fn supply(self: *Self, value: ValueType) void {
            {
                self.mtx.lock();
                defer self.mtx.unlock();
                self.value = value;
                self.supplied = true;
            }
            self.cond.signal();
        }

        pub fn supplyFailure(self: *Self, supply_error: SupplyError) void {
            {
                self.mtx.lock();
                defer self.mtx.unlock();
                self.failed = supply_error;
            }
            self.cond.signal();
        }

        pub fn get(self: *Self, timeout_ns: u64) SupplyError!ValueType {
            self.mtx.lock();
            defer self.mtx.unlock();
            if (self.failed) |e| {
                return e;
            } else if (self.supplied) {
                if (self.value) |_value| {
                    return _value;
                }
            }
            self.cond.timedWait(&self.mtx, timeout_ns) catch return SupplyError.TimedOut;
            if (self.failed) |_error| {
                return _error;
            }
            assert(self.supplied);
            return if (self.value) |_value| _value else SupplyError.AccessFailure;
        }
    };
}

test "concurrency.Future" {
    std.debug.print("test.concurrency.Future\n", .{});

    var happy_future = Future(u64).empty;
    const happy_getter = try std.Thread.spawn(.{}, testGetFuture, .{&happy_future});
    std.Thread.sleep(500_000);
    happy_future.supply(1234);
    std.Thread.join(happy_getter);

    const odd_future = Future(u64).empty;
    var odder_future = odd_future;
    odder_future.supply(5678);
    const odd_getter = try std.Thread.spawn(.{}, testGetFuture, .{&odder_future});
    std.Thread.sleep(500_000);
    std.Thread.join(odd_getter);

    var unhappy_future = Future(u64).empty;
    const unhappy_getter = try std.Thread.spawn(.{}, testGetFuture, .{&unhappy_future});
    std.Thread.sleep(500_000);
    unhappy_future.supplyFailure(CreationError.LimitReached);
    std.Thread.join(unhappy_getter);
}

fn testGetFuture(future: *Future(u64)) void {
    var have_value = false;
    while (!have_value) {
        const result = future.get(100_000) catch |e| {
            switch (e) {
                SupplyError.TimedOut => continue,
                else => {
                    std.debug.print("\tError: {any}\n", .{e});
                    return;
                },
            }
        };
        std.debug.print("\tresult={any}\n", .{result});
        have_value = true;
    }
}
