//! A mutual exclusion lock that serves its waiters in the order they arrived, and may be re-entered by its holder.
//!
//! A plain mutex lets a thread that unlocks and locks again at once take the lock ahead of threads already waiting.
//! A reader stepping through a long scan does exactly that, and an append behind it waits for the whole scan. Here
//! each waiter takes a ticket, so it waits for the holders ahead of it once each and no more.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const Self = @This();

mutex: std.Thread.Mutex = .{},
turn: std.Thread.Condition = .{},
next_ticket: u64 = 0,
now_serving: u64 = 0,
holder: ?std.Thread.Id = null,
depth: usize = 0,
held_since: ?std.time.Instant = null,

pub const init: Self = .{};

pub fn lock(self: *Self) void {
    const me = std.Thread.getCurrentId();
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.holder == me) {
        self.depth += 1;
        return;
    }
    const ticket = self.next_ticket;
    self.next_ticket += 1;
    while (ticket != self.now_serving) {
        self.turn.wait(&self.mutex);
    }
    self.holder = me;
    self.depth = 1;
    self.held_since = std.time.Instant.now() catch null;
}

pub fn unlock(self: *Self) void {
    _ = self.unlockTimed();
}

/// Unlocks, and returns for how many nanoseconds the lock was held if this released it: null when the holder
/// still holds it (it had re-entered), or the clock could not be read.
pub fn unlockTimed(self: *Self) ?u64 {
    self.mutex.lock();
    defer self.mutex.unlock();

    assert(self.holder == std.Thread.getCurrentId() and self.depth > 0);
    self.depth -= 1;
    if (self.depth > 0) return null;
    self.holder = null;
    self.now_serving += 1;
    self.turn.broadcast();
    const since = self.held_since orelse return null;
    const now = std.time.Instant.now() catch return null;
    return now.since(since);
}

test "FairLock: re-entrant, exclusive, and first come first served" {
    var fair_lock: Self = .init;

    fair_lock.lock();
    fair_lock.lock();
    try testing.expectEqual(null, fair_lock.unlockTimed());

    // Waiters queue up one by one behind the holder, then run in that order
    const Waiter = struct {
        fair_lock: *Self,
        order: *std.array_list.Managed(usize),
        id: usize,
        fn run(waiter: *@This()) void {
            waiter.fair_lock.lock();
            defer waiter.fair_lock.unlock();
            waiter.order.append(waiter.id) catch unreachable;
        }
    };
    var order = std.array_list.Managed(usize).init(testing.allocator);
    defer order.deinit();
    var waiters: [8]Waiter = undefined;
    var threads: [8]std.Thread = undefined;
    for (&waiters, &threads, 0..) |*waiter, *thread, id| {
        waiter.* = .{ .fair_lock = &fair_lock, .order = &order, .id = id };
        thread.* = try std.Thread.spawn(.{}, Waiter.run, .{waiter});
        // Wait until this waiter has taken its ticket (the holder's is ticket 0)
        while (true) {
            fair_lock.mutex.lock();
            const queued = fair_lock.next_ticket;
            fair_lock.mutex.unlock();
            if (queued == id + 2) break;
            std.Thread.yield() catch {};
        }
    }
    try testing.expectEqual(0, order.items.len);
    try testing.expect(fair_lock.unlockTimed() != null);
    for (&threads) |*thread| thread.join();
    try testing.expectEqualSlices(usize, &.{ 0, 1, 2, 3, 4, 5, 6, 7 }, order.items);
}
