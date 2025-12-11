//! Queue support.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const AllocationError = @import("errors.zig").AllocationError;

/// A lock-free intrusive MPSC (multi-provider, single consumer) queue.
/// The type T must have a field `next` of type `?*T`.
///
/// This is an implementatin of a Vyukov Queue.
/// See: https://www.1024cores.net/home/lock-free-algorithms/queues/intrusive-mpsc-node-based-queue
/// Inspired by https://github.com/mitchellh/libxev/blob/main/src/queue_mpsc.zig, Copyright (c) 2023 Mitchell Hashimoto,
/// modified for standalone use.
pub fn MpscQueue(comptime T: type) type {
    return struct {
        const Self = @This();

        /// Front of the queue.
        head: *T,
        /// Back of the queue.
        tail: *T,
        /// Stub entry
        stub: T,

        /// Initialize the queue.
        ///
        /// We requires a stable pointer to ourselves, so we allocate during init,
        /// Caller must call `deinit()` to release resources.
        pub fn init(allocator: std.mem.Allocator) AllocationError!*Self {
            const new_self = allocator.create(Self) catch return AllocationError.OutOfMemory;
            errdefer allocator.destroy(new_self);
            var new_stub: T = undefined;
            new_stub.next = null;
            new_self.* = .{
                .stub = new_stub,
                .head = &new_self.stub,
                .tail = &new_self.stub,
            };
            return new_self;
        }

        /// De-initialize the queue.
        ///
        /// We release our own allocation, callers need to have drained the queue and
        /// released any resources held by elements.
        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            defer allocator.destroy(self);
            // Drain queue
            while (self.pop() != null) {}
        }

        /// Push an item onto the queue. This can be called by any number
        /// of producers.
        pub fn push(self: *Self, v: *T) void {
            @atomicStore(?*T, &v.next, null, .unordered);
            const prev = @atomicRmw(*T, &self.head, .Xchg, v, .acq_rel);
            @atomicStore(?*T, &prev.next, v, .release);
        }

        /// Pop the first in element from the queue. This must be called
        /// by only a single consumer at any given time.
        pub fn pop(self: *Self) ?*T {
            var tail = @atomicLoad(*T, &self.tail, .unordered);
            var next_ = @atomicLoad(?*T, &tail.next, .acquire);
            if (tail == &self.stub) {
                const next = next_ orelse return null;
                @atomicStore(*T, &self.tail, next, .unordered);
                tail = next;
                next_ = @atomicLoad(?*T, &tail.next, .acquire);
            }

            if (next_) |next| {
                @atomicStore(*T, &self.tail, next, .release);
                tail.next = null;
                return tail;
            }

            const head = @atomicLoad(*T, &self.head, .unordered);
            if (tail != head) return null;
            self.push(&self.stub);

            next_ = @atomicLoad(?*T, &tail.next, .acquire);
            if (next_) |next| {
                @atomicStore(*T, &self.tail, next, .unordered);
                tail.next = null;
                return tail;
            }

            return null;
        }
    };
}

test MpscQueue {
    std.debug.print("test.queues.MpscQueue\n", .{});
    const allocator = testing.allocator;

    // Types
    const Elem = struct {
        const Self = @This();
        next: ?*Self = null,
    };
    const Queue = MpscQueue(Elem);
    var q: *Queue = try Queue.init(allocator);
    defer q.deinit(allocator);

    // Elems
    var elems: [10]Elem = .{Elem{}} ** 10;

    // One
    try testing.expect(q.pop() == null);
    q.push(&elems[0]);
    try testing.expect(q.pop().? == &elems[0]);
    try testing.expect(q.pop() == null);

    // Two
    try testing.expect(q.pop() == null);
    q.push(&elems[0]);
    q.push(&elems[1]);
    try testing.expect(q.pop().? == &elems[0]);
    try testing.expect(q.pop().? == &elems[1]);
    try testing.expect(q.pop() == null);

    // // Interleaved
    try testing.expect(q.pop() == null);
    q.push(&elems[0]);
    try testing.expect(q.pop().? == &elems[0]);
    q.push(&elems[1]);
    try testing.expect(q.pop().? == &elems[1]);
    try testing.expect(q.pop() == null);
}
