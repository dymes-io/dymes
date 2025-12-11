//! ULID Generator.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const debug = std.debug;
const assert = debug.assert;

const common = @import("dymes_common");
const ulid = common.ulid;
const Ulid = ulid.Ulid;

const msg_store = @import("dymes_msg_store");

const errors = common.errors;
const AllocationError = errors.AllocationError;

const logging = common.logging;
const Logger = logging.Logger;

const LifeCycleControl = common.util.LifeCycleControl;
const Health = common.health.Health;
const ComponentHealthProvider = common.health.ComponentHealthProvider;

const Engine = @import("Engine.zig");

const component_name: []const u8 = "engine.UlidSupplier";

const Self = @This();

pub const GeneratorError = error{
    TimeTooOld,
    TimeOverflow,
    RandomOverflow,
};

mtx_ulids: std.Thread.Mutex = .{},
supplyMilliTimestamp: *const fn () i64 = std.time.milliTimestamp,
supplyPRN: std.Random = std.crypto.random,
last: Ulid = .{ .time = 0, .rand = 0 },

pub fn next(self: *Self) GeneratorError!Ulid {
    const ms = self.supplyMilliTimestamp();
    self.mtx_ulids.lock();
    defer self.mtx_ulids.unlock();
    if (ms < 0) return GeneratorError.TimeTooOld;
    if (ms > std.math.maxInt(u48)) return GeneratorError.TimeOverflow;
    const ms48: u48 = @intCast(ms);

    if (ms48 == self.last.time) {
        if (self.last.rand == std.math.maxInt(u80)) return GeneratorError.RandomOverflow;
        self.last.rand += 1;
    } else {
        self.last.time = ms48;
        self.last.rand = self.supplyPRN.int(u80);
    }

    return self.last;
}

const UlidGenerator = @This();

test "UlidGenerator" {
    var ulid_supplier: UlidGenerator = .{};

    _ = try ulid_supplier.next();
    const id0 = try ulid_supplier.next();
    const id1 = try ulid_supplier.next();

    // timestamp 48 bits
    try std.testing.expectEqual(@bitSizeOf(@TypeOf(id0.time)), 48);
    // randomness 80 bits
    try std.testing.expectEqual(@bitSizeOf(@TypeOf(id0.rand)), 80);
    // timestamp is milliseconds since unix epoch
    try std.testing.expect((std.time.milliTimestamp() - id0.time) < 10);

    // uses crockford base32 alphabet
    const str = id0.encode();
    try std.testing.expectEqual(26, str.len);
    try std.testing.expectEqualDeep(try ulid.decode(&str), id0);
    const str_alloc = try id0.encodeAlloc(std.testing.allocator);
    defer std.testing.allocator.free(str_alloc);
    try std.testing.expect(std.mem.eql(u8, str_alloc, &str));

    // monotonic in the same millisecond
    try std.testing.expectEqual(id0.time, id1.time);
    try std.testing.expect(id0.rand != id1.rand);
    try std.testing.expectEqual(id0.rand + 1, id1.rand);

    // binary encodable
    const bytes = id0.bytes();
    const time_bytes = bytes[0..6].*;
    const rand_bytes = bytes[6..].*;
    try std.testing.expectEqual(@as(u48, @bitCast(time_bytes)), id0.time);
    try std.testing.expectEqual(@as(u80, @bitCast(rand_bytes)), id0.rand);
    try std.testing.expect((try ulid.fromBytes(&bytes)).equals(id0));
}
