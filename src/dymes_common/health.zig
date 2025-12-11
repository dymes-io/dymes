//! Application health information.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const constants = @import("constants.zig");
const limits = @import("limits.zig");
const errors = @import("errors.zig");

const AllocationError = errors.AllocationError;

const logging = @import("logging.zig");
const Logger = logging.Logger;

pub const ComponentHealthProvider = struct {
    const Self = @This();
    pub const ProbeFunction = *const fn (context: *anyopaque) bool;
    component: []const u8,
    context: *anyopaque,
    probeFn: ProbeFunction,

    pub fn init(component: []const u8, context: *anyopaque, probeFn: ProbeFunction) Self {
        return .{
            .component = component,
            .context = context,
            .probeFn = probeFn,
        };
    }

    pub fn probe(self: *Self) bool {
        return self.probeFn(self.context);
    }
};

test "test ComponentHealthProvider" {
    std.debug.print("test.health.ComponentHealthProvider\n", .{});

    const ComponentA = struct {
        number: usize,
        pub fn probeContext(self: *@This()) *anyopaque {
            return self;
        }
        pub fn probeFn(context: *anyopaque) bool {
            return probe(@ptrCast(@alignCast(context)));
        }
        fn probe(self: *@This()) bool {
            std.debug.print("\tComponent A #{d} probed\n", .{self.number});
            return true;
        }
    };
    var component_1: ComponentA = .{ .number = 1 };
    const ComponentB = struct {
        number: usize,
        pub fn probeContext(self: *@This()) *anyopaque {
            return self;
        }
        pub fn probeFn(context: *anyopaque) bool {
            return probe(@ptrCast(@alignCast(context)));
        }
        fn probe(self: *@This()) bool {
            std.debug.print("\tComponent B #{d} probed\n", .{self.number});
            return true;
        }
    };
    var component_2: ComponentB = .{ .number = 2 };

    var chp_1 = ComponentHealthProvider.init("component_1", component_1.probeContext(), ComponentA.probeFn);
    var chp_2 = ComponentHealthProvider.init("component_2", component_2.probeContext(), ComponentB.probeFn);

    const probe_fn_a = ComponentA.probeFn;
    const probe_fn_b = ComponentB.probeFn;

    std.debug.print("\t{s} returned {any}\n", .{ chp_1.component, probe_fn_a(chp_1.context) });
    std.debug.print("\t{s} returned {any}\n", .{ chp_2.component, probe_fn_b(chp_2.context) });

    try testing.expectEqual(chp_1.probe(), probe_fn_a(chp_1.context));
    try testing.expectEqual(chp_2.probe(), probe_fn_b(chp_2.context));
}

pub const Health = struct {
    const Self = @This();
    mtx: std.Thread.Mutex,

    logger: *Logger,
    providers: std.StringArrayHashMap(ComponentHealthProvider),

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .mtx = .{},
            .logger = logging.logger("Health"),
            .providers = std.StringArrayHashMap(ComponentHealthProvider).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.mtx.lock();
        defer self.mtx.unlock();

        defer self.providers.deinit();
    }

    pub fn probe(self: *Self) bool {
        self.mtx.lock();
        defer self.mtx.unlock();

        if (self.providers.count() == 0) {
            return false;
        }

        var pit = self.providers.iterator();
        while (pit.next()) |entry| {
            if (!entry.value_ptr.probe()) {
                self.logger.debug()
                    .msg("Component not ready during health check")
                    .str("component", entry.key_ptr.*)
                    .log();
                return false;
            }
            self.logger.fine()
                .msg("Component ready during health check")
                .str("component", entry.key_ptr.*)
                .log();
        }

        return true;
    }

    /// Adds the given health provider with minimal sanity checking.
    pub fn addProvider(self: *Self, provider: ComponentHealthProvider) AllocationError!void {
        self.mtx.lock();
        defer self.mtx.unlock();

        self.providers.put(provider.component, provider) catch return AllocationError.OutOfMemory;
    }

    /// Removes a health provider (if present).
    pub fn removeProvider(self: *Self, provider_name: []const u8) void {
        self.mtx.lock();
        defer self.mtx.unlock();

        _ = self.providers.swapRemove(provider_name);
    }
};

test "test.Health" {
    std.debug.print("test.Health\n", .{});

    const allocator = testing.allocator;

    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var logger = logging.logger("test.Health");

    var health = Health.init(allocator);
    defer health.deinit();

    try testing.expectEqual(false, health.probe());

    const ComponentA = struct {
        ready: bool = false,
        pub fn probeContext(self: *@This()) *anyopaque {
            return self;
        }
        pub fn probeFn(context: *anyopaque) bool {
            return probe(@ptrCast(@alignCast(context)));
        }
        fn probe(self: *@This()) bool {
            return self.ready;
        }
    };
    var component_1: ComponentA = .{};

    const ComponentB = struct {
        ready: bool = false,
        pub fn probeContext(self: *@This()) *anyopaque {
            return self;
        }
        pub fn probeFn(context: *anyopaque) bool {
            return probe(@ptrCast(@alignCast(context)));
        }
        fn probe(self: *@This()) bool {
            return self.ready;
        }
    };
    var component_2: ComponentB = .{};

    const chp_1 = ComponentHealthProvider.init("A", component_1.probeContext(), ComponentA.probeFn);
    logger.debug()
        .msg("Adding health provider")
        .str("component", chp_1.component)
        .log();

    try health.addProvider(chp_1);
    try testing.expectEqual(false, health.probe());
    component_1.ready = true;
    try testing.expectEqual(true, health.probe());

    const chp_2 = ComponentHealthProvider.init("B", component_2.probeContext(), ComponentB.probeFn);
    logger.debug()
        .msg("Adding another health provider")
        .str("component", chp_2.component)
        .log();
    try health.addProvider(chp_2);
    try testing.expectEqual(false, health.probe());
    component_2.ready = true;
    try testing.expectEqual(true, health.probe());
    component_1.ready = false;
    try testing.expectEqual(false, health.probe());
    logger.debug()
        .msg("Removing a health provider")
        .str("component", chp_2.component)
        .log();
    health.removeProvider(chp_1.component);
    try testing.expectEqual(true, health.probe());
}
