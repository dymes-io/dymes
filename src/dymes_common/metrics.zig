//! Dymes Metrics Support.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const posix = std.posix;
const errors = @import("errors.zig");
const logging = @import("logging.zig");
const testing = std.testing;

// Log level for embedded tests
const module_tests_log_level = logging.LogLevel.none;

/// Metrics exporter exit codes
pub const ExporterExitCode = enum {
    ok,
    usage,
    listen,
    file,
    server,
    mmap,
    write,
};

pub const CounterU64 = extern struct {
    const Self = @This();
    value: u64 = 0,

    pub inline fn init() Self {
        return .{ .value = 0x0 };
    }

    pub fn incrementBy(self: *Self, inc_by: u64) void {
        if (inc_by == 0) return;

        while (true) {
            const old = @atomicLoad(u64, &self.value, .monotonic);
            if (old == std.math.maxInt(u64)) return;

            const new = std.math.add(u64, old, inc_by) catch std.math.maxInt(u64);

            if (@cmpxchgWeak(u64, &self.value, old, new, .monotonic, .monotonic) == null) return;
        }
    }

    pub inline fn increment(self: *Self) void {
        self.incrementBy(1);
    }

    pub inline fn get(self: *const Self) u64 {
        return @atomicLoad(u64, &self.value, .monotonic);
    }
};

pub const GaugeU64 = extern struct {
    const Self = @This();
    value: u64 = 0,

    pub inline fn init() Self {
        return .{ .value = 0x0 };
    }

    pub inline fn set(self: *Self, v: u64) void {
        @atomicStore(u64, &self.value, v, .monotonic);
    }

    pub inline fn get(self: *const Self) u64 {
        return @atomicLoad(u64, &self.value, .monotonic);
    }

    pub fn increment(self: *Self) void {
        while (true) {
            const old = @atomicLoad(u64, &self.value, .monotonic);
            if (old == std.math.maxInt(u64)) return;
            if (@cmpxchgWeak(u64, &self.value, old, old + 1, .monotonic, .monotonic) == null) return;
        }
    }

    pub fn decrement(self: *Self) void {
        while (true) {
            const old = @atomicLoad(u64, &self.value, .monotonic);
            if (old == 0) return;
            if (@cmpxchgWeak(u64, &self.value, old, old - 1, .monotonic, .monotonic) == null) return;
        }
    }
};

pub const MaxGaugeU64 = extern struct {
    const Self = @This();
    value: u64 = 0,

    pub inline fn init() Self {
        return .{ .value = 0x0 };
    }

    pub inline fn get(self: *const Self) u64 {
        return @atomicLoad(u64, &self.value, .monotonic);
    }

    /// Checks if the passed value is higher than the current max, if it is then set that as the new max
    pub fn updateMax(self: *Self, v: u64) void {
        while (true) {
            const old = @atomicLoad(u64, &self.value, .monotonic);
            if (v <= old) return;
            if (@cmpxchgWeak(u64, &self.value, old, v, .monotonic, .monotonic) == null) return;
        }
    }
};

fn runLoggedTest(comptime test_name: []const u8, comptime test_fn: anytype) !void {
    std.debug.print("test.{s}\n", .{test_name});

    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);

    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = module_tests_log_level;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    const logger: *logging.Logger = logging.logger(test_name);
    try test_fn(logger);
}

test "CounterU64.smoke" {
    try runLoggedTest("CounterU64.smoke", struct {
        fn run(logger: *logging.Logger) !void {
            logger.debug().msg("CounterU64: increment").log();

            var c: CounterU64 = .{};
            try testing.expectEqual(@as(u64, 0), c.get());

            c.increment();
            try testing.expectEqual(@as(u64, 1), c.get());

            c.incrementBy(4);
            try testing.expectEqual(@as(u64, 5), c.get());

            c.incrementBy(0);
            try testing.expectEqual(@as(u64, 5), c.get());

            logger.debug().msg("CounterU64: clamping").log();

            @atomicStore(u64, &c.value, std.math.maxInt(u64) - 2, .monotonic);
            c.incrementBy(10);
            try testing.expectEqual(std.math.maxInt(u64), c.get());

            c.increment();
            try testing.expectEqual(std.math.maxInt(u64), c.get());
        }
    }.run);
}

test "GaugeU64.smoke" {
    try runLoggedTest("GaugeU64.smoke", struct {
        fn run(logger: *logging.Logger) !void {
            logger.debug().msg("GaugeU64: set/get").log();

            var g: GaugeU64 = .{};
            try testing.expectEqual(@as(u64, 0), g.get());

            g.set(42);
            try testing.expectEqual(@as(u64, 42), g.get());

            logger.debug().msg("GaugeU64: inc/dec clamping").log();

            g.set(0);
            g.decrement();
            try testing.expectEqual(@as(u64, 0), g.get());

            g.increment();
            try testing.expectEqual(@as(u64, 1), g.get());

            g.decrement();
            try testing.expectEqual(@as(u64, 0), g.get());

            g.set(std.math.maxInt(u64));
            g.increment();
            try testing.expectEqual(std.math.maxInt(u64), g.get());
        }
    }.run);
}

test "MaxGaugeU64.smoke" {
    try runLoggedTest("MaxGaugeU64.smoke", struct {
        fn run(logger: *logging.Logger) !void {
            logger.debug().msg("MaxGaugeU64: updateMax").log();

            var mg: MaxGaugeU64 = .{};
            try testing.expectEqual(@as(u64, 0), mg.get());

            mg.updateMax(5);
            try testing.expectEqual(@as(u64, 5), mg.get());

            mg.updateMax(3);
            try testing.expectEqual(@as(u64, 5), mg.get());

            mg.updateMax(7);
            try testing.expectEqual(@as(u64, 7), mg.get());
        }
    }.run);
}

pub const InitError =
    errors.AccessError ||
    errors.CreationError ||
    errors.IoError ||
    errors.AllocationError ||
    errors.UsageError ||
    errors.StateError;

/// Metrics mapper, reflects in-memory metrics to a memory mapped file.
pub fn MetricsMapper(comptime Metrics: type) type {
    return struct {
        const Self = @This();
        metrics: *Metrics,

        /// Create/open the mmap file and map the metrics region to a pointer.
        pub fn init(path: []const u8) InitError!Self {
            if (path.len == 0) return errors.UsageError.MissingArgument;

            const metrics_size = @sizeOf(Metrics);

            var file = std.fs.cwd().createFile(path, .{
                .truncate = false,
                .read = true,
                .mode = 0o644,
            }) catch |e| return switch (e) {
                error.FileNotFound, error.NotDir => errors.AccessError.FileNotFound,
                error.AccessDenied, error.PermissionDenied, error.SymLinkLoop, error.DeviceBusy, error.NetworkNotFound, error.ProcessNotFound, error.NoDevice => errors.AccessError.AccessFailure,
                error.ProcessFdQuotaExceeded, error.SystemFdQuotaExceeded, error.SystemResources => errors.AllocationError.LimitReached,
                error.InvalidUtf8, error.InvalidWtf8, error.BadPathName, error.NameTooLong => errors.UsageError.IllegalArgument,
                else => errors.AccessError.OtherAccessFailure,
            };
            defer file.close();

            const file_size = file.getEndPos() catch |e| {
                return switch (e) {
                    error.AccessDenied => errors.AccessError.AccessFailure,
                    error.PermissionDenied => errors.AccessError.AccessFailure,
                    else => errors.IoError.ReadFailed,
                };
            };

            if (file_size < metrics_size) {
                file.setEndPos(metrics_size) catch |e| {
                    return switch (e) {
                        error.AccessDenied => errors.AccessError.AccessFailure,
                        error.InputOutput => errors.IoError.WriteFailed,
                        error.FileTooBig => errors.AllocationError.LimitReached,
                        else => errors.IoError.WriteFailed,
                    };
                };
            }

            const prot: u32 = posix.PROT.READ | posix.PROT.WRITE;
            const flags: posix.MAP = @bitCast(@as(u32, 0x01));

            const region = posix.mmap(null, metrics_size, prot, flags, file.handle, 0) catch |e| {
                return switch (e) {
                    error.OutOfMemory => errors.CreationError.OutOfMemory,
                    error.LockedMemoryLimitExceeded, error.ProcessFdQuotaExceeded, error.SystemFdQuotaExceeded => errors.CreationError.LimitReached,
                    error.AccessDenied, error.PermissionDenied => errors.AccessError.AccessFailure,
                    else => errors.CreationError.OtherCreationFailure,
                };
            };

            return .{ .metrics = @ptrCast(@alignCast(region.ptr)) };
        }

        /// Unmaps the metrics region.
        pub fn deinit(self: *const Self) void {
            const metrics_size = @sizeOf(Metrics);
            const bytes: [*]align(std.heap.page_size_min) u8 = @ptrCast(@alignCast(self.metrics));
            posix.munmap(bytes[0..metrics_size]);
        }
    };
}

const DummyMetrics = extern struct {
    const Self = @This();
    requests_total: CounterU64,
    requests_ok_total: CounterU64,
    requests_client_error_total: CounterU64,
    requests_server_error_total: CounterU64,

    request_duration_ns_total: CounterU64,
    request_duration_ns_count: CounterU64,
    request_duration_ns_max: MaxGaugeU64,

    dme_running: GaugeU64,

    pub fn format(self: *const Self, w: *std.Io.Writer) !void {
        try w.print("requests_total {d}\n", .{self.requests_total.get()});
        try w.print("requests_ok_total {d}\n", .{self.requests_ok_total.get()});
    }
};

test "MetricsMapper.smoke" {
    try runLoggedTest("MetricsMapper.smoke", struct {
        fn run(logger: *logging.Logger) !void {
            const allocator = testing.allocator;

            var tmp = testing.tmpDir(.{});
            defer tmp.cleanup();

            const tmp_abs = try tmp.dir.realpathAlloc(allocator, ".");
            defer allocator.free(tmp_abs);

            const mmap_path = try std.fs.path.join(allocator, &.{ tmp_abs, "metrics.mmap" });
            defer allocator.free(mmap_path);

            logger.debug().msg("Metrics.init: first map + write").log();

            const m1m = try MetricsMapper(DummyMetrics).init(mmap_path);
            const m1 = m1m.metrics;

            try testing.expectEqual(@as(u64, 0), m1.requests_total.get());
            try testing.expectEqual(@as(u64, 0), m1.request_duration_ns_max.get());

            m1.requests_total.incrementBy(123);
            m1.requests_ok_total.increment();
            m1.request_duration_ns_total.incrementBy(555);
            m1.request_duration_ns_count.incrementBy(3);
            m1.request_duration_ns_max.updateMax(999);
            m1.dme_running.set(1);

            try testing.expectEqual(@as(u64, 123), m1.requests_total.get());
            try testing.expectEqual(@as(u64, 1), m1.requests_ok_total.get());
            try testing.expectEqual(@as(u64, 555), m1.request_duration_ns_total.get());
            try testing.expectEqual(@as(u64, 3), m1.request_duration_ns_count.get());
            try testing.expectEqual(@as(u64, 999), m1.request_duration_ns_max.get());
            try testing.expectEqual(@as(u64, 1), m1.dme_running.get());

            m1m.deinit();

            logger.debug().msg("Metrics.init: remap + verify persisted values").log();

            const m2m = try MetricsMapper(DummyMetrics).init(mmap_path);
            const m2 = m2m.metrics;

            try testing.expectEqual(@as(u64, 123), m2.requests_total.get());
            try testing.expectEqual(@as(u64, 1), m2.requests_ok_total.get());
            try testing.expectEqual(@as(u64, 555), m2.request_duration_ns_total.get());
            try testing.expectEqual(@as(u64, 3), m2.request_duration_ns_count.get());
            try testing.expectEqual(@as(u64, 999), m2.request_duration_ns_max.get());
            try testing.expectEqual(@as(u64, 1), m2.dme_running.get());

            m2m.deinit();

            try tmp.dir.deleteFile("metrics.mmap");
        }
    }.run);
}
