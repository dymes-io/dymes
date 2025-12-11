//! Address resolution support.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const builtin = @import("builtin");
const errors = @import("../errors.zig");
const logging = @import("../logging.zig");
const Logger = logging.Logger;
const native_os = builtin.os.tag;

pub const ResolverError = errors.AddressError || errors.UsageError;

pub fn resolveIPv4(allocator: std.mem.Allocator, name: []const u8) ResolverError!std.net.Address {
    var logger = logging.logger("resolver");

    logger.fine()
        .msg("Attempting address resolution")
        .str("name", name)
        .log();

    if (std.net.Address.parseIp4(name, 0) catch null) |resolved| {
        logger.fine()
            .msg("Resolved address")
            .str("name", name)
            .any("resolved", resolved)
            .log();
        return resolved;
    }

    const resolved = switch (native_os) {
        .macos, .linux => result: {
            var list = std.net.getAddressList(allocator, name, 0) catch {
                return ResolverError.AddressResolutionFailure;
            };
            defer list.deinit();

            for (list.addrs) |addr| {
                if (addr.any.family == std.posix.AF.INET) {
                    break :result addr;
                }
            }

            return ResolverError.UnsupportedAddressFamily;
        },
        // .freebsd => result: {
        //     //TODO: implement freebsd support
        // },
        else => {
            @panic("Unsupported OS");
        },
    };
    logger.fine()
        .msg("Resolved address")
        .str("name", name)
        .any("resolved", resolved)
        .log();
    return resolved;
}

pub fn resolveIPv4AsString(allocator: std.mem.Allocator, name: []const u8) ResolverError![]const u8 {
    var logger = logging.logger("resolver");

    var buffer: [15]u8 = undefined;

    logger.fine()
        .msg("Attempting address resolution")
        .str("name", name)
        .log();

    if (std.net.Address.parseIp4(name, 0) catch null) |resolved| {
        const ipString = try renderIPv4StringFromAddress(buffer[0..], resolved);
        logger.fine()
            .msg("Resolved address")
            .str("name", name)
            .str("ipString", ipString)
            .log();
        return allocator.dupe(u8, ipString) catch return ResolverError.OutOfMemory;
    }

    return switch (native_os) {
        .macos, .linux => result: {
            var list = std.net.getAddressList(allocator, name, 0) catch {
                return ResolverError.AddressResolutionFailure;
            };
            defer list.deinit();

            for (list.addrs) |addr| {
                if (addr.any.family == std.posix.AF.INET) {
                    const ipString = try renderIPv4StringFromAddress(buffer[0..], addr);
                    logger.fine()
                        .msg("Resolved address")
                        .str("name", name)
                        .str("ipString", ipString)
                        .log();
                    break :result allocator.dupe(u8, ipString) catch return ResolverError.OutOfMemory;
                }
            }

            return ResolverError.UnsupportedAddressFamily;
        },
        // .freebsd => result: {
        //     //TODO: implement freebsd support
        // },
        else => {
            @panic("Unsupported OS");
        },
    };
}

pub fn resolveAllIPv4(allocator: std.mem.Allocator, name: []const u8) ResolverError![]std.net.Address {
    var logger = logging.logger("resolver");
    var result = std.array_list.Managed(std.net.Address).init(allocator);
    errdefer result.deinit();

    logger.fine()
        .msg("Attempting all address resolution")
        .str("name", name)
        .log();

    if (std.net.Address.parseIp4(name, 0) catch null) |resolved| {
        try result.append(resolved);
        logger.fine()
            .msg("Resolved address")
            .str("name", name)
            .any("resolved", result)
            .log();
        return result.toOwnedSlice();
    }

    return switch (native_os) {
        .macos, .linux => result: {
            var list = std.net.getAddressList(allocator, name, 0) catch {
                break :result ResolverError.AddressResolutionFailure;
            };
            defer list.deinit();

            for (list.addrs) |addr| {
                if (addr.any.family == std.posix.AF.INET) {
                    result.append(addr) catch {
                        break :result ResolverError.OutOfMemory;
                    };
                }
            }

            if (result.items.len == 0) {
                break :result ResolverError.UnsupportedAddressFamily;
            }

            const slice = try result.toOwnedSlice();

            logger.fine()
                .msg("Resolved address")
                .str("name", name)
                .any("resolved", slice)
                .log();

            break :result slice;
        },
        // .freebsd => result: {
        //     // TODO: implement freebsd support
        // },
        else => {
            @panic("Unsupported OS");
        },
    };
}

fn renderIPv4StringFromAddress(buffer: []u8, addr: std.net.Address) ResolverError![]const u8 {
    const ip4 = addr.in;
    const bytes = @as(*const [4]u8, @ptrCast(&ip4.sa.addr));
    return std.fmt.bufPrint(buffer, "{}.{}.{}.{}", .{
        bytes[0], bytes[1], bytes[2], bytes[3],
    }) catch return ResolverError.OutOfMemory;
}

test "resolveIPv4 returns valid address for localhost" {
    const allocator = std.testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    const addr = try resolveIPv4(allocator, "localhost");
    try std.testing.expect(addr.any.family == std.posix.AF.INET);
    const ip4 = addr.in;
    const bytes = @as(*const [4]u8, @ptrCast(&ip4.sa.addr));
    try std.testing.expectEqual(127, bytes[0]);
    try std.testing.expectEqual(0, bytes[1]);
    try std.testing.expectEqual(0, bytes[2]);
    try std.testing.expectEqual(1, bytes[3]);
}

test "resolveAllIPv4 returns at least one address for localhost" {
    const allocator = std.testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    const addrs = try resolveAllIPv4(allocator, "127.0.0.1");
    defer allocator.free(addrs);
    try std.testing.expect(addrs.len > 0);
    try std.testing.expect(addrs[0].any.family == std.posix.AF.INET);
}

test "resolveIPv4 fails for invalid hostname" {
    const allocator = std.testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    const result = resolveIPv4(allocator, "invalid.invalid");
    try std.testing.expectError(ResolverError.AddressResolutionFailure, result);
}

test "resolveAllIPv4 fails for invalid hostname" {
    const allocator = std.testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    const result = resolveAllIPv4(allocator, "invalid.invalid");
    try std.testing.expectError(ResolverError.AddressResolutionFailure, result);
}

test "resolveIPv4 runs for google.com" {
    const allocator = std.testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    const addr = try resolveIPv4(allocator, "google.com");
    try std.testing.expect(addr.any.family == std.posix.AF.INET);
}

test "resolveAllIPv4 runs for google.com" {
    const allocator = std.testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    const addrs = try resolveAllIPv4(allocator, "google.com");
    defer allocator.free(addrs);
    try std.testing.expect(addrs.len > 0);
    try std.testing.expect(addrs[0].any.family == std.posix.AF.INET);
}

test "resolveIPv4 fails for IPv6 loopback" {
    const allocator = std.testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    const result = resolveIPv4(allocator, "::1");
    try std.testing.expectError(ResolverError.UnsupportedAddressFamily, result);
}

test "resolveAllIPv4 fails for IPv6 loopback" {
    const allocator = std.testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    const result = resolveAllIPv4(allocator, "::1");
    try std.testing.expectError(ResolverError.UnsupportedAddressFamily, result);
}

// FIXME - build env dependent
// test "resolveIPv4 fails for IPv6-only hostname" {
//     const allocator = std.testing.allocator;
//     try logging.init(allocator, std.std.fs.File.stderr().writer());
//     defer logging.deinit();
//     const result = resolveIPv4(allocator, "ipv6.google.com");
//     try std.testing.expectError(ResolverError.UnsupportedAddressFamily, result);
// }

// FIXME - build env dependent
// test "resolveAllIPv4 fails for IPv6-only hostname" {
//     const allocator = std.testing.allocator;
//     try logging.init(allocator, std.std.fs.File.stderr().writer());
//     defer logging.deinit();
//     const result = resolveAllIPv4(allocator, "ipv6.google.com");
//     try std.testing.expectError(ResolverError.UnsupportedAddressFamily, result);
// }

test "resolveIPv4AsString for localhost" {
    const allocator = std.testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    const ipString = try resolveIPv4AsString(allocator, "localhost");
    defer allocator.free(ipString);
    try std.testing.expectEqualStrings("127.0.0.1", ipString);
}

test "resolveIPv4AsString fails for invalid hostname" {
    const allocator = std.testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    const result = resolveIPv4AsString(allocator, "invalid.invalid");
    try std.testing.expectError(ResolverError.AddressResolutionFailure, result);
}

test "resolveIPv4AsString fails for IPv6 loopback" {
    const allocator = std.testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    const result = resolveIPv4AsString(allocator, "::1");
    try std.testing.expectError(ResolverError.UnsupportedAddressFamily, result);
}

test "resolveIPv4AsString runs for google.com" {
    const allocator = std.testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    const resolved_ip = try resolveIPv4AsString(allocator, "google.com");
    defer allocator.free(resolved_ip);
}

test "resolveIPv4AsString handles numeric hostname" {
    const allocator = std.testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    const ipString = try resolveIPv4AsString(allocator, "8.8.8.8");
    defer allocator.free(ipString);
    try std.testing.expectEqualStrings("8.8.8.8", ipString);
}

// FIXME - build env dependent
// test "resolveIPv4AsString fails for IPv6-only hostname" {
//     const allocator = std.testing.allocator;
//     try logging.init(allocator, std.std.fs.File.stderr().writer());
//     defer logging.deinit();

//     const result = resolveIPv4AsString(allocator, "ipv6.google.com");
//     try std.testing.expectError(ResolverError.UnsupportedAddressFamily, result);
// }
