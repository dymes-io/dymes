//! Support for RFC3339.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const builtin = @import("builtin");
const zeit = @import("zeit");
const ulid = @import("hissylogz").ulid;
const Ulid = ulid.Ulid;
const testing = std.testing;

/// Converts a ULID to an RFC 3339 formatted timestamp string.
/// `myulid`: ULID value to convert.
/// Returns a fixed-size `[20]u8` buffer containing the RFC 3339 timestamp in UTC.
fn ulidToRFC3339(myulid: Ulid) [20]u8 {
    return epochToRFC3339(myulid.time);
}

/// Builds an RFC 3339 timestamp string for the start of the day with optional offset.
/// `input`: a string like "sod+1h30m" specifying the offset.
/// Returns a fixed-size `[28]u8` buffer with the full RFC 3339 timestamp including offset.
fn buildSodWithOffset(input: []const u8) [28]u8 {
    const current_date = getCurrentDate();
    const midnight: [9]u8 = [_]u8{ '0', '0', ':', '0', '0', ':', '0', '0', 'Z' };

    var sign: u8 = 'Z';
    var h: u8 = 0;
    var m: u8 = 0;
    var s: u8 = 0;

    if (input.len > 3) {
        var i: usize = 3;
        sign = input[i];
        i += 1;
        var num: u8 = 0;
        while (i < input.len) : (i += 1) {
            const c = input[i];
            if (c >= '0' and c <= '9') {
                num = num * 10 + (c - '0');
            } else {
                switch (c) {
                    'h' => {
                        h = num;
                        num = 0;
                    },
                    'm' => {
                        m = num;
                        num = 0;
                    },
                    's' => {
                        s = num;
                        num = 0;
                    },
                    else => {},
                }
            }
        }
    }

    var full_rfc: [28]u8 = undefined;
    @memcpy(full_rfc[0..10], current_date[0..10]);
    full_rfc[10] = 'T';

    if (sign == 'Z') {
        @memcpy(full_rfc[11..20], midnight[0..9]);
    } else {
        @memcpy(full_rfc[11..19], midnight[0..8]);
        full_rfc[19] = sign;
        full_rfc[20] = @divTrunc(h, 10) + '0';
        full_rfc[21] = @mod(h, 10) + '0';
        full_rfc[22] = ':';
        full_rfc[23] = @divTrunc(m, 10) + '0';
        full_rfc[24] = @mod(m, 10) + '0';
        full_rfc[25] = ':';
        full_rfc[26] = @divTrunc(s, 10) + '0';
        full_rfc[27] = @mod(s, 10) + '0';
    }

    return full_rfc;
}

/// Builds an RFC 3339 timestamp string for the current time with optional offset.
/// `input`: a string like "now-10m" specifying the offset.
/// Returns a fixed-size `[28]u8` buffer with the full RFC 3339 timestamp including offset.
fn buildNowWithOffset(input: []const u8) [28]u8 {
    const current_date = getCurrentDate();
    const current_time = getCurrentTime();

    var sign: u8 = 'Z';
    var h: u8 = 0;
    var m: u8 = 0;
    var s: u8 = 0;

    if (input.len > 3) {
        var i: usize = 3;
        sign = input[i];
        i += 1;
        var num: u8 = 0;
        while (i < input.len) : (i += 1) {
            const c = input[i];
            if (c >= '0' and c <= '9') {
                num = num * 10 + (c - '0');
            } else {
                switch (c) {
                    'h' => {
                        h = num;
                        num = 0;
                    },
                    'm' => {
                        m = num;
                        num = 0;
                    },
                    's' => {
                        s = num;
                        num = 0;
                    },
                    else => {},
                }
            }
        }
    }

    var full_rfc: [28]u8 = undefined;
    @memcpy(full_rfc[0..10], current_date[0..10]);
    full_rfc[10] = 'T';

    if (sign == 'Z') {
        @memcpy(full_rfc[11..20], current_time[0..9]);
    } else {
        @memcpy(full_rfc[11..19], current_time[0..8]);
        full_rfc[19] = sign;
        full_rfc[20] = @divTrunc(h, 10) + '0';
        full_rfc[21] = @mod(h, 10) + '0';
        full_rfc[22] = ':';
        full_rfc[23] = @divTrunc(m, 10) + '0';
        full_rfc[24] = @mod(m, 10) + '0';
        full_rfc[25] = ':';
        full_rfc[26] = @divTrunc(s, 10) + '0';
        full_rfc[27] = @mod(s, 10) + '0';
    }

    return full_rfc;
}

/// Checks if a string is a time-only representation (HH:MM, HH:MM:SS, or HH:MM:SSZ).
/// `input`: string to check.
/// Returns `true` if the input matches a valid time-only format, otherwise `false`.
fn isTimeOnly(input: []const u8) bool {
    if (input.len != 8 and input.len != 9 and input.len != 5) return false;
    if (input[2] != ':') return false;
    if (!std.ascii.isDigit(input[0]) or !std.ascii.isDigit(input[1])) return false;
    if (!std.ascii.isDigit(input[3]) or !std.ascii.isDigit(input[4])) return false;
    if ((input.len > 7) and (!std.ascii.isDigit(input[6]) or !std.ascii.isDigit(input[7]))) return false;
    if (input.len == 9 and input[8] != 'Z') return false;
    return true;
}

/// Pads a time-only string to the standard HH:MM:SSZ format.
/// `input`: time string like "15:30" or "15:30:45".
/// Returns a fixed-size `[9]u8` buffer with the padded time including 'Z' for UTC.
fn padTime(input: []const u8) [9]u8 {
    var out: [9]u8 = undefined;
    if (input.len == 5) {
        out[0] = input[0];
        out[1] = input[1];
        out[2] = ':';
        out[3] = input[3];
        out[4] = input[4];
        out[5] = ':';
        out[6] = '0';
        out[7] = '0';
        out[8] = 'Z';
    } else if (input.len == 8) {
        out[0] = input[0];
        out[1] = input[1];
        out[2] = ':';
        out[3] = input[3];
        out[4] = input[4];
        out[5] = ':';
        out[6] = input[6];
        out[7] = input[7];
        out[8] = 'Z';
    } else if (input.len == 9 and input[8] == 'Z') {
        out = input[0..9].*;
    } else {
        @panic("padTime: unsupported time format");
    }
    return out;
}

/// Gets the current UTC time in HH:MM:SSZ format.
/// Returns a fixed-size `[9]u8` buffer with the current time in UTC.
fn getCurrentTime() [9]u8 {
    const ms: i64 = std.time.milliTimestamp();
    const seconds: i64 = @divTrunc(ms, std.time.ms_per_s);
    const sec_today: i64 = @mod(seconds, std.time.s_per_day);

    const hours: i64 = @divTrunc(sec_today, std.time.s_per_hour);
    const minutes: i64 = @divTrunc(@mod(sec_today, std.time.s_per_hour), std.time.s_per_min);
    const secs: i64 = @mod(sec_today, std.time.s_per_min);

    var buf: [9]u8 = undefined;
    buf[0] = @as(u8, @intCast(@mod(@divTrunc(hours, 10), 10))) + '0';
    buf[1] = @as(u8, @intCast(@mod(hours, 10))) + '0';
    buf[2] = ':';
    buf[3] = @as(u8, @intCast(@mod(@divTrunc(minutes, 10), 10))) + '0';
    buf[4] = @as(u8, @intCast(@mod(minutes, 10))) + '0';
    buf[5] = ':';
    buf[6] = @as(u8, @intCast(@mod(@divTrunc(secs, 10), 10))) + '0';
    buf[7] = @as(u8, @intCast(@mod(secs, 10))) + '0';
    buf[8] = 'Z';

    return buf;
}

/// Gets the current UTC date in YYYY-MM-DD format.
/// Returns a fixed-size `[10]u8` buffer with the current date in UTC.
fn getCurrentDate() [10]u8 {
    const ms: i64 = std.time.milliTimestamp();
    const seconds: i64 = @divTrunc(ms, std.time.ms_per_s);
    var days: i64 = @divTrunc(seconds, std.time.s_per_day);

    var year: u32 = 1970;
    while (true) {
        const leap: bool = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0);
        const dim: i64 = if (leap) 366 else 365;
        if (days >= dim) {
            days -= dim;
            year += 1;
        } else break;
    }

    const month_days = [_]u32{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    var month: u32 = 1;
    while (true) {
        var dim = month_days[month - 1];
        if (month == 2 and ((year % 4 == 0 and year % 100 != 0) or (year % 400 == 0))) dim = 29;
        if (days >= dim) {
            days -= dim;
            month += 1;
        } else break;
    }

    const day: u32 = @as(u32, @intCast(days + 1));

    var buf: [10]u8 = undefined;
    buf[0] = @as(u8, @intCast((year / 1000) % 10)) + '0';
    buf[1] = @as(u8, @intCast((year / 100) % 10)) + '0';
    buf[2] = @as(u8, @intCast((year / 10) % 10)) + '0';
    buf[3] = @as(u8, @intCast(year % 10)) + '0';
    buf[4] = '-';
    buf[5] = @as(u8, @intCast((month / 10) % 10)) + '0';
    buf[6] = @as(u8, @intCast(month % 10)) + '0';
    buf[7] = '-';
    buf[8] = @as(u8, @intCast((day / 10) % 10)) + '0';
    buf[9] = @as(u8, @intCast(day % 10)) + '0';

    return buf;
}

/// Parses an input string into a `zeit.Instant`.
/// Supports ULIDs, absolute RFC 3339 timestamps, "now" with offsets, "sod" with offsets, or time-only strings.
/// `input`: string representation of the instant.
/// Returns a `zeit.Instant` corresponding to the parsed time.
pub fn parseInstant(input: []const u8) !zeit.Instant {
    if (input.len == 26) {
        const decoded = try ulid.decode(input);
        const rfc3339_buf = ulidToRFC3339(decoded);
        return try zeit.instant(.{ .source = .{ .rfc3339 = rfc3339_buf[0..20] } });
    }

    if (input.len >= 3 and std.mem.eql(u8, input[0..3], "now")) {
        const full_rfc = buildNowWithOffset(input);
        const end_len: usize = if (input.len > 4) 28 else 20;
        const rfc3339_instant = try zeit.instant(.{ .source = .{ .rfc3339 = full_rfc[0..end_len] } });
        return rfc3339_instant;
    }

    if (input.len >= 3 and std.mem.eql(u8, input[0..3], "sod")) {
        const full_rfc = buildSodWithOffset(input);
        const end_len: usize = if (input.len > 4) 28 else 20;
        const rfc3339_instant = try zeit.instant(.{ .source = .{ .rfc3339 = full_rfc[0..end_len] } });
        return rfc3339_instant;
    }

    const is_time_only: bool = isTimeOnly(input);
    if (is_time_only) {
        const full_time = padTime(input);
        const current_date = getCurrentDate();
        var full_rfc: [20]u8 = undefined;
        @memcpy(full_rfc[0..10], current_date[0..10]);
        full_rfc[10] = 'T';
        @memcpy(full_rfc[11..20], full_time[0..9]);
        const rfc3339_instant = try zeit.instant(.{ .source = .{ .rfc3339 = full_rfc[0..20] } });
        return rfc3339_instant;
    } else {
        const rfc3339_instant = try zeit.instant(.{ .source = .{ .rfc3339 = input } });
        return rfc3339_instant;
    }
}

/// Parses an input epoch timestamp and returns a full RFC3339 equivalent
pub fn epochToRFC3339(epoch_time_ms: i64) [20]u8 {
    const seconds: i64 = @divTrunc(epoch_time_ms, std.time.ms_per_s);
    const sec_today: i64 = @mod(seconds, std.time.s_per_day);
    var days: i64 = @divTrunc(seconds, std.time.s_per_day);
    const hours: i64 = @divTrunc(sec_today, std.time.s_per_hour);
    const minutes: i64 = @divTrunc(@mod(sec_today, std.time.s_per_hour), std.time.s_per_min);
    const secs: i64 = @mod(sec_today, std.time.s_per_min);
    var year: u32 = 1970;
    while (true) {
        const leap: bool = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0);
        const dim: i64 = if (leap) 366 else 365;
        if (days >= dim) {
            days -= dim;
            year += 1;
        } else break;
    }

    const month_days = [_]u32{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    var month: u32 = 1;
    while (true) {
        var dim = month_days[month - 1];
        if (month == 2 and ((year % 4 == 0 and year % 100 != 0) or (year % 400 == 0))) dim = 29;
        if (days >= dim) {
            days -= dim;
            month += 1;
        } else break;
    }

    const day: u32 = @as(u32, @intCast(days + 1));

    var buf: [20]u8 = undefined;
    buf[0] = @as(u8, @intCast((year / 1000) % 10)) + '0';
    buf[1] = @as(u8, @intCast((year / 100) % 10)) + '0';
    buf[2] = @as(u8, @intCast((year / 10) % 10)) + '0';
    buf[3] = @as(u8, @intCast(year % 10)) + '0';
    buf[4] = '-';
    buf[5] = @as(u8, @intCast((month / 10) % 10)) + '0';
    buf[6] = @as(u8, @intCast(month % 10)) + '0';
    buf[7] = '-';
    buf[8] = @as(u8, @intCast((day / 10) % 10)) + '0';
    buf[9] = @as(u8, @intCast(day % 10)) + '0';
    buf[10] = 'T';
    buf[11] = @as(u8, @intCast(@mod(@divTrunc(hours, 10), 10))) + '0';
    buf[12] = @as(u8, @intCast(@mod(hours, 10))) + '0';
    buf[13] = ':';
    buf[14] = @as(u8, @intCast(@mod(@divTrunc(minutes, 10), 10))) + '0';
    buf[15] = @as(u8, @intCast(@mod(minutes, 10))) + '0';
    buf[16] = ':';
    buf[17] = @as(u8, @intCast(@mod(@divTrunc(secs, 10), 10))) + '0';
    buf[18] = @as(u8, @intCast(@mod(secs, 10))) + '0';
    buf[19] = 'Z';

    return buf;
}

/// Parses an input epoch timestamp and returns a full RFC3339 equivalent
pub fn epoch128ToRFC3339(epoch_time: i128) [20]u8 {
    const ns = @as(i64, @intCast(epoch_time));
    const ms: i64 = @as(i64, @divTrunc(ns, std.time.ns_per_ms));
    const seconds: i64 = @divTrunc(ms, std.time.ms_per_s);
    const sec_today: i64 = @mod(seconds, std.time.s_per_day);
    var days: i64 = @divTrunc(seconds, std.time.s_per_day);
    const hours: i64 = @divTrunc(sec_today, std.time.s_per_hour);
    const minutes: i64 = @divTrunc(@mod(sec_today, std.time.s_per_hour), std.time.s_per_min);
    const secs: i64 = @mod(sec_today, std.time.s_per_min);
    var year: u32 = 1970;
    while (true) {
        const leap: bool = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0);
        const dim: i64 = if (leap) 366 else 365;
        if (days >= dim) {
            days -= dim;
            year += 1;
        } else break;
    }

    const month_days = [_]u32{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    var month: u32 = 1;
    while (true) {
        var dim = month_days[month - 1];
        if (month == 2 and ((year % 4 == 0 and year % 100 != 0) or (year % 400 == 0))) dim = 29;
        if (days >= dim) {
            days -= dim;
            month += 1;
        } else break;
    }

    const day: u32 = @as(u32, @intCast(days + 1));

    var buf: [20]u8 = undefined;
    buf[0] = @as(u8, @intCast((year / 1000) % 10)) + '0';
    buf[1] = @as(u8, @intCast((year / 100) % 10)) + '0';
    buf[2] = @as(u8, @intCast((year / 10) % 10)) + '0';
    buf[3] = @as(u8, @intCast(year % 10)) + '0';
    buf[4] = '-';
    buf[5] = @as(u8, @intCast((month / 10) % 10)) + '0';
    buf[6] = @as(u8, @intCast(month % 10)) + '0';
    buf[7] = '-';
    buf[8] = @as(u8, @intCast((day / 10) % 10)) + '0';
    buf[9] = @as(u8, @intCast(day % 10)) + '0';
    buf[10] = 'T';
    buf[11] = @as(u8, @intCast(@mod(@divTrunc(hours, 10), 10))) + '0';
    buf[12] = @as(u8, @intCast(@mod(hours, 10))) + '0';
    buf[13] = ':';
    buf[14] = @as(u8, @intCast(@mod(@divTrunc(minutes, 10), 10))) + '0';
    buf[15] = @as(u8, @intCast(@mod(minutes, 10))) + '0';
    buf[16] = ':';
    buf[17] = @as(u8, @intCast(@mod(@divTrunc(secs, 10), 10))) + '0';
    buf[18] = @as(u8, @intCast(@mod(secs, 10))) + '0';
    buf[19] = 'Z';

    return buf;
}

//remove space after sod/now and the offset
test "parseInstant only year" {
    const expected_instant: zeit.Instant = try zeit.instant(.{
        .source = .{
            .rfc3339 = "2025-01-01T00:00:00Z",
        },
    });
    try testing.expectEqual(expected_instant, try parseInstant("2025"));
}

test "parseInstant year and month" {
    const expected_instant: zeit.Instant = try zeit.instant(.{
        .source = .{
            .rfc3339 = "2025-09-01T00:00:00Z",
        },
    });
    try testing.expectEqual(expected_instant, try parseInstant("2025-09"));
}

test "parseInstant full date" {
    const expected_instant: zeit.Instant = try zeit.instant(.{
        .source = .{
            .rfc3339 = "2025-09-05T00:00:00Z",
        },
    });
    try testing.expectEqual(expected_instant, try parseInstant("2025-09-05"));
}

test "parseInstant full rfc no offset" {
    const expected_instant: zeit.Instant = try zeit.instant(.{
        .source = .{
            .rfc3339 = "2025-09-15T13:45:30Z",
        },
    });
    try testing.expectEqual(expected_instant, try parseInstant("2025-09-15T13:45:30Z"));
}

test "parseInstant full rfc offset 1" {
    const expected_instant: zeit.Instant = try zeit.instant(.{
        .source = .{
            .rfc3339 = "2025-09-15T13:45:30+09:00",
        },
    });
    try testing.expectEqual(expected_instant, try parseInstant("2025-09-15T13:45:30+09:00"));
}

test "parseInstant full rfc offset 2" {
    const expected_instant: zeit.Instant = try zeit.instant(.{
        .source = .{
            .rfc3339 = "2025-09-15T13:45:30-12:30",
        },
    });
    try testing.expectEqual(expected_instant, try parseInstant("2025-09-15T13:45:30-12:30"));
}

test "parseInstant full rfc offset 3" {
    const expected_instant: zeit.Instant = try zeit.instant(.{
        .source = .{
            .rfc3339 = "2025-09-15T13:45:30-12:30:15",
        },
    });
    try testing.expectEqual(expected_instant, try parseInstant("2025-09-15T13:45:30-12:30:15"));
}

test "parseInstant time only HH:MM:SSZ" {
    const input = "15:30:45Z";
    const parsed = try parseInstant(input);

    const current_date = getCurrentDate();
    var expected_rfc: [20]u8 = undefined;
    @memcpy(expected_rfc[0..10], current_date[0..10]);
    expected_rfc[10] = 'T';
    @memcpy(expected_rfc[11..20], padTime(input)[0..9]);

    const expected_instant = try zeit.instant(.{
        .source = .{ .rfc3339 = expected_rfc[0..20] },
    });

    try testing.expectEqual(expected_instant, parsed);
}

test "parseInstant time only HH:MM:SS" {
    const input = "15:30:45";
    const parsed = try parseInstant(input);

    const current_date = getCurrentDate();
    var expected_rfc: [20]u8 = undefined;
    @memcpy(expected_rfc[0..10], current_date[0..10]);
    expected_rfc[10] = 'T';
    @memcpy(expected_rfc[11..20], padTime(input)[0..9]);

    const expected_instant = try zeit.instant(.{
        .source = .{ .rfc3339 = expected_rfc[0..20] },
    });

    try testing.expectEqual(expected_instant, parsed);
}

test "parseInstant time only HH:MM" {
    const input = "15:30";
    const parsed = try parseInstant(input);

    const current_date = getCurrentDate();
    var expected_rfc: [20]u8 = undefined;
    @memcpy(expected_rfc[0..10], current_date[0..10]);
    expected_rfc[10] = 'T';
    @memcpy(expected_rfc[11..20], padTime(input)[0..9]);

    const expected_instant = try zeit.instant(.{
        .source = .{ .rfc3339 = expected_rfc[0..20] },
    });

    try testing.expectEqual(expected_instant, parsed);
}

test "parseInstant now" {
    const parsed = try parseInstant("now");

    const current_date = getCurrentDate();
    const current_time = getCurrentTime();

    var expected_rfc: [20]u8 = undefined;
    @memcpy(expected_rfc[0..10], current_date[0..10]);
    expected_rfc[10] = 'T';
    @memcpy(expected_rfc[11..20], current_time[0..9]);

    const expected_instant = try zeit.instant(.{ .source = .{ .rfc3339 = expected_rfc[0..20] } });
    try testing.expectEqual(expected_instant, parsed);
}

test "parseInstant sod" {
    const parsed = try parseInstant("sod");

    const current_date = getCurrentDate();
    var expected_rfc: [20]u8 = undefined;
    @memcpy(expected_rfc[0..10], current_date[0..10]);
    expected_rfc[10] = 'T';

    const midnight: [9]u8 = [_]u8{ '0', '0', ':', '0', '0', ':', '0', '0', 'Z' };
    @memcpy(expected_rfc[11..20], midnight[0..9]);

    const expected_instant = try zeit.instant(.{ .source = .{ .rfc3339 = expected_rfc[0..20] } });
    try testing.expectEqual(expected_instant, parsed);
}

test "parseInstant now offset 1" {
    const parsed = try parseInstant("now+1h");

    const current_date = getCurrentDate();
    const current_time = getCurrentTime();

    var expected_rfc: [28]u8 = undefined;
    @memcpy(expected_rfc[0..10], current_date[0..10]);
    expected_rfc[10] = 'T';
    @memcpy(expected_rfc[11..19], current_time[0..8]);
    expected_rfc[19] = '+';
    expected_rfc[20] = '0';
    expected_rfc[21] = '1';
    expected_rfc[22] = ':';
    expected_rfc[23] = '0';
    expected_rfc[24] = '0';
    expected_rfc[25] = ':';
    expected_rfc[26] = '0';
    expected_rfc[27] = '0';

    const expected_instant = try zeit.instant(.{ .source = .{ .rfc3339 = expected_rfc[0..28] } });
    try testing.expectEqual(expected_instant, parsed);
}

test "parseInstant now offset 2" {
    const parsed = try parseInstant("now-10m");

    const current_date = getCurrentDate();
    const current_time = getCurrentTime();

    var expected_rfc: [28]u8 = undefined;
    @memcpy(expected_rfc[0..10], current_date[0..10]);
    expected_rfc[10] = 'T';
    @memcpy(expected_rfc[11..19], current_time[0..8]);
    expected_rfc[19] = '-';
    expected_rfc[20] = '0';
    expected_rfc[21] = '0';
    expected_rfc[22] = ':';
    expected_rfc[23] = '1';
    expected_rfc[24] = '0';
    expected_rfc[25] = ':';
    expected_rfc[26] = '0';
    expected_rfc[27] = '0';

    const expected_instant = try zeit.instant(.{ .source = .{ .rfc3339 = expected_rfc[0..28] } });
    try testing.expectEqual(expected_instant, parsed);
}

test "parseInstant now offset 3" {
    const parsed = try parseInstant("now+1h30m25s");

    const current_date = getCurrentDate();
    const current_time = getCurrentTime();

    var expected_rfc: [28]u8 = undefined;
    @memcpy(expected_rfc[0..10], current_date[0..10]);
    expected_rfc[10] = 'T';
    @memcpy(expected_rfc[11..19], current_time[0..8]);
    expected_rfc[19] = '+';
    expected_rfc[20] = '0';
    expected_rfc[21] = '1';
    expected_rfc[22] = ':';
    expected_rfc[23] = '3';
    expected_rfc[24] = '0';
    expected_rfc[25] = ':';
    expected_rfc[26] = '2';
    expected_rfc[27] = '5';

    const expected_instant = try zeit.instant(.{ .source = .{ .rfc3339 = expected_rfc[0..28] } });
    try testing.expectEqual(expected_instant, parsed);
}

test "parseInstant now offset 4" {
    const parsed = try parseInstant("now-1h30m25s");

    const current_date = getCurrentDate();
    const current_time = getCurrentTime();

    var expected_rfc: [28]u8 = undefined;
    @memcpy(expected_rfc[0..10], current_date[0..10]);
    expected_rfc[10] = 'T';
    @memcpy(expected_rfc[11..19], current_time[0..8]);
    expected_rfc[19] = '-';
    expected_rfc[20] = '0';
    expected_rfc[21] = '1';
    expected_rfc[22] = ':';
    expected_rfc[23] = '3';
    expected_rfc[24] = '0';
    expected_rfc[25] = ':';
    expected_rfc[26] = '2';
    expected_rfc[27] = '5';

    const expected_instant = try zeit.instant(.{ .source = .{ .rfc3339 = expected_rfc[0..28] } });
    try testing.expectEqual(expected_instant, parsed);
}

test "parseInstant sod offset 1" {
    const parsed = try parseInstant("sod+30s");

    const current_date = getCurrentDate();

    var expected_rfc: [28]u8 = undefined;
    @memcpy(expected_rfc[0..10], current_date[0..10]);
    expected_rfc[10] = 'T';
    expected_rfc[11] = '0';
    expected_rfc[12] = '0';
    expected_rfc[13] = ':';
    expected_rfc[14] = '0';
    expected_rfc[15] = '0';
    expected_rfc[16] = ':';
    expected_rfc[17] = '0';
    expected_rfc[18] = '0';
    expected_rfc[19] = '+';
    expected_rfc[20] = '0';
    expected_rfc[21] = '0';
    expected_rfc[22] = ':';
    expected_rfc[23] = '0';
    expected_rfc[24] = '0';
    expected_rfc[25] = ':';
    expected_rfc[26] = '3';
    expected_rfc[27] = '0';

    const expected_instant = try zeit.instant(.{ .source = .{ .rfc3339 = expected_rfc[0..28] } });
    try testing.expectEqual(expected_instant, parsed);
}

test "parseInstant sod offset 2" {
    const parsed = try parseInstant("sod-20s");

    const current_date = getCurrentDate();

    var expected_rfc: [28]u8 = undefined;
    @memcpy(expected_rfc[0..10], current_date[0..10]);
    expected_rfc[10] = 'T';
    expected_rfc[11] = '0';
    expected_rfc[12] = '0';
    expected_rfc[13] = ':';
    expected_rfc[14] = '0';
    expected_rfc[15] = '0';
    expected_rfc[16] = ':';
    expected_rfc[17] = '0';
    expected_rfc[18] = '0';
    expected_rfc[19] = '-';
    expected_rfc[20] = '0';
    expected_rfc[21] = '0';
    expected_rfc[22] = ':';
    expected_rfc[23] = '0';
    expected_rfc[24] = '0';
    expected_rfc[25] = ':';
    expected_rfc[26] = '2';
    expected_rfc[27] = '0';

    const expected_instant = try zeit.instant(.{ .source = .{ .rfc3339 = expected_rfc[0..28] } });
    try testing.expectEqual(expected_instant, parsed);
}

test "parseInstant sod offset 3" {
    const parsed = try parseInstant("sod+1h30m25s");

    const current_date = getCurrentDate();

    var expected_rfc: [28]u8 = undefined;
    @memcpy(expected_rfc[0..10], current_date[0..10]);
    expected_rfc[10] = 'T';
    expected_rfc[11] = '0';
    expected_rfc[12] = '0';
    expected_rfc[13] = ':';
    expected_rfc[14] = '0';
    expected_rfc[15] = '0';
    expected_rfc[16] = ':';
    expected_rfc[17] = '0';
    expected_rfc[18] = '0';
    expected_rfc[19] = '+';
    expected_rfc[20] = '0';
    expected_rfc[21] = '1';
    expected_rfc[22] = ':';
    expected_rfc[23] = '3';
    expected_rfc[24] = '0';
    expected_rfc[25] = ':';
    expected_rfc[26] = '2';
    expected_rfc[27] = '5';

    const expected_instant = try zeit.instant(.{ .source = .{ .rfc3339 = expected_rfc[0..28] } });
    try testing.expectEqual(expected_instant, parsed);
}

test "parseInstant sod offset 4" {
    const parsed = try parseInstant("sod-1h30m25s");

    const current_date = getCurrentDate();

    var expected_rfc: [28]u8 = undefined;
    @memcpy(expected_rfc[0..10], current_date[0..10]);
    expected_rfc[10] = 'T';
    expected_rfc[11] = '0';
    expected_rfc[12] = '0';
    expected_rfc[13] = ':';
    expected_rfc[14] = '0';
    expected_rfc[15] = '0';
    expected_rfc[16] = ':';
    expected_rfc[17] = '0';
    expected_rfc[18] = '0';
    expected_rfc[19] = '-';
    expected_rfc[20] = '0';
    expected_rfc[21] = '1';
    expected_rfc[22] = ':';
    expected_rfc[23] = '3';
    expected_rfc[24] = '0';
    expected_rfc[25] = ':';
    expected_rfc[26] = '2';
    expected_rfc[27] = '5';

    const expected_instant = try zeit.instant(.{ .source = .{ .rfc3339 = expected_rfc[0..28] } });
    try testing.expectEqual(expected_instant, parsed);
}

test "parseInstant ulid" {
    const expected_instant: zeit.Instant = try zeit.instant(.{
        .source = .{
            .rfc3339 = "2021-06-20T10:10:18Z",
        },
    });
    try testing.expectEqual(expected_instant, try parseInstant("01F8MECHZX3TBDSZ7XRADM79XV"));
}

test "epoch128ToRFC3339" {
    const epoch_time: i128 = 1758109543808751000;
    const expected_rfc3339_timestamp = "2025-09-17T11:45:43Z";
    const rfc3339_timestamp = epoch128ToRFC3339(epoch_time);
    try testing.expectEqualStrings(expected_rfc3339_timestamp, rfc3339_timestamp[0..]);
}

test "epochToRFC3339" {
    const epoch_time_ms: i64 = 1758109543000;
    const expected_rfc3339_timestamp = "2025-09-17T11:45:43Z";
    const rfc3339_timestamp = epochToRFC3339(epoch_time_ms);
    try testing.expectEqualStrings(expected_rfc3339_timestamp, rfc3339_timestamp[0..]);
}
