//! Add conversion of _current_ Ulid zig struct to "canonical" (i.e. big-endian) _binary_ ULID representation in 16 bytes.
//! *NOT* referring to canonical _textual_ representation in 26 bytes, which is fine as-is.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const common = @import("dymes_common");
const logging = common.logging;
const common_ulid = common.ulid;
const Ulid = common.ulid.Ulid;

const FrameAllocator = @import("FrameAllocator.zig");

/// Convert the ULID into a canonical big-endian binary ULID byte array.
pub fn marshalUlid(ulid: Ulid) [16]u8 {
    return @as([16]u8, @bitCast(std.mem.nativeToBig(u128, ulid.asInt())));
}

/// Convert a canonical big-endian binary ULID to mative ULID
pub fn unmarshalUlid(ulid: Ulid) Ulid {
    const native_ulid = std.mem.bigToNative(u128, @as(u128, ulid.asInt()));
    return @bitCast(native_ulid);
}

/// Convert a canonical big-endian binary ULID byte array to ULID
pub fn unmarshalUlidBytes(bytes: [16]u8) Ulid {
    const native_ulid = std.mem.bigToNative(u128, @as(u128, @bitCast(bytes)));
    return @bitCast(native_ulid);
}

test "marshalling" {
    std.debug.print("test.ulid.marshalling\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var ulid_generator = common.ulid.generator();

    var logger = logging.logger("test.ulid.marshalling");

    const native_ulid = try ulid_generator.next();
    const canonical_ulid = marshalUlid(native_ulid);
    const marshalled_ulid = try common_ulid.fromBytes(&canonical_ulid);
    const unmarshalled_ulid = unmarshalUlidBytes(canonical_ulid);
    const unmarshalled_ulid_too = unmarshalUlid(marshalled_ulid);

    logger.debug()
        .msg("ULID marshalling")
        .intb("native_ulid", native_ulid.asInt())
        .intb("marshalled", marshalled_ulid.asInt())
        .intb("unmarshalled", unmarshalled_ulid.asInt())
        .log();

    try testing.expectEqual(native_ulid, unmarshalled_ulid);
    try testing.expectEqual(native_ulid, unmarshalled_ulid_too);
}
