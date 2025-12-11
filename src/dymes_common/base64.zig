//! Base64 utility functions.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const builtin = @import("builtin");
const native_os = builtin.os.tag;
const base64 = std.base64;

const base64_codec = base64.url_safe;

/// Base64 Errors
const Error = error{
    /// An invalid argument was passed
    InvalidArgument,
    /// Memory allocation failed
    OutOfMemory,
};

/// Decodes a Base-64 `encoded` value and returns a decoded slice, allocated using `allocator`.
///
/// It's the caller's responsibility to free the decoded slice.
///
/// Note: By default, we use a URL-safe, non-padding codec.
pub fn decode(allocator: std.mem.Allocator, encoded: []const u8) Error![]const u8 {
    const size = base64_codec.Decoder.calcSizeForSlice(encoded) catch return error.InvalidArgument;
    const decoded = allocator.alloc(u8, size) catch return Error.OutOfMemory;
    errdefer allocator.free(decoded);
    base64_codec.Decoder.decode(decoded, encoded) catch return Error.InvalidArgument;
    return decoded;
}

/// Encodes an `unencoded` value, and returns a Base-64 encoded slice, allocated using `allocator`.
///
/// It's the caller's responsibility to free the encoded slice.
///
/// Note: By default, we use a URL-safe, non-padding codec.
pub fn encode(allocator: std.mem.Allocator, unencoded: []const u8) Error![]const u8 {
    const size = base64_codec.Encoder.calcSize(unencoded.len);
    const encoded = allocator.alloc(u8, size) catch return Error.OutOfMemory;
    errdefer allocator.free(encoded);
    _ = base64_codec.Encoder.encode(encoded, unencoded);
    return encoded;
}

test "encode and decode base64" {
    const allocator = std.testing.allocator;
    std.debug.print("test.base64\n", .{});

    const original: []const u8 = "Payload text";
    std.debug.print("\toriginal={s}\n", .{original});

    const encoded = try encode(allocator, original);
    defer allocator.free(encoded);
    std.debug.print("\tencoded={s}\n", .{encoded});

    const decoded = try decode(allocator, encoded);
    defer allocator.free(decoded);
    std.debug.print("\tdecoded={s}\n", .{decoded});

    try testing.expectEqualSlices(u8, original, decoded);

    if (decode(allocator, "Garbage")) |gigo| {
        std.debug.print("\tgigo={s}\n", .{gigo});
        defer allocator.free(gigo);
    } else |err| {
        try testing.expect(Error.InvalidArgument == err);
    }
}
