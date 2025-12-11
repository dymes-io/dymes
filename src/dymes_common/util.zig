//! Common utility functions.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;
const builtin = @import("builtin");
const native_os = builtin.os.tag;

const Ulid = @import("hissylogz").ulid.Ulid;
const concurrency = @import("util/concurrency.zig");
pub const Future = @import("util/concurrency.zig").Future;
pub const LifeCycleControl = @import("util/LifecycleControl.zig");

/// Returns true if `ulid_l` falls before `ulid_r` in time.
pub fn isBeforeUlid(ulid_l: Ulid, ulid_r: Ulid) bool {
    if (ulid_l.time < ulid_r.time) {
        return true;
    } else if (ulid_l.time == ulid_r.time and ulid_l.rand < ulid_r.rand) {
        return true;
    }
    return false;
}

/// Checks that a type does not have implicit padding.
///
/// This modified function is Copyright © 2024 TigerBeetle, Inc. All Rights Reserved.
/// Included under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)
pub fn checkNoPadding(comptime T: type) bool {
    comptime switch (@typeInfo(T)) {
        .void => return true,
        .int => return @bitSizeOf(T) == 8 * @sizeOf(T),
        .array => |info| return checkNoPadding(info.child),
        .@"struct" => |info| {
            switch (info.layout) {
                .auto => return false,
                .@"extern" => {
                    for (info.fields) |field| {
                        if (!checkNoPadding(field.type)) return false;
                    }

                    // Check offsets of u128 and pseudo-u256 fields.
                    for (info.fields) |field| {
                        if (field.type == u128) {
                            const offset = @offsetOf(T, field.name);
                            if (offset % @sizeOf(u128) != 0) return false;

                            if (@hasField(T, field.name ++ "_padding")) {
                                if (offset % @sizeOf(u256) != 0) return false;
                                if (offset + @sizeOf(u128) !=
                                    @offsetOf(T, field.name ++ "_padding"))
                                {
                                    return false;
                                }
                            }
                        }
                    }

                    var offset = 0;
                    for (info.fields) |field| {
                        const field_offset = @offsetOf(T, field.name);
                        if (offset != field_offset) return false;
                        offset += @sizeOf(field.type);
                    }
                    return offset == @sizeOf(T);
                },
                .@"packed" => return @bitSizeOf(T) == 8 * @sizeOf(T),
            }
        },
        .@"enum" => |info| {
            maybe(info.is_exhaustive);
            return checkNoPadding(info.tag_type);
        },
        .pointer => return false,
        .@"union" => return false,
        else => return false,
    };
}

test checkNoPadding {
    comptime for (.{
        u8,
        extern struct { x: u8 },
        packed struct { x: u7, y: u1 },
        extern struct { x: extern struct { y: u64, z: u64 } },
        enum(u8) { x },
    }) |T| {
        assert(checkNoPadding(T));
    };

    comptime for (.{
        u7,
        struct { x: u7 },
        struct { x: u8 },
        struct { x: u64, y: u32 },
        extern struct { x: extern struct { y: u64, z: u32 } },
        packed struct { x: u7 },
        enum(u7) { x },
    }) |T| {
        assert(!checkNoPadding(T));
    };
}

/// `maybe` is the dual of `assert`: it signals that condition is sometimes true
///  and sometimes false.
///
/// This function is Copyright © 2024 TigerBeetle, Inc. All Rights Reserved.
/// Included under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)
pub fn maybe(ok: bool) void {
    assert(ok or !ok);
}
