//! Dymes Message.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const common = @import("dymes_common");
const logging = common.logging;
const Logger = logging.Logger;
const ulid = common.ulid;
const Ulid = common.ulid.Ulid;

const component_name = "msg.FrameAllocator";

const Self = @This();

underlying_allocator: std.mem.Allocator,
logger: *Logger,

pub fn init(underlying_allocator: std.mem.Allocator) Self {
    return .{
        .underlying_allocator = underlying_allocator,
        .logger = logging.logger(component_name),
    };
}

pub fn allocator(self: *Self) std.mem.Allocator {
    return .{
        .ptr = self,
        .vtable = &.{
            .alloc = alloc,
            .resize = resize,
            .free = free,
            .remap = remap,
        },
    };
}

const frame_alignment: std.mem.Alignment = .@"16";

fn alloc(ctx: *anyopaque, len: usize, _: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
    const self: *Self = @ptrCast(@alignCast(ctx));
    const result = self.underlying_allocator.vtable.alloc(self.underlying_allocator.ptr, len, frame_alignment, ret_addr);
    // self.logger.fine()
    //     .msg("Frame buffer allocated")
    //     .int("len", len)
    //     .log();
    return result;
}

fn resize(ctx: *anyopaque, buf: []u8, _: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
    const self: *Self = @ptrCast(@alignCast(ctx));
    const result = self.underlying_allocator.vtable.resize(self.underlying_allocator.ptr, buf, frame_alignment, new_len, ret_addr);
    // self.logger.fine()
    //     .msg("Frame buffer resized")
    //     .int("from", buf.len)
    //     .int("to", new_len)
    //     .log();
    return result;
}

fn free(ctx: *anyopaque, buf: []u8, _: std.mem.Alignment, ret_addr: usize) void {
    const self: *Self = @ptrCast(@alignCast(ctx));
    self.underlying_allocator.vtable.free(self.underlying_allocator.ptr, buf, frame_alignment, ret_addr);
    // self.logger.fine()
    //     .msg("Frame buffer freed")
    //     .int("len", buf.len)
    //     .log();
}

fn remap(ctx: *anyopaque, memory: []u8, _: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
    const self: *Self = @ptrCast(@alignCast(ctx));
    return self.underlying_allocator.vtable.remap(self.underlying_allocator.ptr, memory, frame_alignment, new_len, ret_addr);
}

const FrameAllocator = @This();

test "FrameAllocator" {
    std.debug.print("test.FrameAllocator\n", .{});
    const test_allocator = testing.allocator;
    const out_buffer = try test_allocator.alloc(u8, 2048);
    defer test_allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(test_allocator, &stderr_writer);
    defer logging.deinit();

    var frame_allocator_holder = FrameAllocator.init(test_allocator);
    const frame_allocator = frame_allocator_holder.allocator();
    // defer frame_allocator.deinit();

    const rats: []u8 = try frame_allocator.alloc(u8, 10);
    defer frame_allocator.free(rats);
}
