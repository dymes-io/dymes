//! Testing helpers.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const debug = std.debug;
const assert = debug.assert;

const dymes_common = @import("dymes_common");
const logging = dymes_common.logging;

const dymes_msg = @import("dymes_msg");

const dymes_errors = @import("dymes_common").errors;
const AllocationError = dymes_errors.AllocationError;
const UsageError = dymes_errors.UsageError;

const vsr_msg = @import("vsr_msg.zig");
const ops = @import("ops.zig");
const wire = @import("wire.zig");
const Allocators = wire.Allocators;

pub fn createTestAppendOp(allocs: Allocators, ulid_generator: *dymes_common.ulid.Generator, log_sequence_no: u64) !ops.VsrOp {
    const msg_to_append_id = try ulid_generator.next();

    const msg_to_append_payload = "I want to be appended";

    const msg_to_append = try dymes_msg.Message.init(
        allocs.msg_frame_allocator,
        log_sequence_no,
        msg_to_append_id,
        702,
        0,
        msg_to_append_payload,
        .{},
    );
    defer msg_to_append.deinit(allocs.msg_frame_allocator);

    return .{ .append = try ops.VsrAppendOp.init(allocs.msg_frame_allocator, msg_to_append) };
}

pub fn createTestVsrMessage(
    allocs: Allocators,
    ulid_generator: *dymes_common.ulid.Generator,
    log_sequence_no: u64,
    op_no: wire.OpNumber,
    view_no: wire.ViewNumber,
) !vsr_msg.VsrMessage {
    var vsr_op_log = ops.VsrOpLog.init();
    defer vsr_op_log.deinit(allocs);

    {
        const vsr_op = try createTestAppendOp(allocs, ulid_generator, log_sequence_no);
        defer vsr_op.deinit(allocs);

        const vsr_op_response: ops.VsrOpResponse = .{ .append = .{ .msg_ulid = ulid_generator.last } };
        try vsr_op_log.addEntry(allocs, log_sequence_no, vsr_op, vsr_op_response);
    }

    var vsr_sv = try vsr_msg.VsrStartView.init(allocs, view_no, op_no, op_no, vsr_op_log);
    defer vsr_sv.deinit(allocs);

    return .{ .vsr_sv = try vsr_sv.clone(allocs) };
}

pub fn createTestRequest(
    allocs: Allocators,
    ulid_generator: *dymes_common.ulid.Generator,
    log_sequence_no: u64,
    client_id: u64,
    request_no: u64,
) !vsr_msg.VsrMessage {
    const vsr_op = try createTestAppendOp(allocs, ulid_generator, log_sequence_no);
    defer vsr_op.deinit(allocs);

    var vsr_req = try vsr_msg.VsrRequest.init(allocs, client_id, request_no, vsr_op);
    defer vsr_req.deinit(allocs);

    return .{ .vsr_request = try vsr_req.clone(allocs) };
}
