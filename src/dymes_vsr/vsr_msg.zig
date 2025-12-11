//! Dymes VSR protocol messages.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const debug = std.debug;
const assert = debug.assert;

const common = @import("dymes_common");
const logging = common.logging;

const constants = @import("constants.zig");
const wire = @import("wire.zig");
const ops = @import("ops.zig");
const dymes_msg = @import("dymes_msg");
const limits = @import("limits.zig");

// Log level for embedded tests
const module_tests_log_level = logging.LogLevel.none;

const AllocationError = wire.AllocationError;
const MarshallingError = wire.MarshallingError;
const UnmarshallingError = wire.UnmarshallingError;

const OpNumber = wire.OpNumber;
const CommitNumber = wire.CommitNumber;
const ReplicaNumber = wire.ReplicaNumber;
const ClientId = wire.ClientId;
const RequestNumber = wire.RequestNumber;
const ViewNumber = wire.ViewNumber;

const Allocators = wire.Allocators;

/// `Client request` message.
///
/// Triggers `VsrPrepare`.
pub const VsrRequest = struct {
    /// Client identifier (client-id)
    client_id: ClientId,

    /// Request number (request-num)
    request_no: RequestNumber,

    /// Operation to perform (op)
    op: ops.VsrOp,

    /// Initializes the `client request` message.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn init(allocs: Allocators, client_id: ClientId, request_no: RequestNumber, op: ops.VsrOp) AllocationError!VsrRequest {
        return .{
            .client_id = client_id,
            .request_no = request_no,
            .op = try op.clone(allocs),
        };
    }

    /// De-initializes the `client request` message, releasing resources.
    pub fn deinit(self: *const VsrRequest, allocs: Allocators) void {
        defer self.op.deinit(allocs);
    }

    /// Clones this `client request` message.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn clone(self: *const VsrRequest, allocs: Allocators) AllocationError!VsrRequest {
        return .{
            .client_id = self.client_id,
            .request_no = self.request_no,
            .op = try self.op.clone(allocs),
        };
    }

    /// Renders the `client request` message to a human-friendly format.
    pub fn format(self: *const VsrRequest, writer: *std.Io.Writer) !void {
        try writer.print("{{client_id={d},request_no={d},op={f}}}", .{
            self.client_id,
            self.request_no,
            self.op,
        });
    }

    /// Calculates the marshalled message size
    pub inline fn calcSize(self: *const VsrRequest) usize {
        const pre_calced: usize = @sizeOf(ClientId) + @sizeOf(RequestNumber);
        return pre_calced + self.op.calcSize();
    }

    /// Marshals the in-memory representation of the `client request` message wire format, returning number of bytes written.
    pub fn marshal(self: *const VsrRequest, writer: *std.Io.Writer) MarshallingError!usize {
        const pre_calced: usize = @sizeOf(ClientId) + @sizeOf(RequestNumber);
        writer.writeInt(ClientId, self.client_id, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(RequestNumber, self.request_no, .little) catch return MarshallingError.WriteFailed;
        return pre_calced + try self.op.marshal(writer);
    }

    /// Unmarshals the `client request` message wire format into an in-memory representation.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn unmarshal(allocs: Allocators, reader: *std.Io.Reader) UnmarshallingError!VsrRequest {
        const client_id = reader.takeInt(ClientId, .little) catch return UnmarshallingError.ReadFailed;
        const request_no = reader.takeInt(RequestNumber, .little) catch return UnmarshallingError.ReadFailed;
        const op = try ops.VsrOp.unmarshal(allocs, reader);
        errdefer op.deinit(allocs);

        return .{
            .client_id = client_id,
            .request_no = request_no,
            .op = op,
        };
    }
};

// Test VsrRequest wire format (marshalling/unmarshalling)
test "VsrRequest.wire" {
    std.debug.print("test.VsrRequest.wire\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = module_tests_log_level;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var frame_allocator_holder = dymes_msg.FrameAllocator.init(allocator);
    const frame_allocator = frame_allocator_holder.allocator();

    const allocs: Allocators = .{
        .gpa = allocator,
        .msg_frame_allocator = frame_allocator,
    };

    var ulid_generator = common.ulid.generator();

    var logger = logging.logger("test.VsrRequest.wire");

    logger.debug()
        .msg("Preparing request")
        .log();

    const msg_to_append_id = try ulid_generator.next();
    const msg_to_append_payload = "I want to be appended";

    const log_sequence_no: OpNumber = 0;
    const msg_to_append = try dymes_msg.Message.init(frame_allocator, log_sequence_no, msg_to_append_id, 702, 0, msg_to_append_payload, .{});
    defer msg_to_append.deinit(frame_allocator);

    const append_op = try ops.VsrAppendOp.init(frame_allocator, msg_to_append);

    const client_id: ClientId = 1066;
    const client_request_no: RequestNumber = 1234;

    const vsr_op: ops.VsrOp = .{ .append = append_op };
    defer vsr_op.deinit(allocs);

    const vsr_request = try VsrRequest.init(allocs, client_id, client_request_no, vsr_op);
    defer vsr_request.deinit(allocs);

    logger.debug()
        .msg("Marshalling request")
        .any("vsr_request", vsr_request)
        .log();

    const wire_buffer = try allocator.alloc(u8, vsr_request.calcSize());
    defer allocator.free(wire_buffer);

    var wire_writer = std.Io.Writer.fixed(wire_buffer);
    const cb_marshalled = try vsr_request.marshal(&wire_writer);
    assert(cb_marshalled == vsr_request.calcSize());

    try wire_writer.flush();

    var wire_reader = std.Io.Reader.fixed(wire_buffer);
    const unmarshalled_request = try VsrRequest.unmarshal(allocs, &wire_reader);
    defer unmarshalled_request.deinit(allocs);

    logger.debug()
        .msg("Unmarshalled request")
        .any("unmarshalled_request", unmarshalled_request)
        .log();

    try testing.expectEqual(vsr_request.client_id, unmarshalled_request.client_id);
    try testing.expectEqual(vsr_request.request_no, unmarshalled_request.request_no);
    try testing.expectEqualStrings(vsr_request.op.append.msg_frame_buf, unmarshalled_request.op.append.msg_frame_buf);
}

/// `Prepare` message used to notify replicas about a new request.
///
/// The primary uses the `VsrPrepare` message to inform replicas about new operations
/// as well as operations which are now safe to perform.
///
/// Triggered by `VsrRequest`.
/// Triggers `VsrPrepareOK`.
pub const VsrPrepare = struct {
    /// Current view number (view-num)
    view_no: ViewNumber,

    /// Desired operation's number (op-num)
    op_no: OpNumber,

    /// Last commit number (commit-num)
    commit_no: CommitNumber,

    /// Message with client request
    client_request: VsrRequest,

    /// Initializes the `prepare` message.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn init(allocs: Allocators, view_no: ViewNumber, op_no: OpNumber, commit_no: CommitNumber, client_request: VsrRequest) AllocationError!VsrPrepare {
        return .{
            .view_no = view_no,
            .op_no = op_no,
            .commit_no = commit_no,
            .client_request = try client_request.clone(allocs),
        };
    }

    /// De-initializes the `prepare` message, releasing resources.
    pub fn deinit(self: *const VsrPrepare, allocs: Allocators) void {
        defer self.client_request.deinit(allocs);
    }

    /// Clones this `prepare` message.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn clone(self: *const VsrPrepare, allocs: Allocators) AllocationError!VsrPrepare {
        return .{
            .view_no = self.view_no,
            .op_no = self.op_no,
            .commit_no = self.commit_no,
            .client_request = try self.client_request.clone(allocs),
        };
    }

    /// Renders the `prepare` message to a human-friendly format.
    pub fn format(self: *const VsrPrepare, writer: *std.Io.Writer) !void {
        try writer.print("{{view_no={d},op_no={d},commit_no={d},client_request={f}}}", .{
            self.view_no,
            self.op_no,
            self.commit_no,
            self.client_request,
        });
    }

    /// Calculates the marshalled message size
    pub inline fn calcSize(self: *const VsrPrepare) usize {
        const pre_calced: usize = @sizeOf(ViewNumber) + @sizeOf(OpNumber) + @sizeOf(CommitNumber);
        return pre_calced + self.client_request.calcSize();
    }

    /// Marshals the in-memory representation of the `prepare` message wire format, returning number of bytes written.
    pub fn marshal(self: *const VsrPrepare, writer: *std.Io.Writer) MarshallingError!usize {
        const pre_calced: usize = @sizeOf(ViewNumber) + @sizeOf(OpNumber) + @sizeOf(CommitNumber);
        writer.writeInt(ViewNumber, self.view_no, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(OpNumber, self.op_no, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(CommitNumber, self.commit_no, .little) catch return MarshallingError.WriteFailed;
        return pre_calced + try self.client_request.marshal(writer);
    }

    /// Unmarshals the `prepare` message wire format into an in-memory representation.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn unmarshal(allocs: Allocators, reader: *std.Io.Reader) UnmarshallingError!VsrPrepare {
        const view_no = reader.takeInt(ViewNumber, .little) catch return UnmarshallingError.ReadFailed;
        const op_no = reader.takeInt(OpNumber, .little) catch return UnmarshallingError.ReadFailed;
        const commit_no = reader.takeInt(CommitNumber, .little) catch return UnmarshallingError.ReadFailed;
        const client_request = try VsrRequest.unmarshal(allocs, reader);
        errdefer client_request.deinit(allocs);

        return .{
            .view_no = view_no,
            .op_no = op_no,
            .commit_no = commit_no,
            .client_request = client_request,
        };
    }
};

// Test VsrPrepare wire format (marshalling/unmarshalling)
test "VsrPrepare.wire" {
    std.debug.print("test.VsrPrepare.wire\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = module_tests_log_level;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var frame_allocator_holder = dymes_msg.FrameAllocator.init(allocator);
    const frame_allocator = frame_allocator_holder.allocator();

    const allocs: Allocators = .{
        .gpa = allocator,
        .msg_frame_allocator = frame_allocator,
    };

    var ulid_generator = common.ulid.generator();

    var logger = logging.logger("test.VsrPrepare.wire");

    logger.debug()
        .msg("Preparing VsrPrepare")
        .log();

    const msg_to_append_id = try ulid_generator.next();
    const msg_to_append_payload = "I want to be appended";

    const log_sequence_no: OpNumber = 0;
    const msg_to_append = try dymes_msg.Message.init(frame_allocator, log_sequence_no, msg_to_append_id, 702, 0, msg_to_append_payload, .{});
    defer msg_to_append.deinit(frame_allocator);

    const append_op = try ops.VsrAppendOp.init(frame_allocator, msg_to_append);

    const client_id: ClientId = 1066;
    const client_request_no: RequestNumber = 1234;

    const vsr_op: ops.VsrOp = .{ .append = append_op };
    defer vsr_op.deinit(allocs);

    const vsr_request = try VsrRequest.init(allocs, client_id, client_request_no, vsr_op);
    defer vsr_request.deinit(allocs);

    const view_no: ViewNumber = 0;
    const commit_no: u64 = 0;

    const vsr_prepare = try VsrPrepare.init(allocs, view_no, log_sequence_no, commit_no, vsr_request);
    defer vsr_prepare.deinit(allocs);

    logger.debug()
        .msg("Marshalling VsrPrepare msg")
        .any("vsr_prepare", vsr_prepare)
        .log();

    const wire_buffer = try allocator.alloc(u8, vsr_prepare.calcSize());
    defer allocator.free(wire_buffer);

    var wire_writer = std.Io.Writer.fixed(wire_buffer);
    const cb_marshalled = try vsr_prepare.marshal(&wire_writer);
    assert(cb_marshalled == vsr_prepare.calcSize());

    try wire_writer.flush();

    var wire_reader = std.Io.Reader.fixed(wire_buffer);
    const unmarshalled_prepare = try VsrPrepare.unmarshal(allocs, &wire_reader);
    defer unmarshalled_prepare.deinit(allocs);

    logger.debug()
        .msg("Unmarshalled VsrPrepare msg")
        .any("unmarshalled_prepare", unmarshalled_prepare)
        .log();

    try testing.expectEqual(vsr_prepare.view_no, unmarshalled_prepare.view_no);
    try testing.expectEqual(vsr_prepare.op_no, unmarshalled_prepare.op_no);
    try testing.expectEqual(vsr_prepare.commit_no, unmarshalled_prepare.commit_no);
    try testing.expectEqual(vsr_prepare.client_request.client_id, unmarshalled_prepare.client_request.client_id);
    try testing.expectEqual(vsr_prepare.client_request.request_no, unmarshalled_prepare.client_request.request_no);
    try testing.expectEqualStrings(vsr_prepare.client_request.op.append.msg_frame_buf, unmarshalled_prepare.client_request.op.append.msg_frame_buf);
}

/// `Prepare-OK` message, an acknowledgment to the primary that the operation,
///  and all previous operations, were successfully prepared.
///
/// Triggered by `VsrPrepare`.
/// Triggers (in primary) `VsrReply`.
pub const VsrPrepareOK = struct {
    /// Current view number (view-num)
    view_no: ViewNumber,

    /// Operation's number (op-num)
    op_no: OpNumber,

    /// The replica number
    replica_no: ReplicaNumber,

    /// Client identifier (client-id)
    client_id: ClientId,

    /// Request number (request-num)
    request_no: RequestNumber,

    /// Initializes the `prepare-ok` acknowledgement message.
    pub fn init(
        view_no: ViewNumber,
        op_no: OpNumber,
        replica_no: ReplicaNumber,
        client_id: ClientId,
        request_no: RequestNumber,
    ) VsrPrepareOK {
        return .{
            .view_no = view_no,
            .op_no = op_no,
            .replica_no = replica_no,
            .client_id = client_id,
            .request_no = request_no,
        };
    }

    /// Clones this `prepare-ok` message.
    pub inline fn clone(self: *const VsrPrepareOK) AllocationError!VsrPrepareOK {
        return self.*;
    }

    /// Renders the `prepare-ok` acknowledgement message to a human-friendly format.
    pub fn format(self: *const VsrPrepareOK, writer: *std.Io.Writer) !void {
        try writer.print("{{view_no={d},op_no={d},replica_no={d},client_id={d},request_no={d}}}", .{
            self.view_no,
            self.op_no,
            self.replica_no,
            self.client_id,
            self.request_no,
        });
    }

    /// Calculates the marshalled message size
    pub inline fn calcSize(_: *const VsrPrepareOK) usize {
        const pre_calced: usize = @sizeOf(ViewNumber) + @sizeOf(OpNumber) + @sizeOf(ReplicaNumber) + @sizeOf(ClientId) + @sizeOf(RequestNumber);
        return pre_calced;
    }

    /// Marshals the in-memory representation of the `prepare-ok` acknowledgement message wire format, returning number of bytes written.
    pub fn marshal(self: *const VsrPrepareOK, writer: *std.Io.Writer) MarshallingError!usize {
        const pre_calced: usize = @sizeOf(ViewNumber) + @sizeOf(OpNumber) + @sizeOf(ReplicaNumber) + @sizeOf(ClientId) + @sizeOf(RequestNumber);
        writer.writeInt(ViewNumber, self.view_no, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(OpNumber, self.op_no, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(ClientId, self.client_id, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(RequestNumber, self.request_no, .little) catch return MarshallingError.WriteFailed;
        writer.writeByte(self.replica_no) catch return MarshallingError.WriteFailed;
        return pre_calced;
    }

    /// Unmarshals the `prepare-ok` acknowledgement message wire format into an in-memory representation.
    pub fn unmarshal(reader: *std.Io.Reader) UnmarshallingError!VsrPrepareOK {
        const view_no = reader.takeInt(ViewNumber, .little) catch return UnmarshallingError.ReadFailed;
        const op_no = reader.takeInt(OpNumber, .little) catch return UnmarshallingError.ReadFailed;
        const client_id = reader.takeInt(ClientId, .little) catch return UnmarshallingError.ReadFailed;
        const request_no = reader.takeInt(RequestNumber, .little) catch return UnmarshallingError.ReadFailed;
        const replica_no = reader.takeByte() catch return UnmarshallingError.ReadFailed;

        return .{
            .view_no = view_no,
            .op_no = op_no,
            .replica_no = replica_no,
            .client_id = client_id,
            .request_no = request_no,
        };
    }
};

// Test VsrPrepareOK wire format (marshalling/unmarshalling)
test "VsrPrepareOK.wire" {
    std.debug.print("test.VsrPrepareOK.wire\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = module_tests_log_level;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var logger = logging.logger("test.VsrPrepareOK.wire");

    logger.debug()
        .msg("Preparing VsrPrepareOK")
        .log();

    const view_no: ViewNumber = 0;
    const op_no: OpNumber = 4321;
    const replica_no: ReplicaNumber = 1;
    const client_id: ClientId = 1066;
    const request_no: ClientId = 0;

    const vsr_prepare_ok = VsrPrepareOK.init(view_no, op_no, replica_no, client_id, request_no);

    logger.debug()
        .msg("Marshalling VsrPrepareOK msg")
        .any("vsr_prepare_ok", vsr_prepare_ok)
        .log();

    const wire_buffer = try allocator.alloc(u8, vsr_prepare_ok.calcSize());
    defer allocator.free(wire_buffer);

    var wire_writer = std.Io.Writer.fixed(wire_buffer);
    const cb_marshalled = try vsr_prepare_ok.marshal(&wire_writer);
    assert(cb_marshalled == vsr_prepare_ok.calcSize());

    try wire_writer.flush();

    var wire_reader = std.Io.Reader.fixed(wire_buffer);
    const unmarshalled_prepare_ok = try VsrPrepareOK.unmarshal(&wire_reader);

    logger.debug()
        .msg("Unmarshalled VsrPrepareOK msg")
        .any("unmarshalled_prepare_ok", unmarshalled_prepare_ok)
        .log();

    try testing.expectEqual(vsr_prepare_ok, unmarshalled_prepare_ok);
}

/// `Reply` message from primary to client after successful preparation and execution of operation.
///
/// Triggered (in primary) by a quorum of `VsrPrepareOK` messages (from 𝑓+1 replicas including itself).
pub const VsrReply = struct {
    /// Current view number (view-num)
    view_no: ViewNumber,

    /// Client identifier (client-id)
    ///
    /// Dymes-specific: Carrying client_id in reply is an extension to VSR
    client_id: ClientId,

    /// Request number (request-num)
    request_no: RequestNumber,

    /// Operation result (response)
    response: ops.VsrOpResponse,

    /// Initializes the `reply` message.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn init(view_no: ViewNumber, client_id: ClientId, request_no: RequestNumber, response: ops.VsrOpResponse) AllocationError!VsrReply {
        return .{
            .view_no = view_no,
            .client_id = client_id,
            .request_no = request_no,
            .response = response,
        };
    }

    /// Renders the `reply` message to a human-friendly format.
    pub fn format(self: *const VsrReply, writer: *std.Io.Writer) !void {
        try writer.print("{{view_no={d},client_id={d},request_no={d},response={f}}}", .{
            self.view_no,
            self.client_id,
            self.request_no,
            self.response,
        });
    }

    /// Calculates the marshalled message size
    pub inline fn calcSize(self: *const VsrReply) usize {
        const pre_calced: usize = @sizeOf(ViewNumber) + @sizeOf(ClientId) + @sizeOf(RequestNumber);
        return pre_calced + self.response.calcSize();
    }

    /// Marshals the in-memory representation of the `reply` message wire format, returning number of bytes written.
    pub fn marshal(self: *const VsrReply, writer: *std.Io.Writer) MarshallingError!usize {
        const pre_calced: usize = @sizeOf(ViewNumber) + @sizeOf(ClientId) + @sizeOf(RequestNumber);
        writer.writeInt(ViewNumber, self.view_no, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(ClientId, self.client_id, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(RequestNumber, self.request_no, .little) catch return MarshallingError.WriteFailed;
        return pre_calced + try self.response.marshal(writer);
    }

    /// Unmarshals the `reply` message wire format into an in-memory representation.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn unmarshal(allocs: Allocators, reader: *std.Io.Reader) UnmarshallingError!VsrReply {
        const view_no = reader.takeInt(ViewNumber, .little) catch return UnmarshallingError.ReadFailed;
        const client_id = reader.takeInt(ClientId, .little) catch return UnmarshallingError.ReadFailed;
        const request_no = reader.takeInt(RequestNumber, .little) catch return UnmarshallingError.ReadFailed;
        const response = try ops.VsrOpResponse.unmarshal(allocs, reader);

        return .{
            .view_no = view_no,
            .client_id = client_id,
            .request_no = request_no,
            .response = response,
        };
    }
};

// Test VsrReply wire format (marshalling/unmarshalling)
test "VsrReply.wire" {
    std.debug.print("test.VsrReply.wire\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = module_tests_log_level;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var logger = logging.logger("test.VsrReply.wire");

    var frame_allocator_holder = dymes_msg.FrameAllocator.init(allocator);
    const frame_allocator = frame_allocator_holder.allocator();

    const allocs: Allocators = .{
        .gpa = allocator,
        .msg_frame_allocator = frame_allocator,
    };

    logger.debug()
        .msg("Preparing VsrReply")
        .log();

    const client_id: ClientId = 666;
    const view_no: ViewNumber = 2;
    const client_request_no: ClientId = 1234;

    const op_response: ops.VsrOpResponse = .{ .append = .{} };

    const vsr_reply = try VsrReply.init(view_no, client_id, client_request_no, op_response);

    logger.debug()
        .msg("Marshalling VsrReply")
        .any("vsr_reply", vsr_reply)
        .log();

    const wire_buffer = try allocator.alloc(u8, vsr_reply.calcSize());
    defer allocator.free(wire_buffer);

    var wire_writer = std.Io.Writer.fixed(wire_buffer);
    const cb_marshalled = try vsr_reply.marshal(&wire_writer);
    assert(cb_marshalled == vsr_reply.calcSize());

    try wire_writer.flush();

    var wire_reader = std.Io.Reader.fixed(wire_buffer);
    const unmarshalled_vsr_reply = try VsrReply.unmarshal(allocs, &wire_reader);

    logger.debug()
        .msg("Unmarshalled VsrReply")
        .any("unmarshalled_vsr_reply", unmarshalled_vsr_reply)
        .log();

    try testing.expectEqual(vsr_reply, unmarshalled_vsr_reply);
}

/// `Commit` message (forms part of `VsrPrepare`/`VsrCommit` heartbeat).
///
/// When a replica receives a `VsrCommit` it will execute all operation in their op-log
/// between the last commit-num and the commit-num in the `VsrCommit` message strictly
/// following the order of operations and advance its commit-num as well.
///
/// The `VsrPrepare` message together with the `VsrCommit` message act as a heartbeat for the primary.
/// Replicas use this to identify when a primary might be dead and elect a new primary.
///
/// Triggered (in primary) by non-receipt of any `VsrPrepare` messages within a configured period.
pub const VsrCommit = struct {
    /// Current view number (view-num)
    view_no: ViewNumber,

    // The last committed operation (commit-num)
    commit_no: CommitNumber,

    /// Initializes the `commit` message.
    pub fn init(view_no: ViewNumber, commit_no: CommitNumber) VsrCommit {
        return .{
            .view_no = view_no,
            .commit_no = commit_no,
        };
    }

    /// Renders the `commit` message to a human-friendly format.
    pub fn format(self: *const VsrCommit, writer: *std.Io.Writer) !void {
        try writer.print("{{view_no={d},commit_no={d}}}", .{
            self.view_no,
            self.commit_no,
        });
    }

    /// Calculates the marshalled message size
    pub inline fn calcSize(_: *const VsrCommit) usize {
        const pre_calced: usize = @sizeOf(ViewNumber) + @sizeOf(CommitNumber);
        return pre_calced;
    }

    /// Marshals the in-memory representation of the `commit` message wire format, returning number of bytes written.
    pub fn marshal(self: *const VsrCommit, writer: *std.Io.Writer) MarshallingError!usize {
        const pre_calced: usize = @sizeOf(ViewNumber) + @sizeOf(CommitNumber);
        writer.writeInt(ViewNumber, self.view_no, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(CommitNumber, self.commit_no, .little) catch return MarshallingError.WriteFailed;
        return pre_calced;
    }

    /// Unmarshals the `commit` message wire format into an in-memory representation.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn unmarshal(_: Allocators, reader: *std.Io.Reader) UnmarshallingError!VsrCommit {
        const view_no = reader.takeInt(ViewNumber, .little) catch return UnmarshallingError.ReadFailed;
        const commit_no = reader.takeInt(CommitNumber, .little) catch return UnmarshallingError.ReadFailed;

        return .{
            .view_no = view_no,
            .commit_no = commit_no,
        };
    }
};

// Test VsrCommit wire format (marshalling/unmarshalling)
test "VsrCommit.wire" {
    std.debug.print("test.VsrCommit.wire\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = module_tests_log_level;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var logger = logging.logger("test.VsrCommit.wire");

    var frame_allocator_holder = dymes_msg.FrameAllocator.init(allocator);
    const frame_allocator = frame_allocator_holder.allocator();

    const allocs: Allocators = .{
        .gpa = allocator,
        .msg_frame_allocator = frame_allocator,
    };

    logger.debug()
        .msg("Preparing VsrCommit")
        .log();

    const view_no: ViewNumber = 2;
    const commit_no: CommitNumber = 1234;

    const vsr_commit = VsrCommit.init(view_no, commit_no);

    logger.debug()
        .msg("Marshalling VsrCommit")
        .any("vsr_commit", vsr_commit)
        .log();

    const wire_buffer = try allocator.alloc(u8, vsr_commit.calcSize());
    defer allocator.free(wire_buffer);

    var wire_writer = std.Io.Writer.fixed(wire_buffer);
    const cb_marshalled = try vsr_commit.marshal(&wire_writer);
    assert(cb_marshalled == vsr_commit.calcSize());

    try wire_writer.flush();

    var wire_reader = std.Io.Reader.fixed(wire_buffer);
    const unmarshalled_vsr_commit = try VsrCommit.unmarshal(allocs, &wire_reader);

    logger.debug()
        .msg("Unmarshalled VsrCommit")
        .any("unmarshalled_vsr_commit", unmarshalled_vsr_commit)
        .log();

    try testing.expectEqual(vsr_commit, unmarshalled_vsr_commit);
}

/// `Start-View-Change` (SVC) message.
///
/// Triggered when a replica detects the primary is unavailable (no `VsrPrepare` or `VsrCommit` seen within timeout).
/// Also triggered when a replica receives an SVC with a higher view-num than theirs.
pub const VsrStartViewChange = struct {
    /// New view number (view-num)
    view_no: ViewNumber,

    /// The replica number
    replica_no: ReplicaNumber,

    /// Initializes the `Start-View-Change` (SVC) message.
    pub fn init(view_no: ViewNumber, replica_no: ReplicaNumber) VsrStartViewChange {
        return .{
            .view_no = view_no,
            .replica_no = replica_no,
        };
    }

    /// Clones this `Start-View-Change` (SVC) message.
    pub inline fn clone(self: *const VsrStartViewChange) AllocationError!VsrStartViewChange {
        return self.*;
    }

    /// Renders the `Start-View-Change` (SVC) message to a human-friendly format.
    pub fn format(self: *const VsrStartViewChange, writer: *std.Io.Writer) !void {
        try writer.print("{{view_no={d},replica_no={d}}}", .{
            self.view_no,
            self.replica_no,
        });
    }

    /// Calculates the marshalled message size
    pub inline fn calcSize(_: *const VsrStartViewChange) usize {
        const pre_calced: usize = @sizeOf(ViewNumber) + @sizeOf(ReplicaNumber);
        return pre_calced;
    }

    /// Marshals the in-memory representation of the `Start-View-Change` (SVC) message wire format, returning number of bytes written.
    pub fn marshal(self: *const VsrStartViewChange, writer: *std.Io.Writer) MarshallingError!usize {
        const pre_calced: usize = @sizeOf(ViewNumber) + @sizeOf(ReplicaNumber);
        writer.writeInt(ViewNumber, self.view_no, .little) catch return MarshallingError.WriteFailed;
        writer.writeByte(self.replica_no) catch return MarshallingError.WriteFailed;
        return pre_calced;
    }

    /// Unmarshals the `Start-View-Change` (SVC) message wire format into an in-memory representation.
    pub fn unmarshal(reader: *std.Io.Reader) UnmarshallingError!VsrStartViewChange {
        const view_no = reader.takeInt(ViewNumber, .little) catch return UnmarshallingError.ReadFailed;
        const replica_no = reader.takeByte() catch return UnmarshallingError.ReadFailed;

        return .{
            .view_no = view_no,
            .replica_no = replica_no,
        };
    }
};

// Test VsrStartViewChange wire format (marshalling/unmarshalling)
test "VsrStartViewChange.wire" {
    std.debug.print("test.VsrStartViewChange.wire\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = module_tests_log_level;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var logger = logging.logger("test.VsrStartViewChange.wire");

    logger.debug()
        .msg("Preparing VsrStartViewChange")
        .log();

    const view_no: ViewNumber = 2;
    const replica_no: ReplicaNumber = 3;

    const vsr_svc = VsrStartViewChange.init(view_no, replica_no);

    logger.debug()
        .msg("Marshalling SVC")
        .any("vsr_svc", vsr_svc)
        .log();

    const wire_buffer = try allocator.alloc(u8, vsr_svc.calcSize());
    defer allocator.free(wire_buffer);

    var wire_writer = std.Io.Writer.fixed(wire_buffer);
    const cb_marshalled = try vsr_svc.marshal(&wire_writer);
    assert(cb_marshalled == vsr_svc.calcSize());

    try wire_writer.flush();

    var wire_reader = std.Io.Reader.fixed(wire_buffer);
    const unmarshalled_vsr_svc = try VsrStartViewChange.unmarshal(&wire_reader);

    logger.debug()
        .msg("Unmarshalled SVC")
        .any("unmarshalled_vsr_svc", unmarshalled_vsr_svc)
        .log();

    try testing.expectEqual(vsr_svc, unmarshalled_vsr_svc);
}

/// `Do-View-Change` (DVC) message.
///
/// Triggered (in replica) when it receives 𝑓+1 (including itself) `VsrStartViewChange` messages
/// with *its own* view-num - this will attempt to make it the new primary.
pub const VsrDoViewChange = struct {
    /// New view number
    view_no: ViewNumber,

    /// Last view number in which the state was normal before change (old-view-number)
    old_view_no: ViewNumber,

    /// Operation's number (op-num)
    op_no: OpNumber,

    // The last committed operation (commit-num)
    commit_no: CommitNumber,

    /// Operations log (op-log)
    op_log: ops.VsrOpLog,

    /// Initializes the `Do-View-Change` (DVC) message.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn init(
        allocs: Allocators,
        view_no: ViewNumber,
        old_view_no: ViewNumber,
        op_no: OpNumber,
        commit_no: CommitNumber,
        op_log: ops.VsrOpLog,
    ) AllocationError!VsrDoViewChange {
        return .{
            .view_no = view_no,
            .old_view_no = old_view_no,
            .op_no = op_no,
            .commit_no = commit_no,
            .op_log = try op_log.clone(allocs),
        };
    }

    /// De-initializes the `Do-View-Change` (DVC) message, releasing resources.
    pub fn deinit(self: *VsrDoViewChange, allocs: Allocators) void {
        defer self.op_log.deinit(allocs);
    }

    /// Clones this `Do-View-Change` (DVC) message.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn clone(self: *const VsrDoViewChange, allocs: Allocators) AllocationError!VsrDoViewChange {
        return .{
            .view_no = self.view_no,
            .old_view_no = self.old_view_no,
            .op_no = self.op_no,
            .commit_no = self.commit_no,
            .op_log = try self.op_log.clone(allocs),
        };
    }

    /// Renders the `Do-View-Change` (DVC) message to a human-friendly format.
    pub fn format(self: *const VsrDoViewChange, writer: *std.Io.Writer) !void {
        try writer.print("{{view_no={d},old_view_no={d},op_no={d},commit_no={d},op_log={f}}}", .{
            self.view_no,
            self.old_view_no,
            self.op_no,
            self.commit_no,
            self.op_log,
        });
    }

    /// Calculates the marshalled message size
    pub inline fn calcSize(self: *const VsrDoViewChange) usize {
        const pre_calced: usize = @sizeOf(ViewNumber) + @sizeOf(ViewNumber) + @sizeOf(OpNumber) + @sizeOf(CommitNumber);
        return pre_calced + self.op_log.calcSize();
    }

    /// Marshals the in-memory representation of the `Do-View-Change` (DVC) message wire format, returning number of bytes written.
    pub fn marshal(self: *const VsrDoViewChange, writer: *std.Io.Writer) MarshallingError!usize {
        const pre_calced: usize = @sizeOf(ViewNumber) + @sizeOf(ViewNumber) + @sizeOf(OpNumber) + @sizeOf(CommitNumber);
        writer.writeInt(ViewNumber, self.view_no, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(ViewNumber, self.old_view_no, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(OpNumber, self.op_no, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(CommitNumber, self.commit_no, .little) catch return MarshallingError.WriteFailed;
        return pre_calced + try self.op_log.marshal(writer);
    }

    /// Unmarshals the `Do-View-Change` (DVC) message wire format into an in-memory representation.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn unmarshal(allocs: Allocators, reader: *std.Io.Reader) UnmarshallingError!VsrDoViewChange {
        const view_no = reader.takeInt(ViewNumber, .little) catch return UnmarshallingError.ReadFailed;
        const old_view_no = reader.takeInt(ViewNumber, .little) catch return UnmarshallingError.ReadFailed;
        const op_no = reader.takeInt(OpNumber, .little) catch return UnmarshallingError.ReadFailed;
        const commit_no = reader.takeInt(CommitNumber, .little) catch return UnmarshallingError.ReadFailed;
        const op_log = try ops.VsrOpLog.unmarshal(allocs, reader);
        errdefer op_log.deinit(allocs);

        return .{
            .view_no = view_no,
            .old_view_no = old_view_no,
            .op_no = op_no,
            .commit_no = commit_no,
            .op_log = op_log,
        };
    }
};

// Test VsrDoViewChange wire format (marshalling/unmarshalling)
test "VsrDoViewChange.wire" {
    std.debug.print("test.VsrDoViewChange.wire\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = module_tests_log_level;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var frame_allocator_holder = dymes_msg.FrameAllocator.init(allocator);
    const frame_allocator = frame_allocator_holder.allocator();

    const allocs: Allocators = .{
        .gpa = allocator,
        .msg_frame_allocator = frame_allocator,
    };

    var ulid_generator = common.ulid.generator();

    var logger = logging.logger("test.VsrDoViewChange.wire");

    logger.debug()
        .msg("Preparing VsrDoViewChange")
        .log();

    const msg_to_append_id = try ulid_generator.next();
    const msg_to_append_payload = "I want to be appended";

    const op_no: OpNumber = 1;

    var vsr_op_log = ops.VsrOpLog.init();
    defer vsr_op_log.deinit(allocs);

    {
        const msg_to_append = try dymes_msg.Message.init(frame_allocator, op_no, msg_to_append_id, 702, 0, msg_to_append_payload, .{});
        defer msg_to_append.deinit(frame_allocator);

        const append_op = try ops.VsrAppendOp.init(frame_allocator, msg_to_append);

        const vsr_op: ops.VsrOp = .{ .append = append_op };
        defer vsr_op.deinit(allocs);

        const vsr_op_response: ops.VsrOpResponse = .{ .append = .{ .msg_ulid = msg_to_append.id() } };
        try vsr_op_log.addEntry(allocs, 0, vsr_op, vsr_op_response);
    }

    const view_no: ViewNumber = 2;
    const old_view_no: ViewNumber = 1;

    var vsr_dvc = try VsrDoViewChange.init(allocs, view_no, old_view_no, op_no, op_no, vsr_op_log);
    defer vsr_dvc.deinit(allocs);

    logger.debug()
        .msg("Marshalling DVC")
        .any("vsr_dvc", vsr_dvc)
        .log();

    const wire_buffer = try allocator.alloc(u8, vsr_dvc.calcSize());
    defer allocator.free(wire_buffer);

    var wire_writer = std.Io.Writer.fixed(wire_buffer);
    const cb_marshalled = try vsr_dvc.marshal(&wire_writer);
    assert(cb_marshalled == vsr_dvc.calcSize());

    try wire_writer.flush();

    var wire_reader = std.Io.Reader.fixed(wire_buffer);
    var unmarshalled_dvc = try VsrDoViewChange.unmarshal(allocs, &wire_reader);
    defer unmarshalled_dvc.deinit(allocs);

    logger.debug()
        .msg("Unmarshalled DVC")
        .any("unmarshalled_dvc", unmarshalled_dvc)
        .log();

    try testing.expectEqual(vsr_dvc.view_no, unmarshalled_dvc.view_no);
    try testing.expectEqual(vsr_dvc.old_view_no, unmarshalled_dvc.old_view_no);
    try testing.expectEqual(vsr_dvc.op_no, unmarshalled_dvc.op_no);
    try testing.expectEqual(vsr_dvc.commit_no, unmarshalled_dvc.commit_no);
    try testing.expectEqual(vsr_dvc.op_log.log_seq_offset, unmarshalled_dvc.op_log.log_seq_offset);
    try testing.expectEqualStrings(vsr_dvc.op_log.ops_entries.items[0].op.append.msg_frame_buf, unmarshalled_dvc.op_log.ops_entries.items[0].op.append.msg_frame_buf);
}

/// `Start-View` (SV) message.
///
/// Triggered (in new primary) when it receives 𝑓+1 (including itself) `VsrDoViewChange` messages with the same view-num.
/// It sends a `VsrStartView` message to all replicas with the new view-num, the most up to date op-log,
/// the corresponding op-num and the highest commit-num.
pub const VsrStartView = struct {
    /// New view number (view-num)
    view_no: ViewNumber,

    /// Most up-to-date operation number (op-num)
    op_no: OpNumber,

    /// Highest commit number (commit-num)
    commit_no: CommitNumber,

    /// Most up-to-date operations log (op-log)
    op_log: ops.VsrOpLog,

    /// Initializes the `Start-View` (SV) message.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn init(allocs: Allocators, view_no: ViewNumber, op_no: OpNumber, commit_no: CommitNumber, op_log: ops.VsrOpLog) AllocationError!VsrStartView {
        return .{
            .view_no = view_no,
            .op_no = op_no,
            .commit_no = commit_no,
            .op_log = try op_log.clone(allocs),
        };
    }

    /// De-initializes the `Start-View` (SV) message, releasing resources.
    pub fn deinit(self: *VsrStartView, allocs: Allocators) void {
        defer self.op_log.deinit(allocs);
    }

    /// Clones this `Start-View` (SV) message.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn clone(self: *const VsrStartView, allocs: Allocators) AllocationError!VsrStartView {
        return .{
            .view_no = self.view_no,
            .op_no = self.op_no,
            .commit_no = self.commit_no,
            .op_log = try self.op_log.clone(allocs),
        };
    }

    /// Renders the `Start-View` (SV) message to a human-friendly format.
    pub fn format(self: *const VsrStartView, writer: *std.Io.Writer) !void {
        try writer.print("{{view_no={d},op_no={d},commit_no={d},op_log={f}}}", .{
            self.view_no,
            self.op_no,
            self.commit_no,
            self.op_log,
        });
    }

    /// Calculates the marshalled message size
    pub inline fn calcSize(self: *const VsrStartView) usize {
        const pre_calced: usize = @sizeOf(ViewNumber) + @sizeOf(OpNumber) + @sizeOf(CommitNumber);
        return pre_calced + self.op_log.calcSize();
    }

    /// Marshals the in-memory representation of the `Start-View` (SV) message wire format, returning number of bytes written.
    pub fn marshal(self: *const VsrStartView, writer: *std.Io.Writer) MarshallingError!usize {
        const pre_calced: usize = @sizeOf(ViewNumber) + @sizeOf(OpNumber) + @sizeOf(CommitNumber);
        writer.writeInt(ViewNumber, self.view_no, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(OpNumber, self.op_no, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(CommitNumber, self.commit_no, .little) catch return MarshallingError.WriteFailed;
        return pre_calced + try self.op_log.marshal(writer);
    }

    /// Unmarshals the `Start-View` (SV) message wire format into an in-memory representation.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn unmarshal(allocs: Allocators, reader: *std.Io.Reader) UnmarshallingError!VsrStartView {
        const view_no = reader.takeInt(ViewNumber, .little) catch return UnmarshallingError.ReadFailed;
        const op_no = reader.takeInt(OpNumber, .little) catch return UnmarshallingError.ReadFailed;
        const commit_no = reader.takeInt(CommitNumber, .little) catch return UnmarshallingError.ReadFailed;
        const op_log = try ops.VsrOpLog.unmarshal(allocs, reader);
        errdefer op_log.deinit(allocs);

        return .{
            .view_no = view_no,
            .op_no = op_no,
            .commit_no = commit_no,
            .op_log = op_log,
        };
    }
};

// Test VsrStartView wire format (marshalling/unmarshalling)
test "VsrStartView.wire" {
    std.debug.print("test.VsrStartView.wire\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = module_tests_log_level;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var frame_allocator_holder = dymes_msg.FrameAllocator.init(allocator);
    const frame_allocator = frame_allocator_holder.allocator();

    const allocs: Allocators = .{
        .gpa = allocator,
        .msg_frame_allocator = frame_allocator,
    };

    var ulid_generator = common.ulid.generator();

    var logger = logging.logger("test.VsrStartView.wire");

    logger.debug()
        .msg("Preparing VsrStartView")
        .log();

    const msg_to_append_id = try ulid_generator.next();
    const msg_to_append_payload = "I want to be appended";

    const commit_no: CommitNumber = 0;
    const op_no: OpNumber = 1;

    var vsr_op_log = ops.VsrOpLog.init();
    defer vsr_op_log.deinit(allocs);

    {
        const msg_to_append = try dymes_msg.Message.init(frame_allocator, op_no, msg_to_append_id, 702, 0, msg_to_append_payload, .{});
        defer msg_to_append.deinit(frame_allocator);

        const append_op = try ops.VsrAppendOp.init(frame_allocator, msg_to_append);

        const vsr_op: ops.VsrOp = .{ .append = append_op };
        defer vsr_op.deinit(allocs);

        const vsr_op_response: ops.VsrOpResponse = .{ .append = .{ .msg_ulid = msg_to_append.id() } };
        try vsr_op_log.addEntry(allocs, 0, vsr_op, vsr_op_response);
    }

    const view_no: ViewNumber = 2;

    var vsr_sv = try VsrStartView.init(allocs, view_no, op_no, commit_no, vsr_op_log);
    defer vsr_sv.deinit(allocs);

    logger.debug()
        .msg("Marshalling SV")
        .any("vsr_sv", vsr_sv)
        .log();

    const wire_buffer = try allocator.alloc(u8, vsr_sv.calcSize());
    defer allocator.free(wire_buffer);

    var wire_writer = std.Io.Writer.fixed(wire_buffer);
    const cb_marshalled = try vsr_sv.marshal(&wire_writer);
    assert(cb_marshalled == vsr_sv.calcSize());

    try wire_writer.flush();

    var wire_reader = std.Io.Reader.fixed(wire_buffer);
    var unmarshalled_sv = try VsrStartView.unmarshal(allocs, &wire_reader);
    defer unmarshalled_sv.deinit(allocs);

    logger.debug()
        .msg("Unmarshalled SV")
        .any("unmarshalled_sv", unmarshalled_sv)
        .log();

    try testing.expectEqual(vsr_sv.view_no, unmarshalled_sv.view_no);
    try testing.expectEqual(vsr_sv.op_no, unmarshalled_sv.op_no);
    try testing.expectEqual(vsr_sv.commit_no, unmarshalled_sv.commit_no);
    try testing.expectEqual(vsr_sv.op_log.log_seq_offset, unmarshalled_sv.op_log.log_seq_offset);
    try testing.expectEqualStrings(vsr_sv.op_log.ops_entries.items[0].op.append.msg_frame_buf, unmarshalled_sv.op_log.ops_entries.items[0].op.append.msg_frame_buf);
}

/// `Get-State` (GS) message.
///
/// Triggered when a replica has fallen behind (such as a partitioned ex-primary rejoining the ensemble).
///
/// In this case, it will set its status to recovery and issue a <GET-STATE> request
/// to any of the other replicas. The <GET-STATE> will contains its current values
/// of the view-num, op-num and commit-num together with its identity. The
/// <GET-STATE> message is the same message used by the replicas to get up to date
/// when the fall behind. The replica who receives the <GET-STATE> message will
/// only respond if its status is normal.
///
/// If the view-num in the <GET-STATE> message is the same as its view-num it means
/// that the requesting node just fell behind, so it will prepare a <NEW-STATE>
/// message with its view-num, its commit-num and its op-num and the portion of the
/// op-log between the op-num in the <GET-STATE> and its op-num.
///
/// If the view-num in the <GET-STATE> message is different, then it means that the
/// node was in a partition during a view change. In this case it will prepare a
/// <NEW-STATE> message with new view-num, its commit-num and its op-num and the
/// portion of the op-log between the commit-num in the <GET-STATE> this time and
/// its op-num, this is because some operations in the minority group might not
/// have survived the view-change.
pub const VsrGetState = struct {
    /// View number (view-num)
    view_no: ViewNumber,

    /// Operation number (op-num)
    op_no: OpNumber,

    /// Last commit number (commit-num)
    commit_no: CommitNumber,

    /// The replica number
    replica_no: ReplicaNumber,

    /// Initializes the `Get-State` (GS) message.
    pub fn init(view_no: ViewNumber, op_no: OpNumber, commit_no: CommitNumber, replica_no: ReplicaNumber) VsrGetState {
        return .{
            .view_no = view_no,
            .op_no = op_no,
            .commit_no = commit_no,
            .replica_no = replica_no,
        };
    }

    /// Clones this `Get-State` (GS) message.
    pub inline fn clone(self: *const VsrGetState) AllocationError!VsrGetState {
        return self.*;
    }

    /// Renders the `Get-State` (GS) message to a human-friendly format.
    pub fn format(self: *const VsrGetState, writer: *std.Io.Writer) !void {
        try writer.print("{{view_no={d},op_no={d},commit_no={d},replica_no={d}}}", .{
            self.view_no,
            self.op_no,
            self.commit_no,
            self.replica_no,
        });
    }

    /// Calculates the marshalled message size
    pub inline fn calcSize(_: *const VsrGetState) usize {
        const pre_calced: usize = @sizeOf(ViewNumber) + @sizeOf(OpNumber) + @sizeOf(CommitNumber) + @sizeOf(ReplicaNumber);
        return pre_calced;
    }

    /// Marshals the in-memory representation of the `Get-State` (GS) message wire format, returning number of bytes written.
    pub fn marshal(self: *const VsrGetState, writer: *std.Io.Writer) MarshallingError!usize {
        const pre_calced: usize = @sizeOf(ViewNumber) + @sizeOf(OpNumber) + @sizeOf(CommitNumber) + @sizeOf(ReplicaNumber);
        writer.writeInt(ViewNumber, self.view_no, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(OpNumber, self.op_no, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(CommitNumber, self.commit_no, .little) catch return MarshallingError.WriteFailed;
        writer.writeByte(self.replica_no) catch return MarshallingError.WriteFailed;
        return pre_calced;
    }

    /// Unmarshals the `Get-State` (GS) message wire format into an in-memory representation.
    pub fn unmarshal(reader: *std.Io.Reader) UnmarshallingError!VsrGetState {
        const view_no = reader.takeInt(ViewNumber, .little) catch return UnmarshallingError.ReadFailed;
        const op_no = reader.takeInt(OpNumber, .little) catch return UnmarshallingError.ReadFailed;
        const commit_no = reader.takeInt(CommitNumber, .little) catch return UnmarshallingError.ReadFailed;
        const replica_no = reader.takeByte() catch return UnmarshallingError.ReadFailed;

        return .{
            .view_no = view_no,
            .op_no = op_no,
            .commit_no = commit_no,
            .replica_no = replica_no,
        };
    }
};

// Test VsrGetState wire format (marshalling/unmarshalling)
test "VsrGetState.wire" {
    std.debug.print("test.VsrGetState.wire\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = module_tests_log_level;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var logger = logging.logger("test.VsrGetState.wire");

    logger.debug()
        .msg("Preparing VsrGetState")
        .log();

    const view_no: ViewNumber = 2;
    const op_no: OpNumber = 66;
    const commit_no: OpNumber = 60;
    const replica_no: ReplicaNumber = 3;

    const vsr_gs = VsrGetState.init(view_no, op_no, commit_no, replica_no);

    logger.debug()
        .msg("Marshalling GS")
        .any("vsr_gs", vsr_gs)
        .log();

    const wire_buffer = try allocator.alloc(u8, vsr_gs.calcSize());
    defer allocator.free(wire_buffer);

    var wire_writer = std.Io.Writer.fixed(wire_buffer);
    const cb_marshalled = try vsr_gs.marshal(&wire_writer);
    assert(cb_marshalled == vsr_gs.calcSize());

    try wire_writer.flush();

    var wire_reader = std.Io.Reader.fixed(wire_buffer);
    const unmarshalled_vsr_gs = try VsrGetState.unmarshal(&wire_reader);

    logger.debug()
        .msg("Unmarshalled GS")
        .any("unmarshalled_vsr_gs", unmarshalled_vsr_gs)
        .log();

    try testing.expectEqual(vsr_gs, unmarshalled_vsr_gs);
}

/// `New-State` (NS) message.
///
/// Triggered in response to a recovering replica having issued a `VsrGetState` (GS).
///
/// In the replica that received the `vsrGetState`, if the view-num in the <GET-STATE>
/// message is the same as its view-num it means that the requesting node just fell behind,
/// so it will prepare a <NEW-STATE> message with its view-num, its commit-num, and its
/// op-num and the portion of the op-log between the op-num in the <GET-STATE> and its op-num.
///
/// If the view-num in the <GET-STATE> message is different, then it means that the
/// node was in a partition during a view change. In this case it will prepare a
/// <NEW-STATE> message with new view-num, its commit-num and its op-num and the
/// portion of the op-log between the commit-num in the <GET-STATE> this time and
/// its op-num, this is because some operations in the minority group might not
/// have survived the view-change.
pub const VsrNewState = struct {
    /// View number (view-num)
    view_no: ViewNumber,

    /// Operation number (op-number)
    op_no: OpNumber,

    /// Commit number (commit-num)
    commit_no: CommitNumber,

    /// Relevant portion of operations log (op-log)
    op_log: ops.VsrOpLog,

    /// Initializes the `New-State` (NS) message.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn init(allocs: Allocators, view_no: ViewNumber, op_no: OpNumber, commit_no: CommitNumber, op_log: ops.VsrOpLog) AllocationError!VsrNewState {
        return .{
            .view_no = view_no,
            .op_no = op_no,
            .commit_no = commit_no,
            .op_log = try op_log.clone(allocs),
        };
    }

    /// De-initializes the `New-State` (NS) message, releasing resources.
    pub fn deinit(self: *VsrNewState, allocs: Allocators) void {
        defer self.op_log.deinit(allocs);
    }

    /// Clones this `New-State` (NS) message.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn clone(self: *const VsrNewState, allocs: Allocators) AllocationError!VsrNewState {
        return .{
            .view_no = self.view_no,
            .op_no = self.op_no,
            .commit_no = self.commit_no,
            .op_log = try self.op_log.clone(allocs),
        };
    }

    /// Renders the `New-State` (NS) message to a human-friendly format.
    pub fn format(self: *const VsrNewState, writer: *std.Io.Writer) !void {
        try writer.print("{{view_no={d},op_no={d},commit_no={d},op_log={f}}}", .{
            self.view_no,
            self.op_no,
            self.commit_no,
            self.op_log,
        });
    }

    /// Calculates the marshalled message size
    pub inline fn calcSize(self: *const VsrNewState) usize {
        const pre_calced: usize = @sizeOf(ViewNumber) + @sizeOf(OpNumber) + @sizeOf(CommitNumber);
        return pre_calced + self.op_log.calcSize();
    }

    /// Marshals the in-memory representation of the `New-State` (NS) message wire format, returning number of bytes written.
    pub fn marshal(self: *const VsrNewState, writer: *std.Io.Writer) MarshallingError!usize {
        const pre_calced: usize = @sizeOf(ViewNumber) + @sizeOf(OpNumber) + @sizeOf(CommitNumber);
        writer.writeInt(ViewNumber, self.view_no, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(OpNumber, self.op_no, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(CommitNumber, self.commit_no, .little) catch return MarshallingError.WriteFailed;
        return pre_calced + try self.op_log.marshal(writer);
    }

    /// Unmarshals the `New-State` (NS) message wire format into an in-memory representation.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn unmarshal(allocs: Allocators, reader: *std.Io.Reader) UnmarshallingError!VsrNewState {
        const view_no = reader.takeInt(ViewNumber, .little) catch return UnmarshallingError.ReadFailed;
        const op_no = reader.takeInt(OpNumber, .little) catch return UnmarshallingError.ReadFailed;
        const commit_no = reader.takeInt(CommitNumber, .little) catch return UnmarshallingError.ReadFailed;
        const op_log = try ops.VsrOpLog.unmarshal(allocs, reader);
        errdefer op_log.deinit(allocs);

        return .{
            .view_no = view_no,
            .op_no = op_no,
            .commit_no = commit_no,
            .op_log = op_log,
        };
    }
};

// Test VsrNewState wire format (marshalling/unmarshalling)
test "VsrNewState.wire" {
    std.debug.print("test.VsrNewState.wire\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = module_tests_log_level;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var frame_allocator_holder = dymes_msg.FrameAllocator.init(allocator);
    const frame_allocator = frame_allocator_holder.allocator();

    const allocs: Allocators = .{
        .gpa = allocator,
        .msg_frame_allocator = frame_allocator,
    };

    var ulid_generator = common.ulid.generator();

    var logger = logging.logger("test.VsrNewState.wire");

    logger.debug()
        .msg("Preparing VsrNewState")
        .log();

    const msg_to_append_id = try ulid_generator.next();
    const msg_to_append_payload = "I want to be appended";

    const op_no: OpNumber = 1;

    var vsr_op_log = ops.VsrOpLog.init();
    defer vsr_op_log.deinit(allocs);

    {
        const msg_to_append = try dymes_msg.Message.init(frame_allocator, op_no, msg_to_append_id, 702, 0, msg_to_append_payload, .{});
        defer msg_to_append.deinit(frame_allocator);

        const append_op = try ops.VsrAppendOp.init(frame_allocator, msg_to_append);

        const vsr_op: ops.VsrOp = .{ .append = append_op };
        defer vsr_op.deinit(allocs);

        const vsr_op_response: ops.VsrOpResponse = .{ .append = .{ .msg_ulid = msg_to_append.id() } };
        try vsr_op_log.addEntry(allocs, 0, vsr_op, vsr_op_response);
    }

    const view_no: ViewNumber = 1;
    const commit_no: OpNumber = 1;

    var vsr_ns = try VsrNewState.init(allocs, view_no, op_no, commit_no, vsr_op_log);
    defer vsr_ns.deinit(allocs);

    logger.debug()
        .msg("Marshalling NS")
        .any("vsr_ns", vsr_ns)
        .log();

    const wire_buffer = try allocator.alloc(u8, vsr_ns.calcSize());
    defer allocator.free(wire_buffer);

    var wire_writer = std.Io.Writer.fixed(wire_buffer);
    const cb_marshalled = try vsr_ns.marshal(&wire_writer);
    assert(cb_marshalled == vsr_ns.calcSize());

    try wire_writer.flush();

    var wire_reader = std.Io.Reader.fixed(wire_buffer);
    var unmarshalled_ns = try VsrNewState.unmarshal(allocs, &wire_reader);
    defer unmarshalled_ns.deinit(allocs);

    logger.debug()
        .msg("Unmarshalled NS")
        .any("unmarshalled_ns", unmarshalled_ns)
        .log();

    try testing.expectEqual(vsr_ns.view_no, unmarshalled_ns.view_no);
    try testing.expectEqual(vsr_ns.op_no, unmarshalled_ns.op_no);
    try testing.expectEqual(vsr_ns.commit_no, unmarshalled_ns.commit_no);
    try testing.expectEqual(vsr_ns.op_log.log_seq_offset, unmarshalled_ns.op_log.log_seq_offset);
    try testing.expectEqualStrings(vsr_ns.op_log.ops_entries.items[0].op.append.msg_frame_buf, unmarshalled_ns.op_log.ops_entries.items[0].op.append.msg_frame_buf);
}

/// VSR message wire types
pub const VsrMessageType = enum {
    vsr_request,
    vsr_prepare,
    vsr_prepare_ok,
    vsr_reply,
    vsr_commit,
    vsr_svc,
    vsr_dvc,
    vsr_sv,
    vsr_gs,
    vsr_ns,
};

/// VSR message wrapper.
///
/// This wrapper facilitates easier unmarshalling and marshalling of different types of messages
/// to a single source or sink, respectively.
pub const VsrMessage = union(VsrMessageType) {
    vsr_request: VsrRequest,
    vsr_prepare: VsrPrepare,
    vsr_prepare_ok: VsrPrepareOK,
    vsr_reply: VsrReply,
    vsr_commit: VsrCommit,
    vsr_svc: VsrStartViewChange,
    vsr_dvc: VsrDoViewChange,
    vsr_sv: VsrStartView,
    vsr_gs: VsrGetState,
    vsr_ns: VsrNewState,

    /// De-initializes the VSR message, releasing resources.
    pub fn deinit(self: *VsrMessage, allocs: Allocators) void {
        switch (self.*) {
            .vsr_request => |_vsr_request| _vsr_request.deinit(allocs),
            .vsr_prepare => |_vsr_prepare| _vsr_prepare.deinit(allocs),
            .vsr_prepare_ok => |_| {},
            .vsr_reply => |_| {},
            .vsr_commit => |_| {},
            .vsr_svc => |_| {},
            .vsr_dvc => |*_vsr_dvc| _vsr_dvc.deinit(allocs),
            .vsr_sv => |*_vsr_sv| _vsr_sv.deinit(allocs),
            .vsr_gs => |_| {},
            .vsr_ns => |*_vsr_ns| _vsr_ns.deinit(allocs),
        }
    }

    /// Clones this VSR message.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn clone(self: *const VsrMessage, allocs: Allocators) AllocationError!VsrMessage {
        return switch (self.*) {
            .vsr_request => |_vsr_request| .{ .vsr_request = try _vsr_request.clone(allocs) },
            .vsr_prepare => |_vsr_prepare| .{ .vsr_prepare = try _vsr_prepare.clone(allocs) },
            .vsr_prepare_ok => |_vsr_prepare_ok| .{ .vsr_prepare_ok = _vsr_prepare_ok },
            .vsr_reply => |_vsr_reply| .{ .vsr_reply = _vsr_reply },
            .vsr_commit => |_vsr_commit| .{ .vsr_commit = _vsr_commit },
            .vsr_svc => |_vsr_svc| .{ .vsr_svc = _vsr_svc },
            .vsr_dvc => |_vsr_dvc| .{ .vsr_dvc = try _vsr_dvc.clone(allocs) },
            .vsr_sv => |_vsr_sv| .{ .vsr_sv = try _vsr_sv.clone(allocs) },
            .vsr_gs => |_vsr_gs| .{ .vsr_gs = _vsr_gs },
            .vsr_ns => |_vsr_ns| .{ .vsr_ns = try _vsr_ns.clone(allocs) },
        };
    }

    /// Renders the VSR message to a human-friendly format.
    pub fn format(self: *const VsrMessage, writer: *std.Io.Writer) !void {
        switch (self.*) {
            .vsr_request => |_vsr_request| try _vsr_request.format(writer),
            .vsr_prepare => |_vsr_prepare| try _vsr_prepare.format(writer),
            .vsr_prepare_ok => |_vsr_prepare_ok| try _vsr_prepare_ok.format(writer),
            .vsr_reply => |_vsr_reply| try _vsr_reply.format(writer),
            .vsr_commit => |_vsr_commit| try _vsr_commit.format(writer),
            .vsr_svc => |_vsr_svc| try _vsr_svc.format(writer),
            .vsr_dvc => |_vsr_dvc| try _vsr_dvc.format(writer),
            .vsr_sv => |_vsr_sv| try _vsr_sv.format(writer),
            .vsr_gs => |_vsr_gs| try _vsr_gs.format(writer),
            .vsr_ns => |_vsr_ns| try _vsr_ns.format(writer),
        }
    }

    /// Calculates the marshalled message size
    pub inline fn calcSize(self: *const VsrMessage) usize {
        const pre_calced: usize = @sizeOf(u64);
        return pre_calced + switch (self.*) {
            .vsr_request => |_vsr_request| _vsr_request.calcSize(),
            .vsr_prepare => |_vsr_prepare| _vsr_prepare.calcSize(),
            .vsr_prepare_ok => |_vsr_prepare_ok| _vsr_prepare_ok.calcSize(),
            .vsr_reply => |_vsr_reply| _vsr_reply.calcSize(),
            .vsr_commit => |_vsr_commit| _vsr_commit.calcSize(),
            .vsr_svc => |_vsr_svc| _vsr_svc.calcSize(),
            .vsr_dvc => |_vsr_dvc| _vsr_dvc.calcSize(),
            .vsr_sv => |_vsr_sv| _vsr_sv.calcSize(),
            .vsr_gs => |_vsr_gs| _vsr_gs.calcSize(),
            .vsr_ns => |_vsr_ns| _vsr_ns.calcSize(),
        };
    }

    /// Marshals the in-memory representation of the VSR message wire format, returning number of bytes written.
    pub fn marshal(self: *const VsrMessage, writer: *std.Io.Writer) MarshallingError!usize {
        const pre_calced: usize = @sizeOf(u64);
        writer.writeInt(u64, @as(u64, @intFromEnum(self.*)), .little) catch return MarshallingError.WriteFailed;
        return pre_calced + try switch (self.*) {
            .vsr_request => |_vsr_request| _vsr_request.marshal(writer),
            .vsr_prepare => |_vsr_prepare| _vsr_prepare.marshal(writer),
            .vsr_prepare_ok => |_vsr_prepare_ok| _vsr_prepare_ok.marshal(writer),
            .vsr_reply => |_vsr_reply| _vsr_reply.marshal(writer),
            .vsr_commit => |_vsr_commit| _vsr_commit.marshal(writer),
            .vsr_svc => |_vsr_svc| _vsr_svc.marshal(writer),
            .vsr_dvc => |_vsr_dvc| _vsr_dvc.marshal(writer),
            .vsr_sv => |_vsr_sv| _vsr_sv.marshal(writer),
            .vsr_gs => |_vsr_gs| _vsr_gs.marshal(writer),
            .vsr_ns => |_vsr_ns| _vsr_ns.marshal(writer),
        };
    }

    /// Unmarshals the VSR message wire format into an in-memory representation.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn unmarshal(allocs: Allocators, reader: *std.Io.Reader) UnmarshallingError!VsrMessage {
        const marshalled_message_type: u64 = reader.takeInt(u64, .little) catch return UnmarshallingError.ReadFailed;
        const message_type: VsrMessageType = @enumFromInt(marshalled_message_type);
        return switch (message_type) {
            .vsr_request => .{ .vsr_request = try VsrRequest.unmarshal(allocs, reader) },
            .vsr_prepare => .{ .vsr_prepare = try VsrPrepare.unmarshal(allocs, reader) },
            .vsr_prepare_ok => .{ .vsr_prepare_ok = try VsrPrepareOK.unmarshal(reader) },
            .vsr_reply => .{ .vsr_reply = try VsrReply.unmarshal(allocs, reader) },
            .vsr_commit => .{ .vsr_commit = try VsrCommit.unmarshal(allocs, reader) },
            .vsr_svc => .{ .vsr_svc = try VsrStartViewChange.unmarshal(reader) },
            .vsr_dvc => .{ .vsr_dvc = try VsrDoViewChange.unmarshal(allocs, reader) },
            .vsr_sv => .{ .vsr_sv = try VsrStartView.unmarshal(allocs, reader) },
            .vsr_gs => .{ .vsr_gs = try VsrGetState.unmarshal(reader) },
            .vsr_ns => .{ .vsr_ns = try VsrNewState.unmarshal(allocs, reader) },
        };
    }
};

// Test VsrMessage wire format (marshalling/unmarshalling)
test "VsrMessage.wire" {
    std.debug.print("test.VsrMessage.wire\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = module_tests_log_level;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var frame_allocator_holder = dymes_msg.FrameAllocator.init(allocator);
    const frame_allocator = frame_allocator_holder.allocator();

    const allocs: Allocators = .{
        .gpa = allocator,
        .msg_frame_allocator = frame_allocator,
    };

    var ulid_generator = common.ulid.generator();

    var logger = logging.logger("test.VsrMessage.wire");

    logger.debug()
        .msg("Preparing wrapped SV message")
        .log();

    const msg_to_append_id = try ulid_generator.next();
    const msg_to_append_payload = "I want to be appended";

    const op_no: OpNumber = 1066;

    var vsr_op_log = ops.VsrOpLog.init();
    defer vsr_op_log.deinit(allocs);

    {
        const msg_to_append = try dymes_msg.Message.init(frame_allocator, op_no, msg_to_append_id, 702, 0, msg_to_append_payload, .{});
        defer msg_to_append.deinit(frame_allocator);

        const append_op = try ops.VsrAppendOp.init(frame_allocator, msg_to_append);

        const vsr_op: ops.VsrOp = .{ .append = append_op };
        defer vsr_op.deinit(allocs);

        const vsr_op_response: ops.VsrOpResponse = .{ .append = .{ .msg_ulid = msg_to_append_id } };
        try vsr_op_log.addEntry(allocs, 0, vsr_op, vsr_op_response);
    }

    const view_no: ViewNumber = 2;

    var vsr_sv = try VsrStartView.init(allocs, view_no, op_no, op_no, vsr_op_log);
    defer vsr_sv.deinit(allocs);

    var vsr_msg: VsrMessage = .{ .vsr_sv = try vsr_sv.clone(allocs) };
    defer vsr_msg.deinit(allocs);

    logger.debug()
        .msg("Marshalling wrapped SV message")
        .any("vsr_msg", vsr_msg)
        .log();

    const wire_buffer = try allocator.alloc(u8, vsr_msg.calcSize());
    defer allocator.free(wire_buffer);

    var wire_writer = std.Io.Writer.fixed(wire_buffer);
    const cb_marshalled = try vsr_msg.marshal(&wire_writer);
    assert(cb_marshalled == vsr_msg.calcSize());

    try wire_writer.flush();

    var wire_reader = std.Io.Reader.fixed(wire_buffer);
    var unmarshalled_msg = try VsrMessage.unmarshal(allocs, &wire_reader);
    defer unmarshalled_msg.deinit(allocs);

    logger.debug()
        .msg("Unmarshalled wrapped SV message")
        .any("unmarshalled_sv", unmarshalled_msg)
        .log();

    try testing.expectEqual(vsr_msg.vsr_sv.view_no, unmarshalled_msg.vsr_sv.view_no);
    try testing.expectEqual(vsr_msg.vsr_sv.op_no, unmarshalled_msg.vsr_sv.op_no);
    try testing.expectEqual(vsr_msg.vsr_sv.op_log.log_seq_offset, unmarshalled_msg.vsr_sv.op_log.log_seq_offset);
    try testing.expectEqualStrings(vsr_msg.vsr_sv.op_log.ops_entries.items[0].op.append.msg_frame_buf, unmarshalled_msg.vsr_sv.op_log.ops_entries.items[0].op.append.msg_frame_buf);
}
