//! Dymes VSR operations.
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
const errors = common.errors;
const dymes_msg = @import("dymes_msg");
const limits = @import("limits.zig");
const constants = @import("constants.zig");
const wire = @import("wire.zig");

// Log level for embedded tests
const module_tests_log_level = logging.LogLevel.none;

const DymesMsgFrameHeader = dymes_msg.FrameHeader;

const AllocationError = wire.AllocationError;
const MarshallingError = wire.MarshallingError;
const UnmarshallingError = wire.UnmarshallingError;
const UsageError = errors.UsageError;

const OpNumber = wire.OpNumber;

const Allocators = wire.Allocators;

pub const Ulid = common.ulid.Ulid;

/// VSR operation type.
///
/// Dymes-specific: Since queries are currently performed via the HTTP API,
/// we only have to deal with `append` operations.
pub const VsrOpType = enum {
    append,
};

/// VSR client append operation.
///
/// Dymes-specific: The `append-op` is represented by a Dymes message frame.
pub const VsrAppendOp = struct {
    msg_frame_buf: []u8,

    /// Initializes the `append-op` from a Dymes message.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn init(msg_frame_allocator: std.mem.Allocator, append_msg: dymes_msg.Message) AllocationError!VsrAppendOp {
        return .{
            .msg_frame_buf = try append_msg.dupeFrame(msg_frame_allocator),
        };
    }

    /// De-initializes the `append-op`, releasing resources.
    pub fn deinit(self: *const VsrAppendOp, msg_frame_allocator: std.mem.Allocator) void {
        msg_frame_allocator.free(self.msg_frame_buf);
    }

    /// Clones the `append-op`.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn clone(self: *const VsrAppendOp, msg_frame_allocator: std.mem.Allocator) AllocationError!VsrAppendOp {
        return .{
            .msg_frame_buf = msg_frame_allocator.dupe(u8, self.msg_frame_buf) catch return AllocationError.OutOfMemory,
        };
    }

    /// Render the message frame buffer as a Dymes Message.
    ///
    /// Do *not* call `deinit()` on the returned message, it's a _view_, ownership is not transferred.
    pub inline fn asMessage(self: *const VsrAppendOp) dymes_msg.Message {
        return dymes_msg.Message.overlay(self.msg_frame_buf);
    }

    /// Calculates the marshalled operation size
    pub inline fn calcSize(self: *const VsrAppendOp) usize {
        const pre_calced: usize = @sizeOf(u64);
        const frame_header: *dymes_msg.FrameHeader = @ptrCast(@alignCast(self.msg_frame_buf));
        return pre_calced + frame_header.frame_size;
    }

    /// Marshals the in-memory representation of an `append-op` to wire format, returning number of bytes written.
    pub fn marshal(self: *const VsrAppendOp, writer: *std.Io.Writer) MarshallingError!usize {
        const pre_calced: usize = @sizeOf(u64);
        writer.writeInt(u64, self.msg_frame_buf.len, .little) catch return MarshallingError.WriteFailed;
        writer.writeAll(self.msg_frame_buf) catch return MarshallingError.WriteFailed;
        return pre_calced + self.msg_frame_buf.len;
    }

    /// Unmarshals the `append-op` wire format into an in-memory representation.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn unmarshal(allocs: Allocators, reader: *std.Io.Reader) UnmarshallingError!VsrAppendOp {
        const msg_frame_len = reader.takeInt(u64, .little) catch return UnmarshallingError.ReadFailed;
        const msg_frame_buf = reader.readAlloc(allocs.msg_frame_allocator, msg_frame_len) catch |e| {
            return switch (e) {
                std.Io.Reader.Error.ReadFailed, std.Io.Reader.Error.EndOfStream => UnmarshallingError.ReadFailed,
                error.OutOfMemory => UnmarshallingError.OutOfMemory,
            };
        };
        errdefer allocs.msg_frame_allocator.free(msg_frame_buf);

        return .{
            .msg_frame_buf = msg_frame_buf,
        };
    }

    pub fn format(self: *const VsrAppendOp, writer: *std.Io.Writer) !void {
        const frame_header: *dymes_msg.FrameHeader = @ptrCast(@alignCast(self.msg_frame_buf));
        try writer.print("{{log_sequence=0x{x}, ulid={f}}}", .{ frame_header.log_sequence, frame_header.id });
    }

    pub fn opNumber(self: *const VsrAppendOp) OpNumber {
        return self.asMessage().logSequence();
    }

    pub inline fn nilResponse(_: *const VsrAppendOp) VsrAppendResponse {
        return .{};
    }
};

// Test VsrAppendOp wire format (marshalling/unmarshalling)
test "VsrAppendOp.wire" {
    std.debug.print("test.VsrAppendOp.wire\n", .{});
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

    const allocators: Allocators = .{
        .gpa = allocator,
        .msg_frame_allocator = frame_allocator,
    };

    var ulid_generator = common.ulid.generator();

    var logger = logging.logger("test.VsrAppendOp.wire");

    logger.debug()
        .msg("Preparing append operation")
        .log();

    const msg_to_append_id = try ulid_generator.next();

    const msg_to_append_payload = "I want to be appended";

    const log_sequence_no: u64 = 0;
    const msg_to_append = try dymes_msg.Message.init(frame_allocator, log_sequence_no, msg_to_append_id, 702, 0, msg_to_append_payload, .{});
    defer msg_to_append.deinit(frame_allocator);

    const append_op = try VsrAppendOp.init(frame_allocator, msg_to_append);
    defer append_op.deinit(frame_allocator);

    logger.debug()
        .msg("Marshalling append operation")
        .any("append_op", append_op)
        .log();

    const wire_buffer = try allocator.alloc(u8, append_op.calcSize());
    defer allocator.free(wire_buffer);

    var wire_writer = std.Io.Writer.fixed(wire_buffer);

    const cb_marshalled = try append_op.marshal(&wire_writer);
    assert(cb_marshalled == append_op.calcSize());
    try wire_writer.flush();

    var wire_reader = std.Io.Reader.fixed(wire_buffer);

    const unmarshalled_op = try VsrAppendOp.unmarshal(allocators, &wire_reader);
    defer unmarshalled_op.deinit(allocators.msg_frame_allocator);

    logger.debug()
        .msg("Unmarshalled append operation")
        .any("unmarshalled_op", unmarshalled_op)
        .log();

    try testing.expectEqualStrings(append_op.msg_frame_buf, unmarshalled_op.msg_frame_buf);
}

/// VSR response to client append operation.
///
/// Dymes-specific: The `append-rsp` (once applied) represents the response to a client's append request.
pub const VsrAppendResponse = struct {
    const NIL_MSG_ID: Ulid = .{ .time = 0x0, .rand = 0x0 };
    msg_ulid: Ulid = NIL_MSG_ID,

    /// Calculates the marshalled size
    pub inline fn calcSize(_: *const VsrAppendResponse) usize {
        const pre_calced: usize = @sizeOf(Ulid);
        return pre_calced;
    }

    /// Marshals the in-memory representation of `append-rsp` to wire format, returning number of bytes written.
    pub fn marshal(self: *const VsrAppendResponse, writer: *std.Io.Writer) MarshallingError!usize {
        const pre_calced: usize = @sizeOf(Ulid);
        writer.writeInt(u128, self.msg_ulid.asInt(), .little) catch return MarshallingError.WriteFailed;
        return pre_calced;
    }

    /// Unmarshals the `append-rsp` wire format into an in-memory representation.
    pub fn unmarshal(_: Allocators, reader: *std.Io.Reader) UnmarshallingError!VsrAppendResponse {
        const raw_ulid = reader.takeInt(u128, .little) catch return UnmarshallingError.ReadFailed;
        return .{
            .msg_ulid = @bitCast(raw_ulid),
        };
    }

    pub fn format(self: *const VsrAppendResponse, writer: *std.Io.Writer) !void {
        try writer.print("{{msg_ulid={f}}}", .{self.msg_ulid});
    }
};

// Test VsrAppendResponse wire format (marshalling/unmarshalling)
test "VsrAppendResponse.wire" {
    std.debug.print("test.VsrAppendResponse.wire\n", .{});
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

    const allocators: Allocators = .{
        .gpa = allocator,
        .msg_frame_allocator = frame_allocator,
    };

    var logger = logging.logger("test.VsrAppendResponse.wire");

    logger.debug()
        .msg("Preparing append response")
        .log();

    var ulid_gen = common.ulid.generator();

    const append_rsp: VsrAppendResponse = .{ .msg_ulid = try ulid_gen.next() };

    logger.debug()
        .msg("Marshalling append response")
        .any("append_rsp", append_rsp)
        .log();

    const wire_buffer = try allocator.alloc(u8, append_rsp.calcSize());
    defer allocator.free(wire_buffer);

    var wire_writer = std.Io.Writer.fixed(wire_buffer);

    const cb_marshalled = try append_rsp.marshal(&wire_writer);
    assert(cb_marshalled == append_rsp.calcSize());

    try wire_writer.flush();

    var wire_reader = std.Io.Reader.fixed(wire_buffer);

    const unmarshalled_rsp = try VsrAppendResponse.unmarshal(allocators, &wire_reader);

    logger.debug()
        .msg("Unmarshalled append response")
        .any("unmarshalled_rsp", unmarshalled_rsp)
        .log();

    try testing.expectEqual(append_rsp, unmarshalled_rsp);
}

/// VSR operation.
pub const VsrOp = union(VsrOpType) {
    append: VsrAppendOp,

    /// De-initializes the VSR operation, releasing resources.
    pub fn deinit(self: *const VsrOp, allocs: Allocators) void {
        switch (self.*) {
            .append => |_append_op| {
                _append_op.deinit(allocs.msg_frame_allocator);
            },
        }
    }

    /// Clones the VSR operation.
    ///
    /// Caller must call `deinit()` to release resources.
    pub inline fn clone(self: *const VsrOp, allocs: Allocators) AllocationError!VsrOp {
        return switch (self.*) {
            .append => |_append_op| return .{ .append = try _append_op.clone(allocs.msg_frame_allocator) },
        };
    }

    /// Calculates the marshalled size
    pub inline fn calcSize(self: *const VsrOp) usize {
        return switch (self.*) {
            .append => |_append_op| @sizeOf(u64) + _append_op.calcSize(),
        };
    }

    /// Marshals the in-memory representation of `op` to wire format, returning number of bytes written.
    pub fn marshal(self: *const VsrOp, writer: *std.Io.Writer) MarshallingError!usize {
        switch (self.*) {
            .append => |_append_op| {
                writer.writeInt(u64, @intFromEnum(VsrOpType.append), .little) catch return MarshallingError.WriteFailed;
                return @sizeOf(u64) + try _append_op.marshal(writer);
            },
        }
    }

    /// Unmarshals the `op` wire format into an in-memory representation.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn unmarshal(allocs: Allocators, reader: *std.Io.Reader) UnmarshallingError!VsrOp {
        const raw_op_type = reader.takeInt(u64, .little) catch return UnmarshallingError.ReadFailed;
        const op_type: VsrOpType = @enumFromInt(raw_op_type);
        return switch (op_type) {
            .append => .{ .append = try VsrAppendOp.unmarshal(allocs, reader) },
        };
    }

    pub fn opNumber(self: *const VsrOp) OpNumber {
        return switch (self.*) {
            .append => |_append_op| _append_op.opNumber(),
        };
    }

    pub fn format(self: *const VsrOp, writer: *std.Io.Writer) !void {
        switch (self.*) {
            .append => |_append_op| try _append_op.format(writer),
        }
    }

    pub inline fn nilResponse(self: *const VsrOp, _: Allocators) AllocationError!VsrOpResponse {
        return switch (self.*) {
            .append => |_append_op| .{ .append = _append_op.nilResponse() },
        };
    }
};

// Test VsrOp wire format (marshalling/unmarshalling)
test "VsrOp.wire" {
    std.debug.print("test.VsrOp.wire\n", .{});
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

    const allocators: Allocators = .{
        .gpa = allocator,
        .msg_frame_allocator = frame_allocator,
    };

    var ulid_generator = common.ulid.generator();

    var logger = logging.logger("test.VsrOp.wire");

    logger.debug()
        .msg("Preparing append operation")
        .log();

    const msg_to_append_id = try ulid_generator.next();

    const msg_to_append_payload = "I want to be appended";

    const log_sequence_no: u64 = 1066;
    const msg_to_append = try dymes_msg.Message.init(frame_allocator, log_sequence_no, msg_to_append_id, 702, 0, msg_to_append_payload, .{});
    defer msg_to_append.deinit(frame_allocator);

    const vsr_op: VsrOp = .{ .append = try VsrAppendOp.init(frame_allocator, msg_to_append) };
    defer vsr_op.deinit(allocators);

    logger.debug()
        .msg("Marshalling VSR operation")
        .any("vsr_op", vsr_op)
        .log();

    const wire_buffer = try allocator.alloc(u8, vsr_op.calcSize());
    defer allocator.free(wire_buffer);

    var wire_writer = std.Io.Writer.fixed(wire_buffer);

    const cb_marshalled = try vsr_op.marshal(&wire_writer);
    assert(cb_marshalled == vsr_op.calcSize());
    try wire_writer.flush();

    var wire_reader = std.Io.Reader.fixed(wire_buffer);

    const unmarshalled_vsr_op = try VsrOp.unmarshal(allocators, &wire_reader);
    defer unmarshalled_vsr_op.deinit(allocators);

    logger.debug()
        .msg("Unmarshalled VSR operation")
        .any("unmarshalled_vsr_op", unmarshalled_vsr_op)
        .log();

    try testing.expectEqualStrings(vsr_op.append.msg_frame_buf, unmarshalled_vsr_op.append.msg_frame_buf);
    try testing.expectEqual(vsr_op.opNumber(), unmarshalled_vsr_op.opNumber());
}

/// VSR operation response.
pub const VsrOpResponse = union(VsrOpType) {
    append: VsrAppendResponse,

    /// Calculates the marshalled size
    pub inline fn calcSize(self: *const VsrOpResponse) usize {
        return switch (self.*) {
            .append => |_append_op_rsp| @sizeOf(u64) + _append_op_rsp.calcSize(),
        };
    }

    /// Marshals the in-memory representation of `op-rsp` to wire format, returning number of bytes written.
    pub fn marshal(self: *const VsrOpResponse, writer: *std.Io.Writer) MarshallingError!usize {
        switch (self.*) {
            .append => |_append_op_rsp| {
                writer.writeInt(u64, @intFromEnum(VsrOpType.append), .little) catch return MarshallingError.WriteFailed;
                return @sizeOf(u64) + try _append_op_rsp.marshal(writer);
            },
        }
    }

    /// Unmarshals the `op-rsp` wire format into an in-memory representation.
    pub fn unmarshal(allocators: Allocators, reader: *std.Io.Reader) UnmarshallingError!VsrOpResponse {
        const raw_op_type = reader.takeInt(u64, .little) catch return UnmarshallingError.ReadFailed;
        const op_type: VsrOpType = @enumFromInt(raw_op_type);
        return switch (op_type) {
            .append => .{ .append = try VsrAppendResponse.unmarshal(allocators, reader) },
        };
    }

    pub fn format(self: *const VsrOpResponse, writer: *std.Io.Writer) !void {
        switch (self.*) {
            .append => |_append_op| try _append_op.format(writer),
        }
    }
};

// Test VsrOpResponse wire format (marshalling/unmarshalling)
test "VsrOpResponse.wire" {
    std.debug.print("test.VsrOpResponse.wire\n", .{});
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

    const allocators: Allocators = .{
        .gpa = allocator,
        .msg_frame_allocator = frame_allocator,
    };

    var logger = logging.logger("test.VsrOpResponse.wire");

    logger.debug()
        .msg("Preparing VSR op response")
        .log();

    const vsr_rsp: VsrOpResponse = .{
        .append = .{},
    };

    logger.debug()
        .msg("Marshalling VSR op response")
        .any("vsr_rsp", vsr_rsp)
        .log();

    const wire_buffer = try allocator.alloc(u8, vsr_rsp.calcSize());
    defer allocator.free(wire_buffer);

    var wire_writer = std.Io.Writer.fixed(wire_buffer);

    const cb_marshalled = try vsr_rsp.marshal(&wire_writer);
    assert(cb_marshalled == vsr_rsp.calcSize());

    try wire_writer.flush();

    var wire_reader = std.Io.Reader.fixed(wire_buffer);

    const unmarshalled_vsr_rsp = try VsrOpResponse.unmarshal(allocators, &wire_reader);

    logger.debug()
        .msg("Unmarshalled VSR response")
        .any("unmarshalled_vsr_rsp", unmarshalled_vsr_rsp)
        .log();

    try testing.expectEqual(vsr_rsp, unmarshalled_vsr_rsp);
}

/// VSR operations log (op-log)
///
/// In 'reference' VSR, this is an array containing op-number entries containing the requests
/// that have been received so far in their assigned order, as well as responses sent to clients (if any).
///
/// Dymes-specific: The op-log contains the smallest subset of append operations that encompass
/// all possible outstanding ops, up to `max_oplog_entries`, after which ingestion will stall.
///
/// Since Dymes DataSets may be viewed as append-only logs, with the messages in the same monotonic order
/// as operations, even recovery from cold start would mean the replica will only need the section of the operation
/// log since its last committed operation (commit-num), i.e. the `log_sequence` of the last appended message.
pub const VsrOpLog = struct {
    const VsrOpEntry = struct {
        op: VsrOp,
        rsp: VsrOpResponse,

        pub fn deinit(self: *const VsrOpEntry, allocs: Allocators) void {
            self.op.deinit(allocs);
        }
        pub fn clone(self: *const VsrOpEntry, allocs: Allocators) AllocationError!VsrOpEntry {
            return .{
                .op = try self.op.clone(allocs),
                .rsp = self.rsp,
            };
        }
    };
    const VsrOpEntryList = std.ArrayList(VsrOpEntry);

    /// Log sequence offset
    log_seq_offset: OpNumber,

    /// Ops entries
    ops_entries: VsrOpEntryList,

    /// Initializes an `op-log`.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn init() VsrOpLog {
        return .{
            .log_seq_offset = 1,
            .ops_entries = .empty,
        };
    }

    /// De-initializes the `op-log`, releasing resources.
    pub fn deinit(self: *VsrOpLog, allocs: Allocators) void {
        defer self.ops_entries.deinit(allocs.gpa);
        for (self.ops_entries.items) |_vsr_op| {
            _vsr_op.deinit(allocs);
        }
    }

    /// Clones this `op-log`.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn clone(self: *const VsrOpLog, allocs: Allocators) AllocationError!VsrOpLog {

        // We need item-wise cloning of ops
        var ops_list = VsrOpEntryList.initCapacity(allocs.gpa, self.ops_entries.items.len) catch return AllocationError.OutOfMemory;
        errdefer ops_list.deinit(allocs.gpa);
        errdefer {
            for (ops_list.items) |_entry| {
                _entry.deinit(allocs);
            }
        }
        for (self.ops_entries.items) |_entry| {
            ops_list.appendBounded(try _entry.clone(allocs)) catch return AllocationError.OutOfMemory;
        }

        return .{
            .log_seq_offset = self.log_seq_offset,
            .ops_entries = ops_list,
        };
    }

    /// Adds an entry to the `op-log`.
    ///
    /// The `op-log` in-memory size is limited, if we hit the bounds, we slide out a portion of
    /// the op-log.
    pub fn addEntry(self: *VsrOpLog, allocs: Allocators, last_commit_no: OpNumber, vsr_op: VsrOp, vsr_op_rsp: VsrOpResponse) AllocationError!void {
        if (self.ops_entries.items.len >= limits.max_oplog_entries) {
            assert(self.ops_entries.items.len == limits.max_oplog_entries);
            const earliest_op_no = self.log_seq_offset - 1;
            const latest_op_no = earliest_op_no + self.ops_entries.items.len;

            const slide_gap: usize = latest_op_no - last_commit_no;
            if (slide_gap >= limits.max_oplog_entries) {
                var logger = logging.logger("vsr.VsrOpLog");
                logger.warn()
                    .msg("Op-log hot window cannot slide")
                    .int("log_seq_offset", self.log_seq_offset)
                    .int("ops_log_entry_count", self.ops_entries.items.len)
                    .int("latest_op_no", latest_op_no)
                    .int("last_commit_no", last_commit_no)
                    .int("slide_gap", slide_gap)
                    .log();
                return AllocationError.LimitReached;
            }

            const slide_size: usize = limits.max_oplog_entries - slide_gap;
            for (self.ops_entries.items[0..slide_size]) |*_item| {
                _item.deinit(allocs);
            }

            const slide_in_len = self.ops_entries.items.len - slide_size;
            @memmove(self.ops_entries.items[0..slide_in_len], self.ops_entries.items[slide_size..]);
            self.ops_entries.shrinkRetainingCapacity(slide_in_len);

            self.log_seq_offset += slide_size;
        }
        self.ops_entries.append(allocs.gpa, .{
            .op = try vsr_op.clone(allocs),
            .rsp = vsr_op_rsp,
        }) catch return AllocationError.OutOfMemory;
    }

    /// Removes the last entry from the `op-log`, if the `op-no` matches.
    ///
    /// Note: This is not for general use, but for erro handling code.
    pub fn removeEntry(self: *VsrOpLog, op_no: OpNumber) bool {
        if (op_no < self.log_seq_offset) {
            // LSN window undershot
            return false;
        }
        const adjusted_idx: usize = @intCast(op_no - self.log_seq_offset);
        if (adjusted_idx >= self.ops_entries.items.len) {
            // LSN window overshot
            return false;
        }
        if (adjusted_idx == self.ops_entries.items.len - 1) {
            self.ops_entries.shrinkRetainingCapacity(self.ops_entries.items.len - 1);
            return true;
        }
        return false;
    }

    /// Fetches a view of an op in the op-log.
    ///
    /// Do *not* call `deinit()` on the returned op, this is a _view_, not a clone.
    pub fn fetchOp(self: *const VsrOpLog, op_no: OpNumber) UsageError!VsrOp {
        if (op_no < self.log_seq_offset) {
            // LSN window undershot
            return UsageError.IllegalArgument;
        }
        const adjusted_idx: usize = @intCast(op_no - self.log_seq_offset);
        if (adjusted_idx >= self.ops_entries.items.len) {
            // LSN window overshot
            return UsageError.IllegalArgument;
        }
        return self.ops_entries.items[adjusted_idx].op;
    }

    /// Fetches a view of an op response in the op-log.
    pub fn fetchResponse(self: *const VsrOpLog, op_no: OpNumber) UsageError!VsrOpResponse {
        if (op_no < self.log_seq_offset) {
            // LSN window undershot
            return UsageError.IllegalArgument;
        }
        const adjusted_idx: usize = @intCast(op_no - self.log_seq_offset);
        if (adjusted_idx >= self.ops_entries.items.len) {
            // LSN window overshot
            return UsageError.IllegalArgument;
        }
        return self.ops_entries.items[adjusted_idx].rsp;
    }

    /// Sets an op response in the op-log.
    pub fn setResponse(self: *const VsrOpLog, op_no: OpNumber, vsr_op_rsp: VsrOpResponse) UsageError!void {
        if (op_no < self.log_seq_offset) {
            // LSN window undershot
            return UsageError.IllegalArgument;
        }
        const adjusted_idx: usize = @intCast(op_no - self.log_seq_offset);
        if (adjusted_idx >= self.ops_entries.items.len) {
            // LSN window overshot
            return UsageError.IllegalArgument;
        }
        self.ops_entries.items[adjusted_idx].rsp = vsr_op_rsp;
    }

    /// Renders the `op-log` to a human-friendly format.
    pub fn format(self: *const VsrOpLog, writer: *std.Io.Writer) !void {
        try writer.print("{{log_seq_offset=0x{x},ops_count={d}}}", .{ self.log_seq_offset, self.ops_entries.items.len });
    }

    /// Calculates the marshalled op-log size
    pub fn calcSize(self: *const VsrOpLog) usize {
        var total_size: usize = @sizeOf(u64) + @sizeOf(u64);
        for (self.ops_entries.items) |_entry| {
            total_size += _entry.op.calcSize();
            total_size += _entry.rsp.calcSize();
        }
        return total_size;
    }

    /// Marshals the in-memory representation of the `op-log` to wire format, returning number of bytes written.
    pub fn marshal(self: *const VsrOpLog, writer: *std.Io.Writer) MarshallingError!usize {
        writer.writeInt(u64, self.ops_entries.items.len, .little) catch return MarshallingError.WriteFailed;
        writer.writeInt(u64, self.log_seq_offset, .little) catch return MarshallingError.WriteFailed;
        var written: usize = @sizeOf(u64) + @sizeOf(u64);
        for (self.ops_entries.items) |_entry| {
            written += _entry.op.marshal(writer) catch return MarshallingError.WriteFailed;
            written += _entry.rsp.marshal(writer) catch return MarshallingError.WriteFailed;
        }
        return written;
    }

    /// Unmarshals the `op-log` wire format into an in-memory representation.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn unmarshal(allocs: Allocators, reader: *std.Io.Reader) UnmarshallingError!VsrOpLog {
        const num_ops_entries = reader.takeInt(u64, .little) catch return UnmarshallingError.ReadFailed;
        const log_seq_offset = reader.takeInt(u64, .little) catch return UnmarshallingError.ReadFailed;

        var ops_entries = VsrOpEntryList.initCapacity(allocs.gpa, num_ops_entries) catch return UnmarshallingError.OutOfMemory;
        errdefer ops_entries.deinit(allocs.gpa);
        errdefer {
            for (ops_entries.items) |_entry| {
                _entry.deinit(allocs);
            }
        }

        for (0..num_ops_entries) |_| {
            const vsr_op = try VsrOp.unmarshal(allocs, reader);
            errdefer vsr_op.deinit(allocs);
            const vsr_op_rsp = try VsrOpResponse.unmarshal(allocs, reader);
            ops_entries.appendBounded(.{
                .op = vsr_op,
                .rsp = vsr_op_rsp,
            }) catch return UnmarshallingError.OutOfMemory;
        }

        return .{
            .log_seq_offset = log_seq_offset,
            .ops_entries = ops_entries,
        };
    }
};

// Test VsrOpLog wire format (marshalling/unmarshalling)
test "VsrOpLog.wire" {
    std.debug.print("test.VsrOpLog.wire\n", .{});
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

    var logger = logging.logger("test.VsrOpLog.wire");

    logger.debug()
        .msg("Preparing append operation")
        .log();

    const msg_to_append_id = try ulid_generator.next();

    const msg_to_append_payload = "I want to be appended";

    const log_sequence_no: OpNumber = 0;
    const msg_to_append = try dymes_msg.Message.init(frame_allocator, log_sequence_no, msg_to_append_id, 702, 0, msg_to_append_payload, .{});
    defer msg_to_append.deinit(frame_allocator);

    const vsr_op: VsrOp = .{ .append = try VsrAppendOp.init(frame_allocator, msg_to_append) };
    defer vsr_op.deinit(allocs);

    const vsr_op_rsp: VsrOpResponse = .{
        .append = .{},
    };

    var vsr_op_log = VsrOpLog.init();
    defer vsr_op_log.deinit(allocs);

    try vsr_op_log.addEntry(allocs, 0, vsr_op, vsr_op_rsp);

    const fetched_op = try vsr_op_log.fetchOp(log_sequence_no + 1);
    try testing.expectEqualStrings(fetched_op.append.msg_frame_buf, vsr_op.append.msg_frame_buf);

    logger.debug()
        .msg("Marshalling VSR op-log")
        .any("vsr_op_log", vsr_op)
        .log();

    const wire_buffer = try allocator.alloc(u8, limits.max_append_frame_size);
    // const wire_buffer = try allocator.alloc(u8, vsr_op_log.calcSize());
    defer allocator.free(wire_buffer);

    var wire_writer = std.Io.Writer.fixed(wire_buffer);

    const cb_marshalled = try vsr_op_log.marshal(&wire_writer);
    assert(cb_marshalled == vsr_op_log.calcSize());

    try wire_writer.flush();

    var wire_reader = std.Io.Reader.fixed(wire_buffer);

    var unmarshalled_vsr_op_log = try VsrOpLog.unmarshal(allocs, &wire_reader);
    defer unmarshalled_vsr_op_log.deinit(allocs);

    logger.debug()
        .msg("Unmarshalled VSR op-log")
        .any("unmarshalled_vsr_op_log", unmarshalled_vsr_op_log)
        .log();

    try testing.expectEqual(vsr_op_log.log_seq_offset, unmarshalled_vsr_op_log.log_seq_offset);
    try testing.expectEqual(vsr_op_log.ops_entries.items.len, unmarshalled_vsr_op_log.ops_entries.items.len);
    try testing.expectEqualSlices(u8, vsr_op_log.ops_entries.items[0].op.append.msg_frame_buf, unmarshalled_vsr_op_log.ops_entries.items[0].op.append.msg_frame_buf);
}

// Test empty VsrOpLog wire format
test "VsrOpLog.wire.empty" {
    std.debug.print("test.VsrOpLog.wire.empty\n", .{});
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

    const allocators: Allocators = .{
        .gpa = allocator,
        .msg_frame_allocator = frame_allocator,
    };

    var vsr_op_log = VsrOpLog.init();
    defer vsr_op_log.deinit(allocators);

    const wire_buffer = try allocator.alloc(u8, vsr_op_log.calcSize());
    defer allocator.free(wire_buffer);

    var wire_writer = std.Io.Writer.fixed(wire_buffer);

    const cb_marshalled = try vsr_op_log.marshal(&wire_writer);
    assert(cb_marshalled == vsr_op_log.calcSize());

    try wire_writer.flush();

    var wire_reader = std.Io.Reader.fixed(wire_buffer);

    var unmarshalled_vsr_op_log = try VsrOpLog.unmarshal(allocators, &wire_reader);
    defer unmarshalled_vsr_op_log.deinit(allocators);

    try testing.expectEqual(vsr_op_log.log_seq_offset, unmarshalled_vsr_op_log.log_seq_offset);
    try testing.expectEqual(vsr_op_log.ops_entries.items.len, unmarshalled_vsr_op_log.ops_entries.items.len);
}

// Test VsrOpLog sliding
test "VsrOpLog.slide" {
    std.debug.print("test.VsrOpLog.slide\n", .{});
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

    var logger = logging.logger("test.VsrOpLog.slide");

    var vsr_op_log = VsrOpLog.init();
    defer vsr_op_log.deinit(allocs);

    try testing.expectEqual(vsr_op_log.log_seq_offset, 1);
    try testing.expectEqual(vsr_op_log.ops_entries.items.len, 0);

    for (0..limits.max_oplog_entries) |_idx| {
        const msg_to_append_id = try ulid_generator.next();

        const msg_to_append_payload = "I want to be appended";

        const log_sequence_no: OpNumber = @intCast(_idx);
        const msg_to_append = try dymes_msg.Message.init(frame_allocator, log_sequence_no, msg_to_append_id, 702, 0, msg_to_append_payload, .{});
        defer msg_to_append.deinit(frame_allocator);

        const vsr_op: VsrOp = .{ .append = try VsrAppendOp.init(frame_allocator, msg_to_append) };
        defer vsr_op.deinit(allocs);

        const vsr_op_rsp: VsrOpResponse = .{
            .append = .{},
        };

        try vsr_op_log.addEntry(allocs, 0, vsr_op, vsr_op_rsp);
    }

    for (0..limits.max_oplog_entries) |_idx| {
        const op = try vsr_op_log.fetchOp(_idx + 1);
        try testing.expectEqual(op.opNumber(), @as(u64, @intCast(_idx)));
    }

    logger.debug()
        .msg("Prepared to slide")
        .log();

    try testing.expectEqual(vsr_op_log.ops_entries.items.len, limits.max_oplog_entries);

    {
        const msg_to_append_id = try ulid_generator.next();

        const msg_to_append_payload = "I want to be appended";

        const log_sequence_no: OpNumber = @intCast(limits.max_oplog_entries);
        const msg_to_append = try dymes_msg.Message.init(frame_allocator, log_sequence_no, msg_to_append_id, 702, 0, msg_to_append_payload, .{});
        defer msg_to_append.deinit(frame_allocator);

        const vsr_op: VsrOp = .{ .append = try VsrAppendOp.init(frame_allocator, msg_to_append) };
        defer vsr_op.deinit(allocs);

        const vsr_op_rsp: VsrOpResponse = .{
            .append = .{
                .msg_ulid = msg_to_append.id(),
            },
        };

        try vsr_op_log.addEntry(allocs, vsr_op_log.ops_entries.items.len - 100, vsr_op, vsr_op_rsp);
    }
    try testing.expectEqual(101, vsr_op_log.ops_entries.items.len);
}
