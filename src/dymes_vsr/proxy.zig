//! Dymes VSR client proxy.
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
const Logger = logging.Logger;

const common_ulid = dymes_common.ulid;
const Ulid = common_ulid.Ulid;
pub const FutureUlid = dymes_common.util.Future(Ulid);

const constants = @import("constants.zig");

const zimq = @import("zimq");

const vsr_ops = @import("ops.zig");
const vsr_msg = @import("vsr_msg.zig");
const vsr_wire = @import("wire.zig");

const Allocators = vsr_wire.Allocators;

const VsrMessage = vsr_msg.VsrMessage;
const VsrRequest = vsr_msg.VsrRequest;
const VsrReply = vsr_msg.VsrReply;

const VsrOpResponse = vsr_ops.VsrOpResponse;
const VsrOpType = vsr_ops.VsrOpType;
const VsrAppendOp = vsr_ops.VsrAppendOp;
const VsrAppendResponse = vsr_ops.VsrAppendResponse;

const ViewNumber = vsr_wire.ViewNumber;
const ClientId = vsr_wire.ClientId;
const RequestNumber = vsr_wire.RequestNumber;

const CommitNumber = vsr_wire.CommitNumber;

const vsr_limits = @import("limits.zig");
const vsr_state = @import("state.zig");
const VsrConfiguration = vsr_state.VsrConfiguration;
const VsrState = vsr_state.VsrState;

const vsr_transport = @import("transport.zig");
const ClientProxyTransport = vsr_transport.ClientProxyTransport;
const TransportCreationError = vsr_transport.TransportCreationError;
const TransportSendError = vsr_transport.TransportSendError;
const TransportReceiveError = vsr_transport.TransportReceiveError;

const dymes_msg = @import("dymes_msg");

const dymes_errors = @import("dymes_common").errors;
pub const AllocationError = dymes_errors.AllocationError;
const UsageError = dymes_errors.UsageError;
const SocketError = dymes_errors.SocketError;
pub const StateError = dymes_errors.StateError;

const dymes_engine = @import("dymes_engine");
const Engine = dymes_engine.Engine;

// Log level for embedded tests
const module_tests_log_level = logging.LogLevel.none;

pub const AppendCompletionEntry = struct {
    request_no: RequestNumber,
    future_ulid: *FutureUlid,
};

pub const CompletionEntry = union(VsrOpType) {
    append: AppendCompletionEntry,
};

/// Request completion map.
///
/// Entries are added when raising a REQUEST, and updated during RESPONSEs.
pub const CompletionMap = std.AutoHashMap(ClientId, CompletionEntry);

/// Dymes VSR client proxy.
pub const VsrClientProxy = struct {
    const component_name = "vsr.VsrClientProxy";

    pub const InitError = UsageError || AllocationError || SocketError;
    pub const ProxyError = AllocationError || TransportSendError;

    logger: *Logger,
    allocs: Allocators,
    config: VsrConfiguration,
    transport: ClientProxyTransport,
    tick_duration: u32,
    replica_no: u8,
    view_no: ViewNumber,

    last_msg_ms: i64,

    completion_mtx: std.Thread.Mutex,
    completion_map: CompletionMap,

    /// Initializes the state machine using Dymes configuration.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn init(
        allocs: Allocators,
        zimq_ctx: *zimq.Context,
        tick_duration: u32,
        replica_no: u8,
        vsr_config: VsrConfiguration,
    ) InitError!VsrClientProxy {
        var logger = logging.logger(component_name);
        if (tick_duration < constants.min_machine_tick_duration) {
            logger.warn()
                .msg("Client proxy tick duration too short")
                .int("tick_duration", tick_duration)
                .int("min_tick_duration", constants.min_machine_tick_duration)
                .log();
            return InitError.IllegalArgument;
        }

        assert(tick_duration >= constants.min_machine_tick_duration);

        logger.fine()
            .msg("Starting client proxy")
            .int("replica_no", replica_no)
            .log();

        const vsr_config_cloned = vsr_config.clone(allocs.gpa) catch return InitError.OutOfMemory;
        errdefer vsr_config_cloned.deinit(allocs.gpa);

        var completion_map = CompletionMap.init(allocs.gpa);
        errdefer completion_map.deinit();

        // Bring up our transport

        const client_proxy_transport = ClientProxyTransport.init(allocs, replica_no, zimq_ctx) catch |_e| {
            logger.warn()
                .msg("Failed to initialize client proxy transport")
                .err(_e)
                .int("replica_no", replica_no)
                .log();
            return InitError.SocketFailure;
        };
        errdefer client_proxy_transport.deinit();

        defer logger.debug()
            .msg("Client proxy started")
            .int("replica_no", replica_no)
            .log();

        return .{
            .logger = logger,
            .allocs = allocs,
            .config = vsr_config_cloned,
            .transport = client_proxy_transport,
            .tick_duration = tick_duration,
            .replica_no = replica_no,
            .view_no = 0,
            .last_msg_ms = std.time.milliTimestamp(),
            .completion_map = completion_map,
            .completion_mtx = .{},
        };
    }

    /// De-initializes the client proxy, releasing resources.
    pub fn deinit(self: *VsrClientProxy) void {
        self.logger.fine()
            .msg("Shutting down client proxy")
            .int("replica_no", self.replica_no)
            .log();
        defer self.logger.debug()
            .msg("Client proxy shut down")
            .int("replica_no", self.replica_no)
            .log();

        defer self.transport.deinit();
        defer self.config.deinit(self.allocs.gpa);
        defer self.completion_map.deinit();
    }

    /// Ticks the client proxy state machine.
    ///
    /// Returns true if messages were processed, false otherwise.
    pub fn tick(self: *VsrClientProxy) !bool {
        // Poll for VSR messages
        const poll_ms: usize = self.tick_duration;
        const current_ms = std.time.milliTimestamp();
        const vsr_messages: []vsr_msg.VsrMessage = try self.transport.poll(poll_ms) orelse {
            // Take action if we've been left idle too long
            // TODO: Client/Replica PING/PONG

            // const idle_ms = current_ms - self.last_msg_ms;
            return false;
        };

        // Now process polled VSR messages
        defer self.allocs.gpa.free(vsr_messages);
        defer for (vsr_messages) |*_vsr_message| {
            _vsr_message.deinit(self.allocs);
        };
        self.last_msg_ms = current_ms;

        for (vsr_messages) |_vsr_msg| {
            switch (_vsr_msg) {
                .vsr_reply => |_vsr_reply| try self.reply(_vsr_reply),
                else => {
                    self.logger.warn()
                        .msg("Received unexpected VSR message")
                        .int("replica_no", self.replica_no)
                        .any("vsr_msg", _vsr_msg)
                        .log();
                },
            }
        }
        return vsr_messages.len > 0;
    }

    /// Raises an append request.
    ///
    /// NB: The `future_ulid`'s lifetime *must* exceed ours!
    ///
    /// We prepare a completion slot and submit a VSR <REQUEST> to our replica,
    /// on a future tick() we handle any potential <REPLY> to earlier <REQUESTs>.
    pub fn append(
        self: *VsrClientProxy,
        client_id: ClientId,
        request_no: RequestNumber,
        append_msg: dymes_msg.Message,
        future_ulid: *FutureUlid,
    ) ProxyError!void {
        self.logger.fine()
            .msg("Raising append request")
            .int("replica_no", self.replica_no)
            .int("view_no", self.view_no)
            .int("client_id", client_id)
            .int("request_no", request_no)
            .ulid("msg_id", append_msg.id())
            .log();

        var vsr_request_msg: VsrMessage = .{ .vsr_request = .{
            .client_id = client_id,
            .request_no = request_no,
            .op = .{ .append = try VsrAppendOp.init(self.allocs.msg_frame_allocator, append_msg) },
        } };
        defer vsr_request_msg.deinit(self.allocs);

        // Prepare completion slot
        {
            self.completion_mtx.lock();
            defer self.completion_mtx.unlock();

            self.completion_map.put(client_id, .{ .append = .{
                .request_no = request_no,
                .future_ulid = future_ulid,
            } }) catch |_e| {
                self.logger.err()
                    .msg("Failed to prepare slot for append request")
                    .err(_e)
                    .int("replica_no", self.replica_no)
                    .ulid("msg_id", append_msg.id())
                    .log();
            };
        }
        errdefer {
            self.completion_mtx.lock();
            defer self.completion_mtx.unlock();
            _ = self.completion_map.remove(client_id);
        }

        // Send <REQUEST> to replica
        self.transport.sendToReplica(&vsr_request_msg) catch |_e| {
            self.logger.err()
                .msg("Failed to raise append request")
                .int("replica_no", self.replica_no)
                .err(_e)
                .ulid("msg_id", append_msg.id())
                .log();
            return _e;
        };
    }

    /// Handles VsrReply <REPLY> in client proxy.
    fn reply(self: *VsrClientProxy, vsr_reply: VsrReply) ProxyError!void {
        self.logger.fine()
            .ctx("REPLY")
            .msg("Processing client reply")
            .int("replica_no", self.replica_no)
            .int("our_view_no", self.view_no)
            .int("their_view_no", vsr_reply.view_no)
            .int("client_id", vsr_reply.client_id)
            .int("request_no", vsr_reply.request_no)
            .log();

        // Update completion slot
        {
            self.completion_mtx.lock();
            defer self.completion_mtx.unlock();

            if (self.completion_map.getPtr(vsr_reply.client_id)) |_entry| {
                switch (_entry.*) {
                    .append => |*_append_entry| {
                        if (vsr_reply.request_no != _append_entry.request_no) {
                            self.logger.warn()
                                .ctx("REPLY")
                                .msg("Client has mismatched reply")
                                .int("replica_no", self.replica_no)
                                .int("our_view_no", self.view_no)
                                .int("their_view_no", vsr_reply.view_no)
                                .int("client_id", vsr_reply.client_id)
                                .int("expected_request_no", _append_entry.request_no)
                                .int("actual_request_no", vsr_reply.request_no)
                                .log();
                            return;
                        }
                        _append_entry.future_ulid.supply(vsr_reply.response.append.msg_ulid);
                    },
                }
            }
        }

        self.logger.debug()
            .ctx("REPLY")
            .msg("Client reply processed")
            .int("replica_no", self.replica_no)
            .int("our_view_no", self.view_no)
            .int("their_view_no", vsr_reply.view_no)
            .int("client_id", vsr_reply.client_id)
            .int("request_no", vsr_reply.request_no)
            .log();
    }
};

// Smoke test VsrClientProxy
test "VsrClientProxy.smoke" {
    std.debug.print("test.VsrClientProxy.smoke\n", .{});
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

    // Prepare ZeroMQ context
    const zimq_context: *zimq.Context = try .init();
    defer zimq_context.deinit();

    const vsr_cfg: VsrConfiguration = cfg_val: {
        var vsr_cfg_bld = VsrConfiguration.Builder.init();
        defer vsr_cfg_bld.deinit(allocs.gpa);

        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1541);
        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1542);
        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1543);

        break :cfg_val try vsr_cfg_bld.build(allocs.gpa);
    };
    defer vsr_cfg.deinit(allocs.gpa);

    var client_proxy = try VsrClientProxy.init(allocs, zimq_context, 100, 0, vsr_cfg);
    defer client_proxy.deinit();
}

// Smoke test VsrClientProxy request/reply handling
test "VsrClientProxy.requestReply" {
    std.debug.print("test.VsrClientProxy.requestReply\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = module_tests_log_level;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();

    var logger = logging.logger("VsrClientProxy.requestReply");

    var ulid_gen = common_ulid.generator();

    var frame_allocator_holder = dymes_msg.FrameAllocator.init(allocator);
    const frame_allocator = frame_allocator_holder.allocator();

    const allocs: Allocators = .{
        .gpa = allocator,
        .msg_frame_allocator = frame_allocator,
    };

    // Prepare ZeroMQ context
    const zimq_context: *zimq.Context = try .init();
    defer zimq_context.deinit();

    var replica_push_zsock = zimq.Socket.init(zimq_context, .push) catch |e| return switch (e) {
        zimq.Socket.InitError.TooManyOpenFiles => TransportCreationError.LimitReached,
        zimq.Socket.InitError.InvalidContext => TransportCreationError.IllegalArgument,
        zimq.Socket.InitError.Unexpected => TransportCreationError.OtherCreationFailure,
    };
    defer replica_push_zsock.deinit();
    replica_push_zsock.connect(vsr_transport.local_client_proxy_endpoint) catch return TransportCreationError.ConnectFailure;

    var replica_pull_zsock = zimq.Socket.init(zimq_context, .pull) catch |e| return switch (e) {
        zimq.Socket.InitError.TooManyOpenFiles => TransportCreationError.LimitReached,
        zimq.Socket.InitError.InvalidContext => TransportCreationError.IllegalArgument,
        zimq.Socket.InitError.Unexpected => TransportCreationError.OtherCreationFailure,
    };
    defer replica_pull_zsock.deinit();
    {
        const inproc_replica_endpoint_name = try vsr_transport.inprocEndpointName(allocs.gpa, 0);
        defer allocs.gpa.free(inproc_replica_endpoint_name);
        replica_pull_zsock.bind(inproc_replica_endpoint_name) catch return TransportCreationError.BindFailure;
    }

    var replica_poller = zimq.Poller.init() catch return TransportCreationError.OutOfMemory;
    defer replica_poller.deinit();
    replica_poller.add(replica_pull_zsock, null, .in) catch |e| return switch (e) {
        error.SocketInvalid => TransportCreationError.IllegalArgument,
        error.TooManyFilesPolled => TransportCreationError.LimitReached,
        error.NoMemory => TransportCreationError.OutOfMemory,
        error.SocketAdded, error.Unexpected => TransportCreationError.OtherCreationFailure,
    };

    const vsr_cfg: VsrConfiguration = cfg_val: {
        var vsr_cfg_bld = VsrConfiguration.Builder.init();
        defer vsr_cfg_bld.deinit(allocs.gpa);

        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1541);
        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1542);
        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1543);

        break :cfg_val try vsr_cfg_bld.build(allocs.gpa);
    };
    defer vsr_cfg.deinit(allocs.gpa);

    var client_proxy = try VsrClientProxy.init(allocs, zimq_context, 100, 0, vsr_cfg);
    defer client_proxy.deinit();

    const client_id: u64 = 1000;
    const request_no: u64 = 1;
    const msg_id: Ulid = try ulid_gen.next();
    const append_msg = try dymes_msg.Message.init(allocs.msg_frame_allocator, 0, msg_id, 0, 0, "Something to be appended", .{});
    defer append_msg.deinit(allocs.msg_frame_allocator);

    var future_ulid: FutureUlid = .empty;

    try client_proxy.append(client_id, request_no, append_msg, &future_ulid);

    var wire_buffer: [1024]u8 = undefined;

    {
        const polled_msg = try vsr_transport.pollHelper(allocs, logger, replica_poller, 500) orelse return error.Unexpected;
        defer allocs.gpa.free(polled_msg);
        for (polled_msg) |*_message| {
            defer _message.deinit(allocs);
            switch (_message.*) {
                .vsr_request => |vsr_req| {
                    const op_response: VsrOpResponse = .{
                        .append = .{
                            .msg_ulid = vsr_req.op.append.asMessage().id(),
                        },
                    };
                    const vsr_reply_msg: VsrMessage = .{
                        .vsr_reply = .{
                            .view_no = 0,
                            .client_id = vsr_req.client_id,
                            .request_no = vsr_req.request_no,
                            .response = op_response,
                        },
                    };
                    try vsr_transport.sendToNodeHelper(
                        logger,
                        replica_push_zsock,
                        &wire_buffer,
                        &vsr_reply_msg,
                    );
                },
                else => {},
            }
        }
    }

    while (try client_proxy.tick()) {
        logger.fine()
            .msg("Ticking client proxy...")
            .log();
    }

    try testing.expectEqual(msg_id, try future_ulid.get(100));
}
