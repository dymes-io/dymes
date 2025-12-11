//! Dymes VSR transport support.
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

const constants = @import("constants.zig");
const wire = @import("wire.zig");
const ClientId = wire.ClientId;
const RequestNumber = wire.RequestNumber;

const ops = @import("ops.zig");
const vsr_msg = @import("vsr_msg.zig");
const limits = @import("limits.zig");
const state = @import("state.zig");

const vsr_testing = @import("testing.zig");

const dymes_msg = @import("dymes_msg");

const zimq = @import("zimq");

// Log level for embedded tests
const module_tests_log_level = logging.LogLevel.none;

const Allocators = wire.Allocators;
const AllocationError = wire.AllocationError;
pub const TransportCreationError = dymes_common.errors.CreationError || dymes_common.errors.UsageError || dymes_common.errors.SocketError;
pub const TransportSendError = wire.MarshallingError || dymes_common.errors.SocketError || dymes_common.errors.StateError || dymes_common.errors.UsageError;
pub const TransportReceiveError = TransportSendError;

const wire_buffer_size: usize = dymes_msg.limits.max_frame_size + @sizeOf(vsr_msg.VsrMessage);

const push_high_water_mark: c_int = 1000;
const pull_high_water_mark: c_int = 3000;

/// Local replica endpoint base name format.
///
/// The local replica binds to this endpoint, and the client proxy connects to it.
pub const local_replica_endpoint_fmt: [:0]const u8 = "inproc://vsr_replica_{d}";

/// Local client proxy endpoint.
///
/// The client proxy binds to this endpoint, and the local replica connects to it.
pub const local_client_proxy_endpoint: [:0]const u8 = "inproc://vsr_client_proxy";

/// Dymes VSR replica transport.
pub const ReplicaTransport = struct {
    const component_name = "vsr.ReplicaTransport";
    const Self = @This();

    allocs: Allocators,
    logger: *Logger,
    replica_pull_zsock: *zimq.Socket,
    replica_poller: *zimq.Poller,
    peer_push_zsocks: []*zimq.Socket,
    client_push_zsock: *zimq.Socket,
    replica_no: wire.ReplicaNumber,
    wire_buffer: []u8,

    /// Initializes the replica transport.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn init(allocs: Allocators, replica_no: wire.ReplicaNumber, vsr_config: state.VsrConfiguration, context: *zimq.Context) TransportCreationError!Self {
        assert(replica_no <= vsr_config.num_replicas);
        var logger = logging.logger(component_name);
        logger.fine()
            .msg("Starting replica transport")
            .int("replica_no", replica_no)
            .int("wire_buffer_size", wire_buffer_size)
            .log();

        // Allocate wire buffer
        const wire_buffer = allocs.gpa.alignedAlloc(u8, .@"8", wire_buffer_size) catch return TransportCreationError.OutOfMemory;
        errdefer allocs.gpa.free(wire_buffer);

        // Prepare the zsocks

        var client_push_zsock = zimq.Socket.init(context, .push) catch |e| return switch (e) {
            zimq.Socket.InitError.TooManyOpenFiles => TransportCreationError.LimitReached,
            zimq.Socket.InitError.InvalidContext => TransportCreationError.IllegalArgument,
            zimq.Socket.InitError.Unexpected => TransportCreationError.OtherCreationFailure,
        };
        errdefer client_push_zsock.deinit();
        client_push_zsock.set(.sndhwm, push_high_water_mark) catch return TransportCreationError.OtherUsageFailure;
        client_push_zsock.connect(local_client_proxy_endpoint) catch |_e| {
            logger.warn()
                .msg("Failed to connect to client proxy")
                .err(_e)
                .int("replica_no", replica_no)
                .log();
            return TransportCreationError.BindFailure;
        };

        const peer_push_zsocks = allocs.gpa.alloc(*zimq.Socket, vsr_config.num_replicas) catch return TransportCreationError.OutOfMemory;
        errdefer allocs.gpa.free(peer_push_zsocks);

        for (0..vsr_config.num_replicas) |_idx| {
            if (_idx == replica_no) {
                // Minor voodoo to allow simple indexing
                // We set up our PULL zsock, and slot it into the PUSH zsocks slice
                peer_push_zsocks[_idx] = zimq.Socket.init(context, .pull) catch |_e| return switch (_e) {
                    zimq.Socket.InitError.TooManyOpenFiles => TransportCreationError.LimitReached,
                    zimq.Socket.InitError.InvalidContext => TransportCreationError.IllegalArgument,
                    zimq.Socket.InitError.Unexpected => TransportCreationError.OtherCreationFailure,
                };
                peer_push_zsocks[_idx].set(.rcvhwm, pull_high_water_mark) catch return TransportCreationError.OtherUsageFailure;
            } else {
                peer_push_zsocks[_idx] = zimq.Socket.init(context, .push) catch |_e| return switch (_e) {
                    zimq.Socket.InitError.TooManyOpenFiles => TransportCreationError.LimitReached,
                    zimq.Socket.InitError.InvalidContext => TransportCreationError.IllegalArgument,
                    zimq.Socket.InitError.Unexpected => TransportCreationError.OtherCreationFailure,
                };
                peer_push_zsocks[_idx].set(.sndhwm, push_high_water_mark) catch return TransportCreationError.OtherUsageFailure;
            }
        }
        errdefer for (peer_push_zsocks) |_zsock| {
            _zsock.deinit();
        };

        // Activate the zsocks
        for (0..vsr_config.num_replicas) |_idx| {
            const replica_endpoint = try tcpEndpointName(allocs.gpa, vsr_config.replicaHostname(_idx), vsr_config.replicaPort(_idx));
            defer allocs.gpa.free(replica_endpoint);
            if (_idx == replica_no) {
                // Bind PULL socket
                {
                    const inproc_endpoint_name = inprocEndpointName(allocs.gpa, replica_no) catch return TransportCreationError.OutOfMemory;
                    defer allocs.gpa.free(inproc_endpoint_name);

                    peer_push_zsocks[_idx].bind(inproc_endpoint_name) catch return TransportCreationError.BindFailure;
                }
                peer_push_zsocks[_idx].bind(replica_endpoint) catch return TransportCreationError.BindFailure;
            } else {
                // CONNECT PUSH socket
                peer_push_zsocks[_idx].connect(replica_endpoint) catch return TransportCreationError.BindFailure;
            }
        }

        // Prepare poller
        var pull_poller = zimq.Poller.init() catch return TransportCreationError.OutOfMemory;
        errdefer pull_poller.deinit();
        pull_poller.add(peer_push_zsocks[replica_no], null, .in) catch |e| return switch (e) {
            error.SocketInvalid => TransportCreationError.IllegalArgument,
            error.TooManyFilesPolled => TransportCreationError.LimitReached,
            error.NoMemory => TransportCreationError.OutOfMemory,
            error.SocketAdded, error.Unexpected => TransportCreationError.OtherCreationFailure,
        };

        defer logger.debug()
            .msg("Replica transport started")
            .int("replica_no", replica_no)
            .log();

        return .{
            .allocs = allocs,
            .logger = logger,
            .replica_no = replica_no,
            .replica_pull_zsock = peer_push_zsocks[replica_no],
            .peer_push_zsocks = peer_push_zsocks,
            .client_push_zsock = client_push_zsock,
            .replica_poller = pull_poller,
            .wire_buffer = wire_buffer,
        };
    }

    /// De-initializes the replica transport, releasing resources.
    pub fn deinit(self: *Self) void {
        self.logger.fine()
            .msg("Stopping replica transport")
            .int("replica_no", self.replica_no)
            .log();
        defer self.allocs.gpa.free(self.wire_buffer);
        defer self.client_push_zsock.deinit();
        // Note that our `replica_pull_zsock` is released as part of peer_push_zsocks cleanup
        defer self.allocs.gpa.free(self.peer_push_zsocks);
        defer for (self.peer_push_zsocks) |_zsock| {
            _zsock.deinit();
        };
        defer self.replica_poller.deinit();
        defer self.logger.debug()
            .msg("Replica transport stopped")
            .int("replica_no", self.replica_no)
            .log();
    }

    /// Send VSR message to client.
    pub fn sendToClient(self: *Self, message: *const vsr_msg.VsrMessage) TransportSendError!void {
        sendToNodeHelper(self.logger, self.client_push_zsock, self.wire_buffer, message) catch |_e| {
            self.logger.warn()
                .msg("Failed to send VSR message to client")
                .err(_e)
                .int("replica_no", self.replica_no)
                .log();
            return _e;
        };
    }

    /// Send VSR message to primary node.
    ///
    /// Since the primary may change over the lifetime of the replica transport, the _current_ primary node number
    /// is taken as an argument.
    pub fn sendToPrimary(self: *Self, primary_replica_no: u8, message: *const vsr_msg.VsrMessage) TransportSendError!void {
        assert(primary_replica_no < self.peer_push_zsocks.len);

        sendToNodeHelper(self.logger, self.peer_push_zsocks[primary_replica_no], self.wire_buffer, message) catch |_e| {
            self.logger.warn()
                .msg("Failed to send VSR message to primary")
                .err(_e)
                .int("replica_no", self.replica_no)
                .int("primary_replica_no", primary_replica_no)
                .log();
            return _e;
        };

        // self.logger.fine()
        //     .msg("Sent VSR message to primary")
        //     .int("replica_no", self.replica_no)
        //     .int("primary_replica_no", primary_replica_no)
        //     .log();
    }

    /// Sends the given VSR message to _all *other*_ replicas.
    ///
    /// This is expected to be called by a master node to message other replicas.
    pub fn sendToReplicas(self: *Self, message: *const vsr_msg.VsrMessage) TransportSendError!void {
        if (message.calcSize() > wire_buffer_size) {
            self.logger.warn()
                .msg("Unable to send VSR message to replicas")
                .int("replica_no", self.replica_no)
                .int("msg_len", message.calcSize())
                .int("wire_buffer_size", wire_buffer_size)
                .log();
        }
        var wire_writer = std.Io.Writer.fixed(self.wire_buffer);
        const msg_len = try message.marshal(&wire_writer);

        for (self.peer_push_zsocks, 0..) |_peer_zsock, _replica_no| {
            if (_replica_no == self.replica_no) {
                continue;
            }
            _peer_zsock.sendSlice(self.wire_buffer[0..msg_len], .{}) catch |e| {
                self.logger.warn()
                    .msg("Failure while sending VSR message to replica")
                    .err(e)
                    .int("replica_no", self.replica_no)
                    .int("target_replica_no", _replica_no)
                    .log();
                return switch (e) {
                    error.InappropriateStateActionFailed, error.ContextInvalid, error.SocketInvalid => TransportSendError.IllegalState,
                    error.MessageInvalid => TransportSendError.InconsistentState,
                    error.Interrupted => TransportSendError.Interrupted,
                    error.SendNotSupported, error.MultipartNotSupported => TransportSendError.InvalidRequest,
                    error.WouldBlock => TransportSendError.WouldBlock,
                    error.Unexpected, error.CannotRoute => TransportSendError.OtherUsageFailure,
                };
            };
        }

        // self.logger.fine()
        //     .msg("Sent VSR message to replicas")
        //     .int("replica_no", self.replica_no)
        //     .log();
    }

    /// Polls for VSR messages sent to this replica.
    pub fn poll(self: *Self, timeout_ms: usize) TransportReceiveError!?[]vsr_msg.VsrMessage {
        return pollHelper(self.allocs, self.logger, self.replica_poller, @intCast(timeout_ms)) catch |_e| {
            if (_e == error.NoEvent) {
                return null;
            } else if (_e == error.Interrupted) {
                self.logger.debug()
                    .msg("Interrupted while polling for VSR messages")
                    .err(_e)
                    .int("replica_no", self.replica_no)
                    .log();
                return TransportReceiveError.Interrupted;
            }
            self.logger.warn()
                .msg("Failure while polling for VSR messages")
                .err(_e)
                .int("replica_no", self.replica_no)
                .log();
            return _e;
        };
    }
};

/// Polls for VSR messages sent to a replica.
pub fn pollHelper(allocs: Allocators, logger: *Logger, poller: *zimq.Poller, timeout_ms: usize) TransportReceiveError!?[]vsr_msg.VsrMessage {
    var poll_events: [10]zimq.Poller.Event = undefined;

    const num_events = poller.waitAll(&poll_events, @intCast(timeout_ms)) catch |e| {
        if (e == error.NoEvent) {
            return null;
        } else if (e == error.Interrupted) {
            return TransportReceiveError.Interrupted;
        }
        return switch (e) {
            error.NoMemory => TransportReceiveError.OutOfMemory,
            error.SocketInvalid, error.SubscriptionInvalid => TransportReceiveError.IllegalArgument,
            error.Interrupted => TransportReceiveError.Interrupted,
            error.NoEvent, error.Unexpected => TransportReceiveError.OtherUsageFailure,
        };
    };
    assert(num_events > 0);
    var polled_messages = allocs.gpa.alloc(vsr_msg.VsrMessage, num_events) catch return TransportReceiveError.OutOfMemory;
    errdefer allocs.gpa.free(polled_messages);

    const polled_events = poll_events[0..num_events];
    for (polled_events, 0..) |_event, _msg_idx| {
        polled_messages[_msg_idx] = try fetchPolledHelper(allocs, logger, _event.socket.?);
    }

    return polled_messages;
}

/// Fetches a waiting message from a peer (already polled, no blocking required).
fn fetchPolledHelper(allocs: Allocators, logger: *Logger, peer_zsock: *zimq.Socket) TransportReceiveError!vsr_msg.VsrMessage {
    var fetched: zimq.Message = .empty();
    const msg_size = peer_zsock.recvMsg(&fetched, .{ .dont_wait = true }) catch |e| {
        return switch (e) {
            error.InappropriateStateActionFailed, error.ContextInvalid, error.SocketInvalid => TransportReceiveError.IllegalState,
            error.MessageInvalid => TransportReceiveError.InconsistentState,
            error.RecvNotSupported => TransportReceiveError.InvalidRequest,
            error.Interrupted => TransportReceiveError.Interrupted,
            error.WouldBlock => TransportReceiveError.WouldBlock,
            error.Unexpected => TransportReceiveError.OtherUsageFailure,
        };
    };
    defer fetched.deinit();

    if (msg_size < 1) {
        return TransportReceiveError.DataTruncated;
    }
    var wire_reader = std.Io.Reader.fixed(fetched.slice());
    return vsr_msg.VsrMessage.unmarshal(allocs, &wire_reader) catch |e| {
        logger.err()
            .msg("Failed to unmarshal VSR message")
            .err(e)
            .any("vsr_msg", fetched.slice())
            .log();
        return e;
    };
}

/// Creates a ZeroMQ TCP/IP endpoint name from hostname and port.
pub fn tcpEndpointName(gpa: std.mem.Allocator, hostname: [:0]const u8, port_no: u16) AllocationError![:0]u8 {
    const replica_endpoint: [:0]u8 = std.fmt.allocPrintSentinel(
        gpa,
        "tcp://{s}:{d}",
        .{
            hostname,
            port_no,
        },
        0,
    ) catch return TransportCreationError.OutOfMemory;
    return replica_endpoint;
}

/// Creates a ZeroMQ in-process endpoint name from hostname and port.
pub fn inprocEndpointName(gpa: std.mem.Allocator, replica_no: u8) AllocationError![:0]u8 {
    const replica_endpoint: [:0]u8 = std.fmt.allocPrintSentinel(
        gpa,
        local_replica_endpoint_fmt,
        .{
            replica_no,
        },
        0,
    ) catch return TransportCreationError.OutOfMemory;
    return replica_endpoint;
}

/// Send VSR message to a node.
pub fn sendToNodeHelper(logger: *Logger, target_zsock: *zimq.Socket, wire_buffer: []u8, message: *const vsr_msg.VsrMessage) TransportSendError!void {
    if (message.calcSize() > wire_buffer.len) {
        return TransportSendError.LimitReached;
    }
    var wire_writer = std.Io.Writer.fixed(wire_buffer);
    const msg_len = try message.marshal(&wire_writer);

    target_zsock.sendSlice(wire_buffer[0..msg_len], .{}) catch |e| {
        logger.warn()
            .msg("Failure while sending VSR message")
            .err(e)
            .log();
        return switch (e) {
            error.InappropriateStateActionFailed, error.ContextInvalid, error.SocketInvalid => TransportSendError.IllegalState,
            error.MessageInvalid => TransportSendError.InconsistentState,
            error.Interrupted => TransportSendError.Interrupted,
            error.SendNotSupported, error.MultipartNotSupported => TransportSendError.InvalidRequest,
            error.WouldBlock => TransportSendError.WouldBlock,
            error.Unexpected, error.CannotRoute => TransportSendError.OtherUsageFailure,
        };
    };
}

// Replica transport smoke test
test "ReplicaTransport.smoke" {
    std.debug.print("test.ReplicaTransport.smoke\n", .{});
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

    var logger = logging.logger("ReplicaTransport.smoke");

    logger.debug()
        .msg("Preparing replica transport")
        .log();

    var ulid_generator = dymes_common.ulid.generator();

    const context: *zimq.Context = try .init();
    defer context.deinit();

    var vsr_cfg_builder = state.VsrConfiguration.Builder.init();
    defer vsr_cfg_builder.deinit(allocs.gpa);

    try vsr_cfg_builder.withReplica(allocs.gpa, "localhost", 51541);
    try vsr_cfg_builder.withReplica(allocs.gpa, "localhost", 51542);
    try vsr_cfg_builder.withReplica(allocs.gpa, "localhost", 51543);

    const vsr_config = try vsr_cfg_builder.build(allocs.gpa);
    defer vsr_config.deinit(allocs.gpa);

    var replica_0_transport: ReplicaTransport = try .init(allocs, 0, vsr_config, context);
    defer replica_0_transport.deinit();
    var replica_1_transport: ReplicaTransport = try .init(allocs, 1, vsr_config, context);
    defer replica_1_transport.deinit();
    var replica_2_transport: ReplicaTransport = try .init(allocs, 2, vsr_config, context);
    defer replica_2_transport.deinit();

    var test_msg_1 = try vsr_testing.createTestVsrMessage(allocs, &ulid_generator, 0, 1, 0);
    defer test_msg_1.deinit(allocs);

    var received_count: usize = 0;

    try replica_0_transport.sendToReplicas(&test_msg_1);
    {
        if (try replica_1_transport.poll(500)) |_polled| {
            defer allocs.gpa.free(_polled);
            for (_polled) |*_message| {
                defer _message.deinit(allocs);
                received_count += 1;
            }
        }

        if (try replica_2_transport.poll(500)) |_polled| {
            defer allocs.gpa.free(_polled);
            for (_polled) |*_message| {
                defer _message.deinit(allocs);
                received_count += 1;
            }
        }
    }
    try testing.expectEqual(2, received_count);

    var test_msg_2 = try vsr_testing.createTestVsrMessage(allocs, &ulid_generator, 1, 2, 0);
    defer test_msg_2.deinit(allocs);

    try replica_1_transport.sendToPrimary(0, &test_msg_2);
    {
        if (try replica_0_transport.poll(500)) |_polled| {
            defer allocs.gpa.free(_polled);
            for (_polled) |*_message| {
                defer _message.deinit(allocs);
                received_count += 1;
            }
        }
        if (try replica_1_transport.poll(500)) |_polled| {
            defer allocs.gpa.free(_polled);
            for (_polled) |*_message| {
                defer _message.deinit(allocs);
                received_count += 1;
            }
        }
        if (try replica_2_transport.poll(500)) |_polled| {
            defer allocs.gpa.free(_polled);
            for (_polled) |*_message| {
                defer _message.deinit(allocs);
                received_count += 1;
            }
        }
    }
    try testing.expectEqual(3, received_count);

    var test_msg_3 = try vsr_testing.createTestVsrMessage(allocs, &ulid_generator, 2, 3, 0);
    defer test_msg_3.deinit(allocs);

    try replica_0_transport.sendToReplicas(&test_msg_3);
    {
        if (try replica_1_transport.poll(500)) |_polled| {
            defer allocs.gpa.free(_polled);
            for (_polled) |*_message| {
                defer _message.deinit(allocs);
                received_count += 1;
            }
        }

        if (try replica_2_transport.poll(500)) |_polled| {
            defer allocs.gpa.free(_polled);
            for (_polled) |*_message| {
                defer _message.deinit(allocs);
                received_count += 1;
            }
        }
    }
    try testing.expectEqual(5, received_count);
}

/// Dymes VSR client proxy transport.
pub const ClientProxyTransport = struct {
    const bind_endpoint = local_client_proxy_endpoint;
    const component_name = "vsr.ClientProxyTransport";
    const Self = @This();

    allocs: Allocators,
    logger: *Logger,
    client_pull_zsock: *zimq.Socket,
    client_poller: *zimq.Poller,
    replica_push_zsock: *zimq.Socket,
    wire_buffer: []u8,

    /// Initializes the client proxy transport.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn init(allocs: Allocators, replica_no: u8, context: *zimq.Context) TransportCreationError!Self {
        var logger = logging.logger(component_name);
        logger.fine()
            .msg("Starting client proxy transport")
            .int("wire_buffer_size", wire_buffer_size)
            .log();

        // Allocate wire buffer
        const wire_buffer = allocs.gpa.alignedAlloc(u8, .@"8", wire_buffer_size) catch return TransportCreationError.OutOfMemory;
        errdefer allocs.gpa.free(wire_buffer);

        // Prepare the zsocks
        var replica_push_zsock = zimq.Socket.init(context, .push) catch |e| return switch (e) {
            zimq.Socket.InitError.TooManyOpenFiles => TransportCreationError.LimitReached,
            zimq.Socket.InitError.InvalidContext => TransportCreationError.IllegalArgument,
            zimq.Socket.InitError.Unexpected => TransportCreationError.OtherCreationFailure,
        };
        errdefer replica_push_zsock.deinit();
        replica_push_zsock.set(.sndhwm, push_high_water_mark) catch return TransportCreationError.OtherUsageFailure;
        {
            const connect_endpoint = inprocEndpointName(allocs.gpa, replica_no) catch return TransportCreationError.OutOfMemory;
            defer allocs.gpa.free(connect_endpoint);

            logger.fine()
                .msg("Connecting to local replica")
                .str("endpoint", connect_endpoint)
                .log();
            replica_push_zsock.connect(connect_endpoint) catch return TransportCreationError.ConnectFailure;
        }

        var client_pull_zsock = zimq.Socket.init(context, .pull) catch |e| return switch (e) {
            zimq.Socket.InitError.TooManyOpenFiles => TransportCreationError.LimitReached,
            zimq.Socket.InitError.InvalidContext => TransportCreationError.IllegalArgument,
            zimq.Socket.InitError.Unexpected => TransportCreationError.OtherCreationFailure,
        };
        errdefer client_pull_zsock.deinit();
        client_pull_zsock.set(.rcvhwm, pull_high_water_mark) catch return TransportCreationError.OtherUsageFailure;
        logger.fine()
            .msg("Binding to client proxy endpoint")
            .str("endpoint", bind_endpoint)
            .log();
        client_pull_zsock.bind(bind_endpoint) catch |_e| {
            logger.err()
                .msg("Failed to bind proxy client pull socket")
                .err(_e)
                .log();
            return TransportCreationError.BindFailure;
        };

        // Prepare poller
        var pull_poller = zimq.Poller.init() catch return TransportCreationError.OutOfMemory;
        errdefer pull_poller.deinit();
        pull_poller.add(client_pull_zsock, null, .in) catch |e| return switch (e) {
            error.SocketInvalid => TransportCreationError.IllegalArgument,
            error.TooManyFilesPolled => TransportCreationError.LimitReached,
            error.NoMemory => TransportCreationError.OutOfMemory,
            error.SocketAdded, error.Unexpected => TransportCreationError.OtherCreationFailure,
        };

        defer logger.debug()
            .msg("Client proxy transport started")
            .log();

        return .{
            .allocs = allocs,
            .logger = logger,
            .replica_push_zsock = replica_push_zsock,
            .client_pull_zsock = client_pull_zsock,
            .client_poller = pull_poller,
            .wire_buffer = wire_buffer,
        };
    }

    /// De-initializes the client proxy transport, releasing resources.
    pub fn deinit(self: *Self) void {
        self.logger.fine()
            .msg("Stopping client proxy transport")
            .log();
        defer self.allocs.gpa.free(self.wire_buffer);
        defer self.client_pull_zsock.deinit();
        defer self.replica_push_zsock.deinit();
        defer self.client_poller.deinit();

        defer self.logger.debug()
            .msg("Client proxy transport stopped")
            .log();
    }

    /// Send VSR message to local replica.
    ///
    /// Since the primary may change over the lifetime of the replica transport, the _current_ primary node number
    /// is taken as an argument.
    pub fn sendToReplica(self: *Self, message: *const vsr_msg.VsrMessage) TransportSendError!void {
        sendToNodeHelper(self.logger, self.replica_push_zsock, self.wire_buffer, message) catch |_e| {
            self.logger.warn()
                .msg("Failed to send VSR message to local replica")
                .err(_e)
                .log();
            return _e;
        };
    }

    /// Polls for VSR messages sent to this client proxy.
    pub fn poll(self: *Self, timeout_ms: usize) TransportReceiveError!?[]vsr_msg.VsrMessage {
        return pollHelper(self.allocs, self.logger, self.client_poller, @intCast(timeout_ms)) catch |_e| {
            if (_e == error.NoEvent) {
                return null;
            } else if (_e == error.Interrupted) {
                self.logger.debug()
                    .msg("Interrupted while polling for VSR messages")
                    .err(_e)
                    .log();
                return TransportReceiveError.Interrupted;
            }
            self.logger.warn()
                .msg("Failure while polling for VSR messages")
                .err(_e)
                .log();
            return _e;
        };
    }
};

// Client proxy transport smoke test
test "ClientProxyTransport.smoke" {
    std.debug.print("test.ClientProxyTransport.smoke\n", .{});
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

    var logger = logging.logger("ClientProxyTransport.smoke");

    logger.debug()
        .msg("Preparing client proxy transport")
        .log();

    var ulid_generator = dymes_common.ulid.generator();

    const context: *zimq.Context = try .init();
    defer context.deinit();

    logger.debug()
        .msg("Preparing replica push socket")
        .log();

    var replica_push_zsock = zimq.Socket.init(context, .push) catch |e| return switch (e) {
        zimq.Socket.InitError.TooManyOpenFiles => TransportCreationError.LimitReached,
        zimq.Socket.InitError.InvalidContext => TransportCreationError.IllegalArgument,
        zimq.Socket.InitError.Unexpected => TransportCreationError.OtherCreationFailure,
    };
    defer replica_push_zsock.deinit();
    replica_push_zsock.set(.sndhwm, push_high_water_mark) catch return TransportCreationError.OtherUsageFailure;
    replica_push_zsock.connect(local_client_proxy_endpoint) catch return TransportCreationError.ConnectFailure;

    logger.debug()
        .msg("Preparing replica pull socket")
        .log();

    var replica_pull_zsock = zimq.Socket.init(context, .pull) catch |e| return switch (e) {
        zimq.Socket.InitError.TooManyOpenFiles => TransportCreationError.LimitReached,
        zimq.Socket.InitError.InvalidContext => TransportCreationError.IllegalArgument,
        zimq.Socket.InitError.Unexpected => TransportCreationError.OtherCreationFailure,
    };
    defer replica_pull_zsock.deinit();
    replica_pull_zsock.set(.rcvhwm, pull_high_water_mark) catch return TransportCreationError.OtherUsageFailure;
    {
        const inproc_replica_endpoint_name = try inprocEndpointName(allocs.gpa, 0);
        defer allocs.gpa.free(inproc_replica_endpoint_name);
        replica_pull_zsock.bind(inproc_replica_endpoint_name) catch return TransportCreationError.BindFailure;
    }

    logger.debug()
        .msg("Preparing replica poller")
        .log();

    var replica_poller = zimq.Poller.init() catch return TransportCreationError.OutOfMemory;
    defer replica_poller.deinit();
    replica_poller.add(replica_pull_zsock, null, .in) catch |e| return switch (e) {
        error.SocketInvalid => TransportCreationError.IllegalArgument,
        error.TooManyFilesPolled => TransportCreationError.LimitReached,
        error.NoMemory => TransportCreationError.OutOfMemory,
        error.SocketAdded, error.Unexpected => TransportCreationError.OtherCreationFailure,
    };

    logger.debug()
        .msg("Preparing client proxy transport")
        .log();

    var proxy_transport: ClientProxyTransport = try .init(allocs, 0, context);
    defer proxy_transport.deinit();

    var test_msg_1 = try vsr_testing.createTestRequest(allocs, &ulid_generator, 0, 1234, 1);
    defer test_msg_1.deinit(allocs);

    var received_count: usize = 0;

    logger.debug()
        .msg("Sending message from client proxy transport")
        .log();

    try proxy_transport.sendToReplica(&test_msg_1);

    logger.debug()
        .msg("Polling replica")
        .log();

    {
        if (try pollHelper(allocs, logger, replica_poller, 500)) |_polled| {
            received_count += 1;
            defer allocs.gpa.free(_polled);
            for (_polled) |*_message| {
                defer _message.deinit(allocs);
            }
        }
    }
    try testing.expectEqual(1, received_count);

    logger.debug()
        .msg("Sending response to client proxy transport")
        .log();

    {
        const vsr_op_response: ops.VsrOpResponse = .{ .append = .{ .msg_ulid = ulid_generator.last } };
        const message: vsr_msg.VsrMessage = .{ .vsr_reply = .{
            .client_id = 1234,
            .request_no = test_msg_1.vsr_request.request_no,
            .view_no = 0,
            .response = vsr_op_response,
        } };
        var wire_buffer: [1024]u8 = undefined;
        try sendToNodeHelper(logger, replica_push_zsock, &wire_buffer, &message);
    }

    logger.debug()
        .msg("Polling client proxy transport")
        .log();

    {
        if (try proxy_transport.poll(500)) |_polled| {
            defer allocs.gpa.free(_polled);
            for (_polled) |*_message| {
                defer _message.deinit(allocs);
                received_count += 1;
            }
        }
    }
    try testing.expectEqual(2, received_count);
}
