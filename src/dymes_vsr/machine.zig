//! Dymes VSR state machine.
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
const Allocators = wire.Allocators;

const zimq = @import("zimq");

const vsr_ops = @import("ops.zig");
const vsr_msg = @import("vsr_msg.zig");
const vsr_wire = @import("wire.zig");

const VsrMessage = vsr_msg.VsrMessage;
const VsrRequest = vsr_msg.VsrRequest;
const VsrPrepare = vsr_msg.VsrPrepare;
const VsrPrepareOK = vsr_msg.VsrPrepareOK;
const VsrReply = vsr_msg.VsrReply;
const VsrCommit = vsr_msg.VsrCommit;
const VsrStartViewChange = vsr_msg.VsrStartViewChange;
const VsrDoViewChange = vsr_msg.VsrDoViewChange;
const VsrStartView = vsr_msg.VsrStartView;
const VsrGetState = vsr_msg.VsrGetState;
const VsrNewState = vsr_msg.VsrNewState;
const VsrOpResponse = vsr_ops.VsrOpResponse;
const VsrOpType = vsr_ops.VsrOpType;
const VsrAppendOp = vsr_ops.VsrAppendOp;
const VsrAppendResponse = vsr_ops.VsrAppendResponse;

const VsrOpLog = vsr_ops.VsrOpLog;

const ViewNumber = vsr_wire.ViewNumber;

const CommitNumber = vsr_wire.CommitNumber;

const vsr_limits = @import("limits.zig");
const vsr_state = @import("state.zig");
const VsrConfiguration = vsr_state.VsrConfiguration;
const VsrState = vsr_state.VsrState;

const vsr_transport = @import("transport.zig");
const ReplicaTransport = vsr_transport.ReplicaTransport;
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

pub const OpsHandlerError = StateError || AllocationError;

/// VFT of `op` handlers.
pub const OpsHandlerVFT = union(VsrOpType) {
    append: *const fn (handler_ctx: *anyopaque, op: VsrAppendOp) OpsHandlerError!VsrAppendResponse,
};

pub const OpsHandler = struct {
    ctx: *anyopaque,
    vft: OpsHandlerVFT,
};

/// Prepare OK tracking entry (per client id)
pub const PrepareOkTrackingEntry = struct {
    request_no: wire.RequestNumber,
    num_prepare_oks: usize = 0,
    committed: bool = false,
};

/// Prepare-OK tracking map.
///
/// Entries are added when raising PREPAREs, and updated during PREPARE-OKs.
/// Entries are removed either by receiving a quorum or Prepare-Oks, or during
/// timeout recovery.
pub const PrepareOkTrackingMap = std.AutoHashMap(wire.ClientId, PrepareOkTrackingEntry);

/// Dymes VSR state machine.
pub const VsrStateMachine = struct {
    const component_name = "vsr.VsrStateMachine";

    pub const InitError = UsageError || AllocationError || SocketError;
    pub const MachineError = AllocationError || TransportSendError;

    logger: *Logger,
    allocs: Allocators,
    config: VsrConfiguration,
    state: vsr_state.VsrState,
    ops_handler: OpsHandler,
    transport: ReplicaTransport,
    tick_duration: u32,

    last_msg_ms: i64,
    prepare_tracking_map: PrepareOkTrackingMap,
    view_change_agreements: u8,
    view_change_my_old_view_no: ViewNumber,
    dvc_latest_old_view_no: ViewNumber,
    dvc_ref_msg: ?VsrDoViewChange = null,
    dvc_count: u8,

    /// Initializes the state machine using Dymes configuration.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn init(
        allocs: Allocators,
        ops_handler: OpsHandler,
        zimq_ctx: *zimq.Context,
        tick_duration: u32,
        dymes_vsr_config: dymes_common.config.Config,
    ) InitError!VsrStateMachine {
        var logger = logging.logger(component_name);
        if (tick_duration < constants.min_machine_tick_duration) {
            logger.warn()
                .msg("State machine tick duration too short")
                .int("tick_duration", tick_duration)
                .int("min_tick_duration", constants.min_machine_tick_duration)
                .log();
            return InitError.IllegalArgument;
        }
        logger.fine()
            .msg("Preparing VSR configuration")
            .log();
        const vsr_config: VsrConfiguration, const replica_no = try buildVsrConfiguration(logger, allocs, dymes_vsr_config);
        errdefer vsr_config.deinit(allocs.gpa);

        const bind_host: []const u8 = try dymes_vsr_config.asString("host") orelse return InitError.MissingArgument;
        logger.fine()
            .msg("VSR configuration prepared")
            .log();

        return try initUsingVsrConfig(allocs, ops_handler, zimq_ctx, tick_duration, replica_no, bind_host, vsr_config);
    }

    /// Initializes the state machine.
    ///
    /// Ownership of the VSR configuration (`vsr_config`) is passed to the state machine.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn initUsingVsrConfig(
        allocs: Allocators,
        ops_handler: OpsHandler,
        zimq_ctx: *zimq.Context,
        tick_duration: u32,
        replica_no: u8,
        bind_host: []const u8,
        vsr_config: VsrConfiguration,
    ) InitError!VsrStateMachine {
        assert(std.mem.eql(u8, bind_host, vsr_config.replicaHostname(replica_no)));
        assert(tick_duration >= constants.min_machine_tick_duration);
        var logger = logging.logger(component_name);

        logger.fine()
            .msg("Starting state machine")
            .int("replica_no", replica_no)
            .log();

        var prepare_tracking_map = PrepareOkTrackingMap.init(allocs.gpa);
        errdefer prepare_tracking_map.deinit();

        var new_state = try VsrState.init(allocs, replica_no, vsr_config);
        errdefer new_state.deinit();

        // TODO - Populate initial op-log from engine data store

        logger.fine()
            .msg("VSR state initialized")
            .int("replica_no", replica_no)
            .boolean("primary", new_state.isPrimary())
            .log();

        // Bring up transport

        const replica_transport = ReplicaTransport.init(allocs, replica_no, vsr_config, zimq_ctx) catch |_e| {
            logger.warn()
                .msg("Failed to initialize replica transport")
                .err(_e)
                .int("replica_no", replica_no)
                .str("bind_host", bind_host)
                .any("vsr_config", vsr_config)
                .log();
            return InitError.SocketFailure;
        };
        errdefer replica_transport.deinit();

        defer logger.debug()
            .msg("State machine started")
            .int("replica_no", replica_no)
            .boolean("primary", new_state.isPrimary())
            .log();

        return .{
            .logger = logger,
            .allocs = allocs,
            .config = vsr_config,
            .state = new_state,
            .ops_handler = ops_handler,
            .transport = replica_transport,
            .tick_duration = tick_duration,
            .prepare_tracking_map = prepare_tracking_map,
            .last_msg_ms = std.time.milliTimestamp(),
            .view_change_my_old_view_no = 0,
            .view_change_agreements = 0,
            .dvc_latest_old_view_no = 0,
            .dvc_count = 0,
        };
    }

    /// De-initializes the state machine, releasing resources.
    pub fn deinit(self: *VsrStateMachine) void {
        const primary = self.state.isPrimary();
        self.logger.fine()
            .msg("Shutting down state machine")
            .int("replica_no", self.state.replica_no)
            .boolean("primary", primary)
            .log();
        defer self.logger.debug()
            .msg("State machine shut down")
            .int("replica_no", self.state.replica_no)
            .boolean("primary", primary)
            .log();

        defer self.state.deinit();
        defer self.config.deinit(self.allocs.gpa);
        defer self.transport.deinit();
        defer self.prepare_tracking_map.deinit();
        if (self.dvc_ref_msg) |*_dvc_msg| {
            _dvc_msg.deinit(self.allocs);
        }
    }

    /// Ticks the state machine.
    ///
    /// Returns true if messages were processed, false otherwise.
    pub fn tick(self: *VsrStateMachine) !bool {
        // Poll for VSR messages
        const poll_ms: usize = self.tick_duration;
        const current_ms = std.time.milliTimestamp();
        const vsr_messages: []vsr_msg.VsrMessage = try self.transport.poll(poll_ms) orelse {
            // Take action if we've been left idle too long
            const idle_ms = current_ms - self.last_msg_ms;
            if (self.state.isPrimary() and idle_ms >= self.state.configuration.idle_commit_timeout_ms) {
                self.logger.debug()
                    .msg("Primary idle timeout reached")
                    .int("replica_no", self.state.replica_no)
                    .int("timeout", self.state.configuration.idle_commit_timeout_ms)
                    .int("idle", idle_ms)
                    .log();
                var commit_msg: VsrMessage = .{ .vsr_commit = VsrCommit.init(self.state.view_no, self.state.commit_no) };
                defer commit_msg.deinit(self.allocs);
                try self.transport.sendToReplicas(&commit_msg);
                self.last_msg_ms = current_ms;
                return true;
            } else if (idle_ms >= self.state.configuration.idle_scv_timeout_ms) {
                self.logger.debug()
                    .msg("Replica idle timeout reached")
                    .int("replica_no", self.state.replica_no)
                    .int("timeout", self.state.configuration.idle_scv_timeout_ms)
                    .int("idle", idle_ms)
                    .log();
                self.last_msg_ms = current_ms;
                try self.initiateViewChange(self.state.view_no + 1);
                return true;
            }
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
                .vsr_request => |_vsr_request| try self.request(_vsr_request),
                .vsr_prepare => |_vsr_prepare| try self.prepare(_vsr_prepare),
                .vsr_prepare_ok => |_vsr_prepare_ok| try self.prepareOk(_vsr_prepare_ok),
                .vsr_reply => |_vsr_reply| try self.reply(_vsr_reply),
                .vsr_commit => |_vsr_commit| try self.commit(_vsr_commit),
                .vsr_svc => |_vsr_svc| try self.startViewChange(_vsr_svc),
                .vsr_dvc => |_vsr_dvc| try self.doViewChange(_vsr_dvc),
                .vsr_sv => |_vsr_sv| try self.startView(_vsr_sv),
                .vsr_gs => |_vsr_gs| try self.getState(_vsr_gs),
                .vsr_ns => |_vsr_ns| try self.newState(_vsr_ns),
            }
        }
        return vsr_messages.len > 0;
    }

    /// Initiates a view change by replica.
    ///
    /// If no <PREPARE> or <COMMIT> are received from the primary, a replica timeout
    /// will expire. At this point the replica will change its status to view-change,
    /// increase its view number (view-num) and create a <START-VIEW-CHANGE> message
    /// with the new view-num and its identity. It will then send this message to all
    /// the replicas.
    ///
    /// When the other replicas receive a <START-VIEW-CHANGE> message with a view-num
    /// bigger than the one they have, they set their status to view-change and set the
    /// view-num to the view number in the message and reply with <START-VIEW-CHANGE>
    /// to all replicas.
    ///
    pub fn initiateViewChange(self: *VsrStateMachine, new_view_no: ViewNumber) MachineError!void {
        self.logger.debug()
            .msg("Initiating view change")
            .int("replica_no", self.state.replica_no)
            .int("view_no", self.state.view_no)
            .int("commit_no", self.state.commit_no)
            .int("new_view_no", new_view_no)
            .log();
        assert(new_view_no > self.state.view_no);

        self.view_change_agreements = 0;
        self.state.status = .view_change;
        self.state.view_no = new_view_no;

        const vsr_svc_msg: VsrMessage = .{ .vsr_svc = VsrStartViewChange.init(
            self.state.view_no,
            self.state.replica_no,
        ) };

        try self.transport.sendToReplicas(&vsr_svc_msg);
    }

    /// Handles VsrRequest <REQUEST> in primary.
    ///
    /// Clients have a unique identifier (client-id), and they communicate only with the
    /// primary. If a client contacts a replica which is not the primary, the replica
    /// drops the requests and returns an error message to the client advising it to
    /// connect to the primary. Each client can only send one request at the time, and
    /// each request has a request number (request#) which is a monotonically
    /// increasing number. The client prepares <REQUEST> message which contains the
    /// client-id, the request request# and the operation (op) to perform.
    ///
    /// The primary only processes the requests if its status is normal, otherwise, it
    /// drops the request and sends an error message to the client advising to try
    /// later. When the primary receives the request from the client, it checks whether
    /// the request is already present in the client table. If the request’s number is
    /// greater of the one in the client table then it is a new request, otherwise, it
    /// means that the client might not have received last response and it is
    /// re-sending the previous request. In this case, the request is dropped and the
    /// primary re-send last response present in the client table.
    ///
    /// If it is a new request, then the primary increases the operation number
    /// (op-num), it appends the requested operation to its operation log (op-log) and
    /// it updates the client-table with the new request.
    ///
    /// Then it needs to notify all the replicas about the new request, so it creates a
    /// <PREPARE> message which contains: the current view number (view-num), the
    /// operation’s op-num, the last commit number (commit-num), and the message which
    /// is the client request itself, and it sends the message to all the replicas.
    pub fn request(self: *VsrStateMachine, vsr_request: VsrRequest) MachineError!void {
        self.logger.fine()
            .ctx("REQUEST")
            .msg("Received VsrRequest")
            .int("client_id", vsr_request.client_id)
            .int("request_no", vsr_request.request_no)
            .log();
        // Check state preconditions
        if (!self.state.isPrimary()) {
            self.logger.warn()
                .ctx("REQUEST")
                .msg("Non-primary rejecting client request")
                .any("message", vsr_request)
                .log();
            return;
        } else if (self.state.status != .normal) {
            self.logger.warn()
                .ctx("REQUEST")
                .msg("Primary not ready to process client request")
                .any("status", self.state.status)
                .any("message", vsr_request)
                .log();
            // TODO - Send backoff to client
            return;
        }

        // Check client request status
        if (self.state.client_table.getEntry(vsr_request.client_id)) |_cte| {
            if (vsr_request.request_no <= _cte.request_no) {
                self.logger.warn()
                    .ctx("REQUEST")
                    .msg("Client has outstanding response")
                    .any("status", self.state.status)
                    .any("message", vsr_request)
                    .log();

                // Send PRIOR response to client (if any)
                if (_cte.response) |_op_rsp| {
                    var client_msg: VsrMessage = .{
                        .vsr_reply = .{
                            .client_id = _cte.client_id,
                            .request_no = _cte.request_no,
                            .view_no = self.state.view_no,
                            .response = _op_rsp,
                        },
                    };
                    defer client_msg.deinit(self.allocs);
                    self.transport.sendToClient(&client_msg) catch |_e| {
                        self.logger.warn()
                            .ctx("PREPARE-OK")
                            .msg("Failed to send response to client")
                            .err(_e)
                            .int("view_no", self.state.view_no)
                            .int("op_no", self.state.op_no)
                            .int("client_id", _cte.client_id)
                            .int("request_no", _cte.request_no)
                            .log();
                    };
                }

                return;
            }
        }

        // Update state
        self.state.op_no += 1;
        errdefer self.state.op_no -= 1;

        try self.state.op_log.addEntry(self.allocs, self.state.commit_no, vsr_request.op, .{ .append = .{} });
        errdefer _ = self.state.op_log.removeEntry(self.state.op_no);

        try self.state.client_table.addRequest(vsr_request.client_id, vsr_request.request_no, vsr_request.op);
        errdefer self.state.client_table.removeRequest(vsr_request.client_id);

        self.prepare_tracking_map.put(vsr_request.client_id, .{ .request_no = vsr_request.request_no }) catch return MachineError.OutOfMemory;
        errdefer _ = self.prepare_tracking_map.remove(vsr_request.client_id);

        // Create VsrPrepare message
        var vsr_prepare_msg: VsrMessage = .{ .vsr_prepare = try VsrPrepare.init(self.allocs, self.state.view_no, self.state.op_no, self.state.commit_no, vsr_request) };
        defer vsr_prepare_msg.deinit(self.allocs);

        try self.transport.sendToReplicas(&vsr_prepare_msg);

        self.logger.fine()
            .ctx("REQUEST")
            .msg("Processing client request")
            .int("op_no", self.state.op_no)
            .int("client_id", vsr_request.client_id)
            .int("request_no", vsr_request.request_no)
            .log();
    }

    /// Handles VsrPrepare <PREPARE> in replicas.
    ///
    /// When a replica receives a <PREPARE> request, it first checks whether the view
    /// number is the same view-num, if its view number is different than the message
    /// view-num it means that a new primary was nominated and, depending on who is
    /// behind, it needs to get up to date. If its view-number is smaller than the
    /// message view-num, then it means that this particular node is behind, so it
    /// needs to change its status to recovery and initiate a state transfer from the
    /// new primary (we will see the primary change process called view change later).
    ///
    /// If its view-number is greater than the message view-num, then it means that the
    /// other replica needs to get up-to-date, so it drops the message. Finally, if the
    /// view-num is the same, then it looks at the op-num in the message. The op-num
    /// needs to be strictly consecutive. If there are gaps it means that this replica
    /// missed one or more <PREPARE> messages, so it drops the message and it initiates
    /// a recovery with a state transfer. If the op-num is strictly consecutive then it
    /// increments its op-num, appends the operation to the op-log and updates the
    /// client table.
    ///
    /// The <PREPARE> message from the primary also contains the commit-num which
    /// shows that the primary has now executed the operation against its state and it
    /// is notifying the replicas that it is safe to do so as well. So the replicas
    /// will perform all requests in their op-log between the last commit-num and the
    /// commit-num in the <PREPARE> message strictly following the order of operations
    /// and advance its commit-num as well. At this point both, primary and replicas
    /// have performed the committed operations in their state.
    ///
    /// Now the replica sends an acknowledgment to the primary that the operation, and
    /// all previous operations, were successfully prepared. It creates a <PREPARE-OK>
    /// message with its view-num, the op-num and its identity. Sending a <PREPARE-OK>
    /// for a given op-num means that all the previous operations in the log have been
    /// prepared as well (no gaps).
    ///
    pub fn prepare(self: *VsrStateMachine, vsr_prepare: VsrPrepare) MachineError!void {
        self.logger.fine()
            .ctx("PREPARE")
            .msg("Received VsrPrepare")
            .int("our_replica_no", self.state.replica_no)
            .int("our_view_no", self.state.view_no)
            .int("our_commit_no", self.state.commit_no)
            .int("client_id", vsr_prepare.client_request.client_id)
            .int("request_no", vsr_prepare.client_request.request_no)
            .int("op_no", vsr_prepare.op_no)
            .int("view_no", vsr_prepare.view_no)
            .int("commit_no", vsr_prepare.commit_no)
            .log();

        if (self.state.status != .normal) {
            self.logger.warn()
                .ctx("PREPARE")
                .msg("Replica not ready to process prepare request")
                .any("status", self.state.status)
                .log();
            return;
        }

        // Check state preconditions
        if (self.state.view_no > vsr_prepare.view_no) {
            // Other replica is out-of-date
            self.logger.warn()
                .ctx("PREPARE")
                .msg("Ignoring prepare from out-of-date replica")
                .int("our_replica_no", self.state.replica_no)
                .int("our_view_no", self.state.view_no)
                .int("our_commit_no", self.state.commit_no)
                .int("their_view_no", vsr_prepare.view_no)
                .int("their_commit_no", vsr_prepare.commit_no)
                .log();
            return;
        } else if (self.state.view_no < vsr_prepare.view_no or (self.state.op_no + 1) != vsr_prepare.op_no) {
            // We are out-of-date
            self.logger.warn()
                .ctx("PREPARE")
                .msg("This replica is out-of-date")
                .int("our_replica_no", self.state.replica_no)
                .int("our_view_no", self.state.view_no)
                .int("our_commit_no", self.state.commit_no)
                .int("their_view_no", vsr_prepare.view_no)
                .int("their_commit_no", vsr_prepare.commit_no)
                .log();

            self.state.status = .recovery;

            // TODO: Initiate state transfer from new primary
            return;
        }

        // Update our op-log
        try self.state.op_log.addEntry(
            self.allocs,
            self.state.commit_no,
            vsr_prepare.client_request.op,
            try vsr_prepare.client_request.op.nilResponse(self.allocs),
        );

        // Update our client table
        try self.state.client_table.addRequest(
            vsr_prepare.client_request.client_id,
            vsr_prepare.client_request.request_no,
            vsr_prepare.client_request.op,
        );

        self.state.op_no += 1;
        assert(self.state.op_no == vsr_prepare.op_no);

        // Perform pending commits

        if (self.state.commit_no < vsr_prepare.commit_no) {
            self.state.commit_no = try self.applyPendingCommits("PREPARE", self.state.commit_no, vsr_prepare.commit_no);
        }

        // assert(self.state.commit_no == vsr_prepare.commit_no);

        // Verify state
        if (self.state.commit_no != vsr_prepare.commit_no) {
            self.logger.warn()
                .ctx("PREPARE")
                .msg("Confusion reigns in PREPARE")
                .int("replica_no", self.state.replica_no)
                .int("view_no", self.state.view_no)
                .int("op_no", self.state.op_no)
                .int("commit_no", self.state.commit_no)
                .int("expected_commit_no", vsr_prepare.commit_no)
                .log();
        }

        // Acknowledge the <PREPARE> with a <PREPARE-OK>
        const prepareOK: VsrMessage = .{
            .vsr_prepare_ok = VsrPrepareOK.init(
                self.state.view_no,
                self.state.op_no,
                self.state.replica_no,
                vsr_prepare.client_request.client_id,
                vsr_prepare.client_request.request_no,
            ),
        };
        try self.transport.sendToPrimary(self.state.primary_no, &prepareOK);
    }

    /// Handles VsrPrepareOK <PREPARE-OK> in primary.
    ///
    /// The primary waits for 𝑓 + 1 (including itself) <PREPARE-OK> messages, at which
    /// point it knows that a quorum of nodes knows about the operation to perform
    /// therefore it is considered safe to proceed as it is guaranteed that the
    /// operation will survive the loss of 𝑓 nodes. When it receives enough
    /// <PREPARE-OK> then it performs the operation, possibly with side effect, it
    /// increases its commit number commit-num, and update the client table with the
    /// operation result. Again, advancing the commit-num must be done in strict order,
    /// and it also means that all previous operations have been committed as well.
    ///
    /// Finally, it prepares a <REPLY> message with the current view-num the client
    /// request number request# and the operation result (response) and it sends it to
    /// the client.
    ///
    /// At this point, the primary is the only node that has performed the operation
    /// (and advanced its commit-num) while the replicas have only prepared the
    /// operation but not applied. Before they can safely apply they need confirmation
    /// from the primary that it is safe to do so.
    pub fn prepareOk(self: *VsrStateMachine, vsr_prepare_ok: VsrPrepareOK) MachineError!void {
        self.logger.fine()
            .ctx("PREPARE-OK")
            .msg("Received VsrPrepareOk")
            .int("our_replica_no", self.state.replica_no)
            .int("our_view_no", self.state.view_no)
            .int("our_commit_no", self.state.commit_no)
            .int("replica_no", vsr_prepare_ok.replica_no)
            .int("client_id", vsr_prepare_ok.client_id)
            .int("request_no", vsr_prepare_ok.request_no)
            .int("op_no", vsr_prepare_ok.op_no)
            .int("view_no", vsr_prepare_ok.view_no)
            .log();

        if (self.state.status != .normal) {
            self.logger.warn()
                .ctx("PREPARE_OK")
                .msg("Primary not ready to process prepare OK")
                .any("status", self.state.status)
                .log();
            return;
        }

        var pte = self.prepare_tracking_map.getPtr(vsr_prepare_ok.client_id) orelse {
            self.logger.fine()
                .ctx("PREPARE-OK")
                .msg("Ignoring superflous PREPARE-OK")
                .int("our_replica_no", self.state.replica_no)
                .int("our_view_no", self.state.view_no)
                .int("our_commit_no", self.state.commit_no)
                .int("replica_no", vsr_prepare_ok.replica_no)
                .int("client_id", vsr_prepare_ok.client_id)
                .int("request_no", vsr_prepare_ok.request_no)
                .int("op_no", vsr_prepare_ok.op_no)
                .int("view_no", vsr_prepare_ok.view_no)
                .log();
            return;
        };

        if (pte.request_no != vsr_prepare_ok.request_no) {
            self.logger.warn()
                .ctx("PREPARE-OK")
                .msg("Ignoring mismatching PREPARE-OK")
                .int("client_id", vsr_prepare_ok.client_id)
                .int("our_request_no", pte.request_no)
                .int("their_request_no", vsr_prepare_ok.request_no)
                .log();
            return;
        }

        // Check if consensus has been reached
        // Note that the we count *ourselves as well*, hence the subtraction from 'real' quorum
        pte.num_prepare_oks += 1;
        if (pte.num_prepare_oks >= (self.state.configuration.quorum - 1)) {
            if (pte.committed) {
                // Cleanup prepare-ok tracking slot
                _ = self.prepare_tracking_map.remove(vsr_prepare_ok.client_id);
                return;
            }

            const op_to_commit = try self.state.op_log.fetchOp(vsr_prepare_ok.op_no);

            self.logger.fine()
                .ctx("PREPARE-OK")
                .msg("Applying pending commit")
                .int("replica_no", self.state.replica_no)
                .int("view_no", self.state.view_no)
                .int("op_no_to_commit", vsr_prepare_ok.op_no)
                .log();

            const commit_rsp: VsrOpResponse = op_rsp_val: {
                switch (op_to_commit) {
                    .append => |_append_op| {
                        const append_rsp = self.ops_handler.vft.append(self.ops_handler.ctx, _append_op) catch |_e| {
                            self.logger.err()
                                .msg("Failed to import message")
                                .err(_e)
                                .int("replica_no", self.state.replica_no)
                                .int("view_no", self.state.view_no)
                                .int("op_no", vsr_prepare_ok.op_no)
                                .log();
                            return MachineError.InconsistentState;
                        };

                        const vsr_op_rsp: VsrOpResponse = .{ .append = append_rsp };
                        try self.state.op_log.setResponse(vsr_prepare_ok.op_no, vsr_op_rsp);

                        self.logger.debug()
                            .ctx("PREPARE-OK")
                            .msg("Pending commit applied")
                            .int("replica_no", self.state.replica_no)
                            .int("view_no", self.state.view_no)
                            .int("op_no", self.state.op_no)
                            .int("commit_no", vsr_prepare_ok.request_no)
                            .log();

                        break :op_rsp_val vsr_op_rsp;
                    },
                }
            };

            // Update state
            try self.state.client_table.setResponse(vsr_prepare_ok.client_id, vsr_prepare_ok.request_no, commit_rsp);
            self.state.commit_no += 1;
            pte.committed = true;

            self.logger.fine()
                .ctx("PREPARE-OK")
                .msg("Client request processed")
                .int("our_view_no", self.state.view_no)
                .int("their_view_no", vsr_prepare_ok.view_no)
                .int("our_op_no", self.state.op_no)
                .int("their_op_no", vsr_prepare_ok.op_no)
                .int("client_id", vsr_prepare_ok.client_id)
                .int("request_no", vsr_prepare_ok.request_no)
                .log();

            // Send response to client (sort of okay if this fails, they can resubmit the request again)
            var client_msg: VsrMessage = .{
                .vsr_reply = .{
                    .client_id = vsr_prepare_ok.client_id,
                    .request_no = vsr_prepare_ok.request_no,
                    .view_no = vsr_prepare_ok.view_no,
                    .response = commit_rsp,
                },
            };
            defer client_msg.deinit(self.allocs);
            self.transport.sendToClient(&client_msg) catch |_e| {
                self.logger.warn()
                    .ctx("PREPARE-OK")
                    .msg("Failed to send response to client")
                    .err(_e)
                    .int("our_view_no", self.state.view_no)
                    .int("their_view_no", vsr_prepare_ok.view_no)
                    .int("our_op_no", self.state.op_no)
                    .int("their_op_no", vsr_prepare_ok.op_no)
                    .int("client_id", vsr_prepare_ok.client_id)
                    .int("request_no", vsr_prepare_ok.request_no)
                    .log();
            };
        }
    }

    /// Stub, this is required in client-proxy.
    pub fn reply(self: *VsrStateMachine, vsr_reply: VsrReply) MachineError!void {
        self.logger.fine()
            .ctx("REPLY")
            .msg("Received VsrReply")
            .int("view_no", vsr_reply.view_no)
            .int("request_no", vsr_reply.request_no)
            .log();
    }

    /// Handles VsrCommit <COMMIT> in replicas.
    ///
    /// If the primary doesn’t receive a new client request within a certain time then
    /// it will create a <COMMIT> message to inform replicas about which operations can
    /// now be performed.
    ///
    /// The <COMMIT> message will only contain the current view number (view-num) and
    /// the last committed operation (commit-num). When a replica receives a <COMMIT>
    /// it will execute all operation in their op-log between the last commit-num and
    /// the commit-num in the <COMMIT> message strictly following the order of
    /// operations and advance its commit-num as well.
    ///
    /// The <PREPARE> message together with the <COMMIT> message act as a sort of
    /// heartbeat for the primary. Replicas use this to identify when a primary might
    /// be dead and elect a new primary. This process is called view change.
    pub fn commit(self: *VsrStateMachine, vsr_commit: VsrCommit) MachineError!void {
        self.logger.fine()
            .ctx("COMMIT")
            .msg("Received VsrCommit")
            .int("commit_no", vsr_commit.commit_no)
            .int("view_no", vsr_commit.view_no)
            .log();

        // Check state preconditions
        if (self.state.status != .normal) {
            self.logger.warn()
                .ctx("COMMIT")
                .msg("Repplica not ready to process commit")
                .any("status", self.state.status)
                .log();
            return;
        }

        if (self.state.view_no > vsr_commit.view_no) {
            // Other replica is out-of-date
            self.logger.warn()
                .ctx("COMMIT")
                .msg("Ignoring commit from out-of-date replica")
                .int("our_replica_no", self.state.replica_no)
                .int("our_view_no", self.state.view_no)
                .int("our_commit_no", self.state.commit_no)
                .int("their_view_no", vsr_commit.view_no)
                .int("their_commit_no", vsr_commit.commit_no)
                .log();
            return;
        } else if (self.state.view_no < vsr_commit.view_no) {
            // We are out-of-date
            self.logger.warn()
                .ctx("COMMIT")
                .msg("This replica is out-of-date")
                .int("our_replica_no", self.state.replica_no)
                .int("our_view_no", self.state.view_no)
                .int("our_commit_no", self.state.commit_no)
                .int("their_view_no", vsr_commit.view_no)
                .int("their_commit_no", vsr_commit.commit_no)
                .log();
            self.state.status = .recovery;
            // TODO: Initiate state transfer from new primary
            return;
        } else if (self.state.commit_no >= vsr_commit.commit_no) {
            // We're already up to date
            self.logger.fine()
                .ctx("COMMIT")
                .msg("Replica already up-to-date")
                .int("replica_no", self.state.replica_no)
                .int("view_no", self.state.view_no)
                .int("op_no", self.state.op_no)
                .int("our_commit_no", self.state.commit_no)
                .int("primary_commit_no", vsr_commit.commit_no)
                .log();
            return;
        }

        // Apply pending commits
        self.state.commit_no = try self.applyPendingCommits("COMMIT", self.state.commit_no, vsr_commit.commit_no);

        // Verify state
        if (self.state.commit_no != vsr_commit.commit_no) {
            self.logger.warn()
                .ctx("COMMIT")
                .msg("Confusion reigns in COMMIT")
                .int("replica_no", self.state.replica_no)
                .int("view_no", self.state.view_no)
                .int("op_no", self.state.op_no)
                .int("commit_no", self.state.commit_no)
                .int("expected_commit_no", vsr_commit.commit_no)
                .log();
        }
    }

    /// Handles VsrStartViewChange <START-VIEW-CHANGE> in replicas.
    ///
    /// If no <PREPARE> or <COMMIT> are received from the primary, a replica timeout
    /// will expire. At this point the replica will change its status to view-change,
    /// increase its view number (view-num) and create a <START-VIEW-CHANGE> message
    /// with the new view-num and its identity. It will then send this message to all
    /// the replicas.
    ///
    /// When the other replicas receive a <START-VIEW-CHANGE> message with a view-num
    /// bigger than the one they have, they set their status to view-change and set the
    /// view-num to the view number in the message and reply with <START-VIEW-CHANGE>
    /// to all replicas.
    ///
    /// When a replica receives 𝑓 + 1 (including itself) <START-VIEW-CHANGE> message
    /// with its view-num then it means that the majority of the nodes agrees on the
    /// view change so it sends a <DO-VIEW-CHANGE> to the new primary (selected as
    /// described above). The <DO-VIEW-CHANGE> message contains the new view-num the
    /// last view number in which the state was normal (old-view-number), its op-num
    /// and its operation log (op-log) and its commit number (commit-num).
    pub fn startViewChange(self: *VsrStateMachine, vsr_svc: VsrStartViewChange) MachineError!void {
        self.logger.debug()
            .ctx("START-VIEW-CHANGE")
            .msg("Received VsrStartViewChange")
            .int("replica_no", self.state.replica_no)
            .int("view_no", self.state.view_no)
            .int("op_no", self.state.op_no)
            .int("commit_no", self.state.commit_no)
            .int("vsr_svc.replica_no", vsr_svc.replica_no)
            .int("vsr_svc.view_no", vsr_svc.view_no)
            .log();

        assert(self.state.replica_no != vsr_svc.replica_no);

        // Check state preconditions
        if (vsr_svc.view_no > self.state.view_no) {
            if (self.state.status != .normal) {
                self.logger.warn()
                    .ctx("START-VIEW-CHANGE")
                    .msg("Replica not ready to process SVC")
                    .any("status", self.state.status)
                    .log();
                return;
            }
            // Since another replica has initiated a view change, so must we...
            try self.initiateViewChange(vsr_svc.view_no);
        } else if (vsr_svc.view_no == self.state.view_no) {
            if (self.state.status != .view_change) {
                self.logger.warn()
                    .ctx("START-VIEW-CHANGE")
                    .msg("Replica not ready to process SVC")
                    .any("status", self.state.status)
                    .log();
                return;
            }
            // Check if consensus has been reached
            // Note that the we count *ourselves as well*, hence the subtraction from 'real' quorum
            self.view_change_agreements += 1;
            if (self.view_change_agreements >= (self.state.configuration.quorum - 1)) {
                // Select new primary
                self.state.primary_no = self.selectPrimary();

                if (self.state.primary_no != self.state.replica_no) {
                    // Send DVC to new primary
                    self.logger.debug()
                        .msg("View change quorum reached, raising DVC")
                        .int("replica_no", self.state.replica_no)
                        .int("view_no", self.state.view_no)
                        .int("old_view_no", self.view_change_my_old_view_no)
                        .int("commit_no", self.state.commit_no)
                        .int("primary_no", self.state.primary_no)
                        .log();

                    var vsr_dvc: VsrMessage = .{ .vsr_dvc = try VsrDoViewChange.init(
                        self.allocs,
                        self.state.view_no,
                        self.view_change_my_old_view_no,
                        self.state.op_no,
                        self.state.commit_no,
                        self.state.op_log,
                    ) };
                    defer vsr_dvc.deinit(self.allocs);
                    try self.transport.sendToPrimary(self.state.primary_no, &vsr_dvc);
                }
            }
        } else {
            self.logger.warn()
                .ctx("START-VIEW-CHANGE")
                .msg("Confusion reigns in START-VIEW-CHANGE")
                .int("replica_no", self.state.replica_no)
                .int("view_no", self.state.view_no)
                .int("op_no", self.state.op_no)
                .int("commit_no", self.state.commit_no)
                .int("svc_view_no", vsr_svc.view_no)
                .log();
        }
    }

    /// Selects a new primary node after a view change.
    ///
    /// The ViewStamped Replication algorithm simply uses the sorted list of IPs
    /// to determine who is the next primary node, and in round-robin fashion
    /// all nodes will, in turn, become the new primary when the previous one
    /// is unavailable.
    ///
    /// Dymes-specific: Our replica node list isn't a sorted list of IPs, using
    /// a list of named host configurations. This allows k8s to re-assign IP
    /// addresses as required, without clobbering our effective configuration.
    fn selectPrimary(self: *const VsrStateMachine) u8 {
        const old_primary_no = self.state.primary_no;

        self.logger.fine()
            .msg("Selecting new primary")
            .int("replica_no", self.state.replica_no)
            .int("view_no", self.state.view_no)
            .int("op_no", self.state.op_no)
            .int("commit_no", self.state.commit_no)
            .int("primary_no", old_primary_no)
            .log();

        // Default, round-robin selection
        const new_primary_no = new_primary_val: {
            if (old_primary_no + 1 >= self.state.configuration.host_configs.len) {
                break :new_primary_val 0;
            } else {
                break :new_primary_val old_primary_no + 1;
            }
        };

        self.logger.debug()
            .msg("New primary selected")
            .int("replica_no", self.state.replica_no)
            .int("view_no", self.state.view_no)
            .int("op_no", self.state.op_no)
            .int("commit_no", self.state.commit_no)
            .int("new_primary", new_primary_no)
            .int("old_primary", old_primary_no)
            .log();

        return new_primary_no;
    }

    /// Handles VsrDoViewChange <DO-VIEW-CHANGE> in new primary.
    ///
    /// When the new primary receives 𝑓 + 1(including itself) <DO-VIEW-CHANGE> messages
    /// with the same view-num it compares the messages against its own information and
    /// pick the most up-to-date.
    ///
    /// It will set the view-num to the new view-num, it will take the operation log
    /// (op-log) from the replicas with the highest old-view-number.
    ///
    /// If many replicas have the same old-view-number it will pick the one with the
    /// largest op-log, it will take the op-num from the chosen op-log and the highest
    /// commit-num and execute all committed operations in the operation log between
    /// its old commit-num value and the new commit-num value.
    ///
    /// At this point, the new primary is ready to accept requests from the client so it
    /// sets its status to normal. Finally, it sends a <START-VIEW> message to all
    /// replicas with the new view-num, the most up to date op-log, the corresponding
    /// op-num and the highest commit-num.
    pub fn doViewChange(self: *VsrStateMachine, vsr_dvc: VsrDoViewChange) MachineError!void {
        self.logger.debug()
            .ctx("DO-VIEW-CHANGE")
            .msg("Received VsrDoViewChange")
            .int("replica_no", self.state.replica_no)
            .int("view_no", self.state.view_no)
            .int("op_no", self.state.op_no)
            .int("commit_no", self.state.commit_no)
            .int("vsr_dvc.view_no", vsr_dvc.view_no)
            .int("vsr_dvc.old_view_no", vsr_dvc.old_view_no)
            .int("vsr_dvc.commit_no", vsr_dvc.commit_no)
            .int("vsr_dvc.op_no", vsr_dvc.op_no)
            .int("vsr_dvc.op_log.size", vsr_dvc.op_log.ops_entries.items.len)
            .log();

        // Sanity checks

        if (self.state.status != .view_change) {
            self.logger.warn()
                .ctx("DO-VIEW-CHANGE")
                .msg("Ignoring DVC while not in view change mode")
                .int("replica_no", self.state.replica_no)
                .int("view_no", self.state.view_no)
                .int("op_no", self.state.op_no)
                .int("commit_no", self.state.commit_no)
                .int("vsr_dvc.view_no", vsr_dvc.view_no)
                .int("vsr_dvc.old_view_no", vsr_dvc.old_view_no)
                .int("vsr_dvc.commit_no", vsr_dvc.commit_no)
                .int("vsr_dvc.op_no", vsr_dvc.op_no)
                .int("vsr_dvc.op_log.size", vsr_dvc.op_log.ops_entries.items.len)
                .log();
            return;
        }

        if (self.state.view_no != vsr_dvc.view_no) {
            self.logger.warn()
                .ctx("DO-VIEW-CHANGE")
                .msg("Ignoring DVC from replica with different view")
                .int("replica_no", self.state.replica_no)
                .int("view_no", self.state.view_no)
                .int("op_no", self.state.op_no)
                .int("commit_no", self.state.commit_no)
                .int("vsr_dvc.view_no", vsr_dvc.view_no)
                .int("vsr_dvc.old_view_no", vsr_dvc.old_view_no)
                .int("vsr_dvc.commit_no", vsr_dvc.commit_no)
                .int("vsr_dvc.op_no", vsr_dvc.op_no)
                .int("vsr_dvc.op_log.size", vsr_dvc.op_log.ops_entries.items.len)
                .log();
            return;
        }

        // Update 'optimal' DVC reference message.
        // We pick the one with the latest view number and largest op-log
        self.dvc_count += 1;
        if (self.dvc_ref_msg) |*_dvc_msg| {
            if (vsr_dvc.old_view_no > _dvc_msg.old_view_no or vsr_dvc.op_log.ops_entries.items.len > _dvc_msg.op_log.ops_entries.items.len) {
                defer _dvc_msg.deinit(self.allocs);
                self.dvc_ref_msg = try vsr_dvc.clone(self.allocs);
            }
        } else {
            self.dvc_ref_msg = try vsr_dvc.clone(self.allocs);
        }

        // Check if consensus has been reached
        // Note that the we count *ourselves as well*, hence the subtraction from 'real' quorum
        if (self.dvc_count > (self.state.configuration.quorum - 1)) {
            // We have reached consensus, and at this point,
            // `self.dvc_ref_msg` contains the latest/largest information

            var latest_dvc_ref_msg = self.dvc_ref_msg orelse {
                self.logger.err()
                    .ctx("DO-VIEW-CHANGE")
                    .msg("No reference DVC message available")
                    .int("replica_no", self.state.replica_no)
                    .int("view_no", self.state.view_no)
                    .int("op_no", self.state.op_no)
                    .int("commit_no", self.state.commit_no)
                    .log();
                return MachineError.IllegalState;
            };
            defer latest_dvc_ref_msg.deinit(self.allocs);
            defer self.dvc_ref_msg = null;

            // Update state and apply pending commits
            self.state.view_no = latest_dvc_ref_msg.view_no;
            self.state.op_no = latest_dvc_ref_msg.op_no;
            self.state.op_log.deinit(self.allocs);
            self.state.op_log = try latest_dvc_ref_msg.op_log.clone(self.allocs);

            self.state.commit_no = try self.applyPendingCommits("DO-VIEW-CHANGE", self.state.commit_no, latest_dvc_ref_msg.commit_no);

            // Verify state
            if (self.state.commit_no != latest_dvc_ref_msg.commit_no) {
                self.logger.warn()
                    .ctx("DO-VIEW-CHANGE")
                    .msg("Confusion reigns in DVC")
                    .int("replica_no", self.state.replica_no)
                    .int("view_no", self.state.view_no)
                    .int("op_no", self.state.op_no)
                    .int("commit_no", self.state.commit_no)
                    .int("expected_commit_no", latest_dvc_ref_msg.commit_no)
                    .log();
            }

            // We are now primary
            self.state.primary_no = self.state.replica_no;
            self.state.status = .normal;

            var vsr_sv: VsrMessage = .{
                .vsr_sv = try VsrStartView.init(
                    self.allocs,
                    self.state.view_no,
                    self.state.op_no,
                    self.state.commit_no,
                    self.state.op_log,
                ),
            };
            defer vsr_sv.deinit(self.allocs);

            try self.transport.sendToReplicas(&vsr_sv);
        }
    }

    /// Handles VsrStartView <START-VIEW> in replicas.
    ///
    /// When the replicas receive a <START-VIEW> message they replace their own
    /// local information with the data in the <START-VIEW> message, specifically they
    /// take: the op-log, the op-num and the view-num. They change their status to
    /// normal and they execute all the operation from their old commit-num to the new
    /// commit-num and they send a <PREPARE-OK> for all operation in the op-log which
    /// haven’t been committed yet.
    pub fn startView(self: *VsrStateMachine, vsr_sv: VsrStartView) MachineError!void {
        self.logger.debug()
            .ctx("START-VIEW")
            .msg("Received VsrStartView")
            .int("replica_no", self.state.replica_no)
            .int("view_no", self.state.view_no)
            .int("op_no", self.state.op_no)
            .int("commit_no", self.state.commit_no)
            .int("vsr_sv.view_no", vsr_sv.view_no)
            .int("vsr_sv.commit_no", vsr_sv.commit_no)
            .int("vsr_sv.op_no", vsr_sv.op_no)
            .int("vsr_sv.op_log.size", vsr_sv.op_log.ops_entries.items.len)
            .log();

        // Sanity checks

        if (self.state.status != .view_change) {
            self.logger.warn()
                .ctx("START-VIEW")
                .msg("Ignoring SV while not in view change mode")
                .int("replica_no", self.state.replica_no)
                .int("view_no", self.state.view_no)
                .int("op_no", self.state.op_no)
                .int("commit_no", self.state.commit_no)
                .int("vsr_sv.view_no", vsr_sv.view_no)
                .int("vsr_sv.commit_no", vsr_sv.commit_no)
                .int("vsr_sv.op_no", vsr_sv.op_no)
                .int("vsr_sv.op_log.size", vsr_sv.op_log.ops_entries.items.len)
                .log();
            return;
        }

        // Update state and apply pending commits
        self.state.view_no = vsr_sv.view_no;
        self.state.op_no = vsr_sv.op_no;
        self.state.op_log.deinit(self.allocs);
        self.state.op_log = try vsr_sv.op_log.clone(self.allocs);

        self.state.commit_no = try self.applyPendingCommits("START-VIEW", self.state.commit_no, vsr_sv.commit_no);

        // Verify state
        if (self.state.commit_no != vsr_sv.commit_no) {
            self.logger.warn()
                .ctx("START-VIEW")
                .msg("Confusion reigns in SV")
                .int("replica_no", self.state.replica_no)
                .int("view_no", self.state.view_no)
                .int("op_no", self.state.op_no)
                .int("commit_no", self.state.commit_no)
                .int("expected_commit_no", vsr_sv.commit_no)
                .log();
        }

        self.state.status = .normal;

        // Send PREPARE-OKs for uncommitted ops

        const from_op_no = self.state.commit_no;
        const to_op_no = self.state.op_no;

        if (from_op_no >= to_op_no) {
            // No uncommitted ops
            self.logger.fine()
                .ctx("START-VIEW")
                .msg("No uncommitted operations")
                .int("replica_no", self.state.replica_no)
                .int("view_no", self.state.view_no)
                .int("op_no", self.state.op_no)
                .int("commit_no", self.state.commit_no)
                .log();
            return;
        }

        const starting_at_op_no = from_op_no + 1;
        self.logger.fine()
            .ctx("START-VIEW")
            .msg("Raising PREPARE-OKs for uncommitted operations")
            .int("replica_no", self.state.replica_no)
            .int("view_no", self.state.view_no)
            .int("op_no", self.state.op_no)
            .int("commit_no", self.state.commit_no)
            .int("from_op_no", from_op_no)
            .int("target_op_no", to_op_no)
            .int("starting_at_op_no", starting_at_op_no)
            .log();

        var sanity_check_op_no = from_op_no;
        for (starting_at_op_no..to_op_no + 1) |_uncommitted_op_no| {
            const op_to_prepare = try self.state.op_log.fetchOp(_uncommitted_op_no);
            switch (op_to_prepare) {
                .append => |_append_op| {
                    const entry = self.state.client_table.getEntryByOp(_append_op.opNumber()) orelse return MachineError.InconsistentState;
                    const prepareOK: VsrMessage = .{
                        .vsr_prepare_ok = VsrPrepareOK.init(
                            self.state.view_no,
                            self.state.op_no,
                            self.state.replica_no,
                            entry.client_id,
                            entry.request_no,
                        ),
                    };
                    try self.transport.sendToPrimary(self.state.primary_no, &prepareOK);
                },
            }
            self.logger.fine()
                .ctx("START-VIEW")
                .msg("Raised PREPARE-OKs for uncommitted operation")
                .int("replica_no", self.state.replica_no)
                .int("view_no", self.state.view_no)
                .int("op_no", self.state.op_no)
                .int("commit_no", _uncommitted_op_no)
                .log();
            sanity_check_op_no += 1;
        }

        if (sanity_check_op_no != to_op_no) {
            self.logger.warn()
                .ctx("START-VIEW")
                .msg("Confusion after raising PREPARE-OKs")
                .int("replica_no", self.state.replica_no)
                .int("view_no", self.state.view_no)
                .int("op_no", self.state.op_no)
                .int("commit_no", self.state.commit_no)
                .int("from_commit_no", from_op_no)
                .int("target_commit_no", to_op_no)
                .int("actual_commit_no", sanity_check_op_no)
                .log();
            return MachineError.InconsistentState;
        }
    }

    /// Handles VsrGetState <GET-STATE> in replicas.
    ///
    /// When a replica falls behind, or a partitioned node rejoins the ensemble,
    /// it will set its status to recovery and issue a <GET-STATE> request
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
    pub fn getState(self: *VsrStateMachine, vsr_gs: VsrGetState) MachineError!void {
        self.logger.debug()
            .ctx("GS")
            .msg("VsrGetState")
            .any("message", vsr_gs)
            .log();
    }

    /// Handles VsrNewState <NEW-STATE> in recovering replicas.
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
    pub fn newState(self: *VsrStateMachine, vsr_ns: VsrNewState) MachineError!void {
        self.logger.debug()
            .ctx("NS")
            .msg("VsrNewState")
            .any("message", vsr_ns)
            .log();
    }

    /// Build VSR configuration using external configuration.
    fn buildVsrConfiguration(logger: *Logger, allocs: Allocators, dymes_vsr_config: dymes_common.config.Config) InitError!struct { VsrConfiguration, wire.ReplicaNumber } {
        var vsr_config_bld = VsrConfiguration.Builder.init();
        defer vsr_config_bld.deinit(allocs.gpa);

        const bind_host: []const u8 = try dymes_vsr_config.asString("host") orelse return InitError.MissingArgument;
        const bind_port_raw: i128 = try dymes_vsr_config.asInt("port") orelse constants.default_vsr_replica_port;
        const bind_port: u16 = @intCast(bind_port_raw);

        const replica_no: u8 = replica_no_val: {
            if (try dymes_vsr_config.asStringList("replicas")) |_nodes| {
                var possible_replica_no: ?u8 = null;
                for (_nodes.items, 0..) |_node, _replica_no| {
                    if (std.mem.lastIndexOfScalar(u8, _node, ':')) |_idx_pre_port| {
                        const parsed_port = std.fmt.parseInt(u16, _node[_idx_pre_port + 1 ..], 0) catch |_e| {
                            logger.warn()
                                .msg("Failed to parse replica configuration")
                                .err(_e)
                                .int("replica_no", _replica_no)
                                .log();
                            return InitError.IllegalArgument;
                        };
                        try vsr_config_bld.withReplica(allocs.gpa, _node[0.._idx_pre_port], parsed_port);
                        if (parsed_port == bind_port and std.mem.eql(u8, bind_host, _node[0.._idx_pre_port])) {
                            possible_replica_no = @intCast(_replica_no);
                        }
                    } else {
                        try vsr_config_bld.withReplica(allocs.gpa, _node[0..], constants.default_vsr_replica_port);
                        if (constants.default_vsr_replica_port == bind_port and std.mem.eql(u8, bind_host, _node[0..])) {
                            possible_replica_no = @intCast(_replica_no);
                        }
                    }
                }
                break :replica_no_val possible_replica_no orelse {
                    logger.warn()
                        .msg("Unable to determine replica number")
                        .log();
                    return InitError.MissingArgument;
                };
            } else {
                logger.warn()
                    .msg("Replica configuration missing replica host information")
                    .log();
                return InitError.MissingArgument;
            }
        };

        return .{
            try vsr_config_bld.build(allocs.gpa),
            replica_no,
        };
    }

    /// Applies pending commits, returning the last commit number.
    ///
    /// Executes all operation in the op-log between the last commit-num and
    /// the given commit-num, strictly following the order of operations and advance
    /// the commit-num.
    fn applyPendingCommits(self: *VsrStateMachine, log_context: []const u8, from_commit_no: CommitNumber, to_commit_no: CommitNumber) MachineError!CommitNumber {
        // Perform pending commits
        if (from_commit_no >= to_commit_no) {
            // We're already up to date
            self.logger.fine()
                .ctx(log_context)
                .msg("No pending commits")
                .int("replica_no", self.state.replica_no)
                .int("view_no", self.state.view_no)
                .int("op_no", self.state.op_no)
                .int("commit_no", self.state.commit_no)
                .int("from_commit_no", from_commit_no)
                .int("target_commit_no", to_commit_no)
                .log();
            return to_commit_no;
        }

        const starting_at_commit = from_commit_no + 1;
        self.logger.fine()
            .ctx(log_context)
            .msg("Applying pending commits")
            .int("replica_no", self.state.replica_no)
            .int("view_no", self.state.view_no)
            .int("op_no", self.state.op_no)
            .int("commit_no", self.state.commit_no)
            .int("from_commit_no", from_commit_no)
            .int("target_commit_no", to_commit_no)
            .int("starting_at_commit_no", starting_at_commit)
            .log();

        var sanity_check_commit_no = from_commit_no;
        for (starting_at_commit..to_commit_no + 1) |_commit_no| {
            const op_to_commit = try self.state.op_log.fetchOp(_commit_no);
            switch (op_to_commit) {
                .append => |_append_op| {
                    _ = self.ops_handler.vft.append(self.ops_handler.ctx, _append_op) catch |_e| {
                        self.logger.err()
                            .ctx(log_context)
                            .msg("Failed to append message")
                            .err(_e)
                            .int("replica_no", self.state.replica_no)
                            .int("view_no", self.state.view_no)
                            .int("op_no", _commit_no)
                            .log();
                        return MachineError.InconsistentState;
                    };
                },
            }
            self.logger.fine()
                .ctx(log_context)
                .msg("Commit applied")
                .int("replica_no", self.state.replica_no)
                .int("view_no", self.state.view_no)
                .int("op_no", self.state.op_no)
                .int("commit_no", _commit_no)
                .log();
            sanity_check_commit_no += 1;
        }

        // Another sanity check
        if (sanity_check_commit_no != to_commit_no) {
            self.logger.warn()
                .ctx(log_context)
                .msg("Confusion after applying commits")
                .int("replica_no", self.state.replica_no)
                .int("view_no", self.state.view_no)
                .int("op_no", self.state.op_no)
                .int("commit_no", self.state.commit_no)
                .int("from_commit_no", from_commit_no)
                .int("target_commit_no", to_commit_no)
                .int("actual_commit_no", sanity_check_commit_no)
                .log();
            return MachineError.InconsistentState;
        } else {
            self.logger.debug()
                .ctx(log_context)
                .msg("Pending commits applied")
                .int("replica_no", self.state.replica_no)
                .int("view_no", self.state.view_no)
                .int("op_no", self.state.op_no)
                .int("from_commit_no", from_commit_no)
                .int("to_commit_no", to_commit_no)
                .log();
        }

        return sanity_check_commit_no;
    }
};

const DummyEngine = struct {
    logger: *Logger,

    pub fn init() DummyEngine {
        return .{ .logger = logging.logger("Dummy Engine") };
    }

    pub fn handleAppend(ctx: *anyopaque, op: VsrAppendOp) OpsHandlerError!VsrAppendResponse {
        const self: *DummyEngine = @ptrCast(@alignCast(ctx));
        self.logger.debug()
            .msg("Handled append operation")
            .any("op", op)
            .log();
        return .{};
    }
};

// Smoke test VsrStateMachine
test "VsrStateMachine.smoke" {
    std.debug.print("test.VsrStateMachine.smoke\n", .{});
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

    // Prepare "engine"

    var dummy_engine: DummyEngine = .init();

    const dummy_ops_handler: OpsHandler = .{
        .ctx = &dummy_engine,
        .vft = .{
            .append = DummyEngine.handleAppend,
        },
    };

    // Prepare ZeroMQ context
    const zimq_context: *zimq.Context = try .init();
    defer zimq_context.deinit();

    // Missing host
    {
        const bad_cfg_source =
            \\vsr:
            \\  port: 1542
        ;

        var bad_cfg = try dymes_common.config.fromYamlString(bad_cfg_source[0..], "badvsr", allocs.gpa);
        defer bad_cfg.deinit();
        const dymes_vsr_config = try bad_cfg.asConfig("vsr") orelse return UsageError.MissingArgument;

        try testing.expectError(error.MissingArgument, VsrStateMachine.init(allocs, dummy_ops_handler, zimq_context, 1, dymes_vsr_config));
    }

    // Missing replicas
    {
        const bad_cfg_source =
            \\vsr:
            \\  port: 1542
            \\  host: "localhost"
        ;

        var bad_cfg = try dymes_common.config.fromYamlString(bad_cfg_source[0..], "badvsr", allocs.gpa);
        defer bad_cfg.deinit();
        const dymes_vsr_config = try bad_cfg.asConfig("vsr") orelse return UsageError.MissingArgument;

        try testing.expectError(error.MissingArgument, VsrStateMachine.init(allocs, dummy_ops_handler, zimq_context, 1, dymes_vsr_config));
    }

    {
        const sane_cfg_source =
            \\vsr:
            \\  port: 1542
            \\  interface: "0.0.0.0"
            \\  host: "localhost"
            \\  replicas:
            \\    - localhost:1541
            \\    - localhost:1542
            \\    - localhost:1543
        ;

        var dymes_cfg = try dymes_common.config.fromYamlString(sane_cfg_source, "vsr", allocs.gpa);
        defer dymes_cfg.deinit();
        const dymes_vsr_config = try dymes_cfg.asConfig("vsr") orelse return UsageError.MissingArgument;

        var vsr_machine = try VsrStateMachine.init(allocs, dummy_ops_handler, zimq_context, 1, dymes_vsr_config);
        defer vsr_machine.deinit();

        try testing.expectEqual(1, vsr_machine.state.replica_no);
        try testing.expectEqual(0, vsr_machine.state.primary_no);
        try testing.expectEqual(0, vsr_machine.state.view_no);
        try testing.expectEqual(.normal, vsr_machine.state.status);
    }
}
