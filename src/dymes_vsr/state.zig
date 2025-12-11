//! Dymes VSR state.
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
const vsr_testing = @import("testing.zig");

const constants = @import("constants.zig");

const Allocators = wire.Allocators;

// Log level for embedded tests
const module_tests_log_level = logging.LogLevel.none;

/// The operational status of a replica.
///
/// The status can assume different values depending on whether the replica is ready to process client requests, or it is getting ready and doing internal preparation.
///
pub const VsrStatus = enum {
    /// Normal operation (ready and up-to-date)
    normal,
    /// View change (switching primary)
    view_change,
    /// Recovery (catching up to primary)
    recovery,
};

/// Replica host configuration.
pub const VsrReplicaHostConfig = struct {
    hostname: [:0]const u8,
    port_no: u16,

    pub fn init(
        gpa: Allocator,
        hostname: []const u8,
        port_no: u16,
    ) AllocationError!VsrReplicaHostConfig {
        return .{
            .hostname = gpa.dupeZ(u8, hostname) catch return AllocationError.OutOfMemory,
            .port_no = port_no,
        };
    }

    pub fn deinit(self: *const VsrReplicaHostConfig, gpa: Allocator) void {
        gpa.free(self.hostname);
    }

    pub fn clone(self: *const VsrReplicaHostConfig, gpa: Allocator) AllocationError!VsrReplicaHostConfig {
        return .{
            .hostname = gpa.dupeZ(u8, self.hostname) catch return AllocationError.OutOfMemory,
            .port_no = self.port_no,
        };
    }
};

/// The ensemble (cluster) configuration.
///
/// In order to be k8s-friendly, we store an _unsorted_ array of the _hostnames_ of the 2𝑓+1 replicas.
/// This allows k8s to re-assign IP addresses as required, without clobbering our effective configuration.
///
/// This is different from 'reference' VSR, where this would be a sorted array containing
/// the IP addresses of each of the 2𝑓+1 replicas.
pub const VsrConfiguration = struct {
    pub const Builder = struct {
        pub const BuildError = UsageError || AllocationError;
        host_configs: std.ArrayList(VsrReplicaHostConfig),
        failure_threshold: u8,
        idle_commit_timeout_ms: u32,
        idle_svc_timeout_ms: u32,

        /// Initializes the VSR configuration builder.
        ///
        /// Caller must call `deinit()` to release resources.
        pub fn init() Builder {
            return .{
                .host_configs = .empty,
                .failure_threshold = constants.default_failure_threshold,
                .idle_commit_timeout_ms = constants.default_idle_commit_timeout_ms,
                .idle_svc_timeout_ms = constants.default_idle_scv_timeout_ms,
            };
        }

        /// De-initializes the VSR configuration builder, releasing resources.
        pub fn deinit(self: *Builder, gpa: Allocator) void {
            defer self.host_configs.deinit(gpa);
            for (self.host_configs.items) |_host_config| {
                _host_config.deinit(gpa);
            }
        }

        /// Adds a replica configuration entry with given hostname and port number.
        pub fn withReplica(self: *Builder, gpa: Allocator, hostname: []const u8, port_no: u16) AllocationError!void {
            const host_config = VsrReplicaHostConfig.init(gpa, hostname, port_no) catch return AllocationError.OutOfMemory;
            errdefer host_config.deinit(gpa);
            self.host_configs.append(gpa, host_config) catch return AllocationError.OutOfMemory;
        }

        /// Sets the number of failed nodes allowed (𝑓).
        pub fn withFailureThreshold(self: *Builder, failure_threshold: u8) UsageError!void {
            if (failure_threshold < constants.min_failure_threshold) {
                logging.logger("vsr.VsrConfiguration.Builder").err()
                    .msg("Failure threshold below minimum")
                    .int("failure_threshold", failure_threshold)
                    .int("min_failure_threshold", constants.min_failure_threshold)
                    .log();

                return UsageError.IllegalArgument;
            }
            self.failure_threshold = failure_threshold;
        }

        /// Sets the idle commit timeout.
        pub fn withIdleCommitTimeout(self: *Builder, idle_commit_timeout_ms: u32) UsageError!void {
            if (idle_commit_timeout_ms < constants.min_idle_commit_timeout_ms) {
                logging.logger("vsr.VsrConfiguration.Builder").err()
                    .msg("Idle commit timeout below minimum")
                    .int("idle_commit_timeout", idle_commit_timeout_ms)
                    .int("min_idle_commit_timeout", constants.min_idle_commit_timeout_ms)
                    .log();

                return UsageError.IllegalArgument;
            }
            self.idle_commit_timeout_ms = idle_commit_timeout_ms;
        }

        /// Sets the idle SVC timeout.
        pub fn withIdleSvcTimeout(self: *Builder, idle_svc_timeout_ms: u32) UsageError!void {
            if (idle_svc_timeout_ms < constants.min_idle_scv_timeout_ms) {
                logging.logger("vsr.VsrConfiguration.Builder").err()
                    .msg("Idle SVC timeout below minimum")
                    .int("idle_svc_timeout", idle_svc_timeout_ms)
                    .int("min_idle_svc_timeout", constants.min_idle_scv_timeout_ms)
                    .log();

                return UsageError.IllegalArgument;
            }
            self.idle_svc_timeout_ms = idle_svc_timeout_ms;
        }

        /// Builds the VSR configuration.
        ///
        /// Caller must call `deinit()` on the returned value to release resources.
        pub fn build(self: *Builder, gpa: Allocator) BuildError!VsrConfiguration {
            const min_ensemble_size: u8 = 2 * self.failure_threshold + 1;
            const num_replicas: u8 = @truncate(self.host_configs.items.len);
            if (num_replicas < min_ensemble_size) {
                logging.logger("vsr.VsrConfiguration.Builder").err()
                    .msg("Fewer hosts than minimum ensemble size")
                    .int("min_ensemble_size", min_ensemble_size)
                    .int("num_hosts", self.host_configs.items.len)
                    .log();
                return UsageError.InvalidRequest;
            } else if (num_replicas > min_ensemble_size) {
                logging.logger("vsr.VsrConfiguration.Builder").debug()
                    .msg("More hosts than minimum ensemble size")
                    .int("min_ensemble_size", min_ensemble_size)
                    .int("num_hosts", self.host_configs.items.len)
                    .log();
            }

            const host_configs = self.host_configs.toOwnedSlice(gpa) catch return AllocationError.OutOfMemory;

            return .{
                .num_replicas = num_replicas,
                .host_configs = host_configs,
                .failure_threshold = self.failure_threshold,
                .quorum = self.failure_threshold + 1, // Quorum is 𝑓 + 1
                .idle_commit_timeout_ms = self.idle_commit_timeout_ms,
                .idle_scv_timeout_ms = self.idle_svc_timeout_ms,
            };
        }
    };

    /// Number of replicas (2𝑓+1)
    num_replicas: u8,

    /// _Unsorted_ array of the _host configuration_ of the (2𝑓+1) replicas
    host_configs: []VsrReplicaHostConfig,

    /// Number of failed nodes allowed (𝑓).
    failure_threshold: u8,

    /// Quorum is 𝑓 + 1
    quorum: u8,

    /// Idle timeout to trigger COMMITs from primary
    idle_commit_timeout_ms: u32,

    /// Idle timeout, to trigger SVCs from replicas
    idle_scv_timeout_ms: u32,

    /// De-initializes client table state, releasing resources.
    pub fn deinit(self: *const VsrConfiguration, gpa: Allocator) void {
        defer gpa.free(self.host_configs);
        for (self.host_configs) |_host_config| {
            _host_config.deinit(gpa);
        }
    }

    pub fn clone(self: *const VsrConfiguration, gpa: Allocator) AllocationError!VsrConfiguration {
        const replica_hosts: []VsrReplicaHostConfig = gpa.alloc(VsrReplicaHostConfig, self.num_replicas) catch return AllocationError.OutOfMemory;
        errdefer gpa.free(replica_hosts);
        for (self.host_configs, 0..) |_host_config, _idx| {
            replica_hosts[_idx] = _host_config.clone(gpa) catch return AllocationError.OutOfMemory;
        }
        errdefer for (replica_hosts) |_host_config| {
            _host_config.deinit(gpa);
        };

        return .{
            .num_replicas = self.num_replicas,
            .host_configs = replica_hosts,
            .failure_threshold = self.failure_threshold,
            .quorum = self.quorum,
            .idle_commit_timeout_ms = self.idle_commit_timeout_ms,
            .idle_scv_timeout_ms = self.idle_scv_timeout_ms,
        };
    }

    pub inline fn replicaHostname(self: *const VsrConfiguration, replica_no: usize) [:0]const u8 {
        assert(replica_no < self.num_replicas);
        return self.host_configs[@truncate(replica_no)].hostname;
    }

    pub inline fn replicaPort(self: *const VsrConfiguration, replica_no: usize) u16 {
        assert(replica_no < self.num_replicas);
        return self.host_configs[@truncate(replica_no)].port_no;
    }
};

// Smoke test VsrConfiguration
test "VsrConfiguration.smoke" {
    std.debug.print("test.VsrConfiguration.smoke\n", .{});
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

    {
        var vsr_cfg_bld = VsrConfiguration.Builder.init();
        defer vsr_cfg_bld.deinit(allocs.gpa);
    }

    {
        var vsr_cfg_bld = VsrConfiguration.Builder.init();
        defer vsr_cfg_bld.deinit(allocs.gpa);

        try testing.expectError(error.IllegalArgument, vsr_cfg_bld.withFailureThreshold(0));
    }

    // Minimal ensemble
    {
        var vsr_cfg_bld = VsrConfiguration.Builder.init();
        defer vsr_cfg_bld.deinit(allocs.gpa);
        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1541);
        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1542);
        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1543);

        const vsr_cfg = try vsr_cfg_bld.build(allocs.gpa);
        defer vsr_cfg.deinit(allocs.gpa);
        try testing.expectEqual(3, vsr_cfg.num_replicas);
        try testing.expectEqual(1, vsr_cfg.failure_threshold);
        try testing.expectEqual(2, vsr_cfg.quorum);
        try testing.expectEqual(1541, vsr_cfg.replicaPort(0));
        try testing.expectEqual(1542, vsr_cfg.replicaPort(1));
        try testing.expectEqual(1543, vsr_cfg.replicaPort(2));
    }

    // Proper ensemble
    {
        var vsr_cfg_bld = VsrConfiguration.Builder.init();
        defer vsr_cfg_bld.deinit(allocs.gpa);
        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1541);
        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1542);
        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1543);
        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1544);
        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1545);
        try vsr_cfg_bld.withFailureThreshold(2);

        const vsr_cfg = try vsr_cfg_bld.build(allocs.gpa);
        defer vsr_cfg.deinit(allocs.gpa);

        try testing.expectEqual(5, vsr_cfg.num_replicas);
        try testing.expectEqual(2, vsr_cfg.failure_threshold);
        try testing.expectEqual(3, vsr_cfg.quorum);
        try testing.expectEqual(1541, vsr_cfg.replicaPort(0));
        try testing.expectEqual(1542, vsr_cfg.replicaPort(1));
        try testing.expectEqual(1543, vsr_cfg.replicaPort(2));
        try testing.expectEqual(1544, vsr_cfg.replicaPort(3));
        try testing.expectEqual(1545, vsr_cfg.replicaPort(4));
    }

    // Below minimum hosts
    {
        var vsr_cfg_bld = VsrConfiguration.Builder.init();
        defer vsr_cfg_bld.deinit(allocs.gpa);
        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1541);
        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1542);

        try vsr_cfg_bld.withFailureThreshold(1);

        try testing.expectError(error.InvalidRequest, vsr_cfg_bld.build(allocs.gpa));
    }

    // Above minimum hosts
    {
        var vsr_cfg_bld = VsrConfiguration.Builder.init();
        defer vsr_cfg_bld.deinit(allocs.gpa);
        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1541);
        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1542);
        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1543);
        try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1544);

        try vsr_cfg_bld.withFailureThreshold(1);

        const vsr_cfg = try vsr_cfg_bld.build(allocs.gpa);
        defer vsr_cfg.deinit(allocs.gpa);

        try testing.expectEqual(4, vsr_cfg.num_replicas);
        try testing.expectEqual(1, vsr_cfg.failure_threshold);
        try testing.expectEqual(2, vsr_cfg.quorum);
        try testing.expectEqual(1541, vsr_cfg.replicaPort(0));
        try testing.expectEqual(1542, vsr_cfg.replicaPort(1));
        try testing.expectEqual(1543, vsr_cfg.replicaPort(2));
        try testing.expectEqual(1544, vsr_cfg.replicaPort(3));
    }
}

/// VSR client table (client-table).
///
/// This is used to keep track of client’s requests.
///
/// Clients are identified by a unique id, and each client can only make one request at the time.
/// Since communication is assumed to be unreliable, clients can re-issue the last request without the risk of duplication in the system.
/// Every client request has a monotonically increasing number which identifies the request of a particular client.
/// Each time the primary receives a client requests it add the request to the client table.
/// If the client re-sends the same requests because didn’t receive the response the primary can verify that the request was already processed and send the cached response.
pub const VsrClientTable = struct {
    pub const UpdateError = AllocationError || UsageError;
    const RequestsByClientId = std.hash_map.AutoHashMap(wire.ClientId, Entry);
    const ClientIdByOpNo = std.hash_map.AutoHashMap(wire.OpNumber, wire.ClientId);
    pub const Entry = struct {
        /// Client identifier (client-id)
        client_id: wire.ClientId,

        /// Request number (request-num)
        request_no: wire.RequestNumber,

        /// Operation requested (op)
        op: ops.VsrOp,

        /// Response
        response: ?ops.VsrOpResponse,

        pub fn init(
            allocs: Allocators,
            client_id: wire.ClientId,
            request_no: wire.RequestNumber,
            op: ops.VsrOp,
        ) AllocationError!Entry {
            const cloned_op = op.clone(allocs) catch return AllocationError.OutOfMemory;
            errdefer cloned_op.deinit(allocs);
            return .{
                .client_id = client_id,
                .request_no = request_no,
                .op = cloned_op,
                .response = null,
            };
        }

        pub fn deinit(self: *const Entry, allocs: Allocators) void {
            defer self.op.deinit(allocs);
        }
    };

    allocs: Allocators,
    requests_by_client: RequestsByClientId,
    client_by_op: ClientIdByOpNo,

    /// Initializes the client table.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn init(allocs: Allocators) AllocationError!VsrClientTable {
        return .{
            .requests_by_client = RequestsByClientId.init(allocs.gpa),
            .client_by_op = ClientIdByOpNo.init(allocs.gpa),
            .allocs = allocs,
        };
    }

    /// De-initializes the client table, releasing resources.
    pub fn deinit(self: *VsrClientTable) void {
        defer self.requests_by_client.deinit();
        defer self.client_by_op.deinit();
        var it = self.requests_by_client.iterator();
        while (it.next()) |_entry| {
            _entry.value_ptr.deinit(self.allocs);
        }
    }

    /// Adds a new client request to the table, with sanity checks.
    pub fn addRequest(self: *VsrClientTable, client_id: wire.ClientId, request_no: wire.RequestNumber, op: ops.VsrOp) UpdateError!void {
        if (self.requests_by_client.getPtr(client_id)) |_entry| {
            if (_entry.request_no >= request_no) {
                return UpdateError.InvalidRequest;
            } else {
                _entry.deinit(self.allocs);
            }
        }

        const new_entry = try Entry.init(self.allocs, client_id, request_no, op);
        errdefer new_entry.deinit(self.allocs);

        self.requests_by_client.put(client_id, new_entry) catch return UpdateError.OutOfMemory;
        errdefer _ = self.requests_by_client.remove(client_id);
        self.client_by_op.put(op.opNumber(), client_id) catch return UpdateError.OutOfMemory;
    }

    /// Removes a client request from the table.
    ///
    /// Note: This is not for general use, but for error handling code.
    pub fn removeRequest(self: *VsrClientTable, client_id: wire.ClientId) void {
        if (self.requests_by_client.getPtr(client_id)) |_entry| {
            _entry.deinit(self.allocs);
            _ = self.requests_by_client.remove(client_id);
            _ = self.client_by_op.remove(_entry.op.opNumber());
        }
    }

    /// Sets the response sent to a client, with sanity checks.
    pub fn setResponse(self: *VsrClientTable, client_id: wire.ClientId, request_no: wire.RequestNumber, response: ops.VsrOpResponse) UpdateError!void {
        if (self.requests_by_client.getPtr(client_id)) |_entry| {
            if (_entry.request_no != request_no) {
                return UpdateError.InvalidRequest;
            }
            _entry.response = response;
        } else {
            return UpdateError.InvalidRequest;
        }
    }

    /// Gets the response sent to client (if any).
    pub fn getResponse(self: *const VsrClientTable, client_id: wire.ClientId, request_no: wire.RequestNumber) ?ops.VsrOpResponse {
        if (self.requests_by_client.get(client_id)) |_entry| {
            if (_entry.request_no == request_no) {
                return _entry.response;
            }
        }
        return null;
    }

    pub fn getEntry(self: *const VsrClientTable, client_id: wire.ClientId) ?Entry {
        return self.requests_by_client.get(client_id);
    }

    pub fn getEntryByOp(self: *const VsrClientTable, op_no: wire.OpNumber) ?Entry {
        if (self.client_by_op.get(op_no)) |_client_id| {
            return self.requests_by_client.get(_client_id);
        }
        return null;
    }
};

// Smoke test VsrClientTable
test "VsrClientTable.smoke" {
    std.debug.print("test.VsrClientTable.smoke\n", .{});
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

    var ulid_generator = dymes_common.ulid.generator();

    var client_table = try VsrClientTable.init(allocs);
    defer client_table.deinit();

    const client_id_0: u64 = 1066;
    const client_id_1: u64 = 3333;
    const client_id_2: u64 = 1212;

    const op_0 = try vsr_testing.createTestAppendOp(allocs, &ulid_generator, 0);
    defer op_0.deinit(allocs);
    const op_1 = try vsr_testing.createTestAppendOp(allocs, &ulid_generator, 1);
    defer op_1.deinit(allocs);
    const op_2 = try vsr_testing.createTestAppendOp(allocs, &ulid_generator, 2);
    defer op_2.deinit(allocs);
    const op_3 = try vsr_testing.createTestAppendOp(allocs, &ulid_generator, 3);
    defer op_3.deinit(allocs);
    const op_4 = try vsr_testing.createTestAppendOp(allocs, &ulid_generator, 4);
    defer op_4.deinit(allocs);
    const op_5 = try vsr_testing.createTestAppendOp(allocs, &ulid_generator, 5);
    defer op_5.deinit(allocs);

    try client_table.addRequest(client_id_0, 0, op_0);
    try testing.expectEqual(null, client_table.getResponse(client_id_0, 0));
    const op_0_rsp: ops.VsrOpResponse = .{ .append = .{} };
    try client_table.setResponse(client_id_0, 0, op_0_rsp);
    try testing.expectEqual(op_0_rsp, client_table.getResponse(client_id_0, 0));
    try testing.expectEqual(op_0_rsp, client_table.getEntryByOp(op_0.opNumber()).?.response);

    try client_table.addRequest(client_id_1, 0, op_1);
    try client_table.addRequest(client_id_1, 1, op_2);
    // attempt adding new request using "last" request number
    try testing.expectError(error.InvalidRequest, client_table.addRequest(client_id_1, 1, op_3));
    // attempt adding new request using old request number
    try testing.expectError(error.InvalidRequest, client_table.addRequest(client_id_1, 0, op_4));

    // attempt setting response to non-existent request
    const op_5_rsp: ops.VsrOpResponse = .{ .append = .{} };
    try testing.expectError(error.InvalidRequest, client_table.setResponse(client_id_2, 0, op_5_rsp));

    // happy path
    try client_table.addRequest(client_id_2, 1234, op_5);
    try client_table.setResponse(client_id_2, 1234, op_5_rsp);
}

/// VSR state.
///
pub const VsrState = struct {
    allocs: Allocators,

    /// The ensemble configuration.
    configuration: VsrConfiguration,

    /// The replica number. An index into the configuration's hostnames array where this replica’s hostname is stored.
    replica_no: wire.ReplicaNumber,

    /// The primary replica number.
    primary_no: wire.ReplicaNumber,

    /// The current view-number, initially 0.
    view_no: wire.ViewNumber = 0,

    /// The op-number assigned to the most recently received request, initially 0.
    op_no: wire.OpNumber = 0,

    /// The commit-number is the op-number of the most recently committed operation.
    commit_no: wire.CommitNumber,

    /// The log. This is an array containing op-number entries.
    /// The entries contain the requests that have been received so far in their assigned order.
    op_log: ops.VsrOpLog,

    /// The client-table.
    /// For each client, it records:
    /// - most recent request number
    /// - the result sent for that request (if the request has been executed)
    client_table: VsrClientTable,

    /// The current status, either normal, view-change, or recovering.
    status: VsrStatus,

    /// Initializes VSR state.
    ///
    /// Caller must call `deinit()` to release resources.
    pub fn init(allocs: Allocators, replica_no: wire.ReplicaNumber, vsr_config: VsrConfiguration) AllocationError!VsrState {
        var op_log = ops.VsrOpLog.init();
        errdefer op_log.deinit(allocs);

        const client_table = try VsrClientTable.init(allocs);
        errdefer client_table.deinit();

        return .{
            .allocs = allocs,
            .configuration = vsr_config,
            .replica_no = replica_no,
            .primary_no = 0, // Default primary is always slot 0
            .view_no = 0,
            .op_no = 0,
            .commit_no = 0,
            .op_log = op_log,
            .client_table = client_table,
            .status = .normal,
        };
    }

    /// De-initializes VSR state, releasing resources.
    pub fn deinit(self: *VsrState) void {
        defer self.op_log.deinit(self.allocs);
        defer self.client_table.deinit();
    }

    pub inline fn isPrimary(self: *const VsrState) bool {
        return self.replica_no == self.primary_no;
    }
};

// Smoke test VsrState
test "VsrState.smoke" {
    std.debug.print("test.VsrState.smoke\n", .{});
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

    var vsr_cfg_bld = VsrConfiguration.Builder.init();
    defer vsr_cfg_bld.deinit(allocs.gpa);
    try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1541);
    try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1542);
    try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", 1543);

    const vsr_cfg = try vsr_cfg_bld.build(allocs.gpa);
    defer vsr_cfg.deinit(allocs.gpa);

    var vsr_state = try VsrState.init(allocs, 0, vsr_cfg);
    defer vsr_state.deinit();
}
