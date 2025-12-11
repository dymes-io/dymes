//! Dymes VSR Simulator.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const assert = std.debug.assert;

const dymes_vsr = @import("dymes_vsr");
const Allocators = dymes_vsr.Allocators;

const LogSequenceNumber = dymes_vsr.LogSequenceNumber;
const OpNumber = dymes_vsr.LogSequenceNumber;
const CommitNumber = dymes_vsr.OpNumber;
const ReplicaNumber = dymes_vsr.ReplicaNumber;
const ViewNumber = dymes_vsr.ViewNumber;
const RequestNumber = dymes_vsr.RequestNumber;
const ClientId = dymes_vsr.ClientId;

const VsrStatus = dymes_vsr.VsrStatus;
const VsrConfiguration = dymes_vsr.VsrConfiguration;
const VsrClientTable = dymes_vsr.VsrClientTable;
const VsrState = dymes_vsr.VsrState;
const VsrClientProxy = dymes_vsr.VsrClientProxy;

const VsrAppendOp = dymes_vsr.VsrAppendOp;
const VsrAppendResponse = dymes_vsr.VsrAppendResponse;
const VsrOpType = dymes_vsr.VsrOpType;
const VsrOp = dymes_vsr.VsrOp;
const VsrOpResponse = dymes_vsr.VsrOpResponse;
const VsrOpLog = dymes_vsr.VsrOpLog;

const VsrMessage = dymes_vsr.VsrMessage;
const VsrRequest = dymes_vsr.VsrRequest;
const VsrPrepare = dymes_vsr.VsrPrepare;
const VsrPrepareOK = dymes_vsr.VsrPrepareOK;
const VsrReply = dymes_vsr.VsrReply;
const VsrCommit = dymes_vsr.VsrCommit;
const VsrStartViewChange = dymes_vsr.VsrStartViewChange;
const VsrDoViewChange = dymes_vsr.VsrDoViewChange;
const VsrStartView = dymes_vsr.VsrStartView;

const ReplicaTransport = dymes_vsr.ReplicaTransport;

const VsrStateMachine = dymes_vsr.VsrStateMachine;

const OpsHandler = dymes_vsr.OpsHandler;
const OpsHandlerError = dymes_vsr.OpsHandlerError;
const OpsHandlerVFT = dymes_vsr.OpsHandlerVFT;

const FutureUlid = dymes_vsr.FutureUlid;

const clap = @import("clap");
const zimq = @import("zimq");

const common = @import("dymes_common");
const Ulid = common.ulid.Ulid;
const config = common.config;
const Config = common.config.Config;

const UsageError = common.errors.UsageError;
const AllocationError = common.errors.AllocationError;
const CreationError = common.errors.CreationError;
const AccessError = common.errors.AccessError;

const logging = common.logging;
const LogLevel = logging.LogLevel;
const LogFormat = logging.LogFormat;

const dymes_engine = @import("dymes_engine");
const Engine = dymes_engine.Engine;

const Message = dymes_engine.Message;
const FrameAllocator = dymes_engine.FrameAllocator;
const constants = dymes_engine.constants;

const HealthProbeListener = common.net.k8s.HealthProbeListener;

const Health = common.health.Health;
const ComponentHealthProvider = common.health.ComponentHealthProvider;
const LifeCycleControl = common.util.LifeCycleControl;

// pub const log_level: std.log.Level = .debug;

pub const std_options = std.Options{ .log_level = .warn, .log_scope_levels = &[_]std.log.ScopeLevel{
    .{ .scope = .kv, .level = .info },
    .{ .scope = .net, .level = .info },
} };

var process_lifecycle: LifeCycleControl = .{};

const SimOptions = struct {
    failure_threshold: u8,
    burst_size: usize,
    tick_idle: u32,

    pub fn fromConfig(dymes_config: Config) !SimOptions {
        const failure_threshold = try dymes_config.asInt("sim.f") orelse return error.IllegalArgument;
        return .{
            .failure_threshold = @truncate(@as(usize, @intCast(failure_threshold))),
            .burst_size = @intCast(try dymes_config.asInt("sim.burst") orelse return error.IllegalArgument),
            .tick_idle = @intCast(try dymes_config.asInt("sim.tick") orelse return error.IllegalArgument),
        };
    }
};

/// Program entry point.
///
pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const semver = common.constants.dymes_version;
    std.debug.print("dymes_vsr_sim v{d}.{d}.{d}", .{ semver.major, semver.minor, semver.patch });
    if (semver.pre) |pre| {
        std.debug.print("-{s}\n", .{pre});
    }
    std.debug.print("\n", .{});

    var sa: std.posix.Sigaction = .{
        .handler = .{ .handler = signalHandler },
        .mask = std.posix.sigemptyset(),
        .flags = 0x0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &sa, null);
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);

    // Run the simulator
    {
        var dymes_config = try buildConfig(allocator);
        defer dymes_config.deinit();

        const out_buffer = try allocator.alloc(u8, 2048);
        defer allocator.free(out_buffer);
        var stderr_writer = std.fs.File.stderr().writer(out_buffer);
        try logging.init(allocator, &stderr_writer);
        defer logging.deinit();
        var logger = logging.logger("vsr.simulator");

        var fah = FrameAllocator.init(allocator);
        const frame_allocator = fah.allocator();

        const allocs: Allocators = .{
            .gpa = allocator,
            .msg_frame_allocator = frame_allocator,
        };

        logger.debug()
            .msg("Dymes VSR Simulator initializing")
            .log();
        defer logger.info()
            .msg("Dymes VSR Simulator terminated")
            .log();

        var health = Health.init(allocator);
        defer health.deinit();

        var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
        defer tmp_dir.cleanup();

        const sim_options = try SimOptions.fromConfig(dymes_config);

        const failure_threshold: u8 = sim_options.failure_threshold;
        const num_replicas: u8 = 2 * failure_threshold + 1;

        var engine_health: []Health = try allocs.gpa.alloc(Health, num_replicas);
        defer allocs.gpa.free(engine_health);
        for (0..num_replicas) |_idx| {
            engine_health[_idx] = Health.init(allocator);
        }
        defer for (0..num_replicas) |_idx| {
            engine_health[_idx].deinit();
        };

        var engine_cfgs: []Config = try allocs.gpa.alloc(Config, num_replicas);
        defer allocs.gpa.free(engine_cfgs);

        for (0..num_replicas) |_idx| {
            const engine_data_path = try std.fmt.allocPrint(allocs.gpa, ".zig-cache/tmp/{s}/vsrengine-{d}", .{ tmp_dir.sub_path, _idx });
            defer allocs.gpa.free(engine_data_path);
            engine_cfgs[_idx] = try buildEngineConfig(allocs, _idx, engine_data_path);
        }
        defer for (0..num_replicas) |_idx| {
            engine_cfgs[_idx].deinit();
        };

        var engine_metrics: []Engine.EngineMetrics = try allocs.gpa.alloc(Engine.EngineMetrics, num_replicas);
        defer allocs.gpa.free(engine_metrics);

        var engines: []*Engine = try allocs.gpa.alloc(*Engine, num_replicas);
        defer allocs.gpa.free(engines);
        for (0..num_replicas) |_replica_no| {
            const replica_engine_cfg = try engine_cfgs[_replica_no].asConfig("engine") orelse return UsageError.OtherUsageFailure;
            engines[_replica_no] = try Engine.init(allocs.gpa, allocs.msg_frame_allocator, &engine_health[_replica_no], &engine_metrics[_replica_no], replica_engine_cfg);
        }
        defer for (engines) |_engine| {
            _engine.deinit();
        };

        {
            // Prepare config

            logger.debug()
                .msg("Preparing simulation configuration")
                .log();
            const vsr_cfg: VsrConfiguration = vsr_cfg_val: {
                var vsr_cfg_bld = VsrConfiguration.Builder.init();
                defer vsr_cfg_bld.deinit(allocs.gpa);

                const replica_port_base: u16 = 41541;
                for (0..num_replicas) |_replica_no| {
                    try vsr_cfg_bld.withReplica(allocs.gpa, "localhost", @as(u16, @intCast(_replica_no)) + replica_port_base);
                }

                try vsr_cfg_bld.withFailureThreshold(failure_threshold);

                break :vsr_cfg_val try vsr_cfg_bld.build(allocs.gpa);
            };
            defer vsr_cfg.deinit(allocs.gpa);

            logger.info()
                .msg("Dymes VSR Simulator initialized")
                .log();

            try runSimulator(allocs, num_replicas, engines[0..], vsr_cfg, sim_options);
            // try runMachineThreads(allocs, num_replicas, engines[0..], vsr_cfg, sim_options);

            defer logger.debug()
                .msg("Dymes VSR Simulator terminating")
                .log();
        }
    }
}

pub fn appendOpHandler(engine_ctx: *anyopaque, append_op: VsrAppendOp) OpsHandlerError!VsrAppendResponse {
    var engine: *Engine = @ptrCast(@alignCast(engine_ctx));
    const msg_to_append = append_op.asMessage();
    const imported_ulid = engine.import(msg_to_append.id(), msg_to_append.channel(), msg_to_append.routing(), msg_to_append.msg_body, .{ .correlation_id = msg_to_append.correlationId() }) catch {
        return OpsHandlerError.InconsistentState;
    };
    assert(imported_ulid == msg_to_append.id());
    return .{ .msg_ulid = imported_ulid };
}

fn buildEngineConfig(allocs: Allocators, replica_no: usize, data_path: []const u8) !Config {
    var cfg_bld = try config.builder("replica-engine", allocs.gpa);
    defer cfg_bld.deinit();
    try cfg_bld.withString("engine.logging.level", @tagName(LogLevel.warn));
    try cfg_bld.withString("engine.logging.format", @tagName(LogFormat.text));
    try cfg_bld.withString("engine.store.data_path", data_path);
    try cfg_bld.withString("engine.http.host", "localhost");
    try cfg_bld.withInt("engine.http.port", 46510 + replica_no);
    return try cfg_bld.build();
}

fn runSimulator(
    allocs: Allocators,
    num_replicas: u8,
    engines: []*Engine,
    vsr_cfg: VsrConfiguration,
    options: SimOptions,
) !void {
    var logger = logging.logger("vsr.simulator");

    var ulid_generator = common.ulid.generator();

    // Prepare ZeroMQ context
    const zimq_context: *zimq.Context = try .init();
    defer zimq_context.deinit();
    try zimq_context.set(.thread_name_prefix, "vsr");
    try zimq_context.set(.io_threads, 8);

    // Prepare state machines

    logger.debug()
        .msg("Preparing state machines")
        .int("num_replicas", num_replicas)
        .log();
    var vsr_state_machines: []VsrStateMachine = allocs.gpa.alloc(VsrStateMachine, num_replicas) catch return AllocationError.OutOfMemory;
    defer allocs.gpa.free(vsr_state_machines);

    for (0..num_replicas) |_replica_no| {
        const engine_ops_handler: OpsHandler = .{
            .ctx = engines[_replica_no],
            .vft = .{
                .append = appendOpHandler,
            },
        };

        const vsr_cfg_cloned = try vsr_cfg.clone(allocs.gpa);
        errdefer vsr_cfg_cloned.deinit(allocs.gpa);
        vsr_state_machines[_replica_no] = try VsrStateMachine.initUsingVsrConfig(
            allocs,
            engine_ops_handler,
            zimq_context,
            1,
            @truncate(_replica_no),
            "localhost",
            vsr_cfg_cloned,
        );
    }
    defer for (0..num_replicas) |_replica_no| {
        vsr_state_machines[_replica_no].deinit();
    };

    const msg_frame_buffer = try allocs.msg_frame_allocator.alloc(u8, 1024);
    defer allocs.msg_frame_allocator.free(msg_frame_buffer);

    logger.info()
        .msg("Kicking the tires..")
        .log();

    // Check initial state
    {
        try testing.expectEqual(true, vsr_state_machines[0].state.isPrimary());
        try testing.expectEqual(0, vsr_state_machines[0].state.primary_no);
        try testing.expectEqual(0, vsr_state_machines[0].state.replica_no);
        try testing.expectEqual(0, vsr_state_machines[0].state.view_no);
        try testing.expectEqual(0, vsr_state_machines[0].state.commit_no);
        try testing.expectEqual(0, vsr_state_machines[0].state.op_no);
        for (1..num_replicas) |_replica_no| {
            try testing.expectEqual(false, vsr_state_machines[_replica_no].state.isPrimary());
            try testing.expectEqual(0, vsr_state_machines[_replica_no].state.primary_no);
            try testing.expectEqual(_replica_no, vsr_state_machines[_replica_no].state.replica_no);
            try testing.expectEqual(0, vsr_state_machines[_replica_no].state.view_no);
            try testing.expectEqual(0, vsr_state_machines[_replica_no].state.commit_no);
            try testing.expectEqual(0, vsr_state_machines[_replica_no].state.op_no);
        }
        logger.debug()
            .msg("Sanity checked")
            .int("primary.view_no", vsr_state_machines[0].state.view_no)
            .int("primary.commit_no", vsr_state_machines[0].state.commit_no)
            .log();
    }

    var client_proxy = try VsrClientProxy.init(allocs, zimq_context, 1, 0, vsr_cfg);
    defer client_proxy.deinit();

    // Tick over state machines and re-check sanity
    _ = try tickMachines(&client_proxy, vsr_state_machines, 1000);
    {
        try testing.expectEqual(true, vsr_state_machines[0].state.isPrimary());
        try testing.expectEqual(0, vsr_state_machines[0].state.view_no);
        try testing.expectEqual(0, vsr_state_machines[0].state.commit_no);
        try testing.expectEqual(0, vsr_state_machines[0].state.op_no);
        for (1..num_replicas) |_replica_no| {
            try testing.expectEqual(false, vsr_state_machines[_replica_no].state.isPrimary());
            try testing.expectEqual(0, vsr_state_machines[_replica_no].state.view_no);
            try testing.expectEqual(0, vsr_state_machines[_replica_no].state.commit_no);
            try testing.expectEqual(0, vsr_state_machines[_replica_no].state.op_no);
        }
        logger.debug()
            .msg("Sanity checked")
            .int("primary.view_no", vsr_state_machines[0].state.view_no)
            .int("primary.commit_no", vsr_state_machines[0].state.commit_no)
            .log();
    }

    // CLIENT REQUEST A

    var sim_request_no: u64 = 0;
    var sim_lsn: u64 = 0;
    const client_id_1: u64 = 1010;
    var future_ulid_A = FutureUlid.empty;
    const msg_id_A = try ulid_generator.next();
    {
        const append_msg = try Message.initOverlay(msg_frame_buffer, sim_lsn, msg_id_A, 0, 0, "Client request A", .{});
        try client_proxy.append(client_id_1, sim_request_no, append_msg, &future_ulid_A);
    }

    // Tick over state machines and re-check sanity
    // We expect PREPAREs to be sent to replicas and them to respond
    // to primary with PREPARE-OK
    _ = try tickMachines(&client_proxy, vsr_state_machines, @as(u32, vsr_cfg.failure_threshold) * 2000);
    {
        try testing.expectEqual(true, vsr_state_machines[0].state.isPrimary());
        try testing.expectEqual(0, vsr_state_machines[0].state.view_no);
        try testing.expectEqual(1, vsr_state_machines[0].state.commit_no);
        try testing.expectEqual(1, vsr_state_machines[0].state.op_no);
        for (1..num_replicas) |_replica_no| {
            try testing.expectEqual(false, vsr_state_machines[_replica_no].state.isPrimary());
            try testing.expectEqual(0, vsr_state_machines[_replica_no].state.view_no);
            try testing.expectEqual(0, vsr_state_machines[_replica_no].state.commit_no);
            try testing.expectEqual(1, vsr_state_machines[_replica_no].state.op_no);
        }
        logger.debug()
            .msg("Sanity checked")
            .int("primary.view_no", vsr_state_machines[0].state.view_no)
            .int("primary.commit_no", vsr_state_machines[0].state.commit_no)
            .log();
    }

    try testing.expectEqual(msg_id_A, future_ulid_A.get(10));

    // CLIENT REQUEST B

    sim_request_no += 1;
    sim_lsn += 1;
    var future_ulid_B = FutureUlid.empty;
    const msg_id_B = try ulid_generator.next();
    {
        const append_msg = try Message.initOverlay(msg_frame_buffer, sim_lsn, msg_id_B, 0, 0, "Client request B", .{});
        try client_proxy.append(client_id_1, sim_request_no, append_msg, &future_ulid_B);
    }

    // Tick over state machines and re-check sanity
    // We expect PREPAREs to be sent to replicas and them to respond
    // to primary with PREPARE-OK
    _ = try tickMachines(&client_proxy, vsr_state_machines, @as(u32, vsr_cfg.failure_threshold) * 2000);
    {
        try testing.expectEqual(true, vsr_state_machines[0].state.isPrimary());
        try testing.expectEqual(0, vsr_state_machines[0].state.view_no);
        try testing.expectEqual(2, vsr_state_machines[0].state.commit_no);
        try testing.expectEqual(2, vsr_state_machines[0].state.op_no);
        for (1..num_replicas) |_replica_no| {
            try testing.expectEqual(false, vsr_state_machines[_replica_no].state.isPrimary());
            try testing.expectEqual(0, vsr_state_machines[_replica_no].state.view_no);
            try testing.expectEqual(1, vsr_state_machines[_replica_no].state.commit_no);
            try testing.expectEqual(2, vsr_state_machines[_replica_no].state.op_no);
        }

        logger.debug()
            .msg("Sanity checked")
            .int("primary.view_no", vsr_state_machines[0].state.view_no)
            .int("primary.commit_no", vsr_state_machines[0].state.commit_no)
            .log();
    }

    try testing.expectEqual(msg_id_B, future_ulid_B.get(10));

    // Simulate a burst of append requests
    const burst_size: usize = options.burst_size;
    logger.info()
        .msg("Sending burst..")
        .int("burst_size", burst_size)
        .log();
    var future_ulids: []FutureUlid = try allocs.gpa.alloc(FutureUlid, burst_size);
    defer allocs.gpa.free(future_ulids);
    for (0..burst_size) |_idx| {
        future_ulids[_idx] = FutureUlid.empty;
    }
    const ms_before_surge = @as(u64, @intCast(std.time.milliTimestamp()));
    const burst_tick_idle: u32 = options.tick_idle;
    var ms_spent_active: u32 = 0;
    for (0..burst_size) |_idx| {
        sim_request_no += 1;
        sim_lsn += 1;
        const client_id: u64 = 5000 + _idx;
        {
            const burst_msg_id = try ulid_generator.next();
            const append_msg = try Message.initOverlay(msg_frame_buffer, sim_lsn, burst_msg_id, 0, 0, "Client burst request", .{});
            try client_proxy.append(client_id, sim_request_no, append_msg, &future_ulids[_idx]);
            ms_spent_active += try tickMachines(&client_proxy, vsr_state_machines, burst_tick_idle);
        }
    }
    const tick_settle_time: u32 = vsr_cfg.idle_commit_timeout_ms + 2_000 * @as(u32, @intCast(vsr_cfg.failure_threshold));
    ms_spent_active += try tickMachines(&client_proxy, vsr_state_machines, tick_settle_time);
    {
        const ms_after_burst: u64 = @as(u64, @intCast(std.time.milliTimestamp()));
        const burst_fsize: f64 = @floatFromInt(burst_size);
        const avg_ms_pop: f64 = @as(f64, @floatFromInt(ms_spent_active)) / burst_fsize;
        const avg_ops_pms: f64 = burst_fsize / @as(f64, @floatFromInt(ms_spent_active));
        const avg_ops_ps: f64 = avg_ops_pms * 1000.0;
        logger.info()
            .msg("Burst processed")
            .int("burst_size", burst_size)
            .int("wall_elapsed_ms", ms_after_burst - ms_before_surge)
            .int("active_elapsed_ms", ms_spent_active)
            .float("avg_ms_pop", avg_ms_pop)
            .float("avg_ops_pms", avg_ops_pms)
            .float("avg_ops_ps", avg_ops_ps)
            .log();
    }

    logger.info()
        .msg("Letting the dust settle..")
        .log();

    std.Thread.sleep(std.time.ns_per_ms * 5000);

    _ = try tickMachines(&client_proxy, vsr_state_machines, vsr_cfg.idle_commit_timeout_ms + 2_000 * @as(u32, @intCast(vsr_cfg.failure_threshold)));
    // _ = try tickMachines(&client_proxy, vsr_state_machines, vsr_cfg.idle_scv_timeout_ms + 2_000 * @as(u32, @intCast(vsr_cfg.failure_threshold)));
    {
        try testing.expectEqual(true, vsr_state_machines[0].state.isPrimary());
        try testing.expectEqual(0, vsr_state_machines[0].state.view_no);
        try testing.expectEqual(burst_size + 2, vsr_state_machines[0].state.commit_no);
        try testing.expectEqual(burst_size + 2, vsr_state_machines[0].state.op_no);
        for (1..num_replicas) |_replica_no| {
            try testing.expectEqual(false, vsr_state_machines[_replica_no].state.isPrimary());
            try testing.expectEqual(0, vsr_state_machines[_replica_no].state.view_no);
            try testing.expectEqual(burst_size + 2, vsr_state_machines[_replica_no].state.commit_no);
            try testing.expectEqual(burst_size + 2, vsr_state_machines[_replica_no].state.op_no);
        }

        logger.debug()
            .msg("Sanity checked")
            .int("machine.0.view_no", vsr_state_machines[0].state.view_no)
            .int("machine.0.commit_no", vsr_state_machines[0].state.commit_no)
            .int("machine.1.view_no", vsr_state_machines[1].state.view_no)
            .int("machine.1.commit_no", vsr_state_machines[1].state.commit_no)
            .int("machine.1.view_no", vsr_state_machines[1].state.view_no)
            .int("machine.1.commit_no", vsr_state_machines[1].state.commit_no)
            .log();
    }

    logger.info()
        .msg("Dust should have settled")
        .log();

    for (future_ulids) |*_future_ulid| {
        _ = try _future_ulid.get(0);
    }
}

fn tickMachines(vsr_client_proxy: *VsrClientProxy, vsr_state_machines: []VsrStateMachine, idle_timeout: u32) !u32 {
    var total_spent_active_ms: u32 = 0;
    var spent_idle_ms: i64 = 0;
    while (spent_idle_ms < idle_timeout) {
        var inner_spent_active_ms: i64 = 0;
        const ms_before_all = std.time.milliTimestamp();
        for (vsr_state_machines) |*_state_machine| {
            const ms_before_tick = std.time.milliTimestamp();
            const machine_processed_something = _state_machine.tick() catch |_e| {
                if (_e == error.Interrupted) {
                    process_lifecycle.shutdown();
                    return total_spent_active_ms;
                }
                return _e;
            };
            const ms_after_tick = std.time.milliTimestamp();
            if (machine_processed_something) {
                total_spent_active_ms += @as(u32, @intCast(ms_after_tick - ms_before_tick));
                inner_spent_active_ms += ms_after_tick - ms_before_tick;
            }
        }

        {
            const ms_before_tick = std.time.milliTimestamp();
            const client_processed_something = vsr_client_proxy.tick() catch |_e| {
                if (_e == error.Interrupted) {
                    process_lifecycle.shutdown();
                    return total_spent_active_ms;
                }
                return _e;
            };
            const ms_after_tick = std.time.milliTimestamp();
            if (client_processed_something) {
                total_spent_active_ms += @as(u32, @intCast(ms_after_tick - ms_before_tick));
                inner_spent_active_ms += ms_after_tick - ms_before_tick;
            }
        }
        const ms_after_all = std.time.milliTimestamp();

        spent_idle_ms += ms_after_all - ms_before_all - inner_spent_active_ms;
    }
    return total_spent_active_ms;
}

fn runMachineThreads(
    allocs: Allocators,
    num_replicas: u8,
    engines: []*Engine,
    vsr_cfg: VsrConfiguration,
    options: SimOptions,
) !void {
    var logger = logging.logger("vsr.simulator");

    var ulid_generator = common.ulid.generator();

    // Prepare ZeroMQ context
    const zimq_context: *zimq.Context = try .init();
    defer zimq_context.deinit();
    try zimq_context.set(.thread_name_prefix, "vsr");
    // try zimq_context.set(.io_threads, 8);
    var num_io_threads: c_int = undefined;
    try zimq_context.get(.io_threads, &num_io_threads);

    // Prepare state machines

    logger.debug()
        .msg("Preparing state machines")
        .int("num_replicas", num_replicas)
        .int("num_io_threads", num_io_threads)
        .log();
    var vsr_state_machines: []VsrStateMachine = allocs.gpa.alloc(VsrStateMachine, num_replicas) catch return AllocationError.OutOfMemory;
    defer allocs.gpa.free(vsr_state_machines);

    for (0..num_replicas) |_replica_no| {
        const engine_ops_handler: OpsHandler = .{
            .ctx = engines[_replica_no],
            .vft = .{
                .append = appendOpHandler,
            },
        };

        const vsr_cfg_cloned = try vsr_cfg.clone(allocs.gpa);
        errdefer vsr_cfg_cloned.deinit(allocs.gpa);
        vsr_state_machines[_replica_no] = try VsrStateMachine.initUsingVsrConfig(
            allocs,
            engine_ops_handler,
            zimq_context,
            1000,
            @truncate(_replica_no),
            "localhost",
            vsr_cfg_cloned,
        );
    }
    defer for (0..num_replicas) |_replica_no| {
        vsr_state_machines[_replica_no].deinit();
    };

    const msg_frame_buffer = try allocs.msg_frame_allocator.alloc(u8, 1024);
    defer allocs.msg_frame_allocator.free(msg_frame_buffer);

    var client_proxy = try VsrClientProxy.init(allocs, zimq_context, 1000, 0, vsr_cfg);
    defer client_proxy.deinit();

    var ticker_threads: []std.Thread = try allocs.gpa.alloc(std.Thread, vsr_state_machines.len);
    defer allocs.gpa.free(ticker_threads);

    for (0..vsr_state_machines.len) |_idx| {
        ticker_threads[_idx] = try std.Thread.spawn(.{}, tickMachine, .{&vsr_state_machines[_idx]});
    }

    {
        var client_thread: std.Thread = try std.Thread.spawn(.{}, tickClient, .{&client_proxy});
        defer client_thread.join();

        defer for (0..vsr_state_machines.len) |_idx| {
            ticker_threads[_idx].join();
        };

        // Simulate a burst of append requests
        const burst_size: usize = options.burst_size;
        logger.info()
            .msg("Sending burst..")
            .int("burst_size", burst_size)
            .log();

        var future_ulids: []*FutureUlid = try allocs.gpa.alloc(*FutureUlid, burst_size);
        defer allocs.gpa.free(future_ulids);
        for (0..burst_size) |_idx| {
            future_ulids[_idx] = try FutureUlid.init(allocs.gpa);
        }
        defer for (future_ulids) |_future_ulid| {
            _future_ulid.deinit(allocs.gpa);
        };

        var sim_request_no: u64 = 0;
        var sim_lsn: u64 = 0;

        for (sim_request_no + 1..burst_size) |_idx| {
            sim_request_no += 1;
            sim_lsn += 1;
            const client_id: u64 = 5000 + _idx;
            {
                const burst_msg_id = try ulid_generator.next();
                const append_msg = try Message.initOverlay(msg_frame_buffer, sim_lsn, burst_msg_id, 0, 0, "Client burst request", .{});
                try client_proxy.append(client_id, sim_request_no, append_msg, future_ulids[_idx]);
            }
        }

        while (!process_lifecycle.mustShutdown()) {
            std.Thread.sleep(100 * std.time.ns_per_ms);
        }
    }

    logger.info()
        .msg("Letting the dust settle..")
        .log();
}

fn tickClient(vsr_client_proxy: *VsrClientProxy) !void {
    while (!process_lifecycle.mustShutdown()) {
        _ = try vsr_client_proxy.tick();
    }
}

fn tickMachine(vsr_state_machine: *VsrStateMachine) !void {
    while (!process_lifecycle.mustShutdown()) {
        _ = try vsr_state_machine.tick();
    }
}

fn runMachines(gpa: Allocator, vsr_state_machines: []VsrStateMachine) !void {
    var ticker_threads: []std.Thread = try gpa.alloc(std.Thread, vsr_state_machines.len);
    defer gpa.free(ticker_threads);

    for (0..vsr_state_machines.len) |_idx| {
        ticker_threads[_idx] = try std.Thread.spawn(.{}, tickMachine, .{&vsr_state_machines[_idx]});
    }

    while (!process_lifecycle.mustShutdown()) {
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }

    for (0..vsr_state_machines.len) |_idx| {
        ticker_threads[_idx].join();
    }
}

fn createVsrRequest(
    allocs: Allocators,
    ulid_generator: *common.ulid.Generator,
    client_id: ClientId,
    request_no: RequestNumber,
    log_sequence_no: LogSequenceNumber,
    payload: []const u8,
) !VsrRequest {
    const vsr_op = try createTestAppendOp(allocs, ulid_generator, log_sequence_no, payload);
    defer vsr_op.deinit(allocs);

    return try VsrRequest.init(allocs, client_id, request_no, vsr_op);
}

pub fn createTestAppendOp(
    allocs: Allocators,
    ulid_generator: *common.ulid.Generator,
    log_sequence_no: LogSequenceNumber,
    payload: []const u8,
) !VsrOp {
    const msg_to_append_id = try ulid_generator.next();

    const msg_to_append = try Message.init(
        allocs.msg_frame_allocator,
        log_sequence_no,
        msg_to_append_id,
        702,
        0,
        payload,
        .{},
    );
    defer msg_to_append.deinit(allocs.msg_frame_allocator);

    return .{ .append = try VsrAppendOp.init(allocs.msg_frame_allocator, msg_to_append) };
}

/// Signal handler
///
/// On SIGINT or SIGTERM, it triggers shutdown via lifecycle control
fn signalHandler(sig: i32) callconv(.c) void {
    switch (sig) {
        std.posix.SIG.INT, std.posix.SIG.TERM => process_lifecycle.shutdown(),
        else => {},
    }
}

/// Builds engine configuration.
///
/// The caller owns the returned config
fn buildConfig(allocator: std.mem.Allocator) !Config {

    // Parse command line
    const params = comptime clap.parseParamsComptime(
        \\-h, --help             Display this help and exit.
        \\-c, --config <str>     Configuration file path.
        \\-l, --loglevel <str>   Logging filter level. One of ("fine", "debug", "info", "warn", "err", "fatal")
        \\-r, --logformat <str>  Logging format. One of ("json", "text")
        \\-f, --failures <u8>    Failure threshold
        \\-b, --burst <usize>    Number of messages in burst
        \\-t, --tick <i64>       Tick idle timeout
        \\
    );
    var diag = clap.Diagnostic{};
    var res = clap.parse(clap.Help, &params, clap.parsers.default, .{
        .diagnostic = &diag,
        .allocator = allocator,
    }) catch |err| {
        // Report useful error and exit
        {
            var stderr_buffer: [1024]u8 = undefined;
            var stderr_writer = std.fs.File.stdout().writer(&stderr_buffer);
            const stderr = &stderr_writer.interface;
            diag.report(stderr, err) catch {};
            stderr.flush() catch {};
            return err;
        }
    };
    defer res.deinit();
    if (res.args.help != 0) {
        var stderr_buffer: [1024]u8 = undefined;
        var stderr_writer = std.fs.File.stdout().writer(&stderr_buffer);
        const stderr = &stderr_writer.interface;
        try clap.help(stderr, clap.Help, &params, .{});
        stderr.flush() catch {};
        std.posix.exit(0);
    }

    // Build base dymes configuration
    const config_path = res.args.config orelse "conf/dymes-vsr-sim.yaml";
    var base_config = try buildBaseConfig(allocator, config_path);
    defer base_config.deinit();

    // Prepare logging
    var desired_logging_level: LogLevel = .info;
    if (try base_config.asString("logging.level")) |logging_level_name| {
        desired_logging_level = try parseLoggingLevelName(logging_level_name);
    }
    if (res.args.loglevel) |override_level| {
        desired_logging_level = try parseLoggingLevelName(override_level);
    }
    logging.default_logging_filter_level = desired_logging_level;

    var desired_log_format: LogFormat = .text;
    if (try base_config.asString("logging.format")) |logging_format_name| {
        desired_log_format = try parseLoggingFormatName(logging_format_name);
    }
    // Command line overrides
    if (res.args.logformat) |logging_format_name| {
        desired_log_format = try parseLoggingFormatName(logging_format_name);
    }
    logging.default_logging_format = desired_log_format;

    // Build effective Dymes config
    var dymes_config_builder = try config.builder("dymes", allocator);
    if (try base_config.asConfig("engine")) |base_engine_config| {
        try dymes_config_builder.merge(base_engine_config);
    }
    if (try base_config.asConfig("vsr")) |base_vsr_config| {
        try dymes_config_builder.merge(base_vsr_config);
    }
    try dymes_config_builder.withString("logging.format", @tagName(desired_log_format));
    try dymes_config_builder.withString("logging.level", @tagName(desired_logging_level));

    const default_failure_threshold: u8 = if (try base_config.asInt("sim.f")) |_failures| @truncate(@as(usize, @intCast(_failures))) else 1;
    const failure_threshold: u8 = if (res.args.failures) |_failures| _failures else default_failure_threshold;

    try dymes_config_builder.withInt("sim.f", failure_threshold);

    const default_burst_size: usize = if (try base_config.asInt("sim.burst")) |_burst| @intCast(_burst) else 1_000;
    const burst_size: usize = if (res.args.burst) |_burst_size| _burst_size else default_burst_size;

    try dymes_config_builder.withInt("sim.burst", burst_size);

    const default_tick_idle: u64 = if (try base_config.asInt("sim.tick")) |_tick_idle| @intCast(_tick_idle) else 2;
    const burst_tick_idle: u64 = if (res.args.tick) |_tick_idle| @intCast(_tick_idle) else default_tick_idle;

    try dymes_config_builder.withInt("sim.tick", burst_tick_idle);

    return dymes_config_builder.build();
}

fn parseLoggingLevelName(logging_level_name: []const u8) UsageError!LogLevel {
    if (std.mem.eql(u8, @tagName(LogLevel.fine), logging_level_name)) {
        return .fine;
    } else if (std.mem.eql(u8, @tagName(LogLevel.debug), logging_level_name)) {
        return .debug;
    } else if (std.mem.eql(u8, @tagName(LogLevel.info), logging_level_name)) {
        return .info;
    } else if (std.mem.eql(u8, @tagName(LogLevel.warn), logging_level_name)) {
        return .warn;
    } else if (std.mem.eql(u8, @tagName(LogLevel.err), logging_level_name)) {
        return .err;
    } else if (std.mem.eql(u8, @tagName(LogLevel.fatal), logging_level_name)) {
        return .fatal;
    }

    return UsageError.IllegalArgument;
}

fn parseLoggingFormatName(logging_format_name: []const u8) UsageError!LogFormat {
    if (std.mem.eql(u8, "json", logging_format_name)) {
        return .json;
    } else if (std.mem.eql(u8, "text", logging_format_name)) {
        return .text;
    }
    return UsageError.IllegalArgument;
}

const ConfigError = UsageError || CreationError || AccessError;

/// Build base configuration
///
/// First the Dymes YAML config is merged, then environment variable overrides are applied.
fn buildBaseConfig(allocator: std.mem.Allocator, config_path: []const u8) ConfigError!Config {
    var config_bld = try config.builder("dymes_base", allocator);
    defer config_bld.deinit();
    {
        var yaml_cfg: ?Config = try buildYamlConfig(allocator, config_path);
        if (yaml_cfg) |*yaml| {
            defer yaml.deinit();
            try config_bld.merge(yaml.*);
        }

        var env_cfg = try config.fromEnvironment(allocator);
        defer env_cfg.deinit();

        try config_bld.merge(env_cfg);
    }
    return try config_bld.build();
}

/// Build config from Dymes YAML configuration
fn buildYamlConfig(allocator: std.mem.Allocator, config_path: []const u8) ConfigError!?Config {
    var yaml_cfg = config.fromYamlPath(std.fs.cwd(), config_path, allocator) catch |e| {
        if (e == config.Error.FileNotFound) {
            return null;
        }
        return e;
    };
    _ = &yaml_cfg;
    return yaml_cfg;
}

test "dymes_vsr_sim dependencies" {
    std.testing.refAllDeclsRecursive(@This());
}
