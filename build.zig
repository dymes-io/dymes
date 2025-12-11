//! Dymes build file.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    //------------ Libraries

    //------ 3rd party

    const clap_module = b.dependency("clap", .{
        .target = target,
        .optimize = optimize,
    }).module("clap");
    const yaml_module = b.dependency("zig_yaml", .{
        .target = target,
        .optimize = optimize,
    }).module("yaml");
    const httpz_module = b.dependency("httpz", .{
        .target = target,
        .optimize = optimize,
    }).module("httpz");
    const zeit_module = b.dependency("zeit", .{
        .target = target,
        .optimize = optimize,
    }).module("zeit");
    const zimq_module = b.dependency("zimq", .{
        .target = target,
        .optimize = optimize,
    }).module("zimq");

    //------ Dymes common

    const hissylogz_module = b.dependency("hissylogz", .{
        .target = target,
        .optimize = optimize,
    }).module("hissylogz");

    const dymes_common_module = b.addModule("dymes_common", .{
        .root_source_file = b.path("src/dymes_common.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "hissylogz", .module = hissylogz_module },
            // .{ .name = "lmdb", .module = lmdb_module },
            .{ .name = "yaml", .module = yaml_module },
            .{ .name = "httpz", .module = httpz_module },
            .{ .name = "zeit", .module = zeit_module },
            .{ .name = "zimq", .module = zimq_module },
        },
    });

    //------ Dymes MSG

    const dymes_msg_module = b.addModule("dymes_msg", .{
        .root_source_file = b.path("src/dymes_msg.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "dymes_common", .module = dymes_common_module },
        },
    });

    //------ Dymes MSGSTORE

    const dymes_msg_store_module = b.addModule("dymes_msg_store", .{
        .root_source_file = b.path("src/dymes_msg_store.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "dymes_common", .module = dymes_common_module },
            .{ .name = "dymes_msg", .module = dymes_msg_module },
        },
    });

    //------ Dymes ENGINE

    const dymes_engine_module = b.addModule("dymes_engine", .{
        .root_source_file = b.path("src/dymes_engine.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "dymes_common", .module = dymes_common_module },
            .{ .name = "dymes_msg", .module = dymes_msg_module },
            .{ .name = "dymes_msg_store", .module = dymes_msg_store_module },
        },
    });

    //------ Dymes VSR

    const dymes_vsr_module = b.addModule("dymes_vsr", .{
        .root_source_file = b.path("src/dymes_vsr.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "dymes_common", .module = dymes_common_module },
            .{ .name = "dymes_msg", .module = dymes_msg_module },
            .{ .name = "dymes_msg_store", .module = dymes_msg_store_module },
            .{ .name = "dymes_engine", .module = dymes_engine_module },
            .{ .name = "zimq", .module = zimq_module },
        },
    });

    const dymes_vsr_lib = b.addLibrary(.{
        .linkage = .static,
        .name = "dymes_vsr",
        .root_module = dymes_vsr_module,
    });
    b.installArtifact(dymes_vsr_lib);

    //------ Dymes Client

    const dymes_client_module = b.addModule("dymes_client", .{
        .root_source_file = b.path("src/dymes_client.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "dymes_common", .module = dymes_common_module },
            .{ .name = "dymes_msg", .module = dymes_msg_module },
        },
    });

    const dymes_client_lib = b.addLibrary(.{
        .linkage = .static,
        .name = "dymes_client",
        .root_module = dymes_client_module,
    });
    b.installArtifact(dymes_client_lib);

    //------ Dymes HTTP

    const dymes_http_module = b.addModule("dymes_http", .{
        .root_source_file = b.path("src/dymes_http.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "httpz", .module = httpz_module },
            .{ .name = "zeit", .module = zeit_module },
            .{ .name = "dymes_common", .module = dymes_common_module },
            .{ .name = "dymes_msg", .module = dymes_msg_module },
            .{ .name = "dymes_msg_store", .module = dymes_msg_store_module },
            .{ .name = "dymes_client", .module = dymes_client_module },
            .{ .name = "dymes_engine", .module = dymes_engine_module },
            .{ .name = "dymes_vsr", .module = dymes_vsr_module },
        },
    });

    //------ Dymes WAL

    const dymes_wal_module = b.addModule("dymes_wal", .{
        .root_source_file = b.path("src/dymes_wal.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "dymes_common", .module = dymes_common_module },
            .{ .name = "dymes_msg", .module = dymes_msg_module },
            .{ .name = "dymes_msg_store", .module = dymes_msg_store_module },
        },
    });

    //------------ Executables

    //------ Dymes node daemon

    const dymes_node_module = b.addModule("dymes_node", .{
        .root_source_file = b.path("src/dymes_node.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "dymes_common", .module = dymes_common_module },
            .{ .name = "dymes_msg", .module = dymes_msg_module },
            .{ .name = "dymes_msg_store", .module = dymes_msg_store_module },
            .{ .name = "dymes_engine", .module = dymes_engine_module },
            .{ .name = "dymes_http", .module = dymes_http_module },
            .{ .name = "clap", .module = clap_module },
        },
    });

    const dymes_node_exe = b.addExecutable(.{
        .name = "dymes_node",
        .root_module = dymes_node_module,
    });
    b.installArtifact(dymes_node_exe);

    const run_node_cmd = b.addRunArtifact(dymes_node_exe);
    run_node_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_node_cmd.addArgs(args);
    }
    const run_node_step = b.step("run", "Run Dymes as single node");
    run_node_step.dependOn(&run_node_cmd.step);

    //------ Dymes metrics exporter daemon

    const dymes_dme_module = b.addModule("dymes_dme", .{
        .root_source_file = b.path("src/dymes_dme.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "httpz", .module = httpz_module },
            // .{ .name = "zeit", .module = zeit_module },
            .{ .name = "dymes_common", .module = dymes_common_module },
            .{ .name = "dymes_engine", .module = dymes_engine_module },
            .{ .name = "dymes_http", .module = dymes_http_module },
            .{ .name = "clap", .module = clap_module },
        },
    });

    const dymes_dme_exe = b.addExecutable(.{
        .name = "dymes_dme",
        .root_module = dymes_dme_module,
    });
    b.installArtifact(dymes_dme_exe);

    const run_dme_cmd = b.addRunArtifact(dymes_dme_exe);
    run_dme_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_dme_cmd.addArgs(args);
    }
    const run_dme_step = b.step("run-dme", "Run Dymes metrics exporter");
    run_dme_step.dependOn(&run_dme_cmd.step);

    //------ Dymes WORKSHOP

    const dymes_workshop_module = b.addModule("dymes_workshop", .{
        .root_source_file = b.path("src/dymes_workshop.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "dymes_common", .module = dymes_common_module },
            .{ .name = "dymes_msg", .module = dymes_msg_module },
            .{ .name = "dymes_msg_store", .module = dymes_msg_store_module },
            .{ .name = "dymes_engine", .module = dymes_engine_module },
            .{ .name = "dymes_http", .module = dymes_http_module },
            .{ .name = "dymes_vsr", .module = dymes_vsr_module },
            .{ .name = "dymes_client", .module = dymes_client_module },
            .{ .name = "clap", .module = clap_module },
        },
    });

    const dymes_workshop_exe = b.addExecutable(.{
        .name = "dymes_workshop",
        .root_module = dymes_workshop_module,
    });
    b.installArtifact(dymes_workshop_exe);

    const run_workshop_cmd = b.addRunArtifact(dymes_workshop_exe);
    run_workshop_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_workshop_cmd.addArgs(args);
    }
    const run_workshop_step = b.step("run-workshop", "Run the Dymes workshop");
    run_workshop_step.dependOn(&run_workshop_cmd.step);

    //------ Dymes VSR simulator

    const dymes_vsr_sim_module = b.addModule("dymes_vsr_sim", .{
        .root_source_file = b.path("src/dymes_vsr_sim.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "dymes_common", .module = dymes_common_module },
            .{ .name = "dymes_msg", .module = dymes_msg_module },
            .{ .name = "dymes_msg_store", .module = dymes_msg_store_module },
            .{ .name = "dymes_engine", .module = dymes_engine_module },
            .{ .name = "clap", .module = clap_module },
            .{ .name = "zimq", .module = zimq_module },
            .{ .name = "dymes_vsr", .module = dymes_vsr_module },
        },
    });

    const dymes_vsr_sim_exe = b.addExecutable(.{
        .name = "dymes_vsr_sim",
        .root_module = dymes_vsr_sim_module,
    });
    b.installArtifact(dymes_vsr_sim_exe);

    const run_vsr_sim_cmd = b.addRunArtifact(dymes_vsr_sim_exe);
    run_vsr_sim_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_vsr_sim_cmd.addArgs(args);
    }
    const run_vsr_sim_step = b.step("run-vsr-sim", "Run VSR simulator");
    run_vsr_sim_step.dependOn(&run_vsr_sim_cmd.step);

    //------ Dymes stress

    const dymes_stress_module = b.addModule("dymes_stress", .{
        .root_source_file = b.path("src/dymes_stress.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "dymes_common", .module = dymes_common_module },
            .{ .name = "dymes_client", .module = dymes_client_module },
            .{ .name = "clap", .module = clap_module },
        },
    });
    _ = &dymes_stress_module;

    const dymes_stress_exe = b.addExecutable(.{
        .name = "dymes_stress",
        .root_module = dymes_stress_module,
    });
    b.installArtifact(dymes_stress_exe);

    const run_stress_cmd = b.addRunArtifact(dymes_stress_exe);
    run_stress_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_stress_cmd.addArgs(args);
    }
    const run_stress_step = b.step("stress", "Run the Dymes stress client");
    run_stress_step.dependOn(&run_stress_cmd.step);

    //------------ Dymes DDT

    const dymes_ddt_module = b.addModule("dymes_ddt", .{
        .root_source_file = b.path("src/dymes_ddt.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "dymes_common", .module = dymes_common_module },
            .{ .name = "dymes_msg", .module = dymes_msg_module },
            .{ .name = "dymes_engine", .module = dymes_engine_module },
            .{ .name = "clap", .module = clap_module },
            .{ .name = "zeit", .module = zeit_module },
        },
    });

    const dymes_ddt_exe = b.addExecutable(.{
        .name = "dymes_ddt",
        .root_module = dymes_ddt_module,
    });
    b.installArtifact(dymes_ddt_exe);

    const run_ddt_cmd = b.addRunArtifact(dymes_ddt_exe);
    run_ddt_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_ddt_cmd.addArgs(args);
    }
    const run_ddt_step = b.step("run-ddt", "Run the Dymes Data Dump Tool");
    run_ddt_step.dependOn(&run_ddt_cmd.step);

    //------------ Dymes DDE

    const dymes_dde_module = b.addModule("dymes_dde", .{
        .root_source_file = b.path("src/dymes_dde.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "dymes_common", .module = dymes_common_module },
            .{ .name = "dymes_msg", .module = dymes_msg_module },
            .{ .name = "dymes_engine", .module = dymes_engine_module },
            .{ .name = "clap", .module = clap_module },
            .{ .name = "zeit", .module = zeit_module },
            .{ .name = "dymes_http", .module = dymes_http_module },
            .{ .name = "dymes_client", .module = dymes_client_module },
        },
    });

    const dymes_dde_exe = b.addExecutable(.{
        .name = "dymes_dde",
        .root_module = dymes_dde_module,
    });
    b.installArtifact(dymes_dde_exe);

    const run_dde_cmd = b.addRunArtifact(dymes_dde_exe);
    run_dde_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_dde_cmd.addArgs(args);
    }
    const run_dde_step = b.step("run-dde", "Run the Dymes Data Exporter Tool");
    run_dde_step.dependOn(&run_dde_cmd.step);

    //------------ Dymes DDI

    const dymes_ddi_module = b.addModule("dymes_ddi", .{
        .root_source_file = b.path("src/dymes_ddi.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "dymes_common", .module = dymes_common_module },
            .{ .name = "dymes_msg", .module = dymes_msg_module },
            .{ .name = "dymes_engine", .module = dymes_engine_module },
            .{ .name = "clap", .module = clap_module },
            .{ .name = "zeit", .module = zeit_module },
            .{ .name = "dymes_http", .module = dymes_http_module },
            .{ .name = "dymes_client", .module = dymes_client_module },
        },
    });

    const dymes_ddi_exe = b.addExecutable(.{
        .name = "dymes_ddi",
        .root_module = dymes_ddi_module,
    });
    b.installArtifact(dymes_ddi_exe);

    const run_ddi_cmd = b.addRunArtifact(dymes_ddi_exe);
    run_ddi_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_ddi_cmd.addArgs(args);
    }

    const run_ddi_step = b.step("run-ddi", "Run the Dymes Data Importer Tool");
    run_ddi_step.dependOn(&run_ddi_cmd.step);

    //------------ Unit tests

    //------ Dymes common

    const dymes_common_unit_tests = b.addTest(.{
        .root_module = dymes_common_module,
    });
    const run_dymes_common_unit_tests = b.addRunArtifact(dymes_common_unit_tests);

    const test_dymes_common_step = b.step("test-common", "Run dymes_common unit tests");
    test_dymes_common_step.dependOn(&run_dymes_common_unit_tests.step);

    //------ Dymes MSG

    const dymes_msg_unit_tests = b.addTest(.{
        .root_module = dymes_msg_module,
    });
    const run_dymes_msg_unit_tests = b.addRunArtifact(dymes_msg_unit_tests);

    const test_dymes_msg_step = b.step("test-msg", "Run dymes_msg unit tests");
    test_dymes_msg_step.dependOn(&run_dymes_msg_unit_tests.step);

    //------ Dymes MSGSTORE

    const dymes_msg_store_unit_tests = b.addTest(.{
        .root_module = dymes_msg_store_module,
    });
    const run_dymes_msg_store_unit_tests = b.addRunArtifact(dymes_msg_store_unit_tests);

    const test_dymes_msg_store_step = b.step("test-msg-store", "Run dymes_msg_store unit tests");
    test_dymes_msg_store_step.dependOn(&run_dymes_msg_unit_tests.step);

    //------ Dymes VSR

    const dymes_vsr_unit_tests = b.addTest(.{
        .root_module = dymes_vsr_module,
    });
    const run_dymes_vsr_unit_tests = b.addRunArtifact(dymes_vsr_unit_tests);

    const test_dymes_vsr_step = b.step("test-vsr", "Run dymes_vsr unit tests");
    test_dymes_vsr_step.dependOn(&run_dymes_vsr_unit_tests.step);

    //------ Dymes VSR simulator tests

    const dymes_vsr_sim_tests = b.addTest(.{
        .root_module = dymes_vsr_module,
    });
    const run_dymes_vsr_sim_tests = b.addRunArtifact(dymes_vsr_sim_tests);

    const test_dymes_vsr_sim_step = b.step("test-vsr-sim", "Run dymes_vsr_sim unit tests");
    test_dymes_vsr_sim_step.dependOn(&run_dymes_vsr_sim_tests.step);

    //------ Dymes Client

    const dymes_client_unit_tests = b.addTest(.{
        .root_module = dymes_client_module,
    });
    const run_dymes_client_unit_tests = b.addRunArtifact(dymes_client_unit_tests);

    const test_dymes_client_step = b.step("test-client", "Run dymes_client unit tests");
    test_dymes_client_step.dependOn(&run_dymes_client_unit_tests.step);

    //------ Dymes ENGINE

    const dymes_engine_unit_tests = b.addTest(.{
        .root_module = dymes_engine_module,
    });
    const run_dymes_engine_unit_tests = b.addRunArtifact(dymes_engine_unit_tests);

    const test_dymes_engine_step = b.step("test-engine", "Run dymes_engine unit tests");
    test_dymes_engine_step.dependOn(&run_dymes_engine_unit_tests.step);

    //------ Dymes HTTP

    const dymes_http_unit_tests = b.addTest(.{
        .root_module = dymes_http_module,
    });
    const run_dymes_http_unit_tests = b.addRunArtifact(dymes_http_unit_tests);

    const test_dymes_http_step = b.step("test-http", "Run dymes_http unit tests");
    test_dymes_http_step.dependOn(&run_dymes_http_unit_tests.step);

    //------ Dymes WAL

    const dymes_wal_unit_tests = b.addTest(.{
        .root_module = dymes_wal_module,
    });
    const run_dymes_wal_unit_tests = b.addRunArtifact(dymes_wal_unit_tests);

    _ = &run_dymes_wal_unit_tests; // Don't have demo yet

    //------ Dymes node

    const dymes_node_unit_tests = b.addTest(.{
        .root_module = dymes_node_module,
    });
    const run_dymes_node_unit_tests = b.addRunArtifact(dymes_node_unit_tests);

    const test_dymes_node_step = b.step("test-node", "Run dymes_node unit tests");
    test_dymes_node_step.dependOn(&run_dymes_node_unit_tests.step);

    //------ Dymes DME

    const dymes_dme_unit_tests = b.addTest(.{
        .root_module = dymes_dme_module,
    });
    const run_dymes_dme_unit_tests = b.addRunArtifact(dymes_dme_unit_tests);

    const test_dymes_dme_step = b.step("test-dme", "Run dymes_dme unit tests");
    test_dymes_dme_step.dependOn(&run_dymes_dme_unit_tests.step);

    //------ Dymes workshop

    const dymes_workshop_unit_tests = b.addTest(.{
        .root_module = dymes_workshop_module,
    });
    const run_dymes_workshop_unit_tests = b.addRunArtifact(dymes_workshop_unit_tests);

    //------ Dymes stress

    const dymes_stress_unit_tests = b.addTest(.{
        .root_module = dymes_stress_module,
    });
    const run_dymes_stress_unit_tests = b.addRunArtifact(dymes_stress_unit_tests);

    //------ Dymes DDT

    const dymes_ddt_unit_tests = b.addTest(.{
        .root_module = dymes_ddt_module,
    });
    const run_dymes_ddt_unit_tests = b.addRunArtifact(dymes_ddt_unit_tests);

    const test_dymes_ddt_step = b.step("test-ddt", "Run dymes_ddt unit tests");
    test_dymes_ddt_step.dependOn(&run_dymes_ddt_unit_tests.step);

    //------ Dymes DDE

    const dymes_dde_unit_tests = b.addTest(.{
        .root_module = dymes_dde_module,
    });
    const run_dymes_dde_unit_tests = b.addRunArtifact(dymes_dde_unit_tests);

    const test_dymes_dde_step = b.step("test-dde", "Run dymes_dde unit tests");
    test_dymes_dde_step.dependOn(&run_dymes_dde_unit_tests.step);

    //------ Dymes DDI

    const dymes_ddi_unit_tests = b.addTest(.{
        .root_module = dymes_ddi_module,
    });
    const run_dymes_ddi_unit_tests = b.addRunArtifact(dymes_ddi_unit_tests);

    const test_dymes_ddi_step = b.step("test-ddi", "Run dymes_ddi unit tests");
    test_dymes_ddi_step.dependOn(&run_dymes_ddi_unit_tests.step);

    //------ GLOBAL test

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_dymes_common_unit_tests.step);
    test_step.dependOn(&run_dymes_msg_unit_tests.step);
    test_step.dependOn(&run_dymes_msg_store_unit_tests.step);
    test_step.dependOn(&run_dymes_client_unit_tests.step);
    test_step.dependOn(&run_dymes_http_unit_tests.step);
    test_step.dependOn(&run_dymes_engine_unit_tests.step);
    test_step.dependOn(&run_dymes_vsr_unit_tests.step);
    test_step.dependOn(&run_dymes_vsr_sim_tests.step);
    test_step.dependOn(&run_dymes_node_unit_tests.step);
    test_step.dependOn(&run_dymes_workshop_unit_tests.step);
    test_step.dependOn(&run_dymes_ddt_unit_tests.step);
    test_step.dependOn(&run_dymes_stress_unit_tests.step);
    test_step.dependOn(&run_dymes_wal_unit_tests.step);
    test_step.dependOn(&run_dymes_ddt_unit_tests.step);
    // FIXME - too flaky - test_step.dependOn(&run_dymes_dde_unit_tests.step);
    test_step.dependOn(&run_dymes_ddi_unit_tests.step);
}
