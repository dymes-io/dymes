//! Dymes VSR constants.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const assert = std.debug.assert;

/// Default interface a Dymes replica listens on.
pub const default_vsr_replica_interface: []const u8 = "0.0.0.0";

/// Default port number a Dymes replica listens on
pub const default_vsr_replica_port: u16 = 1541;

/// Minimum number of failed nodes allowed (𝑓).
pub const min_failure_threshold: u8 = 1;

/// Default number of failed nodes allowed (𝑓).
pub const default_failure_threshold: u8 = min_failure_threshold;

/// Minimum idle timeout (ms) to trigger COMMITs from primary
pub const min_idle_commit_timeout_ms: u32 = 5_000;

/// Default idle timeout (ms) to trigger COMMITs from primary
pub const default_idle_commit_timeout_ms: u32 = 10_000;

/// Minimum idle timeout (ms) to trigger SVCs from replicas
pub const min_idle_scv_timeout_ms: u32 = 15_000;

/// Default idle timeout (ms) to trigger SVCs from replicas
pub const default_idle_scv_timeout_ms: u32 = 30_000;

/// Minimum state machine tick duration (ms)
pub const min_machine_tick_duration: u32 = 1;

/// Default state machine tick duration (ms)
pub const default_machine_tick_duration: u32 = 1000;

// Sanity check the knobs
comptime {
    assert(min_idle_commit_timeout_ms < min_idle_scv_timeout_ms);
    assert(default_idle_commit_timeout_ms >= min_idle_commit_timeout_ms);
    assert(default_idle_scv_timeout_ms >= min_idle_scv_timeout_ms);
    assert(default_machine_tick_duration >= default_machine_tick_duration);
    assert(default_machine_tick_duration < min_idle_commit_timeout_ms);
}
