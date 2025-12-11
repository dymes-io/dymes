//! Dymes Engine constants.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

/// Config file path
///
/// Note that the config file is *optional*, so a missing config file is not an error.
pub const default_config_path: []const u8 = "conf/dymes-local.yaml";

/// Default engine data directory, relative to CWD
pub const default_engine_data_dir_path: []const u8 = "./engine-data";

const time = @import("std").time;

/// Default engine query timeout (30s)
pub const default_query_timeout_ns: u64 = 30 * time.ns_per_s;

/// Default shutdown check interval (10s)
pub const shutdown_check_interval_ns = time.ns_per_s * 10;
