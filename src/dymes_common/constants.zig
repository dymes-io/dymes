//! Dymes constants.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

pub const dymes_version: std.SemanticVersion = .{ .major = 0, .minor = 0, .patch = 0 };

pub const dymes_home_config_key: []const u8 = "dymes.home";
