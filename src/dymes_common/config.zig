//! Configuration support.
//!
//! TODO:
//! - FIX: Merging works when value types match, bad things happen otherwise
//! - COULD: Config support for with/as array/list of integers
//! - COULD: Config support for with/as array/list of floating point numbers
//! - COULD: load from toml
//! - COULD: load from json
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const constants = @import("constants.zig");
const limits = @import("limits.zig");

const YamlError = @import("yaml").Yaml.YamlError;
const Yaml = @import("yaml").Yaml;
const YamlValue = @import("yaml").Value;
const YamlMap = @import("yaml").Yaml.Map;

const logger = std.log.scoped(.dynaconfig);
const test_debug_printing = false;

pub const UsageError = @import("errors.zig").UsageError;
pub const CreationError = @import("errors.zig").CreationError;
pub const AccessError = @import("errors.zig").AccessError;

pub const LoadError = AccessError || CreationError;

pub const Error = UsageError || CreationError || AccessError;

/// Configuration value tag
const ValueTag = enum {
    config_value,
    bool_value,
    int_value,
    float_value,
    string_value,
    string_list_value,
};

const StringList = std.array_list.Managed([]const u8);

/// Configuration value
const Value = union(ValueTag) {
    const Self = @This();

    config_value: *Config,
    bool_value: bool,
    int_value: i128,
    float_value: f128,
    string_value: []const u8,
    string_list_value: StringList,

    /// Returns true if the configuration value is another configuration
    pub fn isConfig(self: Self) bool {
        return switch (self) {
            ValueTag.config_value => true,
            else => false,
        };
    }

    /// Returns configuration value as a configuration
    pub fn asConfig(self: Self) UsageError!*Config {
        if (self.isConfig()) {
            return self.config_value;
        } else {
            logger.err("Value is not a configuration: [{any}].", .{self});
            return UsageError.IllegalConversion;
        }
    }

    /// Returns an optional string with the given name.
    pub fn asString(self: Self) UsageError![]const u8 {
        return switch (self) {
            .string_value => |str| str,
            else => return UsageError.IllegalConversion,
        };
    }

    /// Returns configuration value as a boolean.
    pub fn asBool(self: Self) UsageError!bool {
        return switch (self) {
            .bool_value => |b| b,
            .int_value => |i| i != 0,
            .float_value => |f| f != 0.0,
            .string_value => |str| std.mem.eql(u8, "true", str) or std.mem.eql(u8, "yes", str),
            else => return UsageError.IllegalConversion,
        };
    }

    /// Returns configuration value as an integer.
    pub fn asInt(self: Self) UsageError!i128 {
        return switch (self) {
            .int_value => |i| i,
            .bool_value => |b| @intFromBool(b),
            .float_value => |f| @intFromFloat(f),
            .string_value => |str| std.fmt.parseInt(i128, str, 10) catch UsageError.IllegalConversion,
            else => UsageError.IllegalConversion,
        };
    }

    /// Returns configuration value as a floating point number.
    pub fn asFloat(self: Self) UsageError!f128 {
        return switch (self) {
            .float_value => |f| f,
            .int_value => |i| @floatFromInt(i),
            .bool_value => |b| if (b) 1.0 else 0.0,
            .string_value => |str| std.fmt.parseFloat(f128, str) catch UsageError.IllegalConversion,
            else => UsageError.IllegalConversion,
        };
    }

    /// Returns configuration value as a string list.
    ///
    pub fn asStringList(self: Self) UsageError!StringList {
        return switch (self) {
            .string_list_value => |lst| lst,
            else => return UsageError.IllegalConversion,
        };
    }

    /// Formats the value and prints to the given writer.
    pub fn format(self: *const Self, writer: *std.Io.Writer) !void {
        switch (self.*) {
            .float_value => |f| try writer.print("{any}", .{f}),
            .int_value => |i| try writer.print("{d}", .{i}),
            .bool_value => |b| try writer.print("{s}", .{if (b) "true" else "false"}),
            .string_value => |str| try writer.print("\"{s}\"", .{str}),
            .config_value => |cfg| try writer.print("{f}", .{cfg}),
            .string_list_value => |lst| {
                try writer.print("{{", .{});
                for (lst.items) |lst_entry| {
                    try writer.print("\"{s}\",", .{lst_entry});
                }
                try writer.print("}}", .{});
            },
        }
    }
};

/// Dynamic configuration
pub const Config = struct {
    const Self = @This();
    const ConfigMap = std.array_hash_map.StringArrayHashMap(Value);
    const Entry = struct {
        key: []const u8,
        value: Value,
    };

    config_name: []const u8,
    config_map: ConfigMap,
    parent_config: ?*const Config,
    allocator: std.mem.Allocator,

    /// Initializes the configuration with a name and allocator.
    ///
    /// The caller gains ownership of the return value (deinit() needs to be called)
    pub fn init(config_name: []const u8, allocator: std.mem.Allocator) CreationError!Self {
        const owned_name = allocator.dupe(u8, config_name) catch return CreationError.OutOfMemory;
        return .{
            .config_name = owned_name,
            .config_map = ConfigMap.init(allocator),
            .parent_config = null,
            .allocator = allocator,
        };
    }

    /// Allocates and initializes the configuration with a name and allocator.
    ///
    /// The caller gains ownership of the return value (deinit() needs to be called as well as the pointer destroyed).
    pub fn initAlloc(config_name: []const u8, allocator: std.mem.Allocator, parent: ?*const Self) CreationError!*Self {
        const owned_name = allocator.dupe(u8, config_name) catch return CreationError.OutOfMemory;
        const config_ptr = allocator.create(Config) catch return CreationError.OutOfMemory;
        config_ptr.*.config_name = owned_name;
        config_ptr.*.config_map = ConfigMap.init(allocator);
        config_ptr.*.allocator = allocator;
        if (parent) |parent_ptr| {
            config_ptr.*.parent_config = parent_ptr;
        }
        return config_ptr;
    }

    /// De-initializes the config, and releases resources held by child configurations.
    pub fn deinit(self: *Self) void {
        errdefer self.config_map.deinit();
        var it = self.config_map.iterator();
        while (it.next()) |entry| {
            switch (entry.value_ptr.*) {
                .config_value => |cfg| {
                    cfg.deinit();
                    self.allocator.destroy(cfg);
                },
                .string_value => |str| {
                    self.allocator.free(str);
                },
                .string_list_value => |lst| {
                    for (lst.items) |lst_entry| {
                        self.allocator.free(lst_entry);
                    }
                    lst.deinit();
                },
                else => {},
            }
            self.allocator.free(entry.key_ptr.*);
        }
        self.allocator.free(self.config_name);
        self.config_map.deinit();
        self.* = undefined;
    }

    /// Returns the configuration name.
    pub fn name(self: *const Self) []const u8 {
        return self.config_name;
    }

    /// Returns an optional value with the given name.
    pub fn value(self: *const Self, value_name: []const u8) ?Value {
        if (std.mem.indexOfScalar(u8, value_name, '.')) |idx| {
            const base_name = value_name[0..idx];
            if (self.value(base_name)) |subconfig| {
                if (subconfig.isConfig()) {
                    return subconfig.config_value.value(value_name[idx + 1 ..]);
                } else {
                    logger.err("Looking up [{s}] in [{s}] failed, non-config nodes encountered.", .{ value_name, self.config_name });
                }
            }
            return null;
        }
        return self.config_map.get(value_name) orelse return null;
    }

    /// Returns an optional configuration with the given name.
    ///
    /// Note: The caller *DOES NOT* gain ownership of the returned value (deinit() MUST NOT be called).
    pub fn asConfig(self: *const Self, value_name: []const u8) UsageError!?Config {
        if (self.value(value_name)) |val| {
            var config_val = try val.asConfig();
            _ = &config_val;
            return config_val.*;
        }
        return null;
    }

    /// Returns an optional string with the given name.
    pub fn asString(self: *const Self, value_name: []const u8) UsageError!?[]const u8 {
        if (self.value(value_name)) |val| {
            return try val.asString();
        }
        return null;
    }

    /// Returns an optional boolean with the given name.
    pub fn asBool(self: *const Self, value_name: []const u8) UsageError!?bool {
        if (self.value(value_name)) |val| {
            return try val.asBool();
        }
        return null;
    }

    /// Returns an optional integer with the given name.
    pub fn asInt(self: *const Self, value_name: []const u8) UsageError!?i128 {
        if (self.value(value_name)) |val| {
            return try val.asInt();
        }
        return null;
    }

    /// Returns an optional floating point number with the given name.
    pub fn asFloat(self: *const Self, value_name: []const u8) UsageError!?f128 {
        if (self.value(value_name)) |val| {
            return try val.asFloat();
        }
        return null;
    }

    /// Returns an optional string list with the given name.
    pub fn asStringList(self: *const Self, value_name: []const u8) UsageError!?StringList {
        if (self.value(value_name)) |val| {
            return try val.asStringList();
        }
        return null;
    }

    /// Formats the configuration and prints to the given writer.
    pub fn format(self: *const Self, writer: *std.Io.Writer) !void {
        try writer.print("{{config_name=\"{s}\",config_map={{", .{self.config_name});
        for (self.config_map.keys()) |key_name| {
            if (self.config_map.get(key_name)) |val| {
                try writer.print("{s}={any},", .{ key_name, val });
            }
        }
        try writer.print("}}", .{});
    }

    /// Returns a configuration iterator yielding `Config.Entry` values.
    pub fn iterator(self: *const Self) Iterator {
        return .{
            .iterator = self.config_map.iterator(),
        };
    }

    /// A configuration iterator yielding `Config.Entry` values.
    /// ImplNote: Delegates the grunt work to `std.array_hash_map.StringArrayHashMap(Value).Iterator`
    pub const Iterator = struct {
        iterator: ConfigMap.Iterator,

        pub fn next(it: *Iterator) ?Entry {
            if (it.iterator.next()) |inner_entry| {
                return .{ .key = inner_entry.key_ptr.*, .value = inner_entry.value_ptr.* };
            }
            return null;
        }

        pub fn reset(it: *Iterator) void {
            it.iterator.reset();
        }
    };

    /// (internal) Sets the parent config node
    /// All `Config`s are actually mutable, this is a private back-door for builders
    fn withParent(self: *Self, parent_config: *const Config) void {
        self.config.withParent(parent_config);
    }

    /// (internal) Adds an entry to the config node.
    /// All `Config`s are actually mutable, this is a private back-door for builders
    fn withEntry(self: *Self, entry_name: []const u8, entry_value: Value) CreationError!void {
        if (std.mem.indexOfScalar(u8, entry_name, '.')) |idx| {
            // Dotted name
            const base_name = self.allocator.dupe(u8, entry_name[0..idx]) catch return CreationError.OutOfMemory;
            errdefer self.allocator.free(base_name);
            const following_name = entry_name[idx + 1 ..];

            if (self.value(base_name)) |existing| {
                if (existing.isConfig()) {
                    // Modify existing sub-config
                    if (self.asMutableConfig(base_name) catch return CreationError.OtherCreationFailure) |subconfig| {
                        defer self.allocator.free(base_name);
                        // Add entry to existing sub-config
                        try subconfig.withEntry(following_name, entry_value);
                        return;
                    }
                }
                logger.debug("Replacing existing non-configuration entry [{s}]", .{base_name});
                switch (existing) {
                    .string_value => |str| self.allocator.free(str),
                    .string_list_value => |lst| {
                        for (lst.items) |lst_entry| {
                            self.allocator.free(lst_entry);
                        }
                        lst.deinit();
                    },
                    else => {},
                }
            }

            // Create new sub-config
            var new_subconfig = try initAlloc(base_name, self.allocator, self);
            errdefer new_subconfig.deinit();
            errdefer self.allocator.destroy(new_subconfig);

            // Add entry in new sub-config
            try new_subconfig.withEntry(following_name, entry_value);
            const sub_config_value: Value = .{ .config_value = new_subconfig };
            self.config_map.put(base_name, sub_config_value) catch |err| {
                logger.err("Failed to assign sub-config key [{s}], value [{any}]: {any}", .{ base_name, sub_config_value, err });
                return Error.OutOfMemory;
            };
        } else {
            // Non-dotted name
            const base_name = self.allocator.dupe(u8, entry_name) catch return CreationError.OutOfMemory;
            errdefer self.allocator.free(base_name);
            const assigned_value = switch (entry_value) {
                .string_value => |str| blk: {
                    break :blk Value{ .string_value = self.allocator.dupe(u8, str) catch return CreationError.OutOfMemory };
                },
                .string_list_value => |lst| blk: {
                    var owned_lst = StringList.init(self.allocator);
                    owned_lst.ensureTotalCapacity(lst.capacity) catch return CreationError.OutOfMemory;
                    errdefer owned_lst.deinit();

                    for (lst.items) |lst_entry| {
                        const owned_entry = self.allocator.dupe(u8, lst_entry) catch return CreationError.OutOfMemory;
                        errdefer self.allocator.free(owned_entry);
                        owned_lst.append(owned_entry) catch return CreationError.OutOfMemory;
                    }
                    break :blk Value{ .string_list_value = owned_lst };
                },
                else => entry_value,
            };
            errdefer if (assigned_value == .string_value) self.allocator.free(assigned_value.string_value);
            errdefer if (assigned_value == .string_list_value) for (assigned_value.string_list_value.items) |lst_item| self.allocator.free(lst_item);
            errdefer if (assigned_value == .string_list_value) assigned_value.string_list_value.deinit();

            // Replace existing entry, if any
            const old_entry = self.config_map.fetchPut(base_name, assigned_value) catch |err| {
                logger.err("Failed to assign key [{s}], value [{any}]: {any}", .{ base_name, assigned_value, err });
                return CreationError.OtherCreationFailure;
            };
            if (old_entry) |existing| {
                // Release resources of old entry
                defer self.allocator.free(base_name);
                switch (existing.value) {
                    .config_value => |cfg| {
                        cfg.deinit();
                        self.allocator.destroy(cfg);
                    },
                    .string_value => |str| {
                        self.allocator.free(str);
                    },
                    .string_list_value => |lst| {
                        for (lst.items) |lst_entry| {
                            self.allocator.free(lst_entry);
                        }
                        lst.deinit();
                    },
                    else => {},
                }
            } else {}
        }
    }

    /// (internal) Adds a boolean entry to the config node.
    /// All `Config`s are actually mutable, this is a private back-door for builders
    pub fn withBool(self: *Self, entry_name: []const u8, entry_value: bool) CreationError!void {
        try self.withEntry(entry_name, .{ .bool_value = entry_value });
    }

    /// (internal) Adds an integer entry to the config node.
    /// All `Config`s are actually mutable, this is a private back-door for builders
    pub fn withInt(self: *Self, entry_name: []const u8, entry_value: comptime_int) CreationError!void {
        try self.withEntry(entry_name, .{ .int_value = entry_value });
    }

    /// (internal) Adds a floating point entry to the config node.
    /// All `Config`s are actually mutable, this is a private back-door for builders
    pub fn withFloat(self: *Self, entry_name: []const u8, entry_value: comptime_float) CreationError!void {
        try self.withEntry(entry_name, .{ .float_value = entry_value });
    }

    /// (internal) Adds a string entry to the config node.
    /// All `Config`s are actually mutable, this is a private back-door for builders
    pub fn withString(self: *Self, entry_name: []const u8, entry_value: []const u8) CreationError!void {
        try self.withEntry(entry_name, .{ .string_value = entry_value });
    }

    /// (internal) Adds a string entry to the config node.
    /// All `Config`s are actually mutable, this is a private back-door for builders
    pub fn withStringList(self: *Self, entry_name: []const u8, entry_value: StringList) CreationError!void {
        try self.withEntry(entry_name, .{ .string_list_value = entry_value });
    }

    /// (internal) Adds a sub-configuration to the config node.
    ///
    /// Note: The builder gains ownership of the configuration passed in.
    /// All `Config`s are actually mutable, this is a private back-door for builders
    fn withConfig(self: *Self, entry_value: *Config) CreationError!void {
        try self.withEntry(entry_value.name(), .{ .config_value = entry_value });
    }

    /// (internal) Merges given config into this one.
    /// All `Config`s are actually mutable, this is a private back-door for builders
    pub fn merge(self: *Self, source_config: Config) CreationError!void {
        var it = source_config.iterator();
        while (it.next()) |source_entry| {
            if (self.value(source_entry.key)) |existing| {
                switch (existing) {
                    .config_value => |existing_cfg| {
                        // Merge into existing config value
                        assert(switch (source_entry.value) {
                            .config_value => true,
                            else => false,
                        });
                        // logger.trace("\tmerging into existing config entry [{s}] with value={any}\n", .{ source_entry.key, source_entry.value.config_value.* });
                        try existing_cfg.merge(source_entry.value.config_value.*);
                    },
                    else => {
                        // Replace existing value
                        // logger.trace("replacing existing non-config entry [{s}] with value={any}\n", .{ source_entry.key, source_entry.value });
                        try self.withEntry(source_entry.key, source_entry.value);
                    },
                }
            } else {
                switch (source_entry.value) {
                    .config_value => {
                        // Merge new config entry
                        // logger.trace("merging new config entry [{s}] with value={any}\n", .{ source_entry.key, source_entry.value.config_value.* });
                        var merged_subconfig = try initAlloc(source_entry.key, self.allocator, self);
                        errdefer merged_subconfig.deinit();
                        errdefer self.allocator.destroy(merged_subconfig);
                        try merged_subconfig.merge(source_entry.value.config_value.*);
                        try self.withEntry(source_entry.key, .{ .config_value = merged_subconfig });
                    },
                    else => {
                        // Merge new entry
                        // logger.trace("merging new non-config entry [{s}] with value={any}\n", .{ source_entry.key, source_entry.value });
                        try self.withEntry(source_entry.key, source_entry.value);
                    },
                }
            }
        }
    }

    /// (internal) Yields config value as mutable config node.
    /// All `Config`s are actually mutable, this is a private back-door for builders
    fn asMutableConfig(self: *Self, value_name: []const u8) UsageError!?*Config {
        if (self.value(value_name)) |val| {
            return switch (val) {
                .config_value => |cfg| cfg,
                else => {
                    logger.err("Config value [{s}] is not a config: [{any}].", .{ value_name, val });
                    return Error.IllegalConversion;
                },
            };
        }
        return null;
    }
};

/// Configuration builder.
pub const ConfigBuilder = struct {
    const Self = @This();

    mutable_config: Config,
    built: bool = false,

    /// Initialize with empty configuration
    pub fn init(name: []const u8, allocator: std.mem.Allocator) CreationError!Self {
        return .{
            .mutable_config = try Config.init(name, allocator),
        };
    }

    /// Initialize using given mutable configuration
    pub fn initUsing(mutable_config: Config) CreationError!Self {
        return .{
            .mutable_config = mutable_config,
        };
    }

    /// De-initializes the config builder.
    ///
    /// Builders expect users to call build() and assume ownership of the resulting config, if `build()` was called,
    /// we leave our `mutable_config` alone, otherwise we de-initialize the mutable config we're holding.
    pub fn deinit(self: *Self) void {
        if (!self.built) {
            self.mutable_config.deinit();
        }
    }

    /// Builds and return the configuration.
    ///
    /// Caller gains ownership of the returned configuration.
    pub fn build(self: *Self) CreationError!Config {
        self.built = true;
        return self.mutable_config;
    }

    pub fn merge(self: *Self, source_config: Config) CreationError!void {
        try self.mutable_config.merge(source_config);
    }

    pub fn withEntry(self: *Self, name: []const u8, value: Value) CreationError!void {
        try self.mutable_config.withEntry(name, value);
    }

    /// Adds a boolean entry to the config node.
    pub fn withBool(self: *Self, name: []const u8, value: bool) CreationError!void {
        try self.mutable_config.withEntry(name, .{ .bool_value = value });
    }

    /// Adds an integer entry to the config node.
    pub fn withInt(self: *Self, name: []const u8, value: i128) CreationError!void {
        try self.mutable_config.withEntry(name, .{ .int_value = value });
    }

    /// Adds a floating point entry to the config node.
    pub fn withFloat(self: *Self, name: []const u8, value: comptime_float) CreationError!void {
        try self.mutable_config.withEntry(name, .{ .float_value = value });
    }

    /// Adds a string entry to the config node.
    pub fn withString(self: *Self, name: []const u8, value: []const u8) CreationError!void {
        try self.mutable_config.withEntry(name, .{ .string_value = value });
    }

    /// Adds a string entry to the config node.
    pub fn withStringList(self: *Self, name: []const u8, value: StringList) CreationError!void {
        try self.mutable_config.withEntry(name, .{ .string_list_value = value });
    }

    /// Adds a sub-configuration to the config node.
    ///
    /// Note: The builder gains ownership of the configuration passed in.
    pub fn withConfig(self: *Self, value: *Config) CreationError!void {
        try self.mutable_config.withEntry(value.name(), .{ .config_value = value });
    }
};

pub fn builder(name: []const u8, allocator: std.mem.Allocator) CreationError!ConfigBuilder {
    return ConfigBuilder.init(name, allocator);
}

test "dynamic config from builders" {
    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();
    std.debug.print("test.config.builders\n", .{});

    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // const allocator = gpa.allocator();
    // defer _ = gpa.deinit();
    const allocator = testing.allocator;

    var expected_config_bld = try ConfigBuilder.init("expected", allocator);
    try expected_config_bld.withEntry("first", Value{ .int_value = 32 });
    try expected_config_bld.withEntry("second", Value{ .int_value = 128 });
    try expected_config_bld.withEntry("second", Value{ .int_value = 64 });
    try expected_config_bld.withEntry("name", Value{ .string_value = "test" });
    try expected_config_bld.withEntry("inner.inner-first", Value{ .int_value = 16 });
    var expected_config = try expected_config_bld.build();
    defer expected_config.deinit();
    if (test_debug_printing) {
        std.debug.print("\texpected_config.name()={s}\n", .{expected_config.name()});
        std.debug.print("\texpected_config={f}\n", .{expected_config});
        std.debug.print("\texpected_config iteration\n", .{});
        var it = expected_config.iterator();
        while (it.next()) |entry| {
            std.debug.print("\t\tkey=\"{s}\", value={f}\n", .{ entry.key, entry.value });
        }
    }

    var with_helpers_config_bld = try ConfigBuilder.init("with_helpers", allocator);
    try with_helpers_config_bld.withInt("first", 32);
    try with_helpers_config_bld.withInt("second", 64);
    try with_helpers_config_bld.withString("name", "test");
    var str_lst = StringList.init(allocator);
    defer str_lst.deinit();
    try str_lst.append("one");
    try str_lst.append("two");
    try str_lst.append("three");
    try with_helpers_config_bld.withStringList("list", str_lst);
    try with_helpers_config_bld.withInt("inner.inner-first", 16);
    var with_helpers_config = try with_helpers_config_bld.build();
    defer with_helpers_config.deinit();

    if (test_debug_printing) {
        std.debug.print("\twith_helpers_config.name()={s}\n", .{with_helpers_config.name()});
        std.debug.print("\twith_helpers_config={f}\n", .{with_helpers_config});
    }

    try testing.expectEqual(expected_config.value("first"), with_helpers_config.value("first"));
    const expected_first = try if (try expected_config.asInt("first")) |val| val else error.Wrong;
    const with_helpers_first = try if (try with_helpers_config.asInt("first")) |val| val else error.Wrong;
    try testing.expectEqual(expected_first, with_helpers_first);
    try testing.expectEqual(expected_config.value("second"), with_helpers_config.value("second"));
    const expected_name = try if (try expected_config.asString("name")) |val| val else error.Wrong;
    const with_helpers_name = try if (try with_helpers_config.asString("name")) |val| val else error.Wrong;
    try testing.expectEqualStrings(expected_name, with_helpers_name);

    const expected_inner_config = try if (try expected_config.asConfig("inner")) |cfg| cfg else error.Wrong;
    const with_helpers_inner_config = try if (try with_helpers_config.asConfig("inner")) |cfg| cfg else error.Wrong;

    try testing.expectEqual(expected_inner_config.value("inner-first"), with_helpers_inner_config.value("inner-first"));

    try testing.expectEqual(expected_config.value("inner.inner-first") orelse error.Wrong, with_helpers_config.value("inner.inner-first") orelse error.Wrong);
}

test "merging configurations" {
    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();
    std.debug.print("test.config.merging\n", .{});

    const allocator = testing.allocator;

    std.debug.print("\tbuilding config_part_1...\n", .{});
    var config_part_1_bld = try ConfigBuilder.init("part-1", allocator);
    try config_part_1_bld.withString("string-1", "1");
    try config_part_1_bld.withString("subconfig.string-11", "11");
    var config_part_1 = try config_part_1_bld.build();
    defer config_part_1.deinit();

    std.debug.print("\tbuilding config_part_2...\n", .{});
    var config_part_2_bld = try ConfigBuilder.init("part-2", allocator);
    try config_part_2_bld.withString("string-2", "2");
    try config_part_2_bld.withString("subconfig.string-22", "22");
    try config_part_2_bld.merge(config_part_1);
    var config_part_2 = try config_part_2_bld.build();
    defer config_part_2.deinit();

    std.debug.print("\tbuilding config_part_3...\n", .{});
    var config_part_3_bld = try ConfigBuilder.init("part-3", allocator);
    try config_part_3_bld.withString("string-3", "3");
    try config_part_3_bld.withString("subconfig.string-33", "33");
    try config_part_3_bld.merge(config_part_2);
    var config_part_3 = try config_part_3_bld.build();
    defer config_part_3.deinit();

    const config_final = config_part_3;

    if (test_debug_printing) {
        std.debug.print("\tconfig_part_3\n", .{});
        var it = config_final.iterator();
        while (it.next()) |entry| {
            std.debug.print("\t\tkey=\"{s}\", value={f}\n", .{ entry.key, entry.value });
        }
    }

    try testing.expectEqualStrings("1", (try config_final.asString("string-1")).?);
    try testing.expectEqualStrings("2", (try config_final.asString("string-2")).?);
    try testing.expectEqualStrings("3", (try config_final.asString("string-3")).?);
}

/// Environment configuration builder.
///
/// Generates configuration using environment variables.
/// Environment variable name conversion rules:
/// - Double underscore '__' becomes underscore '_'
/// - "_dash_" becomes '-'
/// - Uppercase character becomes lower case
/// - Underscore '_' becomes period '.'
const EnvironmentConfigBuilder = struct {
    const Self = @This();

    /// Environment configuration builder options.
    /// `.blacklist` allows environment variable name *prefixes* to be blacklisted. Any such entries are skipped.
    pub const Options = struct {
        pub const BlackListEntry = struct {
            prefix: []const u8,
        };
        blacklist: ?[]const BlackListEntry = null,
    };

    mutable_config: Config,
    options: Options,

    pub fn init(name: []const u8, allocator: std.mem.Allocator, options: Options) CreationError!Self {
        return .{
            .options = options,
            .mutable_config = try Config.init(name, allocator),
        };
    }

    /// No-Op de-init.
    /// Builders expect users to call build() and assume ownership of the resulting config.
    pub fn deinit(_: *Self) void {}

    /// Builds and return the configuration.
    ///
    /// Caller gains ownership of the returned configuration.
    pub fn build(self: *Self) CreationError!Config {
        const allocator = self.mutable_config.allocator;
        var env_map = std.process.getEnvMap(allocator) catch return Error.OutOfMemory;
        defer env_map.deinit();
        var env_iter = env_map.iterator();
        iter: while (env_iter.next()) |env_entry| {
            if (self.options.blacklist) |blacklisted| {
                for (blacklisted) |blacklist_entry| {
                    if (std.mem.startsWith(u8, env_entry.key_ptr.*, blacklist_entry.prefix)) {
                        logger.debug("Skipping black-listed environment variable: {s}.", .{env_entry.key_ptr.*});
                        continue :iter;
                    }
                }
            }
            const key_path = try toKeyName(env_entry.key_ptr.*, allocator);
            defer allocator.free(key_path);
            const normalized_path = if (std.mem.indexOfNone(u8, key_path, &[_]u8{'.'})) |idx|
                key_path[idx..]
            else
                key_path;
            try self.mutable_config.withEntry(normalized_path, .{ .string_value = env_entry.value_ptr.* });
        }

        return self.mutable_config;
    }

    /// Returns an environment variable name converted to a config key name.
    ///
    /// Environment variable name conversion rules:
    /// - Double underscore '__' becomes underscore '_'
    /// - "_dash_" becomes '-'
    /// - Uppercase character becomes lower case
    /// - Underscore '_' becomes period '.'
    fn toKeyName(envvar_name: []const u8, allocator: std.mem.Allocator) CreationError![]const u8 {

        // Replace "__" with '\t' (we'll change it to '_' later)
        const pre_target = std.mem.replaceOwned(u8, allocator, envvar_name, "__", "\t") catch return CreationError.OutOfMemory;
        defer allocator.free(pre_target);

        // Replace "_dash_"
        var target = std.mem.replaceOwned(u8, allocator, pre_target, "_dash_", "-") catch return CreationError.OutOfMemory;

        // Convert to lower case
        for (target, 0..) |ch, idx| {
            target[idx] = std.ascii.toLower(ch);
        }

        // Convert underscores to periods
        std.mem.replaceScalar(u8, target, '_', '.');

        // Convert tabs to underscores
        std.mem.replaceScalar(u8, target, '\t', '_');

        return target;
    }
};

test "config from environment" {
    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();
    std.debug.print("test.config.env\n", .{});

    const allocator = testing.allocator;

    std.debug.print("\tbuilding env_config...\n", .{});
    var env_config_bld = try EnvironmentConfigBuilder.init("env", allocator, .{});
    var env_config = try env_config_bld.build();
    defer env_config.deinit();

    if (test_debug_printing) {
        std.debug.print("\tenv_config={f}\n", .{env_config});
        std.debug.print("\titerating env_config:\n", .{});
        var it = env_config.iterator();
        while (it.next()) |entry| {
            std.debug.print("\t\tkey=\"{s}\", value={f}\n", .{ entry.key, entry.value });
        }
    }
}

test "merging config from environment" {
    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();
    std.debug.print("test.config.env.merge\n", .{});

    const allocator = testing.allocator;

    std.debug.print("\tBuilding base config\n", .{});
    var config_bld = try ConfigBuilder.init("base", allocator);
    try config_bld.withString("aaa", "AAA");
    try config_bld.withString("zzz", "ZZZ");
    try config_bld.withString("home", "less");

    std.debug.print("\tBuilding environment config\n", .{});
    var env_bld = try EnvironmentConfigBuilder.init("env", allocator, .{});
    var env_cfg = try env_bld.build();
    defer env_cfg.deinit();

    std.debug.print("\tMerging environment config into base\n", .{});
    try config_bld.merge(env_cfg);
    var config = try config_bld.build();
    defer config.deinit();

    if (test_debug_printing) {
        std.debug.print("\tconfig={f}\n", .{config});
        std.debug.print("\titerating config:\n", .{});
        var it = config.iterator();
        while (it.next()) |entry| {
            std.debug.print("\t\tkey=\"{s}\", value={f}\n", .{ entry.key, entry.value });
        }
    }

    try testing.expectEqualStrings("AAA", (try config.asString("aaa")).?);
    try testing.expectEqualStrings("ZZZ", (try config.asString("zzz")).?);
    try testing.expect(!std.mem.eql(u8, "less", (try config.asString("home")).?));
}

/// Parses environment variables and returns the resulting configuration.
///
/// Caller owns the returned `Config`.
pub fn fromEnvironment(allocator: std.mem.Allocator) Error!Config {
    const default_envvar_prefix_blacklist = [2]EnvironmentConfigBuilder.Options.BlackListEntry{
        .{ .prefix = "_" },
        .{ .prefix = "GITHUB_TOKEN" },
    };

    var env_bld = try EnvironmentConfigBuilder.init("env", allocator, .{ .blacklist = &default_envvar_prefix_blacklist });
    const env_cfg = try env_bld.build();
    logger.debug("Built config from environment: {any}\n", .{env_cfg});
    return env_cfg;
}

test "config from environment with defaults" {
    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();
    std.debug.print("test.config.defaults\n", .{});

    const allocator = testing.allocator;

    std.debug.print("\tbuilding env_config...\n", .{});
    var env_config = try fromEnvironment(allocator);
    defer env_config.deinit();

    if (try env_config.asString("home")) |_home| {
        std.debug.print("\thome={s}\n", .{_home});
    } else {
        std.debug.panic("Environment variable `home` not present in configuration", .{});
    }
}

/// YAML configuration builder.
///
/// Generates configuration using YAML source.
const YamlConfigBuilder = struct {
    const Self = @This();

    name: []const u8,
    source: []const u8,
    allocator: std.mem.Allocator,

    pub fn init(name: []const u8, source: []const u8, allocator: std.mem.Allocator) CreationError!Self {
        return .{
            .allocator = allocator,
            .name = name,
            .source = source,
        };
    }

    /// No-Op de-init.
    /// Builders expect users to call build() and assume ownership of the resulting config.
    pub fn deinit(_: *Self) void {}

    /// Builds and return the configuration.
    ///
    /// Caller gains ownership of the returned configuration.
    pub fn build(self: *Self) LoadError!Config {
        var yaml: Yaml = .{ .source = self.source };
        yaml.load(self.allocator) catch |err| return switch (err) {
            YamlError.OutOfMemory => CreationError.OutOfMemory,
            YamlError.DuplicateMapKey => CreationError.DuplicateEntry,
            else => LoadError.OtherAccessFailure,
        };
        defer yaml.deinit(self.allocator);

        var config = try buildConfig(self.name, yaml, self.allocator);
        _ = &config;

        return config;
    }

    fn buildSubConfig(name: []const u8, parent_config: ?*const Config, yaml_map: YamlMap, allocator: std.mem.Allocator) CreationError!*Config {
        var config = try Config.initAlloc(name, allocator, parent_config);
        errdefer config.deinit();
        errdefer allocator.destroy(config);

        var it = yaml_map.iterator();
        while (it.next()) |yaml_map_entry| {
            const entry_name = yaml_map_entry.key_ptr.*;
            const cfg_entry_val: Value = try buildSubConfigEntryValue(config, yaml_map_entry, allocator);
            try config.withEntry(entry_name, cfg_entry_val);
            switch (cfg_entry_val) {
                .string_list_value => |lst| lst.deinit(),
                else => {},
            }
        }

        return config;
    }

    fn buildSubConfigEntryValue(parent_config: ?*const Config, yaml_map_entry: YamlMap.Entry, allocator: std.mem.Allocator) CreationError!Value {
        const entry_name = yaml_map_entry.key_ptr.*;
        const entry_val = yaml_map_entry.value_ptr.*;
        const cfg_entry_val: Value = try switch (entry_val) {
            .scalar => |scalar_val| Value{ .string_value = scalar_val },
            .list => |list_val| blk: {
                var owned_lst = StringList.init(allocator);
                errdefer owned_lst.deinit();
                for (list_val) |list_entry| {
                    switch (list_entry) {
                        .scalar => |list_entry_val| {
                            // const owned_val = allocator.dupe(u8, list_entry_val) catch return CreationError.OutOfMemory;
                            // owned_lst.append(owned_val) catch return CreationError.OutOfMemory;
                            owned_lst.append(list_entry_val) catch return CreationError.OutOfMemory;
                        },
                        else => break :blk CreationError.OtherCreationFailure,
                    }
                }
                break :blk Value{ .string_list_value = owned_lst };
            },
            .map => |map_val| blk: {
                var subconfig = try buildSubConfig(entry_name, parent_config, map_val, allocator);
                errdefer subconfig.deinit();
                errdefer allocator.destroy(subconfig);
                break :blk Value{ .config_value = subconfig };
            },
            else => blk: {
                logger.err("Unsupported YAML entry: key={s}, value={any}\n", .{ entry_name, yaml_map_entry.value_ptr.* });
                break :blk CreationError.OtherCreationFailure;
            },
        };
        return cfg_entry_val;
    }

    fn buildConfig(config_name: []const u8, yaml: Yaml, allocator: std.mem.Allocator) CreationError!Config {
        assert(yaml.docs.items.len == 1);

        const yaml_map = yaml.docs.items[0].asMap() orelse return CreationError.OtherCreationFailure;
        var config = try Config.init(config_name, allocator);
        errdefer config.deinit();

        var it = yaml_map.iterator();
        while (it.next()) |yaml_map_entry| {
            const entry_name = yaml_map_entry.key_ptr.*;
            const cfg_entry_val: Value = try buildSubConfigEntryValue(&config, yaml_map_entry, allocator);
            try config.withEntry(entry_name, cfg_entry_val);
            switch (cfg_entry_val) {
                .string_list_value => |lst| lst.deinit(),
                else => {},
            }
        }

        return config;
    }
};

test "config from yaml" {
    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();
    std.debug.print("test.config.yaml\n", .{});

    const allocator = testing.allocator;

    // FIXME: Nested Yaml borks during config build (after 0.15.x changes)
    const source =
        \\outer: Worlds
        \\level: 0
        \\list:
        \\  - One
        \\  - Two
        \\  - Three
    ;
    // const source =
    //     \\outer: Worlds
    //     \\level: 0
    //     \\nested:
    //     \\  level: 1
    //     \\  some: one
    //     \\  wick: john
    //     \\list:
    //     \\  - One
    //     \\  - Two
    //     \\  - Three
    // ;

    std.debug.print("\tbuilding yaml_config...\n", .{});
    var yaml_config_bld = try YamlConfigBuilder.init("yaml", source, allocator);
    var yaml_config = try yaml_config_bld.build();
    defer yaml_config.deinit();

    if (test_debug_printing) {
        std.debug.print("\tyaml_config={f}\n", .{yaml_config});
        std.debug.print("\titerating yaml_config:\n", .{});
        var it = yaml_config.iterator();
        while (it.next()) |entry| {
            std.debug.print("\t\tkey=\"{s}\", value={f}\n", .{ entry.key, entry.value });
        }
    }

    try testing.expectEqualStrings("Worlds", (try yaml_config.asString("outer")).?);
    // try testing.expectEqualStrings("john", (try yaml_config.asString("nested.wick")).?);
}

/// Loads and parses YAML string into `Config`
///
/// Caller owns the returned `Config`
pub fn fromYamlString(yaml_string: []const u8, config_name: []const u8, allocator: std.mem.Allocator) Error!Config {
    var yaml_config_bld = try YamlConfigBuilder.init(config_name, yaml_string, allocator);
    const yaml_config = try yaml_config_bld.build();
    logger.debug("Built config from YAML: {f}\n", .{yaml_config});
    return yaml_config;
}

/// Loads and parses YAML file into `Config`
///
/// Caller owns the returned `Config`
pub fn fromYamlFile(yaml_file: std.fs.File, config_name: []const u8, allocator: std.mem.Allocator) Error!Config {
    const yaml_file_content = yaml_file.readToEndAlloc(allocator, limits.max_yaml_config_file_size) catch |err| return switch (err) {
        error.OutOfMemory => CreationError.OutOfMemory,
        else => AccessError.AccessFailure,
    };
    defer allocator.free(yaml_file_content);

    var yaml_config_bld = try YamlConfigBuilder.init(config_name, yaml_file_content, allocator);
    var yaml_config = try yaml_config_bld.build();
    _ = &yaml_config;
    logger.debug("Built config from YAML: {f}\n", .{yaml_config});
    return yaml_config;
}

/// Loads and parses YAML file into `Config`
///
/// Caller owns the returned `Config`
pub fn fromYamlPath(yaml_file_dir: std.fs.Dir, config_file_name: []const u8, allocator: std.mem.Allocator) Error!Config {
    var yml_file = yaml_file_dir.openFile(config_file_name, .{}) catch |err| return switch (err) {
        std.fs.File.OpenError.FileNotFound => Error.FileNotFound,
        std.fs.File.OpenError.AccessDenied => Error.AccessFailure,
        else => Error.OtherAccessFailure,
    };
    defer yml_file.close();

    return fromYamlFile(yml_file, config_file_name, allocator);
}

/// Creates and returns a new configuration with overrides from the environment configuration
///
/// Caller owns the returned `Config`
pub fn withEnvOverrides(base_config: Config, allocator: std.mem.Allocator) Error!Config {

    // Prepare environment configuration
    var env_config = try fromEnvironment(allocator);
    defer env_config.deinit();

    // Merge configurations
    var merged_config_bld = try ConfigBuilder.init(base_config.name(), allocator);
    try merged_config_bld.merge(base_config);
    try merged_config_bld.merge(env_config);

    return merged_config_bld.build();
}

test "configuration with environment overrides" {
    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();
    std.debug.print("test.config.env.overrides\n", .{});

    const allocator = testing.allocator;

    // Prepare YAML configuration
    const yaml_source =
        \\outer: Worlds
        \\path: Inner
        \\nested:
        \\  jane: doe
        \\  wick: john
        \\listed:
        \\  - item 1
        \\  - item 2
    ;

    {
        var yaml_file = try tmp_dir.dir.createFile("env-override-test.yaml", .{ .read = true });
        defer yaml_file.close();
        try yaml_file.writeAll(yaml_source);
    }

    var yaml_config = try fromYamlPath(tmp_dir.dir, "env-override-test.yaml", allocator);
    defer yaml_config.deinit();

    // Override configuration
    var config = try withEnvOverrides(yaml_config, allocator);
    defer config.deinit();

    _ = std.mem.indexOfDiff(u8, "Inner", (try config.asString("path")).?) orelse std.debug.panic("Path not overridden", .{});
    try testing.expectEqualStrings("Worlds", (try config.asString("outer")).?);
    try testing.expectEqualStrings("john", (try config.asString("nested.wick")).?);

    const listed = (try config.asStringList("listed")).?;

    try testing.expectEqualStrings("item 1", listed.items[0]);
    try testing.expectEqualStrings("item 2", listed.items[1]);

    if (try config.asString("home")) |_home| {
        std.debug.print("\thome={s}\n", .{_home});
    } else {
        std.debug.panic("Entry `home` not present in configuration", .{});
    }
}
