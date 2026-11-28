//! Dymes Message Store Read Dataset.
//!
// SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
//
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const common = @import("dymes_common");
const Ulid = common.ulid.Ulid;
const isBeforeUlid = common.util.isBeforeUlid;
const logging = common.logging;
const limits = @import("limits.zig");
const constants = @import("constants.zig");

const dymes_msg = @import("dymes_msg");
const msg_limits = dymes_msg.limits;
const FrameAllocator = dymes_msg.FrameAllocator;

const Filter = dymes_msg.Filter;

const errors = @import("errors.zig");

const AllocationError = errors.AllocationError;
const CreationError = errors.CreationError;
const AccessError = errors.AccessError;
const StateError = errors.StateError;
const SyncError = errors.SyncError;

pub const MessageLocation = @import("MessageLocation.zig");
pub const SegmentLocation = @import("SegmentLocation.zig");

const DatasetScanner = @import("DatasetScanner.zig");

const DataSegment = @import("DataSegment.zig");
const SegmentsUlidIndex = @import("SegmentsUlidIndex.zig");
const MessageIndex = @import("MessageIndex.zig");
const MessageFile = @import("MessageFile.zig");
const ChannelIndex = @import("ChannelIndex.zig");
const ChannelIndexFile = @import("ChannelIndexFile.zig");
const HeaderIndexFile = @import("HeaderIndexFile.zig");

/// Message store dataset options
pub const Options = struct {
    /// Data store directory
    dir: std.fs.Dir,

    /// Segment number
    segment_no: ?u64 = null,
};

/// State of repair
pub const RepairState = enum {
    healthy,
    needs_repair,
    unrepairable,
};

/// Dataset status
pub const Status = struct {
    /// Open state
    opened: bool,

    /// State of repair
    repair_state: RepairState,
};

const Self = @This();

/// Cache of active data segments by segment no
const DataSegmentsCache = @import("DataSegmentsCache.zig");

pub const OpenError = CreationError || AccessError || StateError;

/// Logger
logger: *logging.Logger,

/// Memory allocator
gpa: std.mem.Allocator,

/// Message frame allocator
frame_allocator: std.mem.Allocator,

/// Data store directory
dir: std.fs.Dir,

/// Segment cache
segments_cache: DataSegmentsCache,

/// Segments in use
num_segments: u64,

/// Dataset status
status: Status,

/// First ULID in dataset across segments
first_ulid: Ulid,

/// Last ULID in dataset across segments
last_ulid: Ulid,

/// Segment index (owned by creator)
/// Used for correlation lookup
segments_ulid_idx: *SegmentsUlidIndex,

/// Dataset lock
mtx_dataset: std.Thread.Mutex.Recursive = .init,

/// Channel directory of each closed segment, by segment number, from its channel index file. Null for the
/// segment being appended to, and for a segment whose file could not be built: those are read through the
/// segment cache.
channel_dirs: std.array_list.Managed(?ChannelIndexFile.Directory),

/// Open files of the closed segments channel reads were last in
light_segments: [limits.max_open_channel_read_segments]?LightSegment = [_]?LightSegment{null} ** limits.max_open_channel_read_segments,
light_clock: u64 = 0,

/// The header the store keeps a hash index of, if any (owned)
indexed_header: ?[]u8 = null,

/// Which closed segments have a header index file that has been checked, by segment number (under the dataset lock)
hdx_ready: std.array_list.Managed(bool),

/// Builds and checks header index files in the background, the newest segment first
hdx_builder: ?std.Thread = null,
hdx_mutex: std.Thread.Mutex = .{},
hdx_wake: std.Thread.Condition = .{},
/// Segments waiting for the builder; it takes the last
hdx_queue: std.array_list.Managed(u64),
hdx_stop: bool = false,

/// A closed segment's message file and channel index file, open for reading. Channel reads use these and
/// never load the segment's index.
const LightSegment = struct {
    segment_no: u64,
    msg_file: std.fs.File,
    chx_file: std.fs.File,
    /// The segment's header index file, opened when a read first wants it
    hdx_file: ?std.fs.File = null,
    last_used: u64,

    fn close(self: *LightSegment) void {
        self.msg_file.close();
        self.chx_file.close();
        if (self.hdx_file) |file| file.close();
    }
};

/// Opens a read dataset.
///
/// Caller must call `close()` to release resources.
pub fn open(gpa: std.mem.Allocator, frame_allocator: std.mem.Allocator, segments_ulid_idx: *SegmentsUlidIndex, options: Options) OpenError!Self {
    var logger = logging.logger("msg_store.ReadDataset");

    logger
        .fine()
        .msg("Opening read dataset")
        .log();

    // Scan directory for dataset files
    var scanner = try DatasetScanner.init(gpa, options.dir);
    defer scanner.deinit();
    if (scanner.mismatched) {
        return OpenError.InconsistentState;
    }

    // Determine initial data segment
    const initial_segment_no: u64 = if (options.segment_no) |seq| seq else 0;
    var first_ulid: Ulid = .{ .rand = 0x0, .time = 0x0 };
    var last_ulid: Ulid = .{ .rand = 0x0, .time = 0x0 };
    if (initial_segment_no == 0 and scanner.results.len == 0) {
        // We're a new dataset - this cannot be!
        logger
            .err()
            .msg("Read dataset directory contains no message or index files")
            .log();
        return OpenError.FileNotFound;
    }

    // We take advantage of the scanner results being sorted
    assert(scanner.results.len % 2 == 0);
    const msg_file = scanner.results[scanner.results.len - 2];
    const idx_file = scanner.results[scanner.results.len - 1];
    if (msg_file.segment_no != idx_file.segment_no) {
        return OpenError.InconsistentState;
    }
    const last_segment_no = msg_file.segment_no;

    // Open first segment
    var first_segment = try DataSegment.open(gpa, frame_allocator, options.dir, .{
        .initialize = false,
        .segment_no = 0,
        .timestamp = 0x0,
    });
    first_ulid = first_segment.index_file.first_ulid;
    last_ulid = first_segment.index_file.last_ulid;
    first_segment.close();

    if (last_segment_no != 0) {
        // Open last segment
        var last_segment = try DataSegment.open(gpa, frame_allocator, options.dir, .{
            .initialize = false,
            .segment_no = last_segment_no,
            .timestamp = 0x0,
        });
        defer last_segment.close();
        last_ulid = last_segment.index_file.last_ulid;
    }

    var segments_cache = DataSegmentsCache.init(gpa) catch return OpenError.OutOfMemory;
    errdefer segments_cache.deinit();

    logger
        .debug()
        .msg("Read dataset opened")
        .log();
    return .{
        .logger = logger,
        .gpa = gpa,
        .frame_allocator = frame_allocator,
        .dir = options.dir,
        .num_segments = scanner.results.len / 2,
        .segments_cache = segments_cache,
        .status = .{
            .opened = true,
            .repair_state = .healthy,
        },
        .first_ulid = first_ulid,
        .last_ulid = last_ulid,
        .segments_ulid_idx = segments_ulid_idx,
        .channel_dirs = std.array_list.Managed(?ChannelIndexFile.Directory).init(gpa),
        .hdx_ready = std.array_list.Managed(bool).init(gpa),
        .hdx_queue = std.array_list.Managed(u64).init(gpa),
    };
}

/// Starts keeping a hash index of the header `key`: a background thread checks each closed segment's header
/// index file and builds those that are missing or can't be trusted, the newest segment first. Until a segment's
/// file is ready, reads of it fetch frames as before. Call once the dataset is at its final address.
pub fn startHeaderIndex(self: *Self, key: []const u8) (AllocationError || CreationError)!void {
    assert(self.hdx_builder == null);
    if (key.len == 0) return;
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();
    self.indexed_header = self.gpa.dupe(u8, key) catch return AllocationError.OutOfMemory;
    errdefer {
        self.gpa.free(self.indexed_header.?);
        self.indexed_header = null;
    }
    self.hdx_queue.ensureTotalCapacity(self.channel_dirs.items.len) catch return AllocationError.OutOfMemory;
    for (0..self.channel_dirs.items.len) |segment_no| self.hdx_queue.appendAssumeCapacity(segment_no);
    self.hdx_builder = std.Thread.spawn(.{}, headerIndexBuilder, .{self}) catch return CreationError.OtherCreationFailure;
}

fn headerIndexBuilder(self: *Self) void {
    const key = self.indexed_header.?;
    var built: u64 = 0;
    var checked: u64 = 0;
    while (true) {
        self.hdx_mutex.lock();
        while (self.hdx_queue.items.len == 0 and !self.hdx_stop) {
            if (checked > 0) {
                self.logger.info()
                    .msg("Segment header indexes ready")
                    .str("header", key)
                    .int("segments", checked)
                    .int("built", built)
                    .log();
                checked = 0;
                built = 0;
            }
            self.hdx_wake.wait(&self.hdx_mutex);
        }
        if (self.hdx_stop) {
            self.hdx_mutex.unlock();
            return;
        }
        const segment_no = self.hdx_queue.pop().?;
        self.hdx_mutex.unlock();

        if (!HeaderIndexFile.valid(self.gpa, self.dir, segment_no, key)) {
            // A copy of the segment's channel directory: the dataset may drop its own meanwhile
            var directory: ChannelIndexFile.Directory = copied: {
                self.mtx_dataset.lock();
                defer self.mtx_dataset.unlock();
                const held = self.channelDirectory(segment_no) orelse continue;
                break :copied .{
                    .first_id = held.first_id,
                    .last_id = held.last_id,
                    .num_entries = held.num_entries,
                    .entries = self.gpa.dupe(ChannelIndexFile.DirEntry, held.entries) catch continue,
                };
            };
            defer directory.deinit(self.gpa);
            var timer = std.time.Timer.start() catch null;
            // The general allocator, not the frame allocator: this is not a request thread
            HeaderIndexFile.build(self.gpa, self.gpa, self.dir, segment_no, &directory, key) catch |e| {
                self.logger.warn()
                    .msg("Failed to build a segment's header index; reads of it fetch every frame of a channel")
                    .err(e)
                    .int("segment_no", segment_no)
                    .log();
                continue;
            };
            built += 1;
            // A build reads a whole message file. Resting as long as it took leaves the requests half the disk.
            if (timer) |*_timer| self.restBuilder(_timer.read());
            if (built % 64 == 0) {
                self.logger.info()
                    .msg("Building segment header indexes")
                    .int("segment_no", segment_no)
                    .int("built", built)
                    .log();
            }
        }
        checked += 1;

        self.mtx_dataset.lock();
        defer self.mtx_dataset.unlock();
        // Only while the channel index file it follows is still in use
        if (self.channelDirectory(segment_no) == null) continue;
        while (self.hdx_ready.items.len <= segment_no) self.hdx_ready.append(false) catch break;
        if (segment_no < self.hdx_ready.items.len) self.hdx_ready.items[@intCast(segment_no)] = true;
    }
}

/// Waits for `ns`, or until the dataset is closing.
fn restBuilder(self: *Self, ns: u64) void {
    self.hdx_mutex.lock();
    defer self.hdx_mutex.unlock();
    if (!self.hdx_stop) self.hdx_wake.timedWait(&self.hdx_mutex, ns) catch {};
}

/// Whether a closed segment's header index file is in use (tests).
fn headerIndexReady(self: *Self, segment_no: u64) bool {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();
    return segment_no < self.hdx_ready.items.len and self.hdx_ready.items[@intCast(segment_no)];
}

/// The hash to look for in the header index, if these filters ask for one value of the indexed header.
pub fn headerHashFor(self: *const Self, filters: []const dymes_msg.Filter) ?u32 {
    const key = self.indexed_header orelse return null;
    for (filters) |filter| {
        switch (filter) {
            .kv_filter => |kv_filter| switch (kv_filter) {
                .eql => |pair| if (std.mem.eql(u8, pair.key, key)) return HeaderIndexFile.valueHash(pair.value),
                else => {},
            },
            else => {},
        }
    }
    return null;
}

/// A closed segment's header index file, if it has one that has been checked. Under the dataset lock.
fn headerIndexFile(self: *Self, light: *LightSegment) ?std.fs.File {
    if (light.segment_no >= self.hdx_ready.items.len or !self.hdx_ready.items[@intCast(light.segment_no)]) return null;
    if (light.hdx_file == null) light.hdx_file = HeaderIndexFile.openForRead(self.dir, light.segment_no) catch return null;
    return light.hdx_file;
}

fn queueHeaderIndex(self: *Self, segment_no: u64) void {
    if (self.hdx_builder == null) return;
    self.hdx_mutex.lock();
    defer self.hdx_mutex.unlock();
    self.hdx_queue.append(segment_no) catch return;
    self.hdx_wake.signal();
}

/// Loads the channel directory of every closed segment, building the channel index file of each segment that
/// has none it can trust (all of them, the first time a store is opened by a node that has them).
pub fn prepareChannelIndex(self: *Self) AllocationError!void {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();
    if (self.num_segments == 0) return;

    self.channel_dirs.ensureTotalCapacity(self.num_segments) catch return AllocationError.OutOfMemory;
    var built: u64 = 0;
    // Every segment but the last, which is being appended to
    for (self.channel_dirs.items.len..self.num_segments - 1) |segment_no| {
        const loaded = ChannelIndexFile.load(self.gpa, self.dir, segment_no) catch null;
        if (loaded == null) built += 1;
        self.channel_dirs.appendAssumeCapacity(loaded orelse self.buildChannelIndex(segment_no));
        if (loaded == null and built % 32 == 0) {
            self.logger.info()
                .msg("Building segment channel indexes")
                .int("segment_no", segment_no)
                .int("num_segments", self.num_segments)
                .log();
        }
    }
    if (built > 0) {
        self.logger.info()
            .msg("Segment channel indexes built")
            .int("built", built)
            .int("num_segments", self.num_segments)
            .log();
    }
}

/// Loads the segment being appended to into the segment cache, through which it is read: once when the store
/// opens, not under the first read that reaches it. Needs the segments ULID index.
pub fn warmActiveSegment(self: *Self) void {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();
    if (self.num_segments == 0) return;
    if (self.acquireSegment(self.num_segments - 1)) |dse| self.releaseSegment(dse) else |_| {}
}

fn buildChannelIndex(self: *Self, segment_no: u64) ?ChannelIndexFile.Directory {
    return ChannelIndexFile.build(self.gpa, self.dir, segment_no) catch |e| {
        self.logger.warn()
            .msg("Failed to build a segment's channel index; its channels are read through the segment cache")
            .err(e)
            .int("segment_no", segment_no)
            .log();
        return null;
    };
}

/// A segment was closed by a rollover: from now on it is read through its channel index file.
pub fn segmentClosed(self: *Self, segment_no: u64) void {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();
    while (self.channel_dirs.items.len < segment_no) {
        const missing = self.channel_dirs.items.len;
        self.channel_dirs.append(self.buildChannelIndex(missing)) catch return;
    }
    if (self.channel_dirs.items.len == segment_no) {
        self.channel_dirs.append(self.buildChannelIndex(segment_no)) catch return;
        self.queueHeaderIndex(segment_no);
    }
}

fn channelDirectory(self: *Self, segment_no: u64) ?*const ChannelIndexFile.Directory {
    if (segment_no >= self.channel_dirs.items.len) return null;
    if (self.channel_dirs.items[segment_no]) |*directory| return directory;
    return null;
}

/// Stops using a segment's channel index file and removes it: it did not match the segment. The segment is read
/// through the segment cache until the file is rebuilt, when the store is next opened.
fn dropChannelDirectory(self: *Self, segment_no: u64) void {
    for (&self.light_segments) |*slot| {
        if (slot.*) |*light| {
            if (light.segment_no == segment_no) {
                light.close();
                slot.* = null;
            }
        }
    }
    if (self.channel_dirs.items[segment_no]) |*directory| directory.deinit(self.gpa);
    self.channel_dirs.items[segment_no] = null;
    ChannelIndexFile.remove(self.dir, segment_no);
    // The header index is in the channel index file's order
    if (segment_no < self.hdx_ready.items.len) self.hdx_ready.items[@intCast(segment_no)] = false;
    HeaderIndexFile.remove(self.dir, segment_no);
}

fn lightSegment(self: *Self, segment_no: u64) AccessError!*LightSegment {
    self.light_clock += 1;
    var free_slot: ?*?LightSegment = null;
    var oldest_slot: *?LightSegment = &self.light_segments[0];
    for (&self.light_segments) |*slot| {
        if (slot.*) |*light| {
            if (light.segment_no == segment_no) {
                light.last_used = self.light_clock;
                return light;
            }
            if (oldest_slot.* == null or light.last_used < oldest_slot.*.?.last_used) oldest_slot = slot;
        } else if (free_slot == null) {
            free_slot = slot;
        }
    }

    var name_buf: [64]u8 = undefined;
    const msg_name = std.fmt.bufPrint(&name_buf, MessageFile.msg_file_name_fmt, .{segment_no}) catch unreachable;
    var msg_file = self.dir.openFile(msg_name, .{ .mode = .read_only }) catch return AccessError.AccessFailure;
    errdefer msg_file.close();
    const chx_file = try ChannelIndexFile.openForRead(self.dir, segment_no);

    const slot = free_slot orelse oldest_slot;
    if (slot.*) |*evicted| evicted.close();
    slot.* = .{ .segment_no = segment_no, .msg_file = msg_file, .chx_file = chx_file, .last_used = self.light_clock };
    return &slot.*.?;
}

/// Reads the frame a channel index run entry points at, and checks it is that message.
fn readRunFrame(light: *LightSegment, run_entry: ChannelIndexFile.RunEntry, channel_id: u128, msg_buffer: []u8) (AccessError || StateError)!Message {
    if (run_entry.frame_size < @sizeOf(dymes_msg.FrameHeader) or run_entry.frame_size > msg_buffer.len) {
        return StateError.InconsistentState;
    }
    const frame = msg_buffer[0..run_entry.frame_size];
    if ((light.msg_file.preadAll(frame, run_entry.file_offset) catch return AccessError.AccessFailure) != frame.len) {
        return StateError.DataTruncated;
    }
    // The header is checked before anything is taken from it: these bytes may not be a frame at all
    const frame_header: *const dymes_msg.FrameHeader = @ptrCast(@alignCast(msg_buffer.ptr));
    if (!frame_header.verifyChecksum()) return StateError.ChecksumFailure;
    if (!frame_header.id.equals(run_entry.id) or frame_header.channel != channel_id or frame_header.frame_size != run_entry.frame_size) {
        return StateError.InconsistentState;
    }
    return Message.overlay(msg_buffer);
}

/// Closes (de-initializes) a dataset.
pub fn close(self: *Self) void {
    // Before the lock: the builder takes it
    if (self.hdx_builder) |builder| {
        self.hdx_mutex.lock();
        self.hdx_stop = true;
        self.hdx_mutex.unlock();
        self.hdx_wake.signal();
        builder.join();
        self.hdx_builder = null;
    }
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();
    if (!self.status.opened) {
        self.logger
            .fine()
            .msg("Read dataset already closed")
            .log();
        return;
    }
    self.logger
        .fine()
        .msg("Closing read dataset")
        .log();
    defer self.segments_cache.deinit();
    defer self.channel_dirs.deinit();
    defer self.hdx_ready.deinit();
    defer self.hdx_queue.deinit();
    defer if (self.indexed_header) |key| self.gpa.free(key);
    for (self.channel_dirs.items) |*maybe_directory| {
        if (maybe_directory.*) |*directory| directory.deinit(self.gpa);
    }
    for (&self.light_segments) |*slot| {
        if (slot.*) |*light| light.close();
        slot.* = null;
    }

    self.status.opened = false;
    self.logger
        .debug()
        .msg("Read dataset closed")
        .log();
}

/// Queries whether or not the read dataset may contain the given ULID.
///
/// False positives are possible, but not false negatives.
pub fn mayContain(self: *const Self, id: Ulid) bool {
    return self.first_ulid.equals(id) or self.last_ulid.equals(id) or
        isBeforeUlid(self.first_ulid, id) and isBeforeUlid(id, self.last_ulid);
}

/// Queries the read dataset readiness
pub inline fn ready(self: *const Self) bool {
    return self.status.opened and self.status.repair_state == .healthy;
}

/// Opens a range iterator across segments
///
/// The caller gains ownership of the iterator, and must call `close()` to release resources.
pub fn rangeIterator(self: *Self, first_ulid: Ulid, final_ulid: Ulid, first_segment: u64, final_segment: u64) RangeIterator {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();
    assert(first_segment >= 0);
    assert(final_segment < self.num_segments);
    return RangeIterator.init(self, first_ulid, final_ulid, first_segment, final_segment);
}

/// Opens a channel iterator across segments
///
/// The caller gains ownership of the iterator, and must call `close()` to release resources.
pub fn channelIterator(self: *Self, channel_id: u128, first_ulid: Ulid, final_ulid: Ulid, first_segment: u64, final_segment: u64) AllocationError!ChannelIterator {
    return self.channelIteratorFor(channel_id, first_ulid, final_ulid, first_segment, final_segment, null);
}

/// As `channelIterator`, for a read that wants only messages whose indexed header hashes to `header_hash`
/// (`headerHashFor`). The iterator leaves out messages the header index rules out; it may still return others,
/// so the caller applies its filters as before.
pub fn channelIteratorFor(self: *Self, channel_id: u128, first_ulid: Ulid, final_ulid: Ulid, first_segment: u64, final_segment: u64, header_hash: ?u32) AllocationError!ChannelIterator {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();
    assert(first_segment >= 0);
    assert(final_segment < self.num_segments);
    var it = try ChannelIterator.init(self, channel_id, first_ulid, final_ulid, first_segment, final_segment);
    it.header_hash = header_hash;
    return it;
}

const Message = @import("dymes_msg").Message;

pub const FetchError = ActiveSegmentError;

/// Returns the location of the message with the given identifier
pub fn lookupLocation(self: *Self, segment_no: u64, msg_id: Ulid) FetchError!?MessageLocation {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();

    const dse = try self.acquireSegment(segment_no);
    defer self.releaseSegment(dse);
    if (dse.message_index.lookup(msg_id)) |index_entry| {
        return .{
            .id = index_entry.id,
            .segment_no = segment_no,
            .file_offset = index_entry.msg_file_offset,
            .frame_size = index_entry.msg_frame_size,
        };
    }
    return null;
}

/// Returns a view of the message with the given id if present.
///
/// This is a *view* of the message, the caller _does not_ gain ownership.
/// Future calls to `fetch` may invalidate the message view.
pub fn fetch(self: *Self, segment_no: u64, ulid: Ulid, filters: []Filter) FetchError!?Message {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();

    const dse = try self.acquireSegment(segment_no);
    defer self.releaseSegment(dse);
    if (dse.message_index.lookup(ulid)) |index_entry| {
        const message_ = dse.data_segment.fetchAt(index_entry.msg_file_offset) catch |e| {
            self.status.repair_state = .needs_repair;
            return e;
        };
        if (message_) |message| {
            for (filters) |_filter| {
                if (!_filter.apply(message)) {
                    return null;
                }
            }
            return message;
        }
    }
    return null;
}

/// Returns a view of the message at the given message location, if present.
///
/// This is a *view* of the message, the caller _does not_ gain ownership.
/// Future calls to `fetch` may invalidate the message view.
pub inline fn fetchAt(self: *Self, msg_location: MessageLocation, msg_buffer: []u8) FetchError!?Message {
    return self.fetchFromSegment(.{
        .segment_no = msg_location.segment_no,
        .file_offset = msg_location.file_offset,
        .frame_size = msg_location.frame_size,
    }, msg_buffer);
}

/// Returns a view of the message at the given segment location, if present.
///
/// This is a *view* of the message, the caller _does not_ gain ownership.
/// Future calls to `fetch` may invalidate the message view.
pub fn fetchFromSegment(self: *Self, seg_location: SegmentLocation, msg_buffer: []u8) FetchError!?Message {
    assert(msg_buffer.len >= seg_location.frame_size);
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();

    const dse = try self.acquireSegment(seg_location.segment_no);
    defer self.releaseSegment(dse);
    const msg = dse.data_segment.fetchAt(seg_location.file_offset) catch |e| {
        self.status.repair_state = .needs_repair;
        return e;
    };
    if (msg) |_msg| {
        const used_frame = _msg.usedFrame();
        if (msg_buffer.len < used_frame.len) {
            self.logger.err()
                .msg("Result message buffer too small")
                .intx("msg_frame_size", _msg.frame_header.frame_size)
                .intx("msg_buffer_len", msg_buffer.len)
                .log();
            return FetchError.OtherAccessFailure;
        }
        @memcpy(msg_buffer[0.._msg.frame_header.frame_size], _msg.usedFrame());
        return Message.overlay(msg_buffer);
    }
    return null;
}

/// Handles notification that a new message was appended.
///
/// We 'update' the segment's index and message file contexts (no actual I/O) and the segment's message index
pub fn appended(self: *Self, channel_id: u128, msg_location: MessageLocation, corr_location: ?MessageLocation) CreationError!void {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();
    // Dataset housekeeping
    if (msg_location.segment_no == 0 and 0x0 == @as(u128, @bitCast(self.first_ulid))) {
        self.first_ulid = msg_location.id;
    }
    self.last_ulid = msg_location.id;

    // Update segment cache entry
    if (self.segments_cache.acquire(msg_location.segment_no)) |*entry| {
        defer self.segments_cache.release(entry.*);
        // Update cache entry
        try entry.message_index.store(msg_location, corr_location);
        try entry.channel_index.store(msg_location, channel_id);
        entry.data_segment.index_file.append_entry_no += 1;
        entry.data_segment.index_file.last_ulid = msg_location.id;
        entry.data_segment.message_file.append_offset += msg_location.frame_size;
    }
}

const ActiveSegmentError = CreationError || AccessError || StateError;

/// Acquires the segment cache entry for the given segment_no.
pub fn acquireSegment(self: *Self, segment_no: u64) ActiveSegmentError!DataSegmentsCache.DataSegmentEntry {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();

    if (self.segments_cache.acquire(segment_no)) |_dse| {
        return _dse;
    }

    // Update number of segments _in use_ if required (there may be more segments in use than are active)
    if (segment_no >= self.num_segments) {
        self.num_segments += 1;
    }

    self.logger
        .fine()
        .msg("Udating data segment cache")
        .int("segment_no", segment_no)
        .int("num_segments", self.num_segments)
        .log();

    // Open desired segment
    const data_segment_ptr = try DataSegment.open(self.gpa, self.frame_allocator, self.dir, .{
        .initialize = false,
        .segment_no = segment_no,
        .timestamp = 0x0,
    });
    // Until the cache holds the segment, a failure closes what was opened here; once it does, the cache owns them.
    var cached = false;
    errdefer if (!cached) data_segment_ptr.close();

    // Prepare message index
    const msg_index_ptr = try MessageIndex.init(self.gpa);
    errdefer if (!cached) msg_index_ptr.deinit();

    // Prepare channel index
    const channel_index_ptr = try ChannelIndex.init(self.gpa);
    errdefer if (!cached) channel_index_ptr.deinit();

    // Update cache entry
    const dse: DataSegmentsCache.DataSegmentEntry = dse_val: {
        self.segments_cache.put(.{
            .channel_index = channel_index_ptr,
            .message_index = msg_index_ptr,
            .data_segment = data_segment_ptr,
        }) catch |e| return switch (e) {
            DataSegmentsCache.PutError.OutOfMemory => ActiveSegmentError.OutOfMemory,
            else => ActiveSegmentError.OtherCreationFailure,
        };
        cached = true;
        break :dse_val self.segments_cache.acquire(segment_no) orelse return ActiveSegmentError.AccessFailure;
    };
    // A failure from here takes the entry out of the cache, its resources with it. Closing them while the entry stayed
    // left a closed segment in the cache, and the next eviction read it: the node crashed (extractDseKey).
    errdefer self.segments_cache.discard(segment_no);

    // Populate indexes

    var it = try data_segment_ptr.index_file.iterator();
    defer it.deinit();
    while (try it.next()) |idx_entry| {

        // Lookup correlation location
        const msg_id = idx_entry.frame_header.id;
        const corr_id = idx_entry.frame_header.correlation_id;

        const corr_location: ?MessageLocation = if (corr_id != dymes_msg.constants.NIL_CORRELATION_ID) loc_val: {
            // self.logger.fine()
            //     .msg("Performing correlation lookup")
            //     .ulid("msg_id", msg_id)
            //     .ulid("corr_id", corr_id)
            //     .log();

            // Check earlier in current segment
            if (msg_index_ptr.lookup(corr_id)) |_corr_idx_entry| {
                break :loc_val .{
                    .id = _corr_idx_entry.id,
                    .file_offset = _corr_idx_entry.msg_file_offset,
                    .frame_size = _corr_idx_entry.msg_frame_size,
                    .segment_no = segment_no,
                };
            }

            // In an earlier segment: the index keeps which one. Where it is in that segment is found when a
            // correlation query needs it (CorrelationResults.Iterator.next looks it up from the message's correlation
            // id), so loading this segment never loads another. Looking it up here loaded the earlier segment, whose
            // own first messages correlated to the one before it, and so on, holding every segment of the chain: a
            // read starting deep into a store whose batches straddle segment boundaries needed more than the 64
            // segments the cache holds, failed, and the node crashed.
            if (self.segments_ulid_idx.lookup(corr_id)) |_corr_segment_no| {
                break :loc_val .{
                    .id = corr_id,
                    .file_offset = 0,
                    .frame_size = 0,
                    .segment_no = _corr_segment_no,
                };
            }

            // Invalid correlation id
            self.logger.err()
                .msg("Unable to index message with invalid correlation reference")
                .ulid("msg_id", msg_id)
                .ulid("corr_id", corr_id)
                .log();
            return ActiveSegmentError.IllegalState;
        } else null;

        const msg_location: MessageLocation = .{
            .id = msg_id,
            .segment_no = segment_no,
            .file_offset = idx_entry.msg_file_offset,
            .frame_size = idx_entry.frame_header.frame_size,
        };

        try msg_index_ptr.store(msg_location, corr_location);
        try channel_index_ptr.store(msg_location, idx_entry.frame_header.channel);
    }

    self.logger
        .debug()
        .msg("Data segment cache updated")
        .int("segment_no", segment_no)
        .int("num_segments", self.num_segments)
        .log();

    return dse;
}

pub fn releaseSegment(self: *Self, dse: DataSegmentsCache.DataSegmentEntry) void {
    self.segments_cache.release(dse);
}

const IndexPopulationError = DataSegment.OpenError;
pub fn populateSegmentsUlidIndex(self: *Self, segments_ulid_idx: *SegmentsUlidIndex) IndexPopulationError!void {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();
    self.logger
        .fine()
        .msg("Populating segments ULID index")
        .int("num_segments", self.num_segments)
        .log();
    for (0..self.num_segments) |segment_no| {
        if (self.channelDirectory(segment_no)) |directory| {
            // A closed segment's bounds are in its channel index: no need to open it
            try segments_ulid_idx.update(segment_no, directory.first_id, directory.last_id);
        } else if (self.segments_cache.acquire(segment_no)) |entry| {
            defer self.segments_cache.release(entry);
            try segments_ulid_idx.update(segment_no, entry.data_segment.index_file.first_ulid, entry.data_segment.index_file.last_ulid);
        } else {
            var segment = try DataSegment.open(self.gpa, self.frame_allocator, self.dir, .{
                .initialize = false,
                .segment_no = segment_no,
                .timestamp = 0x0,
            });
            defer segment.close();
            try segments_ulid_idx.update(segment_no, segment.index_file.first_ulid, segment.index_file.last_ulid);
        }
    }
    self.logger
        .debug()
        .msg("Segments ULID index populated")
        .int("num_segments", self.num_segments)
        .log();
}

const ReadDataset = @This();

test "ReadDataset" {
    std.debug.print("test.ReadDataset.smoke\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    var fah = FrameAllocator.init(allocator);
    const frame_allocator = fah.allocator();

    var logger = logging.logger("test.ReadDataset.smoke");

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    try tmp_dir.dir.makeDir("test-read-dataset");

    const timestamp = std.time.nanoTimestamp();

    var dummy_segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer dummy_segments_ulid_idx.deinit(allocator);

    try testing.expectError(OpenError.FileNotFound, ReadDataset.open(allocator, frame_allocator, &dummy_segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    }));

    var ulid_generator = common.ulid.generator();

    var map = std.StringArrayHashMap([]const u8).init(allocator);
    defer map.deinit();

    try map.put("0", "zero");
    try map.put("1", "one");
    try map.put("2", "two");
    try map.put("3", "three");
    try map.put("4", "four");

    const message_frame: []u8 = try frame_allocator.alloc(u8, 1024);
    defer frame_allocator.free(message_frame);
    const message_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

    const older_ulid = try ulid_generator.next();

    // POPULATE

    const number_of_messages = 3;
    var message_ids: [number_of_messages]common.ulid.Ulid = undefined;

    {
        // Open the dataset
        const Dataset = @import("Dataset.zig");
        var dataset = try Dataset.open(allocator, frame_allocator, timestamp, .{
            .dir = tmp_dir.dir,
        });
        defer dataset.close();

        logger.debug().msg("Storing test messages").int("number_of_messages", number_of_messages).log();
        for (0..number_of_messages) |idx| {
            var test_msg =
                try Message.initOverlay(message_frame, idx, try ulid_generator.next(), idx, 101, message_body, .{ .transient_kv_headers = map });
            logger.debug()
                .msg("Storing test message")
                .int("idx", idx)
                .any("test_msg", test_msg)
                .log();
            _ = try dataset.store(&test_msg);
            message_ids[idx] = test_msg.frame_header.id;
        }
        logger.debug().msg("Test messages stored").int("number_of_messages", number_of_messages).log();
        try dataset.sync();
    }

    var segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer segments_ulid_idx.deinit(allocator);

    var read_dataset = try ReadDataset.open(allocator, frame_allocator, &segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    });
    defer read_dataset.close();

    const newer_ulid = try ulid_generator.next();
    try testing.expect(!read_dataset.mayContain(older_ulid));
    try testing.expect(!read_dataset.mayContain(newer_ulid));
    for (0..number_of_messages) |idx| {
        const expected_ulid = message_ids[idx];
        try testing.expect(read_dataset.mayContain(expected_ulid));
    }

    const noop_filter = [_]Filter{};

    logger.debug().msg("Fetching test messages").int("number_of_messages", number_of_messages).log();
    _ = try read_dataset.fetch(0x0, newer_ulid, &noop_filter);
    var timer = try std.time.Timer.start();
    var lookup_time: u64 = 0;
    for (0..number_of_messages) |idx| {
        const expected_ulid = message_ids[idx];
        timer.reset();
        const msg = try read_dataset.fetch(0x0, expected_ulid, &noop_filter) orelse @panic("Oh dear");
        lookup_time += timer.lap();
        try testing.expect(msg.frame_header.id.equals(expected_ulid));
    }
    const avg_ns = lookup_time / number_of_messages;
    logger.debug()
        .msg("Test messages fetched")
        .int("number_of_messages", number_of_messages)
        .int("avg_fetch_µs", avg_ns / std.time.ns_per_us)
        .log();
}

test "ReadDataset.appended" {
    std.debug.print("test.ReadDataset.appended\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    var fah = FrameAllocator.init(allocator);
    const frame_allocator = fah.allocator();

    var logger = logging.logger("test.ReadDataset.appended");

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    try tmp_dir.dir.makeDir("test-read-dataset-appended");

    const abs_dir_name = try tmp_dir.dir.realpathAlloc(allocator, "test-read-dataset-appended");
    defer allocator.free(abs_dir_name);

    const timestamp = std.time.nanoTimestamp();

    var segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer segments_ulid_idx.deinit(allocator);

    try testing.expectError(OpenError.FileNotFound, ReadDataset.open(allocator, frame_allocator, &segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    }));

    // Open the dataset
    const Dataset = @import("Dataset.zig");
    var dataset = try Dataset.open(allocator, frame_allocator, timestamp, .{
        .dir = tmp_dir.dir,
    });
    defer dataset.close();

    // Open read dataset
    var read_dataset = try ReadDataset.open(allocator, frame_allocator, &segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    });
    defer read_dataset.close();

    // Store some messages

    var ulid_generator = common.ulid.generator();

    var map = std.StringArrayHashMap([]const u8).init(allocator);
    defer map.deinit();

    try map.put("0", "zero");
    try map.put("1", "one");
    try map.put("2", "two");
    try map.put("3", "three");
    try map.put("4", "four");

    const message_frame: []u8 = try frame_allocator.alloc(u8, 1024);
    defer frame_allocator.free(message_frame);
    const message_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

    const older_ulid = try ulid_generator.next();
    const noop_filter = [_]Filter{};

    _ = try read_dataset.fetch(0x0, older_ulid, &noop_filter);

    const number_of_messages = 3;
    var message_ids: [number_of_messages]common.ulid.Ulid = undefined;
    logger.debug().msg("Storing test messages with append notification").int("number_of_messages", number_of_messages).log();
    for (0..number_of_messages) |idx| {
        var test_msg =
            try Message.initOverlay(message_frame, idx, try ulid_generator.next(), idx, 101, message_body, .{ .transient_kv_headers = map });
        const msg_loc = try dataset.store(&test_msg);
        try read_dataset.appended(test_msg.channel(), msg_loc, null);
        message_ids[idx] = test_msg.frame_header.id;
    }
    logger.debug().msg("Test messages stored with append notification").int("number_of_messages", number_of_messages).log();
    try dataset.sync();

    const newer_ulid = try ulid_generator.next();
    try testing.expect(!read_dataset.mayContain(older_ulid));
    try testing.expect(!read_dataset.mayContain(newer_ulid));
    for (0..number_of_messages) |idx| {
        const expected_ulid = message_ids[idx];
        try testing.expect(read_dataset.mayContain(expected_ulid));
    }

    logger.debug().msg("Fetching test messages").int("number_of_messages", number_of_messages).log();
    var timer = try std.time.Timer.start();
    var lookup_time: u64 = 0;
    for (0..number_of_messages) |idx| {
        const expected_ulid = message_ids[idx];
        timer.reset();
        const msg = try read_dataset.fetch(0x0, expected_ulid, &noop_filter) orelse @panic("Oh dear");
        lookup_time += timer.lap();
        try testing.expect(msg.frame_header.id.equals(expected_ulid));
    }
    const avg_ns = lookup_time / number_of_messages;
    logger.debug()
        .msg("Test messages fetched")
        .int("number_of_messages", number_of_messages)
        .int("avg_fetch_µs", avg_ns / std.time.ns_per_us)
        .log();
}

const IteratorError = CreationError || AccessError || StateError;

pub const RangeIterator = struct {
    logger: *logging.Logger,
    current_segment: u64,
    first_ulid: Ulid,
    final_ulid: Ulid,
    first_segment: u64,
    final_segment: u64,
    read_dataset: *ReadDataset,
    msg_file_it_: ?MessageFile.Iterator,
    reached_end: bool = false,

    fn init(read_dataset: *ReadDataset, first_ulid: Ulid, final_ulid: Ulid, first_segment: u64, final_segment: u64) RangeIterator {
        assert(first_segment <= final_segment);

        return .{
            .logger = logging.logger("msg_store.ReadDataset.RangeIterator"),
            .read_dataset = read_dataset,
            .first_ulid = first_ulid,
            .final_ulid = final_ulid,
            .first_segment = first_segment,
            .final_segment = final_segment,
            .current_segment = first_segment,
            .msg_file_it_ = null,
        };
    }

    pub fn close(self: *RangeIterator) void {
        // self.logger.fine()
        //     .msg("Closing range iterator")
        //     .int("current_segment", self.current_segment)
        //     .int("first_segment", self.first_segment)
        //     .int("final_segment", self.final_segment)
        //     .ulid("first_ulid", self.first_ulid)
        //     .ulid("final_ulid", self.final_ulid)
        //     .boolean("reached_end", self.reached_end)
        //     .log();

        if (self.msg_file_it_) |*msg_file_it| {
            // self.logger.fine()
            //     .msg("Closing message file iterator")
            //     .int("current_segment", self.current_segment)
            //     .int("first_segment", self.first_segment)
            //     .int("final_segment", self.final_segment)
            //     .ulid("first_ulid", self.first_ulid)
            //     .ulid("final_ulid", self.final_ulid)
            //     .boolean("reached_end", self.reached_end)
            //     .log();
            msg_file_it.close();
        }
    }

    pub fn next(self: *RangeIterator) IteratorError!?Message {
        // A loop, not a recursive call per segment: a read across many segments overflowed a worker
        // thread's stack (#7).
        while (true) {
            // self.logger.fine()
            //     .msg("Start of next range iteration")
            //     .int("current_segment", self.current_segment)
            //     .int("first_segment", self.first_segment)
            //     .int("final_segment", self.final_segment)
            //     .ulid("first_ulid", self.first_ulid)
            //     .ulid("final_ulid", self.final_ulid)
            //     .boolean("reached_end", self.reached_end)
            //     .log();

            if (self.reached_end) {
                // self.logger.fine()
                //     .msg("Short-circuiting next()")
                //     .int("current_segment", self.current_segment)
                //     .int("first_segment", self.first_segment)
                //     .int("final_segment", self.final_segment)
                //     .ulid("first_ulid", self.first_ulid)
                //     .ulid("final_ulid", self.final_ulid)
                //     .boolean("reached_end", self.reached_end)
                //     .log();

                // Short-circuit
                return null;
            }

            // Fetch next from current segment's message file
            if (self.msg_file_it_) |*msg_file_it| {
                const fetched = try msg_file_it.next();
                if (fetched) |val| {
                    // self.logger.fine()
                    //     .msg("Found message")
                    //     .ulid("message_id", val.frame_header.id)
                    //     .int("current_segment", self.current_segment)
                    //     .int("first_segment", self.first_segment)
                    //     .int("final_segment", self.final_segment)
                    //     .ulid("first_ulid", self.first_ulid)
                    //     .ulid("final_ulid", self.final_ulid)
                    //     .boolean("reached_end", self.reached_end)
                    //     .log();
                    if (@as(u128, @bitCast(val.frame_header.id)) == @as(u128, @bitCast(self.final_ulid))) {
                        self.reached_end = true; // Actually found final desired message
                        // self.logger.fine()
                        //     .msg("Reached final message")
                        //     .ulid("message_id", val.frame_header.id)
                        //     .int("current_segment", self.current_segment)
                        //     .int("first_segment", self.first_segment)
                        //     .int("final_segment", self.final_segment)
                        //     .ulid("first_ulid", self.first_ulid)
                        //     .ulid("final_ulid", self.final_ulid)
                        //     .boolean("reached_end", self.reached_end)
                        //     .log();
                    } else if (isBeforeUlid(self.final_ulid, val.frame_header.id)) {
                        self.reached_end = true; // We overshot the final desired message
                        // self.logger.fine()
                        //     .msg("Overshot final message")
                        //     .ulid("message_id", val.frame_header.id)
                        //     .int("current_segment", self.current_segment)
                        //     .int("first_segment", self.first_segment)
                        //     .int("final_segment", self.final_segment)
                        //     .ulid("first_ulid", self.first_ulid)
                        //     .ulid("final_ulid", self.final_ulid)
                        //     .boolean("reached_end", self.reached_end)
                        //     .log();
                        return null;
                    }
                    return val;
                }
                if (self.current_segment == self.final_segment) {
                    self.reached_end = true; // Ran out of messages before reaching final message
                    // self.logger.fine()
                    //     .msg("Ran out of messages before reaching final message")
                    //     .int("current_segment", self.current_segment)
                    //     .int("first_segment", self.first_segment)
                    //     .int("final_segment", self.final_segment)
                    //     .ulid("first_ulid", self.first_ulid)
                    //     .ulid("final_ulid", self.final_ulid)
                    //     .boolean("reached_end", self.reached_end)
                    //     .log();
                    return null;
                } else {
                    // self.logger.fine()
                    //     .msg("Ran out of messages in current segment")
                    //     .int("current_segment", self.current_segment)
                    //     .int("first_segment", self.first_segment)
                    //     .int("final_segment", self.final_segment)
                    //     .ulid("first_ulid", self.first_ulid)
                    //     .ulid("final_ulid", self.final_ulid)
                    //     .boolean("reached_end", self.reached_end)
                    //     .log();

                    // We've run out messages in the current segment, more remains
                    msg_file_it.close();
                    self.msg_file_it_ = null;
                    self.current_segment += 1;
                }
            }

            // No current segment, or non-final segment fully traversed
            // A closed segment is read from its files: where the range starts in it comes from a binary search of
            // its index file, and its messages from its message file. Loading the segment into the segment cache
            // reads its whole index twice, and a range read does that for every segment it enters.
            if (self.read_dataset.segmentClosedNow(self.current_segment)) {
                const iterator_offset: u64 = if (self.current_segment == self.first_segment)
                    try rangeStartOffset(self.read_dataset.dir, self.current_segment, self.first_ulid)
                else
                    @sizeOf(MessageFile.Header);
                if (self.msg_file_it_) |*msg_file_it| {
                    msg_file_it.close();
                    self.msg_file_it_ = null;
                }
                self.msg_file_it_ = try MessageFile.Iterator.openInDir(self.read_dataset.frame_allocator, self.read_dataset.dir, self.current_segment, iterator_offset);
                continue; // Now attempt fetch again
            }

            const dse = try self.read_dataset.acquireSegment(self.current_segment);
            defer self.read_dataset.releaseSegment(dse);

            const iterator_offset: u64 = if (self.current_segment == self.first_segment) offset: {
                // The first message at or after the range's start; none means the range starts after this segment
                const index_entries = dse.message_index.entries[0..dse.message_index.append_idx];
                var low: usize = 0;
                var high: usize = index_entries.len;
                while (low < high) {
                    const mid = (low + high) / 2;
                    if (isBeforeUlid(index_entries[mid].id, self.first_ulid)) low = mid + 1 else high = mid;
                }
                break :offset if (low < index_entries.len) index_entries[low].msg_file_offset else dse.data_segment.message_file.append_offset;
            } else @sizeOf(MessageFile.Header);

            // self.logger.fine()
            //     .msg("Opening segment message file iterator")
            //     .int("iterator_offset", iterator_offset)
            //     .int("current_segment", self.current_segment)
            //     .int("first_segment", self.first_segment)
            //     .int("final_segment", self.final_segment)
            //     .ulid("first_ulid", self.first_ulid)
            //     .ulid("final_ulid", self.final_ulid)
            //     .boolean("reached_end", self.reached_end)
            //     .log();

            if (self.msg_file_it_) |*msg_file_it| {
                msg_file_it.close();
                self.msg_file_it_ = null;
            }

            self.msg_file_it_ = try dse.data_segment.message_file.offsetIterator(iterator_offset);
            errdefer self.msg_file_it_.?.close();
            continue; // Now attempt fetch again
        }
    }

    pub fn rewind(self: *RangeIterator) void {
        if (self.msg_file_it_) |*msg_file_it| {
            msg_file_it.close();
            self.msg_file_it_ = null;
        }
        self.current_segment = self.first_segment;
        self.reached_end = false;
    }
};

test "ReadDataset.RangeIterator" {
    std.debug.print("test.ReadDataset.RangeIterator\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    var fah = FrameAllocator.init(allocator);
    const frame_allocator = fah.allocator();

    var logger = logging.logger("test.ReadDataset.RangeIterator");

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    try tmp_dir.dir.makeDir("test-range-iterator");

    const abs_dir_name = try tmp_dir.dir.realpathAlloc(allocator, "test-range-iterator");
    defer allocator.free(abs_dir_name);

    const timestamp = std.time.nanoTimestamp();

    var dummy_segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer dummy_segments_ulid_idx.deinit(allocator);

    try testing.expectError(OpenError.FileNotFound, ReadDataset.open(allocator, frame_allocator, &dummy_segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    }));

    // POPULATE
    var ulid_generator = common.ulid.generator();

    const number_of_messages: usize = @min((limits.max_message_file_size - 128) / 1024 + 10, limits.max_message_file_entries + 10);
    logger.debug()
        .msg("Will store and range query messages")
        .int("number_of_messages", number_of_messages)
        .log();
    var message_ids: []Ulid = try allocator.alloc(Ulid, number_of_messages);
    defer allocator.free(message_ids);
    const message_id_before_first: Ulid = try ulid_generator.next();
    var message_id_first: Ulid = undefined;
    var message_id_last_in_segment: Ulid = undefined;
    var message_id_first_in_next_segment: Ulid = undefined;
    var message_id_last: Ulid = undefined;
    {
        // Open the dataset
        const Dataset = @import("Dataset.zig");
        var dataset = try Dataset.open(allocator, frame_allocator, timestamp, .{
            .dir = tmp_dir.dir,
        });
        defer dataset.close();

        // Store some messages

        const message_frame: []u8 = try allocator.alloc(u8, 1024);
        defer allocator.free(message_frame);
        // const message_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

        const message_body = "A" ** 896;

        logger.debug().msg("Storing block of test messages").log();
        for (0..number_of_messages) |idx| {
            var test_msg =
                try Message.initOverlay(message_frame, idx, try ulid_generator.next(), idx, 101, message_body, .{});
            _ = try dataset.store(&test_msg);
            try testing.expectEqual(test_msg.frame_header.frame_size, 1024);
            message_ids[idx] = test_msg.frame_header.id;
        }
        logger.debug().msg("Test messages stored").int("number_of_messages", number_of_messages).log();
        message_id_first = message_ids[0];
        message_id_last = message_ids[message_ids.len - 1];
        message_id_last_in_segment = message_ids[limits.max_message_file_entries];
        message_id_first_in_next_segment = message_ids[limits.max_message_file_entries + 1];

        try dataset.sync();
    }

    var segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer segments_ulid_idx.deinit(allocator);

    var read_dataset = try ReadDataset.open(allocator, frame_allocator, &segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    });
    defer read_dataset.close();

    logger.debug().msg("Performing range query").log();
    {
        var found_ids: []Ulid = try allocator.alloc(Ulid, number_of_messages);
        defer allocator.free(found_ids);
        var found_idx: usize = 0;
        var it = read_dataset.rangeIterator(message_id_before_first, message_id_last, 0, 1);
        defer it.close();

        while (try it.next()) |found_msg| : (found_idx += 1) {
            found_ids[found_idx] = found_msg.frame_header.id;
        }
        try testing.expectEqualSlices(Ulid, message_ids[0..], found_ids[0..]);
    }

    logger.debug().msg("Fetching early range of test messages").log();

    {
        var found_ids: []Ulid = try allocator.alloc(Ulid, number_of_messages);
        defer allocator.free(found_ids);
        var found_idx: usize = 0;
        var it = read_dataset.rangeIterator(message_ids[0], message_ids[99], 0, 0);
        defer it.close();

        while (try it.next()) |found_msg| : (found_idx += 1) {
            found_ids[found_idx] = found_msg.frame_header.id;
        }
        try testing.expectEqualSlices(Ulid, message_ids[0..100], found_ids[0..100]);
    }

    logger.debug().msg("Fetching late range of test messages").log();

    {
        var found_ids: []Ulid = try allocator.alloc(Ulid, number_of_messages);
        defer allocator.free(found_ids);
        var found_idx: usize = 0;
        var it = read_dataset.rangeIterator(message_ids[900], message_ids[999], 0, 0);
        defer it.close();

        while (try it.next()) |found_msg| : (found_idx += 1) {
            found_ids[found_idx] = found_msg.frame_header.id;
        }
        try testing.expectEqualSlices(Ulid, message_ids[900..1000], found_ids[0..100]);
    }
}

/// Offset in a closed segment's message file of its first message with id `first_ulid` or later (the end of the
/// message file if it has none), by binary search of the segment's index file.
fn rangeStartOffset(dir: std.fs.Dir, segment_no: u64, first_ulid: Ulid) (AccessError || StateError)!u64 {
    const IndexFile = @import("IndexFile.zig");
    const IndexEntry = @import("IndexEntry.zig");
    var name_buf: [64]u8 = undefined;
    const idx_name = std.fmt.bufPrint(&name_buf, IndexFile.idx_file_name_fmt, .{segment_no}) catch unreachable;
    var idx_file = dir.openFile(idx_name, .{ .mode = .read_only }) catch return AccessError.AccessFailure;
    defer idx_file.close();
    const idx_size = (idx_file.stat() catch return AccessError.AccessFailure).size;
    if (idx_size < @sizeOf(IndexFile.Header)) return StateError.DataTruncated;
    const num_entries = (idx_size - @sizeOf(IndexFile.Header)) / IndexEntry.idx_entry_size;

    var entry_buf: [IndexEntry.idx_entry_size]u8 align(16) = undefined;
    var low: u64 = 0;
    var high: u64 = num_entries;
    var start_offset: ?u64 = null;
    while (low < high) {
        const mid = (low + high) / 2;
        const read = idx_file.preadAll(&entry_buf, @sizeOf(IndexFile.Header) + mid * IndexEntry.idx_entry_size) catch return AccessError.AccessFailure;
        if (read != entry_buf.len) return StateError.DataTruncated;
        const idx_entry = IndexEntry.overlay(&entry_buf);
        if (!idx_entry.verifyChecksum()) return StateError.ChecksumFailure;
        if (isBeforeUlid(idx_entry.frame_header.id, first_ulid)) {
            low = mid + 1;
        } else {
            high = mid;
            start_offset = idx_entry.msg_file_offset;
        }
    }
    if (start_offset) |offset| return offset;

    // Every message of the segment is before the range's start
    var msg_name_buf: [64]u8 = undefined;
    const msg_name = std.fmt.bufPrint(&msg_name_buf, MessageFile.msg_file_name_fmt, .{segment_no}) catch unreachable;
    return (dir.statFile(msg_name) catch return AccessError.AccessFailure).size;
}

/// Whether a later segment exists, so this one is no longer appended to.
fn segmentClosedNow(self: *Self, segment_no: u64) bool {
    self.mtx_dataset.lock();
    defer self.mtx_dataset.unlock();
    return segment_no + 1 < self.num_segments;
}

pub const ChannelIterator = struct {
    const chunk_len = 128;
    const hash_chunk_len = 4096;
    const no_segment = std.math.maxInt(u64);

    logger: *logging.Logger,
    channel_id: u128,
    current_segment: u64,
    first_ulid: Ulid,
    final_ulid: Ulid,
    first_segment: u64,
    final_segment: u64,
    read_dataset: *ReadDataset,
    reached_end: bool = false,
    /// Position in the channel's run in the current segment; null until it has been found
    run_pos: ?u64 = null,
    msg_buffer: []u8,
    /// Run entries read ahead from the current segment's channel index file
    chunk: [chunk_len]ChannelIndexFile.RunEntry = undefined,
    chunk_segment: u64 = no_segment,
    chunk_start: u64 = 0,
    chunk_count: usize = 0,
    /// The hash of the indexed header the read wants, if it wants one value of it
    header_hash: ?u32 = null,
    /// Hashes read ahead from the current segment's header index file
    hashes: [hash_chunk_len]u32 = undefined,
    hash_segment: u64 = no_segment,
    hash_start: u64 = 0,
    hash_count: usize = 0,

    pub const Step = union(enum) {
        message: Message,
        /// A stretch of the channel was passed over without a result; there may be more
        skipped,
        end,
    };

    fn init(read_dataset: *ReadDataset, channel_id: u128, first_ulid: Ulid, final_ulid: Ulid, first_segment: u64, final_segment: u64) AllocationError!ChannelIterator {
        // A range of one id is a range: a query starting at the store's newest message is clamped to it.
        assert(!isBeforeUlid(final_ulid, first_ulid));
        assert(first_segment <= final_segment);

        const msg_buffer = read_dataset.frame_allocator.alloc(u8, msg_limits.max_frame_size) catch return AllocationError.OutOfMemory;
        errdefer read_dataset.frame_allocator.free(msg_buffer);

        return .{
            .logger = logging.logger("msg_store.ReadDataset.ChannelIterator"),
            .channel_id = channel_id,
            .read_dataset = read_dataset,
            .first_ulid = first_ulid,
            .final_ulid = final_ulid,
            .first_segment = first_segment,
            .final_segment = final_segment,
            .current_segment = first_segment,
            .msg_buffer = msg_buffer,
        };
    }

    pub fn close(self: *ChannelIterator, frame_allocator: std.mem.Allocator) void {
        defer frame_allocator.free(self.msg_buffer);
    }

    inline fn nextSegment(self: *ChannelIterator) void {
        self.current_segment += 1;
        self.run_pos = null;
    }

    inline fn end(self: *ChannelIterator) void {
        self.run_pos = null;
        self.reached_end = true;
    }

    /// The run entry at `pos` of the channel's run in the current segment, through the read-ahead chunk.
    fn runEntry(self: *ChannelIterator, light: *LightSegment, directory: *const ChannelIndexFile.Directory, dir_entry: ChannelIndexFile.DirEntry, pos: u64) AccessError!ChannelIndexFile.RunEntry {
        if (self.chunk_segment != self.current_segment or pos < self.chunk_start or pos >= self.chunk_start + self.chunk_count) {
            self.chunk_count = try ChannelIndexFile.readRun(light.chx_file, directory, dir_entry, pos, &self.chunk);
            self.chunk_segment = self.current_segment;
            self.chunk_start = pos;
        }
        return self.chunk[@intCast(pos - self.chunk_start)];
    }

    /// Position in an in-memory channel run of the first message with id `first_ulid` or later. The segment's
    /// message index gives the offset of that id, and a run is in offset order.
    fn runLowerBound(dse: DataSegmentsCache.DataSegmentEntry, channel_run: []const ChannelIndex.ChannelRunEntry, first_ulid: Ulid) u64 {
        const index_entries = dse.message_index.entries[0..dse.message_index.append_idx];
        var low: usize = 0;
        var high: usize = index_entries.len;
        while (low < high) {
            const mid = (low + high) / 2;
            if (isBeforeUlid(index_entries[mid].id, first_ulid)) low = mid + 1 else high = mid;
        }
        if (low == index_entries.len) return channel_run.len;
        const first_offset = index_entries[low].msg_file_offset;

        low = 0;
        high = channel_run.len;
        while (low < high) {
            const mid = (low + high) / 2;
            if (channel_run[mid].file_offset < first_offset) low = mid + 1 else high = mid;
        }
        return low;
    }

    pub fn next(self: *ChannelIterator) IteratorError!?Message {
        while (true) {
            switch (try self.step()) {
                .message => |msg| return msg,
                .skipped => continue,
                .end => return null,
            }
        }
    }

    /// The channel's next message, or the news that a stretch of it held none the read wants: a read that
    /// matches little hands control back between stretches.
    pub fn step(self: *ChannelIterator) IteratorError!Step {
        const read_dataset = self.read_dataset;
        // A loop, not a recursive call per segment: a read across many segments overflowed a worker
        // thread's stack (#7).
        while (true) {
            if (self.reached_end) {
                return .end;
            }
            if (self.current_segment > self.final_segment) {
                self.end();
                return .end;
            }

            read_dataset.mtx_dataset.lock();
            defer read_dataset.mtx_dataset.unlock();

            // A closed segment: its channel index file says whether it holds the channel, where the range starts
            // in it and where each frame is. A segment without the channel costs a lookup in memory.
            if (read_dataset.channelDirectory(self.current_segment)) |directory| {
                const dir_entry = directory.find(self.channel_id) orelse {
                    self.nextSegment();
                    continue;
                };
                if (isBeforeUlid(dir_entry.last_id, self.first_ulid)) {
                    self.nextSegment();
                    continue;
                }
                if (isBeforeUlid(self.final_ulid, dir_entry.first_id)) {
                    self.end();
                    return .end;
                }

                const light = try read_dataset.lightSegment(self.current_segment);
                var pos = self.run_pos orelse found: {
                    const start = if (isBeforeUlid(dir_entry.first_id, self.first_ulid))
                        try ChannelIndexFile.lowerBound(light.chx_file, directory, dir_entry, self.first_ulid)
                    else
                        0;
                    self.run_pos = start;
                    break :found start;
                };
                if (pos >= dir_entry.run_len) {
                    self.nextSegment();
                    continue;
                }

                // A read for one value of the indexed header: pass over the messages whose hash differs
                if (self.header_hash) |wanted| {
                    if (read_dataset.headerIndexFile(light)) |hdx_file| {
                        if (self.hash_segment != self.current_segment or pos < self.hash_start or pos >= self.hash_start + self.hash_count) {
                            self.hash_count = HeaderIndexFile.readHashes(hdx_file, dir_entry, pos, &self.hashes) catch 0;
                            self.hash_segment = self.current_segment;
                            self.hash_start = pos;
                        }
                        // Unreadable: the frames are read instead
                        if (self.hash_count > 0) {
                            var at: usize = @intCast(pos - self.hash_start);
                            while (at < self.hash_count and self.hashes[at] != wanted) at += 1;
                            pos = self.hash_start + at;
                            self.run_pos = pos;
                            if (at == self.hash_count) return .skipped;
                        }
                    }
                }

                const run_entry = try self.runEntry(light, directory, dir_entry, pos);
                if (isBeforeUlid(self.final_ulid, run_entry.id)) {
                    self.end();
                    return .end;
                }
                const msg = readRunFrame(light, run_entry, self.channel_id, self.msg_buffer) catch |e| {
                    // The file does not match its segment. Positions in a run are the same in the segment cache,
                    // so the read goes on from here through it.
                    self.logger.err()
                        .msg("A segment's channel index does not match its message file; removing it")
                        .err(e)
                        .intx("channel_id", self.channel_id)
                        .int("segment_no", self.current_segment)
                        .int("run_pos", pos)
                        .log();
                    read_dataset.dropChannelDirectory(self.current_segment);
                    self.chunk_segment = no_segment;
                    continue;
                };
                self.run_pos = pos + 1;
                return .{ .message = msg };
            }

            // The segment being appended to (or one without a channel index file): through the segment cache
            const dse = try read_dataset.acquireSegment(self.current_segment);
            defer read_dataset.releaseSegment(dse);
            const channel_run = dse.channel_index.lookup(self.channel_id) orelse {
                // A segment without this channel is not the end: later segments may hold more of it.
                self.nextSegment();
                continue;
            };
            const pos = self.run_pos orelse found: {
                const start = runLowerBound(dse, channel_run, self.first_ulid);
                self.run_pos = start;
                break :found start;
            };
            if (pos >= channel_run.len) {
                self.nextSegment();
                continue;
            }

            const run_entry = channel_run[@intCast(pos)];
            const seg_loc: SegmentLocation = .{ .segment_no = self.current_segment, .file_offset = run_entry.file_offset, .frame_size = run_entry.frame_size };
            const msg = try read_dataset.fetchFromSegment(seg_loc, self.msg_buffer) orelse return IteratorError.EntryNotFound;
            if (!isSameOrBeforeUlid(msg.id(), self.final_ulid)) {
                // Overshot final message
                self.end();
                return .end;
            }
            self.run_pos = pos + 1;
            return .{ .message = msg };
        }
    }

    pub fn rewind(self: *ChannelIterator) void {
        self.current_segment = self.first_segment;
        self.reached_end = false;
        self.run_pos = null;
        self.chunk_segment = no_segment;
        self.hash_segment = no_segment;
    }
};

pub inline fn isSameOrBeforeUlid(ulid_l: Ulid, ulid_r: Ulid) bool {
    if (ulid_l.time < ulid_r.time) {
        return true;
    } else if (ulid_l.time == ulid_r.time and ulid_l.rand <= ulid_r.rand) { // DESIGN-DECISION: end-of-range: [] or [)
        return true;
    }
    return false;
}

pub inline fn isSameOrAfterUlid(ulid_l: Ulid, ulid_r: Ulid) bool {
    if (ulid_l.time > ulid_r.time) {
        return true;
    } else if (ulid_l.time == ulid_r.time and ulid_l.rand >= ulid_r.rand) {
        return true;
    }
    return false;
}

test "ReadDataset.ChannelIterator.smoke" {
    std.debug.print("test.ReadDataset.ChannelIterator.smoke\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    var fah = FrameAllocator.init(allocator);
    const frame_allocator = fah.allocator();

    var logger = logging.logger("test.ReadDataset.ChannelIterator.smoke");

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    try tmp_dir.dir.makeDir("test-channel-iterator-smoke");

    const abs_dir_name = try tmp_dir.dir.realpathAlloc(allocator, "test-channel-iterator-smoke");
    defer allocator.free(abs_dir_name);

    const timestamp = std.time.nanoTimestamp();

    var dummy_segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer dummy_segments_ulid_idx.deinit(allocator);

    try testing.expectError(OpenError.FileNotFound, ReadDataset.open(allocator, frame_allocator, &dummy_segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    }));

    // POPULATE
    var ulid_generator = common.ulid.generator();
    const first_ulid: Ulid = try ulid_generator.next();

    // const number_of_messages: usize = @min((limits.max_message_file_size - 128) / 1024 + 10, limits.max_message_file_entries + 10);
    const number_of_channels: usize = 100;
    const messages_per_channel: usize = 100;
    const number_of_messages: usize = number_of_channels * messages_per_channel;
    logger.debug()
        .msg("Will store and channel query messages")
        .int("number_of_messages", number_of_messages)
        .log();

    {
        // Open the dataset
        const Dataset = @import("Dataset.zig");
        var dataset = try Dataset.open(allocator, frame_allocator, timestamp, .{
            .dir = tmp_dir.dir,
        });
        defer dataset.close();

        // Store some messages

        const message_frame: []u8 = try frame_allocator.alloc(u8, 1024);
        defer frame_allocator.free(message_frame);
        // const message_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

        const message_body = "A" ** 896;

        logger.debug().msg("Storing block of test messages").log();
        for (0..number_of_channels) |channel_id| {
            for (0..messages_per_channel) |_idx| {
                var test_msg =
                    try Message.initOverlay(message_frame, _idx, try ulid_generator.next(), channel_id, 101, message_body, .{});
                _ = try dataset.store(&test_msg);
            }
        }
        logger.debug().msg("Test messages stored").int("number_of_messages", number_of_messages).log();

        try dataset.sync();
    }
    const final_ulid: Ulid = try ulid_generator.next();

    var segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer segments_ulid_idx.deinit(allocator);

    var read_dataset = try ReadDataset.open(allocator, frame_allocator, &segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    });
    defer read_dataset.close();

    logger.debug().msg("Hackery").log();
    _ = &first_ulid;
    _ = &final_ulid;

    logger.debug().msg("Performing channel queries").log();
    for (0..number_of_channels) |channel_id| {
        for (0..messages_per_channel) |_| {
            var it = try read_dataset.channelIterator(channel_id, first_ulid, final_ulid, 0, read_dataset.num_segments - 1);
            defer it.close(frame_allocator);

            var actual_count: usize = 0;
            while (try it.next()) |_| {
                actual_count += 1;
            }
            try testing.expectEqual(messages_per_channel, actual_count);
        }
    }
}

test "ReadDataset.ChannelIterator.bounds" {
    std.debug.print("test.ReadDataset.ChannelIterator.bounds\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    var fah = FrameAllocator.init(allocator);
    const frame_allocator = fah.allocator();

    var logger = logging.logger("test.ReadDataset.ChannelIterator.bounds");

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    try tmp_dir.dir.makeDir("test-channel-iterator-bounds");

    const abs_dir_name = try tmp_dir.dir.realpathAlloc(allocator, "test-channel-iterator-bounds");
    defer allocator.free(abs_dir_name);

    const timestamp = std.time.nanoTimestamp();

    var dummy_segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer dummy_segments_ulid_idx.deinit(allocator);

    try testing.expectError(OpenError.FileNotFound, ReadDataset.open(allocator, frame_allocator, &dummy_segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    }));

    // POPULATE
    var ulid_generator = common.ulid.generator();
    const before_first_ulid: Ulid = try ulid_generator.next();
    const first_ulid: Ulid = try ulid_generator.next();
    var final_ulid: Ulid = undefined;
    var newest_ulid: Ulid = undefined;
    const channel_id: u128 = 123456789;

    {
        // Open the dataset
        const Dataset = @import("Dataset.zig");
        var dataset = try Dataset.open(allocator, frame_allocator, timestamp, .{
            .dir = tmp_dir.dir,
        });
        defer dataset.close();

        // Store some messages

        const message_frame: []u8 = try allocator.alloc(u8, 1024);
        defer allocator.free(message_frame);
        // const message_body = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

        const message_body = "A" ** 896;

        logger.debug().msg("Test messages").log();

        var before_first_msg =
            try Message.initOverlay(message_frame, 0, before_first_ulid, channel_id, 101, message_body, .{});
        _ = try dataset.store(&before_first_msg);

        var first_msg =
            try Message.initOverlay(message_frame, 1, first_ulid, channel_id, 101, message_body, .{});
        _ = try dataset.store(&first_msg);
        for (0..3) |idx| {
            var test_msg =
                try Message.initOverlay(message_frame, idx + 2, try ulid_generator.next(), channel_id, 101, message_body, .{});
            _ = try dataset.store(&test_msg);
        }
        final_ulid = try ulid_generator.next();
        var final_msg =
            try Message.initOverlay(message_frame, 5, final_ulid, channel_id, 101, message_body, .{});
        _ = try dataset.store(&final_msg);

        newest_ulid = try ulid_generator.next();
        var after_final_msg =
            try Message.initOverlay(message_frame, 6, newest_ulid, channel_id, 101, message_body, .{});
        _ = try dataset.store(&after_final_msg);

        try dataset.sync();
    }

    var segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer segments_ulid_idx.deinit(allocator);

    var read_dataset = try ReadDataset.open(allocator, frame_allocator, &segments_ulid_idx, .{
        .dir = tmp_dir.dir,
    });
    defer read_dataset.close();

    logger.debug().msg("Performing channel query").log();
    {
        var it = try read_dataset.channelIterator(channel_id, first_ulid, final_ulid, 0, 0);
        defer it.close(frame_allocator);

        var actual_count: usize = 0;
        while (try it.next()) |_| {
            actual_count += 1;
        }
        logger.debug()
            .msg("Expecting 5 results")
            .int("actual_count", actual_count)
            .log();
        try testing.expectEqual(5, actual_count);
    }

    // A range of one id, the store's newest message: that message, not a panic
    {
        var it = try read_dataset.channelIterator(channel_id, newest_ulid, newest_ulid, 0, 0);
        defer it.close(frame_allocator);

        const only = try it.next() orelse return error.TestExpectedMessage;
        try testing.expectEqual(newest_ulid, only.id());
        try testing.expectEqual(null, try it.next());
    }
}

test "ReadDataset.ChannelIterator.channel index files" {
    std.debug.print("test.ReadDataset.ChannelIterator.channel index files\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    var fah = FrameAllocator.init(allocator);
    const frame_allocator = fah.allocator();

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    // Two closed segments and a third being appended to. Three busy channels take turns; a rare channel has a
    // message every 50,000, so each segment holds a few of it.
    const number_of_messages: usize = 2 * limits.max_message_file_entries + 1000;
    const rare_every: usize = 50_000;
    const rare_channel: u128 = 99;
    const absent_channel: u128 = 12345;
    const ids = try allocator.alloc(Ulid, number_of_messages);
    defer allocator.free(ids);
    const channelOf = struct {
        fn of(idx: usize) u128 {
            return if (idx % rare_every == 7) rare_channel else idx % 3;
        }
    }.of;

    {
        const Dataset = @import("Dataset.zig");
        var dataset = try Dataset.open(allocator, frame_allocator, std.time.nanoTimestamp(), .{ .dir = tmp_dir.dir });
        defer dataset.close();

        const message_frame: []u8 = try allocator.alloc(u8, 512);
        defer allocator.free(message_frame);
        var ulid_generator = common.ulid.generator();
        for (0..number_of_messages) |idx| {
            ids[idx] = try ulid_generator.next();
            var test_msg = try Message.initOverlay(message_frame, idx, ids[idx], channelOf(idx), 101, "A" ** 64, .{});
            _ = try dataset.store(&test_msg);
        }
        try dataset.sync();
    }

    var segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer segments_ulid_idx.deinit(allocator);
    var read_dataset = try ReadDataset.open(allocator, frame_allocator, &segments_ulid_idx, .{ .dir = tmp_dir.dir });
    defer read_dataset.close();
    try testing.expectEqual(3, read_dataset.num_segments);

    try read_dataset.prepareChannelIndex();
    try read_dataset.populateSegmentsUlidIndex(&segments_ulid_idx);
    // The closed segments have a channel index file and directory; the one being appended to has neither
    try tmp_dir.dir.access("dymes_0.chx", .{});
    try tmp_dir.dir.access("dymes_1.chx", .{});
    try testing.expectError(error.FileNotFound, tmp_dir.dir.access("dymes_2.chx", .{}));
    try testing.expect(read_dataset.channelDirectory(0) != null and read_dataset.channelDirectory(1) != null);
    try testing.expect(read_dataset.channelDirectory(2) == null);
    try testing.expect(read_dataset.channelDirectory(0).?.find(absent_channel) == null);
    try testing.expectEqual(ids[0], read_dataset.channelDirectory(0).?.first_id);

    const Check = struct {
        /// Reads `channel` over ids[from..to] (both included) and expects exactly its messages there, in order.
        fn range(rd: *ReadDataset, fa: std.mem.Allocator, all_ids: []const Ulid, channel: u128, from: usize, to: usize, of: *const fn (usize) u128) !void {
            var it = try rd.channelIterator(channel, all_ids[from], all_ids[to], 0, rd.num_segments - 1);
            defer it.close(fa);
            for (from..to + 1) |idx| {
                if (of(idx) != channel) continue;
                const msg = try it.next() orelse return error.TestExpectedMessage;
                try testing.expectEqual(all_ids[idx], msg.id());
                try testing.expectEqual(channel, msg.frame_header.channel);
            }
            try testing.expectEqual(null, try it.next());
        }
    };
    const last = number_of_messages - 1;
    const seg = limits.max_message_file_entries;

    // Whole store, across both closed segments and the open one
    try Check.range(&read_dataset, frame_allocator, ids, 0, 0, last, channelOf);
    try Check.range(&read_dataset, frame_allocator, ids, rare_channel, 0, last, channelOf);
    // From the middle of a closed segment into the next, and into the open one
    try Check.range(&read_dataset, frame_allocator, ids, 1, seg / 2, seg + 10, channelOf);
    try Check.range(&read_dataset, frame_allocator, ids, 2, seg + seg / 2, last - 5, channelOf);
    // Wholly inside one closed segment, and wholly inside the open one
    try Check.range(&read_dataset, frame_allocator, ids, 0, 1000, 2000, channelOf);
    try Check.range(&read_dataset, frame_allocator, ids, 1, last - 500, last - 100, channelOf);
    // A range that holds none of the rare channel, and a channel the store does not hold
    try Check.range(&read_dataset, frame_allocator, ids, rare_channel, 100, 40_000, channelOf);
    try Check.range(&read_dataset, frame_allocator, ids, absent_channel, 0, last, channelOf);

    // A channel index file that does not match its segment is dropped mid-read, and the read goes on
    {
        var chx = try tmp_dir.dir.openFile("dymes_1.chx", .{ .mode = .read_write });
        defer chx.close();
        const directory = read_dataset.channelDirectory(1).?;
        const dir_entry = directory.find(1).?;
        // Point the channel's 11th message in the segment somewhere else
        var bad: ChannelIndexFile.RunEntry = undefined;
        try testing.expectEqual(@sizeOf(ChannelIndexFile.RunEntry), try chx.preadAll(std.mem.asBytes(&bad), directory.runOffset(dir_entry, 10)));
        bad.file_offset += 16;
        try chx.pwriteAll(std.mem.asBytes(&bad), directory.runOffset(dir_entry, 10));
    }
    try Check.range(&read_dataset, frame_allocator, ids, 1, seg / 2, seg + seg / 2, channelOf);
    try testing.expect(read_dataset.channelDirectory(1) == null);
    try testing.expectError(error.FileNotFound, tmp_dir.dir.access("dymes_1.chx", .{}));

    // The next time the store is opened the file is rebuilt; a truncated one is too
    {
        var chx = try tmp_dir.dir.openFile("dymes_0.chx", .{ .mode = .read_write });
        defer chx.close();
        try chx.setEndPos(1000);
    }
    try testing.expectEqual(null, try ChannelIndexFile.load(allocator, tmp_dir.dir, 0));
    var reopened_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer reopened_ulid_idx.deinit(allocator);
    var reopened = try ReadDataset.open(allocator, frame_allocator, &reopened_ulid_idx, .{ .dir = tmp_dir.dir });
    defer reopened.close();
    try reopened.prepareChannelIndex();
    try reopened.populateSegmentsUlidIndex(&reopened_ulid_idx);
    try testing.expect(reopened.channelDirectory(0) != null and reopened.channelDirectory(1) != null);
    try Check.range(&reopened, frame_allocator, ids, 1, 0, last, channelOf);
    try Check.range(&reopened, frame_allocator, ids, rare_channel, seg - 10, last, channelOf);
}

test "ReadDataset.RangeIterator.closed segments and range start" {
    std.debug.print("test.ReadDataset.RangeIterator.closed segments and range start\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    var fah = FrameAllocator.init(allocator);
    const frame_allocator = fah.allocator();

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    // Two closed segments and a third being appended to. Ids are two milliseconds apart, so an id can fall
    // between two messages.
    const number_of_messages: usize = 2 * limits.max_message_file_entries + 1000;
    const base_ms: u48 = 1_700_000_000_000;
    const idOf = struct {
        fn of(idx: usize) Ulid {
            return .{ .time = base_ms + @as(u48, @intCast(idx)) * 2, .rand = 0 };
        }
    }.of;
    {
        const Dataset = @import("Dataset.zig");
        var dataset = try Dataset.open(allocator, frame_allocator, std.time.nanoTimestamp(), .{ .dir = tmp_dir.dir });
        defer dataset.close();
        const message_frame: []u8 = try allocator.alloc(u8, 512);
        defer allocator.free(message_frame);
        for (0..number_of_messages) |idx| {
            var test_msg = try Message.initOverlay(message_frame, idx, idOf(idx), idx % 5, 101, "A" ** 64, .{});
            _ = try dataset.store(&test_msg);
        }
        try dataset.sync();
    }

    var segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer segments_ulid_idx.deinit(allocator);
    var read_dataset = try ReadDataset.open(allocator, frame_allocator, &segments_ulid_idx, .{ .dir = tmp_dir.dir });
    defer read_dataset.close();
    try read_dataset.populateSegmentsUlidIndex(&segments_ulid_idx);
    try testing.expectEqual(3, read_dataset.num_segments);

    const seg = limits.max_message_file_entries;
    const Case = struct { from: usize, between: bool, to: usize };
    const cases = [_]Case{
        // From the middle of a closed segment into the next
        .{ .from = seg / 2, .between = false, .to = seg + 10 },
        // The range starts between two messages: the earlier one is not part of it
        .{ .from = seg / 2, .between = true, .to = seg / 2 + 100 },
        .{ .from = seg + 7, .between = true, .to = 2 * seg + 500 },
        // Inside the segment being appended to
        .{ .from = 2 * seg + 100, .between = true, .to = 2 * seg + 900 },
        // The whole store
        .{ .from = 0, .between = false, .to = number_of_messages - 1 },
    };
    for (cases) |case| {
        var first_ulid = idOf(case.from);
        if (case.between) first_ulid.time += 1;
        const first_expected = if (case.between) case.from + 1 else case.from;
        const first_segment = segments_ulid_idx.lookup(idOf(first_expected)).?;
        const final_segment = segments_ulid_idx.lookup(idOf(case.to)).?;

        var it = read_dataset.rangeIterator(first_ulid, idOf(case.to), first_segment, final_segment);
        defer it.close();
        for (first_expected..case.to + 1) |idx| {
            const msg = try it.next() orelse return error.TestExpectedMessage;
            try testing.expectEqual(idOf(idx), msg.id());
        }
        try testing.expectEqual(null, try it.next());
    }
}

test "ReadDataset.ChannelIterator.header index" {
    std.debug.print("test.ReadDataset.ChannelIterator.header index\n", .{});
    const allocator = testing.allocator;
    const out_buffer = try allocator.alloc(u8, 2048);
    defer allocator.free(out_buffer);
    var stderr_writer = std.fs.File.stderr().writer(out_buffer);

    const prev_default_logging_filter_level = logging.default_logging_filter_level;
    defer logging.default_logging_filter_level = prev_default_logging_filter_level;
    logging.default_logging_filter_level = logging.LogLevel.none;

    try logging.init(allocator, &stderr_writer);
    defer logging.deinit();
    var fah = FrameAllocator.init(allocator);
    const frame_allocator = fah.allocator();

    var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_dir.cleanup();

    // Two closed segments and a third being appended to. Three channels take turns. Messages come in runs of
    // 3,000, each run with its own value of the header "run"; every 11th message has no such header.
    const number_of_messages: usize = 2 * limits.max_message_file_entries + 1000;
    const run_len: usize = 3000;
    const ids = try allocator.alloc(Ulid, number_of_messages);
    defer allocator.free(ids);
    const Shape = struct {
        fn channelOf(idx: usize) u128 {
            return idx % 3;
        }
        fn runOf(idx: usize) ?usize {
            return if (idx % 11 == 5) null else idx / run_len;
        }
    };

    {
        const Dataset = @import("Dataset.zig");
        var dataset = try Dataset.open(allocator, frame_allocator, std.time.nanoTimestamp(), .{ .dir = tmp_dir.dir });
        defer dataset.close();

        const message_frame: []u8 = try allocator.alloc(u8, 512);
        defer allocator.free(message_frame);
        var ulid_generator = common.ulid.generator();
        var value_buf: [32]u8 = undefined;
        for (0..number_of_messages) |idx| {
            ids[idx] = try ulid_generator.next();
            var headers = std.StringArrayHashMap([]const u8).init(allocator);
            defer headers.deinit();
            try headers.put("kind", "test");
            if (Shape.runOf(idx)) |run| try headers.put("run", try std.fmt.bufPrint(&value_buf, "run-{d}", .{run}));
            var test_msg = try Message.initOverlay(message_frame, idx, ids[idx], Shape.channelOf(idx), 101, "A" ** 32, .{ .transient_kv_headers = headers });
            _ = try dataset.store(&test_msg);
        }
        try dataset.sync();
    }

    const Check = struct {
        const Counts = struct { results: usize, fetched: usize };

        /// Reads `channel` over ids[from..to] (both included) for the run `run`, as a channel read with a header
        /// filter does, and expects exactly the run's messages on the channel there, in order. Returns how many
        /// frames the iterator fetched to find them.
        fn lookup(rd: *ReadDataset, fa: std.mem.Allocator, all_ids: []const Ulid, channel: u128, from: usize, to: usize, run: usize) !Counts {
            var value_buf: [32]u8 = undefined;
            const value = try std.fmt.bufPrint(&value_buf, "run-{d}", .{run});
            const filters = [_]dymes_msg.Filter{.{ .kv_filter = .{ .eql = .{ .key = "run", .value = value } } }};
            var it = try rd.channelIteratorFor(channel, all_ids[from], all_ids[to], 0, rd.num_segments - 1, rd.headerHashFor(&filters));
            defer it.close(fa);
            var counts: Counts = .{ .results = 0, .fetched = 0 };
            var expected = from;
            while (try it.next()) |msg| {
                counts.fetched += 1;
                if (!filters[0].apply(msg)) continue;
                while (expected <= to and (Shape.channelOf(expected) != channel or Shape.runOf(expected) != run)) expected += 1;
                if (expected > to) return error.TestUnexpectedMessage;
                try testing.expectEqual(all_ids[expected], msg.id());
                expected += 1;
                counts.results += 1;
            }
            while (expected <= to) : (expected += 1) {
                if (Shape.channelOf(expected) == channel and Shape.runOf(expected) == run) return error.TestExpectedMessage;
            }
            return counts;
        }

        fn ready(rd: *ReadDataset, segments: u64) !void {
            var waited: usize = 0;
            while (!(rd.headerIndexReady(0) and (segments < 2 or rd.headerIndexReady(1)))) : (waited += 1) {
                if (waited > 60_000) return error.TestHeaderIndexNotBuilt;
                std.Thread.sleep(std.time.ns_per_ms);
            }
        }
    };
    const last = number_of_messages - 1;
    const seg = limits.max_message_file_entries;
    const in_first = 17; // a run wholly inside the first closed segment
    const across = seg / run_len; // the run that is being written when the first segment closes
    const in_open = last / run_len; // the newest run, in the segment being appended to

    var segments_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer segments_ulid_idx.deinit(allocator);
    {
        var read_dataset = try ReadDataset.open(allocator, frame_allocator, &segments_ulid_idx, .{ .dir = tmp_dir.dir });
        defer read_dataset.close();
        try read_dataset.prepareChannelIndex();
        try read_dataset.populateSegmentsUlidIndex(&segments_ulid_idx);

        // Without a header index every frame of the channel in the range is fetched
        try testing.expectEqual(null, read_dataset.headerHashFor(&.{.{ .kv_filter = .{ .eql = .{ .key = "run", .value = "run-17" } } }}));
        const plain = try Check.lookup(&read_dataset, frame_allocator, ids, 1, 0, seg - 1, in_first);
        try testing.expect(plain.results > 800 and plain.fetched > seg / 4);

        try read_dataset.startHeaderIndex("run");
        try Check.ready(&read_dataset, 2);
        try tmp_dir.dir.access("dymes_0.hdx", .{});
        try tmp_dir.dir.access("dymes_1.hdx", .{});
        try testing.expectError(error.FileNotFound, tmp_dir.dir.access("dymes_2.hdx", .{}));
        // Another header is not indexed
        try testing.expectEqual(null, read_dataset.headerHashFor(&.{.{ .kv_filter = .{ .eql = .{ .key = "kind", .value = "test" } } }}));

        // With it, a read inside the closed segments fetches the run's frames and no others
        const indexed = try Check.lookup(&read_dataset, frame_allocator, ids, 1, 0, seg - 1, in_first);
        try testing.expectEqual(plain.results, indexed.results);
        try testing.expectEqual(indexed.results, indexed.fetched);
        // From the middle of a run; a run across the two closed segments; a range that ends inside the run
        _ = try Check.lookup(&read_dataset, frame_allocator, ids, 0, in_first * run_len + 100, seg + 5, in_first);
        const spanning = try Check.lookup(&read_dataset, frame_allocator, ids, 2, 0, 2 * seg - 1, across);
        try testing.expectEqual(spanning.results, spanning.fetched);
        _ = try Check.lookup(&read_dataset, frame_allocator, ids, 2, 0, across * run_len + 500, across);
        // To the store's end: the segment being appended to has no header index, and is read as before
        const to_end = try Check.lookup(&read_dataset, frame_allocator, ids, 0, 0, last, in_open);
        try testing.expect(to_end.results > 0 and to_end.fetched >= to_end.results);
        _ = try Check.lookup(&read_dataset, frame_allocator, ids, 1, seg / 2, last, in_first);
        // A value no message has
        const none = try Check.lookup(&read_dataset, frame_allocator, ids, 1, 0, 2 * seg - 1, 1_000_000);
        try testing.expectEqual(0, none.results);
    }

    // A damaged file, and one built for another header, are not trusted
    try testing.expect(HeaderIndexFile.valid(allocator, tmp_dir.dir, 0, "run"));
    try testing.expect(!HeaderIndexFile.valid(allocator, tmp_dir.dir, 0, "kind"));
    {
        var hdx = try tmp_dir.dir.openFile("dymes_0.hdx", .{ .mode = .read_write });
        defer hdx.close();
        try hdx.pwriteAll(&[_]u8{ 0xA5, 0xA5, 0xA5, 0xA5 }, @sizeOf(HeaderIndexFile.Header) + 4 * 1234);
    }
    try testing.expect(!HeaderIndexFile.valid(allocator, tmp_dir.dir, 0, "run"));
    try tmp_dir.dir.deleteFile("dymes_1.hdx");

    // The next time the store is opened both are rebuilt
    var reopened_ulid_idx = try SegmentsUlidIndex.init(allocator, 10);
    defer reopened_ulid_idx.deinit(allocator);
    var reopened = try ReadDataset.open(allocator, frame_allocator, &reopened_ulid_idx, .{ .dir = tmp_dir.dir });
    defer reopened.close();
    try reopened.prepareChannelIndex();
    try reopened.populateSegmentsUlidIndex(&reopened_ulid_idx);
    try reopened.startHeaderIndex("run");
    try Check.ready(&reopened, 2);
    try testing.expect(HeaderIndexFile.valid(allocator, tmp_dir.dir, 0, "run"));
    try testing.expect(HeaderIndexFile.valid(allocator, tmp_dir.dir, 1, "run"));
    const rebuilt = try Check.lookup(&reopened, frame_allocator, ids, 1, 0, seg - 1, in_first);
    try testing.expectEqual(rebuilt.results, rebuilt.fetched);

    // When its channel index file is dropped a segment's header index goes with it, and reads still find the run
    reopened.mtx_dataset.lock();
    reopened.dropChannelDirectory(0);
    reopened.mtx_dataset.unlock();
    try testing.expect(!reopened.headerIndexReady(0));
    try testing.expectError(error.FileNotFound, tmp_dir.dir.access("dymes_0.hdx", .{}));
    const dropped = try Check.lookup(&reopened, frame_allocator, ids, 1, 0, seg - 1, in_first);
    try testing.expectEqual(rebuilt.results, dropped.results);
}
