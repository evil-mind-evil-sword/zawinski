const std = @import("std");
const sqlite = @import("sqlite.zig");
const ids = @import("ids.zig");

pub const StoreError = error{
    StoreNotFound,
    TopicNotFound,
    TopicExists,
    MessageNotFound,
    MessageIdAmbiguous,
    InvalidMessageId,
    ParentNotFound,
    DatabaseBusy,
    EmptyTopicName,
    EmptyMessageBody,
} || sqlite.Error;

pub const Topic = struct {
    id: []const u8,
    name: []const u8,
    description: []const u8,
    created_at: i64,

    pub fn deinit(self: *Topic, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.name);
        allocator.free(self.description);
    }
};

pub const Message = struct {
    id: []const u8,
    topic_id: []const u8,
    parent_id: ?[]const u8,
    body: []const u8,
    created_at: i64,
    reply_count: i32 = 0,

    pub fn deinit(self: *Message, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.topic_id);
        if (self.parent_id) |pid| allocator.free(pid);
        allocator.free(self.body);
    }
};

pub const Store = struct {
    allocator: std.mem.Allocator,
    db: *sqlite.c.sqlite3,
    store_dir: []const u8,
    db_path: []const u8,
    jsonl_path: []const u8,
    ulid: ids.Generator,
    lock_file: ?std.fs.File = null,

    const schema_sql =
        \\CREATE TABLE IF NOT EXISTS topics (
        \\  id TEXT PRIMARY KEY,
        \\  name TEXT NOT NULL UNIQUE,
        \\  description TEXT NOT NULL,
        \\  created_at INTEGER NOT NULL
        \\);
        \\
        \\CREATE TABLE IF NOT EXISTS messages (
        \\  id TEXT PRIMARY KEY,
        \\  topic_id TEXT NOT NULL,
        \\  parent_id TEXT,
        \\  body TEXT NOT NULL,
        \\  created_at INTEGER NOT NULL,
        \\  FOREIGN KEY(topic_id) REFERENCES topics(id) ON DELETE CASCADE,
        \\  FOREIGN KEY(parent_id) REFERENCES messages(id) ON DELETE CASCADE
        \\);
        \\
        \\CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages(topic_id, created_at);
        \\CREATE INDEX IF NOT EXISTS idx_messages_parent ON messages(parent_id);
        \\
        \\CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(body, content=messages, content_rowid=rowid);
        \\
        \\CREATE TABLE IF NOT EXISTS meta (
        \\  key TEXT PRIMARY KEY,
        \\  value TEXT NOT NULL
        \\);
    ;

    pub fn init(allocator: std.mem.Allocator, dir: []const u8) !void {
        // Create .zawinski directory
        std.fs.makeDirAbsolute(dir) catch |err| switch (err) {
            error.PathAlreadyExists => return error.StoreNotFound, // Already exists
            else => return err,
        };

        // Create empty messages.jsonl
        const jsonl_path = try std.fs.path.join(allocator, &.{ dir, "messages.jsonl" });
        defer allocator.free(jsonl_path);
        const jsonl_file = try std.fs.createFileAbsolute(jsonl_path, .{ .truncate = false });
        jsonl_file.close();

        // Create .gitignore
        const gitignore_path = try std.fs.path.join(allocator, &.{ dir, ".gitignore" });
        defer allocator.free(gitignore_path);
        const gitignore = try std.fs.createFileAbsolute(gitignore_path, .{ .truncate = true });
        defer gitignore.close();
        try gitignore.writeAll("*.db\n*.db-wal\n*.db-shm\nlock\n");
    }

    pub fn open(allocator: std.mem.Allocator, store_dir: []const u8) !Store {
        const db_path = try std.fs.path.join(allocator, &.{ store_dir, "messages.db" });
        errdefer allocator.free(db_path);

        const jsonl_path = try std.fs.path.join(allocator, &.{ store_dir, "messages.jsonl" });
        errdefer allocator.free(jsonl_path);

        const db = try sqlite.open(db_path);
        errdefer sqlite.close(db);

        var self = Store{
            .allocator = allocator,
            .db = db,
            .store_dir = try allocator.dupe(u8, store_dir),
            .db_path = db_path,
            .jsonl_path = jsonl_path,
            .ulid = ids.Generator.init(@as(u64, @intCast(std.time.nanoTimestamp()))),
        };

        try self.initLock();
        try self.setPragmas();
        try self.ensureSchema();

        return self;
    }

    pub fn deinit(self: *Store) void {
        if (self.lock_file) |lf| {
            lf.close();
        }
        sqlite.close(self.db);
        self.allocator.free(self.store_dir);
        self.allocator.free(self.db_path);
        self.allocator.free(self.jsonl_path);
    }

    fn initLock(self: *Store) !void {
        const lock_path = try std.fs.path.join(self.allocator, &.{ self.store_dir, "lock" });
        defer self.allocator.free(lock_path);
        const file = std.fs.createFileAbsolute(lock_path, .{
            .truncate = false,
            .read = true,
            .mode = 0o600,
        }) catch |err| switch (err) {
            error.PathAlreadyExists => try std.fs.openFileAbsolute(lock_path, .{ .mode = .read_write }),
            else => return err,
        };
        self.lock_file = file;
    }

    fn setPragmas(self: *Store) !void {
        try sqlite.exec(self.db, "PRAGMA journal_mode=WAL;");
        try sqlite.exec(self.db, "PRAGMA synchronous=NORMAL;");
        try sqlite.exec(self.db, "PRAGMA busy_timeout=300000;");
        try sqlite.exec(self.db, "PRAGMA temp_store=MEMORY;");
        try sqlite.exec(self.db, "PRAGMA foreign_keys=ON;");
    }

    fn ensureSchema(self: *Store) !void {
        try sqlite.exec(self.db, schema_sql);
    }

    // ========== Topic Operations ==========

    pub fn createTopic(self: *Store, name: []const u8, description: []const u8) ![]u8 {
        // Validate input
        const trimmed_name = std.mem.trim(u8, name, " \t\r\n");
        if (trimmed_name.len == 0) return StoreError.EmptyTopicName;

        const now_ms = @as(i64, @intCast(std.time.milliTimestamp()));
        const id = try self.ulid.nextNow(self.allocator);
        errdefer self.allocator.free(id);

        try self.beginImmediate();
        errdefer sqlite.exec(self.db, "ROLLBACK;") catch {};

        // Insert into DB
        const stmt = try sqlite.prepare(self.db, "INSERT INTO topics (id, name, description, created_at) VALUES (?, ?, ?, ?);");
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, id);
        try sqlite.bindText(stmt, 2, name);
        try sqlite.bindText(stmt, 3, description);
        try sqlite.bindInt64(stmt, 4, now_ms);
        _ = sqlite.step(stmt) catch |err| {
            if (err == sqlite.Error.SqliteError or err == sqlite.Error.SqliteStepError) {
                return StoreError.TopicExists;
            }
            return err;
        };

        // Append to JSONL
        try self.appendTopicJsonl(id, name, description, now_ms);

        try self.commit();
        return id;
    }

    pub fn fetchTopic(self: *Store, name: []const u8) !Topic {
        const stmt = try sqlite.prepare(self.db, "SELECT id, name, description, created_at FROM topics WHERE name = ?;");
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, name);

        if (try sqlite.step(stmt)) {
            return Topic{
                .id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 0)),
                .name = try self.allocator.dupe(u8, sqlite.columnText(stmt, 1)),
                .description = try self.allocator.dupe(u8, sqlite.columnText(stmt, 2)),
                .created_at = sqlite.columnInt64(stmt, 3),
            };
        }
        return StoreError.TopicNotFound;
    }

    pub fn listTopics(self: *Store) ![]Topic {
        var topics: std.ArrayList(Topic) = .empty;
        errdefer {
            for (topics.items) |*t| t.deinit(self.allocator);
            topics.deinit(self.allocator);
        }

        const stmt = try sqlite.prepare(self.db, "SELECT id, name, description, created_at FROM topics ORDER BY created_at DESC;");
        defer sqlite.finalize(stmt);

        while (try sqlite.step(stmt)) {
            try topics.append(self.allocator, .{
                .id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 0)),
                .name = try self.allocator.dupe(u8, sqlite.columnText(stmt, 1)),
                .description = try self.allocator.dupe(u8, sqlite.columnText(stmt, 2)),
                .created_at = sqlite.columnInt64(stmt, 3),
            });
        }

        return topics.toOwnedSlice(self.allocator);
    }

    // ========== Message Operations ==========

    pub fn createMessage(self: *Store, topic_name: []const u8, parent_id: ?[]const u8, body: []const u8) ![]u8 {
        // Validate input
        const trimmed_body = std.mem.trim(u8, body, " \t\r\n");
        if (trimmed_body.len == 0) return StoreError.EmptyMessageBody;

        const now_ms = @as(i64, @intCast(std.time.milliTimestamp()));

        // Resolve topic
        const topic = try self.fetchTopic(topic_name);
        defer {
            var t = topic;
            t.deinit(self.allocator);
        }

        // Validate parent if provided
        if (parent_id) |pid| {
            const parent_exists = try self.messageExists(pid);
            if (!parent_exists) return StoreError.ParentNotFound;
        }

        const id = try self.ulid.nextNow(self.allocator);
        errdefer self.allocator.free(id);

        try self.beginImmediate();
        errdefer sqlite.exec(self.db, "ROLLBACK;") catch {};

        // Insert into DB
        const stmt = try sqlite.prepare(self.db, "INSERT INTO messages (id, topic_id, parent_id, body, created_at) VALUES (?, ?, ?, ?, ?);");
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, id);
        try sqlite.bindText(stmt, 2, topic.id);
        if (parent_id) |pid| {
            try sqlite.bindText(stmt, 3, pid);
        } else {
            try sqlite.bindNull(stmt, 3);
        }
        try sqlite.bindText(stmt, 4, body);
        try sqlite.bindInt64(stmt, 5, now_ms);
        _ = try sqlite.step(stmt);

        // Update FTS
        const fts_stmt = try sqlite.prepare(self.db, "INSERT INTO messages_fts(rowid, body) VALUES (last_insert_rowid(), ?);");
        defer sqlite.finalize(fts_stmt);
        try sqlite.bindText(fts_stmt, 1, body);
        _ = try sqlite.step(fts_stmt);

        // Append to JSONL
        try self.appendMessageJsonl(id, topic.id, parent_id, body, now_ms);

        try self.commit();
        return id;
    }

    pub fn fetchMessage(self: *Store, id: []const u8) !Message {
        const resolved_id = try self.resolveMessageId(id);
        defer self.allocator.free(resolved_id);

        const stmt = try sqlite.prepare(self.db,
            \\SELECT m.id, m.topic_id, m.parent_id, m.body, m.created_at,
            \\  (SELECT COUNT(*) FROM messages r WHERE r.parent_id = m.id)
            \\FROM messages m WHERE m.id = ?;
        );
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, resolved_id);

        if (try sqlite.step(stmt)) {
            const parent_text = sqlite.columnText(stmt, 2);
            return Message{
                .id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 0)),
                .topic_id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 1)),
                .parent_id = if (parent_text.len > 0) try self.allocator.dupe(u8, parent_text) else null,
                .body = try self.allocator.dupe(u8, sqlite.columnText(stmt, 3)),
                .created_at = sqlite.columnInt64(stmt, 4),
                .reply_count = sqlite.columnInt(stmt, 5),
            };
        }
        return StoreError.MessageNotFound;
    }

    pub fn listMessages(self: *Store, topic_name: []const u8, limit: u32) ![]Message {
        const topic = try self.fetchTopic(topic_name);
        defer {
            var t = topic;
            t.deinit(self.allocator);
        }

        var messages: std.ArrayList(Message) = .empty;
        errdefer {
            for (messages.items) |*m| m.deinit(self.allocator);
            messages.deinit(self.allocator);
        }

        const stmt = try sqlite.prepare(self.db,
            \\SELECT m.id, m.topic_id, m.parent_id, m.body, m.created_at,
            \\  (SELECT COUNT(*) FROM messages r WHERE r.parent_id = m.id)
            \\FROM messages m
            \\WHERE m.topic_id = ? AND m.parent_id IS NULL
            \\ORDER BY m.created_at DESC
            \\LIMIT ?;
        );
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, topic.id);
        try sqlite.bindInt(stmt, 2, @as(i32, @intCast(limit)));

        while (try sqlite.step(stmt)) {
            const parent_text = sqlite.columnText(stmt, 2);
            try messages.append(self.allocator, .{
                .id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 0)),
                .topic_id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 1)),
                .parent_id = if (parent_text.len > 0) try self.allocator.dupe(u8, parent_text) else null,
                .body = try self.allocator.dupe(u8, sqlite.columnText(stmt, 3)),
                .created_at = sqlite.columnInt64(stmt, 4),
                .reply_count = sqlite.columnInt(stmt, 5),
            });
        }

        return messages.toOwnedSlice(self.allocator);
    }

    pub fn fetchThread(self: *Store, message_id: []const u8) ![]Message {
        const resolved_id = try self.resolveMessageId(message_id);
        defer self.allocator.free(resolved_id);

        var messages: std.ArrayList(Message) = .empty;
        errdefer {
            for (messages.items) |*m| m.deinit(self.allocator);
            messages.deinit(self.allocator);
        }

        const stmt = try sqlite.prepare(self.db,
            \\WITH RECURSIVE thread(id, topic_id, parent_id, body, created_at, depth) AS (
            \\  SELECT id, topic_id, parent_id, body, created_at, 0 FROM messages WHERE id = ?
            \\  UNION ALL
            \\  SELECT m.id, m.topic_id, m.parent_id, m.body, m.created_at, t.depth + 1
            \\  FROM messages m JOIN thread t ON m.parent_id = t.id
            \\)
            \\SELECT t.id, t.topic_id, t.parent_id, t.body, t.created_at,
            \\  (SELECT COUNT(*) FROM messages r WHERE r.parent_id = t.id)
            \\FROM thread t ORDER BY t.created_at;
        );
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, resolved_id);

        while (try sqlite.step(stmt)) {
            const parent_text = sqlite.columnText(stmt, 2);
            try messages.append(self.allocator, .{
                .id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 0)),
                .topic_id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 1)),
                .parent_id = if (parent_text.len > 0) try self.allocator.dupe(u8, parent_text) else null,
                .body = try self.allocator.dupe(u8, sqlite.columnText(stmt, 3)),
                .created_at = sqlite.columnInt64(stmt, 4),
                .reply_count = sqlite.columnInt(stmt, 5),
            });
        }

        return messages.toOwnedSlice(self.allocator);
    }

    pub fn fetchReplies(self: *Store, message_id: []const u8) ![]Message {
        const resolved_id = try self.resolveMessageId(message_id);
        defer self.allocator.free(resolved_id);

        var messages: std.ArrayList(Message) = .empty;
        errdefer {
            for (messages.items) |*m| m.deinit(self.allocator);
            messages.deinit(self.allocator);
        }

        const stmt = try sqlite.prepare(self.db,
            \\SELECT m.id, m.topic_id, m.parent_id, m.body, m.created_at,
            \\  (SELECT COUNT(*) FROM messages r WHERE r.parent_id = m.id)
            \\FROM messages m WHERE m.parent_id = ?
            \\ORDER BY m.created_at;
        );
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, resolved_id);

        while (try sqlite.step(stmt)) {
            const parent_text = sqlite.columnText(stmt, 2);
            try messages.append(self.allocator, .{
                .id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 0)),
                .topic_id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 1)),
                .parent_id = if (parent_text.len > 0) try self.allocator.dupe(u8, parent_text) else null,
                .body = try self.allocator.dupe(u8, sqlite.columnText(stmt, 3)),
                .created_at = sqlite.columnInt64(stmt, 4),
                .reply_count = sqlite.columnInt(stmt, 5),
            });
        }

        return messages.toOwnedSlice(self.allocator);
    }

    pub fn searchMessages(self: *Store, query: []const u8, topic_name: ?[]const u8, limit: u32) ![]Message {
        // Sanitize FTS5 query - wrap in quotes and escape internal quotes
        const safe_query = try self.sanitizeFts5Query(query);
        defer self.allocator.free(safe_query);

        var messages: std.ArrayList(Message) = .empty;
        errdefer {
            for (messages.items) |*m| m.deinit(self.allocator);
            messages.deinit(self.allocator);
        }

        if (topic_name) |tn| {
            const topic = try self.fetchTopic(tn);
            defer {
                var t = topic;
                t.deinit(self.allocator);
            }

            const stmt = try sqlite.prepare(self.db,
                \\SELECT m.id, m.topic_id, m.parent_id, m.body, m.created_at,
                \\  (SELECT COUNT(*) FROM messages r WHERE r.parent_id = m.id)
                \\FROM messages_fts
                \\JOIN messages m ON m.rowid = messages_fts.rowid
                \\WHERE messages_fts MATCH ? AND m.topic_id = ?
                \\ORDER BY bm25(messages_fts), m.created_at DESC
                \\LIMIT ?;
            );
            defer sqlite.finalize(stmt);
            try sqlite.bindText(stmt, 1, safe_query);
            try sqlite.bindText(stmt, 2, topic.id);
            try sqlite.bindInt(stmt, 3, @as(i32, @intCast(limit)));

            while (try sqlite.step(stmt)) {
                const parent_text = sqlite.columnText(stmt, 2);
                try messages.append(self.allocator, .{
                    .id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 0)),
                    .topic_id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 1)),
                    .parent_id = if (parent_text.len > 0) try self.allocator.dupe(u8, parent_text) else null,
                    .body = try self.allocator.dupe(u8, sqlite.columnText(stmt, 3)),
                    .created_at = sqlite.columnInt64(stmt, 4),
                    .reply_count = sqlite.columnInt(stmt, 5),
                });
            }
        } else {
            const stmt = try sqlite.prepare(self.db,
                \\SELECT m.id, m.topic_id, m.parent_id, m.body, m.created_at,
                \\  (SELECT COUNT(*) FROM messages r WHERE r.parent_id = m.id)
                \\FROM messages_fts
                \\JOIN messages m ON m.rowid = messages_fts.rowid
                \\WHERE messages_fts MATCH ?
                \\ORDER BY bm25(messages_fts), m.created_at DESC
                \\LIMIT ?;
            );
            defer sqlite.finalize(stmt);
            try sqlite.bindText(stmt, 1, safe_query);
            try sqlite.bindInt(stmt, 2, @as(i32, @intCast(limit)));

            while (try sqlite.step(stmt)) {
                const parent_text = sqlite.columnText(stmt, 2);
                try messages.append(self.allocator, .{
                    .id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 0)),
                    .topic_id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 1)),
                    .parent_id = if (parent_text.len > 0) try self.allocator.dupe(u8, parent_text) else null,
                    .body = try self.allocator.dupe(u8, sqlite.columnText(stmt, 3)),
                    .created_at = sqlite.columnInt64(stmt, 4),
                    .reply_count = sqlite.columnInt(stmt, 5),
                });
            }
        }

        return messages.toOwnedSlice(self.allocator);
    }

    fn sanitizeFts5Query(self: *Store, query: []const u8) ![]u8 {
        // Wrap query in double quotes and escape internal double quotes
        // This prevents FTS5 syntax injection
        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(self.allocator);

        try buf.append(self.allocator, '"');
        for (query) |c| {
            if (c == '"') {
                try buf.append(self.allocator, '"'); // Escape quote by doubling
            }
            try buf.append(self.allocator, c);
        }
        try buf.append(self.allocator, '"');

        return buf.toOwnedSlice(self.allocator);
    }

    // ========== ID Resolution ==========

    fn messageExists(self: *Store, id: []const u8) !bool {
        const stmt = try sqlite.prepare(self.db, "SELECT 1 FROM messages WHERE id = ?;");
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, id);
        return sqlite.step(stmt);
    }

    pub fn resolveMessageId(self: *Store, prefix: []const u8) ![]u8 {
        // First try exact match
        const exact_stmt = try sqlite.prepare(self.db, "SELECT id FROM messages WHERE id = ?;");
        defer sqlite.finalize(exact_stmt);
        try sqlite.bindText(exact_stmt, 1, prefix);
        if (try sqlite.step(exact_stmt)) {
            return self.allocator.dupe(u8, sqlite.columnText(exact_stmt, 0));
        }

        // Try prefix match
        const like_pattern = try std.fmt.allocPrint(self.allocator, "{s}%", .{prefix});
        defer self.allocator.free(like_pattern);

        const stmt = try sqlite.prepare(self.db, "SELECT id FROM messages WHERE id LIKE ? LIMIT 2;");
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, like_pattern);

        var count: u32 = 0;
        var found_id: ?[]u8 = null;
        errdefer if (found_id) |id| self.allocator.free(id);

        while (try sqlite.step(stmt)) {
            count += 1;
            if (count == 1) {
                // Copy immediately - SQLite buffer invalidated after step exhausts
                found_id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 0));
            }
        }

        if (count == 0) return StoreError.MessageNotFound;
        if (count > 1) {
            if (found_id) |id| self.allocator.free(id);
            return StoreError.MessageIdAmbiguous;
        }
        return found_id.?;
    }

    // ========== JSONL Operations ==========

    fn appendTopicJsonl(self: *Store, id: []const u8, name: []const u8, description: []const u8, created_at: i64) !void {
        var out: std.Io.Writer.Allocating = .init(self.allocator);
        defer out.deinit();

        const record = struct {
            type: []const u8,
            id: []const u8,
            name: []const u8,
            description: []const u8,
            created_at: i64,
        }{
            .type = "topic",
            .id = id,
            .name = name,
            .description = description,
            .created_at = created_at,
        };
        try std.json.Stringify.value(record, .{ .whitespace = .minified }, &out.writer);
        try out.writer.writeByte('\n');

        try self.appendJsonlAtomic(out.written());
    }

    fn appendMessageJsonl(self: *Store, id: []const u8, topic_id: []const u8, parent_id: ?[]const u8, body: []const u8, created_at: i64) !void {
        var out: std.Io.Writer.Allocating = .init(self.allocator);
        defer out.deinit();

        const record = struct {
            type: []const u8,
            id: []const u8,
            topic_id: []const u8,
            parent_id: ?[]const u8,
            body: []const u8,
            created_at: i64,
        }{
            .type = "message",
            .id = id,
            .topic_id = topic_id,
            .parent_id = parent_id,
            .body = body,
            .created_at = created_at,
        };
        try std.json.Stringify.value(record, .{ .whitespace = .minified }, &out.writer);
        try out.writer.writeByte('\n');

        try self.appendJsonlAtomic(out.written());
    }

    fn appendJsonlAtomic(self: *Store, payload: []const u8) !void {
        if (self.lock_file) |*lf| {
            try lf.lock(.exclusive);
            defer lf.unlock();
        }

        var file = try std.fs.openFileAbsolute(self.jsonl_path, .{ .mode = .read_write });
        defer file.close();
        try file.seekFromEnd(0);
        try file.writeAll(payload);
        try file.sync();
    }

    // ========== Import/Sync ==========

    pub fn importIfNeeded(self: *Store) !void {
        const stat = std.fs.cwd().statFile(self.jsonl_path) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };

        const stored_offset_raw = (try self.getMetaInt("jsonl_offset")) orelse 0;
        const stored_offset: u64 = if (stored_offset_raw >= 0) @as(u64, @intCast(stored_offset_raw)) else 0;
        const size = @as(u64, @intCast(stat.size));

        if (size == stored_offset) return;
        if (size < stored_offset) {
            // File was truncated, do full reimport
            try self.fullReimport();
            return;
        }

        try self.importFromOffset(stored_offset);
    }

    fn fullReimport(self: *Store) !void {
        try sqlite.exec(self.db, "DELETE FROM messages;");
        try sqlite.exec(self.db, "DELETE FROM topics;");
        try sqlite.exec(self.db, "DELETE FROM messages_fts;");
        try self.importFromOffset(0);
    }

    fn importFromOffset(self: *Store, offset: u64) !void {
        var file = try std.fs.openFileAbsolute(self.jsonl_path, .{ .mode = .read_only });
        defer file.close();

        try file.seekTo(offset);
        const content = try file.readToEndAlloc(self.allocator, 100 * 1024 * 1024);
        defer self.allocator.free(content);

        try self.beginImmediate();
        errdefer sqlite.exec(self.db, "ROLLBACK;") catch {};

        var message_lines: std.ArrayList([]const u8) = .empty;
        defer message_lines.deinit(self.allocator);

        var iter = std.mem.splitScalar(u8, content, '\n');
        while (iter.next()) |raw_line| {
            const line = std.mem.trim(u8, raw_line, "\r\n\t ");
            if (line.len == 0) continue;

            var parsed = std.json.parseFromSlice(std.json.Value, self.allocator, line, .{}) catch continue;
            defer parsed.deinit();

            const obj = parsed.value.object;
            const type_str = if (obj.get("type")) |v| v.string else continue;

            if (std.mem.eql(u8, type_str, "topic")) {
                self.applyTopicRecord(obj) catch continue;
            } else if (std.mem.eql(u8, type_str, "message")) {
                try message_lines.append(self.allocator, try self.allocator.dupe(u8, line));
            }
        }

        // Apply messages after topics
        for (message_lines.items) |line| {
            defer self.allocator.free(line);
            var parsed = std.json.parseFromSlice(std.json.Value, self.allocator, line, .{}) catch continue;
            defer parsed.deinit();
            self.applyMessageRecord(parsed.value.object) catch continue;
        }

        try self.commit();

        const new_offset = offset + @as(u64, @intCast(content.len));
        try self.setMetaInt("jsonl_offset", @as(i64, @intCast(new_offset)));
    }

    fn applyTopicRecord(self: *Store, obj: std.json.ObjectMap) !void {
        const id = if (obj.get("id")) |v| v.string else return error.InvalidMessageId;
        const name = if (obj.get("name")) |v| v.string else return error.InvalidMessageId;
        const description = if (obj.get("description")) |v| v.string else "";
        const created_at = if (obj.get("created_at")) |v| v.integer else return error.InvalidMessageId;

        const stmt = try sqlite.prepare(self.db, "INSERT OR REPLACE INTO topics (id, name, description, created_at) VALUES (?, ?, ?, ?);");
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, id);
        try sqlite.bindText(stmt, 2, name);
        try sqlite.bindText(stmt, 3, description);
        try sqlite.bindInt64(stmt, 4, created_at);
        _ = try sqlite.step(stmt);
    }

    fn applyMessageRecord(self: *Store, obj: std.json.ObjectMap) !void {
        const id = if (obj.get("id")) |v| v.string else return error.InvalidMessageId;
        const topic_id = if (obj.get("topic_id")) |v| v.string else return error.InvalidMessageId;
        const parent_id = if (obj.get("parent_id")) |v| switch (v) {
            .string => |s| s,
            .null => null,
            else => null,
        } else null;
        const body = if (obj.get("body")) |v| v.string else "";
        const created_at = if (obj.get("created_at")) |v| v.integer else return error.InvalidMessageId;

        const stmt = try sqlite.prepare(self.db, "INSERT OR REPLACE INTO messages (id, topic_id, parent_id, body, created_at) VALUES (?, ?, ?, ?, ?);");
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, id);
        try sqlite.bindText(stmt, 2, topic_id);
        if (parent_id) |pid| {
            try sqlite.bindText(stmt, 3, pid);
        } else {
            try sqlite.bindNull(stmt, 3);
        }
        try sqlite.bindText(stmt, 4, body);
        try sqlite.bindInt64(stmt, 5, created_at);
        _ = try sqlite.step(stmt);

        // Update FTS
        const rowid = sqlite.lastInsertRowId(self.db);
        const fts_stmt = try sqlite.prepare(self.db, "INSERT OR REPLACE INTO messages_fts(rowid, body) VALUES (?, ?);");
        defer sqlite.finalize(fts_stmt);
        try sqlite.bindInt64(fts_stmt, 1, rowid);
        try sqlite.bindText(fts_stmt, 2, body);
        _ = try sqlite.step(fts_stmt);
    }

    // ========== Transaction Helpers ==========

    fn beginImmediate(self: *Store) !void {
        try self.execWithRetry("BEGIN IMMEDIATE;");
    }

    fn commit(self: *Store) !void {
        try self.execWithRetry("COMMIT;");
    }

    fn execWithRetry(self: *Store, sql: []const u8) !void {
        var attempt: u32 = 0;
        const max_attempts: u32 = 50;
        var prng = std.Random.DefaultPrng.init(@as(u64, @intCast(std.time.nanoTimestamp())));
        const random = prng.random();

        while (true) {
            sqlite.exec(self.db, sql) catch |err| switch (err) {
                sqlite.Error.SqliteBusy => {
                    if (attempt >= max_attempts) return StoreError.DatabaseBusy;
                    const delay_ms = random.intRangeAtMost(u64, 50, 500);
                    std.Thread.sleep(delay_ms * std.time.ns_per_ms);
                    attempt += 1;
                    continue;
                },
                else => return err,
            };
            return;
        }
    }

    // ========== Meta Helpers ==========

    fn getMetaInt(self: *Store, key: []const u8) !?i64 {
        const stmt = try sqlite.prepare(self.db, "SELECT value FROM meta WHERE key = ?;");
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, key);
        if (try sqlite.step(stmt)) {
            const val_str = sqlite.columnText(stmt, 0);
            return std.fmt.parseInt(i64, val_str, 10) catch null;
        }
        return null;
    }

    fn setMetaInt(self: *Store, key: []const u8, value: i64) !void {
        const val_str = try std.fmt.allocPrint(self.allocator, "{d}", .{value});
        defer self.allocator.free(val_str);

        const stmt = try sqlite.prepare(self.db, "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?);");
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, key);
        try sqlite.bindText(stmt, 2, val_str);
        _ = try sqlite.step(stmt);
    }
};

pub fn discoverStoreDir(allocator: std.mem.Allocator) ![]const u8 {
    var cwd = std.fs.cwd();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const cwd_path = try cwd.realpath(".", &path_buf);

    var current = try allocator.dupe(u8, cwd_path);
    while (true) {
        const zawinski_dir = try std.fs.path.join(allocator, &.{ current, ".zawinski" });
        defer allocator.free(zawinski_dir);

        std.fs.accessAbsolute(zawinski_dir, .{}) catch {
            // Try parent
            const parent = std.fs.path.dirname(current);
            if (parent == null or std.mem.eql(u8, parent.?, current)) {
                allocator.free(current);
                return StoreError.StoreNotFound;
            }
            const new_current = try allocator.dupe(u8, parent.?);
            allocator.free(current);
            current = new_current;
            continue;
        };

        const result = try allocator.dupe(u8, zawinski_dir);
        allocator.free(current);
        return result;
    }
}
