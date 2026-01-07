const std = @import("std");
const sqlite = @import("sqlite.zig");
const ids = @import("ids.zig");

pub const StoreError = error{
    StoreNotFound,
    StoreAlreadyExists,
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

/// Sender identity for a message
pub const Sender = struct {
    id: []const u8,
    name: []const u8,
    model: ?[]const u8 = null,
    role: ?[]const u8 = null,

    pub fn deinit(self: *Sender, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.name);
        if (self.model) |m| allocator.free(m);
        if (self.role) |r| allocator.free(r);
    }
};

/// Git metadata captured when message was created
pub const GitMeta = struct {
    oid: []const u8,
    head: []const u8,
    dirty: bool,
    prefix: []const u8,

    pub fn deinit(self: *GitMeta, allocator: std.mem.Allocator) void {
        allocator.free(self.oid);
        allocator.free(self.head);
        allocator.free(self.prefix);
    }
};

pub const Message = struct {
    id: []const u8,
    topic_id: []const u8,
    parent_id: ?[]const u8,
    body: []const u8,
    created_at: i64,
    reply_count: i32 = 0,
    sender: ?Sender = null,
    git: ?GitMeta = null,

    pub fn deinit(self: *Message, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.topic_id);
        if (self.parent_id) |pid| allocator.free(pid);
        allocator.free(self.body);
        if (self.sender) |*s| s.deinit(allocator);
        if (self.git) |*g| g.deinit(allocator);
    }
};

/// Content-addressable blob storage
pub const Blob = struct {
    id: []const u8, // SHA-256 hash
    size: u64,
    mime_type: ?[]const u8,
    created_at: i64,

    pub fn deinit(self: *Blob, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        if (self.mime_type) |mt| allocator.free(mt);
    }
};

/// Attachment linking a blob to a message
pub const Attachment = struct {
    message_id: []const u8,
    blob_id: []const u8,
    name: ?[]const u8,

    pub fn deinit(self: *Attachment, allocator: std.mem.Allocator) void {
        allocator.free(self.message_id);
        allocator.free(self.blob_id);
        if (self.name) |n| allocator.free(n);
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
        \\  sender_id TEXT,
        \\  sender_name TEXT,
        \\  sender_model TEXT,
        \\  sender_role TEXT,
        \\  git_oid TEXT,
        \\  git_head TEXT,
        \\  git_dirty INTEGER,
        \\  git_prefix TEXT,
        \\  FOREIGN KEY(topic_id) REFERENCES topics(id) ON DELETE CASCADE,
        \\  FOREIGN KEY(parent_id) REFERENCES messages(id) ON DELETE CASCADE
        \\);
        \\
        \\CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages(topic_id, created_at);
        \\CREATE INDEX IF NOT EXISTS idx_messages_parent ON messages(parent_id);
        \\CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender_id);
        \\
        \\CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(body, content=messages, content_rowid=rowid);
        \\
        \\CREATE TABLE IF NOT EXISTS meta (
        \\  key TEXT PRIMARY KEY,
        \\  value TEXT NOT NULL
        \\);
        \\
        \\CREATE TABLE IF NOT EXISTS blobs (
        \\  id TEXT PRIMARY KEY,
        \\  data BLOB NOT NULL,
        \\  size INTEGER NOT NULL,
        \\  mime_type TEXT,
        \\  created_at INTEGER NOT NULL
        \\);
        \\
        \\CREATE TABLE IF NOT EXISTS attachments (
        \\  message_id TEXT NOT NULL,
        \\  blob_id TEXT NOT NULL,
        \\  name TEXT,
        \\  PRIMARY KEY (message_id, blob_id),
        \\  FOREIGN KEY(message_id) REFERENCES messages(id) ON DELETE CASCADE,
        \\  FOREIGN KEY(blob_id) REFERENCES blobs(id)
        \\);
    ;

    pub fn init(allocator: std.mem.Allocator, dir: []const u8) !void {
        // Create store directory
        std.fs.makeDirAbsolute(dir) catch |err| switch (err) {
            error.PathAlreadyExists => return error.StoreAlreadyExists,
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

        // Allocate null-terminated path for SQLite
        const db_path_z = try allocator.dupeZ(u8, db_path);
        defer allocator.free(db_path_z);

        const db = try sqlite.open(db_path_z);
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
        // Migrate existing tables to add new columns (safe if already present)
        try self.migrateSchema();
    }

    fn migrateSchema(self: *Store) !void {
        // Check which columns already exist in messages table
        var existing_columns = std.StringHashMap(void).init(self.allocator);
        defer existing_columns.deinit();

        const info_stmt = try sqlite.prepare(self.db, "PRAGMA table_info(messages);");
        defer sqlite.finalize(info_stmt);
        while (try sqlite.step(info_stmt)) {
            // Column 1 is the column name
            const col_name = sqlite.columnText(info_stmt, 1);
            existing_columns.put(col_name, {}) catch {};
        }

        // Add missing sender/git columns to messages table
        const new_columns = [_]struct { name: []const u8, sql: [:0]const u8 }{
            .{ .name = "sender_id", .sql = "ALTER TABLE messages ADD COLUMN sender_id TEXT;" },
            .{ .name = "sender_name", .sql = "ALTER TABLE messages ADD COLUMN sender_name TEXT;" },
            .{ .name = "sender_model", .sql = "ALTER TABLE messages ADD COLUMN sender_model TEXT;" },
            .{ .name = "sender_role", .sql = "ALTER TABLE messages ADD COLUMN sender_role TEXT;" },
            .{ .name = "git_oid", .sql = "ALTER TABLE messages ADD COLUMN git_oid TEXT;" },
            .{ .name = "git_head", .sql = "ALTER TABLE messages ADD COLUMN git_head TEXT;" },
            .{ .name = "git_dirty", .sql = "ALTER TABLE messages ADD COLUMN git_dirty INTEGER;" },
            .{ .name = "git_prefix", .sql = "ALTER TABLE messages ADD COLUMN git_prefix TEXT;" },
        };

        for (new_columns) |col| {
            if (!existing_columns.contains(col.name)) {
                try sqlite.exec(self.db, col.sql);
            }
        }

        // Ensure sender index exists (IF NOT EXISTS is safe)
        try sqlite.exec(self.db, "CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender_id);");
    }

    // ========== Topic Operations ==========

    pub fn createTopic(self: *Store, name: []const u8, description: []const u8) ![]u8 {
        // Validate and normalize input
        const trimmed_name = std.mem.trim(u8, name, " \t\r\n");
        if (trimmed_name.len == 0) return StoreError.EmptyTopicName;
        const trimmed_desc = std.mem.trim(u8, description, " \t\r\n");

        const now_ms = @as(i64, @intCast(std.time.milliTimestamp()));
        const id = try self.ulid.nextNow(self.allocator);
        errdefer self.allocator.free(id);

        try self.beginImmediate();
        errdefer sqlite.exec(self.db, "ROLLBACK;") catch {};

        // Insert into DB (use trimmed values for consistency)
        const stmt = try sqlite.prepare(self.db, "INSERT INTO topics (id, name, description, created_at) VALUES (?, ?, ?, ?);");
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, id);
        try sqlite.bindText(stmt, 2, trimmed_name);
        try sqlite.bindText(stmt, 3, trimmed_desc);
        try sqlite.bindInt64(stmt, 4, now_ms);
        _ = sqlite.step(stmt) catch |err| {
            if (err == sqlite.Error.SqliteError or err == sqlite.Error.SqliteStepError) {
                return StoreError.TopicExists;
            }
            return err;
        };

        // Append to JSONL
        try self.appendTopicJsonl(id, trimmed_name, trimmed_desc, now_ms);

        try self.commit();
        return id;
    }

    pub fn fetchTopic(self: *Store, name: []const u8) !Topic {
        // Trim to match createTopic behavior
        const trimmed_name = std.mem.trim(u8, name, " \t\r\n");
        const stmt = try sqlite.prepare(self.db, "SELECT id, name, description, created_at FROM topics WHERE name = ?;");
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, trimmed_name);

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

    /// Options for creating a message
    pub const CreateMessageOptions = struct {
        sender: ?Sender = null,
        git: ?GitMeta = null,
    };

    pub fn createMessage(self: *Store, topic_name: []const u8, parent_id: ?[]const u8, body: []const u8, options: CreateMessageOptions) ![]u8 {
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

        // Insert into DB with sender and git metadata
        const stmt = try sqlite.prepare(self.db,
            \\INSERT INTO messages (id, topic_id, parent_id, body, created_at,
            \\  sender_id, sender_name, sender_model, sender_role,
            \\  git_oid, git_head, git_dirty, git_prefix)
            \\VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        );
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

        // Bind sender fields
        if (options.sender) |s| {
            try sqlite.bindText(stmt, 6, s.id);
            try sqlite.bindText(stmt, 7, s.name);
            if (s.model) |m| try sqlite.bindText(stmt, 8, m) else try sqlite.bindNull(stmt, 8);
            if (s.role) |r| try sqlite.bindText(stmt, 9, r) else try sqlite.bindNull(stmt, 9);
        } else {
            try sqlite.bindNull(stmt, 6);
            try sqlite.bindNull(stmt, 7);
            try sqlite.bindNull(stmt, 8);
            try sqlite.bindNull(stmt, 9);
        }

        // Bind git fields
        if (options.git) |g| {
            try sqlite.bindText(stmt, 10, g.oid);
            try sqlite.bindText(stmt, 11, g.head);
            try sqlite.bindInt(stmt, 12, if (g.dirty) @as(i32, 1) else @as(i32, 0));
            try sqlite.bindText(stmt, 13, g.prefix);
        } else {
            try sqlite.bindNull(stmt, 10);
            try sqlite.bindNull(stmt, 11);
            try sqlite.bindNull(stmt, 12);
            try sqlite.bindNull(stmt, 13);
        }

        _ = try sqlite.step(stmt);

        // Update FTS
        const fts_stmt = try sqlite.prepare(self.db, "INSERT INTO messages_fts(rowid, body) VALUES (last_insert_rowid(), ?);");
        defer sqlite.finalize(fts_stmt);
        try sqlite.bindText(fts_stmt, 1, body);
        _ = try sqlite.step(fts_stmt);

        // Append to JSONL
        try self.appendMessageJsonl(id, topic.id, parent_id, body, now_ms, options.sender, options.git);

        try self.commit();
        return id;
    }

    /// Standard SELECT columns for message queries (14 columns total).
    /// Use parseMessageFromRow to parse results from queries using this.
    const message_select_columns =
        \\m.id, m.topic_id, m.parent_id, m.body, m.created_at,
        \\(SELECT COUNT(*) FROM messages r WHERE r.parent_id = m.id),
        \\m.sender_id, m.sender_name, m.sender_model, m.sender_role,
        \\m.git_oid, m.git_head, m.git_dirty, m.git_prefix
    ;

    /// Parse a Message from a statement row using the standard 14-column format.
    /// Uses proper errdefer to clean up partial allocations on failure.
    fn parseMessageFromRow(self: *Store, stmt: *sqlite.c.sqlite3_stmt) !Message {
        // Column indices:
        // 0: id, 1: topic_id, 2: parent_id, 3: body, 4: created_at, 5: reply_count
        // 6: sender_id, 7: sender_name, 8: sender_model, 9: sender_role
        // 10: git_oid, 11: git_head, 12: git_dirty, 13: git_prefix

        // Parse sender if present (with proper cleanup on failure)
        var sender: ?Sender = null;
        const sender_id_text = sqlite.columnText(stmt, 6);
        if (sender_id_text.len > 0) {
            const sid = try self.allocator.dupe(u8, sender_id_text);
            errdefer self.allocator.free(sid);

            const sname = try self.allocator.dupe(u8, sqlite.columnText(stmt, 7));
            errdefer self.allocator.free(sname);

            const sender_model_text = sqlite.columnText(stmt, 8);
            const smodel: ?[]u8 = if (sender_model_text.len > 0) try self.allocator.dupe(u8, sender_model_text) else null;
            errdefer if (smodel) |m| self.allocator.free(m);

            const sender_role_text = sqlite.columnText(stmt, 9);
            const srole: ?[]u8 = if (sender_role_text.len > 0) try self.allocator.dupe(u8, sender_role_text) else null;
            errdefer if (srole) |r| self.allocator.free(r);

            sender = Sender{ .id = sid, .name = sname, .model = smodel, .role = srole };
        }
        errdefer if (sender) |*s| {
            self.allocator.free(s.id);
            self.allocator.free(s.name);
            if (s.model) |m| self.allocator.free(m);
            if (s.role) |r| self.allocator.free(r);
        };

        // Parse git if present (with proper cleanup on failure)
        var git: ?GitMeta = null;
        const git_oid_text = sqlite.columnText(stmt, 10);
        if (git_oid_text.len > 0) {
            const goid = try self.allocator.dupe(u8, git_oid_text);
            errdefer self.allocator.free(goid);

            const ghead = try self.allocator.dupe(u8, sqlite.columnText(stmt, 11));
            errdefer self.allocator.free(ghead);

            const gprefix = try self.allocator.dupe(u8, sqlite.columnText(stmt, 13));
            // No errdefer needed - this is the last allocation in this block

            git = GitMeta{
                .oid = goid,
                .head = ghead,
                .dirty = sqlite.columnInt(stmt, 12) != 0,
                .prefix = gprefix,
            };
        }
        errdefer if (git) |*g| {
            self.allocator.free(g.oid);
            self.allocator.free(g.head);
            self.allocator.free(g.prefix);
        };

        // Parse core message fields
        const msg_id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 0));
        errdefer self.allocator.free(msg_id);

        const topic_id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 1));
        errdefer self.allocator.free(topic_id);

        const parent_text = sqlite.columnText(stmt, 2);
        const parent_id: ?[]u8 = if (parent_text.len > 0) try self.allocator.dupe(u8, parent_text) else null;
        errdefer if (parent_id) |p| self.allocator.free(p);

        const body = try self.allocator.dupe(u8, sqlite.columnText(stmt, 3));
        // No errdefer needed - last allocation

        return Message{
            .id = msg_id,
            .topic_id = topic_id,
            .parent_id = parent_id,
            .body = body,
            .created_at = sqlite.columnInt64(stmt, 4),
            .reply_count = sqlite.columnInt(stmt, 5),
            .sender = sender,
            .git = git,
        };
    }

    pub fn fetchMessage(self: *Store, id: []const u8) !Message {
        const resolved_id = try self.resolveMessageId(id);
        defer self.allocator.free(resolved_id);

        const stmt = try sqlite.prepare(self.db,
            "SELECT " ++ message_select_columns ++ " FROM messages m WHERE m.id = ?;",
        );
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, resolved_id);

        if (try sqlite.step(stmt)) {
            return self.parseMessageFromRow(stmt);
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
            "SELECT " ++ message_select_columns ++
                \\ FROM messages m
                \\WHERE m.topic_id = ? AND m.parent_id IS NULL
                \\ORDER BY m.created_at DESC
                \\LIMIT ?;
            ,
        );
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, topic.id);
        try sqlite.bindInt(stmt, 2, @as(i32, @intCast(limit)));

        while (try sqlite.step(stmt)) {
            try messages.append(self.allocator, try self.parseMessageFromRow(stmt));
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
            \\WITH RECURSIVE thread AS (
            \\  SELECT id, topic_id, parent_id, body, created_at,
            \\    sender_id, sender_name, sender_model, sender_role,
            \\    git_oid, git_head, git_dirty, git_prefix
            \\  FROM messages WHERE id = ?
            \\  UNION ALL
            \\  SELECT m.id, m.topic_id, m.parent_id, m.body, m.created_at,
            \\    m.sender_id, m.sender_name, m.sender_model, m.sender_role,
            \\    m.git_oid, m.git_head, m.git_dirty, m.git_prefix
            \\  FROM messages m JOIN thread t ON m.parent_id = t.id
            \\)
            \\SELECT t.id, t.topic_id, t.parent_id, t.body, t.created_at,
            \\  (SELECT COUNT(*) FROM messages r WHERE r.parent_id = t.id),
            \\  t.sender_id, t.sender_name, t.sender_model, t.sender_role,
            \\  t.git_oid, t.git_head, t.git_dirty, t.git_prefix
            \\FROM thread t ORDER BY t.created_at;
        );
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, resolved_id);

        while (try sqlite.step(stmt)) {
            try messages.append(self.allocator, try self.parseMessageFromRow(stmt));
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
            "SELECT " ++ message_select_columns ++
                \\ FROM messages m WHERE m.parent_id = ?
                \\ORDER BY m.created_at;
            ,
        );
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, resolved_id);

        while (try sqlite.step(stmt)) {
            try messages.append(self.allocator, try self.parseMessageFromRow(stmt));
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
                "SELECT " ++ message_select_columns ++
                    \\ FROM messages_fts
                    \\JOIN messages m ON m.rowid = messages_fts.rowid
                    \\WHERE messages_fts MATCH ? AND m.topic_id = ?
                    \\ORDER BY bm25(messages_fts), m.created_at DESC
                    \\LIMIT ?;
                ,
            );
            defer sqlite.finalize(stmt);
            try sqlite.bindText(stmt, 1, safe_query);
            try sqlite.bindText(stmt, 2, topic.id);
            try sqlite.bindInt(stmt, 3, @as(i32, @intCast(limit)));

            while (try sqlite.step(stmt)) {
                try messages.append(self.allocator, try self.parseMessageFromRow(stmt));
            }
        } else {
            const stmt = try sqlite.prepare(self.db,
                "SELECT " ++ message_select_columns ++
                    \\ FROM messages_fts
                    \\JOIN messages m ON m.rowid = messages_fts.rowid
                    \\WHERE messages_fts MATCH ?
                    \\ORDER BY bm25(messages_fts), m.created_at DESC
                    \\LIMIT ?;
                ,
            );
            defer sqlite.finalize(stmt);
            try sqlite.bindText(stmt, 1, safe_query);
            try sqlite.bindInt(stmt, 2, @as(i32, @intCast(limit)));

            while (try sqlite.step(stmt)) {
                try messages.append(self.allocator, try self.parseMessageFromRow(stmt));
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

    fn appendMessageJsonl(
        self: *Store,
        id: []const u8,
        topic_id: []const u8,
        parent_id: ?[]const u8,
        body: []const u8,
        created_at: i64,
        sender: ?Sender,
        git: ?GitMeta,
    ) !void {
        var out: std.Io.Writer.Allocating = .init(self.allocator);
        defer out.deinit();

        // Build sender sub-object if present
        const SenderJson = struct {
            id: []const u8,
            name: []const u8,
            model: ?[]const u8,
            role: ?[]const u8,
        };
        const sender_json: ?SenderJson = if (sender) |s| .{
            .id = s.id,
            .name = s.name,
            .model = s.model,
            .role = s.role,
        } else null;

        // Build git sub-object if present
        const GitJson = struct {
            oid: []const u8,
            head: []const u8,
            dirty: bool,
            prefix: []const u8,
        };
        const git_json: ?GitJson = if (git) |g| .{
            .oid = g.oid,
            .head = g.head,
            .dirty = g.dirty,
            .prefix = g.prefix,
        } else null;

        const record = struct {
            type: []const u8,
            id: []const u8,
            topic_id: []const u8,
            parent_id: ?[]const u8,
            body: []const u8,
            created_at: i64,
            sender: ?SenderJson,
            git: ?GitJson,
        }{
            .type = "message",
            .id = id,
            .topic_id = topic_id,
            .parent_id = parent_id,
            .body = body,
            .created_at = created_at,
            .sender = sender_json,
            .git = git_json,
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

        try self.importFromOffset(stored_offset, false);
    }

    fn fullReimport(self: *Store) !void {
        try self.importFromOffset(0, true);
    }

    fn importFromOffset(self: *Store, offset: u64, clear_first: bool) !void {
        // Read JSONL content under lock, then release before acquiring SQLite lock.
        // This avoids deadlock with write path (which holds SQLite -> acquires JSONL).
        const content = blk: {
            if (self.lock_file) |*lf| {
                try lf.lock(.shared);
            }
            defer if (self.lock_file) |*lf| lf.unlock();

            var file = try std.fs.openFileAbsolute(self.jsonl_path, .{ .mode = .read_only });
            defer file.close();

            try file.seekTo(offset);
            break :blk try file.readToEndAlloc(self.allocator, 100 * 1024 * 1024);
        };
        defer self.allocator.free(content);

        try self.beginImmediate();
        errdefer sqlite.exec(self.db, "ROLLBACK;") catch {};

        // Clear tables inside transaction for crash safety
        if (clear_first) {
            try sqlite.exec(self.db, "DELETE FROM messages_fts;");
            try sqlite.exec(self.db, "DELETE FROM messages;");
            try sqlite.exec(self.db, "DELETE FROM topics;");
        }

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

        // Update offset inside transaction for crash safety
        const new_offset = offset + @as(u64, @intCast(content.len));
        try self.setMetaInt("jsonl_offset", @as(i64, @intCast(new_offset)));

        try self.commit();
    }

    fn applyTopicRecord(self: *Store, obj: std.json.ObjectMap) !void {
        const id = if (obj.get("id")) |v| v.string else return error.InvalidMessageId;
        const name = if (obj.get("name")) |v| v.string else return error.InvalidMessageId;
        const description = if (obj.get("description")) |v| v.string else "";
        const created_at = if (obj.get("created_at")) |v| v.integer else return error.InvalidMessageId;

        // Use INSERT OR IGNORE to avoid CASCADE DELETE issues with INSERT OR REPLACE.
        // JSONL is append-only, so existing records don't need updating.
        const stmt = try sqlite.prepare(self.db, "INSERT OR IGNORE INTO topics (id, name, description, created_at) VALUES (?, ?, ?, ?);");
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

        // Extract sender fields if present
        var sender_id: ?[]const u8 = null;
        var sender_name: ?[]const u8 = null;
        var sender_model: ?[]const u8 = null;
        var sender_role: ?[]const u8 = null;
        if (obj.get("sender")) |sender_val| {
            if (sender_val == .object) {
                const sender_obj = sender_val.object;
                sender_id = if (sender_obj.get("id")) |v| if (v == .string) v.string else null else null;
                sender_name = if (sender_obj.get("name")) |v| if (v == .string) v.string else null else null;
                sender_model = if (sender_obj.get("model")) |v| if (v == .string) v.string else null else null;
                sender_role = if (sender_obj.get("role")) |v| if (v == .string) v.string else null else null;
            }
        }

        // Extract git fields if present
        var git_oid: ?[]const u8 = null;
        var git_head: ?[]const u8 = null;
        var git_dirty: ?bool = null;
        var git_prefix: ?[]const u8 = null;
        if (obj.get("git")) |git_val| {
            if (git_val == .object) {
                const git_obj = git_val.object;
                git_oid = if (git_obj.get("oid")) |v| if (v == .string) v.string else null else null;
                git_head = if (git_obj.get("head")) |v| if (v == .string) v.string else null else null;
                git_dirty = if (git_obj.get("dirty")) |v| if (v == .bool) v.bool else null else null;
                git_prefix = if (git_obj.get("prefix")) |v| if (v == .string) v.string else null else null;
            }
        }

        // Use INSERT OR IGNORE to avoid CASCADE DELETE issues with INSERT OR REPLACE.
        // JSONL is append-only, so existing records don't need updating.
        const stmt = try sqlite.prepare(self.db,
            \\INSERT OR IGNORE INTO messages (id, topic_id, parent_id, body, created_at,
            \\  sender_id, sender_name, sender_model, sender_role,
            \\  git_oid, git_head, git_dirty, git_prefix)
            \\VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        );
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

        // Bind sender fields
        if (sender_id) |s| try sqlite.bindText(stmt, 6, s) else try sqlite.bindNull(stmt, 6);
        if (sender_name) |s| try sqlite.bindText(stmt, 7, s) else try sqlite.bindNull(stmt, 7);
        if (sender_model) |s| try sqlite.bindText(stmt, 8, s) else try sqlite.bindNull(stmt, 8);
        if (sender_role) |s| try sqlite.bindText(stmt, 9, s) else try sqlite.bindNull(stmt, 9);

        // Bind git fields
        if (git_oid) |s| try sqlite.bindText(stmt, 10, s) else try sqlite.bindNull(stmt, 10);
        if (git_head) |s| try sqlite.bindText(stmt, 11, s) else try sqlite.bindNull(stmt, 11);
        if (git_dirty) |d| try sqlite.bindInt(stmt, 12, if (d) @as(i32, 1) else @as(i32, 0)) else try sqlite.bindNull(stmt, 12);
        if (git_prefix) |s| try sqlite.bindText(stmt, 13, s) else try sqlite.bindNull(stmt, 13);

        _ = try sqlite.step(stmt);

        // Only update FTS if the message was actually inserted (not ignored)
        if (sqlite.changes(self.db) > 0) {
            const rowid = sqlite.lastInsertRowId(self.db);
            const fts_stmt = try sqlite.prepare(self.db, "INSERT INTO messages_fts(rowid, body) VALUES (?, ?);");
            defer sqlite.finalize(fts_stmt);
            try sqlite.bindInt64(fts_stmt, 1, rowid);
            try sqlite.bindText(fts_stmt, 2, body);
            _ = try sqlite.step(fts_stmt);
        }
    }

    // ========== Transaction Helpers ==========

    fn beginImmediate(self: *Store) !void {
        try self.execWithRetry("BEGIN IMMEDIATE;");
    }

    fn commit(self: *Store) !void {
        try self.execWithRetry("COMMIT;");
    }

    fn execWithRetry(self: *Store, sql: [:0]const u8) !void {
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

    // ========== Blob Operations ==========

    /// Store a blob, returns the content hash (sha256:...)
    /// If the blob already exists, returns the existing hash without inserting.
    /// Uses INSERT OR IGNORE for atomic deduplication (no race conditions).
    pub fn putBlob(self: *Store, data: []const u8, mime_type: ?[]const u8) ![]u8 {
        // Compute SHA-256 hash
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(data, &hash, .{});

        // Format as hex string with sha256: prefix
        var hash_buf: [71]u8 = undefined; // "sha256:" + 64 hex chars
        @memcpy(hash_buf[0..7], "sha256:");
        _ = std.fmt.bufPrint(hash_buf[7..71], "{x}", .{hash}) catch unreachable;
        const id = hash_buf[0..71];

        // Insert blob atomically (INSERT OR IGNORE handles duplicates)
        const now_ms = @as(i64, @intCast(std.time.milliTimestamp()));

        const stmt = try sqlite.prepare(self.db, "INSERT OR IGNORE INTO blobs (id, data, size, mime_type, created_at) VALUES (?, ?, ?, ?, ?);");
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, id);
        try sqlite.bindBlob(stmt, 2, data);
        try sqlite.bindInt64(stmt, 3, @as(i64, @intCast(data.len)));
        if (mime_type) |mt| {
            try sqlite.bindText(stmt, 4, mt);
        } else {
            try sqlite.bindNull(stmt, 4);
        }
        try sqlite.bindInt64(stmt, 5, now_ms);
        _ = try sqlite.step(stmt);

        return self.allocator.dupe(u8, id);
    }

    /// Get blob data by id (sha256:...)
    pub fn getBlob(self: *Store, blob_id: []const u8) ![]u8 {
        const stmt = try sqlite.prepare(self.db, "SELECT data FROM blobs WHERE id = ?;");
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, blob_id);

        if (try sqlite.step(stmt)) {
            const data = sqlite.columnBlob(stmt, 0);
            return self.allocator.dupe(u8, data);
        }
        return error.BlobNotFound;
    }

    /// Get blob metadata by id
    pub fn fetchBlob(self: *Store, blob_id: []const u8) !Blob {
        const stmt = try sqlite.prepare(self.db, "SELECT id, size, mime_type, created_at FROM blobs WHERE id = ?;");
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, blob_id);

        if (try sqlite.step(stmt)) {
            const mime_text = sqlite.columnText(stmt, 2);
            return Blob{
                .id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 0)),
                .size = @as(u64, @intCast(sqlite.columnInt64(stmt, 1))),
                .mime_type = if (mime_text.len > 0) try self.allocator.dupe(u8, mime_text) else null,
                .created_at = sqlite.columnInt64(stmt, 3),
            };
        }
        return error.BlobNotFound;
    }

    /// Attach a blob to a message
    pub fn attachBlob(self: *Store, message_id: []const u8, blob_id: []const u8, name: ?[]const u8) !void {
        const stmt = try sqlite.prepare(self.db, "INSERT OR REPLACE INTO attachments (message_id, blob_id, name) VALUES (?, ?, ?);");
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, message_id);
        try sqlite.bindText(stmt, 2, blob_id);
        if (name) |n| {
            try sqlite.bindText(stmt, 3, n);
        } else {
            try sqlite.bindNull(stmt, 3);
        }
        _ = try sqlite.step(stmt);
    }

    /// List attachments for a message
    pub fn listAttachments(self: *Store, message_id: []const u8) ![]Attachment {
        var attachments: std.ArrayList(Attachment) = .empty;
        errdefer {
            for (attachments.items) |*a| a.deinit(self.allocator);
            attachments.deinit(self.allocator);
        }

        const stmt = try sqlite.prepare(self.db, "SELECT message_id, blob_id, name FROM attachments WHERE message_id = ?;");
        defer sqlite.finalize(stmt);
        try sqlite.bindText(stmt, 1, message_id);

        while (try sqlite.step(stmt)) {
            const name_text = sqlite.columnText(stmt, 2);
            try attachments.append(self.allocator, Attachment{
                .message_id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 0)),
                .blob_id = try self.allocator.dupe(u8, sqlite.columnText(stmt, 1)),
                .name = if (name_text.len > 0) try self.allocator.dupe(u8, name_text) else null,
            });
        }

        return attachments.toOwnedSlice(self.allocator);
    }
};

/// Discovers the store directory.
///
/// Priority:
/// 1. Walk up directory tree looking for .jwz or .zawinski (local stores take precedence)
/// 2. JWZ_STORE environment variable (fallback)
///
/// Returns an allocated path; caller must free.
pub fn discoverStoreDir(allocator: std.mem.Allocator) ![]const u8 {
    // Priority 1: Walk up directory tree (local stores take precedence)
    const local_result = walkUpForStore(allocator);
    if (local_result) |path| {
        return path;
    } else |walk_err| switch (walk_err) {
        StoreError.StoreNotFound => {}, // No local store, try env var
        else => return walk_err,
    }

    // Priority 2: JWZ_STORE environment variable (fallback)
    if (std.process.getEnvVarOwned(allocator, "JWZ_STORE")) |env| {
        defer allocator.free(env);
        if (std.fs.path.isAbsolute(env)) {
            return allocator.dupe(u8, env);
        }
        var cwd_buf: [std.fs.max_path_bytes]u8 = undefined;
        const cwd_path = try std.fs.cwd().realpath(".", &cwd_buf);
        return std.fs.path.join(allocator, &.{ cwd_path, env });
    } else |err| switch (err) {
        error.EnvironmentVariableNotFound => {},
        else => return err,
    }

    return StoreError.StoreNotFound;
}

/// Walks up the directory tree looking for .jwz or .zawinski stores.
fn walkUpForStore(allocator: std.mem.Allocator) ![]const u8 {
    var cwd = std.fs.cwd();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const cwd_path = try cwd.realpath(".", &path_buf);

    var current = try allocator.dupe(u8, cwd_path);
    errdefer allocator.free(current);

    while (true) {
        // Try .jwz first (preferred)
        const jwz_dir = try std.fs.path.join(allocator, &.{ current, ".jwz" });
        if (std.fs.accessAbsolute(jwz_dir, .{})) |_| {
            allocator.free(current);
            return jwz_dir;
        } else |_| {
            allocator.free(jwz_dir);
        }

        // Fall back to .zawinski (legacy)
        const zawinski_dir = try std.fs.path.join(allocator, &.{ current, ".zawinski" });
        if (std.fs.accessAbsolute(zawinski_dir, .{})) |_| {
            allocator.free(current);
            return zawinski_dir;
        } else |_| {
            allocator.free(zawinski_dir);
        }

        // Try parent directory
        const parent = std.fs.path.dirname(current);
        if (parent == null or std.mem.eql(u8, parent.?, current)) {
            allocator.free(current);
            return StoreError.StoreNotFound;
        }
        const new_current = try allocator.dupe(u8, parent.?);
        allocator.free(current);
        current = new_current;
    }
}
