const std = @import("std");
const zawinski = @import("zawinski");

const Store = zawinski.store.Store;
const StoreError = zawinski.store.StoreError;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args_z = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args_z);

    var args_list: std.ArrayListUnmanaged([]const u8) = .empty;
    defer args_list.deinit(allocator);
    for (args_z) |arg_z| {
        try args_list.append(allocator, arg_z[0..arg_z.len]);
    }
    const all_args = try args_list.toOwnedSlice(allocator);
    defer allocator.free(all_args);

    var stdout_buf: [4096]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buf);
    const stdout = &stdout_writer.interface;
    defer stdout.flush() catch {};

    // Parse global --store flag (before command)
    var explicit_store: ?[]const u8 = null;
    var args: std.ArrayListUnmanaged([]const u8) = .empty;
    defer args.deinit(allocator);

    var i: usize = 0;
    while (i < all_args.len) {
        if (std.mem.eql(u8, all_args[i], "--store") and i + 1 < all_args.len) {
            explicit_store = all_args[i + 1];
            i += 2;
        } else {
            try args.append(allocator, all_args[i]);
            i += 1;
        }
    }

    if (args.items.len < 2) {
        try printUsage(stdout);
        try stdout.flush();
        return;
    }

    const cmd = args.items[1];

    // Handle init before store discovery
    if (std.mem.eql(u8, cmd, "init")) {
        cmdInit(allocator, stdout, args.items[2..], explicit_store) catch |err| {
            dieOnError(err);
        };
        try stdout.flush();
        return;
    }

    // Discover or use explicit store path
    const store_dir = if (explicit_store) |sp|
        resolveStorePath(allocator, sp) catch |err| dieOnError(err)
    else
        zawinski.store.discoverStoreDir(allocator) catch |err| dieOnError(err);
    defer allocator.free(store_dir);

    var store = Store.open(allocator, store_dir) catch |err| {
        dieOnError(err);
    };
    defer store.deinit();

    store.importIfNeeded() catch |err| {
        dieOnError(err);
    };

    const result: anyerror!void = blk: {
        if (std.mem.eql(u8, cmd, "topic")) {
            break :blk cmdTopic(allocator, stdout, &store, args.items[2..]);
        } else if (std.mem.eql(u8, cmd, "post")) {
            break :blk cmdPost(allocator, stdout, &store, args.items[2..]);
        } else if (std.mem.eql(u8, cmd, "reply")) {
            break :blk cmdReply(allocator, stdout, &store, args.items[2..]);
        } else if (std.mem.eql(u8, cmd, "read")) {
            break :blk cmdRead(allocator, stdout, &store, args.items[2..]);
        } else if (std.mem.eql(u8, cmd, "show")) {
            break :blk cmdShow(allocator, stdout, &store, args.items[2..]);
        } else if (std.mem.eql(u8, cmd, "thread")) {
            break :blk cmdThread(allocator, stdout, &store, args.items[2..]);
        } else if (std.mem.eql(u8, cmd, "search")) {
            break :blk cmdSearch(allocator, stdout, &store, args.items[2..]);
        } else {
            die("unknown command: {s}", .{cmd});
        }
    };

    result catch |err| {
        dieOnError(err);
    };
}

fn printUsage(stdout: anytype) !void {
    try stdout.writeAll(
        \\Usage: jwz [--store PATH] <command> [options]
        \\
        \\Commands:
        \\  init                    Initialize a new store
        \\  topic new <name>        Create a new topic
        \\  topic list              List all topics
        \\  post <topic> -m <msg>   Post a message to a topic
        \\  reply <id> -m <msg>     Reply to a message
        \\  read <topic>            Read messages in a topic
        \\  show <id>               Show a message
        \\  thread <id>             Show a message and all replies
        \\  search <query>          Search messages
        \\
        \\Global Options:
        \\  --store PATH            Use store at PATH instead of auto-discovery
        \\
        \\Command Options:
        \\  --json                  Output as JSON
        \\  --quiet                 Output only ID
        \\
    );
}

fn cmdInit(allocator: std.mem.Allocator, stdout: anytype, args: []const []const u8, explicit_store: ?[]const u8) !void {
    var json = false;
    var i: usize = 0;
    while (i < args.len) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--json")) {
            json = true;
        }
        i += 1;
    }

    // Determine store directory
    const store_dir = if (explicit_store) |sp|
        try resolveStorePath(allocator, sp)
    else blk: {
        var cwd_buf: [std.fs.max_path_bytes]u8 = undefined;
        const cwd = try std.fs.cwd().realpath(".", &cwd_buf);
        break :blk try std.fs.path.join(allocator, &.{ cwd, ".zawinski" });
    };
    defer allocator.free(store_dir);

    // Create parent directories if needed
    if (std.fs.path.dirname(store_dir)) |parent| {
        std.fs.makeDirAbsolute(parent) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };
    }

    Store.init(allocator, store_dir) catch |err| switch (err) {
        error.StoreNotFound => {
            die("store already exists", .{});
        },
        else => return err,
    };

    var store = try Store.open(allocator, store_dir);
    defer store.deinit();

    if (json) {
        const record = .{ .store = store_dir };
        try std.json.Stringify.value(record, .{ .whitespace = .minified }, stdout);
        try stdout.writeByte('\n');
    } else {
        try stdout.print("Initialized store in {s}\n", .{store_dir});
    }
}

fn cmdTopic(allocator: std.mem.Allocator, stdout: anytype, store: *Store, args: []const []const u8) !void {
    if (args.len < 1) {
        die("usage: jwz topic <new|list> [options]", .{});
    }

    const subcmd = args[0];

    if (std.mem.eql(u8, subcmd, "new")) {
        try cmdTopicNew(allocator, stdout, store, args[1..]);
    } else if (std.mem.eql(u8, subcmd, "list")) {
        try cmdTopicList(allocator, stdout, store, args[1..]);
    } else {
        die("unknown topic subcommand: {s}", .{subcmd});
    }
}

fn cmdTopicNew(allocator: std.mem.Allocator, stdout: anytype, store: *Store, args: []const []const u8) !void {
    var name: ?[]const u8 = null;
    var description: []const u8 = "";
    var json = false;
    var quiet = false;

    var i: usize = 0;
    while (i < args.len) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--json")) {
            json = true;
            i += 1;
        } else if (std.mem.eql(u8, arg, "--quiet")) {
            quiet = true;
            i += 1;
        } else if (std.mem.eql(u8, arg, "-d") or std.mem.eql(u8, arg, "--description")) {
            description = nextValue(args, &i, "description");
        } else if (arg.len == 0 or arg[0] != '-') {
            name = arg;
            i += 1;
        } else {
            die("unknown option: {s}", .{arg});
        }
    }

    if (name == null) {
        die("usage: jwz topic new <name> [-d description] [--json|--quiet]", .{});
    }

    const id = try store.createTopic(name.?, description);
    defer allocator.free(id);

    if (quiet) {
        try stdout.print("{s}\n", .{id});
    } else if (json) {
        const record = .{ .id = id, .name = name.?, .description = description };
        try std.json.Stringify.value(record, .{ .whitespace = .minified }, stdout);
        try stdout.writeByte('\n');
    } else {
        try stdout.print("Created topic: {s}\n", .{name.?});
    }
}

fn cmdTopicList(allocator: std.mem.Allocator, stdout: anytype, store: *Store, args: []const []const u8) !void {
    var json = false;
    var i: usize = 0;
    while (i < args.len) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--json")) {
            json = true;
        }
        i += 1;
    }

    const topics = try store.listTopics();
    defer {
        for (topics) |*t| {
            var topic = t.*;
            topic.deinit(allocator);
        }
        allocator.free(topics);
    }

    if (json) {
        try stdout.writeByte('[');
        for (topics, 0..) |topic, idx| {
            if (idx > 0) try stdout.writeByte(',');
            const record = .{
                .id = topic.id,
                .name = topic.name,
                .description = topic.description,
                .created_at = topic.created_at,
            };
            try std.json.Stringify.value(record, .{ .whitespace = .minified }, stdout);
        }
        try stdout.writeAll("]\n");
    } else {
        if (topics.len == 0) {
            try stdout.writeAll("No topics found.\n");
        } else {
            for (topics) |topic| {
                try stdout.print("{s}", .{topic.name});
                if (topic.description.len > 0) {
                    try stdout.print(" - {s}", .{topic.description});
                }
                try stdout.writeByte('\n');
            }
        }
    }
}

fn cmdPost(allocator: std.mem.Allocator, stdout: anytype, store: *Store, args: []const []const u8) !void {
    var topic_name: ?[]const u8 = null;
    var body: ?[]const u8 = null;
    var json = false;
    var quiet = false;

    var i: usize = 0;
    while (i < args.len) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--json")) {
            json = true;
            i += 1;
        } else if (std.mem.eql(u8, arg, "--quiet")) {
            quiet = true;
            i += 1;
        } else if (std.mem.eql(u8, arg, "-m") or std.mem.eql(u8, arg, "--message")) {
            body = nextValue(args, &i, "message");
        } else if (arg.len == 0 or arg[0] != '-') {
            topic_name = arg;
            i += 1;
        } else {
            die("unknown option: {s}", .{arg});
        }
    }

    if (topic_name == null or body == null) {
        die("usage: jwz post <topic> -m <message> [--json|--quiet]", .{});
    }

    const processed_body = processEscapes(allocator, body.?) catch body.?;
    defer if (processed_body.ptr != body.?.ptr) allocator.free(processed_body);

    const id = try store.createMessage(topic_name.?, null, processed_body);
    defer allocator.free(id);

    if (quiet) {
        try stdout.print("{s}\n", .{id});
    } else if (json) {
        const record = .{ .id = id, .topic = topic_name.? };
        try std.json.Stringify.value(record, .{ .whitespace = .minified }, stdout);
        try stdout.writeByte('\n');
    } else {
        try stdout.print("Posted: {s}\n", .{id});
    }
}

fn cmdReply(allocator: std.mem.Allocator, stdout: anytype, store: *Store, args: []const []const u8) !void {
    var parent_id: ?[]const u8 = null;
    var body: ?[]const u8 = null;
    var json = false;
    var quiet = false;

    var i: usize = 0;
    while (i < args.len) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--json")) {
            json = true;
            i += 1;
        } else if (std.mem.eql(u8, arg, "--quiet")) {
            quiet = true;
            i += 1;
        } else if (std.mem.eql(u8, arg, "-m") or std.mem.eql(u8, arg, "--message")) {
            body = nextValue(args, &i, "message");
        } else if (arg.len == 0 or arg[0] != '-') {
            parent_id = arg;
            i += 1;
        } else {
            die("unknown option: {s}", .{arg});
        }
    }

    if (parent_id == null or body == null) {
        die("usage: jwz reply <message-id> -m <message> [--json|--quiet]", .{});
    }

    // Fetch parent to get topic
    const parent = try store.fetchMessage(parent_id.?);
    defer {
        var p = parent;
        p.deinit(allocator);
    }

    // Get topic name
    const stmt = try zawinski.sqlite.prepare(store.db, "SELECT name FROM topics WHERE id = ?;");
    defer zawinski.sqlite.finalize(stmt);
    try zawinski.sqlite.bindText(stmt, 1, parent.topic_id);
    const topic_name = if (try zawinski.sqlite.step(stmt)) zawinski.sqlite.columnText(stmt, 0) else return StoreError.TopicNotFound;

    const processed_body = processEscapes(allocator, body.?) catch body.?;
    defer if (processed_body.ptr != body.?.ptr) allocator.free(processed_body);

    const id = try store.createMessage(topic_name, parent.id, processed_body);
    defer allocator.free(id);

    if (quiet) {
        try stdout.print("{s}\n", .{id});
    } else if (json) {
        const record = .{ .id = id, .parent_id = parent.id };
        try std.json.Stringify.value(record, .{ .whitespace = .minified }, stdout);
        try stdout.writeByte('\n');
    } else {
        try stdout.print("Replied: {s}\n", .{id});
    }
}

fn cmdRead(allocator: std.mem.Allocator, stdout: anytype, store: *Store, args: []const []const u8) !void {
    var topic_name: ?[]const u8 = null;
    var limit: u32 = 20;
    var json = false;

    var i: usize = 0;
    while (i < args.len) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--json")) {
            json = true;
            i += 1;
        } else if (std.mem.eql(u8, arg, "--limit")) {
            const val = nextValue(args, &i, "limit");
            limit = std.fmt.parseInt(u32, val, 10) catch {
                die("invalid limit: {s}", .{val});
            };
        } else if (arg.len == 0 or arg[0] != '-') {
            topic_name = arg;
            i += 1;
        } else {
            die("unknown option: {s}", .{arg});
        }
    }

    if (topic_name == null) {
        die("usage: jwz read <topic> [--limit N] [--json]", .{});
    }

    const topic = try store.fetchTopic(topic_name.?);
    defer {
        var t = topic;
        t.deinit(allocator);
    }

    const messages = try store.listMessages(topic_name.?, limit);
    defer {
        for (messages) |*m| {
            var msg = m.*;
            msg.deinit(allocator);
        }
        allocator.free(messages);
    }

    if (json) {
        try stdout.writeByte('[');
        for (messages, 0..) |msg, idx| {
            if (idx > 0) try stdout.writeByte(',');
            try writeMessageJson(stdout, msg);
        }
        try stdout.writeAll("]\n");
    } else {
        try stdout.print("{s}", .{topic.name});
        if (topic.description.len > 0) {
            try stdout.print(": {s}", .{topic.description});
        }
        try stdout.writeByte('\n');
        try stdout.writeAll("─────────────────────────\n");

        if (messages.len == 0) {
            try stdout.writeAll("No messages.\n");
        } else {
            for (messages) |msg| {
                try printMessageTree(allocator, stdout, store, msg, 0);
            }
        }
    }
}

fn cmdShow(allocator: std.mem.Allocator, stdout: anytype, store: *Store, args: []const []const u8) !void {
    var message_id: ?[]const u8 = null;
    var json = false;

    var i: usize = 0;
    while (i < args.len) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--json")) {
            json = true;
            i += 1;
        } else if (arg.len == 0 or arg[0] != '-') {
            message_id = arg;
            i += 1;
        } else {
            die("unknown option: {s}", .{arg});
        }
    }

    if (message_id == null) {
        die("usage: jwz show <message-id> [--json]", .{});
    }

    const msg = try store.fetchMessage(message_id.?);
    defer {
        var m = msg;
        m.deinit(allocator);
    }

    if (json) {
        try writeMessageJson(stdout, msg);
        try stdout.writeByte('\n');
    } else {
        try stdout.print("{s}", .{msg.id});
        if (msg.reply_count > 0) {
            try stdout.print(" ({d} replies)", .{msg.reply_count});
        }
        try stdout.print(" {s}\n", .{formatTimeAgo(msg.created_at)});
        try stdout.print("  {s}\n", .{msg.body});
    }
}

fn cmdThread(allocator: std.mem.Allocator, stdout: anytype, store: *Store, args: []const []const u8) !void {
    var message_id: ?[]const u8 = null;
    var json = false;

    var i: usize = 0;
    while (i < args.len) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--json")) {
            json = true;
            i += 1;
        } else if (arg.len == 0 or arg[0] != '-') {
            message_id = arg;
            i += 1;
        } else {
            die("unknown option: {s}", .{arg});
        }
    }

    if (message_id == null) {
        die("usage: jwz thread <message-id> [--json]", .{});
    }

    const messages = try store.fetchThread(message_id.?);
    defer {
        for (messages) |*m| {
            var msg = m.*;
            msg.deinit(allocator);
        }
        allocator.free(messages);
    }

    if (json) {
        try stdout.writeByte('[');
        for (messages, 0..) |msg, idx| {
            if (idx > 0) try stdout.writeByte(',');
            try writeMessageJson(stdout, msg);
        }
        try stdout.writeAll("]\n");
    } else {
        // Build depth map for display
        var depth_map = std.StringHashMap(u32).init(allocator);
        defer depth_map.deinit();

        for (messages) |msg| {
            const depth: u32 = if (msg.parent_id) |pid| blk: {
                const parent_depth = depth_map.get(pid) orelse 0;
                break :blk parent_depth + 1;
            } else 0;
            try depth_map.put(msg.id, depth);

            // Print with indentation
            var indent_i: u32 = 0;
            while (indent_i < depth) : (indent_i += 1) {
                try stdout.writeAll("  ");
            }
            if (depth > 0) {
                try stdout.writeAll("└─ ");
            } else {
                try stdout.writeAll("▶ ");
            }

            try stdout.print("{s}", .{msg.id});
            if (msg.reply_count > 0) {
                try stdout.print(" ({d} replies)", .{msg.reply_count});
            }
            try stdout.print(" {s}\n", .{formatTimeAgo(msg.created_at)});

            // Print body with indentation
            indent_i = 0;
            while (indent_i < depth + 1) : (indent_i += 1) {
                try stdout.writeAll("  ");
            }
            try stdout.print("{s}\n\n", .{msg.body});
        }
    }
}

fn cmdSearch(allocator: std.mem.Allocator, stdout: anytype, store: *Store, args: []const []const u8) !void {
    var query: ?[]const u8 = null;
    var topic_name: ?[]const u8 = null;
    var limit: u32 = 20;
    var json = false;

    var i: usize = 0;
    while (i < args.len) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--json")) {
            json = true;
            i += 1;
        } else if (std.mem.eql(u8, arg, "--topic")) {
            topic_name = nextValue(args, &i, "topic");
        } else if (std.mem.eql(u8, arg, "--limit")) {
            const val = nextValue(args, &i, "limit");
            limit = std.fmt.parseInt(u32, val, 10) catch {
                die("invalid limit: {s}", .{val});
            };
        } else if (arg.len == 0 or arg[0] != '-') {
            query = arg;
            i += 1;
        } else {
            die("unknown option: {s}", .{arg});
        }
    }

    if (query == null) {
        die("usage: jwz search <query> [--topic t] [--limit N] [--json]", .{});
    }

    const messages = try store.searchMessages(query.?, topic_name, limit);
    defer {
        for (messages) |*m| {
            var msg = m.*;
            msg.deinit(allocator);
        }
        allocator.free(messages);
    }

    if (json) {
        try stdout.writeByte('[');
        for (messages, 0..) |msg, idx| {
            if (idx > 0) try stdout.writeByte(',');
            try writeMessageJson(stdout, msg);
        }
        try stdout.writeAll("]\n");
    } else {
        if (messages.len == 0) {
            try stdout.writeAll("No results found.\n");
        } else {
            try stdout.print("Found {d} result(s):\n\n", .{messages.len});
            for (messages) |msg| {
                try stdout.print("{s} {s}\n", .{ msg.id, formatTimeAgo(msg.created_at) });
                // Truncate body for display
                const max_len: usize = 80;
                const display_body = if (msg.body.len > max_len) msg.body[0..max_len] else msg.body;
                try stdout.print("  {s}", .{display_body});
                if (msg.body.len > max_len) try stdout.writeAll("...");
                try stdout.writeAll("\n\n");
            }
        }
    }
}

// ========== Helpers ==========

fn printMessageTree(allocator: std.mem.Allocator, stdout: anytype, store: *Store, msg: zawinski.store.Message, depth: u32) !void {
    // Print indentation
    var indent_i: u32 = 0;
    while (indent_i < depth) : (indent_i += 1) {
        try stdout.writeAll("  ");
    }
    if (depth > 0) {
        try stdout.writeAll("├─ ");
    } else {
        try stdout.writeAll("▶ ");
    }

    try stdout.print("{s}", .{msg.id});
    if (msg.reply_count > 0) {
        try stdout.print(" ({d} replies)", .{msg.reply_count});
    }
    try stdout.print(" {s}\n", .{formatTimeAgo(msg.created_at)});

    // Print body
    indent_i = 0;
    while (indent_i < depth + 1) : (indent_i += 1) {
        try stdout.writeAll("  ");
    }
    try stdout.print("{s}\n\n", .{msg.body});

    // Print replies (only first level for read command)
    if (depth < 2) {
        const replies = try store.fetchReplies(msg.id);
        defer {
            for (replies) |*r| {
                var reply = r.*;
                reply.deinit(allocator);
            }
            allocator.free(replies);
        }
        for (replies) |reply| {
            try printMessageTree(allocator, stdout, store, reply, depth + 1);
        }
    }
}

fn writeMessageJson(stdout: anytype, msg: zawinski.store.Message) !void {
    const record = .{
        .id = msg.id,
        .topic_id = msg.topic_id,
        .parent_id = msg.parent_id,
        .body = msg.body,
        .created_at = msg.created_at,
        .reply_count = msg.reply_count,
    };
    try std.json.Stringify.value(record, .{ .whitespace = .minified }, stdout);
}

fn formatTimeAgo(timestamp_ms: i64) []const u8 {
    const now_ms = @as(i64, @intCast(std.time.milliTimestamp()));
    const diff_ms = now_ms - timestamp_ms;
    const diff_s = @divFloor(diff_ms, 1000);

    if (diff_s < 60) return "just now";
    if (diff_s < 3600) return "minutes ago";
    if (diff_s < 86400) return "hours ago";
    return "days ago";
}

fn processEscapes(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    var output: std.ArrayList(u8) = .empty;
    errdefer output.deinit(allocator);

    var i: usize = 0;
    while (i < input.len) {
        if (input[i] == '\\' and i + 1 < input.len) {
            switch (input[i + 1]) {
                'n' => {
                    try output.append(allocator, '\n');
                    i += 2;
                },
                't' => {
                    try output.append(allocator, '\t');
                    i += 2;
                },
                '\\' => {
                    try output.append(allocator, '\\');
                    i += 2;
                },
                else => {
                    try output.append(allocator, input[i]);
                    i += 1;
                },
            }
        } else {
            try output.append(allocator, input[i]);
            i += 1;
        }
    }

    return output.toOwnedSlice(allocator);
}

fn resolveStorePath(allocator: std.mem.Allocator, path: []const u8) ![]const u8 {
    // If absolute, use as-is
    if (std.fs.path.isAbsolute(path)) {
        return allocator.dupe(u8, path);
    }
    // Otherwise, resolve relative to cwd
    var cwd_buf: [std.fs.max_path_bytes]u8 = undefined;
    const cwd = try std.fs.cwd().realpath(".", &cwd_buf);
    return std.fs.path.join(allocator, &.{ cwd, path });
}

fn nextValue(args: []const []const u8, index: *usize, name: []const u8) []const u8 {
    const i = index.*;
    if (i + 1 >= args.len) {
        die("missing value for {s}", .{name});
    }
    index.* = i + 2;
    return args[i + 1];
}

fn die(comptime fmt: []const u8, args: anytype) noreturn {
    std.debug.print(fmt ++ "\n", args);
    std.process.exit(1);
}

fn dieOnError(err: anyerror) noreturn {
    const msg: []const u8 = switch (err) {
        StoreError.StoreNotFound => "No store found. Run 'jwz init' or use --store.",
        StoreError.TopicNotFound => "Topic not found.",
        StoreError.TopicExists => "Topic already exists.",
        StoreError.MessageNotFound => "Message not found.",
        StoreError.MessageIdAmbiguous => "Ambiguous message ID: matches multiple messages. Use more characters.",
        StoreError.InvalidMessageId => "Invalid message ID.",
        StoreError.ParentNotFound => "Parent message not found.",
        StoreError.DatabaseBusy => "Database busy. Please retry.",
        StoreError.EmptyTopicName => "Topic name cannot be empty.",
        StoreError.EmptyMessageBody => "Message body cannot be empty.",
        zawinski.sqlite.Error.SqliteBusy => "Database busy. Please retry.",
        zawinski.sqlite.Error.SqliteError => "Database error.",
        zawinski.sqlite.Error.SqliteStepError => "Database query error.",
        else => {
            std.debug.print("error: {s}\n", .{@errorName(err)});
            std.process.exit(1);
        },
    };
    std.debug.print("{s}\n", .{msg});
    std.process.exit(1);
}
