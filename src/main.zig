const std = @import("std");
const zawinski = @import("zawinski");

const Store = zawinski.store.Store;
const StoreError = zawinski.store.StoreError;
const Sender = zawinski.store.Sender;
const GitMeta = zawinski.store.GitMeta;
const CreateMessageOptions = Store.CreateMessageOptions;

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

    // Handle version before anything else
    if (std.mem.eql(u8, cmd, "version") or std.mem.eql(u8, cmd, "--version") or std.mem.eql(u8, cmd, "-V")) {
        try stdout.writeAll("jwz 0.6.2\n");
        try stdout.flush();
        return;
    }

    // Handle help
    if (std.mem.eql(u8, cmd, "help") or std.mem.eql(u8, cmd, "--help") or std.mem.eql(u8, cmd, "-h")) {
        try printUsage(stdout);
        try stdout.flush();
        return;
    }

    // Handle init before store discovery
    if (std.mem.eql(u8, cmd, "init")) {
        cmdInit(allocator, stdout, args.items[2..], explicit_store) catch |err| {
            dieOnError(err);
        };
        try stdout.flush();
        return;
    }

    // Discover or use explicit store path
    // For 'post' command, auto-initialize if no store found
    const store_dir = if (explicit_store) |sp|
        resolveStorePath(allocator, sp) catch |err| dieOnError(err)
    else blk: {
        break :blk zawinski.store.discoverStoreDir(allocator) catch |err| {
            if (err == StoreError.StoreNotFound and std.mem.eql(u8, cmd, "post")) {
                // Auto-initialize store in cwd (silent for scripting)
                var cwd_buf: [std.fs.max_path_bytes]u8 = undefined;
                const cwd = std.fs.cwd().realpath(".", &cwd_buf) catch |e| dieOnError(e);
                const new_store = std.fs.path.join(allocator, &.{ cwd, ".jwz" }) catch |e| dieOnError(e);
                Store.init(allocator, new_store) catch |init_err| {
                    allocator.free(new_store);
                    dieOnError(init_err);
                };
                break :blk new_store;
            }
            dieOnError(err);
        };
    };
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
        } else if (std.mem.eql(u8, cmd, "read") or std.mem.eql(u8, cmd, "list")) {
            break :blk cmdRead(allocator, stdout, &store, args.items[2..]);
        } else if (std.mem.eql(u8, cmd, "show")) {
            break :blk cmdShow(allocator, stdout, &store, args.items[2..]);
        } else if (std.mem.eql(u8, cmd, "thread")) {
            break :blk cmdThread(allocator, stdout, &store, args.items[2..]);
        } else if (std.mem.eql(u8, cmd, "search")) {
            break :blk cmdSearch(allocator, stdout, &store, args.items[2..]);
        } else if (std.mem.eql(u8, cmd, "blob")) {
            break :blk cmdBlob(allocator, stdout, &store, args.items[2..]);
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
        \\  init                    Initialize a new store (auto-created on post)
        \\  topic new <name>        Create a new topic (auto-created on post)
        \\  topic list              List all topics
        \\  post <topic> -m <msg>   Post a message (auto-inits store and topic)
        \\  reply <id> -m <msg>     Reply to a message
        \\  list <topic>            List messages in a topic (alias: read)
        \\  show <id>               Show a message
        \\  thread <id>             Show a message and all replies
        \\  search <query>          Search messages
        \\  blob put <file>         Store a blob, output content hash
        \\  blob get <hash>         Retrieve blob data by hash
        \\  blob info <hash>        Show blob metadata
        \\  help                    Show this help
        \\
        \\Global Options:
        \\  --store PATH            Use store at PATH instead of auto-discovery
        \\
        \\Read/Thread Options:
        \\  -s, --summary           Show first line of body only (truncated to 80 chars)
        \\
        \\Identity Options (post/reply):
        \\  --as ID                 Sender ID (auto-generated if omitted)
        \\  --model MODEL           Model name (e.g., claude-3-opus)
        \\  --role ROLE             Role description (e.g., code-reviewer)
        \\
        \\Output Options:
        \\  --json                  Output as JSON (thread includes depth field)
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
        break :blk try std.fs.path.join(allocator, &.{ cwd, ".jwz" });
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
        error.StoreAlreadyExists => {
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
    var sender_id_arg: ?[]const u8 = null;
    var model_arg: ?[]const u8 = null;
    var role_arg: ?[]const u8 = null;

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
        } else if (std.mem.eql(u8, arg, "--as")) {
            sender_id_arg = nextValue(args, &i, "as");
        } else if (std.mem.eql(u8, arg, "--model")) {
            model_arg = nextValue(args, &i, "model");
        } else if (std.mem.eql(u8, arg, "--role")) {
            role_arg = nextValue(args, &i, "role");
        } else if (arg.len == 0 or arg[0] != '-') {
            topic_name = arg;
            i += 1;
        } else {
            die("unknown option: {s}", .{arg});
        }
    }

    if (topic_name == null or body == null) {
        die("usage: jwz post <topic> -m <message> [--as ID] [--model M] [--role R] [--json|--quiet]", .{});
    }

    // Auto-create topic if it doesn't exist
    if (store.createTopic(topic_name.?, "")) |topic_id| {
        // New topic was created - warn about UUID-like names (likely a mistake)
        if (looksLikeUuid(topic_name.?)) {
            std.debug.print("warning: topic '{s}' looks like a UUID\n", .{topic_name.?});
            std.debug.print("hint: use descriptive names like 'tasks' or 'research:myproject'\n\n", .{});
        }
        allocator.free(topic_id);
    } else |err| switch (err) {
        StoreError.TopicExists => {}, // Already exists - idempotent
        else => return err,
    }

    const processed_body = processEscapes(allocator, body.?) catch body.?;
    defer if (processed_body.ptr != body.?.ptr) allocator.free(processed_body);

    // Build sender if any identity flags provided
    var options: CreateMessageOptions = .{};
    var sender_id_alloc: ?[]u8 = null;
    defer if (sender_id_alloc) |sid| allocator.free(sid);

    if (sender_id_arg != null or model_arg != null or role_arg != null) {
        const sender_id = if (sender_id_arg) |sid| sid else blk: {
            sender_id_alloc = try store.ulid.nextNow(allocator);
            break :blk sender_id_alloc.?;
        };
        options.sender = Sender{
            .id = sender_id,
            .name = zawinski.names.fromUlid(sender_id),
            .model = model_arg,
            .role = role_arg,
        };
    }

    // Capture git metadata
    if (zawinski.git.capture(allocator)) |git_info| {
        options.git = GitMeta{
            .oid = git_info.oid,
            .head = git_info.head,
            .dirty = git_info.dirty,
            .prefix = git_info.prefix,
        };
    }
    defer if (options.git) |*g| {
        allocator.free(g.oid);
        allocator.free(g.head);
        allocator.free(g.prefix);
    };

    const id = try store.createMessage(topic_name.?, null, processed_body, options);
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
    var sender_id_arg: ?[]const u8 = null;
    var model_arg: ?[]const u8 = null;
    var role_arg: ?[]const u8 = null;

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
        } else if (std.mem.eql(u8, arg, "--as")) {
            sender_id_arg = nextValue(args, &i, "as");
        } else if (std.mem.eql(u8, arg, "--model")) {
            model_arg = nextValue(args, &i, "model");
        } else if (std.mem.eql(u8, arg, "--role")) {
            role_arg = nextValue(args, &i, "role");
        } else if (arg.len == 0 or arg[0] != '-') {
            parent_id = arg;
            i += 1;
        } else {
            die("unknown option: {s}", .{arg});
        }
    }

    if (parent_id == null or body == null) {
        die("usage: jwz reply <message-id> -m <message> [--as ID] [--model M] [--role R] [--json|--quiet]", .{});
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

    // Build sender if any identity flags provided
    var options: CreateMessageOptions = .{};
    var sender_id_alloc: ?[]u8 = null;
    defer if (sender_id_alloc) |sid| allocator.free(sid);

    if (sender_id_arg != null or model_arg != null or role_arg != null) {
        const sender_id = if (sender_id_arg) |sid| sid else blk: {
            sender_id_alloc = try store.ulid.nextNow(allocator);
            break :blk sender_id_alloc.?;
        };
        options.sender = Sender{
            .id = sender_id,
            .name = zawinski.names.fromUlid(sender_id),
            .model = model_arg,
            .role = role_arg,
        };
    }

    // Capture git metadata
    if (zawinski.git.capture(allocator)) |git_info| {
        options.git = GitMeta{
            .oid = git_info.oid,
            .head = git_info.head,
            .dirty = git_info.dirty,
            .prefix = git_info.prefix,
        };
    }
    defer if (options.git) |*g| {
        allocator.free(g.oid);
        allocator.free(g.head);
        allocator.free(g.prefix);
    };

    const id = try store.createMessage(topic_name, parent.id, processed_body, options);
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
    var summary = false;

    var i: usize = 0;
    while (i < args.len) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--json")) {
            json = true;
            i += 1;
        } else if (std.mem.eql(u8, arg, "--summary") or std.mem.eql(u8, arg, "-s")) {
            summary = true;
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
        die("usage: jwz read <topic> [--limit N] [--summary] [--json]", .{});
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
            try writeMessageJsonWithDepth(stdout, msg, 0); // Top-level messages have depth 0
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
            for (messages, 0..) |msg, idx| {
                const is_last = (idx == messages.len - 1);
                try printMessageTree(allocator, stdout, store, msg, 0, is_last, summary);
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
        if (msg.sender) |sender| {
            try stdout.print(" by {s}", .{sender.name});
            if (sender.model) |model| {
                try stdout.print(" [{s}]", .{model});
            }
        }
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
    var summary = false;

    var i: usize = 0;
    while (i < args.len) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--json")) {
            json = true;
            i += 1;
        } else if (std.mem.eql(u8, arg, "--summary") or std.mem.eql(u8, arg, "-s")) {
            summary = true;
            i += 1;
        } else if (arg.len == 0 or arg[0] != '-') {
            message_id = arg;
            i += 1;
        } else {
            die("unknown option: {s}", .{arg});
        }
    }

    if (message_id == null) {
        die("usage: jwz thread <message-id> [--summary] [--json]", .{});
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
        // Build depth map for JSON output
        var depth_map = std.StringHashMap(u32).init(allocator);
        defer depth_map.deinit();

        try stdout.writeByte('[');
        for (messages, 0..) |msg, idx| {
            const depth: u32 = if (msg.parent_id) |pid| blk: {
                const parent_depth = depth_map.get(pid) orelse 0;
                break :blk parent_depth + 1;
            } else 0;
            try depth_map.put(msg.id, depth);

            if (idx > 0) try stdout.writeByte(',');
            try writeMessageJsonWithDepth(stdout, msg, depth);
        }
        try stdout.writeAll("]\n");
    } else {
        // Build depth map and last-sibling map for display
        var depth_map = std.StringHashMap(u32).init(allocator);
        defer depth_map.deinit();

        // Find the last child for each parent (for correct tree symbols)
        var last_child_map = std.StringHashMap([]const u8).init(allocator);
        defer last_child_map.deinit();
        for (messages) |msg| {
            if (msg.parent_id) |pid| {
                try last_child_map.put(pid, msg.id);
            }
        }

        for (messages) |msg| {
            const depth: u32 = if (msg.parent_id) |pid| blk: {
                const parent_depth = depth_map.get(pid) orelse 0;
                break :blk parent_depth + 1;
            } else 0;
            try depth_map.put(msg.id, depth);

            // Determine if this is the last sibling
            const is_last_sibling = if (msg.parent_id) |pid|
                if (last_child_map.get(pid)) |last_id| std.mem.eql(u8, last_id, msg.id) else true
            else
                true;

            // Print with indentation
            var indent_i: u32 = 0;
            while (indent_i < depth) : (indent_i += 1) {
                try stdout.writeAll("  ");
            }
            if (depth > 0) {
                if (is_last_sibling) {
                    try stdout.writeAll("└─ ");
                } else {
                    try stdout.writeAll("├─ ");
                }
            } else {
                try stdout.writeAll("▶ ");
            }

            try stdout.print("{s}", .{msg.id});
            if (msg.sender) |sender| {
                // Show role prominently if available, otherwise show name
                if (sender.role) |role| {
                    try stdout.print(" by {s}", .{role});
                } else {
                    try stdout.print(" by {s}", .{sender.name});
                }
                if (sender.model) |model| {
                    try stdout.print(" [{s}]", .{model});
                }
            }
            if (msg.reply_count > 0) {
                try stdout.print(" ({d} replies)", .{msg.reply_count});
            }
            try stdout.print(" {s}\n", .{formatTimeAgo(msg.created_at)});

            // Print body with indentation (truncated in summary mode)
            const body_indent = depth + 1;
            indent_i = 0;
            while (indent_i < body_indent) : (indent_i += 1) {
                try stdout.writeAll("  ");
            }
            if (summary) {
                // Show first line only, truncated to 80 chars (UTF-8 safe)
                const first_line = if (std.mem.indexOf(u8, msg.body, "\n")) |nl| msg.body[0..nl] else msg.body;
                const truncated = truncateUtf8(first_line, 77);
                try stdout.writeAll(truncated);
                if (first_line.len > 77) try stdout.writeAll("...");
                try stdout.writeAll("\n\n");
            } else {
                try printIndented(stdout, msg.body, body_indent);
                try stdout.writeAll("\n\n");
            }
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

fn cmdBlob(allocator: std.mem.Allocator, stdout: anytype, store: *Store, args: []const []const u8) !void {
    if (args.len < 1) {
        die("usage: jwz blob <put|get|info> [options]", .{});
    }

    const subcmd = args[0];

    if (std.mem.eql(u8, subcmd, "put")) {
        try cmdBlobPut(allocator, stdout, store, args[1..]);
    } else if (std.mem.eql(u8, subcmd, "get")) {
        try cmdBlobGet(allocator, stdout, store, args[1..]);
    } else if (std.mem.eql(u8, subcmd, "info")) {
        try cmdBlobInfo(allocator, stdout, store, args[1..]);
    } else {
        die("unknown blob subcommand: {s}", .{subcmd});
    }
}

fn cmdBlobPut(allocator: std.mem.Allocator, stdout: anytype, store: *Store, args: []const []const u8) !void {
    var file_path: ?[]const u8 = null;
    var mime_type: ?[]const u8 = null;
    var json = false;

    var i: usize = 0;
    while (i < args.len) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--json")) {
            json = true;
            i += 1;
        } else if (std.mem.eql(u8, arg, "--mime") or std.mem.eql(u8, arg, "-t")) {
            mime_type = nextValue(args, &i, "mime");
        } else if (arg.len == 0 or arg[0] != '-') {
            file_path = arg;
            i += 1;
        } else {
            die("unknown option: {s}", .{arg});
        }
    }

    if (file_path == null) {
        die("usage: jwz blob put <file> [--mime TYPE] [--json]", .{});
    }

    // Read file contents
    const file = std.fs.cwd().openFile(file_path.?, .{}) catch |err| {
        die("cannot open file: {s}", .{@errorName(err)});
    };
    defer file.close();

    const data = file.readToEndAlloc(allocator, 100 * 1024 * 1024) catch |err| {
        die("cannot read file: {s}", .{@errorName(err)});
    };
    defer allocator.free(data);

    // Store blob
    const blob_id = try store.putBlob(data, mime_type);
    defer allocator.free(blob_id);

    if (json) {
        const record = .{ .id = blob_id, .size = data.len };
        try std.json.Stringify.value(record, .{ .whitespace = .minified }, stdout);
        try stdout.writeByte('\n');
    } else {
        try stdout.print("{s}\n", .{blob_id});
    }
}

fn cmdBlobGet(allocator: std.mem.Allocator, stdout: anytype, store: *Store, args: []const []const u8) !void {
    var blob_id: ?[]const u8 = null;

    var i: usize = 0;
    while (i < args.len) {
        const arg = args[i];
        if (arg.len == 0 or arg[0] != '-') {
            blob_id = arg;
            i += 1;
        } else {
            die("unknown option: {s}", .{arg});
        }
    }

    if (blob_id == null) {
        die("usage: jwz blob get <hash>", .{});
    }

    const data = store.getBlob(blob_id.?) catch |err| {
        if (err == error.BlobNotFound) {
            die("blob not found: {s}", .{blob_id.?});
        }
        return err;
    };
    defer allocator.free(data);

    // Write raw blob data to stdout
    try stdout.writeAll(data);
}

fn cmdBlobInfo(allocator: std.mem.Allocator, stdout: anytype, store: *Store, args: []const []const u8) !void {
    var blob_id: ?[]const u8 = null;
    var json = false;

    var i: usize = 0;
    while (i < args.len) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--json")) {
            json = true;
            i += 1;
        } else if (arg.len == 0 or arg[0] != '-') {
            blob_id = arg;
            i += 1;
        } else {
            die("unknown option: {s}", .{arg});
        }
    }

    if (blob_id == null) {
        die("usage: jwz blob info <hash> [--json]", .{});
    }

    var blob = store.fetchBlob(blob_id.?) catch |err| {
        if (err == error.BlobNotFound) {
            die("blob not found: {s}", .{blob_id.?});
        }
        return err;
    };
    defer blob.deinit(allocator);

    if (json) {
        const record = .{
            .id = blob.id,
            .size = blob.size,
            .mime_type = blob.mime_type,
            .created_at = blob.created_at,
        };
        try std.json.Stringify.value(record, .{ .whitespace = .minified }, stdout);
        try stdout.writeByte('\n');
    } else {
        try stdout.print("ID: {s}\n", .{blob.id});
        try stdout.print("Size: {d} bytes\n", .{blob.size});
        if (blob.mime_type) |mt| {
            try stdout.print("Type: {s}\n", .{mt});
        }
        try stdout.print("Created: {s}\n", .{formatTimeAgo(blob.created_at)});
    }
}

// ========== Helpers ==========

fn printMessageTree(allocator: std.mem.Allocator, stdout: anytype, store: *Store, msg: zawinski.store.Message, depth: u32, is_last: bool, summary: bool) !void {
    // Print indentation
    var indent_i: u32 = 0;
    while (indent_i < depth) : (indent_i += 1) {
        try stdout.writeAll("  ");
    }
    if (depth > 0) {
        if (is_last) {
            try stdout.writeAll("└─ ");
        } else {
            try stdout.writeAll("├─ ");
        }
    } else {
        try stdout.writeAll("▶ ");
    }

    try stdout.print("{s}", .{msg.id});
    if (msg.sender) |sender| {
        // Show role prominently if available, otherwise show name
        if (sender.role) |role| {
            try stdout.print(" by {s}", .{role});
        } else {
            try stdout.print(" by {s}", .{sender.name});
        }
        if (sender.model) |model| {
            try stdout.print(" [{s}]", .{model});
        }
    }
    if (msg.reply_count > 0) {
        try stdout.print(" ({d} replies)", .{msg.reply_count});
    }
    try stdout.print(" {s}\n", .{formatTimeAgo(msg.created_at)});

    // Print body (truncated in summary mode)
    const body_indent = depth + 1;
    indent_i = 0;
    while (indent_i < body_indent) : (indent_i += 1) {
        try stdout.writeAll("  ");
    }
    if (summary) {
        // Show first line only, truncated to 80 chars (UTF-8 safe)
        const first_line = if (std.mem.indexOf(u8, msg.body, "\n")) |nl| msg.body[0..nl] else msg.body;
        const truncated = truncateUtf8(first_line, 77);
        try stdout.writeAll(truncated);
        if (first_line.len > 77) try stdout.writeAll("...");
        try stdout.writeAll("\n\n");
    } else {
        try printIndented(stdout, msg.body, body_indent);
        try stdout.writeAll("\n\n");
    }

    // Print replies (increased depth limit to 10 for better conversation visibility)
    if (depth < 10) {
        const replies = try store.fetchReplies(msg.id);
        defer {
            for (replies) |*r| {
                var reply = r.*;
                reply.deinit(allocator);
            }
            allocator.free(replies);
        }
        for (replies, 0..) |reply, idx| {
            const reply_is_last = (idx == replies.len - 1);
            try printMessageTree(allocator, stdout, store, reply, depth + 1, reply_is_last, summary);
        }
    }
}

fn writeMessageJson(stdout: anytype, msg: zawinski.store.Message) !void {
    // Use separate struct without depth field for show/search (depth not applicable)
    const SenderJson = struct {
        id: []const u8,
        name: []const u8,
        model: ?[]const u8,
        role: ?[]const u8,
    };
    const sender_json: ?SenderJson = if (msg.sender) |s| .{
        .id = s.id,
        .name = s.name,
        .model = s.model,
        .role = s.role,
    } else null;

    const GitJson = struct {
        oid: []const u8,
        head: []const u8,
        dirty: bool,
        prefix: []const u8,
    };
    const git_json: ?GitJson = if (msg.git) |g| .{
        .oid = g.oid,
        .head = g.head,
        .dirty = g.dirty,
        .prefix = g.prefix,
    } else null;

    const record = struct {
        id: []const u8,
        topic_id: []const u8,
        parent_id: ?[]const u8,
        body: []const u8,
        created_at: i64,
        reply_count: i32,
        sender: ?SenderJson,
        git: ?GitJson,
    }{
        .id = msg.id,
        .topic_id = msg.topic_id,
        .parent_id = msg.parent_id,
        .body = msg.body,
        .created_at = msg.created_at,
        .reply_count = msg.reply_count,
        .sender = sender_json,
        .git = git_json,
    };
    try std.json.Stringify.value(record, .{ .whitespace = .minified }, stdout);
}

fn writeMessageJsonWithDepth(stdout: anytype, msg: zawinski.store.Message, depth: ?u32) !void {
    // Build sender sub-object if present
    const SenderJson = struct {
        id: []const u8,
        name: []const u8,
        model: ?[]const u8,
        role: ?[]const u8,
    };
    const sender_json: ?SenderJson = if (msg.sender) |s| .{
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
    const git_json: ?GitJson = if (msg.git) |g| .{
        .oid = g.oid,
        .head = g.head,
        .dirty = g.dirty,
        .prefix = g.prefix,
    } else null;

    const record = struct {
        id: []const u8,
        topic_id: []const u8,
        parent_id: ?[]const u8,
        body: []const u8,
        created_at: i64,
        reply_count: i32,
        depth: ?u32,
        sender: ?SenderJson,
        git: ?GitJson,
    }{
        .id = msg.id,
        .topic_id = msg.topic_id,
        .parent_id = msg.parent_id,
        .body = msg.body,
        .created_at = msg.created_at,
        .reply_count = msg.reply_count,
        .depth = depth,
        .sender = sender_json,
        .git = git_json,
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

/// Print text with proper indentation on each line
fn printIndented(stdout: anytype, text: []const u8, indent: u32) !void {
    var iter = std.mem.splitScalar(u8, text, '\n');
    var first = true;
    while (iter.next()) |line| {
        if (!first) {
            try stdout.writeByte('\n');
            var i: u32 = 0;
            while (i < indent) : (i += 1) {
                try stdout.writeAll("  ");
            }
        }
        try stdout.writeAll(line);
        first = false;
    }
}

/// Truncate string to max bytes, respecting UTF-8 boundaries
fn truncateUtf8(text: []const u8, max_bytes: usize) []const u8 {
    if (text.len <= max_bytes) return text;

    // Find a safe truncation point that doesn't split a UTF-8 sequence
    var end = max_bytes;
    while (end > 0 and (text[end] & 0xC0) == 0x80) {
        // This byte is a continuation byte, back up
        end -= 1;
    }
    return text[0..end];
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

/// Detect UUID-like strings (8-4-4-4-12 hex format)
/// Returns true for patterns like "f239baf9-e91e-471b-b150-ef77ec071fd6"
fn looksLikeUuid(s: []const u8) bool {
    // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 chars)
    if (s.len != 36) return false;

    // Check hyphens at positions 8, 13, 18, 23
    if (s[8] != '-' or s[13] != '-' or s[18] != '-' or s[23] != '-') return false;

    // Check all other chars are hex
    for (s, 0..) |c, i| {
        if (i == 8 or i == 13 or i == 18 or i == 23) continue;
        const is_hex = (c >= '0' and c <= '9') or (c >= 'a' and c <= 'f') or (c >= 'A' and c <= 'F');
        if (!is_hex) return false;
    }

    return true;
}

fn die(comptime fmt: []const u8, args: anytype) noreturn {
    std.debug.print(fmt ++ "\n", args);
    std.process.exit(1);
}

fn dieOnError(err: anyerror) noreturn {
    const msg: []const u8 = switch (err) {
        StoreError.StoreNotFound => "No store found. Run 'jwz init' or use --store.",
        StoreError.TopicNotFound =>
        \\Topic not found.
        \\
        \\The topic argument must be a topic NAME (e.g., 'tasks', 'research:myproject').
        \\
        \\To fix:
        \\  1. List existing topics: jwz topic list
        \\  2. Create a new topic:   jwz topic new <name>
        ,
        StoreError.TopicExists => "Topic already exists.",
        StoreError.MessageNotFound => "Message not found. Use 'jwz search <query>' to find messages.",
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
