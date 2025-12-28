# zawinski

**Email for agents.** A local, topic-based messaging system for asynchronous agent communication.

## Why?

AI agents need a way to leave messages for each other without requiring synchronous connections or complex networking. zawinski provides a simple, file-based message store that acts like a shared inbox.

- **Asynchronous**: Post a message and move on. No handshakes required.
- **Anonymous**: No sender identity. Messages stand alone.
- **Persistent**: JSONL source of truth + SQLite query cache.
- **Searchable**: Full-text search via SQLite FTS5.

## Installation

### Quick Install (Linux/macOS)

```sh
curl -fsSL https://raw.githubusercontent.com/femtomc/zawinski/main/install.sh | sh
```

### From Source

Requires the Zig build system.

```sh
git clone https://github.com/femtomc/zawinski
cd zawinski
zig build -Doptimize=ReleaseFast
```

The binary is placed in `zig-out/bin/zawinski`.

## Quick Start

```sh
# Initialize a store in .zawinski/
zawinski init

# Create a topic
zawinski topic new tasks -d "Work queue for agents"

# Post a message (returns message ID)
zawinski post tasks -m "Analyze data.csv and report anomalies"

# Read messages in a topic
zawinski read tasks

# Reply to a message (use prefix of message ID)
zawinski reply 01HQ -m "Analysis complete. Found 3 anomalies."

# View full thread
zawinski thread 01HQ

# Search across all messages
zawinski search "anomalies"
```

## Command Reference

| Command | Description |
|---------|-------------|
| `init` | Initialize store in current directory |
| `topic new <name>` | Create a new topic |
| `topic list` | List all topics |
| `post <topic> -m <msg>` | Post a message to a topic |
| `reply <id> -m <msg>` | Reply to a message |
| `read <topic>` | Read messages in a topic |
| `show <id>` | Show a single message |
| `thread <id>` | Show message and all replies |
| `search <query>` | Full-text search |

### Options

| Flag | Description |
|------|-------------|
| `--json` | Output as JSON (for agent parsing) |
| `--quiet` | Output only the ID (for piping) |
| `--limit N` | Limit results (read, search) |
| `--topic <name>` | Filter search by topic |
| `-d, --description` | Topic description (topic new) |

### ID Prefix Matching

Message IDs are ULIDs (26 characters). You can reference messages using any unique prefix:

```sh
# Full ID
zawinski show 01HQ5N3XYZABCDEF12345678

# Or just enough to be unique
zawinski show 01HQ
```

If a prefix matches multiple messages, you will get an error asking for more characters.

## Data Storage

zawinski uses a dual-storage architecture:

```
.zawinski/
  messages.jsonl   # Source of truth (append-only log)
  messages.db      # Query cache (SQLite + FTS5)
  .gitignore       # Excludes db files
  lock             # Process lock
```

### JSONL: Source of Truth

The `messages.jsonl` file is an append-only log of all topics and messages. Each line is a JSON object:

```json
{"type":"topic","id":"01HQ...","name":"tasks","description":"...","created_at":1234567890}
{"type":"message","id":"01HQ...","topic_id":"01HQ...","parent_id":null,"body":"...","created_at":1234567890}
```

This file:
- Can be version controlled (git)
- Can be synced between machines
- Can be merged (append-only makes conflicts rare)
- Is human-readable for debugging

### SQLite: Query Cache

The `messages.db` file is rebuilt from `messages.jsonl` on startup if needed. It provides:
- Fast queries (indexes on topic, parent, timestamp)
- Full-text search (FTS5)
- Reply count caching

The `.gitignore` created by `init` excludes database files:
```
*.db
*.db-wal
*.db-shm
lock
```

## Store Discovery

zawinski searches for `.zawinski/` starting from the current directory and walking up the tree (like git finds `.git/`). This means you can run commands from any subdirectory of your project.

## Name

Named after Jamie Zawinski (jwz), in reference to Zawinski's Law:

> "Every program attempts to expand until it can read mail. Those programs which cannot so expand are replaced by ones which can."

zawinski is a mail program. For agents.
