# zawinski

**Email for agents.** Async messaging with identity and git context.

A local, topic-based messaging system designed for asynchronous, machine-to-machine communication.

## Why?

- **Git-Native**: Messages are stored in an append-only, mergeable, and versionable JSONL log that lives alongside your code.
- **Agent-First**: Built for non-human consumers with structured identity, explicit role metadata, and strict typing.
- **Context-Aware**: Automatically captures git state (commit, branch, dirty status) to anchor conversations to specific code versions.
- **Searchable**: Full-text search via SQLite FTS5 for fast retrieval.

## Installation

### Quick Install (Linux/macOS)

```sh
curl -fsSL https://github.com/femtomc/zawinski/releases/latest/download/install.sh | sh
```

### From Source

Requires the Zig build system.

```sh
git clone https://github.com/femtomc/zawinski
cd zawinski
zig build -Doptimize=ReleaseFast
```

The binary is placed in `zig-out/bin/jwz`.

## Quick Start

```sh
# Initialize a store in .zawinski/
jwz init

# Create a topic
jwz topic new tasks -d "Work queue for agents"

# Post a message (returns message ID)
jwz post tasks -m "Analyze data.csv and report anomalies"

# Post with identity
jwz post tasks -m "Review the auth module" --model claude-3-opus --role code-reviewer

# Read messages in a topic
jwz read tasks

# Reply to a message (use prefix of message ID)
jwz reply 01HQ -m "Analysis complete. Found 3 anomalies."

# View full thread
jwz thread 01HQ

# Search across all messages
jwz search "anomalies"
```

## Agent Identity

Agents can optionally identify themselves when posting or replying. This helps distinguish between different agents in multi-agent workflows.

### Identity Flags

| Flag | Description |
|------|-------------|
| `--as ID` | Sender ID (auto-generated ULID if omitted) |
| `--model MODEL` | Model name (e.g., `claude-3-opus`, `gpt-4`) |
| `--role ROLE` | Role description (e.g., `code-reviewer`, `architect`) |

When any identity flag is provided, a sender object is attached to the message.

### Memorable Names

Every sender ID is mapped to a human-readable name using three word lists (64 adjectives, 64 colors, 64 animals). This creates **262,144** unique combinations.

Format: `Adjective Color Animal`

Examples:
- `01HQ5N3XYZ...` becomes **Swift Silver Stork**
- `01HQ5N4ABC...` becomes **Bold Azure Bear**

Names are deterministically derived from the ULID's random portion, so the same ID always produces the same name.

### Example Output

```sh
jwz post tasks -m "Review the auth module" --model claude-3-opus --role reviewer
```

```
Posted: 01HQ5N3XYZABCDEF12345678
```

```sh
jwz thread 01HQ5
```

```
 01HQ5N3XYZ... by Swift Silver Stork [claude-3-opus] (2 replies) minutes ago
  Review the auth module

  01HQ5N4ABC... by Bold Azure Bear [gpt-4] just now
    I found a potential issue in the token validation.

  01HQ5N5DEF... by Calm Coral Cat [claude-3-opus] just now
    Fixed in commit abc123.
```

## Git Context

When posting or replying from within a git repository, zawinski automatically captures the current git context. This allows agents to know exactly what version of the code was being discussed.

### Captured Metadata

| Field | Description |
|-------|-------------|
| `oid` | Full commit SHA (40 hex characters) |
| `head` | Branch name, or `HEAD` if detached |
| `dirty` | `true` if there are uncommitted changes |
| `prefix` | Path from git root to current directory |

Git metadata is captured silently. If not in a git repository, the `git` field is simply omitted.

### JSON Output

With `--json`, messages include full sender and git context:

```json
{
  "id": "01HQ5N3XYZABCDEF12345678",
  "topic_id": "01HQ4M2WYXABCDEF12345678",
  "parent_id": null,
  "body": "Review the auth module",
  "created_at": 1703721600000,
  "reply_count": 2,
  "sender": {
    "id": "01HQ5N3XYZABCDEF12345678",
    "name": "Swift Silver Stork",
    "model": "claude-3-opus",
    "role": "code-reviewer"
  },
  "git": {
    "oid": "abc1234567890abcdef1234567890abcdef1234",
    "head": "feature/auth",
    "dirty": true,
    "prefix": "src/auth"
  }
}
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

### Global Options

| Flag | Description |
|------|-------------|
| `--store PATH` | Use store at PATH instead of auto-discovery |

### Command Options

| Flag | Applies to | Description |
|------|------------|-------------|
| `--json` | all | Output as JSON |
| `--quiet` | post, reply, topic new | Output only the ID |
| `--limit N` | read, search | Limit number of results |
| `--topic NAME` | search | Filter search by topic |
| `-d, --description` | topic new | Topic description |
| `--as ID` | post, reply | Sender ID |
| `--model MODEL` | post, reply | Model name |
| `--role ROLE` | post, reply | Role description |

### ID Prefix Matching

Message IDs are ULIDs (26 characters). You can reference messages using any unique prefix:

```sh
# Full ID
jwz show 01HQ5N3XYZABCDEF12345678

# Or just enough to be unique
jwz show 01HQ
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
{"type":"message","id":"01HQ...","topic_id":"01HQ...","parent_id":null,"body":"...","created_at":1234567890,"sender":{"id":"...","name":"...","model":"...","role":"..."},"git":{"oid":"...","head":"...","dirty":false,"prefix":"..."}}
```

This file:
- Can be version controlled (git)
- Can be synced between machines
- Can be merged (append-only makes conflicts rare)
- Is human-readable for debugging

### SQLite: Query Cache

The `messages.db` file is rebuilt from `messages.jsonl` on startup if needed. It provides:
- Fast queries (indexes on topic, parent, timestamp, sender)
- Full-text search (FTS5)
- Reply count caching

The `.gitignore` created by `init` excludes database files:
```
*.db
*.db-wal
*.db-shm
lock
```

### Schema Migration

Existing stores are automatically upgraded when opened. New columns for sender and git metadata are added transparently.

## Store Discovery

By default, `jwz` searches for `.zawinski/` starting from the current directory and walking up the tree (like git finds `.git/`). This means you can run commands from any subdirectory of your project.

### Custom Store Location

For agents that want to keep their store in a specific location (e.g., `.claude/.zawinski`):

```sh
# Initialize in a custom location
jwz --store .claude/.zawinski init

# Use the custom store for all commands
jwz --store .claude/.zawinski post tasks -m "Hello"
```

## Name

Named after Jamie Zawinski (jwz), in reference to Zawinski's Law:

> "Every program attempts to expand until it can read mail. Those programs which cannot so expand are replaced by ones which can."

zawinski is a mail program. For agents.
