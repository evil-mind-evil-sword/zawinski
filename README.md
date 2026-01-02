# zawinski

**Email for agents.** Async messaging with identity and git context.

A local, topic-based messaging system designed for asynchronous, machine-to-machine communication.

## Install

```sh
curl -fsSL https://github.com/evil-mind-evil-sword/zawinski/releases/latest/download/install.sh | sh
```

<details>
<summary>From source</summary>

Requires the Zig build system.

```sh
git clone https://github.com/evil-mind-evil-sword/zawinski
cd zawinski
zig build -Doptimize=ReleaseFast
```

The binary is placed in `zig-out/bin/jwz`.
</details>

## Why?

Agents need to communicate asynchronously—one agent kicks off a task, another picks it up later, a third reviews the result. But most messaging systems assume human readers: rich text, threading UIs, notification badges. Agents don't need any of that. They need structured data they can parse reliably.

zawinski stores messages in an append-only JSONL log. This format merges cleanly in git, so multiple agents can work in parallel without conflicts. Each message captures the current git state (commit, branch, dirty status), anchoring conversations to specific code versions. A SQLite cache provides full-text search when you need to find something later.

## Quick Start

```sh
# Just post - store and topic are auto-created
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

## For Agents

This section covers common patterns when using jwz programmatically.

### Auto-Initialization

The `post` command automatically initializes everything:
- **No store?** Creates `.jwz/` in the current directory
- **No topic?** Creates the topic with that name

This makes `post` fully idempotent—just post and it works:

```sh
# This works even with no prior setup
jwz post "research:myproject" -m "Finding: the API uses REST"
```

### Topic Names

Topic names are strings, not IDs. Use descriptive, namespaced names:

```sh
# Good: descriptive, namespaced
jwz post "research:auth-flow" -m "..."
jwz post "issue:bug-123" -m "..."
jwz post "alice:status:session-id" -m "..."

# Bad: raw UUIDs or IDs (these look like topic names but aren't meaningful)
jwz post "f239baf9-e91e-471b-b150-ef77ec071fd6" -m "..."  # Confusing
```

### Best Practices for Agents

1. **Use `--quiet`** to get just the message ID for programmatic use
2. **Use `--json`** when parsing output
3. **Use namespaced topics** like `research:topic` or `issue:id` for organization

```sh
# Robust agent posting pattern
jwz post "research:$TOPIC" --quiet --role agent -m "$MESSAGE"
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
| `init` | Initialize store in current directory (auto on post) |
| `topic new <name>` | Create a new topic (auto on post) |
| `topic list` | List all topics |
| `post <topic> -m <msg>` | Post a message (auto-inits store and topic) |
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
| `--json` | all | Output as JSON (thread includes `depth` field) |
| `--quiet` | post, reply, topic new | Output only the ID |
| `-s, --summary` | read, thread | Show first line of body only (truncated to 80 chars) |
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
.jwz/
  messages.jsonl   # Source of truth (append-only log)
  messages.db      # Query cache (SQLite + FTS5)
  .gitignore       # Excludes db files
  lock             # Process lock
```

> **Note:** Legacy `.zawinski/` directories are still supported for backward compatibility.

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

By default, `jwz` searches for `.jwz/` (or legacy `.zawinski/`) starting from the current directory and walking up the tree (like git finds `.git/`). This means you can run commands from any subdirectory of your project.

### Custom Store Location

For agents that want to keep their store in a specific location (e.g., `.claude/.jwz`):

```sh
# Initialize in a custom location
jwz --store .claude/.jwz init

# Use the custom store for all commands
jwz --store .claude/.jwz post tasks -m "Hello"
```

## Related

zawinski draws from several traditions in distributed systems and agent communication:

**Agent Communication Standards.** The [FIPA ACL](https://en.wikipedia.org/wiki/Agent_Communications_Language) (1996) established speech-act semantics for agent messaging—the idea that messages are intentional actions, not just data transfer. More recently, Google's [A2A Protocol](https://developers.googleblog.com/en/a2a-a-new-era-of-agent-interoperability/) and Anthropic's [MCP](https://modelcontextprotocol.io/) have revived interest in agent interoperability, though they target different layers of the stack.

**Append-Only Logs.** The JSONL storage pattern comes from event sourcing and CRDT research. [ipfs-log](https://github.com/orbitdb-archive/ipfs-log) implements similar append-only semantics on IPFS. For heavier workloads, [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) and [Apache Kafka](https://kafka.apache.org/) offer distributed commit logs with different tradeoffs.

**CRDTs.** The conflict-free merge semantics that make zawinski git-friendly build on work by [Shapiro et al. (2011)](https://hal.inria.fr/inria-00555588/document). [Automerge](https://automerge.org/) and [Yjs](https://yjs.dev/) are mature CRDT implementations for collaborative applications.

## Name

Named after Jamie Zawinski (jwz), in reference to Zawinski's Law:

> "Every program attempts to expand until it can read mail. Those programs which cannot so expand are replaced by ones which can."

zawinski is a mail program. For agents.
