# Custom Message Queue

A lightweight message queue built from scratch in C using raw POSIX sockets and
pthreads. No external dependencies. The system implements publish-subscribe
messaging with write-ahead logging for durability, all in a single binary that
acts as both client and server.

---

## Project Proposal

### Description

This project implements a custom message queue protocol inspired by AMQP
(Advanced Message Queuing Protocol). It provides a broker-based pub/sub system
where producers publish messages to a central queue server, and the server fans
them out to all connected consumers in real time over TCP.

The queue server persists every incoming message to a write-ahead log (WAL)
before attempting delivery. If the server crashes or consumers are unavailable,
undelivered messages survive on disk and are replayed to the next consumer that
connects. This guarantees at-least-once delivery without relying on any
external database or message store.

The entire system — server, producer client, and consumer client — is compiled
into a single binary and controlled through a subcommand-style CLI.

### Goals

1. **Understand socket programming from first principles** — no frameworks,
   no abstractions over `socket()`/`bind()`/`listen()`/`accept()`. Every TCP
   connection is managed with raw POSIX syscalls.

2. **Implement durable messaging** — messages must not be lost on crash. The
   write-ahead log ensures every accepted message is fsynced to disk before
   the server attempts delivery.

3. **Support fan-out delivery** — a single message from one producer is
   broadcast to all connected consumers simultaneously.

4. **Keep it minimal** — no external libraries, no build system beyond Make,
   no runtime dependencies. The project is a learning tool for systems
   programming, not a production broker.

5. **Configurable at runtime** — port, max producers, and max consumers are
   user-configurable and persisted to a file so the server can reload them on
   restart.

### Specifications

#### System Architecture

```
┌──────────┐         TCP          ┌───────────────┐         TCP          ┌──────────┐
│ Producer │ ───────────────────► │  Queue Server  │ ───────────────────► │ Consumer │
│  Client  │   connect + send    │               │   broadcast msgs    │  Client  │
└──────────┘                     │  ┌───────────┐ │                     └──────────┘
                                 │  │  WAL.log  │ │
┌──────────┐         TCP         │  └───────────┘ │         TCP          ┌──────────┐
│ Producer │ ──────────────────► │               │ ───────────────────► │ Consumer │
│  Client  │                     │  ┌───────────┐ │                     │  Client  │
└──────────┘                     │  │ queue.conf│ │                     └──────────┘
                                 │  └───────────┘ │
                                 └───────────────┘
```

#### Protocol

Communication uses a simple text-based protocol over TCP:

| Step | Direction         | Payload                      | Purpose             |
| ---- | ----------------- | ---------------------------- | ------------------- |
| 1    | Client → Server   | `PRODUCER\n` or `CONSUMER\n` | Role identification |
| 2a   | Producer → Server | `<message>\n`                | Publish a message   |
| 2b   | Server → Consumer | `<message>\n`                | Deliver a message   |

- Messages are newline-delimited plain text, max 4096 bytes per message.
- Producers send one message and disconnect (fire-and-forget).
- Consumers maintain a long-lived connection and receive messages as they arrive.

#### Message Lifecycle

```
Producer sends message
        │
        ▼
   append_wal()      ── message written to WAL.log, fsynced to disk
        │
        ▼
 broadcast_message() ── message sent to all connected consumers
        │
        ▼
 remove_wal_message() ── message removed from WAL (delivery confirmed)
```

If the server crashes between append and remove, the message stays in
`WAL.log`. When the next consumer connects, `replay_wal()` sends all
undelivered messages and then truncates the log.

#### Configuration

Stored in `data/queue.conf` as key-value pairs:

| Parameter       | Default | Description                               |
| --------------- | ------- | ----------------------------------------- |
| `port`          | 9294    | TCP port the queue server listens on      |
| `max_producers` | 100     | Maximum simultaneous producer connections |
| `max_consumers` | 100     | Maximum simultaneous consumer connections |
| `message_retention` | 1   | How many consumers must receive message before WAL deletion |

Configure interactively or accept defaults:

```sh
./build/protocol queue configure       # interactive prompts
./build/protocol queue configure -y    # accept all defaults
```

#### CLI Usage

```sh
# Start the queue server
./build/protocol queue start

# Stop the queue server
./build/protocol queue stop

# Publish a message
./build/protocol producer register "hello world"

# Subscribe and receive messages
./build/protocol consumer register

# Unregister (no-op placeholder)
./build/protocol producer unregister
./build/protocol consumer unregister
```

### Design

#### File Structure

```
src/
├── main.c        CLI entry point — dispatches to entity handlers
├── config.c      Read/write queue.conf, interactive + default config
├── queue.c       Client-side connection helpers (connect, publish)
├── server.c      Queue server — socket setup, accept loop, client threads
├── wal.c         Write-ahead log — append, replay, remove
├── message.c     Message struct with timestamp and retention tracking
├── consumer.c    Consumer CLI handler
└── producer.c    Producer CLI handler

include/ampq/
├── config.h      QueueConfig struct and function declarations
├── queue.h       Client-side API (connect_queue, publish_message)
├── server.h      Server API (start_queue, kill_queue)
├── message.h     Message struct and timestamp/retention functions
├── consumer.h    Consumer handler declaration
├── producer.h    Producer handler declaration
└── wal.h         WAL API (append_wal, replay_wal, remove_wal_message)

data/
└── queue.conf    Runtime configuration

WAL.log           Write-ahead log (created at runtime, format: timestamp|content|retention)
```

#### Key Design Decisions

- **Single binary, multiple roles.** The same compiled binary serves as the
  queue server, producer client, and consumer client. The CLI subcommand
  determines which role runs. This simplifies deployment and keeps the project
  self-contained.

- **Thread-per-connection.** Each accepted client gets its own pthread. Simple
  to reason about and sufficient for the target scale (~100 consumers). A
  production system would use `epoll`/`kqueue` for thousands of connections.

- **WAL before broadcast.** Messages hit disk before the server attempts
  delivery. This is the same ordering guarantee used by PostgreSQL and Kafka.
  The tradeoff is write latency (fsync is slow), but durability comes first.

- **Message timestamps and retention tracking.** Each message is stored with:
  - **Timestamp** (ISO 8601 UTC format): When the message was created
  - **Retention count** (integer): How many consumers have received it
  - **Content** (plain text): The actual message
  - Format in WAL: `timestamp|content|retention_count`
  - This enables structured query and replay of messages by time, and supports
    at-least-N-delivery guarantees (messages kept until N consumers receive them)

- **Atomic WAL removal via rename.** `remove_wal_message` writes a filtered
  copy to `WAL.log.tmp`, then atomically renames it over `WAL.log`. This
  prevents corruption if the server crashes mid-removal.

- **SO_REUSEADDR + SIGPIPE ignore.** Two defensive socket options that prevent
  common server crashes: `SO_REUSEADDR` lets the server restart immediately
  after a crash (without waiting for TIME_WAIT), and ignoring `SIGPIPE`
  prevents a disconnecting consumer from killing the server process.

- **Raw syscalls over libc stdio.** File I/O uses `open`/`read`/`write`/`close`
  instead of `fopen`/`fgets`/`fprintf`. This is deliberate — the project is a
  learning exercise in how things work below the C standard library.

#### Planned Work

- [ ] Scheduler for recovery tasks and component breakdown
- [ ] Message persistence beyond WAL (long-term storage)
- [ ] Configurable retry mechanisms (retry count, backoff strategy)
- [ ] Mutex protection for the shared consumer list (thread safety)
- [ ] Graceful shutdown with SIGINT/SIGTERM handling

---

## Build

```sh
make            # compiles to build/protocol
make clean      # removes build/
```

Requires `gcc` and `pthreads` (any Linux system with build-essential).
