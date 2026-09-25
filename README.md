# AMQP — Custom Message Queue Protocol

A lightweight message queue built from scratch in C using raw POSIX sockets and pthreads. 
No external dependencies. Implements publish-subscribe messaging with write-ahead logging 
for durability, real-time dashboard, structured logging, and cross-network support.

---

## Project Proposal

### Description

This project implements a custom message queue protocol inspired by AMQP (Advanced Message 
Queuing Protocol). It provides a broker-based pub/sub system where producers publish 
messages to a central queue server, and the server fans them out to all connected consumers 
in real time over TCP.

The queue server persists every incoming message to a write-ahead log (WAL) with timestamps 
and retention tracking before attempting delivery. If the server crashes or consumers are 
unavailable, undelivered messages survive on disk and are replayed to the next consumer that 
connects. This guarantees at-least-once delivery without relying on any external database.

Features:
- **Cross-network support** — Clients connect to any server address (hostname or IP)
- **Real-time dashboard** — Live consumer count and connection status on server terminal
- **Structured logging** — All events logged with timestamps to configurable log files
- **Message retention** — At-least-N-delivery: messages kept until N consumers receive them
- **Message timestamps** — ISO 8601 UTC timestamps on all messages in WAL

The entire system — server, producer client, and consumer client — is compiled into a 
single binary and controlled through a subcommand-style CLI.

### Goals

1. **Understand socket programming from first principles** — raw POSIX syscalls, hostname resolution, cross-network TCP

2. **Implement durable messaging** — write-ahead log ensures no message loss on crash

3. **Support fan-out delivery** — single message broadcast to all connected consumers

4. **Real-time observability** — live dashboard on server terminal + persistent event logs

5. **Cross-network capability** — producers/consumers on different machines connect to configured server

6. **Keep it minimal** — no external libraries, single binary, learning-focused

### Specifications

#### System Architecture

```
┌─────────────┐                  ┌──────────────────┐                  ┌─────────────┐
│  Producer   │     TCP over     │  Queue Server    │     TCP over     │  Consumer   │
│  (machine A)│ ─────Network────► │  (machine B)     │ ─────Network────► │ (machine C) │
└─────────────┘                  │                  │                  └─────────────┘
                                 │  ┌────────────┐  │
                                 │  │  WAL.log   │  │
                                 │  │ (timestamped  │  │
                                 │  │ +retention)   │  │
                                 │  └────────────┘  │
                                 │  ┌────────────┐  │
                                 │  │queue.conf  │  │
                                 │  └────────────┘  │
                                 └──────────────────┘
```

#### Protocol

Text-based over TCP with role identification:

| Step | Direction | Payload | Purpose |
|------|-----------|---------|---------|
| 1 | Client → Server | `PRODUCER\n` or `CONSUMER\n` | Role identification |
| 2a | Producer → Server | `<message>\n` | Publish a message |
| 2b | Server → Consumer | `<message>\n` | Deliver a message |

#### Message Lifecycle with Timestamps and Retention

```
Producer publishes message
        │
        ▼
   append_wal()
   ├─ Timestamp: 2026-09-25T08:04:01Z
   ├─ Content: message text
   ├─ Retention: 0
        │
        ▼
 broadcast_message()  ── sent to all connected consumers
        │
        ▼
 remove_wal_message() ── increment retention count
   ├─ If retention >= max_retention: DELETE from WAL
   └─ Else: KEEP for next consumer batch
```

#### Configuration

Stored in `data/queue.conf`:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `server_address` | localhost | Server hostname/IP for clients to connect to |
| `port` | 9294 | TCP port the queue server listens on |
| `max_producers` | 100 | Maximum simultaneous producer connections |
| `max_consumers` | 100 | Maximum simultaneous consumer connections |
| `message_retention` | 1 | At-least-N-delivery: messages kept until N consumers receive |
| `logging_interval` | 100 | Flush logs to disk every N milliseconds |
| `log_file` | ./queue.log | Path to event log file |

#### CLI Usage

**Server operations:**
```sh
# Configure queue (interactive)
./build/protocol queue configure

# Configure with defaults
./build/protocol queue configure -y

# Start the queue server
./build/protocol queue start

# Stop the queue server
./build/protocol queue stop
```

**Producer operations:**
```sh
# Publish a message
./build/protocol producer register "hello world"

# Unregister (placeholder)
./build/protocol producer unregister
```

**Consumer operations:**
```sh
# Subscribe and receive messages
./build/protocol consumer register

# Unregister (placeholder)
./build/protocol consumer unregister
```

### Design

#### File Structure

```
src/
├── main.c           CLI entry point — dispatches to entity handlers
├── config.c         Read/write queue.conf, cross-network config
├── queue.c          Client connection helpers (hostname resolution, connect, publish)
├── server.c         Queue server — socket setup, accept loop, client threads
├── wal.c            Write-ahead log — append, replay, remove with retention
├── message.c        Message struct with timestamp and retention tracking
├── display.c        Real-time dashboard rendering (ANSI colors)
├── logger.c         Structured event logging with background flush thread
├── consumer.c       Consumer CLI handler
└── producer.c       Producer CLI handler

include/ampq/
├── config.h         QueueConfig struct (includes server_address)
├── queue.h          Client-side API
├── server.h         Server API
├── message.h        Message struct and retention functions
├── display.h        Dashboard rendering API
├── logger.h         Event logging API
├── consumer.h       Consumer handler
├── producer.h       Producer handler
└── wal.h            WAL API

data/
└── queue.conf       Runtime configuration (includes server_address)

queue.log            Event log file (created at runtime)
WAL.log              Write-ahead log with timestamps and retention counts
```

#### Key Design Decisions

- **Cross-network via hostname resolution.** Clients use `gethostbyname()` to resolve 
  server address (hostname or IP). Server binds to `INADDR_ANY` to accept connections 
  from any interface. Configuration specifies what address clients should connect to.

- **Timestamps on all messages.** `Message` struct includes ISO 8601 UTC timestamp. 
  WAL format: `timestamp|content|retention_count`. Enables time-based queries and audit trails.

- **Message retention tracking.** Each message tracks how many consumers have received it. 
  Only deleted from WAL after reaching `message_retention` threshold. Enables at-least-N-delivery.

- **Real-time dashboard with threading.** Display module renders live consumer status. 
  Updates on every connect/disconnect. Server terminal shows current state without logs.

- **Structured event logging.** Logger module buffers events and flushes to disk every 
  `logging_interval` ms (configurable). Background flush thread prevents blocking. 
  Separate log file allows monitoring without interfering with dashboard.

- **Thread-safe with mutexes.** Consumer list protected by `pthread_mutex_t`. Logger 
  buffer protected by separate mutex. All shared state synchronized.

- **WAL before broadcast.** Messages persisted to disk (with fsync) before delivery. 
  Same ordering as PostgreSQL/Kafka. Durability first.

- **Atomic WAL removal via rename.** `remove_wal_message` writes filtered copy to 
  `WAL.log.tmp`, then atomically renames over original. Prevents corruption on crash.

- **Raw syscalls over libc.** File I/O uses `open`/`read`/`write`/`close`. Socket I/O 
  uses raw syscalls. Learning exercise in systems programming.

#### Planned Work

- [ ] Graceful shutdown with SIGINT/SIGTERM handling
- [ ] Mutex protection for shared consumer list (currently present but could add read-write locks)
- [ ] Connection pooling for producer reconnects
- [ ] Message prioritization/QoS levels
- [ ] Consumer groups for load balancing
- [ ] Metrics/statistics API

---

## Build

```sh
make            # compiles to build/protocol
make clean      # removes build/
```

Requires `gcc`, `pthreads`, and standard POSIX development tools.

---

## Cross-Network Example

**Machine A (server):**
```bash
$ ./build/protocol queue configure -y
$ ./build/protocol queue start
[09:15:23] ✓ Server started on port 9294
```

**Machine B (producer, IP 192.168.1.100):**
```bash
# Edit data/queue.conf on Machine B or config dynamically
# server_address = 192.168.1.100

$ ./build/protocol producer register "hello from B"
```

**Machine C (consumer, different network):**
```bash
# server_address = 192.168.1.100

$ ./build/protocol consumer register
hello from B
```

---

## Logging and Monitoring

**Real-time server dashboard:**
- Displays live consumer count and capacity %
- Updates on every connection/disconnection
- Located in server terminal (doesn't mix with logs)

**Event log file (`queue.log`):**
```
[2026-09-25 09:15:23] INFO | SERVER | Queue server started on port 9294
[2026-09-25 09:15:24] EVENT | CONSUMER | Connected - Total: 1/100
[2026-09-25 09:15:25] MESSAGE | PUBLISHED | hello from B
[2026-09-25 09:15:25] MESSAGE | DELIVERED | to 1 consumer(s)
[2026-09-25 09:15:26] EVENT | CONSUMER | Disconnected - Total: 0/100
```

Monitor in real-time:
```bash
tail -f queue.log
```
