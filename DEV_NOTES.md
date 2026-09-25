# DEV_NOTES.md — AMQP Protocol Implementation

Reference notes on the systems programming concepts used throughout this project.
Organized by source file so you can read the code alongside this document.

---

## Project Architecture

```
main.c          CLI entry point, dispatches to entity handlers
consumer.c      Consumer client — registers to receive messages
producer.c      Producer client — publishes messages to the queue
queue.c         Client-side socket helpers + queue CLI dispatch
server.c        Queue server — accepts connections, routes messages
wal.c           Write-ahead log for message durability
config.c        Configuration loading/saving (file-backed key=value)
message.c       Message struct and retention tracking
```

The binary acts as both **client** and **server**. Running `./protocol queue start`
launches the server. Running `./protocol producer register "hello"` or
`./protocol consumer register` connects to the server as a client.

---

## Networking (queue.c, server.c)

### `socket(AF_INET, SOCK_STREAM, 0)`

Creates a TCP socket. The three arguments:

- **`AF_INET`** — Address family. Tells the kernel this is an IPv4 socket.
  `AF_INET6` would be IPv6. The "AF" stands for Address Family.
- **`SOCK_STREAM`** — Socket type. `SOCK_STREAM` gives a reliable, ordered,
  byte-stream connection (TCP). The alternative `SOCK_DGRAM` gives unreliable
  datagrams (UDP).
- **`0`** — Protocol. Zero means "pick the default protocol for this
  family/type combination," which is TCP for `AF_INET + SOCK_STREAM`.

Returns a **file descriptor** (an integer). All subsequent operations —
`bind`, `listen`, `accept`, `connect`, `read`, `write`, `close` — use this fd.
If creation fails, it returns -1 and sets `errno`.

### `setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))`

Configures socket options before binding. Used in `server.c:start_queue`.

- **`SOL_SOCKET`** — Option level. `SOL_SOCKET` means "this is a general socket
  option, not a protocol-specific one." TCP-specific options would use
  `IPPROTO_TCP` instead.
- **`SO_REUSEADDR`** — Allows binding to an address that's in the `TIME_WAIT`
  state. Without this, if the server crashes and restarts quickly, `bind()`
  would fail with "Address already in use" because the kernel holds the port
  in TIME_WAIT for ~60 seconds (to drain late-arriving packets from the
  previous connection). With `SO_REUSEADDR`, the new socket can bind
  immediately.

### `struct sockaddr_in` and byte order

```c
struct sockaddr_in address = {
    .sin_family = AF_INET,
    .sin_addr.s_addr = htonl(INADDR_ANY),  // or INADDR_LOOPBACK
    .sin_port = htons(config.port)
};
```

- **`htons` / `htonl`** — "Host TO Network Short/Long." Network protocols use
  big-endian byte order. Your CPU might be little-endian. These functions
  convert so the port and address are in the right byte order on the wire.
  `htons` converts a 16-bit value (ports), `htonl` converts 32-bit (addresses).
- **`INADDR_ANY` (0.0.0.0)** — Server uses this to accept connections on any
  network interface (localhost, LAN, etc.).
- **`INADDR_LOOPBACK` (127.0.0.1)** — Client uses this to connect to the server
  on the same machine.

### `bind(fd, addr, addrlen)`

Associates the socket with a specific address and port. Only the server calls
this. After `bind`, the OS knows "traffic arriving on this port goes to this
socket." If two processes try to bind the same port, the second fails — unless
`SO_REUSEADDR` is set.

### `listen(fd, backlog)`

Marks the socket as a **passive** socket — one that will accept incoming
connections rather than initiate them. The `backlog` argument sets the maximum
length of the queue of pending connections. If 10 clients connect simultaneously
before the server calls `accept`, the kernel queues up to `backlog` of them and
refuses the rest.

### `accept(fd, addr, addrlen)`

Blocks until a client connects. Returns a **new** file descriptor for the
specific client connection. The original socket continues listening. This is why
a server has two kinds of fds: one "listening" fd (created by `socket`) and many
"connected" fds (returned by `accept`, one per client).

### `connect(fd, addr, addrlen)`

Client-side counterpart of `accept`. Initiates a TCP connection to the server
at the given address. Blocks until the three-way handshake completes (SYN →
SYN-ACK → ACK). On success returns 0. On failure returns -1 (e.g., server not
running → "Connection refused").

### `shutdown(fd, SHUT_WR)`

Used in `publish_message` after writing. Tells the server "I'm done sending"
by sending a TCP FIN. The server's `recv` returns 0, signaling end-of-stream.
Unlike `close`, `shutdown` lets you still *read* from the socket after shutting
down writes. Here it's used for a clean half-close: the producer writes its
message, signals "no more data," then closes.

### `dprintf(fd, fmt, ...)`

Like `printf`, but writes to a file descriptor instead of stdout. Used to send
the role string ("PRODUCER\n" or "CONSUMER\n") and messages over the socket.
More convenient than formatting into a buffer and calling `write`.

### `signal(SIGPIPE, SIG_IGN)`

When you `write` to a socket whose remote end has closed, the kernel sends your
process `SIGPIPE`, which kills it by default. The server ignores this signal so
that one misbehaving client doesn't crash the whole server. Instead, `write`
returns -1 with `errno = EPIPE`, which the server handles gracefully by removing
the consumer from the list.

---

## Concurrency (server.c)

### `pthread_create(&thread, NULL, handle_client, arg)`

Spawns a new thread to handle each client connection. Each thread runs
`handle_client` with the client's fd passed as the argument. Threads share the
same address space (including the global `Queue q`), which is why all threads
can read/write the consumer list.

The fd is passed via a heap-allocated `int*` rather than casting the fd to
`void*` because the accept loop might overwrite a stack variable before the
thread reads it — a classic race condition.

### `pthread_detach(thread)`

Marks the thread as "I don't need to join this." When a detached thread exits,
its resources are reclaimed automatically. Without detach, you'd need to call
`pthread_join` to avoid leaking thread resources. Since the server doesn't need
the thread's return value, detach is the right choice.

### Thread safety caveats in this project

The global `Queue q` (consumer list, counts) is accessed by multiple threads
without a mutex. This is a known simplification. A production system would
protect `q.consumers[]` and `q.consumer_count` with a `pthread_mutex_t` to
prevent data races when two threads simultaneously add/remove consumers or
broadcast.

---

## Write-Ahead Log (wal.c)

### What is a WAL?

A **Write-Ahead Log** is a durability mechanism. The idea: before performing an
action (delivering a message), first write it to a persistent log on disk. If
the server crashes, the log contains every message that wasn't yet delivered.
On recovery, you **replay** the log to bring the system back to a consistent
state.

The ordering is critical: **write to log first, then act.** If you act first
and crash before logging, the action is lost. If you log first and crash before
acting, you can redo the action on recovery.

WALs are used in databases (PostgreSQL, SQLite), filesystems (ext4 journaling),
and message queues (Kafka, RabbitMQ).

### How this project uses the WAL

The message lifecycle:

```
1. Producer sends message
2. append_wal(message)     — write to WAL.log (fsync to ensure it hits disk)
3. broadcast_message()     — deliver to all connected consumers
4. remove_wal_message()    — message delivered, remove from WAL
```

If the server crashes between steps 2 and 4, the message is still in `WAL.log`.
When a new consumer connects, `replay_wal` sends them everything still in the
log — these are messages that were received but never successfully delivered.

### `replay_wal(client_fd)`

Reads `WAL.log` line by line and sends each message to the newly connected
consumer. After a successful replay, truncates the WAL (opens with `O_TRUNC`)
because those messages have now been delivered. If the WAL file doesn't exist
(`ENOENT`), that's fine — it means there are no undelivered messages.

### `remove_wal_message(message)`

Removes a single message from the WAL after it's been successfully delivered to
at least one consumer. Uses the **write-to-temp-then-rename** pattern:

1. Open `WAL.log` for reading
2. Open `WAL.log.tmp` for writing
3. Copy every line except the first match of `message`
4. `rename("WAL.log.tmp", "WAL.log")` — atomic on POSIX filesystems

This is safer than modifying the file in place because `rename` is atomic: if
the server crashes mid-operation, you either have the old WAL or the new one,
never a corrupted half-written file.

### `fsync(fd)`

Forces the OS to flush file data from kernel buffers to the physical disk.
Without `fsync`, the OS might buffer writes and a crash could lose them.
Called after WAL writes to guarantee the message actually reached disk before
the server attempts delivery. This is the "D" in ACID (Durability).

### `O_WRONLY | O_CREAT | O_APPEND`

Flags passed to `open()` for WAL appends:

- **`O_WRONLY`** — Open for writing only
- **`O_CREAT`** — Create the file if it doesn't exist
- **`O_APPEND`** — Every `write()` atomically appends to the end of the file.
  Prevents interleaving if multiple threads write simultaneously (though this
  project uses single-threaded WAL access per message).

---

## Fan-out / Broadcast (server.c)

### The publish-subscribe pattern

This project implements a simplified **pub/sub** system:

- **Producers** publish messages to the queue.
- **Consumers** subscribe to receive messages.
- The **queue server** fans out each message to all connected consumers.

In `broadcast_message`, the server iterates over all consumer fds and writes
the message to each one. If a write fails (consumer disconnected), the server
removes that consumer from the list using a swap-with-last-element approach:

```c
q.consumers[i] = q.consumers[--q.consumer_count];
```

This is O(1) removal — swap the dead entry with the last entry and shrink the
count. Order doesn't matter for broadcast, so this is more efficient than
shifting all elements.

---

## Configuration (config.c)

### File-backed config (`queue.conf`)

Configuration is stored as a simple `key=value` text file:

```
port=9294
max_producers=100
max_consumers=100
```

`load_config` reads this file character by character, splits each line on `=`,
and populates a `QueueConfig` struct. `put_config` writes it back.

This is a manual implementation of what `getenv` + `.env` files or `ini`
parsers do — useful for understanding how config files are parsed at the
syscall level using `open`/`read`/`write` instead of `fopen`/`fgets`/`fprintf`.

### `O_WRONLY | O_CREAT | O_TRUNC`

Flags used when writing the config file:

- **`O_TRUNC`** — If the file already exists, truncate it to zero length. This
  effectively replaces the old config with the new one. Combined with `O_CREAT`,
  this means "create or overwrite."

---

## Message Retention (message.c, wal.c)

### The Message Struct

```c
typedef struct
{
    char content[4096];
    int retention_count;
} Message;
```

Each message tracks:
- **`content`** — The actual message text (max 4096 bytes)
- **`retention_count`** — How many consumers have successfully received it

### Message Lifecycle with Retention

```
Producer sends message
        │
        ▼
   append_wal()           ── message stored as "content|0"
        │
        ▼
 broadcast_message()      ── send to all consumers
        │
        ├─► Consumer 1 ✓ (receives)
        │
        ├─► Consumer 2 ✓ (receives)
        │
        ▼
remove_wal_message()      ── increment to "content|2"
        │
        ▼
message_should_delete()   ── check if retention >= max_retention
        │
        └─► If yes, delete from WAL
        └─► If no, keep for next consumer batch
```

### Message Serialization

Messages in `WAL.log` are stored with retention metadata using a pipe delimiter:

```
hello world|0
foo bar|1
test message|2
```

Format: `message_content|retention_count`

- **Deserialization** (`message_deserialize`) — splits on the last `|` to separate
  content from count. If no `|` exists (backward compatibility), sets count to 0.
- **Serialization** (`message_serialize`) — formats as `content|count` before
  writing to disk.

### Configuration: `message_retention`

Added to `queue.conf`:

```
port=9294
max_producers=100
max_consumers=100
message_retention=1
```

- **Default:** `1` — message deleted after reaching 1 consumer (fire-and-forget)
- **Value:** `2` or higher — message kept in WAL until that many consumers receive it

This enables **at-least-N-delivery guarantees**: a message won't be discarded from
the queue until at least N distinct consumers have processed it. Useful for
broadcast scenarios where you want to ensure redundancy or multiple systems
receive important events.

### How Retention Works

When a producer publishes `"hello"`:

1. `append_wal("hello")` → writes `"hello|0"` to `WAL.log`
2. `broadcast_message("hello")` → sends to all connected consumers
3. After each successful send, `remove_wal_message("hello", max_retention)` is called
4. This function:
   - Reads `WAL.log` line by line
   - Finds the first line matching `"hello"`
   - Increments its retention count: `"hello|0"` → `"hello|1"`
   - Checks `message_should_delete()`: if retention >= max_retention, skips writing to temp file (effectively deletes it)
   - Otherwise, writes the updated line to temp file
   - Atomically renames temp over original (atomic WAL update)

### Example: max_retention = 2

```
Scenario: 3 consumers, message_retention=2

Initial state:  WAL.log: "hello|0"

Consumer 1 receives hello:  WAL.log: "hello|1"   (keep, 1 < 2)

Consumer 2 receives hello:  WAL.log: (deleted)   (delete, 1 >= 2 after increment)

Consumer 3 arrives later:   replay_wal() sends buffered messages
                            but "hello" is gone (already delivered to 2 consumers)
```

---

## CLI Dispatch (main.c)

The binary uses a two-level command structure:

```
./protocol <entity> <operation> [args]
```

`main.c` dispatches on the entity (`producer`, `consumer`, `queue`), and each
entity handler dispatches on the operation (`register`, `unregister`,
`configure`, `start`, `stop`). This is the same pattern used by `git`
(`git remote add`), `docker` (`docker container ls`), and `kubectl`.

---

## File Descriptors — the unifying concept

Almost everything in this project operates on **file descriptors**: sockets,
config files, the WAL log. In Unix, fds are the universal handle for I/O.
`read(fd, ...)` and `write(fd, ...)` work the same whether `fd` points to a
file on disk, a TCP connection, a pipe, or a device. This is the "everything
is a file" philosophy in practice.

The same `write_all` helper is used to write to both WAL files and socket
connections — it doesn't need to know which one it's writing to.
