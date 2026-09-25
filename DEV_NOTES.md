# DEV_NOTES.md — AMQP Protocol Implementation

Reference notes on the systems programming concepts used throughout this project.
Organized by module and feature so you can read the code alongside this document.

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
message.c       Message struct with timestamp and retention tracking
display.c       Real-time dashboard with ANSI colors
logger.c        Structured event logging with background flush
```

The binary acts as both **client** and **server**. Running `./protocol queue start`
launches the server. Running `./protocol producer register "hello"` or
`./protocol consumer register` connects to the server as a client.

---

## Cross-Network Support (queue.c)

### `gethostbyname(hostname)`

Converts a hostname (or dotted IP address string) to a network address:

```c
struct hostent *host = gethostbyname("server.example.com");
if (host == NULL) {
    perror("gethostbyname");
    return -1;
}
```

Returns a `struct hostent` containing:
- `h_addr_list[0]` — Primary IP address (binary format)
- `h_length` — Length of address (4 for IPv4, 16 for IPv6)

The returned pointer points to static storage, so the result is valid only until 
the next `gethostbyname` call. This is why we `memcpy` the address.

### `memcpy(&address.sin_addr, host->h_addr, host->h_length)`

Copies the resolved IP address into the socket address struct. `sin_addr` is a 
`struct in_addr` (32-bit IPv4 address), and `host->h_addr` points to the same 
format from the DNS resolution.

### Configuration-Driven Connectivity

**Before:** Clients hardcoded to `INADDR_LOOPBACK` (127.0.0.1)
**Now:** Clients read `server_address` from `queue.conf` and resolve it at runtime

This allows:
```
queue.conf on Machine A:  server_address=localhost      (server + local producer)
queue.conf on Machine B:  server_address=192.168.1.100  (remote producer)
queue.conf on Machine C:  server_address=192.168.1.100  (remote consumer)
```

Same binary, different configs, instant cross-network support.

### Server Binding

Server still binds to `INADDR_ANY` (0.0.0.0), which means:
- Accept connections on all network interfaces
- Clients can reach the server from any network they're connected to
- No need to configure the server per interface

---

## Networking (queue.c, server.c)

### `socket(AF_INET, SOCK_STREAM, 0)`

Creates a TCP socket. The three arguments:

- **`AF_INET`** — Address family. IPv4 sockets. `AF_INET6` would be IPv6.
- **`SOCK_STREAM`** — Socket type. Reliable, ordered, byte-stream connection (TCP). 
  `SOCK_DGRAM` would give unreliable datagrams (UDP).
- **`0`** — Protocol. Zero means "pick the default protocol," which is TCP for 
  `AF_INET + SOCK_STREAM`.

Returns a **file descriptor** (an integer). All subsequent operations — `bind`, 
`listen`, `accept`, `connect`, `read`, `write`, `close` — use this fd. 
Returns -1 on failure and sets `errno`.

### `setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))`

Configures socket options before binding. Used in `server.c:start_queue`.

- **`SOL_SOCKET`** — Option level. `SOL_SOCKET` is a general socket option. 
  TCP-specific options use `IPPROTO_TCP`.
- **`SO_REUSEADDR`** — Allows binding to an address in `TIME_WAIT` state. Without this, 
  if the server crashes and restarts quickly, `bind()` fails with "Address already in use" 
  because the kernel holds the port for ~60 seconds to drain late packets. With 
  `SO_REUSEADDR`, new sockets can bind immediately.

### `struct sockaddr_in` and byte order

```c
struct sockaddr_in address = {
    .sin_family = AF_INET,
    .sin_addr.s_addr = htonl(INADDR_ANY),
    .sin_port = htons(config.port)
};
```

- **`htons` / `htonl`** — "Host TO Network Short/Long." Network protocols use big-endian 
  byte order. Your CPU might be little-endian. These functions convert so values are in 
  the right byte order on the wire. `htons` converts 16-bit values (ports), `htonl` 
  converts 32-bit (addresses).
- **`INADDR_ANY` (0.0.0.0)** — Server uses this to accept connections on any network 
  interface (localhost, LAN, WAN, etc.).
- **`INADDR_LOOPBACK` (127.0.0.1)** — Deprecated in this project. Used to be hardcoded 
  for local-only connections.

### `bind(fd, addr, addrlen)`

Associates the socket with a specific address and port. Only the server calls this. 
After `bind`, the OS knows "traffic arriving on this port goes to this socket." 
If two processes try to bind the same port, the second fails — unless `SO_REUSEADDR` 
is set.

### `listen(fd, backlog)`

Marks the socket as **passive** — one that will accept incoming connections rather 
than initiate them. The `backlog` argument sets the maximum length of the queue of 
pending connections. If 10 clients connect simultaneously before the server calls 
`accept`, the kernel queues up to `backlog` of them and refuses the rest.

### `accept(fd, addr, addrlen)`

Blocks until a client connects. Returns a **new** file descriptor for the specific 
client connection. The original socket continues listening. This is why a server has 
two kinds of fds: one "listening" fd (created by `socket`) and many "connected" fds 
(returned by `accept`, one per client).

### `connect(fd, addr, addrlen)`

Client-side counterpart of `accept`. Initiates a TCP connection to the server at 
the given address. Blocks until the three-way handshake completes (SYN → SYN-ACK → ACK). 
Returns 0 on success, -1 on failure (e.g., server not running → "Connection refused").

### `shutdown(fd, SHUT_WR)`

Used in `publish_message` after writing. Tells the server "I'm done sending" by sending 
a TCP FIN. The server's `recv` returns 0, signaling end-of-stream. Unlike `close`, 
`shutdown` lets you still *read* from the socket after shutting down writes. Here it's 
used for a clean half-close: the producer writes its message, signals "no more data," 
then closes.

### `dprintf(fd, fmt, ...)`

Like `printf`, but writes to a file descriptor instead of stdout. Used to send the role 
string ("PRODUCER\n" or "CONSUMER\n") and messages over the socket. More convenient 
than formatting into a buffer and calling `write`.

### `signal(SIGPIPE, SIG_IGN)`

When you `write` to a socket whose remote end has closed, the kernel sends your process 
`SIGPIPE`, which kills it by default. The server ignores this signal so that one 
misbehaving client doesn't crash the whole server. Instead, `write` returns -1 with 
`errno = EPIPE`, which the server handles gracefully by removing the consumer from the list.

---

## Concurrency (server.c)

### `pthread_create(&thread, NULL, handle_client, arg)`

Spawns a new thread to handle each client connection. Each thread runs `handle_client` 
with the client's fd passed as the argument. Threads share the same address space 
(including the global `Queue q`), which is why all threads can read/write the consumer list.

The fd is passed via a heap-allocated `int*` rather than casting the fd to `void*` 
because the accept loop might overwrite a stack variable before the thread reads it — 
a classic race condition.

### `pthread_detach(thread)`

Marks the thread as "I don't need to join this." When a detached thread exits, its 
resources are reclaimed automatically. Without detach, you'd need to call `pthread_join` 
to avoid leaking thread resources. Since the server doesn't need the thread's return 
value, detach is the right choice.

### `pthread_mutex_t` and thread safety

The global `Queue q` (consumer list, counts) is now protected by `pthread_mutex_lock` 
and `pthread_mutex_unlock`:

```c
pthread_mutex_lock(&q.consumer_lock);
// Critical section: read/write q.consumer_count, q.consumers[]
pthread_mutex_unlock(&q.consumer_lock);
```

This prevents data races when two threads simultaneously add/remove consumers or 
broadcast messages.

---

## Write-Ahead Log (wal.c, message.c)

### What is a WAL?

A **Write-Ahead Log** is a durability mechanism. The idea: before performing an action 
(delivering a message), first write it to a persistent log on disk. If the server crashes, 
the log contains every message that wasn't yet delivered. On recovery, you **replay** 
the log to bring the system back to a consistent state.

The ordering is critical: **write to log first, then act.** If you act first and crash 
before logging, the action is lost. If you log first and crash before acting, you can 
redo the action on recovery.

WALs are used in databases (PostgreSQL, SQLite), filesystems (ext4 journaling), and 
message queues (Kafka, RabbitMQ).

### Message Struct with Timestamps

```c
typedef struct {
    char content[4096];           // Message text
    char timestamp[32];           // ISO 8601 UTC: "2026-09-25T08:04:01Z"
    int retention_count;          // How many consumers received it
} Message;
```

Every message is tagged with a timestamp at creation time via `message_set_timestamp()`, 
which calls `gmtime()` and `strftime()`.

### How this project uses the WAL

The message lifecycle:

```
1. Producer sends message
2. append_wal(message)           ── write to WAL.log (fsync to ensure it hits disk)
3. broadcast_message()           ── deliver to all connected consumers
4. remove_wal_message()          ── increment retention, delete if threshold reached
```

If the server crashes between steps 2 and 4, the message is still in `WAL.log`. 
When a new consumer connects, `replay_wal` sends them everything still in the log — 
these are messages that were received but never fully delivered.

### Serialization Format

Messages in `WAL.log` are stored with retention metadata using a pipe delimiter:

```
2026-09-25T08:04:15Z|hello world|0
2026-09-25T08:04:16Z|foo bar|1
2026-09-25T08:04:17Z|test message|2
```

Format: `timestamp|message_content|retention_count`

- **Deserialization** (`message_deserialize`) — splits on the rightmost `|` to get 
  retention, then the second-to-last `|` to get content. Everything before is timestamp.
- **Serialization** (`message_serialize`) — formats as `timestamp|content|retention_count`.

### `replay_wal(client_fd)`

Reads `WAL.log` line by line and sends each message to the newly connected consumer. 
After a successful replay, truncates the WAL (opens with `O_TRUNC`) because those 
messages have now been delivered. If the WAL file doesn't exist (`ENOENT`), that's 
fine — it means there are no undelivered messages.

### Message Retention

Each message tracks how many consumers have successfully received it. Only after 
reaching `message_retention` consumers does the message get deleted from WAL.

Example with `message_retention = 2`:

```
Message "hello" arrives
├─ append_wal("hello")          → WAL contains "timestamp|hello|0"
├─ broadcast to Consumer 1      ✓ received
├─ increment_retention()        → WAL contains "timestamp|hello|1"
├─ broadcast to Consumer 2      ✓ received
├─ increment_retention()        → retention=2 >= max_retention=2 → DELETE
└─ WAL now empty for "hello"
```

This enables **at-least-N-delivery guarantees**: a message won't be discarded from 
the queue until at least N distinct consumers have processed it. Useful for 
redundancy or ensuring multiple systems receive important events.

### `remove_wal_message(message_content, max_retention)`

Increments the retention count for a message and deletes it if the threshold is reached:

1. Read `WAL.log` line by line
2. Find the first line matching `message_content`
3. Increment its retention count
4. Check `message_should_delete()`: if retention >= max_retention, skip writing to temp file (effectively deletes it)
5. Otherwise, write the updated line to temp file
6. Atomically rename temp over original (atomic WAL update)

### `fsync(fd)`

Forces the OS to flush file data from kernel buffers to the physical disk. Without 
`fsync`, the OS might buffer writes and a crash could lose them. Called after WAL 
writes to guarantee the message actually reached disk before the server attempts delivery. 
This is the "D" in ACID (Durability).

---

## Event Logging (logger.c)

### Real-time Buffered Logging

The logger buffers events in memory and periodically flushes them to disk:

```c
logger_init(log_file, logging_interval_ms);
logger_log(LOG_EVENT, "CONSUMER", "Connected - Total: %d/%d", count, max);
// ... more events ...
logger_flush();  // Flushes buffered events to disk
```

- **Buffer size:** 64KB in-memory circular buffer
- **Flush interval:** Configurable (default 100ms)
- **Thread-safe:** Separate mutex on logger buffer

### Background Flush Thread

A detached thread wakes up every `logging_interval` ms and flushes the buffer:

```c
static void *flush_thread_func(void *arg) {
    while (logger.running) {
        usleep(logger.logging_interval_ms * 1000);
        logger_flush();
    }
    return NULL;
}
```

This prevents blocking the main server thread on disk I/O.

### Log Format

```
[2026-09-25 08:04:15] EVENT | CONSUMER | Connected - Total: 1/100
[2026-09-25 08:04:16] MESSAGE | PUBLISHED | hello world
[2026-09-25 08:04:16] MESSAGE | DELIVERED | to 1 consumer(s)
[2026-09-25 08:04:17] ERROR | CONSUMER | Connection rejected - max consumers reached
```

Format: `[YYYY-MM-DD HH:MM:SS] LEVEL | CATEGORY | Message`

Each log entry includes:
- Timestamp (local time via `localtime()` and `strftime()`)
- Log level (INFO, EVENT, ERROR, MESSAGE)
- Category (CONSUMER, SERVER, etc.)
- Formatted message (supports printf-style format strings)

### Log Levels

- **INFO** — Informational messages (server startup, etc.)
- **EVENT** — User-facing events (connections, disconnections)
- **MESSAGE** — Message events (published, delivered)
- **ERROR** — Error conditions (rejected connections, failures)

---

## Real-Time Dashboard (display.c)

### ANSI Color Codes

The dashboard uses ANSI escape sequences for colors and formatting:

```c
#define BOLD "\x1b[1m"
#define GREEN "\x1b[32m"
#define CYAN "\x1b[36m"
#define YELLOW "\x1b[33m"
#define RESET "\x1b[0m"
```

These codes are embedded in printf strings to style terminal output. Most modern 
terminals (Linux, macOS, Windows Terminal) support these.

### Clear Screen

```c
#define CLEAR "\x1b[2J\x1b[H"
```

Clears the screen and moves cursor to home (top-left). Used when initially displaying 
the dashboard header.

### Status Display

```
┌─ Consumer Status ──────────────────────────────────┐
│ Connected: 2/100
│ Capacity:  2.0%
│ Available: 98
└────────────────────────────────────────────────────┘
```

Shows real-time metrics:
- **Connected** — Current count and max
- **Capacity** — Percentage of max consumers in use (color-coded: green if < 80%, yellow if >= 80%)
- **Available** — Slots remaining for new connections

### Event Notifications

```
[08:04:15] + | Active: 1/100
[08:04:16] + | Active: 2/100
[08:04:17] - | Active: 1/100
```

Shows connection/disconnection events with timestamps:
- **+** — Consumer connected (green)
- **-** — Consumer disconnected (yellow)
- **Active** — Current count

---

## Configuration (config.c)

### File-Backed Config (`queue.conf`)

Configuration is stored as a simple `key=value` text file:

```
server_address=localhost
port=9294
max_producers=100
max_consumers=100
message_retention=1
logging_interval=100
log_file=./queue.log
```

`load_config` reads this file character by character, splits each line on `=`, and 
populates a `QueueConfig` struct. `put_config` writes it back.

This is a manual implementation of what `.env` parsers do — useful for understanding 
how config files are parsed at the syscall level using `open`/`read`/`write` instead 
of `fopen`/`fgets`/`fprintf`.

### `O_WRONLY | O_CREAT | O_TRUNC`

Flags used when writing the config file:

- **`O_TRUNC`** — If the file already exists, truncate it to zero length. This 
  effectively replaces the old config with the new one. Combined with `O_CREAT`, 
  this means "create or overwrite."

---

## CLI Dispatch (main.c)

The binary uses a two-level command structure:

```
./protocol <entity> <operation> [args]
```

`main.c` dispatches on the entity (`producer`, `consumer`, `queue`), and each entity 
handler dispatches on the operation (`register`, `unregister`, `configure`, `start`, 
`stop`). This is the same pattern used by `git` (`git remote add`), `docker` 
(`docker container ls`), and `kubectl`.

---

## File Descriptors — The Unifying Concept

Almost everything in this project operates on **file descriptors**: sockets, config 
files, the WAL log, the event log. In Unix, fds are the universal handle for I/O. 
`read(fd, ...)` and `write(fd, ...)` work the same whether `fd` points to:
- A file on disk
- A TCP connection
- A pipe
- A device

This is the "everything is a file" philosophy in practice.

The same `write_all` helper is used to write to both WAL files and socket connections — 
it doesn't need to know which one it's writing to. This abstraction is the power of 
Unix's design.
