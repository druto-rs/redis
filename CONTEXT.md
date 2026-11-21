# `redis` Crate Context & Architecture

## Purpose

The `redis` crate provides asynchronous Redis connection management, multiplexed connection pooling, command
pipelining, atomic transactions, pub/sub messaging channels, and Lua script evaluation for the Druto web framework.
It serves as the distributed infrastructure backbone powering distributed sessions (`crates/session`), distributed
rate limiting (`crates/limiter`), caching, background queues, and real-time event broadcasting (`crates/events`).

## Reference Architecture

This crate encapsulates `fred` as its high-performance asynchronous transport engine, modeling Druto's unified facade
after the proven connection architectures of:

- **AdonisJS**: `@adonisjs/redis` (`RedisManager`, `RedisConnection`, `RedisClusterConnection`, `AbstractConnection`,
  pub/sub isolation, Lua script definition)
- **Laravel**: `illuminate/redis` (`RedisManager`, connection pooling, command pipelines, transactions, pub/sub)
- **Rust Ecosystem**: `fred.rs` (native async multiplexing, zero-copy RESP2/RESP3 framing, built-in Cluster slot
  routing, Sentinel failover, automatic reconnection backoff)
- **Underlying Standards**: RESP2 / RESP3 (Redis Serialization Protocol), Redis Pub/Sub specifications

## Core Responsibilities

1. **Multi-Connection Management & Registry (`RedisManager`)**:
    - **Named Connection Configuration**: Manage multiple independent connection pools (e.g. `"default"`, `"cache"`,
      `"session"`, `"subscriber"`, `"queue"`).
    - **Default Connection Delegation**: `RedisManager` implements `RedisCommands` directly by delegating to its
      configured `"default"` connection, allowing zero-boilerplate usage (`manager.get("key").await?`) while still
      exposing `.connection("name")?` to retrieve dedicated named pools.
    - **Lazy Initialization & Thread-Safe Caching**: Connections are lazily established upon first access and cached
      in thread-safe registries for zero-cost reuse across asynchronous tasks.
    - **Lifecycle & Health Tracking**: Tracks connection states (`Connecting`, `Ready`, `Reconnecting`, `Closed`).
      Provides unified health checks and graceful termination (`quit`, `disconnect`, `quit_all`, `disconnect_all`).

2. **Connection Topologies & Configurations**:
    - **URL & Builder Ergonomics**: Connections can be configured directly from connection URLs (`redis://...`,
      `rediss://...`) or via fluent builders (`StandaloneConfig`, `ClusterConfig`, `SentinelConfig`).
    - **Standalone**: Direct single-node connection with host, port, password/credentials, database index, connection
      timeouts, and TLS configuration.
    - **Cluster**: Distributed Redis Cluster topology with automatic cluster node discovery, slot mapping, transparent
      command routing, and redirection handling (`MOVED` and `ASK`).
    - **Sentinel**: High-availability master/replica topology managed through Sentinel nodes with automatic failover
      detection and active master reelection.
    - **TLS Feature Gating**: Encrypted connections (`rediss://`) are gated behind an optional `tls` feature powered by
      `rustls` with WebPKI roots, ensuring zero-overhead builds for local Docker setups while providing seamless cloud
      encryption when activated.

3. **Multiplexing & Connection Safety**:
    - **Multiplexed Command Execution**: Multiplex asynchronous commands concurrently over a single underlying
      connection, eliminating head-of-line blocking and maximizing throughput.
    - **Resilient Reconnection**: Automatic reconnection with configurable exponential backoff and jitter when
      connections are interrupted.
    - **Pub/Sub Isolation**: In Redis, issuing subscription commands transforms the connection into subscriber mode,
      preventing standard commands. The crate enforces strict separation by provisioning dedicated subscriber
      connections for all Pub/Sub channels, guaranteeing command pools remain unblocked.

4. **Rich Command Surface & Ergonomics (`RedisCommands` Trait)**:
    - **Unified Trait Abstraction**: Native `async fn` `RedisCommands` trait implemented interchangeably by
      `RedisConnection`, `RedisManager`, `RedisPipeline`, `RedisTransaction`, and `RedisFake`.
    - **Raw String & Binary Primitives**: Zero-allocation hot paths for strings, bytes, and numbers (`get`, `set`,
      `setex`, `setnx`, `del`, `incr`, `decr`, `incrby`, `mget`, `mset`, `exists`, `expire`, `ttl`, `get_bytes`,
      `set_bytes`).
    - **Typed JSON Helpers**: Built-in JSON serialization helpers (`get_json<T: DeserializeOwned>`,
      `set_json<T: Serialize>`, `set_json_ex<T: Serialize>`) powered by `serde` and `serde_json` for effortless state
      persistence in sessions and caching.
    - **Hashes**: `hget`, `hset`, `hdel`, `hgetall`, `hincrby`, `hexists`, `hkeys`, `hvals`, `hlen`.
    - **Lists**: `lpush`, `rpush`, `lpop`, `rpop`, `lrange`, `llen`, `lindex`, `ltrim`.
    - **Sets**: `sadd`, `srem`, `smembers`, `sismember`, `scard`, `spop`.
    - **Sorted Sets**: `zadd`, `zrange`, `zrangebyscore`, `zrem`, `zscore`, `zcard`, `zcount`, `zincrby`.
    - **Keys & Server**: `del`, `exists`, `expire`, `expireat`, `ttl`, `keys`, `ping`, `flushdb`.

5. **Command Pipelines, Transactions & Lua Scripts**:
    - **Pipelines**: Batch multiple independent commands into a single round-trip network execution (`pipeline()`),
      dramatically reducing latency over network boundaries.
    - **Transactions**: Atomic `MULTI` / `EXEC` / `DISCARD` execution blocks with optimistic concurrency `WATCH`
      support for atomic multi-key operations.
    - **Lua Script Evaluation (`RedisScript`)**: Strongly typed script struct that pre-computes SHA-1 digests and
      executes via `EVALSHA` with transparent fallback to `EVAL` upon `NOSCRIPT` errors. Supports registering in-memory
      simulation closures (`with_fake_handler`) for execution against `RedisFake`.

6. **Asynchronous Pub/Sub Messaging**:
    - **Stream-First Architecture**: `RedisSubscriber` produces an asynchronous `MessageStream` yielding `RedisMessage`
      instances for subscribed channels and wildcard patterns (`subscribe`, `psubscribe`), allowing idiomatic
      cancellation safety and integration with `tokio::select!` and async loops.
    - **Convenience Listeners**: High-level `.listen(channel, handler)` helper for callback-style event handling.
    - **Event Integration**: Native dispatching bridge with `crates/events` for horizontal event propagation across
      multiple web nodes.

7. **Framework Driver Provider**:
    - Supplies underlying distributed atomic primitives for `crates/session` (`RedisSessionStore`) with TTL expiration
      and payload serialization.
    - Supplies underlying atomic rate-limiting primitives for `crates/limiter` (sliding-window log and token-bucket
      algorithms evaluated via atomic Lua scripts).
    - Provides distributed lock primitives (`Lock` with atomic acquisition, heartbeat renewal, and safe release).

8. **Test Double & In-Memory Mocking (`RedisFake`)**:
    - In-memory, thread-safe Redis test double implementing the `RedisCommands` trait.
    - Supports simulated Lua script execution via `RedisScript` fake handlers, TTL expiration simulation, and isolated
      in-memory pub/sub channels.
    - Enables comprehensive unit and integration testing of Redis-backed components (sessions, rate limiters, pub/sub
      listeners) without requiring an external Redis daemon or Docker container in CI/CD pipelines.

## Boundaries & Non-Responsibilities

- **Not an HTTP Server or Middleware**: Does not handle HTTP requests, headers, or responses directly.
- **Not Session or Limiter Logic**: Provides the distributed storage and atomic execution backend; session state
  machines belong in `crates/session`, and rate limiting rules belong in `crates/limiter`.
- **Runtime Agnostic**: Relies on standard asynchronous traits and primitives (`Future`, `Pin`, `BoxFuture`), avoiding
  hard dependency lock-in on specific runtime engines.

## Public API Surface

- `struct RedisManager`: Multi-connection manager, connection cache, and configuration facade (`connection`,
  `disconnect`, `disconnect_all`, `quit_all`, and default delegating `RedisCommands`).
- `struct RedisConfig`: Configuration container for default connection names and connection definitions.
- `struct RedisConfigBuilder`: Fluent builder for creating multi-connection and URL-based configurations.
- `enum ConnectionTarget`: `Url(String)`, `Standalone(StandaloneConfig)`, `Cluster(ClusterConfig)`,
  `Sentinel(SentinelConfig)`.
- `struct StandaloneConfig`: Host, port, username, password, database, timeouts, TLS configuration.
- `struct ClusterConfig`: Seed node addresses, cluster options.
- `struct SentinelConfig`: Sentinel node addresses, monitored service name, credentials.
- `struct RedisConnection`: Active client connection handle providing multiplexed async command execution.
- `trait RedisCommands`: Unified command contract covering raw strings/bytes, typed JSON helpers, hashes, lists, sets,
  sorted sets, and server commands, implemented by `RedisConnection`, `RedisManager`, and `RedisFake`.
- `type BoxFuture<'a, T>`: Pinned, heap-allocated thread-safe future matching Druto workspace async trait conventions.
- `struct RedisPipeline`: Batched command queue builder and executor.
- `enum PipelineOp`: Strongly typed command operations queued within a pipeline.
- `struct RedisTransaction`: Atomic MULTI/EXEC transaction block with command queuing and `discard()`.
- `struct RedisScript`: Lua script wrapper with pre-computed SHA-1 digest, automatic `EVALSHA` execution with `EVAL`
  fallback, and test simulation support.
- `enum ScriptOutput`: Strongly typed output values (`Integer`, `String`, `Binary`, `Boolean`, `Array`, `Nil`) returned
  by Lua scripts and command executions.
- `trait ScriptContext`: Context interface provided to in-memory script simulation closures during testing.
- `struct RedisSubscriber`: Dedicated pub/sub listener handle with channel and pattern message streams.
- `struct MessageStream`: Asynchronous stream yielding incoming `RedisMessage` values.
- `struct RedisMessage`: Incoming channel/pattern message containing channel name, string payload, binary payload, and
  matched pattern.
- `struct RedisFake`: Thread-safe, in-memory test double implementing `RedisCommands`, script simulation, and pub/sub
  broker for zero-dependency unit and integration testing.
- `enum RedisError`: Strongly typed errors for connection failures, authentication failures, command rejections,
  protocol errors, transaction aborts, serialization errors, script errors, and timeout errors.
