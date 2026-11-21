# druto-redis

[![Crates.io](https://img.shields.io/crates/v/druto-redis.svg)](https://crates.io/crates/druto-redis)
[![Documentation](https://docs.rs/druto-redis/badge.svg)](https://docs.rs/druto-redis)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE-MIT)

Asynchronous Redis connection manager, multiplexed pooling, pub/sub messaging channels, Lua scripts, and atomic transactions for Druto.

Encapsulating `fred` as its high-performance asynchronous engine, `druto-redis` delivers a cohesive, type-safe API for distributed caching, session storage, rate limiting, and real-time event broadcasting.

---

## Features

- **Multi-Connection Registry (`RedisManager`)**:
  - Manage multiple named connection pools (e.g. `"default"`, `"cache"`, `"session"`, `"subscriber"`).
  - Direct delegation: `RedisManager` implements `RedisCommands` directly against its default connection.
  - Thread-safe lazy initialization and shared connection caching across async tasks.
  - Lifecycle management and graceful termination (`quit_all`, `disconnect_all`).
- **Connection Topologies**:
  - Standalone, Cluster (with automatic slot mapping & redirection handling), and Sentinel failover setups.
  - Direct URL configuration (`redis://...`, `rediss://...`) or fluent builder options.
  - Optional TLS encryption gated behind the `tls` feature via `rustls`.
- **Rich Command Surface (`RedisCommands` Trait)**:
  - Primitives: Strings, bytes, integers, keys, expiration, and server commands.
  - Data structures: Hashes, Lists, Sets, and Sorted Sets.
  - Typed JSON helpers: `get_json<T>` and `set_json<T>` powered by `serde`.
- **Pipelines & Transactions**:
  - Command pipelining for batch network efficiency.
  - Atomic transactions (`MULTI` / `EXEC` / `DISCARD`) with optimistic concurrency (`WATCH`).
- **Lua Scripting (`RedisScript`)**:
  - Pre-computed SHA-1 evaluation via `EVALSHA` with transparent fallback to `EVAL`.
  - In-memory simulation handlers for unit testing without a live Redis server.
- **Asynchronous Pub/Sub (`RedisSubscriber`)**:
  - Stream-based pub/sub producing an asynchronous stream of `RedisMessage` instances.
  - Pattern subscriptions (`PSUBSCRIBE`) and dedicated subscriber connections to avoid blocking command pools.
- **In-Memory Test Double (`RedisFake`)**:
  - Zero-IO test mock implementing `RedisCommands` for unit and integration testing.

---

## Installation

Add `druto-redis` to your `Cargo.toml`:

```toml
[dependencies]
druto-redis = "0.1.0"
```

For TLS connection support:

```toml
[dependencies]
druto-redis = { version = "0.1.0", features = ["tls"] }
```

---

## Quick Start

### Basic Commands via `RedisManager`

```rust
use druto_redis::{RedisConfig, RedisManager, commands::RedisCommands};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RedisConfig::from_url("redis://127.0.0.1:6379")?;
    let manager = RedisManager::new(config);

    // Default connection delegation
    manager.set("user:100:name", "Alice").await?;
    let name: Option<String> = manager.get("user:100:name").await?;
    println!("Retrieved: {:?}", name);

    // Hashes and TTL
    manager.hset("user:100:meta", "role", "admin").await?;
    manager.expire("user:100:meta", 3600).await?;

    Ok(())
}
```

### In-Memory Testing with `RedisFake`

```rust
use druto_redis::{commands::RedisCommands, fake::RedisFake};

#[tokio::test]
async fn test_cache_logic() {
    let fake = RedisFake::new();

    fake.set("session_key", "active").await.unwrap();
    assert_eq!(fake.get("session_key").await.unwrap(), Some("active".to_string()));
}
```

---

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
