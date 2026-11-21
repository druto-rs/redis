# `redis` Agent Instructions

## Scope

This crate owns asynchronous Redis client management, connection pooling, multiplexed command execution, batch
pipelining, atomic transactions, pub/sub channels, Lua script caching, driver provider implementations, and testing
doubles.

## Design Rules

1. **Pub/Sub Isolation Invariant**:
    - Pub/Sub connections must remain strictly isolated from standard command/query connection pools. Subscribing to
      channels must never monopolize or block the multiplexed connection used for general commands.

2. **Atomic Multi-Key Mutation Invariant**:
    - Multi-key mutations and race-sensitive operations must utilize atomic transactions (`MULTI`/`EXEC`), pipelined
      batches, or cached Lua scripts to ensure atomicity across horizontally distributed nodes.

3. **Transparent Script Caching & Fallback**:
    - Registered Lua scripts must pre-compute SHA-1 digests and execute via `EVALSHA`. If the server returns a
      `NOSCRIPT` error, the execution must transparently fallback to `EVAL` and populate the server's script cache.

4. **Multiplexing & Graceful Reconnection**:
    - Connections must support concurrent multiplexing of asynchronous commands over a shared connection handle, with
      automatic reconnection handling and backoff upon network interruption.

5. **High-Fidelity Test Doubles**:
    - `RedisFake` must faithfully simulate Redis semantics (key expirations, hash fields, list push/pop ordering, set
      membership, sorted set scores) to allow zero-dependency testing of Redis-backed framework crates (sessions, rate
      limiters).

## Testing Requirements

- **Connection Management**: Test multi-connection resolution (`connection("default")`, `connection("cache")`), lazy
  instantiation, and graceful termination (`quit`, `disconnect`, `quit_all`, `disconnect_all`).
- **Core Commands**: Test comprehensive key-value, hash, list, set, and sorted set operations across both live and
  `RedisFake` drivers.
- **Pipelines**: Verify that batched commands execute in an atomic round-trip and preserve response ordering.
- **Transactions (`MULTI`/`EXEC`)**: Verify that transaction blocks isolate mutations until commit and handle `DISCARD`
  cleanly.
- **Lua Script Evaluation**: Test script SHA-1 caching, `EVALSHA` execution, and automatic fallback on `NOSCRIPT`.
- **Pub/Sub Isolation**: Test channel and pattern subscriptions, ensuring active subscriptions deliver messages without
  blocking parallel command operations.
- **Error Handling**: Verify strongly typed errors for connection failures, authentication rejections, protocol errors,
  and transaction conflicts.
- **Test Double Fidelity**: Verify that `RedisFake` mirrors real Redis semantics for expiration, hash fields, list
  ordering, set uniqueness, and sorted set scoring.
