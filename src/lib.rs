//! Asynchronous Redis client manager, connection pooling, pub/sub messaging,
//! and command pipelines for Druto.
//!
//! # Architecture Overview
//!
//! The `redis` crate provides a unified, production-grade asynchronous interface for
//! interacting with Redis and Valkey instances. It supports standalone, clustered, and
//! Sentinel topologies with automatic connection multiplexing, failover handling, and
//! connection pooling.
//!
//! Core primitives include:
//! - [`RedisManager`]: Central registry managing multi-connection pools with default delegation.
//! - [`RedisCommands`]: Unified asynchronous command contract covering strings, hashes, lists,
//!   sets, sorted sets, and server operations.
//! - [`RedisConnection`]: Live client handle multiplexing concurrent async operations.
//! - [`RedisPipeline`]: Batched command execution in a single network round-trip.
//! - [`RedisTransaction`]: Atomic `MULTI` / `EXEC` / `DISCARD` execution blocks.
//! - [`RedisScript`]: Pre-computed SHA-1 Lua script evaluation with automatic `EVALSHA` fallback.
//! - [`RedisSubscriber`]: Dedicated, isolated Pub/Sub subscriber stream.
//! - [`RedisFake`]: High-fidelity in-memory test double for zero-dependency testing.

pub mod commands;
pub mod config;
pub mod connection;
pub mod error;
pub mod fake;
pub mod manager;
pub mod pipeline;
pub mod pubsub;
pub mod script;
pub mod transaction;

pub use commands::RedisCommands;
pub use config::{
    ClusterConfig, ConnectionTarget, DEFAULT_CONNECTION_NAME, DEFAULT_REDIS_HOST,
    DEFAULT_REDIS_PORT, RedisConfig, RedisConfigBuilder, SentinelConfig, StandaloneConfig,
};
pub use connection::RedisConnection;
pub use error::RedisError;
pub use fake::{FakeStore, RedisFake, normalize_redis_range};
pub use manager::RedisManager;
pub use pipeline::{PipelineOp, RedisPipeline};
pub use pubsub::{MessageStream, RedisMessage, RedisSubscriber};
pub use script::{RedisScript, ScriptContext, ScriptOutput, ScriptSimulation};
pub use transaction::RedisTransaction;

/// Convenient prelude re-exporting the primary traits, structs, and error types.
pub mod prelude {
    pub use crate::{
        ClusterConfig, MessageStream, RedisCommands, RedisConfig, RedisConnection, RedisError,
        RedisFake, RedisManager, RedisMessage, RedisPipeline, RedisScript, RedisSubscriber,
        RedisTransaction, ScriptOutput, StandaloneConfig,
    };
}
