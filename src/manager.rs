//! Central multi-connection manager and default connection facade.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use fred::{interfaces::ClientLike, types::Builder};
use tokio::sync::RwLock;

use crate::{
    commands::{BoxFuture, RedisCommands},
    config::RedisConfig,
    connection::RedisConnection,
    error::RedisError,
    pubsub::RedisSubscriber,
    script::{RedisScript, ScriptOutput},
};

/// Central multi-connection manager maintaining lazy-initialized connection pools
/// and delegating commands directly to the default connection.
#[derive(Clone, Debug)]
pub struct RedisManager {
    config: RedisConfig,
    active_connections: Arc<RwLock<HashMap<String, RedisConnection>>>,
}

impl Default for RedisManager {
    fn default() -> Self {
        Self::new(RedisConfig::default())
    }
}

impl RedisManager {
    /// Creates a new manager instance configured with the supplied [`RedisConfig`].
    pub fn new(config: RedisConfig) -> Self {
        Self {
            config,
            active_connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Accesses the underlying configuration object.
    pub fn config(&self) -> &RedisConfig {
        &self.config
    }

    /// Retrieves an active connection handle by name, lazily initializing it if not yet open.
    pub async fn connection(&self, name: &str) -> Result<RedisConnection, RedisError> {
        {
            let read_guard = self.active_connections.read().await;
            if let Some(conn) = read_guard.get(name) {
                return Ok(conn.clone());
            }
        }

        let target = self.config.get_connection(name).ok_or_else(|| {
            RedisError::configuration(format!("Redis connection '{name}' not configured"))
        })?;

        let driver_config = target.to_driver_config()?;
        let client = Builder::from_config(driver_config).build()?;

        client.connect();
        client.wait_for_connect().await?;

        let connection = RedisConnection::new(name, client);

        let mut write_guard = self.active_connections.write().await;
        if let Some(existing) = write_guard.get(name) {
            let _ = connection.disconnect().await;
            return Ok(existing.clone());
        }
        write_guard.insert(name.to_string(), connection.clone());

        Ok(connection)
    }

    /// Retrieves the default connection handle, lazily initializing it if needed.
    pub async fn default_connection(&self) -> Result<RedisConnection, RedisError> {
        self.connection(self.config.default_connection_name()).await
    }

    /// Spawns an isolated pub/sub subscriber client attached to the named connection endpoint.
    pub async fn subscriber(&self, name: &str) -> Result<RedisSubscriber, RedisError> {
        let conn = self.connection(name).await?;
        conn.subscriber().await
    }

    /// Spawns an isolated pub/sub subscriber client attached to the default connection endpoint.
    pub async fn default_subscriber(&self) -> Result<RedisSubscriber, RedisError> {
        let conn = self.default_connection().await?;
        conn.subscriber().await
    }

    /// Gracefully closes all active connections.
    pub async fn quit_all(&self) -> Result<(), RedisError> {
        let connections = {
            let mut write_guard = self.active_connections.write().await;
            write_guard
                .drain()
                .map(|(_, conn)| conn)
                .collect::<Vec<_>>()
        };

        for connection in connections {
            connection.disconnect().await?;
        }
        Ok(())
    }

    /// Forcefully disconnects a specific named connection.
    pub async fn disconnect(&self, name: &str) -> Result<(), RedisError> {
        let conn = {
            let mut write_guard = self.active_connections.write().await;
            write_guard.remove(name)
        };

        if let Some(connection) = conn {
            connection.disconnect().await?;
        }
        Ok(())
    }

    /// Forcefully disconnects all active connections.
    pub async fn disconnect_all(&self) -> Result<(), RedisError> {
        self.quit_all().await
    }

    /// Returns the number of currently active, open connection handles.
    pub async fn active_connections_count(&self) -> usize {
        let read_guard = self.active_connections.read().await;
        read_guard.len()
    }
}

impl RedisCommands for RedisManager {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<String>, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.get(key).await
        })
    }

    fn get_bytes<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.get_bytes(key).await
        })
    }

    fn set<'a>(&'a self, key: &'a str, value: &'a str) -> BoxFuture<'a, Result<(), RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.set(key, value).await
        })
    }

    fn set_bytes<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
    ) -> BoxFuture<'a, Result<(), RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.set_bytes(key, value).await
        })
    }

    fn set_ex<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
        seconds: u64,
    ) -> BoxFuture<'a, Result<(), RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.set_ex(key, value, seconds).await
        })
    }

    fn set_bytes_ex<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        seconds: u64,
    ) -> BoxFuture<'a, Result<(), RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.set_bytes_ex(key, value, seconds).await
        })
    }

    fn set_nx<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.set_nx(key, value).await
        })
    }

    fn del<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.del(key).await
        })
    }

    fn del_many<'a>(&'a self, keys: &'a [&'a str]) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.del_many(keys).await
        })
    }

    fn exists<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.exists(key).await
        })
    }

    fn expire<'a>(&'a self, key: &'a str, seconds: u64) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.expire(key, seconds).await
        })
    }

    fn ttl<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<i64, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.ttl(key).await
        })
    }

    fn incr_by<'a>(&'a self, key: &'a str, amount: i64) -> BoxFuture<'a, Result<i64, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.incr_by(key, amount).await
        })
    }

    fn mget<'a>(
        &'a self,
        keys: &'a [&'a str],
    ) -> BoxFuture<'a, Result<Vec<Option<String>>, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.mget(keys).await
        })
    }

    fn mset<'a>(
        &'a self,
        pairs: &'a [(&'a str, &'a str)],
    ) -> BoxFuture<'a, Result<(), RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.mset(pairs).await
        })
    }

    fn hget<'a>(
        &'a self,
        key: &'a str,
        field: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.hget(key, field).await
        })
    }

    fn hset<'a>(
        &'a self,
        key: &'a str,
        field: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.hset(key, field, value).await
        })
    }

    fn hdel<'a>(&'a self, key: &'a str, field: &'a str) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.hdel(key, field).await
        })
    }

    fn hexists<'a>(
        &'a self,
        key: &'a str,
        field: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.hexists(key, field).await
        })
    }

    fn hget_all<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<HashMap<String, String>, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.hget_all(key).await
        })
    }

    fn hincr_by<'a>(
        &'a self,
        key: &'a str,
        field: &'a str,
        amount: i64,
    ) -> BoxFuture<'a, Result<i64, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.hincr_by(key, field, amount).await
        })
    }

    fn hlen<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.hlen(key).await
        })
    }

    fn hkeys<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<String>, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.hkeys(key).await
        })
    }

    fn hvals<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<String>, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.hvals(key).await
        })
    }

    fn lpush<'a>(
        &'a self,
        key: &'a str,
        elements: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.lpush(key, elements).await
        })
    }

    fn rpush<'a>(
        &'a self,
        key: &'a str,
        elements: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.rpush(key, elements).await
        })
    }

    fn lpop<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<String>, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.lpop(key).await
        })
    }

    fn rpop<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<String>, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.rpop(key).await
        })
    }

    fn llen<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.llen(key).await
        })
    }

    fn lrange<'a>(
        &'a self,
        key: &'a str,
        start: i64,
        stop: i64,
    ) -> BoxFuture<'a, Result<Vec<String>, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.lrange(key, start, stop).await
        })
    }

    fn sadd<'a>(
        &'a self,
        key: &'a str,
        members: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.sadd(key, members).await
        })
    }

    fn srem<'a>(
        &'a self,
        key: &'a str,
        members: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.srem(key, members).await
        })
    }

    fn smembers<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<HashSet<String>, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.smembers(key).await
        })
    }

    fn sismember<'a>(
        &'a self,
        key: &'a str,
        member: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.sismember(key, member).await
        })
    }

    fn scard<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.scard(key).await
        })
    }

    fn zadd<'a>(
        &'a self,
        key: &'a str,
        score: f64,
        member: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.zadd(key, score, member).await
        })
    }

    fn zrem<'a>(
        &'a self,
        key: &'a str,
        members: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.zrem(key, members).await
        })
    }

    fn zscore<'a>(
        &'a self,
        key: &'a str,
        member: &'a str,
    ) -> BoxFuture<'a, Result<Option<f64>, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.zscore(key, member).await
        })
    }

    fn zcard<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.zcard(key).await
        })
    }

    fn zcount<'a>(
        &'a self,
        key: &'a str,
        min: f64,
        max: f64,
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.zcount(key, min, max).await
        })
    }

    fn zrange<'a>(
        &'a self,
        key: &'a str,
        start: i64,
        stop: i64,
    ) -> BoxFuture<'a, Result<Vec<String>, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.zrange(key, start, stop).await
        })
    }

    fn zremrangebyscore<'a>(
        &'a self,
        key: &'a str,
        min: f64,
        max: f64,
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.zremrangebyscore(key, min, max).await
        })
    }

    fn ping(&self) -> BoxFuture<'_, Result<String, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.ping().await
        })
    }

    fn flushdb(&self) -> BoxFuture<'_, Result<(), RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.flushdb().await
        })
    }

    fn keys<'a>(&'a self, pattern: &'a str) -> BoxFuture<'a, Result<Vec<String>, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.keys(pattern).await
        })
    }

    fn eval_script<'a>(
        &'a self,
        script: &'a RedisScript,
        keys: &'a [&'a str],
        args: &'a [&'a str],
    ) -> BoxFuture<'a, Result<ScriptOutput, RedisError>> {
        Box::pin(async move {
            let conn = self.default_connection().await?;
            conn.eval_script(script, keys, args).await
        })
    }
}
