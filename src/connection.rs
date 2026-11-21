//! Active multiplexed Redis connection handle.

use std::collections::{HashMap, HashSet};

use fred::{
    interfaces::*,
    types::{Builder, ClientState, ClusterHash, CustomCommand, Expiration, Map, SetOptions, Value},
};

use crate::{
    commands::{BoxFuture, RedisCommands},
    error::RedisError,
    pipeline::RedisPipeline,
    pubsub::RedisSubscriber,
    script::{RedisScript, ScriptOutput},
    transaction::RedisTransaction,
};

/// Active multiplexed Redis connection handle.
#[derive(Clone, Debug)]
pub struct RedisConnection {
    name: String,
    client: fred::clients::Client,
}

impl RedisConnection {
    /// Creates a new connection handle with an assigned name and initialized driver client.
    pub fn new(name: impl Into<String>, client: fred::clients::Client) -> Self {
        Self {
            name: name.into(),
            client,
        }
    }

    /// Returns the logical identifier of this connection.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Checks whether the underlying connection is currently active and connected.
    pub fn is_connected(&self) -> bool {
        self.client.state() == ClientState::Connected
    }

    /// Accesses the underlying driver client.
    pub fn raw_client(&self) -> &fred::clients::Client {
        &self.client
    }

    /// Initiates a multi-command pipelined batch execution.
    pub fn pipeline(&self) -> RedisPipeline {
        RedisPipeline::new()
    }

    /// Initiates an atomic multi-command transaction block.
    pub fn transaction(&self) -> RedisTransaction {
        RedisTransaction::new()
    }

    /// Executes a queued pipeline against this connection.
    pub async fn execute_pipeline(
        &self,
        pipeline: &RedisPipeline,
    ) -> Result<Vec<ScriptOutput>, RedisError> {
        pipeline.execute_on_client(&self.client).await
    }

    /// Commits an atomic transaction block against this connection.
    pub async fn commit_transaction(
        &self,
        transaction: &RedisTransaction,
    ) -> Result<Vec<ScriptOutput>, RedisError> {
        transaction.commit_on_client(&self.client).await
    }

    /// Spawns an isolated pub/sub subscriber client attached to the same endpoint.
    pub async fn subscriber(&self) -> Result<RedisSubscriber, RedisError> {
        let subscriber_client =
            Builder::from_config(self.client.client_config()).build_subscriber_client()?;
        subscriber_client.connect();
        subscriber_client.wait_for_connect().await?;

        Ok(RedisSubscriber::from_client(subscriber_client))
    }

    /// Forcefully reconnects or closes the connection immediately.
    pub async fn disconnect(&self) -> Result<(), RedisError> {
        self.client
            .force_reconnection()
            .await
            .map_err(RedisError::from)
    }

    async fn set_with_expiration(
        &self,
        key: &str,
        value: Value,
        seconds: u64,
    ) -> Result<(), RedisError> {
        self.client
            .set::<(), _, _>(
                key,
                value,
                Some(Expiration::EX(seconds as i64)),
                None,
                false,
            )
            .await
            .map_err(RedisError::from)
    }
}

impl RedisCommands for RedisConnection {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<String>, RedisError>> {
        Box::pin(async move {
            self.client
                .get::<Option<String>, _>(key)
                .await
                .map_err(RedisError::from)
        })
    }

    fn get_bytes<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, RedisError>> {
        Box::pin(async move {
            self.client
                .get::<Option<Vec<u8>>, _>(key)
                .await
                .map_err(RedisError::from)
        })
    }

    fn set<'a>(&'a self, key: &'a str, value: &'a str) -> BoxFuture<'a, Result<(), RedisError>> {
        Box::pin(async move {
            self.client
                .set::<(), _, _>(key, value, None, None, false)
                .await
                .map_err(RedisError::from)
        })
    }

    fn set_bytes<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
    ) -> BoxFuture<'a, Result<(), RedisError>> {
        Box::pin(async move {
            self.client
                .set::<(), _, _>(key, value, None, None, false)
                .await
                .map_err(RedisError::from)
        })
    }

    fn set_ex<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
        seconds: u64,
    ) -> BoxFuture<'a, Result<(), RedisError>> {
        Box::pin(async move { self.set_with_expiration(key, value.into(), seconds).await })
    }

    fn set_bytes_ex<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        seconds: u64,
    ) -> BoxFuture<'a, Result<(), RedisError>> {
        Box::pin(async move { self.set_with_expiration(key, value.into(), seconds).await })
    }

    fn set_nx<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let res: Option<String> = self
                .client
                .set(key, value, None, Some(SetOptions::NX), false)
                .await
                .map_err(RedisError::from)?;
            Ok(res.is_some())
        })
    }

    fn del<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let count: u64 = self.client.del(key).await.map_err(RedisError::from)?;
            Ok(count > 0)
        })
    }

    fn del_many<'a>(&'a self, keys: &'a [&'a str]) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            if keys.is_empty() {
                return Ok(0);
            }
            let key_strings = keys.iter().map(|k| (*k).to_string()).collect::<Vec<_>>();
            let count: u64 = self
                .client
                .del(key_strings)
                .await
                .map_err(RedisError::from)?;
            Ok(count)
        })
    }

    fn exists<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let count: u64 = self.client.exists(key).await.map_err(RedisError::from)?;
            Ok(count > 0)
        })
    }

    fn expire<'a>(&'a self, key: &'a str, seconds: u64) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let count: u64 = self
                .client
                .expire(key, seconds as i64, None)
                .await
                .map_err(RedisError::from)?;
            Ok(count > 0)
        })
    }

    fn ttl<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<i64, RedisError>> {
        Box::pin(async move { self.client.ttl(key).await.map_err(RedisError::from) })
    }

    fn incr_by<'a>(&'a self, key: &'a str, amount: i64) -> BoxFuture<'a, Result<i64, RedisError>> {
        Box::pin(async move {
            self.client
                .incr_by(key, amount)
                .await
                .map_err(RedisError::from)
        })
    }

    fn mget<'a>(
        &'a self,
        keys: &'a [&'a str],
    ) -> BoxFuture<'a, Result<Vec<Option<String>>, RedisError>> {
        Box::pin(async move {
            if keys.is_empty() {
                return Ok(Vec::new());
            }
            let key_strings = keys.iter().map(|k| (*k).to_string()).collect::<Vec<_>>();
            let values: Vec<Option<String>> = self
                .client
                .mget(key_strings)
                .await
                .map_err(RedisError::from)?;
            Ok(values)
        })
    }

    fn mset<'a>(
        &'a self,
        pairs: &'a [(&'a str, &'a str)],
    ) -> BoxFuture<'a, Result<(), RedisError>> {
        Box::pin(async move {
            if pairs.is_empty() {
                return Ok(());
            }
            let mut map = Map::new();
            for (k, v) in pairs {
                map.insert((*k).into(), (*v).into());
            }
            self.client.mset(map).await.map_err(RedisError::from)
        })
    }

    fn hget<'a>(
        &'a self,
        key: &'a str,
        field: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>, RedisError>> {
        Box::pin(async move { self.client.hget(key, field).await.map_err(RedisError::from) })
    }

    fn hset<'a>(
        &'a self,
        key: &'a str,
        field: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let count: u64 = self
                .client
                .hset(key, (field, value))
                .await
                .map_err(RedisError::from)?;
            Ok(count > 0)
        })
    }

    fn hdel<'a>(&'a self, key: &'a str, field: &'a str) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let count: u64 = self
                .client
                .hdel(key, field)
                .await
                .map_err(RedisError::from)?;
            Ok(count > 0)
        })
    }

    fn hexists<'a>(
        &'a self,
        key: &'a str,
        field: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let count: u64 = self
                .client
                .hexists(key, field)
                .await
                .map_err(RedisError::from)?;
            Ok(count > 0)
        })
    }

    fn hget_all<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<HashMap<String, String>, RedisError>> {
        Box::pin(async move { self.client.hgetall(key).await.map_err(RedisError::from) })
    }

    fn hincr_by<'a>(
        &'a self,
        key: &'a str,
        field: &'a str,
        amount: i64,
    ) -> BoxFuture<'a, Result<i64, RedisError>> {
        Box::pin(async move {
            self.client
                .hincrby(key, field, amount)
                .await
                .map_err(RedisError::from)
        })
    }

    fn hlen<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move { self.client.hlen(key).await.map_err(RedisError::from) })
    }

    fn hkeys<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<String>, RedisError>> {
        Box::pin(async move { self.client.hkeys(key).await.map_err(RedisError::from) })
    }

    fn hvals<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<String>, RedisError>> {
        Box::pin(async move { self.client.hvals(key).await.map_err(RedisError::from) })
    }

    fn lpush<'a>(
        &'a self,
        key: &'a str,
        elements: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            if elements.is_empty() {
                return self.llen(key).await;
            }
            let elements_vec = elements
                .iter()
                .map(|e| (*e).to_string())
                .collect::<Vec<_>>();
            let count: u64 = self
                .client
                .lpush(key, elements_vec)
                .await
                .map_err(RedisError::from)?;
            Ok(count)
        })
    }

    fn rpush<'a>(
        &'a self,
        key: &'a str,
        elements: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            if elements.is_empty() {
                return self.llen(key).await;
            }
            let elements_vec = elements
                .iter()
                .map(|e| (*e).to_string())
                .collect::<Vec<_>>();
            let count: u64 = self
                .client
                .rpush(key, elements_vec)
                .await
                .map_err(RedisError::from)?;
            Ok(count)
        })
    }

    fn lpop<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<String>, RedisError>> {
        Box::pin(async move { self.client.lpop(key, None).await.map_err(RedisError::from) })
    }

    fn rpop<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<String>, RedisError>> {
        Box::pin(async move { self.client.rpop(key, None).await.map_err(RedisError::from) })
    }

    fn llen<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move { self.client.llen(key).await.map_err(RedisError::from) })
    }

    fn lrange<'a>(
        &'a self,
        key: &'a str,
        start: i64,
        stop: i64,
    ) -> BoxFuture<'a, Result<Vec<String>, RedisError>> {
        Box::pin(async move {
            self.client
                .lrange(key, start, stop)
                .await
                .map_err(RedisError::from)
        })
    }

    fn sadd<'a>(
        &'a self,
        key: &'a str,
        members: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            if members.is_empty() {
                return Ok(0);
            }
            let members_vec = members.iter().map(|m| (*m).to_string()).collect::<Vec<_>>();
            self.client
                .sadd(key, members_vec)
                .await
                .map_err(RedisError::from)
        })
    }

    fn srem<'a>(
        &'a self,
        key: &'a str,
        members: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            if members.is_empty() {
                return Ok(0);
            }
            let members_vec = members.iter().map(|m| (*m).to_string()).collect::<Vec<_>>();
            self.client
                .srem(key, members_vec)
                .await
                .map_err(RedisError::from)
        })
    }

    fn smembers<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<HashSet<String>, RedisError>> {
        Box::pin(async move { self.client.smembers(key).await.map_err(RedisError::from) })
    }

    fn sismember<'a>(
        &'a self,
        key: &'a str,
        member: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            self.client
                .sismember(key, member)
                .await
                .map_err(RedisError::from)
        })
    }

    fn scard<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move { self.client.scard(key).await.map_err(RedisError::from) })
    }

    fn zadd<'a>(
        &'a self,
        key: &'a str,
        score: f64,
        member: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let count: u64 = self
                .client
                .zadd(key, None, None, false, false, (score, member))
                .await
                .map_err(RedisError::from)?;
            Ok(count > 0)
        })
    }

    fn zrem<'a>(
        &'a self,
        key: &'a str,
        members: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            if members.is_empty() {
                return Ok(0);
            }
            let members_vec = members.iter().map(|m| (*m).to_string()).collect::<Vec<_>>();
            self.client
                .zrem(key, members_vec)
                .await
                .map_err(RedisError::from)
        })
    }

    fn zscore<'a>(
        &'a self,
        key: &'a str,
        member: &'a str,
    ) -> BoxFuture<'a, Result<Option<f64>, RedisError>> {
        Box::pin(async move {
            self.client
                .zscore(key, member)
                .await
                .map_err(RedisError::from)
        })
    }

    fn zcard<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move { self.client.zcard(key).await.map_err(RedisError::from) })
    }

    fn zcount<'a>(
        &'a self,
        key: &'a str,
        min: f64,
        max: f64,
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            self.client
                .zcount(key, min, max)
                .await
                .map_err(RedisError::from)
        })
    }

    fn zrange<'a>(
        &'a self,
        key: &'a str,
        start: i64,
        stop: i64,
    ) -> BoxFuture<'a, Result<Vec<String>, RedisError>> {
        Box::pin(async move {
            self.client
                .zrange(key, start, stop, None, false, None, false)
                .await
                .map_err(RedisError::from)
        })
    }

    fn zremrangebyscore<'a>(
        &'a self,
        key: &'a str,
        min: f64,
        max: f64,
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            self.client
                .zremrangebyscore(key, min, max)
                .await
                .map_err(RedisError::from)
        })
    }

    fn ping(&self) -> BoxFuture<'_, Result<String, RedisError>> {
        Box::pin(async move { self.client.ping(None).await.map_err(RedisError::from) })
    }

    fn flushdb(&self) -> BoxFuture<'_, Result<(), RedisError>> {
        Box::pin(async move {
            self.client
                .custom::<(), String>(
                    CustomCommand::new_static("FLUSHDB", ClusterHash::default(), false),
                    vec![],
                )
                .await
                .map_err(RedisError::from)
        })
    }

    fn keys<'a>(&'a self, pattern: &'a str) -> BoxFuture<'a, Result<Vec<String>, RedisError>> {
        Box::pin(async move {
            self.client
                .custom::<Vec<String>, String>(
                    CustomCommand::new_static("KEYS", ClusterHash::FirstKey, false),
                    vec![pattern.to_string()],
                )
                .await
                .map_err(RedisError::from)
        })
    }

    fn eval_script<'a>(
        &'a self,
        script: &'a RedisScript,
        keys: &'a [&'a str],
        args: &'a [&'a str],
    ) -> BoxFuture<'a, Result<ScriptOutput, RedisError>> {
        Box::pin(async move {
            let key_strings = keys.iter().map(|k| (*k).to_string()).collect::<Vec<_>>();
            let arg_strings = args.iter().map(|a| (*a).to_string()).collect::<Vec<_>>();

            match self
                .client
                .evalsha::<Value, _, _, _>(script.sha1(), key_strings.clone(), arg_strings.clone())
                .await
            {
                Ok(val) => Ok(ScriptOutput::from(val)),
                Err(err) => {
                    let err_string = err.to_string();
                    if err_string.contains("NOSCRIPT") {
                        let fallback_val: Value = self
                            .client
                            .eval(script.source(), key_strings, arg_strings)
                            .await?;
                        Ok(ScriptOutput::from(fallback_val))
                    } else {
                        Err(RedisError::from(err))
                    }
                }
            }
        })
    }
}
