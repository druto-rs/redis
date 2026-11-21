//! Atomic MULTI / EXEC / DISCARD transaction block abstraction.

use crate::{
    error::RedisError,
    pipeline::{PipelineOp, RedisPipeline},
    script::ScriptOutput,
};

/// Transaction block encapsulating atomic commands executed under `MULTI` and `EXEC`.
#[derive(Clone, Debug, Default)]
pub struct RedisTransaction {
    pipeline: RedisPipeline,
}

impl RedisTransaction {
    /// Creates a new, empty transaction block.
    pub fn new() -> Self {
        Self {
            pipeline: RedisPipeline::new(),
        }
    }

    /// Queues a `GET` command within this transaction.
    pub fn get(&mut self, key: impl Into<String>) -> &mut Self {
        self.pipeline.get(key);
        self
    }

    /// Queues a `SET` command within this transaction.
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.pipeline.set(key, value);
        self
    }

    /// Queues a `SETEX` command with time-to-live in seconds within this transaction.
    pub fn set_ex(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
        seconds: u64,
    ) -> &mut Self {
        self.pipeline.set_ex(key, value, seconds);
        self
    }

    /// Queues a `SETNX` command within this transaction.
    pub fn set_nx(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.pipeline.set_nx(key, value);
        self
    }

    /// Queues a `DEL` command within this transaction.
    pub fn del(&mut self, key: impl Into<String>) -> &mut Self {
        self.pipeline.del(key);
        self
    }

    /// Queues an `EXISTS` command within this transaction.
    pub fn exists(&mut self, key: impl Into<String>) -> &mut Self {
        self.pipeline.exists(key);
        self
    }

    /// Queues an `INCRBY` command within this transaction.
    pub fn incr_by(&mut self, key: impl Into<String>, amount: i64) -> &mut Self {
        self.pipeline.incr_by(key, amount);
        self
    }

    /// Queues an `HGET` command within this transaction.
    pub fn hget(&mut self, key: impl Into<String>, field: impl Into<String>) -> &mut Self {
        self.pipeline.hget(key, field);
        self
    }

    /// Queues an `HSET` command within this transaction.
    pub fn hset(
        &mut self,
        key: impl Into<String>,
        field: impl Into<String>,
        value: impl Into<String>,
    ) -> &mut Self {
        self.pipeline.hset(key, field, value);
        self
    }

    /// Queues an `HDEL` command within this transaction.
    pub fn hdel(&mut self, key: impl Into<String>, field: impl Into<String>) -> &mut Self {
        self.pipeline.hdel(key, field);
        self
    }

    /// Queues an `LPUSH` command within this transaction.
    pub fn lpush(&mut self, key: impl Into<String>, elements: &[&str]) -> &mut Self {
        self.pipeline.lpush(key, elements);
        self
    }

    /// Queues an `RPUSH` command within this transaction.
    pub fn rpush(&mut self, key: impl Into<String>, elements: &[&str]) -> &mut Self {
        self.pipeline.rpush(key, elements);
        self
    }

    /// Queues an `SADD` command within this transaction.
    pub fn sadd(&mut self, key: impl Into<String>, members: &[&str]) -> &mut Self {
        self.pipeline.sadd(key, members);
        self
    }

    /// Queues a `ZADD` command within this transaction.
    pub fn zadd(
        &mut self,
        key: impl Into<String>,
        score: f64,
        member: impl Into<String>,
    ) -> &mut Self {
        self.pipeline.zadd(key, score, member);
        self
    }

    /// Returns the number of commands currently queued in the transaction.
    pub fn len(&self) -> usize {
        self.pipeline.len()
    }

    /// Returns whether the transaction has any queued commands.
    pub fn is_empty(&self) -> bool {
        self.pipeline.is_empty()
    }

    /// Returns the list of queued operations.
    pub fn operations(&self) -> &[PipelineOp] {
        self.pipeline.operations()
    }

    /// Discards all queued commands in this transaction without executing.
    pub fn discard(&mut self) {
        self.pipeline.clear();
    }

    /// Commits the transaction atomically against a live [`fred::clients::Client`].
    pub async fn commit_on_client(
        &self,
        client: &fred::clients::Client,
    ) -> Result<Vec<ScriptOutput>, RedisError> {
        use fred::{
            interfaces::*,
            types::{Expiration, SetOptions, Value},
        };

        let transaction = client.multi();

        for op in self.pipeline.operations() {
            crate::pipeline::dispatch_pipeline_op!(transaction, op);
        }

        let raw_results = transaction.exec::<Vec<Value>>(true).await?;
        Ok(raw_results.into_iter().map(ScriptOutput::from).collect())
    }
}
