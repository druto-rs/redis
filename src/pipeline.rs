//! Batched Redis command pipeline builder and atomic round-trip executor.

use crate::{error::RedisError, script::ScriptOutput};

/// An individual operation queued within a [`RedisPipeline`].
#[derive(Clone, Debug, PartialEq)]
pub enum PipelineOp {
    /// String `GET` command.
    Get(String),
    /// String `SET` command.
    Set(String, String),
    /// String `SETEX` command with expiration in seconds.
    SetEx(String, String, u64),
    /// String `SETNX` command.
    SetNx(String, String),
    /// Key `DEL` command.
    Del(String),
    /// Key `EXISTS` command.
    Exists(String),
    /// Key `INCRBY` command.
    IncrBy(String, i64),
    /// Hash `HGET` command.
    HGet(String, String),
    /// Hash `HSET` command.
    HSet(String, String, String),
    /// Hash `HDEL` command.
    HDel(String, String),
    /// List `LPUSH` command.
    LPush(String, Vec<String>),
    /// List `RPUSH` command.
    RPush(String, Vec<String>),
    /// Set `SADD` command.
    SAdd(String, Vec<String>),
    /// Sorted set `ZADD` command.
    ZAdd(String, f64, String),
}

/// Batched command pipeline aggregating multiple operations for a single round-trip execution.
#[derive(Clone, Debug, Default)]
pub struct RedisPipeline {
    operations: Vec<PipelineOp>,
}

macro_rules! dispatch_pipeline_op {
    ($target:expr, $op:expr) => {
        match $op {
            $crate::pipeline::PipelineOp::Get(key) => {
                let _ = $target.get::<Value, _>(key).await;
            }
            $crate::pipeline::PipelineOp::Set(key, value) => {
                let _ = $target
                    .set::<Value, _, _>(key, value, None, None, false)
                    .await;
            }
            $crate::pipeline::PipelineOp::SetEx(key, value, seconds) => {
                let _ = $target
                    .set::<Value, _, _>(
                        key,
                        value,
                        Some(Expiration::EX(*seconds as i64)),
                        None,
                        false,
                    )
                    .await;
            }
            $crate::pipeline::PipelineOp::SetNx(key, value) => {
                let _ = $target
                    .set::<Value, _, _>(key, value, None, Some(SetOptions::NX), false)
                    .await;
            }
            $crate::pipeline::PipelineOp::Del(key) => {
                let _ = $target.del::<Value, _>(key).await;
            }
            $crate::pipeline::PipelineOp::Exists(key) => {
                let _ = $target.exists::<Value, _>(key).await;
            }
            $crate::pipeline::PipelineOp::IncrBy(key, amount) => {
                let _ = $target.incr_by::<Value, _>(key, *amount).await;
            }
            $crate::pipeline::PipelineOp::HGet(key, field) => {
                let _ = $target.hget::<Value, _, _>(key, field).await;
            }
            $crate::pipeline::PipelineOp::HSet(key, field, value) => {
                let _ = $target
                    .hset::<Value, _, _>(key, (field.as_str(), value.as_str()))
                    .await;
            }
            $crate::pipeline::PipelineOp::HDel(key, field) => {
                let _ = $target.hdel::<Value, _, _>(key, field).await;
            }
            $crate::pipeline::PipelineOp::LPush(key, elements) => {
                let element_slices = elements.clone();
                let _ = $target.lpush::<Value, _, _>(key, element_slices).await;
            }
            $crate::pipeline::PipelineOp::RPush(key, elements) => {
                let element_slices = elements.clone();
                let _ = $target.rpush::<Value, _, _>(key, element_slices).await;
            }
            $crate::pipeline::PipelineOp::SAdd(key, members) => {
                let member_slices = members.clone();
                let _ = $target.sadd::<Value, _, _>(key, member_slices).await;
            }
            $crate::pipeline::PipelineOp::ZAdd(key, score, member) => {
                let _ = $target
                    .zadd::<Value, _, _>(key, None, None, false, false, (*score, member.clone()))
                    .await;
            }
        }
    };
}
pub(crate) use dispatch_pipeline_op;

impl RedisPipeline {
    /// Creates a new, empty command pipeline.
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    /// Queues a `GET` command.
    pub fn get(&mut self, key: impl Into<String>) -> &mut Self {
        self.operations.push(PipelineOp::Get(key.into()));
        self
    }

    /// Queues a `SET` command with no expiration.
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.operations
            .push(PipelineOp::Set(key.into(), value.into()));
        self
    }

    /// Queues a `SETEX` command with time-to-live in seconds.
    pub fn set_ex(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
        seconds: u64,
    ) -> &mut Self {
        self.operations
            .push(PipelineOp::SetEx(key.into(), value.into(), seconds));
        self
    }

    /// Queues a `SETNX` command.
    pub fn set_nx(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.operations
            .push(PipelineOp::SetNx(key.into(), value.into()));
        self
    }

    /// Queues a `DEL` command.
    pub fn del(&mut self, key: impl Into<String>) -> &mut Self {
        self.operations.push(PipelineOp::Del(key.into()));
        self
    }

    /// Queues an `EXISTS` command.
    pub fn exists(&mut self, key: impl Into<String>) -> &mut Self {
        self.operations.push(PipelineOp::Exists(key.into()));
        self
    }

    /// Queues an `INCRBY` command.
    pub fn incr_by(&mut self, key: impl Into<String>, amount: i64) -> &mut Self {
        self.operations.push(PipelineOp::IncrBy(key.into(), amount));
        self
    }

    /// Queues an `HGET` command.
    pub fn hget(&mut self, key: impl Into<String>, field: impl Into<String>) -> &mut Self {
        self.operations
            .push(PipelineOp::HGet(key.into(), field.into()));
        self
    }

    /// Queues an `HSET` command.
    pub fn hset(
        &mut self,
        key: impl Into<String>,
        field: impl Into<String>,
        value: impl Into<String>,
    ) -> &mut Self {
        self.operations
            .push(PipelineOp::HSet(key.into(), field.into(), value.into()));
        self
    }

    /// Queues an `HDEL` command.
    pub fn hdel(&mut self, key: impl Into<String>, field: impl Into<String>) -> &mut Self {
        self.operations
            .push(PipelineOp::HDel(key.into(), field.into()));
        self
    }

    /// Queues an `LPUSH` command.
    pub fn lpush(&mut self, key: impl Into<String>, elements: &[&str]) -> &mut Self {
        let strings = elements.iter().map(|item| item.to_string()).collect();
        self.operations.push(PipelineOp::LPush(key.into(), strings));
        self
    }

    /// Queues an `RPUSH` command.
    pub fn rpush(&mut self, key: impl Into<String>, elements: &[&str]) -> &mut Self {
        let strings = elements.iter().map(|item| item.to_string()).collect();
        self.operations.push(PipelineOp::RPush(key.into(), strings));
        self
    }

    /// Queues an `SADD` command.
    pub fn sadd(&mut self, key: impl Into<String>, members: &[&str]) -> &mut Self {
        let strings = members.iter().map(|item| item.to_string()).collect();
        self.operations.push(PipelineOp::SAdd(key.into(), strings));
        self
    }

    /// Queues a `ZADD` command.
    pub fn zadd(
        &mut self,
        key: impl Into<String>,
        score: f64,
        member: impl Into<String>,
    ) -> &mut Self {
        self.operations
            .push(PipelineOp::ZAdd(key.into(), score, member.into()));
        self
    }

    /// Returns the number of commands currently queued in the pipeline.
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Returns whether the pipeline has any queued commands.
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Clears all queued commands from the pipeline.
    pub fn clear(&mut self) {
        self.operations.clear();
    }

    /// Returns a slice of the queued operations.
    pub fn operations(&self) -> &[PipelineOp] {
        &self.operations
    }

    /// Consumes the pipeline and returns the inner vector of operations.
    pub fn into_operations(self) -> Vec<PipelineOp> {
        self.operations
    }

    /// Executes the batched pipeline against a live [`fred::clients::Client`].
    pub async fn execute_on_client(
        &self,
        client: &fred::clients::Client,
    ) -> Result<Vec<ScriptOutput>, RedisError> {
        use fred::{
            interfaces::*,
            types::{Expiration, SetOptions, Value},
        };

        let pipeline = client.pipeline();

        for op in &self.operations {
            dispatch_pipeline_op!(pipeline, op);
        }

        let raw_results = pipeline.all::<Vec<Value>>().await?;
        Ok(raw_results.into_iter().map(ScriptOutput::from).collect())
    }
}
