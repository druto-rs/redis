//! In-memory, thread-safe Redis test double implementing [`RedisCommands`].

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::{RwLock, broadcast};

use crate::{
    commands::{BoxFuture, RedisCommands},
    error::RedisError,
    pipeline::{PipelineOp, RedisPipeline},
    pubsub::{MessageStream, RedisMessage, RedisSubscriber},
    script::{RedisScript, ScriptContext, ScriptOutput},
    transaction::RedisTransaction,
};

/// Normalizes Redis range indices (supporting negative offsets relative to the end).
pub fn normalize_redis_range(len: usize, start: i64, stop: i64) -> (usize, usize) {
    if len == 0 {
        return (0, 0);
    }
    let signed_len = len as i64;
    let normalized_start = if start < 0 {
        (signed_len + start).max(0)
    } else {
        start
    };
    let normalized_stop = if stop < 0 {
        (signed_len + stop).max(0)
    } else {
        stop
    };
    if normalized_start > normalized_stop || normalized_start >= signed_len {
        return (0, 0);
    }
    let start_idx = normalized_start as usize;
    let stop_idx = (normalized_stop as usize + 1).min(len);
    (start_idx, stop_idx)
}

/// Internal in-memory storage holding keys, values, expirations, and data structures.
#[derive(Debug, Default)]
pub struct FakeStore {
    strings: HashMap<String, Vec<u8>>,
    hashes: HashMap<String, HashMap<String, String>>,
    lists: HashMap<String, VecDeque<String>>,
    sets: HashMap<String, HashSet<String>>,
    sorted_sets: HashMap<String, Vec<(f64, String)>>,
    expirations: HashMap<String, Instant>,
}

impl FakeStore {
    /// Creates a new, empty in-memory store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Checks if a key has expired.
    fn is_expired(&self, key: &str) -> bool {
        if let Some(expiry) = self.expirations.get(key) {
            Instant::now() >= *expiry
        } else {
            false
        }
    }

    /// Prunes expired keys.
    fn check_expiration(&mut self, key: &str) {
        if self.is_expired(key) {
            self.del(key);
        }
    }
}

impl ScriptContext for FakeStore {
    fn get(&self, key: &str) -> Option<String> {
        if self.is_expired(key) {
            return None;
        }
        if let Some(bytes) = self.strings.get(key) {
            return String::from_utf8(bytes.clone()).ok();
        }
        None
    }

    fn set(&mut self, key: &str, value: &str) {
        self.strings
            .insert(key.to_string(), value.as_bytes().to_vec());
        self.expirations.remove(key);
    }

    fn set_ex(&mut self, key: &str, value: &str, seconds: u64) {
        self.strings
            .insert(key.to_string(), value.as_bytes().to_vec());
        let expiry = Instant::now() + Duration::from_secs(seconds);
        self.expirations.insert(key.to_string(), expiry);
    }

    fn del(&mut self, key: &str) -> bool {
        self.expirations.remove(key);
        let mut removed = false;
        if self.strings.remove(key).is_some() {
            removed = true;
        }
        if self.hashes.remove(key).is_some() {
            removed = true;
        }
        if self.lists.remove(key).is_some() {
            removed = true;
        }
        if self.sets.remove(key).is_some() {
            removed = true;
        }
        if self.sorted_sets.remove(key).is_some() {
            removed = true;
        }
        removed
    }

    fn exists(&self, key: &str) -> bool {
        if self.is_expired(key) {
            return false;
        }
        self.strings.contains_key(key)
            || self.hashes.contains_key(key)
            || self.lists.contains_key(key)
            || self.sets.contains_key(key)
            || self.sorted_sets.contains_key(key)
    }

    fn incr_by(&mut self, key: &str, amount: i64) -> i64 {
        self.check_expiration(key);
        let current_val = if let Some(bytes) = self.strings.get(key) {
            String::from_utf8_lossy(bytes).parse::<i64>().unwrap_or(0)
        } else {
            0
        };

        let new_val = current_val + amount;
        self.strings
            .insert(key.to_string(), new_val.to_string().into_bytes());
        new_val
    }

    fn expire(&mut self, key: &str, seconds: u64) -> bool {
        self.check_expiration(key);
        if self.exists(key) {
            let expiry = Instant::now() + Duration::from_secs(seconds);
            self.expirations.insert(key.to_string(), expiry);
            true
        } else {
            false
        }
    }

    fn ttl(&self, key: &str) -> i64 {
        if !self.exists(key) {
            return -2;
        }
        if let Some(expiry) = self.expirations.get(key) {
            let now = Instant::now();
            if now >= *expiry {
                -2
            } else {
                expiry.duration_since(now).as_secs() as i64
            }
        } else {
            -1
        }
    }
}

/// In-memory, thread-safe test double simulating Redis operations without external dependencies.
#[derive(Clone, Debug)]
pub struct RedisFake {
    store: Arc<RwLock<FakeStore>>,
    broadcast_sender: Arc<broadcast::Sender<RedisMessage>>,
}

impl Default for RedisFake {
    fn default() -> Self {
        Self::new()
    }
}

impl RedisFake {
    /// Creates a new in-memory Redis test double.
    pub fn new() -> Self {
        let (broadcast_sender, _) = broadcast::channel(1024);
        Self {
            store: Arc::new(RwLock::new(FakeStore::new())),
            broadcast_sender: Arc::new(broadcast_sender),
        }
    }

    /// Provides access to the underlying storage lock for advanced test inspections.
    pub fn store(&self) -> &Arc<RwLock<FakeStore>> {
        &self.store
    }

    /// Publishes a message directly through the in-memory pub/sub broker.
    pub async fn publish(&self, channel: &str, payload: &str) -> Result<usize, RedisError> {
        let message = RedisMessage::new(channel, None, payload, payload.as_bytes().to_vec());

        let subscriber_count = self.broadcast_sender.receiver_count();
        let _ = self.broadcast_sender.send(message);
        Ok(subscriber_count)
    }

    /// Spawns an isolated subscriber attached to this in-memory instance.
    pub fn subscriber(&self) -> RedisSubscriber {
        RedisSubscriber::from_broadcast_sender(Arc::clone(&self.broadcast_sender))
    }

    /// Creates an isolated subscriber stream listening for channel messages.
    pub async fn subscribe(&self, channel: &str) -> Result<MessageStream, RedisError> {
        let subscriber = self.subscriber();
        subscriber.subscribe(channel).await
    }

    /// Creates an isolated subscriber stream listening for wildcard pattern messages.
    pub async fn psubscribe(&self, pattern: &str) -> Result<MessageStream, RedisError> {
        let subscriber = self.subscriber();
        subscriber.psubscribe(pattern).await
    }

    /// Spawns a new [`RedisPipeline`] to batch commands for in-memory execution.
    pub fn pipeline(&self) -> RedisPipeline {
        RedisPipeline::new()
    }

    /// Spawns a new [`RedisTransaction`] block for atomic in-memory execution.
    pub fn transaction(&self) -> RedisTransaction {
        RedisTransaction::new()
    }

    /// Executes a pipeline or transaction atomically against the in-memory store.
    pub async fn execute_pipeline(
        &self,
        pipeline: &RedisPipeline,
    ) -> Result<Vec<ScriptOutput>, RedisError> {
        let mut store = self.store.write().await;
        let mut outputs = Vec::new();

        for op in pipeline.operations() {
            match op {
                PipelineOp::Get(key) => {
                    store.check_expiration(key);
                    let output = match store.strings.get(key) {
                        Some(bytes) => {
                            ScriptOutput::String(String::from_utf8_lossy(bytes).to_string())
                        }
                        None => ScriptOutput::Nil,
                    };
                    outputs.push(output);
                }
                PipelineOp::Set(key, value) => {
                    store.set(key, value);
                    outputs.push(ScriptOutput::String("OK".to_string()));
                }
                PipelineOp::SetEx(key, value, seconds) => {
                    store.set_ex(key, value, *seconds);
                    outputs.push(ScriptOutput::String("OK".to_string()));
                }
                PipelineOp::SetNx(key, value) => {
                    store.check_expiration(key);
                    if store.strings.contains_key(key) {
                        outputs.push(ScriptOutput::Integer(0));
                    } else {
                        store.set(key, value);
                        outputs.push(ScriptOutput::Integer(1));
                    }
                }
                PipelineOp::Del(key) => {
                    let removed = store.del(key);
                    outputs.push(ScriptOutput::Integer(if removed { 1 } else { 0 }));
                }
                PipelineOp::Exists(key) => {
                    let exists = store.exists(key);
                    outputs.push(ScriptOutput::Integer(if exists { 1 } else { 0 }));
                }
                PipelineOp::IncrBy(key, amount) => {
                    let new_val = store.incr_by(key, *amount);
                    outputs.push(ScriptOutput::Integer(new_val));
                }
                PipelineOp::HGet(key, field) => {
                    let output = store
                        .hashes
                        .get(key)
                        .and_then(|h| h.get(field))
                        .map(|v| ScriptOutput::String(v.clone()))
                        .unwrap_or(ScriptOutput::Nil);
                    outputs.push(output);
                }
                PipelineOp::HSet(key, field, value) => {
                    let is_new = store
                        .hashes
                        .entry(key.clone())
                        .or_default()
                        .insert(field.clone(), value.clone())
                        .is_none();
                    outputs.push(ScriptOutput::Integer(if is_new { 1 } else { 0 }));
                }
                PipelineOp::HDel(key, field) => {
                    let removed = store
                        .hashes
                        .get_mut(key)
                        .map(|h| h.remove(field).is_some())
                        .unwrap_or(false);
                    outputs.push(ScriptOutput::Integer(if removed { 1 } else { 0 }));
                }
                PipelineOp::LPush(key, elements) => {
                    let list = store.lists.entry(key.clone()).or_default();
                    for elem in elements {
                        list.push_front(elem.clone());
                    }
                    outputs.push(ScriptOutput::Integer(list.len() as i64));
                }
                PipelineOp::RPush(key, elements) => {
                    let list = store.lists.entry(key.clone()).or_default();
                    for elem in elements {
                        list.push_back(elem.clone());
                    }
                    outputs.push(ScriptOutput::Integer(list.len() as i64));
                }
                PipelineOp::SAdd(key, members) => {
                    let set = store.sets.entry(key.clone()).or_default();
                    let mut added = 0;
                    for member in members {
                        if set.insert(member.clone()) {
                            added += 1;
                        }
                    }
                    outputs.push(ScriptOutput::Integer(added));
                }
                PipelineOp::ZAdd(key, score, member) => {
                    let zset = store.sorted_sets.entry(key.clone()).or_default();
                    let mut is_new = true;
                    if let Some(pos) = zset.iter().position(|(_, m)| m == member) {
                        zset.remove(pos);
                        is_new = false;
                    }
                    zset.push((*score, member.clone()));
                    zset.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                    outputs.push(ScriptOutput::Integer(if is_new { 1 } else { 0 }));
                }
            }
        }

        Ok(outputs)
    }

    /// Commits a transaction block atomically against the in-memory store.
    pub async fn commit_transaction(
        &self,
        transaction: &RedisTransaction,
    ) -> Result<Vec<ScriptOutput>, RedisError> {
        let mut pipeline = RedisPipeline::new();
        for op in transaction.operations() {
            match op {
                PipelineOp::Get(k) => {
                    pipeline.get(k);
                }
                PipelineOp::Set(k, v) => {
                    pipeline.set(k, v);
                }
                PipelineOp::SetEx(k, v, s) => {
                    pipeline.set_ex(k, v, *s);
                }
                PipelineOp::SetNx(k, v) => {
                    pipeline.set_nx(k, v);
                }
                PipelineOp::Del(k) => {
                    pipeline.del(k);
                }
                PipelineOp::Exists(k) => {
                    pipeline.exists(k);
                }
                PipelineOp::IncrBy(k, a) => {
                    pipeline.incr_by(k, *a);
                }
                PipelineOp::HGet(k, f) => {
                    pipeline.hget(k, f);
                }
                PipelineOp::HSet(k, f, v) => {
                    pipeline.hset(k, f, v);
                }
                PipelineOp::HDel(k, f) => {
                    pipeline.hdel(k, f);
                }
                PipelineOp::LPush(k, items) => {
                    let slices = items.iter().map(|s| s.as_str()).collect::<Vec<_>>();
                    pipeline.lpush(k, &slices);
                }
                PipelineOp::RPush(k, items) => {
                    let slices = items.iter().map(|s| s.as_str()).collect::<Vec<_>>();
                    pipeline.rpush(k, &slices);
                }
                PipelineOp::SAdd(k, items) => {
                    let slices = items.iter().map(|s| s.as_str()).collect::<Vec<_>>();
                    pipeline.sadd(k, &slices);
                }
                PipelineOp::ZAdd(k, score, m) => {
                    pipeline.zadd(k, *score, m);
                }
            }
        }

        self.execute_pipeline(&pipeline).await
    }
}

impl RedisCommands for RedisFake {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<String>, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            store.check_expiration(key);
            Ok(store
                .strings
                .get(key)
                .and_then(|b| String::from_utf8(b.clone()).ok()))
        })
    }

    fn get_bytes<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            store.check_expiration(key);
            Ok(store.strings.get(key).cloned())
        })
    }

    fn set<'a>(&'a self, key: &'a str, value: &'a str) -> BoxFuture<'a, Result<(), RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            store.set(key, value);
            Ok(())
        })
    }

    fn set_bytes<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
    ) -> BoxFuture<'a, Result<(), RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            store.strings.insert(key.to_string(), value.to_vec());
            store.expirations.remove(key);
            Ok(())
        })
    }

    fn set_ex<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
        seconds: u64,
    ) -> BoxFuture<'a, Result<(), RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            store.set_ex(key, value, seconds);
            Ok(())
        })
    }

    fn set_bytes_ex<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        seconds: u64,
    ) -> BoxFuture<'a, Result<(), RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            store.strings.insert(key.to_string(), value.to_vec());
            let expiry = Instant::now() + Duration::from_secs(seconds);
            store.expirations.insert(key.to_string(), expiry);
            Ok(())
        })
    }

    fn set_nx<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            store.check_expiration(key);
            if store.strings.contains_key(key) {
                Ok(false)
            } else {
                store.set(key, value);
                Ok(true)
            }
        })
    }

    fn del<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            store.check_expiration(key);
            Ok(store.del(key))
        })
    }

    fn del_many<'a>(&'a self, keys: &'a [&'a str]) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            if keys.is_empty() {
                return Ok(0);
            }
            let mut store = self.store.write().await;
            let mut count = 0;
            for key in keys {
                store.check_expiration(key);
                if store.del(key) {
                    count += 1;
                }
            }
            Ok(count)
        })
    }

    fn exists<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            store.check_expiration(key);
            Ok(store.exists(key))
        })
    }

    fn expire<'a>(&'a self, key: &'a str, seconds: u64) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            Ok(store.expire(key, seconds))
        })
    }

    fn ttl<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<i64, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            store.check_expiration(key);
            Ok(store.ttl(key))
        })
    }

    fn incr_by<'a>(&'a self, key: &'a str, amount: i64) -> BoxFuture<'a, Result<i64, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            Ok(store.incr_by(key, amount))
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
            let mut store = self.store.write().await;
            let mut results = Vec::new();
            for key in keys {
                store.check_expiration(key);
                let val = store
                    .strings
                    .get(*key)
                    .and_then(|b| String::from_utf8(b.clone()).ok());
                results.push(val);
            }
            Ok(results)
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
            let mut store = self.store.write().await;
            for (key, val) in pairs {
                store.set(key, val);
            }
            Ok(())
        })
    }

    fn hget<'a>(
        &'a self,
        key: &'a str,
        field: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>, RedisError>> {
        Box::pin(async move {
            let store = self.store.read().await;
            Ok(store.hashes.get(key).and_then(|h| h.get(field).cloned()))
        })
    }

    fn hset<'a>(
        &'a self,
        key: &'a str,
        field: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            let is_new = store
                .hashes
                .entry(key.to_string())
                .or_default()
                .insert(field.to_string(), value.to_string())
                .is_none();
            Ok(is_new)
        })
    }

    fn hdel<'a>(&'a self, key: &'a str, field: &'a str) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            let removed = store
                .hashes
                .get_mut(key)
                .map(|h| h.remove(field).is_some())
                .unwrap_or(false);
            Ok(removed)
        })
    }

    fn hexists<'a>(
        &'a self,
        key: &'a str,
        field: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let store = self.store.read().await;
            Ok(store
                .hashes
                .get(key)
                .map(|h| h.contains_key(field))
                .unwrap_or(false))
        })
    }

    fn hget_all<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<HashMap<String, String>, RedisError>> {
        Box::pin(async move {
            let store = self.store.read().await;
            Ok(store.hashes.get(key).cloned().unwrap_or_default())
        })
    }

    fn hincr_by<'a>(
        &'a self,
        key: &'a str,
        field: &'a str,
        amount: i64,
    ) -> BoxFuture<'a, Result<i64, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            let hash = store.hashes.entry(key.to_string()).or_default();
            let current = hash
                .get(field)
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0);
            let updated = current + amount;
            hash.insert(field.to_string(), updated.to_string());
            Ok(updated)
        })
    }

    fn hlen<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let store = self.store.read().await;
            Ok(store.hashes.get(key).map(|h| h.len() as u64).unwrap_or(0))
        })
    }

    fn hkeys<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<String>, RedisError>> {
        Box::pin(async move {
            let store = self.store.read().await;
            Ok(store
                .hashes
                .get(key)
                .map(|h| h.keys().cloned().collect())
                .unwrap_or_default())
        })
    }

    fn hvals<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<String>, RedisError>> {
        Box::pin(async move {
            let store = self.store.read().await;
            Ok(store
                .hashes
                .get(key)
                .map(|h| h.values().cloned().collect())
                .unwrap_or_default())
        })
    }

    fn lpush<'a>(
        &'a self,
        key: &'a str,
        elements: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            let list = store.lists.entry(key.to_string()).or_default();
            for elem in elements {
                list.push_front((*elem).to_string());
            }
            Ok(list.len() as u64)
        })
    }

    fn rpush<'a>(
        &'a self,
        key: &'a str,
        elements: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            let list = store.lists.entry(key.to_string()).or_default();
            for elem in elements {
                list.push_back((*elem).to_string());
            }
            Ok(list.len() as u64)
        })
    }

    fn lpop<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<String>, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            Ok(store.lists.get_mut(key).and_then(|l| l.pop_front()))
        })
    }

    fn rpop<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<String>, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            Ok(store.lists.get_mut(key).and_then(|l| l.pop_back()))
        })
    }

    fn llen<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let store = self.store.read().await;
            Ok(store.lists.get(key).map(|l| l.len() as u64).unwrap_or(0))
        })
    }

    fn lrange<'a>(
        &'a self,
        key: &'a str,
        start: i64,
        stop: i64,
    ) -> BoxFuture<'a, Result<Vec<String>, RedisError>> {
        Box::pin(async move {
            let store = self.store.read().await;
            let list = match store.lists.get(key) {
                Some(l) => l,
                None => return Ok(Vec::new()),
            };

            let (start_idx, stop_idx) = normalize_redis_range(list.len(), start, stop);
            let items = list
                .range(start_idx..stop_idx)
                .cloned()
                .collect::<Vec<String>>();
            Ok(items)
        })
    }

    fn sadd<'a>(
        &'a self,
        key: &'a str,
        members: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            let set = store.sets.entry(key.to_string()).or_default();
            let mut added = 0;
            for member in members {
                if set.insert((*member).to_string()) {
                    added += 1;
                }
            }
            Ok(added)
        })
    }

    fn srem<'a>(
        &'a self,
        key: &'a str,
        members: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            let set = match store.sets.get_mut(key) {
                Some(s) => s,
                None => return Ok(0),
            };

            let mut removed = 0;
            for member in members {
                if set.remove(*member) {
                    removed += 1;
                }
            }
            Ok(removed)
        })
    }

    fn smembers<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<HashSet<String>, RedisError>> {
        Box::pin(async move {
            let store = self.store.read().await;
            Ok(store.sets.get(key).cloned().unwrap_or_default())
        })
    }

    fn sismember<'a>(
        &'a self,
        key: &'a str,
        member: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let store = self.store.read().await;
            Ok(store
                .sets
                .get(key)
                .map(|s| s.contains(member))
                .unwrap_or(false))
        })
    }

    fn scard<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let store = self.store.read().await;
            Ok(store.sets.get(key).map(|s| s.len() as u64).unwrap_or(0))
        })
    }

    fn zadd<'a>(
        &'a self,
        key: &'a str,
        score: f64,
        member: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            let zset = store.sorted_sets.entry(key.to_string()).or_default();
            let mut is_new = true;
            if let Some(pos) = zset.iter().position(|(_, m)| m == member) {
                zset.remove(pos);
                is_new = false;
            }
            zset.push((score, member.to_string()));
            zset.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            Ok(is_new)
        })
    }

    fn zrem<'a>(
        &'a self,
        key: &'a str,
        members: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            let zset = match store.sorted_sets.get_mut(key) {
                Some(z) => z,
                None => return Ok(0),
            };

            let initial_len = zset.len();
            zset.retain(|(_, m)| !members.contains(&m.as_str()));
            Ok((initial_len - zset.len()) as u64)
        })
    }

    fn zscore<'a>(
        &'a self,
        key: &'a str,
        member: &'a str,
    ) -> BoxFuture<'a, Result<Option<f64>, RedisError>> {
        Box::pin(async move {
            let store = self.store.read().await;
            Ok(store
                .sorted_sets
                .get(key)
                .and_then(|z| z.iter().find(|(_, m)| m == member).map(|(s, _)| *s)))
        })
    }

    fn zcard<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let store = self.store.read().await;
            Ok(store
                .sorted_sets
                .get(key)
                .map(|z| z.len() as u64)
                .unwrap_or(0))
        })
    }

    fn zcount<'a>(
        &'a self,
        key: &'a str,
        min: f64,
        max: f64,
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let store = self.store.read().await;
            Ok(store
                .sorted_sets
                .get(key)
                .map(|z| z.iter().filter(|(s, _)| *s >= min && *s <= max).count() as u64)
                .unwrap_or(0))
        })
    }

    fn zrange<'a>(
        &'a self,
        key: &'a str,
        start: i64,
        stop: i64,
    ) -> BoxFuture<'a, Result<Vec<String>, RedisError>> {
        Box::pin(async move {
            let store = self.store.read().await;
            let zset = match store.sorted_sets.get(key) {
                Some(z) => z,
                None => return Ok(Vec::new()),
            };

            let (start_idx, stop_idx) = normalize_redis_range(zset.len(), start, stop);
            let items = zset[start_idx..stop_idx]
                .iter()
                .map(|(_, m)| m.clone())
                .collect();
            Ok(items)
        })
    }

    fn zremrangebyscore<'a>(
        &'a self,
        key: &'a str,
        min: f64,
        max: f64,
    ) -> BoxFuture<'a, Result<u64, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            let zset = match store.sorted_sets.get_mut(key) {
                Some(z) => z,
                None => return Ok(0),
            };

            let initial_len = zset.len();
            zset.retain(|(s, _)| *s < min || *s > max);
            Ok((initial_len - zset.len()) as u64)
        })
    }

    fn ping(&self) -> BoxFuture<'_, Result<String, RedisError>> {
        Box::pin(async move { Ok("PONG".to_string()) })
    }

    fn flushdb(&self) -> BoxFuture<'_, Result<(), RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            store.strings.clear();
            store.hashes.clear();
            store.lists.clear();
            store.sets.clear();
            store.sorted_sets.clear();
            store.expirations.clear();
            Ok(())
        })
    }

    fn keys<'a>(&'a self, pattern: &'a str) -> BoxFuture<'a, Result<Vec<String>, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            let mut matched = Vec::new();

            let string_keys = store.strings.keys().cloned().collect::<Vec<_>>();
            for key in string_keys {
                store.check_expiration(&key);
                if store.strings.contains_key(&key) && crate::pubsub::wildcard_match(pattern, &key)
                {
                    matched.push(key);
                }
            }

            for key in store.hashes.keys() {
                if crate::pubsub::wildcard_match(pattern, key) && !matched.contains(key) {
                    matched.push(key.clone());
                }
            }

            for key in store.lists.keys() {
                if crate::pubsub::wildcard_match(pattern, key) && !matched.contains(key) {
                    matched.push(key.clone());
                }
            }

            for key in store.sets.keys() {
                if crate::pubsub::wildcard_match(pattern, key) && !matched.contains(key) {
                    matched.push(key.clone());
                }
            }

            for key in store.sorted_sets.keys() {
                if crate::pubsub::wildcard_match(pattern, key) && !matched.contains(key) {
                    matched.push(key.clone());
                }
            }

            Ok(matched)
        })
    }

    fn eval_script<'a>(
        &'a self,
        script: &'a RedisScript,
        keys: &'a [&'a str],
        args: &'a [&'a str],
    ) -> BoxFuture<'a, Result<ScriptOutput, RedisError>> {
        Box::pin(async move {
            let mut store = self.store.write().await;
            script.run_simulation(keys, args, &mut *store)
        })
    }
}
