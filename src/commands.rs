//! Unified, asynchronous command contract implemented by live connections, pipelines,
//! transaction blocks, and the in-memory test double.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
};

use serde::{Serialize, de::DeserializeOwned};

use crate::{
    error::RedisError,
    script::{RedisScript, ScriptOutput},
};

/// A pinned, heap-allocated future that is thread-safe across executors.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Unified asynchronous command contract for key-value, hash, list, set, sorted set,
/// server administration, and Lua script execution.
pub trait RedisCommands: Send + Sync {
    // ------------------------------------------------------------------------
    // String & Key Primitives
    // ------------------------------------------------------------------------

    /// Retrieves the string value stored at the given key.
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<String>, RedisError>>;

    /// Retrieves raw binary bytes stored at the given key.
    fn get_bytes<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, RedisError>>;

    /// Stores a string value at the given key with no expiration.
    fn set<'a>(&'a self, key: &'a str, value: &'a str) -> BoxFuture<'a, Result<(), RedisError>>;

    /// Stores raw binary bytes at the given key with no expiration.
    fn set_bytes<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
    ) -> BoxFuture<'a, Result<(), RedisError>>;

    /// Stores a string value at the given key with a time-to-live expiration in seconds.
    fn set_ex<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
        seconds: u64,
    ) -> BoxFuture<'a, Result<(), RedisError>>;

    /// Stores raw binary bytes at the given key with a time-to-live expiration in seconds.
    fn set_bytes_ex<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        seconds: u64,
    ) -> BoxFuture<'a, Result<(), RedisError>>;

    /// Stores a string value at the key only if the key does not already exist.
    /// Returns `true` if the key was set, `false` if it already existed.
    fn set_nx<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>>;

    /// Deletes a key from the database. Returns `true` if the key was removed.
    fn del<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<bool, RedisError>>;

    /// Deletes multiple keys from the database. Returns the count of keys that were removed.
    fn del_many<'a>(&'a self, keys: &'a [&'a str]) -> BoxFuture<'a, Result<u64, RedisError>>;

    /// Returns whether the specified key exists in the database.
    fn exists<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<bool, RedisError>>;

    /// Configures an expiration timeout on the given key in seconds.
    fn expire<'a>(&'a self, key: &'a str, seconds: u64) -> BoxFuture<'a, Result<bool, RedisError>>;

    /// Returns the remaining time-to-live of a key in seconds.
    /// Returns `-2` if the key does not exist, and `-1` if the key exists with no expiration.
    fn ttl<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<i64, RedisError>>;

    /// Increments the integer value stored at key by one.
    fn incr<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<i64, RedisError>> {
        self.incr_by(key, 1)
    }

    /// Increments the integer value stored at key by the given integer amount.
    fn incr_by<'a>(&'a self, key: &'a str, amount: i64) -> BoxFuture<'a, Result<i64, RedisError>>;

    /// Decrements the integer value stored at key by one.
    fn decr<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<i64, RedisError>> {
        self.decr_by(key, 1)
    }

    /// Decrements the integer value stored at key by the given integer amount.
    fn decr_by<'a>(&'a self, key: &'a str, amount: i64) -> BoxFuture<'a, Result<i64, RedisError>> {
        self.incr_by(key, -amount)
    }

    /// Retrieves the string values associated with multiple keys in a single operation.
    fn mget<'a>(
        &'a self,
        keys: &'a [&'a str],
    ) -> BoxFuture<'a, Result<Vec<Option<String>>, RedisError>>;

    /// Sets multiple key-value pairs simultaneously in a single operation.
    fn mset<'a>(&'a self, pairs: &'a [(&'a str, &'a str)])
    -> BoxFuture<'a, Result<(), RedisError>>;

    // ------------------------------------------------------------------------
    // Typed JSON Helpers
    // ------------------------------------------------------------------------

    /// Retrieves and deserializes a JSON payload stored at the specified key.
    fn get_json<'a, Target: DeserializeOwned + Send + 'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Target>, RedisError>> {
        Box::pin(async move {
            match self.get(key).await? {
                Some(raw) => serde_json::from_str(&raw)
                    .map(Some)
                    .map_err(RedisError::from),
                None => Ok(None),
            }
        })
    }

    /// Serializes a struct to JSON and stores it at the specified key with no expiration.
    fn set_json<'a, Target: Serialize + Sync + 'a>(
        &'a self,
        key: &'a str,
        value: &'a Target,
    ) -> BoxFuture<'a, Result<(), RedisError>> {
        Box::pin(async move {
            let serialized = serde_json::to_string(value)?;
            self.set(key, &serialized).await
        })
    }

    /// Serializes a struct to JSON and stores it at the specified key with an expiration in seconds.
    fn set_json_ex<'a, Target: Serialize + Sync + 'a>(
        &'a self,
        key: &'a str,
        value: &'a Target,
        seconds: u64,
    ) -> BoxFuture<'a, Result<(), RedisError>> {
        Box::pin(async move {
            let serialized = serde_json::to_string(value)?;
            self.set_ex(key, &serialized, seconds).await
        })
    }

    // ------------------------------------------------------------------------
    // Hash Operations
    // ------------------------------------------------------------------------

    /// Retrieves a single field from a hash stored at key.
    fn hget<'a>(
        &'a self,
        key: &'a str,
        field: &'a str,
    ) -> BoxFuture<'a, Result<Option<String>, RedisError>>;

    /// Sets a field in a hash stored at key to the given value. Returns `true` if field is new.
    fn hset<'a>(
        &'a self,
        key: &'a str,
        field: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>>;

    /// Removes a field from a hash stored at key. Returns `true` if the field was removed.
    fn hdel<'a>(&'a self, key: &'a str, field: &'a str) -> BoxFuture<'a, Result<bool, RedisError>>;

    /// Returns whether the specified field exists in the hash stored at key.
    fn hexists<'a>(
        &'a self,
        key: &'a str,
        field: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>>;

    /// Returns all field-value pairs from the hash stored at key.
    fn hget_all<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<HashMap<String, String>, RedisError>>;

    /// Increments the integer value of a hash field by the specified amount.
    fn hincr_by<'a>(
        &'a self,
        key: &'a str,
        field: &'a str,
        amount: i64,
    ) -> BoxFuture<'a, Result<i64, RedisError>>;

    /// Returns the number of fields in the hash stored at key.
    fn hlen<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<u64, RedisError>>;

    /// Returns all field names in the hash stored at key.
    fn hkeys<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<String>, RedisError>>;

    /// Returns all values in the hash stored at key.
    fn hvals<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<String>, RedisError>>;

    // ------------------------------------------------------------------------
    // List Operations
    // ------------------------------------------------------------------------

    /// Prepend one or more elements to a list. Returns the length of the list after the push.
    fn lpush<'a>(
        &'a self,
        key: &'a str,
        elements: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>>;

    /// Append one or more elements to a list. Returns the length of the list after the push.
    fn rpush<'a>(
        &'a self,
        key: &'a str,
        elements: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>>;

    /// Removes and returns the first element of a list.
    fn lpop<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<String>, RedisError>>;

    /// Removes and returns the last element of a list.
    fn rpop<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<String>, RedisError>>;

    /// Returns the length of the list stored at key.
    fn llen<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<u64, RedisError>>;

    /// Returns a range of elements from the list between `start` and `stop` offsets.
    fn lrange<'a>(
        &'a self,
        key: &'a str,
        start: i64,
        stop: i64,
    ) -> BoxFuture<'a, Result<Vec<String>, RedisError>>;

    // ------------------------------------------------------------------------
    // Set Operations
    // ------------------------------------------------------------------------

    /// Adds one or more members to a set. Returns the count of newly added elements.
    fn sadd<'a>(
        &'a self,
        key: &'a str,
        members: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>>;

    /// Removes one or more members from a set. Returns the count of removed elements.
    fn srem<'a>(
        &'a self,
        key: &'a str,
        members: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>>;

    /// Returns all members in the set stored at key.
    fn smembers<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<HashSet<String>, RedisError>>;

    /// Returns whether the specified member exists in the set stored at key.
    fn sismember<'a>(
        &'a self,
        key: &'a str,
        member: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>>;

    /// Returns the cardinality (count of elements) of the set stored at key.
    fn scard<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<u64, RedisError>>;

    // ------------------------------------------------------------------------
    // Sorted Set Operations
    // ------------------------------------------------------------------------

    /// Adds a member with the specified score to a sorted set. Returns `true` if newly added.
    fn zadd<'a>(
        &'a self,
        key: &'a str,
        score: f64,
        member: &'a str,
    ) -> BoxFuture<'a, Result<bool, RedisError>>;

    /// Removes one or more members from a sorted set. Returns the count of removed elements.
    fn zrem<'a>(
        &'a self,
        key: &'a str,
        members: &'a [&'a str],
    ) -> BoxFuture<'a, Result<u64, RedisError>>;

    /// Retrieves the score of a member in a sorted set, if it exists.
    fn zscore<'a>(
        &'a self,
        key: &'a str,
        member: &'a str,
    ) -> BoxFuture<'a, Result<Option<f64>, RedisError>>;

    /// Returns the cardinality of a sorted set.
    fn zcard<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<u64, RedisError>>;

    /// Returns the count of elements in a sorted set with scores within the given range.
    fn zcount<'a>(
        &'a self,
        key: &'a str,
        min: f64,
        max: f64,
    ) -> BoxFuture<'a, Result<u64, RedisError>>;

    /// Returns a range of members ordered by score from index `start` to `stop`.
    fn zrange<'a>(
        &'a self,
        key: &'a str,
        start: i64,
        stop: i64,
    ) -> BoxFuture<'a, Result<Vec<String>, RedisError>>;

    /// Removes all members in a sorted set with scores between `min` and `max`.
    fn zremrangebyscore<'a>(
        &'a self,
        key: &'a str,
        min: f64,
        max: f64,
    ) -> BoxFuture<'a, Result<u64, RedisError>>;

    // ------------------------------------------------------------------------
    // Server & Key Management
    // ------------------------------------------------------------------------

    /// Pings the server to verify connectivity, returning the pong response string.
    fn ping(&self) -> BoxFuture<'_, Result<String, RedisError>>;

    /// Flushes all keys from the currently active database.
    fn flushdb(&self) -> BoxFuture<'_, Result<(), RedisError>>;

    /// Searches for keys matching the glob pattern.
    fn keys<'a>(&'a self, pattern: &'a str) -> BoxFuture<'a, Result<Vec<String>, RedisError>>;

    // ------------------------------------------------------------------------
    // Lua Script Evaluation
    // ------------------------------------------------------------------------

    /// Evaluates a Lua script via `EVALSHA` using its pre-computed SHA-1 digest,
    /// transparently falling back to `EVAL` if the server returns `NOSCRIPT`.
    fn eval_script<'a>(
        &'a self,
        script: &'a RedisScript,
        keys: &'a [&'a str],
        args: &'a [&'a str],
    ) -> BoxFuture<'a, Result<ScriptOutput, RedisError>>;
}
