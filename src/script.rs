//! Strongly typed Lua script modeling, SHA-1 digestion, and test simulation support.

use std::{fmt, sync::Arc};

use serde::de::DeserializeOwned;
use sha1::{Digest, Sha1};

use crate::error::RedisError;

/// Context interface provided to in-memory script simulation handlers during testing.
pub trait ScriptContext: Send + Sync {
    /// Retrieves a string value for the given key, if it exists and has not expired.
    fn get(&self, key: &str) -> Option<String>;
    /// Sets a string value for the given key without expiration.
    fn set(&mut self, key: &str, value: &str);
    /// Sets a string value for the given key with an expiration in seconds.
    fn set_ex(&mut self, key: &str, value: &str, seconds: u64);
    /// Deletes a key, returning whether the key existed.
    fn del(&mut self, key: &str) -> bool;
    /// Returns whether the specified key exists and is unexpired.
    fn exists(&self, key: &str) -> bool;
    /// Increments an integer counter stored at key by the given amount.
    fn incr_by(&mut self, key: &str, amount: i64) -> i64;
    /// Configures an expiration in seconds on the key.
    fn expire(&mut self, key: &str, seconds: u64) -> bool;
    /// Returns the remaining time-to-live in seconds, or -2 if missing, -1 if no TTL.
    fn ttl(&self, key: &str) -> i64;
}

/// Closure type representing an in-memory script simulation for [`crate::fake::RedisFake`].
pub type ScriptSimulation = Arc<
    dyn Fn(&[&str], &[&str], &mut dyn ScriptContext) -> Result<ScriptOutput, RedisError>
        + Send
        + Sync,
>;

/// Encapsulation of a Redis Lua script with pre-computed SHA-1 digest and optional test simulation.
#[derive(Clone)]
pub struct RedisScript {
    source: String,
    sha1: String,
    simulation: Option<ScriptSimulation>,
}

impl fmt::Debug for RedisScript {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisScript")
            .field("sha1", &self.sha1)
            .field("has_simulation", &self.simulation.is_some())
            .finish()
    }
}

impl RedisScript {
    /// Creates a new script instance, computing the SHA-1 digest of the provided Lua source code.
    pub fn new(source: impl Into<String>) -> Self {
        let source_string = source.into();
        let mut hasher = Sha1::new();
        hasher.update(source_string.as_bytes());
        let digest = hasher.finalize();
        let sha1 = digest
            .iter()
            .map(|byte| format!("{:02x}", byte))
            .collect::<String>();

        Self {
            source: source_string,
            sha1,
            simulation: None,
        }
    }

    /// Attaches an in-memory test simulation handler for executing this script against [`crate::fake::RedisFake`].
    pub fn with_simulation<Function>(mut self, handler: Function) -> Self
    where
        Function: Fn(&[&str], &[&str], &mut dyn ScriptContext) -> Result<ScriptOutput, RedisError>
            + Send
            + Sync
            + 'static,
    {
        self.simulation = Some(Arc::new(handler));
        self
    }

    /// Returns the raw Lua source code.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Returns the pre-computed 40-character hexadecimal SHA-1 digest.
    pub fn sha1(&self) -> &str {
        &self.sha1
    }

    /// Returns whether this script has a test simulation closure configured.
    pub fn has_simulation(&self) -> bool {
        self.simulation.is_some()
    }

    /// Executes the simulation handler against the provided [`ScriptContext`].
    pub fn run_simulation(
        &self,
        keys: &[&str],
        args: &[&str],
        context: &mut dyn ScriptContext,
    ) -> Result<ScriptOutput, RedisError> {
        if let Some(ref simulation) = self.simulation {
            simulation(keys, args, context)
        } else {
            Err(RedisError::script(format!(
                "No simulation handler registered for script '{}' on in-memory test double",
                self.sha1
            )))
        }
    }
}

/// Generic output value returned by a Redis Lua script or command evaluation.
#[derive(Clone, Debug, PartialEq)]
pub enum ScriptOutput {
    /// Integer scalar response (e.g. from `INCR` or counter computations).
    Integer(i64),
    /// String response.
    String(String),
    /// Raw binary response.
    Binary(Vec<u8>),
    /// Boolean response.
    Boolean(bool),
    /// Heterogeneous list or array of nested script outputs.
    Array(Vec<ScriptOutput>),
    /// Nil or missing response.
    Nil,
}

impl ScriptOutput {
    /// Attempts to interpret this output as an integer.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Integer(val) => Some(*val),
            Self::String(val) => val.parse::<i64>().ok(),
            Self::Boolean(true) => Some(1),
            Self::Boolean(false) => Some(0),
            _ => None,
        }
    }

    /// Attempts to interpret this output as a string slice.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(val) => Some(val.as_str()),
            _ => None,
        }
    }

    /// Attempts to interpret this output as a boolean.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(val) => Some(*val),
            Self::Integer(val) => Some(*val != 0),
            Self::String(val) => match val.as_str() {
                "1" | "true" | "TRUE" | "OK" => Some(true),
                "0" | "false" | "FALSE" => Some(false),
                _ => None,
            },
            Self::Nil => Some(false),
            _ => None,
        }
    }

    /// Consumes this output and returns the inner string if available.
    pub fn into_string(self) -> Option<String> {
        match self {
            Self::String(val) => Some(val),
            Self::Binary(val) => String::from_utf8(val).ok(),
            Self::Integer(val) => Some(val.to_string()),
            _ => None,
        }
    }

    /// Attempts to deserialize a JSON payload from this script output.
    pub fn into_json<Target: DeserializeOwned>(self) -> Result<Target, RedisError> {
        let string_payload = self.into_string().ok_or_else(|| {
            RedisError::serialization("Cannot deserialize JSON from non-string script output")
        })?;

        serde_json::from_str(&string_payload).map_err(RedisError::from)
    }

    /// Returns whether this output represents nil.
    pub fn is_nil(&self) -> bool {
        matches!(self, Self::Nil)
    }
}

impl From<i64> for ScriptOutput {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<String> for ScriptOutput {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for ScriptOutput {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<bool> for ScriptOutput {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<Vec<u8>> for ScriptOutput {
    fn from(value: Vec<u8>) -> Self {
        Self::Binary(value)
    }
}

impl From<fred::types::Value> for ScriptOutput {
    fn from(value: fred::types::Value) -> Self {
        match value {
            fred::types::Value::Integer(val) => Self::Integer(val),
            fred::types::Value::String(val) => Self::String(val.to_string()),
            fred::types::Value::Bytes(val) => Self::Binary(val.to_vec()),
            fred::types::Value::Boolean(val) => Self::Boolean(val),
            fred::types::Value::Array(items) => {
                Self::Array(items.into_iter().map(ScriptOutput::from).collect())
            }
            fred::types::Value::Null => Self::Nil,
            fred::types::Value::Double(val) => Self::String(val.to_string()),
            _ => Self::String(format!("{value:?}")),
        }
    }
}
