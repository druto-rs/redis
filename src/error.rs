//! Strongly typed error definitions for Redis operations.

use std::fmt;

/// Strongly typed errors produced during Redis client management, command execution,
/// network transport, or serialization operations.
#[derive(Debug)]
pub enum RedisError {
    /// Failure during socket connection, network I/O, or connection loss.
    Connection(String),
    /// Authentication or ACL authorization failure.
    Authentication(String),
    /// Command execution failure returned by the Redis server.
    Command(String),
    /// Violation of the Redis Serialization Protocol (RESP) or frame decoding failure.
    Protocol(String),
    /// Requested key or field was not found when strict existence was expected.
    NotFound(String),
    /// Network timeout awaiting a response from the server.
    Timeout(String),
    /// Failure serializing or deserializing payload data (e.g., JSON conversion).
    Serialization(String),
    /// Lua script compilation, evaluation, or hash calculation failure.
    Script(String),
    /// Transaction abort or conflict (e.g., watched key modified).
    Transaction(String),
    /// Invalid client or connection configuration (e.g., malformed URL).
    Configuration(String),
    /// Connection pool acquisition failure or exhausted pool resources.
    Pool(String),
}

impl RedisError {
    /// Creates a new connection error with the provided detail message.
    pub fn connection(message: impl Into<String>) -> Self {
        Self::Connection(message.into())
    }

    /// Creates a new authentication error with the provided detail message.
    pub fn authentication(message: impl Into<String>) -> Self {
        Self::Authentication(message.into())
    }

    /// Creates a new command error with the provided detail message.
    pub fn command(message: impl Into<String>) -> Self {
        Self::Command(message.into())
    }

    /// Creates a new protocol error with the provided detail message.
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol(message.into())
    }

    /// Creates a new not found error with the provided detail message.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    /// Creates a new timeout error with the provided detail message.
    pub fn timeout(message: impl Into<String>) -> Self {
        Self::Timeout(message.into())
    }

    /// Creates a new serialization error with the provided detail message.
    pub fn serialization(message: impl Into<String>) -> Self {
        Self::Serialization(message.into())
    }

    /// Creates a new script error with the provided detail message.
    pub fn script(message: impl Into<String>) -> Self {
        Self::Script(message.into())
    }

    /// Creates a new transaction error with the provided detail message.
    pub fn transaction(message: impl Into<String>) -> Self {
        Self::Transaction(message.into())
    }

    /// Creates a new configuration error with the provided detail message.
    pub fn configuration(message: impl Into<String>) -> Self {
        Self::Configuration(message.into())
    }

    /// Creates a new pool error with the provided detail message.
    pub fn pool(message: impl Into<String>) -> Self {
        Self::Pool(message.into())
    }
}

impl fmt::Display for RedisError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connection(message) => write!(formatter, "Redis connection error: {message}"),
            Self::Authentication(message) => {
                write!(formatter, "Redis authentication error: {message}")
            }
            Self::Command(message) => write!(formatter, "Redis command error: {message}"),
            Self::Protocol(message) => write!(formatter, "Redis protocol error: {message}"),
            Self::NotFound(message) => write!(formatter, "Redis key/field not found: {message}"),
            Self::Timeout(message) => write!(formatter, "Redis operation timed out: {message}"),
            Self::Serialization(message) => {
                write!(formatter, "Redis serialization error: {message}")
            }
            Self::Script(message) => write!(formatter, "Redis script error: {message}"),
            Self::Transaction(message) => write!(formatter, "Redis transaction error: {message}"),
            Self::Configuration(message) => {
                write!(formatter, "Redis configuration error: {message}")
            }
            Self::Pool(message) => write!(formatter, "Redis pool error: {message}"),
        }
    }
}

impl std::error::Error for RedisError {}

impl From<fred::error::Error> for RedisError {
    fn from(error: fred::error::Error) -> Self {
        use fred::error::ErrorKind;
        match error.kind() {
            ErrorKind::Auth => Self::Authentication(error.to_string()),
            ErrorKind::Timeout => Self::Timeout(error.to_string()),
            ErrorKind::Protocol => Self::Protocol(error.to_string()),
            ErrorKind::Config => Self::Configuration(error.to_string()),
            ErrorKind::NotFound => Self::NotFound(error.to_string()),
            ErrorKind::Canceled | ErrorKind::IO => Self::Connection(error.to_string()),
            _ => Self::Command(error.to_string()),
        }
    }
}

impl From<serde_json::Error> for RedisError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error.to_string())
    }
}

impl From<std::string::FromUtf8Error> for RedisError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        Self::Serialization(error.to_string())
    }
}
