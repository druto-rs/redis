//! Configuration models and fluent builders for Redis connections and topologies.

use std::collections::HashMap;

use fred::types::config::{ClusterDiscoveryPolicy, Config as FredConfig, Server, ServerConfig};

use crate::error::RedisError;

/// Default name assigned to the primary connection when none is explicitly specified.
pub const DEFAULT_CONNECTION_NAME: &str = "default";

/// Default Redis port defined by official network specifications.
pub const DEFAULT_REDIS_PORT: u16 = 6379;

/// Default host address for local standalone instances.
pub const DEFAULT_REDIS_HOST: &str = "127.0.0.1";

/// Master configuration defining default connection naming and connection profiles.
#[derive(Clone, Debug)]
pub struct RedisConfig {
    default_connection: String,
    connections: HashMap<String, ConnectionTarget>,
}

impl Default for RedisConfig {
    fn default() -> Self {
        let mut connections = HashMap::new();
        connections.insert(
            DEFAULT_CONNECTION_NAME.to_string(),
            ConnectionTarget::Standalone(StandaloneConfig::default()),
        );

        Self {
            default_connection: DEFAULT_CONNECTION_NAME.to_string(),
            connections,
        }
    }
}

impl RedisConfig {
    /// Creates a configuration builder with default settings.
    pub fn builder() -> RedisConfigBuilder {
        RedisConfigBuilder::new()
    }

    /// Creates a configuration pointing to a single standalone URL for the default connection.
    pub fn from_url(url: impl Into<String>) -> Self {
        let mut connections = HashMap::new();
        connections.insert(
            DEFAULT_CONNECTION_NAME.to_string(),
            ConnectionTarget::Url(url.into()),
        );

        Self {
            default_connection: DEFAULT_CONNECTION_NAME.to_string(),
            connections,
        }
    }

    /// Returns the name of the default connection.
    pub fn default_connection_name(&self) -> &str {
        &self.default_connection
    }

    /// Retrieves the connection target configuration for a given named connection.
    pub fn get_connection(&self, name: &str) -> Option<&ConnectionTarget> {
        self.connections.get(name)
    }

    /// Returns an iterator over all configured connection entries.
    pub fn iter_connections(&self) -> impl Iterator<Item = (&String, &ConnectionTarget)> {
        self.connections.iter()
    }

    /// Returns the total count of configured connections.
    pub fn len(&self) -> usize {
        self.connections.len()
    }

    /// Returns whether any connections are configured.
    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }
}

/// Fluent builder for constructing [`RedisConfig`] instances.
#[derive(Clone, Debug)]
pub struct RedisConfigBuilder {
    default_connection: String,
    connections: HashMap<String, ConnectionTarget>,
}

impl Default for RedisConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RedisConfigBuilder {
    /// Initializes an empty builder with `"default"` as the default connection identifier.
    pub fn new() -> Self {
        Self {
            default_connection: DEFAULT_CONNECTION_NAME.to_string(),
            connections: HashMap::new(),
        }
    }

    /// Overrides the name of the primary connection.
    pub fn default_connection(mut self, name: impl Into<String>) -> Self {
        self.default_connection = name.into();
        self
    }

    /// Adds a connection configured via a connection URL string.
    pub fn add_url(mut self, name: impl Into<String>, url: impl Into<String>) -> Self {
        self.connections
            .insert(name.into(), ConnectionTarget::Url(url.into()));
        self
    }

    /// Adds a connection configured via a standalone configuration struct.
    pub fn add_standalone(mut self, name: impl Into<String>, config: StandaloneConfig) -> Self {
        self.connections
            .insert(name.into(), ConnectionTarget::Standalone(config));
        self
    }

    /// Adds a connection configured for a distributed cluster topology.
    pub fn add_cluster(mut self, name: impl Into<String>, config: ClusterConfig) -> Self {
        self.connections
            .insert(name.into(), ConnectionTarget::Cluster(config));
        self
    }

    /// Adds a connection configured for a high-availability Sentinel topology.
    pub fn add_sentinel(mut self, name: impl Into<String>, config: SentinelConfig) -> Self {
        self.connections
            .insert(name.into(), ConnectionTarget::Sentinel(config));
        self
    }

    /// Builds and validates the resulting [`RedisConfig`].
    pub fn build(self) -> Result<RedisConfig, RedisError> {
        if self.connections.is_empty() {
            return Err(RedisError::configuration(
                "Cannot build RedisConfig with zero connection targets configured",
            ));
        }

        if !self.connections.contains_key(&self.default_connection) {
            return Err(RedisError::configuration(format!(
                "Configured default connection '{}' does not exist in connection list",
                self.default_connection
            )));
        }

        Ok(RedisConfig {
            default_connection: self.default_connection,
            connections: self.connections,
        })
    }
}

/// Specific target topology and parameters for a Redis connection.
#[derive(Clone, Debug)]
pub enum ConnectionTarget {
    /// Standard connection URL (e.g., `redis://localhost:6379` or `rediss://...`).
    Url(String),
    /// Standalone single-node connection configuration.
    Standalone(StandaloneConfig),
    /// Clustered topology configuration across multiple seed nodes.
    Cluster(ClusterConfig),
    /// High-availability Sentinel configuration.
    Sentinel(SentinelConfig),
}

impl ConnectionTarget {
    /// Converts this connection target into an underlying driver configuration.
    pub fn to_driver_config(&self) -> Result<FredConfig, RedisError> {
        match self {
            Self::Url(url) => FredConfig::from_url(url).map_err(|error| {
                RedisError::configuration(format!("Failed to parse connection URL: {error}"))
            }),
            Self::Standalone(config) => {
                let database = if config.database > 0 {
                    Some(config.database)
                } else {
                    None
                };
                let driver_config = FredConfig {
                    server: ServerConfig::Centralized {
                        server: Server::new(&config.host, config.port),
                    },
                    username: config.username.clone(),
                    password: config.password.clone(),
                    database,
                    ..FredConfig::default()
                };
                Ok(driver_config)
            }
            Self::Cluster(config) => {
                let hosts = config
                    .nodes
                    .iter()
                    .map(|address| {
                        if let Some((host, port_str)) = address.split_once(':') {
                            let port = port_str.parse::<u16>().unwrap_or(DEFAULT_REDIS_PORT);
                            Server::new(host, port)
                        } else {
                            Server::new(address, DEFAULT_REDIS_PORT)
                        }
                    })
                    .collect::<Vec<_>>();

                if hosts.is_empty() {
                    return Err(RedisError::configuration(
                        "Cluster configuration requires at least one seed node",
                    ));
                }

                let driver_config = FredConfig {
                    server: ServerConfig::Clustered {
                        hosts,
                        policy: ClusterDiscoveryPolicy::default(),
                    },
                    username: config.username.clone(),
                    password: config.password.clone(),
                    ..FredConfig::default()
                };
                Ok(driver_config)
            }
            Self::Sentinel(config) => {
                let hosts = config
                    .sentinels
                    .iter()
                    .map(|address| {
                        if let Some((host, port_str)) = address.split_once(':') {
                            let port = port_str.parse::<u16>().unwrap_or(26379);
                            Server::new(host, port)
                        } else {
                            Server::new(address, 26379)
                        }
                    })
                    .collect::<Vec<_>>();

                if hosts.is_empty() {
                    return Err(RedisError::configuration(
                        "Sentinel configuration requires at least one sentinel host",
                    ));
                }

                let driver_config = FredConfig {
                    server: ServerConfig::Sentinel {
                        hosts,
                        service_name: config.service_name.clone(),
                    },
                    username: config.username.clone(),
                    password: config.password.clone(),
                    ..FredConfig::default()
                };
                Ok(driver_config)
            }
        }
    }
}

/// Standalone single-instance connection parameters.
#[derive(Clone, Debug)]
pub struct StandaloneConfig {
    /// Hostname or IP address of the target Redis server.
    pub host: String,
    /// Network port of the target Redis server.
    pub port: u16,
    /// Optional ACL username.
    pub username: Option<String>,
    /// Optional password or AUTH secret.
    pub password: Option<String>,
    /// Database index.
    pub database: u8,
    /// Whether TLS is enabled for this connection.
    pub tls: bool,
}

impl Default for StandaloneConfig {
    fn default() -> Self {
        Self {
            host: DEFAULT_REDIS_HOST.to_string(),
            port: DEFAULT_REDIS_PORT,
            username: None,
            password: None,
            database: 0,
            tls: false,
        }
    }
}

impl StandaloneConfig {
    /// Creates a new standalone configuration targeting the given host and port.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            username: None,
            password: None,
            database: 0,
            tls: false,
        }
    }

    /// Configures optional credentials.
    pub fn credentials(mut self, username: Option<String>, password: Option<String>) -> Self {
        self.username = username;
        self.password = password;
        self
    }

    /// Sets the target database index.
    pub fn database(mut self, database: u8) -> Self {
        self.database = database;
        self
    }

    /// Configures whether TLS encryption is active.
    pub fn tls(mut self, tls: bool) -> Self {
        self.tls = tls;
        self
    }
}

/// Clustered deployment configuration with multiple seed nodes.
#[derive(Clone, Debug, Default)]
pub struct ClusterConfig {
    /// List of cluster seed node addresses (e.g., `["127.0.0.1:7000", "127.0.0.1:7001"]`).
    pub nodes: Vec<String>,
    /// Optional ACL username.
    pub username: Option<String>,
    /// Optional password or AUTH secret.
    pub password: Option<String>,
    /// Whether TLS is enabled across the cluster.
    pub tls: bool,
}

impl ClusterConfig {
    /// Creates a cluster configuration from a list of seed node addresses.
    pub fn new(nodes: Vec<String>) -> Self {
        Self {
            nodes,
            username: None,
            password: None,
            tls: false,
        }
    }

    /// Configures optional credentials for cluster authentication.
    pub fn credentials(mut self, username: Option<String>, password: Option<String>) -> Self {
        self.username = username;
        self.password = password;
        self
    }

    /// Configures TLS for the cluster nodes.
    pub fn tls(mut self, tls: bool) -> Self {
        self.tls = tls;
        self
    }
}

/// Sentinel high-availability deployment configuration.
#[derive(Clone, Debug)]
pub struct SentinelConfig {
    /// Addresses of the Sentinel nodes.
    pub sentinels: Vec<String>,
    /// Master group service name monitored by the Sentinels.
    pub service_name: String,
    /// Optional ACL username.
    pub username: Option<String>,
    /// Optional password or AUTH secret.
    pub password: Option<String>,
    /// Whether TLS is enabled for Sentinel communication.
    pub tls: bool,
}

impl SentinelConfig {
    /// Creates a new Sentinel configuration with target sentinel nodes and a monitored service name.
    pub fn new(sentinels: Vec<String>, service_name: impl Into<String>) -> Self {
        Self {
            sentinels,
            service_name: service_name.into(),
            username: None,
            password: None,
            tls: false,
        }
    }

    /// Configures optional credentials for Sentinel authentication.
    pub fn credentials(mut self, username: Option<String>, password: Option<String>) -> Self {
        self.username = username;
        self.password = password;
        self
    }

    /// Configures TLS for Sentinel connections.
    pub fn tls(mut self, tls: bool) -> Self {
        self.tls = tls;
        self
    }
}
