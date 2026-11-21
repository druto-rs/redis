//! Comprehensive integration and unit test suite for the `redis` crate.

use std::{collections::HashSet, time::Duration};

use druto_redis as redis;
use druto_redis::{
    commands::RedisCommands,
    config::{
        ClusterConfig, DEFAULT_CONNECTION_NAME, DEFAULT_REDIS_HOST, DEFAULT_REDIS_PORT,
        RedisConfig, SentinelConfig, StandaloneConfig,
    },
    error::RedisError,
    fake::{RedisFake, normalize_redis_range},
    manager::RedisManager,
    pubsub::RedisMessage,
    script::{RedisScript, ScriptOutput},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct UserSession {
    user_id: u64,
    username: String,
    roles: Vec<String>,
}

// ============================================================================
// 1. Configuration & Builder Tests
// ============================================================================

#[test]
fn test_default_config() {
    let config = RedisConfig::default();
    assert_eq!(config.default_connection_name(), DEFAULT_CONNECTION_NAME);
    assert_eq!(config.len(), 1);
    assert!(!config.is_empty());
    assert!(config.get_connection(DEFAULT_CONNECTION_NAME).is_some());
}

#[test]
fn test_from_url_config() {
    let url = "redis://:secret@127.0.0.1:6380/2";
    let config = RedisConfig::from_url(url);
    assert_eq!(config.default_connection_name(), DEFAULT_CONNECTION_NAME);
    assert_eq!(config.len(), 1);

    let target = config.get_connection(DEFAULT_CONNECTION_NAME).unwrap();
    let driver_config = target.to_driver_config();
    assert!(driver_config.is_ok());
}

#[test]
fn test_config_builder_full() {
    let standalone = StandaloneConfig::new("127.0.0.1", 6379)
        .credentials(Some("admin".to_string()), Some("pass".to_string()))
        .database(1)
        .tls(true);

    let cluster = ClusterConfig::new(vec![
        "127.0.0.1:7000".to_string(),
        "127.0.0.1:7001".to_string(),
    ])
    .credentials(None, Some("clusterpass".to_string()))
    .tls(false);

    let sentinel = SentinelConfig::new(
        vec!["127.0.0.1:26379".to_string(), "127.0.0.1:26380".to_string()],
        "mymaster",
    )
    .credentials(None, None)
    .tls(false);

    let config = RedisConfig::builder()
        .default_connection("main")
        .add_standalone("main", standalone)
        .add_cluster("cluster_pool", cluster)
        .add_sentinel("sentinel_pool", sentinel)
        .add_url("cache", "redis://localhost:6379/3")
        .build()
        .expect("Config build should succeed");

    assert_eq!(config.default_connection_name(), "main");
    assert_eq!(config.len(), 4);
    assert!(config.get_connection("main").is_some());
    assert!(config.get_connection("cluster_pool").is_some());
    assert!(config.get_connection("sentinel_pool").is_some());
    assert!(config.get_connection("cache").is_some());

    // Test driver config generation
    assert!(
        config
            .get_connection("main")
            .unwrap()
            .to_driver_config()
            .is_ok()
    );
    assert!(
        config
            .get_connection("cluster_pool")
            .unwrap()
            .to_driver_config()
            .is_ok()
    );
    assert!(
        config
            .get_connection("sentinel_pool")
            .unwrap()
            .to_driver_config()
            .is_ok()
    );
    assert!(
        config
            .get_connection("cache")
            .unwrap()
            .to_driver_config()
            .is_ok()
    );
}

#[test]
fn test_config_builder_errors() {
    // Empty config
    let empty_builder = RedisConfig::builder();
    assert!(empty_builder.build().is_err());

    // Default connection missing
    let missing_default = RedisConfig::builder()
        .default_connection("primary")
        .add_url("secondary", "redis://127.0.0.1:6379");
    assert!(missing_default.build().is_err());

    // Empty cluster nodes
    let empty_cluster = ClusterConfig::new(vec![]);
    let target = redis::config::ConnectionTarget::Cluster(empty_cluster);
    assert!(target.to_driver_config().is_err());

    // Empty sentinel hosts
    let empty_sentinel = SentinelConfig::new(vec![], "master");
    let target_sentinel = redis::config::ConnectionTarget::Sentinel(empty_sentinel);
    assert!(target_sentinel.to_driver_config().is_err());

    // Malformed URL
    let bad_url = redis::config::ConnectionTarget::Url("not a valid url".to_string());
    assert!(bad_url.to_driver_config().is_err());
}

#[test]
fn test_standalone_config_defaults() {
    let standalone = StandaloneConfig::default();
    assert_eq!(standalone.host, DEFAULT_REDIS_HOST);
    assert_eq!(standalone.port, DEFAULT_REDIS_PORT);
    assert_eq!(standalone.database, 0);
    assert!(!standalone.tls);
}

// ============================================================================
// 2. Error Types & Conversions Tests
// ============================================================================

#[test]
fn test_error_variants_and_display() {
    let conn_err = RedisError::connection("failed to connect");
    assert!(conn_err.to_string().contains("Redis connection error"));

    let auth_err = RedisError::authentication("invalid auth");
    assert!(auth_err.to_string().contains("Redis authentication error"));

    let cmd_err = RedisError::command("unknown command");
    assert!(cmd_err.to_string().contains("Redis command error"));

    let proto_err = RedisError::protocol("bad frame");
    assert!(proto_err.to_string().contains("Redis protocol error"));

    let nf_err = RedisError::not_found("key missing");
    assert!(nf_err.to_string().contains("Redis key/field not found"));

    let timeout_err = RedisError::timeout("deadline exceeded");
    assert!(
        timeout_err
            .to_string()
            .contains("Redis operation timed out")
    );

    let ser_err = RedisError::serialization("json failure");
    assert!(ser_err.to_string().contains("Redis serialization error"));

    let script_err = RedisError::script("lua syntax error");
    assert!(script_err.to_string().contains("Redis script error"));

    let tx_err = RedisError::transaction("watch key touched");
    assert!(tx_err.to_string().contains("Redis transaction error"));

    let cfg_err = RedisError::configuration("bad config");
    assert!(cfg_err.to_string().contains("Redis configuration error"));

    let pool_err = RedisError::pool("pool exhausted");
    assert!(pool_err.to_string().contains("Redis pool error"));
}

#[test]
fn test_error_from_conversions() {
    // serde_json::Error conversion
    let bad_json_result: Result<UserSession, serde_json::Error> =
        serde_json::from_str("invalid json");
    let ser_err = RedisError::from(bad_json_result.unwrap_err());
    assert!(matches!(ser_err, RedisError::Serialization(_)));

    // std::string::FromUtf8Error conversion
    let bad_utf8 = vec![0, 159, 146, 150];
    let utf8_err = String::from_utf8(bad_utf8).unwrap_err();
    let redis_err = RedisError::from(utf8_err);
    assert!(matches!(redis_err, RedisError::Serialization(_)));

    // fred::error::Error conversions
    let auth_fred = fred::error::Error::new(fred::error::ErrorKind::Auth, "bad auth");
    assert!(matches!(
        RedisError::from(auth_fred),
        RedisError::Authentication(_)
    ));

    let timeout_fred = fred::error::Error::new(fred::error::ErrorKind::Timeout, "timed out");
    assert!(matches!(
        RedisError::from(timeout_fred),
        RedisError::Timeout(_)
    ));

    let proto_fred = fred::error::Error::new(fred::error::ErrorKind::Protocol, "bad resp");
    assert!(matches!(
        RedisError::from(proto_fred),
        RedisError::Protocol(_)
    ));

    let config_fred = fred::error::Error::new(fred::error::ErrorKind::Config, "config invalid");
    assert!(matches!(
        RedisError::from(config_fred),
        RedisError::Configuration(_)
    ));

    let notfound_fred = fred::error::Error::new(fred::error::ErrorKind::NotFound, "missing");
    assert!(matches!(
        RedisError::from(notfound_fred),
        RedisError::NotFound(_)
    ));

    let io_fred = fred::error::Error::new(fred::error::ErrorKind::IO, "broken pipe");
    assert!(matches!(
        RedisError::from(io_fred),
        RedisError::Connection(_)
    ));

    let other_fred = fred::error::Error::new(fred::error::ErrorKind::Unknown, "server err");
    assert!(matches!(
        RedisError::from(other_fred),
        RedisError::Command(_)
    ));
}

// ============================================================================
// 3. RedisFake: String & Binary Operations
// ============================================================================

#[tokio::test]
async fn test_fake_string_crud_and_bytes() {
    let fake = RedisFake::new();

    // Key should not exist initially
    assert_eq!(fake.get("key1").await.unwrap(), None);
    assert!(!fake.exists("key1").await.unwrap());

    // Set and Get string
    fake.set("key1", "hello world").await.unwrap();
    assert_eq!(
        fake.get("key1").await.unwrap(),
        Some("hello world".to_string())
    );
    assert!(fake.exists("key1").await.unwrap());

    // Binary get/set
    let binary_data = vec![1, 2, 3, 4, 255];
    fake.set_bytes("bin_key", &binary_data).await.unwrap();
    assert_eq!(fake.get_bytes("bin_key").await.unwrap(), Some(binary_data));

    // Delete
    assert!(fake.del("key1").await.unwrap());
    assert_eq!(fake.get("key1").await.unwrap(), None);
    assert!(!fake.del("key1").await.unwrap()); // deleting again returns false

    // Del many
    fake.set("k1", "v1").await.unwrap();
    fake.set("k2", "v2").await.unwrap();
    fake.set("k3", "v3").await.unwrap();
    let deleted_count = fake.del_many(&["k1", "k2", "nonexistent"]).await.unwrap();
    assert_eq!(deleted_count, 2);
    assert_eq!(fake.get("k3").await.unwrap(), Some("v3".to_string()));
}

#[tokio::test]
async fn test_fake_set_nx() {
    let fake = RedisFake::new();

    // First call sets value
    let set_first = fake.set_nx("lock", "active").await.unwrap();
    assert!(set_first);
    assert_eq!(fake.get("lock").await.unwrap(), Some("active".to_string()));

    // Second call fails because key exists
    let set_second = fake.set_nx("lock", "new_active").await.unwrap();
    assert!(!set_second);
    assert_eq!(fake.get("lock").await.unwrap(), Some("active".to_string()));
}

#[tokio::test]
async fn test_fake_mget_mset() {
    let fake = RedisFake::new();

    fake.mset(&[("a", "1"), ("b", "2"), ("c", "3")])
        .await
        .unwrap();

    let values = fake.mget(&["a", "b", "missing", "c"]).await.unwrap();
    assert_eq!(
        values,
        vec![
            Some("1".to_string()),
            Some("2".to_string()),
            None,
            Some("3".to_string()),
        ]
    );
}

#[tokio::test]
async fn test_fake_counter_operations() {
    let fake = RedisFake::new();

    // Incr from 0
    assert_eq!(fake.incr("counter").await.unwrap(), 1);
    assert_eq!(fake.incr_by("counter", 5).await.unwrap(), 6);
    assert_eq!(fake.decr("counter").await.unwrap(), 5);
    assert_eq!(fake.decr_by("counter", 10).await.unwrap(), -5);
    assert_eq!(fake.get("counter").await.unwrap(), Some("-5".to_string()));
}

#[tokio::test]
async fn test_fake_expiration_and_ttl() {
    let fake = RedisFake::new();

    // No TTL on unconfigured key
    assert_eq!(fake.ttl("missing").await.unwrap(), -2);

    // Set with no expiration
    fake.set("permanent", "val").await.unwrap();
    assert_eq!(fake.ttl("permanent").await.unwrap(), -1);

    // Set with expiration (set_ex)
    fake.set_ex("expiring", "temp", 10).await.unwrap();
    let ttl = fake.ttl("expiring").await.unwrap();
    assert!(ttl > 0 && ttl <= 10);

    // Set bytes with expiration
    fake.set_bytes_ex("expiring_bytes", b"temp_bytes", 5)
        .await
        .unwrap();
    assert!(fake.exists("expiring_bytes").await.unwrap());

    // Explicit expire call
    fake.set("to_expire", "val").await.unwrap();
    assert!(fake.expire("to_expire", 20).await.unwrap());
    assert!(!fake.expire("nonexistent_key", 20).await.unwrap());
    assert!(fake.ttl("to_expire").await.unwrap() > 0);
}

// ============================================================================
// 4. RedisFake: Typed JSON Helpers
// ============================================================================

#[tokio::test]
async fn test_fake_json_helpers() {
    let fake = RedisFake::new();

    let session = UserSession {
        user_id: 42,
        username: "hitarashi".to_string(),
        roles: vec!["admin".to_string(), "editor".to_string()],
    };

    // Store JSON
    fake.set_json("session:42", &session).await.unwrap();

    // Retrieve JSON
    let retrieved: Option<UserSession> = fake.get_json("session:42").await.unwrap();
    assert_eq!(retrieved, Some(session.clone()));

    // Store JSON with expiration
    fake.set_json_ex("session:temp", &session, 3600)
        .await
        .unwrap();
    let retrieved_temp: Option<UserSession> = fake.get_json("session:temp").await.unwrap();
    assert_eq!(retrieved_temp, Some(session));

    // Missing key returns Ok(None)
    let missing: Option<UserSession> = fake.get_json("session:missing").await.unwrap();
    assert_eq!(missing, None);

    // Invalid JSON payload returns Err
    fake.set("invalid:json", "not a valid json string")
        .await
        .unwrap();
    let result: Result<Option<UserSession>, RedisError> = fake.get_json("invalid:json").await;
    assert!(result.is_err());
}

// ============================================================================
// 5. RedisFake: Hash Operations
// ============================================================================

#[tokio::test]
async fn test_fake_hashes() {
    let fake = RedisFake::new();

    // Set fields
    assert!(fake.hset("user:1", "name", "John").await.unwrap());
    assert!(
        fake.hset("user:1", "email", "john@example.com")
            .await
            .unwrap()
    );
    assert!(!fake.hset("user:1", "name", "Johnny").await.unwrap()); // Update returns false

    // Get field
    assert_eq!(
        fake.hget("user:1", "name").await.unwrap(),
        Some("Johnny".to_string())
    );
    assert_eq!(fake.hget("user:1", "missing").await.unwrap(), None);

    // Exists & Len
    assert!(fake.hexists("user:1", "name").await.unwrap());
    assert!(!fake.hexists("user:1", "age").await.unwrap());
    assert_eq!(fake.hlen("user:1").await.unwrap(), 2);

    // Increment
    assert_eq!(fake.hincr_by("user:1", "visits", 1).await.unwrap(), 1);
    assert_eq!(fake.hincr_by("user:1", "visits", 4).await.unwrap(), 5);

    // Keys & Vals
    let keys = fake.hkeys("user:1").await.unwrap();
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&"name".to_string()));
    assert!(keys.contains(&"email".to_string()));
    assert!(keys.contains(&"visits".to_string()));

    let vals = fake.hvals("user:1").await.unwrap();
    assert_eq!(vals.len(), 3);

    // Get all
    let all = fake.hget_all("user:1").await.unwrap();
    assert_eq!(all.get("name").unwrap(), "Johnny");
    assert_eq!(all.get("visits").unwrap(), "5");

    // Delete field
    assert!(fake.hdel("user:1", "visits").await.unwrap());
    assert!(!fake.hexists("user:1", "visits").await.unwrap());
    assert_eq!(fake.hlen("user:1").await.unwrap(), 2);
}

// ============================================================================
// 6. RedisFake: List Operations
// ============================================================================

#[tokio::test]
async fn test_fake_lists() {
    let fake = RedisFake::new();

    // Push front (LPUSH)
    assert_eq!(fake.lpush("tasks", &["task1", "task2"]).await.unwrap(), 2);
    // Push back (RPUSH)
    assert_eq!(fake.rpush("tasks", &["task3"]).await.unwrap(), 3);

    // Length
    assert_eq!(fake.llen("tasks").await.unwrap(), 3);

    // Range
    let all_tasks = fake.lrange("tasks", 0, -1).await.unwrap();
    assert_eq!(all_tasks, vec!["task2", "task1", "task3"]);

    let partial_tasks = fake.lrange("tasks", 1, 2).await.unwrap();
    assert_eq!(partial_tasks, vec!["task1", "task3"]);

    // Pop front (LPOP)
    assert_eq!(fake.lpop("tasks").await.unwrap(), Some("task2".to_string()));

    // Pop back (RPOP)
    assert_eq!(fake.rpop("tasks").await.unwrap(), Some("task3".to_string()));
    assert_eq!(fake.llen("tasks").await.unwrap(), 1);

    // Empty list pop
    fake.lpop("tasks").await.unwrap();
    assert_eq!(fake.lpop("tasks").await.unwrap(), None);
}

// ============================================================================
// 7. RedisFake: Set Operations
// ============================================================================

#[tokio::test]
async fn test_fake_sets() {
    let fake = RedisFake::new();

    // SADD
    assert_eq!(
        fake.sadd("tags", &["rust", "async", "web"]).await.unwrap(),
        3
    );
    // Adding duplicates returns only count of newly added
    assert_eq!(fake.sadd("tags", &["rust", "framework"]).await.unwrap(), 1);

    // SCARD
    assert_eq!(fake.scard("tags").await.unwrap(), 4);

    // SISMEMBER
    assert!(fake.sismember("tags", "rust").await.unwrap());
    assert!(!fake.sismember("tags", "python").await.unwrap());

    // SMEMBERS
    let members = fake.smembers("tags").await.unwrap();
    let expected: HashSet<String> = ["rust", "async", "web", "framework"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(members, expected);

    // SREM
    assert_eq!(fake.srem("tags", &["rust", "missing"]).await.unwrap(), 1);
    assert_eq!(fake.scard("tags").await.unwrap(), 3);
    assert!(!fake.sismember("tags", "rust").await.unwrap());
}

// ============================================================================
// 8. RedisFake: Sorted Set Operations
// ============================================================================

#[tokio::test]
async fn test_fake_sorted_sets() {
    let fake = RedisFake::new();

    // ZADD
    assert!(fake.zadd("leaderboard", 100.0, "alice").await.unwrap());
    assert!(fake.zadd("leaderboard", 200.0, "bob").await.unwrap());
    assert!(fake.zadd("leaderboard", 150.0, "charlie").await.unwrap());
    assert!(!fake.zadd("leaderboard", 250.0, "alice").await.unwrap()); // updating existing returns false

    // ZCARD
    assert_eq!(fake.zcard("leaderboard").await.unwrap(), 3);

    // ZSCORE
    assert_eq!(
        fake.zscore("leaderboard", "alice").await.unwrap(),
        Some(250.0)
    );
    assert_eq!(fake.zscore("leaderboard", "missing").await.unwrap(), None);

    // ZCOUNT
    assert_eq!(fake.zcount("leaderboard", 150.0, 250.0).await.unwrap(), 3);
    assert_eq!(fake.zcount("leaderboard", 0.0, 100.0).await.unwrap(), 0);

    // ZRANGE ordered by score ascending (charlie: 150, bob: 200, alice: 250)
    let top_two = fake.zrange("leaderboard", 0, 1).await.unwrap();
    assert_eq!(top_two, vec!["charlie", "bob"]);

    let all_ranked = fake.zrange("leaderboard", 0, -1).await.unwrap();
    assert_eq!(all_ranked, vec!["charlie", "bob", "alice"]);

    // ZREMRANGEBYSCORE
    let removed = fake
        .zremrangebyscore("leaderboard", 140.0, 210.0)
        .await
        .unwrap();
    assert_eq!(removed, 2);
    assert_eq!(fake.zcard("leaderboard").await.unwrap(), 1);
    assert_eq!(
        fake.zrange("leaderboard", 0, -1).await.unwrap(),
        vec!["alice"]
    );

    // ZREM
    assert_eq!(fake.zrem("leaderboard", &["alice"]).await.unwrap(), 1);
    assert_eq!(fake.zcard("leaderboard").await.unwrap(), 0);
}

// ============================================================================
// 9. Server & Utility Operations
// ============================================================================

#[tokio::test]
async fn test_fake_server_and_keys() {
    let fake = RedisFake::new();

    // Ping
    assert_eq!(fake.ping().await.unwrap(), "PONG");

    // Keys wildcard
    fake.set("user:1", "a").await.unwrap();
    fake.set("user:2", "b").await.unwrap();
    fake.set("order:100", "c").await.unwrap();

    let user_keys = fake.keys("user:*").await.unwrap();
    assert_eq!(user_keys.len(), 2);
    assert!(user_keys.contains(&"user:1".to_string()));
    assert!(user_keys.contains(&"user:2".to_string()));

    let all_keys = fake.keys("*").await.unwrap();
    assert_eq!(all_keys.len(), 3);

    // Flushdb clears everything
    fake.flushdb().await.unwrap();
    assert_eq!(fake.keys("*").await.unwrap().len(), 0);
}

// ============================================================================
// 10. Pipeline Execution Tests
// ============================================================================

#[tokio::test]
async fn test_pipeline_builder_and_fake_execution() {
    let fake = RedisFake::new();

    let mut pipe = fake.pipeline();
    assert!(pipe.is_empty());

    pipe.set("p1", "val1")
        .set_ex("p2", "val2", 60)
        .set_nx("p3", "val3")
        .get("p1")
        .del("p1")
        .exists("p2")
        .incr_by("counter", 10)
        .hset("hash", "field", "hval")
        .hget("hash", "field")
        .hdel("hash", "field")
        .lpush("list", &["elem1", "elem2"])
        .rpush("list", &["elem3"])
        .sadd("set", &["m1", "m2"])
        .zadd("zset", 50.0, "member");

    assert_eq!(pipe.len(), 14);
    assert!(!pipe.is_empty());

    let results = fake.execute_pipeline(&pipe).await.unwrap();
    assert_eq!(results.len(), 14);

    // Result checks
    assert_eq!(results[0], ScriptOutput::String("OK".to_string()));
    assert_eq!(results[1], ScriptOutput::String("OK".to_string()));
    assert_eq!(results[2], ScriptOutput::Integer(1)); // setnx success
    assert_eq!(results[3], ScriptOutput::String("val1".to_string())); // get
    assert_eq!(results[4], ScriptOutput::Integer(1)); // del
    assert_eq!(results[5], ScriptOutput::Integer(1)); // exists
    assert_eq!(results[6], ScriptOutput::Integer(10)); // incr_by
    assert_eq!(results[7], ScriptOutput::Integer(1)); // hset new
    assert_eq!(results[8], ScriptOutput::String("hval".to_string())); // hget
    assert_eq!(results[9], ScriptOutput::Integer(1)); // hdel
    assert_eq!(results[10], ScriptOutput::Integer(2)); // lpush len
    assert_eq!(results[11], ScriptOutput::Integer(3)); // rpush len
    assert_eq!(results[12], ScriptOutput::Integer(2)); // sadd count
    assert_eq!(results[13], ScriptOutput::Integer(1)); // zadd new

    // Clear pipeline
    pipe.clear();
    assert_eq!(pipe.len(), 0);
    assert!(pipe.is_empty());
}

// ============================================================================
// 11. Transaction Execution Tests
// ============================================================================

#[tokio::test]
async fn test_transaction_builder_and_fake_commit() {
    let fake = RedisFake::new();

    let mut tx = fake.transaction();
    assert!(tx.is_empty());

    tx.set("account:a", "100")
        .set_ex("temp_exp", "val", 120)
        .set_nx("account:lock", "locked")
        .get("account:a")
        .del("temp_exp")
        .exists("account:a")
        .incr_by("account:a", -25)
        .hset("user:meta", "role", "admin")
        .hget("user:meta", "role")
        .hdel("user:meta", "role")
        .lpush("audit:log", &["entry1"])
        .rpush("audit:log", &["entry2"])
        .sadd("permissions", &["read", "write"])
        .zadd("scores", 99.5, "alice");

    assert_eq!(tx.len(), 14);
    assert_eq!(tx.operations().len(), 14);

    let results = fake.commit_transaction(&tx).await.unwrap();
    assert_eq!(results.len(), 14);

    assert_eq!(fake.get("account:a").await.unwrap(), Some("75".to_string()));

    // Test discard
    let mut tx_discard = fake.transaction();
    tx_discard.set("temp", "never_committed");
    assert_eq!(tx_discard.len(), 1);
    tx_discard.discard();
    assert!(tx_discard.is_empty());
}

// ============================================================================
// 12. Strongly Typed Lua Scripting Tests
// ============================================================================

#[tokio::test]
async fn test_redis_script_with_simulation() {
    let fake = RedisFake::new();

    let lua_code = r#"
        local current = redis.call('get', KEYS[1])
        if not current then
            redis.call('set', KEYS[1], ARGV[1])
            return 1
        else
            return 0
        end
    "#;

    let script = RedisScript::new(lua_code).with_simulation(|keys, args, context| {
        let key = keys
            .first()
            .ok_or_else(|| RedisError::script("missing key"))?;
        let val = args
            .first()
            .ok_or_else(|| RedisError::script("missing arg"))?;

        if context.exists(key) {
            Ok(ScriptOutput::Integer(0))
        } else {
            context.set(key, val);
            Ok(ScriptOutput::Integer(1))
        }
    });

    assert_eq!(script.source(), lua_code);
    assert_eq!(script.sha1().len(), 40); // 40 hex characters
    assert!(script.has_simulation());

    // First execution against Fake
    let output1 = fake
        .eval_script(&script, &["lock_key"], &["token_123"])
        .await
        .unwrap();
    assert_eq!(output1.as_i64(), Some(1));
    assert_eq!(output1.as_bool(), Some(true));
    assert_eq!(
        fake.get("lock_key").await.unwrap(),
        Some("token_123".to_string())
    );

    // Second execution against Fake (already locked)
    let output2 = fake
        .eval_script(&script, &["lock_key"], &["token_456"])
        .await
        .unwrap();
    assert_eq!(output2.as_i64(), Some(0));
    assert_eq!(output2.as_bool(), Some(false));
}

#[tokio::test]
async fn test_script_output_conversions() {
    let int_out = ScriptOutput::Integer(42);
    assert_eq!(int_out.as_i64(), Some(42));
    assert_eq!(int_out.as_bool(), Some(true));
    assert_eq!(int_out.into_string(), Some("42".to_string()));

    let str_out = ScriptOutput::String("true".to_string());
    assert_eq!(str_out.as_str(), Some("true"));
    assert_eq!(str_out.as_bool(), Some(true));

    let bin_out = ScriptOutput::Binary(b"binary data".to_vec());
    assert_eq!(bin_out.into_string(), Some("binary data".to_string()));

    let nil_out = ScriptOutput::Nil;
    assert!(nil_out.is_nil());
    assert_eq!(nil_out.as_bool(), Some(false));

    // JSON deserialization from script output
    let session = UserSession {
        user_id: 1,
        username: "root".to_string(),
        roles: vec!["superadmin".to_string()],
    };
    let json_str = serde_json::to_string(&session).unwrap();
    let json_output = ScriptOutput::String(json_str);
    let parsed: UserSession = json_output.into_json().unwrap();
    assert_eq!(parsed, session);
}

// ============================================================================
// 13. Pub/Sub In-Memory Messaging Tests
// ============================================================================

#[tokio::test]
async fn test_pubsub_channel_and_pattern() {
    let (subscriber, broadcast_tx) = redis::pubsub::RedisSubscriber::new_mock();

    // Subscribe to explicit channel
    let mut channel_stream = subscriber.subscribe("alerts").await.unwrap();

    // Subscribe to pattern
    let mut pattern_stream = subscriber.psubscribe("orders.*").await.unwrap();

    // Broadcast messages
    let msg1 = RedisMessage::new("alerts", None, "system_reboot", b"system_reboot".to_vec());
    let msg2 = RedisMessage::new("orders.created", None, "order_999", b"order_999".to_vec());
    let msg3 = RedisMessage::new("unrelated", None, "ignored", b"ignored".to_vec());

    let _ = broadcast_tx.send(msg1.clone());
    let _ = broadcast_tx.send(msg2.clone());
    let _ = broadcast_tx.send(msg3);

    // Channel stream should only receive "alerts"
    tokio::select! {
        received = channel_stream.next_message() => {
            let msg = received.expect("Should receive message");
            assert_eq!(msg.channel(), "alerts");
            assert_eq!(msg.payload(), "system_reboot");
            assert_eq!(msg.binary_payload(), b"system_reboot");
        }
        _ = tokio::time::sleep(Duration::from_millis(500)) => {
            panic!("Timeout waiting for channel message");
        }
    }

    // Pattern stream should receive "orders.created"
    tokio::select! {
        received = pattern_stream.next_message() => {
            let msg = received.expect("Should receive message");
            assert_eq!(msg.channel(), "orders.created");
            assert_eq!(msg.pattern(), Some("orders.*"));
            assert_eq!(msg.payload(), "order_999");
        }
        _ = tokio::time::sleep(Duration::from_millis(500)) => {
            panic!("Timeout waiting for pattern message");
        }
    }

    // Unsubscribe tests
    assert!(subscriber.unsubscribe("alerts").await.is_ok());
    assert!(subscriber.punsubscribe("orders.*").await.is_ok());
    assert!(subscriber.quit().await.is_ok());
}

#[tokio::test]
async fn test_pubsub_json_payload() {
    let session = UserSession {
        user_id: 88,
        username: "tester".to_string(),
        roles: vec!["user".to_string()],
    };
    let json_payload = serde_json::to_string(&session).unwrap();

    let message = RedisMessage::new(
        "user.events",
        None,
        json_payload.clone(),
        json_payload.into_bytes(),
    );

    let parsed: UserSession = message.into_json().expect("JSON parsing should succeed");
    assert_eq!(parsed, session);
}

// ============================================================================
// 14. RedisManager Multi-Connection & Config Tests
// ============================================================================

#[tokio::test]
async fn test_manager_configuration_and_connections() {
    let config = RedisConfig::builder()
        .default_connection("primary")
        .add_url("primary", "redis://127.0.0.1:6379/0")
        .add_url("cache", "redis://127.0.0.1:6379/1")
        .build()
        .unwrap();

    let manager = RedisManager::new(config);
    assert_eq!(manager.config().default_connection_name(), "primary");
    assert_eq!(manager.active_connections_count().await, 0);

    // Nonexistent connection returns configuration error
    let missing_result = manager.connection("missing_pool").await;
    assert!(missing_result.is_err());
    assert!(matches!(
        missing_result.unwrap_err(),
        RedisError::Configuration(_)
    ));
}

// ============================================================================
// 15. Range Normalization Utility Tests
// ============================================================================

#[test]
fn test_redis_range_normalization() {
    // Empty list
    assert_eq!(normalize_redis_range(0, 0, -1), (0, 0));

    // Full range (0 to -1)
    assert_eq!(normalize_redis_range(5, 0, -1), (0, 5));

    // Positive slice (1 to 3)
    assert_eq!(normalize_redis_range(5, 1, 3), (1, 4));

    // Negative slice (-2 to -1) -> items at index 3 and 4
    assert_eq!(normalize_redis_range(5, -2, -1), (3, 5));

    // Out of bounds stop clamped to len
    assert_eq!(normalize_redis_range(5, 2, 100), (2, 5));

    // Start beyond stop returns (0, 0)
    assert_eq!(normalize_redis_range(5, 4, 2), (0, 0));

    // Start beyond len returns (0, 0)
    assert_eq!(normalize_redis_range(5, 10, 20), (0, 0));
}

// ============================================================================
// 16. Additional Edge-Case & Coverage Tests
// ============================================================================

#[test]
fn test_cluster_and_sentinel_driver_configs() {
    use redis::config::ConnectionTarget;

    // Cluster config with explicit ports and default ports
    let cluster = ClusterConfig::new(vec!["10.0.0.1:7000".to_string(), "10.0.0.2".to_string()]);
    let target = ConnectionTarget::Cluster(cluster);
    assert!(target.to_driver_config().is_ok());

    // Empty cluster nodes error
    let empty_cluster = ClusterConfig::new(vec![]);
    let empty_target = ConnectionTarget::Cluster(empty_cluster);
    let err = empty_target.to_driver_config().unwrap_err();
    assert!(matches!(err, RedisError::Configuration(_)));

    // Sentinel config with explicit ports and default ports
    let sentinel = SentinelConfig::new(
        vec!["10.0.0.1:26379".to_string(), "10.0.0.2".to_string()],
        "mymaster",
    );
    let sen_target = ConnectionTarget::Sentinel(sentinel);
    assert!(sen_target.to_driver_config().is_ok());

    // Empty sentinel nodes error
    let empty_sentinel = SentinelConfig::new(vec![], "mymaster");
    let empty_sen_target = ConnectionTarget::Sentinel(empty_sentinel);
    let sen_err = empty_sen_target.to_driver_config().unwrap_err();
    assert!(matches!(sen_err, RedisError::Configuration(_)));
}

#[tokio::test]
async fn test_redis_script_without_simulation() {
    let fake = RedisFake::new();
    let script = RedisScript::new("return 1");
    assert!(!script.has_simulation());

    let result = fake.eval_script(&script, &["key"], &["arg"]).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), RedisError::Script(_)));
}

#[test]
fn test_script_output_full_accessors() {
    let str_out = ScriptOutput::String("OK".to_string());
    assert_eq!(str_out.as_str(), Some("OK"));
    assert_eq!(str_out.as_bool(), Some(true));
    assert_eq!(str_out.as_i64(), None);

    let bin_out = ScriptOutput::Binary(b"hello".to_vec());
    assert_eq!(bin_out.into_string(), Some("hello".to_string()));

    let bool_out = ScriptOutput::Boolean(false);
    assert_eq!(bool_out.as_bool(), Some(false));
    assert_eq!(bool_out.as_i64(), Some(0));

    let array_out = ScriptOutput::Array(vec![
        ScriptOutput::String("item1".to_string()),
        ScriptOutput::Integer(42),
    ]);
    assert!(!array_out.is_nil());
    assert_eq!(array_out.as_str(), None);

    let nil_out = ScriptOutput::Nil;
    assert!(nil_out.is_nil());
    assert_eq!(nil_out.as_bool(), Some(false));
    assert_eq!(nil_out.as_i64(), None);
}

#[tokio::test]
async fn test_fake_decrement_operations() {
    let fake = RedisFake::new();

    // Default counter starts at 0
    assert_eq!(fake.decr("counter").await.unwrap(), -1);
    assert_eq!(fake.decr_by("counter", 4).await.unwrap(), -5);
    assert_eq!(fake.incr("counter").await.unwrap(), -4);
}

#[test]
fn test_pipeline_and_transaction_mutations() {
    let mut pipeline = redis::pipeline::RedisPipeline::new();
    assert!(pipeline.is_empty());
    assert_eq!(pipeline.len(), 0);

    pipeline.get("key1").set("key2", "val2");
    assert!(!pipeline.is_empty());
    assert_eq!(pipeline.len(), 2);
    assert_eq!(pipeline.operations().len(), 2);

    let ops = pipeline.clone().into_operations();
    assert_eq!(ops.len(), 2);

    pipeline.clear();
    assert!(pipeline.is_empty());
    assert_eq!(pipeline.len(), 0);

    let mut transaction = redis::transaction::RedisTransaction::new();
    assert!(transaction.is_empty());
    assert_eq!(transaction.len(), 0);

    transaction.get("tx_key").del("tx_key");
    assert!(!transaction.is_empty());
    assert_eq!(transaction.len(), 2);
    assert_eq!(transaction.operations().len(), 2);

    transaction.discard();
    assert!(transaction.is_empty());
    assert_eq!(transaction.len(), 0);
}

#[tokio::test]
async fn test_manager_disconnect_all() {
    let config = RedisConfig::default();
    let manager = RedisManager::new(config);

    assert!(manager.disconnect("nonexistent").await.is_ok());
    assert!(manager.disconnect_all().await.is_ok());
    assert!(manager.quit_all().await.is_ok());
}

#[test]
fn test_pubsub_json_invalid_payload() {
    let msg = RedisMessage::new("channel", None, "invalid-json", b"invalid-json".to_vec());
    let res: Result<UserSession, _> = msg.into_json();
    assert!(res.is_err());
    assert!(matches!(res.unwrap_err(), RedisError::Serialization(_)));
}

#[tokio::test]
async fn test_fake_sorted_set_rem_and_score() {
    let fake = RedisFake::new();

    // Remove from non-existing sorted set
    assert_eq!(fake.zrem("empty_zset", &["none"]).await.unwrap(), 0);

    fake.zadd("zset", 10.0, "m1").await.unwrap();
    fake.zadd("zset", 20.0, "m2").await.unwrap();

    assert_eq!(fake.zscore("zset", "m1").await.unwrap(), Some(10.0));
    assert_eq!(fake.zscore("zset", "m3").await.unwrap(), None);

    assert_eq!(fake.zrem("zset", &["m1"]).await.unwrap(), 1);
    assert_eq!(fake.zscore("zset", "m1").await.unwrap(), None);
}

#[tokio::test]
async fn test_fake_set_remove_missing() {
    let fake = RedisFake::new();

    // SREM on nonexistent set
    assert_eq!(fake.srem("empty_set", &["foo"]).await.unwrap(), 0);

    fake.sadd("my_set", &["a", "b"]).await.unwrap();
    assert_eq!(fake.srem("my_set", &["c"]).await.unwrap(), 0);
    assert_eq!(fake.srem("my_set", &["a"]).await.unwrap(), 1);
}

#[tokio::test]
async fn test_fake_pubsub_direct_stream() {
    let fake = RedisFake::new();

    let mut stream = fake.subscribe("orders").await.unwrap();
    let mut pstream = fake.psubscribe("orders.*").await.unwrap();

    let count = fake.publish("orders", "direct_payload").await.unwrap();
    assert!(count >= 1);

    tokio::select! {
        msg_opt = stream.next_message() => {
            let msg = msg_opt.expect("Should receive message on channel");
            assert_eq!(msg.channel(), "orders");
            assert_eq!(msg.payload(), "direct_payload");
        }
        _ = tokio::time::sleep(Duration::from_millis(500)) => {
            panic!("Timeout waiting for direct channel message");
        }
    }

    fake.publish("orders.created", "new_order").await.unwrap();

    tokio::select! {
        msg_opt = pstream.next_message() => {
            let msg = msg_opt.expect("Should receive message on pattern");
            assert_eq!(msg.channel(), "orders.created");
            assert_eq!(msg.pattern(), Some("orders.*"));
            assert_eq!(msg.payload(), "new_order");
        }
        _ = tokio::time::sleep(Duration::from_millis(500)) => {
            panic!("Timeout waiting for pattern message");
        }
    }
}

#[tokio::test]
async fn test_subscriber_listen_callback() {
    let (subscriber, broadcast_tx) = redis::pubsub::RedisSubscriber::new_mock();
    let received_payloads = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let payloads_clone = std::sync::Arc::clone(&received_payloads);

    let handle = subscriber
        .listen("notifications", move |msg| {
            let clone = std::sync::Arc::clone(&payloads_clone);
            async move {
                let mut guard = clone.lock().await;
                guard.push(msg.payload().to_string());
            }
        })
        .await
        .unwrap();

    let msg = RedisMessage::new("notifications", None, "ping", b"ping".to_vec());
    let _ = broadcast_tx.send(msg);

    // Wait briefly for asynchronous callback processing
    tokio::time::sleep(Duration::from_millis(50)).await;

    let items = received_payloads.lock().await.clone();
    assert_eq!(items, vec!["ping".to_string()]);

    handle.abort();
}

#[tokio::test]
async fn test_fake_multitype_ttl_expiration() {
    let fake = RedisFake::new();

    // Hashes TTL
    fake.hset("hash:1", "field", "val").await.unwrap();
    assert_eq!(fake.ttl("hash:1").await.unwrap(), -1);
    assert!(fake.expire("hash:1", 60).await.unwrap());
    assert!(fake.ttl("hash:1").await.unwrap() > 0);

    // Lists TTL
    fake.lpush("list:1", &["item"]).await.unwrap();
    assert!(fake.expire("list:1", 30).await.unwrap());
    assert!(fake.ttl("list:1").await.unwrap() > 0);

    // Sets TTL
    fake.sadd("set:1", &["a"]).await.unwrap();
    assert!(fake.expire("set:1", 45).await.unwrap());
    assert!(fake.ttl("set:1").await.unwrap() > 0);

    // Nonexistent key TTL
    assert!(!fake.expire("missing:key", 10).await.unwrap());
    assert_eq!(fake.ttl("missing:key").await.unwrap(), -2);
}

#[tokio::test]
async fn test_empty_slice_operations_safety() {
    let fake = RedisFake::new();

    assert_eq!(fake.del_many(&[]).await.unwrap(), 0);
    assert!(fake.mget(&[]).await.unwrap().is_empty());
    assert!(fake.mset(&[]).await.is_ok());
    assert_eq!(fake.sadd("set", &[]).await.unwrap(), 0);
    assert_eq!(fake.srem("set", &[]).await.unwrap(), 0);
    assert_eq!(fake.zrem("zset", &[]).await.unwrap(), 0);
}

#[tokio::test]
async fn test_wildcard_middle_matching() {
    let (subscriber, broadcast_tx) = redis::pubsub::RedisSubscriber::new_mock();
    let mut stream = subscriber.psubscribe("user.*.created").await.unwrap();

    let msg = RedisMessage::new("user.123.created", None, "ok", b"ok".to_vec());
    let _ = broadcast_tx.send(msg);

    tokio::select! {
        received = stream.next_message() => {
            let received_msg = received.expect("Should receive pattern match");
            assert_eq!(received_msg.channel(), "user.123.created");
            assert_eq!(received_msg.pattern(), Some("user.*.created"));
        }
        _ = tokio::time::sleep(Duration::from_millis(500)) => {
            panic!("Timeout waiting for pattern message");
        }
    }
}

#[tokio::test]
async fn test_subscriber_stream_ext_and_unsubscriptions() {
    use futures_util::StreamExt;

    let (subscriber, broadcast_tx) = redis::pubsub::RedisSubscriber::new_mock();
    let channel_stream = subscriber.subscribe("events").await.unwrap();
    drop(channel_stream);
    let mut pattern_stream = subscriber.psubscribe("events.*").await.unwrap();

    let msg = RedisMessage::new("events.order", None, "placed", b"placed".to_vec());
    assert_eq!(msg.channel(), "events.order");
    assert_eq!(msg.payload(), "placed");
    assert_eq!(msg.binary_payload(), b"placed");
    let json_err: Result<UserSession, _> = msg.to_json();
    assert!(json_err.is_err());

    let _ = broadcast_tx.send(msg);

    // StreamExt next()
    let received = pattern_stream.next().await.unwrap();
    assert_eq!(received.channel(), "events.order");
    assert_eq!(received.pattern(), Some("events.*"));

    // Unsubscribe and punsubscribe
    assert!(subscriber.unsubscribe("events").await.is_ok());
    assert!(subscriber.punsubscribe("events.*").await.is_ok());
    assert!(subscriber.quit().await.is_ok());
}

#[tokio::test]
async fn test_wildcard_pattern_match_variations() {
    let (subscriber, broadcast_tx) = redis::pubsub::RedisSubscriber::new_mock();
    let mut stream_star = subscriber.psubscribe("*").await.unwrap();
    let mut stream_exact = subscriber.psubscribe("exact").await.unwrap();

    let msg1 = RedisMessage::new("any.channel", None, "data", b"data".to_vec());
    let _ = broadcast_tx.send(msg1);
    let rec1 = stream_star.next_message().await.unwrap();
    assert_eq!(rec1.channel(), "any.channel");

    let msg2 = RedisMessage::new("exact", None, "data", b"data".to_vec());
    let _ = broadcast_tx.send(msg2);
    let rec2 = stream_exact.next_message().await.unwrap();
    assert_eq!(rec2.channel(), "exact");
}

#[test]
fn test_script_and_output_exhaustive() {
    let script = RedisScript::new("return 100;");
    assert_eq!(script.source(), "return 100;");
    assert!(!script.has_simulation());
    let debug_repr = format!("{script:?}");
    assert!(debug_repr.contains("RedisScript"));
    assert!(debug_repr.contains("has_simulation: false"));

    // ScriptOutput from conversions
    let out_i64 = ScriptOutput::from(42i64);
    assert_eq!(out_i64.as_i64(), Some(42));
    assert_eq!(out_i64.into_string(), Some("42".to_string()));

    let out_str = ScriptOutput::from("hello");
    assert_eq!(out_str.as_str(), Some("hello"));

    let out_bool = ScriptOutput::from(true);
    assert_eq!(out_bool.as_bool(), Some(true));
    assert_eq!(out_bool.as_i64(), Some(1));
    assert_eq!(ScriptOutput::from(false).as_i64(), Some(0));

    let out_bytes = ScriptOutput::from(b"binary_data".to_vec());
    assert_eq!(
        out_bytes.clone().into_string(),
        Some("binary_data".to_string())
    );

    // as_bool variations
    assert_eq!(ScriptOutput::String("0".to_string()).as_bool(), Some(false));
    assert_eq!(
        ScriptOutput::String("false".to_string()).as_bool(),
        Some(false)
    );
    assert_eq!(
        ScriptOutput::String("FALSE".to_string()).as_bool(),
        Some(false)
    );
    assert_eq!(ScriptOutput::String("1".to_string()).as_bool(), Some(true));
    assert_eq!(ScriptOutput::String("OK".to_string()).as_bool(), Some(true));
    assert_eq!(ScriptOutput::String("unknown".to_string()).as_bool(), None);
    assert_eq!(ScriptOutput::Nil.as_bool(), Some(false));
    assert_eq!(ScriptOutput::Binary(vec![1]).as_bool(), None);

    // into_json errors
    let non_string = ScriptOutput::Nil;
    let json_res: Result<UserSession, _> = non_string.into_json();
    assert!(json_res.is_err());

    // fred::types::Value conversions
    let val_int = fred::types::Value::Integer(10);
    assert_eq!(ScriptOutput::from(val_int), ScriptOutput::Integer(10));

    let val_str = fred::types::Value::String("fred_str".into());
    assert_eq!(
        ScriptOutput::from(val_str),
        ScriptOutput::String("fred_str".to_string())
    );

    let val_bytes = fred::types::Value::Bytes(vec![1, 2, 3].into());
    assert_eq!(
        ScriptOutput::from(val_bytes),
        ScriptOutput::Binary(vec![1, 2, 3])
    );

    let val_bool = fred::types::Value::Boolean(true);
    assert_eq!(ScriptOutput::from(val_bool), ScriptOutput::Boolean(true));

    let val_null = fred::types::Value::Null;
    assert_eq!(ScriptOutput::from(val_null), ScriptOutput::Nil);

    let val_double = fred::types::Value::Double(12.5);
    assert!(
        matches!(ScriptOutput::from(val_double), ScriptOutput::String(ref s) if s.contains("12.5"))
    );

    let val_array = fred::types::Value::Array(vec![
        fred::types::Value::Integer(1),
        fred::types::Value::String("second".into()),
    ]);
    let out_arr = ScriptOutput::from(val_array);
    assert!(matches!(out_arr, ScriptOutput::Array(ref items) if items.len() == 2));
}

#[test]
fn test_pipeline_and_transaction_builders_extended() {
    let mut pipeline = redis::RedisPipeline::new();
    assert!(pipeline.is_empty());
    assert_eq!(pipeline.len(), 0);

    pipeline
        .set_nx("nx_k", "nx_v")
        .hdel("h_k", "h_field")
        .rpush("list_k", &["v1", "v2"]);

    assert!(!pipeline.is_empty());
    assert_eq!(pipeline.len(), 3);
    assert_eq!(pipeline.operations().len(), 3);

    let ops = pipeline.clone().into_operations();
    assert_eq!(ops.len(), 3);

    pipeline.clear();
    assert!(pipeline.is_empty());

    let mut tx = redis::RedisTransaction::new();
    assert!(tx.is_empty());
    assert_eq!(tx.len(), 0);

    tx.set_nx("tx_nx", "v")
        .hdel("tx_h", "f")
        .rpush("tx_l", &["item"]);

    assert!(!tx.is_empty());
    assert_eq!(tx.len(), 3);
    assert_eq!(tx.operations().len(), 3);

    tx.discard();
    assert!(tx.is_empty());
}

#[tokio::test]
async fn test_connection_and_manager_handles() {
    use fred::interfaces::ClientLike;

    let config = RedisConfig::default();
    let target = config.get_connection(DEFAULT_CONNECTION_NAME).unwrap();
    let driver_config = target.to_driver_config().unwrap();
    let client = fred::types::Builder::from_config(driver_config)
        .build()
        .unwrap();
    let conn = redis::RedisConnection::new("standalone_handle", client);

    assert_eq!(conn.name(), "standalone_handle");
    assert!(!conn.is_connected());
    assert_eq!(
        conn.raw_client().state(),
        fred::types::ClientState::Disconnected
    );
    assert!(conn.pipeline().is_empty());
    assert!(conn.transaction().is_empty());

    let config = RedisConfig::default();
    let manager = RedisManager::new(config);
    assert_eq!(manager.config().len(), 1);

    let default_mgr = RedisManager::default();
    assert_eq!(default_mgr.config().len(), 1);

    let unknown_conn = manager.connection("nonexistent").await;
    assert!(unknown_conn.is_err());
    assert!(matches!(
        unknown_conn.unwrap_err(),
        RedisError::Configuration(_)
    ));

    let unknown_sub = manager.subscriber("nonexistent").await;
    assert!(matches!(unknown_sub, Err(RedisError::Configuration(_))));

    assert!(manager.quit_all().await.is_ok());
    assert!(manager.disconnect_all().await.is_ok());
}

#[tokio::test]
async fn test_script_context_coverage_on_fake() {
    let fake = RedisFake::new();
    let script = RedisScript::new("test_script").with_simulation(|keys, args, context| {
        let key = keys[0];
        let arg = args[0];

        context.set(key, arg);
        assert!(context.exists(key));
        assert_eq!(context.get(key), Some(arg.to_string()));

        context.set_ex(key, "temp", 100);
        assert!(context.ttl(key) > 0);

        context.incr_by("counter", 10);
        assert_eq!(context.get("counter"), Some("10".to_string()));

        assert!(context.expire(key, 200));
        assert!(context.del(key));
        assert!(!context.exists(key));
        assert_eq!(context.get(key), None);

        Ok(ScriptOutput::String("simulation_ok".to_string()))
    });

    let res = fake
        .eval_script(&script, &["script_key"], &["script_arg"])
        .await
        .unwrap();
    assert_eq!(res, ScriptOutput::String("simulation_ok".to_string()));
}
