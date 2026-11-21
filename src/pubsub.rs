//! Asynchronous Pub/Sub messaging channels, pattern subscriptions, and message streaming.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use fred::interfaces::EventInterface;
use futures_util::Stream;
use serde::de::DeserializeOwned;
use tokio::sync::{RwLock, broadcast, mpsc};

use crate::error::RedisError;

/// Message received from an active Redis Pub/Sub channel or pattern subscription.
#[derive(Clone, Debug, PartialEq)]
pub struct RedisMessage {
    /// The specific channel on which this message was published.
    pub channel: String,
    /// Pattern string if received via a pattern subscription (`PSUBSCRIBE`).
    pub pattern: Option<String>,
    /// UTF-8 decoded string payload.
    pub payload: String,
    /// Raw binary bytes payload.
    pub binary_payload: Vec<u8>,
}

impl RedisMessage {
    /// Creates a new message instance.
    pub fn new(
        channel: impl Into<String>,
        pattern: Option<String>,
        payload: impl Into<String>,
        binary_payload: Vec<u8>,
    ) -> Self {
        Self {
            channel: channel.into(),
            pattern,
            payload: payload.into(),
            binary_payload,
        }
    }

    /// Returns the channel name.
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// Returns the matched pattern, if the message was delivered via pattern matching.
    pub fn pattern(&self) -> Option<&str> {
        self.pattern.as_deref()
    }

    /// Returns the string message payload.
    pub fn payload(&self) -> &str {
        &self.payload
    }

    /// Returns the raw binary payload.
    pub fn binary_payload(&self) -> &[u8] {
        &self.binary_payload
    }

    /// Consumes the message and deserializes its payload as a JSON struct.
    pub fn into_json<Target: DeserializeOwned>(self) -> Result<Target, RedisError> {
        serde_json::from_str(&self.payload).map_err(RedisError::from)
    }

    /// Deserializes the message payload as a JSON struct by reference.
    pub fn to_json<Target: DeserializeOwned>(&self) -> Result<Target, RedisError> {
        serde_json::from_str(&self.payload).map_err(RedisError::from)
    }
}

/// Asynchronous stream of incoming [`RedisMessage`] items from a subscription.
pub struct MessageStream {
    receiver: mpsc::UnboundedReceiver<RedisMessage>,
}

impl MessageStream {
    /// Creates a new stream wrapping an unbounded receiver.
    pub fn new(receiver: mpsc::UnboundedReceiver<RedisMessage>) -> Self {
        Self { receiver }
    }

    /// Asynchronously awaits the next available message, returning `None` if the stream is closed.
    pub async fn next_message(&mut self) -> Option<RedisMessage> {
        self.receiver.recv().await
    }
}

impl Stream for MessageStream {
    type Item = RedisMessage;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(context)
    }
}

/// Dedicated, isolated Redis subscriber handle ensuring pub/sub commands do not block command pools.
#[derive(Clone)]
pub struct RedisSubscriber {
    client: Option<fred::clients::SubscriberClient>,
    broadcast_sender: Arc<broadcast::Sender<RedisMessage>>,
    active_subscriptions: Arc<RwLock<Vec<String>>>,
}

impl RedisSubscriber {
    /// Creates a subscriber instance backed by an underlying driver client.
    pub fn from_client(client: fred::clients::SubscriberClient) -> Self {
        let (broadcast_sender, _) = broadcast::channel(1024);
        let broadcast_sender_arc = Arc::new(broadcast_sender);

        let sender_clone = Arc::clone(&broadcast_sender_arc);
        let mut driver_rx = client.message_rx();

        tokio::spawn(async move {
            while let Ok(msg) = driver_rx.recv().await {
                let channel = msg.channel.to_string();
                let payload = msg
                    .value
                    .as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| {
                        msg.value
                            .as_bytes()
                            .map(|b| String::from_utf8_lossy(b).to_string())
                            .unwrap_or_default()
                    });
                let binary_payload = msg
                    .value
                    .as_bytes()
                    .map(|b| b.to_vec())
                    .unwrap_or_else(|| payload.as_bytes().to_vec());

                let message = RedisMessage::new(channel, None, payload, binary_payload);
                let _ = sender_clone.send(message);
            }
        });

        Self {
            client: Some(client),
            broadcast_sender: broadcast_sender_arc,
            active_subscriptions: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Creates an in-memory subscriber handle linked to a shared broadcast channel.
    pub fn from_broadcast_sender(broadcast_sender: Arc<broadcast::Sender<RedisMessage>>) -> Self {
        Self {
            client: None,
            broadcast_sender,
            active_subscriptions: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Creates an in-memory test subscriber handle for testing.
    pub fn new_mock() -> (Self, broadcast::Sender<RedisMessage>) {
        let (broadcast_sender, _) = broadcast::channel(1024);
        let subscriber = Self::from_broadcast_sender(Arc::new(broadcast_sender.clone()));
        (subscriber, broadcast_sender)
    }

    /// Subscribes to the specified channel, returning an asynchronous [`MessageStream`].
    pub async fn subscribe(&self, channel: &str) -> Result<MessageStream, RedisError> {
        if let Some(ref client) = self.client {
            use fred::interfaces::PubsubInterface;
            client.subscribe(channel).await?;
        }

        let mut active = self.active_subscriptions.write().await;
        if !active.contains(&channel.to_string()) {
            active.push(channel.to_string());
        }

        let target_channel = channel.to_string();
        let mut broadcast_rx = self.broadcast_sender.subscribe();
        let (stream_tx, stream_rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            while let Ok(message) = broadcast_rx.recv().await {
                if message.channel == target_channel && stream_tx.send(message).is_err() {
                    break;
                }
            }
        });

        Ok(MessageStream::new(stream_rx))
    }

    /// Subscribes to a wildcard channel pattern, returning an asynchronous [`MessageStream`].
    pub async fn psubscribe(&self, pattern: &str) -> Result<MessageStream, RedisError> {
        if let Some(ref client) = self.client {
            use fred::interfaces::PubsubInterface;
            client.psubscribe(pattern).await?;
        }

        let mut active = self.active_subscriptions.write().await;
        if !active.contains(&pattern.to_string()) {
            active.push(pattern.to_string());
        }

        let pattern_string = pattern.to_string();
        let mut broadcast_rx = self.broadcast_sender.subscribe();
        let (stream_tx, stream_rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            while let Ok(mut message) = broadcast_rx.recv().await {
                if wildcard_match(&pattern_string, &message.channel) {
                    message.pattern = Some(pattern_string.clone());
                    if stream_tx.send(message).is_err() {
                        break;
                    }
                }
            }
        });

        Ok(MessageStream::new(stream_rx))
    }

    /// Unsubscribes from the specified channel.
    pub async fn unsubscribe(&self, channel: &str) -> Result<(), RedisError> {
        if let Some(ref client) = self.client {
            use fred::interfaces::PubsubInterface;
            client.unsubscribe(channel).await?;
        }

        let mut active = self.active_subscriptions.write().await;
        active.retain(|c| c != channel);
        Ok(())
    }

    /// Unsubscribes from the specified wildcard pattern.
    pub async fn punsubscribe(&self, pattern: &str) -> Result<(), RedisError> {
        if let Some(ref client) = self.client {
            use fred::interfaces::PubsubInterface;
            client.punsubscribe(pattern).await?;
        }

        let mut active = self.active_subscriptions.write().await;
        active.retain(|p| p != pattern);
        Ok(())
    }

    /// Registers a background listener callback invoked for every message on the target channel.
    pub async fn listen<Function, FutureResult>(
        &self,
        channel: &str,
        handler: Function,
    ) -> Result<tokio::task::JoinHandle<()>, RedisError>
    where
        Function: Fn(RedisMessage) -> FutureResult + Send + Sync + 'static,
        FutureResult: Future<Output = ()> + Send + 'static,
    {
        let mut stream = self.subscribe(channel).await?;
        let handle = tokio::spawn(async move {
            while let Some(msg) = stream.next_message().await {
                handler(msg).await;
            }
        });

        Ok(handle)
    }

    /// Gracefully closes the subscriber connection.
    pub async fn quit(&self) -> Result<(), RedisError> {
        if let Some(ref client) = self.client {
            use fred::interfaces::ClientLike;
            client.quit().await?;
        }
        Ok(())
    }
}

/// Helper function evaluating glob wildcard patterns for `psubscribe`.
pub(crate) fn wildcard_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    if let Some((prefix, suffix)) = pattern.split_once('*') {
        text.starts_with(prefix) && text[prefix.len()..].ends_with(suffix)
    } else {
        pattern == text
    }
}
