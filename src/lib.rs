// #![deny(clippy::future_not_send)]
mod blocker;
pub mod channel;
mod raw_deserializer;
mod raw_serializer;

use core::panic;
use std::hash::Hasher;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use channel::auto_serialization_format;
use channel::Channel;
use channel::ChannelMessage;
use channel::SerializationFormat;
use embedded_svc::mqtt::client::asynch;
use embedded_svc::mqtt::client::Event;
use embedded_svc::mqtt::client::EventPayload;
use embedded_svc::mqtt::client::QoS;
#[cfg(feature = "embedded")]
use esp_idf_svc::mqtt::client::{EspAsyncMqttClient, MqttClientConfiguration};
use log::trace;
use log::{error, warn};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::pin::pin;

const META_TOPIC: &str = "sync/meta";

#[derive(Clone)]
pub struct SyncStorage {
    inner: Arc<Mutex<SyncStorageInner>>,
}

struct SyncStorageInner {
    client_id: String,
    containers: Vec<Arc<dyn Container>>,
    queue_sender: tokio::sync::mpsc::Sender<QueueMessage>,
}

#[async_trait::async_trait]
trait Container: Send + Sync + 'static {
    fn topic(&self) -> &str;
    async fn on_message(&self, topic: &str, payload: &[u8]) -> Result<(), BrevduvaError>;
    fn serialize(&self) -> Vec<u8>;
    fn has_received_message(&self) -> bool;
    fn up_to_date(&self) -> &blocker::Blocker;
    fn read_only(&self) -> bool;
    fn can_sync_state(&self) -> bool;
}

#[derive(Debug)]
enum QueueMessage {
    SyncContainer {
        container_id: usize,
    },
    PublishOnChannel {
        container_id: usize,
        data: Vec<u8>,
        qos: QoS,
    },
    Subscribe {
        container_id: usize,
    },
}

#[derive(Serialize, Deserialize)]
enum MetaMessage {
    ReceivedAllMessages {
        client: String,
        connection_id: u32,
        topic: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadWriteMode {
    ReadOnly,
    ReadWrite,
    Driven,
}

pub struct SyncedContainer<T> {
    id: usize,
    topic: String,
    data: Mutex<Option<T>>,
    callback: Mutex<Option<Box<dyn Fn(&T) + Send + Sync>>>,
    queue: tokio::sync::mpsc::Sender<QueueMessage>,
    up_to_date: blocker::Blocker,
    has_received_message: Mutex<bool>,
    mode: ReadWriteMode,
    format: crate::channel::SerializationFormat,
}

#[derive(thiserror::Error, Debug)]
pub enum BrevduvaError {
    #[error("A container with the name \"{0}\" already exists")]
    ContainerAlreadyExists(String),
    #[error("Failed to deserialize message")]
    MessageDeserialize(#[from] serde_json::Error),
    #[error("Failed to deserialize message")]
    StringDeserialize(#[from] std::string::FromUtf8Error),
    #[error("Failed to deserialize message")]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error("Failed to deserialize message")]
    StringDeserialize2(#[from] raw_deserializer::RawError),
    #[error("Failed to deserialize postcard message")]
    MessageDeserializePostcard(#[from] postcard::Error),
    #[error("Cannot set data on driven container")]
    DrivenContainer,
}

fn hash_equal<T: std::hash::Hash>(a: &T, b: &T) -> bool {
    let mut hash1 = std::collections::hash_map::DefaultHasher::new();
    let mut hash2 = std::collections::hash_map::DefaultHasher::new();
    a.hash(&mut hash1);
    b.hash(&mut hash2);
    hash1.finish() == hash2.finish()
}

impl<T: Serialize + DeserializeOwned + Send + Sync + 'static + std::hash::Hash> SyncedContainer<T> {
    fn new(
        id: usize,
        topic: String,
        data: T,
        queue: tokio::sync::mpsc::Sender<QueueMessage>,
        mode: ReadWriteMode,
        callback: Option<Box<dyn Fn(&T) + Send + Sync>>,
        format: SerializationFormat,
    ) -> Self {
        Self {
            id,
            topic,
            data: Mutex::new(Some(data)),
            queue,
            up_to_date: if mode == ReadWriteMode::Driven {
                blocker::Blocker::new_unblocked()
            } else {
                blocker::Blocker::new()
            },
            has_received_message: Mutex::new(false),
            mode,
            callback: Mutex::new(callback),
            format: if format == SerializationFormat::Auto {
                auto_serialization_format::<T>()
            } else {
                format
            },
        }
    }

    pub(crate) async fn subscribe(&self) {
        self.queue
            .send(QueueMessage::Subscribe {
                container_id: self.id,
            })
            .await
            .unwrap();
    }

    pub fn on_change(&self, f: impl Fn(&T) + Send + Sync + 'static) {
        let mut callback = self.callback.lock().unwrap();
        *callback = Some(Box::new(f));
    }

    pub async fn wait_for_sync(&self) {
        self.up_to_date.wait().await;
    }

    pub fn get(&self) -> std::sync::MutexGuard<Option<T>> {
        self.data.lock().unwrap()
    }

    pub async fn update<F>(&self, f: F)
    where
        F: FnOnce(&mut T) + Send,
    {
        if self.mode == ReadWriteMode::ReadOnly {
            panic!("Cannot set data on a read-only container");
        }

        {
            let mut d = self.data.lock().unwrap();
            if let Some(d) = d.as_mut() {
                f(d);

                if let Some(callback) = self.callback.lock().unwrap().as_ref() {
                    callback(d);
                }
            }
        }
        self.queue
            .send(QueueMessage::SyncContainer {
                container_id: self.id,
            })
            .await
            .unwrap();
    }

    pub async fn set(&self, data: T) {
        if self.mode == ReadWriteMode::ReadOnly {
            panic!("Cannot set data on a read-only container");
        }

        {
            let mut d = self.data.lock().unwrap();
            if hash_equal(&Some(&data), &d.as_ref()) {
                return;
            }
            *d = Some(data);
            if let Some(callback) = self.callback.lock().unwrap().as_ref() {
                callback(d.as_ref().unwrap())
            }
        }
        self.queue
            .send(QueueMessage::SyncContainer {
                container_id: self.id,
            })
            .await
            .unwrap();
    }
}

#[async_trait::async_trait]
impl<
        T: Serialize + DeserializeOwned + Send + Sync + std::fmt::Debug + std::hash::Hash + 'static,
    > Container for SyncedContainer<T>
{
    fn topic(&self) -> &str {
        &self.topic
    }

    fn read_only(&self) -> bool {
        self.mode == ReadWriteMode::ReadOnly
    }

    fn can_sync_state(&self) -> bool {
        !self.read_only()
    }

    fn up_to_date(&self) -> &blocker::Blocker {
        &self.up_to_date
    }

    async fn on_message(&self, _topic: &str, payload: &[u8]) -> Result<(), BrevduvaError> {
        // For raw strings, we treat the payload as a string directly, instead of json
        let d: T = channel::deserialize_from_slice(payload, self.format)?;

        if self.mode == ReadWriteMode::Driven {
            if hash_equal(&self.data.lock().unwrap().as_ref(), &Some(&d)) {
                // No change
                return Ok(());
            } else {
                trace!("Received message on driven container. Ignoring and pushing our state to the server");
                self.queue
                    .send(QueueMessage::SyncContainer {
                        container_id: self.id,
                    })
                    .await
                    .unwrap();
                return Err(BrevduvaError::DrivenContainer);
            }
        }

        let mut data = self.data.lock().unwrap();
        *data = Some(d);
        trace!("Deserialized message: {data:?}");
        *self.has_received_message.lock().unwrap() = true;

        if let Some(callback) = self.callback.lock().unwrap().as_ref() {
            callback(data.as_ref().unwrap());
        }
        Ok(())
    }

    fn has_received_message(&self) -> bool {
        *self.has_received_message.lock().unwrap()
    }

    fn serialize(&self) -> Vec<u8> {
        let data = self.data.lock().unwrap();
        channel::serialize_to_vec(&*data, self.format).unwrap()
    }
}

fn matches_topic(topic_filter: &str, topic: &str) -> bool {
    // let cf = topic_filter.chars();
    let mut c2 = topic.chars().peekable();

    for c1 in topic_filter.chars() {
        match c1 {
            '+' => loop {
                match c2.peek() {
                    Some('/') => {
                        break;
                    }
                    Some(_) => {
                        c2.next();
                    }
                    None => {
                        return false;
                    }
                }
            },
            '#' => {
                return true;
            }
            _ => {
                if Some(c1) != c2.next() {
                    return false;
                }
            }
        }
    }

    c2.peek().is_none()
}

#[test]
fn test_matches_topic() {
    assert!(matches_topic("a/b/c", "a/b/c"));
    assert!(!matches_topic("a/b/c", "a/b/c/d"));
    assert!(matches_topic("a/+/c", "a/b/c"));
    assert!(matches_topic("aaabbbcccddd/+/c", "aaabbbcccddd/b/c"));
    assert!(matches_topic("aa/#", "aa/b"));
    assert!(matches_topic("aa/#", "aa/b/c"));
    assert!(!matches_topic("aa/#", "bb/aa/b/c"));
    assert!(!matches_topic("a/b/c", ""));
    assert!(!matches_topic("", "a"));
    assert!(matches_topic("", ""));
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SessionPersistance {
    Persistent,
    Transient,
}

impl SyncStorage {
    #[cfg(feature = "embedded")]
    pub async fn new<'a>(
        client_id: &'a str,
        host: &'static str,
        username: &'a str,
        password: &'a str,
        persistance: SessionPersistance,
    ) -> Self {
        use esp_idf_svc::mqtt::client::LwtConfiguration;

        let online_status_topic = format!("devices/{client_id}/status");
        let mqtt_config = MqttClientConfiguration::<'a> {
            client_id: Some(client_id),
            username: Some(username),
            password: Some(password),
            // Make the broker remember the state for the client between connections
            disable_clean_session: persistance == SessionPersistance::Persistent,
            lwt: Some(LwtConfiguration {
                topic: Box::leak(Box::new(format!("sync/{online_status_topic}"))),
                payload: "\"offline\"".as_bytes(),
                qos: QoS::AtLeastOnce,
                retain: true,
            }),
            ..Default::default()
        };

        let (queue_sender, queue_receiver) = tokio::sync::mpsc::channel::<QueueMessage>(4);
        let storage = Self {
            inner: Arc::new(Mutex::new(SyncStorageInner {
                containers: Vec::new(),
                queue_sender,
                client_id: client_id.to_string(),
            })),
        };

        let storage2 = storage.clone();
        let (client, connection) = EspAsyncMqttClient::new(host, &mqtt_config).unwrap();
        tokio::spawn(async move {
            Self::start(client, connection, queue_receiver, storage2)
                .await
                .unwrap()
        });

        storage.publish_online_status(online_status_topic).await;

        storage
    }

    #[cfg(feature = "edge-mqtt")]
    pub async fn new<'a>(
        client_id: &'a str,
        host: &str,
        username: &'a str,
        password: &'a str,
        persistance: SessionPersistance,
    ) -> Self {
        use edge_mqtt::io::LastWill;

        let host = host
            .strip_prefix("mqtt://")
            .expect("Host should start with 'mqtt://'");
        let (host, port) = host
            .rsplit_once(":")
            .expect("Host should be on the format 'mqtt://host:port'");
        trace!("Connecting to {}:{}", host, port);
        let port: u16 = port.parse().unwrap();
        let mut mqtt_options = edge_mqtt::io::MqttOptions::new(client_id, host, port);

        mqtt_options.set_keep_alive(core::time::Duration::from_secs(10));
        mqtt_options.set_clean_session(persistance == SessionPersistance::Transient);
        mqtt_options.set_credentials(username, password);

        let online_status_topic = format!("devices/{client_id}/status");
        mqtt_options.set_last_will(LastWill {
            topic: format!("sync/{online_status_topic}"),
            message: "\"offline\"".to_string().into(),
            qos: edge_mqtt::io::QoS::AtLeastOnce,
            retain: true,
        });

        let (rumqttc_client, rumqttc_eventloop) = edge_mqtt::io::AsyncClient::new(mqtt_options, 10);

        let client = edge_mqtt::io::MqttClient::new(rumqttc_client);
        let connection = edge_mqtt::io::MqttConnection::new(rumqttc_eventloop);

        let (queue_sender, queue_receiver) = tokio::sync::mpsc::channel::<QueueMessage>(4);
        let storage = Self {
            inner: Arc::new(Mutex::new(SyncStorageInner {
                containers: Vec::new(),
                queue_sender,
                client_id: client_id.to_string(),
            })),
        };

        let storage2 = storage.clone();
        tokio::spawn(async move {
            Self::start(client, connection, queue_receiver, storage2)
                .await
                .unwrap()
        });

        storage.publish_online_status(online_status_topic).await;

        storage
    }

    async fn publish_online_status(&self, online_status_topic: String) {
        self.add_container_with_mode(
            &online_status_topic,
            "online".to_string(),
            SerializationFormat::Json,
            ReadWriteMode::Driven,
        )
        .await
        .unwrap();
    }

    pub async fn wait_for_sync(&self) {
        let (sender, containers) = {
            let inner = self.inner.lock().unwrap();
            (inner.queue_sender.clone(), inner.containers.clone())
        };
        for container in &containers {
            container.up_to_date().wait().await;
        }

        for (id, container) in containers.iter().enumerate() {
            if container.can_sync_state() && !container.has_received_message() {
                trace!(
                    "Container {} did not have server state. Pushing our state to the server",
                    container.topic()
                );
                sender
                    .send(QueueMessage::SyncContainer { container_id: id })
                    .await
                    .unwrap();
            }
        }
    }

    pub async fn add_channel<T: Serialize + DeserializeOwned + Send + Sync + 'static>(
        &self,
        name: &str,
        format: crate::channel::SerializationFormat,
    ) -> Result<
        (
            Arc<crate::channel::Channel<T>>,
            tokio::sync::mpsc::Receiver<ChannelMessage<T>>,
        ),
        BrevduvaError,
    > {
        let topic = format!("sync/{}", name);

        let (sender, receiver) = tokio::sync::mpsc::channel::<ChannelMessage<T>>(8);
        let container = {
            let mut inner = self.inner.lock().unwrap();
            if inner.containers.iter().any(|c| c.topic() == topic) {
                return Err(BrevduvaError::ContainerAlreadyExists(name.to_string()));
            }

            let read_only = topic.contains('+') || topic.contains('#') || topic.contains('$');

            let container = Arc::new(Channel::new(
                inner.containers.len(),
                topic,
                inner.queue_sender.clone(),
                read_only,
                sender,
                format,
            ));
            inner.containers.push(container.clone());
            container
        };

        container.subscribe().await;

        Ok((container, receiver))
    }

    pub async fn add_container<
        T: Serialize + DeserializeOwned + Send + Sync + std::fmt::Debug + std::hash::Hash + 'static,
    >(
        &self,
        name: &str,
        inital: T,
        format: crate::channel::SerializationFormat,
    ) -> Result<Arc<SyncedContainer<T>>, BrevduvaError> {
        self.add_container_with_mode(name, inital, format, ReadWriteMode::ReadWrite)
            .await
    }

    pub async fn add_container_with_mode<
        T: Serialize + DeserializeOwned + Send + Sync + std::fmt::Debug + std::hash::Hash + 'static,
    >(
        &self,
        name: &str,
        inital: T,
        format: crate::channel::SerializationFormat,
        mode: ReadWriteMode,
    ) -> Result<Arc<SyncedContainer<T>>, BrevduvaError> {
        let topic = format!("sync/{}", name);
        let container = {
            let mut inner = self.inner.lock().unwrap();
            if inner.containers.iter().any(|c| c.topic() == topic) {
                return Err(BrevduvaError::ContainerAlreadyExists(name.to_string()));
            }

            let has_wildcard = topic.contains('+') || topic.contains('#') || topic.contains('$');
            if has_wildcard && mode != ReadWriteMode::ReadOnly {
                panic!("Wildcard topics must be read-only");
            }

            let container = Arc::new(SyncedContainer::new(
                inner.containers.len(),
                topic,
                inital,
                inner.queue_sender.clone(),
                mode,
                None,
                format,
            ));
            inner.containers.push(container.clone());
            container
        };

        container.subscribe().await;

        Ok(container)
    }

    async fn start<C: asynch::Client + asynch::Publish + Send, Conn: asynch::Connection + Send>(
        client: C,
        mut connection: Conn,
        mut queue: tokio::sync::mpsc::Receiver<QueueMessage>,
        storage: SyncStorage,
    ) -> Result<(), C::Error> {
        let client_id = storage.inner.lock().unwrap().client_id.clone();
        let client_id = &client_id;

        // Downgrade to a weak reference, so that we don't keep the storage alive indefinitely just by running this task
        let weak_storage = Arc::downgrade(&storage.inner);
        drop(storage);
        let storage = weak_storage;

        let t0 = std::time::Instant::now();
        let (reconnect_sender, mut reconnect_receiver) = tokio::sync::mpsc::channel::<u32>(2);

        let client = Arc::new(tokio::sync::Mutex::new(client));
        let client2 = client.clone();

        // We to immediately start pumping the connection for messages, or else subscribe() and publish() below will not work.
        // I think the channel has limited capacity, so if we don't listen for messages all the time, we can end up in deadlocks.
        //
        // Note also that if you go to http://tools.emqx.io/ and then connect and send a message to topic
        // "esp-mqtt-demo", the client configured here should receive it.
        let storage2 = storage.clone();
        let first_connect = AtomicBool::new(false);
        let first_connect = &first_connect;
        let connection_id = Arc::new(Mutex::new(0u32));
        let connection_id2 = connection_id.clone();
        let meta_topic = format!("{META_TOPIC}/{client_id}");
        let meta_topic = &meta_topic;
        let listen = pin!(async move {
            let mut current_connection_id = 0u32;
            while let Ok(event) = connection.next().await {
                let t = t0.elapsed();
                let payload = event.payload();

                match payload {
                    EventPayload::Connected(_) => {
                        trace!("Connected to MQTT broker");
                        first_connect.store(true, Ordering::Relaxed);
                        current_connection_id += 1;
                        *connection_id2.lock().unwrap() = current_connection_id;
                        reconnect_sender.send(current_connection_id).await.unwrap();
                    }
                    EventPayload::Received {
                        topic: Some(topic),
                        data,
                        ..
                    } => {
                        if topic == meta_topic {
                            let meta: MetaMessage = match serde_json::from_slice(data) {
                                Ok(m) => m,
                                Err(e) => {
                                    error!("Failed to parse meta message: {e:?}");
                                    continue;
                                }
                            };
                            match meta {
                                MetaMessage::ReceivedAllMessages {
                                    client,
                                    connection_id: message_connection_id,
                                    topic,
                                } => {
                                    if *client_id != client
                                        || message_connection_id != current_connection_id
                                    {
                                        // Ignore messages from other clients
                                        continue;
                                    }
                                    let container = {
                                        if let Some(inner) = storage2.upgrade() {
                                            let inner = inner.lock().unwrap();
                                            inner
                                                .containers
                                                .iter()
                                                .find(|c| c.topic() == topic)
                                                .cloned()
                                        } else {
                                            break;
                                        }
                                    };
                                    match container {
                                        Some(container) => {
                                            trace!("Container \"{}\" has received all pending messages", topic);
                                            container.up_to_date().unblock().await;
                                        }
                                        None => {
                                            warn!(
                                                "Received meta message for unknown topic: \"{}\"",
                                                topic
                                            );
                                        }
                                    }
                                }
                            }
                            continue;
                        } else {
                            let containers = {
                                if let Some(inner) = storage2.upgrade() {
                                    let inner = inner.lock().unwrap();
                                    inner
                                        .containers
                                        .iter()
                                        .filter(|c| matches_topic(c.topic(), topic))
                                        .cloned()
                                        .collect::<Vec<_>>()
                                } else {
                                    break;
                                }
                            };
                            if containers.is_empty() {
                                warn!("Received message on unknown topic: \"{}\"", topic);
                            } else {
                                let stripped_topic = topic.strip_prefix("sync/").unwrap_or(topic);
                                for container in containers {
                                    match container.on_message(&stripped_topic, data).await {
                                        Ok(()) => {}
                                        Err(BrevduvaError::DrivenContainer) => {}
                                        Err(e) => {
                                            error!("{e}. Ignoring message on topic \"{topic}\"");
                                        }
                                    }
                                }
                            }
                        }
                    }
                    EventPayload::Subscribed(_) => {
                        // It's not guaranteed that all pending messages have been received when the Subscribed event happens.
                        // We instead send a fake message and wait for that to be recieved.
                        // Since messages (with the same QoS) are received in order, our fake message will be received after all pending messages.
                        // info!("Subscribed to some topic");
                    }
                    _ => {
                        trace!("[Queue] Event: {} at {}", payload, t.as_millis());
                    }
                }
            }

            trace!("Connection closed");

            Result::<(), C::Error>::Ok(())
        });

        let storage3 = storage.clone();
        let publish = pin!(async move {
            while let Some(event) = queue.recv().await {
                match event {
                    QueueMessage::SyncContainer { container_id } => {
                        trace!("Publishing container {}", container_id);
                        let (topic, data) = {
                            if let Some(inner) = storage3.upgrade() {
                                let inner = inner.lock().unwrap();
                                let container = &inner.containers[container_id];
                                (container.topic().to_string(), container.serialize())
                            } else {
                                break;
                            }
                        };
                        client
                            .lock()
                            .await
                            .publish(&topic, QoS::AtMostOnce, true, &data)
                            .await?;
                    }
                    QueueMessage::PublishOnChannel {
                        container_id,
                        data,
                        qos,
                    } => {
                        trace!("Publishing channel {}", container_id);
                        let topic = {
                            if let Some(inner) = storage3.upgrade() {
                                let inner = inner.lock().unwrap();
                                let container = &inner.containers[container_id];
                                container.topic().to_string()
                            } else {
                                break;
                            }
                        };
                        client
                            .lock()
                            .await
                            .publish(&topic, qos, false, &data)
                            .await?;
                    }
                    QueueMessage::Subscribe { container_id } => {
                        if !first_connect.load(Ordering::Relaxed) {
                            trace!("Not connected, skipping subscribe. Will subscribe when connected");
                            continue;
                        }

                        let topic = {
                            if let Some(inner) = storage3.upgrade() {
                                let inner = inner.lock().unwrap();
                                let container = &inner.containers[container_id];
                                container.topic().to_string()
                            } else {
                                break;
                            }
                        };
                        let mut client = client.lock().await;
                        if let Err(e) = client.subscribe(&topic, QoS::AtLeastOnce).await {
                            error!("Failed to subscribe to topic \"{topic}\". Trying again when connecting: {e:?}");
                        } else {
                            let connection_id = *connection_id.lock().unwrap();
                            client
                                .publish(
                                    meta_topic,
                                    QoS::AtLeastOnce,
                                    false,
                                    &serde_json::to_vec(&MetaMessage::ReceivedAllMessages {
                                        client: client_id.clone(),
                                        connection_id,
                                        topic: topic.clone(),
                                    })
                                    .unwrap(),
                                )
                                .await
                                .unwrap();
                        }
                    }
                }
            }

            Result::<(), C::Error>::Ok(())
        });

        let res = tokio::select! {
                _ = async move {
                    // Listens to a connected message, and ensures we subscribe every time we reconnect.
                    // We cannot do this in the event loop itself, since we can deadlock due to the event
                    // channel not being pumped.
                    while let Some(connection_id) = reconnect_receiver.recv().await {
                        trace!("Connected, subscribing to all topics");
                        let mut topics = {
                            if let Some(inner) = storage.upgrade() {
                                let inner = inner.lock().unwrap();
                                inner.containers.iter().map(|c| c.topic().to_owned()).collect::<Vec<_>>()
                            } else {
                                break;
                            }
                        };
                        topics.insert(0, meta_topic.to_owned());

                        for topic in topics {
                            while let Err(e) = client2.lock().await.subscribe(&topic, QoS::AtLeastOnce).await {
                                error!("Failed to subscribe to topic \"{topic}\": {e:?}, retrying...");

                                // Re-try in 0.5s
                                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                            }

                            if topic != *meta_topic {
                                client2
                                    .lock()
                                    .await
                                    .publish(
                                        meta_topic,
                                        QoS::AtLeastOnce,
                                        false,
                                        &serde_json::to_vec(&MetaMessage::ReceivedAllMessages {
                                            client: client_id.clone(),
                                            connection_id,
                                            topic,
                                        })
                                        .unwrap(),
                                    )
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                } => {
                    unreachable!()
                },
                a = listen => a,
                b = publish => b,
        };

        res
    }
}
