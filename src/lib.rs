// #![deny(clippy::future_not_send)]
mod blocker;
pub mod channel;

use core::panic;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use channel::Channel;
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

trait Container: Send + Sync + 'static {
    fn topic(&self) -> &str;
    fn on_message(&self, payload: &[u8]) -> Result<(), BrevduvaError>;
    fn serialize(&self) -> String;
    fn has_received_message(&self) -> bool;
    fn up_to_date(&self) -> &blocker::Blocker;
    fn read_only(&self) -> bool;
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

pub struct SyncedContainer<T> {
    id: usize,
    topic: String,
    data: Mutex<Option<T>>,
    callback: Mutex<Option<Box<dyn Fn(&T) + Send + Sync>>>,
    queue: tokio::sync::mpsc::Sender<QueueMessage>,
    up_to_date: blocker::Blocker,
    has_received_message: Mutex<bool>,
    read_only: bool,
}

#[derive(thiserror::Error, Debug)]
pub enum BrevduvaError {
    #[error("A container with the name \"{0}\" already exists")]
    ContainerAlreadyExists(String),
    #[error("Failed to deserialize message")]
    MessageDeserialize(#[from] serde_json::Error),
    #[error("Failed to deserialize postcard message")]
    MessageDeserializePostcard(#[from] postcard::Error),
}

impl<T: Serialize + DeserializeOwned + Send + Sync + 'static + std::hash::Hash> SyncedContainer<T> {
    fn new(
        id: usize,
        topic: String,
        data: T,
        queue: tokio::sync::mpsc::Sender<QueueMessage>,
        read_only: bool,
        callback: Option<Box<dyn Fn(&T) + Send + Sync>>,
    ) -> Self {
        Self {
            id,
            topic,
            data: Mutex::new(Some(data)),
            queue,
            up_to_date: blocker::Blocker::new(),
            has_received_message: Mutex::new(false),
            read_only,
            callback: Mutex::new(callback),
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
        if self.read_only {
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
        if self.read_only {
            panic!("Cannot set data on a read-only container");
        }

        {
            let mut d = self.data.lock().unwrap();
            let mut hash1 = std::collections::hash_map::DefaultHasher::new();
            let mut hash2 = std::collections::hash_map::DefaultHasher::new();
            let k1 = Some(&data);
            let k2 = d.as_ref();
            k1.hash(&mut hash1);
            k2.hash(&mut hash2);
            if hash1.finish() == hash2.finish() {
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

impl<T: Serialize + DeserializeOwned + Send + Sync + std::fmt::Debug + 'static> Container
    for SyncedContainer<T>
{
    fn topic(&self) -> &str {
        &self.topic
    }

    fn read_only(&self) -> bool {
        self.read_only
    }

    fn up_to_date(&self) -> &blocker::Blocker {
        &self.up_to_date
    }

    fn on_message(&self, payload: &[u8]) -> Result<(), BrevduvaError> {
        let d = serde_json::from_slice(payload).map_err(BrevduvaError::MessageDeserialize)?;
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

    fn serialize(&self) -> String {
        let data = self.data.lock().unwrap();
        serde_json::to_string(&*data).unwrap()
    }
}

impl SyncStorage {
    #[cfg(feature = "embedded")]
    pub async fn new<'a>(
        client_id: &'a str,
        host: &'static str,
        username: &'a str,
        password: &'a str,
    ) -> Self {
        use esp_idf_svc::mqtt::client::LwtConfiguration;

        let online_status_topic = format!("devices/{client_id}/status");
        let mqtt_config = MqttClientConfiguration::<'a> {
            client_id: Some(client_id),
            username: Some(username),
            password: Some(password),
            // Make the broker remember the state for the client between connections
            disable_clean_session: true,
            lwt: Some(LwtConfiguration {
                topic: Box::leak(Box::new(format!("sync/{online_status_topic}"))),
                payload: "offline".as_bytes(),
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

        storage
            .add_container(&online_status_topic, "online".to_string())
            .await
            .unwrap();

        let storage2 = storage.clone();
        let (client, connection) = EspAsyncMqttClient::new(host, &mqtt_config).unwrap();
        tokio::spawn(async move {
            Self::start(client, connection, queue_receiver, storage2)
                .await
                .unwrap()
        });

        storage
    }

    #[cfg(feature = "edge-mqtt")]
    pub async fn new<'a>(
        client_id: &'a str,
        host: &'static str,
        username: &'a str,
        password: &'a str,
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
        mqtt_options.set_clean_session(false);
        mqtt_options.set_credentials(username, password);

        let online_status_topic = format!("devices/{client_id}/status");
        mqtt_options.set_last_will(LastWill {
            topic: format!("sync/{online_status_topic}"),
            message: "offline".to_string().into(),
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

        storage
            .add_container(&online_status_topic, "online".to_string())
            .await
            .unwrap();

        let storage2 = storage.clone();
        tokio::spawn(async move {
            Self::start(client, connection, queue_receiver, storage2)
                .await
                .unwrap()
        });

        storage
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
            if !container.has_received_message() && !container.read_only() {
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
    ) -> Result<
        (
            Arc<crate::channel::Channel<T>>,
            tokio::sync::mpsc::Receiver<T>,
        ),
        BrevduvaError,
    > {
        let topic = format!("sync/{}", name);

        let (sender, receiver) = tokio::sync::mpsc::channel::<T>(1);
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
    ) -> Result<Arc<SyncedContainer<T>>, BrevduvaError> {
        let topic = format!("sync/{}", name);
        let container = {
            let mut inner = self.inner.lock().unwrap();
            if inner.containers.iter().any(|c| c.topic() == topic) {
                return Err(BrevduvaError::ContainerAlreadyExists(name.to_string()));
            }

            let read_only = topic.contains('+') || topic.contains('#') || topic.contains('$');

            let container = Arc::new(SyncedContainer::new(
                inner.containers.len(),
                topic,
                inital,
                inner.queue_sender.clone(),
                read_only,
                None,
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
                                    if let Err(e) = container.on_message(data) {
                                        error!("{e}. Ignoring message on topic \"{topic}\"");
                                    }
                                }
                                None => {
                                    warn!("Received message on unknown topic: \"{}\"", topic);
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
                            .publish(&topic, QoS::AtMostOnce, true, data.as_bytes())
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
                            trace!("Not connected, skipping subscribe");
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
                        topics.push(meta_topic.to_owned());

                        for topic in topics {
                            while let Err(e) = client2.lock().await.subscribe(&topic, QoS::AtLeastOnce).await {
                                error!("Failed to subscribe to topic \"{topic}\": {e:?}, retrying...");

                                // Re-try in 0.5s
                                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
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
