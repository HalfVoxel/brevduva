// #![deny(clippy::future_not_send)]
mod blocker;

use std::sync::{Arc, Mutex};

use embedded_svc::mqtt::client::asynch;
use embedded_svc::mqtt::client::Event;
use embedded_svc::mqtt::client::EventPayload;
use embedded_svc::mqtt::client::QoS;
#[cfg(feature = "embedded")]
use esp_idf_svc::mqtt::client::{EspAsyncMqttClient, MqttClientConfiguration};
use log::{error, info, warn};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::pin::pin;

const META_TOPIC: &str = "sync/meta";

#[derive(Clone)]
pub struct SyncStorage {
    inner: Arc<Mutex<SyncStorageInner>>,
}

struct SyncStorageInner {
    containers: Vec<Arc<dyn Container>>,
    queue_sender: tokio::sync::mpsc::Sender<QueueMessage>,
}

trait Container: Send + Sync + 'static {
    fn topic(&self) -> &str;
    fn on_message(&self, payload: &[u8]);
    fn serialize(&self) -> String;
    fn has_received_message(&self) -> bool;
    fn up_to_date(&self) -> &blocker::Blocker;
}

#[derive(Debug)]
enum QueueMessage {
    SyncContainer { container_id: usize },
    Subscribe { container_id: usize },
}

#[derive(Serialize, Deserialize)]
enum MetaMessage {
    ReceivedAllMessages { topic: String },
}

pub struct SyncedContainer<T> {
    id: usize,
    topic: String,
    data: Mutex<Option<T>>,
    queue: tokio::sync::mpsc::Sender<QueueMessage>,
    up_to_date: blocker::Blocker,
    has_received_message: Mutex<bool>,
}

#[derive(thiserror::Error, Debug)]
pub enum BrevduvaError {
    #[error("A container with the name \"{0}\" already exists")]
    ContainerAlreadyExists(String),
}

impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> SyncedContainer<T> {
    fn new(
        id: usize,
        topic: String,
        data: T,
        queue: tokio::sync::mpsc::Sender<QueueMessage>,
    ) -> Self {
        Self {
            id,
            topic,
            data: Mutex::new(Some(data)),
            queue,
            up_to_date: blocker::Blocker::new(),
            has_received_message: Mutex::new(false),
        }
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
        {
            let mut d = self.data.lock().unwrap();
            if let Some(d) = d.as_mut() {
                f(d);
            }
        }
        info!("Sending sync message for container {}", self.topic);
        self.queue
            .send(QueueMessage::SyncContainer {
                container_id: self.id,
            })
            .await
            .unwrap();
    }

    pub async fn set(&self, data: T) {
        {
            let mut d = self.data.lock().unwrap();
            *d = Some(data);
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

    fn up_to_date(&self) -> &blocker::Blocker {
        &self.up_to_date
    }

    fn on_message(&self, payload: &[u8]) {
        let d = serde_json::from_slice(payload).unwrap();
        let mut data = self.data.lock().unwrap();
        *data = Some(d);
        println!("Received message: {:?}", data);
        *self.has_received_message.lock().unwrap() = true;
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
        let mqtt_config = MqttClientConfiguration::<'a> {
            client_id: Some(client_id),
            username: Some(username),
            password: Some(password),
            // Make the broker remember the state for the client between connections
            disable_clean_session: true,
            ..Default::default()
        };

        let (queue_sender, queue_receiver) = tokio::sync::mpsc::channel::<QueueMessage>(4);
        let storage = Self {
            inner: Arc::new(Mutex::new(SyncStorageInner {
                containers: Vec::new(),
                queue_sender,
            })),
        };

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
        let host = host
            .strip_prefix("mqtt://")
            .expect("Host should start with 'mqtt://'");
        let (host, port) = host
            .rsplit_once(":")
            .expect("Host should be on the format 'mqtt://host:port'");
        println!("Connecting to {} : {}", host, port);
        let port: u16 = port.parse().unwrap();
        let mut mqtt_options = edge_mqtt::io::MqttOptions::new(client_id, host, port);

        mqtt_options.set_keep_alive(core::time::Duration::from_secs(10));
        mqtt_options.set_clean_session(false);
        mqtt_options.set_credentials(username, password);

        let (rumqttc_client, rumqttc_eventloop) = edge_mqtt::io::AsyncClient::new(mqtt_options, 10);

        let client = edge_mqtt::io::MqttClient::new(rumqttc_client);
        let connection = edge_mqtt::io::MqttConnection::new(rumqttc_eventloop);

        let (queue_sender, queue_receiver) = tokio::sync::mpsc::channel::<QueueMessage>(4);
        let storage = Self {
            inner: Arc::new(Mutex::new(SyncStorageInner {
                containers: Vec::new(),
                queue_sender,
            })),
        };

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
            if !container.has_received_message() {
                info!(
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

    pub async fn add_container<
        T: Serialize + DeserializeOwned + Send + Sync + std::fmt::Debug + 'static,
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

            let container = Arc::new(SyncedContainer::new(
                inner.containers.len(),
                topic,
                inital,
                inner.queue_sender.clone(),
            ));
            inner.containers.push(container.clone());
            container
        };

        container
            .queue
            .send(QueueMessage::Subscribe {
                container_id: container.id,
            })
            .await
            .unwrap();

        Ok(container)
    }

    async fn start<C: asynch::Client + asynch::Publish + Send, Conn: asynch::Connection + Send>(
        client: C,
        mut connection: Conn,
        mut queue: tokio::sync::mpsc::Receiver<QueueMessage>,
        storage: SyncStorage,
    ) -> Result<(), C::Error> {
        // Downgrade to a weak reference, so that we don't keep the storage alive indefinitely just by running this task
        let weak_storage = Arc::downgrade(&storage.inner);
        drop(storage);
        let storage = weak_storage;

        let t0 = std::time::Instant::now();
        let (reconnect_sender, mut reconnect_receiver) = tokio::sync::mpsc::channel::<()>(2);

        let client = Arc::new(tokio::sync::Mutex::new(client));
        let client2 = client.clone();

        // We to immediately start pumping the connection for messages, or else subscribe() and publish() below will not work.
        // I think the channel has limited capacity, so if we don't listen for messages all the time, we can end up in deadlocks.
        //
        // Note also that if you go to http://tools.emqx.io/ and then connect and send a message to topic
        // "esp-mqtt-demo", the client configured here should receive it.
        let storage2 = storage.clone();
        let listen = pin!(async move {
            while let Ok(event) = connection.next().await {
                let t = t0.elapsed();
                let payload = event.payload();

                match payload {
                    EventPayload::Connected(_) => {
                        info!("Connected to MQTT broker");
                        reconnect_sender.send(()).await.unwrap();
                    }
                    EventPayload::Received {
                        topic: Some(topic),
                        data,
                        ..
                    } => {
                        if topic == META_TOPIC {
                            let meta: MetaMessage = serde_json::from_slice(data).unwrap();
                            match meta {
                                MetaMessage::ReceivedAllMessages { topic } => {
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
                                            info!("Container \"{}\" has received all pending messages", topic);
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
                                    info!("Received message on topic \"{}\"", topic);
                                    container.on_message(data);

                                    // Should already be unblocked, but just in case
                                    // container.up_to_date().unblock().await;
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
                        warn!("[Queue] Event: {} at {}", payload, t.as_millis());
                    }
                }
            }

            warn!("Connection closed");

            Result::<(), C::Error>::Ok(())
        });

        let storage3 = storage.clone();
        let publish = pin!(async move {
            info!("Running publish loop");
            while let Some(event) = queue.recv().await {
                info!("Got publish event {:?}", event);
                match event {
                    QueueMessage::SyncContainer { container_id } => {
                        info!("Publishing container {}", container_id);
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
                            .publish(&topic, QoS::AtLeastOnce, true, data.as_bytes())
                            .await?;
                    }
                    QueueMessage::Subscribe { container_id } => {
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
                            client
                                .publish(
                                    META_TOPIC,
                                    QoS::AtMostOnce,
                                    false,
                                    &serde_json::to_vec(&MetaMessage::ReceivedAllMessages {
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
                    while reconnect_receiver.recv().await.is_some() {
                        info!("Connected, subscribing to all topics");
                        let mut topics = {
                            if let Some(inner) = storage.upgrade() {
                                let inner = inner.lock().unwrap();
                                inner.containers.iter().map(|c| c.topic().to_owned()).collect::<Vec<_>>()
                            } else {
                                break;
                            }
                        };
                        topics.push(META_TOPIC.to_owned());

                        for topic in topics {
                            while let Err(e) = client2.lock().await.subscribe(&topic, QoS::AtLeastOnce).await {
                                error!("Failed to subscribe to topic \"{topic}\": {e:?}, retrying...");

                                // Re-try in 0.5s
                                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                            }

                            if topic != META_TOPIC {
                                client2
                                    .lock()
                                    .await
                                    .publish(
                                        META_TOPIC,
                                        QoS::AtMostOnce,
                                        false,
                                        &serde_json::to_vec(&MetaMessage::ReceivedAllMessages {
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
