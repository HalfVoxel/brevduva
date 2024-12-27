use std::sync::Mutex;

use embedded_svc::mqtt::client::QoS;
use serde::{de::DeserializeOwned, Serialize};

use crate::{blocker, Container, QueueMessage};

enum SerializationFormat {
    Postcard,
    Json,
}

pub struct Channel<T> {
    id: usize,
    topic: String,
    queue: tokio::sync::mpsc::Sender<QueueMessage>,
    channel: tokio::sync::mpsc::Sender<T>,
    up_to_date: blocker::Blocker,
    read_only: bool,
    has_received_message: Mutex<bool>,
    format: SerializationFormat,
}

#[async_trait::async_trait]
impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> Container for Channel<T> {
    fn topic(&self) -> &str {
        &self.topic
    }

    async fn on_message(&self, payload: &'_ [u8]) -> Result<(), crate::BrevduvaError> {
        let message: T = match self.format {
            SerializationFormat::Postcard => postcard::from_bytes(payload)?,
            SerializationFormat::Json => serde_json::from_slice(payload)?,
        };
        *self.has_received_message.lock().unwrap() = true;

        self.channel.send(message).await.unwrap();
        Ok(())
    }

    fn serialize(&self) -> String {
        panic!("Cannot serialize a channel");
    }

    fn has_received_message(&self) -> bool {
        *self.has_received_message.lock().unwrap()
    }

    fn up_to_date(&self) -> &blocker::Blocker {
        &self.up_to_date
    }

    fn read_only(&self) -> bool {
        self.read_only
    }
}

impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> Channel<T> {
    pub fn id(&self) -> usize {
        self.id
    }

    pub(crate) async fn subscribe(&self) {
        self.queue
            .send(QueueMessage::Subscribe {
                container_id: self.id,
            })
            .await
            .unwrap();
    }

    pub(crate) fn new(
        id: usize,
        topic: String,
        queue: tokio::sync::mpsc::Sender<QueueMessage>,
        read_only: bool,
        sender: tokio::sync::mpsc::Sender<T>,
    ) -> Self {
        Self {
            id,
            topic,
            queue,
            up_to_date: blocker::Blocker::new(),
            has_received_message: Mutex::new(false),
            read_only,
            channel: sender,
            format: if std::any::TypeId::of::<T>() == std::any::TypeId::of::<Vec<u8>>() {
                SerializationFormat::Postcard
            } else {
                SerializationFormat::Json
            },
        }
    }

    pub async fn wait_for_sync(&self) {
        self.up_to_date.wait().await;
    }

    pub async fn send(&self, message: T) {
        if self.read_only {
            panic!("Cannot send to a read-only channel");
        }

        let message = match self.format {
            SerializationFormat::Postcard => postcard::to_stdvec(&message).unwrap(),
            SerializationFormat::Json => serde_json::to_vec(&message).unwrap(),
        };
        self.queue
            .send(QueueMessage::PublishOnChannel {
                container_id: self.id,
                data: message,
                qos: QoS::AtMostOnce,
            })
            .await
            .unwrap();
    }
}
