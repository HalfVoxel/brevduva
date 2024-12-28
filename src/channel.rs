use std::sync::Mutex;

use embedded_svc::mqtt::client::QoS;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    blocker,
    raw_deserializer::{self},
    Container, QueueMessage,
};

#[derive(Copy, Clone)]
pub(crate) enum SerializationFormat {
    Postcard,
    Json,
    String,
}

pub struct Channel<T: 'static> {
    id: usize,
    topic: String,
    queue: tokio::sync::mpsc::Sender<QueueMessage>,
    channel: tokio::sync::mpsc::Sender<ChannelMessage<T>>,
    up_to_date: blocker::Blocker,
    read_only: bool,
    has_received_message: Mutex<bool>,
    format: SerializationFormat,
}

#[derive(Clone)]
pub struct ChannelMessage<T: 'static> {
    pub topic: String,
    pub message: T,
}

#[async_trait::async_trait]
impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> Container for Channel<T> {
    fn topic(&self) -> &str {
        &self.topic
    }

    async fn on_message(&self, topic: &str, payload: &[u8]) -> Result<(), crate::BrevduvaError> {
        let message: T = deserialize_from_slice(payload, self.format)?;
        *self.has_received_message.lock().unwrap() = true;

        self.channel
            .send(ChannelMessage {
                topic: topic.to_string(),
                message,
            })
            .await
            .unwrap();
        Ok(())
    }

    fn serialize(&self) -> Vec<u8> {
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

    fn can_sync_state(&self) -> bool {
        false
    }
}

pub(crate) fn auto_serialization_format<T: 'static>() -> SerializationFormat {
    if std::any::TypeId::of::<T>() == std::any::TypeId::of::<Vec<u8>>() {
        SerializationFormat::Postcard
    // } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<String>() {
    //     SerializationFormat::String
    } else {
        SerializationFormat::Json
    }
}

pub(crate) fn serialize_to_vec<T: Serialize>(
    value: &T,
    format: SerializationFormat,
) -> Result<Vec<u8>, crate::BrevduvaError> {
    match format {
        SerializationFormat::Postcard => Ok(postcard::to_stdvec(value)?),
        SerializationFormat::Json => Ok(serde_json::to_vec(value)?),
        SerializationFormat::String => Ok(crate::raw_serializer::to_vec(value)?),
    }
}

pub(crate) fn deserialize_from_slice<T: DeserializeOwned>(
    input: &[u8],
    format: SerializationFormat,
) -> Result<T, crate::BrevduvaError> {
    match format {
        SerializationFormat::Postcard => Ok(postcard::from_bytes(input)?),
        SerializationFormat::Json => Ok(serde_json::from_slice(input)?),
        SerializationFormat::String => Ok(raw_deserializer::from_slice(input)?),
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
        sender: tokio::sync::mpsc::Sender<ChannelMessage<T>>,
    ) -> Self {
        Self {
            id,
            topic,
            queue,
            up_to_date: blocker::Blocker::new(),
            has_received_message: Mutex::new(false),
            read_only,
            channel: sender,
            format: auto_serialization_format::<T>(),
        }
    }

    pub async fn wait_for_sync(&self) {
        self.up_to_date.wait().await;
    }

    pub async fn send(&self, message: T) {
        if self.read_only {
            panic!("Cannot send to a read-only channel");
        }

        let payload = serialize_to_vec(&message, self.format).unwrap();
        self.queue
            .send(QueueMessage::PublishOnChannel {
                container_id: self.id,
                data: payload,
                qos: QoS::AtMostOnce,
            })
            .await
            .unwrap();
    }
}
