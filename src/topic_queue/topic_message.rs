use serde::{Deserialize, Serialize};
use anyhow::Result;

#[derive(Debug)]
pub struct TopicMessage {
    pub delay_timestamp: i64,
    pub message: Message,
}

impl TopicMessage {
    pub fn new(timestamp: i64, message: Message) -> Self {
        TopicMessage {
            delay_timestamp: timestamp,
            message,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Message {
    pub topic: String,
    pub body: String,
}
impl Message {
    pub fn deserialize(json: &str) -> Result<Self> {
        Ok(serde_json::from_str(json)?)
    }
    pub fn serialize(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }
}