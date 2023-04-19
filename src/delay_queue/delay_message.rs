use crate::topic_queue::topic_message::Message;

#[derive(Debug, Clone)]
pub struct DelayMessage {
    pub message: Message,
    pub delay_time: i64,
}
impl DelayMessage {
    pub fn new(message: Message, delay_time: i64) -> Self {
        DelayMessage {
            message,
            delay_time,
        }
    }
}