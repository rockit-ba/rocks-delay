










/// 测试模块
#[cfg(test)]
mod tests {
    use chrono::Local;
    use crate::storage::db_server::RocksDBServer;
    use crate::topic_queue::topic_message::{Message, TopicMessage};

    #[test]
    fn test_mod() {
        let db_server = RocksDBServer::new();
        let message = Message {
            topic: "hello".to_string(),
            body: "world".to_string(),
        };
        let message = TopicMessage::new(1681825478, message);
        db_server.put_message(&message).unwrap();
        let message2 = Message {
            topic: "pello".to_string(),
            body: "world".to_string(),
        };
        let message2 = TopicMessage::new(1681825478, message2);
        db_server.put_message(&message2).unwrap();

        let vec = db_server.fetch_messages(10).unwrap();
        println!("{:?}", vec);
    }

    #[test]
    fn time_mod() {
        println!("{:?}", Local::now().timestamp());
    }
}