//! rocks db server


use std::str::FromStr;
use rocksdb::{DB, Options};
use anyhow::Result;
use chrono::Local;
use crate::delay_queue::delay_message::DelayMessage;
use crate::topic_queue::topic_message::{Message, TopicMessage};

const KEY_SPLIT: &str = ":";

pub struct RocksDBServer {
    db_server: DB,
}

impl RocksDBServer {
    pub fn new() -> Self {
        let path = "./rocksdb";
        let mut option = Options::default();
        option.create_if_missing(true);
        let db = DB::open(&option, path).expect("open rocksdb failed");
        RocksDBServer {
            db_server: db,
        }
    }
    /// 使用 RocksDB 的迭代器 获取指定条数的消息
    pub fn fetch_messages(&self, count: usize) -> Result<Vec<DelayMessage>> {
        let mut messages = Vec::new();
        let mut iter = self.db_server.iterator(rocksdb::IteratorMode::Start);
        while let Some(ele) = iter.next() {
            if let Ok(kv) = &ele {
                let message = Self::trans_to_delay_message(&kv.0, &kv.1)?;
                messages.push(message);
            }
            if messages.len() >= count {
                break;
            }
        }
        Ok(messages)
    }

    fn trans_to_delay_message(k: &Box<[u8]>, v: &Box<[u8]>) -> Result<DelayMessage> {
        let key = String::from_utf8(k.to_vec())?;
        let split = key.split(KEY_SPLIT).collect::<Vec<&str>>();

        let delay_timestamp = i64::from_str(split[0])?;
        let now = Local::now().timestamp();
        let delay_time = now - delay_timestamp;

        let body = String::from_utf8(v.to_vec())?;
        let message = DelayMessage::new(Message::deserialize(&body)?, delay_time);
        Ok(message)
    }

    /// 新增消息数据
    pub fn put_message(&self, topic_message: &TopicMessage) -> Result<()> {
        let key = format!("{}{}{}",
                          &topic_message.delay_timestamp,
                          KEY_SPLIT,
                          uuid::Uuid::new_v4().to_string());
        let message = &topic_message.message;
        self.db_server.put(key.as_bytes(), message.serialize()?.as_bytes())?;
        Ok(())
    }

    /// 删除消息数据
    pub fn delete_message(&self, key: &str) -> Result<()> {
        self.db_server.delete(key.as_bytes())?;
        Ok(())
    }
}
