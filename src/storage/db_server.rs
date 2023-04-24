//! rocks db 延迟消息操作封装

use std::str::FromStr;
use rocksdb::{DB, Options};
use anyhow::Result;
use chrono::Local;
use uuid::Uuid;
use crate::delay_queue::delay_message::DelayMessage;
use crate::topic_queue::topic_message::{Message, TopicMessage};
/// key 时间戳和 uuid 的分隔符
const KEY_SPLIT: &str = ":";

/// 存储的key 实体
///
/// 存储示例： delay_timestamp:uuid
pub struct Key {
    /// 到期时间戳
    pub delay_timestamp: i64,
    /// 防止相同延迟时间戳的消息被覆盖
    uuid: Uuid,
}
impl ToString for Key {
    #[inline]
    fn to_string(&self) -> String {
        format!("{}{}{}", self.delay_timestamp, KEY_SPLIT, self.uuid)
    }
}
impl FromStr for Key {
    type Err = anyhow::Error;

    #[inline]
    fn from_str(key: &str) -> Result<Self, Self::Err> {
        let results = key.split(KEY_SPLIT).collect::<Vec<&str>>();
        let delay_timestamp = i64::from_str(results[0])?;
        let uuid = Uuid::parse_str(results[1])?;
        Ok(Key {
            delay_timestamp,
            uuid,
        })
    }
}
impl Key {
    pub fn new(delay_timestamp: i64) -> Self {
        Key {
            delay_timestamp,
            uuid: Uuid::new_v4(),
        }
    }
}

/// 存储实例
pub struct StorageInstance {
    /// 嵌入式 rocksdb 实例
    db: DB,
}

impl StorageInstance {
    /// 根据存储路径创建一个实例
    pub fn new(path: &str) -> Self {
        let path = path;
        let mut option = Options::default();
        option.create_if_missing(true);
        let db = DB::open(&option, path).expect("open rocksdb failed !");
        StorageInstance {
            db,
        }
    }
    /// 使用 RocksDB 的迭代器 获取指定条数的消息
    ///
    /// count: 要获取的消息条数
    pub fn fetch_messages(&self, count: usize) -> Result<Vec<DelayMessage>> {
        let mut messages = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for ele in iter {
            if let Ok(kv) = &ele {
                let message = trans_to_delay_message(&kv.0, &kv.1)?;
                messages.push(message);
            }
            if messages.len() >= count {
                break;
            }
        }
        Ok(messages)
    }

    /// 新增消息数据
    ///
    /// 返回消息的 key
    pub fn put_message(&self, topic_message: &TopicMessage) -> Result<String> {
        let key = Key::new(topic_message.delay_timestamp).to_string();
        let message = &topic_message.message;
        self.db.put(key.as_bytes(), message.serialize()?.as_bytes())?;
        Ok(key)
    }

    /// 删除消息数据
    pub fn delete_message(&self, key: &str) -> Result<()> {
        self.db.delete(key.as_bytes())?;
        Ok(())
    }
}

/// 根据 rocksdb 迭代元素 转换为 DelayMessage
fn trans_to_delay_message(k: &[u8], v: &[u8]) -> Result<DelayMessage> {
    let key = String::from_utf8(k.to_vec())?;
    let delay_timestamp = Key::from_str(&key)?.delay_timestamp;
    let delay_time = delay_timestamp - Local::now().timestamp();

    let str_msg = String::from_utf8(v.to_vec())?;
    Ok(DelayMessage::new(Message::deserialize(&str_msg)?, delay_time))
}



/// 测试模块
#[cfg(test)]
mod tests {
    use chrono::Local;
    use crate::storage::db_server::StorageInstance;
    use crate::topic_queue::topic_message::{Message, TopicMessage};

    #[test]
    fn test_mod() {
        let db_server = StorageInstance::new("./rocksdb");
        let message = Message {
            topic: "hello".to_string(),
            body: "1".to_string(),
        };
        let message = TopicMessage::new(1683165053, message);
        let string = db_server.put_message(&message).unwrap();
        println!("{:?}", string);
        let message2 = Message {
            topic: "pello".to_string(),
            body: "2".to_string(),
        };
        let message2 = TopicMessage::new(1683165059, message2);
        let string1 = db_server.put_message(&message2).unwrap();
        println!("{:?}", string1);
        let vec = db_server.fetch_messages(5).unwrap();
        vec.iter().for_each(|message| {
            println!("{:?}", message);
        });
    }

    #[test]
    fn time_mod() {
        // 1682165032
        // 1682165053
        println!("{:?}", Local::now().timestamp());
    }
}


