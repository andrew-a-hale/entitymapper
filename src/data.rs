use arrow::array::RecordBatch;
use postgres::{Error, Row};

use crate::map;

pub struct Repository {
    pub database: Box<dyn Database>,
}

pub trait Database {
    fn load(&mut self, batch: RecordBatch);
    fn query(&mut self, sql: &str) -> Result<Vec<Row>, Error>;
    fn to_messages(&mut self, schema: map::Schema) -> Vec<Message>;
}

#[derive(Debug, Clone)]
pub struct Message {
    pub key: String,
    pub value: String,
}

pub struct Webhook {
    pub hook: Box<dyn Hook>,
}

pub trait Hook {
    fn _send(&self, msg: Message) -> bool;
    fn send(&self, msgs: Vec<Message>) -> bool;
    fn set_topic(&mut self, topic: String);
    fn get_topic(&self) -> String;
}
