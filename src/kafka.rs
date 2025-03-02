use crate::data;
use futures::executor::block_on;
use std::time::Duration;

use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};

pub struct Provider {
    pub topic: Option<String>,
    pub client: FutureProducer,
}

pub fn from_config(file_path: std::path::PathBuf) -> data::Webhook {
    let file = std::fs::File::open(file_path).expect("failed to open file");
    let kafka: serde_yaml::Value = serde_yaml::from_reader(file).expect("failed to read config");
    let uri = kafka["uri"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or("failed to parse uri from config")
        .unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &uri)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("failed to connect to kafka");

    data::Webhook {
        hook: Box::new(Provider {
            client: producer,
            topic: None,
        }),
    }
}

impl data::Hook for Provider {
    fn send(&self, key: &str, value: &str) -> bool {
        let topic = self.get_topic();
        let record = FutureRecord::to(&topic).key(key).payload(value);
        block_on(self.client.send(record, Duration::from_secs(0))).expect("failed to deliver");
        println!("sent message: {}: {}", key, value);

        true
    }

    fn set_topic(&mut self, topic: String) {
        self.topic = Some(topic)
    }

    fn get_topic(&self) -> String {
        self.topic.as_ref().unwrap().clone()
    }
}
