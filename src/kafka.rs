use crate::data;
use futures::executor::block_on;
use std::time::Duration;

use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};

pub struct Provider {
    pub topic: Option<String>,
    pub producer: FutureProducer,
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
            producer,
            topic: None,
        }),
    }
}

impl data::Hook for Provider {
    fn _send(&self, msg: data::Message) -> bool {
        let topic = self.get_topic();
        let record = FutureRecord::to(&topic).key(&msg.key).payload(&msg.value);
        block_on(self.producer.send(record, Duration::from_secs(0))).expect("failed to deliver");
        println!("sent message: {}: {}", msg.key, msg.value);

        true
    }

    fn send(&self, msgs: Vec<data::Message>) -> bool {
        let sent = msgs
            .iter()
            .map(|msg| self._send(msg.clone()))
            .collect::<Vec<bool>>();

        !sent.contains(&false)
    }

    fn set_topic(&mut self, topic: String) {
        self.topic = Some(topic)
    }

    fn get_topic(&self) -> String {
        self.topic.as_ref().unwrap().clone()
    }
}
