pub struct Repository {
    pub database: Box<dyn Database>,
}

pub trait Database {
    fn load(&self) -> bool;
    fn get_uri(&self) -> String;
    fn get_destination(&self) -> String;
}

pub struct Webhook {
    pub hook: Box<dyn Hook>,
}

pub trait Hook {
    fn send(&self, key: &str, value: &str) -> bool;
    fn set_topic(&mut self, topic: String);
    fn get_topic(&self) -> String;
}
