use crate::data;
use clap::{arg, ArgMatches, Command};

pub fn create_cmd() -> Command {
    Command::new("create")
        .about("create stub for file")
        .arg(arg!(<MAPPING> "mapping file").required(true))
        .arg(arg!(<WEBHOOK_TOPIC> "").required(true))
        .arg_required_else_help(true)
}

pub fn handler(matches: &ArgMatches, webhook: &mut data::Webhook) {
    let topic = matches
        .get_one::<String>("WEBHOOK_TOPIC")
        .expect("required");

    webhook.hook.set_topic(topic.clone());
    webhook.hook.send("hello", "world");
}
