use crate::data;
use crate::map;
use clap::{arg, ArgMatches, Command};

pub fn create_cmd() -> Command {
    Command::new("create")
        .about("create stub for file")
        .arg(arg!(<WEBHOOK_TOPIC> "webhook topic").required(true))
        .arg_required_else_help(true)
}

pub fn handler(
    matches: &ArgMatches,
    db: &mut data::Repository,
    webhook: &mut data::Webhook,
    mapping: map::Schema,
) {
    let topic = matches
        .get_one::<String>("WEBHOOK_TOPIC")
        .expect("required");

    let messages = db.database.to_messages(mapping);
    webhook.hook.set_topic(topic.clone());
    webhook.hook.send(messages);
}
