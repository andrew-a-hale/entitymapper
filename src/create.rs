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
    _mapping: map::Schema,
) {
    let rows = db.database.query("select 1 as id");
    println!("{:?}", rows);
    let topic = matches
        .get_one::<String>("WEBHOOK_TOPIC")
        .expect("required");

    webhook.hook.set_topic(topic.clone());
    webhook.hook.send("hello", "world");
}
