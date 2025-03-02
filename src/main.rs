use clap::{arg, Command};
use std::path;
mod create;
mod data;
mod kafka;
mod load;
mod postgres;

fn cli() -> Command {
    Command::new("em")
        .about("EntityMapper CLI")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .arg(arg!(<SERVER> "database url").required(true))
        .arg(arg!(<USER> "user").required(true))
        .arg(arg!(<PASSWORD> "password").required(true))
        .arg(arg!(<FQN_TABLE> "database fully qualified name for destination table").required(true))
        .subcommand(load::create_cmd())
        .subcommand(create::create_cmd())
}

fn main() {
    let matches = cli().get_matches();

    let db = &mut postgres::from_args(&matches);
    let wh = &mut kafka::from_config(path::PathBuf::from("hook.yml"));

    match matches.subcommand() {
        Some(("load", sub_matches)) => load::handler(sub_matches, db),
        Some(("create", sub_matches)) => create::handler(sub_matches, db, wh),
        _ => unreachable!(),
    }
}
