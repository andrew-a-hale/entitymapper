use crate::data;
use clap::{arg, ArgMatches, Command};

pub fn create_cmd() -> Command {
    Command::new("load")
        .about("load file to database")
        .arg(arg!(<FILE> "file to load"))
        .arg_required_else_help(true)
}

pub fn handler(matches: &ArgMatches, repo: &mut data::Repository) {
    match repo.database.load() {
        true => println!(
            "loading {} to {}",
            matches.get_one::<String>("FILE").expect("required"),
            repo.database.get_destination(),
        ),
        false => println!(
            "nothing to load to {} to {:?}",
            matches.get_one::<String>("FILE").expect("required"),
            repo.database.get_destination(),
        ),
    }
}
