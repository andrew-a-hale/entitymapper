use crate::data;
use clap::{arg, ArgMatches, Command};

pub fn create_cmd() -> Command {
    Command::new("load")
        .about("load file to database")
        .arg(arg!(<SOURCE> "file to load or url"))
        .arg_required_else_help(true)
}

pub fn handler(matches: &ArgMatches, repo: &mut data::Repository) {
    let source = matches.get_one::<String>("SOURCE").expect("required");

    match repo.database.load(source.clone()) {
        true => println!(
            "loading {} to {}",
            matches.get_one::<String>("SOURCE").expect("required"),
            repo.database.get_destination(),
        ),
        false => println!(
            "nothing to load to {} to {:?}",
            matches.get_one::<String>("SOURCE").expect("required"),
            repo.database.get_destination(),
        ),
    }
}
