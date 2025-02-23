use crate::data;
use clap::Command;

pub fn create_cmd() -> Command {
    Command::new("stub").about("create stub for file")
}

pub fn handler(repo: &mut data::Repository) {
    println!("creating stub for {}", repo.database.get_destination())
}
