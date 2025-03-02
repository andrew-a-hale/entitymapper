use crate::data;
use clap::ArgMatches;
use postgres::{Client, Error, NoTls, Row};
use std::ffi::OsStr;
use std::path;

pub struct Provider {
    pub uri: String,
    pub fqn_table: String,
    pub client: Client,
}

impl data::Database for Provider {
    fn load(&self, filepath: String) -> bool {
        let file = path::Path::new(&filepath);
        let ext = file.extension().and_then(OsStr::to_str);

        match ext {
            Some("csv") => println!("csv file: {}", file.to_str().unwrap()),
            Some("json") => println!("json file: {}", file.to_str().unwrap()),
            Some("jsonl") => println!("jsonl file: {}", file.to_str().unwrap()),
            Some("parquet") => println!("parquet file: {}", file.to_str().unwrap()),
            _ => unreachable!(),
        }

        println!("{}", self.fqn_table);
        true
    }

    fn get_uri(&self) -> String {
        self.uri.clone()
    }

    fn get_destination(&self) -> String {
        self.fqn_table.clone()
    }

    fn query(&mut self, sql: &str) -> Result<Vec<Row>, Error> {
        self.client.query(&sql.to_string(), &[])
    }
}

pub fn from_args(matches: &ArgMatches) -> data::Repository {
    let uri = matches
        .get_one::<String>("SERVER")
        .expect("required")
        .clone();
    let user = matches.get_one::<String>("USER").expect("required").clone();
    let password = matches
        .get_one::<String>("PASSWORD")
        .expect("required")
        .clone();
    let fqn_table = matches
        .get_one::<String>("FQN_TABLE")
        .expect("required")
        .clone();

    let db = fqn_table.split_once(".").map(|(x, _)| x).unwrap();

    let client = Client::connect(
        format!(
            "host={} user={} password={} dbname={}",
            uri, user, password, db
        )
        .as_str(),
        NoTls,
    )
    .expect("failed to connect to postgres");

    data::Repository {
        database: Box::new(Provider {
            uri,
            fqn_table,
            client,
        }),
    }
}
