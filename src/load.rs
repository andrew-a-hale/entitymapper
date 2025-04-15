use crate::data;
use crate::map;
use arrow::array::RecordBatch;
use clap::{arg, ArgMatches, Command};
use reqwest::header::CONTENT_TYPE;
use std::fs;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use url::Url;

pub fn create_cmd() -> Command {
    Command::new("load")
        .about("load file to database")
        .arg(arg!(<SOURCE> "file to load or url"))
        .arg_required_else_help(true)
}

pub fn handle_file(path: &Path, mapping: &map::Schema) -> Option<RecordBatch> {
    if !path.is_file() {
        eprintln!("failed to find file")
    }

    let buf = fs::read(path).unwrap();

    match path.extension() {
        Some(x) if x == "csv" => handle_csv(buf, mapping),
        Some(x) if x == "json" => handle_json(buf, mapping),
        Some(x) if x == "jsonl" => handle_json(buf, mapping),
        Some(_) => unimplemented!(),
        None => None,
    }
}

fn handle_csv(content: Vec<u8>, mapping: &map::Schema) -> Option<RecordBatch> {
    let buf = Cursor::new(content);
    let mut csv = arrow_csv::reader::ReaderBuilder::new(Arc::new(mapping.fields.clone()))
        .with_header(true)
        .with_escape(b'"')
        .build(buf)
        .unwrap();

    Some(csv.next().unwrap().unwrap())
}

fn handle_json(content: Vec<u8>, mapping: &map::Schema) -> Option<RecordBatch> {
    let buf = Cursor::new(content);
    let mut csv = arrow_json::reader::ReaderBuilder::new(Arc::new(mapping.fields.clone()))
        .build(buf)
        .unwrap();

    Some(csv.next().unwrap().unwrap())
}

fn handle_url(path: Url, mapping: &map::Schema) -> Option<RecordBatch> {
    if let Ok(mut x) = reqwest::blocking::get(path) {
        let mut buf: Vec<u8> = vec![];
        let _ = x.copy_to(&mut buf);

        let content_type_str = x
            .headers()
            .get(CONTENT_TYPE)
            .expect("invalid http headers")
            .to_str()
            .expect("invalid utf-8")
            .split_once(";")
            .expect("found type")
            .0
            .split_once("/")
            .expect("found type")
            .1;

        match content_type_str {
            "csv" => handle_csv(buf, mapping),
            "plain" => handle_csv(buf, mapping),
            "json" => handle_json(buf, mapping),
            _ => unimplemented!(),
        }
    } else {
        None
    }
}

pub fn handler(matches: &ArgMatches, repo: &mut data::Repository, mapping: map::Schema) {
    let source = matches.get_one::<String>("SOURCE").expect("required");
    let path = Path::new(source);

    let batch: Option<RecordBatch> = if path.exists() {
        handle_file(path, &mapping)
    } else {
        match Url::parse(source) {
            Ok(x) => handle_url(x, &mapping),
            Err(_) => None,
        }
    };

    match batch {
        Some(x) => repo.database.load(x),
        None => eprintln!("failed to load source"),
    };
}
