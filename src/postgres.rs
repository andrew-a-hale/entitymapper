use crate::data;
use arrow::array::{PrimitiveArray, StringArray};
use arrow::datatypes::{DataType, Float64Type};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use clap::ArgMatches;
use postgres::{Client, Error, NoTls, Row};
use std::fmt::Write;

pub struct Provider {
    // pub uri: String,
    pub fqn_table: String,
    pub client: Client,
}

impl data::Database for Provider {
    fn load(&mut self, batch: RecordBatch) {
        let cols: Vec<String> = batch
            .schema()
            .fields()
            .clone()
            .iter()
            .map(|field| field.name().clone())
            .collect();

        let mut dml = String::new();
        writeln!(dml, "insert into {} ({})", self.fqn_table, cols.join(", "))
            .expect("failed to write sql insert");

        writeln!(dml, "values").expect("failed to write values");

        for row in 0..batch.num_rows() {
            let sep = if row == 0 { "" } else { ", " };
            write!(dml, "\t{}(", sep).expect("failed to write values");
            for col in 0..batch.num_columns() {
                let sep = if col == 0 { "" } else { ", " };
                write!(
                    dml,
                    "{}{}",
                    sep,
                    match batch.column(col).data_type().clone() {
                        DataType::Float64 => batch
                            .column(col)
                            .as_any()
                            .downcast_ref::<PrimitiveArray<Float64Type>>()
                            .expect("failed to downcast")
                            .value(row)
                            .to_string(),
                        DataType::Utf8 => {
                            let v = batch
                                .column(col)
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .expect("failed to downcast")
                                .value(row)
                                .to_string();
                            format!("'{}'", v)
                        }
                        _ => unimplemented!(),
                    }
                )
                .expect("failed to write values");
            }
            writeln!(dml, ")").expect("failed to write values");
        }

        let mut sql = schema_to_ddl(&self.fqn_table, batch.schema());
        sql.push(dml);
        sql.iter().for_each(|sql| {
            self.client
                .query(sql, &[])
                .expect("failed to execute query");
        });
    }

    // fn get_uri(&self) -> String {
    //     self.uri.clone()
    // }

    // fn get_destination(&self) -> String {
    //     self.fqn_table.clone()
    // }

    fn query(&mut self, sql: &str) -> Result<Vec<Row>, Error> {
        self.client.query(&sql.to_string(), &[])
    }
}

fn schema_to_ddl(fqn_name: &String, schema: SchemaRef) -> Vec<String> {
    let mut create = String::new();
    writeln!(
        create,
        "create schema if not exists \"{}\";",
        fqn_name.split(".").nth(1).unwrap()
    )
    .unwrap();

    let mut drop = String::new();
    writeln!(drop, "drop table if exists {};", fqn_name).unwrap();

    let mut ddl = String::new();
    writeln!(ddl, "create table {} (", fqn_name).unwrap();
    schema
        .fields()
        .into_iter()
        .enumerate()
        .map(|(i, x)| {
            let leading = if i == 0 { "" } else { ", " };
            match x.data_type() {
                DataType::Utf8 => writeln!(ddl, "\t{}{} varchar", leading, x.name()),
                DataType::Boolean => writeln!(ddl, "\t{}{} bool", leading, x.name()),
                DataType::Int64 => writeln!(ddl, "\t{}{} int", leading, x.name()),
                DataType::Float64 => writeln!(ddl, "\t{}{} float", leading, x.name()),
                _ => unimplemented!(),
            }
        })
        .for_each(|_| {});
    writeln!(ddl, ");").unwrap();

    vec![create, drop, ddl]
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
            // uri,
            fqn_table,
            client,
        }),
    }
}
