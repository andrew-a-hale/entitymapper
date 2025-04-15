use crate::data;
use crate::map;
use arrow::array::RecordBatch;
use arrow::array::{Date64Array, Int64Array, PrimitiveArray, StringArray};
use arrow::datatypes;
use clap::ArgMatches;
use postgres::{Client, Error, NoTls, Row};
use serde_json::json;
use std::collections::HashMap;
use std::fmt::Write;

pub struct Provider {
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
                    "{}'{}'",
                    sep,
                    match batch.column(col).data_type().clone() {
                        datatypes::DataType::Int64 => batch
                            .column(col)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("failed to downcast")
                            .value(row)
                            .to_string(),
                        datatypes::DataType::Float64 => batch
                            .column(col)
                            .as_any()
                            .downcast_ref::<PrimitiveArray<datatypes::Float64Type>>()
                            .expect("failed to downcast")
                            .value(row)
                            .to_string(),
                        datatypes::DataType::Date64 => batch
                            .column(col)
                            .as_any()
                            .downcast_ref::<Date64Array>()
                            .expect("failed to downcast")
                            .value(row)
                            .to_string(),
                        datatypes::DataType::Utf8 => batch
                            .column(col)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .expect("failed to downcast")
                            .value(row)
                            .to_string()
                            .replace("'", "''"),
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

    fn query(&mut self, sql: &str) -> Result<Vec<Row>, Error> {
        self.client.query(&sql.to_string(), &[])
    }

    fn to_messages(&mut self, schema: map::Schema) -> Vec<data::Message> {
        let res = self.query(schema.get_sql_source());
        let mut messages: Vec<data::Message> = vec![];
        match res {
            Err(msg) => eprintln!("failed to get source: {}", msg),
            Ok(rows) => {
                rows.iter().for_each(|row| {
                    row_into_messages(row, &schema, &mut messages);
                });
            }
        }

        messages
    }
}

type Label = String;
type Value = String;
type DataType = String;
type MessageField = (Label, Value, DataType);

fn row_into_messages(row: &Row, schema: &map::Schema, messages: &mut Vec<data::Message>) {
    let mut entities: HashMap<String, Vec<MessageField>> = HashMap::new();
    let mut entity_ids: HashMap<String, String> = HashMap::new();
    let mut relationships: HashMap<String, Vec<MessageField>> = HashMap::new();

    schema
        .fields
        .fields()
        .iter()
        .enumerate()
        .for_each(|(i, field)| {
            let col_type = row.columns().get(i).map(|c| c.type_().name()).unwrap();
            let value = match col_type {
                "varchar" => row.get::<usize, String>(i),
                "int4" => row.get::<usize, i32>(i).to_string(),
                "int8" => row.get::<usize, i64>(i).to_string(),
                _ => unimplemented!(),
            };

            let label = field.metadata().get("label").unwrap();
            let data_type = field.metadata().get("dataType").unwrap();
            match field.metadata().get("entity") {
                Some(x) if x.contains("-") => {
                    relationships
                        .entry(x.clone())
                        .and_modify(|x| x.push((label.clone(), value.clone(), data_type.clone())))
                        .or_insert(vec![(label.clone(), value, data_type.clone())]);
                }
                Some(x) => {
                    if label == "sourceId" {
                        entity_ids.insert(x.clone(), value.clone());
                    }
                    entities
                        .entry(x.clone())
                        .and_modify(|x| x.push((label.clone(), value.clone(), data_type.clone())))
                        .or_insert(vec![(label.clone(), value, data_type.clone())]);
                }
                None => {
                    unimplemented!()
                }
            };
        });

    entities.iter().for_each(|(k, v)| {
        let tmp = k.replace("!", ".");
        let mut tmp_iter = tmp.split(".");
        let type_ = tmp_iter.next().unwrap();
        let sub_type = tmp_iter.next().unwrap();
        let set_id = tmp_iter.next().unwrap_or("");
        let id = entity_ids.get(k).unwrap();
        let key = format!("{}.{}.{}", type_, sub_type, id);

        let props: serde_json::Value = v
            .iter()
            .map(|prop| {
                json!({
                    "label": prop.0,
                    "value": prop.1,
                    "dataType": prop.2,
                })
            })
            .collect();

        messages.push(data::Message {
            key,
            value: json!({
                "fqn": k,
                "type": type_,
                "subType": sub_type,
                "setId": set_id,
                "sourceId": id,
                "props": props,
            })
            .to_string(),
        });
    });

    schema.relationships.iter().for_each(|x| {
        let reference_split: Vec<&str> = x.reference.split("-").collect();
        let from = reference_split.first().to_owned().unwrap();
        let to = &reference_split.last().unwrap().to_owned();
        let from_id = entity_ids.get(*from).unwrap();
        let to_id = entity_ids.get(*to).unwrap();
        let key = format!("{}.{}-{}", x.reference, from_id, to_id);
        let props: serde_json::Value = relationships
            .get(&x.reference)
            .unwrap()
            .iter()
            .map(|prop| {
                json!({
                    "label": prop.0,
                    "value": prop.1,
                    "dataType": prop.2,
                })
            })
            .collect();

        messages.push(data::Message {
            key,
            value: json!({
                "relType": x.label,
                "fromId": from_id,
                "toId": to_id,
                "props": props,
            })
            .to_string(),
        });
    })
}

fn schema_to_ddl(fqn_name: &String, schema: datatypes::SchemaRef) -> Vec<String> {
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
                datatypes::DataType::Utf8 => writeln!(ddl, "\t{}{} varchar", leading, x.name()),
                datatypes::DataType::Boolean => writeln!(ddl, "\t{}{} bool", leading, x.name()),
                datatypes::DataType::Int64 => writeln!(ddl, "\t{}{} int", leading, x.name()),
                datatypes::DataType::Float64 => writeln!(ddl, "\t{}{} float", leading, x.name()),
                datatypes::DataType::Date64 => writeln!(ddl, "\t{}{} bigint", leading, x.name()),
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
        database: Box::new(Provider { fqn_table, client }),
    }
}
