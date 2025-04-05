use arrow::datatypes::{DataType, Field};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;

#[derive(Serialize, Deserialize, Debug)]
enum MapFieldType {
    Int64,
    String,
    Float64,
}

// - label: source_id
//   dataType: int
//   entity: PERSON.Person
#[derive(Serialize, Deserialize, Debug)]
pub struct MapField {
    label: String,
    data_type: MapFieldType,
    entity: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Map {
    fields: Vec<MapField>,
    relationships: Vec<String>,
    source: HashMap<String, String>,
}

#[derive(Debug)]
pub struct Schema {
    pub fields: arrow::datatypes::Schema,
    relationships: Vec<String>,
    source: HashMap<String, String>,
}

pub fn from_mapping(file: File) -> Schema {
    let map: Map = serde_yaml::from_reader(file).expect("failed to serialise yaml");
    let fields: Vec<Field> = map
        .fields
        .iter()
        .map(|dict| match dict.data_type {
            MapFieldType::Int64 => Field::new(dict.label.clone(), DataType::Int64, false),
            MapFieldType::Float64 => Field::new(dict.label.clone(), DataType::Float64, false),
            MapFieldType::String => Field::new(dict.label.clone(), DataType::Utf8, false),
        })
        .collect();

    Schema {
        fields: arrow::datatypes::Schema::new(fields),
        relationships: map.relationships,
        source: map.source,
    }
}
