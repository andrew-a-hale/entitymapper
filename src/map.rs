use arrow::datatypes::{DataType, Field};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::File;

enum SourceKind {
    Sql,
}

#[derive(Serialize, Deserialize, Debug)]
enum MapFieldType {
    Int,
    String,
    Float,
    Date,
}

impl MapFieldType {
    fn as_string(&self) -> String {
        match self {
            MapFieldType::Int => "Int",
            MapFieldType::String => "String",
            MapFieldType::Float => "Float",
            MapFieldType::Date => "Date",
        }
        .to_string()
    }
}

// - label: sourceId
//   labelOverride: source_id
//   dataType: String
//   entity: PERSON.Person!0
#[derive(Serialize, Deserialize, Debug)]
#[allow(non_snake_case)]
pub struct MapField {
    label: String,
    labelOverride: Option<String>,
    dataType: MapFieldType,
    reference: Option<String>,
}

// - label: PersonResidesAtAddress
//   pattern: (PERSON:Person)-[RESIDES_AT]->(LOCATION:Address)
//   props:
//   - residencyStartDate
//   - residencyEndDate
#[derive(Serialize, Deserialize, Debug)]
pub struct Relationship {
    pub label: String,
    pub reference: String,
    pub props: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Map {
    fields: Vec<MapField>,
    relationships: Option<Vec<Relationship>>,
    source: HashMap<String, String>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct Schema {
    pub fields: arrow::datatypes::Schema,
    pub relationships: Vec<Relationship>,
    pub source: HashMap<String, String>,
}

impl Schema {
    fn get_source(&self, key: SourceKind) -> &String {
        match key {
            SourceKind::Sql => self.source.get("sql").unwrap(),
        }
    }

    pub fn get_sql_source(&self) -> &String {
        self.get_source(SourceKind::Sql)
    }
}

pub fn from_mapping(file: File) -> Schema {
    let map: Map = serde_yaml::from_reader(file).expect("failed to serialise yaml");
    let fields: Vec<Field> = map
        .fields
        .iter()
        .map(|dict| {
            // merge arrow and custom schema with metadata
            let mut meta: HashMap<String, String> = HashMap::new();
            let label = match &dict.labelOverride {
                Some(x) => x.clone(),
                _ => dict.label.clone(),
            };
            meta.insert("label".to_string(), label);
            meta.insert("dataType".to_string(), dict.dataType.as_string());
            if let Some(x) = &dict.reference {
                meta.insert("entity".to_string(), x.clone());
            };

            match dict.dataType {
                MapFieldType::Int => Field::new(dict.label.clone(), DataType::Int64, false),
                MapFieldType::Float => Field::new(dict.label.clone(), DataType::Float64, false),
                MapFieldType::String => Field::new(dict.label.clone(), DataType::Utf8, false),
                MapFieldType::Date => Field::new(dict.label.clone(), DataType::Date64, false),
            }
            .with_metadata(meta)
        })
        .collect();

    Schema {
        fields: arrow::datatypes::Schema::new(fields),
        relationships: map.relationships.unwrap(),
        source: map.source,
    }
}
