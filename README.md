# Entity Mapper

## Commands

- load file to database
  - support json, jsonl, csv, parquet
  - use repo pattern
- generate mapper stub
- create entities

## Services

- Postgres
- Kafka

## Mapping Format

```yaml
fields:
- label: source_id
  dataType: int
  entity: PERSON.Person
- label: age
  dataType: int
  entity: PERSON.Person
- label: name
  dataType: string
  entity: PERSON.Person
- label: address
  dataType: string
  entity: LOCATION.Address
relationships:
- "(PERSON:Person)-[RESIDES_AT]->(LOCATION:Address)"
source:
  sql: select * from postgres.schema.table
```

## Loading

Decide on loading process?

### From GET

- reqwest -> read text -> reader on text

### From local file

- reader on file

### Reader -> Database

1. reader -> arrow record batch -> postgres
