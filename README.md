# Entity Mapper

## Commands

- load file to database
  - support json, jsonl, csv, parquet, arvo
  - use repo pattern
- generate mapper stub
- create entities

## Services

- Postgres
- Kafka

## Mapping Format

```json
{
  "fields": [
    "0": {"label": "source_id", "dataType": "int", "entity": "PERSON.Person"},
    "1": {"label": "age", "dataType": "int", "entity": "PERSON.Person"},
    "2": {"label": "name", "dataType": "string", "entity": "PERSON.Person"},
    "3": {"label": "address", "dataType": "string", "entity": "LOCATION.Address"},
  ],
  "relationships": [
    "(PERSON:Person)-[RESIDES_AT]->(LOCATION:Address)"
  ],
  "source": {
    "sql": "select * from postgres.schema.table"
  }
}
```
