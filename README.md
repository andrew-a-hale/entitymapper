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
- label: sourceId
  dataType: String
  reference: PERSON.Person!0
- label: age
  dataType: Int
  reference: PERSON.Person!0
- label: name
  dataType: String
  reference: PERSON.Person!0
- label: addressId
  labelOverride: sourceId
  dataType: String
  reference: LOCATION.Address
- label: address
  dataType: String
  reference: LOCATION.Address
- label: residencyStartDate
  dataType: Date
  reference: PERSON.Person!0-LOCATION.Address
- label: residencyEndDate
  dataType: Date
  reference: PERSON.Person!0-LOCATION.Address
- label: parentSourceId
  labelOverride: sourceId
  dataType: String
  reference: PERSON.Person!1
- label: parent
  labelOverride: name
  dataType: String
  reference: PERSON.Person!1
- label: type
  dataType: String
  reference: PERSON.Person!0-PERSON.Person!1
relationships:
- label: RESIDES_AT
  reference: PERSON.Person!0-LOCATION.Address
  props:
  - residencyStartDate
  - residencyEndDate
- label: PersonChildOfPerson
  reference: PERSON.Person!0-PERSON.Person!1
```

## Message Format

### Entity

```json
{
  "fqn": "PERSON.Person!0",
  "type": "PERSON",
  "subType": "Person",
  "setId": 0,
  "props": [
    {
      "label": "sourceId",
      "dataType": "string",
      "value": 0

    },
    {
      "label": "name",
      "dataType": "string",
      "value": "Andrew",
    },
    {
      "label": "age",
      "dataType": "int"
      "value": 30,
    }
  ]
}
```

```json
{
  "fqn": "PERSON.Person!1",
  "type": "PERSON",
  "subType": "Person",
  "setId": 1,
  "props": [
    {
      "label": "sourceId",
      "dataType": "string",
      "value": 1
    },
    {
      "label": "name",
      "dataType": "string",
      "value": "John"
    }
  ]
}
```

```json
{
  "fqn": "LOCATION.Address",
  "type": "LOCATION",
  "subType": "Address",
  "props": [
    {
      "label": "sourceId",
      "dataType": "string",
      "value": 0
    },
    {
      "label": "address",
      "dataType": "string",
      "value": "Home"
    }
  ]
}
```

### Relationship

```json
{
  "relType": "RESIDES_AT",
  "fromId": "0",
  "toId": "0",
  "props": [
    {
      "label": "residencyStartDate",
      "dataType": "date",
      "value": "2024-01-01",
    },
    {
      "label": "residencyEndDate",
      "dataType": "date",
      "value": "2024-12-12",
    }
  ]
}
```

```json
{
  "relType": "CHILD_OF",
  "fromId": "0",
  "toId": "1",
  "props": [
    {
      "label": "type",
      "dataType": "string",
      "value": "biological",
    }
  ]
}
```
