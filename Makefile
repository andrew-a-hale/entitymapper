default:
	docker compose up -d

load:
	cargo run localhost user password postgres.schema.table load file.csv

create:
	cargo run localhost user password postgres.schema.table create map topic
