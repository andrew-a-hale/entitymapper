default:
	docker compose up -d

load:
	cargo run localhost user password postgres.schema.table load file

stub:
	cargo run test user pw table stub

create:
	cargo run test user pw create map topic
