default:
	docker compose up -d

load-file:
	cargo run localhost user password postgres.schema.table load resources/test.json
	# cargo run localhost user password postgres.schema.table load resources/test.csv
	# cargo run localhost user password postgres.schema.table load resources/test-lines.json
	# cargo run localhost user password postgres.schema.table load resources/test.parquet

load-url:
	cargo run localhost user password testdb.default.iris resources/iris.yaml \
		load https://raw.githubusercontent.com/mwaskom/seaborn-data/refs/heads/master/iris.csv

create:
	cargo run localhost user password postgres.schema.table create map topic
