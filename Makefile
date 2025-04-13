default:
	docker compose up -d

load-file:
	cargo run localhost user password testdb.default.test resources/test.yaml load resources/test-lines.jsonl

load-url:
	cargo run localhost user password testdb.default.iris resources/iris.yaml \
		load https://raw.githubusercontent.com/mwaskom/seaborn-data/refs/heads/master/iris.csv

create:
	cargo run --release localhost user password testdb.default.test resources/test.yaml create test
