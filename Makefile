default:
	docker compose up -d

load-file:
	cargo run localhost user password testdb.default.test resources/gen.yaml load resources/gen.csv

load-url:
	cargo run localhost user password testdb.default.iris resources/iris.yaml \
		load https://raw.githubusercontent.com/mwaskom/seaborn-data/refs/heads/master/iris.csv

create:
	cargo run --release localhost user password testdb.default.test resources/gen.yaml create test
