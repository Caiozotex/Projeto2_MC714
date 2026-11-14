.PHONY: build test up3 up5 up7 down clean correctness faults experiments plots test-all

build:
	docker compose build

test:
	go test ./... -count=1

up3:
	./scripts/up.sh 3

up5:
	./scripts/up.sh 5

up7:
	./scripts/up.sh 7

down:
	./scripts/clean.sh

clean: down

correctness:
	./scripts/test_correctness.sh

faults:
	./scripts/test_faults.sh

experiments:
	./scripts/run_experiments.sh

plots:
	python3 scripts/plot_results.py

test-all: test correctness faults
