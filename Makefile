.PHONY: up down build shell install test analyse format format-check \
        run run-debug run-worker bench example

up:
	docker compose up -d

down:
	docker compose down

build:
	docker compose build

shell: up
	docker compose exec php bash

install: up
	docker compose exec php composer install

test: up
	docker compose exec php composer test

analyse: up
	docker compose exec php composer analyse

run: up
	docker compose exec php php bin/run.php

run-debug: up
	docker compose exec php bash -c "XDEBUG_TRIGGER=1 php bin/run.php"

# The long-running process. SIGTERM it from another shell to watch a
# graceful shutdown: `docker compose exec php pkill -TERM -f bin/worker.php`
run-worker: up
	docker compose exec php php bin/worker.php

# make bench ARGS="10000 8 0 file"
#   -> <jobs> <workers> <work-microseconds> <none|memory|file>
bench: up
	docker compose exec php php bin/bench.php $(ARGS)

# One mechanism at a time: basic-job, failed-job, delayed-job,
# worker-crash, priority-jobs.
example: up
	docker compose exec php php examples/$(EXAMPLE).php

format: up
	docker compose exec php composer format

format-check: up
	docker compose exec php composer format:check
