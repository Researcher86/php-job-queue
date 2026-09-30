.PHONY: up down build shell install test analyse format format-check \
        run run-debug run-worker bench example

# CI has no terminal to attach to (GitHub Actions sets CI=true), so exec runs
# without one there; locally it keeps the TTY for colours and interactive use.
TTY := $(if $(CI),-T,)

up:
	docker compose up -d

down:
	docker compose down

build:
	docker compose build

shell: up
	docker compose exec $(TTY) php bash

install: up
	docker compose exec $(TTY) php composer install

test: up
	docker compose exec $(TTY) php composer test

analyse: up
	docker compose exec $(TTY) php composer analyse

run: up
	docker compose exec $(TTY) php php bin/run.php

run-debug: up
	docker compose exec $(TTY) php bash -c "XDEBUG_TRIGGER=1 php bin/run.php"

# The long-running process. SIGTERM it from another shell to watch a
# graceful shutdown: `docker compose exec $(TTY) php pkill -TERM -f bin/worker.php`
run-worker: up
	docker compose exec $(TTY) php php bin/worker.php

# make bench ARGS="10000 8 0 file"
#   -> <jobs> <workers> <work-microseconds> <none|memory|file>
bench: up
	docker compose exec $(TTY) php php bin/bench.php $(ARGS)

# One mechanism at a time: basic-job, failed-job, delayed-job,
# worker-crash, priority-jobs.
example: up
	docker compose exec $(TTY) php php examples/$(EXAMPLE).php

format: up
	docker compose exec $(TTY) php composer format

format-check: up
	docker compose exec $(TTY) php composer format:check
