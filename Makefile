.PHONY: install test analyse lint fix run up down build shell \
        docker-install docker-test docker-analyse docker-lint docker-fix \
        docker-run docker-run-debug

install:
	composer install

test:
	vendor/bin/phpunit

analyse:
	vendor/bin/phpstan analyse

# check reports without touching anything (what CI runs); fix rewrites.
lint:
	vendor/bin/php-cs-fixer check --diff

fix:
	vendor/bin/php-cs-fixer fix

run:
	php bin/run.php

up:
	docker compose up -d

down:
	docker compose down

build:
	docker compose build

shell: up
	docker compose exec php bash

docker-install: up
	docker compose exec php composer install

docker-test: up
	docker compose exec php composer test

docker-analyse: up
	docker compose exec php composer analyse

docker-lint: up
	docker compose exec php composer lint

docker-fix: up
	docker compose exec php composer fix

docker-run: up
	docker compose exec php php bin/run.php

docker-run-debug: up
	docker compose exec php bash -c "XDEBUG_TRIGGER=1 php bin/run.php"
