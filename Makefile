.PHONY: install test analyse run up down build shell

install:
	composer install

test:
	vendor/bin/phpunit

analyse:
	vendor/bin/phpstan analyse

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

docker-run: up
	docker compose exec php php bin/run.php

docker-run-debug: up
	docker compose exec php bash -c "XDEBUG_TRIGGER=1 php bin/run.php"
