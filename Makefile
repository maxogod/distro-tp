build:
	docker compose build

up:
	docker compose up

up-build:
	docker compose up --build

down:
	docker compose down -v

compare:
	./scripts/compare_all_outputs.sh .expected_full/

clean:
	sudo rm -rf .output* .storage

check_clients:
	docker compose logs | grep Pikachu
