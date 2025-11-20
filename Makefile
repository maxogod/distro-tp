build:
	docker compose build

up:
	docker compose up

up-build:
	docker compose up --build

down:
	docker compose down

reset:
	docker compose down -v

compare:
	./compare_all_outputs.sh .expected_full/

clean:
	sudo rm -rf .output*
