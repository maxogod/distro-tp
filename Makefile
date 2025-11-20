.PHONY: docker-up docker-up-build

docker-up:
	docker compose up

docker-up-build:
	docker compose up --build

docker-down:
	docker compose down

docker-reset:
	docker compose down -v

compare-full:
	./compare_all_outputs.sh .expected_full/

remove-outputs:
	sudo rm -rf .output*
