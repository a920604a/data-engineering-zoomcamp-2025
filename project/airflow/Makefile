PLATFORM ?= linux/amd64


up:
	docker-compose up --build --remove-orphans  -d



## Stops all services in docker-compose
stop:
	docker-compose stop

down:
	docker-compose down --remove-orphans

clean:
	sudo rm -rf postgres-dz-volume/ postgres-db-volume/

restart:
	docker-compose restart