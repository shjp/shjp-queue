build:
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o shjp-queue .

docker_up: build
	docker-compose build
	docker-compose up -d