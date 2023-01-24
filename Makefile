up:
	docker-compose up -d

test:
	go test -parallel 20 ./...

test_v:
	go test -parallel 20 -v ./...

test_short:
	go test -parallel 20 ./... -short

test_race:
	go test ./... -short -race

test_stress:
	go test -v -tags=stress -parallel 30 -timeout=45m ./...

fmt:
	go fmt ./...
	goimports -l -w .

build:
	go build ./...

wait:
	go run github.com/ThreeDotsLabs/wait-for@latest localhost:6379

update_watermill:
	go get -u github.com/ThreeDotsLabs/watermill
	go mod tidy

	sed -i '\|go 1\.|d' go.mod
	go mod edit -fmt
