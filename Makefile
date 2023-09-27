up:
	docker-compose up -d

test:
	go test ./...

test_v:
	go test -v ./...

test_short:
	go test ./... -short

test_race:
	go test ./... -short -race

test_stress:
	go test -v -tags=stress -timeout=45m ./...

fmt:
	go fmt ./...
	goimports -l -w .

build:
	go build ./...

test_codecov: up wait
	go test -coverprofile=coverage.out -covermode=atomic ./...

wait:
	go run github.com/ThreeDotsLabs/wait-for@latest localhost:6379

update_watermill:
	go get -u github.com/ThreeDotsLabs/watermill
	go mod tidy

	sed -i '\|go 1\.|d' go.mod
	go mod edit -fmt
