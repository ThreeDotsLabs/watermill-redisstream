test:
	go test -count=1 -parallel 20 ./...

test_v:
	go test -count=1 -parallel 20 -v ./...

test_short:
	go test -count=1 -parallel 20 ./... -short

test_race:
	go test ./... -count=1 -race

test_stress:
	go test -count=1 -tags=stress -parallel 30 -timeout=15m ./...

test_reconnect:
	go test -tags=reconnect ./...

fmt:
	go fmt ./...
	goimports -l -w .
