BINARY := cloud-tasks-emulator
IMAGE  := cloud-tasks-emulator

.PHONY: build test cover vet run docker hooks clean

build:
	go build -o $(BINARY) .

test:
	go test ./...

cover:
	go test -race -coverprofile=cover.out ./...
	go tool cover -func=cover.out | tail -1

vet:
	go vet ./...

run: build
	./$(BINARY)

docker:
	docker build -t $(IMAGE) .

hooks:
	lefthook install

clean:
	rm -f $(BINARY)
