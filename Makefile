BINARY := cloud-tasks-emulator
IMAGE  := cloud-tasks-emulator

.PHONY: build test vet run docker clean

build:
	go build -o $(BINARY) .

test:
	go test ./...

vet:
	go vet ./...

run: build
	./$(BINARY)

docker:
	docker build -t $(IMAGE) .

clean:
	rm -f $(BINARY)
