BINARY := cloud-tasks-emulator
IMAGE  := cloud-tasks-emulator
# Override to point the conformance checks at a virtualenv or another runtime.
PYTHON ?= python3
NODE   ?= node

.PHONY: build test cover vet lint conformance run docker hooks clean

build:
	go build -o $(BINARY) .

test:
	go test ./...

cover:
	go test -race -coverprofile=cover.out ./...
	go tool cover -func=cover.out | tail -1

vet:
	go vet ./...

lint:
	golangci-lint run

# Drive a locally built emulator with the official client libraries. Install
# their dependencies first: pip install -r conformance/python/requirements.txt
# and (cd conformance/node && npm ci).
conformance: build
	@./$(BINARY) -host 127.0.0.1 -port 8123 -rest-port 8124 -openid-issuer http://127.0.0.1:8980 & \
		emulator=$$!; \
		trap "kill $$emulator 2>/dev/null" EXIT; \
		sleep 1; \
		EMULATOR_ADDR=127.0.0.1:8123 $(PYTHON) conformance/python/check.py && \
		EMULATOR_ADDR=127.0.0.1:8123 OPENID_ISSUER=http://127.0.0.1:8980 \
			$(PYTHON) conformance/python/check_oidc.py && \
		REST_ADDR=http://127.0.0.1:8124 $(PYTHON) conformance/python/check_rest.py && \
		cd conformance/node && EMULATOR_ADDR=127.0.0.1:8123 $(NODE) check.mjs && \
		REST_ADDR=127.0.0.1:8124 $(NODE) check_rest.mjs

run: build
	./$(BINARY)

docker:
	docker build -t $(IMAGE) .

hooks:
	lefthook install

clean:
	rm -f $(BINARY)
