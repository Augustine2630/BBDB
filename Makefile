COVERAGE_THRESHOLD := 65
COVERAGE_OUT := coverage.out

# ── Build ──────────────────────────────────────────────────────────────────────

.PHONY: build
build:
	go build ./...

# ── Test ───────────────────────────────────────────────────────────────────────

.PHONY: test
test:
	go test ./... -count=1

.PHONY: test/cover
test/cover:
	go test ./... -count=1 -coverprofile=$(COVERAGE_OUT)
	go tool cover -func=$(COVERAGE_OUT) | tail -1

.PHONY: test/cover/check
test/cover/check:
	go test ./... -count=1 -coverprofile=$(COVERAGE_OUT)
	@go tool cover -func=$(COVERAGE_OUT) | tail -1
	@total=$$(go tool cover -func=$(COVERAGE_OUT) | tail -1 | awk '{gsub(/%/,""); print int($$NF)}'); \
	if [ "$$total" -lt "$(COVERAGE_THRESHOLD)" ]; then \
		echo "FAIL: coverage $$total% is below threshold $(COVERAGE_THRESHOLD)%"; exit 1; \
	else \
		echo "OK: coverage $$total% >= $(COVERAGE_THRESHOLD)%"; \
	fi

# ── Per-plan test targets ───────────────────────────────────────────────────────

.PHONY: test/plan1
test/plan1: ## Foundation: meta + tier
	go test ./internal/meta/... ./internal/tier/... -v -count=1 -coverprofile=$(COVERAGE_OUT)
	go tool cover -func=$(COVERAGE_OUT) | grep -E "^BBDB|^total"

.PHONY: test/plan2
test/plan2: ## Write Path: ingestion + block + index
	go test ./internal/ingestion/... ./internal/block/... ./internal/index/... -v -count=1 -coverprofile=$(COVERAGE_OUT)
	go tool cover -func=$(COVERAGE_OUT) | grep -E "^BBDB|^total"

.PHONY: test/plan3
test/plan3: ## Read Path: query
	go test ./internal/query/... -v -count=1 -coverprofile=$(COVERAGE_OUT)
	go tool cover -func=$(COVERAGE_OUT) | grep -E "^BBDB|^total"

.PHONY: test/plan4
test/plan4: ## TTL reaper + janitor
	go test ./internal/ttl/... -v -count=1 -coverprofile=$(COVERAGE_OUT)
	go tool cover -func=$(COVERAGE_OUT) | grep -E "^BBDB|^total"

.PHONY: test/integration
test/integration: ## End-to-end integration tests (write+read+TTL)
	go test ./tests/integration/... -v -count=1

.PHONY: test/integration/short
test/integration/short: ## Integration tests excluding high-volume (fast CI)
	go test ./tests/integration/... -v -count=1 -short

.PHONY: test/plan5
test/plan5: ## Config + CLI: config + server
	go test ./internal/config/... ./internal/server/... -v -count=1 -coverprofile=$(COVERAGE_OUT)
	go tool cover -func=$(COVERAGE_OUT) | grep -E "^BBDB|^total"

# Configurable: CONFIG=configs/bbdb.example.yaml make run
CONFIG ?=
IMAGE  ?= bbdb
TAG    ?= latest

.PHONY: build/bbdb
build/bbdb: ## Build the bbdb binary → bin/bbdb
	go build -trimpath -ldflags="-s -w" -o bin/bbdb ./cmd/bbdb/

.PHONY: run
run: build/bbdb ## Run bbdb (CONFIG=path/to/cfg.yaml make run)
	./bin/bbdb start $(if $(CONFIG),--config $(CONFIG),)

# ── Docker ─────────────────────────────────────────────────────────────────────

.PHONY: docker/build
docker/build: ## Build Docker image (IMAGE=bbdb TAG=latest)
	docker build -t $(IMAGE):$(TAG) .

.PHONY: docker/run
docker/run: ## Run Docker image (CONFIG=path/to/cfg.yaml IMAGE=bbdb TAG=latest)
	docker run --rm \
		$(if $(CONFIG),-v $(abspath $(CONFIG)):/etc/bbdb/config.yaml,) \
		$(IMAGE):$(TAG) \
		$(if $(CONFIG),--config /etc/bbdb/config.yaml,)

# ── Proto ─────────────────────────────────────────────────────────────────────

.PHONY: proto/gen
proto/gen: ## Generate Go code from proto files (requires buf)
	buf generate

# ── Single package ─────────────────────────────────────────────────────────────

.PHONY: test/pkg
test/pkg: ## Usage: make test/pkg PKG=./internal/meta/...
	go test $(PKG) -v -count=1 -coverprofile=$(COVERAGE_OUT)
	go tool cover -func=$(COVERAGE_OUT) | grep -E "^BBDB|^total"

# ── Single test ────────────────────────────────────────────────────────────────

.PHONY: test/run
test/run: ## Usage: make test/run PKG=./internal/meta/... RUN=TestWALNextSeq
	go test $(PKG) -v -count=1 -run $(RUN)

# ── Lint / Vet ─────────────────────────────────────────────────────────────────

.PHONY: vet
vet:
	go vet ./...

# ── Tidy ──────────────────────────────────────────────────────────────────────

.PHONY: tidy
tidy:
	go mod tidy

# ── Help ──────────────────────────────────────────────────────────────────────

.PHONY: help
help:
	@grep -E '^[a-zA-Z/_%.-]+:.*##' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*##"}; {printf "  %-22s %s\n", $$1, $$2}'
