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
test/plan4: ## TTL + API: ttl + api
	go test ./internal/ttl/... ./api/... -v -count=1 -coverprofile=$(COVERAGE_OUT)
	go tool cover -func=$(COVERAGE_OUT) | grep -E "^BBDB|^total"

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
