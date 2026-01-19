.PHONY: build test lint docker-build docker-push run clean help

# Variables
BINARY_NAME=pvc-plumber
DOCKER_IMAGE=ghcr.io/mitchross/pvc-plumber
VERSION?=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS=-ldflags "-X main.version=$(VERSION)"

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Build binary locally
	@echo "Building $(BINARY_NAME)..."
	CGO_ENABLED=0 go build -a -installsuffix cgo $(LDFLAGS) -o $(BINARY_NAME) ./cmd/pvc-plumber
	@echo "Built $(BINARY_NAME)"

test: ## Run tests with coverage
	@echo "Running tests..."
	go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...
	@echo "Coverage report:"
	go tool cover -func=coverage.txt | tail -1

test-coverage: test ## Generate HTML coverage report
	go tool cover -html=coverage.txt -o coverage.html
	@echo "Coverage report generated: coverage.html"

lint: ## Run golangci-lint (requires golangci-lint to be installed)
	@echo "Running linter..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed, skipping..."; \
		echo "Install it with: curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b \$$(go env GOPATH)/bin v1.55.2"; \
	fi

vet: ## Run go vet
	@echo "Running go vet..."
	go vet ./...

fmt: ## Format code
	@echo "Formatting code..."
	go fmt ./...

docker-build: ## Build Docker image
	@echo "Building Docker image..."
	docker build -t $(DOCKER_IMAGE):$(VERSION) .
	docker tag $(DOCKER_IMAGE):$(VERSION) $(DOCKER_IMAGE):latest
	@echo "Built $(DOCKER_IMAGE):$(VERSION)"

docker-build-debug: ## Build debug Docker image
	@echo "Building debug Docker image..."
	docker build -f Dockerfile.debug -t $(DOCKER_IMAGE):$(VERSION)-debug .
	@echo "Built $(DOCKER_IMAGE):$(VERSION)-debug"

docker-push: ## Push Docker image to registry
	@echo "Pushing Docker image..."
	docker push $(DOCKER_IMAGE):$(VERSION)
	docker push $(DOCKER_IMAGE):latest

run: build ## Run locally with env vars
	@echo "Running $(BINARY_NAME)..."
	@if [ -z "$$S3_ENDPOINT" ] || [ -z "$$S3_BUCKET" ]; then \
		echo "ERROR: S3_ENDPOINT and S3_BUCKET must be set"; \
		echo "Example: S3_ENDPOINT=http://localhost:9000 S3_BUCKET=my-bucket make run"; \
		exit 1; \
	fi
	./$(BINARY_NAME)

clean: ## Clean build artifacts
	@echo "Cleaning..."
	rm -f $(BINARY_NAME)
	rm -f coverage.txt coverage.html
	go clean

all: fmt vet test build ## Run fmt, vet, test and build
