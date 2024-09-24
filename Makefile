.PHONY: go
go:
	@echo "Running Go examples..."
	@bash -c "cd go && go build && ./main && cd .."

.PHONY: python
python:
	@echo "Running Python examples..."
	@bash -c "cd python && python3 main.py && cd .."

.PHONY: rust
rust:
	@echo "Running Rust examples..."
	@bash -c "cd rust && cargo run && cd .."

# TODO: support java.
.PHONY: java
java:
	@echo "Running Java examples..."
	@echo "Sorry, Java is not supported for now"

.PHONY: build
build:
	@echo "Building..."
	@bash -c "./build.sh"

# TODO: support java.
.PHONY: format
format:
	@echo "Formatting code for all languages..."
	gofmt -w go
	black python
	cargo fmt --manifest-path rust/Cargo.toml 2>/dev/null

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  go        - Run Go examples"
	@echo "  python    - Run Python examples"
	@echo "  rust      - Run Rust examples"
	@echo "  java      - Run Java examples"
	@echo "  build     - Build for all languages"
	@echo "  format    - Format code for all languages"
