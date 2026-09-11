#!/bin/bash
set -e

echo "--- 1. Building Rust Node ---"
(
    cd rust
    cargo build --release
)

echo "--- 2. Running Go <-> Rust Integration Tests ---"
go test -v -timeout 180s ./tests/integration/...

echo "--- Tests finished ---"
