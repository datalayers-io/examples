#!/bin/bash

# Download and install dependencies for all supported languages.
# For languages that require a build step before running, this script will also handle the build process.

set -e

# For Go.
echo "Building for Go..."
pushd go
wget -q https://go.dev/dl/go1.22.3.linux-amd64.tar.gz
rm -rf /usr/local/go && tar -C /usr/local -xzf go1.22.3.linux-amd64.tar.gz && rm -f go1.22.3.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
go mod tidy
go build
popd

# For Python.
echo "Building for Python..."
pushd python
TZ=Asia/Shanghai DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends python3 python3-pip
pip3 install --break-system-packages -r requirements.txt
popd

# For Rust.
echo "Building for Rust..."
pushd rust
if ! command -v rustc > /dev/null; then \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
    . "$HOME/.cargo/env"
fi
cargo build
popd

# TODO: support java.
# For Java.
echo "Building for Java..."
echo "Sorry, nothing to build for Java for now"
