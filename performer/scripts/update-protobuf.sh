#!/bin/sh

# USAGE: Run this script from anywhere.
# It fetches the latest proto files from the `couchbaselabs/fit-protocol` repo
# and copies them to performer/proto.
# Then manually commit any changes.

set -e

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

DEST_DIR=performer/proto
TMP_DIR=$(mktemp -d)

trap 'rm -rf "$TMP_DIR"' EXIT

curl --location --fail https://github.com/couchbaselabs/fit-protocol/archive/refs/heads/main.zip -o "$TMP_DIR/fit-protocol.zip"
unzip -q "$TMP_DIR/fit-protocol.zip" -d "$TMP_DIR/unzipped"

SRC_DIR="$TMP_DIR/unzipped/fit-protocol-main"
[ -d "$SRC_DIR" ] || { echo "ERROR: Missing expected directory: $SRC_DIR" >&2; exit 1; }

rm -rf "$DEST_DIR"
mkdir -p "$DEST_DIR"
cp -R "$SRC_DIR"/. "$DEST_DIR"
git add --all "$DEST_DIR"

echo
echo "Protobuf update complete! Please manually commit any modified files."
echo
