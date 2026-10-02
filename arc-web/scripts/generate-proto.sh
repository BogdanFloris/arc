#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
source="$root/../arc-proto/proto"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/proto"
cp "$source/wire.proto" "$source/events.proto" "$source/memory.proto" "$tmp/proto/"
cat > "$tmp/buf.yaml" <<'YAML'
version: v2
modules:
  - path: proto
YAML
cat > "$tmp/buf.gen.yaml" <<YAML
version: v2
plugins:
  - local: $root/node_modules/.bin/protoc-gen-es
    out: $root/src/lib/arc/gen
    opt: target=ts
YAML
cd "$tmp"
buf generate
