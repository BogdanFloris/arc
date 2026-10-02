#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
npm run build

deploy_root="${XDG_DATA_HOME:-$HOME/.local/share}/arc-web"
mkdir -p "$deploy_root/releases"
release=$(mktemp -d "$deploy_root/releases/build.XXXXXX")
trap 'rm -f "$release.link"' EXIT

cp -R dist/. "$release/"
ln -s "$release" "$release.link"
mv -Tf "$release.link" "$deploy_root/current"

printf '\nPublished ARC to %s/current\n' "$deploy_root"
printf 'One-time HTTPS setup:\n  sudo tailscale serve --bg "%s/current"\n' "$deploy_root"
