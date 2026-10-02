#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
mkdir -p public/icons
rsvg-convert --background-color=white -w 180 -h 180 assets/logo.svg -o public/icons/apple-touch-icon.png
rsvg-convert --background-color=white -w 192 -h 192 assets/logo.svg -o public/icons/icon-192.png
rsvg-convert --background-color=white -w 512 -h 512 assets/logo.svg -o public/icons/icon-512.png
rsvg-convert --background-color=white -w 512 -h 512 assets/logo.svg -o public/icons/icon-maskable-512.png
