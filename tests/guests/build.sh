#!/bin/sh
# Rebuild the checked-in test guests with the Nix shell's WASI SDK clang.
set -eu
cd "$(dirname "$0")"
clang="${FLOWER_WASI_SDK:?run inside nix develop}/bin/clang"
for guest in counter; do
  "$clang" --target=wasm32 -O2 -ffreestanding -fno-builtin -nostdlib -Wl,--no-entry -Wl,--strip-all \
    -o "$guest.wasm" "$guest.c"
done
