#!/usr/bin/env bash
# T104: build/run with `--features real` against the RIGHT OpenSSL.
#
# Why this exists: Homebrew's default `openssl` is 4.x on current macOS.
# OpenSSL 4 no longer exports the ENGINE API, but librdkafka 2.12.1 (vendored by
# rdkafka-sys 4.10) still calls ENGINE_* from rdkafka_ssl.c, so linking against
# openssl@4 dies with `Undefined symbols: _ENGINE_by_id ...`. openssl@3 is
# keg-only, so it is never what pkg-config finds on its own -- we must point at
# it explicitly. `cargo check` does not link and therefore passes regardless;
# only `cargo build`/`cargo run` show the problem.
#
# Usage: scripts/real-run.sh [cargo args...]
#   default: run -p main-bin --features real
set -euo pipefail
cd "$(dirname "$0")/.."

prefix=""
if command -v brew >/dev/null 2>&1; then
    prefix="$(brew --prefix openssl@3 2>/dev/null || true)"
fi

if [ -n "$prefix" ] && [ -d "$prefix/lib/pkgconfig" ]; then
    export OPENSSL_DIR="$prefix"
    export PKG_CONFIG_PATH="$prefix/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
    echo "openssl: pinned to $prefix ($("$prefix/bin/openssl" version 2>/dev/null | cut -d' ' -f1,2))"
elif command -v pkg-config >/dev/null 2>&1 && pkg-config --exists openssl; then
    echo "openssl: using the default pkg-config hit ($(pkg-config --modversion openssl))"
    echo "note: if the link fails on _ENGINE_*, that OpenSSL is too new; install openssl@3 and retry." >&2
else
    echo "openssl: not found. macOS: \`brew install openssl@3\`; Debian/Ubuntu: \`sudo apt-get install -y libssl-dev pkg-config\`." >&2
    exit 1
fi

if [ "$#" -eq 0 ]; then
    set -- run -p main-bin --features real
fi
echo "cargo $*"
exec cargo "$@"
