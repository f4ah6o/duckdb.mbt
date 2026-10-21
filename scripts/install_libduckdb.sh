#!/usr/bin/env bash
# Install the libduckdb headers and shared library for a released DuckDB
# version.
#
# DuckDB 2.0 renamed the shared-library release assets from
# libduckdb-<platform>.zip to duckdb-shared-libs-<platform>.tar.gz and started
# shipping duckdb_v2.h alongside duckdb.h. CI and local verification both go
# through this script so a version bump only has to update the version matrix.
#
# usage: scripts/install_libduckdb.sh <version> <prefix>
#   version  DuckDB release without the leading "v", e.g. 1.5.6 or 2.0.0
#   prefix   install prefix; headers go to <prefix>/include, libraries to
#            <prefix>/lib. Pass a writable prefix or run with sudo.
set -euo pipefail

usage() {
  sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'
}

if [ "$#" -ne 2 ]; then
  usage >&2
  exit 2
fi

version="$1"
prefix="$2"

case "$version" in
  [0-9]*.[0-9]*.[0-9]*) ;;
  *)
    echo "invalid DuckDB version '$version' (expected x.y.z)" >&2
    exit 2
    ;;
esac

major="${version%%.*}"
os="$(uname -s)"
arch="$(uname -m)"

case "$os" in
  Darwin)
    platform="osx-universal"
    library="libduckdb.dylib"
    ;;
  Linux)
    case "$arch" in
      x86_64 | amd64) platform="linux-amd64" ;;
      aarch64 | arm64) platform="linux-arm64" ;;
      *)
        echo "unsupported Linux architecture '$arch'" >&2
        exit 1
        ;;
    esac
    library="libduckdb.so"
    ;;
  *)
    echo "unsupported operating system '$os'" >&2
    exit 1
    ;;
esac

if [ "$major" -ge 2 ]; then
  asset="duckdb-shared-libs-${platform}.tar.gz"
else
  asset="libduckdb-${platform}.zip"
fi

url="https://github.com/duckdb/duckdb/releases/download/v${version}/${asset}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

echo "downloading ${url}"
curl -fsSL --retry 3 -o "$work/$asset" "$url"

case "$asset" in
  *.tar.gz) tar -xzf "$work/$asset" -C "$work" ;;
  *.zip) unzip -q "$work/$asset" -d "$work" ;;
esac

if [ ! -f "$work/$library" ]; then
  echo "$asset does not contain $library" >&2
  exit 1
fi

headers=("$work"/*.h)
if [ ! -f "${headers[0]}" ]; then
  echo "$asset does not contain C headers" >&2
  exit 1
fi

install -d "$prefix/include" "$prefix/lib"
install -m 644 "${headers[@]}" "$prefix/include/"
install -m 755 "$work/$library" "$prefix/lib/"

if [ "$os" = "Linux" ]; then
  if [ "$(id -u)" -eq 0 ]; then
    ldconfig
  else
    echo "run 'sudo ldconfig' so the linker can find libduckdb.so" >&2
  fi
fi

echo "installed libduckdb v${version} into ${prefix}"
