#!/usr/bin/env bash
# Build bvm's four static dependencies into a prefix. Default prefix is quiver/exec/deps,
# which the Makefile picks up automatically, so the full from-source flow is just:
#
#     quiver/exec/build-deps.sh && make -C quiver/exec bvm
#
# A custom prefix works too:  build-deps.sh /opt/bvm-deps && make -C quiver/exec bvm DEPS=/opt/bvm-deps
#
# Pinned versions with sha256; needs only curl, tar, make, sh and a C compiler (no cmake,
# no autotools regeneration -- libsodium ships a configure script, BLAKE3's C files are
# compiled directly). ~2 minutes. CC/AR/CFLAGS/JOBS are honoured; everything is -fPIC so
# bvm can stay a PIE. This is also what the manylinux wheel build runs (release.yml).
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PREFIX=${1:-$HERE/deps}
JOBS=${JOBS:-$(nproc 2>/dev/null || echo 4)}
CC=${CC:-cc}; AR=${AR:-ar}
export CC AR
CFLAGS="${CFLAGS:--O2} -fPIC"

ZSTD_V=1.5.6;    ZSTD_SHA=8c29e06cf42aacc1eafc4077ae2ec6c6fcb96a626157e0593d5e82a34fd403c1
SODIUM_V=1.0.20; SODIUM_SHA=ebb65ef6ca439333c2bb41a0c1990587288da07f6c7fd07cb3a18cc18d30ce19
URING_V=2.7;     URING_SHA=56202ad443c50e684dde3692819be3b91bbe003e1c14bf5abfa11973669978c1
BLAKE3_V=1.5.5;  BLAKE3_SHA=6feba0750efc1a99a79fb9a495e2628b5cd1603e15f56a06b1d6cb13ac55c618

WORK=$(mktemp -d "${TMPDIR:-/tmp}/bvm-deps.XXXXXX")
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$PREFIX/include" "$PREFIX/lib"

# fetch URL SHA256 -> prints the extracted top-level directory
fetch() {
    local url=$1 sha=$2 tgz="$WORK/$(basename "$1")"
    curl -fsSL --retry 3 -o "$tgz" "$url"
    echo "$sha  $tgz" | sha256sum -c - >/dev/null
    local dir; dir=$(tar tzf "$tgz" | head -1 | cut -d/ -f1)
    tar xzf "$tgz" -C "$WORK"
    echo "$WORK/$dir"
}

echo ">>> zstd $ZSTD_V"
d=$(fetch "https://github.com/facebook/zstd/releases/download/v$ZSTD_V/zstd-$ZSTD_V.tar.gz" "$ZSTD_SHA")
make -s -C "$d/lib" -j"$JOBS" libzstd.a MOREFLAGS=-fPIC
cp "$d/lib/libzstd.a" "$PREFIX/lib/"
cp "$d/lib/zstd.h" "$d/lib/zdict.h" "$d/lib/zstd_errors.h" "$PREFIX/include/"

echo ">>> libsodium $SODIUM_V"
d=$(fetch "https://github.com/jedisct1/libsodium/releases/download/$SODIUM_V-RELEASE/libsodium-$SODIUM_V.tar.gz" "$SODIUM_SHA")
( cd "$d" \
  && ./configure -q --prefix="$PREFIX" --libdir="$PREFIX/lib" --disable-shared --enable-static --with-pic CFLAGS="$CFLAGS" \
  && make -s -j"$JOBS" >/dev/null && make -s install >/dev/null )

echo ">>> liburing $URING_V"
d=$(fetch "https://github.com/axboe/liburing/archive/refs/tags/liburing-$URING_V.tar.gz" "$URING_SHA")
# configure generates include/liburing/compat.h + io_uring_version.h; we only need the static
# lib, so copy it and the headers ourselves instead of using the install target.
( cd "$d" && ./configure --prefix="$PREFIX" --cc="$CC" >/dev/null \
  && make -s -C src -j"$JOBS" liburing.a CFLAGS="$CFLAGS" )
cp "$d/src/liburing.a" "$PREFIX/lib/"
cp "$d/src/include/liburing.h" "$PREFIX/include/"
rm -rf "$PREFIX/include/liburing" && cp -r "$d/src/include/liburing" "$PREFIX/include/liburing"

echo ">>> BLAKE3 $BLAKE3_V"
d=$(fetch "https://github.com/BLAKE3-team/BLAKE3/archive/refs/tags/$BLAKE3_V.tar.gz" "$BLAKE3_SHA")/c
srcs="blake3.c blake3_dispatch.c blake3_portable.c"
b3flags=""
case "$(uname -m)" in
    x86_64)        srcs="$srcs blake3_sse2_x86-64_unix.S blake3_sse41_x86-64_unix.S blake3_avx2_x86-64_unix.S blake3_avx512_x86-64_unix.S" ;;
    aarch64|arm64) srcs="$srcs blake3_neon.c"; b3flags="-DBLAKE3_USE_NEON=1" ;;
    *)             b3flags="-DBLAKE3_NO_SSE2 -DBLAKE3_NO_SSE41 -DBLAKE3_NO_AVX2 -DBLAKE3_NO_AVX512" ;;
esac
( cd "$d" && for s in $srcs; do $CC $CFLAGS $b3flags -c "$s" -o "${s%.*}.o"; done \
  && rm -f "$PREFIX/lib/libblake3.a" && $AR rcs "$PREFIX/lib/libblake3.a" ./*.o )
cp "$d/blake3.h" "$PREFIX/include/"

echo ">>> done: $PREFIX"
ls -1 "$PREFIX/lib"
