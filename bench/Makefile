# quiver benchmark suite — see README.md
# Static deps: the same prefix bvm uses (../quiver/exec/build-deps.sh), or DEPS=<prefix>.
DEPS   ?= $(abspath ../quiver/exec/deps)
ZSTD   ?= $(DEPS)
SODIUM ?= $(DEPS)
URING  ?= $(DEPS)
BLAKE3 ?= $(DEPS)
CFLAGS ?= -O2 -pthread -w

all: fsbw fsops cfr rw dwrite dwriteq uread smallread framecap chunking hashes crypto scanprobe unlink_threads numa_zstd

fsbw: fsbw.c ; $(CC) $(CFLAGS) -o $@ $<
fsops: fsops.c ; $(CC) $(CFLAGS) -o $@ $<
cfr: cfr.c ; $(CC) $(CFLAGS) -o $@ $<
rw: rw.c ; $(CC) $(CFLAGS) -o $@ $<
dwrite: dwrite.c ; $(CC) $(CFLAGS) -o $@ $<
uread: uread.c ; $(CC) $(CFLAGS) -I$(URING)/include -o $@ $< $(URING)/lib/liburing.a
dwriteq: dwriteq.c ; $(CC) $(CFLAGS) -I$(URING)/include -o $@ $< $(URING)/lib/liburing.a
smallread: smallread.c ; $(CC) $(CFLAGS) -I$(URING)/include -o $@ $< $(URING)/lib/liburing.a
framecap: framecap.c ; $(CC) $(CFLAGS) -I$(ZSTD)/include -o $@ $< $(ZSTD)/lib/libzstd.a
chunking: chunking.c ; $(CC) $(CFLAGS) -march=native -o $@ $<
scanprobe: scanprobe.c ; $(CC) $(CFLAGS) -o $@ $<
unlink_threads: unlink_threads.c ; $(CC) $(CFLAGS) -o $@ $<
hashes: hashes.c ; $(CC) $(CFLAGS) -march=native -I$(SODIUM)/include -o $@ $< $(SODIUM)/lib/libsodium.a
crypto: crypto.c ; $(CC) $(CFLAGS) -march=native -I$(SODIUM)/include -o $@ $< $(SODIUM)/lib/libsodium.a
clean: ; rm -f fsbw fsops cfr rw smallread framecap chunking hashes crypto scanprobe unlink_threads numa_zstd
NUMA_FLAGS := $(shell test -f /usr/include/numa.h && echo "-DHAVE_NUMA -lnuma")
numa_zstd: numa_zstd.c ; $(CC) $(CFLAGS) -I$(ZSTD)/include -o $@ $< $(ZSTD)/lib/libzstd.a $(NUMA_FLAGS)
