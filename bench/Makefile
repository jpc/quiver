# quiver benchmark suite — see README.md
ZSTD   ?= /mnt/weka/jpc/miniconda3/pkgs/zstd-1.5.6-hc292b87_0
SODIUM ?= /mnt/weka/jpc/miniconda3/envs/brouhaha
BLAKE3 ?= /mnt/weka/jpc/tmp/blake3-install
CFLAGS ?= -O2 -pthread -w

all: fsbw fsops cfr rw framecap chunking hashes crypto scanprobe unlink_threads numa_zstd

fsbw: fsbw.c ; $(CC) $(CFLAGS) -o $@ $<
fsops: fsops.c ; $(CC) $(CFLAGS) -o $@ $<
cfr: cfr.c ; $(CC) $(CFLAGS) -o $@ $<
rw: rw.c ; $(CC) $(CFLAGS) -o $@ $<
framecap: framecap.c ; $(CC) $(CFLAGS) -I$(ZSTD)/include -o $@ $< $(ZSTD)/lib/libzstd.a
chunking: chunking.c ; $(CC) $(CFLAGS) -march=native -o $@ $<
scanprobe: scanprobe.c ; $(CC) $(CFLAGS) -o $@ $<
unlink_threads: unlink_threads.c ; $(CC) $(CFLAGS) -o $@ $<
hashes: hashes.c ; $(CC) $(CFLAGS) -march=native -I$(SODIUM)/include -o $@ $< $(SODIUM)/lib/libsodium.a
crypto: crypto.c ; $(CC) $(CFLAGS) -march=native -I$(SODIUM)/include -o $@ $< $(SODIUM)/lib/libsodium.a
clean: ; rm -f fsbw fsops cfr rw framecap chunking hashes crypto scanprobe unlink_threads numa_zstd
NUMA_FLAGS := $(shell test -f /usr/include/numa.h && echo "-DHAVE_NUMA -lnuma")
numa_zstd: numa_zstd.c ; $(CC) $(CFLAGS) -I$(ZSTD)/include -o $@ $< $(ZSTD)/lib/libzstd.a $(NUMA_FLAGS)
