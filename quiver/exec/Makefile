CFLAGS ?= -O2 -Wall -Wextra -pthread
all: quiver-exec
ipc_gen.h: ../compiler/gen_templates.py
	cd ../.. && PYTHONPATH=. python3 quiver/compiler/gen_templates.py quiver/exec/ipc_gen.h
# MD5 is vendored (md5.c, public domain); build with
#   make CFLAGS="... -DHAVE_OPENSSL" LDLIBS=-lcrypto
# to use OpenSSL's asm implementation instead.
quiver-exec: quiver-exec.c md5.c md5.h ipc_gen.h
	$(CC) $(CFLAGS) -o $@ quiver-exec.c md5.c -luring -lzstd $(LDLIBS)
clean:
	rm -f quiver-exec ipc_gen.h

# --- bvm (the BLOCKS executor; docs/BLOCKS.md, docs/DATAFLOW.md) ---
# zstd static + libsodium static so bvm depends only on glibc + libz (portable).
ZSTD   ?= /mnt/weka/jpc/miniconda3/pkgs/zstd-1.5.6-hc292b87_0
SODIUM ?= /mnt/weka/jpc/miniconda3/envs/brouhaha
URING  ?= /mnt/weka/jpc/tmp/liburing-install
BLAKE3 ?= /mnt/weka/jpc/tmp/blake3-install
bvm: bvm.c ipc_gen.h
	$(CC) -O2 -pthread -w -I$(ZSTD)/include -I$(SODIUM)/include -I$(URING)/include -I$(BLAKE3)/include -o $@ bvm.c \
	  $(ZSTD)/lib/libzstd.a $(SODIUM)/lib/libsodium.a $(URING)/lib/liburing.a $(BLAKE3)/lib/libblake3.a
