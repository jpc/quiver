CFLAGS ?= -O2 -Wall -Wextra -pthread
all: bvm
ipc_gen.h: ../compiler/gen_templates.py
	cd ../.. && PYTHONPATH=. python3 quiver/compiler/gen_templates.py quiver/exec/ipc_gen.h
clean:
	rm -f bvm ipc_gen.h

# --- bvm (the BLOCKS executor; docs/BLOCKS.md, docs/DATAFLOW.md) ---
# zstd static + libsodium static so bvm depends only on glibc + libz (portable).
ZSTD   ?= /mnt/weka/jpc/miniconda3/pkgs/zstd-1.5.6-hc292b87_0
SODIUM ?= /mnt/weka/jpc/miniconda3/envs/brouhaha
URING  ?= /mnt/weka/jpc/tmp/liburing-install
BLAKE3 ?= /mnt/weka/jpc/tmp/blake3-install
# Link to a temp name and RENAME into place. Writing the executable directly replaces the
# text of every process already running it -- and over a shared filesystem the kernel's
# ETXTBSY guard does not reach the nodes running remote executors. rename(2) is atomic and
# leaves them on their original inode. `mv -f` within one directory is that rename.
BVM_OUT ?= bvm
bvm: bvm.c ipc_gen.h
	$(CC) -O2 -pthread -w -I$(ZSTD)/include -I$(SODIUM)/include -I$(URING)/include -I$(BLAKE3)/include -o $(BVM_OUT).tmp bvm.c \
	  $(ZSTD)/lib/libzstd.a $(SODIUM)/lib/libsodium.a $(URING)/lib/liburing.a $(BLAKE3)/lib/libblake3.a
	mv -f $(BVM_OUT).tmp $(BVM_OUT)

# --- qvm2 (ISA v3 prototype; docs/ISA3.md) — same static-link discipline as bvm ---
qvm2: qvm2.c qvm2_gen.h
	$(CC) -O2 -pthread -w -I$(ZSTD)/include -I$(URING)/include -I$(BLAKE3)/include -o qvm2.tmp qvm2.c \
	  $(ZSTD)/lib/libzstd.a $(URING)/lib/liburing.a $(BLAKE3)/lib/libblake3.a
	mv -f qvm2.tmp qvm2
qvm2_gen.h: ../compiler/gen_qvm2.py
	cd ../.. && PYTHONPATH=. python3 quiver/compiler/gen_qvm2.py quiver/exec/qvm2_gen.h
