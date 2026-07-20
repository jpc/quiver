CFLAGS ?= -O2 -Wall -Wextra -pthread
all: quiver-exec
ipc_gen.h: ../compiler/gen_templates.py
	cd .. && PYTHONPATH=../.. python3 compiler/gen_templates.py exec/ipc_gen.h
# MD5 is vendored (md5.c, public domain); build with
#   make CFLAGS="... -DHAVE_OPENSSL" LDLIBS=-lcrypto
# to use OpenSSL's asm implementation instead.
quiver-exec: quiver-exec.c md5.c md5.h ipc_gen.h
	$(CC) $(CFLAGS) -o $@ quiver-exec.c md5.c -luring -lzstd $(LDLIBS)
clean:
	rm -f quiver-exec ipc_gen.h
