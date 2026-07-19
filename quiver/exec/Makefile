CFLAGS ?= -O2 -Wall -Wextra -pthread
all: quiver-exec
ipc_gen.h: ../compiler/gen_templates.py
	cd .. && PYTHONPATH=../.. python3 compiler/gen_templates.py exec/ipc_gen.h
quiver-exec: quiver-exec.c ipc_gen.h
	$(CC) $(CFLAGS) -o $@ quiver-exec.c -luring -lcrypto
clean:
	rm -f quiver-exec ipc_gen.h
