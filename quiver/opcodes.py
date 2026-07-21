"""Single source of truth for executor opcodes.

Both the Python control plane (quiver/wire.py) and the C data plane
(quiver-exec.c, via generated #defines in ipc_gen.h) derive their opcode
constants from this dict — so a new opcode can't drift between the two."""

OPCODES = {
    "UNLINK": 2, "RMDIR": 3, "MKDIR": 4, "COPY": 5, "CKSUM": 6,
    "FBARRIER": 7, "SETMETA": 8, "EXTRACT": 9, "COMPRESS": 10,
}
