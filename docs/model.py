"""
model.py — a simplified reference model of the quiver IO machine.

This is NOT the implementation (that's a Polars compiler + a C executor talking
Arrow-IPC over pipes). It's the mental model in plain Python, so the components
in docs/ISA.md and docs/MACHINE.md have concrete shapes. Primitives like
walk()/decompress()/zstd() are stubbed — the point is the structure.

Three levels (docs/ISA.md §0):
    INSTRUCTIONS  what the machine executes        (generators, transformers, fence)
    PROGRAMS      compile intent into instructions  (rm, cp, rsync, pack, recompress)
    SERVICES      wrap the streams, aren't executed (linker, WAL, S3 uploader)
"""
from dataclasses import dataclass, field


# ── Addressing modes (ISA.md §2): where operand bytes come from / go ─────────
@dataclass
class File:    path: str; off: int = 0; length: int = -1    # a filesystem range
@dataclass
class Archive: off: int = 0; length: int = 0                 # a range of the host
@dataclass
class Inline:  data: bytes = b""                             # bytes in the instr
@dataclass
class Stream:  source_id: int = 0; ordinal: int = 0         # a tar.zstd member
@dataclass
class Append:  sink: int = 0                                 # append to sink's host
@dataclass
class Upload:  sink: int = 0                                 # append to an S3 object

IDENTITY, ZSTD_C, ZSTD_D, CKSUM = "identity", "zstd_c", "zstd_d", "cksum"


# ── The instruction word (CMD_SCHEMA, simplified) and its completion ─────────
@dataclass
class Instr:
    op: str                       # the opcode
    src: object = None            # source addressing mode (File/Archive/Inline/Stream)
    dst: object = None            # sink addressing mode   (File/Append/Upload/None)
    xform: str = IDENTITY         # transform applied in flight
    level: int = 0               # xform_param (e.g. zstd level) — not overloaded here
    frame: int = -1              # which output frame (COMPRESS groups members)
    dep: int = 0                 # ordering group / epoch (the DAG, collapsed)
    tag: int = 0                 # user_data: the reorder-buffer tag


@dataclass
class Done:                       # a retirement (COMP, with role-named results)
    tag: int                      # which instruction this retires
    status: int = 0              # errno
    out_off: int = 0             # run-time destination offset  (real ISA: read_size)
    out_len: int = 0             # run-time length / checksum   (real ISA: cksum)


# ── Generators: read an address space → rows (scan port, 1 request → N) ──────
# These are the machine's "read state" side — the operand supply the compiler
# plans over. One per source address space.
def SCAN(root):                                   # namespace → stat rows
    for path in walk(root):                       # getdents + statx (in C, off-GIL)
        yield stat_row(path)

def ZSCAN(sources):                               # tar.zstd → member rows (ZMETA)
    for sid, src in enumerate(sources):
        for ordinal, member in enumerate(parse(decompress(src))):
            yield member_row(sid, ordinal, member)


# ── The execute port: command stream → completion stream ─────────────────────
CONTROL = {"MKDIR", "UNLINK", "RMDIR", "SETMETA"}   # → io_uring ring
DATA    = {"COPY", "CKSUM", "EXTRACT", "COMPRESS"}  # → 64-worker pool

def execute(program, host):
    """The interpreter. Epochs run in order (the ordering model); within an
    epoch, independent instructions run in parallel on their functional unit."""
    for epoch in sorted_by(program, key=lambda i: i.dep):
        for instr in parallel(epoch):
            unit = RING if instr.op in CONTROL else POOL   # pick the functional unit
            yield unit.run(instr, host)                    # → a Done (completion)


def run_compress(instr, host):
    """ONE fused data-path instruction, cracked into 3 µops in the executor
    (ISA.md §9). The ~16 MB intermediate never leaves the data plane."""
    buf  = fetch(instr.src)                        # µop 1: LOAD  (decompress + gather)
    comp = zstd(buf, instr.level)                 # µop 2: XFORM (compress)
    off  = host.append(instr.dst.sink, comp)      # µop 3: SINK  (append / upload)
    return Done(instr.tag, out_off=off, out_len=len(comp))   # dynamic offsets retire


# ── Programs: compile intent into an instruction stream (this is the planner) ─
def rm(root):
    for r in SCAN(root):                          # children before parents:
        op = "RMDIR" if r.is_dir else "UNLINK"    # deeper depth → earlier epoch
        yield Instr(op, src=File(r.path), dep=-r.depth)

def rsync(src, dst):                              # a reconciliation program
    have = {r.relpath: r for r in SCAN(dst)}
    for s in SCAN(src):
        d = have.pop(s.relpath, None)
        if d is None or (d.mtime, d.size) != (s.mtime, s.size):
            yield Instr("COPY", src=File(s.path), dst=File(dst + s.relpath))
    for stray in have.values():                   # delete what src no longer has
        yield Instr("UNLINK", src=File(stray.path))

def pack(src, host):                              # serialize: static offsets
    off = 0
    for r in SCAN(src):
        yield Instr("COPY", src=File(r.path), dst=Archive(off), frame=-1)
        off += r.size                             # sizes known → offsets are static

def recompress(inputs, sink_of=lambda m: 0):     # = pack with STREAM + ZSTD_C
    for m in plan_frames(ZSCAN(inputs)):          # filter / frame-assign in Polars
        yield Instr("COMPRESS",
                    src=Stream(m.source_id, m.ordinal),
                    dst=Append(sink_of(m)),        # reshard = a per-instr sink field
                    xform=ZSTD_C, level=10, frame=m.frame)
# extract, du, cp, sync are the same shape: SCAN/read-footer → yield instrs.
# du yields none — it's a pure Polars aggregation over SCAN's rows.


# ── Services: operate on the streams, not in them ────────────────────────────
def link(plan, completions):
    """The linker: build the footer (the relocation table) — static offsets from
    the plan, run-time offsets from completions, merged into name → byte range."""
    at = {c.tag: (c.out_off, c.out_len) for c in completions}
    return [(m.path, m.frame, *at[m.tag]) for m in plan]

def with_wal(completions, wal):
    """Journal retirements for crash-resume; on restart the compiler re-plans and
    drops any instruction whose tag is already committed (re-decode is cheap)."""
    for c in completions:
        wal.append(c); wal.fsync_on_tick()
        yield c


# ── Putting it together ──────────────────────────────────────────────────────
def run(program, host, wal=None):
    plan = list(program)                          # compiled instruction stream
    dones = execute(plan, host)                   # execute port → completions
    if wal is not None:
        dones = with_wal(dones, wal)
    return link(plan, list(dones))                # linker resolves the footer

# distribute(program) = shard `plan` across several execute() interpreters by
# subtree affinity and merge their completions — because it's just a stream.
