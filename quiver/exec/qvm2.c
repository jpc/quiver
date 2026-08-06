#define _GNU_SOURCE
/* qvm2 — ISA v3 prototype: fibers over dynamic dataflow values (docs/ISA3.md).
 *
 * MILESTONES (each must pack real bytes end-to-end and self-verify):
 *   M0  scheduler core: fibers/spawn/join over io_uring + compute pool (eventfd),
 *       chunk-list vals + byte budget, mov {fs,val,inline}->{val,sink}, codec vals
 *       (zstd), sink reserve. Self-test: pack a dir's files into tar-compat zstd
 *       frames, decompress + compare -> PASS/FAIL.                     [DONE]
 *   M0.5 Arrow instruction-stream front-end (`qvm2 run prog.arrow`): narrow
 *       10-col schema, vals as PLANNER-ASSIGNED dataflow-edge ids resolved in a
 *       growable table; Python (polars _ipc_bytes) drives the binary directly —
 *       tests need no C harness.                                       [DONE]
 *   M1  STREAM vals (chase + release-behind), DIGEST-on-mov (blake3), unzstd
 *       codec vals, fs-dst scatter with TRUNC, val ranges, EMIT records.  [DONE]
 *       Round-trip gate: pack w/ digests -> EMIT records -> unpack generated
 *       FROM the records, decode window chased by a separate consumer fiber,
 *       byte-exact + digests match python's. Two bugs caught by the gate:
 *       eventfd double-read (latent since M0 — the armed ring read already
 *       consumes the counter; a second read blocked the scheduler), and the
 *       stream walker stepping onto NULL and losing its anchor.
 *   M2  namespace ops (mkdir/symlink/link/setmeta/unlink/rmdir) as blocking-pool
 *       jobs (ONE completion mechanism; uring-native variants are an M3+ optim),
 *       FENCE as a documented no-op. Gate: the FULL ISA3 §4.6 unpack shape —
 *       dirs+symlinks+hardlink+restrictive-modes-last as program structure. [DONE]
 *   M3  STREAMING WIRE + wave-scheduled scan.                          [DONE]
 *       Instruction batches over stdin DURING execution; tid 0 parks on the
 *       open stream; pipe close is EOF (Arrow EOS markers are per-blob noise).
 *       SCANDIR generator leaf: readdir on pool, sequential statx quanta, one
 *       batched emit per dir; the frontier holds NO fds — the DirJob
 *       fd-exhaustion class is structurally gone. Planner reschedules deeper
 *       fibers per wave; gate measured waves == depth exactly, and the whole
 *       run is REPLAYABLE from the logged instruction stream.
 *   M4  a wire expander for ONE bvm verb (META), side-by-side with bvm.
 *
 * NON-GOALS still: footers, WAL, stats, bvm wire verbs (until M3).
 *
 * COMPLEXITY GUARDRAILS (rethink if violated):
 *   - one scheduler thread; workers touch ONLY their job + eventfd
 *   - a fiber suspends on exactly one thing at a time (cqe | job | val | join | budget)
 *   - no instruction owns memory; vals own chunks, scopes own vals, that's it
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <sys/eventfd.h>
#include <sys/stat.h>
#include <liburing.h>
#include <sys/syscall.h>
#include <dirent.h>
#include <zstd.h>
#include <blake3.h>
#include "qvm2_gen.h"

#define CHUNK   (4 << 20)                       /* val chunk size */
#define QD      4096                            /* ring depth */
static int NWORK = 8;                           /* compute pool (env QVM2_WORKERS) */
#define NWORK_MAX 64

/* ------------------------------------------------------------------ vals */
typedef struct VChunk { uint8_t *b; int64_t len; struct VChunk *next; } VChunk;
typedef struct Fiber Fiber;
typedef struct Val {
    VChunk *head, *tail;
    int64_t len;                                /* bytes appended (post-codec) */
    int     closed;                             /* no more producers */
    int     codec;                              /* 0 none, 1 zstd-c, 2 zstd-d */
    int     clevel;
    int64_t charged;                            /* budget held by this val's chunks */
    _Atomic int jobs;                           /* codec jobs in flight/queued: FREE
                                                 * must wait for zero — enforced, not
                                                 * assumed (ASan caught a straggler) */
    int64_t pledged;                            /* known-in-advance raw size: stamps the
                                                 * zstd frame's content size, which the
                                                 * structural verify's anti-truncation
                                                 * check depends on (bvm frames have it) */
    int     stream;                             /* STREAM: single consumer chases the
                                                 * producer; chunks free behind it.
                                                 * 0 = RANDOM: consumers wait for close. */
    ZSTD_CCtx *cc; ZSTD_DCtx *dc;               /* codec state, created lazily */
    Fiber  *waiters;                            /* parked consumers (close OR growth) */
    struct Val *snext;                          /* scope free-list */
} Val;

static int64_t g_budget = 4LL << 30;   /* env QVM2_BUDGET (MB) */ static _Atomic int64_t g_live = 0;
static Fiber  *g_budget_waiters = NULL;
static int     g_nlive_fibers = 0;              /* sole-runner rule input */

/* --------------------------------------------- Arrow IPC reader (from bvm.c) */
static uint16_t fb_u16(const uint8_t *b, int64_t o){ uint16_t v; memcpy(&v,b+o,2); return v; }
static int32_t  fb_i32(const uint8_t *b, int64_t o){ int32_t v;  memcpy(&v,b+o,4); return v; }
static uint32_t fb_u32(const uint8_t *b, int64_t o){ uint32_t v; memcpy(&v,b+o,4); return v; }
static int64_t  fb_i64(const uint8_t *b, int64_t o){ int64_t v;  memcpy(&v,b+o,8); return v; }
static int64_t  fb_root(const uint8_t *b) { return fb_u32(b, 0); }
static int64_t fb_field(const uint8_t *b, int64_t table, int id) {
    int64_t vt = table - fb_i32(b, table);
    int slot = 4 + 2 * id;
    if (slot >= fb_u16(b, vt)) return -1;
    uint16_t voff = fb_u16(b, vt + slot);
    return voff ? table + voff : -1;
}
static int64_t fb_offset_field(const uint8_t *b, int64_t table, int id) {
    int64_t p = fb_field(b, table, id);
    return p < 0 ? -1 : p + fb_u32(b, p);
}
static int64_t arrow_next(const uint8_t **cur, const uint8_t **bufp, int nbufs_expect) {
    for (;;) {
        const uint8_t *q = *cur;
        uint32_t mlen; memcpy(&mlen, q + 4, 4);
        if (mlen == 0) return 0;
        const uint8_t *meta = q + 8;
        int64_t rt = fb_root(meta);
        int64_t blp = fb_field(meta, rt, 3);
        int64_t blen = blp >= 0 ? fb_i64(meta, blp) : 0;
        const uint8_t *body = meta + mlen;
        *cur = body + blen;
        int64_t htp = fb_field(meta, rt, 1);
        if (htp < 0) return -1;
        if (meta[htp] != 3) continue;
        int64_t rb = fb_offset_field(meta, rt, 2);
        int64_t n = fb_i64(meta, fb_field(meta, rb, 0));
        int64_t bufs = fb_offset_field(meta, rb, 2);
        if ((int)fb_u32(meta, bufs) != nbufs_expect) return -1;
        for (int i = 0; i < nbufs_expect; i++) bufp[i] = body + fb_i64(meta, bufs + 4 + 16 * i);
        return n;
    }
}

/* ---------------------------------------------- QREC columnar emit (C Arrow) */
/* Records leave the VM as Arrow IPC batches built from compile-time templates
 * (qvm2_gen.h, the bvm bc_arrow pattern) — the planner reads them VECTORIZED
 * (pl.read_ipc_stream) instead of a per-record Python loop. */
typedef struct { const void *p; int64_t len; } QWBuf;
typedef struct {
    int64_t *noff; char *ndat; size_t ndcap, ndlen;
    uint8_t *kind; int32_t *mode; int64_t *size, *mt; int32_t *uid, *gid;
    uint32_t n, cap;
} QC;
static void qc_add(QC *q, const char *name, size_t nlen, uint8_t kind, uint32_t mode,
                   int64_t size, int64_t mt, uint32_t uid, uint32_t gid) {
    if (q->n == q->cap) {
        uint32_t c = q->cap ? q->cap * 2 : 1024; q->cap = c;
        q->noff = realloc(q->noff, (c + 1) * 8); q->kind = realloc(q->kind, c);
        q->mode = realloc(q->mode, c * 4); q->size = realloc(q->size, c * 8);
        q->mt = realloc(q->mt, c * 8); q->uid = realloc(q->uid, c * 4); q->gid = realloc(q->gid, c * 4);
    }
    if (q->ndlen + nlen > q->ndcap) { q->ndcap = (q->ndlen + nlen) * 2 + 4096; q->ndat = realloc(q->ndat, q->ndcap); }
    if (q->n == 0) q->noff[0] = 0;
    memcpy(q->ndat + q->ndlen, name, nlen); q->ndlen += nlen;
    q->noff[q->n + 1] = (int64_t)q->ndlen;
    q->kind[q->n] = kind; q->mode[q->n] = (int32_t)mode; q->size[q->n] = size;
    q->mt[q->n] = mt; q->uid[q->n] = (int32_t)uid; q->gid[q->n] = (int32_t)gid;
    q->n++;
}
static void qc_free(QC *q) {
    free(q->noff); free(q->ndat); free(q->kind); free(q->mode);
    free(q->size); free(q->mt); free(q->uid); free(q->gid); memset(q, 0, sizeof *q);
}
static uint8_t *qc_arrow(QC *q, int32_t tid, uint8_t final_, uint8_t phase, size_t *outlen) {
    /* ONE bare batch message (no schema, no EOS): the sink carries the schema once
     * at open, so the whole emit file is a single Arrow stream the planner parses
     * with one C++ stream reader per poll instead of one call per block. */
    int64_t n = q->n, zero = 0; static const int64_t zoff = 0;
    const void *no = q->noff ? (const void *)q->noff : (const void *)&zoff;
    int32_t *tida = malloc(n * 4 + 4); uint8_t *fina = malloc(n + 1), *pha = malloc(n + 1);
    for (int64_t i = 0; i < n; i++) tida[i] = tid;
    memset(fina, final_, (size_t)n); memset(pha, phase, (size_t)n);
    QWBuf bufs[QREC_N_BUFS] = {
        {NULL,0},{no,8*(n+1)},{q->ndat,(int64_t)q->ndlen},
        {NULL,0},{q->kind,n},
        {NULL,0},{q->mode,4*n}, {NULL,0},{q->size,8*n}, {NULL,0},{q->mt,8*n},
        {NULL,0},{q->uid,4*n},  {NULL,0},{q->gid,4*n},
        {NULL,0},{tida,4*n},    {NULL,0},{fina,n},      {NULL,0},{pha,n}};
    uint8_t meta[QREC_TMPL_LEN]; memcpy(meta, QREC_BATCH_TMPL, QREC_TMPL_LEN);
    int64_t pos = 0;
    for (int i = 0; i < QREC_N_BUFS; i++) {
        memcpy(meta+QREC_BUF_OFF[i], &pos, 8); memcpy(meta+QREC_BUF_OFF[i]+8, &bufs[i].len, 8);
        pos += (bufs[i].len + 7) & ~7LL;
    }
    memcpy(meta+QREC_OFF_BODYLEN, &pos, 8); memcpy(meta+QREC_OFF_RBLEN, &n, 8);
    for (int i = 0; i < QREC_N_NODES; i++) {
        memcpy(meta+QREC_NODE_OFF[i], &n, 8); memcpy(meta+QREC_NODE_OFF[i]+8, &zero, 8);
    }
    size_t arrow = 8+QREC_TMPL_LEN + (size_t)pos;
    uint8_t *out = malloc(arrow), *pp = out;
    uint32_t fr[2] = {0xFFFFFFFFu, QREC_TMPL_LEN};
    memcpy(pp,fr,8); pp+=8; memcpy(pp,meta,QREC_TMPL_LEN); pp+=QREC_TMPL_LEN;
    for (int i = 0; i < QREC_N_BUFS; i++) if (bufs[i].len) {
        memcpy(pp,bufs[i].p,(size_t)bufs[i].len); pp+=bufs[i].len;
        size_t pad = (size_t)((-bufs[i].len) & 7); memset(pp,0,pad); pp+=pad;
    }
    free(tida); free(fina); free(pha);
    *outlen = arrow;
    return out;
}

/* ------------------------------------------------------------------ instrs *//* ------------------------------------------------------------------ instrs */
enum { I_NEWVAL, I_MOV, I_CLOSE, I_SPAWN, I_JOIN, I_SINK, I_EMIT,
       I_MKDIR, I_SYMLINK, I_LINK, I_SETMETA, I_UNLINK, I_RMDIR, I_FENCE,
       I_READDIR, I_STATB,                      /* wave-scan leaves (coarse fan-out only:
                                                 * per-dir waves measured 85x off a flat
                                                 * walker — see the I_SCAN generator) */
       I_SCAN,                                  /* the C walker as a generator leaf */
       I_FREE };                                /* explicit val release — v1 lifetime:
                                                 * macros FREE after the last consumer;
                                                 * scope-freeing (ISA3) supersedes later */
enum { E_FS, E_VAL, E_INLINE, E_SINK };         /* endpoint kinds */
/* Vals are named by PLANNER-ASSIGNED ids — dataflow edge names, not slots: the
 * runtime maps id -> Val* in a growable table at execution, ids carry no
 * capacity, no reuse, no placement. NEWVAL(vid) creates; the planner contract
 * is that a fiber's NEWVAL precedes any SPAWN of fibers that reference it. */
typedef struct {
    uint8_t op;
    uint8_t skind, dkind;                       /* mov endpoint kinds */
    const char *path;                           /* E_FS src (points into prog blob) */
    int64_t svid, dvid;                         /* E_VAL src/dst ids */
    int64_t soff, slen;                         /* src range (-1 len = to end) */
    const uint8_t *inl; int64_t inlen;          /* E_INLINE (points into prog blob) */
    int sink;                                   /* E_SINK dst */
    int lo, hi;                                 /* spawn/join */
    int64_t cvid;                               /* close / newval target */
    int codec, clevel, stream_flag;             /* newval */
    const char *target;                         /* symlink/link source path */
    int nofollow;                               /* setmeta: lchown + NOFOLLOW utimens */
    int64_t mode, mtime, uid, gid;              /* namespace metadata operands */
    int digest;                                 /* mov: hash source bytes in passing */
    int64_t fsize;                              /* mov ->fs: TRUNC size (-1 none) */
} Instr;

static Val **g_vals; static int64_t g_nvals;
static Val *val_at(int64_t vid) { return (vid >= 0 && vid < g_nvals) ? g_vals[vid] : NULL; }
static void val_bind(int64_t vid, Val *v) {
    if (vid >= g_nvals) {
        int64_t nc = vid + 64;
        g_vals = realloc(g_vals, nc * sizeof(Val *));
        memset(g_vals + g_nvals, 0, (nc - g_nvals) * sizeof(Val *));
        g_nvals = nc;
    }
    g_vals[vid] = v;
}

/* ------------------------------------------------------------------ fibers */
struct Fiber {
    int tid;
    Instr *prog; int n, pc;
    enum { F_INERT, F_READY, F_WAIT_CQE, F_WAIT_JOB, F_WAIT_VAL,
           F_WAIT_BUDGET, F_WAIT_JOIN, F_WAIT_STREAM, F_DONE } st;
    int pcap;                                   /* prog capacity (streamed appends) */
    int join_lo, join_hi;
    /* THE CURSOR — architectural per-fiber transfer state, the ONLY in-flight
     * state any instruction may keep (guardrail: an op needing more than
     * {FRESH, RUN} must be split into smaller ISA ops instead). Zeroed at every
     * pc advance; every op advances by exactly ONE quantum (one cqe / one job /
     * one chunk) and either retires (pc++, cursor reset) or stays at pc. */
    struct {
        uint8_t phase;                          /* 0 FRESH, 1 RUN */
        int fd;                                 /* fs source (open at FRESH) */
        int64_t off, remain;                    /* source progress */
        VChunk *c;                              /* val-source chunk walker */
        VChunk *fin;                            /* STREAM: consumed chunk, freed on the
                                                 * NEXT quantum (its bytes may still be
                                                 * under an in-flight write) */
        int64_t coff;                           /* offset within cur.c (range reads) */
        int64_t skip;                           /* val-source bytes still to skip (soff) */
        int64_t dst_off, base;                  /* sink/fs dst cursor / base */
        blake3_hasher bh; int hashing;          /* DIGEST-in-passing */
        void *dents; int64_t dlen, didx;        /* SCANDIR: dirent blob walker */
        QC qc;                                  /* scan records, columnar (QREC emit) */
        uint8_t *ablk; int64_t ablen;           /* serialized block awaiting its write */
        int64_t rec_len;                        /* -1 = final block written */
        struct statx stx;                       /* SCANDIR: current entry's stat */
    } cur;
    int64_t dbg_base, dbg_len, dbg_digest;                           /* last sink reservation base — a completion
                                                 * RECORDS (EMIT carries them), not resume
                                                 * state; never consulted by ops */
    uint8_t *iob;                               /* staging buffer (chunk-sized) */
    Fiber *wnext, *rnext;
};

static Fiber **g_fib; static int g_nfib, g_fibcap;
static uint8_t *g_pb; static size_t g_pbcap, g_pblen, g_ppos;   /* streamed program */
static int g_stdin_tag;
static int g_open_inflight;                     /* opens in io-wq: ADMISSION-gated.
                                                 * Unthrottled IOSQE_ASYNC let 113k
                                                 * fibers submit before one reap —
                                                 * SQ/CQ overflow, scheduler livelock
                                                 * in sqe_get (3600s timeout). */
static int OPEN_GATE = 512;                     /* env QVM2_OPEN_GATE (debug: tiny
                                                 * values exercise the park/wake paths
                                                 * locally in seconds) */
static uint8_t g_stage[1 << 16];
static void stream_parse(void);
static void stdin_arm(void);
static int g_stream_eof = 1;                    /* file mode: whole program up front */
static Fiber *fib_get(int tid) {                /* grow-safe: pointers never move */
    if (tid >= g_fibcap) {
        int nc = tid + 256;
        g_fib = realloc(g_fib, nc * sizeof(Fiber *));
        memset(g_fib + g_fibcap, 0, (nc - g_fibcap) * sizeof(Fiber *));
        g_fibcap = nc;
    }
    if (!g_fib[tid]) {
        Fiber *f = calloc(1, sizeof(Fiber));
        f->tid = tid; f->st = F_INERT;
        g_fib[tid] = f;
        if (tid >= g_nfib) g_nfib = tid + 1;
    }
    return g_fib[tid];
}
static int g_trace = 0;
#define TR(...) do { if (g_trace) fprintf(stderr, __VA_ARGS__); } while (0)
static Fiber *g_ready_h, *g_ready_t;
/* O(1) join accounting: a parked JOIN registers a countdown; each completion
 * decrements matching registrations. The previous scheme scanned the WHOLE
 * fiber table per completion and the whole range per hit — O(n^2) that cost
 * 79 min of a 1M-fiber unpack while looking like "scheduler overhead". */
typedef struct JoinWait { Fiber *joiner; int lo, hi; int64_t remaining;
                          struct JoinWait *next; } JoinWait;
static JoinWait *g_joins;
static struct io_uring g_ring;
static struct io_uring_sqe *sqe_get(void) {     /* SQ full: flush and retry — thousands
                                                 * of scan fibers WILL fill any depth */
    struct io_uring_sqe *sq = io_uring_get_sqe(&g_ring);
    while (!sq) { io_uring_submit(&g_ring); sq = io_uring_get_sqe(&g_ring); }
    return sq;
}
static int g_evfd;

static void ready_push(Fiber *f) {
    if (f->st == F_READY) return;
    f->st = F_READY; f->rnext = NULL;
    if (g_ready_t) g_ready_t->rnext = f; else g_ready_h = f;
    g_ready_t = f;
}
static Fiber *ready_pop(void) {
    Fiber *f = g_ready_h;
    if (f) { g_ready_h = f->rnext; if (!g_ready_h) g_ready_t = NULL; f->rnext = NULL; }
    return f;
}

/* ---- budget: charge n bytes for fiber f; 0 = parked (sole-runner may exceed) */
static int budget_charge(Fiber *f, int64_t n) {
    /* ADMISSION GATE only — accounting moved to val_append (output bytes), release
     * to I_FREE / STREAM fin / val_free. LIMITATION (v1, documented loudly): a single
     * val larger than the whole budget parks its own producer with nothing to free —
     * scopes fix this properly; until then size QVM2_BUDGET above the largest frame. */
    (void)n;
    if (g_live > 0 && g_live >= g_budget) {
        f->wnext = g_budget_waiters; g_budget_waiters = f; f->st = F_WAIT_BUDGET;
        return 0;
    }
    return 1;
}
static void budget_release(int64_t n) {
    g_live -= n;
    Fiber *w = g_budget_waiters; g_budget_waiters = NULL;
    while (w) { Fiber *nx = w->wnext; ready_push(w); w = nx; }
}

static Val *val_new(int codec, int clevel, int stream) {
    Val *v = calloc(1, sizeof(Val));
    v->codec = codec; v->clevel = clevel; v->stream = stream;
    return v;
}
static void val_append(Val *v, const uint8_t *b, int64_t n) {   /* raw append; CHARGES
                                                 * the budget on OUTPUT bytes (atomic:
                                                 * codec workers append off-thread) */
    while (n > 0) {
        if (!v->tail || v->tail->len == CHUNK) {
            VChunk *c = malloc(sizeof(VChunk));
            c->b = malloc(CHUNK); c->len = 0; c->next = NULL;
            if (v->tail) v->tail->next = c; else v->head = c;
            v->tail = c;
        }
        int64_t take = CHUNK - v->tail->len; if (take > n) take = n;
        memcpy(v->tail->b + v->tail->len, b, take);
        v->tail->len += take; v->len += take; v->charged += take; b += take; n -= take;
        g_live += take;
    }
}
static void val_wake(Val *v);
static void val_close(Val *v) {
    if (v->closed) return;
    if (v->dc) { ZSTD_freeDCtx(v->dc); v->dc = NULL; }   /* zstd-d: nothing to flush */
    if (v->cc) {                                /* flush the codec stream */
        uint8_t out[1 << 16];
        for (;;) {
            ZSTD_outBuffer ob = { out, sizeof out, 0 };
            ZSTD_inBuffer  ib = { NULL, 0, 0 };
            size_t r = ZSTD_compressStream2(v->cc, &ob, &ib, ZSTD_e_end);
            if (ob.pos) val_append(v, out, (int64_t)ob.pos);
            if (r == 0) break;
        }
        ZSTD_freeCCtx(v->cc); v->cc = NULL;
    }
    v->closed = 1;
    val_wake(v);
}
static void val_wake(Val *v) {                  /* scheduler-side only (guardrail 1) */
    Fiber *w = v->waiters; v->waiters = NULL;
    while (w) { Fiber *nx = w->wnext; ready_push(w); w = nx; }
}
static void mkparents(char *full) {             /* as bvm.c: EEXIST benign */
    for (char *q = strchr(full + 1, '/'); q; q = strchr(q + 1, '/')) { *q = 0; mkdir(full, 0777); *q = '/'; }
}
static void val_free(Val *v) {
    for (VChunk *c = v->head; c; ) { VChunk *nx = c->next; free(c->b); free(c); c = nx; }
    budget_release(v->charged);
    free(v);
}

/* ------------------------------------------------------------------ compute pool */
/* A job = "push these bytes through this val's codec". Completion -> eventfd.
 * The scheduler only hands one job per codec-val at a time (fiber order), so no
 * codec state is ever touched by two workers. */
typedef struct Job {
    int kind;                                   /* 0 codec, 1 namespace, 2 readdir */
    Val *v; const uint8_t *src; int64_t n;      /* codec */
    struct {                                    /* nsop: COPIED from the instr — a
                                                 * streamed fiber's prog reallocs, so
                                                 * workers must never hold Instr* */
        uint8_t op; int nofollow;
        const char *path, *target;              /* strings are stable allocations */
        int64_t mode, mtime, uid, gid;
    } ns;
    void *rbuf; int64_t rn; int rfd;            /* readdir result: dirent blob + dirfd */
    QC qc;                                      /* statb result: records built on the worker */
    const uint8_t *names; int64_t nlen;         /* statb input: packed [u8 len][name]* */
    Fiber *f;
    struct Job *next;
} Job;
static Job *g_jq_h, *g_jq_t; static pthread_mutex_t g_jmu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_jcv = PTHREAD_COND_INITIALIZER;
static Job *g_done_h; static pthread_mutex_t g_dmu = PTHREAD_MUTEX_INITIALIZER;
static int g_pool_stop = 0;

static void *worker(void *arg) {
    (void)arg;
    for (;;) {
        pthread_mutex_lock(&g_jmu);
        while (!g_jq_h && !g_pool_stop) pthread_cond_wait(&g_jcv, &g_jmu);
        if (g_pool_stop && !g_jq_h) { pthread_mutex_unlock(&g_jmu); return NULL; }
        Job *j = g_jq_h; g_jq_h = j->next; if (!g_jq_h) g_jq_t = NULL;
        pthread_mutex_unlock(&g_jmu);
        if (j->kind == 3) {                     /* STATB: one job = whole batch of statx.
                                                 * Per-ENTRY ring quanta measured 53x slower
                                                 * than bvm — every completion funneled through
                                                 * the one scheduler thread. Per-BATCH pool jobs
                                                 * cut scheduler traffic by the batch size. */
            int fd = open(j->ns.path, O_RDONLY | O_DIRECTORY);
            const char *nm = (const char *)j->names; const char *end = nm + j->nlen;
            while (fd >= 0 && nm < end) {                    /* NUL-separated names — the
                                                              * planner authors them with ONE
                                                              * polars str.join aggregation */
                size_t nl = strlen(nm);
                struct statx stx;
                if (nl && statx(fd, nm, AT_SYMLINK_NOFOLLOW, STATX_BASIC_STATS, &stx) == 0) {
                    int64_t mt = (int64_t)stx.stx_mtime.tv_sec * 1000000000LL + stx.stx_mtime.tv_nsec;
                    qc_add(&j->qc, nm, (uint8_t)nl,
                           S_ISDIR(stx.stx_mode) ? 1 : S_ISREG(stx.stx_mode) ? 0 : 2,
                           stx.stx_mode, (int64_t)stx.stx_size, mt, stx.stx_uid, stx.stx_gid);
                }
                nm += nl + 1;
            }
            if (fd >= 0) close(fd);
            pthread_mutex_lock(&g_dmu); j->next = g_done_h; g_done_h = j; pthread_mutex_unlock(&g_dmu);
            uint64_t one4 = 1; (void)!write(g_evfd, &one4, 8);
            continue;
        }
        if (j->kind == 2) {                     /* readdir: whole dir in one job */
            int fd = open(j->ns.path, O_RDONLY | O_DIRECTORY);
            if (fd >= 0) {
                size_t cap = 1 << 16, len = 0; uint8_t *buf = malloc(cap);
                for (;;) {
                    if (cap - len < (1 << 15)) { cap *= 2; buf = realloc(buf, cap); }
                    long r = syscall(SYS_getdents64, fd, buf + len, cap - len);
                    if (r <= 0) break;
                    len += (size_t)r;
                }
                j->rbuf = buf; j->rn = (int64_t)len; j->rfd = fd;   /* fd stays open for statx */
            } else { j->rbuf = NULL; j->rn = -errno; j->rfd = -1; }
            pthread_mutex_lock(&g_dmu); j->next = g_done_h; g_done_h = j; pthread_mutex_unlock(&g_dmu);
            uint64_t one3 = 1; (void)!write(g_evfd, &one3, 8);
            continue;
        }
        if (j->kind == 1) {                     /* namespace syscall — the blocking
                                                 * pool IS the io-wq analog; io_uring
                                                 * has no chmod/chown/utimensat anyway,
                                                 * so ONE completion mechanism for all */
            struct { uint8_t op; int nofollow; const char *path, *target;
                     int64_t mode, mtime, uid, gid; } *I = (void *)&j->ns;
            char *dp = (char *)I->path;
            struct timespec ts[2];
            switch (I->op) {
            case I_MKDIR:
                if (mkdir(dp, 0777) && errno == ENOENT) { mkparents(dp); mkdir(dp, 0777); }
                break;                          /* EEXIST benign: ranks share parents */
            case I_SYMLINK:
                (void)!unlink(dp);
                if (symlink(I->target, dp) && errno == ENOENT) { mkparents(dp); (void)!symlink(I->target, dp); }
                if (I->uid || I->gid) (void)!lchown(dp, (uid_t)I->uid, (gid_t)I->gid);
                if (I->mtime) { ts[0].tv_nsec = UTIME_OMIT;
                    ts[1].tv_sec = I->mtime / 1000000000LL; ts[1].tv_nsec = I->mtime % 1000000000LL;
                    (void)!utimensat(AT_FDCWD, dp, ts, AT_SYMLINK_NOFOLLOW); }
                break;
            case I_LINK:
                (void)!unlink(dp);
                if (link(I->target, dp) && errno == ENOENT) { mkparents(dp); (void)!link(I->target, dp); }
                break;
            case I_SETMETA:                     /* chown BEFORE chmod (setgid strip), times LAST */
                if (I->uid || I->gid)
                    (void)!(I->nofollow ? lchown(dp, (uid_t)I->uid, (gid_t)I->gid)
                                        : chown(dp, (uid_t)I->uid, (gid_t)I->gid));
                if (I->mode && !I->nofollow) (void)!chmod(dp, I->mode & 07777);
                if (I->mtime) { ts[0].tv_nsec = UTIME_OMIT;
                    ts[1].tv_sec = I->mtime / 1000000000LL; ts[1].tv_nsec = I->mtime % 1000000000LL;
                    (void)!utimensat(AT_FDCWD, dp, ts, I->nofollow ? AT_SYMLINK_NOFOLLOW : 0); }
                break;
            case I_UNLINK: (void)!unlink(dp); break;
            case I_RMDIR:  (void)!rmdir(dp);  break;   /* M2: no ENOTEMPTY retry yet */
            }
            pthread_mutex_lock(&g_dmu); j->next = g_done_h; g_done_h = j; pthread_mutex_unlock(&g_dmu);
            uint64_t one2 = 1; (void)!write(g_evfd, &one2, 8);
            continue;
        }
        Val *v = j->v;
        uint8_t out[1 << 16];
        ZSTD_inBuffer ib = { j->src, (size_t)j->n, 0 };
        if (v->codec == 2) {                    /* zstd-d: decompress the piece */
            if (!v->dc) v->dc = ZSTD_createDCtx();
            while (ib.pos < ib.size) {
                ZSTD_outBuffer ob = { out, sizeof out, 0 };
                size_t rr = ZSTD_decompressStream(v->dc, &ob, &ib);
                if (ZSTD_isError(rr)) break;
                if (ob.pos) val_append(v, out, (int64_t)ob.pos);
                if (rr == 0 && ib.pos >= ib.size) break;
            }
        } else {
            if (!v->cc) { v->cc = ZSTD_createCCtx();
                ZSTD_CCtx_setParameter(v->cc, ZSTD_c_compressionLevel, v->clevel);
                if (v->pledged > 0) ZSTD_CCtx_setPledgedSrcSize(v->cc, (uint64_t)v->pledged); }
            while (ib.pos < ib.size) {
                ZSTD_outBuffer ob = { out, sizeof out, 0 };
                ZSTD_compressStream2(v->cc, &ob, &ib, ZSTD_e_continue);
                if (ob.pos) val_append(v, out, (int64_t)ob.pos);   /* only this worker touches v */
            }
        }
        v->jobs--;
        pthread_mutex_lock(&g_dmu); j->next = g_done_h; g_done_h = j; pthread_mutex_unlock(&g_dmu);
        uint64_t one = 1; (void)!write(g_evfd, &one, 8);
    }
}
static void job_push_ns(Instr *I, Fiber *f) {
    Job *j = calloc(1, sizeof(Job)); j->kind = 1; j->f = f;
    j->ns.op = I->op; j->ns.nofollow = I->nofollow;
    j->ns.path = I->path; j->ns.target = I->target;
    j->ns.mode = I->mode; j->ns.mtime = I->mtime; j->ns.uid = I->uid; j->ns.gid = I->gid;
    pthread_mutex_lock(&g_jmu);
    if (g_jq_t) g_jq_t->next = j; else g_jq_h = j;
    g_jq_t = j; pthread_cond_signal(&g_jcv); pthread_mutex_unlock(&g_jmu);
}
static void job_push(Val *v, const uint8_t *src, int64_t n, Fiber *f) {
    Job *j = calloc(1, sizeof(Job)); j->v = v; j->src = src; j->n = n; j->f = f; j->next = NULL;
    v->jobs++;
    pthread_mutex_lock(&g_jmu);
    if (g_jq_t) g_jq_t->next = j; else g_jq_h = j;
    g_jq_t = j; pthread_cond_signal(&g_jcv); pthread_mutex_unlock(&g_jmu);
}

/* ------------------------------------------------------------------ sinks */
typedef struct { int fd; int64_t cursor; pthread_mutex_t mu; } Sink;
static Sink g_sinks[8] = {
    {0,0,PTHREAD_MUTEX_INITIALIZER},{0,0,PTHREAD_MUTEX_INITIALIZER},
    {0,0,PTHREAD_MUTEX_INITIALIZER},{0,0,PTHREAD_MUTEX_INITIALIZER},
    {0,0,PTHREAD_MUTEX_INITIALIZER},{0,0,PTHREAD_MUTEX_INITIALIZER},
    {0,0,PTHREAD_MUTEX_INITIALIZER},{0,0,PTHREAD_MUTEX_INITIALIZER}};

/* ------------------------------------------------------------------ scheduler */
/* Drive fiber f forward until it parks or finishes. Guardrail 2: every await
 * point returns; the cqe/eventfd reaper re-readies the fiber. */
/* ------------------------------------------ I_SCAN: C walker generator leaf */
/* The namespace walk at bvm's granularity: a walker-thread pool over a shared
 * dir queue, per-walker columnar batches flushed straight to the (mutexed) sink.
 * The fiber system sees ONE op; per-dir waves are for planner-scale fan-out only
 * (measured: 85x gap). EMFILE: the job re-queues with a path, holding NO fd —
 * the DirJob fd-exhaustion class stays structurally gone. ONE active scan per VM
 * (a scan-instance table is deliberate future work, not accidental complexity). */
typedef struct SDir { char *rel; struct SDir *next; } SDir;
static struct {
    pthread_mutex_t mu; pthread_cond_t cv;
    SDir *h, *t; int pending, stop;
    int rootfd, sink; int32_t tid;
    Fiber *owner;
    pthread_t th[NWORK_MAX]; int nth;
} g_scan;
static void scan_push(SDir *d) {                 /* mu held */
    d->next = NULL;
    if (g_scan.t) g_scan.t->next = d; else g_scan.h = d;
    g_scan.t = d;
}
static void scan_flush(QC *qc, int final_) {
    if (!qc->n && !final_) return;
    if (final_) qc_add(qc, "", 0, 255, 0, 0, 0, 0, 0);
    size_t alen; uint8_t *ab = qc_arrow(qc, g_scan.tid, final_ ? 1 : 0, 1, &alen);
    qc_free(qc);
    Sink *sk = &g_sinks[g_scan.sink];
    pthread_mutex_lock(&sk->mu);
    int64_t off = sk->cursor; sk->cursor += (int64_t)alen;
    pthread_mutex_unlock(&sk->mu);
    int64_t done = 0;                            /* full write, blocking: walker thread */
    while (done < (int64_t)alen) {
        ssize_t w = pwrite(sk->fd, ab + done, (size_t)(alen - done), off + done);
        if (w <= 0) break;
        done += w;
    }
    free(ab);
}
#define SCAN_POP 16                             /* dirs batch-opened per walker pop */
static void scan_one_dir(SDir *d, int dfd, QC *qc) {
    char rel[4096];
    size_t rl = strlen(d->rel);
    if (rl) { memcpy(rel, d->rel, rl); rel[rl] = '/'; }
    if (dfd >= 0) {
        uint8_t buf[1 << 16];
        long r;
        while ((r = syscall(SYS_getdents64, dfd, buf, sizeof buf)) > 0) {
            for (long o = 0; o < r; ) {
                struct dirent64 *de = (struct dirent64 *)(buf + o); o += de->d_reclen;
                if (!strcmp(de->d_name, ".") || !strcmp(de->d_name, "..")) continue;
                size_t nl = strlen(de->d_name);
                size_t tot = (rl ? rl + 1 : 0) + nl;
                if (tot >= sizeof rel) continue;
                memcpy(rel + (rl ? rl + 1 : 0), de->d_name, nl + 1);
                struct statx stx;
                if (statx(dfd, de->d_name, AT_SYMLINK_NOFOLLOW, STATX_BASIC_STATS, &stx))
                    continue;
                int isdir = S_ISDIR(stx.stx_mode);
                int64_t mt = (int64_t)stx.stx_mtime.tv_sec * 1000000000LL + stx.stx_mtime.tv_nsec;
                qc_add(qc, rel, tot, isdir ? 1 : S_ISREG(stx.stx_mode) ? 0 : 2,
                       stx.stx_mode, (int64_t)stx.stx_size, mt, stx.stx_uid, stx.stx_gid);
                if (isdir) {
                    SDir *nd = malloc(sizeof *nd);
                    nd->rel = strdup(rel);
                    pthread_mutex_lock(&g_scan.mu);
                    g_scan.pending++; scan_push(nd); pthread_cond_signal(&g_scan.cv);
                    pthread_mutex_unlock(&g_scan.mu);
                }
            }
        }
        close(dfd);
    }
    if (qc->n >= 20000) scan_flush(qc, 0);
    free(d->rel); free(d);
    pthread_mutex_lock(&g_scan.mu);
    g_scan.pending--;
    if (g_scan.pending == 0) pthread_cond_broadcast(&g_scan.cv);
    pthread_mutex_unlock(&g_scan.mu);
}
static void *scan_walker(void *arg) {
    (void)arg;
    QC qc = {0};
    struct io_uring wr; int wr_ok = io_uring_queue_init(SCAN_POP * 2, &wr, 0) == 0;
    SDir *batch[SCAN_POP]; int fds[SCAN_POP];
    for (;;) {
        int n = 0;
        pthread_mutex_lock(&g_scan.mu);
        while (!g_scan.h && g_scan.pending > 0 && !g_scan.stop)
            pthread_cond_wait(&g_scan.cv, &g_scan.mu);
        if ((!g_scan.h && g_scan.pending == 0) || g_scan.stop) {
            pthread_mutex_unlock(&g_scan.mu); break;
        }
        while (g_scan.h && n < SCAN_POP) {              /* batch pop: fds exist only in
                                                         * WALKERS, never in the queue */
            SDir *d = g_scan.h; g_scan.h = d->next; if (!g_scan.h) g_scan.t = NULL;
            batch[n++] = d;
        }
        pthread_mutex_unlock(&g_scan.mu);
        if (wr_ok && n >= 2) {                          /* ring-batched opens: one latency
                                                         * round for the whole pop (bvm's
                                                         * 0.94->0.79s trick, invariant-safe) */
            for (int i = 0; i < n; i++) {
                struct io_uring_sqe *sq = io_uring_get_sqe(&wr);
                if (batch[i]->rel[0])
                    io_uring_prep_openat(sq, g_scan.rootfd, batch[i]->rel,
                                         O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC, 0);
                else
                    io_uring_prep_openat(sq, g_scan.rootfd, ".",
                                         O_RDONLY | O_DIRECTORY | O_CLOEXEC, 0);
                io_uring_sqe_set_data64(sq, (uint64_t)i);
            }
            io_uring_submit(&wr);
            for (int i = 0; i < n; i++) {
                struct io_uring_cqe *cq;
                if (io_uring_wait_cqe(&wr, &cq) < 0) break;
                fds[(int)io_uring_cqe_get_data64(cq)] = cq->res;
                io_uring_cqe_seen(&wr, cq);
            }
        } else {
            for (int i = 0; i < n; i++) {
                fds[i] = batch[i]->rel[0]
                       ? openat(g_scan.rootfd, batch[i]->rel, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
                       : dup(g_scan.rootfd);
                if (fds[i] < 0) fds[i] = -errno;
            }
        }
        for (int i = 0; i < n; i++) {
            if (fds[i] == -EMFILE || fds[i] == -ENFILE) {
                usleep(1000);                            /* re-queue holding NOTHING */
                pthread_mutex_lock(&g_scan.mu); scan_push(batch[i]);
                pthread_cond_signal(&g_scan.cv); pthread_mutex_unlock(&g_scan.mu);
                continue;
            }
            scan_one_dir(batch[i], fds[i] < 0 ? -1 : fds[i], &qc);
        }
    }
    scan_flush(&qc, 0);
    if (wr_ok) io_uring_queue_exit(&wr);
    return NULL;
}
static void *scan_closer(void *arg) {
    (void)arg;
    for (int i = 0; i < g_scan.nth; i++) pthread_join(g_scan.th[i], NULL);
    QC fin = {0};
    scan_flush(&fin, 1);                         /* the ONE final-marker block */
    close(g_scan.rootfd);
    Job *j = calloc(1, sizeof(Job)); j->kind = 4; j->f = g_scan.owner;
    pthread_mutex_lock(&g_dmu); j->next = g_done_h; g_done_h = j; pthread_mutex_unlock(&g_dmu);
    uint64_t one5 = 1; (void)!write(g_evfd, &one5, 8);
    return NULL;
}
static int scan_start(Fiber *f, const char *root, int sink, int nwalk) {
    int rfd = open(root, O_RDONLY | O_DIRECTORY);
    if (rfd < 0) return -errno;
    memset(&g_scan, 0, sizeof g_scan);
    pthread_mutex_init(&g_scan.mu, NULL); pthread_cond_init(&g_scan.cv, NULL);
    g_scan.rootfd = rfd; g_scan.sink = sink; g_scan.tid = f->tid; g_scan.owner = f;
    if (nwalk < 1) nwalk = 32; if (nwalk > NWORK_MAX) nwalk = NWORK_MAX;
    g_scan.nth = nwalk;
    SDir *d0 = malloc(sizeof *d0); d0->rel = strdup("");
    g_scan.pending = 1; scan_push(d0);
    for (int i = 0; i < nwalk; i++) pthread_create(&g_scan.th[i], NULL, scan_walker, NULL);
    pthread_t ct; pthread_create(&ct, NULL, scan_closer, NULL); pthread_detach(ct);
    return 0;
}

static void fib_step(Fiber *f);

static void join_note_done(int tid) {           /* O(active joins) per completion */
    JoinWait **pp = &g_joins;
    while (*pp) {
        JoinWait *jw = *pp;
        if (tid >= jw->lo && tid <= jw->hi && --jw->remaining == 0) {
            ready_push(jw->joiner);
            *pp = jw->next; free(jw);
            continue;
        }
        pp = &jw->next;
    }
}

static void fib_retire(Fiber *f) {         /* pc advance = cursor reset, no exceptions */
    Instr *I = f->pc < f->n ? &f->prog[f->pc] : NULL;
    if (I && I->op == I_MOV && I->dkind == E_FS && I->fsize >= 0 && f->cur.fd > 0)
        (void)!ftruncate(f->cur.fd, I->fsize);
    if (f->cur.hashing) {                       /* completion record, not resume state */
        uint8_t h[8]; blake3_hasher_finalize(&f->cur.bh, h, 8);
        memcpy(&f->dbg_digest, h, 8);
    }
    if (f->cur.fin) { budget_release(f->cur.fin->len); free(f->cur.fin->b); free(f->cur.fin); }
    /* NOTE: fin belonged to a val whose ->charged already dropped via the quantum path */
    if (f->cur.dents) free(f->cur.dents);
    if (f->cur.ablk) free(f->cur.ablk);
    qc_free(&f->cur.qc);
    if (f->cur.fd > 0) close(f->cur.fd);
    memset(&f->cur, 0, sizeof f->cur);
    f->pc++;
}

static void fib_step(Fiber *f) {
    for (;;) {
        if (f->pc >= f->n) {
            if (f->tid == 0 && !g_stream_eof) { f->st = F_WAIT_STREAM; return; }
            f->st = F_DONE; g_nlive_fibers--;
            join_note_done(f->tid);
            return;
        }
        Instr *I = &f->prog[f->pc];
        TR("t%d pc%d op%d ph%d rem%lld\n", f->tid, f->pc, I->op, f->cur.phase, (long long)f->cur.remain);
        switch (I->op) {
        case I_SPAWN:
            for (int t = I->lo; t <= I->hi; t++) { g_nlive_fibers++; ready_push(fib_get(t)); }
            f->pc++; break;
        case I_JOIN: {
            int64_t rem = 0;                    /* one O(range) count at PARK time */
            for (int t = I->lo; t <= I->hi; t++) if (fib_get(t)->st != F_DONE) rem++;
            if (rem == 0) { f->pc++; break; }
            JoinWait *jw = malloc(sizeof *jw);
            jw->joiner = f; jw->lo = I->lo; jw->hi = I->hi; jw->remaining = rem;
            jw->next = g_joins; g_joins = jw;
            f->join_lo = I->lo; f->join_hi = I->hi; f->st = F_WAIT_JOIN; return;
        }
        case I_NEWVAL:
            { Val *nv = val_new(I->codec, I->clevel, I->stream_flag);
              nv->pledged = I->fsize; val_bind(I->cvid, nv); f->pc++; break; }
        case I_SINK: {
            Sink *sk = &g_sinks[I->sink];
            sk->fd = open(I->path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            sk->cursor = 0;
            if (I->mode == 1) {                  /* arrow sink: schema once, then batches */
                uint8_t hd[8]; uint32_t fr2[2] = {0xFFFFFFFFu, QREC_SCHEMA_LEN};
                memcpy(hd, fr2, 8);
                (void)!pwrite(sk->fd, hd, 8, 0);
                (void)!pwrite(sk->fd, QREC_SCHEMA_META, QREC_SCHEMA_LEN, 8);
                sk->cursor = 8 + QREC_SCHEMA_LEN;
            }
            f->pc++; break;
        }
        case I_CLOSE:
            val_close(val_at(I->cvid)); f->pc++; break;
        case I_MOV: {
            /* FRESH: one-time setup, then RUN quanta until the transfer drains. */
            if (f->cur.phase == 0) {
                f->cur.phase = 1;
                if (I->digest) { blake3_hasher_init(&f->cur.bh); f->cur.hashing = 1; }
                if (I->skind == E_FS) {
                    if (g_open_inflight >= OPEN_GATE) {      /* admission: park, re-FRESH */
                        f->cur.phase = 0;
                        f->wnext = g_budget_waiters; g_budget_waiters = f;
                        f->st = F_WAIT_BUDGET; return;
                    }
                    /* ASYNC open — a sync open() here serialized every file open on
                     * the SCHEDULER thread: 113k dst opens x ~4ms WEKA latency was
                     * the entire 468s frames phase (the '1 D' histogram tail was
                     * this thread, not a worker). fd: 0 = not opened, -1 = in flight. */
                    struct io_uring_sqe *sq = sqe_get();
                    io_uring_prep_openat(sq, AT_FDCWD, I->path, O_RDONLY, 0);
                    /* IOSQE_ASYNC: without it, io_uring attempts the open INLINE in
                     * submit context and wekafs completes it synchronously — every
                     * open still ran on the scheduler thread (main=D in the phase
                     * profile, io-wq workers asleep). Force the offload. */
                    io_uring_sqe_set_flags(sq, IOSQE_ASYNC);
                    io_uring_sqe_set_data(sq, f);
                    io_uring_submit(&g_ring);
                    g_open_inflight++;
                    f->cur.fd = -1;
                    f->st = F_WAIT_CQE; return;
                } else if (I->skind == E_INLINE) {
                    f->cur.remain = I->inlen;
                } else if (I->skind == E_VAL) {
                    Val *sv = val_at(I->svid);
                    if (!sv->stream && !sv->closed) {       /* RANDOM: park till close */
                        f->cur.phase = 0;
                        f->wnext = sv->waiters; sv->waiters = f; f->st = F_WAIT_VAL; return;
                    }
                    f->cur.c = sv->head; f->cur.coff = 0; f->cur.skip = I->soff;
                    f->cur.remain = I->slen >= 0 ? I->slen
                                  : (sv->closed ? sv->len - I->soff : -1);  /* -1: until close */
                    if (I->dkind == E_SINK) {               /* reserve once — needs a length,
                                                             * so STREAM->sink is a plan error */
                        Sink *sk = &g_sinks[I->sink];
                        f->cur.base = f->cur.dst_off = sk->cursor; sk->cursor += sv->len;
                        f->dbg_base = f->cur.base; f->dbg_len = sv->len;
                    }
                }
                if (I->dkind == E_FS) {                     /* scatter destination: async */
                    if (g_open_inflight >= OPEN_GATE) {      /* admission: park, re-FRESH */
                        f->cur.phase = 0;
                        f->wnext = g_budget_waiters; g_budget_waiters = f;
                        f->st = F_WAIT_BUDGET; return;
                    }
                    struct io_uring_sqe *sq = sqe_get();
                    io_uring_prep_openat(sq, AT_FDCWD, I->path, O_WRONLY | O_CREAT, 0644);
                    io_uring_sqe_set_flags(sq, IOSQE_ASYNC);     /* see src-open note */
                    io_uring_sqe_set_data(sq, f);
                    io_uring_submit(&g_ring);
                    g_open_inflight++;
                    f->cur.fd = -1;
                    f->st = F_WAIT_CQE; return;
                }
            }
            if (f->cur.remain == 0) { fib_retire(f); break; }
            /* ---- exactly one quantum ---- */
            if (I->skind == E_INLINE) {                     /* whole payload = one quantum */
                Val *dv = val_at(I->dvid);
                if (!budget_charge(f, I->inlen)) return;
                f->cur.remain = 0;
                if (f->cur.hashing) blake3_hasher_update(&f->cur.bh, I->inl, I->inlen);
                if (dv->codec) { job_push(dv, I->inl, I->inlen, f); f->st = F_WAIT_JOB; return; }
                val_append(dv, I->inl, I->inlen);
                fib_retire(f); break;
            }
            if (I->skind == E_FS) {                         /* one chunk read */
                int64_t want = f->cur.remain > CHUNK ? CHUNK : f->cur.remain;
                struct io_uring_sqe *sq = sqe_get();
                io_uring_prep_read(sq, f->cur.fd, f->iob ? f->iob : (f->iob = malloc(CHUNK)),
                                   (unsigned)want, (uint64_t)f->cur.off);
                io_uring_sqe_set_data(sq, f);
                io_uring_submit(&g_ring);
                f->st = F_WAIT_CQE; return;
            }
            if (I->skind == E_VAL) {
                Val *sv = val_at(I->svid);
                if (f->cur.fin) {                           /* release-behind: freed one quantum
                                                             * late so in-flight writes finish */
                    budget_release(f->cur.fin->len);
                    sv->charged -= f->cur.fin->len;
                    sv->head = f->cur.fin->next;
                    free(f->cur.fin->b); free(f->cur.fin); f->cur.fin = NULL;
                }
                VChunk *c = f->cur.c;
                if (!c) {                                   /* never started: anchor at head.
                                                             * Safe for STREAM too — chunks free
                                                             * only BEHIND this consumer, so head
                                                             * is untouched until we pass it. */
                    if (sv->head) { c = f->cur.c = sv->head; f->cur.coff = 0; }
                    else if (sv->closed) { fib_retire(f); break; }
                    else { f->wnext = sv->waiters; sv->waiters = f; f->st = F_WAIT_VAL; return; }
                }
                if (f->cur.coff >= c->len) {                /* drained this chunk */
                    if (c->next) {                          /* step ONLY onto a real chunk —
                                                             * stepping onto NULL loses the anchor:
                                                             * the producer links next on the chunk
                                                             * we LEFT, and a NULL cursor can never
                                                             * find it again (M1's first bug) */
                        if (sv->stream) f->cur.fin = c;
                        f->cur.c = c->next; f->cur.coff = 0;
                        break;                              /* next quantum frees fin / moves */
                    }
                    if (sv->closed) { fib_retire(f); break; }
                    f->wnext = sv->waiters; sv->waiters = f;   /* caught up: park ON the chunk */
                    f->st = F_WAIT_VAL; return;
                }
                int64_t avail = c->len - f->cur.coff;
                if (f->cur.skip > 0) {                      /* skip quantum (range start) */
                    int64_t sk = f->cur.skip < avail ? f->cur.skip : avail;
                    f->cur.skip -= sk; f->cur.coff += sk;
                    break;
                }
                int64_t n = avail;
                if (f->cur.remain >= 0 && n > f->cur.remain) n = f->cur.remain;
                const uint8_t *src = c->b + f->cur.coff;
                if (f->cur.hashing) blake3_hasher_update(&f->cur.bh, src, n);
                if (I->dkind == E_SINK || I->dkind == E_FS) {
                    int wfd = I->dkind == E_SINK ? g_sinks[I->sink].fd : f->cur.fd;
                    struct io_uring_sqe *sq = sqe_get();
                    io_uring_prep_write(sq, wfd, src, (unsigned)n, (uint64_t)f->cur.dst_off);
                    io_uring_sqe_set_data(sq, f);
                    io_uring_submit(&g_ring);
                    f->cur.dst_off += n; f->cur.coff += n;
                    if (f->cur.remain > 0) f->cur.remain -= n;
                    f->st = F_WAIT_CQE; return;
                }
                if (I->dkind == E_VAL) {
                    Val *dv = val_at(I->dvid);
                    if (!budget_charge(f, n)) return;
                    f->cur.coff += n; if (f->cur.remain > 0) f->cur.remain -= n;
                    if (dv->codec) { job_push(dv, src, n, f); f->st = F_WAIT_JOB; return; }
                    val_append(dv, src, n); break;
                }
            }
            fib_retire(f); break;
        }
        case I_MKDIR: case I_SYMLINK: case I_LINK:
        case I_SETMETA: case I_UNLINK: case I_RMDIR:
            if (f->cur.phase == 0) { f->cur.phase = 1; job_push_ns(I, f); f->st = F_WAIT_JOB; return; }
            fib_retire(f); break;
        case I_READDIR: {
            /* names up: readdir (pool job) -> name records [d_type, nlen, name] in
             * bounded blocks. The pending cqe kind is determined by cursor state
             * alone: no dents yet -> the job; dents -> only flush writes remain. */
            if (f->cur.phase == 0) {
                f->cur.phase = 1;
                Job *j = calloc(1, sizeof(Job)); j->kind = 2; j->f = f; j->ns.path = I->path;
                pthread_mutex_lock(&g_jmu);
                if (g_jq_t) g_jq_t->next = j; else g_jq_h = j;
                g_jq_t = j; pthread_cond_signal(&g_jcv); pthread_mutex_unlock(&g_jmu);
                f->st = F_WAIT_JOB; return;
            }
            if (f->cur.rec_len < 0) { fib_retire(f); break; }
            if (f->cur.ablk) { free(f->cur.ablk); f->cur.ablk = NULL; }   /* prior block flushed */
            {   /* stage names into the columnar batch, flush bounded blocks */
                while (f->cur.didx < f->cur.dlen && f->cur.qc.n < 60000) {
                    struct dirent64 *de = (struct dirent64 *)((uint8_t *)f->cur.dents + f->cur.didx);
                    f->cur.didx += de->d_reclen;
                    if (!strcmp(de->d_name, ".") || !strcmp(de->d_name, "..")) continue;
                    qc_add(&f->cur.qc, de->d_name, (uint8_t)strlen(de->d_name),
                           de->d_type, 0, 0, 0, 0, 0);
                }
                int more = f->cur.didx < f->cur.dlen;
                if (!more) qc_add(&f->cur.qc, "", 0, 255, 0, 0, 0, 0, 0);   /* done marker:
                                                     * exactly one kind==255 row per fiber, so
                                                     * an EMPTY final block still accounts */
                size_t alen;
                uint8_t *ab = qc_arrow(&f->cur.qc, f->tid, more ? 0 : 1, 0, &alen);
                qc_free(&f->cur.qc);
                Sink *sk = &g_sinks[I->sink];
                f->cur.ablk = ab; f->cur.ablen = (int64_t)alen;
                f->cur.dst_off = sk->cursor; sk->cursor += f->cur.ablen;
                struct io_uring_sqe *sq = sqe_get();
                io_uring_prep_write(sq, sk->fd, f->cur.ablk, (unsigned)f->cur.ablen,
                                    (uint64_t)f->cur.dst_off);
                io_uring_sqe_set_data(sq, f);
                io_uring_submit(&g_ring);
                f->cur.rec_len = more ? 0 : -1;
                f->st = F_WAIT_CQE; return;
            }
        }
        case I_STATB: {
            /* bounded stat batch on the POOL (one completion per batch); the emit
             * flush is the only ring quantum this op ever issues. */
            if (f->cur.phase == 0) {
                f->cur.phase = 1;
                Job *j = calloc(1, sizeof(Job)); j->kind = 3; j->f = f;
                j->ns.path = I->path; j->names = I->inl; j->nlen = I->inlen;
                pthread_mutex_lock(&g_jmu);
                if (g_jq_t) g_jq_t->next = j; else g_jq_h = j;
                g_jq_t = j; pthread_cond_signal(&g_jcv); pthread_mutex_unlock(&g_jmu);
                f->st = F_WAIT_JOB; return;
            }
            if (f->cur.rec_len < 0) { fib_retire(f); break; }
            {   /* single final block */
                if (f->cur.ablk) { free(f->cur.ablk); f->cur.ablk = NULL; }
                qc_add(&f->cur.qc, "", 0, 255, 0, 0, 0, 0, 0);              /* done marker */
                size_t alen;
                uint8_t *ab = qc_arrow(&f->cur.qc, f->tid, 1, 1, &alen);
                qc_free(&f->cur.qc);
                Sink *sk = &g_sinks[I->sink];
                f->cur.ablk = ab; f->cur.ablen = (int64_t)alen;
                f->cur.dst_off = sk->cursor; sk->cursor += f->cur.ablen;
                struct io_uring_sqe *sq = sqe_get();
                io_uring_prep_write(sq, sk->fd, f->cur.ablk, (unsigned)f->cur.ablen,
                                    (uint64_t)f->cur.dst_off);
                io_uring_sqe_set_data(sq, f);
                io_uring_submit(&g_ring);
                f->cur.rec_len = -1;
                f->st = F_WAIT_CQE; return;
            }
        }
        case I_SCAN:
            if (f->cur.phase == 0) {
                f->cur.phase = 1;
                if (scan_start(f, I->path, I->sink, (int)I->mode) < 0)
                    fprintf(stderr, "qvm2: scan %s: %m\n", I->path);
                else { f->st = F_WAIT_JOB; return; }
            }
            fib_retire(f); break;
        case I_FREE: {
            Val *fv = val_at(I->cvid);
            if (fv && fv->jobs > 0) {           /* straggler job: WAIT, witnessed */
                TR("t%d FREE defer vid=%lld jobs=%d\n", f->tid, (long long)I->cvid, (int)fv->jobs);
                f->wnext = fv->waiters; fv->waiters = f; f->st = F_WAIT_VAL; return;
            }
            if (fv) { val_free(fv); val_bind(I->cvid, NULL); }
            fib_retire(f); break;
        }
        case I_FENCE:
            /* no-op TODAY: every op completes before its fiber advances and cross-
             * fiber order is spawn/join. Becomes real when namespace ops pipeline. */
            fib_retire(f); break;
        case I_EMIT: {                                      /* 32B completion record -> sink */
            Sink *sk = &g_sinks[I->sink];
            if (f->cur.phase == 0) {
                f->cur.phase = 1;
                f->cur.dst_off = sk->cursor; sk->cursor += 32;
                if (!f->iob) f->iob = malloc(CHUNK);
                memcpy(f->iob, &f->tid, 4); memset(f->iob + 4, 0, 4);
                memcpy(f->iob + 8, &f->dbg_base, 8);
                memcpy(f->iob + 16, &f->dbg_len, 8);
                memcpy(f->iob + 24, &f->dbg_digest, 8);
                struct io_uring_sqe *sq = sqe_get();
                io_uring_prep_write(sq, sk->fd, f->iob, 32, (uint64_t)f->cur.dst_off);
                io_uring_sqe_set_data(sq, f);
                io_uring_submit(&g_ring);
                f->st = F_WAIT_CQE; return;
            }
            fib_retire(f); break;
        }
        }
    }
}

/* resume after a read cqe: bytes landed in f->iob */
static void fib_resume_read(Fiber *f, int res) {
    Instr *I = &f->prog[f->pc];
    if (I->skind == E_FS && I->dkind == E_VAL && res > 0) {
        Val *dv = val_at(I->dvid);
        if (!budget_charge(f, res)) return;                  /* replay quantum untouched */
        f->cur.off += res; f->cur.remain -= res;
        if (f->cur.hashing) blake3_hasher_update(&f->cur.bh, f->iob, res);
        if (dv->codec) { job_push(dv, f->iob, res, f); f->st = F_WAIT_JOB; return; }
        val_append(dv, f->iob, res);
    }
    if (res <= 0) f->cur.remain = 0;
    ready_push(f);
}

static void run(void) {
    for (;;) {
        Fiber *f;
        while ((f = ready_pop())) { f->st = F_READY; fib_step(f); }
        /* all parked: reap ONE completion (cqe or eventfd). Liveness is the
         * COUNTER, not a table scan — the scan was O(fibers) per reap iteration:
         * 1.06M fibers x ~2M iterations = 1.4 TRILLION cycles, 64% of the whole
         * unpack (perf-witnessed), the same O(n*events) disease as the join scan. */
        if (g_nlive_fibers == 0 && g_stream_eof) return;
        struct io_uring_cqe *cqe;
        int rc = io_uring_wait_cqe(&g_ring, &cqe);
        if (rc < 0) { fprintf(stderr, "qvm2: wait_cqe: %s\n", strerror(-rc)); return; }
        void *ud = io_uring_cqe_get_data(cqe);
        int res = cqe->res;
        io_uring_cqe_seen(&g_ring, cqe);
        if (ud == (void *)&g_stdin_tag) {                   /* streamed program bytes */
            TR("stdin res=%d\n", res);
            if (res > 0) {
                if (g_pblen + (size_t)res > g_pbcap) {
                    g_pbcap = (g_pblen + res) * 2 + (1 << 20);
                    g_pb = realloc(g_pb, g_pbcap);
                }
                memcpy(g_pb + g_pblen, g_stage, (size_t)res); g_pblen += res;
                stream_parse();
                stdin_arm();
            } else g_stream_eof = 1;                        /* pipe closed */
            { Fiber *r0 = g_nfib ? g_fib[0] : NULL;
              if (r0 && r0->st == F_WAIT_STREAM) ready_push(r0); }
            continue;
        }
        if (ud == (void *)&g_evfd) {                        /* pool completions */
            /* the ARMED ring read already consumed the eventfd counter into evbuf —
             * a second manual read() here blocked the scheduler forever whenever no
             * other worker completion refilled the counter in the window (latent
             * since M0; multi-fiber tests passed on scheduling luck, the single-fiber
             * case hung deterministically). Drain the done list; that is the truth. */
            pthread_mutex_lock(&g_dmu); Job *d = g_done_h; g_done_h = NULL; pthread_mutex_unlock(&g_dmu);
            while (d) { Job *nx = d->next;
                if (d->kind == 3) {                          /* statb records -> cursor */
                    d->f->cur.qc = d->qc; memset(&d->qc, 0, sizeof d->qc);
                    d->f->cur.didx = d->f->cur.dlen = 0;         /* straight to final flush */
                }
                if (d->kind == 2) {                          /* readdir result -> cursor */
                    Fiber *df = d->f;
                    df->cur.dents = d->rbuf; df->cur.dlen = d->rn > 0 ? d->rn : 0;
                    df->cur.didx = 0; df->cur.fd = d->rfd; df->cur.rec_len = 0;
                    if (!df->iob) df->iob = malloc(CHUNK);
                }
                ready_push(d->f); if (d->v) val_wake(d->v); free(d); d = nx; }
            struct io_uring_sqe *s = sqe_get();   /* re-arm */
            static uint64_t evbuf;
            io_uring_prep_read(s, g_evfd, &evbuf, 8, 0);
            io_uring_sqe_set_data(s, &g_evfd);
            io_uring_submit(&g_ring);
        } else {
            Fiber *f = ud;
            if (f->st == F_WAIT_CQE) {
                Instr *I = &f->prog[f->pc];
                if (I->op == I_MOV && f->cur.fd == -1) {         /* async open landed */
                    g_open_inflight--;
                    if (g_budget_waiters) budget_release(0); /* a slot just freed: wake
                                                              * parked openers unconditionally.
                                                              * The old `< GATE-128` hysteresis
                                                              * lost the wakeup when the last
                                                              * opens drained before a fiber
                                                              * parked (and was negative for
                                                              * GATE<128 -> never woke). */
                    if (res == -ENOENT && I->dkind == E_FS) {    /* rare: parents missing */
                        char *dp = (char *)I->path;
                        mkparents(dp);
                        res = open(dp, O_WRONLY | O_CREAT, 0644);
                        if (res < 0) res = -errno;
                    }
                    if (res < 0) {
                        fprintf(stderr, "qvm2: open %s: %s\n", I->path, strerror(-res));
                        fib_retire(f); ready_push(f);
                    } else {
                        f->cur.fd = res;
                        if (I->skind == E_FS) {                  /* src: size the transfer */
                            struct stat st; fstat(f->cur.fd, &st);
                            f->cur.off = I->soff;
                            f->cur.remain = I->slen >= 0 ? I->slen : st.st_size - I->soff;
                        } else {
                            f->cur.dst_off = 0;                  /* dst: begin at 0 */
                        }
                        ready_push(f);
                    }
                }
                else if (I->op == I_MOV && I->skind == E_FS) fib_resume_read(f, res);
                else ready_push(f);                          /* write done / etc */
            }
        }
    }
}

/* -------------------------------------------------- program loader (Arrow) */
/* One row = one instruction. Narrow schema, 10 cols -> 22 buffers:
 *   tid u32 | op u8 | k1 u8 | k2 u8 | a b c d i64 | path large_utf8 | payload large_binary
 * Operand mapping:
 *   NEWVAL  a=vid b=codec c=level
 *   MOV     k1=skind k2=dkind; FS src: path,a=soff,b=slen; VAL src: a=vid;
 *           INLINE src: payload; VAL dst: d=vid; SINK dst: d=sink
 *   CLOSE   a=vid       SPAWN/JOIN a=lo b=hi       SINK a=sink, path
 * The blob stays mapped for the run: path/payload pointers alias it. */
static char *prog_strdup_range(const char *d, int64_t a, int64_t b) {
    char *s2 = malloc(b - a + 1); memcpy(s2, d + a, b - a); s2[b - a] = 0; return s2;
}
static Instr *fib_append(Fiber *f) {
    if (f->n == f->pcap) { f->pcap = f->pcap ? f->pcap * 2 : 16;
        f->prog = realloc(f->prog, f->pcap * sizeof(Instr)); }
    Instr *I = &f->prog[f->n++]; memset(I, 0, sizeof *I); return I;
}
/* Decode one record batch of instruction rows, appending to fibers. Paths AND
 * inline payloads are COPIED — the streaming parse buffer reallocs under us. */
static void feed_rows(const uint8_t **bp, int64_t n) {
    const uint32_t *tid = (const uint32_t *)bp[1];
    const uint8_t *op = bp[3], *k1 = bp[5], *k2 = bp[7];
    const int64_t *a = (const int64_t *)bp[9],  *b = (const int64_t *)bp[11];
    const int64_t *c = (const int64_t *)bp[13], *d = (const int64_t *)bp[15];
    const int64_t *po = (const int64_t *)bp[17]; const char *pd = (const char *)bp[18];
    const int64_t *yo = (const int64_t *)bp[20]; const uint8_t *yd = bp[21];
    for (int64_t k = 0; k < n; k++) {
        Fiber *f = fib_get(tid[k]);
        Instr *I = fib_append(f);
        I->op = op[k];
        switch (op[k]) {
        case I_NEWVAL: I->cvid = a[k]; I->codec = (int)b[k]; I->clevel = (int)c[k];
            /* d: 1 = STREAM val; >1 = pledged raw size (content-size stamp) */
            I->stream_flag = d[k] == 1; I->fsize = d[k] > 1 ? d[k] : 0; break;
        case I_EMIT: I->sink = (int)a[k]; break;
        case I_MKDIR: case I_UNLINK: case I_RMDIR: case I_FENCE:
            if (po[k + 1] > po[k]) I->path = prog_strdup_range(pd, po[k], po[k + 1]);
            break;
        case I_SYMLINK: case I_LINK:
            I->path = prog_strdup_range(pd, po[k], po[k + 1]);
            I->target = prog_strdup_range((const char *)yd, yo[k], yo[k + 1]);
            I->mode = a[k]; I->mtime = b[k]; I->uid = c[k]; I->gid = d[k];
            break;
        case I_READDIR:
            I->path = prog_strdup_range(pd, po[k], po[k + 1]);
            I->sink = (int)a[k];
            break;
        case I_SCAN:
            I->path = prog_strdup_range(pd, po[k], po[k + 1]);
            I->sink = (int)a[k]; I->mode = b[k];   /* b = walker count */
            break;
        case I_STATB: {
            I->path = prog_strdup_range(pd, po[k], po[k + 1]);
            I->sink = (int)a[k];
            int64_t ln = yo[k + 1] - yo[k];
            uint8_t *cp = malloc(ln ? ln : 1); memcpy(cp, yd + yo[k], ln);
            I->inl = cp; I->inlen = ln;
            break;
        }
        case I_SETMETA:
            I->path = prog_strdup_range(pd, po[k], po[k + 1]);
            I->nofollow = k1[k] & 1;
            I->mode = a[k]; I->mtime = b[k]; I->uid = c[k]; I->gid = d[k];
            break;
case I_CLOSE: case I_FREE: I->cvid = a[k]; break;
        case I_SPAWN: case I_JOIN: I->lo = (int)a[k]; I->hi = (int)b[k]; break;
        case I_SINK:
            I->sink = (int)a[k]; I->mode = b[k];   /* b==1: arrow sink (schema at open) */
            I->path = prog_strdup_range(pd, po[k], po[k + 1]); break;
        case I_MOV:
            I->digest = (k1[k] >> 7) & 1;
            I->skind = k1[k] & 0x7f; I->dkind = k2[k];
            I->fsize = -1;
            if (I->skind == E_FS) { I->path = prog_strdup_range(pd, po[k], po[k + 1]);
                I->soff = a[k]; I->slen = b[k]; }
            if (I->skind == E_VAL)    { I->svid = a[k]; I->soff = b[k]; I->slen = c[k]; }
            if (I->skind == E_INLINE) {
                int64_t ln = yo[k + 1] - yo[k];
                uint8_t *cp = malloc(ln ? ln : 1); memcpy(cp, yd + yo[k], ln);
                I->inl = cp; I->inlen = ln;
            }
            if (I->dkind == E_VAL)    I->dvid = d[k];
            if (I->dkind == E_SINK)   I->sink = (int)d[k];
            if (I->dkind == E_FS)     { I->path = prog_strdup_range(pd, po[k], po[k + 1]);
                I->fsize = d[k]; }
            break;
        }
    }
}
static int load_program(const uint8_t *blob) {
    const uint8_t *bp[22]; const uint8_t *cur = blob; int64_t n;
    while ((n = arrow_next(&cur, bp, 22)) > 0) feed_rows(bp, n);
    if (n < 0) { fprintf(stderr, "qvm2: bad program stream\n"); return -1; }
    return 0;
}

/* ---------------------------------------------- streaming wire (M3) */
/* Instruction batches arrive on stdin DURING execution: the planner reschedules
 * deeper scan fibers wave by wave (cost: one pipelined round-trip per DEPTH).
 * tid 0 parks on the open stream when it runs out of program; arriving batches
 * append and wake it; stdin EOF (or the Arrow EOS marker) is the final gate. */
static void stream_parse(void) {
    for (;;) {
        size_t have = g_pblen - g_ppos;
        if (have < 8) return;
        const uint8_t *q = g_pb + g_ppos;
        uint32_t mlen; memcpy(&mlen, q + 4, 4);
        if (mlen == 0) { g_ppos += 8; continue; }   /* Arrow EOS: every _ipc_bytes blob
                                                     * carries one — NOT end of stream.
                                                     * The pipe closing is the real EOF. */
        if (have < 8 + mlen) return;
        const uint8_t *meta = q + 8;
        int64_t rt = fb_root(meta);
        int64_t blp = fb_field(meta, rt, 3);
        int64_t blen = blp >= 0 ? fb_i64(meta, blp) : 0;
        if (have < 8 + mlen + (size_t)blen) return;
        int64_t htp = fb_field(meta, rt, 1);
        if (htp >= 0 && meta[htp] == 3) {
            const uint8_t *body = meta + mlen;
            int64_t rb = fb_offset_field(meta, rt, 2);
            int64_t nn = fb_i64(meta, fb_field(meta, rb, 0));
            int64_t bufs = fb_offset_field(meta, rb, 2);
            if ((int)fb_u32(meta, bufs) == 22) {
                const uint8_t *bp[22];
                for (int i = 0; i < 22; i++) bp[i] = body + fb_i64(meta, bufs + 4 + 16 * i);
                feed_rows(bp, nn);
            }
        }
        g_ppos += 8 + mlen + (size_t)blen;
    }
}
static void stdin_arm(void) {
    struct io_uring_sqe *sq = sqe_get();
    io_uring_prep_read(sq, 0, g_stage, sizeof g_stage, (uint64_t)-1);
    io_uring_sqe_set_data(sq, &g_stdin_tag);
    io_uring_submit(&g_ring);
}

static int run_program(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp) { fprintf(stderr, "qvm2: %s: %m\n", path); return 2; }
    fseek(fp, 0, SEEK_END); long len = ftell(fp); fseek(fp, 0, SEEK_SET);
    uint8_t *blob = malloc(len);
    if (fread(blob, 1, len, fp) != (size_t)len) { fclose(fp); return 2; }
    fclose(fp);
    if (load_program(blob)) return 2;
    g_nlive_fibers = 1; ready_push(fib_get(0));   /* only thread 0 starts */
    run();
    for (int i = 0; i < 8; i++) if (g_sinks[i].fd > 0) close(g_sinks[i].fd);
    return 0;
}

/* ------------------------------------------------------------------ M0 self-test */
/* pack: for each input file — spawn a fiber:
 *     z = val CODEC(zstd)          (frame)
 *     mov inline:hdr -> z          (fake 64-byte header, stands in for tar)
 *     mov fs:path    -> z
 *     close z
 *     mov z -> sink:0              (reserve + write)
 * then decompress each frame and byte-compare header+body. */
static Fiber *fib_at(int tid, int cap) {
    Fiber *f = fib_get(tid);
    f->prog = calloc(cap, sizeof(Instr)); f->n = 0; f->pc = 0;
    return f;
}
static Instr *fib_push(Fiber *f, uint8_t op) { Instr *I = &f->prog[f->n++]; memset(I, 0, sizeof *I); I->op = op; return I; }

int main(int argc, char **argv) {
    g_trace = getenv("QVM2_TRACE") != NULL;
    { const char *og = getenv("QVM2_OPEN_GATE");
      if (og && atoi(og) > 0) OPEN_GATE = atoi(og); }
    { const char *bg = getenv("QVM2_BUDGET");
      if (bg) { long v = atol(bg); if (v > 0) g_budget = v << 20; } }
    { const char *w = getenv("QVM2_WORKERS");
      if (w) { NWORK = atoi(w); if (NWORK < 1) NWORK = 1; if (NWORK > NWORK_MAX) NWORK = NWORK_MAX; } }
    if (argc >= 2 && !strcmp(argv[1], "stream")) {
        io_uring_queue_init(QD, &g_ring, 0);
        { unsigned cap = 128;                    /* io-wq caps, BOTH pools (openat draws
                                                  * from the bounded one — capping only
                                                  * slot 1 let it spawn 800+ workers) */
          const char *iw = getenv("QVM2_IOWQ");
          if (iw && atoi(iw) > 0) cap = (unsigned)atoi(iw);
          unsigned v[2] = {cap, cap};
          io_uring_register_iowq_max_workers(&g_ring, v); }
        g_evfd = eventfd(0, 0);
        { struct io_uring_sqe *sq = sqe_get();
          static uint64_t evbuf2; io_uring_prep_read(sq, g_evfd, &evbuf2, 8, 0);
          io_uring_sqe_set_data(sq, &g_evfd); io_uring_submit(&g_ring); }
        pthread_t wk3[NWORK_MAX];
        for (int i = 0; i < NWORK; i++) pthread_create(&wk3[i], 0, worker, 0);
        g_stream_eof = 0;
        stdin_arm();
        g_nlive_fibers = 1; ready_push(fib_get(0));
        run();
        for (int i = 0; i < 8; i++) if (g_sinks[i].fd > 0) close(g_sinks[i].fd);
        g_pool_stop = 1; pthread_cond_broadcast(&g_jcv);
        for (int i = 0; i < NWORK; i++) pthread_join(wk3[i], 0);
        return 0;
    }
    if (argc >= 3 && !strcmp(argv[1], "run")) {
        io_uring_queue_init(QD, &g_ring, 0);
        g_evfd = eventfd(0, 0);
        { struct io_uring_sqe *sq = sqe_get();
          static uint64_t evbuf; io_uring_prep_read(sq, g_evfd, &evbuf, 8, 0);
          io_uring_sqe_set_data(sq, &g_evfd); io_uring_submit(&g_ring); }
        pthread_t wk2[NWORK_MAX];
        for (int i = 0; i < NWORK; i++) pthread_create(&wk2[i], 0, worker, 0);
        int rc = run_program(argv[2]);
        g_pool_stop = 1; pthread_cond_broadcast(&g_jcv);
        for (int i = 0; i < NWORK; i++) pthread_join(wk2[i], 0);
        return rc;
    }
    if (argc < 3) { fprintf(stderr, "usage: qvm2 run <prog.arrow> | qvm2 <out> <file>...\n"); return 2; }
    int nfiles = argc - 2;
    io_uring_queue_init(QD, &g_ring, 0);
    g_evfd = eventfd(0, 0);
    { struct io_uring_sqe *s = sqe_get();   /* arm eventfd read */
      static uint64_t evbuf; io_uring_prep_read(s, g_evfd, &evbuf, 8, 0);
      io_uring_sqe_set_data(s, &g_evfd); io_uring_submit(&g_ring); }
    pthread_t wk[NWORK_MAX];
    for (int i = 0; i < NWORK; i++) pthread_create(&wk[i], 0, worker, 0);

    g_sinks[0].fd = open(argv[1], O_WRONLY | O_CREAT | O_TRUNC, 0644);

    uint8_t (*hdrs)[64] = calloc(nfiles, 64);
    for (int i = 0; i < nfiles; i++) {
        snprintf((char *)hdrs[i], 64, "HDR:%s", argv[2 + i]);
        val_bind(i, val_new(1, 3, 0));
        Fiber *f = fib_at(1 + i, 8);
        Instr *I;
        I = fib_push(f, I_MOV); I->skind = E_INLINE; I->dkind = E_VAL; I->inl = hdrs[i]; I->inlen = 64; I->dvid = i;
        I = fib_push(f, I_MOV); I->skind = E_FS; I->dkind = E_VAL; I->path = argv[2 + i]; I->slen = -1; I->dvid = i; I->fsize = -1;
        I = fib_push(f, I_CLOSE); I->cvid = i;
        I = fib_push(f, I_MOV); I->skind = E_VAL; I->dkind = E_SINK; I->svid = i; I->sink = 0; I->slen = -1;
    }
    Fiber *root = fib_at(0, 4);
    Instr *I = fib_push(root, I_SPAWN); I->lo = 1; I->hi = nfiles;
    I = fib_push(root, I_JOIN); I->lo = 1; I->hi = nfiles;
    g_nlive_fibers = 1; ready_push(root);
    run();
    close(g_sinks[0].fd);

    /* ---- verify: decompress each frame, compare header + body bytes ---- */
    int bad = 0;
    /* frames land in DRAIN order, not fiber order — each fiber recorded its
     * reserved base; read every frame from where it actually landed */
    for (int i = 0; i < nfiles; i++) {
        Val *v = val_at(i);
        int64_t base = g_fib[1 + i]->dbg_base;
        uint8_t *comp = malloc(v->len); int64_t p = 0;
        FILE *fp = fopen(argv[1], "rb"); fseek(fp, base, SEEK_SET);
        (void)!fread(comp, 1, (size_t)v->len, fp); fclose(fp);
        size_t dcap = 64 + (16 << 20);
        uint8_t *dec = malloc(dcap);
        size_t dn = ZSTD_decompress(dec, dcap, comp, (size_t)v->len);
        if (ZSTD_isError(dn)) { printf("frame %d: decompress error\n", i); bad++; goto next; }
        if (memcmp(dec, hdrs[i], 64)) { printf("frame %d: header mismatch\n", i); bad++; goto next; }
        {
            FILE *sf = fopen(argv[2 + i], "rb");
            fseek(sf, 0, SEEK_END); long fsz = ftell(sf); fseek(sf, 0, SEEK_SET);
            uint8_t *body = malloc(fsz); (void)!fread(body, 1, fsz, sf); fclose(sf);
            if ((int64_t)dn != 64 + fsz || memcmp(dec + 64, body, fsz))
                { printf("frame %d: body mismatch (%zu vs %ld+64)\n", i, dn, fsz); bad++; }
            free(body);
        }
    next:
        free(dec); free(comp); (void)p;
    }
    printf(bad ? "M0 FAIL (%d bad frames)\n" : "M0 PASS (%d frames round-tripped)\n",
           bad ? bad : nfiles);
    g_pool_stop = 1; pthread_cond_broadcast(&g_jcv);
    for (int i = 0; i < NWORK; i++) pthread_join(wk[i], 0);
    return bad != 0;
}
