/* qvm.c — quiver ISA v2 executor (docs/ISA2.md), first slice.
 *
 * A cooperative FIBER scheduler over a generic async-task worker pool — the
 * unified completion model: IO ops (and later codecs) are submitted as Tasks,
 * a worker runs them, and posts a Completion the scheduler reaps. One worker
 * pool serves everything; io_uring is a later swap-in for the IO backend.
 *
 * Threads are numbered; only thread 0 runs at launch. `spawn lo,hi` activates a
 * contiguous range; `join lo,hi` waits for it. Same tid = sequential; different
 * tids = parallel, bounded by the buffer pool (alloc blocks when full).
 *
 * Ops: alloc/free, mov (inline/fs/buf/arch, fs->fs = copy_file_range), mkdir,
 * setmeta, spawn, join, and the codecs inflate/deflate. deflate appends to a
 * Sink (an fd + cursor guarded by a mutex; the reservation is the only critical
 * section) and reports {frame_id, coff, clen} for the footer. Enough for cp,
 * uncompressed pack, and the compressed pack/unpack byte paths; the planner
 * modes (plan_pack/plan_unpack) wire them end-to-end next.
 *
 * Build/test (libzstd required):
 *   cc -O2 -pthread -DQVM_TEST -I<zstd>/include -o /tmp/qvm quiver/exec/qvm.c \
 *      <zstd>/lib/libzstd.a && /tmp/qvm
 */
#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <zstd.h>
#include "qvm_comp.h"           /* Arrow-emit template for the completion schema */
#include "qvm_scan.h"           /* Arrow-emit template for the fs-scan schema */
#include "qvm_etag.h"           /* Arrow-emit template for the S3-etag schema */
#include "md5.h"                /* vendored MD5 (shared with quiver-exec.c) */
#ifdef QVM_URING
#include <liburing.h>           /* optional io_uring backend for per-file read/write */
#endif

/* ----------------------------------------------------------------- tracing  */
/* When QVM_TRACE=<path> is set, every op is logged with monotonic start/end so
 * the visualizer can draw the fiber timeline + buffer-pool occupancy. Events
 * are appended only from the scheduler thread (worker times are carried back on
 * the Task), so no locking. Dump: [i64 span][u32 n][n × 9×i64]. */
static int64_t tr_now(void){
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}
/* off/aux carry op parameters for the viz: mov → (offset, endpoint-kind),
 * deflate → (frame_id, input_len), inflate → (arch_off, frame_id). */
typedef struct { int64_t t0, t1, tid, op, buf, detail, epoch, off, aux; } TraceEv;

/* ------------------------------------------------------------------ opcodes */
enum {
    OP_ALLOC = 1, OP_FREE, OP_MOV, OP_MKDIR, OP_SETMETA,
    OP_SPAWN, OP_JOIN, OP_INFLATE, OP_DEFLATE, OP_CALL,
    OP_UNLINK, OP_RMDIR, OP_FBARRIER,     /* teardown + durability (mirror/sync) */
};
/* endpoint kinds for mov src/dst */
enum { E_NONE = 0, E_FS, E_BUF, E_INLINE, E_ARCH };

/* one decoded instruction (wire encoding comes later — see ISA2 §3) */
typedef struct {
    uint32_t tid;
    uint8_t  op;
    uint8_t  src, dst;            /* mov endpoint kinds */
    int32_t  buf_id;             /* buffer operand (alloc/free/mov) */
    int64_t  buf_off, len;       /* buffer offset + byte count */
    int64_t  cap;                /* alloc capacity; also spawn/join hi */
    int64_t  lo;                 /* spawn/join lo */
    int64_t  arch_off;           /* offset into the output archive fd */
    const char *path, *dpath;    /* fs path(s): src / cp-dst */
    const uint8_t *payload; int64_t payload_len;   /* inline bytes */
    int32_t  mode; int64_t mtime_ns;
    int32_t  sink; int32_t level; int64_t frame_id;  /* deflate: sink/level/tag */
} Instr;

/* an append sink: fd + cursor guarded by a mutex. A seekable file releases the
 * lock after reserving (writes concurrent via pwrite); a pipe/socket has no
 * positioned write, so the lock is held THROUGH the sequential write. */
typedef struct { int fd; int64_t cursor; int is_pipe; pthread_mutex_t mu; } Sink;

/* --------------------------------------------------------------- task queue */
/* A Task is a unit of async work handed to the worker pool; buffer operands are
 * resolved to raw pointers at submit time so workers never touch scheduler
 * state. The worker fills res (0 or -errno) and posts the tid back. */
/* xxHash64 (Yann Collet, BSD) — frame content digest for integrity. Streaming
 * (reset/update/digest) so a multi-run gather hashes its runs in place; the
 * one-shot result over the concatenation equals inflate's one-shot over the
 * decoded output, so pack and unpack digests match. */
#define XXH_P1 0x9E3779B185EBCA87ULL
#define XXH_P2 0xC2B2AE3D27D4EB4FULL
#define XXH_P3 0x165667B19E3779F9ULL
#define XXH_P4 0x85EBCA77C2B2AE63ULL
#define XXH_P5 0x27D4EB2F165667C5ULL
static inline uint64_t xxr(uint64_t x, int r){ return (x<<r)|(x>>(64-r)); }
static inline uint64_t xxround(uint64_t a, uint64_t in){ a += in*XXH_P2; a = xxr(a,31); return a*XXH_P1; }
static inline uint64_t xxmerge(uint64_t a, uint64_t v){ v = xxround(0,v); a ^= v; return a*XXH_P1 + XXH_P4; }
typedef struct { uint64_t v[4], total; uint8_t mem[32]; int msz; uint64_t seed; } XXH;
static void xxh_reset(XXH *s){ s->v[0]=XXH_P1+XXH_P2; s->v[1]=XXH_P2; s->v[2]=0;
    s->v[3]=0-XXH_P1; s->total=0; s->msz=0; s->seed=0; }
static void xxh_update(XXH *s, const uint8_t *p, size_t len){
    const uint8_t *end = p+len; s->total += len;
    if (s->msz + len < 32){ memcpy(s->mem+s->msz, p, len); s->msz += (int)len; return; }
    if (s->msz){ memcpy(s->mem+s->msz, p, 32-s->msz); uint64_t k;
        memcpy(&k,s->mem,8);    s->v[0]=xxround(s->v[0],k);
        memcpy(&k,s->mem+8,8);  s->v[1]=xxround(s->v[1],k);
        memcpy(&k,s->mem+16,8); s->v[2]=xxround(s->v[2],k);
        memcpy(&k,s->mem+24,8); s->v[3]=xxround(s->v[3],k);
        p += 32-s->msz; s->msz=0; }
    if (p+32<=end){ const uint8_t *lim=end-32;
        do { uint64_t k;
            memcpy(&k,p,8);  s->v[0]=xxround(s->v[0],k); p+=8;
            memcpy(&k,p,8);  s->v[1]=xxround(s->v[1],k); p+=8;
            memcpy(&k,p,8);  s->v[2]=xxround(s->v[2],k); p+=8;
            memcpy(&k,p,8);  s->v[3]=xxround(s->v[3],k); p+=8;
        } while (p<=lim); }
    if (p<end){ memcpy(s->mem, p, (size_t)(end-p)); s->msz=(int)(end-p); }
}
static uint64_t xxh_digest(XXH *s){
    const uint8_t *p=s->mem, *end=s->mem+s->msz; uint64_t h;
    if (s->total>=32){ h=xxr(s->v[0],1)+xxr(s->v[1],7)+xxr(s->v[2],12)+xxr(s->v[3],18);
        h=xxmerge(h,s->v[0]); h=xxmerge(h,s->v[1]); h=xxmerge(h,s->v[2]); h=xxmerge(h,s->v[3]);
    } else h = XXH_P5;
    h += s->total;
    while (p+8<=end){ uint64_t k; memcpy(&k,p,8); h ^= xxround(0,k); h=xxr(h,27)*XXH_P1+XXH_P4; p+=8; }
    if (p+4<=end){ uint32_t k; memcpy(&k,p,4); h ^= (uint64_t)k*XXH_P1; h=xxr(h,23)*XXH_P2+XXH_P3; p+=4; }
    while (p<end){ h ^= (*p)*XXH_P5; h=xxr(h,11)*XXH_P1; p++; }
    h^=h>>33; h*=XXH_P2; h^=h>>29; h*=XXH_P3; h^=h>>32; return h;
}

typedef struct Task {
    uint32_t tid;
    uint8_t  kind;               /* mirrors the mov case / namespace op */
    int      arch_fd;
    uint8_t *buf; int64_t buf_off, arch_off, len;
    const char *path, *dpath;
    const uint8_t *payload; int64_t payload_len;
    int32_t mode; int64_t mtime_ns;
    Sink    *sink; int level; int64_t frame_id, coff, clen;  /* codec/sink */
    int      digest_on; int64_t digest;   /* xxh64 of the frame's decoded content */
    int      res;
    int      op; int64_t buf_log, detail; int64_t wt0, wt1;   /* trace */
    int      epoch;              /* owning batch */
} Task;
enum { TK_INLINE_TO_BUF_UNUSED, TK_FS_TO_BUF, TK_BUF_TO_FS, TK_BUF_TO_ARCH,
       TK_INLINE_TO_ARCH, TK_CFR_FS_TO_FS, TK_CFR_FS_TO_ARCH,
       TK_MKDIR, TK_SETMETA, TK_INFLATE, TK_DEFLATE,
       TK_UNLINK, TK_RMDIR, TK_FBARRIER,   /* teardown + durability */
       TK_BATCH_READY };   /* reader thread → scheduler: a CALL response arrived */

#define QCAP 4096
typedef struct {
    Task  *q[QCAP]; int head, tail;
    pthread_mutex_t mu; pthread_cond_t cv;
    int stop;
} TQ;
static void tq_init(TQ *q){ q->head=q->tail=q->stop=0;
    pthread_mutex_init(&q->mu,0); pthread_cond_init(&q->cv,0); }
static void tq_push(TQ *q, Task *t){
    pthread_mutex_lock(&q->mu);
    q->q[q->tail++ % QCAP] = t; pthread_cond_signal(&q->cv);
    pthread_mutex_unlock(&q->mu);
}
static Task *tq_pop(TQ *q){          /* blocks; NULL when stopped and drained */
    pthread_mutex_lock(&q->mu);
    while (q->head==q->tail && !q->stop) pthread_cond_wait(&q->cv,&q->mu);
    Task *t = NULL;
    if (q->head!=q->tail) t = q->q[q->head++ % QCAP];
    pthread_mutex_unlock(&q->mu);
    return t;
}
static Task *tq_trypop(TQ *q){       /* non-blocking; NULL if empty */
    pthread_mutex_lock(&q->mu);
    Task *t = (q->head!=q->tail) ? q->q[q->head++ % QCAP] : NULL;
    pthread_mutex_unlock(&q->mu);
    return t;
}

/* ----------------------------------------------------------------- workers  */
static ssize_t do_cfr(int in, int64_t *ioff, int out, int64_t *ooff, int64_t n){
    /* copy_file_range with a read/write fallback (EXDEV/ENOSYS). */
    int64_t done = 0;
    while (done < n) {
        ssize_t r = copy_file_range(in, ioff, out, ooff, (size_t)(n-done), 0);
        if (r < 0) {
            if (errno==EXDEV || errno==ENOSYS || errno==EINVAL) break;
            return -1;
        }
        if (r == 0) break;
        done += r;
    }
    if (done == n) return done;
    /* fallback ring copy at the current offsets */
    uint8_t b[1<<20];
    while (done < n) {
        ssize_t want = (n-done) > (1<<20) ? (1<<20) : (n-done);
        ssize_t r = pread(in, b, (size_t)want, *ioff);
        if (r <= 0) return r<0 ? -1 : done;
        if (pwrite(out, b, (size_t)r, *ooff) != r) return -1;
        *ioff += r; *ooff += r; done += r;
    }
    return done;
}

static void run_task(Task *t){
    t->res = 0;
    switch (t->kind) {
    case TK_FS_TO_BUF: {
        int fd = open(t->path, O_RDONLY);
        if (fd < 0) { t->res = -errno; return; }
        int64_t got = 0;                         /* arch_off = source file offset */
        while (got < t->len) {
            ssize_t r = pread(fd, t->buf + t->buf_off + got,
                              (size_t)(t->len-got), t->arch_off + got);
            if (r < 0) { t->res = -errno; break; }
            if (r == 0) break;
            got += r;
        }
        close(fd); break;
    }
    case TK_BUF_TO_FS: {
        int fd = open(t->path, O_WRONLY|O_CREAT|O_TRUNC,
                      t->mode>=0 ? (mode_t)t->mode : 0644);
        if (fd < 0) { t->res = -errno; return; }
        if (pwrite(fd, t->buf + t->buf_off, (size_t)t->len, 0) != t->len)
            t->res = -errno;
        close(fd); break;
    }
    case TK_BUF_TO_ARCH:
        if (pwrite(t->arch_fd, t->buf + t->buf_off, (size_t)t->len,
                   t->arch_off) != t->len) t->res = -errno;
        break;
    case TK_INLINE_TO_ARCH:
        if (pwrite(t->arch_fd, t->payload, (size_t)t->payload_len,
                   t->arch_off) != t->payload_len) t->res = -errno;
        break;
    case TK_CFR_FS_TO_FS: {
        int in = open(t->path, O_RDONLY);
        if (in < 0) { t->res = -errno; return; }
        int out = open(t->dpath, O_WRONLY|O_CREAT|O_TRUNC,
                       t->mode>=0 ? (mode_t)t->mode : 0644);
        if (out < 0) { t->res = -errno; close(in); return; }
        int64_t io=0, oo=0;
        if (do_cfr(in, &io, out, &oo, t->len) < 0) t->res = -errno;
        close(in); close(out); break;
    }
    case TK_CFR_FS_TO_ARCH: {
        int in = open(t->path, O_RDONLY);
        if (in < 0) { t->res = -errno; return; }
        int64_t io=0, oo=t->arch_off;
        if (do_cfr(in, &io, t->arch_fd, &oo, t->len) < 0) t->res = -errno;
        close(in); break;
    }
    case TK_MKDIR:
        if (mkdir(t->path, t->mode>=0 ? (mode_t)t->mode : 0755) < 0
            && errno != EEXIST) t->res = -errno;
        break;
    case TK_SETMETA: {
        if (t->mode >= 0 && chmod(t->path, (mode_t)t->mode) < 0)
            t->res = -errno;
        if (t->res==0 && t->mtime_ns >= 0) {
            struct timespec ts[2] = {
                {t->mtime_ns/1000000000, t->mtime_ns%1000000000},
                {t->mtime_ns/1000000000, t->mtime_ns%1000000000}};
            if (utimensat(AT_FDCWD, t->path, ts, 0) < 0) t->res = -errno;
        }
        break;
    }
    case TK_UNLINK:                       /* mirror/sync: drop an extraneous file */
        if (unlink(t->path) < 0 && errno != ENOENT) t->res = -errno;
        break;
    case TK_RMDIR:                        /* remove a now-empty dir (deepest-first) */
        if (rmdir(t->path) < 0 && errno != ENOENT) t->res = -errno;
        break;
    case TK_FBARRIER: {                   /* durability: fsync a path, or the archive */
        int fd = t->path && t->path[0] ? open(t->path, O_RDONLY)
                                       : (t->arch_fd >= 0 ? t->arch_fd : -1);
        if (fd < 0) { t->res = -errno; break; }
        if (fsync(fd) < 0) t->res = -errno;
        if (t->path && t->path[0]) close(fd);        /* don't close the archive fd */
        break;
    }
    case TK_DEFLATE: {
        /* compress and append to the sink. payload (if present) is a packed list
         * of (off,len) i64 runs to GATHER from the buffer in place (filter /
         * reshard from a decoded window — no coalescing copy); otherwise the one
         * contiguous run [buf_off,len]. The reservation (cursor bump) is the only
         * critical section; a seekable file writes off-lock. Reports (coff,clen). */
        static __thread ZSTD_CCtx *cc = NULL;
        uint8_t *cb; size_t cl;
        if (t->payload_len > 0) {                /* multi-run gather */
            int nruns = (int)(t->payload_len / 16);
            const int64_t *rn = (const int64_t *)t->payload;
            int64_t total = 0;
            for (int i = 0; i < nruns; i++) total += rn[i*2 + 1];
            size_t bound = ZSTD_compressBound((size_t)total);
            cb = malloc(bound ? bound : 1);
            if (!cb) { t->res = -ENOMEM; break; }
            if (!cc) cc = ZSTD_createCCtx();
            ZSTD_CCtx_reset(cc, ZSTD_reset_session_only);
            ZSTD_CCtx_setParameter(cc, ZSTD_c_compressionLevel, t->level);
            ZSTD_CCtx_setPledgedSrcSize(cc, (unsigned long long)total);
            ZSTD_outBuffer out = {cb, bound, 0}; int err = 0;
            XXH xh; if (t->digest_on) xxh_reset(&xh);
            for (int i = 0; i < nruns && !err; i++) {
                ZSTD_inBuffer in = {t->buf + rn[i*2], (size_t)rn[i*2 + 1], 0};
                if (t->digest_on) xxh_update(&xh, t->buf + rn[i*2], (size_t)rn[i*2+1]);
                while (in.pos < in.size)
                    if (ZSTD_isError(ZSTD_compressStream2(cc,&out,&in,ZSTD_e_continue)))
                        { err = 1; break; }
            }
            if (t->digest_on) t->digest = (int64_t)xxh_digest(&xh);
            size_t rem;
            do { ZSTD_inBuffer e = {NULL,0,0};
                 rem = ZSTD_compressStream2(cc, &out, &e, ZSTD_e_end);
                 if (ZSTD_isError(rem)) { err = 1; break; }
            } while (rem);
            if (err) { t->res = -EIO; free(cb); break; }
            cl = out.pos;
        } else {                                 /* single contiguous run */
            size_t bound = ZSTD_compressBound((size_t)t->len);
            cb = malloc(bound ? bound : 1);
            if (!cb) { t->res = -ENOMEM; break; }
            cl = ZSTD_compress(cb, bound, t->buf + t->buf_off,
                               (size_t)t->len, t->level);
            if (ZSTD_isError(cl)) { t->res = -EIO; free(cb); break; }
            if (t->digest_on) { XXH xh; xxh_reset(&xh);
                xxh_update(&xh, t->buf + t->buf_off, (size_t)t->len);
                t->digest = (int64_t)xxh_digest(&xh); }
        }
        Sink *s = t->sink;
        if (s->is_pipe) {                        /* no pwrite: hold through write */
            pthread_mutex_lock(&s->mu);
            int64_t coff = s->cursor; size_t off = 0;
            while (off < cl) {
                ssize_t w = write(s->fd, cb + off, cl - off);
                if (w <= 0) { t->res = -errno; break; }
                off += (size_t)w;
            }
            s->cursor += (int64_t)cl;
            pthread_mutex_unlock(&s->mu);
            t->coff = coff; t->clen = (int64_t)cl;
        } else {                                 /* seekable: reserve, write off-lock */
            pthread_mutex_lock(&s->mu);
            int64_t coff = s->cursor; s->cursor += (int64_t)cl;
            pthread_mutex_unlock(&s->mu);
            if (pwrite(s->fd, cb, cl, coff) != (ssize_t)cl) t->res = -errno;
            t->coff = coff; t->clen = (int64_t)cl;
        }
        free(cb); break;
    }
    case TK_INFLATE: {
        /* read the compressed frame from arch[arch_off, len] and decompress it
         * into buf[buf_off]; the frame header carries the content size. */
        uint8_t *cb = malloc((size_t)t->len);
        if (!cb) { t->res = -ENOMEM; break; }
        if (pread(t->arch_fd, cb, (size_t)t->len, t->arch_off) != t->len) {
            t->res = -errno; free(cb); break;
        }
        unsigned long long dsz = ZSTD_getFrameContentSize(cb, (size_t)t->len);
        if (dsz == ZSTD_CONTENTSIZE_ERROR || dsz == ZSTD_CONTENTSIZE_UNKNOWN) {
            t->res = -EIO; free(cb); break;
        }
        size_t z = ZSTD_decompress(t->buf + t->buf_off, dsz, cb, (size_t)t->len);
        if (ZSTD_isError(z)) t->res = -EIO;
        else if (t->digest_on) { XXH xh; xxh_reset(&xh);   /* verify: hash decoded */
            xxh_update(&xh, t->buf + t->buf_off, z);
            t->digest = (int64_t)xxh_digest(&xh); }
        free(cb); break;
    }
    }
}

typedef struct { TQ *in; TQ *out; } Worker;
static void *worker_main(void *a){
    Worker *w = a;
    for (;;) {
        Task *t = tq_pop(w->in);
        if (!t) break;
        t->wt0 = tr_now(); run_task(t); t->wt1 = tr_now();
        tq_push(w->out, t);
    }
    return NULL;
}

/* ------------------------------------------------------------ buffer pool   */
/* buf_id == physical slot (the planner draws ids from a ring of pool size).
 * alloc blocks when the slot is still in use — that IS the backpressure. */
typedef struct BufSlot {
    uint8_t *mem; size_t cap; int in_use;
    struct Thread *waiters;      /* threads parked on this slot */
} BufSlot;

/* --------------------------------------------------------------- threads    */
/* Every thread belongs to a BATCH (epoch). All batches' threads live in ONE
 * scheduler and one ready queue, so a prefetch loader in an outer batch and the
 * gather in a CALL-returned batch run fully concurrently. A thread is identified
 * by (epoch, tid). */
typedef struct Thread {
    uint32_t tid; int epoch;
    Instr *prog; int nprog, pc;
    enum { T_INERT, T_READY, T_WAIT_IO, T_WAIT_JOIN, T_WAIT_ALLOC,
           T_WAIT_CALL, T_DONE } st;
    int64_t join_lo, join_hi;    /* range this thread is joining on */
    int      last_res;
    struct Thread *wnext;        /* buffer-waiter list link */
    struct Thread *rnext;        /* ready-queue link */
} Thread;

/* a batch = a thread set + a completion count; when it finishes it wakes the
 * fiber that CALLed it (its waiter) and frees the instruction memory it owns. */
typedef struct { Thread *th; int nth, ndone; int wep, wtid, hasw;
                 void *pm, *ap, *ad, *raw; } Batch;

/* a CALL awaiting its response (FIFO, scheduler-thread only) */
typedef struct PW { int ep, tid; int64_t t0, cid; struct PW *next; } PW;

typedef struct {
    Batch *bat; int nbat, batcap; int cur_epoch;   /* batch registry */
    BufSlot *pool; int npool;
    int arch_fd;
    Sink *sinks; int nsinks;
    TQ tasks, comps;
    int inflight;
    Thread *ready_head, *ready_tail;
    int failed;                  /* first -errno seen */
    int64_t *cf, *cc, *cl, *cd; int ncomp, ccap;  /* completions: frame,coff,clen,digest */
    TraceEv *tr; int ntr, trcap; int64_t t_base;   /* execution trace (opt) */
    int call_fd;                 /* OP_CALL request channel (qvm -> Python) */
    PW *pw_head, *pw_tail; int pw_count;   /* CALLs awaiting responses */
    pthread_t reader_th; int has_reader;   /* off-thread CALL-response reader */
    int wal_fd, wal_n;                     /* WAL: append each committed frame */
#ifdef QVM_URING
    int use_uring, qd; TQ ring_tasks; pthread_t ring_th; struct io_uring ring;
#endif
} Sched;

#ifdef QVM_URING
/* io_uring backend: per-file reads (fs->buf) and writes (buf->fs) become a READ
 * or WRITE SQE against a freshly opened fd, kept deep in the ring so many files
 * are in flight at once. Everything else (copy_file_range — no io_uring op — and
 * the codecs) stays on the worker pool. The ring worker posts completions to the
 * SAME queue the scheduler reaps, so the two backends are indistinguishable. */
typedef struct { Task *t; int fd; } RingOp;
static int ring_submit(Sched *S, Task *k){
    int rd = (k->kind == TK_FS_TO_BUF);
    int fd = open(k->path, rd ? O_RDONLY : (O_WRONLY|O_CREAT|O_TRUNC),
                  k->mode >= 0 ? (mode_t)k->mode : 0644);
    if (fd < 0) { k->res = -errno; tq_push(&S->comps, k); return 0; }
    struct io_uring_sqe *sqe = io_uring_get_sqe(&S->ring);
    RingOp *ro = malloc(sizeof *ro); ro->t = k; ro->fd = fd;
    if (rd) io_uring_prep_read(sqe, fd, k->buf + k->buf_off, (unsigned)k->len,
                              (__u64)k->arch_off);
    else    io_uring_prep_write(sqe, fd, k->buf + k->buf_off, (unsigned)k->len, 0);
    io_uring_sqe_set_data(sqe, ro);
    return 1;
}
static void ring_reap(Sched *S, struct io_uring_cqe *cqe){
    RingOp *ro = io_uring_cqe_get_data(cqe);
    Task *k = ro->t; int res = cqe->res;
    io_uring_cqe_seen(&S->ring, cqe);
    close(ro->fd); free(ro);
    k->res = res < 0 ? res : (res != (int)k->len ? -EIO : 0);
    tq_push(&S->comps, k);
}
static void *ring_worker(void *arg){
    Sched *S = arg; int inflight = 0;
    for (;;) {
        Task *k;
        while (inflight < S->qd && (k = tq_trypop(&S->ring_tasks)))
            inflight += ring_submit(S, k);
        if (inflight) io_uring_submit(&S->ring);
        if (inflight == 0) {                       /* idle: block for work */
            k = tq_pop(&S->ring_tasks);
            if (!k) break;                         /* stopped and drained */
            inflight += ring_submit(S, k);
            if (inflight) io_uring_submit(&S->ring);
            if (inflight == 0) continue;
        }
        struct io_uring_cqe *cqe;
        if (io_uring_wait_cqe(&S->ring, &cqe) == 0) { ring_reap(S, cqe); inflight--; }
        while (io_uring_peek_cqe(&S->ring, &cqe) == 0) { ring_reap(S, cqe); inflight--; }
    }
    return NULL;
}
#endif

static void tr_log(Sched *S, int64_t t0, int64_t t1, uint32_t tid, int op,
                   int64_t buf, int64_t detail, int64_t off, int64_t aux){
    if (!S->t_base) return;                       /* tracing off */
    if (S->ntr == S->trcap) { S->trcap = S->trcap ? S->trcap*2 : 1024;
        S->tr = realloc(S->tr, S->trcap * sizeof(TraceEv)); }
    S->tr[S->ntr++] = (TraceEv){ t0 - S->t_base, t1 - S->t_base,
                                 tid, op, buf, detail, S->cur_epoch, off, aux };
}

static uint8_t *read_framed(int fd, size_t *out);
static Instr *qvm_decode_arrow(uint8_t *data, size_t sz, int *n_out,
                               char **ap, char **ad);
static Thread *TH(Sched *S, int ep, int tid){ return &S->bat[ep].th[tid]; }

static void ready_push(Sched *S, Thread *t){
    t->st = T_READY; t->rnext = NULL;
    if (S->ready_tail) S->ready_tail->rnext = t; else S->ready_head = t;
    S->ready_tail = t;
}
static Thread *ready_pop(Sched *S){
    Thread *t = S->ready_head;
    if (t) { S->ready_head = t->rnext; if (!S->ready_head) S->ready_tail = NULL; }
    return t;
}

/* pending-CALL FIFO (scheduler thread only): a CALL enqueues its waiter; the
 * matching response (read in order by the reader thread) dequeues it. */
static void pw_push(Sched *S, int ep, int tid, int64_t t0, int64_t cid){
    PW *w = malloc(sizeof *w);
    w->ep = ep; w->tid = tid; w->t0 = t0; w->cid = cid; w->next = NULL;
    if (S->pw_tail) S->pw_tail->next = w; else S->pw_head = w;
    S->pw_tail = w; S->pw_count++;
}
static PW *pw_pop(Sched *S){
    PW *w = S->pw_head;
    if (w) { S->pw_head = w->next; if (!S->pw_head) S->pw_tail = NULL; S->pw_count--; }
    return w;
}

static int pool_alloc(Sched *S, Thread *t, int id, int64_t cap, int zero){
    BufSlot *b = &S->pool[id];
    if (b->in_use) {                     /* backpressure: park until freed */
        t->wnext = b->waiters; b->waiters = t; t->st = T_WAIT_ALLOC;
        return 0;
    }
    if (b->cap < (size_t)cap) { b->mem = realloc(b->mem, cap); b->cap = cap; }
    /* Zeroing is OPT-IN (pack needs clean tar padding). It is skipped by default
     * because it runs INLINE on the scheduler thread and, on a fresh slot, faults
     * in `cap` bytes — serializing allocs and stalling scheduling. When the buffer
     * is fully overwritten anyway (window load, inflate, gather) the first-touch
     * fault is deferred to the worker that writes it, in parallel and off-thread. */
    if (zero && cap > 0) memset(b->mem, 0, (size_t)cap);
    b->in_use = 1;
    return 1;
}
static void pool_free(Sched *S, int id){
    BufSlot *b = &S->pool[id];
    b->in_use = 0;
    Thread *w = b->waiters; b->waiters = NULL;   /* wake all; first to re-run wins */
    while (w) { Thread *n = w->wnext; ready_push(S, w); w = n; }
}

/* join range is within the thread's own batch (epoch ep) */
static int range_done(Sched *S, int ep, int64_t lo, int64_t hi){
    for (int64_t i = lo; i <= hi; i++) if (S->bat[ep].th[i].st != T_DONE) return 0;
    return 1;
}
static void thread_done(Sched *S, Thread *t){
    t->st = T_DONE;
    int ep = t->epoch;
    Batch *b = &S->bat[ep];
    for (int i = 0; i < b->nth; i++) {           /* wake same-batch join-waiters */
        Thread *w = &b->th[i];
        if (w->st == T_WAIT_JOIN && range_done(S, ep, w->join_lo, w->join_hi))
            { w->pc++; ready_push(S, w); }
    }
    if (++b->ndone == b->nth) {                  /* batch complete → wake its CALLer */
        if (b->hasw) { Thread *w = TH(S, b->wep, b->wtid); w->pc++; ready_push(S, w); }
        free(b->th); b->th = NULL;               /* batch retired: free its memory */
        free(b->pm); free(b->ap); free(b->ad); free(b->raw);
        b->pm = b->ap = b->ad = b->raw = NULL;
    }
}

/* resolve a mov into a Task and submit it (async), or do it inline (sync). */
static void submit_mov(Sched *S, Thread *t, Instr *I){
    /* INLINE -> BUF is pure memory: do it synchronously, no task. */
    if (I->src == E_INLINE && I->dst == E_BUF) {
        int64_t tt = tr_now();
        memcpy(S->pool[I->buf_id].mem + I->buf_off, I->payload,
               (size_t)I->payload_len);
        tr_log(S, tt, tr_now(), t->tid, OP_MOV, I->buf_id, I->payload_len,
               I->buf_off, 100);                  /* aux 100 = inline->buf */
        t->pc++; return;
    }
    Task *k = calloc(1, sizeof *k);
    k->tid = t->tid; k->epoch = t->epoch; k->arch_fd = S->arch_fd;
    k->op = I->op; k->buf_log = I->buf_id;
    k->detail = I->len ? I->len : (int64_t)I->payload_len;
    k->path = I->path; k->dpath = I->dpath;
    k->payload = I->payload; k->payload_len = I->payload_len;
    k->buf_off = I->buf_off; k->arch_off = I->arch_off; k->len = I->len;
    k->mode = I->mode; k->mtime_ns = I->mtime_ns;
    if (I->buf_id >= 0) k->buf = S->pool[I->buf_id].mem;
    if      (I->src==E_FS  && I->dst==E_BUF ) k->kind = TK_FS_TO_BUF;
    else if (I->src==E_BUF && I->dst==E_FS  ) k->kind = TK_BUF_TO_FS;
    else if (I->src==E_BUF && I->dst==E_ARCH) k->kind = TK_BUF_TO_ARCH;
    else if (I->src==E_INLINE && I->dst==E_ARCH) k->kind = TK_INLINE_TO_ARCH;
    else if (I->src==E_FS && I->dst==E_FS   ) k->kind = TK_CFR_FS_TO_FS;
    else if (I->src==E_FS && I->dst==E_ARCH ) k->kind = TK_CFR_FS_TO_ARCH;
    t->pc++;                              /* advance BEFORE suspend */
    S->inflight++;
#ifdef QVM_URING
    if (S->use_uring && (k->kind == TK_FS_TO_BUF || k->kind == TK_BUF_TO_FS))
        tq_push(&S->ring_tasks, k);      /* per-file read/write → the ring */
    else
#endif
        tq_push(&S->tasks, k);
    t->st = T_WAIT_IO;
}

/* run a thread from pc until it suspends (task in flight / parked) or finishes */
static void run_thread(Sched *S, Thread *t){
    S->cur_epoch = t->epoch;                      /* for tr_log */
    while (t->pc < t->nprog) {
        Instr *I = &t->prog[t->pc];
        switch (I->op) {
        case OP_ALLOC: {
            int64_t tt = tr_now();
            if (!pool_alloc(S, t, I->buf_id, I->cap, I->mode > 0)) return; /* parked */
            tr_log(S, tt, tr_now(), t->tid, OP_ALLOC, I->buf_id, I->cap, 0, I->mode>0);
            t->pc++; break;
        }
        case OP_FREE:
            tr_log(S, tr_now(), tr_now(), t->tid, OP_FREE, I->buf_id, 0, 0, 0);
            pool_free(S, I->buf_id); t->pc++; break;
        case OP_MOV:
            submit_mov(S, t, I);
            if (t->st == T_WAIT_IO) return;    /* async: wait for completion */
            break;                              /* sync (inline->buf): continue */
        case OP_MKDIR: case OP_SETMETA:
        case OP_UNLINK: case OP_RMDIR: case OP_FBARRIER: {
            Task *k = calloc(1, sizeof *k);
            k->tid = t->tid; k->epoch = t->epoch; k->path = I->path; k->op = I->op; k->buf_log = -1;
            k->mode = I->mode; k->mtime_ns = I->mtime_ns; k->arch_fd = S->arch_fd;
            k->kind = I->op==OP_MKDIR ? TK_MKDIR : I->op==OP_SETMETA ? TK_SETMETA :
                      I->op==OP_UNLINK ? TK_UNLINK : I->op==OP_RMDIR ? TK_RMDIR :
                      TK_FBARRIER;
            t->pc++; S->inflight++; tq_push(&S->tasks, k); t->st = T_WAIT_IO;
            return;
        }
        case OP_INFLATE: case OP_DEFLATE: {
            Task *k = calloc(1, sizeof *k);
            k->tid = t->tid; k->epoch = t->epoch; k->arch_fd = S->arch_fd;
            k->op = I->op; k->buf_log = I->buf_id; k->detail = I->len;
            k->buf = I->buf_id >= 0 ? S->pool[I->buf_id].mem : NULL;
            k->buf_off = I->buf_off; k->len = I->len; k->arch_off = I->arch_off;
            k->level = I->level; k->frame_id = I->frame_id;
            k->payload = I->payload; k->payload_len = I->payload_len;  /* runs */
            k->digest_on = I->mode > 0;    /* DIGEST flag: hash decoded content */
            if (I->op == OP_DEFLATE) { k->kind = TK_DEFLATE;
                k->sink = &S->sinks[I->sink]; }
            else k->kind = TK_INFLATE;
            t->pc++; S->inflight++; tq_push(&S->tasks, k); t->st = T_WAIT_IO;
            return;
        }
        case OP_SPAWN: {
            tr_log(S, tr_now(), tr_now(), t->tid, OP_SPAWN, I->lo, I->cap, 0, 0);
            Batch *B = &S->bat[t->epoch];
            for (int64_t i = I->lo; i <= I->cap; i++)
                if (B->th[i].st == T_INERT) ready_push(S, &B->th[i]);
            t->pc++; break;
        }
        case OP_JOIN:
            if (range_done(S, t->epoch, I->lo, I->cap)) { t->pc++; break; }
            tr_log(S, tr_now(), tr_now(), t->tid, OP_JOIN, I->lo, I->cap, 0, 0);
            t->join_lo = I->lo; t->join_hi = I->cap; t->st = T_WAIT_JOIN;
            return;
        case OP_CALL:
            /* Call into Python: emit the call id and SUSPEND. The READER THREAD
             * fetches the response (so the scheduler never blocks on the read)
             * and delivers it as a TK_BATCH_READY event; run_sched then adds the
             * returned batch's threads and wakes us when it completes. Meanwhile
             * this batch's other work (a prefetch loader) AND any prior batch's
             * compression keep running concurrently. pc advances on wake. */
            if (S->call_fd < 0 || write(S->call_fd, &I->frame_id, 8) != 8) {
                S->failed = S->failed ? S->failed : -EIO; t->pc++; break;
            }
            pw_push(S, t->epoch, t->tid, tr_now(), I->frame_id);
            t->st = T_WAIT_CALL;
            return;
        default: t->pc++; break;
        }
    }
    thread_done(S, t);
}

/* register a new batch (epoch) from a flat, tid-sorted Instr array; returns the
 * epoch. Threads are per-batch, so tids never collide across concurrent batches. */
static int build_batch(Sched *S, Instr *ins, int n){
    if (S->nbat == S->batcap) { S->batcap = S->batcap ? S->batcap*2 : 16;
        S->bat = realloc(S->bat, S->batcap * sizeof(Batch)); }
    int ep = S->nbat++;
    Batch *B = &S->bat[ep]; memset(B, 0, sizeof *B);
    int maxtid = 0;
    for (int i = 0; i < n; i++) if ((int)ins[i].tid > maxtid) maxtid = ins[i].tid;
    B->nth = maxtid + 1;
    B->th = calloc(B->nth, sizeof(Thread));
    for (int i = 0; i < B->nth; i++) {
        B->th[i].tid = i; B->th[i].epoch = ep; B->th[i].st = T_INERT; }
    int i = 0;
    while (i < n) {
        uint32_t tid = ins[i].tid; int j = i;
        while (j < n && ins[j].tid == tid) j++;
        B->th[tid].prog = &ins[i]; B->th[tid].nprog = j - i;
        i = j;
    }
    return ep;
}

/* deflate completions accumulate here for the footer; qvm_run returns them so
 * the caller can write {frame_id, coff, clen} back to the planner. */
static void comp_add(Sched *S, int64_t f, int64_t co, int64_t cl, int64_t dg){
    if (S->ncomp == S->ccap) { S->ccap = S->ccap ? S->ccap*2 : 64;
        S->cf = realloc(S->cf, S->ccap*8); S->cc = realloc(S->cc, S->ccap*8);
        S->cl = realloc(S->cl, S->ccap*8); S->cd = realloc(S->cd, S->ccap*8); }
    S->cf[S->ncomp] = f; S->cc[S->ncomp] = co; S->cl[S->ncomp] = cl;
    S->cd[S->ncomp] = dg; S->ncomp++;
}

/* Persistent scheduler: the pool, worker pool, sinks, completions and trace
 * live across incrementally-fed instruction batches, so a buffer allocated in
 * one batch survives into the next (the discovery→plan→execute feedback loop). */
static pthread_t g_wt[64]; static int g_nworkers;

/* reader thread: fetch framed CALL responses off stdin so the scheduler never
 * blocks on the read. Each response (and a NULL sentinel at EOF) is delivered as
 * a TK_BATCH_READY event on the same queue as task completions. */
static void *reader_main(void *arg){
    Sched *S = arg;
    for (;;) {
        size_t blen; uint8_t *b = read_framed(0, &blen);
        Task *k = calloc(1, sizeof *k);
        k->kind = TK_BATCH_READY; k->buf = b; k->len = b ? (int64_t)blen : 0;
        tq_push(&S->comps, k);
        if (!b) break;
    }
    return NULL;
}

static void qvm_open(Sched *S, int arch_fd, int *sink_fds, int nsinks,
                     int npool, int nworkers, int call_fd){
    memset(S, 0, sizeof *S);
    S->arch_fd = arch_fd; S->npool = npool; S->call_fd = call_fd;
    S->pool = calloc(npool, sizeof(BufSlot));
    S->nsinks = nsinks;
    /* WAL: resume-after-crash. QVM_WAL=<path> → append (frame,coff,clen,digest)
     * per committed deflate, fsync'd periodically. QVM_SINK_STARTS=off0,off1,...
     * positions each sink's cursor at its committed high-water and truncates the
     * torn tail, so the run continues exactly where it stopped. */
    S->wal_fd = -1;
    const char *walp = getenv("QVM_WAL");
    if (walp) S->wal_fd = open(walp, O_WRONLY|O_APPEND|O_CREAT|O_CLOEXEC, 0644);
    const char *starts = getenv("QVM_SINK_STARTS");
    S->sinks = calloc(nsinks ? nsinks : 1, sizeof(Sink));
    for (int i = 0; i < nsinks; i++) {
        int64_t st0 = 0;
        if (starts) { st0 = atoll(starts); const char *c = strchr(starts, ',');
                      starts = c ? c+1 : ""; }
        S->sinks[i].fd = sink_fds[i]; S->sinks[i].cursor = st0;
        struct stat st;                          /* pipe/socket → hold-through-write */
        S->sinks[i].is_pipe = (fstat(sink_fds[i], &st) == 0
                               && (S_ISFIFO(st.st_mode) || S_ISSOCK(st.st_mode)));
        if (!S->sinks[i].is_pipe) { if (ftruncate(sink_fds[i], st0)) {} }  /* 0=fresh */
        pthread_mutex_init(&S->sinks[i].mu, NULL);
    }
    tq_init(&S->tasks); tq_init(&S->comps);
    if (getenv("QVM_TRACE")) S->t_base = tr_now();
#ifdef QVM_URING
    const char *ur = getenv("QVM_URING");        /* opt-in per-file ring backend */
    S->use_uring = ur ? atoi(ur) : 0;
    if (S->use_uring) {
        S->qd = S->use_uring > 1 ? S->use_uring : 128;
        if (io_uring_queue_init(S->qd, &S->ring, 0) < 0) S->use_uring = 0;
        else { tq_init(&S->ring_tasks);
               pthread_create(&S->ring_th, 0, ring_worker, S); }
    }
#endif
    static Worker w; w.in = &S->tasks; w.out = &S->comps;
    g_nworkers = nworkers;
    for (int k = 0; k < nworkers; k++) pthread_create(&g_wt[k], 0, worker_main, &w);
    if (call_fd >= 0) {                          /* CALL responses read off-thread */
        S->has_reader = 1;
        pthread_create(&S->reader_th, 0, reader_main, S);
    }
}

/* THE scheduler loop — one loop over ALL batches' threads. Drain the ready
 * queue, then: if a CALL is pending, read its response batch from Python and add
 * its threads (the in-flight prefetch loader keeps running on the workers DURING
 * this blocking read — that is the overlap); otherwise reap a completion and
 * resume its thread. Runs until nothing is ready, pending, or in flight. */
static void run_sched(Sched *S){
    for (;;) {
        Thread *t;
        while ((t = ready_pop(S))) run_thread(S, t);
        if (S->inflight == 0 && S->pw_count == 0) break;  /* nothing left to do */
        Task *k = tq_pop(&S->comps);         /* one event: completion OR response */
        if (k->kind == TK_BATCH_READY) {     /* a CALL response the reader fetched */
            uint8_t *raw = k->buf; size_t rawlen = (size_t)k->len; free(k);
            PW *w = pw_pop(S);               /* matches the oldest pending CALL */
            if (!raw || !w) {                /* reader EOF with a CALL outstanding */
                if (w) { S->failed = S->failed ? S->failed : -EIO; free(w); }
                free(raw); if (S->pw_count == 0 && S->inflight == 0) break; else continue;
            }
            S->cur_epoch = w->ep;
            tr_log(S, w->t0, tr_now(), w->tid, OP_CALL, w->cid, 0, 0, 0); /* Python */
            int nn; char *nap, *nad;
            Instr *nins = qvm_decode_arrow(raw, rawlen, &nn, &nap, &nad);
            int ne = build_batch(S, nins, nn);
            Batch *B = &S->bat[ne];
            B->wep = w->ep; B->wtid = w->tid; B->hasw = 1;
            B->pm = nins; B->ap = nap; B->ad = nad; B->raw = raw;  /* freed at retire */
            ready_push(S, &B->th[0]);
            free(w);
            continue;
        }
        S->inflight--;
        if (k->res < 0 && !S->failed) S->failed = k->res;
        if (k->res == 0 && k->kind == TK_DEFLATE) {
            comp_add(S, k->frame_id, k->coff, k->clen, k->digest);
            if (S->wal_fd >= 0) {           /* durable record: bytes are in the sink */
                int64_t rec[4] = {k->frame_id, k->coff, k->clen, k->digest};
                if (write(S->wal_fd, rec, 32) != 32) S->failed = S->failed ? S->failed : -EIO;
                if (++S->wal_n >= 256) { fsync(S->wal_fd); S->wal_n = 0; }
            }
        } else if (k->res == 0 && k->kind == TK_INFLATE && k->digest_on)
            comp_add(S, k->frame_id, -1, -1, k->digest);   /* verify: report digest */
        S->cur_epoch = k->epoch;
        {   /* op params for the viz: mov→(arch_off, endpoint-kind), */
            int64_t off = k->arch_off, aux = 0;   /* deflate→(frame_id, in_len) */
            if (k->op == OP_DEFLATE) { off = k->frame_id; aux = k->len; }
            else if (k->op == OP_INFLATE) { aux = k->frame_id; }
            else if (k->op == OP_MOV) { aux = k->kind; }
            tr_log(S, k->wt0, k->wt1, k->tid, k->op, k->buf_log,
                   k->kind == TK_DEFLATE ? k->clen : k->detail, off, aux);
        }
        Thread *ct = TH(S, k->epoch, k->tid);
        if (ct->st == T_WAIT_IO) {                     /* resume at pc */
            ct->st = T_READY;   /* clear WAIT: a following sync op (inline->buf)
                                 * must not see stale WAIT_IO */
            run_thread(S, ct);
        }
        free(k);
    }
}

static void qvm_close(Sched *S){
    if (S->wal_fd >= 0) { fsync(S->wal_fd); close(S->wal_fd); S->wal_fd = -1; }
    if (S->has_reader) {                         /* close the request pipe → Python
                                                  * closes stdin → reader hits EOF */
        if (S->call_fd >= 0) { close(S->call_fd); S->call_fd = -1; }
        pthread_join(S->reader_th, 0);
        Task *k;                                 /* drain leftover response events */
        while ((k = tq_trypop(&S->comps))) {
            if (k->kind == TK_BATCH_READY) free(k->buf);
            free(k);
        }
    }
    while (S->pw_head) free(pw_pop(S));
#ifdef QVM_URING
    if (S->use_uring) {
        S->ring_tasks.stop = 1; pthread_cond_broadcast(&S->ring_tasks.cv);
        pthread_join(S->ring_th, 0);
        io_uring_queue_exit(&S->ring);
    }
#endif
    S->tasks.stop = 1; pthread_cond_broadcast(&S->tasks.cv);
    for (int k = 0; k < g_nworkers; k++) pthread_join(g_wt[k], 0);
    const char *trp = getenv("QVM_TRACE");
    if (trp && S->t_base) {                       /* dump the trace */
        int tf = open(trp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (tf >= 0) {
            int64_t span = tr_now() - S->t_base; uint32_t nt = (uint32_t)S->ntr;
            if (write(tf, &span, 8) != 8 || write(tf, &nt, 4) != 4)
                S->failed = S->failed ? S->failed : -EIO;
            if (S->ntr && write(tf, S->tr, (size_t)S->ntr * sizeof(TraceEv))
                != (ssize_t)(S->ntr * sizeof(TraceEv)))
                S->failed = S->failed ? S->failed : -EIO;
            close(tf);
        }
    }
    free(S->tr);
    for (int i = 0; i < S->npool; i++) free(S->pool[i].mem);
    for (int i = 0; i < S->nsinks; i++) pthread_mutex_destroy(&S->sinks[i].mu);
    for (int i = 0; i < S->nbat; i++) {          /* any un-retired batch memory */
        free(S->bat[i].th); free(S->bat[i].pm);
        free(S->bat[i].ap); free(S->bat[i].ad); free(S->bat[i].raw);
    }
    free(S->bat); free(S->pool); free(S->sinks);
}

/* single-batch wrapper (unit tests): open, one batch, close. */
static int qvm_run(Instr *ins, int n, int arch_fd, int *sink_fds, int nsinks,
                   int npool, int nworkers, Sched *out){
    Sched S;
    qvm_open(&S, arch_fd, sink_fds, nsinks, npool, nworkers, -1);
    int ep = build_batch(&S, ins, n);            /* one batch, no CALL */
    ready_push(&S, &S.bat[ep].th[0]);
    run_sched(&S);
    qvm_close(&S);
    int rc = S.failed;
    if (out) *out = S;
    else { free(S.cf); free(S.cc); free(S.cl); free(S.cd); }
    return rc;
}

/* ----------------------------------------------------- Arrow decode + CLI   */
/* The instruction stream is an Arrow-IPC batch (quiver.ipc, compat=oldest) —
 * one serialization path with the rest of the system, produced vectorized by
 * Polars. We read the columns straight out of the batch buffers. Minimal
 * flatbuffer navigation, mirroring quiver-exec.c's input side. */
static uint16_t fb_u16(const uint8_t *b, int64_t o){ uint16_t v; memcpy(&v,b+o,2); return v; }
static int32_t  fb_i32(const uint8_t *b, int64_t o){ int32_t v;  memcpy(&v,b+o,4); return v; }
static uint32_t fb_u32(const uint8_t *b, int64_t o){ uint32_t v; memcpy(&v,b+o,4); return v; }
static int64_t  fb_i64(const uint8_t *b, int64_t o){ int64_t v;  memcpy(&v,b+o,8); return v; }
static int64_t  fb_root(const uint8_t *b){ return fb_u32(b,0); }
static int64_t fb_field(const uint8_t *b, int64_t table, int id){
    int64_t vt = table - fb_i32(b, table);
    int slot = 4 + 2*id;
    if (slot >= fb_u16(b, vt)) return -1;
    uint16_t voff = fb_u16(b, vt + slot);
    return voff ? table + voff : -1;
}
static int64_t fb_offset_field(const uint8_t *b, int64_t table, int id){
    int64_t p = fb_field(b, table, id);
    return p < 0 ? -1 : p + fb_u32(b, p);
}

/* Buffer indices for the instruction schema (column order = qplan.INSTR_COLS,
 * compat=oldest: primitives = [validity, values]; large_utf8/large_binary =
 * [validity, offsets(i64), data]). 33 buffers total. */
enum {
    B_TID=1, B_OP=3, B_SRC=5, B_DST=7, B_BUFID=9, B_BUFOFF=11, B_LEN=13,
    B_CAP=15, B_LO=17, B_AOFF=19,
    B_PATH_O=21, B_PATH_D=22, B_DPATH_O=24, B_DPATH_D=25, B_PAY_O=27, B_PAY_D=28,
    B_MODE=30, B_MTIME=32, B_SINK=34, B_LEVEL=36, B_FRAMEID=38,
};
static const uint8_t *abuf(const uint8_t *meta, int64_t bufs,
                          const uint8_t *body, int k){
    return body + fb_i64(meta, bufs + 4 + 16*(int64_t)k);  /* buffer k's offset */
}
/* build a \0-terminated arena from a large_utf8 (offsets,data); set ptrs[i] */
static char *str_arena(const int64_t *off, const uint8_t *dat, int n,
                       const char **ptrs){
    int64_t total = n ? off[n] : 0;
    char *a = malloc((size_t)total + n + 1); int64_t c = 0;
    for (int i = 0; i < n; i++) {
        int64_t len = off[i+1] - off[i];
        memcpy(a + c, dat + off[i], (size_t)len); a[c+len] = 0;
        ptrs[i] = a + c; c += len + 1;
    }
    return a;
}

/* Decode the single Arrow record batch into an Instr array. `data` is retained
 * by the caller (payload bytes point into it); path/dpath get \0 arenas. */
static Instr *qvm_decode_arrow(uint8_t *data, size_t sz, int *n_out,
                               char **arena_path, char **arena_dpath){
    const uint8_t *meta = NULL, *body = NULL; int64_t bufs = 0, nrows = 0;
    size_t pos = 0;
    while (pos + 8 <= sz) {
        uint32_t cont, mlen;
        memcpy(&cont, data+pos, 4); memcpy(&mlen, data+pos+4, 4);
        if (cont != 0xFFFFFFFFu || mlen == 0) break;         /* EOS/EOF */
        const uint8_t *m = data + pos + 8;
        int64_t rt = fb_root(m);
        int64_t htp = fb_field(m, rt, 1); int htype = htp>=0 ? m[htp] : 0;
        int64_t blp = fb_field(m, rt, 3); int64_t blen = blp>=0 ? fb_i64(m,blp):0;
        const uint8_t *bd = m + mlen;
        if (htype == 3) {                                    /* RecordBatch */
            int64_t rb = fb_offset_field(m, rt, 2);
            int64_t lf = fb_field(m, rb, 0);   /* length; omitted (==-1) when 0 —
                                                * flatbuffers drop default fields */
            nrows = lf >= 0 ? fb_i64(m, lf) : 0;
            bufs  = fb_offset_field(m, rb, 2);
            meta = m; body = bd; break;                      /* single batch */
        }
        size_t adv = 8 + mlen + (size_t)blen; adv = (adv + 7) & ~(size_t)7;
        pos += adv;
    }
    int n = (int)nrows;
    Instr *ins = calloc(n ? n : 1, sizeof(Instr));
    *arena_path = *arena_dpath = NULL; *n_out = n;
    if (!meta || !n) return ins;

    const int64_t *tid=(const int64_t*)abuf(meta,bufs,body,B_TID);
    const uint8_t *op =abuf(meta,bufs,body,B_OP), *src=abuf(meta,bufs,body,B_SRC),
                  *dst=abuf(meta,bufs,body,B_DST);
    const int32_t *bufid=(const int32_t*)abuf(meta,bufs,body,B_BUFID),
                  *mode =(const int32_t*)abuf(meta,bufs,body,B_MODE),
                  *sink =(const int32_t*)abuf(meta,bufs,body,B_SINK),
                  *lvl  =(const int32_t*)abuf(meta,bufs,body,B_LEVEL);
    const int64_t *fid  =(const int64_t*)abuf(meta,bufs,body,B_FRAMEID);
    const int64_t *boff=(const int64_t*)abuf(meta,bufs,body,B_BUFOFF),
                  *len =(const int64_t*)abuf(meta,bufs,body,B_LEN),
                  *cap =(const int64_t*)abuf(meta,bufs,body,B_CAP),
                  *lo  =(const int64_t*)abuf(meta,bufs,body,B_LO),
                  *aoff=(const int64_t*)abuf(meta,bufs,body,B_AOFF),
                  *mt  =(const int64_t*)abuf(meta,bufs,body,B_MTIME);
    const int64_t *po_p=(const int64_t*)abuf(meta,bufs,body,B_PATH_O);
    const uint8_t *pd_p=abuf(meta,bufs,body,B_PATH_D);
    const int64_t *po_d=(const int64_t*)abuf(meta,bufs,body,B_DPATH_O);
    const uint8_t *pd_d=abuf(meta,bufs,body,B_DPATH_D);
    const int64_t *po_y=(const int64_t*)abuf(meta,bufs,body,B_PAY_O);
    const uint8_t *pd_y=abuf(meta,bufs,body,B_PAY_D);

    const char **pp = malloc(sizeof(char*)*n), **pd = malloc(sizeof(char*)*n);
    *arena_path  = str_arena(po_p, pd_p, n, pp);
    *arena_dpath = str_arena(po_d, pd_d, n, pd);
    for (int i = 0; i < n; i++) {
        Instr *I = &ins[i];
        I->tid=(uint32_t)tid[i]; I->op=op[i]; I->src=src[i]; I->dst=dst[i];
        I->buf_id=bufid[i]; I->mode=mode[i];
        I->buf_off=boff[i]; I->len=len[i]; I->cap=cap[i]; I->lo=lo[i];
        I->arch_off=aoff[i]; I->mtime_ns=mt[i];
        I->sink=sink[i]; I->level=lvl[i]; I->frame_id=fid[i];
        I->path=pp[i]; I->dpath=pd[i];
        I->payload = pd_y + po_y[i]; I->payload_len = po_y[i+1]-po_y[i];
    }
    free(pp); free(pd);
    return ins;
}

/* one framed instruction batch: [u32 len][len bytes]. NULL at clean EOF. */
static uint8_t *read_framed(int fd, size_t *out){
    uint32_t len;
    ssize_t r = 0, got = 0;
    while (got < 4) {                            /* the length prefix */
        r = read(fd, (uint8_t *)&len + got, 4 - got);
        if (r <= 0) return NULL;                 /* EOF before/at a boundary */
        got += r;
    }
    uint8_t *b = malloc(len ? len : 1); size_t bg = 0;
    while (bg < len) {
        r = read(fd, b + bg, len - bg);
        if (r <= 0) { free(b); return NULL; }
        bg += r;
    }
    *out = len; return b;
}

/* ---------------------------------------------------- Arrow-emit (output) --
 * qvm reads Arrow instruction batches; it now EMITS the footer completions as
 * Arrow too (one serialization format both ways), via the compiler-generated
 * template. Lifted from quiver-exec.c's output side. */
struct WBuf { const void *p; int64_t len; };   /* p==NULL → empty validity */

static int write_full(int fd, const void *p, size_t n){
    const uint8_t *b = p;
    while (n) { ssize_t r = write(fd, b, n);
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        b += r; n -= (size_t)r; }
    return 0;
}
static void emit_schema(int fd, const unsigned char *meta, int len){
    uint32_t frame[2] = {0xFFFFFFFFu, (uint32_t)len};
    if (write_full(fd, frame, 8) || write_full(fd, meta, (size_t)len)) { /* ignore */ }
}
static int emit_batch(int fd, const unsigned char *tmpl, int tmpl_len,
                      int off_bodylen, int off_rblen,
                      const int *node_off, int n_nodes,
                      const int *buf_off, int n_bufs,
                      int64_t n_rows, const struct WBuf *bufs){
    uint8_t *meta = malloc((size_t)tmpl_len);
    memcpy(meta, tmpl, (size_t)tmpl_len);
    int64_t pos = 0, zero = 0;
    for (int i = 0; i < n_bufs; i++) {
        memcpy(meta + buf_off[i], &pos, 8);
        memcpy(meta + buf_off[i] + 8, &bufs[i].len, 8);
        pos += (bufs[i].len + 7) & ~7LL;
    }
    memcpy(meta + off_bodylen, &pos, 8);
    memcpy(meta + off_rblen, &n_rows, 8);
    for (int i = 0; i < n_nodes; i++) {
        memcpy(meta + node_off[i], &n_rows, 8);
        memcpy(meta + node_off[i] + 8, &zero, 8);
    }
    uint32_t fr[2] = {0xFFFFFFFFu, (uint32_t)tmpl_len};
    static const uint8_t pad[8] = {0};
    int rc = write_full(fd, fr, 8) || write_full(fd, meta, (size_t)tmpl_len);
    for (int i = 0; !rc && i < n_bufs; i++)
        if (bufs[i].len)
            rc = write_full(fd, bufs[i].p, (size_t)bufs[i].len) ||
                 write_full(fd, pad, (size_t)((-bufs[i].len) & 7));
    free(meta);
    return rc ? -1 : 0;
}

/* write the accumulated completions as one Arrow-IPC stream (schema+batch+EOS) */
static void emit_completions(int fd, Sched *S){
    emit_schema(fd, QCOMP_SCHEMA_META, QCOMP_SCHEMA_LEN);
    struct WBuf b[QCOMP_N_BUFS] = {
        {NULL,0},{S->cf, 8*(int64_t)S->ncomp}, {NULL,0},{S->cc, 8*(int64_t)S->ncomp},
        {NULL,0},{S->cl, 8*(int64_t)S->ncomp}, {NULL,0},{S->cd, 8*(int64_t)S->ncomp}};
    emit_batch(fd, QCOMP_BATCH_TMPL, QCOMP_TMPL_LEN, QCOMP_OFF_BODYLEN,
               QCOMP_OFF_RBLEN, QCOMP_NODE_OFF, QCOMP_N_NODES,
               QCOMP_BUF_OFF, QCOMP_N_BUFS, S->ncomp, b);
    uint32_t eos[2] = {0xFFFFFFFFu, 0}; write_full(fd, eos, 8);
}

/* --------------------------------------------------------- filesystem scan */
/* A parallel directory walk (a dir queue drained by N worker threads; each dir
 * is opened relative to its parent's fd, so no path re-walk and no PATH_MAX
 * limit). Emits ONE Arrow batch — relative path, is_dir, size, mode, mtime_ns,
 * uid, gid — the same rows the planner consumes from wire.scan (root excluded,
 * dirs + files, incl. empty dirs). Makes qvm self-sufficient for discovery. */
typedef struct SDir { int dfd; char *rel; struct SDir *next; } SDir;
typedef struct {
    pthread_mutex_t qmu; pthread_cond_t qcv;
    SDir *qh, *qt; int64_t pending; int done;
    pthread_mutex_t amu;                          /* shared row accumulator */
    char *pdata; int64_t plen, pcap;              /* large_utf8 path data */
    int64_t *poff; uint8_t *isdir; int64_t *size, *mtime;
    int32_t *mode, *uid, *gid; int64_t n, cap;
} Scan;

static void sacc_add(Scan *s, const char *rel, size_t rl, int isdir, int64_t sz,
                     int32_t md, int64_t mt, int32_t uid, int32_t gid){
    pthread_mutex_lock(&s->amu);
    if (s->n == s->cap) { s->cap = s->cap ? s->cap*2 : 4096;
        s->poff  = realloc(s->poff, (s->cap+1)*8);
        s->isdir = realloc(s->isdir, s->cap);
        s->size  = realloc(s->size, s->cap*8); s->mtime = realloc(s->mtime, s->cap*8);
        s->mode  = realloc(s->mode, s->cap*4); s->uid = realloc(s->uid, s->cap*4);
        s->gid   = realloc(s->gid, s->cap*4);
        if (s->n == 0) s->poff[0] = 0; }
    if (s->plen + (int64_t)rl > s->pcap) {
        s->pcap = s->pcap ? s->pcap : 1<<16;
        while (s->plen + (int64_t)rl > s->pcap) s->pcap *= 2;
        s->pdata = realloc(s->pdata, s->pcap); }
    memcpy(s->pdata + s->plen, rel, rl); s->plen += rl;
    s->poff[s->n+1] = s->plen;
    s->isdir[s->n]=(uint8_t)isdir; s->size[s->n]=sz; s->mode[s->n]=md;
    s->mtime[s->n]=mt; s->uid[s->n]=uid; s->gid[s->n]=gid; s->n++;
    pthread_mutex_unlock(&s->amu);
}
static void sq_push(Scan *s, int dfd, char *rel){   /* takes ownership of rel */
    SDir *d = malloc(sizeof *d); d->dfd = dfd; d->rel = rel; d->next = NULL;
    pthread_mutex_lock(&s->qmu);
    if (s->qt) s->qt->next = d; else s->qh = d; s->qt = d; s->pending++;
    pthread_cond_signal(&s->qcv); pthread_mutex_unlock(&s->qmu);
}
static void *scan_worker(void *arg){
    Scan *s = arg;
    for (;;) {
        pthread_mutex_lock(&s->qmu);
        while (!s->qh && !s->done) pthread_cond_wait(&s->qcv, &s->qmu);
        if (!s->qh) { pthread_mutex_unlock(&s->qmu); break; }   /* done && empty */
        SDir *d = s->qh; s->qh = d->next; if (!s->qh) s->qt = NULL;
        pthread_mutex_unlock(&s->qmu);

        DIR *dp = fdopendir(d->dfd);                /* takes ownership of dfd */
        if (dp) { struct dirent *e; size_t pl = strlen(d->rel);
            while ((e = readdir(dp))) {
                const char *nm = e->d_name;
                if (nm[0]=='.' && (!nm[1] || (nm[1]=='.' && !nm[2]))) continue;
                size_t nl = strlen(nm);
                char *cr = malloc(pl + 1 + nl + 1);  /* child relative path */
                if (pl) { memcpy(cr, d->rel, pl); cr[pl]='/'; memcpy(cr+pl+1, nm, nl+1); }
                else memcpy(cr, nm, nl+1);
                size_t crl = pl ? pl+1+nl : nl;
                struct stat st;
                if (fstatat(dirfd(dp), nm, &st, AT_SYMLINK_NOFOLLOW) < 0) { free(cr); continue; }
                int isdir = S_ISDIR(st.st_mode) ? 1 : 0;
                sacc_add(s, cr, crl, isdir, (int64_t)st.st_size, (int32_t)st.st_mode,
                         (int64_t)st.st_mtim.tv_sec*1000000000 + st.st_mtim.tv_nsec,
                         (int32_t)st.st_uid, (int32_t)st.st_gid);
                if (isdir) { int cfd = openat(dirfd(dp), nm,
                                 O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);
                    if (cfd >= 0) sq_push(s, cfd, cr); else free(cr); }
                else free(cr);
            }
            closedir(dp);
        }
        free(d->rel); free(d);
        pthread_mutex_lock(&s->qmu);
        if (--s->pending == 0) { s->done = 1; pthread_cond_broadcast(&s->qcv); }
        pthread_mutex_unlock(&s->qmu);
    }
    return NULL;
}
static int qvm_scan(const char *root, int nthreads, int outfd){
    int rfd = open(root, O_RDONLY|O_DIRECTORY|O_CLOEXEC);
    if (rfd < 0) { fprintf(stderr, "qvm scan: open %s: %s\n", root, strerror(errno)); return 1; }
    Scan s; memset(&s, 0, sizeof s);
    pthread_mutex_init(&s.qmu,0); pthread_cond_init(&s.qcv,0); pthread_mutex_init(&s.amu,0);
    if (nthreads < 1) nthreads = 1;
    sq_push(&s, rfd, strdup(""));                   /* seed: root, rel "" */
    pthread_t *th = malloc(nthreads*sizeof *th);
    for (int i=0;i<nthreads;i++) pthread_create(&th[i],0,scan_worker,&s);
    for (int i=0;i<nthreads;i++) pthread_join(th[i],0);

    emit_schema(outfd, QSCAN_SCHEMA_META, QSCAN_SCHEMA_LEN);
    int64_t n = s.n;
    struct WBuf b[QSCAN_N_BUFS] = {
        {NULL,0},{s.poff, 8*(n+1)},{s.pdata, s.plen},
        {NULL,0},{s.isdir, n},   {NULL,0},{s.size, 8*n},
        {NULL,0},{s.mode, 4*n},  {NULL,0},{s.mtime, 8*n},
        {NULL,0},{s.uid, 4*n},   {NULL,0},{s.gid, 4*n}};
    if (n == 0) b[1] = (struct WBuf){NULL,0};       /* no offsets when empty */
    emit_batch(outfd, QSCAN_BATCH_TMPL, QSCAN_TMPL_LEN, QSCAN_OFF_BODYLEN,
               QSCAN_OFF_RBLEN, QSCAN_NODE_OFF, QSCAN_N_NODES,
               QSCAN_BUF_OFF, QSCAN_N_BUFS, n, b);
    uint32_t eos[2] = {0xFFFFFFFFu, 0}; write_full(outfd, eos, 8);
    free(s.poff); free(s.pdata); free(s.isdir); free(s.size); free(s.mtime);
    free(s.mode); free(s.uid); free(s.gid); free(th);
    return 0;
}

/* ------------------------------------------ S3 ETag + CRC64NVME (content sync) */
/* Compute each file's S3-compatible ETag locally: single PutObject (size <=
 * part_size) → MD5(file), no suffix; multipart → MD5(concat of per-part MD5s) +
 * "-<nparts>". Plus the full-object CRC64NVME. Parallel ACROSS files. Lets S3
 * sync be content-addressed — upload only files whose listed ETag differs. */
static uint64_t g_crc64[256]; static pthread_once_t g_crc64_once = PTHREAD_ONCE_INIT;
static void crc64_setup(void){
    for (int i = 0; i < 256; i++) { uint64_t c = (uint64_t)i;
        for (int k = 0; k < 8; k++) c = (c>>1) ^ ((c&1) ? 0x9a6c9329ac4bc9b5ULL : 0);
        g_crc64[i] = c; }
}
static uint64_t crc64_upd(uint64_t crc, const uint8_t *p, size_t n){
    while (n--) crc = g_crc64[(crc ^ *p++) & 0xff] ^ (crc >> 8);
    return crc;
}
static int etag_of(const char *path, int64_t part_size, char *etag, uint64_t *crc_out){
    struct stat st;
    if (stat(path, &st) < 0) return -errno;
    int64_t size = st.st_size;
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -errno;
    int64_t nparts = size <= part_size ? 1 : (size + part_size - 1) / part_size;
    uint8_t (*dig)[16] = malloc((size_t)nparts * 16);
    uint8_t *buf = malloc(1<<20);
    uint64_t crc = ~0ULL; int err = 0;
    for (int64_t p = 0; p < nparts; p++) {
        int64_t off = p*part_size, len = size - off; if (len > part_size) len = part_size;
        MD5_CTX md; MD5_Init(&md); int64_t done = 0;
        while (done < len) { int64_t want = len - done; if (want > (1<<20)) want = 1<<20;
            ssize_t r = pread(fd, buf, (size_t)want, off + done);
            if (r <= 0) { err = r < 0 ? -errno : -EIO; goto out; }
            MD5_Update(&md, buf, (unsigned long)r);
            crc = crc64_upd(crc, buf, (size_t)r); done += r; }
        MD5_Final(dig[p], &md);
    }
    uint8_t fmd[16];
    if (size <= part_size) memcpy(fmd, dig[0], 16);      /* PutObject: MD5(file) */
    else { MD5_CTX md; MD5_Init(&md);                    /* multipart: MD5(concat) */
        MD5_Update(&md, dig, 16*(unsigned long)nparts); MD5_Final(fmd, &md); }
    for (int i = 0; i < 16; i++) sprintf(etag + i*2, "%02x", fmd[i]);
    if (size > part_size) sprintf(etag + 32, "-%lld", (long long)nparts);
    *crc_out = ~crc;
out: free(dig); free(buf); close(fd);
    return err;
}

typedef struct { char **paths; int n; char **etags; uint64_t *crc; int *err;
                 int next; pthread_mutex_t mu; int64_t part_size; } EtagJob;
static void *etag_worker(void *arg){
    EtagJob *j = arg; char buf[48];
    for (;;) {
        pthread_mutex_lock(&j->mu); int i = j->next < j->n ? j->next++ : -1;
        pthread_mutex_unlock(&j->mu);
        if (i < 0) break;
        buf[0] = 0; j->err[i] = etag_of(j->paths[i], j->part_size, buf, &j->crc[i]);
        j->etags[i] = strdup(buf);
    }
    return NULL;
}
static int qvm_etag(int64_t part_size, int nthreads, int outfd){
    pthread_once(&g_crc64_once, crc64_setup);
    /* read newline-separated paths from stdin */
    size_t cap = 1<<16, len = 0; char *in = malloc(cap);
    ssize_t r; while ((r = read(0, in+len, cap-len)) > 0) { len += r;
        if (len == cap) { cap *= 2; in = realloc(in, cap); } }
    char **paths = NULL; int n = 0, pc = 0;
    for (size_t i = 0, s = 0; i <= len; i++) {
        if (i == len || in[i] == '\n') { if (i > s) {
            if (n == pc) { pc = pc ? pc*2 : 256; paths = realloc(paths, pc*sizeof(char*)); }
            paths[n] = malloc(i-s+1); memcpy(paths[n], in+s, i-s); paths[n][i-s]=0; n++; }
            s = i+1; } }
    EtagJob j = { paths, n, calloc(n?n:1,sizeof(char*)), calloc(n?n:1,8),
                  calloc(n?n:1,4), 0, PTHREAD_MUTEX_INITIALIZER, part_size };
    if (nthreads < 1) nthreads = 1;
    pthread_t *th = malloc(nthreads*sizeof *th);
    for (int i=0;i<nthreads;i++) pthread_create(&th[i],0,etag_worker,&j);
    for (int i=0;i<nthreads;i++) pthread_join(th[i],0);
    /* build large_utf8 (path, etag) + i64 cksum, emit QETAG */
    int64_t *poff = malloc((n+1)*8), *eoff = malloc((n+1)*8);
    int64_t pl=0, el=0; poff[0]=eoff[0]=0;
    for (int i=0;i<n;i++){ pl += strlen(paths[i]); poff[i+1]=pl;
        el += strlen(j.etags[i]?j.etags[i]:""); eoff[i+1]=el; }
    char *pd = malloc(pl?pl:1), *ed = malloc(el?el:1);
    int64_t po=0, eo=0;
    for (int i=0;i<n;i++){ size_t a=strlen(paths[i]); memcpy(pd+po,paths[i],a); po+=a;
        const char *e=j.etags[i]?j.etags[i]:""; size_t b=strlen(e); memcpy(ed+eo,e,b); eo+=b; }
    int64_t *cks = malloc((n?n:1)*8);
    for (int i=0;i<n;i++) cks[i] = (int64_t)j.crc[i];
    emit_schema(outfd, QETAG_SCHEMA_META, QETAG_SCHEMA_LEN);
    struct WBuf b[QETAG_N_BUFS] = {
        {NULL,0},{poff,8*(n+1)},{pd,pl}, {NULL,0},{eoff,8*(n+1)},{ed,el},
        {NULL,0},{cks,8*n}};
    if (n==0){ b[1]=(struct WBuf){NULL,0}; b[4]=(struct WBuf){NULL,0}; }
    emit_batch(outfd, QETAG_BATCH_TMPL, QETAG_TMPL_LEN, QETAG_OFF_BODYLEN,
               QETAG_OFF_RBLEN, QETAG_NODE_OFF, QETAG_N_NODES,
               QETAG_BUF_OFF, QETAG_N_BUFS, n, b);
    uint32_t eos[2] = {0xFFFFFFFFu, 0}; write_full(outfd, eos, 8);
    for (int i=0;i<n;i++){ free(paths[i]); free(j.etags[i]); }
    free(paths); free(j.etags); free(j.crc); free(j.err); free(th);
    free(poff); free(eoff); free(pd); free(ed); free(cks); free(in);
    return 0;
}

#ifndef QVM_TEST
/* qvm <arch|-> <npool> <nworkers> <comp|-> [sink ...] : read an Arrow
 * instruction stream on stdin and execute it. `arch` is the archive fd
 * (E_ARCH movs / inflate source; O_RDWR|CREAT); each `sink` is a deflate output
 * (O_RDWR|CREAT|TRUNC). Deflate completions {frame_id, coff, clen} are written
 * to the `comp` file path as [u32 n][n×3 i64] unless it is "-". */
int main(int argc, char **argv){
    if (argc >= 3 && strcmp(argv[1], "scan") == 0)   /* qvm scan <root> [threads] */
        return qvm_scan(argv[2], argc > 3 ? atoi(argv[3]) : 8, 1);
    if (argc >= 3 && strcmp(argv[1], "etag") == 0)   /* qvm etag <part_size> [threads] <stdin paths */
        return qvm_etag(atoll(argv[2]), argc > 3 ? atoi(argv[3]) : 8, 1);
    if (argc < 2 || strcmp(argv[1], "qvm") != 0) {
        fprintf(stderr, "usage: %s qvm <arch|-> [npool] [nworkers] [comp|-] "
                        "[callfd|-] [sink ...]\n  or:  %s scan <root> [threads]\n",
                argv[0], argv[0]);
        return 2;
    }
    const char *arch = argc > 2 ? argv[2] : "-";
    int npool = argc > 3 ? atoi(argv[3]) : 16;
    int nworkers = argc > 4 ? atoi(argv[4]) : 8;
    const char *comp = argc > 5 ? argv[5] : "-";
    const char *callarg = argc > 6 ? argv[6] : "-";   /* OP_CALL request fd */
    int call_fd = strcmp(callarg, "-") ? atoi(callarg) : -1;
    int nsinks = argc > 7 ? argc - 7 : 0;
    int *sink_fds = nsinks ? calloc(nsinks, sizeof(int)) : NULL;
    for (int i = 0; i < nsinks; i++) {
        const char *sa = argv[7 + i];
        if (strncmp(sa, "fd:", 3) == 0) sink_fds[i] = atoi(sa + 3);  /* inherited pipe */
        else sink_fds[i] = open(sa, O_RDWR | O_CREAT, 0644);   /* qvm_open truncates
                                                * to the (resume) start offset */
        if (sink_fds[i] < 0) { perror("open sink"); return 2; }
    }
    int arch_fd = -1;
    if (strcmp(arch, "-") != 0) {
        arch_fd = open(arch, O_RDWR | O_CREAT, 0644);
        if (arch_fd < 0) { perror("open arch"); return 2; }
    }
    Sched out;
    qvm_open(&out, arch_fd, sink_fds, nsinks, npool, nworkers, call_fd);
    /* CALL is the sole entry point: a single bootstrap CALL fetches the entry
     * batch from Python (call id -1); that batch may itself CALL (drivers loop;
     * windows fetch per-window gathers). The CALL channel is the process's own
     * stdout (requests) + stdin (responses) — no fd-passing — so this runs
     * identically local or behind ["ssh", host] (call_fd is typically 1). */
    Instr boot; memset(&boot, 0, sizeof boot);
    boot.op = OP_CALL; boot.frame_id = -1; boot.buf_id = -1;
    boot.path = ""; boot.dpath = "";
    int ep = build_batch(&out, &boot, 1);        /* bootstrap: one CALL(-1) */
    ready_push(&out, &out.bat[ep].th[0]);
    run_sched(&out);
    qvm_close(&out);
    int rc = out.failed;
    if (strcmp(comp, "-") != 0) {               /* footer completions → Arrow */
        int cfd = open(comp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (cfd >= 0) { emit_completions(cfd, &out); close(cfd); }
    }
    free(out.cf); free(out.cc); free(out.cl); free(out.cd);
    if (arch_fd >= 0) close(arch_fd);
    for (int i = 0; i < nsinks; i++) close(sink_fds[i]);
    free(sink_fds);
    if (rc < 0) { fprintf(stderr, "qvm: op failed: %d\n", rc); return 1; }
    return 0;
}
#endif

#ifdef QVM_TEST
/* ------------------------------------------------------------------- tests */
#include <assert.h>
#include <dirent.h>

static Instr *IB; static int IN, ICAP;
static Instr *emit(void){ if (IN==ICAP){ICAP=ICAP?ICAP*2:64;
    IB=realloc(IB,ICAP*sizeof(Instr));} Instr *p=&IB[IN++]; memset(p,0,sizeof*p);
    p->buf_id=-1; p->mode=-1; p->mtime_ns=-1; return p; }
static void reset(void){ IN=0; }

static void wr(const char *p, const char *s){
    int fd=open(p,O_WRONLY|O_CREAT|O_TRUNC,0644); write(fd,s,strlen(s)); close(fd);}
static char *rd(const char *p, char *b, int n){
    int fd=open(p,O_RDONLY); if(fd<0)return 0; int r=read(fd,b,n-1);
    close(fd); if(r<0)r=0; b[r]=0; return b; }

static void test_cp(void){
    system("rm -rf /tmp/qvm_src /tmp/qvm_dst; mkdir -p /tmp/qvm_src/a/b");
    wr("/tmp/qvm_src/a/f1", "hello-one");
    wr("/tmp/qvm_src/a/b/f2", "hello-two-longer");
    reset();
    /* thread 0: spawn per-file threads [1..2], join */
    Instr *s=emit(); s->tid=0; s->op=OP_SPAWN; s->lo=1; s->cap=2;
    Instr *j=emit(); j->tid=0; j->op=OP_JOIN;  j->lo=1; j->cap=2;
    /* thread 1: mkdir a, cp f1 */
    Instr *m1=emit(); m1->tid=1; m1->op=OP_MKDIR; m1->path="/tmp/qvm_dst"; m1->mode=0755;
    Instr *m2=emit(); m2->tid=1; m2->op=OP_MKDIR; m2->path="/tmp/qvm_dst/a"; m2->mode=0755;
    Instr *c1=emit(); c1->tid=1; c1->op=OP_MOV; c1->src=E_FS; c1->dst=E_FS;
        c1->path="/tmp/qvm_src/a/f1"; c1->dpath="/tmp/qvm_dst/a/f1"; c1->len=9; c1->mode=0644;
    /* thread 2: mkdir a/b, cp f2 */
    Instr *n1=emit(); n1->tid=2; n1->op=OP_MKDIR; n1->path="/tmp/qvm_dst"; n1->mode=0755;
    Instr *n2=emit(); n2->tid=2; n2->op=OP_MKDIR; n2->path="/tmp/qvm_dst/a"; n2->mode=0755;
    Instr *n3=emit(); n3->tid=2; n3->op=OP_MKDIR; n3->path="/tmp/qvm_dst/a/b"; n3->mode=0755;
    Instr *c2=emit(); c2->tid=2; c2->op=OP_MOV; c2->src=E_FS; c2->dst=E_FS;
        c2->path="/tmp/qvm_src/a/b/f2"; c2->dpath="/tmp/qvm_dst/a/b/f2"; c2->len=16; c2->mode=0644;
    int rc = qvm_run(IB, IN, -1, NULL, 0, 8, 8, NULL);
    assert(rc == 0);
    char b[64];
    assert(strcmp(rd("/tmp/qvm_dst/a/f1",b,64), "hello-one")==0);
    assert(strcmp(rd("/tmp/qvm_dst/a/b/f2",b,64), "hello-two-longer")==0);
    printf("  ok cp: fs->fs copy_file_range, spawn-range + join, mkdir\n");
}

static void test_buffer_path(void){
    /* reassemble a file from an inline header + an fs body via a buffer:
     * alloc, mov inline->buf@0, mov fs->buf@6, mov buf->fs, free  (unpack shape) */
    system("rm -f /tmp/qvm_body /tmp/qvm_out");
    wr("/tmp/qvm_body", "BODYBYTES");
    reset();
    Instr *s=emit(); s->tid=0; s->op=OP_SPAWN; s->lo=1; s->cap=1;
    Instr *j=emit(); j->tid=0; j->op=OP_JOIN;  j->lo=1; j->cap=1;
    Instr *a=emit(); a->tid=1; a->op=OP_ALLOC; a->buf_id=0; a->cap=64;
    Instr *h=emit(); h->tid=1; h->op=OP_MOV; h->src=E_INLINE; h->dst=E_BUF;
        h->buf_id=0; h->buf_off=0; h->payload=(const uint8_t*)"HDR:: "; h->payload_len=6;
    Instr *b=emit(); b->tid=1; b->op=OP_MOV; b->src=E_FS; b->dst=E_BUF;
        b->buf_id=0; b->buf_off=6; b->path="/tmp/qvm_body"; b->len=9;
    Instr *o=emit(); o->tid=1; o->op=OP_MOV; o->src=E_BUF; o->dst=E_FS;
        o->buf_id=0; o->buf_off=0; o->len=15; o->path="/tmp/qvm_out"; o->mode=0644;
    Instr *f=emit(); f->tid=1; f->op=OP_FREE; f->buf_id=0;
    int rc = qvm_run(IB, IN, -1, NULL, 0, 8, 8, NULL);
    assert(rc == 0);
    char bb[64];
    assert(strcmp(rd("/tmp/qvm_out",bb,64), "HDR:: BODYBYTES")==0);
    printf("  ok buffer: alloc/inline->buf/fs->buf/buf->fs/free (unpack shape)\n");
}

static void test_fanout_backpressure(void){
    /* 32 threads each alloc a buffer, but only 4 pool slots -> alloc backpressure.
     * Each thread copies its input file through its buffer to an output. */
    system("rm -rf /tmp/qvm_fan; mkdir -p /tmp/qvm_fan");
    for (int i=0;i<32;i++){ char p[64],c[32]; sprintf(p,"/tmp/qvm_fan/in%02d",i);
        sprintf(c,"payload-%02d-xyz",i); wr(p,c); }
    reset();
    Instr *s=emit(); s->tid=0; s->op=OP_SPAWN; s->lo=1; s->cap=32;
    Instr *j=emit(); j->tid=0; j->op=OP_JOIN;  j->lo=1; j->cap=32;
    static char ip[32][64], op[32][64];
    for (int i=0;i<32;i++){
        int tid=i+1, slot=i%4;         /* ring of 4 slots -> contention */
        sprintf(ip[i],"/tmp/qvm_fan/in%02d",i); sprintf(op[i],"/tmp/qvm_fan/out%02d",i);
        Instr *a=emit(); a->tid=tid; a->op=OP_ALLOC; a->buf_id=slot; a->cap=64;
        Instr *r=emit(); r->tid=tid; r->op=OP_MOV; r->src=E_FS; r->dst=E_BUF;
            r->buf_id=slot; r->buf_off=0; r->path=ip[i]; r->len=14;
        Instr *w=emit(); w->tid=tid; w->op=OP_MOV; w->src=E_BUF; w->dst=E_FS;
            w->buf_id=slot; w->buf_off=0; w->len=14; w->path=op[i]; w->mode=0644;
        Instr *f=emit(); f->tid=tid; f->op=OP_FREE; f->buf_id=slot;
    }
    int rc = qvm_run(IB, IN, -1, NULL, 0, 4, 8, NULL);
    assert(rc == 0);
    for (int i=0;i<32;i++){ char b[64],want[32]; sprintf(want,"payload-%02d-xyz",i);
        assert(strcmp(rd(op[i],b,64),want)==0); }
    printf("  ok fan-out: 32 threads / 4 buffer slots, alloc backpressure holds\n");
}

static void test_codec(void){
    /* deflate a buffer to a sink (reserve+write), decompress the sink to verify,
     * then inflate the sink-frame back through a buffer to a file. */
    system("rm -f /tmp/qvm_cdata /tmp/qvm_sink0 /tmp/qvm_decoded");
    static char data[4000];
    for (int i = 0; i < 4000; i++) data[i] = 'A' + (i % 16);
    { int fd=open("/tmp/qvm_cdata",O_WRONLY|O_CREAT|O_TRUNC,0644);
      if (write(fd,data,4000)!=4000) abort(); close(fd); }
    int sfd = open("/tmp/qvm_sink0", O_RDWR|O_CREAT|O_TRUNC, 0644);

    reset();
    Instr *s=emit(); s->tid=0; s->op=OP_SPAWN; s->lo=1; s->cap=1;
    Instr *j=emit(); j->tid=0; j->op=OP_JOIN;  j->lo=1; j->cap=1;
    Instr *a=emit(); a->tid=1; a->op=OP_ALLOC; a->buf_id=0; a->cap=8192;
    Instr *r=emit(); r->tid=1; r->op=OP_MOV; r->src=E_FS; r->dst=E_BUF;
        r->buf_id=0; r->buf_off=0; r->path="/tmp/qvm_cdata"; r->len=4000;
    Instr *df=emit(); df->tid=1; df->op=OP_DEFLATE; df->buf_id=0; df->buf_off=0;
        df->len=4000; df->sink=0; df->level=3; df->frame_id=7;
    Instr *f=emit(); f->tid=1; f->op=OP_FREE; f->buf_id=0;
    int sinks[1] = { sfd };
    Sched out;
    int rc = qvm_run(IB, IN, -1, sinks, 1, 8, 8, &out);
    assert(rc == 0);
    assert(out.ncomp==1 && out.cf[0]==7 && out.cc[0]==0);
    int64_t clen = out.cl[0];
    free(out.cf); free(out.cc); free(out.cl); free(out.cd);
    static char comp[8192], dcomp[8192];
    int cfd=open("/tmp/qvm_sink0",O_RDONLY);
    if (pread(cfd,comp,8192,0) < clen) abort(); close(cfd);
    size_t z = ZSTD_decompress(dcomp, 8192, comp, (size_t)clen);
    assert(!ZSTD_isError(z) && z==4000 && memcmp(dcomp,data,4000)==0);
    printf("  ok codec: deflate buf->sink (reserve+write), comp {7,0,%ld}; "
           "frame decompresses byte-exact\n", (long)clen);

    int afd = open("/tmp/qvm_sink0", O_RDONLY);
    reset();
    Instr *s2=emit(); s2->tid=0; s2->op=OP_SPAWN; s2->lo=1; s2->cap=1;
    Instr *j2=emit(); j2->tid=0; j2->op=OP_JOIN;  j2->lo=1; j2->cap=1;
    Instr *a2=emit(); a2->tid=1; a2->op=OP_ALLOC; a2->buf_id=0; a2->cap=8192;
    Instr *in=emit(); in->tid=1; in->op=OP_INFLATE; in->buf_id=0; in->buf_off=0;
        in->arch_off=0; in->len=clen;
    Instr *w=emit(); w->tid=1; w->op=OP_MOV; w->src=E_BUF; w->dst=E_FS;
        w->buf_id=0; w->buf_off=0; w->len=4000; w->path="/tmp/qvm_decoded"; w->mode=0644;
    Instr *f2=emit(); f2->tid=1; f2->op=OP_FREE; f2->buf_id=0;
    rc = qvm_run(IB, IN, afd, NULL, 0, 8, 8, NULL);
    assert(rc == 0); close(afd);
    static char got[8192];
    int gfd=open("/tmp/qvm_decoded",O_RDONLY); int gn=pread(gfd,got,8192,0); close(gfd);
    assert(gn==4000 && memcmp(got,data,4000)==0);
    printf("  ok codec: inflate sink-frame -> buf -> fs, byte-exact round-trip\n");
}

int main(void){
    setvbuf(stdout, NULL, _IONBF, 0);
    printf("qvm v1 tests:\n");
    test_cp();
    test_buffer_path();
    test_fanout_backpressure();
    test_codec();
    printf("all qvm tests passed\n");
    return 0;
}
#endif
