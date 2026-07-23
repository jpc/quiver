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
 * This slice covers CODEC=NONE: alloc/free, mov (inline/fs/buf/arch), mkdir,
 * setmeta, spawn, join — enough for cp and uncompressed pack. inflate/deflate
 * and the sink lock land next.
 *
 * Build/test:  cc -O2 -pthread -DQVM_TEST -o /tmp/qvm quiver/exec/qvm.c && /tmp/qvm
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/* ------------------------------------------------------------------ opcodes */
enum {
    OP_ALLOC = 1, OP_FREE, OP_MOV, OP_MKDIR, OP_SETMETA,
    OP_SPAWN, OP_JOIN,
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
} Instr;

/* --------------------------------------------------------------- task queue */
/* A Task is a unit of async work handed to the worker pool; buffer operands are
 * resolved to raw pointers at submit time so workers never touch scheduler
 * state. The worker fills res (0 or -errno) and posts the tid back. */
typedef struct {
    uint32_t tid;
    uint8_t  kind;               /* mirrors the mov case / namespace op */
    int      arch_fd;
    uint8_t *buf; int64_t buf_off, arch_off, len;
    const char *path, *dpath;
    const uint8_t *payload; int64_t payload_len;
    int32_t mode; int64_t mtime_ns;
    int      res;
} Task;
enum { TK_INLINE_TO_BUF_UNUSED, TK_FS_TO_BUF, TK_BUF_TO_FS, TK_BUF_TO_ARCH,
       TK_INLINE_TO_ARCH, TK_CFR_FS_TO_FS, TK_CFR_FS_TO_ARCH,
       TK_MKDIR, TK_SETMETA };

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
        int64_t got = 0;
        while (got < t->len) {
            ssize_t r = pread(fd, t->buf + t->buf_off + got,
                              (size_t)(t->len-got), got);
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
    }
}

typedef struct { TQ *in; TQ *out; } Worker;
static void *worker_main(void *a){
    Worker *w = a;
    for (;;) {
        Task *t = tq_pop(w->in);
        if (!t) break;
        run_task(t);
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
typedef struct Thread {
    uint32_t tid;
    Instr *prog; int nprog, pc;
    enum { T_INERT, T_READY, T_WAIT_IO, T_WAIT_JOIN, T_WAIT_ALLOC, T_DONE } st;
    int64_t join_lo, join_hi;    /* range this thread is joining on */
    int      last_res;
    struct Thread *wnext;        /* buffer-waiter list link */
    struct Thread *rnext;        /* ready-queue link */
} Thread;

typedef struct {
    Thread *th; int nth;         /* threads indexed by tid */
    BufSlot *pool; int npool;
    int arch_fd;
    TQ tasks, comps;
    int inflight;
    Thread *ready_head, *ready_tail;
    int failed;                  /* first -errno seen */
} Sched;

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

static int pool_alloc(Sched *S, Thread *t, int id, int64_t cap){
    BufSlot *b = &S->pool[id];
    if (b->in_use) {                     /* backpressure: park until freed */
        t->wnext = b->waiters; b->waiters = t; t->st = T_WAIT_ALLOC;
        return 0;
    }
    if (b->cap < (size_t)cap) { b->mem = realloc(b->mem, cap); b->cap = cap; }
    b->in_use = 1;
    return 1;
}
static void pool_free(Sched *S, int id){
    BufSlot *b = &S->pool[id];
    b->in_use = 0;
    Thread *w = b->waiters; b->waiters = NULL;   /* wake all; first to re-run wins */
    while (w) { Thread *n = w->wnext; ready_push(S, w); w = n; }
}

/* mark thread done; wake any join-waiters whose range is now fully done */
static int range_done(Sched *S, int64_t lo, int64_t hi){
    for (int64_t i = lo; i <= hi; i++) if (S->th[i].st != T_DONE) return 0;
    return 1;
}
static void thread_done(Sched *S, Thread *t){
    t->st = T_DONE;
    for (int i = 0; i < S->nth; i++) {
        Thread *w = &S->th[i];
        if (w->st == T_WAIT_JOIN && range_done(S, w->join_lo, w->join_hi))
            { w->pc++; ready_push(S, w); }
    }
}

/* resolve a mov into a Task and submit it (async), or do it inline (sync). */
static void submit_mov(Sched *S, Thread *t, Instr *I){
    /* INLINE -> BUF is pure memory: do it synchronously, no task. */
    if (I->src == E_INLINE && I->dst == E_BUF) {
        memcpy(S->pool[I->buf_id].mem + I->buf_off, I->payload,
               (size_t)I->payload_len);
        t->pc++; return;
    }
    Task *k = calloc(1, sizeof *k);
    k->tid = t->tid; k->arch_fd = S->arch_fd;
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
    S->inflight++; tq_push(&S->tasks, k);
    t->st = T_WAIT_IO;
}

/* run a thread from pc until it suspends (task in flight / parked) or finishes */
static void run_thread(Sched *S, Thread *t){
    while (t->pc < t->nprog) {
        Instr *I = &t->prog[t->pc];
        switch (I->op) {
        case OP_ALLOC:
            if (!pool_alloc(S, t, I->buf_id, I->cap)) return;   /* parked */
            t->pc++; break;
        case OP_FREE:
            pool_free(S, I->buf_id); t->pc++; break;
        case OP_MOV:
            submit_mov(S, t, I);
            if (t->st == T_WAIT_IO) return;    /* async: wait for completion */
            break;                              /* sync (inline->buf): continue */
        case OP_MKDIR: case OP_SETMETA: {
            Task *k = calloc(1, sizeof *k);
            k->tid = t->tid; k->path = I->path;
            k->mode = I->mode; k->mtime_ns = I->mtime_ns;
            k->kind = I->op==OP_MKDIR ? TK_MKDIR : TK_SETMETA;
            t->pc++; S->inflight++; tq_push(&S->tasks, k); t->st = T_WAIT_IO;
            return;
        }
        case OP_SPAWN:
            for (int64_t i = I->lo; i <= I->cap; i++)
                if (S->th[i].st == T_INERT) ready_push(S, &S->th[i]);
            t->pc++; break;
        case OP_JOIN:
            if (range_done(S, I->lo, I->cap)) { t->pc++; break; }
            t->join_lo = I->lo; t->join_hi = I->cap; t->st = T_WAIT_JOIN;
            return;
        default: t->pc++; break;
        }
    }
    thread_done(S, t);
}

/* group a flat, tid-sorted Instr array into per-thread programs */
static void build_threads(Sched *S, Instr *ins, int n){
    int maxtid = 0;
    for (int i = 0; i < n; i++) if ((int)ins[i].tid > maxtid) maxtid = ins[i].tid;
    S->nth = maxtid + 1;
    S->th = calloc(S->nth, sizeof(Thread));
    for (int i = 0; i < S->nth; i++) { S->th[i].tid = i; S->th[i].st = T_INERT; }
    int i = 0;
    while (i < n) {
        uint32_t tid = ins[i].tid; int j = i;
        while (j < n && ins[j].tid == tid) j++;
        S->th[tid].prog = &ins[i]; S->th[tid].nprog = j - i;
        i = j;
    }
}

static int qvm_run(Instr *ins, int n, int arch_fd, int npool, int nworkers){
    Sched S; memset(&S, 0, sizeof S);
    S.arch_fd = arch_fd; S.npool = npool; S.failed = 0;
    S.pool = calloc(npool, sizeof(BufSlot));
    tq_init(&S.tasks); tq_init(&S.comps);
    build_threads(&S, ins, n);

    pthread_t wt[64]; Worker w = { &S.tasks, &S.comps };
    for (int k = 0; k < nworkers; k++) pthread_create(&wt[k], 0, worker_main, &w);

    ready_push(&S, &S.th[0]);            /* only thread 0 starts */
    for (;;) {
        Thread *t;
        while ((t = ready_pop(&S))) run_thread(&S, t);
        if (S.inflight == 0) break;
        Task *k = tq_pop(&S.comps);      /* block for a completion */
        S.inflight--;
        if (k->res < 0 && !S.failed) S.failed = k->res;
        Thread *ct = &S.th[k->tid];
        if (ct->st == T_WAIT_IO) run_thread(&S, ct);   /* resume at pc */
        free(k);
    }
    S.tasks.stop = 1; pthread_cond_broadcast(&S.tasks.cv);
    for (int k = 0; k < nworkers; k++) pthread_join(wt[k], 0);
    int rc = S.failed;
    for (int i = 0; i < npool; i++) free(S.pool[i].mem);
    free(S.pool); free(S.th);
    return rc;
}

/* ----------------------------------------------------- wire decode + CLI    */
/* Matches quiver/exec/qplan.py _REC (packed, little-endian, 88 bytes). */
typedef struct __attribute__((packed)) {
    uint32_t tid; uint8_t op, src, dst, pad;
    int32_t buf_id, mode;
    int64_t buf_off, len, cap, lo, arch_off, mtime_ns;
    uint32_t path_off, path_len, dpath_off, dpath_len, payload_off, payload_len;
} Rec;

/* Decode [u32 N][u64 heap_len][N×Rec][heap] into an Instr array. Strings point
 * straight into the (retained) heap, which is \0-terminated per entry. */
static Instr *qvm_decode(const uint8_t *data, size_t sz, int *n_out,
                         uint8_t **heap_out){
    if (sz < 12) return NULL;
    uint32_t n; uint64_t hlen;
    memcpy(&n, data, 4); memcpy(&hlen, data + 4, 8);
    const Rec *rec = (const Rec *)(data + 12);
    if (sz < 12 + (size_t)n * sizeof(Rec) + hlen) return NULL;
    const uint8_t *heap = data + 12 + (size_t)n * sizeof(Rec);
    uint8_t *hcopy = malloc(hlen ? hlen : 1);
    memcpy(hcopy, heap, hlen);
    Instr *ins = calloc(n ? n : 1, sizeof(Instr));
    for (uint32_t i = 0; i < n; i++) {
        const Rec *r = &rec[i]; Instr *I = &ins[i];
        I->tid = r->tid; I->op = r->op; I->src = r->src; I->dst = r->dst;
        I->buf_id = r->buf_id; I->mode = r->mode;
        I->buf_off = r->buf_off; I->len = r->len; I->cap = r->cap;
        I->lo = r->lo; I->arch_off = r->arch_off; I->mtime_ns = r->mtime_ns;
        I->path    = r->path_len    ? (const char *)(hcopy + r->path_off)  : "";
        I->dpath   = r->dpath_len   ? (const char *)(hcopy + r->dpath_off) : "";
        I->payload = r->payload_len ? hcopy + r->payload_off : NULL;
        I->payload_len = r->payload_len;
    }
    *n_out = (int)n; *heap_out = hcopy;
    return ins;
}

static uint8_t *read_all_fd(int fd, size_t *out){
    size_t cap = 1<<20, len = 0; uint8_t *b = malloc(cap);
    for (;;) {
        if (len == cap) { cap *= 2; b = realloc(b, cap); }
        ssize_t r = read(fd, b + len, cap - len);
        if (r < 0) { free(b); return NULL; }
        if (r == 0) break;
        len += r;
    }
    *out = len; return b;
}

#ifndef QVM_TEST
/* qvm <arch|-> <npool> <nworkers> : read an encoded instruction stream on
 * stdin and execute it. `arch` is the output archive fd for E_ARCH movs. */
int main(int argc, char **argv){
    if (argc < 2 || strcmp(argv[1], "qvm") != 0) {
        fprintf(stderr, "usage: %s qvm <arch|-> [npool] [nworkers]\n", argv[0]);
        return 2;
    }
    const char *arch = argc > 2 ? argv[2] : "-";
    int npool = argc > 3 ? atoi(argv[3]) : 16;
    int nworkers = argc > 4 ? atoi(argv[4]) : 8;
    int arch_fd = -1;
    if (strcmp(arch, "-") != 0) {
        arch_fd = open(arch, O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (arch_fd < 0) { perror("open arch"); return 2; }
    }
    size_t sz; uint8_t *data = read_all_fd(0, &sz);
    if (!data) { perror("read stdin"); return 2; }
    int n; uint8_t *heap;
    Instr *ins = qvm_decode(data, sz, &n, &heap);
    if (!ins) { fprintf(stderr, "qvm: bad instruction stream\n"); return 2; }
    int rc = qvm_run(ins, n, arch_fd, npool, nworkers);
    if (arch_fd >= 0) close(arch_fd);
    free(ins); free(heap); free(data);
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
    int rc = qvm_run(IB, IN, -1, 8, 8);
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
    int rc = qvm_run(IB, IN, -1, 8, 8);
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
    int rc = qvm_run(IB, IN, -1, 4, 8);
    assert(rc == 0);
    for (int i=0;i<32;i++){ char b[64],want[32]; sprintf(want,"payload-%02d-xyz",i);
        assert(strcmp(rd(op[i],b,64),want)==0); }
    printf("  ok fan-out: 32 threads / 4 buffer slots, alloc backpressure holds\n");
}

int main(void){
    setvbuf(stdout, NULL, _IONBF, 0);
    printf("qvm v1 (CODEC=NONE) tests:\n");
    test_cp();
    test_buffer_path();
    test_fanout_backpressure();
    printf("all qvm tests passed\n");
    return 0;
}
#endif
