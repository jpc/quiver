// dwriteq — can a DECOUPLED writer pool reach O_DIRECT's bandwidth without stalling producers?
//
// The finding this exists to act on: in bvm, flipping BVM_SINK_DIRECT is 1.2-1.5x SLOWER even
// though a raw O_DIRECT pwrite is FASTER than a buffered one (bench/dwrite: 6.38 vs 5.25 GB/s).
// The reason is not the syscall, it is who waits for it. A buffered pwrite returns once it has
// copied into page cache, so a bvm worker goes straight back to read+hash+compress while the
// kernel writes back underneath. O_DIRECT makes that same worker block for the whole
// round-trip: ns_write went 8.8 -> 250 aggregate worker-seconds, parking ~33 of 128 workers
// inside write(). The page-cache copy is not overhead, it is pipelining.
//
// So the only way to get direct-I/O bandwidth is to stop making the compressing thread wait.
// Three backends, identical producers, same bytes:
//
//   coupled   every producer writes its own frame (what bvm does today)
//   threads   producers hand frames to a bounded queue; W writer threads pwrite O_DIRECT
//   uring     one writer thread keeps QD O_DIRECT writes in flight on an io_uring
//
// The queue is bounded and buffers are recycled, so this also answers the capacity question:
// how many frame buffers must be in flight before producers stop stalling. Two counters make
// the verdict readable rather than inferred:
//
//   prod_stall   producer time waiting for a FREE buffer  -> writers are the bottleneck
//   wr_idle      writer time waiting for a FULL buffer    -> producers are the bottleneck
//
// LIMITATION, on purpose: producers here only memcpy (plus an optional CPU spin), they do not
// read from WEKA. That isolates the write path and hands it the whole fabric. Real workers
// contend for the same fabric on the read side, so the absolute GB/s here is an upper bound;
// what transfers is the RANKING of the three backends and the buffer count needed.
//
//   ./dwriteq <dir> [total_gb] [chunk_mb] [nprod] [nwriter] [nbuf] [prod_us]
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <stdint.h>
#include <errno.h>
#include <liburing.h>

#define MAXBUF 512
#define MAXSINK 64

static double now(void){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+1e-9*t.tv_nsec; }

/* bounded queue of buffer indices */
typedef struct {
    int v[MAXBUF]; int cap, head, tail, n, closed;
    pthread_mutex_t mu; pthread_cond_t ne, nf;
    double wait;                                  /* seconds blocked in pop */
} Q;

static void q_init(Q *q, int cap){
    memset(q, 0, sizeof *q); q->cap = cap;
    pthread_mutex_init(&q->mu, NULL); pthread_cond_init(&q->ne, NULL); pthread_cond_init(&q->nf, NULL);
}
static void q_push(Q *q, int x){
    pthread_mutex_lock(&q->mu);
    while (q->n == q->cap) pthread_cond_wait(&q->nf, &q->mu);
    q->v[q->tail] = x; q->tail = (q->tail + 1) % q->cap; q->n++;
    pthread_cond_signal(&q->ne); pthread_mutex_unlock(&q->mu);
}
/* blocking pop; -1 once closed and drained. Accumulates blocked time. */
static int q_pop(Q *q){
    pthread_mutex_lock(&q->mu);
    double t0 = now(); int waited = 0;
    while (q->n == 0 && !q->closed){ waited = 1; pthread_cond_wait(&q->ne, &q->mu); }
    if (waited) q->wait += now() - t0;
    if (q->n == 0){ pthread_mutex_unlock(&q->mu); return -1; }
    int x = q->v[q->head]; q->head = (q->head + 1) % q->cap; q->n--;
    pthread_cond_signal(&q->nf); pthread_mutex_unlock(&q->mu);
    return x;
}
/* non-blocking pop, for the ring writer topping up its in-flight window */
static int q_try(Q *q){
    pthread_mutex_lock(&q->mu);
    int x = -1;
    if (q->n){ x = q->v[q->head]; q->head = (q->head + 1) % q->cap; q->n--; pthread_cond_signal(&q->nf); }
    pthread_mutex_unlock(&q->mu);
    return x;
}
static void q_close(Q *q){
    pthread_mutex_lock(&q->mu); q->closed = 1;
    pthread_cond_broadcast(&q->ne); pthread_mutex_unlock(&q->mu);
}

static struct {
    uint8_t *buf[MAXBUF]; size_t chunk;
    int fd[MAXSINK], nsink;
    int64_t cursor[MAXSINK];                      /* next offset per sink */
    pthread_mutex_t cmu;
    Q freeq, fullq;
    int64_t frames_left; pthread_mutex_t fmu;     /* work budget */
    double bytes; pthread_mutex_t bmu;
    int prod_us;
    uint8_t *srcbuf;                              /* producers memcpy from here */
} G;

/* claim a frame of work; 0 when exhausted */
static int take_frame(void){
    pthread_mutex_lock(&G.fmu);
    int ok = G.frames_left > 0; if (ok) G.frames_left--;
    pthread_mutex_unlock(&G.fmu);
    return ok;
}
/* reserve an offset, exactly as bvm reserves sink cursor space under a lock */
static void reserve(int *sink, int64_t *off, size_t len){
    pthread_mutex_lock(&G.cmu);
    static int rr = 0;
    int s = rr++ % G.nsink;
    *sink = s; *off = G.cursor[s]; G.cursor[s] += (int64_t)len;
    pthread_mutex_unlock(&G.cmu);
}
static void add_bytes(double b){ pthread_mutex_lock(&G.bmu); G.bytes += b; pthread_mutex_unlock(&G.bmu); }

static int pwrite_all(int fd, const void *b, size_t n, off_t off){
    size_t o = 0;
    while (o < n){ ssize_t r = pwrite(fd, (const char*)b + o, n - o, off + o);
        if (r <= 0){ fprintf(stderr, "pwrite: %s\n", strerror(errno)); return -1; } o += (size_t)r; }
    return 0;
}
/* the per-frame CPU cost a real worker pays before the bytes are writable */
static void produce_into(uint8_t *dst){
    memcpy(dst, G.srcbuf, G.chunk);               /* stands in for zstd emitting its output */
    if (G.prod_us){ double t = now() + G.prod_us / 1e6; while (now() < t) ; }
}

/* --- coupled: producer writes its own frame, blocking (today's bvm) --- */
static void *th_coupled(void *p){
    int id = (int)(intptr_t)p;
    uint8_t *b = G.buf[id];
    while (take_frame()){
        produce_into(b);
        int s; int64_t off; reserve(&s, &off, G.chunk);
        if (pwrite_all(G.fd[s], b, G.chunk, off)) break;
        add_bytes((double)G.chunk);
    }
    return NULL;
}

/* --- decoupled: producers only fill buffers --- */
static void *th_prod(void *p){
    (void)p;
    while (take_frame()){
        int i = q_pop(&G.freeq); if (i < 0) break;
        produce_into(G.buf[i]);
        q_push(&G.fullq, i);
    }
    return NULL;
}
static void *th_writer(void *p){
    (void)p;
    for (;;){
        int i = q_pop(&G.fullq); if (i < 0) break;
        int s; int64_t off; reserve(&s, &off, G.chunk);
        if (pwrite_all(G.fd[s], G.buf[i], G.chunk, off)) break;
        add_bytes((double)G.chunk);
        q_push(&G.freeq, i);
    }
    return NULL;
}

/* --- decoupled + io_uring: ONE thread, QD writes in flight --- */
static int g_qd = 32, g_iowq = 0, g_nring = 1;
static void *th_ring(void *p){
    (void)p;
    struct io_uring ring;
    if (io_uring_queue_init(g_qd * 2, &ring, 0) < 0){ perror("io_uring_queue_init"); return NULL; }
    /* wekafs almost certainly cannot complete a write inline (no IOCB_NOWAIT path), so every
     * SQE is punted to the ring's io-wq BOUNDED pool. That pool, not QD, then caps real
     * concurrency -- the same thing that limited unlinkat (BVM_URING_WORKERS). Raise it and
     * see whether the ring was ever queue-limited or just worker-limited. */
    if (g_iowq){ unsigned v[2] = { (unsigned)g_iowq, (unsigned)g_iowq };
                 io_uring_register_iowq_max_workers(&ring, v); }
    int inflight = 0;
    for (;;){
        /* top up the window: block only when nothing is in flight, else just drain */
        while (inflight < g_qd){
            int i = inflight ? q_try(&G.fullq) : q_pop(&G.fullq);
            if (i < 0) break;
            int s; int64_t off; reserve(&s, &off, G.chunk);
            struct io_uring_sqe *sq = io_uring_get_sqe(&ring);
            if (!sq){ q_push(&G.fullq, i); break; }
            io_uring_prep_write(sq, G.fd[s], G.buf[i], G.chunk, off);
            io_uring_sqe_set_data64(sq, (uint64_t)i);
            inflight++;
        }
        if (!inflight) break;                     /* queue closed and drained */
        io_uring_submit(&ring);
        struct io_uring_cqe *c;
        if (io_uring_wait_cqe(&ring, &c) < 0) break;
        unsigned head; int seen = 0;
        io_uring_for_each_cqe(&ring, head, c){
            int i = (int)io_uring_cqe_get_data64(c);
            if (c->res > 0) add_bytes((double)c->res);
            else fprintf(stderr, "uring write: %s\n", strerror(-c->res));
            q_push(&G.freeq, i); inflight--; seen++;
        }
        io_uring_cq_advance(&ring, seen);
    }
    io_uring_queue_exit(&ring);
    return NULL;
}

static double run(const char *dir, const char *mode, int64_t total, size_t chunk, int nprod,
                  int nwr, int nbuf, int nsink, int direct, double *stall, double *idle){
    G.chunk = chunk; G.nsink = nsink; G.bytes = 0;
    G.frames_left = total / (int64_t)chunk;
    pthread_mutex_init(&G.cmu, NULL); pthread_mutex_init(&G.fmu, NULL); pthread_mutex_init(&G.bmu, NULL);
    memset(G.cursor, 0, sizeof G.cursor);
    char path[4096];
    for (int i = 0; i < nsink; i++){
        snprintf(path, sizeof path, "%s/dq.%d", dir, i);
        G.fd[i] = open(path, O_RDWR|O_CREAT|O_TRUNC|(direct?O_DIRECT:0), 0644);
        if (G.fd[i] < 0){ perror(path); exit(1); }
    }
    int coupled = !strcmp(mode, "coupled");
    int nb = coupled ? nprod : nbuf;
    if (nb > MAXBUF) nb = MAXBUF;
    for (int i = 0; i < nb; i++) G.buf[i] = aligned_alloc(4096, chunk);
    G.srcbuf = aligned_alloc(4096, chunk); memset(G.srcbuf, 0x5a, chunk);
    q_init(&G.freeq, nb + 1); q_init(&G.fullq, nb + 1);
    if (!coupled) for (int i = 0; i < nb; i++) q_push(&G.freeq, i);

    pthread_t pt[512], wt[64];
    double t0 = now();
    if (coupled){
        for (int i = 0; i < nprod; i++) pthread_create(&pt[i], NULL, th_coupled, (void*)(intptr_t)i);
        for (int i = 0; i < nprod; i++) pthread_join(pt[i], NULL);
    } else {
        int ring = !strcmp(mode, "uring");
        int nwt = ring ? g_nring : nwr;
        for (int i = 0; i < nwt; i++) pthread_create(&wt[i], NULL, ring ? th_ring : th_writer, NULL);
        for (int i = 0; i < nprod; i++) pthread_create(&pt[i], NULL, th_prod, NULL);
        for (int i = 0; i < nprod; i++) pthread_join(pt[i], NULL);
        q_close(&G.fullq);                        /* producers done -> writers drain and exit */
        for (int i = 0; i < nwt; i++) pthread_join(wt[i], NULL);
    }
    for (int i = 0; i < nsink; i++){ fsync(G.fd[i]); close(G.fd[i]); }
    double dt = now() - t0;
    *stall = G.freeq.wait; *idle = G.fullq.wait;
    for (int i = 0; i < nb; i++) free(G.buf[i]);
    free(G.srcbuf);
    q_close(&G.freeq);
    return G.bytes / dt / 1e9;
}

int main(int argc, char **argv){
    if (argc < 2){
        fprintf(stderr, "usage: %s <dir> [total_gb] [chunk_mb] [nprod] [nwriter] [nbuf] [prod_us] [qd]\n", argv[0]);
        return 1;
    }
    const char *dir = argv[1];
    int64_t total = (int64_t)((argc>2?atof(argv[2]):8.0) * 1e9);
    size_t chunk = (size_t)(argc>3?atoi(argv[3]):64) << 20;
    int nprod = argc>4?atoi(argv[4]):32;
    int nwr   = argc>5?atoi(argv[5]):8;
    int nbuf  = argc>6?atoi(argv[6]):48;
    G.prod_us = argc>7?atoi(argv[7]):0;
    g_qd      = argc>8?atoi(argv[8]):32;
    g_iowq    = argc>9?atoi(argv[9]):0;
    g_nring   = argc>10?atoi(argv[10]):1;
    int nsink = 16;

    printf("dwriteq: %.1f GB, %zu MB frames, %d producers, %d sinks, %d buffers, prod_us %d, qd %d\n",
           total/1e9, chunk>>20, nprod, nsink, nbuf, G.prod_us, g_qd);
    printf("  %-22s %10s   %12s %12s\n", "backend", "GB/s", "prod_stall", "wr_idle");
    struct { const char *m; int nwr; int direct; const char *lbl; } V[] = {
        { "coupled", 0,    0, "coupled buffered" },
        { "coupled", 0,    1, "coupled O_DIRECT" },
        { "threads", nwr,  1, "queue+threads DIRECT" },
        { "uring",   1,    1, "queue+uring DIRECT" },
        { "threads", nwr,  0, "queue+threads buf" },
    };
    const char *only = getenv("DWQ_ONLY");      /* run one backend, for targeted sweeps */
    for (unsigned i = 0; i < sizeof V/sizeof *V; i++){
        if (only && !strstr(V[i].lbl, only)) continue;
        double stall = 0, idle = 0;
        double g = run(dir, V[i].m, total, chunk, nprod, V[i].nwr, nbuf, nsink, V[i].direct, &stall, &idle);
        printf("  %-22s %7.2f GB/s   %9.1f s %11.1f s\n", V[i].lbl, g, stall, idle);
        fflush(stdout);
    }
    return 0;
}
