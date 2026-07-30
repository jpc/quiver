// uread — how should bvm read MANY SMALL FILES through io_uring?
//
// Reads are 92% of worker busy time and opens average 9391 us with 76% over 500 us, so the
// read path, not the write path, is where a backup of a home tree actually spends its life.
// bvm already has ring_prefetch() for members <= 48 KB, but it is STRICTLY PHASED:
//
//     submit every open -> wait for ALL of them -> submit every read -> wait for ALL
//     -> close() each one SYNCHRONOUSLY
//
// so one slow open stalls every read in the batch, and the closes never reach the ring at all.
// The obvious fix is to link open->read->close per file and let the ring pipeline across files
// with no barriers. That is NOT possible with ordinary fds: the read has to name a file that is
// not open yet. It needs DIRECT DESCRIPTORS -- openat_direct assigns a slot in a registered
// table, so the linked read can reference the slot before the open completes. That is precisely
// why the existing code is phased, and it is the thing worth measuring.
//
// Modes, all reading the same real files:
//   sync      thread pool, open/read/close per file          (what the big-member path does)
//   phased    batched opens, then batched reads, sync close  (what ring_prefetch does today)
//   phclose   phased, but closes go on the ring too
//   linked    openat_direct -> read(FIXED_FILE) -> close_direct, IOSQE_IO_LINK, no barriers
//
// openat is never NOWAIT-capable, so the kernel always punts it to the ring's io-wq pool --
// which means the BOUNDED WORKER COUNT, not QD, can be the real limit. --iowq exposes that.
//
//   ./uread <list-file> <max_kb> [threads] [qd] [iowq] [mode]
// The list is real paths (bench/mklists.py); synthesising 600k files took longer than the
// benchmark and got killed the one time I tried it.
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

static double now(void){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+1e-9*t.tv_nsec; }

static char **g_paths; static long g_n; static size_t g_cap; static int g_qd, g_iowq;
static int g_direct_io;                      /* O_DIRECT on the source files */

typedef struct { long lo, hi; double bytes; long files, err; } Arg;

/* ---- baseline: one thread per worker, open/read/close, nothing in flight ---- */
static void *run_sync(void *p){
    Arg *a = p;
    uint8_t *buf = aligned_alloc(4096, g_cap);
    for (long i = a->lo; i < a->hi; i++){
        int fd = open(g_paths[i], O_RDONLY | (g_direct_io ? O_DIRECT : 0));
        if (fd < 0){ a->err++; continue; }
        ssize_t got = 0, r;
        while ((size_t)got < g_cap && (r = pread(fd, buf + got, g_cap - got, got)) > 0) got += r;
        posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
        close(fd);
        if (got > 0){ a->bytes += got; a->files++; }
    }
    free(buf);
    return NULL;
}

static int ring_setup(struct io_uring *ring, int nfiles){
    if (io_uring_queue_init(g_qd * 4, ring, 0) < 0) return -1;
    if (g_iowq){ unsigned v[2] = { (unsigned)g_iowq, (unsigned)g_iowq };
                 io_uring_register_iowq_max_workers(ring, v); }
    if (nfiles > 0){
        /* register_files_sparse needs kernel 5.19; this box is 5.15, where the equivalent is
         * registering an explicit table of -1 (empty slots). openat_direct itself landed in
         * 5.15, so linked chains are still reachable here -- just not via the newer helper. */
        int r = io_uring_register_files_sparse(ring, nfiles);
        if (r < 0){
            int *tbl = malloc(sizeof(int) * nfiles);
            for (int i = 0; i < nfiles; i++) tbl[i] = -1;
            r = io_uring_register_files(ring, tbl, nfiles);
            free(tbl);
            if (r < 0){
                static int once = 0;
                if (!__atomic_fetch_add(&once, 1, __ATOMIC_RELAXED))
                    fprintf(stderr, "  register_files: %s (direct descriptors unavailable)\n",
                            strerror(-r));
                return -2;
            }
        }
    }
    return 0;
}

/* ---- what ring_prefetch does today: open barrier, read barrier, sync (or ring) close ---- */
static void *run_phased(void *p){
    Arg *a = p;
    struct io_uring ring;
    extern int g_ring_close;
    if (ring_setup(&ring, 0) < 0){ a->err++; return NULL; }
    int qd = g_qd;
    uint8_t *buf = aligned_alloc(4096, (size_t)qd * g_cap);
    int *fds = calloc(qd, sizeof(int));
    for (long base = a->lo; base < a->hi; base += qd){
        int k = (int)((a->hi - base < qd) ? (a->hi - base) : qd);
        for (int i = 0; i < k; i++){
            struct io_uring_sqe *s = io_uring_get_sqe(&ring);
            io_uring_prep_openat(s, AT_FDCWD, g_paths[base+i], O_RDONLY | (g_direct_io?O_DIRECT:0), 0);
            io_uring_sqe_set_data64(s, i);
        }
        io_uring_submit(&ring);
        for (int i = 0; i < k; i++){                       /* BARRIER: all opens */
            struct io_uring_cqe *c; if (io_uring_wait_cqe(&ring, &c) < 0) break;
            fds[io_uring_cqe_get_data64(c)] = c->res; io_uring_cqe_seen(&ring, c);
        }
        int live = 0;
        for (int i = 0; i < k; i++){
            if (fds[i] < 0){ a->err++; continue; }
            struct io_uring_sqe *s = io_uring_get_sqe(&ring);
            io_uring_prep_read(s, fds[i], buf + (size_t)i * g_cap, (unsigned)g_cap, 0);
            io_uring_sqe_set_data64(s, i); live++;
        }
        if (live) io_uring_submit(&ring);
        for (int i = 0; i < live; i++){                    /* BARRIER: all reads */
            struct io_uring_cqe *c; if (io_uring_wait_cqe(&ring, &c) < 0) break;
            if (c->res > 0){ a->bytes += c->res; a->files++; }
            io_uring_cqe_seen(&ring, c);
        }
        if (g_ring_close){                                 /* closes on the ring */
            int nc = 0;
            for (int i = 0; i < k; i++) if (fds[i] >= 0){
                struct io_uring_sqe *s = io_uring_get_sqe(&ring);
                io_uring_prep_close(s, fds[i]); io_uring_sqe_set_data64(s, i); nc++;
            }
            if (nc) io_uring_submit(&ring);
            for (int i = 0; i < nc; i++){
                struct io_uring_cqe *c; if (io_uring_wait_cqe(&ring, &c) < 0) break;
                io_uring_cqe_seen(&ring, c);
            }
        } else {
            for (int i = 0; i < k; i++) if (fds[i] >= 0) close(fds[i]);
        }
    }
    free(buf); free(fds); io_uring_queue_exit(&ring);
    return NULL;
}
int g_ring_close = 0;

/* ---- linked chains: open_direct -> read(FIXED_FILE) -> close_direct, no barriers ----
 * The read names a registered SLOT, not an fd, so it can be submitted before the open has
 * completed. IOSQE_IO_LINK keeps the three ordered per file while different files overlap
 * freely. A failed open cancels its own read and close (ECANCELED) and nothing else. */
static void *run_linked(void *p){
    Arg *a = p;
    struct io_uring ring;
    int qd = g_qd;
    if (ring_setup(&ring, qd) < 0){ a->err++; return NULL; }
    uint8_t *buf = aligned_alloc(4096, (size_t)qd * g_cap);
    long i = a->lo;
    int inflight = 0;                                   /* chains, not SQEs */
    int *slot_busy = calloc(qd, sizeof(int));
    while (i < a->hi || inflight){
        while (i < a->hi && inflight < qd){
            int sl = -1;
            for (int x = 0; x < qd; x++) if (!slot_busy[x]){ sl = x; break; }
            if (sl < 0) break;
            struct io_uring_sqe *s = io_uring_get_sqe(&ring);
            if (!s) break;
            io_uring_prep_openat_direct(s, AT_FDCWD, g_paths[i],
                                        O_RDONLY | (g_direct_io?O_DIRECT:0), 0, (unsigned)sl);
            s->flags |= IOSQE_IO_LINK;
            io_uring_sqe_set_data64(s, (uint64_t)(sl * 4 + 0));

            s = io_uring_get_sqe(&ring);
            io_uring_prep_read(s, sl, buf + (size_t)sl * g_cap, (unsigned)g_cap, 0);
            s->flags |= IOSQE_FIXED_FILE | IOSQE_IO_LINK;
            io_uring_sqe_set_data64(s, (uint64_t)(sl * 4 + 1));

            s = io_uring_get_sqe(&ring);
            io_uring_prep_close_direct(s, (unsigned)sl);
            io_uring_sqe_set_data64(s, (uint64_t)(sl * 4 + 2));

            slot_busy[sl] = 3;                          /* three completions owed */
            inflight++; i++;
        }
        io_uring_submit(&ring);
        struct io_uring_cqe *c;
        if (io_uring_wait_cqe(&ring, &c) < 0) break;
        unsigned head; int seen = 0;
        io_uring_for_each_cqe(&ring, head, c){
            uint64_t d = io_uring_cqe_get_data64(c);
            int sl = (int)(d / 4), op = (int)(d % 4);
            if (op == 1 && c->res > 0){ a->bytes += c->res; a->files++; }
            else if (op == 0 && c->res < 0){
                a->err++;
                static int once = 0;
                if (!__atomic_fetch_add(&once, 1, __ATOMIC_RELAXED))
                    fprintf(stderr, "  linked open failed: %s\n", strerror(-c->res));
            } else if (op == 1 && c->res < 0){
                static int once1 = 0;
                if (!__atomic_fetch_add(&once1, 1, __ATOMIC_RELAXED))
                    fprintf(stderr, "  linked read failed: %s\n", strerror(-c->res));
            }
            if (--slot_busy[sl] == 0) inflight--;
            seen++;
        }
        io_uring_cq_advance(&ring, seen);
    }
    free(buf); free(slot_busy); io_uring_queue_exit(&ring);
    return NULL;
}

/* direct descriptors WITHOUT linking: isolates "openat_direct works on this kernel" from
 * "IOSQE_IO_LINK chains work". If this flies and linked does not, the chain is the problem. */
static void *run_dnolink(void *p){
    Arg *a = p;
    struct io_uring ring;
    int qd = g_qd;
    if (ring_setup(&ring, qd) < 0){ a->err++; return NULL; }
    uint8_t *buf = aligned_alloc(4096, (size_t)qd * g_cap);
    for (long base = a->lo; base < a->hi; base += qd){
        int k = (int)((a->hi - base < qd) ? (a->hi - base) : qd);
        for (int i = 0; i < k; i++){
            struct io_uring_sqe *s = io_uring_get_sqe(&ring);
            io_uring_prep_openat_direct(s, AT_FDCWD, g_paths[base+i],
                                        O_RDONLY | (g_direct_io?O_DIRECT:0), 0, (unsigned)i);
            io_uring_sqe_set_data64(s, i);
        }
        io_uring_submit(&ring);
        int ok = 0; int *good = calloc(k, sizeof(int));
        for (int i = 0; i < k; i++){
            struct io_uring_cqe *c; if (io_uring_wait_cqe(&ring, &c) < 0) break;
            int idx = (int)io_uring_cqe_get_data64(c);
            if (c->res >= 0){ good[idx] = 1; ok++; }
            else { a->err++;
                   static int once = 0;
                   if (!__atomic_fetch_add(&once, 1, __ATOMIC_RELAXED))
                       fprintf(stderr, "  openat_direct failed: %s\n", strerror(-c->res)); }
            io_uring_cqe_seen(&ring, c);
        }
        int live = 0;
        for (int i = 0; i < k; i++) if (good[i]){
            struct io_uring_sqe *s = io_uring_get_sqe(&ring);
            io_uring_prep_read(s, i, buf + (size_t)i * g_cap, (unsigned)g_cap, 0);
            s->flags |= IOSQE_FIXED_FILE;
            io_uring_sqe_set_data64(s, i); live++;
        }
        if (live) io_uring_submit(&ring);
        for (int i = 0; i < live; i++){
            struct io_uring_cqe *c; if (io_uring_wait_cqe(&ring, &c) < 0) break;
            if (c->res > 0){ a->bytes += c->res; a->files++; }
            io_uring_cqe_seen(&ring, c);
        }
        int nc = 0;
        for (int i = 0; i < k; i++) if (good[i]){
            struct io_uring_sqe *s = io_uring_get_sqe(&ring);
            io_uring_prep_close_direct(s, (unsigned)i); nc++;
        }
        if (nc) io_uring_submit(&ring);
        for (int i = 0; i < nc; i++){
            struct io_uring_cqe *c; if (io_uring_wait_cqe(&ring, &c) < 0) break;
            io_uring_cqe_seen(&ring, c);
        }
        free(good);
    }
    free(buf); io_uring_queue_exit(&ring);
    return NULL;
}

static char **load(const char *f, long *n){
    FILE *fp = fopen(f, "r"); if (!fp){ perror(f); exit(1); }
    long cap = 4096, k = 0; char **v = malloc(cap * sizeof *v); char line[4096];
    while (fgets(line, sizeof line, fp)){
        size_t L = strlen(line); while (L && (line[L-1]=='\n'||line[L-1]=='\r')) line[--L]=0;
        if (!L) continue;
        if (k == cap){ cap *= 2; v = realloc(v, cap * sizeof *v); }
        v[k++] = strdup(line);
    }
    fclose(fp); *n = k; return v;
}

/* Modes run over DISJOINT slices of the list. Sharing files let the first mode warm the page
 * cache for the second: uring once came back at 10.3 GB/s, above the fabric's 4.7 GB/s read
 * ceiling, which is only reachable from memory. */
static double sweep(const char *label, void *(*fn)(void *), int nthr, long lo, long hi){
    pthread_t th[512]; Arg a[512];
    double t0 = now();
    for (int i = 0; i < nthr; i++){
        a[i] = (Arg){ lo + (hi-lo)*i/nthr, lo + (hi-lo)*(i+1)/nthr, 0, 0, 0 };
        pthread_create(&th[i], NULL, fn, &a[i]);
    }
    double by = 0; long nf = 0, ne = 0;
    for (int i = 0; i < nthr; i++){ pthread_join(th[i], NULL); by += a[i].bytes; nf += a[i].files; ne += a[i].err; }
    double dt = now() - t0;
    printf("  %-9s %8ld files %7.2f GB  %6.3f GB/s  %9.0f files/s  %7.0f us/file  err %ld\n",
           label, nf, by/1e9, by/dt/1e9, nf/dt, nf ? dt*1e6*nthr/nf : 0, ne);
    fflush(stdout);
    return by/dt/1e9;
}

int main(int argc, char **argv){
    if (argc < 3){ fprintf(stderr,"usage: %s <list> <max_kb> [threads] [qd] [iowq] [mode]\n",argv[0]); return 1; }
    g_paths = load(argv[1], &g_n);
    g_cap = (size_t)atol(argv[2]) << 10;
    g_cap = (g_cap + 4095) & ~(size_t)4095;              /* O_DIRECT wants 4 KB multiples */
    int nthr = argc > 3 ? atoi(argv[3]) : 32;
    g_qd     = argc > 4 ? atoi(argv[4]) : 64;
    g_iowq   = argc > 5 ? atoi(argv[5]) : 0;
    const char *mode = argc > 6 ? argv[6] : "all";
    g_direct_io = getenv("UREAD_BUFFERED") == NULL;

    printf("uread: %s  %ld files, buf %zu KB, %d threads, qd %d, iowq %d %s\n",
           argv[1], g_n, g_cap >> 10, nthr, g_qd, g_iowq,
           g_direct_io ? "(O_DIRECT)" : "(buffered)");
    struct { const char *l; void *(*fn)(void*); int rc; } V[] = {
        { "sync",    run_sync,   0 },
        { "phased",  run_phased, 0 },   /* what ring_prefetch does today */
        { "phclose", run_phased, 1 },   /* + closes on the ring */
        { "dnolink", run_dnolink, 0 },  /* direct descriptors, phased (isolates linking) */
        { "linked",  run_linked, 0 },   /* open_direct -> read -> close_direct, chained */
    };
    int nv = (int)(sizeof V / sizeof *V);
    int run = 0;
    for (int i = 0; i < nv; i++) if (!strcmp(mode,"all") || !strcmp(mode, V[i].l)) run++;
    if (!run){ fprintf(stderr, "unknown mode %s\n", mode); return 1; }
    long per = g_n / run, at = 0;
    for (int i = 0; i < nv; i++){
        if (strcmp(mode,"all") && strcmp(mode, V[i].l)) continue;
        g_ring_close = V[i].rc;
        sweep(V[i].l, V[i].fn, nthr, at, at + per);       /* disjoint slice per mode */
        at += per;
    }
    return 0;
}
