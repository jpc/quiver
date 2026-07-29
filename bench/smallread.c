// smallread — what does reading the same bytes as MANY SMALL FILES cost?
//
// The bulk of a backup is read in 16 MB pieces and the read-size sweep is flat from 1 MB to
// 64 MB, so block size is not a lever. But 98% of a home tree's read REQUESTS are small
// files: 1.83M members, median 31 KB, mean 121 KB. This reads a fixed number of bytes three
// ways -- as 8 KB files, 32 KB files, 128 KB files -- and against one large file, so the
// per-file cost is isolated from the per-byte cost.
//
// Each size is measured twice:
//   sync   one thread per worker: open, read, close, next file (what bvm's pack loop does)
//   uring  the same work through io_uring, batched QD deep: opens submitted together, then
//          reads, then closes -- so a worker has many requests outstanding instead of one
//
// If sync and uring are equal, per-file latency is already hidden by thread count and
// io_uring buys nothing. If uring wins, small files want a different execution path.
//
//   ./smallread <list-file> <max_file_kb> [threads] [qd]
// The list is REAL files, filtered out of a store's footer by size -- so the layout,
// directory spread and placement are the ones a backup actually meets, and no time
// goes into synthesising 600k files first.
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
#include <sys/stat.h>
#include <liburing.h>

static const char *g_buffered;
static double now(void){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+1e-9*t.tv_nsec; }

typedef struct {
    const char *dir; int id, nthr, qd, uring; size_t fsz;
    long lo, hi;                       /* this thread's slice of the path list */
    double bytes; long files;
    char **paths;                      /* REAL files, listed from a store's footer */
} Arg;

static void *run_sync(void *p){
    Arg *a = p;
    /* O_DIRECT throughout: these are real files a backup just read, so the page cache is
     * warm and buffered numbers came back ABOVE the fabric's 4.7 GB/s ceiling -- 10.5 GB/s,
     * i.e. memory, not storage. Direct I/O needs an aligned buffer and 4 KB-multiple lengths;
     * a short read past EOF is fine and is how we learn the real size. */
    size_t cap = (a->fsz + 4095) & ~(size_t)4095;
    uint8_t *buf = aligned_alloc(4096, cap);
    for (long i = a->lo; i < a->hi; i++){
        int fd = open(a->paths[i], O_RDONLY | (a->dir ? 0 : O_DIRECT));
        if (fd < 0) continue;
        ssize_t got = 0, r;
        while ((size_t)got < cap && (r = pread(fd, buf + got, cap - got, got)) > 0) got += r;
        /* a->fsz is the buffer cap; real files vary, a short read just means we are done */
        posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);   /* do not warm the cache for the
                                                          * other mode, or for ourselves */
        close(fd);
        a->bytes += got; a->files++;
    }
    free(buf);
    return NULL;
}

static void *run_uring(void *p){
    Arg *a = p;
    struct io_uring ring;
    if (io_uring_queue_init(a->qd * 2, &ring, 0) < 0) return NULL;
    int qd = a->qd;
    size_t cap = (a->fsz + 4095) & ~(size_t)4095;
    uint8_t *buf = aligned_alloc(4096, (size_t)qd * cap);
    int *fds = calloc(qd, sizeof(int));
    for (long base = a->lo; base < a->hi; base += qd){
        int n = (int)((a->hi - base < qd) ? (a->hi - base) : qd);
        /* phase 1: submit n opens together */
        for (int i = 0; i < n; i++){
            struct io_uring_sqe *s = io_uring_get_sqe(&ring);
            io_uring_prep_openat(s, AT_FDCWD, a->paths[base + i], O_RDONLY | (a->dir ? 0 : O_DIRECT), 0);
            io_uring_sqe_set_data64(s, i);
        }
        io_uring_submit(&ring);
        for (int i = 0; i < n; i++){
            struct io_uring_cqe *c;
            if (io_uring_wait_cqe(&ring, &c) < 0) break;
            fds[io_uring_cqe_get_data64(c)] = c->res;
            io_uring_cqe_seen(&ring, c);
        }
        /* phase 2: submit n reads together */
        int live = 0;
        for (int i = 0; i < n; i++){
            if (fds[i] < 0) continue;
            struct io_uring_sqe *s = io_uring_get_sqe(&ring);
            io_uring_prep_read(s, fds[i], buf + (size_t)i * cap, cap, 0);
            io_uring_sqe_set_data64(s, i);
            live++;
        }
        if (live) io_uring_submit(&ring);
        for (int i = 0; i < live; i++){
            struct io_uring_cqe *c;
            if (io_uring_wait_cqe(&ring, &c) < 0) break;
            if (c->res > 0) { a->bytes += c->res; a->files++; }
            io_uring_cqe_seen(&ring, c);
        }
        /* phase 3: close them */
        for (int i = 0; i < n; i++) if (fds[i] >= 0) {
            posix_fadvise(fds[i], 0, 0, POSIX_FADV_DONTNEED);
            close(fds[i]);
        }
    }
    free(buf); free(fds);
    io_uring_queue_exit(&ring);
    return NULL;
}

static char **load(const char *listfile, long *n){
    FILE *f = fopen(listfile, "r");
    if (!f) { perror(listfile); exit(1); }
    long cap = 1024, k = 0; char **v = malloc(cap * sizeof(char *)); char line[4096];
    while (fgets(line, sizeof line, f)){
        size_t L = strlen(line); while (L && (line[L-1]=='\n' || line[L-1]=='\r')) line[--L] = 0;
        if (!L) continue;
        if (k == cap) { cap *= 2; v = realloc(v, cap * sizeof(char *)); }
        v[k++] = strdup(line);
    }
    fclose(f); *n = k; return v;
}

static double sweep(char **paths, size_t fsz, long nfile, int nthr, int qd, int uring){
    pthread_t th[512]; Arg a[512];
    double t0 = now();
    for (int i = 0; i < nthr; i++){
        a[i] = (Arg){g_buffered ? "b" : NULL, i, nthr, qd, uring, fsz,
                     nfile * i / nthr, nfile * (i + 1) / nthr, 0, 0, paths};
        pthread_create(&th[i], NULL, uring ? run_uring : run_sync, &a[i]);
    }
    double by = 0; long nf = 0;
    for (int i = 0; i < nthr; i++){ pthread_join(th[i], NULL); by += a[i].bytes; nf += a[i].files; }
    double dt = now() - t0;
    printf("    %-6s %7ld files, %7.2f GB   %6.3f GB/s   %9.0f files/s   (%.1f s)\n",
           uring ? "uring" : "sync", nf, by / 1e9, by / dt / 1e9, nf / dt, dt);
    fflush(stdout);
    return by / dt / 1e9;
}

int main(int argc, char **argv){
    if (argc < 3){
        fprintf(stderr, "usage: %s <list-file> <max_file_kb> [threads] [qd]\n", argv[0]);
        return 1;
    }
    long n = 0;
    char **paths = load(argv[1], &n);
    size_t fsz = (size_t)atol(argv[2]) << 10;      /* read buffer cap per file */
    int nthr = argc > 3 ? atoi(argv[3]) : 32;
    int qd = argc > 4 ? atoi(argv[4]) : 64;
    g_buffered = getenv("SMALLREAD_BUFFERED");
    /* DISJOINT HALVES. Running both modes over the same files let the first warm the page
     * cache for the second: uring came out at 10.3 GB/s, above the fabric's measured 4.7 GB/s
     * read ceiling, which is only possible from memory. Each mode now gets its own half. */
    printf("  %s: %ld files, buffer %zu KB, %d threads, qd %d  %s\n",
           argv[1], n, fsz >> 10, nthr, qd, g_buffered ? " (buffered)" : " (O_DIRECT)");
    sweep(paths, fsz, n, nthr, qd, 0);
    sweep(paths, fsz, n, nthr, qd, 1);
    return 0;
}
