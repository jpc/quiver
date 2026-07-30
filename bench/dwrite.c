// dwrite — why is bvm's O_DIRECT sink path 2.3x SLOWER when the microbenchmark says
// O_DIRECT should be 30% FASTER?
//
// bench/rw measures, on one node, 16 sinks:      read 4.31  write 4.73  concurrent 5.60 GB/s
// bvm with BVM_SINK_DIRECT on the same tree:     191.4 s vs 84.5 s buffered  (2.3x slower)
//
// Same flag, opposite outcome, so the difference is in HOW bvm writes, not in O_DIRECT. The
// one axis where they demonstrably disagree is the size of a single write: rw (and the older
// fsbw run quoted in bvm.c) issue 8 MB pwrites, while bvm issues ONE pwrite per frame -- at
// frame_cap 64 MB that is a single ~52 MB direct write. Everything else about the two paths
// is the same syscall on the same fd.
//
// So sweep the per-pwrite chunk size against a fixed byte count and see where it falls over.
// Small and fast on purpose: 8 GB per config, ~2 s each, the whole sweep in well under a
// minute, so this can be iterated on instead of waiting 3 minutes for a tree walk.
//
//   ./dwrite <dir> [total_gb] [nsink] [threads_per_sink]
//
// Reports GB/s per chunk size for O_DIRECT and buffered, and separately times the
// sync_file_range+fadvise pacing bvm performs after every frame -- under O_DIRECT there are
// no dirty pages for it to write back, so if it costs anything it is pure waste.
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

static double now(void){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+1e-9*t.tv_nsec; }

typedef struct {
    int fd; size_t chunk; int64_t bytes; int direct, pace;
    uint8_t *buf; int64_t off0;
    double bytes_done, ns_pace;
} Arg;

static void *run(void *p){
    Arg *a = p;
    int64_t off = a->off0, end = a->off0 + a->bytes;
    int64_t flushed = off, unflushed = 0;
    while (off < end){
        size_t n = (size_t)((end - off < (int64_t)a->chunk) ? (end - off) : (int64_t)a->chunk);
        if (a->direct) n = (n + 4095) & ~(size_t)4095;        /* O_DIRECT: 4 KB multiples */
        size_t o = 0;
        while (o < n){
            ssize_t r = pwrite(a->fd, a->buf + o, n - o, off + o);
            if (r <= 0) { fprintf(stderr, "pwrite %zu@%ld: %s\n", n-o, off+o, strerror(errno)); return NULL; }
            o += (size_t)r;
        }
        off += (int64_t)n; a->bytes_done += (double)n; unflushed += (int64_t)n;
        /* exactly what bvm does after every frame, including under O_DIRECT */
        if (a->pace && unflushed >= (64LL << 20)){
            double t = now();
            sync_file_range(a->fd, flushed, off - flushed, SYNC_FILE_RANGE_WRITE);
            posix_fadvise(a->fd, flushed, off - flushed, POSIX_FADV_DONTNEED);
            a->ns_pace += now() - t;
            flushed = off; unflushed = 0;
        }
    }
    return NULL;
}

/* one config: nsink files, nthr threads each, `total` bytes total. fsync is INSIDE the timed
 * region so the buffered numbers are durable and comparable to the direct ones. */
static double once(const char *dir, int64_t total, int nsink, int nthr, size_t chunk,
                   int direct, int pace, double *pace_s){
    int fd[64]; char path[4096];
    for (int i = 0; i < nsink; i++){
        snprintf(path, sizeof path, "%s/dw.%d", dir, i);
        fd[i] = open(path, O_RDWR|O_CREAT|O_TRUNC|(direct?O_DIRECT:0), 0644);
        if (fd[i] < 0) { perror(path); exit(1); }
    }
    int nt = nsink * nthr;
    pthread_t th[512]; Arg a[512];
    uint8_t *buf = aligned_alloc(4096, chunk);
    memset(buf, 0x5a, chunk);
    int64_t per = total / nt;
    double t0 = now();
    for (int i = 0; i < nt; i++){
        a[i] = (Arg){ .fd = fd[i % nsink], .chunk = chunk, .bytes = per, .direct = direct,
                      .pace = pace, .buf = buf, .off0 = per * (int64_t)(i / nsink) };
        pthread_create(&th[i], NULL, run, &a[i]);
    }
    double by = 0, ps = 0;
    for (int i = 0; i < nt; i++){ pthread_join(th[i], NULL); by += a[i].bytes_done; ps += a[i].ns_pace; }
    for (int i = 0; i < nsink; i++){ fsync(fd[i]); close(fd[i]); }
    double dt = now() - t0;
    free(buf);
    if (pace_s) *pace_s = ps;
    return by / dt / 1e9;
}

int main(int argc, char **argv){
    if (argc < 2){ fprintf(stderr, "usage: %s <dir> [total_gb] [nsink] [thr_per_sink]\n", argv[0]); return 1; }
    const char *dir = argv[1];
    int64_t total = (int64_t)((argc>2?atof(argv[2]):8.0) * 1e9);
    int nsink = argc>3 ? atoi(argv[3]) : 16;
    int nthr  = argc>4 ? atoi(argv[4]) : 1;
    static const size_t KB = 1024, MB = 1024*1024;
    size_t chunks[] = { 1*MB, 2*MB, 4*MB, 8*MB, 16*MB, 32*MB, 64*MB, 128*MB };
    (void)KB;
    printf("dwrite: %s  %.1f GB total, %d sinks x %d threads\n", dir, total/1e9, nsink, nthr);
    printf("  %-9s %10s %10s %12s\n", "chunk", "O_DIRECT", "buffered", "pace cost");
    for (unsigned i = 0; i < sizeof chunks/sizeof *chunks; i++){
        double ps = 0;
        double d = once(dir, total, nsink, nthr, chunks[i], 1, 0, NULL);
        double b = once(dir, total, nsink, nthr, chunks[i], 0, 1, &ps);
        double dp = once(dir, total, nsink, nthr, chunks[i], 1, 1, &ps);
        printf("  %5zu MB  %7.2f GB/s %7.2f GB/s   direct+pace %6.2f GB/s\n",
               chunks[i]/MB, d, b, dp);
        fflush(stdout);
    }
    return 0;
}
