/* wekabw — clean WEKA write-bandwidth benchmark.
 *
 * Data generation must NOT be the bottleneck: one shared buffer is filled ONCE with a
 * xorshift128+ PRNG, then written repeatedly (each thread perturbs 64B per chunk so the
 * stream isn't literally identical — WEKA does no dedup/compression, but this removes
 * any doubt). Steady-state cost per chunk is therefore a pure memcpy-free write().
 *
 * Each thread writes its OWN file (no shared-offset contention). We report:
 *   write()   — throughput of the write syscalls alone (page-cache absorbed)
 *   durable   — including fsync of every file (what the storage actually ingested)
 * usage: wekabw <dir> <threads> <GB per thread> [chunk_MB] [direct] [shared]
 * `shared` = ALL threads pwrite into ONE file at reserved offsets — exactly quiver's sink
 * pattern (vs the default of one file per thread).
 * `direct` uses O_DIRECT: no page cache, so the number IS the storage path (the buffered
 * write() figure is just DRAM absorption and wildly overstates the fabric).
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>

static double now(void){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+t.tv_nsec*1e-9; }
static uint8_t *g_buf; static size_t g_chunk;
static const char *g_dir; static int64_t g_per_thread; static int g_direct;
static int g_shared, g_nfiles = 1; static int g_fds[64];
static int64_t g_curs[64]; static pthread_mutex_t g_cmus[64];
static double g_t_write_done[256];

typedef struct { int id; int64_t written; double t_write, t_fsync; } Th;

static void *worker(void *a) {
    Th *t = a;
    char path[4096];
    int fd;
    int slot = t->id % g_nfiles;
    if (g_shared) fd = g_fds[slot];               /* K sink files, reserved offsets (quiver sinks) */
    else {
        snprintf(path, sizeof path, "%s/bw_%d.dat", g_dir, t->id);
        fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | (g_direct ? O_DIRECT : 0), 0644);
        if (fd < 0) { fprintf(stderr, "open %s: %s\n", path, strerror(errno)); return NULL; }
    }
    uint8_t *mine = NULL;
    if (posix_memalign((void **)&mine, 4096, g_chunk)) return NULL;   /* O_DIRECT needs alignment */
    memcpy(mine, g_buf, g_chunk);                 /* private copy: perturb without racing */
    double t0 = now();
    int64_t done = 0; uint64_t seq = (uint64_t)t->id << 40;
    while (done < g_per_thread) {
        memcpy(mine, &seq, 8); seq++;             /* cheap uniqueness, O(1) per chunk */
        size_t want = g_chunk;
        if (g_per_thread - done < (int64_t)want) want = (size_t)(g_per_thread - done);
        size_t off = 0;
        if (g_shared) {                            /* reserve an offset like Sink.cursor does */
            pthread_mutex_lock(&g_cmus[slot]); int64_t at = g_curs[slot]; g_curs[slot] += (int64_t)want;
            pthread_mutex_unlock(&g_cmus[slot]);
            while (off < want) {
                ssize_t r = pwrite(fd, mine + off, want - off, at + (int64_t)off);
                if (r <= 0) { if (errno == EINTR) continue; fprintf(stderr, "pwrite: %s\n", strerror(errno)); goto out; }
                off += (size_t)r;
            }
        } else {
            while (off < want) {
                ssize_t r = write(fd, mine + off, want - off);
                if (r <= 0) { if (errno == EINTR) continue; fprintf(stderr, "write: %s\n", strerror(errno)); goto out; }
                off += (size_t)r;
            }
        }
        done += (int64_t)want;
    }
out:
    t->t_write = now() - t0;
    double f0 = now();
    if (!g_shared) fsync(fd);
    t->t_fsync = now() - f0;
    t->written = done;
    if (!g_shared) close(fd);
    free(mine);
    return NULL;
}

int main(int argc, char **argv) {
    if (argc < 4) { fprintf(stderr, "usage: %s <dir> <threads> <GB/thread> [chunk_MB]\n", argv[0]); return 2; }
    g_dir = argv[1];
    int T = atoi(argv[2]);
    double gb = atof(argv[3]);
    g_chunk = (size_t)(argc > 4 ? atoi(argv[4]) : 8) << 20;
    g_direct = argc > 5 && !strcmp(argv[5], "direct");
    g_shared = argc > 6 && !strncmp(argv[6], "shared", 6);
    if (g_shared) { g_nfiles = argc > 7 ? atoi(argv[7]) : 1; if (g_nfiles < 1) g_nfiles = 1; if (g_nfiles > 64) g_nfiles = 64; }
    g_per_thread = (int64_t)(gb * 1e9);
    if (T > 256) T = 256;

    /* fill the source buffer ONCE (xorshift128+) and time it, to prove it isn't the limit */
    g_buf = malloc(g_chunk);
    uint64_t s0 = 0x9E3779B97F4A7C15ULL, s1 = 0xBF58476D1CE4E5B9ULL;
    double gt = now();
    for (size_t i = 0; i + 8 <= g_chunk; i += 8) {
        uint64_t x = s0, y = s1; s0 = y;
        x ^= x << 23; x ^= x >> 17; x ^= y ^ (y >> 26); s1 = x;
        uint64_t v = s0 + s1; memcpy(g_buf + i, &v, 8);
    }
    double gen = now() - gt;
    /* generation is ONE-TIME: the buffer is reused ~(per_thread/chunk) times per thread */
    double amort = gen / ((double)g_per_thread * T / g_chunk);
    printf("prng fill: %.1f MB once in %.3fs (%.2f GB/s); reused %.0fx/thread -> amortized %.3f%% of runtime\n",
           g_chunk / 1e6, gen, g_chunk / gen / 1e9, (double)g_per_thread / g_chunk,
           100.0 * amort * T / ((double)g_per_thread * T / 1e9 / 5.0));

    if (g_shared) {
        for (int i = 0; i < g_nfiles; i++) {
            char sp[4096]; snprintf(sp, sizeof sp, "%s/sink_%d.dat", g_dir, i);
            g_fds[i] = open(sp, O_WRONLY | O_CREAT | O_TRUNC | (g_direct ? O_DIRECT : 0), 0644);
            if (g_fds[i] < 0) { perror("open sink"); return 1; }
            pthread_mutex_init(&g_cmus[i], 0);
        }
    }
    pthread_t th[256]; Th ts[256];
    double t0 = now();
    for (int i = 0; i < T; i++) { ts[i] = (Th){ .id = i }; pthread_create(&th[i], 0, worker, &ts[i]); }
    for (int i = 0; i < T; i++) pthread_join(th[i], 0);
    if (g_shared) for (int i = 0; i < g_nfiles; i++) { fsync(g_fds[i]); close(g_fds[i]); }
    double wall = now() - t0;

    int64_t total = 0; double maxw = 0, maxf = 0;
    for (int i = 0; i < T; i++) { total += ts[i].written; if (ts[i].t_write > maxw) maxw = ts[i].t_write;
                                  if (ts[i].t_fsync > maxf) maxf = ts[i].t_fsync; }
    char host[128]; gethostname(host, sizeof host);
    printf("NODE %s mode=%s%s threads=%d chunk=%zuMB total=%.1f GB | DURABLE %.2f GB/s"
           " | write-syscall %.2f GB/s%s | slowest write %.1fs fsync %.1fs\n",
           host, g_direct ? (g_shared ? "O_DIRECT/sinks" : "O_DIRECT") : (g_shared ? "buffered/sinks" : "buffered"),
           g_shared ? ({ static char nb[16]; snprintf(nb, sizeof nb, "=%d", g_nfiles); nb; }) : "",
           T, g_chunk >> 20, total / 1e9,
           total / wall / 1e9, total / maxw / 1e9,
           g_direct ? "" : " (page-cache, NOT the fabric)", maxw, maxf);
    return 0;
}
