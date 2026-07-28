/* fsops — filesystem operations quiver actually performs, beyond raw sequential write.
 *
 *   read   <dir> <threads> <GB/thr> <chunkMB> [direct] [shared]
 *          Read bandwidth. `shared` = all threads read ONE file (restore from a single
 *          nock) vs one file each. Caches are dropped per file with POSIX_FADV_DONTNEED
 *          so this measures storage, not page cache.
 *
 *   dirs   <dir> <threads> <files/thr> <fileKB> <ndirs>
 *          Create+write many small files spread over N directories. Parallel filesystems
 *          take an exclusive lock on the parent directory per create, so the SAME work
 *          across 1 vs many dirs can differ by an order of magnitude. This is why quiver
 *          shuffles work across directories.
 *
 *   meta   <dir> <threads> <files/thr> <ndirs>
 *          Metadata op rates measured separately: create, stat, unlink.
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
#include <sys/stat.h>

static double now(void){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+t.tv_nsec*1e-9; }
static const char *g_dir; static int g_threads, g_direct, g_shared, g_ndirs, g_nfiles;
static size_t g_chunk, g_fsize; static int64_t g_per_thread;
static double g_t_create, g_t_stat, g_t_unlink;
typedef struct { int id; int64_t bytes; double secs; double c, s, u; } T;

static void *rd(void *a) {
    T *t = a; char path[4096];
    if (g_shared) snprintf(path, sizeof path, "%s/sink_0.dat", g_dir);   /* written by fsbw ... shared */
    else snprintf(path, sizeof path, "%s/bw_%d.dat", g_dir, t->id);
    int fd = open(path, O_RDONLY | (g_direct ? O_DIRECT : 0));
    if (fd < 0) { fprintf(stderr, "open %s: %s\n", path, strerror(errno)); return NULL; }
    if (!g_direct) posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);   /* measure storage, not cache */
    uint8_t *buf; if (posix_memalign((void **)&buf, 4096, g_chunk)) return NULL;
    int64_t done = 0, off = g_shared ? (int64_t)t->id * g_per_thread : 0;
    double t0 = now();
    while (done < g_per_thread) {
        ssize_t r = pread(fd, buf, g_chunk, off + done);
        if (r <= 0) break;
        done += r;
    }
    t->secs = now() - t0; t->bytes = done;
    close(fd); free(buf);
    return NULL;
}

static void *mkfiles(void *a) {
    T *t = a; char p[4096];
    uint8_t *buf = calloc(1, g_fsize ? g_fsize : 1);
    double c0 = now();
    for (int i = 0; i < g_nfiles; i++) {
        int d = (t->id * g_nfiles + i) % g_ndirs;          /* spread across g_ndirs dirs */
        snprintf(p, sizeof p, "%s/d%04d/f_%d_%d", g_dir, d, t->id, i);
        int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) continue;
        if (g_fsize) { ssize_t w = write(fd, buf, g_fsize); (void)w; }
        close(fd);
    }
    t->c = now() - c0;
    double s0 = now();
    for (int i = 0; i < g_nfiles; i++) {
        int d = (t->id * g_nfiles + i) % g_ndirs;
        snprintf(p, sizeof p, "%s/d%04d/f_%d_%d", g_dir, d, t->id, i);
        struct stat st; if (lstat(p, &st)) { }
    }
    t->s = now() - s0;
    double u0 = now();
    for (int i = 0; i < g_nfiles; i++) {
        int d = (t->id * g_nfiles + i) % g_ndirs;
        snprintf(p, sizeof p, "%s/d%04d/f_%d_%d", g_dir, d, t->id, i);
        if (unlink(p)) { }
    }
    t->u = now() - u0;
    t->bytes = (int64_t)g_nfiles * g_fsize;
    free(buf);
    return NULL;
}

int main(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: fsops read|dirs|meta <dir> ...\n"); return 2; }
    const char *cmd = argv[1]; g_dir = argv[2];
    pthread_t th[512]; T ts[512];
    if (!strcmp(cmd, "read")) {
        g_threads = atoi(argv[3]); g_per_thread = (int64_t)(atof(argv[4]) * 1e9);
        g_chunk = (size_t)atoi(argv[5]) << 20;
        g_direct = argc > 6 && !strcmp(argv[6], "direct");
        g_shared = argc > 7 && !strcmp(argv[7], "shared");
        double t0 = now();
        for (int i = 0; i < g_threads; i++) { ts[i] = (T){ .id = i }; pthread_create(&th[i], 0, rd, &ts[i]); }
        int64_t tot = 0;
        for (int i = 0; i < g_threads; i++) { pthread_join(th[i], 0); tot += ts[i].bytes; }
        double wall = now() - t0;
        printf("READ mode=%s%s threads=%d chunk=%zuMB  %.2f GB/s  (%.1f GB in %.1fs)\n",
               g_direct ? "O_DIRECT" : "buffered", g_shared ? "/1file" : "", g_threads,
               g_chunk >> 20, tot / wall / 1e9, tot / 1e9, wall);
    } else if (!strcmp(cmd, "dirs") || !strcmp(cmd, "meta")) {
        g_threads = atoi(argv[3]); g_nfiles = atoi(argv[4]);
        g_fsize = strcmp(cmd, "meta") ? (size_t)atoi(argv[5]) * 1024 : 0;
        g_ndirs = atoi(argv[strcmp(cmd, "meta") ? 6 : 5]);
        char p[4096];
        for (int d = 0; d < g_ndirs; d++) { snprintf(p, sizeof p, "%s/d%04d", g_dir, d); mkdir(p, 0755); }
        double t0 = now();
        for (int i = 0; i < g_threads; i++) { ts[i] = (T){ .id = i }; pthread_create(&th[i], 0, mkfiles, &ts[i]); }
        double C = 0, S = 0, U = 0; int64_t tot = 0;
        for (int i = 0; i < g_threads; i++) { pthread_join(th[i], 0); tot += ts[i].bytes;
            if (ts[i].c > C) C = ts[i].c; if (ts[i].s > S) S = ts[i].s; if (ts[i].u > U) U = ts[i].u; }
        double wall = now() - t0;
        long n = (long)g_threads * g_nfiles;
        printf("%s threads=%d files=%ld dirs=%d size=%zuKB | create %.0f/s | stat %.0f/s | unlink %.0f/s"
               " | %.2f GB/s | wall %.1fs\n",
               strcmp(cmd, "meta") ? "DIRS" : "META", g_threads, n, g_ndirs, g_fsize >> 10,
               n / C, n / S, n / U, tot / wall / 1e9, wall);
    } else { fprintf(stderr, "unknown cmd %s\n", cmd); return 2; }
    return 0;
}
