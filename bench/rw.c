// rw — what does the fabric give when you read and write AT THE SAME TIME?
//
// Every other ceiling in this suite is single-direction: fsbw writes, fsops read reads. A
// backup does both at once — it read 1.14 GB/s and wrote 0.97 GB/s per node concurrently —
// so neither number tells you whether 2.1 GB/s combined is near the limit or 4x off it.
// This measures the same threads in three modes over the same files, so the only variable
// is whether the other direction is running.
//
//   ./rw <dir> [threads] [gb_per_thread] [chunk_mb] [sinks]
// Both directions use O_DIRECT, matching fsbw/fsops "direct" mode, so the three numbers are
// comparable to each other and to the rest of the suite.
//
// read  : every thread preads its own source file, discards the bytes
// write : every thread pwrites to a sink (round-robin over `sinks` files, since one inode
//         serializes writers on a parallel filesystem)
// rw    : each thread alternates — read a chunk, write it — which is the backup's shape
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <stdint.h>
#include <sys/stat.h>

static double now(void){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+1e-9*t.tv_nsec; }

enum { M_READ, M_WRITE, M_RW };
typedef struct {
    const char *dir; int mode, id, nthr, nsink; size_t chunk, bytes;
    int *sfd;                       /* shared sink fds */
    double rd, wr;                  /* bytes moved, per direction */
    off_t *soff; pthread_mutex_t *smu;
} Arg;

static void *run(void *p){
    Arg *a = p;
    uint8_t *buf = aligned_alloc(4096, a->chunk);
    memset(buf, 0x5a, a->chunk);
    char sp[512];
    snprintf(sp, sizeof sp, "%s/src.%d", a->dir, a->id);
    int sd = -1;
    /* O_DIRECT: the sources were written moments ago by this same process, so a buffered
     * read returns page cache and reports tens of GB/s that the fabric never carried. */
    if (a->mode != M_WRITE) { sd = open(sp, O_RDONLY | O_DIRECT); if (sd < 0) { free(buf); return NULL; } }
    size_t left = a->bytes; off_t roff = 0;
    while (left) {
        size_t k = left < a->chunk ? left : a->chunk;
        if (a->mode != M_WRITE) {
            ssize_t r = pread(sd, buf, k, roff);
            if (r <= 0) { roff = 0; continue; }
            roff += r; a->rd += r; k = (size_t)r;
        }
        if (a->mode != M_READ) {
            int s = (a->id + (int)(a->wr / a->chunk)) % a->nsink;   /* spread across sinks */
            pthread_mutex_lock(&a->smu[s]);
            off_t o = a->soff[s]; a->soff[s] += k;
            pthread_mutex_unlock(&a->smu[s]);
            ssize_t w = pwrite(a->sfd[s], buf, k, o);
            if (w > 0) a->wr += w;
        }
        left -= k;
    }
    if (sd >= 0) close(sd);
    free(buf);
    return NULL;
}

int main(int argc, char **argv){
    if (argc < 2){ fprintf(stderr, "usage: %s <dir> [threads] [gb/thread] [chunk_mb] [sinks]\n", argv[0]); return 1; }
    const char *dir = argv[1];
    int nthr  = argc > 2 ? atoi(argv[2]) : 32;
    double gbt = argc > 3 ? atof(argv[3]) : 2.0;
    size_t chunk = (size_t)(argc > 4 ? atoi(argv[4]) : 8) << 20;
    int nsink = argc > 5 ? atoi(argv[5]) : 16;
    size_t bytes = (size_t)(gbt * 1e9);
    mkdir(dir, 0755);

    // sources: one per thread, so reads never contend on an inode (the favourable case)
    uint8_t *fill = malloc(chunk);
    for (size_t i = 0; i < chunk; i++) fill[i] = (uint8_t)(i * 2654435761u >> 24);
    char p[512];
    for (int i = 0; i < nthr; i++){
        snprintf(p, sizeof p, "%s/src.%d", dir, i);
        struct stat st;
        if (stat(p, &st) == 0 && (size_t)st.st_size >= bytes) continue;
        int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        for (size_t o = 0; o < bytes; o += chunk){
            size_t k = bytes - o < chunk ? bytes - o : chunk;
            if (pwrite(fd, fill, k, o) <= 0) break;
        }
        fsync(fd); close(fd);
    }
    free(fill);

    printf("rw: %d threads x %.1f GB, %zu MB chunks, %d sinks, dir=%s\n",
           nthr, gbt, chunk >> 20, nsink, dir);
    const char *names[] = {"read", "write", "rw"};
    for (int m = 0; m < 3; m++){
        int *sfd = calloc(nsink, sizeof(int));
        off_t *soff = calloc(nsink, sizeof(off_t));
        pthread_mutex_t *smu = calloc(nsink, sizeof(pthread_mutex_t));
        for (int i = 0; i < nsink; i++){
            snprintf(p, sizeof p, "%s/sink.%d", dir, i);
            sfd[i] = open(p, O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, 0644);
            pthread_mutex_init(&smu[i], NULL);
        }
        pthread_t th[512]; Arg a[512];
        double t0 = now();
        for (int i = 0; i < nthr; i++){
            a[i] = (Arg){dir, m, i, nthr, nsink, chunk, bytes, sfd, 0, 0, soff, smu};
            pthread_create(&th[i], NULL, run, &a[i]);
        }
        double rd = 0, wr = 0;
        for (int i = 0; i < nthr; i++){ pthread_join(th[i], NULL); rd += a[i].rd; wr += a[i].wr; }
        for (int i = 0; i < nsink; i++){ fsync(sfd[i]); close(sfd[i]); }
        double dt = now() - t0;
        printf("  %-6s read %6.2f GB/s   write %6.2f GB/s   combined %6.2f GB/s   (%.1f s)\n",
               names[m], rd/dt/1e9, wr/dt/1e9, (rd+wr)/dt/1e9, dt);
        fflush(stdout);
        free(sfd); free(soff); free(smu);
    }
    for (int i = 0; i < nsink; i++){ snprintf(p, sizeof p, "%s/sink.%d", dir, i); unlink(p); }
    return 0;
}
