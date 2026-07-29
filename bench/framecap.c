// framecap — how big must a frame be before zstd stops caring?
//
// quiver splits an oversized member into `frame_cap`-sized pieces, each compressed as its
// own independent frame, so the cap trades three things off at once:
//   smaller  -> less worker memory (a worker materializes one piece), more restore
//               parallelism, but each frame restarts zstd's match window and entropy tables
//   bigger   -> better ratio, up to the point where the window is the binding constraint
// zstd's window is set by the LEVEL (windowLog), so the knee moves with -L. This sweeps the
// whole (level x cap) grid on a real corpus and prints, per level, the smallest cap within
// a given tolerance of the asymptotic ratio.
//
//   usage: ./framecap <corpus-file> [--levels 1,3,6,...] [--caps 1,2,4,...MB] [--tol 0.5]
//                     [--threads N]
// Output is TSV on stdout (level, cap_mb, ratio, mb_per_s) plus a knee summary on stderr,
// so it drops straight into run_bench.py.
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
#define ZSTD_STATIC_LINKING_ONLY   /* ZSTD_getCParams: windowLog per level */
#include <zstd.h>

static double now(void){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+1e-9*t.tv_nsec; }

typedef struct { int level; size_t cap; size_t clen; double secs; } Cell;
typedef struct { const uint8_t *src; size_t n; Cell *cells; int ncell; int *next; pthread_mutex_t *mu; } Job;

static void *run(void *p){
    Job *j = p;
    ZSTD_CCtx *c = ZSTD_createCCtx();
    size_t obcap = 0; void *ob = NULL;
    for (;;){
        pthread_mutex_lock(j->mu);
        int i = (*j->next)++;
        pthread_mutex_unlock(j->mu);
        if (i >= j->ncell) break;
        Cell *cl = &j->cells[i];
        size_t need = ZSTD_compressBound(cl->cap);
        if (need > obcap){ obcap = need; ob = realloc(ob, obcap); }
        size_t tot = 0;
        double t0 = now();
        for (size_t o = 0; o < j->n; o += cl->cap){
            size_t k = j->n - o < cl->cap ? j->n - o : cl->cap;
            size_t r = ZSTD_compressCCtx(c, ob, obcap, j->src + o, k, cl->level);
            if (ZSTD_isError(r)){ tot = 0; break; }
            tot += r;
        }
        cl->secs = now() - t0;
        cl->clen = tot;
    }
    free(ob); ZSTD_freeCCtx(c);
    return NULL;
}

static int parse_list(char *s, long *out, int max){
    int n = 0;
    for (char *t = strtok(s, ","); t && n < max; t = strtok(NULL, ",")) out[n++] = atol(t);
    return n;
}

int main(int argc, char **argv){
    if (argc < 2){ fprintf(stderr, "usage: %s <corpus> [--levels L] [--caps MB] [--tol %%] [--threads N]\n", argv[0]); return 1; }
    const char *path = argv[1];
    long levels[32], caps[32];
    char dl[] = "1,3,6,9,12,15,19", dc[] = "1,2,4,8,16,32,64,128,256,1024,4096";
    int nl = parse_list(dl, levels, 32), nc = parse_list(dc, caps, 32);
    double tol = 0.5;
    int nthr = 0;
    for (int i = 2; i < argc - 1; i++){
        if (!strcmp(argv[i], "--levels")) nl = parse_list(argv[++i], levels, 32);
        else if (!strcmp(argv[i], "--caps")) nc = parse_list(argv[++i], caps, 32);
        else if (!strcmp(argv[i], "--tol")) tol = atof(argv[++i]);
        else if (!strcmp(argv[i], "--threads")) nthr = atoi(argv[++i]);
    }
    if (nthr <= 0) nthr = (int)sysconf(_SC_NPROCESSORS_ONLN) - 2;
    if (nthr < 1) nthr = 1;

    struct stat st;
    if (stat(path, &st)){ perror(path); return 1; }
    size_t n = st.st_size;
    int fd = open(path, O_RDONLY);
    uint8_t *src = malloc(n);
    for (size_t o = 0; o < n; ){ ssize_t r = pread(fd, src + o, n - o, o); if (r <= 0) break; o += r; }
    close(fd);

    int ncell = nl * nc;
    Cell *cells = calloc(ncell, sizeof *cells);
    for (int a = 0; a < nl; a++)
        for (int b = 0; b < nc; b++)
            cells[a * nc + b] = (Cell){ (int)levels[a], (size_t)caps[b] << 20, 0, 0 };

    // The grid is embarrassingly parallel and wildly uneven (level 19 is ~100x level 1), so
    // hand out cells from a shared cursor rather than striping.
    int next = 0; pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
    Job j = { src, n, cells, ncell, &next, &mu };
    pthread_t *th = calloc(nthr, sizeof *th);
    double t0 = now();
    for (int i = 0; i < nthr; i++) pthread_create(&th[i], NULL, run, &j);
    for (int i = 0; i < nthr; i++) pthread_join(th[i], NULL);
    fprintf(stderr, "# %s: %.1f MB, %d levels x %d caps on %d threads in %.1f s\n",
            path, n / 1e6, nl, nc, nthr, now() - t0);

    printf("level\tcap_mb\tratio\tmb_per_s\twindow_mb\n");
    for (int a = 0; a < nl; a++){
        ZSTD_compressionParameters cp = ZSTD_getCParams((int)levels[a], n, 0);
        double win = (double)(1ull << cp.windowLog) / 1e6;
        double best = 0;
        for (int b = 0; b < nc; b++){
            Cell *c = &cells[a * nc + b];
            double r = c->clen ? (double)n / c->clen : 0;
            if (r > best) best = r;
            printf("%d\t%zu\t%.4f\t%.1f\t%.2f\n", c->level, c->cap >> 20, r,
                   c->secs > 0 ? n / c->secs / 1e6 : 0, win);
        }
        size_t knee = 0; double kr = 0;
        for (int b = 0; b < nc; b++){                       // smallest cap within tol of best
            Cell *c = &cells[a * nc + b];
            double r = c->clen ? (double)n / c->clen : 0;
            if (r >= best * (1.0 - tol / 100.0)){ knee = c->cap >> 20; kr = r; break; }
        }
        fprintf(stderr, "  level %-3d window %5.1f MB   best %6.3fx   knee (<= %.1f%% off): "
                        "%zu MB at %.3fx\n", (int)levels[a], win, best, tol, knee, kr);
    }
    return 0;
}
