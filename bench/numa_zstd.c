/* numa_zstd — fully-loaded compression throughput vs NUMA binding.
 *
 * quiver runs one zstd context per worker across every core of a 2-socket node while the
 * filesystem client spin-polls on its own cores. Under full load, cross-socket memory
 * traffic can cost more than the extra cores buy. This measures the whole node compressing
 * flat out under four placements:
 *   none        threads and memory wherever the scheduler puts them (quiver's default)
 *   local       each thread pinned to a core, memory allocated on THAT core's NUMA node
 *   interleave  pinned cores, pages interleaved across both nodes
 *   cpuonly     pinned cores, DEFAULT allocation policy (isolates pinning from mempolicy)
 *   remote      pinned cores, memory deliberately on the FAR node (worst case, the control)
 * Run it under `numactl` for the process-wide variants too; this binary does the per-thread
 * placement itself so one run compares all four.
 *
 * usage: numa_zstd <threads> <MB per thread> [level] [mode]
 *        mode: all (default) | none | local | interleave | remote
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <time.h>
#include <zstd.h>
#ifdef HAVE_NUMA
#include <numa.h>
#include <numaif.h>
#endif

static double now(void){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+t.tv_nsec*1e-9; }
static int g_level = 6, g_mode = 0, g_nnodes = 2, g_ncpu;
static size_t g_bytes;
typedef struct { int id; double secs, alloc_secs; size_t in, out; } T;

/* cpu -> numa node, read once from sysfs (no libnuma dependency required) */
static int cpu_node(int cpu) {
    char p[256]; for (int n = 0; n < 8; n++) {
        snprintf(p, sizeof p, "/sys/devices/system/node/node%d/cpu%d", n, cpu);
        if (access(p, F_OK) == 0) return n;
    }
    return 0;
}
static int *g_cpus_of_node[8], g_ncpus_of_node[8];
/* Only ever pin to CPUs we are ALLOWED to use: under `taskset`/cgroup restriction this
 * lets us pin *around* a storage client's spin-poll cores instead of onto them. */
static cpu_set_t g_allowed;

static void *worker(void *a) {
    T *t = a;
    int node = 0, cpu = -1;
    if (g_mode != 0) {                                  /* pin to a core, round-robin over nodes */
        int nd = t->id % g_nnodes;
        cpu = g_cpus_of_node[nd][(t->id / g_nnodes) % g_ncpus_of_node[nd]];
        cpu_set_t set; CPU_ZERO(&set); CPU_SET(cpu, &set);
        pthread_setaffinity_np(pthread_self(), sizeof set, &set);
        node = nd;
    }
    /* allocate + FIRST-TOUCH the buffers on the desired node (first touch decides placement) */
#ifdef HAVE_NUMA
    if (g_mode == 2) numa_set_interleave_mask(numa_all_nodes_ptr);
    else if (g_mode == 1) numa_set_preferred(node);
    else if (g_mode == 3) numa_set_preferred((node + 1) % g_nnodes);   /* deliberately remote */
    /* mode 4 (cpuonly): pin the CPU but DO NOT touch mempolicy — an explicit mempolicy can
     * suppress transparent huge pages, which costs far more than NUMA locality buys. */
#endif
    double a0 = now();
    uint8_t *src = malloc(g_bytes);
    uint64_t x = 0x9E3779B97F4A7C15ULL ^ (uint64_t)t->id;
    for (size_t i = 0; i + 8 <= g_bytes; i += 8) {      /* semi-compressible: realistic ratio */
        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        uint64_t v = (i % 512 < 256) ? (x & 0x0f0f0f0f0f0f0f0fULL) : x;
        memcpy(src + i, &v, 8);
    }
    size_t bound = ZSTD_compressBound(g_bytes);
    uint8_t *dst = malloc(bound);
    memset(dst, 0, bound);                              /* first-touch dst too */
    t->alloc_secs = now() - a0;                 /* alloc + first-touch: NUMA policy hits HERE */
    ZSTD_CCtx *c = ZSTD_createCCtx();
    double t0 = now();
    size_t cl = ZSTD_compressCCtx(c, dst, bound, src, g_bytes, g_level);
    t->secs = now() - t0;
    t->in = g_bytes; t->out = ZSTD_isError(cl) ? 0 : cl;
    ZSTD_freeCCtx(c); free(src); free(dst);
    return NULL;
}

static void run(const char *label, int T_, int mode) {
    g_mode = mode;
    pthread_t th[512]; T ts[512];
    double t0 = now();
    for (int i = 0; i < T_; i++) { ts[i] = (T){ .id = i }; pthread_create(&th[i], 0, worker, &ts[i]); }
    for (int i = 0; i < T_; i++) pthread_join(th[i], 0);
    double wall = now() - t0;
    size_t in = 0, out = 0; double slow = 0, slowa = 0, sumc = 0;
    for (int i = 0; i < T_; i++) { in += ts[i].in; out += ts[i].out; sumc += ts[i].secs;
        if (ts[i].secs > slow) slow = ts[i].secs;
        if (ts[i].alloc_secs > slowa) slowa = ts[i].alloc_secs; }
    /* COMPRESS-ONLY throughput = sum(bytes)/sum(per-thread compress time) * threads, i.e. the
     * rate the cores actually achieved, with buffer setup excluded. `wall` includes setup. */
    printf("  %-11s threads=%d  compress %.2f GB/s | end-to-end %.2f GB/s"
           " (setup %.1fs of %.1fs wall; slowest compress %.2fs, ratio %.3f)\n",
           label, T_, in / (sumc / T_) / 1e9 / T_ * T_, in / wall / 1e9, slowa, wall, slow,
           (double)out / in);
    fflush(stdout);
}

int main(int argc, char **argv) {
    int T_ = argc > 1 ? atoi(argv[1]) : 0;      /* 0 => exactly one thread per ALLOWED cpu */
    double mb = argc > 2 ? atof(argv[2]) : 64;
    g_level = argc > 3 ? atoi(argv[3]) : 6;
    const char *only = argc > 4 ? argv[4] : "all";
    g_bytes = (size_t)(mb * (1 << 20));
    g_ncpu = (int)sysconf(_SC_NPROCESSORS_ONLN);
    for (int n = 0; n < 8; n++) g_cpus_of_node[n] = calloc(g_ncpu, sizeof(int));
    int maxnode = 0;
    CPU_ZERO(&g_allowed);
    if (sched_getaffinity(0, sizeof g_allowed, &g_allowed)) for (int c = 0; c < g_ncpu; c++) CPU_SET(c, &g_allowed);
    int nallowed = 0;
    for (int c = 0; c < g_ncpu; c++) { if (!CPU_ISSET(c, &g_allowed)) continue; nallowed++;
        int n = cpu_node(c); if (n > maxnode) maxnode = n;
        g_cpus_of_node[n][g_ncpus_of_node[n]++] = c; }
    g_nnodes = maxnode + 1;
    if (T_ <= 0) T_ = nallowed;                 /* 1:1 — the ONLY fair pinned/unpinned comparison */
    printf("node: %d CPUs (%d allowed) across %d NUMA node(s); zstd level %d; %d threads x %.0f MB\n",
           g_ncpu, nallowed, g_nnodes, g_level, T_, mb);
    if (T_ > nallowed)
        printf("  !! %d threads > %d allowed CPUs: PINNED modes oversubscribe (2 threads share a\n"
               "     core and the run waits for the slowest) while unpinned is load-balanced by the\n"
               "     scheduler. That comparison measures oversubscription, NOT placement.\n",
               T_, nallowed);
#ifndef HAVE_NUMA
    printf("  (built without libnuma: 'local/remote/interleave' pin CPUs only — "
           "memory follows first-touch, which still tracks the pinned core)\n");
#endif
    if (!strcmp(only, "all") || !strcmp(only, "none"))       run("none", T_, 0);
    if (!strcmp(only, "all") || !strcmp(only, "cpuonly"))    run("cpuonly", T_, 4);
    if (!strcmp(only, "all") || !strcmp(only, "local"))      run("local", T_, 1);
    if (!strcmp(only, "all") || !strcmp(only, "interleave")) run("interleave", T_, 2);
    if (!strcmp(only, "all") || !strcmp(only, "remote"))     run("remote", T_, 3);
    return 0;
}
