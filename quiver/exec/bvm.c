#define _GNU_SOURCE
/* bvm.c — the BLOCKS executor (docs/BLOCKS.md), C port of quiver/exec/blocks.py.
 *
 * A PERSISTENT executor driven by a stdin COMMAND STREAM (not one mode per argv):
 * Py opens sources (tars / fs subtrees) and sends COPY/SCATTER plans; C is the
 * sensor+actuator — it SCANs/DECODEs (streaming member STAT up) and executes
 * (parallel compress / inflate+scatter, streaming DONE). One process can hold N
 * tars AND M fs scans open at once (shared worker pool + block budget), which is
 * what multi-tar decode and fine-grained two-tree diff need. A FRAME is the unit,
 * a BOUNDED job queue makes the old QCAP overflow impossible. libzstd + pthreads.
 *
 *   bvm <nworkers> <budget_mb>
 * Py->C  [u32 len][u8 type][payload]:
 *   1 SINK  [u16 plen][path][i64 start]     3 NOCK [u32 id][u16 plen][path]
 *   2 DEST  [u16 plen][path]
 *   4 OPEN_TAR [u32 sid][i64 frame_bytes][u8 flags: bit0=stat_only bit1=tar_compat][u16 plen][path]
 *   5 SCAN_FS  [u32 sid][u16 plen][root]        (full stat per entry; dir rows too)
 *  14 SCAN_NAMES [u32 sid][u16 plen][root]      (readdir + d_type only, NO lstat — rm/enumerate)
 *   6 COPY_BLOCK [u64 block][u64 frame][u32 level]
 *   7 PACK_FILES [frame][level][sink][tc][root][n] + COLUMNS mode/size/poff/paths (+hoff/hdrs)
 *   8 SCATTER  [coff][clen][nock_id][n] + COLUMNS in_off/size/mode/mtime/uid/gid/poff/paths
 *   9 FREE/RETIRE [u64 block]   — drop the planner's owner-ref; block frees when no worker is reading it
 *  12 COPY_MEMBERS [u64 block][u64 frame][u32 level][u32 sink][u8 tar_compat][u32 n]( [i64 in_off][i64 size][u32 mode][u16 plen][path] [u32 hlen][hdr]? ) — frame from a member subset of a block
 *  13 UNLINK   [u8 removedir][u32 n] + COLUMNS poff/paths (io_uring batch)
 * C->Py:
 *   0 STAT [u32 sid][i64 block][u32 n]( [i64 in_off][i64 size][u32 mode][i64 mtime][u32 uid][u32 gid][u16 plen][path][u16 llen][link] ) — llen>0 = symlink target
 *   1 SRC_EOF [u32 sid]          2 DONE [u32 n]( [u64 frame][u64 coff][u64 clen] )
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/uio.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <dirent.h>
#include <fnmatch.h>
#include "blake3.h"
#include <zstd.h>
#include <sodium.h>
#include <liburing.h>

/* ------------------------------------------------------------------ globals */
static pthread_mutex_t g_out_mu = PTHREAD_MUTEX_INITIALIZER;   /* stdout */
static int g_nock_fds[1024];             /* NOCK sources by id; SCATTER picks one (multi-nock unpack) */
static unsigned char g_key[crypto_secretstream_xchacha20poly1305_KEYBYTES]; static int g_have_key;
/* SINKS: a COPY targets a sink by id. A sink is a seekable FILE (pwrite at a
 * reserved offset) or a TCP SOCKET stream (frame written as [frame_id][clen][bytes],
 * receiver assigns offsets) — several socket sinks give parallel data connections
 * for networked rsync, frames sharded across them. */
typedef struct { int fd; int is_sock; int enc; int64_t cursor; int64_t flushed; pthread_mutex_t mu;
                 int64_t unflushed;                      /* page-cache hygiene high-water */
                 crypto_secretstream_xchacha20poly1305_state st; } Sink;
static Sink g_sinks[64]; static int g_nsinks;
static int write_all(int fd, const void *buf, size_t n){ size_t o=0; while(o<n){ ssize_t r=write(fd,(const char*)buf+o,n-o);
    if(r<=0){ if(r<0&&errno==EINTR) continue; return -1; } o+=r; } return 0; }
static int pwrite_all(int fd, const void *buf, size_t n, off_t off){ size_t o=0; while(o<n){
    ssize_t r=pwrite(fd,(const char*)buf+o,n-o,off+o);   /* Linux caps a pwrite at ~2GB: LOOP */
    if(r<=0){ if(r<0&&errno==EINTR) continue; return -1; } o+=r; } return 0; }
static void emit_err(const char *path, int negerrno);         /* per-op ERROR to stdout (defined w/ STAT out) */
static int tcp_connect(const char *hostport){       /* "host:port" -> connected fd */
    char hp[256]; strncpy(hp,hostport,sizeof hp-1); hp[sizeof hp-1]=0;
    char *colon=strrchr(hp,':'); if(!colon) return -1; *colon=0; const char *host=hp, *port=colon+1;
    struct addrinfo hints={0}, *res=0; hints.ai_family=AF_INET; hints.ai_socktype=SOCK_STREAM;
    if(getaddrinfo(host,port,&hints,&res)||!res) return -1;
    int fd=-1;
    for(int a=0; a<200; a++){                          /* retry: receiver may not be listening yet */
        fd=socket(res->ai_family,res->ai_socktype,res->ai_protocol); if(fd<0) break;
        if(connect(fd,res->ai_addr,res->ai_addrlen)==0) break;
        close(fd); fd=-1; usleep(50000);
    }
    freeaddrinfo(res); return fd;
}
static char g_dest[4096]; static size_t g_destlen;
static int64_t now_ns(void){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec*1000000000LL+t.tv_nsec; }
/* Writeback pacing. 64 MB, not 256: measured on ~/eot, smaller and MORE FREQUENT flushes win,
 * because a large sync_file_range stalls the writer until it completes.
 *     64 MB  80.6s   256 MB  88.1s   1 GB  100.8s   4 GB  99.0s   never  76.9s
 * `never` is fastest but not shippable -- this pacing exists because dirty page cache counts
 * against the job's cgroup and OOM-killed the process at healthy RSS. 64 MB is within 4.6% of
 * unpaced AND bounds the dirty window four times tighter than the old default. BVM_FLUSH_MB
 * overrides; 0 disables. */
static int64_t g_flush_bytes = 64LL << 20;
/* BVM_SINK_DIRECT: open sinks O_DIRECT. Buffered pwrite reaches 1.57 GB/s here while fsbw
 * measures 6.01 GB/s durable with O_DIRECT and 8 MB chunks, so the page cache round trip is
 * worth testing. Direct I/O needs the offset, length and buffer all 4 KB-aligned: frames are
 * therefore placed on 4 KB boundaries and written rounded up. The footer records each frame's
 * exact (coff, clen), so the padding between frames is invisible to every reader -- and at
 * <=4095 B per frame it is 0.04% of a 6 TB store. */
static int g_sink_direct;
#define SINK_ALIGN 4096
static int64_t align_up(int64_t v, int64_t a){ return (v + a - 1) / a * a; }
static int64_t g_start_ns;                        /* frame times are relative to it */
/* PER-FRAME record. A 1 Hz sampler blurs exactly what you want to see -- which subtree was
 * expensive, which worker stalled, how deep the queue was when a frame started -- so every
 * frame carries its own cost instead. The planner knows frame_id -> path, so no strings
 * cross the wire, and it writes the whole table to a SIDECAR next to the store: the archive
 * format stays untouched and the telemetry is optional to keep. 64 B/frame, i.e. 36 MB for
 * a 556k-frame whole-home backup. */
typedef struct {
    uint64_t frame_id, coff, clen;
    int64_t  t0_ns, t1_ns, in_bytes;                 /* start/end relative to g_start_ns */
    int32_t  jq_depth, busy, worker, sink;           /* queue state WHEN THE FRAME STARTED */
} Done;
static Done *g_done; static int g_ndone, g_donecap; static pthread_mutex_t g_done_mu = PTHREAD_MUTEX_INITIALIZER;

static int ensure(uint8_t **b, size_t *cap, size_t need) {
    if (*cap >= need) return 0;
    size_t nc = *cap ? *cap : (1 << 20); while (nc < need) nc <<= 1;
    /* 4 KB-aligned so any of these buffers can back an O_DIRECT write (BVM_SINK_DIRECT).
     * aligned_alloc + copy rather than realloc: realloc gives no alignment guarantee. */
    uint8_t *p = aligned_alloc(4096, (nc + 4095) & ~(size_t)4095);
    if (!p) return -1;
    if (*b) { memcpy(p, *b, *cap); free(*b); }
    *b = p; *cap = nc; return 0;
}
static void mkparents(char *full) {
    for (char *p = strchr(full + 1, '/'); p; p = strchr(p + 1, '/')) { *p = 0; mkdir(full, 0777); *p = '/'; }
}
/* io_uring batch unlink/rmdir: keep QD ops in flight so WEKA metadata RPC latency is hidden
 * (deep async queue instead of thread-per-unlink). Synchronous — drains before returning.
 * Paths must stay valid until reaped, so the caller holds the array. Returns ops that hit ==0. */
static int g_uring_qd = 4096;            /* io_uring in-flight depth (env BVM_URING_QD) */
static int g_uring_workers = 0;          /* io-wq bounded worker cap; 0=kernel default (env BVM_URING_WORKERS).
                                          * unlinkat isn't natively async -> runs on this per-ring pool, which
                                          * (not QD) caps single-process concurrency. */
static int g_scatter_meta = 3;           /* unpack metadata RPCs: bit0=mtime, bit1=chown (env BVM_SCATTER_META) */
static int g_uring_rings = 1;            /* parallel io_uring rings/threads per UNLINK (env BVM_URING_RINGS) */
/* `failed`, if non-NULL, is n bytes: set to the +errno of each path whose unlink failed for a
 * REAL reason (ENOENT = already gone = success). Lets the caller retry rmdirs (WEKA can
 * transiently report a just-emptied dir as ENOTEMPTY across client frontends) and REPORT
 * the rest instead of dropping them on the floor. */
static int64_t io_unlink_batch_one(char **paths, int n, int removedir, uint8_t *failed) {
    struct io_uring ring;
    int QD = g_uring_qd;
    if (io_uring_queue_init(QD, &ring, 0) < 0) return -1;
    if (g_uring_workers > 0) {            /* raise the io-wq bounded pool so ONE ring runs more unlinks at once */
        unsigned vals[2] = { (unsigned)g_uring_workers, (unsigned)g_uring_workers };
        io_uring_register_iowq_max_workers(&ring, vals);
    }
    int flag = removedir ? AT_REMOVEDIR : 0, i = 0, inflight = 0; int64_t ok = 0;
    while (i < n || inflight > 0) {
        while (inflight < QD && i < n) {
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
            if (!sqe) break;
            io_uring_prep_unlinkat(sqe, AT_FDCWD, paths[i], flag);
            io_uring_sqe_set_data64(sqe, (uint64_t)i);
            i++; inflight++;
        }
        io_uring_submit(&ring);
        struct io_uring_cqe *cqe;
        if (io_uring_wait_cqe(&ring, &cqe) < 0) break;
        unsigned head; int c = 0;
        io_uring_for_each_cqe(&ring, head, cqe) {
            if (cqe->res == 0) ok++;
            else if (failed && cqe->res != -ENOENT) {          /* ENOENT = already gone */
                uint64_t idx = io_uring_cqe_get_data64(cqe);
                if (idx < (uint64_t)n) failed[idx] = (uint8_t)(-cqe->res > 255 ? 255 : -cqe->res); }
            c++;
        }
        io_uring_cq_advance(&ring, c); inflight -= c;
    }
    io_uring_queue_exit(&ring);
    return ok;
}
/* multi-ring dispatch: R threads each with their OWN io_uring ring over a slice of the paths.
 * Distinguishes single-submit-thread bound (this helps) from WEKA-client-per-process bound (flat). */
typedef struct { char **paths; int n, removedir; int64_t ok; uint8_t *failed; } URun;
static void *unlink_thread(void *a) { URun *u = a; u->ok = io_unlink_batch_one(u->paths, u->n, u->removedir, u->failed); return NULL; }
static int64_t io_unlink_batch(char **paths, int n, int removedir, uint8_t *failed) {
    int R = g_uring_rings; if (R < 1) R = 1; if (R > 64) R = 64;
    if (R == 1 || n < R) return io_unlink_batch_one(paths, n, removedir, failed);
    pthread_t th[64]; URun a[64]; int per = (n + R - 1) / R; int64_t ok = 0;
    for (int i = 0; i < R; i++) { int s = i * per, e = s + per > n ? n : s + per;
        a[i] = (URun){ paths + s, e - s, removedir, 0, failed ? failed + s : NULL }; pthread_create(&th[i], 0, unlink_thread, &a[i]); }
    for (int i = 0; i < R; i++) { pthread_join(th[i], 0); ok += a[i].ok; }
    return ok;
}
static int read_full(int fd, void *buf, size_t n) {
    size_t got = 0; while (got < n) { ssize_t r = read(fd, (char *)buf + got, n - got);
        if (r < 0) { if (errno == EINTR) continue; return -1; } if (r == 0) return got == 0 ? 0 : -1; got += r; }
    return 1;
}
#define RD(T, p) ({ T _v; memcpy(&_v, (p), sizeof(T)); (p) += sizeof(T); _v; })
/* Flush the frame records collected so far. STREAMED, not held to exit: holding them meant
 * the planner learned every frame's locator and cost in one lump at the very end, so nothing
 * could watch a run in flight, nothing could checkpoint, and a crash at 95% left TBs on disk
 * that nothing knew the offsets of. The planner takes repeated type-2 messages fine -- it
 * just keeps updating its locator map. */
#define DONE_FLUSH 2048
static void flush_done_locked(void) {
    if (!g_ndone) return;
    uint32_t body = 1 + 4 + g_ndone * (uint32_t)sizeof(Done);
    uint8_t hdr[9]; memcpy(hdr, &body, 4); hdr[4] = 2;
    uint32_t nd = (uint32_t)g_ndone; memcpy(hdr + 5, &nd, 4);
    pthread_mutex_lock(&g_out_mu);
    write_all(1, hdr, 9);
    for (int i = 0; i < g_ndone; i++) write_all(1, &g_done[i], sizeof(Done));
    pthread_mutex_unlock(&g_out_mu);
    g_ndone = 0;
}
static void record_done_x(uint64_t f, uint64_t coff, uint64_t clen, int64_t t0, int64_t in_b,
                          int32_t jq, int32_t busy, int32_t worker, int32_t sink) {
    pthread_mutex_lock(&g_done_mu);
    if (g_ndone == g_donecap) { g_donecap = g_donecap ? g_donecap * 2 : 1024; g_done = realloc(g_done, g_donecap * sizeof(Done)); }
    g_done[g_ndone++] = (Done){ f, coff, clen, t0, now_ns() - g_start_ns, in_b, jq, busy, worker, sink };
    if (g_ndone >= DONE_FLUSH) flush_done_locked();
    pthread_mutex_unlock(&g_done_mu);
}
#define record_done(f, coff, clen) \
    record_done_x((f), (coff), (clen), w->jt0, (int64_t)len, w->jjq, w->jbusy, w->id, (int32_t)sink_id)

/* ------------------------------------------------------------------ perf stats */
/* Always-on counters at every cost center; emitted as ONE type-4 message at exit so the
 * planner can tell users WHERE their job spent its time (CPU vs WEKA bandwidth vs
 * metadata RPCs vs starvation) instead of guessing. Overhead: one atomic add + two
 * clock_gettime per instrumented op — noise against syscalls/compression. */
typedef struct {
    int64_t rd_bytes, wr_bytes, comp_in, comp_out, dcomp_out;
    int64_t ns_read, ns_comp, ns_dcomp, ns_write, ns_hash;
    int64_t n_open, ns_open, slow_open;         /* file creates/opens; slow = >500us (backend RPC) */
    int64_t n_meta, ns_meta;                    /* fchown/futimens/fchmod */
    int64_t n_unlink, ns_unlink;
    int64_t scan_entries, ns_scan_stat;
    int64_t ns_idle, ns_emit, ns_qfull;         /* worker starvation / stdout backpressure / planner-ahead */
    int64_t n_raw_frames, raw_bytes;            /* incompressible: codec skipped (raw fs path) */
    int64_t ring_batches, ring_members;         /* small members prefetched via io_uring */
    int64_t n_probe, probe_in;                  /* stratified incompressibility probe windows */
    int64_t cfr_bytes;                          /* unpacked via copy_file_range (zero-copy) */
    /* QUEUE OCCUPANCY (100Hz sampler): Little's-law bottleneck attribution — a bounded
     * queue that is persistently FULL means its CONSUMER is the constraint; persistently
     * EMPTY means its PRODUCER is. Sums are divided by q_samples in the report. */
    int64_t q_samples, jq_sum, jq_full, jq_empty, dq_sum, busy_sum, pack_used_sum, blk_live_sum;
} PStats;
static PStats g_st;

/* ---- pack-buffer budget: J_PACK/J_COPY_MEMBERS materialize a whole frame (+ compress
 * bound ~= 2x) per worker; giant single-member frames (multi-GB model files) times many
 * workers OOM'd a 1TB node. Reserve bytes before buffering; a single job larger than the
 * whole budget proceeds ALONE (never deadlocks). Default: 35% of MemTotal, override
 * BVM_PACK_BUDGET_MB. Worker scratch also SHRINKS after oversized jobs — ensure() only
 * ever grew, so every worker that ever touched a giant kept ~2x its size forever. */
static int64_t g_pack_budget = 0, g_pack_used = 0;
static pthread_mutex_t g_pb_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_pb_cv = PTHREAD_COND_INITIALIZER;
static void pack_reserve(int64_t n) {
    pthread_mutex_lock(&g_pb_mu);
    while (g_pack_used > 0 && g_pack_used + n > g_pack_budget)
        pthread_cond_wait(&g_pb_cv, &g_pb_mu);
    g_pack_used += n; pthread_mutex_unlock(&g_pb_mu);
}
static void pack_release(int64_t n) {
    pthread_mutex_lock(&g_pb_mu); g_pack_used -= n;
    pthread_cond_broadcast(&g_pb_cv); pthread_mutex_unlock(&g_pb_mu);
}
#define SHRINK_CAP (128LL << 20)
static void wshrink(uint8_t **b, size_t *cap) {
    if (*cap > (size_t)SHRINK_CAP) { free(*b); *b = NULL; *cap = 0; }
}
#define ST(f, v) __atomic_fetch_add(&g_st.f, (int64_t)(v), __ATOMIC_RELAXED)

/* ------------------------------------------------------------------ content digests */
/* Digests: BLAKE3 (static libblake3, runtime-dispatched SSE2/SSE4.1/AVX2/AVX512) — 4.5 GB/s
 * vs BLAKE2b 0.67 (6.7x), tree-structured (natural fit for our Merkle footers + content
 * addressing) and it carries keyed-MAC + KDF modes natively, which is exactly what encrypted
 * chunk identity will need. libsodium stays for AEAD/KX/Argon2. Chunk ids = BLAKE3-128
 * (first 16B of the XOF); whole-file `digest` (footer i64) = first 8B. hash-id byte in the
 * manifest header distinguishes this from the old BLAKE2b stores. */
static void blake3_hash(const uint8_t *p, size_t len, uint8_t *out, size_t outlen) {
    blake3_hasher h; blake3_hasher_init(&h);
    blake3_hasher_update(&h, p, len); blake3_hasher_finalize(&h, out, outlen);
}
static int64_t blake3_i64(const uint8_t *p, size_t len) {
    uint8_t h[8]; blake3_hash(p, len, h, 8); int64_t v; memcpy(&v, h, 8); return v;
}
/* FastCDC: min 16K / avg 64K / max 512K; files < CDC_CUTOFF are their own single chunk
 * (no manifest stored — the whole-file digest covers them). Gear masks target the HIGH
 * bits (h<<1 accumulates entropy upward); normalized: harder mask before avg, easier after. */
#define CDC_CUTOFF (128*1024)
#define CDC_MINSZ  (16*1024)
#define CDC_AVGSZ  (64*1024)
#define CDC_MAXSZ  (512*1024)
#define CDC_MASK_S (((1ULL<<18)-1)<<44)   /* 16+2 bits */
#define CDC_MASK_L (((1ULL<<14)-1)<<44)   /* 16-2 bits */
static uint64_t GEAR[256];
static void gear_init(void){ uint64_t x=0x2545F4914F6CDD1DULL;
    for(int i=0;i<256;i++){ x^=x>>12; x^=x<<25; x^=x>>27; GEAR[i]=x*0x2545F4914F6CDD1DULL; } }
/* append the CDC manifest of buf[0..n) to *out (grown):
 * [u8 hashid=2 (BLAKE3-128)][u8 pad x3][u32 nchunks][n x (u32 len, 16B digest)].
 * Returns manifest length in bytes. */
/* Where does the next chunk end in b[0..n)? `eof` = b ends the file, so a short tail is a
 * chunk in its own right; without it, 0 means "no cut in this window, give me more bytes".
 * Stateless — the gear hash restarts at every chunk boundary — so scanning a partial window
 * and rescanning it after a refill yields exactly the same cuts as scanning the whole file.
 * That is what lets manifest_stream() work in bounded memory with identical output. */
static size_t cdc_next(const uint8_t *b, size_t n, int eof){
    if (n == 0) return 0;
    if (n <= CDC_MINSZ) return eof ? n : 0;
    size_t mx = n < CDC_MAXSZ ? n : CDC_MAXSZ, av = n < CDC_AVGSZ ? n : CDC_AVGSZ;
    uint64_t h = 0; size_t i = CDC_MINSZ;
    for (; i < av; i++){ h=(h<<1)+GEAR[b[i]]; if(!(h&CDC_MASK_S)) return i+1; }
    for (; i < mx; i++){ h=(h<<1)+GEAR[b[i]]; if(!(h&CDC_MASK_L)) return i+1; }
    if (n >= CDC_MAXSZ) return CDC_MAXSZ;                        /* forced cut at the max */
    return eof ? n : 0;                                          /* tail, or need more bytes */
}
static void man_init(uint8_t **out, size_t *cap, size_t *w){
    if (*cap < 4096) { *cap = 4096; *out = realloc(*out, *cap); }
    *w = 8;
}
static void man_add(uint8_t **out, size_t *cap, size_t *w, uint32_t *nc,
                    const uint8_t *b, size_t cut){
    size_t need = *w + 20;
    if (need > *cap){ while(*cap < need) *cap *= 2; *out = realloc(*out, *cap); }
    uint32_t cl = (uint32_t)cut;
    memcpy(*out + *w, &cl, 4);
    blake3_hash(b, cut, *out + *w + 4, 16);                      /* BLAKE3-128 chunk id */
    *w += 20; (*nc)++;
}
static void man_end(uint8_t *out, uint32_t nc){
    out[0] = 2; out[1] = out[2] = out[3] = 0;                    /* hashid 2 = BLAKE3-128 */
    memcpy(out + 4, &nc, 4);
}
static size_t cdc_manifest(const uint8_t *b, size_t n, uint8_t **out, size_t *cap){
    uint32_t nc = 0; size_t w, off = 0;
    man_init(out, cap, &w);
    while (off < n){
        size_t cut = cdc_next(b + off, n - off, 1);
        man_add(out, cap, &w, &nc, b + off, cut); off += cut;
    }
    man_end(*out, nc);
    return w;
}

/* ------------------------------------------------------------------ STAT out (Arrow IPC) */
/* STAT leaves bvm as ARROW RECORD BATCHES built straight from columnar accumulators —
 * templates baked by compiler/gen_templates.py (BSTAT in ipc_gen.h), the same mechanism
 * qvm uses. Python reads each payload with pl.read_ipc_stream (native Rust): no per-row
 * Python parsing (the hand-rolled row encoding this replaces measured 17x slower). The
 * schema is the pwalk2/ducl superset (inode graph + full timestamps) so ducl can consume
 * bvm scans directly; modes that lack a field emit zeros. Framing on stdout stays
 * [u32 body][u8 type=0][u32 sid][i64 block] + [schema][batch][EOS]. */
#include "ipc_gen.h"

typedef struct { const void *p; int64_t len; } WBuf;

/* ── flatbuffer navigation (INPUT side — command payloads are Arrow IPC streams written
 * by polars with compat_level=oldest, the same writer quiver-exec parses; the reader is
 * ported from quiver-exec.c). A command's member table parses STRAIGHT FROM THE WIRE:
 * typed column pointers into the payload, no per-member decode. */
static uint16_t fb_u16(const uint8_t *b, int64_t o){ uint16_t v; memcpy(&v,b+o,2); return v; }
static int32_t  fb_i32(const uint8_t *b, int64_t o){ int32_t v;  memcpy(&v,b+o,4); return v; }
static uint32_t fb_u32(const uint8_t *b, int64_t o){ uint32_t v; memcpy(&v,b+o,4); return v; }
static int64_t  fb_i64(const uint8_t *b, int64_t o){ int64_t v;  memcpy(&v,b+o,8); return v; }
static int64_t  fb_root(const uint8_t *b) { return fb_u32(b, 0); }
static int64_t fb_field(const uint8_t *b, int64_t table, int id) {
    int64_t vt = table - fb_i32(b, table);
    int slot = 4 + 2 * id;
    if (slot >= fb_u16(b, vt)) return -1;
    uint16_t voff = fb_u16(b, vt + slot);
    return voff ? table + voff : -1;
}
static int64_t fb_offset_field(const uint8_t *b, int64_t table, int id) {
    int64_t p = fb_field(b, table, id);
    return p < 0 ? -1 : p + fb_u32(b, p);
}
/* Arrow IPC stream ITERATOR. polars splits a stream into MANY record batches (~300k rows
 * each) regardless of DataFrame chunking — reading only the first silently DROPS the rest
 * (this cost a 4.6TB home backup 1.33M members). arrow_next() walks messages properly,
 * skipping metadata AND body, and yields each record batch's buffer pointers.
 * Usage:  const uint8_t *cur = p; int64_t n;
 *         while ((n = arrow_next(&cur, bufp, NBUFS)) > 0) { ...consume n rows... }
 *         n < 0 => malformed/shape mismatch (refuse, don't guess); 0 => EOS. */
static int64_t arrow_next(const uint8_t **cur, const uint8_t **bufp, int nbufs_expect) {
    for (;;) {
        const uint8_t *q = *cur;
        uint32_t cont, mlen;
        memcpy(&cont, q, 4); memcpy(&mlen, q + 4, 4);
        if (mlen == 0) return 0;                          /* EOS */
        const uint8_t *meta = q + 8;
        int64_t rt = fb_root(meta);
        int64_t blp = fb_field(meta, rt, 3);
        int64_t blen = blp >= 0 ? fb_i64(meta, blp) : 0;
        const uint8_t *body = meta + mlen;
        *cur = body + blen;                               /* next message */
        int64_t htp = fb_field(meta, rt, 1);
        if (htp < 0) return -1;
        if (meta[htp] != 3) continue;                     /* schema/dictionary: skip */
        int64_t rb = fb_offset_field(meta, rt, 2);
        int64_t n = fb_i64(meta, fb_field(meta, rb, 0));
        int64_t bufs = fb_offset_field(meta, rb, 2);
        if ((int)fb_u32(meta, bufs) != nbufs_expect) return -1;
        for (int i = 0; i < nbufs_expect; i++) bufp[i] = body + fb_i64(meta, bufs + 4 + 16 * i);
        return n;
    }
}
typedef struct {                                   /* columnar STAT accumulator */
    int64_t *poff; char *pdat; size_t pdcap, plen; /* path: i64 offsets + bytes */
    int64_t *loff; char *ldat; size_t ldcap, llen; /* link */
    int64_t *in_off, *size, *blocks, *mt, *at, *ct;
    uint64_t *ino, *pino, *dev;
    int32_t *mode, *uid, *gid, *nlink, *depth; uint8_t *isdir;
    int64_t *dg;                                   /* whole-file BLAKE3-64 (-1 = absent) */
    int64_t *koff; uint8_t *kdat; size_t kdcap, klen;   /* chunks manifest: offsets + blob */
    uint32_t n, cap;
} BC;
static void bc_reserve(BC *b) {
    if (b->n < b->cap) return;
    uint32_t c = b->cap ? b->cap * 2 : 4096; b->cap = c;
    b->poff=realloc(b->poff,(c+1)*8); b->loff=realloc(b->loff,(c+1)*8);
    b->in_off=realloc(b->in_off,c*8); b->size=realloc(b->size,c*8); b->blocks=realloc(b->blocks,c*8);
    b->mt=realloc(b->mt,c*8); b->at=realloc(b->at,c*8); b->ct=realloc(b->ct,c*8);
    b->ino=realloc(b->ino,c*8); b->pino=realloc(b->pino,c*8); b->dev=realloc(b->dev,c*8);
    b->mode=realloc(b->mode,c*4); b->uid=realloc(b->uid,c*4); b->gid=realloc(b->gid,c*4);
    b->nlink=realloc(b->nlink,c*4); b->depth=realloc(b->depth,c*4); b->isdir=realloc(b->isdir,c);
    b->dg=realloc(b->dg,c*8); b->koff=realloc(b->koff,(c+1)*8);
}
static void bc_str(char **dat, size_t *cap, size_t *len, const char *s, size_t sl) {
    if (*len + sl > *cap) { size_t c = *cap ? *cap : 65536; while (*len + sl > c) c *= 2; *dat = realloc(*dat, c); *cap = c; }
    memcpy(*dat + *len, s, sl); *len += sl;
}
/* full-stat row (fs scan): everything from `st`, plus the walk's pino/depth/isdir */
static void bc_add_fs(BC *b, const char *path, const char *link, int64_t in_off,
                      const struct stat *st, uint64_t pino, int32_t depth, int isdir) {
    bc_reserve(b);
    if (b->n == 0) { b->poff[0] = 0; b->loff[0] = 0; b->koff[0] = 0; }
    size_t pl = strlen(path), ll = link ? strlen(link) : 0;
    bc_str(&b->pdat, &b->pdcap, &b->plen, path, pl); b->poff[b->n+1] = (int64_t)b->plen;
    if (ll) bc_str(&b->ldat, &b->ldcap, &b->llen, link, ll);
    b->loff[b->n+1] = (int64_t)b->llen;
    b->koff[b->n+1] = (int64_t)b->klen;              /* no manifest by default */
    uint32_t i = b->n; b->dg[i] = -1;
    b->in_off[i]=in_off; b->size[i]=st->st_size; b->blocks[i]=st->st_blocks;
    b->mt[i]=(int64_t)st->st_mtim.tv_sec*1000000000LL+st->st_mtim.tv_nsec;
    b->at[i]=(int64_t)st->st_atim.tv_sec*1000000000LL+st->st_atim.tv_nsec;
    b->ct[i]=(int64_t)st->st_ctim.tv_sec*1000000000LL+st->st_ctim.tv_nsec;
    b->ino[i]=st->st_ino; b->pino[i]=pino; b->dev[i]=st->st_dev;
    b->mode[i]=(int32_t)st->st_mode; b->uid[i]=(int32_t)st->st_uid; b->gid[i]=(int32_t)st->st_gid;
    b->nlink[i]=(int32_t)st->st_nlink; b->depth[i]=depth; b->isdir[i]=(uint8_t)isdir; b->n++;
}
/* raw row (tar scan / names-only): only the fields the source knows; the rest zero */
static void bc_add_raw(BC *b, const char *path, const char *link, int64_t in_off,
                       int64_t size, uint32_t mode, int64_t mtime, uint32_t uid, uint32_t gid, int isdir) {
    bc_reserve(b);
    if (b->n == 0) { b->poff[0] = 0; b->loff[0] = 0; b->koff[0] = 0; }
    size_t pl = strlen(path), ll = link ? strlen(link) : 0;
    bc_str(&b->pdat, &b->pdcap, &b->plen, path, pl); b->poff[b->n+1] = (int64_t)b->plen;
    if (ll) bc_str(&b->ldat, &b->ldcap, &b->llen, link, ll);
    b->loff[b->n+1] = (int64_t)b->llen;
    b->koff[b->n+1] = (int64_t)b->klen;              /* no manifest by default */
    uint32_t i = b->n; b->dg[i] = -1;
    b->in_off[i]=in_off; b->size[i]=size; b->blocks[i]=0; b->mt[i]=mtime; b->at[i]=0; b->ct[i]=0;
    b->ino[i]=0; b->pino[i]=0; b->dev[i]=0; b->mode[i]=(int32_t)mode; b->uid[i]=(int32_t)uid;
    b->gid[i]=(int32_t)gid; b->nlink[i]=0; b->depth[i]=0; b->isdir[i]=(uint8_t)isdir; b->n++;
}
/* set the LAST row's digest + append its manifest (pack worker, body in hand) */
static void bc_tail_digest(BC *b, int64_t dg, const uint8_t *man, size_t mlen) {
    if (!b->n) return;
    b->dg[b->n-1] = dg;
    if (mlen) {
        if (b->klen + mlen > b->kdcap) { size_t c = b->kdcap ? b->kdcap : 4096;
            while (b->klen + mlen > c) c *= 2; b->kdat = realloc(b->kdat, c); b->kdcap = c; }
        memcpy(b->kdat + b->klen, man, mlen); b->klen += mlen;
        b->koff[b->n] = (int64_t)b->klen;
    }
}
static void bc_free(BC *b) {
    free(b->poff); free(b->pdat); free(b->loff); free(b->ldat);
    free(b->in_off); free(b->size); free(b->blocks); free(b->mt); free(b->at); free(b->ct);
    free(b->ino); free(b->pino); free(b->dev); free(b->mode); free(b->uid); free(b->gid);
    free(b->nlink); free(b->depth); free(b->isdir);
    free(b->dg); free(b->koff); free(b->kdat); memset(b, 0, sizeof *b);
}
/* serialize a BC as one in-memory Arrow stream [schema][batch][body][EOS] (BSTAT schema).
 * Does NOT reset the BC. Used for the stdout STAT wire AND the network-apply stat blob
 * (rx parses it with arrow_batch — full typed metadata crosses the data plane). */
static uint8_t *bc_arrow(BC *b, size_t *outlen) {
    int64_t n = b->n, zero = 0; static const int64_t zoff = 0;
    const void *po = b->poff ? (const void *)b->poff : (const void *)&zoff;   /* n==0: one 0 offset */
    const void *lo = b->loff ? (const void *)b->loff : (const void *)&zoff;
    WBuf bufs[BSTAT_N_BUFS] = {
        {NULL,0},{po,8*(n+1)},{b->pdat,(int64_t)b->plen},
        {NULL,0},{lo,8*(n+1)},{b->ldat,(int64_t)b->llen},
        {NULL,0},{b->in_off,8*n}, {NULL,0},{b->size,8*n},  {NULL,0},{b->blocks,8*n},
        {NULL,0},{b->mt,8*n},     {NULL,0},{b->at,8*n},    {NULL,0},{b->ct,8*n},
        {NULL,0},{b->ino,8*n},    {NULL,0},{b->pino,8*n},  {NULL,0},{b->dev,8*n},
        {NULL,0},{b->mode,4*n},   {NULL,0},{b->uid,4*n},   {NULL,0},{b->gid,4*n},
        {NULL,0},{b->nlink,4*n},  {NULL,0},{b->depth,4*n}, {NULL,0},{b->isdir,n},
        {NULL,0},{b->dg,8*n},
        {NULL,0},{b->koff ? (const void *)b->koff : (const void *)&zoff, 8*(n+1)},
        {b->kdat,(int64_t)b->klen}};
    uint8_t meta[BSTAT_TMPL_LEN]; memcpy(meta, BSTAT_BATCH_TMPL, BSTAT_TMPL_LEN);
    int64_t pos = 0;
    for (int i = 0; i < BSTAT_N_BUFS; i++) {
        memcpy(meta+BSTAT_BUF_OFF[i], &pos, 8); memcpy(meta+BSTAT_BUF_OFF[i]+8, &bufs[i].len, 8);
        pos += (bufs[i].len + 7) & ~7LL;
    }
    memcpy(meta+BSTAT_OFF_BODYLEN, &pos, 8); memcpy(meta+BSTAT_OFF_RBLEN, &n, 8);
    for (int i = 0; i < BSTAT_N_NODES; i++) {
        memcpy(meta+BSTAT_NODE_OFF[i], &n, 8); memcpy(meta+BSTAT_NODE_OFF[i]+8, &zero, 8);
    }
    size_t arrow = 8+BSTAT_SCHEMA_LEN + 8+BSTAT_TMPL_LEN + (size_t)pos + 8;
    uint8_t *out = malloc(arrow), *p = out;
    uint32_t fr[2] = {0xFFFFFFFFu, BSTAT_SCHEMA_LEN};
    memcpy(p,fr,8); p+=8; memcpy(p,BSTAT_SCHEMA_META,BSTAT_SCHEMA_LEN); p+=BSTAT_SCHEMA_LEN;
    fr[1] = BSTAT_TMPL_LEN;
    memcpy(p,fr,8); p+=8; memcpy(p,meta,BSTAT_TMPL_LEN); p+=BSTAT_TMPL_LEN;
    for (int i = 0; i < BSTAT_N_BUFS; i++) if (bufs[i].len) {
        memcpy(p,bufs[i].p,(size_t)bufs[i].len); p+=bufs[i].len;
        size_t pad = (size_t)((-bufs[i].len) & 7); memset(p,0,pad); p+=pad;
    }
    fr[1] = 0; memcpy(p,fr,8); p+=8;                                   /* EOS */
    *outlen = arrow;
    return out;
}
static void emit_bstat(uint32_t sid, int64_t block, BC *b) {
    if (!b->n && block < 0) return;       /* block>=0 MUST emit even empty: it registers the block id */
    size_t alen; uint8_t *a = bc_arrow(b, &alen);
    uint32_t body = 1 + 4 + 8 + (uint32_t)alen; uint8_t hdr[17];   /* [body][type][sid][block] */
    memcpy(hdr,&body,4); hdr[4]=0; memcpy(hdr+5,&sid,4); memcpy(hdr+9,&block,8);
    { int64_t te = now_ns();
      pthread_mutex_lock(&g_out_mu);
      write_all(1, hdr, 17); write_all(1, a, alen);
      pthread_mutex_unlock(&g_out_mu);
      ST(ns_emit, now_ns() - te); }
    free(a); b->n = 0; b->plen = 0; b->llen = 0; b->klen = 0;          /* keep capacity */
}
static void emit_eof(uint32_t sid) {
    uint32_t body = 1 + 4; uint8_t hdr[9]; memcpy(hdr, &body, 4); hdr[4] = 1; memcpy(hdr + 5, &sid, 4);
    pthread_mutex_lock(&g_out_mu); write_all(1, hdr, 9); pthread_mutex_unlock(&g_out_mu);
}
/* per-op ERROR (type 3): [i32 -errno][u16 plen][path]. The planner collects these and
 * raises — partial failure is REPORTED, never silent (a failed member/frame/unlink names
 * itself). Layout stays deterministic regardless (see J_PACK_FILES), so one bad file can
 * no longer shift its neighbours' offsets. */
static void emit_err(const char *path, int negerrno) {
    uint16_t pl = (uint16_t)strnlen(path, 4096);
    uint32_t body = 1 + 4 + 2 + pl; uint8_t hdr[11];
    memcpy(hdr, &body, 4); hdr[4] = 3; memcpy(hdr + 5, &negerrno, 4); memcpy(hdr + 9, &pl, 2);
    pthread_mutex_lock(&g_out_mu); write_all(1, hdr, 11); write_all(1, path, pl); pthread_mutex_unlock(&g_out_mu);
}


/* ------------------------------------------------------------------ tar header synthesis */
/* PACK-side tar headers are generated HERE, in the workers, from the STAT columns already on
 * the wire — not shipped as per-member blobs (they dominated the message: tarfile's PAX form
 * was 1536 B/member; this is ustar 512 B, longname only when needed — the same deterministic
 * rule the old TarFormat polars expressions used, so the planner's hlen budget stays a
 * vectorized formula: plen<=100 -> 512, else 1024+pad512(plen+1)). mtime = integer seconds
 * (ustar); size >= 8 GiB uses GNU base-256. */
static void tar_octal(char *f, int w, uint64_t v) { snprintf(f, w, "%0*llo", w - 1, (unsigned long long)v); }
static void tar_hdr512(uint8_t *o, const char *name, size_t nlen, uint32_t mode,
                       uint32_t uid, uint32_t gid, uint64_t size, int64_t mtime_s, char type) {
    memset(o, 0, 512);
    memcpy(o, name, nlen > 100 ? 100 : nlen);
    tar_octal((char *)o + 100, 8, mode & 07777);
    tar_octal((char *)o + 108, 8, uid & 07777777); tar_octal((char *)o + 116, 8, gid & 07777777);
    if (size < (1ULL << 33)) tar_octal((char *)o + 124, 12, size);
    else { o[124] = 0x80; for (int i = 0; i < 8; i++) o[135 - i] = (uint8_t)(size >> (8 * i)); }
    tar_octal((char *)o + 136, 12, (uint64_t)(mtime_s < 0 ? 0 : mtime_s));
    o[156] = (uint8_t)type;
    memcpy(o + 257, "ustar", 6); memcpy(o + 263, "00", 2);
    memset(o + 148, ' ', 8);                              /* checksum: spaces while summing */
    unsigned chk = 0; for (int i = 0; i < 512; i++) chk += o[i];
    snprintf((char *)o + 148, 7, "%06o", chk); o[154] = 0; o[155] = ' ';
}
/* header length for a member — MUST match the planner's vectorized formula (_tar_hlens):
 * base 512, + GNU 'L' longname blocks for >100-byte paths, + GNU 'K' longlink blocks for
 * >100-byte link targets (links only). */
static int64_t tar_hlen2(size_t plen, size_t llen) {
    int64_t h = 512;
    if (plen > 100) h += 512 + (int64_t)((plen + 1 + 511) & ~511ULL);
    if (llen > 100) h += 512 + (int64_t)((llen + 1 + 511) & ~511ULL);
    return h;
}
static int64_t tar_hlen(size_t plen) { return tar_hlen2(plen, 0); }
/* emit header(s) for one TYPED member at `o`; returns bytes written (== tar_hlen2).
 * mtype: 0 regular file ('0'), 5 dir ('5'), 2 symlink ('2'), 1 hardlink ('1').
 * `link` = target for sym/hardlinks (NULL/"" otherwise). Bodyless types carry size 0. */
static int64_t tar_emit_hdr2(uint8_t *o, const char *path, const char *link, int mtype,
                             uint32_t mode, uint32_t uid, uint32_t gid,
                             uint64_t size, int64_t mtime_s) {
    size_t plen = strlen(path), llen = link ? strlen(link) : 0;
    char tf = mtype == 5 ? '5' : mtype == 2 ? '2' : mtype == 1 ? '1' : '0';
    uint64_t hsz = mtype == 0 ? size : 0;                 /* dirs+links: no body in the tar */
    uint8_t *q = o;
    if (llen > 100) {                                     /* GNU longlink: 'K' + target blocks */
        size_t pad = (llen + 1 + 511) & ~511ULL;
        tar_hdr512(q, "././@LongLink", 13, 0644, 0, 0, llen + 1, mtime_s, 'K');
        memset(q + 512, 0, pad); memcpy(q + 512, link, llen); q += 512 + pad;
    }
    if (plen > 100) {                                     /* GNU longname: 'L' + path blocks */
        size_t pad = (plen + 1 + 511) & ~511ULL;
        tar_hdr512(q, "././@LongLink", 13, 0644, 0, 0, plen + 1, mtime_s, 'L');
        memset(q + 512, 0, pad); memcpy(q + 512, path, plen); q += 512 + pad;
    }
    tar_hdr512(q, path, plen, mode, uid, gid, hsz, mtime_s, tf);
    if (llen) memcpy(q + 157, link, llen > 100 ? 100 : llen);   /* linkname field (truncated ok: 'K' has it) */
    if (llen) {                                           /* linkname changed the checksum: redo it */
        memset(q + 148, ' ', 8);
        unsigned chk = 0; for (int i = 0; i < 512; i++) chk += q[i];
        snprintf((char *)q + 148, 7, "%06o", chk); q[154] = 0; q[155] = ' ';
    }
    return (q - o) + 512;
}
/* legacy single-type wrapper (regular file) */
static int64_t tar_emit_hdr(uint8_t *o, const char *path, uint32_t mode, uint32_t uid,
                            uint32_t gid, uint64_t size, int64_t mtime_s) {
    return tar_emit_hdr2(o, path, NULL, 0, mode, uid, gid, size, mtime_s);
}

/* ------------------------------------------------------------------ jobs */
enum { J_COPY_BLOCK, J_PACK_FILES, J_SCATTER, J_COPY_MEMBERS, J_MANIFEST };
typedef struct { int64_t in_off, size; uint32_t mode; char *path; uint8_t *hdr; uint32_t hlen;
                 int64_t mtime; uint32_t uid, gid; uint8_t mtype; char *link;
                 int64_t out_off, fsize; } Member;      /* scatter extent piece (fsize: whole-file size) */
/* mtype: 0 file, 5 dir, 2 symlink, 1 hardlink (dirs+links: bodyless tar entries) */
typedef struct {
    int kind; uint64_t frame_id; uint32_t level, sink_id;
    uint64_t block_id;                       /* J_COPY_BLOCK */
    uint32_t nmemb; Member *memb;            /* J_PACK_FILES (mode+rel path) / J_SCATTER */
    char *root;                              /* J_PACK_FILES: read root/rel, embed rel */
    int tar_compat;                          /* J_PACK_FILES: write hdr+body+pad (valid tar) */
    int64_t coff, clen;                      /* J_SCATTER */
} Job;
static void job_free(Job *j) { if (j->memb) { for (uint32_t i = 0; i < j->nmemb; i++) { free(j->memb[i].path); free(j->memb[i].hdr); free(j->memb[i].link); } free(j->memb); } free(j->root); free(j); }

typedef struct { Job **q; int cap, head, tail, n, done; pthread_mutex_t mu; pthread_cond_t ne, nf; } JQ;
static void jq_init(JQ *q, int cap){ q->q=calloc(cap,sizeof(Job*)); q->cap=cap; q->head=q->tail=q->n=q->done=0;
    pthread_mutex_init(&q->mu,0); pthread_cond_init(&q->ne,0); pthread_cond_init(&q->nf,0); }
static void jq_push(JQ *q, Job *j){ pthread_mutex_lock(&q->mu);
    if(q->n==q->cap){ int64_t t0=now_ns(); while(q->n==q->cap) pthread_cond_wait(&q->nf,&q->mu);
        ST(ns_qfull, now_ns()-t0); }
    q->q[q->tail]=j; q->tail=(q->tail+1)%q->cap; q->n++; pthread_cond_signal(&q->ne); pthread_mutex_unlock(&q->mu); }
static Job *jq_pop(JQ *q){ pthread_mutex_lock(&q->mu);
    if(q->n==0 && !q->done){ int64_t t0=now_ns(); while(q->n==0 && !q->done) pthread_cond_wait(&q->ne,&q->mu);
        ST(ns_idle, now_ns()-t0); }
    Job *j=NULL; if(q->n){ j=q->q[q->head]; q->head=(q->head+1)%q->cap; q->n--; pthread_cond_signal(&q->nf);} pthread_mutex_unlock(&q->mu); return j; }
static void jq_finish(JQ *q){ pthread_mutex_lock(&q->mu); q->done=1; pthread_cond_broadcast(&q->ne); pthread_mutex_unlock(&q->mu); }
static JQ g_jq;

/* ------------------------------------------------------------------ held decoded blocks
 * Lifecycle: a block is OWNED by its SCAN result. The planner issues copies (each takes a
 * read-ref at ENQUEUE, released when the worker finishes reading) then one RETIRE per block.
 * A block frees when RETIRED and no worker is still reading it (busy==0). This handles
 * skipped members for free (they just never take a copy-ref) and is race-safe: the ref is
 * taken in the single-threaded command loop BEFORE a later RETIRE can be processed. Using a
 * copy of an unknown/retired block is a planner bug -> caught, not UB. */
typedef struct { uint64_t id; uint8_t *buf; size_t len; int busy; char retired; } BBlock;
static BBlock **g_blocks; static int g_nblk, g_blkcap;
static int64_t g_live, g_budget = 512LL << 20;
static int g_fatal = 0;                                       /* a plan referenced a freed block */
static pthread_mutex_t g_blk_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_blk_cv = PTHREAD_COND_INITIALIZER;
static uint64_t g_next_block = 0;
static uint64_t blk_add(uint8_t *buf, size_t len) {           /* budget-bounded */
    pthread_mutex_lock(&g_blk_mu);
    /* wait only while something is LIVE to free — an oversize member (len > budget) with an
     * empty pool must proceed as its own frame (docs/ISA §frame), not deadlock: the old
     * `g_nblk > 0` guard was permanently true because g_nblk never shrinks. */
    while (g_live > 0 && g_live + (int64_t)len > g_budget) pthread_cond_wait(&g_blk_cv, &g_blk_mu);
    BBlock *b = calloc(1, sizeof(BBlock)); b->id = g_next_block++; b->buf = buf; b->len = len;
    if (g_nblk == g_blkcap) { g_blkcap = g_blkcap ? g_blkcap * 2 : 64; g_blocks = realloc(g_blocks, g_blkcap * sizeof(BBlock *)); }
    g_blocks[g_nblk++] = b; g_live += len; uint64_t id = b->id; pthread_mutex_unlock(&g_blk_mu);
    return id;
}
/* id IS the insertion index (ids are sequential, the array never compacts): O(1), was an
 * O(N) scan per lookup -> O(N^2) total under one mutex (~10^13 compares at EVI scale). */
static BBlock *blk_find(uint64_t id){ return id < (uint64_t)g_nblk ? g_blocks[id] : NULL; }
static BBlock *blk_get(uint64_t id){ pthread_mutex_lock(&g_blk_mu); BBlock *b=blk_find(id); pthread_mutex_unlock(&g_blk_mu); return b; }
static void _blk_free_locked(BBlock *b){ g_live-=b->len; free(b->buf);
    g_blocks[b->id]=NULL; free(b); pthread_cond_broadcast(&g_blk_cv); }
/* command loop: read-ref a block for a COPY at enqueue. 0 ok; -1 if gone/retired (plan bug). */
static int blk_ref(uint64_t id){ pthread_mutex_lock(&g_blk_mu); BBlock *b=blk_find(id);
    int ok=(b && !b->retired); if(ok) b->busy++; pthread_mutex_unlock(&g_blk_mu); return ok?0:-1; }
static void blk_release(uint64_t id){ pthread_mutex_lock(&g_blk_mu); BBlock *b=blk_find(id);   /* worker done reading */
    if(b && --b->busy==0 && b->retired) _blk_free_locked(b); pthread_mutex_unlock(&g_blk_mu); }
static void blk_retire(uint64_t id){ pthread_mutex_lock(&g_blk_mu); BBlock *b=blk_find(id);    /* planner FREE */
    if(b){ b->retired=1; if(b->busy==0) _blk_free_locked(b); } pthread_mutex_unlock(&g_blk_mu); }

/* ------------------------------------------------------------------ workers */
typedef struct { ZSTD_CCtx *c; ZSTD_DCtx *d; uint8_t *cb; size_t ccap; uint8_t *ob; size_t ocap;
                 uint8_t *mb; size_t mbcap; uint8_t *ctb; size_t ctcap; uint8_t *sb; size_t sbcap;
                 int64_t files; int err;
                 int id; int64_t jt0; int32_t jjq, jbusy;         /* per-frame cost accounting */
                 int ffd; char fpath[4096];                       /* last source fd, kept open */
                 struct io_uring ring; int ring_ok, ring_direct;  /* small-member batch reader */
                 int64_t *boff, *bgot; uint32_t bcap; } W;        /* per-member slot / bytes read */
/* STREAMING whole-file BLAKE3 + CDC manifest, in a fixed window instead of the whole file.
 * The manifest job used to materialize the entire file to hash it — the same unbounded
 * allocation that made a giant member OOM the pack path. BLAKE3 is incremental and FastCDC
 * never looks more than CDC_MAXSZ ahead, so a window comfortably larger than that produces
 * byte-identical results at O(window) memory. Returns 0, or -errno. */
#define MANIFEST_WIN (8u << 20)
/* ---- SMALL-MEMBER BATCH READS -------------------------------------------------
 * One open+read+close round trip per file is what a worker pays for a small member. Batching
 * the same work through io_uring is 2.03x at 31 KB on real data (bench/smallread.c, buffered,
 * cold, disjoint sets) -- and 0.95x at 64 KB, 0.98x at 128 KB, 0.90x at 512 KB. The win is
 * real but NARROW, so this is a size-triggered path, not a new I/O model: members above the
 * crossover keep the plain pread loop, where uring measured 1.00x exactly.
 *
 * The batch already exists. A frame targets ~1 MB of member bytes, so at 31 KB it holds ~32
 * members -- queue depth follows the job rather than a fixed constant, since there is nothing
 * to gain from a 256-deep ring for a 32-member frame.
 *
 * Reads land directly at each member's planner-assigned slot offset, so nothing is copied.
 * A member that fails here is simply left with bgot = 0; the caller zero-fills and emit_errs
 * exactly as the synchronous path does, and the cum-sum layout never shifts.
 */
/* Where the ring stops paying, re-measured with bench/uread over real files in disjoint
 * slices (GB/s, sync vs best ring backend):
 *     8 KB 0.046 -> 0.227 (4.9x)   128 KB 0.354 -> 2.308 (6.5x)   1 MB 1.985 -> 4.515 (2.3x)
 *    32 KB 0.122 -> 1.046 (8.6x)   256 KB 0.832 -> 3.250 (3.9x)   4 MB 4.912 -> 3.939 (SYNC WINS)
 * Below ~1 MB the cost is per-file latency and batching hides it; by 4 MB a single read already
 * saturates the fabric and the ring only adds overhead. The reversal is somewhere in 2-4 MB, so
 * this sits at 1 MB -- inside the region measured to win, not on an unmeasured boundary. The
 * old value was 48 KB, which sent everything from 48 KB to 1 MB down the 3-6x slower path. */
#define RING_MAX_MEMBER_DEFAULT (1 << 20)
static size_t g_ring_max = RING_MAX_MEMBER_DEFAULT;   /* BVM_RING_MAX_KB, for A/B-ing the bound */
#define RING_MAX_MEMBER g_ring_max
#define RING_MIN_MEMBERS 8             /* fewer than this and the setup outweighs the batch */
#define RING_QD_CAP 64

static int ring_prefetch(W *w, Job *j, const char *root, size_t rl) {
    uint32_t n = j->nmemb, small = 0;
    for (uint32_t i = 0; i < n; i++) {
        Member *m = &j->memb[i];
        if (m->mtype == 0 && m->size > 0 && m->size <= RING_MAX_MEMBER) small++;
    }
    if (small < RING_MIN_MEMBERS || getenv("BVM_NO_RING")) return 0;   /* not worth a batch */
    uint32_t qd = small < RING_QD_CAP ? small : RING_QD_CAP;
    if (!w->ring_ok) {
        if (io_uring_queue_init(RING_QD_CAP, &w->ring, 0) < 0) return 0;
        /* DIRECT DESCRIPTORS: openat_direct parks the file in a registered slot instead of the
         * process fd table, so there is no fd allocation, no table locking and no close()
         * syscall. Measured 22-47% over the plain-fd batch, margin growing with member size.
         * register_files_sparse needs kernel 5.19; on 5.15 the equivalent is registering an
         * explicit all -1 table. If neither works we fall back to the plain-fd path below.
         * NOT linked chains: on 5.15 a FIXED_FILE read resolves its file at prep time, before
         * a linked open has installed the slot, so open->read->close returns ECANCELED/EBADF
         * at every queue depth. Phased is what this kernel supports. */
        w->ring_direct = 0;
        if (io_uring_register_files_sparse(&w->ring, RING_QD_CAP) == 0) w->ring_direct = 1;
        else {
            int tbl[RING_QD_CAP];
            for (int t = 0; t < RING_QD_CAP; t++) tbl[t] = -1;
            if (io_uring_register_files(&w->ring, tbl, RING_QD_CAP) == 0) w->ring_direct = 1;
        }
        w->ring_ok = 1;
    }
    int *fds = malloc(sizeof(int) * qd);
    uint32_t *slot = malloc(sizeof(uint32_t) * qd);
    char **names = malloc(sizeof(char *) * qd);
    uint32_t i = 0, done = 0;
    while (i < n) {
        uint32_t k = 0;
        while (i < n && k < qd) {                        /* collect the next batch */
            Member *m = &j->memb[i];
            if (m->mtype == 0 && m->size > 0 && m->size <= RING_MAX_MEMBER) {
                size_t pl = strlen(m->path);
                names[k] = malloc(rl + pl + 1);
                memcpy(names[k], root, rl); memcpy(names[k] + rl, m->path, pl);
                names[k][rl + pl] = 0;
                slot[k] = i; fds[k] = -1; k++;
            }
            i++;
        }
        if (!k) break;
        for (uint32_t x = 0; x < k; x++) {               /* phase 1: every open at once */
            struct io_uring_sqe *s = io_uring_get_sqe(&w->ring);
            if (w->ring_direct)                          /* into registered slot x, not an fd */
                io_uring_prep_openat_direct(s, AT_FDCWD, names[x], O_RDONLY, 0, x);
            else
                io_uring_prep_openat(s, AT_FDCWD, names[x], O_RDONLY, 0);
            io_uring_sqe_set_data64(s, x);
        }
        io_uring_submit(&w->ring);
        for (uint32_t x = 0; x < k; x++) {
            struct io_uring_cqe *cq;
            if (io_uring_wait_cqe(&w->ring, &cq) < 0) break;
            /* direct: res is 0 on success and the SLOT is x, so record x (not a real fd) */
            uint32_t ix = (uint32_t)io_uring_cqe_get_data64(cq);
            fds[ix] = w->ring_direct ? (cq->res == 0 ? (int)ix : -1) : cq->res;
            io_uring_cqe_seen(&w->ring, cq);
        }
        uint32_t live = 0;                               /* phase 2: every read at once */
        for (uint32_t x = 0; x < k; x++) {
            if (fds[x] < 0) continue;
            Member *m = &j->memb[slot[x]];
            struct io_uring_sqe *s = io_uring_get_sqe(&w->ring);
            io_uring_prep_read(s, fds[x], w->ob + w->boff[slot[x]], (unsigned)m->size,
                               (unsigned long long)m->out_off);
            if (w->ring_direct) s->flags |= IOSQE_FIXED_FILE;   /* fds[x] is a slot index */
            io_uring_sqe_set_data64(s, x);
            live++;
        }
        if (live) io_uring_submit(&w->ring);
        for (uint32_t x = 0; x < live; x++) {
            struct io_uring_cqe *cq;
            if (io_uring_wait_cqe(&w->ring, &cq) < 0) break;
            uint32_t y = (uint32_t)io_uring_cqe_get_data64(cq);
            w->bgot[slot[y]] = cq->res > 0 ? cq->res : 0;
            done++;
            io_uring_cqe_seen(&w->ring, cq);
        }
        if (w->ring_direct) {                            /* phase 3: free the slots on the ring */
            uint32_t nc = 0;
            for (uint32_t x = 0; x < k; x++) if (fds[x] >= 0) {
                struct io_uring_sqe *s = io_uring_get_sqe(&w->ring);
                io_uring_prep_close_direct(s, x);
                io_uring_sqe_set_data64(s, x); nc++;
            }
            if (nc) io_uring_submit(&w->ring);
            for (uint32_t x = 0; x < nc; x++) {
                struct io_uring_cqe *cq;
                if (io_uring_wait_cqe(&w->ring, &cq) < 0) break;
                io_uring_cqe_seen(&w->ring, cq);
            }
        } else {
            for (uint32_t x = 0; x < k; x++) if (fds[x] >= 0) close(fds[x]);
        }
        for (uint32_t x = 0; x < k; x++) free(names[x]);
    }
    free(fds); free(slot); free(names);
    ST(n_open, small); ST(ring_batches, 1); ST(ring_members, done);
    return (int)done;
}

static int manifest_stream(int fd, W *w, int64_t *dg_out, size_t *mlen_out, int64_t *nread){
    blake3_hasher hs; blake3_hasher_init(&hs);
    if (ensure(&w->ob, &w->ocap, MANIFEST_WIN)) return -ENOMEM;
    size_t have = 0, mw; uint32_t nc = 0; int eof = 0; int64_t total = 0;
    man_init(&w->mb, &w->mbcap, &mw);
    for (;;){
        while (!eof && have < MANIFEST_WIN){
            ssize_t r = read(fd, w->ob + have, MANIFEST_WIN - have);
            if (r < 0){ if (errno == EINTR) continue; return -errno; }
            if (r == 0){ eof = 1; break; }
            blake3_hasher_update(&hs, w->ob + have, (size_t)r);   /* hash bytes as they arrive */
            have += (size_t)r; total += r;
        }
        size_t used = 0, cut;
        while ((cut = cdc_next(w->ob + used, have - used, eof)) != 0){
            man_add(&w->mb, &w->mbcap, &mw, &nc, w->ob + used, cut); used += cut;
        }
        if (used && used < have) memmove(w->ob, w->ob + used, have - used);
        have -= used;
        if (eof) break;
    }
    man_end(w->mb, nc);
    uint8_t h8[8]; blake3_hasher_finalize(&hs, h8, 8); memcpy(dg_out, h8, 8);
    *mlen_out = total >= CDC_CUTOFF ? mw : 0;                    /* small files: digest only */
    *nread = total;
    return 0;
}
/* build a VALID zstd frame of Raw_Blocks ("stored" frame): magic + single-segment header
 * (8-byte FCS) + [3B block hdr + <=128KB verbatim bytes]... Any zstd decodes it at
 * memcpy speed; `zstd -dc` and the tar view keep working; overhead 13B + 3B/128KB
 * (~0.0023%). Returns the frame length. */
static size_t stored_frame_len(size_t len){ size_t nb=(len+131071)/131072; if(!nb) nb=1;
    return 13 + nb*3 + len; }
static void stored_frame_hdr(uint8_t *h, size_t len){         /* the fixed 13-byte prologue */
    h[0]=0x28; h[1]=0xB5; h[2]=0x2F; h[3]=0xFD; h[4]=0xE0;
    uint64_t fcs = len; memcpy(h+5, &fcs, 8); }
static void stored_blk_hdr(uint8_t *h, size_t n, int last){
    uint32_t bh = (uint32_t)last | (0u << 1) | ((uint32_t)n << 3); memcpy(h, &bh, 3); }
static int pwritev_all(int fd, struct iovec *iov, int n, off_t off){
    while (n > 0){
        ssize_t r = pwritev(fd, iov, n, off);
        if (r < 0){ if (errno==EINTR) continue; return -1; }
        if (r == 0){ errno = EIO; return -1; }
        off += r;
        while (n > 0 && (size_t)r >= (ssize_t)iov->iov_len){ r -= iov->iov_len; iov++; n--; }
        if (n > 0 && r){ iov->iov_base = (char*)iov->iov_base + r; iov->iov_len -= r; }
    }
    return 0;
}
/* STORED FRAME, WITHOUT THE SECOND COPY. The payload is already sitting in the pack buffer;
 * the only reason the buffered version below exists is to interleave a 3-byte Raw_Block
 * header every 128 KiB. writev does that interleave in the kernel, so we skip a full-size
 * memcpy AND the compressBound-sized scratch allocation — on a real home backup that is
 * 567 GB of memcpy and the bulk of the pack-buffer pressure. (The bytes still pass through
 * RAM once: we must read them to compute the BLAKE3 digest + CDC manifest, which is why
 * copy_file_range is not an option on the pack side.) */
static int stored_frame_pwritev(int fd, const uint8_t *src, size_t len, off_t off, uint8_t *bhs) {
    uint8_t fh[13]; stored_frame_hdr(fh, len);
    size_t nb = (len + 131071) / 131072; if (!nb) nb = 1;
    struct iovec iov[1024];
    size_t b = 0, o = 0; off_t cur = off; int first = 1;
    while (b < nb) {
        int k = 0; off_t base = cur;
        if (first) { iov[k].iov_base = fh; iov[k].iov_len = 13; k++; first = 0; cur += 13; }
        while (b < nb && k <= 1022) {                     /* IOV_MAX is 1024: 2 iovecs/block */
            size_t bl = len - o > 131072 ? 131072 : len - o;
            stored_blk_hdr(bhs + b*3, bl, b + 1 == nb);
            iov[k].iov_base = bhs + b*3;      iov[k].iov_len = 3;  k++;
            iov[k].iov_base = (void *)(src + o); iov[k].iov_len = bl; k++;
            o += bl; b++; cur += 3 + bl;
        }
        if (pwritev_all(fd, iov, k, base)) return -1;
    }
    return 0;
}
static size_t zstd_stored_frame(uint8_t *dst, const uint8_t *src, size_t len) {
    uint8_t *q = dst;
    *q++ = 0x28; *q++ = 0xB5; *q++ = 0x2F; *q++ = 0xFD;   /* magic (LE) */
    *q++ = 0xE0;                                           /* FHD: FCS_flag=3, Single_Segment */
    uint64_t fcs = len; memcpy(q, &fcs, 8); q += 8;
    size_t off = 0;
    do {
        size_t n = len - off > 131072 ? 131072 : len - off;
        uint32_t last = (off + n == len);
        uint32_t bh = last | (0u << 1) | ((uint32_t)n << 3);   /* Raw_Block */
        memcpy(q, &bh, 3); q += 3;
        memcpy(q, src + off, n); q += n; off += n;
    } while (off < len);
    return (size_t)(q - dst);
}
static void do_compress(W *w, uint8_t *src, size_t len, uint64_t frame_id, uint32_t level, uint32_t sink_id,
                        uint8_t *stat, uint32_t statlen, int allow_raw) {
    /* INCOMPRESSIBLE data (model weights, opus/jpeg...) skips the codec: probe a few 128KB
     * windows at level 1; if they won't shrink, emit a STORED frame (Raw_Blocks) — still a
     * valid zstd frame, so readers/`zstd -dc`/tar-compat are untouched while both encode and
     * decode run at memcpy speed. (allow_raw kept for call-site symmetry; stored frames are
     * legal everywhere.)
     *
     * The probe is STRATIFIED, not head-only. A big member is its own frame, so one window at
     * offset 0 decided the fate of the whole file — and plenty of incompressible formats open
     * with something that compresses (safetensors' JSON header, tar/zip headers, an embedded
     * index). That mis-verdict sent multi-GB bodies through level-6 zstd for ~0% gain. Up to
     * PROBE_MAX evenly-spaced windows cost ~2MB of level-1 work no matter how big the member
     * is — a few ms against minutes of pointless codec. The reverse error matters too: a
     * compressible body behind an incompressible header would have been stored raw. */
    (void)allow_raw;
    int inc = 0;
    if (len >= 262144) {
        /* One window per 2 MB, capped at 16. Scaling per 64 MB gave a 16 MB piece exactly ONE
         * window, at offset 0 -- head-only sampling, reintroduced at piece granularity once
         * frame_cap started splitting big files, and applied ~400k times in a whole-tree
         * backup. Measured cost: 1,472 GB stored codec-free against 567 GB for the same tree
         * under whole-file framing, i.e. 2.6x more data skipping the codec because one
         * unlucky 128 KB head decided for the whole piece. The probe stays under 12.5% of
         * any frame (1 window per 1 MB at the small end, 8 of a 16 MB piece, 16 above 32 MB). */
        enum { PROBE_MAX = 16, PROBE_SPAN = 2u << 20 };
        size_t sn = 131072;
        int np = (int)(len / PROBE_SPAN);
        if (np > PROBE_MAX) np = PROBE_MAX;
        if (np < 1) np = 1;
        if ((size_t)np * sn > len) np = (int)(len / sn);
        if (np < 1) np = 1;
        if (ensure(&w->cb, &w->ccap, ZSTD_compressBound(sn))) { w->err = -1; return; }
        size_t r[PROBE_MAX]; int nr = 0, bad = 0;
        int64_t tp = now_ns();
        for (int i = 0; i < np; i++) {
            size_t off = np == 1 ? 0 : (len - sn) / (size_t)(np - 1) * (size_t)i;
            size_t pl_ = ZSTD_compressCCtx(w->c, w->cb, w->ccap, src + off, sn, 1);
            if (ZSTD_isError(pl_)) { bad = 1; break; }
            int k = nr++;                                  /* insertion sort: np <= 16 */
            while (k > 0 && r[k - 1] > pl_) { r[k] = r[k - 1]; k--; }
            r[k] = pl_;
        }
        ST(ns_comp, now_ns() - tp); ST(probe_in, (int64_t)nr * (int64_t)sn); ST(n_probe, nr);
        /* MEDIAN, not the mean of the windows. Equal-size windows sample the RATE, not the
         * file: window 0 sits on the header, so a 256KB JSON prologue in a 512MB tensor file
         * is 1/9 of the sample but 1/2000 of the bytes — averaging let it veto the fast path
         * for the whole member. The median ignores a minority of outlier windows in either
         * direction, and still flips honestly once a real fraction of the file compresses. */
        inc = !bad && nr && r[nr / 2] >= sn - (sn >> 5);    /* < ~3% gain: incompressible */
    }
    Sink *sk = &g_sinks[sink_id];
    /* stored + file sink: write the frame straight out of `src` with writev (no second copy,
     * no compressBound scratch). Socket sinks need one contiguous message; O_DIRECT sinks
     * cannot use writev either, because the 3-byte Raw_Block headers make the iovecs
     * unaligned. Decide ONCE -- the scratch allocation and the write path must agree, or an
     * incompressible frame skips the compressBound alloc and then writes len bytes into a
     * buffer sized for 3 bytes per 128 KiB (heap corruption, found exactly that way). */
    int direct = inc && !sk->is_sock && !g_sink_direct;
    if (!direct && ensure(&w->cb, &w->ccap, ZSTD_compressBound(len) + SINK_ALIGN)) { w->err = -1; return; }
    /* stored + file sink: write the frame straight out of `src` with writev (no second
     * copy, no compressBound scratch). Socket sinks still need a contiguous message. */
    int64_t t0 = now_ns();
    size_t cl;
    if (inc) {
        cl = stored_frame_len(len);
        if (!direct) cl = zstd_stored_frame(w->cb, src, len);
        ST(n_raw_frames, 1); ST(raw_bytes, len);
    } else {
        cl = ZSTD_compressCCtx(w->c, w->cb, w->ccap, src, len, (int)level);
    }
    ST(ns_comp, now_ns() - t0); ST(comp_in, len); ST(comp_out, cl);
    if (!inc && ZSTD_isError(cl)) { w->err = -1; return; }
    if (sk->is_sock) {                                  /* stream: [u32 L][ msg | AEAD(msg) ] */
        size_t msglen = 20 + statlen + cl; uint64_t c = cl;   /* msg=[frame_id][clen][statlen][stat][compressed] */
        if (ensure(&w->mb, &w->mbcap, msglen) ||
            (sk->enc && ensure(&w->ctb, &w->ctcap, msglen + crypto_secretstream_xchacha20poly1305_ABYTES))) { w->err = -1; return; }
        uint8_t *q = w->mb; memcpy(q, &frame_id, 8); q += 8; memcpy(q, &c, 8); q += 8;
        memcpy(q, &statlen, 4); q += 4; if (statlen) { memcpy(q, stat, statlen); q += statlen; } memcpy(q, w->cb, cl);
        pthread_mutex_lock(&sk->mu);
        if (sk->enc) {
            unsigned long long ctlen = 0;
            crypto_secretstream_xchacha20poly1305_push(&sk->st, w->ctb, &ctlen, w->mb, msglen, NULL, 0, 0);
            uint32_t L = (uint32_t)ctlen; write_all(sk->fd, &L, 4); write_all(sk->fd, w->ctb, ctlen);
        } else { uint32_t L = (uint32_t)msglen; write_all(sk->fd, &L, 4); write_all(sk->fd, w->mb, msglen); }
        pthread_mutex_unlock(&sk->mu);
        record_done(frame_id, (uint64_t)-1, cl);
    } else {                                            /* file sink: pwrite at reserved offset */
        size_t wlen = cl;                                /* padding room reserved above */
        pthread_mutex_lock(&sk->mu);
        if (g_sink_direct) {                             /* 4 KB-aligned slot, rounded length */
            sk->cursor = align_up(sk->cursor, SINK_ALIGN);
            wlen = (size_t)align_up((int64_t)cl, SINK_ALIGN);
        }
        int64_t coff = sk->cursor; sk->cursor += (int64_t)wlen;
        pthread_mutex_unlock(&sk->mu);
        int64_t tw = now_ns(); int wrc;
        if (direct) {                                   /* block headers only: 3B per 128KB */
            size_t nb = (len + 131071) / 131072; if (!nb) nb = 1;
            if (ensure(&w->cb, &w->ccap, nb * 3)) { w->err = -ENOMEM; emit_err("stored-frame", -ENOMEM); return; }
            wrc = stored_frame_pwritev(sk->fd, src, len, coff, w->cb);
        } else {
            wrc = pwrite_all(sk->fd, w->cb, wlen, coff);   /* wlen == cl unless direct */
        }
        ST(ns_write, now_ns() - tw); ST(wr_bytes, cl);
        /* STREAMING-WRITE HYGIENE: we never re-read the sink; without this, TBs of dirty
         * page cache accrue against the job's CGROUP (which counts cache, not just RSS) and
         * the kernel OOMs the process at ~healthy RSS.
         *
         * Flush the RANGE JUST WRITTEN, not the whole file. offset 0 / len 0 means the entire
         * sink, so every 256 MB the kernel was asked to walk and write back a file that grows
         * to 128 GB -- work quadratic in shard size, performed ~512 times per shard. It is the
         * same mistake the read path had (fadvise 0,0 evicting a whole file per piece), and it
         * is why workers sat 75-92% of their time in write() while the fabric idled: writes ran
         * at 1.33 GB/s against a 2.93 GB/s concurrent ceiling. */
        pthread_mutex_lock(&sk->mu);
        sk->unflushed += cl;
        int64_t lo = -1, hi = -1;
        if (g_flush_bytes && sk->unflushed >= g_flush_bytes) {
            lo = sk->flushed; hi = sk->cursor; sk->flushed = hi; sk->unflushed = 0;
        }
        pthread_mutex_unlock(&sk->mu);
        if (lo >= 0 && hi > lo) {
            (void)!sync_file_range(sk->fd, lo, hi - lo, SYNC_FILE_RANGE_WRITE);
            (void)posix_fadvise(sk->fd, lo, hi - lo, POSIX_FADV_DONTNEED);
        }
        if (wrc) {                                          /* real ENOSPC/EIO (short writes now looped) */
            char fb[64]; snprintf(fb, sizeof fb, "frame %llu", (unsigned long long)frame_id);
            w->err = -errno; emit_err(fb, errno ? -errno : -EIO); return;
        }
        record_done(frame_id, (uint64_t)coff, cl);
    }
}
int g_busy;                                      /* workers currently EXECUTING a job */
static void *worker(void *arg) {
    W *w = arg; char full[4096];
    w->ffd = -1;
    for (;;) {
        Job *j = jq_pop(&g_jq); if (!j) break;
        w->jt0 = now_ns() - g_start_ns;                  /* queue state AT START, not at end: */
        w->jjq = (int32_t)g_jq.n;                        /* a frame that waited on a full queue */
        w->jbusy = (int32_t)__atomic_load_n(&g_busy, __ATOMIC_RELAXED);   /* is the thing to see */
        __atomic_fetch_add(&g_busy, 1, __ATOMIC_RELAXED);
        if (j->kind == J_COPY_BLOCK) {
            BBlock *b = blk_get(j->block_id);               /* ref held since enqueue -> present */
            if (b) do_compress(w, b->buf, b->len, j->frame_id, j->level, j->sink_id, NULL, 0, 0);
            blk_release(j->block_id);                        /* release read-ref (free if retired) */
        } else if (j->kind == J_PACK_FILES) {
            int64_t est = 0;
            for (uint32_t i = 0; i < j->nmemb; i++) {
                Member *m = &j->memb[i];
                int64_t psz = m->mtype == 0 ? m->size : 0;
                est += (j->tar_compat ? tar_hlen2(strlen(m->path), m->link ? strlen(m->link) : 0) : 0)
                     + psz + (j->tar_compat ? 512 : 0);
            }
            int64_t resv = est * 2 + (16 << 20);          /* frame + compressBound-ish */
            pack_reserve(resv);
            /* LAYOUT IS THE PLANNER'S: every member occupies exactly hlen + m->size (+pad)
             * bytes at the planner's cum-sum offset, no matter what happens at read time —
             * an unreadable/shrunk/grown file zero-fills its slot and reports emit_err
             * instead of SHIFTING every later member (the old code skipped failed members,
             * silently desyncing the footer's offsets from the bytes). */
            char full[4096]; size_t rl = strlen(j->root); memcpy(full, j->root, rl);
            if (rl && full[rl - 1] != '/') full[rl++] = '/';   /* root/ + rel */
            /* Pass 1: the planner's layout is deterministic, so every member's slot offset is
             * known before a byte is read. That is what lets the small members be fetched as
             * ONE batch straight into their slots. */
            if (j->nmemb > w->bcap) {
                w->boff = realloc(w->boff, sizeof(int64_t) * j->nmemb);
                w->bgot = realloc(w->bgot, sizeof(int64_t) * j->nmemb);
                w->bcap = j->nmemb;
            }
            {
                size_t at = 0; int ok = 1;
                for (uint32_t i = 0; i < j->nmemb; i++) {
                    Member *m = &j->memb[i];
                    int64_t psz = m->mtype == 0 ? m->size : 0;
                    int64_t hl = j->tar_compat
                        ? tar_hlen2(strlen(m->path), m->link ? strlen(m->link) : 0) : 0;
                    w->boff[i] = (int64_t)(at + hl);
                    w->bgot[i] = -1;                     /* -1 = not prefetched */
                    at += hl + psz + (j->tar_compat ? (((psz + 511) & ~511LL) - psz) : 0);
                }
                if (!ensure(&w->ob, &w->ocap, at)) ring_prefetch(w, j, full, rl);
                else ok = 0;
                (void)ok;
            }
            size_t blen = 0;
            int frame_lost = 0;                          /* slot never materialized -> see below */
            BC sb = {0};                                 /* stat blob = TYPED BSTAT Arrow batch:
                                                          * full metadata crosses the data plane
                                                          * (rx applies mtime/uid/gid, dirs, links) */
            for (uint32_t i = 0; i < j->nmemb; i++) {
                Member *m = &j->memb[i]; size_t pl = strlen(m->path);
                int64_t psz = m->mtype == 0 ? m->size : 0;   /* bodyless types: header only */
                int64_t pad = j->tar_compat ? (((psz + 511) & ~511LL) - psz) : 0;
                int64_t hlen = j->tar_compat ? tar_hlen2(pl, m->link ? strlen(m->link) : 0) : 0;
                if (ensure(&w->ob, &w->ocap, blen + hlen + psz + pad)) {
                    /* Cannot buffer this member's slot (a member is materialized whole — a
                     * 50 TiB SPARSE file asks for a 50 TiB buffer). The old code broke out of
                     * the loop but still compressed+recorded the frame, so the footer ended up
                     * pointing every member of this frame at a 9-byte EMPTY zstd frame, with
                     * digest -1 so `verify` skipped it and the run reported lost=0. Fail the
                     * whole frame instead: an unrecorded frame is exactly what the planner
                     * already knows how to handle — it drops its members and reports them
                     * LOST, which is the truth. */
                    w->err = -ENOMEM; emit_err(m->path, -ENOMEM); frame_lost = 1; break;
                }
                if (j->tar_compat)                       /* header SYNTHESIZED here from STAT cols */
                    blen += tar_emit_hdr2(w->ob + blen, m->path, m->link, m->mtype,
                                          m->mode, m->uid, m->gid, (uint64_t)psz, m->mtime / 1000000000LL);
                int64_t body_off = blen; int64_t got = 0;
                if (m->mtype != 0) { }                   /* dir/link: no body to read */
                else if (w->bgot[i] >= 0) {              /* already fetched in the batch above */
                    got = w->bgot[i];
                    if (got != psz) { w->err = -EIO; emit_err(m->path, -EIO); }
                }
                else if (rl + pl + 1 >= sizeof full) { w->err = -ENAMETOOLONG; emit_err(m->path, -ENAMETOOLONG); }
                else {
                    memcpy(full + rl, m->path, pl); full[rl + pl] = 0;
                    /* KEEP THE FD. A split member's pieces are separate members, so a 30 GB
                     * file used to cost ~1,900 open/close pairs on the same inode -- one per
                     * piece. Pieces are dispatched in order across N workers, so each worker
                     * sees this file again every Nth piece: a one-entry cache hits for exactly
                     * the big files that generate the pieces. */
                    int fd;
                    if (w->ffd >= 0 && !strcmp(w->fpath, full)) {
                        fd = w->ffd;
                    } else {
                        if (w->ffd >= 0) close(w->ffd);
                        int64_t to = now_ns();
                        fd = open(full, O_RDONLY);
                        int64_t od = now_ns() - to; ST(ns_open, od); ST(n_open, 1);
                        if (od > 500000) ST(slow_open, 1);
                        w->ffd = fd;
                        if (fd >= 0) { memcpy(w->fpath, full, rl + pl + 1); }
                    }
                    if (fd < 0) { w->ffd = -1; w->err = -errno; emit_err(full, -errno); }
                    else { ssize_t r; int64_t tr = now_ns();  /* read AT MOST psz from src_off */
                        while (got < psz && (r = pread(fd, w->ob + blen + got, psz - got, m->out_off + got)) > 0) got += r;
                        ST(ns_read, now_ns() - tr); ST(rd_bytes, got);
                        /* Drop ONLY what we read. offset 0 / len 0 means the WHOLE FILE: with
                         * pieces of one file in flight on many workers at once, every piece
                         * evicted every other piece's pages -- and the readahead with them. */
                        (void)posix_fadvise(fd, m->out_off, psz, POSIX_FADV_DONTNEED);
                        if (got != psz) { w->err = -EIO; emit_err(full, -EIO); }   /* shrunk/short: report */
                    }
                }
                if (got < psz) memset(w->ob + blen + got, 0, psz - got);          /* keep the slot's extent */
                blen += psz;
                if (j->tar_compat) { memset(w->ob + blen, 0, pad); blen += pad; }  /* pad to 512 */
                bc_add_raw(&sb, m->path, m->link,
                           m->mtype == 0 ? body_off : (m->mtype == 1 ? 1 : 0),   /* files: body off; links: kind */
                           psz, m->mode, m->mtime, m->uid, m->gid, m->mtype == 5);
                if (m->mtype == 0) {                     /* body in hand: digest (+manifest >=128K) */
                    int64_t th = now_ns();
                    int64_t dg = blake3_i64(w->ob + body_off, (size_t)psz);
                    size_t mlen = 0;
                    if (psz >= CDC_CUTOFF)
                        mlen = cdc_manifest(w->ob + body_off, (size_t)psz, &w->mb, &w->mbcap);
                    ST(ns_hash, now_ns() - th);
                    bc_tail_digest(&sb, dg, w->mb, mlen);
                }
            }
            if (!frame_lost) {
                size_t slen; uint8_t *sblob = bc_arrow(&sb, &slen);
                do_compress(w, w->ob, blen, j->frame_id, j->level, j->sink_id, sblob, (uint32_t)slen,
                            !j->tar_compat);
                free(sblob);
                emit_bstat(0, (int64_t)j->frame_id, &sb); /* digests -> planner (footer) */
            }
            bc_free(&sb);
            pack_release(resv);
            wshrink(&w->ob, &w->ocap); wshrink(&w->cb, &w->ccap); wshrink(&w->mb, &w->mbcap);
        } else if (j->kind == J_MANIFEST) {
            /* delta-backup planning primitive: per path, whole-file BLAKE3 + CDC/BLAKE3
             * manifest, emitted as a BSTAT batch (sid rides frame_id). No frames written. */
            char full[4096]; size_t rl = strlen(j->root); memcpy(full, j->root, rl);
            if (rl && full[rl - 1] != '/') full[rl++] = '/';
            BC sb = {0};
            for (uint32_t i = 0; i < j->nmemb; i++) {
                Member *m = &j->memb[i]; size_t pl = strlen(m->path);
                if (rl + pl + 1 >= sizeof full) { emit_err(m->path, -ENAMETOOLONG); continue; }
                memcpy(full + rl, m->path, pl); full[rl + pl] = 0;
                int fd = open(full, O_RDONLY);
                if (fd < 0) { emit_err(full, -errno); continue; }
                int64_t dg = 0, got = 0; size_t mlen = 0;
                int mrc = manifest_stream(fd, w, &dg, &mlen, &got);   /* BOUNDED: see above */
                (void)posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);           /* one-shot read */
                close(fd);
                if (mrc) { emit_err(full, mrc); continue; }
                bc_add_raw(&sb, m->path, NULL, 0, got, 0, 0, 0, 0, 0);
                bc_tail_digest(&sb, dg, w->mb, mlen);
            }
            emit_bstat((uint32_t)j->frame_id, 0, &sb);    /* block 0 -> ALWAYS emitted (even empty) */
            bc_free(&sb);
        } else if (j->kind == J_COPY_MEMBERS) {
            /* assemble a frame from a SUBSET of a held block's members — IDENTICAL to
             * J_PACK_FILES except the body source is the block, not open()ed files: same
             * planner-size layout truth, same C-synthesized tar headers (tar_emit_hdr),
             * same zero-fill + emit_err discipline on a bad source range. */
            BBlock *b = blk_get(j->block_id);               /* ref held since enqueue -> present */
            if (!b) { w->err = -1; blk_release(j->block_id); __atomic_fetch_sub(&g_busy,1,__ATOMIC_RELAXED); job_free(j); continue; }
            int64_t est = 0;
            for (uint32_t i = 0; i < j->nmemb; i++) {
                Member *m = &j->memb[i];
                est += (m->mtype == 0 ? m->size : 0) + 1024;
            }
            int64_t resv = est * 2 + (16 << 20);
            pack_reserve(resv);
            size_t blen = 0; BC sb = {0};                /* typed BSTAT stat blob (see J_PACK_FILES) */
            for (uint32_t i = 0; i < j->nmemb; i++) {
                Member *m = &j->memb[i]; size_t pl = strlen(m->path);
                int64_t sz = m->mtype == 0 ? m->size : 0;    /* bodyless types: header only */
                int64_t pad = j->tar_compat ? (((sz + 511) & ~511LL) - sz) : 0;
                int64_t hlen = j->tar_compat ? tar_hlen2(pl, m->link ? strlen(m->link) : 0) : 0;
                if (ensure(&w->ob, &w->ocap, blen + hlen + sz + pad)) { w->err = -ENOMEM; emit_err(m->path, -ENOMEM); break; }
                if (j->tar_compat)
                    blen += tar_emit_hdr2(w->ob + blen, m->path, m->link, m->mtype,
                                          m->mode, m->uid, m->gid, (uint64_t)sz, m->mtime / 1000000000LL);
                int64_t body_off = blen;
                if (m->mtype != 0) { }                       /* no body */
                else if (m->in_off >= 0 && (size_t)(m->in_off + sz) <= b->len) memcpy(w->ob + blen, b->buf + m->in_off, sz);
                else { memset(w->ob + blen, 0, (size_t)sz); w->err = -EIO; emit_err(m->path, -EIO); }
                blen += sz;
                if (j->tar_compat) { memset(w->ob + blen, 0, pad); blen += pad; }
                bc_add_raw(&sb, m->path, m->link,
                           m->mtype == 0 ? body_off : (m->mtype == 1 ? 1 : 0),
                           sz, m->mode, m->mtime, m->uid, m->gid, m->mtype == 5);
            }
            size_t slen; uint8_t *sblob = bc_arrow(&sb, &slen); bc_free(&sb);
            do_compress(w, w->ob, blen, j->frame_id, j->level, j->sink_id, sblob, (uint32_t)slen,
                        !j->tar_compat);
            free(sblob);
            pack_release(resv);
            wshrink(&w->ob, &w->ocap); wshrink(&w->cb, &w->ccap);
            blk_release(j->block_id);                        /* release read-ref (free if retired) */
        } else if (j->kind == J_SCATTER) {
            char fb[64];
            int nockfd = g_nock_fds[j->block_id];
            /* OUR stored frames have DETERMINISTIC geometry (13B header, 3B block hdr per
             * 128KiB): member bytes map to file ranges arithmetically -> copy_file_range
             * them nock->dest with ZERO userspace copies (no frame read, no decode); falls
             * back to pread/pwrite per run, then to the buffered path on any mismatch. */
            uint8_t shd[13]; uint64_t fcs = 0; int stored = 0;
            if (pread(nockfd, shd, 13, j->coff) == 13 &&
                shd[0] == 0x28 && shd[1] == 0xB5 && shd[2] == 0x2F && shd[3] == 0xFD && shd[4] == 0xE0) {
                memcpy(&fcs, shd + 5, 8);
                uint64_t nblk = fcs ? (fcs + 131071) / 131072 : 1;
                if ((uint64_t)j->clen == 13 + nblk * 3 + fcs) stored = 1;
            }
            if (stored) {
                for (uint32_t i = 0; i < j->nmemb; i++) {
                    Member *m = &j->memb[i]; size_t pl = strlen(m->path);
                    if (g_destlen + pl + 1 >= sizeof full) { emit_err(m->path, -ENAMETOOLONG); continue; }
                    memcpy(full, g_dest, g_destlen);
                    memcpy(full + g_destlen, m->path, pl); full[g_destlen + pl] = 0;
                    int64_t to = now_ns();
                    int fd = open(full, O_WRONLY | O_CREAT, m->mode & 07777);
                    if (fd < 0 && errno == ENOENT) { mkparents(full); fd = open(full, O_WRONLY | O_CREAT, m->mode & 07777); }
                    int64_t od = now_ns() - to; ST(ns_open, od); ST(n_open, 1);
                    if (od > 500000) ST(slow_open, 1);
                    if (fd < 0) { w->err = -errno; emit_err(full, -errno); continue; }
                    (void)!ftruncate(fd, m->fsize);
                    if ((uint64_t)(m->in_off + m->size) > fcs) { emit_err(full, -EIO); close(fd); continue; }
                    int64_t tw2 = now_ns();
                    int64_t rem = m->size, mo = m->in_off; off_t dsto = m->out_off; int bad = 0;
                    while (rem > 0 && !bad) {
                        int64_t blk = mo / 131072, inblk = mo % 131072;
                        int64_t n = rem < 131072 - inblk ? rem : 131072 - inblk;
                        off_t so = j->coff + 13 + blk * 131075 + 3 + inblk;
                        int64_t doneb = 0;
                        while (doneb < n) {
                            ssize_t r = copy_file_range(nockfd, &so, fd, &dsto, (size_t)(n - doneb), 0);
                            if (r > 0) { doneb += r; ST(cfr_bytes, r); continue; }
                            /* CFR unsupported/cross-fs: pread/pwrite this run (still unbuffered frame) */
                            char tbuf[65536];
                            size_t take = (size_t)(n - doneb) < sizeof tbuf ? (size_t)(n - doneb) : sizeof tbuf;
                            ssize_t rr = pread(nockfd, tbuf, take, so);
                            if (rr <= 0 || pwrite(fd, tbuf, (size_t)rr, dsto) != rr) { bad = 1; break; }
                            so += rr; dsto += rr; doneb += rr;
                        }
                        rem -= n; mo += n;
                    }
                    ST(ns_write, now_ns() - tw2); ST(wr_bytes, m->size - rem);
                    if (bad) { w->err = -EIO; emit_err(full, -EIO); }
                    int64_t tm3 = now_ns(); int didm = 0;
                    if ((g_scatter_meta & 2) && (m->uid || m->gid)) {
                        (void)!fchown(fd, m->uid, m->gid); didm = 1;
                        if (m->mode & 06000) (void)!fchmod(fd, m->mode & 07777);
                    }
                    if (g_scatter_meta & 1) {
                        struct timespec ts[2]; ts[0].tv_nsec = UTIME_OMIT;
                        ts[1].tv_sec = m->mtime / 1000000000LL; ts[1].tv_nsec = m->mtime % 1000000000LL;
                        futimens(fd, ts); didm = 1;
                    }
                    if (didm) { ST(ns_meta, now_ns() - tm3); ST(n_meta, 1); }
                    close(fd); w->files++;
                }
                __atomic_fetch_sub(&g_busy,1,__ATOMIC_RELAXED); job_free(j); continue;
            }
            int64_t tr0 = now_ns();
            if (ensure(&w->cb, &w->ccap, (size_t)j->clen) || pread(nockfd, w->cb, j->clen, j->coff) != j->clen) {
                snprintf(fb, sizeof fb, "frame@%lld+%lld", (long long)j->coff, (long long)j->clen);
                w->err = -EIO; emit_err(fb, -EIO); __atomic_fetch_sub(&g_busy,1,__ATOMIC_RELAXED); job_free(j); continue; }
            ST(ns_read, now_ns() - tr0); ST(rd_bytes, j->clen);
            size_t got; uint8_t *body;
            uint32_t fm = j->clen >= 4 ? (uint32_t)w->cb[0] | ((uint32_t)w->cb[1] << 8) |
                          ((uint32_t)w->cb[2] << 16) | ((uint32_t)w->cb[3] << 24) : 0;
            if (fm != 0xFD2FB528u) {                    /* legacy naked-raw: serve from buffer */
                body = w->cb; got = (size_t)j->clen;
            } else {
                unsigned long long dsz2 = ZSTD_getFrameContentSize(w->cb, (size_t)j->clen);
                size_t need2 = (dsz2 != ZSTD_CONTENTSIZE_UNKNOWN && dsz2 != ZSTD_CONTENTSIZE_ERROR)
                               ? (size_t)dsz2 : (size_t)j->clen * 20;
                if (ensure(&w->ob, &w->ocap, need2 ? need2 : 1)) { w->err = -ENOMEM; __atomic_fetch_sub(&g_busy,1,__ATOMIC_RELAXED); job_free(j); continue; }
                int64_t td = now_ns();
                got = ZSTD_decompressDCtx(w->d, w->ob, w->ocap, w->cb, (size_t)j->clen);
                ST(ns_dcomp, now_ns() - td); ST(dcomp_out, ZSTD_isError(got) ? 0 : got);
                if (ZSTD_isError(got)) {
                    snprintf(fb, sizeof fb, "frame@%lld+%lld", (long long)j->coff, (long long)j->clen);
                    w->err = -EIO; emit_err(fb, -EIO); __atomic_fetch_sub(&g_busy,1,__ATOMIC_RELAXED); job_free(j); continue; }
                body = w->ob;
            }
            memcpy(full, g_dest, g_destlen);
            for (uint32_t i = 0; i < j->nmemb; i++) {
                Member *m = &j->memb[i]; size_t pl = strlen(m->path);
                if (g_destlen + pl + 1 >= sizeof full) { w->err = -ENAMETOOLONG; emit_err(m->path, -ENAMETOOLONG); continue; }
                memcpy(full + g_destlen, m->path, pl); full[g_destlen + pl] = 0;
                int64_t to = now_ns();
                int fd = open(full, O_WRONLY | O_CREAT, m->mode & 07777);   /* umask 0 -> exact mode; NO
                                                          * O_TRUNC: extent pieces land concurrently */
                if (fd < 0 && errno == ENOENT) { mkparents(full); fd = open(full, O_WRONLY | O_CREAT, m->mode & 07777); }
                int64_t od = now_ns() - to; ST(ns_open, od); ST(n_open, 1);
                if (od > 500000) ST(slow_open, 1);
                if (fd < 0) { w->err = -errno; emit_err(full, -errno); continue; }
                (void)!ftruncate(fd, m->fsize);          /* idempotent: every piece carries the same fsize */
                if (m->in_off + m->size <= (int64_t)got) {
                    int64_t tw2 = now_ns();
                    if (pwrite(fd, body + m->in_off, (size_t)m->size, m->out_off) != (ssize_t)m->size) {
                        w->err = -errno; emit_err(full, -errno); }
                    ST(ns_write, now_ns() - tw2); ST(wr_bytes, m->size);
                } else { w->err = -EIO; emit_err(full, -EIO); }   /* frame shorter than the plan says */
                int64_t tm = now_ns(); int didmeta = 0;
                if ((g_scatter_meta & 2) && (m->uid || m->gid)) {
                    (void)!fchown(fd, m->uid, m->gid); didmeta = 1;   /* best-effort */
                    if (m->mode & 06000) (void)!fchmod(fd, m->mode & 07777);   /* chown STRIPS setuid/sgid */
                }
                if (g_scatter_meta & 1) {                                 /* restore mtime (leave atime) */
                    struct timespec ts[2]; ts[0].tv_nsec = UTIME_OMIT;
                    ts[1].tv_sec = m->mtime / 1000000000LL; ts[1].tv_nsec = m->mtime % 1000000000LL;
                    futimens(fd, ts); didmeta = 1;
                }
                if (didmeta) { ST(ns_meta, now_ns() - tm); ST(n_meta, 1); }
                close(fd); w->files++;
            }
        }
        __atomic_fetch_sub(&g_busy, 1, __ATOMIC_RELAXED);
        job_free(j);
    }
    return NULL;
}

/* ------------------------------------------------------------------ receiver
 * bvm as the far end: LISTEN accepts N data connections; each reader thread reads
 * [u32 L][ msg | AEAD(msg) ], decrypts (secretstream pull) with the session key,
 * and writes the frame's compressed bytes to sink 0 (the output nock) at a reserved
 * offset, reporting DONE(frame_id, coff, clen). The planner builds the footer from
 * the member STAT (control plane) + these offsets. Symmetric C-to-C transfer. */
static int g_apply;                                  /* receiver: 1 = inflate+scatter to g_dest, 0 = store nock */
typedef struct { int fd; } RxArg;
/* ---- network-apply metadata stashes: links + dir meta apply AFTER all connections ---- */
typedef struct RxM { char *path, *link; int kind; uint32_t mode, uid, gid; int64_t mt; struct RxM *next; } RxM;
static RxM *g_rx_links = NULL, *g_rx_dirs = NULL;
static pthread_mutex_t g_rx_mu = PTHREAD_MUTEX_INITIALIZER;
static void rx_stash(RxM **head, const char *path, size_t pl, const char *link, size_t ll,
                     int kind, uint32_t mode, uint32_t uid, uint32_t gid, int64_t mt) {
    RxM *m = malloc(sizeof *m);
    m->path = strndup(path, pl); m->link = ll ? strndup(link, ll) : NULL;
    m->kind = kind; m->mode = mode; m->uid = uid; m->gid = gid; m->mt = mt;
    pthread_mutex_lock(&g_rx_mu); m->next = *head; *head = m; pthread_mutex_unlock(&g_rx_mu);
}
/* called from main AFTER every rx thread joins: links first (hardlink targets now all
 * exist regardless of which connection carried them), then dir modes/mtimes LAST
 * (restrictive modes can't block writes; child creation bumped parent mtimes). */
static void rx_apply_meta(void) {
    char full[4096], tgt[4096];
    for (RxM *m = g_rx_links; m; m = m->next) {
        size_t pl = strlen(m->path);
        if (g_destlen + pl + 1 >= sizeof full) { emit_err(m->path, -ENAMETOOLONG); continue; }
        memcpy(full, g_dest, g_destlen); memcpy(full + g_destlen, m->path, pl); full[g_destlen + pl] = 0;
        (void)!unlink(full);
        if (m->kind == 1) {                              /* hardlink: target rel -> dest-abs */
            size_t ll = strlen(m->link);
            if (g_destlen + ll + 1 >= sizeof tgt) { emit_err(m->link, -ENAMETOOLONG); continue; }
            memcpy(tgt, g_dest, g_destlen); memcpy(tgt + g_destlen, m->link, ll); tgt[g_destlen + ll] = 0;
            if (link(tgt, full) && errno == ENOENT) { mkparents(full); if (link(tgt, full)) emit_err(full, -errno); }
        } else {                                         /* symlink: target verbatim */
            if (symlink(m->link, full) && errno == ENOENT) { mkparents(full); if (symlink(m->link, full)) emit_err(full, -errno); }
            if (g_scatter_meta & 2) (void)!lchown(full, (uid_t)m->uid, (gid_t)m->gid);
            if (g_scatter_meta & 1) {
                struct timespec ts[2]; ts[0].tv_nsec = UTIME_OMIT;
                ts[1].tv_sec = m->mt / 1000000000LL; ts[1].tv_nsec = m->mt % 1000000000LL;
                (void)!utimensat(AT_FDCWD, full, ts, AT_SYMLINK_NOFOLLOW);
            }
        }
    }
    for (RxM *m = g_rx_dirs; m; m = m->next) {
        size_t pl = strlen(m->path);
        if (g_destlen + pl + 1 >= sizeof full) continue;
        memcpy(full, g_dest, g_destlen); memcpy(full + g_destlen, m->path, pl); full[g_destlen + pl] = 0;
        if (g_scatter_meta & 2) {
            (void)!chown(full, (uid_t)m->uid, (gid_t)m->gid);
        }
        if (m->mode) (void)!chmod(full, m->mode & 07777);   /* after chown (setgid strip) */
        if (g_scatter_meta & 1) {
            struct timespec ts[2]; ts[0].tv_nsec = UTIME_OMIT;
            ts[1].tv_sec = m->mt / 1000000000LL; ts[1].tv_nsec = m->mt % 1000000000LL;
            (void)!utimensat(AT_FDCWD, full, ts, 0);
        }
    }
}

static void *rx_reader(void *a) {
    RxArg *r = a; int fd = r->fd; free(r);
    crypto_secretstream_xchacha20poly1305_state st; int enc = g_have_key;
    if (enc) { unsigned char hdr[crypto_secretstream_xchacha20poly1305_HEADERBYTES];
        if (read_full(fd, hdr, sizeof hdr) != 1) { close(fd); return NULL; }
        crypto_secretstream_xchacha20poly1305_init_pull(&st, hdr, g_key); }
    uint8_t *ctb = NULL, *mb = NULL, *ob = NULL; size_t ctcap = 0, mbcap = 0, ocap = 0;
    ZSTD_DCtx *dc = g_apply ? ZSTD_createDCtx() : NULL; Sink *sk = &g_sinks[0];
    char full[4096]; if (g_apply) memcpy(full, g_dest, g_destlen);
    for (;;) {
        uint32_t L; if (read_full(fd, &L, 4) <= 0) break;
        if (ensure(&ctb, &ctcap, L) || read_full(fd, ctb, L) != 1) break;
        uint8_t *msg; size_t msglen;
        if (enc) { if (ensure(&mb, &mbcap, L)) break; unsigned long long ol = 0; unsigned char tag;
            if (crypto_secretstream_xchacha20poly1305_pull(&st, mb, &ol, &tag, ctb, L, NULL, 0) != 0) break;
            msg = mb; msglen = ol; } else { msg = ctb; msglen = L; }
        if (msglen < 20) break;                          /* [fid][clen][statlen][stat][compressed] */
        uint64_t fid, clen; uint32_t statlen; memcpy(&fid, msg, 8); memcpy(&clen, msg + 8, 8); memcpy(&statlen, msg + 16, 4);
        uint8_t *stat = msg + 20, *comp = stat + statlen;
        if (g_apply) {                                   /* inflate + scatter to the target fs */
            unsigned long long dsz = ZSTD_getFrameContentSize(comp, clen);
            size_t need = (dsz != ZSTD_CONTENTSIZE_UNKNOWN && dsz != ZSTD_CONTENTSIZE_ERROR) ? (size_t)dsz : (size_t)clen * 20;
            if (ensure(&ob, &ocap, need ? need : 1)) break;
            size_t got = ZSTD_decompressDCtx(dc, ob, ocap, comp, clen);
            if (ZSTD_isError(got)) {                     /* dropped frame is REPORTED, not silent */
                char fb[64]; snprintf(fb, sizeof fb, "rx frame %llu", (unsigned long long)fid);
                emit_err(fb, -EIO); continue; }
            /* stat = a TYPED BSTAT Arrow batch: full metadata crosses the data plane.
             * Files apply inline (mode+mtime+chown, setuid-safe); dirs mkdir now, meta
             * stashed; links stashed — both applied in rx_apply_meta AFTER every data
             * connection drains (a hardlink's target may arrive on another connection). */
            const uint8_t *bp[BSTAT_N_BUFS];
            const uint8_t *scur = stat; int64_t nm;
            while ((nm = arrow_next(&scur, bp, BSTAT_N_BUFS)) > 0) {
            const int64_t *po = (const int64_t *)bp[1]; const char *pd = (const char *)bp[2];
            const int64_t *lo = (const int64_t *)bp[4]; const char *ld = (const char *)bp[5];
            const int64_t *io_ = (const int64_t *)bp[7], *sz_ = (const int64_t *)bp[9];
            const int64_t *mt_ = (const int64_t *)bp[13];
            const int32_t *md_ = (const int32_t *)bp[25], *ui_ = (const int32_t *)bp[27], *gi_ = (const int32_t *)bp[29];
            const uint8_t *isd = bp[35];
            for (int64_t i = 0; i < nm; i++) {
                size_t pl = (size_t)(po[i+1] - po[i]), ll = (size_t)(lo[i+1] - lo[i]);
                if (g_destlen + pl + 1 >= sizeof full) { emit_err("rx path too long", -ENAMETOOLONG); continue; }
                memcpy(full + g_destlen, pd + po[i], pl); full[g_destlen + pl] = 0;
                uint32_t md = (uint32_t)md_[i];
                if (isd[i]) {                            /* dir: create now, stash meta for last */
                    if (mkdir(full, 0777) && errno == ENOENT) { mkparents(full); (void)!mkdir(full, 0777); }
                    rx_stash(&g_rx_dirs, pd + po[i], pl, NULL, 0, 0, md, (uint32_t)ui_[i], (uint32_t)gi_[i], mt_[i]);
                    continue;
                }
                if (ll) {                                /* sym/hardlink (kind rides in_off) */
                    rx_stash(&g_rx_links, pd + po[i], pl, ld + lo[i], ll, (int)io_[i],
                             md, (uint32_t)ui_[i], (uint32_t)gi_[i], mt_[i]);
                    continue;
                }
                int wfd = open(full, O_WRONLY | O_CREAT | O_TRUNC, md & 07777);
                if (wfd < 0 && errno == ENOENT) { mkparents(full); wfd = open(full, O_WRONLY | O_CREAT | O_TRUNC, md & 07777); }
                if (wfd < 0) { emit_err(full, -errno); continue; }
                if (io_[i] + sz_[i] <= (int64_t)got) { if (write_all(wfd, ob + io_[i], (size_t)sz_[i])) emit_err(full, -errno); }
                else emit_err(full, -EIO);
                if ((g_scatter_meta & 2) && (ui_[i] || gi_[i])) {
                    (void)!fchown(wfd, (uid_t)ui_[i], (gid_t)gi_[i]);
                    if (md & 06000) (void)!fchmod(wfd, md & 07777);   /* chown strips setuid/sgid */
                }
                if (g_scatter_meta & 1) {
                    struct timespec ts[2]; ts[0].tv_nsec = UTIME_OMIT;
                    ts[1].tv_sec = mt_[i] / 1000000000LL; ts[1].tv_nsec = mt_[i] % 1000000000LL;
                    (void)!futimens(wfd, ts);
                }
                close(wfd);
            }
            }
            if (nm < 0) emit_err("rx stat batch shape mismatch", -EIO);
        } else {                                         /* store: append compressed to the nock */
            pthread_mutex_lock(&sk->mu); int64_t coff = sk->cursor; sk->cursor += clen; pthread_mutex_unlock(&sk->mu);
            if (pwrite_all(sk->fd, comp, (size_t)clen, coff)) {
                char fb[64]; snprintf(fb, sizeof fb, "rx frame %llu", (unsigned long long)fid);
                emit_err(fb, errno ? -errno : -EIO); continue; }
            record_done_x(fid, (uint64_t)coff, clen, 0, (int64_t)clen, 0, 0, -1, 0);
        }
    }
    if (dc) ZSTD_freeDCtx(dc); close(fd); free(ctb); free(mb); free(ob); return NULL;
}

/* ------------------------------------------------------------------ parallel fs scan */
/* seen-inode map: the 1st path at an (dev,ino) with nlink>1 is the file; later paths are
 * HARD LINKs to it (link=first path, kind=1 via in_off). Per-scan; shared across the scan's
 * dirs, so guarded by ScanCtx.mu. */
typedef struct { dev_t dev; ino_t ino; char *path; } Inode;
/* HARDLINK MAP. imap_get was a LINEAR SCAN of the whole map, under the scan-wide mutex, for
 * every entry with nlink>1. That is O(n^2): a Python venv tree is 47% hardlinked (99,130 of
 * 211,058 files here, because uv/pip hardlink package payloads), so ~99k lookups each walked
 * a map of tens of thousands -- billions of comparisons, all serialized. It is why the scan
 * used 1.3 of 64 cores and why per-walker accumulators only bought 13%: the accumulator was
 * never the constraint, this was. Open-addressed hash index over the same array. */
typedef struct { Inode *v; int n, cap; int32_t *idx; int icap; } IMap;
static uint64_t ihash(dev_t d, ino_t i){
    uint64_t x = (uint64_t)i * 0x9E3779B97F4A7C15ull ^ (uint64_t)d;
    x ^= x >> 29; x *= 0xBF58476D1CE4E5B9ull; x ^= x >> 32; return x; }
static void imap_reindex(IMap *m, int cap){          /* power of two, slot = v index + 1 */
    free(m->idx); m->idx = calloc(cap, sizeof *m->idx); m->icap = cap;
    for (int i = 0; i < m->n; i++){
        uint64_t h = ihash(m->v[i].dev, m->v[i].ino) & (uint64_t)(cap - 1);
        while (m->idx[h]) h = (h + 1) & (uint64_t)(cap - 1);
        m->idx[h] = i + 1;
    } }
static const char *imap_get(IMap *m, dev_t dev, ino_t ino){
    if (!m->icap) return NULL;
    uint64_t h = ihash(dev, ino) & (uint64_t)(m->icap - 1);
    for (;;){
        int32_t sl = m->idx[h];
        if (!sl) return NULL;
        Inode *e = &m->v[sl - 1];
        if (e->dev == dev && e->ino == ino) return e->path;
        h = (h + 1) & (uint64_t)(m->icap - 1);
    } }
static void imap_add(IMap *m, dev_t dev, ino_t ino, const char *path){
    if (m->n==m->cap){ m->cap=m->cap?m->cap*2:64; m->v=realloc(m->v,m->cap*sizeof(Inode)); }
    m->v[m->n].dev=dev; m->v[m->n].ino=ino; m->v[m->n].path=strdup(path); m->n++;
    if (m->n * 2 >= m->icap) imap_reindex(m, m->icap ? m->icap * 2 : 1024);
    else { uint64_t h = ihash(dev, ino) & (uint64_t)(m->icap - 1);
           while (m->idx[h]) h = (h + 1) & (uint64_t)(m->icap - 1);
           m->idx[h] = m->n; } }
static void imap_free(IMap *m){ for (int i=0;i<m->n;i++) free(m->v[i].path); free(m->v); free(m->idx); }

/* PARALLEL fs scan (ported from qvm scan): a DIRECTORY is the unit of work, so the persistent
 * walker pool drains ONE scan across many dirs at once — a single-root scan parallelizes (the
 * old design recursed a whole subtree in one thread, so a single root was single-threaded).
 * Dirs are opened fd-relative (openat/fstatat from the parent's dir fd) — no full-path
 * re-resolution / per-component lookup RPCs on WEKA. Two modes per ScanCtx: names_only
 * (readdir + d_type, NO stat — rm/enumerate, ~14x) and full stat (fstatat + hardlink map —
 * pack/du/diff/ducl). Full-stat mode ALSO emits a row per DIRECTORY (is_dir=1, with
 * parent_ino/depth) — the inode graph ducl aggregates over; names-only emits files only.
 * One EOF per sid, emitted when the scan's last dir drains. */
typedef struct IgnorePat { char *pat; uint8_t flags; } IgnorePat;  /* 1=anchored 2=dir_only 4=negate */
typedef struct ScanCtx {
    uint32_t sid; int names_only;
    IgnorePat *ign; int nign;                            /* .quiverignore: pruned IN THE WALK */
    pthread_mutex_t mu;                                  /* guards pending, im, and the accumulator */
    long pending;                                        /* dirs enqueued for this scan, not yet finished */
    IMap im;                                             /* hardlink map (full-stat) */
    BC bc;                                               /* columnar accumulator, flushed at 20000 rows */
} ScanCtx;
typedef struct DirJob { ScanCtx *ctx; int dfd; char *rel; uint64_t ino; int32_t depth; struct DirJob *next; } DirJob;
/* PER-WALKER ACCUMULATOR. Every walker used to append its rows into the ONE ScanCtx.bc while
 * holding c->mu, so all 233k row-appends of a scan serialized on a single mutex: 64 threads
 * delivered 1.26 busy cores (117 sleeping, 10.6 in D) and the scan queue sat 2,934 dirs DEEP
 * -- blocked, not starved. Raising threads did nothing (32/64/128 -> 3.49/3.25/3.24 s).
 * pwalk2 does the same work on 5.18 cores with per-thread buffers.
 * Each walker now fills its own BC lock-free and emits it directly (emit_bstat takes g_out_mu
 * itself and resets the buffer). `owner` tracks which scan the rows belong to, since a walker
 * may take dirs from different ScanCtxs; switching scans flushes first. */
/* SCAN STAGE TIMERS (BVM_SCAN_PROF=1). Deliberately NOT on the 42-field stats wire: that
 * wire mis-decodes silently when the two sides disagree on field count, which already cost a
 * session of wrong queue numbers. These print to stderr when a scan drains. */
static int g_scan_prof;
static _Atomic int64_t sp_readdir, sp_open, sp_stat, sp_bc, sp_emit, sp_dirs, sp_ents, sp_wall;
#define SP(f, v) do { if (g_scan_prof) __atomic_fetch_add(&(f), (v), __ATOMIC_RELAXED); } while (0)
#define SPT(f, stmt) do { if (g_scan_prof) { int64_t _t0 = now_ns(); stmt; \
                            __atomic_fetch_add(&(f), now_ns() - _t0, __ATOMIC_RELAXED); } \
                          else { stmt; } } while (0)

/* BATCHED CHILD-DIRECTORY OPENS. openat is ~1 ms on wekafs -- 23 of the 44 thread-seconds a
 * scan spends blocked -- and issuing them one at a time leaves a walker with ONE outstanding
 * metadata op. getdents cannot go on a ring (IORING_OP_GETDENTS was never merged upstream),
 * so this covers the half that can. Each walker owns a ring; opened lazily, only for
 * directories with enough children to be worth a submit. */
#define RING_MIN_DIRS 4
#define RING_DIR_QD   64
typedef struct { BC bc; ScanCtx *owner; struct io_uring ring; int ring_ok; } WScan;
static int wscan_ring(WScan *ws){
    if (!ws->ring_ok) {
        if (io_uring_queue_init(RING_DIR_QD, &ws->ring, 0) < 0) { ws->ring_ok = -1; return 0; }
        ws->ring_ok = 1;
    }
    return ws->ring_ok > 0;
}
static WScan *g_wscan; static int g_nwscan;
static pthread_mutex_t g_wsflush = PTHREAD_MUTEX_INITIALIZER;

static void wscan_flush(WScan *ws) {
    if (ws->owner && ws->bc.n) emit_bstat(ws->owner->sid, -1, &ws->bc);
}
/* Flush every walker's rows for `c`. Called only by the thread that saw pending hit 0, so no
 * walker is mid-append for this scan: each appends BEFORE decrementing pending under c->mu,
 * and that mutex orders those writes ahead of this sweep. */
static void wscan_flush_all(ScanCtx *c) {
    pthread_mutex_lock(&g_wsflush);
    for (int i = 0; i < g_nwscan; i++)
        if (g_wscan[i].owner == c) {
            wscan_flush(&g_wscan[i]);
            /* CLEAR THE BACK-POINTER. The caller frees the ScanCtx right after the EOF, so a
             * walker that later picks up a different scan would flush through a dangling
             * owner (`ws->owner->sid`) -- which showed up as "double free or corruption". */
            g_wscan[i].owner = NULL;
        }
    pthread_mutex_unlock(&g_wsflush);
}
/* unbounded linked-list dir queue (unbounded so self-feeding pushes never deadlock a full ring) */
typedef struct { DirJob *h, *t; int done; int n; pthread_mutex_t mu; pthread_cond_t ne; } DQ;
static DQ g_dq;
static void dq_init(DQ *q){ q->h=q->t=NULL; q->done=0; q->n=0; pthread_mutex_init(&q->mu,0); pthread_cond_init(&q->ne,0); }
static void dq_push(DQ *q, DirJob *j){ j->next=NULL; pthread_mutex_lock(&q->mu);
    if(q->t) q->t->next=j; else q->h=j; q->t=j; q->n++; pthread_cond_signal(&q->ne); pthread_mutex_unlock(&q->mu); }
static DirJob *dq_pop(DQ *q){ pthread_mutex_lock(&q->mu); while(!q->h && !q->done) pthread_cond_wait(&q->ne,&q->mu);
    DirJob *j=q->h; if(j){ q->h=j->next; if(!q->h) q->t=NULL; q->n--; } pthread_mutex_unlock(&q->mu); return j; }
static void dq_finish(DQ *q){ pthread_mutex_lock(&q->mu); q->done=1; pthread_cond_broadcast(&q->ne); pthread_mutex_unlock(&q->mu); }
static void scan_flush(ScanCtx *c){ emit_bstat(c->sid, -1, &c->bc); } /* hold c->mu */

/* 100Hz occupancy sampler: reads queue depths WITHOUT locks (racy by design — sampling,
 * not accounting). Little's law: a bounded queue persistently FULL => its consumer is the
 * constraint; persistently EMPTY => its producer is. Sums / q_samples in the report. */
static volatile int g_sampling = 1;
extern int g_busy;
static int g_nworkers;
/* type-4 STATS: the whole counter block. Every counter is CUMULATIVE, so the planner
 * differences consecutive samples to get per-interval rates. Emitted once a second as well
 * as at exit: a whole-run average blends phases that behave nothing alike (1.8M small files
 * are metadata-bound, 9k multi-GB files are bandwidth-bound) and reports a single blurred
 * bottleneck for both. */
static void emit_stats(void) {
    int64_t st_[42] = { g_st.rd_bytes, g_st.wr_bytes, g_st.comp_in, g_st.comp_out,
        g_st.dcomp_out, g_st.ns_read, g_st.ns_comp, g_st.ns_dcomp, g_st.ns_write,
        g_st.ns_hash, g_st.n_open, g_st.ns_open, g_st.slow_open, g_st.n_meta,
        g_st.ns_meta, g_st.n_unlink, g_st.ns_unlink, g_st.scan_entries,
        g_st.ns_scan_stat, g_st.ns_idle, g_st.ns_emit, g_st.ns_qfull,
        g_st.n_raw_frames, g_st.raw_bytes, g_st.cfr_bytes, g_st.n_probe, g_st.probe_in,
        /* ring_batches/ring_members were added to PERF_FIELDS on the planner side but never
         * emitted here, so the initializer supplied 40 of 42 values. sizeof matched (the array
         * is [42], zero-filled), nothing errored, and EVERY field from q_samples on was read
         * two positions early -- wall_ns and nworkers landed on the zero fill, which is what
         * made util/queue percentages nonsense (and the live dashboard's dirs + full/empty%). */
        g_st.ring_batches, g_st.ring_members,
        g_st.q_samples, g_st.jq_sum, g_st.jq_full, g_st.jq_empty, g_st.dq_sum,
        g_st.busy_sum, g_st.pack_used_sum, g_st.blk_live_sum,
        (int64_t)g_jq.cap, g_pack_budget, g_budget,
        now_ns() - g_start_ns, (int64_t)g_nworkers };
    uint32_t body = 1 + sizeof st_; uint8_t hdr4[5];
    memcpy(hdr4, &body, 4); hdr4[4] = 4;
    pthread_mutex_lock(&g_out_mu);
    write_all(1, hdr4, 5); write_all(1, st_, sizeof st_);
    pthread_mutex_unlock(&g_out_mu);
}

static void *sampler(void *unused) {
    (void)unused;
    int64_t tick = 0;
    while (__atomic_load_n(&g_sampling, __ATOMIC_RELAXED)) {
        int n = g_jq.n, cap = g_jq.cap;
        ST(q_samples, 1); ST(jq_sum, n);
        if (n >= cap) ST(jq_full, 1);
        if (n == 0) ST(jq_empty, 1);
        ST(dq_sum, g_dq.n);
        ST(busy_sum, __atomic_load_n(&g_busy, __ATOMIC_RELAXED));
        ST(pack_used_sum, g_pack_used); ST(blk_live_sum, g_live);
        if (++tick % 100 == 0) {                     /* 100 ticks x 10ms = one sample/second */
            emit_stats();
            /* Also flush frame records on a TIMER, not only when DONE_FLUSH accumulate: a
             * slow or small job would otherwise show a watcher nothing until it exits. */
            pthread_mutex_lock(&g_done_mu); flush_done_locked(); pthread_mutex_unlock(&g_done_mu);
        }
        struct timespec ts = {0, 10 * 1000 * 1000};
        nanosleep(&ts, NULL);
    }
    return NULL;
}

/* gitignore-ish matcher: LAST matching pattern wins; dir_only patterns only ever match
 * dirs; anchored patterns (contain '/') match the root-relative path, unanchored match
 * the basename (then the rel path as fallback). Excluding a DIR prunes its whole subtree
 * (never descended -> a `tmp/` exclude skips millions of entries, it doesn't scan-then-
 * drop them); a negation can therefore not resurrect anything INSIDE an excluded dir —
 * the same caveat gitignore has. */
static int ign_match(ScanCtx *c, const char *rel, const char *name, int isdir) {
    int excl = 0;
    for (int i = 0; i < c->nign; i++) {
        IgnorePat *g = &c->ign[i];
        if ((g->flags & 2) && !isdir) continue;
        int m;
        if (g->flags & 1) m = fnmatch(g->pat, rel, 0) == 0;
        else m = fnmatch(g->pat, name, 0) == 0 || fnmatch(g->pat, rel, 0) == 0;
        if (m) excl = !(g->flags & 4);
    }
    return excl;
}

/* one collected entry (row deferred until under c->mu; the fstatat/readlink RPC that
 * produced it ran OUTSIDE the lock). Dirs carry isdir=1 (emitted, full-stat mode). */
typedef struct { char *rel; struct stat st; int islnk, hard, isdir; char *tgt; } FEnt;

static void *scan_walker(void *arg) {
    WScan *ws = arg;
    for (;;) {
        DirJob *dj = dq_pop(&g_dq); if (!dj) break;
        ScanCtx *c = dj->ctx; int no = c->names_only;
        if (ws->owner != c) {                            /* rows must not cross scans */
            pthread_mutex_lock(&g_wsflush); wscan_flush(ws); ws->owner = c;
            pthread_mutex_unlock(&g_wsflush);
        }
        size_t pl = strlen(dj->rel); int32_t cdepth = dj->depth + 1;   /* children's depth */
        FEnt *fe=NULL; long nfe=0, cfe=0;                    /* rows to emit */
        struct { char *cr; struct stat st; int fd; } *pd=NULL; long npd=0, cpd=0;  /* child dirs */
        DirJob *kh=NULL, *kt=NULL; long nkids=0;             /* child dirs to enqueue */
        DIR *dp = fdopendir(dj->dfd);                        /* takes ownership of dfd */
        if (dp) { struct dirent *e; int dfd = dirfd(dp);
            for (;;) {
                SPT(sp_readdir, e = readdir(dp));
                if (!e) break;
                const char *nm = e->d_name;
                if (nm[0]=='.' && (!nm[1] || (nm[1]=='.' && !nm[2]))) continue;
                size_t nl = strlen(nm);
                char *cr = malloc(pl + 1 + nl + 1);          /* child path relative to scan root */
                if (pl) { memcpy(cr,dj->rel,pl); cr[pl]='/'; memcpy(cr+pl+1,nm,nl+1); } else memcpy(cr,nm,nl+1);
                int isdir, islnk=0; struct stat st; memset(&st,0,sizeof st);
                if (no) {                                    /* names-only: d_type, stat only on DT_UNKNOWN */
                    if (e->d_type==DT_DIR) isdir=1;
                    else if (e->d_type==DT_UNKNOWN) { if (fstatat(dfd,nm,&st,AT_SYMLINK_NOFOLLOW)){free(cr);continue;} isdir=S_ISDIR(st.st_mode); }
                    else isdir=0;
                } else {                                     /* full stat */
                    int64_t ts0 = now_ns();
                    int rc_ = fstatat(dfd,nm,&st,AT_SYMLINK_NOFOLLOW);
                    { int64_t _d = now_ns() - ts0; ST(ns_scan_stat, _d); ST(scan_entries, 1);
                      SP(sp_stat, _d); SP(sp_ents, 1); }
                    if (rc_) { free(cr); continue; }
                    isdir = S_ISDIR(st.st_mode); islnk = S_ISLNK(st.st_mode);
                    if (!isdir && !islnk && !S_ISREG(st.st_mode)) { free(cr); continue; }  /* skip specials */
                }
                if (c->nign && ign_match(c, cr, nm, isdir)) { free(cr); continue; }   /* PRUNED */
                if (isdir) {                                 /* defer the open; batched below */
                    if (npd==cpd){ cpd=cpd?cpd*2:16; pd=realloc(pd,cpd*sizeof(*pd)); }
                    pd[npd].cr=cr; pd[npd].st=st; pd[npd].fd=-1; npd++;
                    {   /* the dir itself is a row in BOTH modes: full-stat feeds ducl's inode
                         * graph; names-only feeds rm the COMPLETE dir set (EMPTY dirs were
                         * invisible before -> left behind + silent parent-rmdir failures) */
                        if (nfe==cfe){ cfe=cfe?cfe*2:32; fe=realloc(fe,cfe*sizeof(FEnt)); }
                        FEnt *f=&fe[nfe++]; memset(f,0,sizeof *f); f->rel=strdup(cr); f->st=st; f->isdir=1;
                    }
                    continue;
                }
                if (nfe==cfe){ cfe=cfe?cfe*2:32; fe=realloc(fe,cfe*sizeof(FEnt)); }
                FEnt *f=&fe[nfe++]; memset(f,0,sizeof *f); f->rel=cr; f->st=st;
                if (!no && islnk) { char t[4096]; ssize_t tl=readlinkat(dfd,nm,t,sizeof t-1);
                    if (tl<0){ nfe--; free(cr); continue; } t[tl]=0; f->islnk=1; f->st.st_size=tl; f->tgt=strdup(t); }
                else if (!no && st.st_nlink>1) f->hard=1;
            }
            /* open the child dirs while the parent fd is still ours (closedir takes it) */
            if (npd) {
                int64_t _o0 = g_scan_prof ? now_ns() : 0;
                if (npd >= RING_MIN_DIRS && wscan_ring(ws)) {
                    for (long i=0;i<npd;){
                        long k = npd-i; if (k > RING_DIR_QD) k = RING_DIR_QD;
                        for (long j=0;j<k;j++){
                            const char *bn = strrchr(pd[i+j].cr,'/'); bn = bn?bn+1:pd[i+j].cr;
                            struct io_uring_sqe *sq = io_uring_get_sqe(&ws->ring);
                            if (!sq) { k = j; break; }
                            io_uring_prep_openat(sq, dfd, bn,
                                                 O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC, 0);
                            io_uring_sqe_set_data64(sq, (uint64_t)j);
                        }
                        if (!k) break;
                        io_uring_submit(&ws->ring);
                        for (long j=0;j<k;j++){
                            struct io_uring_cqe *cq;
                            if (io_uring_wait_cqe(&ws->ring,&cq)<0) break;
                            pd[i + (long)io_uring_cqe_get_data64(cq)].fd = cq->res;
                            io_uring_cqe_seen(&ws->ring,cq);
                        }
                        i += k;
                    }
                } else {
                    for (long i=0;i<npd;i++){
                        const char *bn = strrchr(pd[i].cr,'/'); bn = bn?bn+1:pd[i].cr;
                        pd[i].fd = openat(dfd, bn, O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);
                    }
                }
                if (g_scan_prof) __atomic_fetch_add(&sp_open, now_ns()-_o0, __ATOMIC_RELAXED);
                for (long i=0;i<npd;i++){
                    if (pd[i].fd < 0) { free(pd[i].cr); continue; }
                    DirJob *k2 = malloc(sizeof *k2); k2->ctx=c; k2->dfd=pd[i].fd; k2->next=NULL;
                    k2->ino=(uint64_t)pd[i].st.st_ino; k2->depth=cdepth; k2->rel=pd[i].cr;
                    if (kt) kt->next=k2; else kh=k2; kt=k2; nkids++;
                }
            }
            free(pd); pd=NULL;
            closedir(dp);
        } else { close(dj->dfd); for (long i=0;i<npd;i++) free(pd[i].cr); free(pd); pd=NULL; }

        /* LOCK-FREE row append into this walker's own buffer. c->mu is now taken only for the
         * hardlink map (which is genuinely shared, and only consulted for nlink>1 entries) and
         * for the pending counter. */
        int64_t _bc0 = g_scan_prof ? now_ns() : 0;
        for (long i=0;i<nfe;i++) { FEnt *f=&fe[i];
            if (no)
                bc_add_raw(&ws->bc, f->rel, NULL, 0, 0, 0, 0, 0, 0, f->isdir);
            else if (f->isdir || f->islnk)                   /* dir row / symlink row (kind 0, link=target) */
                bc_add_fs(&ws->bc, f->rel, f->tgt, 0, &f->st, dj->ino, cdepth, f->isdir);
            else if (f->hard) {                              /* shared inode map: needs the lock */
                pthread_mutex_lock(&c->mu);
                const char *first = imap_get(&c->im, f->st.st_dev, f->st.st_ino);
                if (!first) imap_add(&c->im, f->st.st_dev, f->st.st_ino, f->rel);
                /* copy: `first` points into the map, which another thread may grow */
                char *fc = first ? strdup(first) : NULL;
                pthread_mutex_unlock(&c->mu);
                if (fc) { bc_add_fs(&ws->bc, f->rel, fc, 1, &f->st, dj->ino, cdepth, 0); free(fc); }
                else bc_add_fs(&ws->bc, f->rel, NULL, 0, &f->st, dj->ino, cdepth, 0);
            }
            else bc_add_fs(&ws->bc, f->rel, NULL, 0, &f->st, dj->ino, cdepth, 0);
            if (ws->bc.n >= 20000) SPT(sp_emit, emit_bstat(c->sid, -1, &ws->bc));
            free(f->rel); free(f->tgt);
        }
        if (g_scan_prof) { __atomic_fetch_add(&sp_bc, now_ns() - _bc0, __ATOMIC_RELAXED);
                           __atomic_fetch_add(&sp_dirs, 1, __ATOMIC_RELAXED); }
        /* PENDING IS RAISED BEFORE THE KIDS ARE VISIBLE. Pushing first and counting after
         * lets another walker pop a child, finish it and decrement past zero before this
         * thread has counted it -- pending transiently hits 0, EOF fires early and the scan
         * silently truncates (measured: 20,063 rows instead of 233,746, varying per run).
         * Kids still go on the queue OUTSIDE c->mu; only the counter needs the lock. */
        pthread_mutex_lock(&c->mu);
        c->pending += nkids;
        pthread_mutex_unlock(&c->mu);
        for (DirJob *k=kh; k; ) { DirJob *nx=k->next; dq_push(&g_dq, k); k=nx; }
        pthread_mutex_lock(&c->mu);
        long left = --c->pending;                            /* this dir done */
        pthread_mutex_unlock(&c->mu);
        if (left==0) {
            wscan_flush_all(c); emit_eof(c->sid);
            if (g_scan_prof) {
                int64_t w = now_ns() - __atomic_load_n(&sp_wall, __ATOMIC_RELAXED);
                fprintf(stderr,
                  "bvm scan-prof: wall %.2fs  dirs %lld  entries %lld\n"
                  "  readdir %7.2fs   openat %7.2fs   fstatat %7.2fs\n"
                  "  bc_add  %7.2fs   emit   %7.2fs   (thread-seconds, summed over walkers)\n",
                  w/1e9, (long long)sp_dirs, (long long)sp_ents,
                  sp_readdir/1e9, sp_open/1e9, sp_stat/1e9, sp_bc/1e9, sp_emit/1e9);
            }
        }

        free(fe); free(dj->rel); free(dj);
        if (left==0) { imap_free(&c->im); bc_free(&c->bc);
            for (int gi2 = 0; gi2 < c->nign; gi2++) free(c->ign[gi2].pat);
            free(c->ign); pthread_mutex_destroy(&c->mu); free(c); }
    }
    return NULL;
}
/* seed a scan: open root, make a ScanCtx + its first DirJob (pending=1) */
static void scan_start(uint32_t sid, const char *root, int names_only, IgnorePat *ign, int nign) {
    int rfd = open(root, O_RDONLY|O_DIRECTORY|O_CLOEXEC);
    if (rfd < 0) { for (int i = 0; i < nign; i++) free(ign[i].pat); free(ign);
        emit_eof(sid); return; }                            /* unreadable root: empty scan */
    struct stat rst; uint64_t rino = fstat(rfd,&rst)==0 ? (uint64_t)rst.st_ino : 0;
    ScanCtx *c = calloc(1, sizeof *c); c->sid=sid; c->names_only=names_only; c->pending=1;
    c->ign = ign; c->nign = nign;
    pthread_mutex_init(&c->mu, 0);
    if (g_scan_prof) __atomic_store_n(&sp_wall, now_ns(), __ATOMIC_RELAXED);
    DirJob *j = malloc(sizeof *j); j->ctx=c; j->dfd=rfd; j->rel=strdup(""); j->next=NULL;
    j->ino=rino; j->depth=0;                                /* root's children get depth 1 (qvm/ducl convention) */
    dq_push(&g_dq, j);
}

/* ------------------------------------------------------------------ OPEN_TAR thread (streaming decode) */
typedef struct { int fd; ZSTD_DStream *ds; int raw; uint8_t *in; size_t incap,inlen,inpos; uint8_t *out; size_t outcap,outlen,outpos; int eof; } Src;
static int src_fill(Src *s){ if(s->outpos<s->outlen) return 1; s->outpos=s->outlen=0;
    for(;;){ if(s->inpos==s->inlen && !s->eof){ ssize_t r=read(s->fd,s->in,s->incap); if(r<0)return -1; if(r==0)s->eof=1; else{s->inpos=0;s->inlen=r;} }
        if(s->raw){ if(s->inpos==s->inlen) return 0; size_t n=s->inlen-s->inpos; if(n>s->outcap){s->out=realloc(s->out,n);s->outcap=n;}
            memcpy(s->out,s->in+s->inpos,n); s->inpos=s->inlen; s->outlen=n; return 1; }
        ZSTD_inBuffer ib={s->in,s->inlen,s->inpos}; ZSTD_outBuffer ob={s->out,s->outcap,0};
        size_t rc=ZSTD_decompressStream(s->ds,&ob,&ib); s->inpos=ib.pos; if(ZSTD_isError(rc)) return -1;
        if(ob.pos){ s->outlen=ob.pos; return 1; } if(s->eof && s->inpos==s->inlen) return 0; } }
static int src_read(Src *s, uint8_t *dst, size_t n){ size_t got=0; while(got<n){ if(s->outpos==s->outlen){ int r=src_fill(s); if(r<=0) return got==0?r:-1; }
    size_t take=s->outlen-s->outpos; if(take>n-got)take=n-got; memcpy(dst+got,s->out+s->outpos,take); s->outpos+=take; got+=take; } return 1; }
static void src_skip(Src *s, int64_t n){ while(n>0){ if(s->outpos==s->outlen){ if(src_fill(s)<=0) return; }
    int64_t take=s->outlen-s->outpos; if(take>n)take=n; s->outpos+=take; n-=take; } }
static int64_t octal(const char *p, int n){ int64_t v=0; while(n-- && *p==' ')p++; while(n-->=0 && *p>='0'&&*p<='7') v=v*8+(*p++ -'0'); return v; }
static void pax_parse(uint8_t *buf, int64_t len, char *pending, int *have, int64_t *psize,
                      int64_t *pmtime, int64_t *puid, int64_t *pgid, char *plink, int *phlink){
    uint8_t *p=buf,*end=buf+len; while(p<end){ uint8_t *sp=memchr(p,' ',end-p); if(!sp)break; long rl=strtol((char*)p,0,10); if(rl<=0)break;
        uint8_t *re=p+rl; if(re>end)break; uint8_t *kv=sp+1,*eq=memchr(kv,'=',re-kv);
        if(eq){ int kl=eq-kv; uint8_t *v=eq+1; int vl=re-1-v; if(vl<0)vl=0;
            if(kl==4&&!memcmp(kv,"path",4)){ memcpy(pending,v,vl); pending[vl]=0; *have=1; }
            else if(kl==8&&!memcmp(kv,"linkpath",8)){ memcpy(plink,v,vl); plink[vl]=0; *phlink=1; }   /* symlink target */
            else if(kl==4&&!memcmp(kv,"size",4)) *psize=strtoll((char*)v,0,10);
            else if(kl==5&&!memcmp(kv,"mtime",5)) *pmtime=(int64_t)(strtod((char*)v,0)*1e9);   /* float secs -> ns */
            else if(kl==3&&!memcmp(kv,"uid",3)) *puid=strtoll((char*)v,0,10);
            else if(kl==3&&!memcmp(kv,"gid",3)) *pgid=strtoll((char*)v,0,10); } p=re; } }
/* grow-append helpers for tar_compat (frame = verbatim raw tar bytes) */
static void gcat(uint8_t **buf, size_t *cap, size_t *len, const uint8_t *src, size_t n){
    if(*len+n > *cap){ size_t nc=*len+n; *cap = nc>*cap*2?nc:*cap*2; *buf=realloc(*buf,*cap); }
    if(src) memcpy(*buf+*len, src, n); *len += n; }
static int gread(Src *s, uint8_t **buf, size_t *cap, size_t *len, int64_t n){
    if((int64_t)(*len)+n > (int64_t)*cap){ size_t nc=*len+n; *cap = nc>*cap*2?nc:*cap*2; *buf=realloc(*buf,*cap); }
    if(n>0 && src_read(s,*buf+*len,(size_t)n)<=0) return -1; *len+=n; return 1; }
typedef struct { uint32_t sid; char path[4096]; int64_t frame_bytes; int stat_only, tar_compat; } TarArg;
static void *tar_thread(void *a) {
    TarArg *ta = a; Src s; memset(&s,0,sizeof s);
    s.fd = open(ta->path, O_RDONLY); if (s.fd < 0) { emit_eof(ta->sid); free(ta); return NULL; }
    uint8_t mg[6]; ssize_t g = read(s.fd, mg, 6); lseek(s.fd, 0, SEEK_SET);
    s.raw = !(g>=4 && mg[0]==0x28 && mg[1]==0xb5 && mg[2]==0x2f && mg[3]==0xfd);
    if (s.raw && g >= 3 &&                             /* other codecs: FAIL LOUDLY (parsing gzip
        bytes as tar found no headers -> silently produced an EMPTY archive before) */
        ((mg[0]==0x1f && mg[1]==0x8b) ||                                    /* gzip */
         (g>=6 && mg[0]==0xfd && mg[1]=='7' && mg[2]=='z' && mg[3]=='X' && mg[4]=='Z') ||  /* xz */
         (mg[0]=='B' && mg[1]=='Z' && mg[2]=='h'))) {                       /* bzip2 */
        emit_err(ta->path, -ENOTSUP); emit_eof(ta->sid);
        close(s.fd); free(ta); return NULL;              /* s.in/s.out not yet allocated here */
    }
    s.incap=1<<20; s.in=malloc(s.incap); s.outcap=4<<20; s.out=malloc(s.outcap);
    if(!s.raw){ s.ds=ZSTD_createDStream(); ZSTD_initDStream(s.ds); }
    uint8_t hdr[512]; uint8_t *blk=NULL; size_t bcap=0,blen=0;
    BC bc={0};                                       /* member rows for the current frame */
    BC sbc={0};                                      /* sym/hardlinks: block=-1, carry the target */
    uint8_t *pre=NULL; size_t precap=0, prelen=0;    /* compat: raw preamble (x/L/K/g) of the current member */
    char pending[8192]; int have_name=0; char pending_link[8192]; int have_link=0;
    int64_t pax_size=-1, pax_mtime=-1, pax_uid=-1, pax_gid=-1;
    int cmp = ta->tar_compat && !ta->stat_only;      /* frame = verbatim raw tar bytes */
    for(;;){
        if(src_read(&s,hdr,512)<=0) break;
        int zero=1; for(int i=0;i<512;i++) if(hdr[i]){zero=0;break;} if(zero) break;
        int64_t size=octal((char*)hdr+124,12); char type=hdr[156]; int64_t padded=(size+511)&~511LL;
        if(type=='L'||type=='K'){          /* GNU long name ('L') / long linkname ('K') */
            char *dst = (type=='K') ? pending_link : pending;
            int64_t got=size<8191?size:8191;
            if(cmp){ gcat(&pre,&precap,&prelen,hdr,512); size_t at=prelen; gread(&s,&pre,&precap,&prelen,padded); memcpy(dst,pre+at,got); }
            else { src_read(&s,(uint8_t*)dst,got); for(int64_t k=got;k<padded;k++){uint8_t b;src_read(&s,&b,1);} }
            dst[got]=0; if(type=='K') have_link=1; else have_name=1;
            continue; }
        if(type=='x'){
            if(cmp){ gcat(&pre,&precap,&prelen,hdr,512); size_t at=prelen; gread(&s,&pre,&precap,&prelen,padded);
                pax_parse(pre+at,size,pending,&have_name,&pax_size,&pax_mtime,&pax_uid,&pax_gid,pending_link,&have_link); }
            else { uint8_t *pb=malloc(size?size:1); src_read(&s,pb,size); for(int64_t k=size;k<padded;k++){uint8_t b;src_read(&s,&b,1);}
                pax_parse(pb,size,pending,&have_name,&pax_size,&pax_mtime,&pax_uid,&pax_gid,pending_link,&have_link); free(pb); }
            continue; }
        if(type=='g'){
            if(cmp){ gcat(&pre,&precap,&prelen,hdr,512); gread(&s,&pre,&precap,&prelen,padded); }
            else { for(int64_t k=0;k<padded;k++){uint8_t b;src_read(&s,&b,1);} }
            continue; }
        char name[8192];
        if(have_name){ strcpy(name,pending); have_name=0; }
        else { int nl=0; for(;nl<100&&hdr[nl];nl++)name[nl]=hdr[nl]; name[nl]=0;
            if(hdr[345]){ char pfx[160]; int pl=0; for(;pl<155&&hdr[345+pl];pl++)pfx[pl]=hdr[345+pl]; pfx[pl]=0;
                char tmp[512]; snprintf(tmp,sizeof tmp,"%s/%s",pfx,name); strcpy(name,tmp); } }
        if(pax_size>=0){ size=pax_size; pax_size=-1; padded=(size+511)&~511LL; }
        uint32_t mode = octal((char*)hdr+100,8)&07777;
        int64_t mtime = pax_mtime>=0 ? pax_mtime : octal((char*)hdr+136,12)*1000000000LL;
        uint32_t uid = pax_uid>=0 ? (uint32_t)pax_uid : (uint32_t)octal((char*)hdr+108,8);
        uint32_t gid = pax_gid>=0 ? (uint32_t)pax_gid : (uint32_t)octal((char*)hdr+116,8);
        pax_mtime=pax_uid=pax_gid=-1;
        size_t nlen=strlen(name);
        int is_reg = (type=='0'||type=='\0'||type=='7');
        int is_dir = (type=='5'||(nlen&&name[nlen-1]=='/'));
        int is_sym = (type=='2'), is_hard = (type=='1'), is_link = is_sym||is_hard;
        int kind = is_hard ? 1 : 0;                              /* 0=symlink, 1=hardlink */
        char link[8192]; link[0]=0;
        if(is_link){                                             /* target: 'K'/PAX ext, else ustar hdr[157] */
            if(have_link){ strncpy(link,pending_link,sizeof link-1); link[sizeof link-1]=0; have_link=0; }
            else { int ll=0; for(;ll<100&&hdr[157+ll];ll++) link[ll]=hdr[157+ll]; link[ll]=0; }
        }
        if(cmp){                                                 /* verbatim: append pre+hdr+body+pad to the frame */
            int64_t rawneed = (int64_t)prelen + 512 + padded;
            if(blen && blen+rawneed>(size_t)ta->frame_bytes){    /* close frame at a member boundary */
                uint64_t id=blk_add(blk,blen); emit_bstat(ta->sid,(int64_t)id,&bc);
                blk=NULL;bcap=blen=0; }
            if(prelen){ gcat(&blk,&bcap,&blen,pre,prelen); prelen=0; }
            gcat(&blk,&bcap,&blen,hdr,512);
            int64_t body_off=(int64_t)blen; gread(&s,&blk,&bcap,&blen,padded);   /* body + padding, verbatim */
            if(is_reg) bc_add_raw(&bc, name, NULL, body_off, size, mode, mtime, uid, gid, 0);
            else if(is_link) bc_add_raw(&sbc, name, link, kind, (int64_t)strlen(link), mode, mtime, uid, gid, 0);
            else if(is_dir) bc_add_raw(&sbc, name, NULL, 0, 0, mode, mtime, uid, gid, 1);   /* dir: footer row (frame -1) */
            continue;                                            /* dir/link stays in the tar AND gets a footer row */
        }
        if(is_link){                                             /* footer sym/hardlink row; body is empty */
            bc_add_raw(&sbc, name, link, kind, (int64_t)strlen(link), mode, mtime, uid, gid, 0);
            for(int64_t k=0;k<padded;k++){uint8_t b;src_read(&s,&b,1);}
            if(sbc.n>=20000) emit_bstat(ta->sid,-1,&sbc);
            continue; }
        if(is_dir){ bc_add_raw(&sbc, name, NULL, 0, 0, mode, mtime, uid, gid, 1);   /* footer row (frame -1) */
            for(int64_t k=0;k<padded;k++){uint8_t b;src_read(&s,&b,1);}
            if(sbc.n>=20000) emit_bstat(ta->sid,-1,&sbc);
            continue; }
        if(!is_reg){ for(int64_t k=0;k<padded;k++){uint8_t b;src_read(&s,&b,1);} continue; }
        if(ta->stat_only){                                       /* diff/scan: STAT only, skip body */
            bc_add_raw(&bc, name, NULL, 0, size, mode, mtime, uid, gid, 0);
            src_skip(&s, padded);
            if(bc.n>=20000) emit_bstat(ta->sid,-1,&bc);
            continue;
        }
        if(blen && blen+size>(size_t)ta->frame_bytes){          /* close frame */
            uint64_t id=blk_add(blk,blen); emit_bstat(ta->sid,(int64_t)id,&bc);
            blk=NULL;bcap=blen=0;
        }
        size_t off=blen; if(blen+size>bcap){ bcap=blen+size>bcap*2?blen+size:bcap*2; if(bcap<(size_t)size)bcap=size; blk=realloc(blk,bcap); }
        int64_t got=0; while(got<size){ size_t take=size-got; uint8_t tmp[65536]; if(take>sizeof tmp)take=sizeof tmp;
            if(src_read(&s,tmp,take)<=0)break; memcpy(blk+off+got,tmp,take); got+=take; }
        for(int64_t k=size;k<padded;k++){uint8_t b;src_read(&s,&b,1);}
        bc_add_raw(&bc, name, NULL, off, size, mode, mtime, uid, gid, 0);
        blen+=size;
    }
    if(cmp){                                                     /* flush last frame; planner appends the tar EOF */
        if(prelen){ gcat(&blk,&bcap,&blen,pre,prelen); prelen=0; }
        if(blen){ uint64_t id=blk_add(blk,blen); emit_bstat(ta->sid,(int64_t)id,&bc); } else free(blk);
    }
    else if(ta->stat_only){ emit_bstat(ta->sid,-1,&bc); free(blk); }
    else if(blen){ uint64_t id=blk_add(blk,blen); emit_bstat(ta->sid,(int64_t)id,&bc); } else free(blk);
    emit_bstat(ta->sid,-1,&sbc);                                 /* footer sym/hardlink rows (block=-1) */
    bc_free(&bc); bc_free(&sbc); free(pre); emit_eof(ta->sid);
    if(s.ds) ZSTD_freeDStream(s.ds); close(s.fd); free(s.in); free(s.out); free(ta); return NULL;
}

/* ------------------------------------------------------------------ command loop */
static pthread_t *g_srcs; static int g_nsrc, g_srccap;
static void track(pthread_t t){ if(g_nsrc==g_srccap){ g_srccap=g_srccap?g_srccap*2:16; g_srcs=realloc(g_srcs,g_srccap*sizeof(pthread_t)); } g_srcs[g_nsrc++]=t; }
static char *readstr(uint8_t **p){ uint16_t l=RD(uint16_t,*p); char *s=malloc(l+1); memcpy(s,*p,l); s[l]=0; *p+=l; return s; }

int main(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: bvm <nworkers> <budget_mb> [--connect host:port]\n"); return 2; }
    /* --connect: take the COMMAND STREAM over TCP instead of stdin/stdout, by dup2'ing the
     * socket onto fd 0 and 1 -- every line of the protocol below then works unchanged.
     *
     * Why: one executor per srun means one ALLOCATION per executor, and a partial allocation
     * is silent. Observed: 2 of 3 nodes granted, the third queued behind another job, and the
     * planner picked it (zero outstanding wins every least-loaded tie-break), wrote to a pipe
     * nobody was draining, and blocked -- two live nodes idle for four minutes. One
     * `srun -N k -n k` is all-or-nothing, but its k tasks share ONE stdout, so the planner
     * could not tell them apart. A socket per task solves both: gang-scheduled allocation,
     * and a private full-duplex channel each. */
    for (int i = 3; i + 1 < argc; i++) {
        if (strcmp(argv[i], "--connect")) continue;
        int s = tcp_connect(argv[i + 1]);
        if (s < 0) { fprintf(stderr, "bvm: cannot connect to %s\n", argv[i + 1]); return 3; }
        if (dup2(s, 0) < 0 || dup2(s, 1) < 0) { perror("dup2"); return 3; }
        if (s > 2) close(s);
        break;
    }
    { const char *fm = getenv("BVM_FLUSH_MB"); if (fm) g_flush_bytes = atoll(fm) << 20; }
    g_sink_direct = getenv("BVM_SINK_DIRECT") != NULL;
    g_scan_prof   = getenv("BVM_SCAN_PROF") != NULL;
    { const char *e = getenv("BVM_RING_MAX_KB");   /* A/B the ring bound without a rebuild */
      if (e && atol(e) > 0) g_ring_max = (size_t)atol(e) << 10; }
    int nw = atoi(argv[1]);
    g_nworkers = nw;                                 /* emit_stats() runs on the sampler thread */ g_budget = atoll(argv[2]) << 20; if (g_budget <= 0) g_budget = 512LL << 20;
    for (int i = 0; i < 1024; i++) g_nock_fds[i] = -1;
    { char *q = getenv("BVM_URING_QD"); if (q) { g_uring_qd = atoi(q); if (g_uring_qd < 64) g_uring_qd = 64;
        if (g_uring_qd > 32768) g_uring_qd = 32768; } }
    { char *m = getenv("BVM_SCATTER_META"); if (m) g_scatter_meta = atoi(m); }
    { char *w = getenv("BVM_URING_WORKERS"); if (w) g_uring_workers = atoi(w); }
    { char *r = getenv("BVM_URING_RINGS"); if (r) g_uring_rings = atoi(r); }
    umask(0);                            /* so open(O_CREAT, mode) sets the exact perms (drops fchmod) */
    gear_init(); (void)sodium_init();    /* BLAKE2b chunk ids need no RNG, but init is cheap hygiene */
    g_start_ns = now_ns();
    { char *pb = getenv("BVM_PACK_BUDGET_MB");
      if (pb) g_pack_budget = atoll(pb) << 20;
      else {
          FILE *mf = fopen("/proc/meminfo", "r"); long kb = 0;
          if (mf) { fscanf(mf, "MemTotal: %ld kB", &kb); fclose(mf); }
          g_pack_budget = kb > 0 ? (int64_t)kb * 1024 * 35 / 100 : (64LL << 30);
      } }
    jq_init(&g_jq, nw * 4);
    W *ws = calloc(nw, sizeof(W)); pthread_t *th = calloc(nw, sizeof(pthread_t));
    for (int i = 0; i < nw; i++) { ws[i].c = ZSTD_createCCtx(); ws[i].d = ZSTD_createDCtx(); pthread_create(&th[i], 0, worker, &ws[i]); }
    pthread_t smp; pthread_create(&smp, 0, sampler, 0);          /* queue-occupancy sampler */
    int sn = nw; dq_init(&g_dq); pthread_t *sth = calloc(sn, sizeof(pthread_t));   /* parallel scan pool */
    g_nwscan = sn; g_wscan = calloc(sn, sizeof *g_wscan);
    for (int i = 0; i < sn; i++) pthread_create(&sth[i], 0, scan_walker, &g_wscan[i]);
    for (;;) {
        uint32_t len; if (read_full(0, &len, 4) <= 0) break;
        uint8_t *msg = malloc(len); if (read_full(0, msg, len) != 1) { free(msg); break; }
        uint8_t type = msg[0]; uint8_t *p = msg + 1;
        if (type == 1) { uint32_t sid = RD(uint32_t, p); uint8_t kind = RD(uint8_t, p); int64_t start = RD(int64_t, p);
            char *spec = readstr(&p); Sink *sk = &g_sinks[sid]; pthread_mutex_init(&sk->mu, 0);
            if (kind == 1) { sk->fd = tcp_connect(spec); sk->is_sock = 1;
                if (sk->fd >= 0 && g_have_key) {        /* AEAD: send the stream header first */
                    unsigned char hdr[crypto_secretstream_xchacha20poly1305_HEADERBYTES];
                    crypto_secretstream_xchacha20poly1305_init_push(&sk->st, hdr, g_key);
                    write_all(sk->fd, hdr, sizeof hdr); sk->enc = 1; } }
            else { sk->fd = open(spec, O_RDWR | O_CREAT | (g_sink_direct ? O_DIRECT : 0), 0644);
                   sk->is_sock = 0; sk->cursor = start; sk->flushed = start; }
            if ((int)sid + 1 > g_nsinks) g_nsinks = sid + 1; free(spec); }
        else if (type == 2) { char *path = readstr(&p); mkdir(path, 0777); g_destlen = strlen(path);
            memcpy(g_dest, path, g_destlen); if (g_destlen && g_dest[g_destlen-1] != '/') g_dest[g_destlen++] = '/'; g_dest[g_destlen] = 0; free(path); }
        else if (type == 3) { uint32_t id = RD(uint32_t, p); char *path = readstr(&p);
            if (id < 1024) g_nock_fds[id] = open(path, O_RDONLY); free(path); }
        else if (type == 4) { TarArg *ta = calloc(1, sizeof(TarArg)); ta->sid = RD(uint32_t, p); ta->frame_bytes = RD(int64_t, p);
            uint8_t fl = RD(uint8_t, p); ta->stat_only = fl & 1; ta->tar_compat = (fl >> 1) & 1;   /* flags byte */
            char *path = readstr(&p); strcpy(ta->path, path); free(path);
            pthread_t t; pthread_create(&t, 0, tar_thread, ta); track(t); }
        else if (type == 5 || type == 14) {             /* SCAN_FS / SCAN_NAMES (+ ignore patterns) */
            uint32_t sid = RD(uint32_t, p); char *root = readstr(&p);
            uint16_t npat = RD(uint16_t, p);
            IgnorePat *ign = npat ? calloc(npat, sizeof(IgnorePat)) : NULL;
            for (uint16_t k = 0; k < npat; k++) { ign[k].flags = RD(uint8_t, p); ign[k].pat = readstr(&p); }
            scan_start(sid, root, type == 14, ign, npat); free(root); }
        else if (type == 15) {                          /* MANIFEST: digest+chunk paths (delta planning) */
            uint32_t sid = RD(uint32_t, p); char *root = readstr(&p);
            const uint8_t *bp[3]; const uint8_t *cur = p; int64_t n;
            while ((n = arrow_next(&cur, bp, 3)) > 0) {
                const int64_t *po = (const int64_t *)bp[1]; const char *pd = (const char *)bp[2];
                Job *j = calloc(1, sizeof(Job)); j->kind = J_MANIFEST; j->frame_id = sid;
                j->root = strdup(root);
                j->nmemb = (uint32_t)n; j->memb = calloc(n, sizeof(Member));
                for (int64_t k = 0; k < n; k++)
                    j->memb[k].path = strndup(pd + po[k], (size_t)(po[k+1] - po[k]));
                jq_push(&g_jq, j);
            }
            if (n < 0) { fprintf(stderr, "bvm: MANIFEST batch shape mismatch\n"); g_fatal = 1; }
            free(root); }
        else if (type == 6) { Job *j = calloc(1, sizeof(Job)); j->kind = J_COPY_BLOCK;
            j->block_id = RD(uint64_t, p); j->frame_id = RD(uint64_t, p); j->level = RD(uint32_t, p); j->sink_id = RD(uint32_t, p);
            if (blk_ref(j->block_id)) { fprintf(stderr, "bvm: COPY_BLOCK on freed/unknown block %llu\n", (unsigned long long)j->block_id); g_fatal = 1; free(j); }
            else jq_push(&g_jq, j); }
        else if (type == 7) {
            /* [level][sink][tar_compat][root] + ONE Arrow batch, MANY frames, TYPED members:
             * fid i64, type u8 (0 file / 5 dir / 2 symlink / 1 hardlink), mode i32, size i64,
             * mtime_ns i64, uid i32, gid i32, src_off i64 (range reads: pack file bytes
             * [src_off, src_off+size) — delta literal runs), path, link -> 22 buffers. Dirs+links are
             * bodyless tar entries synthesized in the workers — FULL tar parity, one path. */
            uint32_t level = RD(uint32_t, p), sink_id = RD(uint32_t, p);
            uint8_t tc = RD(uint8_t, p); char *root = readstr(&p);
            const uint8_t *bp[22]; const uint8_t *cur = p; int64_t n;
            while ((n = arrow_next(&cur, bp, 22)) > 0) {
            const int64_t *fi = (const int64_t *)bp[1], *sz = (const int64_t *)bp[7], *mt = (const int64_t *)bp[9];
            const uint8_t *ty = bp[3];
            const int32_t *md = (const int32_t *)bp[5], *ui = (const int32_t *)bp[11], *gi = (const int32_t *)bp[13];
            const int64_t *so = (const int64_t *)bp[15];
            const int64_t *po = (const int64_t *)bp[17]; const char *pd = (const char *)bp[18];
            const int64_t *lo = (const int64_t *)bp[20]; const char *ld = (const char *)bp[21];
            for (int64_t s = 0; s < n; ) {
                int64_t e = s + 1;
                while (e < n && fi[e] == fi[s]) e++;
                Job *j = calloc(1, sizeof(Job)); j->kind = J_PACK_FILES;
                j->frame_id = (uint64_t)fi[s]; j->level = level; j->sink_id = sink_id;
                j->tar_compat = tc; j->root = strdup(root);   /* NOTE: a frame may straddle two
                                                               * record batches -> two jobs with the
                                                               * same fid; planner cuts message
                                                               * chunks on frame bounds so this can
                                                               * only happen within one message. */
                j->nmemb = (uint32_t)(e - s); j->memb = calloc(e - s, sizeof(Member));
                for (int64_t k = s; k < e; k++) { Member *m = &j->memb[k - s];
                    m->mtype = ty[k]; m->mode = (uint32_t)md[k]; m->size = sz[k]; m->mtime = mt[k];
                    m->uid = (uint32_t)ui[k]; m->gid = (uint32_t)gi[k]; m->out_off = so[k];   /* src_off */
                    m->path = strndup(pd + po[k], (size_t)(po[k+1] - po[k]));
                    m->link = lo[k+1] > lo[k] ? strndup(ld + lo[k], (size_t)(lo[k+1] - lo[k])) : NULL; }
                jq_push(&g_jq, j);
                s = e;
            }
            }
            if (n < 0) { fprintf(stderr, "bvm: PACK_FILES batch shape mismatch\n"); g_fatal = 1; }
            free(root); }
        else if (type == 8) {
            /* ONE Arrow batch, MANY frames: nock i32, coff i64, clen i64, in_off i64, size i64,
             * out_off i64, fsize i64, mode i32, mtime_ns i64, uid i32, gid i32, path -> 25 buffers.
             * out_off/fsize: EXTENT pieces — pwrite at out_off, ftruncate(fsize) (idempotent);
             * a whole member is the out_off=0, fsize=size case (ftruncate == the old O_TRUNC).
             * Frame identity is a COLUMN — one message covers ~1M members; jobs are cut here
             * at (nock,coff,clen) boundary changes. Kills the planner's per-frame Python
             * (polars pays ~0.4 ms fixed per IPC write; at ~8-member frames that dominated). */
            const uint8_t *bp[25]; const uint8_t *cur = p; int64_t n;
            while ((n = arrow_next(&cur, bp, 25)) > 0) {
            const int32_t *nk = (const int32_t *)bp[1], *md = (const int32_t *)bp[15],
                          *ui = (const int32_t *)bp[19], *gi = (const int32_t *)bp[21];
            const int64_t *co = (const int64_t *)bp[3], *cl = (const int64_t *)bp[5],
                          *io = (const int64_t *)bp[7], *sz = (const int64_t *)bp[9],
                          *oo = (const int64_t *)bp[11], *fs = (const int64_t *)bp[13],
                          *mt = (const int64_t *)bp[17];
            const int64_t *po = (const int64_t *)bp[23]; const char *pd = (const char *)bp[24];
            for (int64_t s = 0; s < n; ) {
                int64_t e = s + 1;
                while (e < n && nk[e] == nk[s] && co[e] == co[s] && cl[e] == cl[s]) e++;
                Job *j = calloc(1, sizeof(Job)); j->kind = J_SCATTER;
                j->coff = co[s]; j->clen = cl[s]; j->block_id = (uint64_t)(uint32_t)nk[s];
                j->nmemb = (uint32_t)(e - s); j->memb = calloc(e - s, sizeof(Member));
                for (int64_t k = s; k < e; k++) { Member *m = &j->memb[k - s];
                    m->in_off = io[k]; m->size = sz[k]; m->mode = (uint32_t)md[k];
                    m->mtime = mt[k]; m->uid = (uint32_t)ui[k]; m->gid = (uint32_t)gi[k];
                    m->out_off = oo[k]; m->fsize = fs[k];
                    m->path = strndup(pd + po[k], (size_t)(po[k+1] - po[k])); }
                jq_push(&g_jq, j);                       /* bounded queue = backpressure */
                s = e;
            }
            }
            if (n < 0) { fprintf(stderr, "bvm: SCATTER batch shape mismatch\n"); g_fatal = 1; } }
        else if (type == 9) { uint64_t bid = RD(uint64_t, p); blk_retire(bid); }   /* FREE = retire the owner */
        else if (type == 13) {                          /* UNLINK: io_uring batch unlink/rmdir */
            /* [u8 removedir] + ARROW batch: path large_utf8 -> 3 buffers */
            uint8_t removedir = RD(uint8_t, p);
            const uint8_t *bp[3]; const uint8_t *cur = p; int64_t bn; 
            for (;;) {
            bn = arrow_next(&cur, bp, 3);
            if (bn <= 0) break;
            uint32_t nn = (uint32_t)bn;
            char **paths = malloc((nn ? nn : 1) * sizeof(char *));
            uint8_t *failed = calloc(nn ? nn : 1, 1);
            { const int64_t *po = (const int64_t *)bp[1]; const char *pd = (const char *)bp[2];
              for (uint32_t k = 0; k < nn; k++) paths[k] = strndup(pd + po[k], (size_t)(po[k+1] - po[k])); }
            { int64_t tu = now_ns(); io_unlink_batch(paths, nn, removedir, failed);
              ST(ns_unlink, now_ns() - tu); ST(n_unlink, nn); }
            if (removedir) {                            /* WEKA: a just-emptied dir can transiently
                                                         * report ENOTEMPTY across client FEs — retry */
                for (int round = 0; round < 40; round++) {
                    uint32_t nf = 0;
                    for (uint32_t k = 0; k < nn; k++) if (failed[k] == ENOTEMPTY || failed[k] == EBUSY) nf++;
                    if (!nf) break;
                    struct timespec ts = {0, 25 * 1000000}; nanosleep(&ts, NULL);
                    char **rp = malloc(nf * sizeof(char *)); uint32_t *ri = malloc(nf * sizeof(uint32_t));
                    uint8_t *rf = calloc(nf, 1); uint32_t m = 0;
                    for (uint32_t k = 0; k < nn; k++) if (failed[k] == ENOTEMPTY || failed[k] == EBUSY) { rp[m] = paths[k]; ri[m] = k; m++; }
                    io_unlink_batch(rp, (int)nf, 1, rf);
                    for (uint32_t k2 = 0; k2 < nf; k2++) failed[ri[k2]] = rf[k2];
                    free(rp); free(ri); free(rf);
                }
            }
            int reported = 0;
            for (uint32_t k = 0; k < nn && reported < 20; k++)   /* cap the flood; count is in the errors */
                if (failed[k]) { emit_err(paths[k], -(int)failed[k]); reported++; }
            for (uint32_t k = 0; k < nn; k++) free(paths[k]);
            free(paths); free(failed);
            }
            if (bn < 0) { fprintf(stderr, "bvm: UNLINK batch shape mismatch\n"); g_fatal = 1; } }
        else if (type == 12) { Job *j = calloc(1, sizeof(Job)); j->kind = J_COPY_MEMBERS;
            /* [block][frame][level][sink][tc] + ARROW batch, TYPED (same cols as PACK_FILES
             * with in_off replacing fid): in_off i64, type u8, mode i32, size i64, mtime_ns
             * i64, uid i32, gid i32, path, link -> 20 bufs. in_off = SOURCE offset in the
             * block (bodyless types ignore it); headers synthesized in C. */
            j->block_id = RD(uint64_t, p); j->frame_id = RD(uint64_t, p); j->level = RD(uint32_t, p);
            j->sink_id = RD(uint32_t, p); j->tar_compat = RD(uint8_t, p);
            const uint8_t *bp[20]; const uint8_t *cur0 = p; int64_t n = arrow_next(&cur0, bp, 20);
            { const uint8_t *tmpc = cur0; const uint8_t *tb[20];
              if (arrow_next(&tmpc, tb, 20) > 0) {        /* one block per message by construction */
                  fprintf(stderr, "bvm: COPY_MEMBERS multi-batch stream\n"); g_fatal = 1; } }
            if (n < 0) { fprintf(stderr, "bvm: COPY_MEMBERS batch shape mismatch\n"); g_fatal = 1; free(j); continue; }
            j->nmemb = (uint32_t)n; j->memb = calloc(n ? n : 1, sizeof(Member));
            const int64_t *io = (const int64_t *)bp[1], *sz = (const int64_t *)bp[7], *mt = (const int64_t *)bp[9];
            const uint8_t *ty = bp[3];
            const int32_t *md = (const int32_t *)bp[5], *ui = (const int32_t *)bp[11], *gi = (const int32_t *)bp[13];
            const int64_t *po = (const int64_t *)bp[15]; const char *pd = (const char *)bp[16];
            const int64_t *lo = (const int64_t *)bp[18]; const char *ld = (const char *)bp[19];
            for (int64_t k = 0; k < n; k++) { Member *m = &j->memb[k];
                m->in_off = io[k]; m->mtype = ty[k]; m->size = sz[k]; m->mode = (uint32_t)md[k];
                m->mtime = mt[k]; m->uid = (uint32_t)ui[k]; m->gid = (uint32_t)gi[k];
                m->path = strndup(pd + po[k], (size_t)(po[k+1] - po[k]));
                m->link = lo[k+1] > lo[k] ? strndup(ld + lo[k], (size_t)(lo[k+1] - lo[k])) : NULL; }
            if (blk_ref(j->block_id)) { fprintf(stderr, "bvm: COPY_MEMBERS on freed/unknown block %llu\n", (unsigned long long)j->block_id); g_fatal = 1; job_free(j); }
            else jq_push(&g_jq, j); }
        else if (type == 10) { if (sodium_init() >= 0) { memcpy(g_key, p, sizeof g_key); g_have_key = 1; } }   /* KEY (32B) */
        else if (type == 11) { uint32_t port = RD(uint32_t, p); uint32_t nc = RD(uint32_t, p); g_apply = RD(uint8_t, p);   /* LISTEN */
            int ls = socket(AF_INET, SOCK_STREAM, 0); int one = 1; setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
            struct sockaddr_in sa; memset(&sa, 0, sizeof sa); sa.sin_family = AF_INET; sa.sin_addr.s_addr = INADDR_ANY; sa.sin_port = htons((uint16_t)port);
            if (bind(ls, (struct sockaddr *)&sa, sizeof sa) == 0 && listen(ls, nc) == 0)
                for (uint32_t k = 0; k < nc; k++) { int c = accept(ls, 0, 0); if (c < 0) break;
                    RxArg *r = malloc(sizeof(RxArg)); r->fd = c; pthread_t t; pthread_create(&t, 0, rx_reader, r); track(t); }
            close(ls); }
        free(msg);
    }
    dq_finish(&g_dq);
    for (int i = 0; i < sn; i++) pthread_join(sth[i], 0);          /* scan pool drained */
    for (int i = 0; i < sn; i++) { bc_free(&g_wscan[i].bc);
        if (g_wscan[i].ring_ok > 0) io_uring_queue_exit(&g_wscan[i].ring); }
    free(g_wscan); g_wscan = NULL; g_nwscan = 0;
    for (int i = 0; i < g_nsrc; i++) pthread_join(g_srcs[i], 0);   /* tar decode done */
    jq_finish(&g_jq);
    int64_t err = 0, files = 0;
    for (int i = 0; i < nw; i++) { pthread_join(th[i], 0); files += ws[i].files; if (ws[i].err && !err) err = ws[i].err;
        if (ws[i].ffd >= 0) close(ws[i].ffd);            /* the kept source fd */
        ZSTD_freeCCtx(ws[i].c); ZSTD_freeDCtx(ws[i].d); free(ws[i].cb); free(ws[i].ob); free(ws[i].mb); free(ws[i].ctb); free(ws[i].sb); }
    __atomic_store_n(&g_sampling, 0, __ATOMIC_RELAXED); pthread_join(smp, 0);
    emit_stats();                                    /* final sample: the run's totals */
    if (g_apply) rx_apply_meta();                    /* links + dir metadata: after ALL rx drains */
    for (int i = 0; i < g_nsinks; i++) if (g_sinks[i].fd >= 0) close(g_sinks[i].fd);   /* flush/EOF sinks */
    pthread_mutex_lock(&g_done_mu); flush_done_locked(); pthread_mutex_unlock(&g_done_mu);
    (void)err;                                       /* per-op errors are REPORTED (type 3) and the
                                                      * planner decides (strict=) — they must NOT turn
                                                      * into rc=1, which discards the whole run at
                                                      * finish() (lost two complete 4.5TB home packs
                                                      * to a handful of racing/unreadable files). */
    return g_fatal ? 1 : 0;                          /* g_fatal: a plan referenced a freed block */
}
