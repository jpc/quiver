/*
 * quiver-exec.c — quiver's filesystem execution engine + scanner as one process.
 *
 *   artar_exec exec <archive|-> [uring|sync]
 *       stdin : Arrow IPC stream of command rows (CMD schema below)
 *       stdout: Arrow IPC stream of completions {user_data,res,read_size}
 *   artar_exec scan <root> [uring|sync]
 *       stdout: Arrow IPC stream of stat rows
 *               {path,size,blocks,mtime_ns,ino,mode,uid,gid,nlink,is_dir}
 *
 * CMD schema (column order is the contract):
 *   user_data:u64  opcode:u8  dep_group:i64  path:large_string
 *   dst_path:large_string  header:large_binary  header_offset:i64
 *   data_offset:i64  size:i64  pad_align:i64  mode:i32
 *
 * Opcodes:
 *   0 COPY       copy `path` into the archive fd at data_offset,
 *                header bytes at header_offset, payload zero-padded
 *   2 UNLINK     unlinkat(path)
 *   3 RMDIR      unlinkat(path, AT_REMOVEDIR)
 *   4 MKDIR      mkdirat(path, mode)      (-EEXIST treated as success)
 *   5 COPY_FILE  copy `path` → `dst_path` (O_CREAT|O_TRUNC, mode)
 *
 * Ordering: rows must arrive sorted by dep_group. Epoch e+1 does not
 * start until every row of epoch e has completed — this is the barrier
 * mechanism (rmdir-after-children, mkdir-before-copy, delete-last).
 * The scheduler is free within an epoch; io_uring chains only encode
 * per-row op order (IOSQE_IO_LINK), never cross-row dependencies.
 *
 * Lineage note: the scan path is the ducl/pwalk2 design (getdents +
 * batched IORING_OP_STATX) except the output is Arrow record batches
 * on stdout instead of CSV — templates in ipc_gen.h, no arrow dep.
 */

#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fnmatch.h>
#include <liburing.h>
#include <linux/stat.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "ipc_gen.h"

#define QD 1024
#define WINDOW 128            /* max data-movement rows in flight on the pool */
#define SCAN_BATCH 4096       /* stat rows per emitted batch */
#define STATX_CHUNK 256       /* statx SQEs in flight per directory chunk */

/* S4: one copy opcode. Target = archive fd when dst_path is empty,
 * else open(dst, O_CREAT|O_TRUNC, mode). header bytes (if any) land at
 * header_offset, payload (padded to pad_align) at data_offset.
 * OP_* are #defined in ipc_gen.h, generated from quiver/opcodes.py — the
 * single source shared with the Python control plane. OP_EXTRACT reads
 * archive[data_offset,size] -> path; OP_COMPRESS zstd's a header payload
 * (level=pad_align), appends it, and reports the frame's (coff in read_size,
 * clen in cksum) — the un-plannable half of the zframe layout. */
/* mode/uid/gid/mtime_ns use -1 as "unspecified": COPY/MKDIR fall back
 * to 0644/0755, SETMETA leaves the attribute untouched. */
#define DEFAULT_FILE_MODE 0644
#define DEFAULT_DIR_MODE  0755

#include "md5.h"   /* vendored public-domain MD5 (Solar Designer);
                      -DHAVE_OPENSSL -lcrypto swaps in OpenSSL's asm */
#include <zstd.h>  /* OP_COMPRESS: zframe frame compression */

/* Serialized append point for OP_COMPRESS: frames from the pool are
 * appended in completion order, each getting the current offset. (The
 * multi-node path gives each node its own shard so this stays local.) */
static pthread_mutex_t g_append_mu = PTHREAD_MUTEX_INITIALIZER;
static int64_t g_append_off = 0;

/* ── CRC-64/NVME: reflected, poly 0xad93d23594c93659, composable ────────
 * Table-driven reference (~1 GB/s); the production path is PCLMULQDQ
 * (ISA-L / aws-checksums, tens of GB/s) behind the same interface. */
static uint64_t crc64_tab8[8][256];
#define crc64_tab crc64_tab8[0]
static void crc64_init_slices(void) {
    for (int i = 0; i < 256; i++)
        for (int t = 1; t < 8; t++)
            crc64_tab8[t][i] = crc64_tab8[t-1][i] >> 8
                ^ crc64_tab[crc64_tab8[t-1][i] & 0xff];
}

/* crc(A||B) from crc(A), crc(B), len(B): GF(2) matrix exponentiation
 * (zlib's crc32_combine generalized to the 64-bit reflected poly).
 * This is what makes part-parallel CRC composable. */
static uint64_t gf2_times(const uint64_t *mat, uint64_t v) {
    uint64_t s = 0;
    for (int b = 0; v; b++, v >>= 1) if (v & 1) s ^= mat[b];
    return s;
}
static void gf2_square(uint64_t *dst, const uint64_t *src) {
    for (int b = 0; b < 64; b++) dst[b] = gf2_times(src, src[b]);
}
static uint64_t crc64_combine(uint64_t crcA, uint64_t crcB, uint64_t lenB) {
    if (lenB == 0) return crcA;
    uint64_t even[64], odd[64];
    odd[0] = 0x9a6c9329ac4bc9b5ULL;              /* reflected poly */
    for (int b = 1; b < 64; b++) odd[b] = 1ULL << (b - 1);
    gf2_square(even, odd);                        /* x^2 */
    gf2_square(odd, even);                        /* x^4 */
    do {
        gf2_square(even, odd);
        if (lenB & 1) crcA = gf2_times(even, crcA);
        lenB >>= 1;
        if (!lenB) break;
        gf2_square(odd, even);
        if (lenB & 1) crcA = gf2_times(odd, crcA);
        lenB >>= 1;
    } while (lenB);
    return crcA ^ crcB;
}

static void crc64_init(void) {
    for (int i = 0; i < 256; i++) {
        uint64_t c = (uint64_t)i;
        for (int k = 0; k < 8; k++)
            c = (c >> 1) ^ ((c & 1) ? 0x9a6c9329ac4bc9b5ULL : 0);
        crc64_tab[i] = c;
    }
}
static uint64_t crc64_update(uint64_t crc, const uint8_t *p, size_t n) {
    while (n && ((uintptr_t)p & 7)) {            /* align head */
        crc = crc64_tab[(crc ^ *p++) & 0xff] ^ (crc >> 8); n--;
    }
    while (n >= 8) {                             /* slice-by-8 */
        uint64_t w; memcpy(&w, p, 8); w ^= crc;
        crc = crc64_tab8[7][ w        & 0xff] ^
              crc64_tab8[6][(w >>  8) & 0xff] ^
              crc64_tab8[5][(w >> 16) & 0xff] ^
              crc64_tab8[4][(w >> 24) & 0xff] ^
              crc64_tab8[3][(w >> 32) & 0xff] ^
              crc64_tab8[2][(w >> 40) & 0xff] ^
              crc64_tab8[1][(w >> 48) & 0xff] ^
              crc64_tab8[0][(w >> 56) & 0xff];
        p += 8; n -= 8;
    }
    while (n--) crc = crc64_tab[(crc ^ *p++) & 0xff] ^ (crc >> 8);
    return crc;
}

/* ── low-level IO ──────────────────────────────────────────────────────── */

static int read_full(int fd, void *p, size_t n) {
    uint8_t *b = p;
    while (n) {
        ssize_t r = read(fd, b, n);
        if (r == 0) return 1;
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        b += r; n -= (size_t)r;
    }
    return 0;
}

static int write_full(int fd, const void *p, size_t n) {
    const uint8_t *b = p;
    while (n) {
        ssize_t r = write(fd, b, n);
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        b += r; n -= (size_t)r;
    }
    return 0;
}

/* ── flatbuffer navigation (input side) ────────────────────────────────── */

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

/* ── generic template-patched batch emit (output side) ─────────────────── */

struct WBuf { const void *p; int64_t len; };   /* p==NULL → empty validity */

static int emit_batch(int fd, const unsigned char *tmpl, int tmpl_len,
                      int off_bodylen, int off_rblen,
                      const int *node_off, int n_nodes,
                      const int *buf_off, int n_bufs,
                      int64_t n_rows, const struct WBuf *bufs) {
    uint8_t *meta = malloc((size_t)tmpl_len);
    memcpy(meta, tmpl, (size_t)tmpl_len);
    int64_t pos = 0, zero = 0;
    for (int i = 0; i < n_bufs; i++) {
        memcpy(meta + buf_off[i], &pos, 8);
        memcpy(meta + buf_off[i] + 8, &bufs[i].len, 8);
        pos += (bufs[i].len + 7) & ~7LL;
    }
    memcpy(meta + off_bodylen, &pos, 8);
    memcpy(meta + off_rblen, &n_rows, 8);
    for (int i = 0; i < n_nodes; i++) {
        memcpy(meta + node_off[i], &n_rows, 8);
        memcpy(meta + node_off[i] + 8, &zero, 8);
    }
    uint32_t frame[2] = {0xFFFFFFFFu, (uint32_t)tmpl_len};
    static const uint8_t pad[8] = {0};
    int rc = write_full(fd, frame, 8) || write_full(fd, meta, (size_t)tmpl_len);
    for (int i = 0; !rc && i < n_bufs; i++) {
        if (bufs[i].len)
            rc = write_full(fd, bufs[i].p, (size_t)bufs[i].len) ||
                 write_full(fd, pad, (size_t)((-bufs[i].len) & 7));
    }
    free(meta);
    return rc ? -1 : 0;
}

static void emit_schema(int fd, const unsigned char *meta, int len) {
    uint32_t frame[2] = {0xFFFFFFFFu, (uint32_t)len};
    write_full(fd, frame, 8);
    write_full(fd, meta, (size_t)len);
}

static void emit_eos(int fd) {
    uint32_t eos[2] = {0xFFFFFFFFu, 0};
    write_full(fd, eos, 8);
}

/* ── command batch view + NUL-terminated path arena ────────────────────── */

typedef struct {
    int64_t n_rows;
    const uint64_t *user_data;
    const uint8_t  *opcode;
    const int64_t  *dep_group;
    const int64_t  *hdr_off; const uint8_t *hdr_data;
    const int64_t  *header_offset, *data_offset, *size, *pad_align;
    const int32_t  *mode;
    const int64_t  *mtime_ns;
    const int32_t  *uid, *gid;
    const int64_t  *parent_row;      /* -1 = free; else batch row index
                                        that must wait for this row */
    char **path;   /* NUL-terminated copies (arena) */
    char **dst;
    char *arena;
} CmdBatch;

enum { CB_UD_D=1, CB_OP_D=3, CB_DEP_D=5, CB_PATH_O=7, CB_PATH_D=8,
       CB_DST_O=10, CB_DST_D=11, CB_HDR_O=13, CB_HDR_D=14, CB_HO_D=16,
       CB_DO_D=18, CB_SZ_D=20, CB_PAD_D=22, CB_MODE_D=24,
       CB_MTIME_D=26, CB_UID_D=28, CB_GID_D=30, CB_PR_D=32,
       N_CMD_BUFS=33 };

static int parse_cmd_batch(const uint8_t *meta, const uint8_t *body,
                           CmdBatch *c) {
    int64_t rt = fb_root(meta);
    int64_t htp = fb_field(meta, rt, 1);
    if (htp < 0 || meta[htp] != 3) return -1;
    int64_t rb = fb_offset_field(meta, rt, 2);
    c->n_rows = fb_i64(meta, fb_field(meta, rb, 0));
    int64_t bufs = fb_offset_field(meta, rb, 2);
    if (fb_u32(meta, bufs) != N_CMD_BUFS) return -1;
    const uint8_t *p[N_CMD_BUFS];
    for (int i = 0; i < N_CMD_BUFS; i++)
        p[i] = body + fb_i64(meta, bufs + 4 + 16 * i);
    c->user_data     = (const uint64_t *)p[CB_UD_D];
    c->opcode        = p[CB_OP_D];
    c->dep_group     = (const int64_t *)p[CB_DEP_D];
    c->hdr_off       = (const int64_t *)p[CB_HDR_O];
    c->hdr_data      = p[CB_HDR_D];
    c->header_offset = (const int64_t *)p[CB_HO_D];
    c->data_offset   = (const int64_t *)p[CB_DO_D];
    c->size          = (const int64_t *)p[CB_SZ_D];
    c->pad_align     = (const int64_t *)p[CB_PAD_D];
    c->mode          = (const int32_t *)p[CB_MODE_D];
    c->mtime_ns      = (const int64_t *)p[CB_MTIME_D];
    c->uid           = (const int32_t *)p[CB_UID_D];
    c->gid           = (const int32_t *)p[CB_GID_D];
    c->parent_row    = (const int64_t *)p[CB_PR_D];

    const int64_t *po = (const int64_t *)p[CB_PATH_O];
    const int64_t *dofs = (const int64_t *)p[CB_DST_O];
    int64_t n = c->n_rows;
    c->arena = malloc((size_t)(po[n] + dofs[n] + 2 * n + 2));
    c->path = malloc(sizeof(char *) * (size_t)n);
    c->dst  = malloc(sizeof(char *) * (size_t)n);
    char *w = c->arena;
    for (int64_t i = 0; i < n; i++) {
        int64_t l = po[i + 1] - po[i];
        c->path[i] = w;
        memcpy(w, p[CB_PATH_D] + po[i], (size_t)l); w[l] = 0; w += l + 1;
        l = dofs[i + 1] - dofs[i];
        c->dst[i] = w;
        memcpy(w, p[CB_DST_D] + dofs[i], (size_t)l); w[l] = 0; w += l + 1;
    }
    return 0;
}

static void free_cmd_batch(CmdBatch *c) {
    free(c->arena); free(c->path); free(c->dst);
}

/* ── sync execution (fallback engine + short-read replay path) ─────────── */

typedef struct {
    int32_t res; int64_t read_size;
    uint64_t cksum; uint8_t etag[16]; int has_etag; int32_t parts;
} RowResult;

/* OP_CKSUM: stream the file once computing (a) per-part MD5s with the
 * deterministic part size carried in pad_align, folded into the S3
 * composite ETag, and (b) the full-object CRC64NVME. Single pass, one
 * buffer. Parallel plan (not in prototype): parts are independent →
 * per-part jobs; many-files case → ISA-L md5_mb lanes fed from the
 * io_uring reap loop, hashing batch A while the ring reads batch B. */
#define CK_THREADS 4

typedef struct {
    const char *path;
    int64_t part_size, first, step, nparts, fsize;
    uint8_t (*digests)[16];
    uint64_t *part_crc;
    int64_t *part_len;
    int err;
} CkJob;

static void *ck_worker(void *arg) {
    CkJob *j = arg;
    int fd = open(j->path, O_RDONLY);
    if (fd < 0) { j->err = -errno; return NULL; }
    uint8_t *buf = malloc(1 << 20);
    MD5_CTX md;
    for (int64_t p = j->first; p < j->nparts; p += j->step) {
        int64_t off = p * j->part_size;
        int64_t len = j->fsize - off;
        if (len > j->part_size) len = j->part_size;
        MD5_Init(&md);
        uint64_t crc = ~0ULL;
        int64_t done = 0;
        while (done < len) {
            ssize_t want = len - done;
            if (want > (1 << 20)) want = 1 << 20;
            ssize_t r = pread(fd, buf, (size_t)want, off + done);
            if (r <= 0) { j->err = r < 0 ? -errno : -EIO; goto out; }
            MD5_Update(&md, buf, (unsigned long)r);
            crc = crc64_update(crc, buf, (size_t)r);
            done += r;
        }
        MD5_Final(j->digests[p], &md);
        j->part_crc[p] = ~crc;
        j->part_len[p] = len;
    }
out:
    free(buf); close(fd);
    return NULL;
}

static int row_cksum_parallel(const char *path, int64_t part_size,
                              int64_t fsize, RowResult *out) {
    int64_t nparts = (fsize + part_size - 1) / part_size;
    uint8_t (*digests)[16] = malloc((size_t)nparts * 16);
    uint64_t *pcrc = malloc((size_t)nparts * 8);
    int64_t *plen = malloc((size_t)nparts * 8);
    int nt = nparts < CK_THREADS ? (int)nparts : CK_THREADS;
    pthread_t tid[CK_THREADS];
    CkJob jobs[CK_THREADS];
    for (int t = 0; t < nt; t++) {
        jobs[t] = (CkJob){path, part_size, t, nt, nparts, fsize,
                          digests, pcrc, plen, 0};
        pthread_create(&tid[t], NULL, ck_worker, &jobs[t]);
    }
    int err = 0;
    for (int t = 0; t < nt; t++) {
        pthread_join(tid[t], NULL);
        if (jobs[t].err && !err) err = jobs[t].err;
    }
    if (!err) {
        uint64_t crc = ~0ULL; crc = ~crc;        /* crc of empty = 0 */
        crc = 0;
        for (int64_t p = 0; p < nparts; p++)
            crc = crc64_combine(crc, pcrc[p], (uint64_t)plen[p]);
        out->cksum = crc;
        out->read_size = fsize;
        out->has_etag = 1;
        MD5_CTX md;
        MD5_Init(&md);
        MD5_Update(&md, digests, 16 * (unsigned long)nparts);
        MD5_Final(out->etag, &md);
        out->parts = (int)nparts;
    } else out->res = err;
    free(digests); free(pcrc); free(plen);
    return err;
}

static void row_cksum(const char *path, int64_t part_size, RowResult *out) {
    struct stat st;
    if (stat(path, &st) == 0 && st.st_size > 2 * part_size) {
        row_cksum_parallel(path, part_size, st.st_size, out);
        return;
    }
    int fd = open(path, O_RDONLY);
    if (fd < 0) { out->res = -errno; return; }
    uint64_t crc = ~0ULL;
    uint8_t digests[10000][16];
    int nparts = 0;
    MD5_CTX md;
    MD5_Init(&md);
    static __thread uint8_t *buf; 
    if (!buf) buf = malloc(1 << 20);
    int64_t in_part = 0, total = 0;
    for (;;) {
        ssize_t r = read(fd, buf, 1 << 20);
        if (r < 0) { out->res = -errno; break; }
        if (r == 0) break;
        crc = crc64_update(crc, buf, (size_t)r);
        ssize_t o = 0;
        while (o < r) {
            ssize_t take = r - o;
            if (take > part_size - in_part) take = part_size - in_part;
            MD5_Update(&md, buf + o, (unsigned long)take);
            o += take; in_part += take; total += take;
            if (in_part == part_size) {
                MD5_Final(digests[nparts++], &md);
                MD5_Init(&md);
                in_part = 0;
            }
        }
    }
    close(fd);
    if (out->res == 0) {
        if (in_part > 0 || nparts == 0)
            MD5_Final(digests[nparts++], &md);
        out->cksum = ~crc;
        out->read_size = total;
        out->has_etag = 1;
        if (nparts == 1 && total <= part_size) {
            memcpy(out->etag, digests[0], 16);       /* single-part PUT */
            out->parts = 0;
        } else {                                     /* composite */
            MD5_Init(&md);
            MD5_Update(&md, digests, 16 * (unsigned long)nparts);
            MD5_Final(out->etag, &md);
            out->parts = nparts;
        }
    }
}

static void row_sync(const CmdBatch *c, int64_t i, int afd, RowResult *out) {
    int64_t size = c->size[i], align = c->pad_align[i];
    int64_t hlen = c->hdr_off[i + 1] - c->hdr_off[i];
    out->res = 0; out->read_size = size;
    switch (c->opcode[i]) {
    case OP_CKSUM:
        row_cksum(c->path[i], align > 0 ? align : (5 << 20), out);
        return;
    case OP_UNLINK:
        if (unlink(c->path[i]) < 0) out->res = -errno;
        return;
    case OP_RMDIR:
        /* WEKA (and other distributed filesystems) can briefly report a
         * just-emptied directory as ENOTEMPTY: a child unlink/rmdir that
         * completed on one client frontend is not yet visible to this
         * rmdir on another. The epoch barrier guarantees the children's
         * syscalls returned; this absorbs the cross-frontend visibility
         * lag. Bounded (~0.2s worst case) so a genuinely non-empty dir —
         * a planner bug — still surfaces instead of hanging. */
        for (int t = 0; ; t++) {
            if (rmdir(c->path[i]) == 0) break;
            if (errno != ENOTEMPTY || t >= 40) { out->res = -errno; break; }
            usleep(t < 8 ? 100 * (t + 1) : 5000);
        }
        return;
    case OP_MKDIR:
        if (mkdir(c->path[i], c->mode[i] >= 0 ? (mode_t)c->mode[i]
                                              : DEFAULT_DIR_MODE) < 0
            && errno != EEXIST)
            out->res = -errno;
        return;
    case OP_FBARRIER: {
        /* durability barrier: empty path → fsync the archive fd;
         * path set → fsync that file/dir. Footer commit orders after
         * this epoch (nock §3.3: footer is the commit point). */
        int fd = c->path[i][0] ? open(c->path[i], O_RDONLY) : afd;
        if (fd < 0) { out->res = -errno; return; }
        if (fsync(fd) < 0) out->res = -errno;
        if (c->path[i][0]) close(fd);
        return;
    }
    case OP_SETMETA:
        /* chown/chmod/utimes have no io_uring opcodes → the sync pool.
         * -1 fields are left untouched. */
        if (c->mode[i] >= 0 &&
            chmod(c->path[i], (mode_t)c->mode[i]) < 0) out->res = -errno;
        if (out->res == 0 && (c->uid[i] >= 0 || c->gid[i] >= 0) &&
            chown(c->path[i], (uid_t)c->uid[i], (gid_t)c->gid[i]) < 0)
            out->res = -errno;
        if (out->res == 0 && c->mtime_ns[i] >= 0) {
            struct timespec ts[2] = {
                {c->mtime_ns[i] / 1000000000, c->mtime_ns[i] % 1000000000},
                {c->mtime_ns[i] / 1000000000, c->mtime_ns[i] % 1000000000}};
            if (utimensat(AT_FDCWD, c->path[i], ts, 0) < 0)
                out->res = -errno;
        }
        return;
    case OP_COPY: {
        int64_t dlen = 0;
        { const char *d = c->dst[i]; dlen = (int64_t)strlen(d); }
        int tfd = afd;
        if (dlen > 0) {
            /* chunked-write rule: a row targeting offset 0 owns the
             * truncate; rows at nonzero offsets append into place
             * (inline-payload chains order chunk0 first). */
            int tr = (c->header_offset[i] == 0 && c->data_offset[i] == 0)
                     ? O_TRUNC : 0;
            tfd = open(c->dst[i], O_WRONLY | O_CREAT | tr,
                       c->mode[i] >= 0 ? (mode_t)c->mode[i]
                                       : DEFAULT_FILE_MODE);
            if (tfd < 0) { out->res = -errno; return; }
        }
        if (hlen > 0 &&
            pwrite(tfd, c->hdr_data + c->hdr_off[i], (size_t)hlen,
                   c->header_offset[i]) != hlen) out->res = -errno;
        if (out->res == 0 && size > 0) {
            int sfd = open(c->path[i], O_RDONLY);
            if (sfd < 0) out->res = -errno;
            else {
                int64_t padded = (size + align - 1) / align * align;
                uint8_t *buf = calloc(1, (size_t)padded);
                int64_t got = 0;
                while (got < size) {
                    ssize_t r = read(sfd, buf + got, (size_t)(size - got));
                    if (r < 0) { out->res = -errno; break; }
                    if (r == 0) break;
                    got += r;
                }
                out->read_size = got;
                if (out->res == 0 &&
                    pwrite(tfd, buf, (size_t)padded,
                           c->data_offset[i]) != padded) out->res = -errno;
                free(buf); close(sfd);
            }
        }
        if (dlen > 0) close(tfd);
        return;
    }
    case OP_EXTRACT: {
        /* raw (zero-copy) EXTRACT: archive[data_offset, size] -> create path.
         * The compressed case (a member inside a zstd frame) is NOT here — it
         * is a COPY(BUF→FILE) inside a decode group, scattered from the buffer
         * an INFLATE filled (docs/ISA.md §2-§4, run_decode_epoch). */
        int dfd = open(c->path[i], O_WRONLY | O_CREAT | O_TRUNC,
                       c->mode[i] >= 0 ? (mode_t)c->mode[i]
                                       : DEFAULT_FILE_MODE);
        if (dfd < 0) { out->res = -errno; return; }
        static __thread uint8_t *buf;
        if (!buf) buf = malloc(1 << 20);
        int64_t left = size, got = 0;
        while (left > 0) {
            ssize_t want = left > (1 << 20) ? (1 << 20) : left;
            ssize_t r = pread(afd, buf, (size_t)want,
                              c->data_offset[i] + got);
            if (r <= 0) { out->res = r < 0 ? -errno : -EIO; break; }
            if (write(dfd, buf, (size_t)r) != r) { out->res = -errno; break; }
            left -= r; got += r;
        }
        out->read_size = got;
        close(dfd);
        return;
    }
    case OP_COMPRESS: {
        /* zstd the frame's inline payload (header column) and append it to
         * the archive; report (coff, clen) so the planner finalizes the
         * footer's frame offsets from completions. */
        int64_t hlen = c->hdr_off[i + 1] - c->hdr_off[i];
        const void *src = c->hdr_data + c->hdr_off[i];
        int lvl = align > 0 ? (int)align : 3;         /* level via pad_align */
        size_t bound = ZSTD_compressBound((size_t)hlen);
        uint8_t *comp = malloc(bound);
        if (!comp) { out->res = -ENOMEM; return; }
        size_t clen = ZSTD_compress(comp, bound, src, (size_t)hlen, lvl);
        if (ZSTD_isError(clen)) { out->res = -EIO; free(comp); return; }
        pthread_mutex_lock(&g_append_mu);
        int64_t coff = g_append_off;
        if (pwrite(afd, comp, clen, coff) != (ssize_t)clen)
            out->res = -errno;
        else
            g_append_off += (int64_t)clen;
        pthread_mutex_unlock(&g_append_mu);
        out->read_size = coff;                        /* frame offset */
        out->cksum = (uint64_t)clen;                  /* compressed length */
        free(comp);
        return;
    }
    default:
        out->res = -EINVAL;
    }
}

/* ── engine: sync pool + single-op ring, one epoch/refcount scheduler ─────
 *
 * Division of labour, settled by measurement (see docs/BENCH-IREN.md):
 *   • the io_uring ring runs ONLY single-op, native-opcode metadata —
 *     unlinkat / mkdirat / rmdir / fsync-on-archive. rm is ~20× coreutils
 *     this way and it is kernel-portable back to 5.6.
 *   • everything that moves bytes — COPY, EXTRACT, CKSUM, SETMETA,
 *     path-fsync — runs on the sync thread pool. On wekafs the pool beat
 *     io_uring read→write chains ~15× (io-wq punting serializes there),
 *     and it is the ONLY path when there is no ring (engine=sync, macOS).
 * So there is no io_uring data path, no fixed-file table, no direct-fd
 * chains, no pre-5.17 probe: the pool is the data plane on every kernel. */

typedef struct { int slot; } RowState;   /* pool rows carry only their slot */

#define OPEN_POOL 64

/* Leaf group ops (defined below); the persistent worker calls them by kind. */
static void decode_group(const CmdBatch *c, int64_t g, int afd, RowResult *out,
                         uint8_t **cb, size_t *ccap, uint8_t **ub, size_t *ucap);
static void encode_group(const CmdBatch *c, int64_t d, int afd, RowResult *out,
                         uint8_t **buf, size_t *bcap);

/* ── persistent unified worker pool ────────────────────────────────────────
 * ONE pool for the whole exec session — spawned once, reused across every
 * batch and epoch. (Measured: spawning+joining 64 threads per batch cost
 * ~12 ms/round-trip and capped the Python↔executor loop at ~80 rt/s; a
 * persistent pool lifts that ~8× and drops the viable stream block from
 * ~100 MB to ~15 MB.) It dispatches three work-item kinds:
 *   WK_ROW    → row_sync   (metadata, raw EXTRACT, COPY, CKSUM)
 *   WK_DECODE → decode_group (INFLATE a frame, scatter members)
 *   WK_ENCODE → encode_group (gather a frame, DEFLATE it)
 * Each worker keeps its own decode/encode scratch buffers, recycled across the
 * whole session (this also subsumes the per-frame malloc the old epochs did).
 * On wekafs, N plain threads scale where 5.15's io-wq punting doesn't
 * (measured: 20k copies t16 2.6s / t64 0.7s vs 36s through chains). */
enum { WK_ROW = 0, WK_DECODE = 1, WK_ENCODE = 2 };
typedef struct {
    const CmdBatch *c; RowResult *out; int afd;
    pthread_mutex_t mu; pthread_cond_t cv_work, cv_done;
    int64_t q[WINDOW]; uint8_t qk[WINDOW]; int qn;   /* work items waiting */
    int64_t dq[WINDOW]; int dn;                      /* rows fully executed */
    int active, stop, nthreads;
    pthread_t tid[OPEN_POOL];
} OpenPool;

static void *open_worker(void *arg) {
    OpenPool *p = arg;
    uint8_t *cb = NULL, *ub = NULL, *eb = NULL;      /* per-worker scratch */
    size_t ccap = 0, ucap = 0, ecap = 0;
    for (;;) {
        pthread_mutex_lock(&p->mu);
        while (p->qn == 0 && !p->stop)
            pthread_cond_wait(&p->cv_work, &p->mu);
        if (p->qn == 0 && p->stop) { pthread_mutex_unlock(&p->mu); break; }
        int idx = --p->qn;
        int64_t i = p->q[idx]; uint8_t k = p->qk[idx];
        p->active++;
        pthread_mutex_unlock(&p->mu);

        if (k == WK_ROW)         row_sync(p->c, i, p->afd, &p->out[i]);
        else if (k == WK_DECODE) decode_group(p->c, i, p->afd, p->out,
                                              &cb, &ccap, &ub, &ucap);
        else                     encode_group(p->c, i, p->afd, p->out,
                                              &eb, &ecap);

        pthread_mutex_lock(&p->mu);
        p->dq[p->dn++] = i;
        p->active--;
        pthread_cond_signal(&p->cv_done);
        pthread_mutex_unlock(&p->mu);
    }
    free(cb); free(ub); free(eb);
    return NULL;
}

static void pool_start(OpenPool *p) {
    memset(p, 0, sizeof *p);
    pthread_mutex_init(&p->mu, NULL);
    pthread_cond_init(&p->cv_work, NULL);
    pthread_cond_init(&p->cv_done, NULL);
    p->nthreads = OPEN_POOL;
    for (int t = 0; t < p->nthreads; t++)
        pthread_create(&p->tid[t], NULL, open_worker, p);
}

/* Point the (idle) pool at the current batch. Safe because a batch fully
 * drains before the next begins, so no worker references the old CmdBatch. */
static void pool_bind(OpenPool *p, const CmdBatch *c, RowResult *out, int afd) {
    p->c = c; p->out = out; p->afd = afd;
}

static void pool_stop(OpenPool *p) {
    pthread_mutex_lock(&p->mu);
    p->stop = 1;
    pthread_cond_broadcast(&p->cv_work);
    pthread_mutex_unlock(&p->mu);
    for (int t = 0; t < p->nthreads; t++) pthread_join(p->tid[t], NULL);
    pthread_mutex_destroy(&p->mu);
    pthread_cond_destroy(&p->cv_work);
    pthread_cond_destroy(&p->cv_done);
}

static void pool_push(OpenPool *p, uint8_t kind, int64_t i) {
    pthread_mutex_lock(&p->mu);
    p->qk[p->qn] = kind; p->q[p->qn] = i; p->qn++;
    pthread_cond_signal(&p->cv_work);
    pthread_mutex_unlock(&p->mu);
}

/* Harvest completed items. If `block`, wait until at least one is ready
 * (caller guarantees work is outstanding). Returns count. */
static int pool_harvest(OpenPool *p, int64_t *out, int block) {
    pthread_mutex_lock(&p->mu);
    if (block)
        while (p->dn == 0 && (p->qn > 0 || p->active > 0))
            pthread_cond_wait(&p->cv_done, &p->mu);
    int n = p->dn;
    memcpy(out, p->dq, (size_t)n * sizeof(int64_t));
    p->dn = 0;
    pthread_mutex_unlock(&p->mu);
    return n;
}

static int pool_outstanding(OpenPool *p) {
    pthread_mutex_lock(&p->mu);
    int n = p->qn + p->active + p->dn;
    pthread_mutex_unlock(&p->mu);
    return n;
}

/* Run a buffered epoch [e0,e1) through the persistent pool: push each group's
 * header row (INFLATE for decode, DEFLATE for encode) and wait for all to
 * retire. Bounded to WINDOW queued so q[] never overflows; worker scratch
 * buffers cap resident frame memory to OPEN_POOL frames. Members ride inside
 * their group (dispatched by the leaf op), so only header rows are queued. */
static void run_group_epoch(OpenPool *p, uint8_t kind, uint8_t hop,
                            int64_t e0, int64_t e1) {
    const CmdBatch *c = p->c;
    for (int64_t i = e0; i < e1; i++) {
        p->out[i].res = 0; p->out[i].read_size = c->size[i];
    }
    int64_t ng = 0;
    for (int64_t i = e0; i < e1; i++) if (c->opcode[i] == hop) ng++;
    int64_t got[WINDOW];
    int64_t i = e0, done = 0;
    while (done < ng) {
        pthread_mutex_lock(&p->mu);
        while (i < e1 && p->qn < WINDOW) {
            if (c->opcode[i] == hop) { p->qk[p->qn] = kind; p->q[p->qn++] = i; }
            i++;
        }
        pthread_cond_broadcast(&p->cv_work);
        pthread_mutex_unlock(&p->mu);
        done += pool_harvest(p, got, 1);
    }
}

/* ── buffered decode groups (docs/ISA.md §3-§4) ────────────────────────────
 * A decode group is one INFLATE row (archive[data_offset,size] → a worker-owned
 * buffer) immediately followed by K = header_offset member EXTRACT rows, each
 * scattering a buffer slice [header_offset, +size] to its file. One worker owns
 * one grow-on-demand buffer for a whole group and decodes the frame exactly
 * once — no cache, no refcount, no single-flight; decode-once is structural.
 * Buffers resident = min(#groups, OPEN_POOL). */

/* Source-file table for sharded decode (docs/ISA.md §10.5). The planner knows
 * the shard set ahead of time, so the files are declared on argv and opened
 * ONCE at startup (read-only, shared — pread is positioned + thread-safe); an
 * INFLATE selects its source by index in pad_align (shard_id). No table → the
 * single archive fd (linear). This is AOT, not a runtime path→fd cache. */
#define SRC_MAX 4096
static struct { int fds[SRC_MAX]; int n; } g_src;
static void src_open_all(int argc, char **argv, int first) {
    for (int i = first; i < argc && g_src.n < SRC_MAX; i++) {
        int fd = open(argv[i], O_RDONLY);          /* -1 → INFLATE reports err */
        if (fd < 0) perror(argv[i]);
        g_src.fds[g_src.n++] = fd;
    }
}
static inline int src_fd(int64_t sid, int afd) {
    return (g_src.n > 0 && sid >= 0 && sid < g_src.n) ? g_src.fds[sid] : afd;
}

static void decode_group(const CmdBatch *c, int64_t g, int afd, RowResult *out,
                         uint8_t **cb, size_t *ccap, uint8_t **ub, size_t *ucap) {
    int64_t coff = c->data_offset[g], clen = c->size[g], k = c->header_offset[g];
    int sfd = src_fd(c->pad_align[g], afd);        /* shard_id → source, or afd */
    out[g].res = 0; out[g].read_size = 0;
    if (sfd < 0) { out[g].res = -errno; return; }
    if ((size_t)clen > *ccap) { *cb = realloc(*cb, (size_t)clen); *ccap = (size_t)clen; }
    int64_t got = 0;
    while (got < clen) {
        ssize_t r = pread(sfd, *cb + got, (size_t)(clen - got), coff + got);
        if (r <= 0) { out[g].res = r < 0 ? -errno : -EIO; return; }
        got += r;
    }
    unsigned long long ds = ZSTD_getFrameContentSize(*cb, (size_t)clen);
    if (ds == ZSTD_CONTENTSIZE_ERROR || ds == ZSTD_CONTENTSIZE_UNKNOWN) {
        out[g].res = -EIO; return;
    }
    if (ds > *ucap) { *ub = realloc(*ub, (size_t)ds ? (size_t)ds : 1); *ucap = (size_t)ds; }
    size_t z = ZSTD_decompress(*ub, (size_t)ds, *cb, (size_t)clen);
    if (ZSTD_isError(z) || z != ds) { out[g].res = -EIO; return; }
    out[g].read_size = (int64_t)z;
    for (int64_t m = g + 1; m <= g + k; m++) {          /* scatter members */
        int64_t io = c->header_offset[m], sz = c->size[m];
        out[m].res = 0; out[m].read_size = sz;
        if (io < 0 || io + sz > (int64_t)z) { out[m].res = -EIO; continue; }
        int dfd = open(c->path[m], O_WRONLY | O_CREAT | O_TRUNC,
                       c->mode[m] >= 0 ? (mode_t)c->mode[m] : DEFAULT_FILE_MODE);
        if (dfd < 0) { out[m].res = -errno; continue; }
        int64_t left = sz, w = 0;
        while (left > 0) {
            ssize_t r = write(dfd, *ub + io + w, (size_t)left);
            if (r < 0) { out[m].res = -errno; break; }
            left -= r; w += r;
        }
        out[m].read_size = w;
        close(dfd);
    }
}

/* ── buffered encode groups (the dual: pack_fs, docs/ISA.md §3-§4) ──────────
 * A DEFLATE row heads a group: size = total assembled frame length, K =
 * header_offset = member count, pad_align = zstd level. Its K member rows
 * gather into a worker-owned buffer — each carries the inline `header` (its
 * PAX header, planner-formatted) at header_offset and its file body at
 * data_offset (pread of `path`, length `size`). Pad bytes are the zeroed
 * buffer. Then DEFLATE compresses [0,total], appends, and reports (coff in
 * read_size, clen in cksum) for the footer. Structural mirror of decode. */
static void encode_group(const CmdBatch *c, int64_t d, int afd, RowResult *out,
                         uint8_t **buf, size_t *bcap) {
    int64_t total = c->size[d], k = c->header_offset[d];
    int level = c->pad_align[d] > 0 ? (int)c->pad_align[d] : 3;
    out[d].res = 0; out[d].read_size = 0;
    if ((size_t)total > *bcap) { *buf = realloc(*buf, (size_t)total); *bcap = (size_t)total; }
    memset(*buf, 0, (size_t)total);
    for (int64_t m = d + 1; m <= d + k; m++) {
        out[m].res = 0; out[m].read_size = c->size[m];
        int64_t hlen = c->hdr_off[m + 1] - c->hdr_off[m];
        int64_t hoff = c->header_offset[m], boff = c->data_offset[m], sz = c->size[m];
        if (hoff < 0 || hoff + hlen > total || boff < 0 || boff + sz > total) {
            out[m].res = -EIO; out[d].res = -EIO; return;
        }
        memcpy(*buf + hoff, c->hdr_data + c->hdr_off[m], (size_t)hlen);
        int fd = open(c->path[m], O_RDONLY);
        if (fd < 0) { out[m].res = -errno; out[d].res = -errno; return; }
        int64_t got = 0;
        while (got < sz) {
            ssize_t r = pread(fd, *buf + boff + got, (size_t)(sz - got), got);
            if (r <= 0) { out[m].res = r < 0 ? -errno : -EIO; break; }
            got += r;
        }
        close(fd);
        if (out[m].res) { out[d].res = out[m].res; return; }
    }
    size_t bound = ZSTD_compressBound((size_t)total);
    uint8_t *comp = malloc(bound);
    if (!comp) { out[d].res = -ENOMEM; return; }
    size_t clen = ZSTD_compress(comp, bound, *buf, (size_t)total, level);
    if (ZSTD_isError(clen)) { out[d].res = -EIO; free(comp); return; }
    pthread_mutex_lock(&g_append_mu);
    int64_t coff = g_append_off;
    if (pwrite(afd, comp, clen, coff) != (ssize_t)clen) out[d].res = -errno;
    else g_append_off += (int64_t)clen;
    pthread_mutex_unlock(&g_append_mu);
    out[d].read_size = coff;                          /* frame offset */
    out[d].cksum = (uint64_t)clen;                    /* compressed length */
    free(comp);
}

/* The scheduler is engine-agnostic: epochs, refcount deps, and the slot
 * bound are identical whether a row executes as a single ring SQE or on
 * the pool. ring == NULL (engine=sync, or platforms without io_uring)
 * routes every row through the pool. */
static int run_batch_uring(struct io_uring *ring, OpenPool *pool,
                           const CmdBatch *c, int afd, RowResult *out) {
    int64_t n = c->n_rows;
    int use_ring = ring != NULL;
    RowState *rs = calloc((size_t)n, sizeof *rs);
    int64_t *rc = calloc((size_t)n, sizeof(int64_t));   /* children left */
    int64_t *ready = malloc((size_t)n * sizeof(int64_t));
    pool_bind(pool, c, out, afd);      /* persistent pool → this batch */
    int free_slots[WINDOW], n_free = WINDOW;
    for (int i = 0; i < WINDOW; i++) free_slots[i] = i;

    for (int64_t i = 0; i < n; i++) {
        int64_t p = c->parent_row[i];
        if (p < 0) continue;
        if (p >= n) { free(rs); free(rc); free(ready); return -EINVAL; }
        /* epoch-locality guard: a parent in a different epoch is
         * already ordered by the epoch barrier; counting it would
         * deadlock (its refcount could never drain). */
        if (c->dep_group[p] == c->dep_group[i]) rc[p]++;
    }

    int64_t e0 = 0;
    while (e0 < n) {
        int64_t e1 = e0;
        while (e1 < n && c->dep_group[e1] == c->dep_group[e0]) e1++;
        /* Buffered decode epoch: dispatch by group, not by row (§4). The
         * planner keeps each INFLATE + its members contiguous and in one epoch,
         * with parent_row=-1 (intra-group order is worker-sequential). */
        if (c->opcode[e0] == OP_INFLATE) {
            run_group_epoch(pool, WK_DECODE, OP_INFLATE, e0, e1);
            e0 = e1; continue;
        }
        if (c->opcode[e0] == OP_DEFLATE) {
            run_group_epoch(pool, WK_ENCODE, OP_DEFLATE, e0, e1);
            e0 = e1; continue;
        }
        int64_t done = 0, span = e1 - e0, n_ready = 0;
        int64_t chains_inflight = 0;               /* rows with SQEs pending */
        for (int64_t i = e1 - 1; i >= e0; i--)     /* pop ≈ batch order */
            if (rc[i] == 0) ready[n_ready++] = i;

        #define COMPLETE(i) do { \
            int64_t _p = c->parent_row[(i)]; \
            if (_p >= 0 && c->dep_group[_p] == c->dep_group[(i)] \
                && --rc[_p] == 0) ready[n_ready++] = _p; \
            done++; } while (0)

        while (done < span) {
            while (n_ready > 0 &&
                   (!use_ring || io_uring_sq_space_left(ring) >= 1)) {
                int64_t i = ready[--n_ready];
                uint8_t op = c->opcode[i];
                RowState *r = &rs[i];
                r->slot = -1;
                out[i].res = 0; out[i].read_size = c->size[i];

                /* Ring handles only single-op metadata; all data movement
                 * (and everything when there's no ring) goes to the pool. */
                /* RMDIR runs on the pool (row_sync) so it gets the
                 * ENOTEMPTY-retry that a distributed fs needs; the ring
                 * keeps the other single-op metadata. */
                int ring_op = use_ring &&
                    (op == OP_UNLINK || op == OP_MKDIR ||
                     (op == OP_FBARRIER && !c->path[i][0]));
                if (!ring_op) {
                    if (n_free == 0) { ready[n_ready++] = i; break; }
                    r->slot = free_slots[--n_free];
                    pool_push(pool, WK_ROW, i);
                    continue;
                }
                struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
                if (op == OP_MKDIR)
                    io_uring_prep_mkdirat(sqe, AT_FDCWD, c->path[i],
                            c->mode[i] >= 0 ? (mode_t)c->mode[i]
                                            : DEFAULT_DIR_MODE);
                else if (op == OP_FBARRIER)
                    io_uring_prep_fsync(sqe, afd, 0);
                else
                    io_uring_prep_unlinkat(sqe, AT_FDCWD, c->path[i],
                            op == OP_RMDIR ? AT_REMOVEDIR : 0);
                sqe->user_data = (uint64_t)i;
                chains_inflight++;
            }
            {                   /* rows come back from the pool fully
                                   executed — just release and account */
                int64_t got[WINDOW];
                /* block iff the pull loop can't progress either (no ready
                 * rows, or slots all held by queued rows) — otherwise a
                 * zero-CQE iteration would spin hot */
                int block = chains_inflight == 0 && done < span &&
                            (n_ready == 0 || n_free == 0) &&
                            pool_outstanding(pool) > 0;
                int k = pool_harvest(pool, got, block);
                for (int q = 0; q < k; q++) {
                    int64_t i = got[q];
                    free_slots[n_free++] = rs[i].slot; rs[i].slot = -1;
                    COMPLETE(i);
                }
            }
            if (done == span) break;
            if (chains_inflight == 0) continue;  /* progress came from the
                                                    pool or pull loop */
            int ret = io_uring_submit_and_wait(ring, 1);
            if (ret < 0 && ret != -EINTR) {
                free(rs); free(rc); free(ready); return ret;
            }

            struct io_uring_cqe *cqe;
            unsigned head, seen = 0;
            io_uring_for_each_cqe(ring, head, cqe) {
                seen++;
                int64_t i = (int64_t)cqe->user_data;   /* single op per row */
                if (cqe->res < 0 && !(c->opcode[i] == OP_MKDIR &&
                                      cqe->res == -EEXIST))
                    out[i].res = cqe->res;
                chains_inflight--;
                COMPLETE(i);
            }
            io_uring_cq_advance(ring, seen);
        }
        #undef COMPLETE
        e0 = e1;
    }
    free(rs); free(rc); free(ready);
    return 0;
}

/* ── exec mode main loop ───────────────────────────────────────────────── */

static int run_exec(int afd, int use_uring, struct io_uring *ring) {
    emit_schema(1, COMP_SCHEMA_META, COMP_SCHEMA_LEN);
    uint8_t *meta = NULL, *body = NULL;
    size_t mcap = 0, bcap = 0;
    OpenPool pool; pool_start(&pool);     /* one pool for the whole session */
    for (;;) {
        uint32_t hdr[2];
        int r = read_full(0, hdr, 8);
        if (r == 1 || (r == 0 && hdr[1] == 0)) break;
        if (r < 0 || hdr[0] != 0xFFFFFFFFu) return 1;
        if (hdr[1] > mcap) meta = realloc(meta, mcap = hdr[1]);
        if (read_full(0, meta, hdr[1])) return 1;
        int64_t rt = fb_root(meta);
        int64_t blp = fb_field(meta, rt, 3);
        int64_t blen = blp >= 0 ? fb_i64(meta, blp) : 0;
        if (blen > 0) {
            if ((size_t)blen > bcap) body = realloc(body, bcap = blen);
            if (read_full(0, body, (size_t)blen)) return 1;
        }
        int64_t htp = fb_field(meta, rt, 1);
        if (htp >= 0 && meta[htp] == 1) continue;          /* Schema msg */

        CmdBatch cb;
        if (parse_cmd_batch(meta, body, &cb)) return 1;
        RowResult *rr = calloc((size_t)cb.n_rows, sizeof *rr);
        if (run_batch_uring(use_uring ? ring : NULL, &pool, &cb, afd, rr))
            return 1;
        int64_t n = cb.n_rows;
        int32_t *res = malloc(4 * (size_t)n);
        int64_t *rsz = malloc(8 * (size_t)n);
        uint64_t *ck = malloc(8 * (size_t)n);
        int32_t *pr = malloc(4 * (size_t)n);
        int64_t *eo = malloc(8 * (size_t)(n + 1));
        uint8_t *ed = malloc(16 * (size_t)n);
        eo[0] = 0;
        for (int64_t i = 0; i < n; i++) {
            res[i] = rr[i].res; rsz[i] = rr[i].read_size;
            ck[i] = rr[i].cksum; pr[i] = rr[i].parts;
            int l = rr[i].has_etag ? 16 : 0;
            memcpy(ed + eo[i], rr[i].etag, (size_t)l);
            eo[i + 1] = eo[i] + l;
        }
        struct WBuf bufs[COMP_N_BUFS] = {
            {NULL, 0}, {cb.user_data, 8 * n},
            {NULL, 0}, {res, 4 * n},
            {NULL, 0}, {rsz, 8 * n},
            {NULL, 0}, {ck, 8 * n},
            {NULL, 0}, {eo, 8 * (n + 1)}, {ed, eo[n]},
            {NULL, 0}, {pr, 4 * n},
        };
        int rc = emit_batch(1, COMP_BATCH_TMPL, COMP_TMPL_LEN,
                            COMP_OFF_BODYLEN, COMP_OFF_RBLEN,
                            COMP_NODE_OFF, COMP_N_NODES,
                            COMP_BUF_OFF, COMP_N_BUFS, cb.n_rows, bufs);
        free(rr); free(res); free(rsz);
        free(ck); free(pr); free(eo); free(ed); free_cmd_batch(&cb);
        if (rc) return 1;
    }
    pool_stop(&pool);
    emit_eos(1);
    if (afd >= 0) fsync(afd);
    return 0;
}

/* ── scanner: pwalk2-model threaded walk ──────────────────────────────────
 *
 * Port of ducl/pwalk2's worker design (heavily tuned on large WEKA trees):
 *   - FIFO work queue of directories (mutex+cond), atomic dirs_in_flight
 *     for termination
 *   - per-worker io_uring + statx buffers; DOUBLE-BUFFERED pipeline:
 *     while the ring processes statx for batch A, getdents64 reads batch B,
 *     overlapping the two network-metadata latency sources
 *   - getdents64 concurrency gate bounding simultaneous readdir RPCs
 *   - output under a mutex; here the unit is a whole Arrow record batch,
 *     so worker batches interleave losslessly in one IPC stream
 * Omitted vs pwalk2 (noted): stuck-op watchdog (SIGUSR1), exclude lists.
 */

#include <pthread.h>
#include <stdatomic.h>
#include <sys/sysmacros.h>

#define ENT_CAP 3072              /* getdents may overshoot the target */
#define STATX_TARGET 256          /* ≈ per-worker ring depth */

struct dent64 { uint64_t d_ino; int64_t d_off; unsigned short d_reclen;
                unsigned char d_type; char d_name[]; };

typedef struct { char name[256]; struct statx stx; int valid;
                 unsigned char d_type; } Ent;

typedef struct Work {
    struct Work *next;
    uint64_t dir_ino;             /* parent_ino for this dir's entries */
    int32_t depth;                /* entries emitted at depth+1 */
    char rel[];
} Work;

static struct {
    Work *head, *tail;
    pthread_mutex_t mu; pthread_cond_t cv;
    _Atomic int in_flight;
    pthread_mutex_t out_mu;
    pthread_mutex_t gd_mu; pthread_cond_t gd_cv;
    int gd_active, gd_limit;
    const char *root;
    int use_uring;
    const char *prefix;              /* stage-1 pushdown: subtree prune */
    const char *glob;                /* stage-2 pushdown: skip statx    */
    int emit_closes;                 /* emit dir close-events (rm/rsync) */
    _Atomic long dirs_opened, statx_done, emitted;
} G;

/* rel is under the prefix (emit) */
static int under_prefix(const char *rel) {
    size_t pl_ = strlen(G.prefix);
    if (!pl_) return 1;
    return strncmp(rel, G.prefix, pl_) == 0 &&
           (rel[pl_] == 0 || rel[pl_] == '/');
}

/* rel is an ancestor of the prefix (descend but don't emit) */
static int ancestor_of_prefix(const char *rel) {
    size_t rl = strlen(rel);
    if (!*G.prefix) return 0;
    if (!rl) return 1;
    return strncmp(G.prefix, rel, rl) == 0 && G.prefix[rl] == '/';
}

static void q_push(Work *w) {
    pthread_mutex_lock(&G.mu);
    w->next = NULL;
    if (G.tail) G.tail->next = w; else G.head = w;
    G.tail = w;
    pthread_cond_signal(&G.cv);
    pthread_mutex_unlock(&G.mu);
}

static Work *q_pop(void) {
    pthread_mutex_lock(&G.mu);
    while (!G.head) {
        if (atomic_load(&G.in_flight) == 0) {
            pthread_cond_broadcast(&G.cv);
            pthread_mutex_unlock(&G.mu);
            return NULL;
        }
        pthread_cond_wait(&G.cv, &G.mu);
    }
    Work *w = G.head;
    G.head = w->next;
    if (!G.head) G.tail = NULL;
    pthread_mutex_unlock(&G.mu);
    return w;
}

static void gd_enter(void) {
    pthread_mutex_lock(&G.gd_mu);
    while (G.gd_active >= G.gd_limit)
        pthread_cond_wait(&G.gd_cv, &G.gd_mu);
    G.gd_active++;
    pthread_mutex_unlock(&G.gd_mu);
}

static void gd_exit(void) {
    pthread_mutex_lock(&G.gd_mu);
    G.gd_active--;
    pthread_cond_signal(&G.gd_cv);
    pthread_mutex_unlock(&G.gd_mu);
}

/* ── columnar builder for STAT batches ─────────────────────────────────── */

typedef struct {
    int64_t n, cap;
    char *pdata; int64_t pdata_len, pdata_cap;
    int64_t *poff;
    int64_t *size, *blocks, *mtime, *atime, *ctime;
    uint64_t *ino, *pino, *dev;
    int32_t *mode, *uid, *gid, *nlink, *depth;
    uint8_t *is_dir;
    int64_t *child_count;         /* -1 normal row; >=0 dir close-event */
} StatBuilder;

static void sb_init(StatBuilder *b) {
    memset(b, 0, sizeof *b);
    b->cap = SCAN_BATCH;
    b->pdata_cap = 1 << 20;
    b->pdata = malloc((size_t)b->pdata_cap);
    b->poff = malloc(8 * (size_t)(b->cap + 1)); b->poff[0] = 0;
    int64_t **i64s[] = {&b->size,&b->blocks,&b->mtime,&b->atime,&b->ctime};
    for (unsigned k = 0; k < 5; k++) *i64s[k] = malloc(8 * (size_t)b->cap);
    uint64_t **u64s[] = {&b->ino,&b->pino,&b->dev};
    for (unsigned k = 0; k < 3; k++) *u64s[k] = malloc(8 * (size_t)b->cap);
    int32_t **i32s[] = {&b->mode,&b->uid,&b->gid,&b->nlink,&b->depth};
    for (unsigned k = 0; k < 5; k++) *i32s[k] = malloc(4 * (size_t)b->cap);
    b->is_dir = malloc((size_t)b->cap);
    b->child_count = malloc(8 * (size_t)b->cap);
}

static void sb_row(StatBuilder *b, const char *rel, int64_t rel_len,
                   const struct statx *sx, uint64_t parent_ino,
                   int32_t depth) {
    while (b->pdata_len + rel_len > b->pdata_cap)
        b->pdata = realloc(b->pdata, (size_t)(b->pdata_cap *= 2));
    memcpy(b->pdata + b->pdata_len, rel, (size_t)rel_len);
    b->pdata_len += rel_len;
    int64_t i = b->n++;
    b->poff[i + 1] = b->pdata_len;
    b->size[i] = (int64_t)sx->stx_size;
    b->blocks[i] = (int64_t)sx->stx_blocks;
    b->mtime[i] = (int64_t)sx->stx_mtime.tv_sec * 1000000000LL + sx->stx_mtime.tv_nsec;
    b->atime[i] = (int64_t)sx->stx_atime.tv_sec * 1000000000LL + sx->stx_atime.tv_nsec;
    b->ctime[i] = (int64_t)sx->stx_ctime.tv_sec * 1000000000LL + sx->stx_ctime.tv_nsec;
    b->ino[i] = sx->stx_ino;
    b->pino[i] = parent_ino;
    b->dev[i] = makedev(sx->stx_dev_major, sx->stx_dev_minor);
    b->mode[i] = sx->stx_mode;
    b->uid[i] = (int32_t)sx->stx_uid; b->gid[i] = (int32_t)sx->stx_gid;
    b->nlink[i] = (int32_t)sx->stx_nlink;
    b->depth[i] = depth;
    b->is_dir[i] = S_ISDIR(sx->stx_mode) ? 1 : 0;
    b->child_count[i] = -1;                    /* normal stat row */
}

/* directory close-event: a row with path=dir, is_dir=1, child_count=N
 * (its emitted-child count). Consumers keyed on path arithmetic — a
 * child "d/f" decrements remaining["d"]; dirname of a root child is ""
 * which is the root's own rel, so the root close event lines up too. */
static void sb_close(StatBuilder *b, const char *rel, int64_t rel_len,
                     int64_t nchild, int32_t depth) {
    while (b->pdata_len + rel_len > b->pdata_cap)
        b->pdata = realloc(b->pdata, (size_t)(b->pdata_cap *= 2));
    memcpy(b->pdata + b->pdata_len, rel, (size_t)rel_len);
    b->pdata_len += rel_len;
    int64_t i = b->n++;
    b->poff[i + 1] = b->pdata_len;
    b->size[i] = 0; b->blocks[i] = 0;
    b->mtime[i] = 0; b->atime[i] = 0; b->ctime[i] = 0;
    b->ino[i] = 0; b->pino[i] = 0; b->dev[i] = 0;
    b->mode[i] = 0; b->uid[i] = 0; b->gid[i] = 0; b->nlink[i] = 0;
    b->depth[i] = depth; b->is_dir[i] = 1;
    b->child_count[i] = nchild;
}

static int sb_flush(StatBuilder *b) {          /* holds the output mutex */
    if (b->n == 0) return 0;
    struct WBuf bufs[STAT_N_BUFS] = {
        {NULL,0},{b->poff, 8*(b->n+1)},{b->pdata, b->pdata_len},
        {NULL,0},{b->size, 8*b->n}, {NULL,0},{b->blocks, 8*b->n},
        {NULL,0},{b->mtime, 8*b->n}, {NULL,0},{b->atime, 8*b->n},
        {NULL,0},{b->ctime, 8*b->n}, {NULL,0},{b->ino, 8*b->n},
        {NULL,0},{b->pino, 8*b->n}, {NULL,0},{b->dev, 8*b->n},
        {NULL,0},{b->mode, 4*b->n}, {NULL,0},{b->uid, 4*b->n},
        {NULL,0},{b->gid, 4*b->n}, {NULL,0},{b->nlink, 4*b->n},
        {NULL,0},{b->depth, 4*b->n}, {NULL,0},{b->is_dir, b->n},
        {NULL,0},{b->child_count, 8*b->n},
    };
    pthread_mutex_lock(&G.out_mu);
    int rc = emit_batch(1, STAT_BATCH_TMPL, STAT_TMPL_LEN,
                        STAT_OFF_BODYLEN, STAT_OFF_RBLEN,
                        STAT_NODE_OFF, STAT_N_NODES,
                        STAT_BUF_OFF, STAT_N_BUFS, b->n, bufs);
    pthread_mutex_unlock(&G.out_mu);
    b->n = 0; b->pdata_len = 0; b->poff[0] = 0;
    return rc;
}

/* ── worker ────────────────────────────────────────────────────────────── */

typedef struct {
    struct io_uring ring; int uring_ok;
    StatBuilder b;
    Ent *batch[2];
    uint8_t gdbuf[1 << 16];
} Worker;

/* getdents64 until ≥ STATX_TARGET entries or EOF (each getdents buffer is
 * fully parsed, so counts can overshoot — Ent arrays are sized for it). */
static int read_batch(Worker *w, int dfd, Ent *ents, int *cnt, int *eof) {
    *cnt = 0;
    gd_enter();
    while (*cnt < STATX_TARGET && !*eof) {
        long nr = syscall(SYS_getdents64, dfd, w->gdbuf, sizeof w->gdbuf);
        if (nr < 0) { gd_exit(); return -1; }
        if (nr == 0) { *eof = 1; break; }
        long off = 0;
        while (off < nr && *cnt < ENT_CAP) {
            struct dent64 *d = (struct dent64 *)(w->gdbuf + off);
            off += d->d_reclen;
            if (d->d_name[0] == '.' && (d->d_name[1] == 0 ||
                (d->d_name[1] == '.' && d->d_name[2] == 0)))
                continue;
            Ent *e = &ents[(*cnt)++];
            strncpy(e->name, d->d_name, sizeof e->name - 1);
            e->name[sizeof e->name - 1] = 0;
            e->valid = 0;
            e->d_type = d->d_type;
        }
    }
    gd_exit();
    return 0;
}

/* stage-2 pushdown: compact away entries that need no statx — a known
 * regular file that can neither be emitted (prefix/glob miss) nor
 * descended. The SQEs for dropped entries are never submitted; on a
 * network filesystem this is where the win lives. */
static int filter_batch(const char *dir_rel, Ent *ents, int cnt) {
    char rel[4400];
    int out = 0;
    for (int k = 0; k < cnt; k++) {
        snprintf(rel, sizeof rel, "%s%s%s", dir_rel,
                 *dir_rel ? "/" : "", ents[k].name);
        int emit = under_prefix(rel) &&
                   (!*G.glob || fnmatch(G.glob, ents[k].name, 0) == 0);
        int maybe_dir = ents[k].d_type == 4 /*DT_DIR*/ ||
                        ents[k].d_type == 0 /*DT_UNKNOWN*/;
        int descend = maybe_dir && (under_prefix(rel) ||
                                    ancestor_of_prefix(rel));
        if (emit || descend)
            ents[out++] = ents[k];
    }
    return out;
}

static void stat_batch_submit(Worker *w, int dfd, Ent *ents, int cnt) {
    for (int k = 0; k < cnt; k++) {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&w->ring);
        io_uring_prep_statx(sqe, dfd, ents[k].name, AT_SYMLINK_NOFOLLOW,
                            STATX_BASIC_STATS, &ents[k].stx);
        sqe->user_data = (uint64_t)k;
    }
    io_uring_submit(&w->ring);
    atomic_fetch_add(&G.statx_done, cnt);
}

static void stat_batch_reap(Worker *w, Ent *ents, int cnt) {
    int got = 0;
    while (got < cnt) {
        io_uring_submit_and_wait(&w->ring, 1);
        struct io_uring_cqe *cqe;
        unsigned head, seen = 0;
        io_uring_for_each_cqe(&w->ring, head, cqe) {
            seen++; got++;
            ents[cqe->user_data].valid = cqe->res >= 0;
        }
        io_uring_cq_advance(&w->ring, seen);
    }
}

static void process_batch(Worker *w, const char *dir_rel, uint64_t dir_ino,
                          int32_t depth, Ent *ents, int cnt,
                          int64_t *nchild) {
    char rel[4400];
    for (int k = 0; k < cnt; k++) {
        if (!ents[k].valid) continue;
        int rl = snprintf(rel, sizeof rel, "%s%s%s", dir_rel,
                          *dir_rel ? "/" : "", ents[k].name);
        int emit = under_prefix(rel) &&
                   (!*G.glob || fnmatch(G.glob, ents[k].name, 0) == 0);
        if (emit) {
            sb_row(&w->b, rel, rl, &ents[k].stx, dir_ino, depth + 1);
            atomic_fetch_add(&G.emitted, 1);
            (*nchild)++;    /* one decrement of this dir (unlink or rmdir) */
        }
        if (S_ISDIR(ents[k].stx.stx_mode)) {
            Work *nw = malloc(sizeof(Work) + (size_t)rl + 1);
            nw->dir_ino = ents[k].stx.stx_ino;
            nw->depth = depth + 1;
            memcpy(nw->rel, rel, (size_t)rl + 1);
            atomic_fetch_add(&G.in_flight, 1);
            q_push(nw);
        }
        if (w->b.n >= SCAN_BATCH) sb_flush(&w->b);
    }
}

static void *scan_worker(void *arg) {
    Worker *w = arg;
    char abs[4400];
    Work *wk;
    while ((wk = q_pop()) != NULL) {
        snprintf(abs, sizeof abs, "%s%s%s", G.root,
                 *wk->rel ? "/" : "", wk->rel);
        int dfd = open(abs, O_RDONLY | O_DIRECTORY);
        if (dfd >= 0) {
            atomic_fetch_add(&G.dirs_opened, 1);
            int64_t nchild = 0;
            int a = 0, cntA = 0, cntB = 0, eof = 0;
            read_batch(w, dfd, w->batch[a], &cntA, &eof);
            cntA = filter_batch(wk->rel, w->batch[a], cntA);
            while (cntA > 0) {
                if (w->uring_ok)
                    stat_batch_submit(w, dfd, w->batch[a], cntA);
                if (!eof) {                     /* overlap: readdir next */
                    read_batch(w, dfd, w->batch[1 - a], &cntB, &eof);
                    cntB = filter_batch(wk->rel, w->batch[1 - a], cntB);
                } else cntB = 0;
                if (w->uring_ok)
                    stat_batch_reap(w, w->batch[a], cntA);
                else {
                    for (int k = 0; k < cntA; k++)
                        w->batch[a][k].valid = statx(dfd, w->batch[a][k].name,
                            AT_SYMLINK_NOFOLLOW, STATX_BASIC_STATS,
                            &w->batch[a][k].stx) == 0;
                    atomic_fetch_add(&G.statx_done, cntA);
                }
                process_batch(w, wk->rel, wk->dir_ino, wk->depth,
                              w->batch[a], cntA, &nchild);
                a = 1 - a; cntA = cntB;
            }
            close(dfd);
            if (G.emit_closes) {                /* dir fully walked */
                if (w->b.n >= SCAN_BATCH) sb_flush(&w->b);   /* ensure room */
                sb_close(&w->b, wk->rel, (int64_t)strlen(wk->rel),
                         nchild, wk->depth);
                if (w->b.n >= SCAN_BATCH) sb_flush(&w->b);   /* keep n<cap */
            }
        }
        free(wk);
        if (atomic_fetch_sub(&G.in_flight, 1) == 1) {
            pthread_mutex_lock(&G.mu);          /* last dir: wake sleepers */
            pthread_cond_broadcast(&G.cv);
            pthread_mutex_unlock(&G.mu);
        }
    }
    sb_flush(&w->b);
    return NULL;
}

static int run_scan(const char *root, int use_uring, int threads,
                    const char *prefix, const char *glob, int emit_closes) {
    emit_schema(1, STAT_SCHEMA_META, STAT_SCHEMA_LEN);
    G.root = root; G.use_uring = use_uring;
    G.prefix = prefix; G.glob = glob; G.emit_closes = emit_closes;
    pthread_mutex_init(&G.mu, NULL); pthread_cond_init(&G.cv, NULL);
    pthread_mutex_init(&G.out_mu, NULL);
    pthread_mutex_init(&G.gd_mu, NULL); pthread_cond_init(&G.gd_cv, NULL);
    G.gd_limit = threads;          /* pwalk2 default: gate = thread count */

    struct statx rsx;
    if (statx(AT_FDCWD, root, 0, STATX_BASIC_STATS, &rsx) < 0) {
        perror("root"); return 2;
    }
    Work *rw = malloc(sizeof(Work) + 1);
    rw->dir_ino = rsx.stx_ino; rw->depth = 0; rw->rel[0] = 0;
    atomic_store(&G.in_flight, 1);
    q_push(rw);

    Worker *ws = calloc((size_t)threads, sizeof *ws);
    pthread_t *tids = malloc(sizeof(pthread_t) * (size_t)threads);
    for (int i = 0; i < threads; i++) {
        Worker *w = &ws[i];
        sb_init(&w->b);
        w->batch[0] = malloc(sizeof(Ent) * ENT_CAP);
        w->batch[1] = malloc(sizeof(Ent) * ENT_CAP);
        w->uring_ok = use_uring &&
                      io_uring_queue_init(2 * STATX_TARGET, &w->ring, 0) >= 0;
        pthread_create(&tids[i], NULL, scan_worker, w);
    }
    for (int i = 0; i < threads; i++) pthread_join(tids[i], NULL);
    emit_eos(1);
    fprintf(stderr, "quiver-exec scan: dirs=%ld statx=%ld emitted=%ld\n",
            atomic_load(&G.dirs_opened), atomic_load(&G.statx_done),
            atomic_load(&G.emitted));
    return 0;
}

/* ── main ──────────────────────────────────────────────────────────────── */

/* ── zpack: tar.zstd → per-batch-frame nock, fully in C (GIL-free) ─────────
 * Reader threads decompress+parse sources and batch members into frame
 * buffers; a compress pool zstd's each frame, appends it, and assigns the
 * frame index + compressed offset (the un-plannable half). Per-member footer
 * records go to stdout; Python writes the trailing skippable-frame index.
 * Parse AND compress are both off the GIL — the Python thread version capped
 * at ~10 cores; this saturates all of them. */

typedef struct { uint8_t *ib, *ob; size_t icap, isz, ipos, ocap, osz, opos;
                 int fd, eof; ZSTD_DStream *ds; } Zsrc;

static int zsrc_open(Zsrc *z, const char *p) {
    z->fd = open(p, O_RDONLY);
    if (z->fd < 0) return -1;
    z->ds = ZSTD_createDStream(); ZSTD_initDStream(z->ds);
    z->icap = ZSTD_DStreamInSize(); z->ib = malloc(z->icap);
    z->ocap = ZSTD_DStreamOutSize(); z->ob = malloc(z->ocap);
    z->isz = z->ipos = z->osz = z->opos = 0; z->eof = 0;
    return 0;
}
static void zsrc_close(Zsrc *z) {
    ZSTD_freeDStream(z->ds); free(z->ib); free(z->ob); close(z->fd);
}
static size_t zsrc_read(Zsrc *z, uint8_t *dst, size_t n) {
    size_t got = 0;
    while (got < n) {
        while (z->opos >= z->osz) {                 /* refill decompressed */
            if (z->ipos >= z->isz && !z->eof) {
                ssize_t r = read(z->fd, z->ib, z->icap);
                if (r <= 0) z->eof = 1; else { z->isz = r; z->ipos = 0; }
            }
            if (z->ipos >= z->isz && z->eof) return got;
            ZSTD_inBuffer in = {z->ib, z->isz, z->ipos};
            ZSTD_outBuffer out = {z->ob, z->ocap, 0};
            size_t rc = ZSTD_decompressStream(z->ds, &out, &in);
            if (ZSTD_isError(rc)) return got;
            z->ipos = in.pos; z->osz = out.pos; z->opos = 0;
        }
        size_t take = z->osz - z->opos;
        if (take > n - got) take = n - got;
        memcpy(dst + got, z->ob + z->opos, take);
        z->opos += take; got += take;
    }
    return got;
}

static int64_t zoctal(const uint8_t *b, int n) {
    if (b[0] & 0x80) {                      /* GNU base-256 */
        int64_t v = b[0] & 0x7f;
        for (int i = 1; i < n; i++) v = (v << 8) | b[i];
        return v;
    }
    int64_t v = 0;
    for (int i = 0; i < n; i++) {
        if (b[i] < '0' || b[i] > '7') continue;
        v = v * 8 + (b[i] - '0');
    }
    return v;
}


typedef struct { char *path; int64_t size, mtime, in_off; int32_t mode, uid, gid; } ZRec;
/* lframe: logical frame label for the footer; -1 = assign physical index at
 * append (zpack fused mode). zexec sets it from the plan so a frame keeps its
 * planned id regardless of which compressor finishes first. */
typedef struct ZJob { struct ZJob *next; uint8_t *buf; int64_t len, cap;
                      ZRec *recs; int nrecs; int64_t lframe; int sink; } ZJob;

/* ── fixed-size frame-buffer pool (docs/ISA.md §5) ──────────────────────────
 * Streaming recompress frames are member-aligned — the cut is only ever tested
 * after a whole member lands in the buffer, so a frame is a whole number of
 * members and no member ever crosses a frame boundary. That makes the buffer a
 * fixed quantum (cap = batch + slack) in the common case, so we recycle a pool
 * of them instead of malloc/free per frame (readers churn one buffer per ~16 MB
 * frame). A member larger than the buffer makes its frame oversized — grown in
 * place, then freed rather than recycled (cap != pool cap). bp_acquire also
 * caps resident buffers, so memory is bounded to max_live × cap. */
static struct {
    pthread_mutex_t mu; pthread_cond_t cv;
    uint8_t **freelist; int nfree; int64_t cap; int live, max_live;
} BP;
static void bp_init(int64_t cap, int max_live) {
    pthread_mutex_init(&BP.mu, NULL); pthread_cond_init(&BP.cv, NULL);
    BP.freelist = calloc((size_t)max_live, sizeof(uint8_t *));
    BP.nfree = 0; BP.cap = cap; BP.live = 0; BP.max_live = max_live;
}
static uint8_t *bp_acquire(int64_t *cap_out) {
    pthread_mutex_lock(&BP.mu);
    while (BP.live >= BP.max_live && BP.nfree == 0)
        pthread_cond_wait(&BP.cv, &BP.mu);
    uint8_t *b = BP.nfree > 0 ? BP.freelist[--BP.nfree]
                              : malloc((size_t)BP.cap);
    BP.live++;
    pthread_mutex_unlock(&BP.mu);
    *cap_out = BP.cap;
    return b;
}
static void bp_release(uint8_t *b, int64_t cap) {
    if (!b) return;
    pthread_mutex_lock(&BP.mu);
    BP.live--;
    if (cap == BP.cap && BP.nfree < BP.max_live) BP.freelist[BP.nfree++] = b;
    else free(b);                          /* oversized (grown) frame buffer */
    pthread_cond_signal(&BP.cv);
    pthread_mutex_unlock(&BP.mu);
}
static void bp_destroy(void) {
    for (int i = 0; i < BP.nfree; i++) free(BP.freelist[i]);
    free(BP.freelist); BP.freelist = NULL;
}

static struct {
    const char **srcs; int nsrc; _Atomic int src_i;
    ZJob *qh, *qt; int qn, readers_done, nreaders_left;
    pthread_mutex_t qmu; pthread_cond_t qcv;
    int level; int64_t batch;
    _Atomic int64_t frame_idx;
    int nsink; int *fd; int64_t *aoff;    /* per-sink output fd + append cursor */
    pthread_mutex_t *slock;               /* per-sink lock: independent streams,
                                           * so a slow (e.g. S3) sink can't stall
                                           * the others; also serializes each
                                           * sink's writes into offset order */
    const char *pattern;                  /* out path template with %d for sink */
    pthread_mutex_t omu;
} Z;

/* plan (zexec): the member→(frame,sink) keep-list is an OP_COMPRESS *command
 * stream* (Arrow IPC, CMD schema) the planner writes — read here with the same
 * parse_cmd_batch the exec loop uses, no bespoke format. The four plan operands
 * ride existing command columns: source_id→data_offset, ordinal→size,
 * frame→dep_group, sink→parent_row (level→pad_align). nsink and per-sink resume
 * `start` offsets are execution params, passed on argv. */
typedef struct { int32_t ordinal, frame, sink; } PlanEnt;
static struct { int nsrc, nsink; int64_t *start; int32_t *counts;
                PlanEnt **ents; } P;

static int plan_load(const char *path, int nsink, const char *starts) {
    /* The plan is a zstd-compressed OP_COMPRESS command stream — whole-stream
     * compression, so the sparse constant columns collapse together. Decompress
     * on the fly with the same streaming reader zpack uses; parse the IPC
     * messages out of it exactly as the exec loop does. */
    Zsrc z;
    if (zsrc_open(&z, path)) { perror("plan"); return -1; }
    uint8_t *meta = NULL, *body = NULL; size_t mcap = 0, bcap = 0;
    PlanEnt *ents = NULL; int32_t *srcs = NULL; int64_t ne = 0, cap = 0;
    int maxsrc = -1;
    for (;;) {
        uint32_t hdr[2];
        if (zsrc_read(&z, (uint8_t *)hdr, 8) < 8) break;   /* EOF */
        if (hdr[0] != 0xFFFFFFFFu) { zsrc_close(&z); return -1; }
        if (hdr[1] == 0) break;                            /* EOS */
        if (hdr[1] > mcap) meta = realloc(meta, mcap = hdr[1]);
        if (zsrc_read(&z, meta, hdr[1]) < hdr[1]) { zsrc_close(&z); return -1; }
        int64_t rt = fb_root(meta);
        int64_t blp = fb_field(meta, rt, 3);
        int64_t blen = blp >= 0 ? fb_i64(meta, blp) : 0;
        if (blen > 0) {
            if ((size_t)blen > bcap) body = realloc(body, bcap = (size_t)blen);
            if (zsrc_read(&z, body, (size_t)blen) < (size_t)blen) {
                zsrc_close(&z); return -1;
            }
        }
        int64_t htp = fb_field(meta, rt, 1);
        if (htp >= 0 && meta[htp] == 1) continue;          /* Schema msg */
        CmdBatch cb;
        if (parse_cmd_batch(meta, body, &cb)) { zsrc_close(&z); return -1; }
        int64_t n = cb.n_rows;
        if (ne + n > cap) {
            cap = (ne + n) * 2;
            ents = realloc(ents, sizeof(PlanEnt) * (size_t)cap);
            srcs = realloc(srcs, 4 * (size_t)cap);
        }
        for (int64_t i = 0; i < n; i++) {
            int32_t s = (int32_t)cb.data_offset[i];        /* source_id */
            srcs[ne] = s;
            ents[ne].ordinal = (int32_t)cb.size[i];        /* ordinal */
            ents[ne].frame   = (int32_t)cb.dep_group[i];   /* frame */
            ents[ne].sink    = (int32_t)cb.parent_row[i];  /* sink */
            if (s > maxsrc) maxsrc = s;
            ne++;
        }
        free(cb.arena); free(cb.path); free(cb.dst);
    }
    free(meta); free(body); zsrc_close(&z);
    /* rows arrive sorted by (source_id, ordinal), so each source is contiguous */
    P.nsrc = maxsrc + 1; P.nsink = nsink;
    int ns = P.nsrc > 0 ? P.nsrc : 1;
    P.counts = calloc((size_t)ns, sizeof(int32_t));
    for (int64_t i = 0; i < ne; i++) P.counts[srcs[i]]++;
    P.ents = malloc(sizeof(PlanEnt *) * (size_t)ns);
    int64_t off = 0;
    for (int s = 0; s < P.nsrc; s++) { P.ents[s] = ents + off; off += P.counts[s]; }
    free(srcs);
    P.start = calloc((size_t)(nsink > 0 ? nsink : 1), sizeof(int64_t));
    if (starts && strcmp(starts, "-")) {                   /* resume offsets */
        const char *q = starts; int si = 0;
        while (*q && si < nsink) {
            P.start[si++] = strtoll(q, (char **)&q, 10);
            if (*q == ',') q++;
        }
    }
    return 0;
}

static void z_emit(ZRec *r, int32_t frame, int64_t coff, int64_t clen,
                   int32_t sink) {
    uint16_t plen = (uint16_t)strlen(r->path);
    /* one record: [u16 plen][path][i64 size][i64 mtime][i32 mode][i32 uid]
     *   [i32 gid][i32 frame][i64 coff][i64 clen][i64 in_off][i32 sink] */
    fwrite(&plen, 2, 1, stdout); fwrite(r->path, plen, 1, stdout);
    fwrite(&r->size, 8, 1, stdout); fwrite(&r->mtime, 8, 1, stdout);
    fwrite(&r->mode, 4, 1, stdout); fwrite(&r->uid, 4, 1, stdout);
    fwrite(&r->gid, 4, 1, stdout); fwrite(&frame, 4, 1, stdout);
    fwrite(&coff, 8, 1, stdout); fwrite(&clen, 8, 1, stdout);
    fwrite(&r->in_off, 8, 1, stdout); fwrite(&sink, 4, 1, stdout);
}

static void z_push(ZJob *j) {
    pthread_mutex_lock(&Z.qmu);
    while (Z.qn >= 96) pthread_cond_wait(&Z.qcv, &Z.qmu);   /* backpressure */
    j->next = NULL;
    if (Z.qt) Z.qt->next = j; else Z.qh = j;
    Z.qt = j; Z.qn++;
    pthread_cond_broadcast(&Z.qcv);
    pthread_mutex_unlock(&Z.qmu);
}
static ZJob *z_pop(void) {
    pthread_mutex_lock(&Z.qmu);
    while (!Z.qh && !Z.readers_done)
        pthread_cond_wait(&Z.qcv, &Z.qmu);
    ZJob *j = Z.qh;
    if (j) { Z.qh = j->next; if (!Z.qh) Z.qt = NULL; Z.qn--;
             pthread_cond_broadcast(&Z.qcv); }
    pthread_mutex_unlock(&Z.qmu);
    return j;
}

static void z_parse_pax(const uint8_t *b, int64_t n, char *path, int *hp,
                        int64_t *psize) {
    int64_t i = 0;
    while (i < n) {
        int64_t s = i;
        while (i < n && b[i] != ' ') i++;      /* "LEN key=value\n" */
        i++;                                    /* skip space */
        const uint8_t *kv = b + i;
        int64_t end = s + strtol((const char *)b + s, NULL, 10);
        int64_t klen = 0;
        while (kv[klen] != '=' && (b + i + klen) < b + end) klen++;
        if (klen == 4 && !memcmp(kv, "path", 4)) {
            int64_t vlen = end - (i + klen + 1) - 1;   /* minus trailing \n */
            if (vlen > 4094) vlen = 4094;
            memcpy(path, kv + klen + 1, vlen); path[vlen] = 0; *hp = 1;
        } else if (klen == 4 && !memcmp(kv, "size", 4)) {
            *psize = strtoll((const char *)kv + klen + 1, NULL, 10);
        }
        i = end;
    }
}

static void *z_reader(void *arg) {
    (void)arg;
    for (;;) {
        int si = atomic_fetch_add(&Z.src_i, 1);
        if (si >= Z.nsrc) break;
        Zsrc z;
        if (zsrc_open(&z, Z.srcs[si])) continue;
        int64_t bcap; uint8_t *buf = bp_acquire(&bcap); int64_t blen = 0;
        int rcap = 8192, nrec = 0;
        ZRec *recs = malloc(sizeof(ZRec) * rcap);
        char pax_path[4096], gnu[4096];
        int has_pax = 0, has_gnu = 0; int64_t pax_size = -1;
        uint8_t hdr[512];
        for (;;) {
            if (zsrc_read(&z, hdr, 512) < 512) break;
            int allz = 1;
            for (int k = 0; k < 512; k++) if (hdr[k]) { allz = 0; break; }
            if (allz) break;
            int typ = hdr[156];
            int64_t size = zoctal(hdr + 124, 12);
            int64_t bl = (size + 511) / 512 * 512;
            if (blen + 512 + bl + 1024 > (int64_t)bcap) {
                bcap = (blen + 512 + bl + 1024) * 2; buf = realloc(buf, bcap);
            }
            if (typ == 'x' || typ == 'g') {
                memcpy(buf + blen, hdr, 512);
                zsrc_read(&z, buf + blen + 512, bl);
                z_parse_pax(buf + blen + 512, size, pax_path, &has_pax, &pax_size);
                blen += 512 + bl; continue;
            }
            if (typ == 'L') {
                memcpy(buf + blen, hdr, 512);
                zsrc_read(&z, buf + blen + 512, bl);
                int nl = size < 4095 ? (int)size : 4095;
                memcpy(gnu, buf + blen + 512, nl);
                while (nl && gnu[nl - 1] == 0) nl--;
                gnu[nl] = 0; has_gnu = 1;
                blen += 512 + bl; continue;
            }
            char name[4096];
            if (has_pax) strcpy(name, pax_path);
            else if (has_gnu) strcpy(name, gnu);
            else {
                char nm[101], pre[156];
                memcpy(nm, hdr, 100); nm[100] = 0;
                memcpy(pre, hdr + 345, 155); pre[155] = 0;
                if (pre[0]) snprintf(name, sizeof name, "%s/%s", pre, nm);
                else { strncpy(name, nm, sizeof name - 1); name[sizeof name-1]=0; }
            }
            int64_t rsize = pax_size >= 0 ? pax_size : size;
            int64_t rbl = (rsize + 511) / 512 * 512;
            if (blen + 512 + rbl + 1024 > bcap) {      /* oversized: grow frame */
                bcap = (blen + 512 + rbl + 1024) * 2; buf = realloc(buf, (size_t)bcap);
            }
            int64_t body_off = blen + 512;
            memcpy(buf + blen, hdr, 512);
            zsrc_read(&z, buf + blen + 512, rbl);
            blen += 512 + rbl;
            if (typ == '0' || typ == 0) {
                if (nrec >= rcap) { rcap *= 2; recs = realloc(recs, sizeof(ZRec)*rcap); }
                recs[nrec].path = strdup(name);
                recs[nrec].size = rsize;
                recs[nrec].mode = zoctal(hdr + 100, 8);
                recs[nrec].mtime = zoctal(hdr + 136, 12) * 1000000000LL;
                recs[nrec].uid = zoctal(hdr + 108, 8);
                recs[nrec].gid = zoctal(hdr + 116, 8);
                recs[nrec].in_off = body_off;
                nrec++;
            }
            has_pax = 0; has_gnu = 0; pax_size = -1;
            if (blen >= Z.batch) {
                ZJob *j = malloc(sizeof *j);
                j->buf = buf; j->cap = bcap; j->len = blen;
                j->recs = recs; j->nrecs = nrec; j->lframe = -1; j->sink = 0;
                z_push(j);
                buf = bp_acquire(&bcap); blen = 0;
                rcap = 8192; nrec = 0; recs = malloc(sizeof(ZRec) * rcap);
            }
        }
        if (blen) {                                    /* source's final frame */
            ZJob *j = malloc(sizeof *j);
            j->buf = buf; j->cap = bcap; j->len = blen;
            j->recs = recs; j->nrecs = nrec; j->lframe = -1; j->sink = 0;
            z_push(j);
        } else { bp_release(buf, bcap); free(recs); }
        zsrc_close(&z);
    }
    /* last reader out flips readers_done */
    pthread_mutex_lock(&Z.qmu);
    if (--Z.nreaders_left == 0) { Z.readers_done = 1;
        pthread_cond_broadcast(&Z.qcv); }
    pthread_mutex_unlock(&Z.qmu);
    return NULL;
}

static void *z_compressor(void *arg) {
    (void)arg;
    ZSTD_CCtx *cc = ZSTD_createCCtx();
    for (;;) {
        ZJob *j = z_pop();
        if (!j) break;
        size_t bound = ZSTD_compressBound((size_t)j->len);
        uint8_t *comp = malloc(bound);
        size_t clen = ZSTD_compressCCtx(cc, comp, bound, j->buf,
                                        (size_t)j->len, Z.level);
        int sink = j->sink;
        int64_t phys = atomic_fetch_add(&Z.frame_idx, 1);
        int64_t fidx = (j->lframe >= 0) ? j->lframe : phys;
        /* per-sink critical section: open (once), take the next offset, and
         * write the frame — sequential, so the destination may be a pipe to a
         * streaming uploader as readily as a seekable file. */
        pthread_mutex_lock(&Z.slock[sink]);
        if (Z.fd[sink] < 0) {                  /* lazy-open this sink's output */
            char path[4096];
            snprintf(path, sizeof path, Z.pattern, sink);
            Z.fd[sink] = open(path, O_WRONLY | O_CREAT, 0644);
            if (Z.fd[sink] < 0) perror("sink");
            /* keep [0, aoff) (committed prefix on resume; empty on fresh run),
             * drop any torn tail; no-ops on a FIFO. aoff is still the start
             * offset here — nothing has been appended yet. */
            else if (ftruncate(Z.fd[sink], Z.aoff[sink]) == 0)
                lseek(Z.fd[sink], Z.aoff[sink], SEEK_SET);
        }
        int64_t coff = Z.aoff[sink];
        Z.aoff[sink] += (int64_t)clen;
        for (size_t off = 0; off < clen; ) {   /* full write (pipes short-write) */
            ssize_t w = write(Z.fd[sink], comp + off, clen - off);
            if (w < 0) { perror("write"); break; }
            off += (size_t)w;
        }
        pthread_mutex_unlock(&Z.slock[sink]);
        pthread_mutex_lock(&Z.omu);
        for (int r = 0; r < j->nrecs; r++)
            z_emit(&j->recs[r], (int32_t)fidx, coff, (int64_t)clen, sink);
        pthread_mutex_unlock(&Z.omu);
        for (int r = 0; r < j->nrecs; r++) free(j->recs[r].path);
        free(comp); bp_release(j->buf, j->cap); free(j->recs); free(j);
    }
    ZSTD_freeCCtx(cc);
    return NULL;
}

static int run_zpack(const char **srcs, int nsrc, const char *out,
                     int level, int64_t batch, int readers, int compressors) {
    memset(&Z, 0, sizeof Z);
    Z.srcs = srcs; Z.nsrc = nsrc; Z.level = level; Z.batch = batch;
    Z.nreaders_left = readers;
    pthread_mutex_init(&Z.qmu, NULL); pthread_cond_init(&Z.qcv, NULL);
    pthread_mutex_init(&Z.omu, NULL);
    Z.nsink = 1;                               /* single output, preopened */
    Z.fd = malloc(sizeof(int)); Z.aoff = calloc(1, sizeof(int64_t));
    Z.slock = malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(&Z.slock[0], NULL);
    Z.fd[0] = open(out, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (Z.fd[0] < 0) { perror("output"); return 2; }
    pthread_t rt[64], ct[128];
    if (readers > 64) readers = 64;
    if (compressors > 128) compressors = 128;
    bp_init(batch + (4 << 20), readers + compressors + 96 + 32);
    for (int i = 0; i < compressors; i++)
        pthread_create(&ct[i], NULL, z_compressor, NULL);
    for (int i = 0; i < readers; i++)
        pthread_create(&rt[i], NULL, z_reader, NULL);
    for (int i = 0; i < readers; i++) pthread_join(rt[i], NULL);
    for (int i = 0; i < compressors; i++) pthread_join(ct[i], NULL);
    bp_destroy();
    fflush(stdout);
    close(Z.fd[0]);
    fprintf(stderr, "zpack: %ld frames, %ld bytes\n",
            (long)Z.frame_idx, (long)Z.aoff[0]);
    return 0;
}

/* ── zscan / zexec: the fold with a plan seam ──────────────────────────────
 * zscan decompresses+parses only, emitting member metadata (no compression)
 * so the Polars planner can apply globs/filters and assign members to frames.
 * zexec re-streams the sources and, gated by that plan, compresses only the
 * kept members into their assigned frames. Decompress is ~3% of level-10
 * compress, so the extra scan pass is nearly free — the price of a real
 * plan step between decompression and compression. */

static void zsrc_skip(Zsrc *z, int64_t n) {
    static __thread uint8_t tmp[65536];
    while (n > 0) {
        size_t k = n > (int64_t)sizeof tmp ? sizeof tmp : (size_t)n;
        size_t got = zsrc_read(z, tmp, k);
        if (!got) break;
        n -= got;
    }
}

/* zscan emits member metadata as ZMETA Arrow-IPC batches — the same
 * StreamReader the planner uses for scan, not a bespoke record. */
typedef struct {
    int64_t n, cap;
    char *pdata; int64_t pdata_len, pdata_cap; int64_t *poff;
    int32_t *source_id, *ordinal, *mode, *uid, *gid;
    int64_t *size, *mtime, *span;
    int grow;                    /* 1: grow at cap (zstream); 0: flush (zscan) */
} MetaBuilder;

static void mb_grow(MetaBuilder *b) {
    b->cap *= 2;
    b->poff = realloc(b->poff, 8 * (size_t)(b->cap + 1));
    b->source_id = realloc(b->source_id, 4 * (size_t)b->cap);
    b->ordinal = realloc(b->ordinal, 4 * (size_t)b->cap);
    b->mode = realloc(b->mode, 4 * (size_t)b->cap);
    b->uid = realloc(b->uid, 4 * (size_t)b->cap);
    b->gid = realloc(b->gid, 4 * (size_t)b->cap);
    b->size = realloc(b->size, 8 * (size_t)b->cap);
    b->mtime = realloc(b->mtime, 8 * (size_t)b->cap);
    b->span = realloc(b->span, 8 * (size_t)b->cap);
}

static void mb_init(MetaBuilder *b) {
    memset(b, 0, sizeof *b);
    b->cap = SCAN_BATCH;
    b->pdata_cap = 1 << 20; b->pdata = malloc((size_t)b->pdata_cap);
    b->poff = malloc(8 * (size_t)(b->cap + 1)); b->poff[0] = 0;
    b->source_id = malloc(4 * (size_t)b->cap);
    b->ordinal = malloc(4 * (size_t)b->cap);
    b->mode = malloc(4 * (size_t)b->cap);
    b->uid = malloc(4 * (size_t)b->cap);
    b->gid = malloc(4 * (size_t)b->cap);
    b->size = malloc(8 * (size_t)b->cap);
    b->mtime = malloc(8 * (size_t)b->cap);
    b->span = malloc(8 * (size_t)b->cap);
}
static void mb_free(MetaBuilder *b) {
    free(b->pdata); free(b->poff); free(b->source_id); free(b->ordinal);
    free(b->mode); free(b->uid); free(b->gid); free(b->size); free(b->mtime);
    free(b->span);
}
static int mb_flush(MetaBuilder *b) {          /* col order matches ZMETA */
    if (b->n == 0) return 0;
    struct WBuf bufs[ZMETA_N_BUFS] = {
        {NULL,0},{b->poff, 8*(b->n+1)},{b->pdata, b->pdata_len},
        {NULL,0},{b->source_id, 4*b->n}, {NULL,0},{b->ordinal, 4*b->n},
        {NULL,0},{b->size, 8*b->n}, {NULL,0},{b->mode, 4*b->n},
        {NULL,0},{b->mtime, 8*b->n}, {NULL,0},{b->uid, 4*b->n},
        {NULL,0},{b->gid, 4*b->n}, {NULL,0},{b->span, 8*b->n},
    };
    pthread_mutex_lock(&Z.omu);
    int rc = emit_batch(1, ZMETA_BATCH_TMPL, ZMETA_TMPL_LEN,
                        ZMETA_OFF_BODYLEN, ZMETA_OFF_RBLEN,
                        ZMETA_NODE_OFF, ZMETA_N_NODES,
                        ZMETA_BUF_OFF, ZMETA_N_BUFS, b->n, bufs);
    pthread_mutex_unlock(&Z.omu);
    b->n = 0; b->pdata_len = 0; b->poff[0] = 0;
    return rc;
}
static void mb_row(MetaBuilder *b, const char *name, int32_t src, int32_t ord,
                   int64_t size, int32_t mode, int64_t mtime,
                   int32_t uid, int32_t gid, int64_t span) {
    int64_t nl = (int64_t)strlen(name);
    while (b->pdata_len + nl > b->pdata_cap)
        b->pdata = realloc(b->pdata, (size_t)(b->pdata_cap *= 2));
    memcpy(b->pdata + b->pdata_len, name, (size_t)nl); b->pdata_len += nl;
    int64_t i = b->n++;
    b->poff[i + 1] = b->pdata_len;
    b->source_id[i] = src; b->ordinal[i] = ord; b->size[i] = size;
    b->mode[i] = mode; b->mtime[i] = mtime; b->uid[i] = uid; b->gid[i] = gid;
    b->span[i] = span;
    if (b->n >= b->cap) { if (b->grow) mb_grow(b); else mb_flush(b); }
}

static void *z_scan_reader(void *arg) {
    (void)arg;
    for (;;) {
        int si = atomic_fetch_add(&Z.src_i, 1);
        if (si >= Z.nsrc) break;
        Zsrc z;
        if (zsrc_open(&z, Z.srcs[si])) continue;
        MetaBuilder mb; mb_init(&mb);
        uint8_t hdr[512];
        size_t scap = 1 << 16; uint8_t *scr = malloc(scap);
        char pax_path[4096], gnu[4096];
        int has_pax = 0, has_gnu = 0; int64_t pax_size = -1;
        int32_t ordinal = 0;
        for (;;) {
            if (zsrc_read(&z, hdr, 512) < 512) break;
            int allz = 1;
            for (int k = 0; k < 512; k++) if (hdr[k]) { allz = 0; break; }
            if (allz) break;
            int typ = hdr[156];
            int64_t size = zoctal(hdr + 124, 12);
            int64_t bl = (size + 511) / 512 * 512;
            if ((size_t)bl > scap) { scap = bl; scr = realloc(scr, scap); }
            if (typ == 'x' || typ == 'g') {
                zsrc_read(&z, scr, bl);
                z_parse_pax(scr, size, pax_path, &has_pax, &pax_size);
                continue;
            }
            if (typ == 'L') {
                zsrc_read(&z, scr, bl);
                int nl = size < 4095 ? (int)size : 4095;
                memcpy(gnu, scr, nl);
                while (nl && gnu[nl - 1] == 0) nl--;
                gnu[nl] = 0; has_gnu = 1;
                continue;
            }
            char name[4096];
            if (has_pax) strcpy(name, pax_path);
            else if (has_gnu) strcpy(name, gnu);
            else {
                char nm[101], pre[156];
                memcpy(nm, hdr, 100); nm[100] = 0;
                memcpy(pre, hdr + 345, 155); pre[155] = 0;
                if (pre[0]) snprintf(name, sizeof name, "%s/%s", pre, nm);
                else { strncpy(name, nm, sizeof name - 1); name[sizeof name-1]=0; }
            }
            int64_t rsize = pax_size >= 0 ? pax_size : size;
            zsrc_skip(&z, (rsize + 511) / 512 * 512);
            if (typ == '0' || typ == 0) {
                mb_row(&mb, name, si, ordinal, rsize, zoctal(hdr + 100, 8),
                       zoctal(hdr + 136, 12) * 1000000000LL,
                       zoctal(hdr + 108, 8), zoctal(hdr + 116, 8),
                       512 + (rsize + 511) / 512 * 512);   /* tar footprint */
                ordinal++;
            }
            has_pax = 0; has_gnu = 0; pax_size = -1;
        }
        mb_flush(&mb); mb_free(&mb);
        free(scr); zsrc_close(&z);
    }
    return NULL;
}

static int run_zscan(const char **srcs, int nsrc, int readers) {
    memset(&Z, 0, sizeof Z);
    Z.srcs = srcs; Z.nsrc = nsrc;
    pthread_mutex_init(&Z.omu, NULL);
    if (readers > 64) readers = 64;
    if (readers < 1) readers = 1;
    emit_schema(1, ZMETA_SCHEMA_META, ZMETA_SCHEMA_LEN);   /* Arrow IPC framing */
    pthread_t rt[64];
    for (int i = 0; i < readers; i++)
        pthread_create(&rt[i], NULL, z_scan_reader, NULL);
    for (int i = 0; i < readers; i++) pthread_join(rt[i], NULL);
    emit_eos(1);
    fflush(stdout);
    return 0;
}

static void *z_exec_reader(void *arg) {
    (void)arg;
    for (;;) {
        int si = atomic_fetch_add(&Z.src_i, 1);
        if (si >= Z.nsrc) break;
        Zsrc z;
        if (zsrc_open(&z, Z.srcs[si])) continue;
        PlanEnt *pp = (si < P.nsrc) ? P.ents[si] : NULL;
        PlanEnt *pe = pp ? pp + P.counts[si] : NULL;
        /* one open frame buffer per sink — a source's members interleave
         * sinks in stream order, so each sink accumulates independently and
         * flushes at its own planned-frame boundary. Lazily allocated so a
         * source that touches few sinks costs little. */
        int NS = Z.nsink;
        uint8_t **buf = calloc(NS, sizeof *buf);
        int64_t *blen = calloc(NS, sizeof *blen);
        int64_t *bcap = calloc(NS, sizeof *bcap);
        int64_t *curf = malloc(NS * sizeof *curf);
        ZRec   **recs = calloc(NS, sizeof *recs);
        int     *nrec = calloc(NS, sizeof *nrec);
        int     *rcap = calloc(NS, sizeof *rcap);
        for (int k = 0; k < NS; k++) curf[k] = -1;
        size_t mcap = 1 << 20; uint8_t *mbuf = malloc(mcap); int64_t mlen = 0;
        char pax_path[4096], gnu[4096];
        int has_pax = 0, has_gnu = 0; int64_t pax_size = -1;
        int32_t ordinal = 0;
        uint8_t hdr[512];
        for (;;) {
            if (zsrc_read(&z, hdr, 512) < 512) break;
            int allz = 1;
            for (int k = 0; k < 512; k++) if (hdr[k]) { allz = 0; break; }
            if (allz) break;
            int typ = hdr[156];
            int64_t size = zoctal(hdr + 124, 12);
            int64_t bl = (size + 511) / 512 * 512;
            if (mlen + 512 + bl > (int64_t)mcap) {
                mcap = (mlen + 512 + bl) * 2; mbuf = realloc(mbuf, mcap);
            }
            if (typ == 'x' || typ == 'g') {           /* stage ext header */
                memcpy(mbuf + mlen, hdr, 512);
                zsrc_read(&z, mbuf + mlen + 512, bl);
                z_parse_pax(mbuf + mlen + 512, size, pax_path, &has_pax, &pax_size);
                mlen += 512 + bl; continue;
            }
            if (typ == 'L') {
                memcpy(mbuf + mlen, hdr, 512);
                zsrc_read(&z, mbuf + mlen + 512, bl);
                int nl = size < 4095 ? (int)size : 4095;
                memcpy(gnu, mbuf + mlen + 512, nl);
                while (nl && gnu[nl - 1] == 0) nl--;
                gnu[nl] = 0; has_gnu = 1;
                mlen += 512 + bl; continue;
            }
            char name[4096];
            if (has_pax) strcpy(name, pax_path);
            else if (has_gnu) strcpy(name, gnu);
            else {
                char nm[101], pre[156];
                memcpy(nm, hdr, 100); nm[100] = 0;
                memcpy(pre, hdr + 345, 155); pre[155] = 0;
                if (pre[0]) snprintf(name, sizeof name, "%s/%s", pre, nm);
                else { strncpy(name, nm, sizeof name - 1); name[sizeof name-1]=0; }
            }
            int64_t rsize = pax_size >= 0 ? pax_size : size;
            int64_t rbl = (rsize + 511) / 512 * 512;
            if (mlen + 512 + rbl > (int64_t)mcap) {
                mcap = (mlen + 512 + rbl) * 2; mbuf = realloc(mbuf, mcap);
            }
            int64_t hoff = mlen;                       /* file header pos in mbuf */
            memcpy(mbuf + mlen, hdr, 512);
            zsrc_read(&z, mbuf + mlen + 512, rbl);
            mlen += 512 + rbl;
            int isfile = (typ == '0' || typ == 0);
            int keep = 0; int64_t lframe = -1; int sk = 0;
            if (isfile) {
                if (pp && pp < pe && pp->ordinal == ordinal) {
                    keep = 1; lframe = pp->frame; sk = pp->sink; pp++;
                }
                ordinal++;
            }
            if (keep) {
                if (!buf[sk]) {                    /* lazy-acquire sink frame buf */
                    buf[sk] = bp_acquire(&bcap[sk]);
                    rcap[sk] = 8192; recs[sk] = malloc(sizeof(ZRec) * rcap[sk]);
                }
                if (blen[sk] > 0 && lframe != curf[sk]) {    /* frame boundary */
                    ZJob *j = malloc(sizeof *j);
                    j->buf = buf[sk]; j->cap = bcap[sk]; j->len = blen[sk];
                    j->recs = recs[sk]; j->nrecs = nrec[sk];
                    j->lframe = curf[sk]; j->sink = sk;
                    z_push(j);
                    buf[sk] = bp_acquire(&bcap[sk]); blen[sk] = 0;
                    rcap[sk] = 8192; nrec[sk] = 0;
                    recs[sk] = malloc(sizeof(ZRec) * rcap[sk]);
                }
                curf[sk] = lframe;
                if (blen[sk] + mlen > bcap[sk]) {         /* oversized: grow */
                    bcap[sk] = (blen[sk] + mlen) * 2;
                    buf[sk] = realloc(buf[sk], (size_t)bcap[sk]);
                }
                int64_t body_off = blen[sk] + hoff + 512;
                memcpy(buf[sk] + blen[sk], mbuf, mlen); blen[sk] += mlen;
                if (nrec[sk] >= rcap[sk]) {
                    rcap[sk] *= 2;
                    recs[sk] = realloc(recs[sk], sizeof(ZRec) * rcap[sk]);
                }
                ZRec *rr = &recs[sk][nrec[sk]];
                rr->path = strdup(name); rr->size = rsize;
                rr->mode = zoctal(hdr + 100, 8);
                rr->mtime = zoctal(hdr + 136, 12) * 1000000000LL;
                rr->uid = zoctal(hdr + 108, 8); rr->gid = zoctal(hdr + 116, 8);
                rr->in_off = body_off; nrec[sk]++;
            }
            mlen = 0; has_pax = 0; has_gnu = 0; pax_size = -1;
        }
        for (int k = 0; k < NS; k++) {             /* flush each sink's tail */
            if (buf[k] && blen[k] > 0) {
                ZJob *j = malloc(sizeof *j);
                j->buf = buf[k]; j->cap = bcap[k]; j->len = blen[k];
                j->recs = recs[k]; j->nrecs = nrec[k];
                j->lframe = curf[k]; j->sink = k;
                z_push(j);
            } else { bp_release(buf[k], bcap[k]); free(recs[k]); }
        }
        free(buf); free(blen); free(bcap); free(curf);
        free(recs); free(nrec); free(rcap);
        free(mbuf); zsrc_close(&z);
    }
    pthread_mutex_lock(&Z.qmu);
    if (--Z.nreaders_left == 0) { Z.readers_done = 1;
        pthread_cond_broadcast(&Z.qcv); }
    pthread_mutex_unlock(&Z.qmu);
    return NULL;
}

static int run_zexec(const char *plan_path, const char **srcs, int nsrc,
                     const char *pattern, int level, int readers,
                     int compressors, int nsink, const char *starts) {
    memset(&Z, 0, sizeof Z);
    Z.srcs = srcs; Z.nsrc = nsrc; Z.level = level; Z.batch = 16 << 20;
    Z.nreaders_left = readers;
    if (plan_load(plan_path, nsink, starts)) return 2;
    pthread_mutex_init(&Z.qmu, NULL); pthread_cond_init(&Z.qcv, NULL);
    pthread_mutex_init(&Z.omu, NULL);
    Z.nsink = P.nsink < 1 ? 1 : P.nsink;
    Z.pattern = pattern;                       /* %d → sink; each opened lazily */
    Z.fd = malloc(sizeof(int) * Z.nsink);
    Z.aoff = calloc(Z.nsink, sizeof(int64_t));
    Z.slock = malloc(sizeof(pthread_mutex_t) * Z.nsink);
    for (int i = 0; i < Z.nsink; i++) {
        Z.fd[i] = -1; pthread_mutex_init(&Z.slock[i], NULL);
        Z.aoff[i] = P.start ? P.start[i] : 0;  /* resume high-water, else 0 */
    }
    pthread_t rt[64], ct[128];
    if (readers > 64) readers = 64;
    if (compressors > 128) compressors = 128;
    /* each reader may hold one filling buffer per sink at once */
    bp_init(Z.batch + (4 << 20), readers * Z.nsink + compressors + 96 + 32);
    for (int i = 0; i < compressors; i++)
        pthread_create(&ct[i], NULL, z_compressor, NULL);
    for (int i = 0; i < readers; i++)
        pthread_create(&rt[i], NULL, z_exec_reader, NULL);
    for (int i = 0; i < readers; i++) pthread_join(rt[i], NULL);
    for (int i = 0; i < compressors; i++) pthread_join(ct[i], NULL);
    bp_destroy();
    fflush(stdout);
    int64_t total = 0;
    for (int i = 0; i < Z.nsink; i++) {
        if (Z.fd[i] >= 0) { total += Z.aoff[i]; close(Z.fd[i]); }
    }
    fprintf(stderr, "zexec: %ld frames, %d sinks, %ld bytes\n",
            (long)Z.frame_idx, Z.nsink, (long)total);
    return 0;
}

/* ── zstream: one-pass planned recompress (docs/ISA.md §5) ──────────────────
 * The last fork closed: decompress the source ONCE into a live buffer, emit
 * member metadata (ZMETA on stdout), read back a PLAN (member→frame, from the
 * Polars planner over stdin), compress the planned frame slices from the SAME
 * live buffer, and report (frame→coff,clen) as COMP on a second fd — the
 * 60-byte record retired, both directions on the standard schemas. This first
 * cut is single-reader + synchronous per window (emit ZMETA → read PLAN →
 * compress); the union/async pipeline (keep the sinks fed) is the follow-up.
 * Frames are contiguous member runs, so a frame is a byte slice of the buffer
 * (no gather; filter/reshard gather is the next increment). */
static int read_cmd_stream(CmdBatch *cb, uint8_t **meta, size_t *mcap,
                           uint8_t **body, size_t *bcap) {
    for (;;) {
        uint32_t hdr[2];
        int r = read_full(0, hdr, 8);
        if (r == 1 || (r == 0 && hdr[1] == 0)) return 1;      /* EOS/EOF */
        if (r < 0 || hdr[0] != 0xFFFFFFFFu) return -1;
        if (hdr[1] > *mcap) *meta = realloc(*meta, *mcap = hdr[1]);
        if (read_full(0, *meta, hdr[1])) return -1;
        int64_t rt = fb_root(*meta);
        int64_t blp = fb_field(*meta, rt, 3);
        int64_t blen = blp >= 0 ? fb_i64(*meta, blp) : 0;
        if (blen > 0) {
            if ((size_t)blen > *bcap) *body = realloc(*body, *bcap = (size_t)blen);
            if (read_full(0, *body, (size_t)blen)) return -1;
        }
        int64_t htp = fb_field(*meta, rt, 1);
        if (htp >= 0 && (*meta)[htp] == 1) continue;          /* schema msg */
        if (parse_cmd_batch(*meta, *body, cb)) return -1;
        return 0;
    }
}

/* One frame's COMP row emitted per compressed slice: user_data = the frame id
 * the planner assigned (dep_group), read_size = coff, cksum = clen. */
static int emit_comp_batch(int fd, int64_t n, uint64_t *ud, int64_t *coff,
                           uint64_t *clen) {
    int32_t *res = calloc((size_t)n, 4);
    int32_t *parts = calloc((size_t)n, 4);
    int64_t *eo = calloc((size_t)(n + 1), 8);
    struct WBuf bufs[COMP_N_BUFS] = {
        {NULL,0},{ud, 8*n}, {NULL,0},{res, 4*n}, {NULL,0},{coff, 8*n},
        {NULL,0},{clen, 8*n}, {NULL,0},{eo, 8*(n+1)},{NULL,0},
        {NULL,0},{parts, 4*n},
    };
    int rc = emit_batch(fd, COMP_BATCH_TMPL, COMP_TMPL_LEN,
                        COMP_OFF_BODYLEN, COMP_OFF_RBLEN, COMP_NODE_OFF,
                        COMP_N_NODES, COMP_BUF_OFF, COMP_N_BUFS, n, bufs);
    free(res); free(parts); free(eo);
    return rc;
}

/* One file member in a window. `start` is the buffer offset where this
 * member's region begins — i.e. the end of the previous file member, so any
 * intervening dir/PAX/GNU blocks belong to THIS member's span. `span` (filled
 * at window cut) = next member's start (or buffer end) − start, and absorbs
 * those blocks, so a frame = the contiguous byte range [mem[a].start,
 * mem[r].start) with nothing dropped and no gather. */
typedef struct { int64_t start, span, size, mtime; int32_t mode, uid, gid;
                 char *name; } SMem;

/* ── lightweight span tracer (env QUIVER_TRACE=path → JSON) ─────────────────
 * Records (lane, kind, t0, t1, detail) spans so the pipeline can be visualized
 * offline. Off unless QUIVER_TRACE is set; a global mutex-guarded array flushed
 * at the end — fine for a short trace run, zero cost when off. */
enum { TR_DECODE, TR_EXCH_WAIT, TR_EXCH, TR_COMPRESS };
typedef struct { int lane, kind; double t0, t1; int64_t a, b; } TrSpan;
static struct { pthread_mutex_t mu; TrSpan *v; int n, cap, on; } TR =
    { .mu = PTHREAD_MUTEX_INITIALIZER };
static double tr_now(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e3 + ts.tv_nsec / 1e6;          /* ms */
}
static void tr_init(void) { TR.on = getenv("QUIVER_TRACE") != NULL; }
static void tr_span(int lane, int kind, double t0, double t1,
                    int64_t a, int64_t b) {
    if (!TR.on) return;
    pthread_mutex_lock(&TR.mu);
    if (TR.n >= TR.cap) { TR.cap = TR.cap ? TR.cap * 2 : 4096;
                          TR.v = realloc(TR.v, sizeof(TrSpan) * (size_t)TR.cap); }
    TR.v[TR.n++] = (TrSpan){lane, kind, t0, t1, a, b};
    pthread_mutex_unlock(&TR.mu);
}
static void tr_dump(void) {
    const char *p = getenv("QUIVER_TRACE");
    if (!p || !TR.on) return;
    FILE *f = fopen(p, "w");
    if (!f) return;
    double base = TR.n ? TR.v[0].t0 : 0;
    for (int i = 1; i < TR.n; i++) if (TR.v[i].t0 < base) base = TR.v[i].t0;
    fprintf(f, "[\n");
    for (int i = 0; i < TR.n; i++) {
        TrSpan *s = &TR.v[i];
        fprintf(f, "  {\"lane\":%d,\"kind\":%d,\"t0\":%.3f,\"t1\":%.3f,"
                   "\"a\":%lld,\"b\":%lld}%s\n",
                s->lane, s->kind, s->t0 - base, s->t1 - base,
                (long long)s->a, (long long)s->b, i + 1 < TR.n ? "," : "");
    }
    fprintf(f, "]\n");
    fclose(f);
    fprintf(stderr, "trace: %d spans -> %s\n", TR.n, p);
}

/* ── zstream compressor pool (docs/ISA.md §5: keep the sink fed) ────────────
 * A persistent pool of M workers compresses a window's frames in parallel and
 * appends each to the output. The window is a large planning unit (many 16 MB
 * frames), so within one ZMETA→PLAN round-trip the pool always has frames
 * queued — the sink never idles on compression, the bottleneck at production
 * levels. Offsets are assigned under a lock in job order; the compress itself
 * (the slow part) runs outside it, and pwrite is positioned/non-overlapping. */
typedef struct { const uint8_t *src; int64_t len; int level;
                 int64_t coff; size_t clen; int err; } ZFrameJob;
static struct {
    pthread_mutex_t mu; pthread_cond_t cv_work, cv_done;
    ZFrameJob *jobs; int njobs, next, done, stop, ofd;
    pthread_mutex_t amu; int64_t append;
    pthread_t tid[128]; int nthreads;
} ZP;

static void *zp_worker(void *arg) {
    int lane = 1000 + *(int *)arg;              /* compressor lanes: 1000+j */
    uint8_t *cb = NULL; size_t ccap = 0;
    for (;;) {
        pthread_mutex_lock(&ZP.mu);
        while (ZP.next >= ZP.njobs && !ZP.stop)
            pthread_cond_wait(&ZP.cv_work, &ZP.mu);
        if (ZP.next >= ZP.njobs && ZP.stop) { pthread_mutex_unlock(&ZP.mu); break; }
        int i = ZP.next++;
        pthread_mutex_unlock(&ZP.mu);

        ZFrameJob *j = &ZP.jobs[i];
        double t0 = tr_now();
        size_t bound = ZSTD_compressBound((size_t)j->len);
        if (bound > ccap) { cb = realloc(cb, bound); ccap = bound; }
        size_t cl = ZSTD_compress(cb, ccap, j->src, (size_t)j->len, j->level);
        if (ZSTD_isError(cl)) { j->err = 1; }
        else {
            pthread_mutex_lock(&ZP.amu);
            int64_t off = ZP.append; ZP.append += (int64_t)cl;
            pthread_mutex_unlock(&ZP.amu);
            j->coff = off; j->clen = cl;
            if (pwrite(ZP.ofd, cb, cl, off) != (ssize_t)cl) j->err = 1;
        }
        tr_span(lane, TR_COMPRESS, t0, tr_now(), j->len, (int64_t)cl);
        pthread_mutex_lock(&ZP.mu);
        ZP.done++;
        pthread_cond_signal(&ZP.cv_done);
        pthread_mutex_unlock(&ZP.mu);
    }
    free(cb);
    return NULL;
}

static int ZP_ids[128];
static void zp_start(int ofd, int compressors) {
    memset(&ZP, 0, sizeof ZP);
    pthread_mutex_init(&ZP.mu, NULL); pthread_mutex_init(&ZP.amu, NULL);
    pthread_cond_init(&ZP.cv_work, NULL); pthread_cond_init(&ZP.cv_done, NULL);
    ZP.ofd = ofd;
    ZP.nthreads = compressors > 128 ? 128 : (compressors < 1 ? 1 : compressors);
    for (int t = 0; t < ZP.nthreads; t++) {
        ZP_ids[t] = t;
        pthread_create(&ZP.tid[t], NULL, zp_worker, &ZP_ids[t]);
    }
}
/* Compress a window's `n` frame jobs in parallel; blocks until all done. */
static int zp_run_window(ZFrameJob *jobs, int n) {
    pthread_mutex_lock(&ZP.mu);
    ZP.jobs = jobs; ZP.njobs = n; ZP.next = 0; ZP.done = 0;
    pthread_cond_broadcast(&ZP.cv_work);
    while (ZP.done < n) pthread_cond_wait(&ZP.cv_done, &ZP.mu);
    ZP.njobs = 0;                      /* workers idle until the next window */
    pthread_mutex_unlock(&ZP.mu);
    for (int i = 0; i < n; i++) if (jobs[i].err) return -1;
    return 0;
}
static void zp_stop(void) {
    pthread_mutex_lock(&ZP.mu);
    ZP.stop = 1; pthread_cond_broadcast(&ZP.cv_work);
    pthread_mutex_unlock(&ZP.mu);
    for (int t = 0; t < ZP.nthreads; t++) pthread_join(ZP.tid[t], NULL);
}

/* Shared context for the zstream reader pool. Readers decode DIFFERENT sources
 * in parallel (decode is the single-stream bottleneck, ~700 MB/s — the EVI
 * win); the plan exchange (emit ZMETA → read PLAN) + parallel compress + emit
 * COMP is serialized by `exch` so stdin/stdout/comp_fd stay ordered and Python
 * stays synchronous. That serialized section runs the whole compressor pool, so
 * its throughput cap (~1/T_compress) sits above the single-node WEKA write
 * ceiling — parallel decode, not this lock, is what bounds a node. */
static struct {
    const char **srcs; int nsrc; _Atomic int src_i;
    int level, comp_fd; int64_t batch;
    pthread_mutex_t exch;
    _Atomic int64_t nframes;
    int rc;
} ZS;

static void *zstream_reader(void *arg) {
    int lane = *(int *)arg;                      /* reader lanes: 0..R-1 */
    int64_t bcap = ZS.batch + (4 << 20); uint8_t *buf = malloc((size_t)bcap);
    int mcap_m = SCAN_BATCH; SMem *mem = malloc(sizeof(SMem) * (size_t)mcap_m);
    uint8_t *pmeta = NULL, *pbody = NULL; size_t pmc = 0, pbc = 0;
    int jcap = 256; ZFrameJob *jobs = malloc(sizeof(ZFrameJob) * (size_t)jcap);
    int64_t batch = ZS.batch; int level = ZS.level, comp_fd = ZS.comp_fd;
    double win_t0 = tr_now();                    /* start of the current window */

    for (;;) {
        int si = atomic_fetch_add(&ZS.src_i, 1);
        if (si >= ZS.nsrc) break;
        Zsrc z;
        if (zsrc_open(&z, ZS.srcs[si])) continue;
        MetaBuilder mb; mb_init(&mb); mb.grow = 1;  /* one ZMETA batch/window */
        int64_t blen = 0, mstart = 0; int nm = 0; int32_t ordinal = 0;
        char pax_path[4096], gnu[4096], name[4096];
        int has_pax = 0, has_gnu = 0; int64_t pax_size = -1;
        uint8_t hdr[512]; int done = 0;
        while (!done) {
            int eof = zsrc_read(&z, hdr, 512) < 512;
            int allz = 1;
            if (!eof) for (int k = 0; k < 512; k++) if (hdr[k]) { allz = 0; break; }
            if (eof || allz) done = 1;
            else {
                int typ = hdr[156];
                int64_t sz = zoctal(hdr + 124, 12);
                int64_t bl = (sz + 511) / 512 * 512;
                if (blen + 512 + bl + 8192 > bcap) {
                    bcap = (blen + 512 + bl + 8192) * 2;
                    buf = realloc(buf, (size_t)bcap);
                }
                if (typ == 'x' || typ == 'g') {          /* PAX: keep in buffer */
                    memcpy(buf + blen, hdr, 512); zsrc_read(&z, buf + blen + 512, bl);
                    z_parse_pax(buf + blen + 512, sz, pax_path, &has_pax, &pax_size);
                    blen += 512 + bl; continue;
                }
                if (typ == 'L') {                        /* GNU long name */
                    memcpy(buf + blen, hdr, 512); zsrc_read(&z, buf + blen + 512, bl);
                    int nl = sz < 4095 ? (int)sz : 4095;
                    memcpy(gnu, buf + blen + 512, nl);
                    while (nl && gnu[nl-1] == 0) nl--;
                    gnu[nl] = 0; has_gnu = 1;
                    blen += 512 + bl; continue;
                }
                if (has_pax) strcpy(name, pax_path);
                else if (has_gnu) strcpy(name, gnu);
                else {
                    char nmb[101], pre[156];
                    memcpy(nmb, hdr, 100); nmb[100] = 0;
                    memcpy(pre, hdr + 345, 155); pre[155] = 0;
                    if (pre[0]) snprintf(name, sizeof name, "%s/%s", pre, nmb);
                    else { strncpy(name, nmb, sizeof name-1); name[sizeof name-1]=0; }
                }
                int64_t rsize = pax_size >= 0 ? pax_size : sz;
                int64_t rbl = (rsize + 511) / 512 * 512;
                memcpy(buf + blen, hdr, 512); zsrc_read(&z, buf + blen + 512, rbl);
                blen += 512 + rbl;
                /* Only files become members; a dir (typ '5') etc. stays in the
                 * buffer and is absorbed into the NEXT file member's span. */
                if (typ == '0' || typ == 0) {
                    if (nm >= mcap_m) mem = realloc(mem, sizeof(SMem)*(size_t)(mcap_m*=2));
                    mem[nm].start = mstart; mem[nm].size = rsize;
                    mem[nm].mtime = zoctal(hdr+136,12)*1000000000LL;
                    mem[nm].mode = (int32_t)zoctal(hdr+100,8);
                    mem[nm].uid = (int32_t)zoctal(hdr+108,8);
                    mem[nm].gid = (int32_t)zoctal(hdr+116,8);
                    mem[nm].name = strdup(name);
                    nm++; ordinal++;
                    mstart = blen;                   /* next member starts here */
                }
                has_pax = has_gnu = 0; pax_size = -1;
            }
            /* window full (or source done): finalize spans, plan, compress.
             * mb.grow keeps the whole window in one ZMETA batch (↔ one PLAN ↔
             * one COMP batch), so the window is bounded only by `batch` bytes. */
            if ((blen >= batch || (done && nm)) && nm) {
                /* span[m] absorbs trailing/leading non-file blocks; the last
                 * member's span runs to blen (buffer end). */
                for (int m = 0; m < nm; m++) {
                    int64_t next = (m + 1 < nm) ? mem[m+1].start : blen;
                    mem[m].span = next - mem[m].start;
                    mb_row(&mb, mem[m].name, 0, (int32_t)m, mem[m].size,
                           mem[m].mode, mem[m].mtime, mem[m].uid, mem[m].gid,
                           mem[m].span);
                }
                tr_span(lane, TR_DECODE, win_t0, tr_now(), nm, blen);
                /* Serialized exchange: emit ZMETA, read PLAN, compress the
                 * window's frames (pool), emit COMP — one reader at a time, so
                 * the three fds stay ordered and Python is synchronous. */
                double w0 = tr_now();
                pthread_mutex_lock(&ZS.exch);
                double w1 = tr_now(); tr_span(lane, TR_EXCH_WAIT, w0, w1, 0, 0);
                mb_flush(&mb);                       /* ZMETA(window) → stdout */
                CmdBatch pc;
                if (read_cmd_stream(&pc, &pmeta, &pmc, &pbody, &pbc)) {
                    ZS.rc = 1; pthread_mutex_unlock(&ZS.exch); break;
                }
                /* pc: one row per member (ZMETA order); dep_group = frame id.
                 * Build one job per frame (contiguous [mem[a].start, mem[r].start)
                 * — includes interleaved dirs), then compress them in parallel. */
                int64_t cn = pc.n_rows, wf = 0;
                uint64_t *fg = malloc(8*(size_t)cn);      /* per-frame plan id */
                int64_t r = 0;
                while (r < cn) {
                    int64_t g = pc.dep_group[r], a = r;
                    while (r < cn && pc.dep_group[r] == g) r++;   /* frame [a,r) */
                    int64_t foff = mem[a].start;
                    int64_t fend = (r < nm) ? mem[r].start : blen;
                    if (wf >= jcap) {
                        jcap *= 2; jobs = realloc(jobs, sizeof(ZFrameJob)*(size_t)jcap);
                        fg = realloc(fg, 8*(size_t)jcap);
                    }
                    jobs[wf] = (ZFrameJob){buf + foff, fend - foff, level, 0, 0, 0};
                    fg[wf] = (uint64_t)g; wf++;
                }
                int cerr = zp_run_window(jobs, (int)wf); /* parallel compress */
                uint64_t *fco = malloc(8*(size_t)wf), *fcl = malloc(8*(size_t)wf);
                for (int64_t i = 0; i < wf; i++) {
                    fco[i] = (uint64_t)jobs[i].coff; fcl[i] = (uint64_t)jobs[i].clen;
                }
                emit_comp_batch(comp_fd, wf, fg, (int64_t *)fco, fcl);
                pthread_mutex_unlock(&ZS.exch);
                tr_span(lane, TR_EXCH, w1, tr_now(), wf, 0);
                atomic_fetch_add(&ZS.nframes, wf);
                free(fg); free(fco); free(fcl);
                free_cmd_batch(&pc);
                for (int m = 0; m < nm; m++) free(mem[m].name);
                blen = 0; mstart = 0; nm = 0;        /* recycle the buffer */
                win_t0 = tr_now();                   /* next window starts now */
                if (cerr) { ZS.rc = 1; done = 1; }
            }
        }
        mb_free(&mb); zsrc_close(&z);
        if (ZS.rc) break;
    }
    free(buf); free(mem); free(jobs); free(pmeta); free(pbody);
    return NULL;
}

static int run_zstream(int comp_fd, const char **srcs, int nsrc,
                       const char *out, int level, int64_t batch,
                       int compressors, int readers) {
    int ofd = open(out, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (ofd < 0) { perror("out"); return 2; }
    tr_init();
    memset(&Z, 0, sizeof Z); pthread_mutex_init(&Z.omu, NULL);
    emit_schema(1, ZMETA_SCHEMA_META, ZMETA_SCHEMA_LEN);   /* stdout: ZMETA */
    emit_schema(comp_fd, COMP_SCHEMA_META, COMP_SCHEMA_LEN);/* comp_fd: COMP */
    zp_start(ofd, compressors);

    memset(&ZS, 0, sizeof ZS);
    ZS.srcs = srcs; ZS.nsrc = nsrc; ZS.level = level; ZS.comp_fd = comp_fd;
    ZS.batch = batch; pthread_mutex_init(&ZS.exch, NULL);
    if (readers < 1) readers = 1;
    if (readers > 64) readers = 64;
    pthread_t rt[64]; int rid[64];
    for (int i = 0; i < readers; i++) {
        rid[i] = i;
        pthread_create(&rt[i], NULL, zstream_reader, &rid[i]);
    }
    for (int i = 0; i < readers; i++) pthread_join(rt[i], NULL);

    zp_stop();
    emit_eos(1); emit_eos(comp_fd);
    close(ofd);
    tr_dump();
    fprintf(stderr, "zstream: %ld frames, %ld bytes (%d readers)\n",
            (long)ZS.nframes, (long)ZP.append, readers);
    return ZS.rc;
}


int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: %s exec <archive|-> [uring|sync]\n"
                        "       %s scan <root> [uring|sync]\n",
                argv[0], argv[0]);
        return 2;
    }
    crc64_init();
    crc64_init_slices();
    const char *mode = argc > 3 ? argv[3] : "auto";
    int use_uring = strcmp(mode, "sync") != 0;

    if (!strcmp(argv[1], "scan")) {
        int threads = argc > 4 ? atoi(argv[4]) : 8;
        if (use_uring && strcmp(mode, "uring") != 0) {
            struct io_uring probe;                 /* auto-detect */
            if (io_uring_queue_init(8, &probe, 0) < 0) use_uring = 0;
            else io_uring_queue_exit(&probe);
        }
        const char *prefix = argc > 5 ? argv[5] : "";
        const char *glob = argc > 6 ? argv[6] : "";
        int emit_closes = argc > 7 && argv[7][0] == '1';
        fprintf(stderr, "quiver-exec scan: engine=%s threads=%d\n",
                use_uring ? "uring" : "sync", threads);
        return run_scan(argv[2], use_uring, threads, prefix, glob,
                        emit_closes);
    }

    if (!strcmp(argv[1], "zpack")) {
        /* zpack <out> <level> <batch> <readers> <compressors> <src...> */
        if (argc < 8) { fprintf(stderr, "zpack: too few args\n"); return 2; }
        return run_zpack((const char **)&argv[7], argc - 7, argv[2],
                         atoi(argv[3]), atoll(argv[4]), atoi(argv[5]),
                         atoi(argv[6]));
    }

    if (!strcmp(argv[1], "zscan")) {
        /* zscan <readers> <src...> — emit member metadata for the planner */
        if (argc < 3) { fprintf(stderr, "zscan: too few args\n"); return 2; }
        return run_zscan((const char **)&argv[3], argc - 3, atoi(argv[2]));
    }

    if (!strcmp(argv[1], "zexec")) {
        /* zexec <plan> <pattern> <level> <readers> <compressors>
         *       <nsink> <starts> <src...> */
        if (argc < 10) { fprintf(stderr, "zexec: too few args\n"); return 2; }
        return run_zexec(argv[2], (const char **)&argv[9], argc - 9, argv[3],
                         atoi(argv[4]), atoi(argv[5]), atoi(argv[6]),
                         atoi(argv[7]), argv[8]);
    }

    if (!strcmp(argv[1], "zstream")) {
        /* zstream <comp_fd> <out> <level> <batch> <compressors> <readers>
         *         <src...> — one-pass planned recompress: ZMETA on stdout,
         * PLAN on stdin, COMP on comp_fd. `readers` decode DIFFERENT sources
         * in parallel; `compressors` compress a window's frames in parallel. */
        if (argc < 9) { fprintf(stderr, "zstream: too few args\n"); return 2; }
        return run_zstream(atoi(argv[2]), (const char **)&argv[8], argc - 8,
                           argv[3], atoi(argv[4]), atoll(argv[5]),
                           atoi(argv[6]), atoi(argv[7]));
    }

    struct io_uring ring;
    if (use_uring) {
        /* metadata-only ring: no registered files needed (single-op SQEs
         * against AT_FDCWD / the archive fd), so this works on every
         * kernel with io_uring_queue_init — back to 5.6. */
        int rc = io_uring_queue_init(QD, &ring, 0);
        if (rc < 0) {
            if (strcmp(mode, "uring") == 0) {
                fprintf(stderr, "io_uring: %s\n", strerror(-rc));
                return 2;
            }
            use_uring = 0;
        }
    }
    fprintf(stderr, "quiver-exec %s: engine=%s\n", argv[1],
            use_uring ? "uring" : "sync");

    if (!strcmp(argv[1], "exec")) {
        int afd = -1;
        if (strcmp(argv[2], "-") != 0) {
            afd = open(argv[2], O_RDWR | O_CREAT, 0644);  /* R:EXTRACT W:COPY/COMPRESS */
            if (afd < 0) { perror("archive"); return 2; }
        }
        src_open_all(argc, argv, 4);          /* argv[4..] = shard sources (AOT) */
        return run_exec(afd, use_uring, &ring);
    }
    fprintf(stderr, "unknown mode %s\n", argv[1]);
    return 2;
}
