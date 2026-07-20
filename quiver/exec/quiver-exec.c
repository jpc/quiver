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
 * header_offset, payload (padded to pad_align) at data_offset. */
enum { OP_UNLINK = 2, OP_RMDIR = 3, OP_MKDIR = 4,
       OP_COPY = 5, OP_CKSUM = 6, OP_FBARRIER = 7, OP_SETMETA = 8,
       OP_EXTRACT = 9 };   /* archive[data_offset,size] -> path */
/* mode/uid/gid/mtime_ns use -1 as "unspecified": COPY/MKDIR fall back
 * to 0644/0755, SETMETA leaves the attribute untouched. */
#define DEFAULT_FILE_MODE 0644
#define DEFAULT_DIR_MODE  0755

#include "md5.h"   /* vendored public-domain MD5 (Solar Designer);
                      -DHAVE_OPENSSL -lcrypto swaps in OpenSSL's asm */

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
        if (rmdir(c->path[i]) < 0) out->res = -errno;
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
        /* inverse COPY: archive[data_offset, size] -> create path */
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
typedef struct {
    const CmdBatch *c; RowState *rs; RowResult *out; int afd;
    pthread_mutex_t mu; pthread_cond_t cv_work, cv_done;
    int64_t q[WINDOW];  int qn;      /* rows waiting for a worker */
    int64_t dq[WINDOW]; int dn;      /* rows fully executed */
    int active, stop;
    pthread_t tid[OPEN_POOL]; int nthreads;
} OpenPool;

/* Workers run the ENTIRE row via row_sync (thread-safe: __thread bufs,
 * per-row out slots, distinct offsets on the shared archive fd). On
 * wekafs, N plain threads scale where 5.15's io-wq punting doesn't
 * (measured: 20k copies t16 2.6s / t64 0.7s vs 36s through chains). */
static void *open_worker(void *arg) {
    OpenPool *p = arg;
    for (;;) {
        pthread_mutex_lock(&p->mu);
        while (p->qn == 0 && !p->stop)
            pthread_cond_wait(&p->cv_work, &p->mu);
        if (p->qn == 0 && p->stop) { pthread_mutex_unlock(&p->mu); return NULL; }
        int64_t i = p->q[--p->qn];
        p->active++;
        pthread_mutex_unlock(&p->mu);

        row_sync(p->c, i, p->afd, &p->out[i]);

        pthread_mutex_lock(&p->mu);
        p->dq[p->dn++] = i;
        p->active--;
        pthread_cond_signal(&p->cv_done);
        pthread_mutex_unlock(&p->mu);
    }
}

static void pool_start(OpenPool *p, const CmdBatch *c, RowState *rs,
                       RowResult *out, int afd) {
    memset(p, 0, sizeof *p);
    p->c = c; p->rs = rs; p->out = out; p->afd = afd;
    pthread_mutex_init(&p->mu, NULL);
    pthread_cond_init(&p->cv_work, NULL);
    pthread_cond_init(&p->cv_done, NULL);
    p->nthreads = OPEN_POOL;
    for (int t = 0; t < p->nthreads; t++)
        pthread_create(&p->tid[t], NULL, open_worker, p);
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

static void pool_push(OpenPool *p, int64_t i) {
    pthread_mutex_lock(&p->mu);
    p->q[p->qn++] = i;
    pthread_cond_signal(&p->cv_work);
    pthread_mutex_unlock(&p->mu);
}

/* Harvest opened rows. If `block`, wait until at least one is ready
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

/* The scheduler is engine-agnostic: epochs, refcount deps, and the slot
 * bound are identical whether a row executes as a single ring SQE or on
 * the pool. ring == NULL (engine=sync, or platforms without io_uring)
 * routes every row through the pool. */
static int run_batch_uring(struct io_uring *ring, const CmdBatch *c, int afd,
                           RowResult *out) {
    int64_t n = c->n_rows;
    int use_ring = ring != NULL;
    RowState *rs = calloc((size_t)n, sizeof *rs);
    int64_t *rc = calloc((size_t)n, sizeof(int64_t));   /* children left */
    int64_t *ready = malloc((size_t)n * sizeof(int64_t));
    OpenPool pool; int pool_on = 0;    /* started on first thread-open row */
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
                int ring_op = use_ring &&
                    (op == OP_UNLINK || op == OP_RMDIR || op == OP_MKDIR ||
                     (op == OP_FBARRIER && !c->path[i][0]));
                if (!ring_op) {
                    if (n_free == 0) { ready[n_ready++] = i; break; }
                    r->slot = free_slots[--n_free];
                    if (!pool_on) { pool_start(&pool, c, rs, out, afd);
                                    pool_on = 1; }
                    pool_push(&pool, i);
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
            if (pool_on) {      /* rows come back from the pool fully
                                   executed — just release and account */
                int64_t got[WINDOW];
                /* block iff the pull loop can't progress either (no ready
                 * rows, or slots all held by queued rows) — otherwise a
                 * zero-CQE iteration would spin hot */
                int block = chains_inflight == 0 && done < span &&
                            (n_ready == 0 || n_free == 0) &&
                            pool_outstanding(&pool) > 0;
                int k = pool_harvest(&pool, got, block);
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
                if (pool_on) pool_stop(&pool);
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
    if (pool_on) pool_stop(&pool);
    free(rs); free(rc); free(ready);
    return 0;
}

/* ── exec mode main loop ───────────────────────────────────────────────── */

static int run_exec(int afd, int use_uring, struct io_uring *ring) {
    emit_schema(1, COMP_SCHEMA_META, COMP_SCHEMA_LEN);
    uint8_t *meta = NULL, *body = NULL;
    size_t mcap = 0, bcap = 0;
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
        if (run_batch_uring(use_uring ? ring : NULL, &cb, afd, rr))
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
                          int32_t depth, Ent *ents, int cnt) {
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
                              w->batch[a], cntA);
                a = 1 - a; cntA = cntB;
            }
            close(dfd);
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
                    const char *prefix, const char *glob) {
    emit_schema(1, STAT_SCHEMA_META, STAT_SCHEMA_LEN);
    G.root = root; G.use_uring = use_uring;
    G.prefix = prefix; G.glob = glob;
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
        fprintf(stderr, "quiver-exec scan: engine=%s threads=%d\n",
                use_uring ? "uring" : "sync", threads);
        return run_scan(argv[2], use_uring, threads, prefix, glob);
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
            afd = open(argv[2], O_RDWR);   /* R for EXTRACT, W for COPY */
            if (afd < 0) { perror("archive"); return 2; }
        }
        return run_exec(afd, use_uring, &ring);
    }
    fprintf(stderr, "unknown mode %s\n", argv[1]);
    return 2;
}
