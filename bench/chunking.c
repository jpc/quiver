/* rolling.c — prototype + benchmark: delta-sync algorithms for quiver rsync.
 *
 *   A) classic rsync: fixed 4K blocks on OLD; NEW scanned with a rolling weak sum
 *      (adler-style a,b) + xxh64 strong verify on weak hit. Byte-precise deltas,
 *      but per-byte hashtable probes and an O(file) scan on the sender.
 *   B) FastCDC: gear-hash content-defined chunking (normalized, min/avg/max);
 *      both sides chunk independently, delta = chunks of NEW whose xxh64 is not
 *      in OLD's chunk set. Chunk-granular deltas, one linear pass, no per-file
 *      block exchange (a chunk-digest manifest can ride the control plane).
 *
 * modes:
 *   chunk  <file>                  FastCDC throughput + chunk stats
 *   xxh    <file>                  xxh64 throughput baseline
 *   dcdc   <old> <new>             CDC delta: time + literal bytes to send
 *   drsync <old> <new>             classic delta: time + literal bytes to send
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <sys/stat.h>

static double now(void){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+t.tv_nsec*1e-9; }
static uint8_t *load(const char *p, size_t *n){
    int fd=open(p,O_RDONLY); struct stat st; fstat(fd,&st); *n=(size_t)st.st_size;
    uint8_t *b=malloc(*n?*n:1); size_t o=0; ssize_t r;
    while(o<*n&&(r=read(fd,b+o,*n-o))>0) o+=(size_t)r; close(fd); return b;
}

/* ---- xxh64 (public-domain algorithm, compact impl) ---- */
#define P1 0x9E3779B185EBCA87ULL
#define P2 0xC2B2AE3D27D4EB4FULL
#define P3 0x165667B19E3779F9ULL
#define P4 0x85EBCA77C2B2AE63ULL
#define P5 0x27D4EB2F165667C5ULL
static inline uint64_t rotl(uint64_t x,int r){ return (x<<r)|(x>>(64-r)); }
static inline uint64_t rd64(const uint8_t*p){ uint64_t v; memcpy(&v,p,8); return v; }
static inline uint32_t rd32(const uint8_t*p){ uint32_t v; memcpy(&v,p,4); return v; }
static uint64_t xxh64(const uint8_t *p, size_t len, uint64_t seed){
    const uint8_t *end=p+len; uint64_t h;
    if(len>=32){
        uint64_t v1=seed+P1+P2, v2=seed+P2, v3=seed, v4=seed-P1;
        const uint8_t *lim=end-32;
        do{ v1=rotl(v1+rd64(p)*P2,31)*P1; p+=8; v2=rotl(v2+rd64(p)*P2,31)*P1; p+=8;
            v3=rotl(v3+rd64(p)*P2,31)*P1; p+=8; v4=rotl(v4+rd64(p)*P2,31)*P1; p+=8; }while(p<=lim);
        h=rotl(v1,1)+rotl(v2,7)+rotl(v3,12)+rotl(v4,18);
        v1*=P2; v1=rotl(v1,31); v1*=P1; h^=v1; h=h*P1+P4;
        v2*=P2; v2=rotl(v2,31); v2*=P1; h^=v2; h=h*P1+P4;
        v3*=P2; v3=rotl(v3,31); v3*=P1; h^=v3; h=h*P1+P4;
        v4*=P2; v4=rotl(v4,31); v4*=P1; h^=v4; h=h*P1+P4;
    } else h=seed+P5;
    h+=(uint64_t)len;
    while(p+8<=end){ uint64_t k=rd64(p); k*=P2; k=rotl(k,31); k*=P1; h^=k; h=rotl(h,27)*P1+P4; p+=8; }
    if(p+4<=end){ h^=(uint64_t)rd32(p)*P1; h=rotl(h,23)*P2+P3; p+=4; }
    while(p<end){ h^=(*p++)*P5; h=rotl(h,11)*P1; }
    h^=h>>33; h*=P2; h^=h>>29; h*=P3; h^=h>>32;
    return h;
}

/* ---- FastCDC (gear hash, normalized chunking) ---- */
static uint64_t GEAR[256];
static void gear_init(void){ uint64_t x=0x2545F4914F6CDD1DULL;
    for(int i=0;i<256;i++){ x^=x>>12; x^=x<<25; x^=x>>27; GEAR[i]=x*0x2545F4914F6CDD1DULL; } }
static size_t CDC_MIN=2*1024, CDC_AVG=8*1024, CDC_MAX=64*1024;
static uint64_t MASK_S, MASK_L;
static void cdc_config(void){                       /* env CDC_AVG_KB; min=avg/4, max=avg*8 */
    const char *e=getenv("CDC_AVG_KB");
    size_t avg=(e?strtoul(e,0,10):8)*1024;
    CDC_AVG=avg; CDC_MIN=avg/4; CDC_MAX=avg*8;
    int bits=0; while(((size_t)1<<bits)<avg) bits++;   /* log2(avg) */
    /* normalized chunking: harder mask before avg (bits+2), easier after (bits-2);
     * gear entropy lives in the HIGH bits (h<<1 per byte) -> mask the top */
    MASK_S=((1ULL<<(bits+2))-1)<<(62-bits);
    MASK_L=((1ULL<<(bits-2))-1)<<(62-bits);
}
typedef void (*chunk_cb)(size_t off, size_t len, void *u);
static void fastcdc(const uint8_t *b, size_t n, chunk_cb cb, void *u){
    size_t off=0;
    while(off<n){
        size_t rem=n-off;
        if(rem<=CDC_MIN){ cb(off,rem,u); break; }
        size_t max=rem<CDC_MAX?rem:CDC_MAX, avg=rem<CDC_AVG?rem:CDC_AVG;
        uint64_t h=0; size_t i=CDC_MIN;
        for(;i<avg;i++){ h=(h<<1)+GEAR[b[off+i]]; if(!(h&MASK_S)){ i++; goto cut; } }
        for(;i<max;i++){ h=(h<<1)+GEAR[b[off+i]]; if(!(h&MASK_L)){ i++; goto cut; } }
    cut:
        cb(off,i,u); off+=i;
    }
}

/* chunk-hash set (open addressing) */
typedef struct { uint64_t *v; size_t cap, n; } HSet;
static void hs_init(HSet *s, size_t want){ s->cap=64; while(s->cap<want*2) s->cap<<=1; s->v=calloc(s->cap,8); s->n=0; }
static void hs_add(HSet *s, uint64_t h){ if(!h)h=1; size_t i=h&(s->cap-1);
    while(s->v[i]){ if(s->v[i]==h) return; i=(i+1)&(s->cap-1); } s->v[i]=h; s->n++; }
static int hs_has(HSet *s, uint64_t h){ if(!h)h=1; size_t i=h&(s->cap-1);
    while(s->v[i]){ if(s->v[i]==h) return 1; i=(i+1)&(s->cap-1); } return 0; }

typedef struct { const uint8_t *b; HSet *old; size_t nchunks, lit, matched, minc, maxc; } DctX;
static void cb_count(size_t off, size_t len, void *u){ DctX *x=u; (void)off; x->nchunks++;
    if(!x->minc||len<x->minc)x->minc=len; if(len>x->maxc)x->maxc=len; }
static void cb_add(size_t off, size_t len, void *u){ DctX *x=u; hs_add(x->old, xxh64(x->b+off,len,0)); x->nchunks++; }
static void cb_delta(size_t off, size_t len, void *u){ DctX *x=u;
    if(hs_has(x->old, xxh64(x->b+off,len,0))) x->matched+=len; else x->lit+=len; x->nchunks++; }

/* ---- classic rsync ---- */
#define RBLK 4096
typedef struct { uint32_t weak; uint64_t strong; int32_t next; } RB;
static inline uint32_t weak_of(const uint8_t *p, size_t n){
    uint32_t a=0,b=0; for(size_t i=0;i<n;i++){ a+=p[i]; b+=a; } return (a&0xffff)|(b<<16); }
int main(int argc, char **argv){
    gear_init(); cdc_config();
    const char *mode=argv[1];
    size_t n1; uint8_t *f1=load(argv[2],&n1);
    if(!strcmp(mode,"xxh")){
        double t=now(); uint64_t h=0;
        for(int r=0;r<3;r++) h^=xxh64(f1,n1,r);
        double dt=(now()-t)/3;
        printf("xxh64      %8.2f MB in %6.3fs  %8.2f GB/s  (h=%016lx)\n", n1/1e6, dt, n1/dt/1e9, h);
    } else if(!strcmp(mode,"chunk")){
        DctX x={0}; double t=now();
        for(int r=0;r<3;r++){ x.nchunks=0; x.minc=x.maxc=0; fastcdc(f1,n1,cb_count,&x); }
        double dt=(now()-t)/3;
        printf("fastcdc    %8.2f MB in %6.3fs  %8.2f GB/s  %zu chunks avg %zu B (min %zu max %zu)\n",
               n1/1e6, dt, n1/dt/1e9, x.nchunks, x.nchunks?n1/x.nchunks:0, x.minc, x.maxc);
        /* chunk + hash together (the real sender cost) */
        HSet hs; hs_init(&hs, n1/CDC_AVG+16); DctX y={f1,&hs,0,0,0,0,0};
        t=now(); fastcdc(f1,n1,cb_add,&y); dt=now()-t;
        printf("cdc+xxh64  %8.2f MB in %6.3fs  %8.2f GB/s  (%zu uniq of %zu)\n",
               n1/1e6, dt, n1/dt/1e9, hs.n, y.nchunks);
    } else if(!strcmp(mode,"dcdc")){
        size_t n2; uint8_t *f2=load(argv[3],&n2);
        double t=now();
        HSet hs; hs_init(&hs, n1/CDC_AVG+16);
        DctX xo={f1,&hs,0,0,0,0,0}; fastcdc(f1,n1,cb_add,&xo);       /* receiver-side chunk+hash */
        DctX xn={f2,&hs,0,0,0,0,0}; fastcdc(f2,n2,cb_delta,&xn);     /* sender-side chunk+lookup */
        double dt=now()-t;
        printf("cdc-delta  old %.2f MB new %.2f MB  %6.3fs (%5.2f GB/s total)  send %8.3f MB (%5.2f%%)  chunks old %zu new %zu\n",
               n1/1e6, n2/1e6, dt, (n1+n2)/dt/1e9, xn.lit/1e6, 100.0*xn.lit/(n2?n2:1), xo.nchunks, xn.nchunks);
    } else if(!strcmp(mode,"drsync")){
        size_t n2; uint8_t *f2=load(argv[3],&n2);
        double t=now();
        size_t nb=(n1+RBLK-1)/RBLK;
        RB *tab=malloc(nb*sizeof(RB));
        int32_t *head=malloc((1<<20)*4); memset(head,-1,(1<<20)*4);   /* weak-hash buckets */
        for(size_t i=0;i<nb;i++){ size_t off=i*RBLK, len=off+RBLK<=n1?RBLK:n1-off;
            tab[i].weak=weak_of(f1+off,len); tab[i].strong=xxh64(f1+off,len,0);
            uint32_t bkt=(tab[i].weak^(tab[i].weak>>12))&((1<<20)-1);
            tab[i].next=head[bkt]; head[bkt]=(int32_t)i; }
        double t_tab=now()-t;
        /* sender scan: roll over NEW */
        size_t lit=0, matched=0, probes=0, strongs=0;
        if(n2>=RBLK){
            uint32_t a=0,b=0;
            for(size_t i=0;i<RBLK;i++){ a+=f2[i]; b+=a; }
            size_t w=0, litstart=0;
            for(;;){
                uint32_t weak=(a&0xffff)|(b<<16);
                uint32_t bkt=(weak^(weak>>12))&((1<<20)-1);
                int hit=0;
                for(int32_t j=head[bkt]; j>=0; j=tab[j].next){
                    probes++;
                    if(tab[j].weak==weak){
                        strongs++;
                        if(tab[j].strong==xxh64(f2+w,RBLK,0)){ hit=1; break; }
                    }
                }
                if(hit){
                    lit+=w-litstart; matched+=RBLK;
                    w+=RBLK; litstart=w;
                    if(w+RBLK>n2) break;
                    a=0;b=0; for(size_t i=0;i<RBLK;i++){ a+=f2[w+i]; b+=a; }
                } else {
                    if(w+RBLK>=n2) break;
                    a=a-f2[w]+f2[w+RBLK];
                    b=b-(uint32_t)RBLK*f2[w]+a;
                    w++;
                }
            }
        }
        (void)0; lit = n2 - matched;      /* every byte not in a matched block is literal */
        double dt=now()-t;
        printf("rsync-delta old %.2f MB new %.2f MB  %6.3fs (%5.2f GB/s scan)  send %8.3f MB (%5.2f%%)  tab %5.3fs  probes %zu strongs %zu\n",
               n1/1e6, n2/1e6, dt, n2/(dt-t_tab)/1e9, lit/1e6, 100.0*lit/(n2?n2:1), t_tab, probes, strongs);
    }
    return 0;
}
