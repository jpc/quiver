/* hash candidates for CDC chunk identity: throughput on 64K chunks (the real shape)
 * + 1MB streams. two-seed xxh64 (composite 128b) vs libsodium BLAKE2b-128/256,
 * SHA-256, SipHash64 (reference; too narrow for identity). */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <sodium.h>
static double now(void){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+t.tv_nsec*1e-9; }
#define XP1 0x9E3779B185EBCA87ULL
#define XP2 0xC2B2AE3D27D4EB4FULL
#define XP3 0x165667B19E3779F9ULL
#define XP4 0x85EBCA77C2B2AE63ULL
#define XP5 0x27D4EB2F165667C5ULL
static inline uint64_t rot(uint64_t x,int r){ return (x<<r)|(x>>(64-r)); }
static inline uint64_t rd64(const uint8_t*p){ uint64_t v; memcpy(&v,p,8); return v; }
static uint64_t xxh64(const uint8_t *p, size_t len, uint64_t seed){
    const uint8_t *end=p+len; uint64_t h;
    if(len>=32){ uint64_t v1=seed+XP1+XP2,v2=seed+XP2,v3=seed,v4=seed-XP1; const uint8_t*lim=end-32;
        do{ v1=rot(v1+rd64(p)*XP2,31)*XP1;p+=8; v2=rot(v2+rd64(p)*XP2,31)*XP1;p+=8;
            v3=rot(v3+rd64(p)*XP2,31)*XP1;p+=8; v4=rot(v4+rd64(p)*XP2,31)*XP1;p+=8; }while(p<=lim);
        h=rot(v1,1)+rot(v2,7)+rot(v3,12)+rot(v4,18);
        v1*=XP2;v1=rot(v1,31);v1*=XP1;h^=v1;h=h*XP1+XP4; v2*=XP2;v2=rot(v2,31);v2*=XP1;h^=v2;h=h*XP1+XP4;
        v3*=XP2;v3=rot(v3,31);v3*=XP1;h^=v3;h=h*XP1+XP4; v4*=XP2;v4=rot(v4,31);v4*=XP1;h^=v4;h=h*XP1+XP4;
    } else h=seed+XP5;
    h+=(uint64_t)len;
    while(p+8<=end){ uint64_t k=rd64(p); k*=XP2;k=rot(k,31);k*=XP1; h^=k; h=rot(h,27)*XP1+XP4; p+=8; }
    while(p<end){ h^=(*p++)*XP5; h=rot(h,11)*XP1; }
    h^=h>>33;h*=XP2;h^=h>>29;h*=XP3;h^=h>>32; return h;
}
typedef uint64_t (*hfn)(const uint8_t*, size_t);
static uint64_t h_xxh2seed(const uint8_t*p,size_t n){ return xxh64(p,n,0)^xxh64(p,n,XP1); }
static uint64_t h_b2b128(const uint8_t*p,size_t n){ uint8_t o[16]; crypto_generichash(o,16,p,n,0,0); uint64_t v; memcpy(&v,o,8); return v; }
static uint64_t h_b2b256(const uint8_t*p,size_t n){ uint8_t o[32]; crypto_generichash(o,32,p,n,0,0); uint64_t v; memcpy(&v,o,8); return v; }
static uint64_t h_sha256(const uint8_t*p,size_t n){ uint8_t o[32]; crypto_hash_sha256(o,p,n); uint64_t v; memcpy(&v,o,8); return v; }
static uint8_t sipk[16]={1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
static uint64_t h_sip(const uint8_t*p,size_t n){ uint8_t o[8]; crypto_shorthash(o,p,n,sipk); uint64_t v; memcpy(&v,o,8); return v; }
static void bench(const char*name, hfn f, const uint8_t*buf, size_t total, size_t piece){
    double t=now(); uint64_t acc=0; size_t done=0;
    while(done+piece<=total){ acc^=f(buf+done,piece); done+=piece; }
    double dt=now()-t;
    printf("  %-22s %7.2f GB/s   (%zuK pieces, acc=%016lx)\n", name, done/dt/1e9, piece/1024, acc);
}
int main(void){
    if(sodium_init()<0) return 1;
    size_t N=512ull<<20; uint8_t *buf=malloc(N);
    uint64_t x=1; for(size_t i=0;i<N;i+=8){ x^=x>>12;x^=x<<25;x^=x>>27; memcpy(buf+i,&x,8); }
    for(int pass=0;pass<2;pass++){
      size_t piece = pass? (1<<20) : (64<<10);
      printf("%s pieces over %zu MB:\n", pass?"1MB":"64KB", N>>20);
      bench("xxh64 two-seed (128b)", h_xxh2seed, buf, N, piece);
      bench("BLAKE2b-128 (sodium)",  h_b2b128,   buf, N, piece);
      bench("BLAKE2b-256 (sodium)",  h_b2b256,   buf, N, piece);
      bench("SHA-256 (sodium)",      h_sha256,   buf, N, piece);
      bench("SipHash64 (reference)", h_sip,      buf, N, piece);
    }
    return 0;
}
