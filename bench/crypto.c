#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <sodium.h>
static double now(void){struct timespec t;clock_gettime(CLOCK_MONOTONIC,&t);return t.tv_sec+t.tv_nsec*1e-9;}
int main(void){
  if(sodium_init()<0)return 1;
  size_t N=512ull<<20; uint8_t *in=malloc(N), *out=malloc(N+64);
  uint64_t x=1; for(size_t i=0;i<N;i+=8){x^=x>>12;x^=x<<25;x^=x>>27;memcpy(in+i,&x,8);}
  uint8_t key[32], k2[32], npub[24], mac[32]; randombytes_buf(key,32); randombytes_buf(k2,32); randombytes_buf(npub,24);
  int aesok = crypto_aead_aes256gcm_is_available();
  for(int pass=0;pass<2;pass++){
    size_t pc = pass?(1<<20):(64<<10);
    printf("%s pieces:\n", pass?"1MB":"64KB");
    // keyed BLAKE2b-256 (chunk id under a repo key)
    {double t=now();uint64_t acc=0;size_t d=0;
     while(d+pc<=N){crypto_generichash(mac,32,in+d,pc,key,32);acc^=*(uint64_t*)mac;d+=pc;}
     printf("  keyed-BLAKE2b-256   %6.2f GB/s\n", d/(now()-t)/1e9);}
    // XChaCha20-Poly1305 AEAD encrypt
    {double t=now();size_t d=0;unsigned long long cl;
     while(d+pc<=N){crypto_aead_xchacha20poly1305_ietf_encrypt(out,&cl,in+d,pc,NULL,0,NULL,npub,key);d+=pc;}
     printf("  XChaCha20-Poly1305  %6.2f GB/s\n", d/(now()-t)/1e9);}
    // AES-256-GCM (AES-NI) if available
    if(aesok){crypto_aead_aes256gcm_state st;crypto_aead_aes256gcm_beforenm(&st,key);
     double t=now();size_t d=0;unsigned long long cl;
     while(d+pc<=N){crypto_aead_aes256gcm_encrypt_afternm(out,&cl,in+d,pc,NULL,0,NULL,npub,&st);d+=pc;}
     printf("  AES-256-GCM (NI)    %6.2f GB/s\n", d/(now()-t)/1e9);}
    else printf("  AES-256-GCM: no HW\n");
  }
  return 0;
}
