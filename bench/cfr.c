// cfr — is copy_file_range worth it on the PACK side?
//
// Incompressible members (already-compressed media, model weights, .zst/.gz) are stored
// codec-free as a zstd frame of Raw_Blocks. On a real home-directory backup that was 567 GB
// of 4.9 TB — bytes that today are pread into a pack buffer, memcpy'd into the output buffer
// with the block headers interleaved, and pwritten back out. None of that data needs to be
// in user space.
//
// The catch: a zstd frame of Raw_Blocks is NOT one contiguous run. The format is
//     [4 magic][1 FHD][8 FCS]  then per block  [3 block header][<=131072 raw bytes]
// so a zero-copy path has to interleave a 3-byte pwrite before every 128 KiB copy_file_range
// — 8 syscall pairs per MB. This measures whether that trade pays.
//
//   modes: rw     pread into a buffer + pwrite out (no framing — the floor)
//          rwz    same, plus the interleave memcpy into a framed output buffer (bvm today)
//          wv     pwritev the frame/block headers and the SAME buffer as an iovec (no memcpy)
//          cfr    3-byte pwrite + copy_file_range per 128 KiB block (the zero-copy proposal)
//          cfr1   one whole-file copy_file_range, no framing (the upper bound)
//   usage: ./cfr <dir> [file_mb] [nfiles] [threads] [modes...]
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <errno.h>

#define BLK 131072                       // zstd Raw_Block maximum
static double now(void){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+1e-9*t.tv_nsec; }

static size_t framed_len(size_t n){ size_t nb=(n+BLK-1)/BLK; return 13+nb*3+n; }

// the 13-byte frame header: magic, FHD (single segment, 8-byte FCS), then the content size
static void frame_hdr(unsigned char *h, size_t n){
    h[0]=0x28;h[1]=0xB5;h[2]=0x2F;h[3]=0xFD; h[4]=0xE0;
    for(int i=0;i<8;i++) h[5+i]=(unsigned char)(n>>(8*i));
}
static void blk_hdr(unsigned char *h, size_t len, int last){   // last | type(0=raw) | size
    unsigned int v = (unsigned int)((len<<3) | (0u<<1) | (last?1u:0u));
    h[0]=v&0xFF; h[1]=(v>>8)&0xFF; h[2]=(v>>16)&0xFF;
}

typedef struct { const char *dir; int mode, id, nfiles, nthr; size_t fsz; double bytes; } Arg;
enum { M_RW, M_RWZ, M_WV, M_CFR, M_CFR1 };

static int pwritev_all(int fd, struct iovec *iov, int n, off_t off){
    while (n > 0){
        ssize_t r = pwritev(fd, iov, n, off);
        if (r < 0){ if (errno==EINTR) continue; return -1; }
        if (r == 0){ errno=EIO; return -1; }
        off += r;
        while (n > 0 && (size_t)r >= iov->iov_len){ r -= iov->iov_len; iov++; n--; }
        if (n > 0 && r){ iov->iov_base=(char*)iov->iov_base+r; iov->iov_len-=r; }
    }
    return 0;
}

static void *run(void *p){
    Arg *a=p; size_t n=a->fsz;
    unsigned char *ib = aligned_alloc(4096, n), *ob = NULL, *bhs = NULL;
    if (a->mode==M_RWZ) ob = malloc(framed_len(n));
    if (a->mode==M_WV)  bhs = malloc(((n+BLK-1)/BLK)*3 + 3);
    char sp[512], dp[512];
    snprintf(dp,sizeof dp,"%s/out.%d",a->dir,a->id);
    int od = open(dp, O_WRONLY|O_CREAT|O_TRUNC, 0644);
    off_t doff = 0;
    for (int f=a->id; f<a->nfiles; f+=a->nthr){
        snprintf(sp,sizeof sp,"%s/src.%d",a->dir,f);
        int sd = open(sp, O_RDONLY);
        if (sd<0) continue;
        if (a->mode==M_CFR1){
            off_t si=0, di=doff; size_t left=n;
            while (left){ ssize_t r=copy_file_range(sd,&si,od,&di,left,0); if(r<=0) break; left-=r; }
            doff += n;
        } else if (a->mode==M_CFR){
            unsigned char h[13]; frame_hdr(h,n);
            pwrite(od,h,13,doff); doff+=13;
            size_t off=0;
            while (off<n){
                size_t bl = n-off < BLK ? n-off : BLK;
                unsigned char bh[3]; blk_hdr(bh,bl, off+bl>=n);
                pwrite(od,bh,3,doff); doff+=3;
                off_t si=off, di=doff; size_t left=bl;
                while (left){ ssize_t r=copy_file_range(sd,&si,od,&di,left,0); if(r<=0) break; left-=r; }
                doff += bl; off += bl;
            }
        } else {
            size_t got=0; while (got<n){ ssize_t r=pread(sd,ib+got,n-got,got); if(r<=0) break; got+=r; }
            if (a->mode==M_WV){
                unsigned char fh[13]; frame_hdr(fh,n);
                size_t nb=(n+BLK-1)/BLK; if(!nb) nb=1;
                struct iovec iov[1024]; size_t b=0, off=0; int first=1;
                while (b < nb){
                    int k=0; off_t base=doff;
                    if (first){ iov[k].iov_base=fh; iov[k].iov_len=13; k++; first=0; doff+=13; }
                    while (b < nb && k <= 1022){
                        size_t bl = n-off < BLK ? n-off : BLK;
                        blk_hdr(bhs+b*3, bl, b+1==nb);
                        iov[k].iov_base=bhs+b*3; iov[k].iov_len=3; k++;
                        iov[k].iov_base=ib+off;  iov[k].iov_len=bl; k++;
                        off+=bl; b++; doff += 3+bl;
                    }
                    pwritev_all(od, iov, k, base);
                }
            } else if (a->mode==M_RWZ){
                unsigned char *o=ob; frame_hdr(o,n); o+=13;
                for (size_t off=0; off<n; off+=BLK){
                    size_t bl = n-off < BLK ? n-off : BLK;
                    blk_hdr(o,bl, off+bl>=n); o+=3;
                    memcpy(o, ib+off, bl); o+=bl;             // the copy we want to delete
                }
                size_t fl=framed_len(n), put=0;
                while (put<fl){ ssize_t w=pwrite(od,ob+put,fl-put,doff+put); if(w<=0) break; put+=w; }
                doff += fl;
            } else {
                size_t put=0;
                while (put<n){ ssize_t w=pwrite(od,ib+put,n-put,doff+put); if(w<=0) break; put+=w; }
                doff += n;
            }
        }
        close(sd);
        a->bytes += n;
    }
    fsync(od); close(od); free(ib); free(ob); free(bhs);
    return NULL;
}

int main(int argc, char **argv){
    if (argc<2){ fprintf(stderr,"usage: %s <dir> [file_mb] [nfiles] [threads] [modes..]\n",argv[0]); return 1; }
    const char *dir=argv[1];
    size_t fsz = (size_t)(argc>2?atoi(argv[2]):64) << 20;
    int nfiles = argc>3?atoi(argv[3]):16, nthr = argc>4?atoi(argv[4]):8;
    const char *modes[8]; int nm=0;
    for (int i=5;i<argc && nm<8;i++) modes[nm++]=argv[i];
    if (!nm){ modes[nm++]="rw"; modes[nm++]="rwz"; modes[nm++]="wv";
              modes[nm++]="cfr"; modes[nm++]="cfr1"; }
    mkdir(dir,0755);
    // sources: incompressible bytes (this path only ever sees data zstd gave up on)
    unsigned char *rnd = malloc(fsz);
    unsigned long long s=88172645463325252ULL;
    for (size_t i=0;i<fsz;i+=8){ s^=s<<13; s^=s>>7; s^=s<<17; memcpy(rnd+i,&s,8<fsz-i?8:fsz-i); }
    char p[512];
    for (int f=0; f<nfiles; f++){
        snprintf(p,sizeof p,"%s/src.%d",dir,f);
        struct stat st;
        if (stat(p,&st)==0 && (size_t)st.st_size==fsz) continue;
        int fd=open(p,O_WRONLY|O_CREAT|O_TRUNC,0644);
        for (size_t o=0;o<fsz;) { ssize_t w=pwrite(fd,rnd+o,fsz-o,o); if(w<=0) break; o+=w; }
        fsync(fd); close(fd);
    }
    free(rnd);
    printf("cfr: %d files x %zu MB, %d threads, dir=%s\n", nfiles, fsz>>20, nthr, dir);
    for (int m=0;m<nm;m++){
        int mi = !strcmp(modes[m],"rw")?M_RW : !strcmp(modes[m],"rwz")?M_RWZ
               : !strcmp(modes[m],"wv")?M_WV : !strcmp(modes[m],"cfr")?M_CFR : M_CFR1;
        pthread_t th[256]; Arg a[256];
        double t0=now();
        for (int i=0;i<nthr;i++){ a[i]=(Arg){dir,mi,i,nfiles,nthr,fsz,0}; pthread_create(&th[i],NULL,run,&a[i]); }
        double tot=0;
        for (int i=0;i<nthr;i++){ pthread_join(th[i],NULL); tot+=a[i].bytes; }
        double dt=now()-t0;
        printf("  %-5s %7.2f GB/s   (%.2f GB in %.2f s)\n", modes[m], tot/dt/1e9, tot/1e9, dt);
        fflush(stdout);
    }
    for (int i=0;i<nthr;i++){ snprintf(p,sizeof p,"%s/out.%d",dir,i); unlink(p); }
    return 0;
}
