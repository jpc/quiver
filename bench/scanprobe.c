#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
static long n_ent, n_stat, n_unknown, n_dir, n_syscall;
static double now(){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+t.tv_nsec*1e-9; }
struct linux_dirent64 { ino64_t d_ino; off64_t d_off; unsigned short d_reclen; unsigned char d_type; char d_name[]; };
// 0=glibc readdir(d_type); 1=readdir+lstat-every; 2=raw getdents64 1MB buf (d_type)
static void walk(const char*path,int mode){
  if(mode==2){
    int fd=open(path,O_RDONLY|O_DIRECTORY); if(fd<0) return;
    size_t BUF=1<<20; char*buf=malloc(BUF); long nr;          /* heap per call: recursion-safe */
    char (*subs)[4096]=NULL; long nsub=0,csub=0;              /* defer recursion until after read loop */
    for(;;){
      nr=syscall(SYS_getdents64,fd,buf,BUF); n_syscall++;
      if(nr<=0) break;
      for(long bpos=0;bpos<nr;){
        struct linux_dirent64*e=(struct linux_dirent64*)(buf+bpos); bpos+=e->d_reclen;
        if(!strcmp(e->d_name,".")||!strcmp(e->d_name,"..")) continue;
        n_ent++; char fp[4096]; snprintf(fp,sizeof fp,"%s/%s",path,e->d_name);
        int isdir;
        if(e->d_type==DT_DIR) isdir=1;
        else if(e->d_type==DT_UNKNOWN){ n_unknown++; struct stat st; if(lstat(fp,&st))continue; n_stat++; isdir=S_ISDIR(st.st_mode); }
        else isdir=0;
        if(isdir){ n_dir++; if(nsub==csub){ csub=csub?csub*2:16; subs=realloc(subs,csub*4096);} strcpy(subs[nsub++],fp); }
      }
    }
    close(fd); free(buf);
    for(long i=0;i<nsub;i++) walk(subs[i],2);
    free(subs); return;
  }
  DIR*d=opendir(path); if(!d) return; struct dirent*e;
  while((e=readdir(d))){
    if(!strcmp(e->d_name,".")||!strcmp(e->d_name,"..")) continue;
    n_ent++; char fp[4096]; snprintf(fp,sizeof fp,"%s/%s",path,e->d_name);
    int isdir;
    if(mode==1){ struct stat st; if(lstat(fp,&st))continue; n_stat++; isdir=S_ISDIR(st.st_mode); }
    else { if(e->d_type==DT_UNKNOWN){ n_unknown++; struct stat st; if(lstat(fp,&st))continue; n_stat++; isdir=S_ISDIR(st.st_mode); }
           else isdir=(e->d_type==DT_DIR); }
    if(isdir){ n_dir++; walk(fp,mode); }
  }
  closedir(d);
}
int main(int argc,char**argv){
  int mode=atoi(argv[2]);
  const char*names[]={"readdir+dtype","readdir+lstat","getdents64+dtype"};
  double t=now(); walk(argv[1],mode); double dt=now()-t;
  printf("mode=%d %-17s entries=%ld dirs=%ld lstats=%ld unknown=%ld getdents_calls=%ld  %.2fs  %.0f ent/s\n",
         mode,names[mode],n_ent,n_dir,n_stat,n_unknown,n_syscall,dt,n_ent/dt);
  return 0;
}
