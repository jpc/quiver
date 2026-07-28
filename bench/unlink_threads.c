#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
static char **paths; static long np; static long idx; static pthread_mutex_t mu=PTHREAD_MUTEX_INITIALIZER;
static double now(){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+t.tv_nsec*1e-9; }
static void *w(void *a){
  for(;;){ long i; pthread_mutex_lock(&mu); i=idx++; pthread_mutex_unlock(&mu);
    if(i>=np) break; unlink(paths[i]); }
  return NULL;
}
int main(int argc,char**argv){          /* argv: pathfile nthreads */
  FILE*f=fopen(argv[1],"r"); int nt=atoi(argv[2]);
  long cap=1<<20; paths=malloc(cap*sizeof(char*)); char buf[8192];
  while(fgets(buf,sizeof buf,f)){ size_t l=strlen(buf); if(l&&buf[l-1]=='\n')buf[l-1]=0;
    if(np==cap){cap*=2;paths=realloc(paths,cap*sizeof(char*));} paths[np++]=strdup(buf); }
  fclose(f);
  pthread_t*th=malloc(nt*sizeof(pthread_t));
  double t=now();
  for(int i=0;i<nt;i++) pthread_create(&th[i],0,w,0);
  for(int i=0;i<nt;i++) pthread_join(th[i],0);
  double dt=now()-t;
  printf("  C threads t%-4d         : %ld in %6.2fs  %9.0f unlinks/s\n",nt,np,dt,np/dt);
  return 0;
}
