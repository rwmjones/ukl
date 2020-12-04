#include <unistd.h>
#include <sys/syscall.h>   /* For SYS_xxx definitions */
int main(){
  int i;
  while(1){
    syscall(SYS_getppid);
  }
}
