#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>

int forkexample(int x)
{
	int y = 0;
	if (fork() == 0){
		y = x*2;
		printf("Child has x = %d\n", y);
		return y;
	} else {
		printf("Parent has x = %d\n", x);
		return x;
	}
}
int main()
{
	int ret;

	ret = forkexample(1);
	ret = forkexample(ret);
	ret = forkexample(ret);
	return 0;
}

