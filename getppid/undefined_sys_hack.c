extern int printk(const char *fmt, ...);
int __dlopen(){printk("function %s NYI\n","__dlopen"); while(1);}
int __dlclose(){printk("function %s NYI\n","__dlclose"); while(1);}
int __dlsym(){printk("function %s NYI\n","__dlsym"); while(1);}
int __dlvsym(){printk("function %s NYI\n","__dlvsym"); while(1);}
int __dlerror(){printk("function %s NYI\n","__dlerror"); while(1);}
int __dladdr(){printk("function %s NYI\n","__dladdr"); while(1);}
int __dladdr1(){printk("function %s NYI\n","__dladdr1"); while(1);}
int __dlinfo(){printk("function %s NYI\n","__dlinfo"); while(1);}
int __dlmopen(){printk("function %s NYI\n","__dlmopen"); while(1);}
int __libc_memmove(){printk("function %s NYI\n","__libc_memmove"); while(1);}
int __fstatfs64(){printk("function %s NYI\n","__fstatfs64"); while(1);}
int __libc_register_dlfcn_hook(){printk("function %s NYI\n","__libc_register_dlfcn_hook"); while(1);}
int prctl(){printk("function %s NYI\n","prctl"); while(1);}

int getsid(){printk("function %s NYI\n",""); while(1);}
int readlinkat(){printk("function %s NYI\n",""); while(1);}
int ioctl(){printk("function %s NYI\n",""); while(1);}
int sched_yield(){printk("function %s NYI\n",""); while(1);}
int sched_setparam(){printk("function %s NYI\n",""); while(1);}
int getpid(){printk("function %s NYI\n",""); while(1);}
/* int (){printk("function %s NYI\n",""); while(1);} */
