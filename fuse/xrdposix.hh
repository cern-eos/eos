/************************************************************************/
/* xrdposix.hh                                                          */
/*                                                                      */
/* Auther: Wei Yang (Stanford Linear Accelerator Center, 2007)          */
/*                                                                      */
/* C wrapper to some of the Xrootd Posix library functions              */
/* Modified: Andreas-Joachim Peters (CERN,2008) XCFS                    */
/* Modified: Andreas-Joachim Peters (CERN,2010) EOS                     */
/************************************************************************/

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/param.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <pwd.h>

#define PAGESIZE 128 * 1024  
extern char* fdbuffermap[65535];

struct dirbuf {
  char *p;
  size_t size;
};

#ifdef __cplusplus
  extern "C" {
#endif

void           xrd_socks4(const char* host, const char* port);

int            xrd_stat(const char *file_name, struct stat *buf);
    int            xrd_statfs(const char *url, const char* path, struct statvfs *stbuf);

DIR           *xrd_opendir(const char *dirname);
struct dirent *xrd_readdir(DIR *dirp);
int            xrd_closedir(DIR *dir);
int            xrd_mkdir(const char *path, mode_t mode);
int            xrd_rmdir(const char *path);

int            xrd_open(const char *pathname, int flags, mode_t mode);
int            xrd_truncate(int fildes, off_t offset);
off_t          xrd_lseek(int fildes, off_t offset, int whence);
ssize_t        xrd_read(int fd, void *buf, size_t count);
ssize_t        xrd_pread(int fildes, void *buf, size_t nbyte, off_t offset);
int            xrd_close(int fd);
ssize_t        xrd_write(int fildes, const void *buf, size_t nbyte);
ssize_t        xrd_pwrite(int fildes, const void *buf, size_t nbyte, off_t offset);
int            xrd_fsync(int fildes);
int            xrd_unlink(const char *path);
int            xrd_rename(const char *oldpath, const char *newpath);
int            xrd_chmod(const char *path, mode_t mode);    
int            xrd_symlink(const char* url, const char* oldpath, const char* newpath);
int            xrd_link(const char* url, const char* oldpath, const char* newpath);
int            xrd_readlink(const char* path, char* buf, size_t bufsize);
int            xrd_access(const char* path, int mode);

const char*    xrd_mapuser(uid_t uid);
void           xrd_store_inode(long long inode, const char* name);
const char*    xrd_get_name_for_inode(long long inode); 
void           xrd_init();
int            xrd_deleteall(const char *rdrurl, const char *path);
int            xrd_renameall(const char *rdrurl, const char *from, const char *to);
int            xrd_utimes(const char* path, struct timespec *tvp);
void           xrd_forget_inode (long long inode);

int            xrd_inodirlist(unsigned long long dirinode, const char *path);
int            xrd_inodirlist_entry(unsigned long long dirinode, int index, char** name, unsigned long long* inode);

void           xrd_inodirlist_delete(unsigned long long dirinode);
struct dirbuf* xrd_inodirlist_getbuffer(unsigned long long dirinode);
int            xrd_mknodopenfilelist_release(int fd, unsigned long long inode);
int            xrd_mknodopenfilelist_add(int fd, unsigned long long inode);
int            xrd_mknodopenfilelist_get(unsigned long long inode);

int            xrd_readopenfilelist_lease(unsigned long long inode, uid_t uid);
int            xrd_readopenfilelist_add(int fd, unsigned long long inode, uid_t uid, double readopentime);
int            xrd_readopenfilelist_get(unsigned long long inode, uid_t uid);

#ifdef __cplusplus
  }
#endif

