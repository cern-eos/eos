//////////////////////////////////////////////////////////////////////////////////
// ALL POSIX
//////////////////////////////////////////////////////////////////////////////////
#include <sys/types.h>
#include <dirent.h>
#include <stdio.h>
//////////////////////////////////////////////////////////////////////////////////
extern "C"
{
int access(const char *path, int amode);
int chdir(const char *path);
int close(int fildes);
int closedir(DIR *dirp);
int creat64(const char *path, mode_t mode);
int fclose(FILE *stream);
int fcntl64(int fd, int cmd, ...);
#if !defined(__APPLE__)
int fdatasync(int fildes);
#endif
int fflush(FILE *stream);
FILE*
    fopen64(const char *path, const char *mode);
size_t
    fread(void *ptr, size_t size, size_t nitems, FILE *stream);
int fseek(FILE *stream, long offset, int whence);
int fseeko64(FILE *stream, off64_t offset, int whence);
#if defined(__linux__) and defined(_STAT_VER) and __GNUC__ and __GNUC__ >= 2
int __fxstat64(int ver, int fildes, struct stat64 *buf);
#else
int fstat64(         int fildes, struct stat64 *buf);
#endif
int fsync(int fildes);
long
    ftell(FILE *stream);
off64_t
    ftello64(FILE *stream);
int ftruncate64(int fildes, off_t offset);
size_t
    fwrite(const void *ptr, size_t size, size_t nitems, FILE *stream);
ssize_t
    fgetxattr (int fd, const char *name, void *value, size_t size);
ssize_t
    getxattr (const char *path, const char *name, void *value, size_t size);
ssize_t
    lgetxattr (const char *path, const char *name, void *value, size_t size);
off64_t
    lseek64(int fildes, off64_t offset, int whence);
off_t
    llseek(int fildes, off_t    offset, int whence);
int __lxstat64(int ver, const char *path, struct stat64 *buf);
int lstat64(         const char *path, struct stat64 *buf);
int mkdir(const char *path, mode_t mode);
int open64(const char *path, int oflag, ...);
DIR*
    opendir(const char *path);
ssize_t
    pread64(int fildes, void *buf, size_t nbyte, off_t offset);
ssize_t
    pwrite64(int fildes, const void *buf, size_t nbyte, off_t offset);
ssize_t
    read(int fildes, void *buf, size_t nbyte);
ssize_t
    readv(int fildes, const struct iovec *iov, int iovcnt);
struct dirent64*
    readdir64(DIR *dirp);
int readdir64_r(DIR *dirp, struct dirent64 *entry, struct dirent64 **result);
int rename(const char *oldpath, const char *newpath);
void
    rewinddir(DIR *dirp);
int rmdir(const char *path);
void
     seekdir(DIR *dirp, long loc);
int  __xstat64(int ver, const char *path, struct stat64 *buf);
int  stat64(         const char *path, struct stat64 *buf);
int  statfs64(       const char *path, struct statfs64 *buf);
int  statvfs64(         const char *path, struct statvfs64 *buf);
long telldir(DIR *dirp);
int  truncate64(const char *path, off_t offset);
int  unlink(const char *path);
ssize_t
     write(int fildes, const void *buf, size_t nbyte);
ssize_t
     writev(int fildes, const struct iovec *iov, int iovcnt);
}
