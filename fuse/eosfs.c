/* --------------------------------------------------------------------*/
/* File: eosfs.c                                                       */
/* Author: Andreas-Joachim Peters - CERN                               */
/* --------------------------------------------------------------------*/

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/



/*
    FUSE: Filesystem in Userspace
    Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

    This program can be distributed under the terms of the GNU GPL.
    See the file COPYING.

    gcc -Wall `pkg-config fuse --cflags --libs` fusexmp.c -o fusexmp
*/

#define FUSE_USE_VERSION 26

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef linux
/* For pread()/pwrite() */
#define _XOPEN_SOURCE 500
#endif

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#include <pthread.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

#include "xrdposix.hh"

char *rdr;
static time_t eosatime; // we need to track the access time of / to use autofs


static int eosdfs_getattr(const char *path, struct stat *stbuf)
{
  int res;
  char rootpath[4096];
  if (strcmp(path,"/")) {
    eosatime = time(0);
  }
  
  rootpath[0]='\0';
  strcat(rootpath,rdr);
  strcat(rootpath,path);
  res = xrd_stat(rootpath, stbuf);
  
  if (res == 0) {
    if (S_ISREG(stbuf->st_mode)) {
      stbuf->st_mode &= 0772777;  /* remove sticky bit and suid bit */
      stbuf->st_blksize = 32768;  /* unfortunately, it is ignored, see include/fuse.h */
      return 0;
    } else if (S_ISDIR(stbuf->st_mode)) {
      stbuf->st_mode &= 0772777;  /* remove sticky bit and suid bit */
      if (!strcmp(path,"/")) {
	stbuf->st_atime = eosatime;
      }
      return 0;
    } else if (S_ISLNK(stbuf->st_mode)) {
      return 0;
    } else {
      return -EIO;
    }
  } else {
    return -errno;
  }
}

static int eosdfs_access(const char *path, int mask)
{
  // we don't call access, we have access control in every other call!
  return 0;
  /*
  int res;
  char rootpath[4096];
  eosatime = time(0);  
  rootpath[0]='\0';
  strcat(rootpath,rdr);
  strcat(rootpath,path);

  res = xrd_access(rootpath, mask);
  if (res == -1)
    return -errno;
  return 0;
  */
}

static int eosdfs_readlink(const char *path, char *buf, size_t size)
{
  
  int res;
  char rootpath[4096];
  eosatime = time(0);    
  rootpath[0]='\0';
  strcat(rootpath,rdr);
  strcat(rootpath,path);
  
  res = xrd_readlink(rootpath, buf, size - 1);
  if (res == -1)
    return -errno;
  
  return 0;
}

static int eosdfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			    off_t offset, struct fuse_file_info *fi)
{
  struct dirent *de;
  eosatime = time(0);    
  (void) offset;
  (void) fi;
  
  char rootpath[4096];
  rootpath[0]='\0';
  
  strcat(rootpath,rdr);
  strcat(rootpath,path);
  
  if (!strcmp(path,"/")) {
    filler(buf,".",NULL,0);
    filler(buf,"..",NULL,0);
  }

  while ( (de = xrd_readdir( path ) ) != NULL) {
    if (filler(buf, de->d_name, NULL, 0))
      break;
  }

  return 0;
}

static int eosdfs_mknod(const char *path, mode_t mode, dev_t rdev)
{
  int res;
  eosatime = time(0);      
  /* On Linux this could just be 'mknod(path, mode, rdev)' but this
     is more portable */
  char rootpath[4096];
  
  if (S_ISREG(mode)) {
    rootpath[0]='\0';
    strcat(rootpath,rdr);
    strcat(rootpath,path);
    
    res = xrd_open(rootpath, O_CREAT | O_EXCL | O_WRONLY, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH); 
    if (res == -1)
      return -errno;
    xrd_close(res, 0);
  }    
  return 0;
}

static int eosdfs_mkdir(const char *path, mode_t mode)
{
  int res;
  char rootpath[4096];
  eosatime = time(0);    
  /*  Posix Mkdir() fails on the current version of Xrootd, 20071101-0808p1 
      So we avoid doing that. This is fixed in CVS head version.
      
      rootpath[0]='\0';
      strcat(rootpath,rdr);
      strcat(rootpath,path);
      
      res = xrd_mkdir(rootpath, mode);
      if (res == -1)
      return -errno;
  */
  rootpath[0]='\0';
  strcat(rootpath,rdr);
  strcat(rootpath,path);
  
  res = xrd_mkdir(rootpath, mode);
  if (res == -1)
    return -errno;
  else
    return 0;
}

static int eosdfs_unlink(const char *path)
{
  int res;
  char rootpath[4096]; 
  eosatime = time(0);     
  rootpath[0]='\0';
  strcat(rootpath,rdr);
  strcat(rootpath,path);
  
  res = xrd_unlink(rootpath);
  if (res == -1 && errno != ENOENT)  /* ofs.notify rm may have already done the job on CNS */
    return -errno;

  xrd_forget_p2i( xrd_inode( path ) );

  return 0;
}

static int eosdfs_rmdir(const char* path)
{
  int res;
  char rootpath[4096];
  eosatime = time(0);    
  rootpath[0]='\0';
  strcat(rootpath,rdr);
  strcat(rootpath,path);
  
  res = xrd_rmdir(rootpath);

  if (res == -1 && errno != ENOENT)
    return -errno;

  xrd_forget_p2i( xrd_inode( path ) );

  return 0;
}

static int eosdfs_symlink(const char *from, const char *to)
{
  
  int res;
  char rootpath[4096];
  eosatime = time(0);    
  rootpath[0]='\0';
  strcat(rootpath,rdr);
  strcat(rootpath,"/");
  
  if (from[0] == '/') {
    return -EINVAL;
  }

  res = xrd_symlink(rootpath, from, to);
  if (res == -1)
    return -errno;
  
  return 0;
}

static int eosdfs_rename(const char *from, const char *to)
{
  int res;
  char from_path[4096] = "", to_path[4096] = "";
  eosatime = time(0);    
  strcat(from_path, rdr);
  strcat(from_path, from);
  
  strcat(to_path, rdr);
  strcat(to_path, to);
  if (xrd_rename(from_path, to_path) != 0)
    return -errno;
  return 0;
}

static int eosdfs_link(const char *from, const char *to)
{
  int res;
  char rootpath[4096];
  eosatime = time(0);    
  rootpath[0]='\0';
  strcat(rootpath,rdr);
  strcat(rootpath,"/");

  if (from[0] == '/') {
    return -EINVAL;
  }

  res = xrd_symlink(rootpath, from, to);
  if (res == -1)
    return -errno;

  return 0;
}

static int eosdfs_chmod(const char *path, mode_t mode)
{
  char rootpath[4096];
  eosatime = time(0);    
  rootpath[0]='\0';
  strcat(rootpath,rdr);
  strcat(rootpath,path);
  
  return xrd_chmod(rootpath, mode);
}

static int eosdfs_chown(const char *path, uid_t uid, gid_t gid)
{
  /* we forbid chown via the mounted filesystem */
  eosatime = time(0);    
  /* fake that it would work ... */
  return 0;
  return -EPERM;
}

static int eosdfs_truncate(const char *path, off_t size)
{
  int res;
  char rootpath[4096];
  eosatime = time(0);    
  rootpath[0]='\0';
  strcat(rootpath,rdr);
  strcat(rootpath,path);
  
  /* Xrootd doesn't provide truncate(), So we use open() to truncate file zero size */
  res = xrd_open(rootpath, O_WRONLY | O_TRUNC, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
  if (res == -1)
    return -errno;
  
  xrd_truncate(res, size, 0);
  xrd_close(res, 0);
  return 0;
}

static int eosdfs_utimens(const char *path, const struct timespec ts[2])
{
  
  int res;
  char rootpath[4096];
  eosatime = time(0);    
  rootpath[0]='\0';
  strcat(rootpath,rdr);
  strcat(rootpath,path);
  
  struct timespec tv[2];
  
  tv[0].tv_sec = ts[0].tv_sec;
  tv[0].tv_nsec = ts[0].tv_nsec;
  tv[1].tv_sec = ts[1].tv_sec;
  tv[1].tv_nsec = ts[1].tv_nsec;
  
  res = xrd_utimes(rootpath, tv);
  if (res == -1)
    return -errno;
  
  return 0;
}

static int eosdfs_open(const char *path, struct fuse_file_info *fi)
{
  int res;
  char rootpath[4096]="";
  strcat(rootpath,rdr);
  strcat(rootpath,path);
  eosatime = time(0);    
  res = xrd_open(rootpath, fi->flags, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
  if (res == -1)
    return -errno;
  
  fi->fh = res;
  return 0;
}

static int eosdfs_read(const char *path, char *buf, size_t size, off_t offset,
                    struct fuse_file_info *fi)
{
  int fd;
  int res;
  eosatime = time(0);    
  fd = (int) fi->fh;
  res = xrd_pread(fd, buf, size, offset, 0);
  if (res == -1) {
    if (errno == ENOSYS)
      errno = EIO;
    res = -errno;
  }
  return res;
}

static int eosdfs_write(const char *path, const char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi)
{
  int fd;
  int res;
  
  /* 
     File already existed. FUSE uses eosdfs_open() and eosdfs_truncate() to open and
     truncate a file before calling eosdfs_write() 
  */
  eosatime = time(0);    
  fd = (int) fi->fh;
  res = xrd_pwrite(fd, buf, size, offset, 0);
  if (res == -1)
    res = -errno;
  
  return res;
}

static int eosdfs_statfs(const char *path, struct statvfs *stbuf)
{
  int res;
  eosatime = time(0);   
  char rootpath[4096];
  eosatime = time(0);  
  rootpath[0]='\0';
  strcat(rootpath,rdr);
  strcat(rootpath,"/");
  
  res = xrd_statfs(rootpath, path, stbuf);
  if (res == -1)
    return -errno;
  
  return 0;
}

static int eosdfs_release(const char *path, struct fuse_file_info *fi)
{
  /* Just a stub.  This method is optional and can safely be left
     unimplemented */
  
  int fd, oflag;
  struct stat xrdfile, cnsfile;
  char rootpath[4096];
  eosatime = time(0);    
  fd = (int) fi->fh;
  xrd_close(fd, 0);
  fi->fh = 0;
  return 0;
}

static int eosdfs_fsync(const char *path, int isdatasync,
                     struct fuse_file_info *fi)
{
    /* Just a stub.  This method is optional and can safely be left
       unimplemented */
  eosatime = time(0);    
  (void) path;
  (void) isdatasync;
  (void) fi;
  return 0;
}

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int eosdfs_setxattr(const char *path, const char *name, const char *value,
			     size_t size, int flags)
{
  /*
    int res = lsetxattr(path, name, value, size, flags);
    if (res == -1)
      return -errno;
  */
  eosatime = time(0);    
  return 0;
}

static int eosdfs_getxattr(const char *path, const char *name, char *value,
			     size_t size)
{
  /*
    int res = lgetxattr(path, name, value, size);
    if (res == -1)
    return -errno;
    return res;
  */
  eosatime = time(0);    
  return 0;
}

static int eosdfs_listxattr(const char *path, char *list, size_t size)
{
  /*
    int res = llistxattr(path, list, size);
    if (res == -1)
    return -errno;
    return res;
  */
  eosatime = time(0);    
  return 0;
}

static int eosdfs_removexattr(const char *path, const char *name)
{
  /*
    int res = lremovexattr(path, name);
    if (res == -1)
    return -errno;
  */
  eosatime = time(0);    
  return 0;
}
#endif /* HAVE_SETXATTR */

static struct fuse_operations eosdfs_oper = {
  .getattr	= eosdfs_getattr,
  .access	= eosdfs_access,
  .readlink	= eosdfs_readlink,
  .readdir	= eosdfs_readdir,
  .mknod	= eosdfs_mknod,
  .mkdir	= eosdfs_mkdir,
  .symlink	= eosdfs_symlink,
  .unlink	= eosdfs_unlink,
  .rmdir	= eosdfs_rmdir,
  .rename	= eosdfs_rename,
  .link	        = eosdfs_link,
  .chmod	= eosdfs_chmod,
  .chown	= eosdfs_chown,
  .truncate	= eosdfs_truncate,
  .utimens	= eosdfs_utimens,
  .open	        = eosdfs_open,
  .read	        = eosdfs_read,
  .write	= eosdfs_write,
  .statfs	= eosdfs_statfs,
  .release	= eosdfs_release,
  .fsync	= eosdfs_fsync,
#ifdef HAVE_SETXATTR
  .setxattr	= eosdfs_setxattr,
  .getxattr	= eosdfs_getxattr,
  .listxattr	= eosdfs_listxattr,
  .removexattr  = eosdfs_removexattr,
#endif
};

void
usage() {
  fprintf(stderr,"usage: eosfs <mountpoint> [-o<fuseoptionlist] [<mgm-url>]\n");
  exit(-1);
}

int main(int argc, char *argv[])
{
  char* spos=0;
  char* epos=0;
  int i;
  eosatime=time(0);
  char rdrurl[1024];
  char path[1024];
  char* ordr=0;
  char copy[1024];

  int margc=argc;

  if (argc <2) {
    usage;
  }

  int shift = 0;

  for (i=0; i< argc; i++) {
    if (!strncmp(argv[i],"root://", 7)) {
      // this is the url where to go
      ordr = strdup(argv[i]);
      margc = argc-1;
      shift = 1;
    } else {
      if (shift) {
        argv[i-1]=argv[i];
      }
    }
  }
  
  for (i=0; i < margc; i++ ) {
    printf("%d: %s\n", i, argv[i]);
  }

  if (getenv("EOS_FUSE_MGM_URL")) {
    snprintf(rdrurl,sizeof(rdrurl)-1,"%s", getenv("EOS_FUSE_MGM_URL"));
  } else {
    if (ordr) {
      snprintf(rdrurl,sizeof(rdrurl)-1,"%s", ordr);
    } else {
      fprintf(stderr,"error: no host defined via env:EOS_FUSE_MGM_URL and no url given as mount option");
      usage();
      exit(-1);
    }
  }
  
  if (getenv("EOS_SOCKS4_HOST") && getenv("EOS_SOCKS4_PORT")) {
    fprintf(stdout,"EOS_SOCKS4_HOST=%s\n", getenv("EOS_SOCKS4_HOST"));
    fprintf(stdout,"EOS_SOCKS4_PORT=%s\n", getenv("EOS_SOCKS4_PORT"));
  }

  rdr = rdrurl;
  
  if (! rdr) {
    fprintf(stderr,"error: EOSFS_RDRURL is not defined or add root://<host>// to the options argument\n");
    exit(-1);
  }

  pid_t m_pid=fork();
  if(m_pid<0) {
    fprintf(stderr,"ERROR: Failed to fork daemon process\n");
    exit(-1);
  }
  
  // kill the parent
  if(m_pid>0) {
    sleep(1);
    exit(0);
  }
  
  umask(0);
  
  pid_t sid;
  if((sid=setsid()) < 0) {
    fprintf(stderr,"ERROR: failed to create new session (setsid())\n");
    exit(-1);
  }
  
  if ((chdir("/")) < 0) {
    /* Log any failure here */
    exit(-1);
  }
  
  close(STDIN_FILENO);
  close(STDOUT_FILENO);
  // = > don't close STDERR because we redirect that to a file!
  
  xrd_init();
  
  umask(0);
  return fuse_main(margc, argv, &eosdfs_oper, NULL);
}
