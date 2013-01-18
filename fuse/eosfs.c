//------------------------------------------------------------------------------
//! @file: eosfs.c
//! @author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

//------------------------------------------------------------------------------
/*
    FUSE: Filesystem in Userspace
    Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

    This program can be distributed under the terms of the GNU GPL.
    See the file COPYING.

    gcc -Wall `pkg-config fuse --cflags --libs` fusexmp.c -o fusexmp
*/
//------------------------------------------------------------------------------

#define FUSE_USE_VERSION 26

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef linux
/* For pread()/pwrite() */
#define _XOPEN_SOURCE 500
#endif

/*----------------------------------------------------------------------------*/
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
/*----------------------------------------------------------------------------*/
#include "xrdposix.hh"
/*----------------------------------------------------------------------------*/


//! Mount hostport;
char mounthostport[1024];

//! Mount prefix
char mountprefix[1024];

//! We need to track the access time of/to use autofs
static time_t eosatime; 


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_getattr( const char* path, struct stat* stbuf )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  int res;
  char rootpath[4096];

  if ( strcmp( path, "/" ) ) {
    eosatime = time( 0 );
  }

  rootpath[0] = '\0';
  strcat( rootpath, mountprefix );
  strcat( rootpath, path );
  res = xrd_stat( rootpath, stbuf );

  if ( res == 0 ) {
    if ( S_ISREG( stbuf->st_mode ) ) {
      stbuf->st_mode &= 0772777;  // remove sticky bit and suid bit
      stbuf->st_blksize = 32768;  // unfortunately, it is ignored, see include/fuse.h
      fprintf( stderr, "[%s] Return 0 for file. \n", __FUNCTION__ );
      return 0;
    } else if ( S_ISDIR( stbuf->st_mode ) ) {
      stbuf->st_mode &= 0772777;  // remove sticky bit and suid bit

      if ( !strcmp( path, "/" ) ) {
        stbuf->st_atime = eosatime;
      }

      fprintf( stderr, "[%s] Return 0 for directory. \n", __FUNCTION__ );
      return 0;
    } else if ( S_ISLNK( stbuf->st_mode ) ) {
      return 0;
    } else {
      return -EIO;
    }
  } else {
    return -errno;
  }
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_access( const char* path, int mask )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  // we don't call access, we have access control in every other call!
  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_readlink( const char* path, char* buf, size_t size )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  int res;
  char rootpath[4096];
  eosatime = time( 0 );
  rootpath[0] = '\0';
  strcat( rootpath, mountprefix );
  strcat( rootpath, path );
  res = xrd_readlink( rootpath, buf, size - 1 );

  if ( res == -1 )
    return -errno;

  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_readdir( const char*            path,
                           void*                  buf,
                           fuse_fill_dir_t        filler,
                           off_t                  offset,
                           struct fuse_file_info* fi )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  size_t size = -1;
  struct dirent* de;
  eosatime = time( 0 );
  ( void ) offset;
  ( void ) fi;
  char rootpath[4096];
  rootpath[0] = '\0';
  strcat( rootpath, mountprefix );
  strcat( rootpath, path );
  
  if ( !strcmp( path, "/" ) ) {
    filler( buf, ".", NULL, 0 );
    filler( buf, "..", NULL, 0 );
  }

  de = xrd_readdir( rootpath, &size );
  fprintf( stderr, "[%s] The size is: %zu. \n", __FUNCTION__, size );

  if ( size ) {
    size_t i = 0;
    while ( i < size ) {
      fprintf( stderr, "[%s] Name: %s. \n", __FUNCTION__, de[i].d_name );
      if ( filler( buf, de[i].d_name, NULL, 0 ) ) {
        break;
      }
      i++;
    }
  }

  //............................................................................
  // Free memory allocated in xrd_readdir
  //............................................................................
  free( de );

  fprintf( stderr, "[%s] Finish. \n", __FUNCTION__ );
  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_mknod( const char* path, mode_t mode, dev_t rdev )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  int res;
  eosatime = time( 0 );
  //............................................................................
  // On Linux this could just be 'mknod(path, mode, rdev)' but this
  // is more portable
  //............................................................................
  char rootpath[4096];

  if ( S_ISREG( mode ) ) {
    rootpath[0] = '\0';
    sprintf( rootpath, "root://%s%s%s", mounthostport, mountprefix, path);
    res = xrd_open( rootpath,
                    O_CREAT | O_EXCL | O_WRONLY,
                    S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH );

    if ( res == -1 )
      return -errno;

    xrd_close( res, 0 );
    xrd_remove_fd2file( res );     
  }

  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_mkdir( const char* path, mode_t mode )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  int res;
  char rootpath[4096];
  eosatime = time( 0 );
  rootpath[0] = '\0';
  strcat( rootpath, mountprefix );
  strcat( rootpath, path );
  res = xrd_mkdir( rootpath, mode );

  if ( res == -1 )
    return -errno;
  else
    return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_unlink( const char* path )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  int res;
  char rootpath[4096];
  eosatime = time( 0 );
  rootpath[0] = '\0';
  strcat( rootpath, mountprefix );
  strcat( rootpath, path );
  res = xrd_unlink( rootpath );

  if ( res == -1 && errno != ENOENT ) {
    //..........................................................................
    // ofs.notify rm may have already done the job on CNS
    //..........................................................................
    return -errno;
  }

  xrd_forget_p2i( xrd_inode( path ) );
  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_rmdir( const char* path )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  int res;
  char rootpath[4096];
  eosatime = time( 0 );
  rootpath[0] = '\0';
  strcat( rootpath, mountprefix );
  strcat( rootpath, path );
  res = xrd_rmdir( rootpath );

  if ( res == -1 && errno != ENOENT )
    return -errno;

  xrd_forget_p2i( xrd_inode( path ) );
  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_symlink( const char* from, const char* to )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, from );
  int res;
  char rootpath[4096];
  eosatime = time( 0 );
  rootpath[0] = '\0';
  strcat( rootpath, mountprefix );
  strcat( rootpath, "/" );

  if ( from[0] == '/' ) {
    return -EINVAL;
  }

  res = xrd_symlink( rootpath, from, to );

  if ( res == -1 )
    return -errno;

  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_rename( const char* from, const char* to )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, from );
  int res;
  char from_path[4096] = "", to_path[4096] = "";
  eosatime = time( 0 );
  strcat( from_path, mountprefix );
  strcat( from_path, from );
  strcat( to_path, mountprefix );
  strcat( to_path, to );

  if ( xrd_rename( from_path, to_path ) != 0 )
    return -errno;

  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_link( const char* from, const char* to )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, from );
  int res;
  char rootpath[4096];
  eosatime = time( 0 );
  rootpath[0] = '\0';
  strcat( rootpath, mountprefix );
  strcat( rootpath, "/" );

  if ( from[0] == '/' ) {
    return -EINVAL;
  }

  res = xrd_symlink( rootpath, from, to );

  if ( res == -1 )
    return -errno;

  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_chmod( const char* path, mode_t mode )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  char rootpath[4096];
  eosatime = time( 0 );
  rootpath[0] = '\0';
  strcat( rootpath, mountprefix );
  strcat( rootpath, path );
  return xrd_chmod( rootpath, mode );
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_chown( const char* path, uid_t uid, gid_t gid )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  //............................................................................
  // We forbid chown via the mounted filesystem */
  //............................................................................
  eosatime = time( 0 );
  //............................................................................
  // Fake that it would work ...
  //............................................................................
  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_truncate( const char* path, off_t size )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  int res;
  char rootpath[4096];
  eosatime = time( 0 );
  rootpath[0] = '\0';
  strcat( rootpath, mountprefix );
  strcat( rootpath, path );

  //............................................................................
  // Xrootd doesn't provide truncate(), So we use open() to truncate file to 0
  //............................................................................
  res = xrd_open( rootpath,
                  O_WRONLY | O_TRUNC,
                  S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH );

  if ( res == -1 )
    return -errno;

  xrd_truncate( res, size, 0 );
  xrd_close( res, 0 );
  xrd_remove_fd2file( res ); 
  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_utimens( const char* path, const struct timespec ts[2] )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  int res;
  char rootpath[4096];
  eosatime = time( 0 );
  rootpath[0] = '\0';
  strcat( rootpath, mountprefix );
  strcat( rootpath, path );
  struct timespec tv[2];
  tv[0].tv_sec = ts[0].tv_sec;
  tv[0].tv_nsec = ts[0].tv_nsec;
  tv[1].tv_sec = ts[1].tv_sec;
  tv[1].tv_nsec = ts[1].tv_nsec;
  res = xrd_utimes( rootpath, tv );

  if ( res == -1 )
    return -errno;

  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_open( const char* path, struct fuse_file_info* fi )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  int res;
  char rootpath[4096] = "";
  sprintf( rootpath, "root://%s%s%s", mounthostport, mountprefix, path);
  eosatime = time( 0 );
  res = xrd_open( rootpath, fi->flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH );

  if ( res == -1 )
    return -errno;

  //............................................................................
  // This memory  has to be freed once we're done with the file, usually in
  // the close/release method
  //............................................................................
  fd_user_info* info =
    (struct fd_user_info*) calloc( 1, sizeof( struct fd_user_info ) );
  info->fd = res;
  info->uid =  0;
  fi->fh = (uint64_t) info;
  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_read( const char*            path,
                        char*                  buf,
                        size_t                 size,
                        off_t                  offset,
                        struct fuse_file_info* fi )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  int fd;
  int res;
  eosatime = time( 0 );
  struct fd_user_info* info = (fd_user_info*) fi->fh;
  fd = ( int ) info->fd;
  res = xrd_pread( fd, buf, size, offset, 0 );

  if ( res == -1 ) {
    if ( errno == ENOSYS )
      errno = EIO;

    res = -errno;
  }

  return res;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_write( const char*            path,
                         const char*            buf,
                         size_t                 size,
                         off_t                  offset,
                         struct fuse_file_info* fi )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  int fd;
  int res;

  //............................................................................
  // File already existed. FUSE uses eosdfs_open() and eosdfs_truncate()
  // to open and truncate a file before calling eosdfs_write()
  //............................................................................
  eosatime = time( 0 );
  struct fd_user_info* info = (fd_user_info*) fi->fh;
  fd = ( int ) info->fd;
  res = xrd_pwrite( fd, buf, size, offset, 0 );

  if ( res == -1 )
    res = -errno;

  return res;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_statfs( const char* path, struct statvfs* stbuf )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  int res;
  eosatime = time( 0 );
  char rootpath[4096];
  eosatime = time( 0 );
  rootpath[0] = '\0';
  strcat( rootpath, mountprefix );
  strcat( rootpath, "/" );
  res = xrd_statfs( rootpath, path, stbuf );

  if ( res == -1 )
    return -errno;

  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_release( const char* path, struct fuse_file_info* fi )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  int fd, oflag;
  struct stat xrdfile, cnsfile;
  char rootpath[4096];
  eosatime = time( 0 );
  struct fd_user_info* info = (struct fd_user_info*) fi->fh;
  fd = ( int ) info->fd;

  xrd_close( fd, 0 );
  xrd_release_read_buffer( fd );
  xrd_remove_fd2file( fd ); 

  //............................................................................
  // Free memory allocated in eosdfs_open
  //............................................................................
  free( info );
  fi->fh = 0;
  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_fsync( const char*            path,
                         int                    isdatasync,
                         struct fuse_file_info* fi )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  //............................................................................
  // This method is optional and can safely be left unimplemented
  //............................................................................
  eosatime = time( 0 );
  ( void ) path;
  ( void ) isdatasync;
  ( void ) fi;
  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
#ifdef HAVE_SETXATTR
//..............................................................................
// Xattr operations are optional and can safely be left unimplemented
//..............................................................................
static int eosdfs_setxattr( const char* path, const char* name, const char* value,
                            size_t size, int flags )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  /*
    int res = lsetxattr(path, name, value, size, flags);
    if (res == -1)
      return -errno;
  */
  eosatime = time( 0 );
  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_getxattr( const char* path, const char* name, char* value,
                            size_t size )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  /*
    int res = lgetxattr(path, name, value, size);
    if (res == -1)
    return -errno;
    return res;
  */
  eosatime = time( 0 );
  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_listxattr( const char* path, char* list, size_t size )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  /*
    int res = llistxattr(path, list, size);
    if (res == -1)
    return -errno;
    return res;
  */
  eosatime = time( 0 );
  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
static int eosdfs_removexattr( const char* path, const char* name )
{
  fprintf( stderr, "[%s] path = %s.\n", __FUNCTION__, path );
  /*
    int res = lremovexattr(path, name);
    if (res == -1)
    return -errno;
  */
  eosatime = time( 0 );
  return 0;
}
#endif /* HAVE_SETXATTR */

static struct fuse_operations eosdfs_oper = {
  .getattr     = eosdfs_getattr,
  .access      = eosdfs_access,
  .readlink    = eosdfs_readlink,
  .readdir     = eosdfs_readdir,
  .mknod       = eosdfs_mknod,
  .mkdir       = eosdfs_mkdir,
  .symlink     = eosdfs_symlink,
  .unlink      = eosdfs_unlink,
  .rmdir       = eosdfs_rmdir,
  .rename      = eosdfs_rename,
  .link        = eosdfs_link,
  .chmod       = eosdfs_chmod,
  .chown       = eosdfs_chown,
  .truncate    = eosdfs_truncate,
  .utimens     = eosdfs_utimens,
  .open        = eosdfs_open,
  .read        = eosdfs_read,
  .write       = eosdfs_write,
  .statfs      = eosdfs_statfs,
  .release     = eosdfs_release,
  .fsync       = eosdfs_fsync,
#ifdef HAVE_SETXATTR
  .setxattr    = eosdfs_setxattr,
  .getxattr    = eosdfs_getxattr,
  .listxattr   = eosdfs_listxattr,
  .removexattr = eosdfs_removexattr,
#endif
};


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
usage()
{
  fprintf( stderr, "usage: eosfs <mountpoint> [-o<fuseoptionlist] [<mgm-url>]\n" );
  exit( -1 );
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int main( int argc, char* argv[] )
{
  char* spos = 0;
  char* epos = 0;
  int i;
  eosatime = time( 0 );
  char rdrurl[1024];
  char path[1024];
  char* ordr = 0;
  char copy[1024];
  int margc = argc;

  if ( argc < 2 ) {
    usage();
  }

  int shift = 0;

  for ( i = 0; i < argc; i++ ) {
    if ( !strncmp( argv[i], "root://", 7 ) ) {
      //........................................................................
      // This is the url where to go
      //........................................................................
      ordr = strdup( argv[i] );
      margc = argc - 1;
      shift = 1;
    } else {
      if ( shift ) {
        argv[i - 1] = argv[i];
      }
    }
  }

  //  for ( i = 0; i < margc; i++ ) {
  //    printf( "%d: %s\n", i, argv[i] );
  //  }

  if ( getenv( "EOS_FUSE_MGM_URL" ) ) {
    snprintf( rdrurl, sizeof( rdrurl ) - 1, "%s", getenv( "EOS_FUSE_MGM_URL" ) );
  } else {
    if ( ordr ) {
      snprintf( rdrurl, sizeof( rdrurl ) - 1, "%s", ordr );
    } else {
      fprintf( stderr, "error: no host defined via env:EOS_FUSE_MGM_URL "
               "and no url given as mount option" );
      usage();
      exit( -1 );
    }
  }

  if ( getenv( "EOS_SOCKS4_HOST" ) && getenv( "EOS_SOCKS4_PORT" ) ) {
    fprintf( stdout, "EOS_SOCKS4_HOST=%s\n", getenv( "EOS_SOCKS4_HOST" ) );
    fprintf( stdout, "EOS_SOCKS4_PORT=%s\n", getenv( "EOS_SOCKS4_PORT" ) );
  }

  //............................................................................
  // Parse input into mount host and port and mount prefix
  //............................................................................
  char* pmounthostport = 0;
  char* smountprefix = 0;

  pmounthostport = strstr( rdrurl, "root://" );

  if ( !pmounthostport ) {
    fprintf( stderr, "error: EOS_RDRURL or url option is not valid\n" );
    exit( -1 );
  }

  pmounthostport += 7;
  strcpy( mounthostport, pmounthostport );

  if ( !( smountprefix = strstr( mounthostport, "//" ) ) ) {
    fprintf( stderr, "error: EOS_RDRURL or url option is not valid\n" );
    exit( -1 );
  } else {
    smountprefix++;
    strcpy( mountprefix, smountprefix );
    *smountprefix = 0;

    if ( mountprefix[strlen( mountprefix ) - 1] == '/' ) {
      mountprefix[strlen( mountprefix ) - 1] = 0;
    }

    if ( mountprefix[strlen( mountprefix ) - 1] == '/' ) {
      mountprefix[strlen( mountprefix ) - 1] = 0;
    }
  }

  pid_t m_pid = fork();

  if ( m_pid < 0 ) {
    fprintf( stderr, "ERROR: Failed to fork daemon process\n" );
    exit( -1 );
  }

  //............................................................................
  // Kill the parent
  //............................................................................
  if ( m_pid > 0 ) {
    sleep( 1 );
    exit( 0 );
  }

  umask( 0 );
  pid_t sid;

  if ( ( sid = setsid() ) < 0 ) {
    fprintf( stderr, "ERROR: failed to create new session (setsid())\n" );
    exit( -1 );
  }

  if ( ( chdir( "/" ) ) < 0 ) {
    //..........................................................................
    // Log any failure here
    //..........................................................................
    exit( -1 );
  }

  close( STDIN_FILENO );
  close( STDOUT_FILENO );

  //............................................................................
  // = > don't close STDERR because we redirect that to a file!
  //............................................................................
  xrd_init();
  umask( 0 );

  //  fprintf ( stderr, "Call fuse_main with the following parameters: \n" );
  //  for ( i = 0; i < margc; i++ ) {
  //    fprintf( stderr, "argv[%i] = %s \n", i, argv[i] );
  //  }
  
  return fuse_main( margc, argv, &eosdfs_oper, NULL );
}



