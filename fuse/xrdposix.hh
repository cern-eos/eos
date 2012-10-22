// ----------------------------------------------------------------------
// File: xrdposix.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

/************************************************************************/
/* xrdposix.hh                                                          */
/*                                                                      */
/* Auther: Wei Yang (Stanford Linear Accelerator Center, 2007)          */
/*                                                                      */
/* C wrapper to some of the Xrootd Posix library functions              */
/* Modified: Andreas-Joachim Peters (CERN,2008) XCFS                    */
/* Modified: Andreas-Joachim Peters (CERN,2010) EOS                     */
/************************************************************************/

#ifndef __XRD_POSIX__HH__
#define __XRD_POSIX__HH__

/*----------------------------------------------------------------------------*/
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
#include <fuse/fuse_lowlevel.h>
/*----------------------------------------------------------------------------*/

#define PAGESIZE 128 * 1024

struct dirbuf {
  char* p;
  size_t size;
};


#ifdef __cplusplus
extern "C" {
#endif

  /*****************************************************************************/
  /* be carefull - this structure was copied out of the fuse<XX>.c source code */
  /* it might change in newer fuse version                                     */
  /*****************************************************************************/
  struct fuse_ll;
  struct fuse_req {
    struct fuse_ll* f;
    uint64_t unique;
    int ctr;
    pthread_mutex_t lock;
    struct fuse_ctx ctx;
    struct fuse_chan* ch;
    int interrupted;
    union {
      struct {
        uint64_t unique;
      } i;
      struct {
        fuse_interrupt_func_t func;
        void* data;
      } ni;
    } u;
    struct fuse_req* next;
    struct fuse_req* prev;
  };

  struct fuse_ll {
    int debug;
    int allow_root;
    struct fuse_lowlevel_ops op;
    int got_init;
    void* userdata;
    uid_t owner;
    struct fuse_conn_info conn;
    struct fuse_req list;
    struct fuse_req interrupts;
    pthread_mutex_t lock;
    int got_destroy;
  };

  // ---------------------------------------------------------------------------
  // C interface functions
  // ---------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  // ******* Path translation *******
  //----------------------------------------------------------------------------

  void           xrd_lock_r_p2i();   // lock for path or inode translation (read)
  void           xrd_unlock_r_p2i(); // unlock after path or inode translation (read)
  void           xrd_lock_w_p2i();   // lock for path or inode translation (write)
  void           xrd_unlock_w_p2i(); // unlock after path or inode translation (write)

  // translate from inode to path
  const          char* xrd_path( unsigned long long inode );

  // return the basename of a file
  char*          xrd_basename( unsigned long long inode );

  // translate from path          to inode
  unsigned long  long xrd_inode( const char* path );

  // store an inode/path mapping
  void           xrd_store_p2i( unsigned long long inode, const char* path );

  // store an inode/path mapping starting from the parent
  // inode + child inode + child base name
  void           xrd_store_child_p2i( unsigned long long inode,
                                      unsigned long long childinode,
                                      const char* name );

  // forget an inode/path mapping by inode
  void           xrd_forget_p2i( unsigned long long inode );

  //----------------------------------------------------------------------------
  // IO buffers
  //----------------------------------------------------------------------------

  // guarantee a buffer for reading of at least 'size' for the specified fd
  char*          xrd_attach_read_buffer( int fd, size_t  size );

  // release a read buffer for the specified fd
  void           xrd_release_read_buffer( int fd );

  //----------------------------------------------------------------------------
  // ******* DIR Listrings *******
  //----------------------------------------------------------------------------

  void           xrd_lock_r_dirview();   // lock dirview (read)
  void           xrd_unlock_r_dirview(); // unlock dirview (read)
  void           xrd_lock_w_dirview();   // lock dirview (write)
  void           xrd_unlock_w_dirview(); // unlock dirview (write)

  // path should be attached beforehand into path translation
  void           xrd_dirview_create( unsigned long long inode );
  void           xrd_dirview_delete( unsigned long long inode );

  // returns entry with index 'index'
  unsigned long long xrd_dirview_entry( unsigned long long dirinode,
                                        size_t             index,
                                        int                get_lock );

  // returns a buffer for a directory inode
  struct dirbuf* xrd_dirview_getbuffer( unsigned long long dirinode,
                                        int                get_lock );

  //----------------------------------------------------------------------------
  // ******* POSIX opened files *******
  //----------------------------------------------------------------------------

  // add fd as an open file descriptor to speed-up mknod
  void           xrd_add_open_fd( int fd, unsigned long long inode, uid_t uid );

  // return posix fd for inode
  int            xrd_get_open_fd( unsigned long long inode, uid_t uid );

  // release an attached file descriptor
  void           xrd_lease_open_fd( unsigned long long inode, uid_t uid );



  //----------------------------------------------------------------------------
  // ******* FUSE Directory Cache *******
  //----------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  //!
  //! Get a cached directory
  //!
  //! @param inode inode value of the directory to be cached
  //! @param mtime modification time
  //! @param fullpath full path of the directory
  //! @param b dirbuf structure
  //!
  //! @return true if found, otherwise false
  //!
  //----------------------------------------------------------------------------
  int xrd_dir_cache_get( unsigned long long inode,
                         struct timespec    mtime,
                         struct dirbuf**    b );


  //----------------------------------------------------------------------------
  //! Get a subentry from a cached directory
  //!
  //! @param req
  //! @param inode directory inode
  //! @param einode entry inode
  //! @param efullpath full path of the subentry
  //!
  //! @return true if entry found, otherwise false
  //!
  //----------------------------------------------------------------------------
  int xrd_dir_cache_get_entry( fuse_req_t          req,
                               unsigned long long  inode,
                               unsigned long long  einode,
                               const char*         ifullpath );


  //----------------------------------------------------------------------------
  //!
  //!Add new subentry to a cached directory
  //!
  //! @param inode directory inode
  //! @param entry_inode subentry inode
  //! @param e fuse_entry_param structure
  //!
  //----------------------------------------------------------------------------
  void xrd_dir_cache_add_entry( unsigned long long       inode,
                                unsigned long long       entry_inode,
                                struct fuse_entry_param* e );


  //----------------------------------------------------------------------------
  //! Add or update a cache directory entry
  //!
  //! @param inode directory inode value
  //! @param fullpath directory full path
  //! @param number of entries in the current directory
  //! @param mtime modifcation time
  //! @param b dirbuf structure
  //!
  //----------------------------------------------------------------------------
  void xrd_dir_cache_sync( unsigned long long inode,
                           int                nentries,
                           struct timespec    mtime,
                           struct dirbuf*     b );



  //----------------------------------------------------------------------------
  // ******* XROOT interfacing ********
  //----------------------------------------------------------------------------
  int            xrd_stat( const char* file_name, struct stat* buf );

  int            xrd_statfs( const char*     url,
                             const char*     path,
                             struct statvfs* stbuf );

  int            xrd_getxattr( const char* path,
                               const char* xattr_name,
                               char**      xattr_value,
                               size_t*     size );

  int            xrd_listxattr( const char* path,
                                char**      xattr_list,
                                size_t*     size );

  int            xrd_setxattr( const char* path,
                               const char* xattr_name,
                               const char* xattr_value,
                               size_t      size );

  int            xrd_rmxattr( const char* path, const char* xattr_name );

  struct dirent* xrd_readdir( const char* path_dir );

  int            xrd_mkdir( const char* path, mode_t mode );
  int            xrd_rmdir( const char* path );

  int            xrd_open( const char* pathname, int flags, mode_t mode );

  int            xrd_truncate( int fildes, off_t offset, unsigned long inode );

  ssize_t        xrd_read( int fd, void* buf, size_t count, unsigned long inode );

  ssize_t        xrd_pread( int           fildes,
                            void*         buf,
                            size_t        nbyte,
                            off_t         offset,
                            unsigned long inode );

  int            xrd_close( int fd, unsigned long inode );

  ssize_t        xrd_write( int           fildes,
                            const void*   buf,
                            size_t        nbyte,
                            unsigned long inode );

  ssize_t        xrd_pwrite( int           fildes,
                             const void*   buf,
                             size_t        nbyte,
                             off_t         offset,
                             unsigned long inode );

  int            xrd_fsync( int fildes, unsigned long inode );

  int            xrd_unlink( const char* path );
  int            xrd_rename( const char* oldpath, const char* newpath );
  int            xrd_chmod( const char* path, mode_t mode );

  int            xrd_symlink( const char* url,
                              const char* oldpath,
                              const char* newpath );

  int            xrd_link( const char* url,
                           const char* oldpath,
                           const char* newpath );
  int            xrd_readlink( const char* path, char* buf, size_t bufsize );
  int            xrd_access( const char* path, int mode );
  int            xrd_utimes( const char* path, struct timespec* tvp );
  int            xrd_inodirlist( unsigned long long dirinode, const char* path );

  // - USER mapping
  const char*    xrd_mapuser( uid_t uid );

  // - INITIALITZATION
  void           xrd_init();


#ifdef __cplusplus
}
#endif

#endif // __XRD_POSIX_HH__
