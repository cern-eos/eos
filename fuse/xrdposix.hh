// ----------------------------------------------------------------------
//! @file xrdposix.hh
//! @author Elvin-Alin Sindrilaru / Andreas-Joachim Peters - CERN
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
#ifndef __APPLE__
#include <sys/vfs.h>
#endif
#include <unistd.h>
#include <pwd.h>

#ifdef __APPLE__
#define FUSE_USE_VERSION 27
#else
#define FUSE_USE_VERSION 26
#endif

/*----------------------------------------------------------------------------*/
#include <fuse/fuse_lowlevel.h>
/*----------------------------------------------------------------------------*/

#define PAGESIZE 128 * 1024


//! Dirbuf structure used to save the list of subentries in a directory
struct dirbuf {
  char* p;
  size_t size;
};


#ifdef __cplusplus
extern "C" {
#endif

  typedef struct fd_user_info {
    unsigned long long fd;
    uid_t uid;
  } fd_user_info;
  
 
  /*****************************************************************************/
  /* be carefull - this structure was copied out of the fuse<XX>.c source code */
  /* it might change in newer fuse version                                     */
  /*****************************************************************************/
  struct fuse_ll;

  //! Structure copied from fuse<XX>.cc - it might change in the future
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

  //! Structure copied from fuse<XX>.cc - it might change in the future
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
  //                ******* C interface functions *******
  // ---------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  //                ******* Path translation *******
  //----------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  //! Lock for path or inode translation (read)
  //----------------------------------------------------------------------------
  void xrd_lock_r_p2i();

  //----------------------------------------------------------------------------
  //! Unlock after path or inode translation (read)
  //----------------------------------------------------------------------------
  void xrd_unlock_r_p2i();

  //----------------------------------------------------------------------------
  //! Lock for path or inode translation (write)
  //----------------------------------------------------------------------------
  void xrd_lock_w_p2i();

  //----------------------------------------------------------------------------
  //! Unlock after path or inode translation (write)
  //----------------------------------------------------------------------------
  void xrd_unlock_w_p2i();

  //----------------------------------------------------------------------------
  //! Translate from inode to path
  //----------------------------------------------------------------------------
  const char* xrd_path( unsigned long long inode );

  //----------------------------------------------------------------------------
  //! Return the basename of a file
  //----------------------------------------------------------------------------
  char* xrd_basename( unsigned long long inode );

  //----------------------------------------------------------------------------
  //! Translate from path to inode
  //----------------------------------------------------------------------------
  unsigned long long xrd_inode( const char* path );

  //----------------------------------------------------------------------------
  //! Store an inode/path mapping
  //----------------------------------------------------------------------------
  void  xrd_store_p2i( unsigned long long inode, const char* path );

  //----------------------------------------------------------------------------
  //! Store an inode/path mapping starting from the parent:
  //! inode + child inode + child base name
  //----------------------------------------------------------------------------
  void xrd_store_child_p2i( unsigned long long inode,
                            unsigned long long childinode,
                            const char* name );

  //----------------------------------------------------------------------------
  //! Forget an inode/path mapping by inode
  //----------------------------------------------------------------------------
  void xrd_forget_p2i( unsigned long long inode );



  //----------------------------------------------------------------------------
  //                ******* IO buffers *******
  //----------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  //! Guarantee a buffer for reading of at least 'size' for the specified fd
  //----------------------------------------------------------------------------
  char* xrd_attach_read_buffer( int fd, size_t  size );

  //----------------------------------------------------------------------------
  //! Release a read buffer for the specified fd
  //----------------------------------------------------------------------------
  void xrd_release_read_buffer( int fd );



  //----------------------------------------------------------------------------
  //                ******* DIR Listrings *******
  //----------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  //! Lock dirview (read)
  //----------------------------------------------------------------------------
  void xrd_lock_r_dirview();

  //----------------------------------------------------------------------------
  //! Unlock dirview (read)
  //----------------------------------------------------------------------------
  void xrd_unlock_r_dirview();

  //----------------------------------------------------------------------------
  //! Lock dirview (write)
  //----------------------------------------------------------------------------
  void xrd_lock_w_dirview();

  //----------------------------------------------------------------------------
  //! Unlock dirview (write)
  //----------------------------------------------------------------------------
  void xrd_unlock_w_dirview();

  //----------------------------------------------------------------------------
  //! Create a new directory listing. Path should be attached beforehand into
  //! path translation.
  //----------------------------------------------------------------------------
  void xrd_dirview_create( unsigned long long inode );

  //----------------------------------------------------------------------------
  //! Delete a directory listing. Path should be attached beforehand into
  //! path translation.
  //----------------------------------------------------------------------------
  void xrd_dirview_delete( unsigned long long inode );

  //----------------------------------------------------------------------------
  //! Returns subentry with index 'index' from the directory
  //!
  //! @param dirinode directory inode
  //! @param index index of entry
  //! @param get_lock if true, user does not take care of locking
  //!
  //! @return inode of the subentry
  //!
  //----------------------------------------------------------------------------
  unsigned long long xrd_dirview_entry( unsigned long long dirinode,
                                        size_t             index,
                                        int                get_lock );

  //----------------------------------------------------------------------------
  //! Returns a buffer for a directory inode
  //----------------------------------------------------------------------------
  struct dirbuf* xrd_dirview_getbuffer( unsigned long long dirinode,
                                        int                get_lock );



  //----------------------------------------------------------------------------
  //              ******* POSIX opened file descriptors *******
  //----------------------------------------------------------------------------


  //----------------------------------------------------------------------------
  //! Create an artificial file descriptor
  //----------------------------------------------------------------------------
  int xrd_generate_fd();

  //----------------------------------------------------------------------------
  //! Return the fd value back to the pool
  //----------------------------------------------------------------------------
  void xrd_release_fd( int fd );

  //----------------------------------------------------------------------------
  //! Remove file descriptor from mapping
  //----------------------------------------------------------------------------
  void xrd_remove_fd2file( int fd );

  //----------------------------------------------------------------------------
  //! Release a file descriptor held by a user ona file
  //----------------------------------------------------------------------------
  void xrd_release_open_fd( unsigned long long inode, uid_t uid );

  //----------------------------------------------------------------------------
  //! Add fd as an open file descriptor to speed-up mknod
  //----------------------------------------------------------------------------
  void xrd_add_open_fd( int fd, unsigned long long inode, uid_t uid );

  //----------------------------------------------------------------------------
  //! Return posix fd for inode
  //----------------------------------------------------------------------------
  unsigned long long xrd_get_open_fd( unsigned long long inode, uid_t uid );



  //----------------------------------------------------------------------------
  //              ******* FUSE Directory Cache *******
  //----------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  //!
  //! Get a cached directory
  //!
  //! @param inode inode value of the directory to be cached
  //! @param mtime modification time
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
  //! @param ifullpath full path of the subentry
  //!
  //! @return true if entry found, otherwise false
  //!
  //----------------------------------------------------------------------------
  int xrd_dir_cache_get_entry( fuse_req_t          req,
                               unsigned long long  inode,
                               unsigned long long  einode,
                               const char*         ifullpath );


  //----------------------------------------------------------------------------
  //! Add new subentry to a cached directory
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
  //! @param nentries number of entries in the current directory
  //! @param mtime modifcation time
  //! @param b dirbuf structure
  //!
  //----------------------------------------------------------------------------
  void xrd_dir_cache_sync( unsigned long long inode,
                           int                nentries,
                           struct timespec    mtime,
                           struct dirbuf*     b );



  //----------------------------------------------------------------------------
  //              ******* XROOT interfacing ********
  //----------------------------------------------------------------------------


  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_stat( const char* file_name, struct stat* buf );

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int  xrd_statfs( const char*     url,
                   const char*     path,
                   struct statvfs* stbuf );
  
  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_getxattr( const char* path,
                    const char* xattr_name,
                    char**      xattr_value,
                    size_t*     size );

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_listxattr( const char* path,
                     char**      xattr_list,
                     size_t*     size );
  
  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_setxattr( const char* path,
                    const char* xattr_name,
                    const char* xattr_value,
                    size_t      size );

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_rmxattr( const char* path, const char* xattr_name );

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  struct dirent* xrd_readdir( const char* path_dir, size_t *size );

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_mkdir( const char* path, mode_t mode );

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_rmdir( const char* path );

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_open( const char* pathname, int flags, mode_t mode );

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_open_retc_map( int retc );
  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_truncate( int fildes, off_t offset, unsigned long inode );

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  ssize_t xrd_pread( int           fildes,
                     void*         buf,
                     size_t        nbyte,
                     off_t         offset,
                     unsigned long inode );
  
  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_close( int fd, unsigned long inode );


  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_flush( int fd, unsigned long inode );

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  ssize_t xrd_pwrite( int           fildes,
                      const void*   buf,
                      size_t        nbyte,
                      off_t         offset,
                      unsigned long inode );

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_fsync( int fildes, unsigned long inode );

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_unlink( const char* path );

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_rename( const char* oldpath, const char* newpath );

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_chmod( const char* path, mode_t mode );

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_symlink( const char* url,
                   const char* oldpath,
                   const char* newpath );
  
  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_link( const char* url,
                const char* oldpath,
                const char* newpath );
  
  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_readlink( const char* path, char* buf, size_t bufsize );

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_access( const char* path, int mode );

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_utimes( const char* path, struct timespec* tvp );

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int xrd_inodirlist( unsigned long long dirinode, const char* path );

  //----------------------------------------------------------------------------
  //! Do user mapping
  //----------------------------------------------------------------------------
  const char*  xrd_mapuser( uid_t uid , pid_t pid );

  //----------------------------------------------------------------------------
  //! Initialisation function
  //----------------------------------------------------------------------------
  void xrd_init();


#ifdef __cplusplus
}
#endif

#endif // __XRD_POSIX_HH__
