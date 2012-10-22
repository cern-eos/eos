// ----------------------------------------------------------------------
// File: xrdposix.cc
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
/* Based on 'xrdposix.cc' XRootD Software                               */
/* xrdposix.cc                                                          */
/*                                                                      */
/* Author: Wei Yang (Stanford Linear Accelerator Center, 2007)          */
/*                                                                      */
/* C wrapper to some of the Xrootd Posix library functions              */
/*                                                                      */
/* Modified: Andreas-Joachim Peters (CERN,2008) XCFS                    */
/* Modified: Andreas-Joachim Peters (CERN,2010) EOS                     */
/************************************************************************/

#define _FILE_OFFSET_BITS 64

/*----------------------------------------------------------------------------*/
#include "xrdposix.hh"
#include "FuseCacheEntry.hh"
/*----------------------------------------------------------------------------*/
#include <climits>
#include <stdint.h>
#include <iostream>
#include <libgen.h>
#include <pwd.h>
/*----------------------------------------------------------------------------*/
#include "XrdCache/XrdFileCache.hh"
#include "XrdCache/FileAbstraction.hh"
#include "XrdClient/XrdClientEnv.hh"
#include "XrdClient/XrdClientAdmin.hh"
#include "XrdClient/XrdClientConn.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTable.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdClient/XrdClientConst.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "common/Path.hh"
#include "common/RWMutex.hh"
/*----------------------------------------------------------------------------*/
#include <google/dense_hash_map>
#include <google/sparse_hash_map>
#include <google/sparsehash/densehashtable.h>
/*----------------------------------------------------------------------------*/

#ifndef __macos__
#define OSPAGESIZE 4096
#else
#define OSPAGESIZE 65536
#endif

static XrdCl::FileSystem* fs;
static XrdFileCache* XFC;

static XrdOucHash<XrdOucString>* passwdstore;
static XrdOucHash<XrdOucString>* stringstore;

XrdSysMutex passwdstoremutex;
XrdSysMutex stringstoremutex;

int  fuse_cache_read;
int  fuse_cache_write;


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
char*
STRINGSTORE( const char* __charptr__ )
{
  XrdOucString* yourstring;

  if ( !__charptr__ ) return ( char* )"";

  if ( ( yourstring = stringstore->Find( __charptr__ ) ) ) {
    return ( ( char* )yourstring->c_str() );
  } else {
    XrdOucString* newstring = new XrdOucString( __charptr__ );
    stringstoremutex.Lock();
    stringstore->Add( __charptr__, newstring );
    stringstoremutex.UnLock();
    return ( char* )newstring->c_str();
  }
}



//------------------------------------------------------------------------------
// Implementation Translations
//------------------------------------------------------------------------------

// protecting the path/inode translation table
eos::common::RWMutex mutex_inode_path;

// Mapping path name to inode
google::dense_hash_map<std::string, unsigned long long> path2inode;

// Mapping inode to path name
google::dense_hash_map<unsigned long long, std::string> inode2path;


//------------------------------------------------------------------------------
// Lock read
//------------------------------------------------------------------------------
void xrd_lock_r_p2i()
{
  mutex_inode_path.LockRead();
}


//------------------------------------------------------------------------------
// Unlock read
//------------------------------------------------------------------------------
void xrd_unlock_r_p2i()
{
  mutex_inode_path.UnLockRead();
}


//------------------------------------------------------------------------------
// Drop the basename and return only the last level path name
//------------------------------------------------------------------------------
char*
xrd_basename( unsigned long long inode )
{
  eos::common::RWMutexReadLock vLock( mutex_inode_path );
  const char* fname = xrd_path( inode );

  if ( fname ) {
    std::string spath = fname;
    size_t len = spath.length();

    if ( len ) {
      if ( spath[len - 1] == '/' ) {
        spath.erase( len - 1 );
      }
    }

    size_t spos = spath.rfind( "/" );

    if ( spos != std::string::npos ) {
      spath.erase( 0, spos + 1 );
    }

    return static_cast<char*>( STRINGSTORE( spath.c_str() ) );
  }

  return 0;
}


//------------------------------------------------------------------------------
// Translate from inode to path
//------------------------------------------------------------------------------
const char*
xrd_path( unsigned long long inode )
{
  // Obs: use xrd_lock_r_p2i/xrd_unlock_r_p2i in the scope of the returned string
  if ( inode2path.count( inode ) )
    return inode2path[inode].c_str();
  else
    return 0;
}


//------------------------------------------------------------------------------
// Translate from path to inode
//------------------------------------------------------------------------------
unsigned long long
xrd_inode( const char* path )
{
  eos::common::RWMutexReadLock rd_lock( mutex_inode_path );
  unsigned long long ret = 0;

  if ( path2inode.count( path ) )
    ret = path2inode[path];

  return ret;
}


//------------------------------------------------------------------------------
// Store an inode <-> path mapping
//------------------------------------------------------------------------------
void
xrd_store_p2i( unsigned long long inode, const char* path )
{
  eos::common::RWMutexWriteLock wr_lock( mutex_inode_path );
  path2inode[path] = inode;
  inode2path[inode] = path;
}


//------------------------------------------------------------------------------
// Store an inode <-> path mapping given the parent inode
//------------------------------------------------------------------------------
void
xrd_store_child_p2i( unsigned long long inode,
                     unsigned long long childinode,
                     const char*        name )
{
  eos::common::RWMutexWriteLock wr_lock( mutex_inode_path );
  std::string fullpath = inode2path[inode];
  std::string sname = name;

  if ( sname != "." ) {
    // we don't need to store this one
    if ( sname == ".." ) {
      if ( inode == 1 ) {
        fullpath = "/";
      } else {
        size_t spos = fullpath.rfind( "/" );

        if ( spos != std::string::npos ) {
          fullpath.erase( spos );
        }
      }
    } else {
      fullpath += "/";
      fullpath += name;
    }

    fprintf( stderr, "sname=%s fullpath=%s inode=%llu childinode=%llu\n",
             sname.c_str(), fullpath.c_str(), inode, childinode );

    path2inode[fullpath] = childinode;
    inode2path[childinode] = fullpath;
  }
}


//------------------------------------------------------------------------------
// Delete an inode <-> path mapping given the inode
//------------------------------------------------------------------------------
void
xrd_forget_p2i( unsigned long long inode )
{
  eos::common::RWMutexWriteLock wr_lock( mutex_inode_path );

  if ( inode2path.count( inode ) ) {
    std::string path = inode2path[inode];
    path2inode.erase( path );
    inode2path.erase( inode );
  }
}



// -----------------------------------------------------------------------------
// Implementation of the directory listing table
// -----------------------------------------------------------------------------

// protecting the directory listing table
eos::common::RWMutex mutex_dir2inodelist;

// dir listing map
google::dense_hash_map<unsigned long long, std::vector<unsigned long long> >dir2inodelist;
google::dense_hash_map<unsigned long long, struct dirbuf> dir2dirbuf;


// -----------------------------------------------------------------------------
// Lock read
// -----------------------------------------------------------------------------
void xrd_lock_r_dirview()
{
  mutex_dir2inodelist.LockRead();
}

// -----------------------------------------------------------------------------
// Unlock read
// -----------------------------------------------------------------------------
void xrd_unlock_r_dirview()
{
  mutex_dir2inodelist.UnLockRead();
}


// -----------------------------------------------------------------------------
// Lock write 
// -----------------------------------------------------------------------------
void xrd_lock_w_dirview()
{
  mutex_dir2inodelist.LockWrite();
}


// -----------------------------------------------------------------------------
// Unlock write 
// -----------------------------------------------------------------------------
void xrd_unlock_w_dirview()
{
  mutex_dir2inodelist.UnLockWrite();
}


// -----------------------------------------------------------------------------
// Create a new entry in the maps for the current inode
// -----------------------------------------------------------------------------
void
xrd_dirview_create( unsigned long long inode )
{
  eos_static_debug( "inode=%llu", inode );
  //Obs: path should be attached beforehand into path translation
  eos::common::RWMutexWriteLock vLock( mutex_dir2inodelist );
  dir2inodelist[inode].clear();
  dir2dirbuf[inode].p    = 0;
  dir2dirbuf[inode].size = 0;
}


// -----------------------------------------------------------------------------
// Delete entry from maps for current inode
// -----------------------------------------------------------------------------
void
xrd_dirview_delete( unsigned long long inode )
{
  eos_static_debug( "inode=%llu", inode );
  eos::common::RWMutexWriteLock vLock( mutex_dir2inodelist );

  if ( dir2inodelist.count( inode ) ) {
    if ( dir2dirbuf[inode].p ) {
      free( dir2dirbuf[inode].p );
    }

    dir2dirbuf.erase( inode );
    dir2inodelist[inode].clear();
    dir2inodelist.erase( inode );
  }

}


// -----------------------------------------------------------------------------
// Get entry's inode with index 'index' from directory
// -----------------------------------------------------------------------------
unsigned long long
xrd_dirview_entry( unsigned long long dirinode, size_t index )
{
  eos_static_debug( "dirinode=%llu, index=%zu", dirinode, index );
  //Obj:  should have xrd_lock_dirview in the scope of the call

  if ( ( dir2inodelist.count( dirinode ) ) &&
       ( dir2inodelist[dirinode].size() > index ) ) {
    return dir2inodelist[dirinode][index];
  }

  return 0;
}


// -----------------------------------------------------------------------------
// Get dirbuf corresponding to inode
// -----------------------------------------------------------------------------
struct dirbuf* xrd_dirview_getbuffer( unsigned long long inode ) {
  //Obs: should have xrd_lock_dirview in the scope of the call
  return &dir2dirbuf[inode];
}



// -----------------------------------------------------------------------------
// Implementation of the FUSE directory cache
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Get maximum number of directories in cache
// -----------------------------------------------------------------------------
static const unsigned long long GetMaxCacheSize()
{
  return 128 * 1024;
}


eos::common::RWMutex mutex_fuse_cache;  //< protecting the cache entry map
google::dense_hash_map<unsigned long long, FuseCacheEntry*> inode2cache;  //< inode cache


/*----------------------------------------------------------------------------*/
/**
 *
 * Get a cached directory
 *
 * @param inode inode value of the directory to be cached
 * @param mtime modification time
 * @param fullpath full path of the directory
 * @param b dirbuf structure
 *
 * @return DirStatus code
 *         -3 - error
 *         -2 - not in cache
 *         -1 - in cache but outdated, needs update
 *          0 - dir in cache and valid
 *
 */
/*----------------------------------------------------------------------------*/
int
xrd_dir_cache_get( unsigned long long inode,
                   struct timespec    mtime,
                   char*              fullpath,
                   struct dirbuf**    b )
{
  int retc;
  FuseCacheEntry* dir = 0;

  fprintf( stderr, "[%s] Calling function.\n", __FUNCTION__ );

  eos::common::RWMutexReadLock rd_lock( mutex_fuse_cache );

  if ( inode2cache.count( inode ) && ( dir = inode2cache[inode] ) ) {
    struct timespec oldtime = dir->GetModifTime();

    if ( ( oldtime.tv_sec == mtime.tv_sec ) && ( oldtime.tv_nsec == mtime.tv_nsec ) ) {
      // valid timestamp
      xrd_lock_r_dirview();  // =>

      if ( !xrd_dirview_entry( inode, 0 ) ) {
        // there is no listing yet, create one!
        xrd_unlock_r_dirview();  // <=
        xrd_inodirlist( ( unsigned long long )inode, fullpath );
        xrd_lock_r_dirview();    // =>
        *b = xrd_dirview_getbuffer( ( unsigned long long )inode );

        if ( !( *b ) ) {
          retc = dError;  // error
        } else {
          dir->GetDirbuf( **b );
          retc = dValid;   // success
        }
      } else {
        // dir in cache and valid
        fprintf( stderr, "[%s] Dir in cache and valid. \n", __FUNCTION__ );
        dir->GetDirbuf( **b );
        //*b = xrd_dirview_getbuffer( ( unsigned long long )inode );
        //if ( *b == 0 )
        //  fprintf( stderr, "[%s] The b buffer is empty. \n", __FUNCTION__ );
        retc = dValid;
      }

      xrd_unlock_r_dirview(); // <=
    } else {
      retc = dOutdated;
    }
  } else {
    retc = dNotInCache;
  }

  return retc;
}


/*----------------------------------------------------------------------------*/
/**
 *
 * Add or update a cache directory entry
 *
 * @param inode directory inode value
 * @param fullpath directory full path
 * @param number of entries in the current directory
 * @param mtime modifcation time
 * @param b dirbuf structure
 *
 */
/*----------------------------------------------------------------------------*/
void
xrd_dir_cache_sync( unsigned long long inode,
                    char*              fullpath,
                    int                nentries,
                    struct timespec    mtime,
                    struct dirbuf*     b )
{
  eos::common::RWMutexWriteLock wr_lock( mutex_fuse_cache );
  FuseCacheEntry* dir = 0;

  if ( ( inode2cache.count( inode ) ) && ( dir = inode2cache[inode] ) ) {
    // update
    dir->Update( nentries, mtime, b );
  } else {
    // add new entry
    if ( inode2cache.size() >= GetMaxCacheSize() ) {
      // size control of the cache
      unsigned long long indx = 0;
      unsigned long long entries_del = ( unsigned long long )( 0.25 * GetMaxCacheSize() );
      google::dense_hash_map<unsigned long long, FuseCacheEntry*>::iterator iter;
      iter = inode2cache.begin();

      while ( ( indx <= entries_del ) && ( iter != inode2cache.end() ) ) {
        dir = ( FuseCacheEntry* ) iter->second;
        inode2cache.erase( iter++ );
        delete dir;
        indx++;
      }
    }

    dir = new FuseCacheEntry( nentries, mtime, b );
    inode2cache[inode] = dir;
  }

  return;
}


/*----------------------------------------------------------------------------*/
/**
 *
 * Get a subentry from a cached directory
 *
 * @param req
 * @param inode directory inode
 * @param einode entry inode
 * @param efullpath full path of the subentry
 *
 * @return SubentryStatus value
 *         -1 - directory not filled or entry not found
 *          0 - entry found
 *
 */
/*----------------------------------------------------------------------------*/
int
xrd_dir_cache_get_entry( fuse_req_t         req,
                         unsigned long long inode,
                         unsigned long long entry_inode,
                         const char*        efullpath )
{
  int retc = eDirNotFound;
  eos::common::RWMutexReadLock rd_lock( mutex_fuse_cache );
  FuseCacheEntry* dir;

  if ( ( inode2cache.count( inode ) ) && ( dir = inode2cache[inode] ) ) {
    if ( dir->IsFilled() ) {
      struct fuse_entry_param e;
      if ( dir->GetEntry( entry_inode, e ) ) {
        xrd_store_p2i( entry_inode, efullpath );
        fuse_reply_entry( req, &e );
        retc = eFound;  //success
      }
    }
  }

  return retc;
}


/*----------------------------------------------------------------------------*/
/**
 *
 * Add new subentry to a cached directory
 *
 * @param inode directory inode
 * @param entry_inode subentry inode
 * @param e fuse_entry_param structure
 *
 */
/*----------------------------------------------------------------------------*/
void
xrd_dir_cache_add_entry( unsigned long long       inode,
                         unsigned long long       entry_inode,
                         struct fuse_entry_param* e )
{
  eos::common::RWMutexReadLock rd_lock( mutex_fuse_cache );
  FuseCacheEntry* dir = 0;

  if ( ( inode2cache.count( inode ) ) && ( dir = inode2cache[inode] ) ) {
    dir->AddEntry( entry_inode, e );
  }

  return;
}



// ---------------------------------------------------------------
// Implementation the open File Descriptor map
// ---------------------------------------------------------------

// protecting the open filedescriptor map
XrdSysMutex OpenPosixXrootFdLock;

// open xrootd fd table
class PosixFd
{
  public:
    PosixFd() {
      fd = 0;
      nuser = 0;
    }
    ~PosixFd() {
    }

    void   setFd( int FD ) {
      fd = FD;
      Inc();
    }
    int    getFd()       {
      Inc();
      return fd;
    }
    size_t getUser()     {
      return nuser;
    }

    void Inc() {
      nuser++;
    }
    void Dec() {
      if ( nuser ) nuser--;
    }

    static std::string Index( unsigned long long inode, uid_t uid ) {
      char index[256];
      snprintf( index, sizeof( index ) - 1, "%llu-%u", inode, uid );
      return index;
    }

  private:
    int fd;       // POSIX fd to store
    size_t nuser; // number of users attached to this fd
};

google::dense_hash_map<std::string, PosixFd> OpenPosixXrootdFd;


//------------------------------------------------------------------------------
void
xrd_add_open_fd( int fd, unsigned long long inode, uid_t uid )
{
  // add fd as an open file descriptor to speed-up mknod
  XrdSysMutexHelper vLock( OpenPosixXrootFdLock );

  OpenPosixXrootdFd[PosixFd::Index( inode, uid )].setFd( fd );
}


//------------------------------------------------------------------------------
int
xrd_get_open_fd( unsigned long long inode, uid_t uid )
{
  // return posix fd for inode - increases 'nuser'
  XrdSysMutexHelper vLock( OpenPosixXrootFdLock );

  return OpenPosixXrootdFd[PosixFd::Index( inode, uid )].getFd();
}


//------------------------------------------------------------------------------
void
xrd_lease_open_fd( unsigned long long inode, uid_t uid )
{
  // release an attached file descriptor
  XrdSysMutexHelper vLock( OpenPosixXrootFdLock );
  OpenPosixXrootdFd[PosixFd::Index( inode, uid )].Dec();

  if ( !OpenPosixXrootdFd[PosixFd::Index( inode, uid )].getUser() ) {
    OpenPosixXrootdFd.erase( PosixFd::Index( inode, uid ) );
  }
}


// ---------------------------------------------------------------
// Implementation IO Buffer Management
// ---------------------------------------------------------------

// protecting the IO buffer map
XrdSysMutex IoBufferLock;

class IoBuf
{
  private:
    void* buffer;
    size_t size;

  public:

    IoBuf() {
      buffer = 0;
      size = 0;
    }

    virtual ~IoBuf() {
      if ( buffer && size ) free( buffer );
    }
    char* getBuffer() {
      return ( char* )buffer;
    }
    size_t getSize() {
      return size;
    }
    void resize( size_t newsize ) {
      if ( newsize > size ) {
        size = ( newsize < ( 128 * 1024 ) ) ? 128 * 1024 : newsize;
        buffer = realloc( buffer, size );
      }
    }
};

// IO buffer table
std::map<int, IoBuf> IoBufferMap;

//------------------------------------------------------------------------------
char*
xrd_attach_read_buffer( int fd, size_t  size )
{
  // guarantee a buffer for reading of at least 'size' for the specified fd
  XrdSysMutexHelper vlock( IoBufferLock );

  IoBufferMap[fd].resize( size );
  return ( char* )IoBufferMap[fd].getBuffer();
}


//------------------------------------------------------------------------------
void
xrd_release_read_buffer( int fd )
{
  // release a read buffer for the specified fd
  XrdSysMutexHelper vlock( IoBufferLock );
  IoBufferMap.erase( fd );
  return;
}


//------------------------------------------------------------------------------
int
xrd_rmxattr( const char* path, const char* xattr_name )
{
  eos_static_info( "path=%s xattr_name=%s", path, xattr_name );
  eos::common::Timing rmxattrtiming( "rmxattr" );
  TIMING( "START", &rmxattrtiming );

  int retc;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;

  request = path;
  request += "?";
  request += "mgm.pcmd=xattr&";
  request += "mgm.subcmd=rm&";
  request += "mgm.xattrname=";
  request +=  xattr_name;
  arg.FromString( request );

  XrdCl::XRootDStatus status = fs->Query( XrdCl::QueryCode::OpaqueFile,
                                          arg, response );

  TIMING( "GETPLUGIN", &rmxattrtiming );

  if ( status.IsOK() ) {
    //parse the output
    int items = 0;
    char tag[1024];

    items = sscanf( response->GetBuffer(), "%s retc=%i", tag, &retc );

    if ( ( items != 2 ) || ( strcmp( tag, "rmxattr:" ) ) ) {
      errno = ENOENT;
      retc = EFAULT;
    }
  } else {
    retc = EFAULT;
  }

  COMMONTIMING( "END", &rmxattrtiming );

  if ( EOS_LOGS_DEBUG ) {
    rmxattrtiming.Print();
  }

  delete response;
  return retc;
}


//------------------------------------------------------------------------------
int
xrd_setxattr( const char* path,
              const char* xattr_name,
              const char* xattr_value,
              size_t      size )
{
  eos_static_info( "path=%s xattr_name=%s xattr_value=%s", path, xattr_name, xattr_value );
  eos::common::Timing setxattrtiming( "setxattr" );
  COMMONTIMING( "START", &setxattrtiming );

  int retc;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;

  request = path;
  request += "?";
  request += "mgm.pcmd=xattr&";
  request += "mgm.subcmd=set&";
  request += "mgm.xattrname=";
  request +=  xattr_name;
  request += "&";
  request += "mgm.xattrvalue=";
  request += xattr_value;
  arg.FromString( request );

  XrdCl::XRootDStatus status = fs->Query( XrdCl::QueryCode::OpaqueFile,
                                          arg, response );

  COMMONTIMING( "GETPLUGIN", &setxattrtiming );

  if ( status.IsOK() ) {
    //parse the output
    int items = 0;
    char tag[1024];

    items = sscanf( response->GetBuffer(), "%s retc=%i", tag, &retc );

    if ( ( items != 2 ) || ( strcmp( tag, "setxattr:" ) ) ) {
      errno = ENOENT;
      retc = EFAULT;
    }
  } else {
    retc = EFAULT;
  }
  COMMONTIMING( "END", &setxattrtiming );

  if ( EOS_LOGS_DEBUG ) {
    setxattrtiming.Print();
  }

  delete response;
  return retc;
}


//------------------------------------------------------------------------------
int
xrd_getxattr( const char* path,
              const char* xattr_name,
              char**      xattr_value,
              size_t*     size )
{
  eos_static_info( "path=%s xattr_name=%s", path, xattr_name );
  eos::common::Timing getxattrtiming( "getxattr" );
  COMMONTIMING( "START", &getxattrtiming );

  int retc;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;

  request = path;
  request += "?";
  request += "mgm.pcmd=xattr&";
  request += "mgm.subcmd=get&";
  request += "mgm.xattrname=";
  request +=  xattr_name;
  arg.FromString( request );

  XrdCl::XRootDStatus status = fs->Query( XrdCl::QueryCode::OpaqueFile,
                                          arg, response );

  COMMONTIMING( "GETPLUGIN", &getxattrtiming );

  if ( status.IsOK() ) {
    //parse the output
    int items = 0;
    char tag[1024];
    char rval[4096];

    items = sscanf( response->GetBuffer(), "%s retc=%i value=%s", tag, &retc, rval );

    if ( ( items != 3 ) || ( strcmp( tag, "getxattr:" ) ) ) {
      errno = ENOENT;
      delete response;
      return EFAULT;
    } else {
      if ( strcmp( xattr_name, "user.eos.XS" ) == 0 ) {
        char* ptr = rval;

        for ( unsigned int i = 0; i < strlen( rval ); i++, ptr++ ) {
          if ( *ptr == '_' )
            *ptr = ' ';
        }
      }

      *size = strlen( rval );
      *xattr_value = ( char* ) calloc( ( *size ) + 1, sizeof( char ) );
      *xattr_value = strncpy( *xattr_value, rval, *size );
    }
  } else {
    retc = EFAULT;
  }

  COMMONTIMING( "END", &getxattrtiming );

  if ( EOS_LOGS_DEBUG ) {
    getxattrtiming.Print();
  }

  delete response;
  return retc;
}


//------------------------------------------------------------------------------
int
xrd_listxattr( const char* path, char** xattr_list, size_t* size )
{
  eos_static_info( "path=%s", path );
  eos::common::Timing listxattrtiming( "listxattr" );
  COMMONTIMING( "START", &listxattrtiming );

  int retc;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;

  request = path;
  request += "?";
  request += "mgm.pcmd=xattr&";
  request += "mgm.subcmd=ls";
  arg.FromString( request );

  XrdCl::XRootDStatus status = fs->Query( XrdCl::QueryCode::OpaqueFile,
                                          arg, response );

  TIMING( "GETPLUGIN", &listxattrtiming );

  if ( status.IsOK() ) {
    //parse the output
    int items = 0;
    char tag[1024];
    char rval[16384];

    items = sscanf( response->GetBuffer(), "%s retc=%i %s", tag, &retc, rval );

    if ( ( items != 3 ) || ( strcmp( tag, "lsxattr:" ) ) ) {
      errno = ENOENT;
      delete response;
      return EFAULT;
    } else {
      *size = strlen( rval );
      char* ptr = rval;

      for ( unsigned int i = 0; i < ( *size ); i++, ptr++ ) {
        if ( *ptr == '&' )
          *ptr = '\0';
      }

      *xattr_list = ( char* ) calloc( ( *size ) + 1, sizeof( char ) );
      *xattr_list = ( char* ) memcpy( *xattr_list, rval, *size );
    }
  } else {
    retc = EFAULT;
  }

  COMMONTIMING( "END", &listxattrtiming );

  if ( EOS_LOGS_DEBUG ) {
    listxattrtiming.Print();
  }

  delete response;
  return retc;
}


//------------------------------------------------------------------------------
int
xrd_stat( const char* path, struct stat* buf )
{
  eos_static_info( "path=%s", path );
  eos::common::Timing stattiming( "xrd_stat" );
  COMMONTIMING( "START", &stattiming );

  int retc;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;

  request = path;
  request += "?";
  request += "mgm.pcmd=stat";
  arg.FromString( request );

  XrdCl::XRootDStatus status = fs->Query( XrdCl::QueryCode::OpaqueFile,
                                          arg, response );

  COMMONTIMING( "GETPLUGIN", &stattiming );

  if ( status.IsOK() ) {
    unsigned long long sval[10];
    unsigned long long ival[6];
    char tag[1024];

    // parse the stat output
    int items = sscanf( response->GetBuffer(),
                        "%s %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
                        tag, ( unsigned long long* )&sval[0],
                        ( unsigned long long* )&sval[1],
                        ( unsigned long long* )&sval[2],
                        ( unsigned long long* )&sval[3],
                        ( unsigned long long* )&sval[4],
                        ( unsigned long long* )&sval[5],
                        ( unsigned long long* )&sval[6],
                        ( unsigned long long* )&sval[7],
                        ( unsigned long long* )&sval[8],
                        ( unsigned long long* )&sval[9],
                        ( unsigned long long* )&ival[0],
                        ( unsigned long long* )&ival[1],
                        ( unsigned long long* )&ival[2],
                        ( unsigned long long* )&ival[3],
                        ( unsigned long long* )&ival[4],
                        ( unsigned long long* )&ival[5] );

    if ( ( items != 17 ) || ( strcmp( tag, "stat:" ) ) ) {
      errno = ENOENT;
      delete response;
      return EFAULT;
    } else {
      buf->st_dev = ( dev_t ) sval[0];
      buf->st_ino = ( ino_t ) sval[1];
      buf->st_mode = ( mode_t ) sval[2];
      buf->st_nlink = ( nlink_t ) sval[3];
      buf->st_uid = ( uid_t ) sval[4];
      buf->st_gid = ( gid_t ) sval[5];
      buf->st_rdev = ( dev_t ) sval[6];
      buf->st_size = ( off_t ) sval[7];
      buf->st_blksize = ( blksize_t ) sval[8];
      buf->st_blocks  = ( blkcnt_t ) sval[9];
      buf->st_atime = ( time_t ) ival[0];
      buf->st_mtime = ( time_t ) ival[1];
      buf->st_ctime = ( time_t ) ival[2];
      buf->st_atim.tv_sec = ( time_t ) ival[0];
      buf->st_mtim.tv_sec = ( time_t ) ival[1];
      buf->st_ctim.tv_sec = ( time_t ) ival[2];
      buf->st_atim.tv_nsec = ( time_t ) ival[3];
      buf->st_mtim.tv_nsec = ( time_t ) ival[4];
      buf->st_ctim.tv_nsec = ( time_t ) ival[5];
      retc = 0;
    }
  } else {
    retc = EFAULT;
  }

  COMMONTIMING( "END", &stattiming );

  if ( EOS_LOGS_DEBUG ) {
    //stattiming.Print();
  }

  delete response;
  return retc;
}


//------------------------------------------------------------------------------
int
xrd_statfs( const char* url, const char* path, struct statvfs* stbuf )
{
  eos_static_info( "url=%s path=%s", url, path );
  static unsigned long long a1 = 0;
  static unsigned long long a2 = 0;
  static unsigned long long a3 = 0;
  static unsigned long long a4 = 0;

  static XrdSysMutex statmutex;
  static time_t laststat = 0;
  statmutex.Lock();

  if ( ( time( NULL ) - laststat ) < ( ( 15 + ( int )5.0 * rand() / RAND_MAX ) ) ) {
    stbuf->f_bsize  = 4096;
    stbuf->f_frsize = 4096;
    stbuf->f_blocks = a3 / 4096;
    stbuf->f_bfree  = a1 / 4096;
    stbuf->f_bavail = a1 / 4096;
    stbuf->f_files  = a4;
    stbuf->f_ffree  = a2;
    stbuf->f_fsid     = 0xcafe;
    stbuf->f_namemax  = 256;
    statmutex.UnLock();
    return 0;
  }

  eos::common::Timing statfstiming( "xrd_statfs" );
  COMMONTIMING( "START", &statfstiming );

  int retc;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;

  request = url;
  request += "?";
  request += "mgm.pcmd=statvfs&";
  request += "path=";
  request += path;
  arg.FromString( request );

  XrdCl::XRootDStatus status = fs->Query( XrdCl::QueryCode::OpaqueFile,
                                          arg, response );

  COMMONTIMING( "END", &statfstiming );

  if ( EOS_LOGS_DEBUG ) {
    statfstiming.Print();
  }

  if ( status.IsOK() ) {
    char tag[1024];

    if ( !response->GetBuffer() ) {
      statmutex.UnLock();
      delete response;
      return -EFAULT;
    }

    // parse the stat output
    int items = sscanf( response->GetBuffer(),
                        "%s retc=%d f_avail_bytes=%llu f_avail_files=%llu f_max_bytes=%llu f_max_files=%llu",
                        tag, &retc, &a1, &a2, &a3, &a4 );

    if ( ( items != 6 ) || ( strcmp( tag, "statvfs:" ) ) ) {
      statmutex.UnLock();
      delete response;
      return -EFAULT;
    }

    laststat = time( NULL );

    statmutex.UnLock();
    stbuf->f_bsize  = 4096;
    stbuf->f_frsize = 4096;
    stbuf->f_blocks = a3 / 4096;
    stbuf->f_bfree  = a1 / 4096;
    stbuf->f_bavail = a1 / 4096;
    stbuf->f_files  = a4;
    stbuf->f_ffree  = a2;
  } else {
    statmutex.UnLock();
    retc = -EFAULT;
  }

  delete response;
  return retc;
}


//------------------------------------------------------------------------------
int
xrd_chmod( const char* path, mode_t mode )
{
  eos_static_info( "path=%s mode=%x", path, mode );
  eos::common::Timing chmodtiming( "xrd_chmod" );
  COMMONTIMING( "START", &chmodtiming );



  int retc;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;

  request = path;
  request += "?";
  request += "mgm.pcmd=chmod&mode=";
  request += ( int )mode;
  arg.FromString( request );

  XrdCl::XRootDStatus status = fs->Query( XrdCl::QueryCode::OpaqueFile,
                                          arg, response );

  COMMONTIMING( "END", &chmodtiming );

  if ( EOS_LOGS_DEBUG ) {
    chmodtiming.Print();
  }

  if ( status.IsOK() ) {
    char tag[1024];

    if ( !response->GetBuffer() ) {
      delete response;
      return -EFAULT;
    }

    // parse the stat output
    int items = sscanf( response->GetBuffer(), "%s retc=%d", tag, &retc );

    if ( ( items != 2 ) || ( strcmp( tag, "chmod:" ) ) ) {
      delete response;
      return -EFAULT;
    }
  } else {
    retc = -EFAULT;
  }

  delete response;
  return retc;
}

//------------------------------------------------------------------------------
int
xrd_symlink( const char* url, const char* destpath, const char* sourcepath )
{
  eos_static_info( "url=%s destpath=%s,sourcepath=%s", url, destpath, sourcepath );
  eos::common::Timing symlinktiming( "xrd_symlink" );
  COMMONTIMING( "START", &symlinktiming );

  int retc;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;

  request = url;
  request += "?";
  request += "mgm.pcmd=symlink&linkdest=";
  request += destpath;
  request += "&linksource=";
  request += sourcepath;
  arg.FromString( request );

  XrdCl::XRootDStatus status = fs->Query( XrdCl::QueryCode::OpaqueFile,
                                          arg, response );

  COMMONTIMING( "END", &symlinktiming );

  if ( EOS_LOGS_DEBUG ) {
    symlinktiming.Print();
  }

  if ( status.IsOK() ) {
    char tag[1024];
    int items = sscanf( response->GetBuffer(), "%s retc=%d", tag, &retc );

    if ( ( items != 2 ) || ( strcmp( tag, "symlink:" ) ) ) {
      delete response;
      return -EFAULT;
    }
  } else {
    retc = -EFAULT;
  }

  delete response;
  return retc;
}


//------------------------------------------------------------------------------
int
xrd_link( const char* url, const char* destpath, const char* sourcepath )
{
  eos_static_info( "url=%s destpath=%s sourcepath=%s", url, destpath, sourcepath );
  eos::common::Timing linktiming( "xrd_link" );
  COMMONTIMING( "START", &linktiming );

  int retc;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;

  request = url;
  request += "?";
  request += "mgm.pcmd=link&linkdest=";
  request += destpath;
  request += "&linksource=";
  request += sourcepath;
  arg.FromString( request );

  XrdCl::XRootDStatus status = fs->Query( XrdCl::QueryCode::OpaqueFile,
                                          arg, response );

  COMMONTIMING( "END", &linktiming );

  if ( EOS_LOGS_DEBUG ) {
    linktiming.Print();
  }

  if ( status.IsOK() ) {
    char tag[1024];
    int items = sscanf( response->GetBuffer(), "%s retc=%d", tag, &retc );

    if ( ( items != 2 ) || ( strcmp( tag, "link:" ) ) ) {
      delete response;
      return -EFAULT;
    }
  } else {
    retc = -EFAULT;
  }

  delete response;
  return retc;

}


//------------------------------------------------------------------------------
int
xrd_readlink( const char* path, char* buf, size_t bufsize )
{

  eos_static_info( "path=%s", path );
  eos::common::Timing readlinktiming( "xrd_readlink" );
  COMMONTIMING( "START", &readlinktiming );

  int retc;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;

  request = path;
  request += "?";
  request += "mgm.pcmd=readlink";
  arg.FromString( request );

  XrdCl::XRootDStatus status = fs->Query( XrdCl::QueryCode::OpaqueFile,
                                          arg, response );

  COMMONTIMING( "END", &readlinktiming );

  if ( status.IsOK() ) {
    char tag[1024];
    char link[4096];
    link[0] = 0;
    // parse the stat output
    int items = sscanf( response->GetBuffer(), "%s retc=%d link=%s", tag, &retc, link );

    if ( ( items != 3 ) || ( strcmp( tag, "readlink:" ) ) ) {
      delete response;
      return -EFAULT;
    }

    strncpy( buf, link, ( bufsize < OSPAGESIZE ) ? bufsize : ( OSPAGESIZE - 1 ) );
  } else {
    retc = -EFAULT;
  }

  if ( EOS_LOGS_DEBUG ) {
    readlinktiming.Print();
  }

  delete response;
  return retc;
}


//------------------------------------------------------------------------------
int
xrd_utimes( const char* path, struct timespec* tvp )
{
  eos_static_info( "path=%s", path );
  eos::common::Timing utimestiming( "xrd_utimes" );
  COMMONTIMING( "START", &utimestiming );

  int retc;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;

  request = path;
  request += "?";
  request += "mgm.pcmd=utimes&tv1_sec=";
  char lltime[1024];
  sprintf( lltime, "%llu", ( unsigned long long )tvp[0].tv_sec );
  request += lltime;
  request += "&tv1_nsec=";
  sprintf( lltime, "%llu", ( unsigned long long )tvp[0].tv_nsec );
  request += lltime;
  request += "&tv2_sec=";
  sprintf( lltime, "%llu", ( unsigned long long )tvp[1].tv_sec );
  request += lltime;
  request += "&tv2_nsec=";
  sprintf( lltime, "%llu", ( unsigned long long )tvp[1].tv_nsec );
  request += lltime;
  arg.FromString( request );

  XrdCl::XRootDStatus status = fs->Query( XrdCl::QueryCode::OpaqueFile,
                                          arg, response );

  COMMONTIMING( "END", &utimestiming );

  if ( EOS_LOGS_DEBUG ) {
    utimestiming.Print();
  }

  if ( status.IsOK() ) {
    char tag[1024];
    int items = sscanf( response->GetBuffer(), "%s retc=%d", tag, &retc );

    if ( ( items != 2 ) || ( strcmp( tag, "utimes:" ) ) ) {
      errno = EFAULT;
      delete response;
      return -EFAULT;
    }

  } else {
    errno = EFAULT;
    retc = -EFAULT;
  }

  delete response;
  return retc;
}


//------------------------------------------------------------------------------
int
xrd_access( const char* path, int mode )
{
  eos_static_info( "path=%s mode=%d", path, mode );
  eos::common::Timing accesstiming( "xrd_access" );
  COMMONTIMING( "START", &accesstiming );

  int retc;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;

  if ( ( getenv( "EOS_FUSE_NOACCESS" ) ) &&
       ( !strcmp( getenv( "EOS_FUSE_NOACCESS" ), "1" ) ) ) {
    return 0;
  }

  request = path;
  request += "?";
  request += "mgm.pcmd=access&mode=";
  request += ( int )mode;
  arg.FromString( request );

  XrdCl::XRootDStatus status = fs->Query( XrdCl::QueryCode::OpaqueFile,
                                          arg, response );


  COMMONTIMING( "STOP", &accesstiming );

  if ( EOS_LOGS_DEBUG ) {
    accesstiming.Print();
  }

  if ( status.IsOK() ) {
    char tag[1024];
    int items = sscanf( response->GetBuffer(), "%s retc=%d", tag, &retc );

    if ( ( items != 2 ) || ( strcmp( tag, "access:" ) ) ) {
      errno = EFAULT;
      delete response;
      return -EFAULT;
    }

    fprintf( stderr, "retc=%d\n", retc );
    errno = retc;

  } else {
    errno = EFAULT;
    retc = -EFAULT;
  }

  delete response;
  return retc;
}


//------------------------------------------------------------------------------
int
xrd_inodirlist( unsigned long long dirinode, const char* path )
{
  eos_static_info( "inode=%llu path=%s", dirinode, path );
  eos::common::Timing inodirtiming( "xrd_inodirlist" );
  COMMONTIMING( "START", &inodirtiming );

  int retc;
  char* ptr = 0;
  char* value = 0;
  int doinodirlist = -1;
  std::string request = path;

  COMMONTIMING( "GETSTSTREAM", &inodirtiming );

  XrdCl::File* file = new XrdCl::File();
  XrdCl::XRootDStatus status = file->Open( request.c_str(), XrdCl::OpenFlags::Flags::Read );

  if ( !status.IsOK() ) {
    delete file;
    return ENOENT;
  }

  // start to read
  int npages = 1;
  off_t offset = 0;
  unsigned int nbytes = 0;
  value = ( char* ) malloc( PAGESIZE + 1 );

  COMMONTIMING( "READSTSTREAM", &inodirtiming );

  status = file->Read( offset, PAGESIZE, value + offset, nbytes );

  while ( ( status.IsOK() ) && ( nbytes == PAGESIZE ) ) {
    npages++;
    value = ( char* ) realloc( value, npages * PAGESIZE + 1 );
    offset += PAGESIZE;
    status = file->Read( offset, PAGESIZE, value + offset, nbytes );
  }

  if ( nbytes >= 0 ) offset += nbytes;

  value[offset] = 0;
  delete file;

  xrd_dirview_create( ( unsigned long long ) dirinode );

  COMMONTIMING( "PARSESTSTREAM", &inodirtiming );

  xrd_lock_w_dirview(); // =>

  if ( nbytes >= 0 ) {
    char dirpath[4096];
    unsigned long long inode;
    char tag[128];

    retc = 0;

    // parse the stat output
    int items = sscanf( value, "%s retc=%d", tag, &retc );

    if ( ( items != 2 ) || ( strcmp( tag, "inodirlist:" ) ) ) {
      free( value );
      xrd_unlock_w_dirview(); // <=
      xrd_dirview_delete( ( unsigned long long ) dirinode );
      return -EFAULT;
    }

    ptr = strchr( value, ' ' );

    if ( ptr ) ptr = strchr( ptr + 1, ' ' );

    char* endptr = value + strlen( value ) - 1 ;

    while ( ( ptr ) && ( ptr < endptr ) ) {
      int items = sscanf( ptr, "%s %llu", dirpath, &inode );

      if ( items != 2 ) {
        free( value );
        xrd_unlock_w_dirview(); // <=
        xrd_dirview_delete( ( unsigned long long ) dirinode );
        return -EFAULT;
      }

      XrdOucString whitespacedirpath = dirpath;
      whitespacedirpath.replace( "%20", " " );
      xrd_store_child_p2i( dirinode, inode, whitespacedirpath.c_str() );
      dir2inodelist[dirinode].push_back( inode );

      // to the next entries
      if ( ptr ) ptr = strchr( ptr + 1, ' ' );

      if ( ptr ) ptr = strchr( ptr + 1, ' ' );

      eos_static_info( "name=%s inode=%llu", whitespacedirpath.c_str(), inode );
    }

    doinodirlist = 0;
  }

  xrd_unlock_w_dirview(); // <=

  COMMONTIMING( "END", &inodirtiming );

  if ( EOS_LOGS_DEBUG ) {
    //inodirtiming.Print();
  }

  free( value );
  return doinodirlist;
}


//------------------------------------------------------------------------------
struct dirent* xrd_readdir( const char* path_dir ) {
  eos_static_info( "path=%s", path_dir );
  //TODO:
  return NULL;
  //return XrdPosixXrootd::Readdir(  );
}


//------------------------------------------------------------------------------
int xrd_mkdir( const char* path, mode_t mode )
{
  eos_static_info( "path=%s mode=%d", path, mode );
  uint16_t dir_mode = 0;

  if ( mode & S_IRUSR ) dir_mode |= XrdCl::Access::Mode::UR;

  if ( mode & S_IWUSR ) dir_mode |= XrdCl::Access::Mode::UW;

  if ( mode & S_IXUSR ) dir_mode |= XrdCl::Access::Mode::UX;

  if ( mode & S_IRGRP ) dir_mode |= XrdCl::Access::Mode::GR;

  if ( mode & S_IWGRP ) dir_mode |= XrdCl::Access::Mode::GW;

  if ( mode & S_IXGRP ) dir_mode |= XrdCl::Access::Mode::GX;

  if ( mode & S_IROTH ) dir_mode |= XrdCl::Access::Mode::OR;

  if ( mode & S_IXOTH ) dir_mode |= XrdCl::Access::Mode::OX;

  XrdCl::XRootDStatus status = fs->MkDir( path, XrdCl::MkDirFlags::MakePath, dir_mode );
  return status.errNo;
}


//------------------------------------------------------------------------------
int xrd_rmdir( const char* path )
{
  eos_static_info( "path=%s", path );

  XrdCl::XRootDStatus status = fs->RmDir( path );
  return status.errNo;
}


// map used for associating file descriptors with XrdCl::File objects
eos::common::RWMutex rwmutex_map;
google::dense_hash_map<int, XrdCl::File*> map_fd_fobj;

// pool of available file descriptors
unsigned int base_fd = 1;
std::queue<int> pool_fd;


//------------------------------------------------------------------------------
int
GetFd()
{
  int retc = -1;

  if ( !pool_fd.empty() ) {
    retc = pool_fd.front();
    pool_fd.pop();
  } else if ( base_fd < UINT_MAX ) {
    base_fd++;
    retc = base_fd;
  }

  return retc;
}


//------------------------------------------------------------------------------
void ReleaseFd( int fd )
{
  pool_fd.push( fd );
}


//------------------------------------------------------------------------------
int
AddFile( XrdCl::File* obj )
{
  int fd = -1;
  eos::common::RWMutexWriteLock wr_lock( rwmutex_map );

  fd = GetFd();

  if ( fd ) {
    map_fd_fobj[fd] = obj;
  } else {
    fprintf( stderr, "Error while getting file descriptor. \n" );
  }

  return fd;
}


//------------------------------------------------------------------------------
XrdCl::File*
GetFile( int fd )
{
  eos::common::RWMutexReadLock rd_lock( rwmutex_map );

  if ( map_fd_fobj.count( fd ) ) {
    return map_fd_fobj[fd];
  } else return 0;
}


//------------------------------------------------------------------------------
void
RemoveMapping( int fd )
{
  eos::common::RWMutexWriteLock wr_lock( rwmutex_map );

  if ( map_fd_fobj.count( fd ) )
    map_fd_fobj.erase( fd );

  ReleaseFd( fd );
}




//------------------------------------------------------------------------------
int
xrd_open( const char* path, int oflags, mode_t mode )
{
  eos_static_info( "path=%s flags=%d mode=%d", path, oflags, mode );

  int t0;
  int retc = -1;
  XrdOucString spath = path;

  uint16_t flags_xrdcl = 0;
  uint16_t mode_xrdcl = 0;

  if ( oflags & ( O_CREAT | O_EXCL ) ) flags_xrdcl |= XrdCl::OpenFlags::Flags::New;

  if ( oflags & ( O_RDWR | O_WRONLY ) ) flags_xrdcl |= XrdCl::OpenFlags::Flags::Update;

  if ( mode & S_IRUSR ) mode_xrdcl |= XrdCl::Access::UR;

  if ( mode & S_IWUSR ) mode_xrdcl |= XrdCl::Access::UW;

  if ( mode & S_IXUSR ) mode_xrdcl |= XrdCl::Access::UX;

  if ( mode & S_IRGRP ) mode_xrdcl |= XrdCl::Access::GR;

  if ( mode & S_IWGRP ) mode_xrdcl |= XrdCl::Access::GW;

  if ( mode & S_IXGRP ) mode_xrdcl |= XrdCl::Access::GX;

  if ( mode & S_IROTH ) mode_xrdcl |= XrdCl::Access::OR;

  if ( mode & S_IWOTH ) mode_xrdcl |= XrdCl::Access::OW;

  if ( mode & S_IXOTH ) mode_xrdcl |= XrdCl::Access::OX;

  if ( ( t0 = spath.find( "/proc/" ) ) != STR_NPOS ) {
    // clean the path
    int t1 = spath.find( "//" );
    int t2 = spath.find( "//", t1 + 2 );
    spath.erase( t2 + 2, t0 - t2 - 2 );

    while ( spath.replace( "///", "//" ) ) {};

    // force a reauthentication to the head node
    if ( spath.endswith( "/proc/reconnect" ) ) {
      XrdClientAdmin* client = new XrdClientAdmin( path );

      if ( client ) {
        if ( client->Connect() ) {
          client->GetClientConn()->Disconnect( true );
          errno = ENETRESET;
          return -1;
        }

        delete client;
      }

      errno = ECONNABORTED;
      return -1;
    }

    // return the 'whoami' information in that file
    if ( spath.endswith( "/proc/whoami" ) ) {
      spath.replace( "/proc/whoami", "/proc/user/" );
      spath += "?mgm.cmd=whoami&mgm.format=fuse";

      XrdCl::File* file = new XrdCl::File();
      XrdCl::XRootDStatus status = file->Open( spath.c_str(), flags_xrdcl, mode_xrdcl );

      if ( status.IsOK() ) {
        retc = AddFile( file );
      }
    }

    if ( spath.endswith( "/proc/who" ) ) {
      spath.replace( "/proc/who", "/proc/user/" );
      spath += "?mgm.cmd=who&mgm.format=fuse";

      XrdCl::File* file = new XrdCl::File();
      XrdCl::XRootDStatus status = file->Open( spath.c_str(), flags_xrdcl, mode_xrdcl );

      if ( status.IsOK() ) {
        retc = AddFile( file );
      }
    }

    if ( spath.endswith( "/proc/quota" ) ) {
      spath.replace( "/proc/quota", "/proc/user/" );
      spath += "?mgm.cmd=quota&mgm.subcmd=ls&mgm.format=fuse";

      XrdCl::File* file = new XrdCl::File();
      XrdCl::XRootDStatus status = file->Open( spath.c_str(), flags_xrdcl, mode_xrdcl );

      if ( status.IsOK() ) {
        retc = AddFile( file );
      }
    }
  }

  XrdCl::File* file = new XrdCl::File();
  XrdCl::XRootDStatus status = file->Open( spath.c_str(), flags_xrdcl, mode_xrdcl );

  if ( status.IsOK() ) {
    retc = AddFile( file );
  }

  return retc;
}


//------------------------------------------------------------------------------
int
xrd_close( int fildes, unsigned long inode )
{
  eos_static_info("fd=%d inode=%lu", fildes, inode);
  if (XFC && inode) {
    FileAbstraction* fAbst = XFC->getFileObj(inode, false);
    if (fAbst && (fAbst->getSizeWrites() != 0)) {
      XFC->waitWritesAndRemove(*fAbst);
    }
  }

  XrdCl::File* file = GetFile( fildes );
  XrdCl::XRootDStatus status = file->Close();

  RemoveMapping( fildes );
  delete file;

  return status.errNo;
}


//------------------------------------------------------------------------------
int
xrd_flush(int fd, unsigned long long inode)
{
  int errc = 0;
  eos_static_info("fd=%d ", fd);
  
  if (XFC && inode) {
    FileAbstraction* fAbst = XFC->getFileObj(inode, false);
    if (fAbst) {
      XFC->waitFinishWrites(*fAbst);
      ConcurrentQueue<error_type> err_queue = fAbst->getErrorQueue();
      error_type error;
      
      if ( err_queue.try_pop( error ) ) {
        eos_static_info("Extract error from queue ");
        errc = error.first;
      }
      
      fAbst->decrementNoReferences();
    }
  }

  return errc;
}


//------------------------------------------------------------------------------
int
xrd_truncate( int fildes, off_t offset, unsigned long inode )
{
  eos_static_info( "fd=%d offset=%llu inode=%lu", fildes, ( unsigned long long )offset, inode );

  if ( XFC && inode ) {
    XFC->waitFinishWrites( inode );
  }

  XrdCl::File* file = GetFile( fildes );
  XrdCl::XRootDStatus status = file->Truncate( offset );

  return status.errNo;
}


/*
//------------------------------------------------------------------------------
ssize_t
xrd_read( int fildes, void* buf, size_t nbyte, unsigned long inode )
{
  eos_static_info( "fd=%d nbytes=%lu inode=%lu", fildes, ( unsigned long )nbyte, ( unsigned long )inode );
  uint32_t ret = 0;
  FileAbstraction* fAbst = 0;

  if ( XFC && fuse_cache_read && inode ) {
    fAbst = XFC->getFileObj( inode, true );
    XFC->waitFinishWrites( *fAbst );
    off_t offset = XrdPosixXrootd::Lseek( fildes, 0, SEEK_SET );

    if ( ( ret = XFC->getRead( *fAbst, buf, offset, nbyte ) ) != nbyte ) {
      ret = XrdPosixXrootd::Read( fildes, buf, nbyte );
    }

    fAbst->decrementNoReferences();
  } else {
    ret = XrdPosixXrootd::Read( fildes, buf, nbyte );
  }

  return ret;
}
*/

//------------------------------------------------------------------------------
ssize_t
xrd_pread( int fildes, void* buf, size_t nbyte, off_t offset, unsigned long inode )
{
  eos::common::Timing xpr( "xrd_pread" );
  COMMONTIMING( "start", &xpr );

  eos_static_debug( "fd=%d nbytes=%lu offset=%llu inode=%lu",
                    fildes, ( unsigned long )nbyte,
                    ( unsigned long long )offset,
                    ( unsigned long ) inode );

  uint32_t ret;

  if ( XFC && fuse_cache_read && inode ) {
    FileAbstraction* fAbst = 0;
    fAbst = XFC->getFileObj( inode, true );
    XFC->waitFinishWrites( *fAbst );
    COMMONTIMING( "wait writes", &xpr );

    if ( ( ret = XFC->getRead( *fAbst, buf, offset, nbyte ) ) != nbyte ) {
      COMMONTIMING( "read in", &xpr );
      eos_static_debug( "Block not found in cache: off=%zu, len=%zu", offset, nbyte );

      XrdCl::File* file = GetFile( fildes );
      XrdCl::XRootDStatus status = file->Read( offset, nbyte, buf, ret );

      TIMING( "read out", &xpr );
      XFC->putRead( *fAbst, fildes, buf, offset, nbyte );
      COMMONTIMING( "put read", &xpr );
    } else {
      eos_static_debug( "Block found in cache: off=%zu, len=%zu", offset, nbyte );
      COMMONTIMING( "block in cache", &xpr );
    }

    fAbst->decrementNoReferences();
  } else {
    XrdCl::File* file = GetFile( fildes );
    XrdCl::XRootDStatus status = file->Read( offset, nbyte, buf, ret );
  }

  COMMONTIMING( "end", &xpr );

  if ( EOS_LOGS_DEBUG ) {
    xpr.Print();
  }

  return ret;
}


/*
//------------------------------------------------------------------------------
ssize_t
xrd_write( int fildes, const void* buf, size_t nbyte, unsigned long inode )
{
  eos_static_info( "fd=%d nbytes=%lu inode=%lu", fildes,
                   ( unsigned long )nbyte, ( unsigned long ) inode );
  size_t ret;

  if ( XFC && fuse_cache_write && inode ) {
    off_t offset = XrdPosixXrootd::Lseek( fildes, 0, SEEK_SET );
    XFC->submitWrite( inode, fildes, const_cast<void*>( buf ), offset, nbyte );
    ret = nbyte;
  } else {
    ret = XrdPosixXrootd::Write( fildes, buf, nbyte );
  }

  return ret;
}
*/


//------------------------------------------------------------------------------
ssize_t
xrd_pwrite( int fildes, const void* buf, size_t nbyte, off_t offset, unsigned long inode )
{
  eos::common::Timing xpw( "xrd_pwrite" );
  COMMONTIMING( "start", &xpw );

  eos_static_debug( "fd=%d nbytes=%lu inode=%lu cache=%d cache-w=%d",
                    fildes, ( unsigned long )nbyte, ( unsigned long ) inode,
                    XFC ? 1 : 0, fuse_cache_write );
  uint32_t ret = 0;

  if ( XFC && fuse_cache_write && inode ) {
    XFC->submitWrite( inode, fildes, const_cast<void*>( buf ), offset, nbyte );
    ret = nbyte;
  } else {
    XrdCl::File* file = GetFile( fildes );
    XrdCl::XRootDStatus status = file->Write( offset, nbyte, const_cast<void*>( buf ), ret );
  }

  COMMONTIMING( "end", &xpw );

  if ( EOS_LOGS_DEBUG ) {
    xpw.Print();
  }

  return ret;
}


//------------------------------------------------------------------------------
int
xrd_fsync( int fildes, unsigned long inode )
{
  eos_static_info( "fd=%d inode=%lu", fildes, ( unsigned long )inode );

  if ( XFC && inode ) {
    XFC->waitFinishWrites( inode );
  }

  XrdCl::File* file = GetFile( fildes );
  XrdCl::XRootDStatus status = file->Sync();

  return status.errNo;
}


//------------------------------------------------------------------------------
int xrd_unlink( const char* path )
{
  eos_static_info( "path=%s", path );
  XrdCl::XRootDStatus status = fs->Rm( path );
  return status.errNo;
}


//------------------------------------------------------------------------------
int xrd_rename( const char* oldpath, const char* newpath )
{
  eos_static_info( "oldpath=%s newpath=%s", oldpath, newpath );
  XrdCl::XRootDStatus status = fs->Mv( oldpath, newpath );
  return status.errNo;
}

//------------------------------------------------------------------------------
const char*
xrd_mapuser( uid_t uid )
{
  eos_static_debug( "uid=%lu", ( unsigned long ) uid );
  struct passwd* pw;
  XrdOucString sid = "";
  XrdOucString* spw = NULL;
  sid += ( int )( uid );
  passwdstoremutex.Lock();

  if ( !( spw = passwdstore->Find( sid.c_str() ) ) ) {
    pw = getpwuid( uid );

    if ( pw ) {
      spw = new XrdOucString( pw->pw_name );
      passwdstore->Add( sid.c_str(), spw, 60 );
      passwdstoremutex.UnLock();
    } else {
      passwdstoremutex.UnLock();
      return NULL;
    }
  }

  passwdstoremutex.UnLock();

  // ----------------------------------------------------------------------------------
  // setup the default locations for GSI authentication and KRB5 Authentication
  XrdOucString userproxy  = "/tmp/x509up_u";
  XrdOucString krb5ccname = "/tmp/krb5cc_";
  userproxy  += ( int ) uid;
  krb5ccname += ( int ) uid;
  setenv( "X509_USER_PROXY",  userproxy.c_str(), 1 );
  setenv( "KRB5CCNAME", krb5ccname.c_str(), 1 );
  // ----------------------------------------------------------------------------------

  return STRINGSTORE( spw->c_str() );
}

//------------------------------------------------------------------------------
const char* xrd_get_dir( DIR* dp, int entry )
{
  return 0;
}

void
xrd_init()
{
  FILE* fstderr ;

  // open a log file
  if ( getuid() ) {
    char logfile[1024];
    snprintf( logfile, sizeof( logfile ) - 1, "/tmp/eos-fuse.%d.log", getuid() );
    // running as a user ... we log into /tmp/eos-fuse.$UID.log

    if ( !( fstderr = freopen( logfile, "a+", stderr ) ) ) {
      fprintf( stderr, "error: cannot open log file %s\n", logfile );
    }
  } else {
    // running as root ... we log into /var/log/eos/fuse
    eos::common::Path cPath( "/var/log/eos/fuse/fuse.log" );
    cPath.MakeParentPath( S_IRWXU | S_IRGRP | S_IROTH );

    if ( !( fstderr = freopen( cPath.GetPath(), "a+", stderr ) ) ) {
      fprintf( stderr, "error: cannot open log file %s\n", cPath.GetPath() );
    }
  }

  setvbuf( fstderr, ( char* ) NULL, _IONBF, 0 );

  // initialize hashes
  path2inode.set_empty_key( "" );
  inode2path.set_empty_key( 0 );
  dir2inodelist.set_empty_key( 0 );
  dir2dirbuf.set_empty_key( 0 );
  inode2cache.set_empty_key( 0 );
  OpenPosixXrootdFd.set_empty_key( "" );
  map_fd_fobj.set_empty_key( -1 );

  path2inode.set_deleted_key( "#__deleted__#" );
  inode2path.set_deleted_key( 0xffffffffll );
  dir2inodelist.set_deleted_key( 0xffffffffll );
  dir2dirbuf.set_deleted_key( 0xffffffffll );
  inode2cache.set_deleted_key( 0xffffffffll );
  OpenPosixXrootdFd.set_deleted_key( "#__deleted__#" );
  map_fd_fobj.set_deleted_key( -2 );

  // create the root entry
  path2inode["/"] = 1;
  inode2path[1] = "/";

  eos::common::Mapping::VirtualIdentity_t vid;
  eos::common::Mapping::Root( vid );
  eos::common::Logging::Init();
  eos::common::Logging::SetUnit( "FUSE@localhost" );
  eos::common::Logging::gShortFormat = true;

  XrdOucString fusedebug = getenv( "EOS_FUSE_DEBUG" );

  if ( ( getenv( "EOS_FUSE_DEBUG" ) ) && ( fusedebug != "0" ) ) {
    eos::common::Logging::SetLogPriority( LOG_DEBUG );
  } else {
    eos::common::Logging::SetLogPriority( LOG_DEBUG );
  }

  EnvPutInt( "NAME_MAXREDIRECTCOUNT", 3 );
  EnvPutInt( "NAME_RECONNECTWAIT", 10 );

  setenv( "XRDPOSIX_POPEN", "1", 1 );

  // initialise the XrdClFileSystem object
  if ( fs ) {
    delete fs;
    fs = 0;
  }

  std::string address = "root://localhost:1094";
  XrdCl::URL url( address );

  if ( !url.IsValid() ) {
    eos_static_info( "URL is not valid. \n" );
  }

  fs = new XrdCl::FileSystem( url );

  if ( fs ) {
    eos_static_info( "Got new FileSystem object. \n" );
  }


  //initialise the XrdFileCache
  fuse_cache_read = false;
  fuse_cache_write = false;

  if ( !( getenv( "EOS_FUSE_CACHE" ) ) ) {
    eos_static_notice( "cache=false" );
    XFC = NULL;
  } else {
    if ( !getenv( "EOS_FUSE_CACHE_SIZE" ) ) {
      setenv( "EOS_FUSE_CACHE_SIZE", "30000000", 1 ); // ~300MB
    }

    eos_static_notice( "cache=true size=%s cache-read=%s, cache-write=%s",
                       getenv( "EOS_FUSE_CACHE_SIZE" ),
                       getenv( "EOS_FUSE_CACHE_READ" ),
                       getenv( "EOS_FUSE_CACHE_WRITE" ) );

    XFC = XrdFileCache::getInstance( static_cast<size_t>( atol( getenv( "EOS_FUSE_CACHE_SIZE" ) ) ) );

    if ( getenv( "EOS_FUSE_CACHE_READ" ) && atoi( getenv( "EOS_FUSE_CACHE_READ" ) ) ) {
      fuse_cache_read = true;
    }

    if ( getenv( "EOS_FUSE_CACHE_WRITE" ) && atoi( getenv( "EOS_FUSE_CACHE_WRITE" ) ) ) {
      fuse_cache_write = true;
    }
  }

  passwdstore = new XrdOucHash<XrdOucString> ();
  stringstore = new XrdOucHash<XrdOucString> ();
}


