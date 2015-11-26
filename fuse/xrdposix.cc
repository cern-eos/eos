//------------------------------------------------------------------------------
// File: xrdposix.cc
// Author: Elvin-Alin Sindrilaru / Andreas-Joachim Peters - CERN
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
#include "ProcCacheC.h"
#include "fst/layout/LayoutPlugin.hh"
#include "fst/layout/PlainLayout.hh"
#include "fst/layout/RaidDpLayout.hh"
#include "fst/layout/ReedSLayout.hh"
/*----------------------------------------------------------------------------*/
#include <climits>
#include <cstdlib>
#include <queue>
#include <sstream>
#include <string>
#include <stdint.h>
#include <iostream>
#include <libgen.h>
#include <pwd.h>
#include <string.h>
#include <pthread.h>
#include <algorithm>
#include <limits>
/*----------------------------------------------------------------------------*/
#include "FuseCache/FuseWriteCache.hh"
#include "FuseCache/FileAbstraction.hh"
#include "FuseCache/LayoutWrapper.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTable.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "common/Path.hh"
#include "common/RWMutex.hh"
#include "common/SymKeys.hh"
#include "common/Macros.hh"
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

static FuseWriteCache* XFC;
static XrdOucHash<XrdOucString>* passwdstore;
static XrdOucHash<XrdOucString>* stringstore;

XrdSysMutex passwdstoremutex;
XrdSysMutex stringstoremutex;
XrdSysMutex environmentmutex;
XrdSysMutex connectionIdMutex;
std::vector<RWMutex> proccachemutexes;
uint64_t pid_max;
uint64_t uid_max;
int connectionId = 0;

bool link_pidmap; ///< indicated if mapping between pid and strong authentication is symlinked in /var/run/eosd/credentials/pidXXX
bool use_user_krb5cc; ///< indicated if user krb5cc file should be used for authentication
bool use_user_gsiproxy; ///< indicated if user gsi proxy should be used for authentication
bool tryKrb5First; ///< indicated if Krb5 should be tried before Gsi
bool do_rdahead = false; ///< true if readahead is to be enabled, otherwise false
std::string rdahead_window = "131072"; ///< size of the readahead window
int rm_level_protect; ///< number of levels in hierarchy protected against rm -rf
std::string rm_command; ///< full path of the system rm command (e.g. "/bin/rm" )
bool rm_watch_relpath; ///< indicated if the system rm command changes it CWD making relative path expansion impossible
int fuse_cache_write; ///< 0 if fuse cache write disabled, otherwise 1
bool fuse_exec = false; ///< indicates if files should be make exectuble
bool fuse_shared = false; ///< indicated if this is eosd = true or eosfsd = false
int use_localtime_consistency = 0; ///< indicated if this stat and get attr shou
extern "C" char* local_mount_dir;
XrdOucString gMgmHost; ///< host name of the FUSE contact point

using eos::common::LayoutId;

void xrd_logdebug(const char *msg)
{
  eos_static_debug(msg);
}

char *
myrealpath (const char * __restrict path, char * __restrict resolved, pid_t pid);

//------------------------------------------------------------------------------
// String store
//------------------------------------------------------------------------------

char*
STRINGSTORE (const char* __charptr__)
{
  XrdOucString* yourstring;

  if (!__charptr__) return (char*) "";

  if ((yourstring = stringstore->Find(__charptr__)))
  {
    return ( (char*) yourstring->c_str());
  }
  else
  {
    XrdOucString* newstring = new XrdOucString(__charptr__);
    stringstoremutex.Lock();
    stringstore->Add(__charptr__, newstring);
    stringstoremutex.UnLock();
    return (char*) newstring->c_str();
  }
}

const char* xrd_user_url (uid_t uid, gid_t gid, pid_t pid);
const char* xrd_strongauth_cgi (pid_t pid);

//------------------------------------------------------------------------------
//             ******* Implementation Translations *******
//------------------------------------------------------------------------------

// Protecting the path/inode translation table
eos::common::RWMutex mutex_inode_path;

// Mapping path name to inode
google::dense_hash_map<std::string, unsigned long long> path2inode;

// Mapping inode to path name
google::dense_hash_map<unsigned long long, std::string> inode2path;

//------------------------------------------------------------------------------
// Lock read
//------------------------------------------------------------------------------

void
xrd_lock_r_p2i ()
{
  mutex_inode_path.LockRead();
}


//------------------------------------------------------------------------------
// Unlock read
//------------------------------------------------------------------------------

void
xrd_unlock_r_p2i ()
{
  mutex_inode_path.UnLockRead();
}

//------------------------------------------------------------------------------
// Lock
//------------------------------------------------------------------------------

void
xrd_lock_r_pcache (pid_t pid)
{
  proccachemutexes[pid].LockRead();
}

void
xrd_lock_w_pcache (pid_t pid)
{
  proccachemutexes[pid].LockWrite();
}


//------------------------------------------------------------------------------
// Unlock
//------------------------------------------------------------------------------

void
xrd_unlock_r_pcache (pid_t pid)
{
  proccachemutexes[pid].UnLockRead();
}

void
xrd_unlock_w_pcache (pid_t pid)
{
  proccachemutexes[pid].UnLockWrite();
}



//------------------------------------------------------------------------------
// Drop the basename and return only the last level path name
//------------------------------------------------------------------------------

char*
xrd_basename (unsigned long long inode)
{
  eos::common::RWMutexReadLock vLock(mutex_inode_path);
  const char* fname = xrd_path(inode);

  if (fname)
  {
    std::string spath = fname;
    size_t len = spath.length();

    if (len)
    {
      if (spath[len - 1] == '/')
      {
        spath.erase(len - 1);
      }
    }

    size_t spos = spath.rfind("/");

    if (spos != std::string::npos)
    {
      spath.erase(0, spos + 1);
    }

    return static_cast<char*> (STRINGSTORE(spath.c_str()));
  }

  return 0;
}

//----------------------------------------------------------------------------
//! Return the CGI of an URL
//----------------------------------------------------------------------------

const char*
get_cgi (const char* url)
{
  return url ? (strchr(url, '?')) : 0;
}

//----------------------------------------------------------------------------
//! Return the CGI of an URL
//----------------------------------------------------------------------------

XrdOucString
get_url_nocgi (const char* url)
{
  XrdOucString surl = url;
  surl.erase(surl.find("?"));
  return surl;
}

//------------------------------------------------------------------------------
// Translate from inode to path
//------------------------------------------------------------------------------

const char*
xrd_path (unsigned long long inode)
{
  // Obs: use xrd_lock_r_p2i/xrd_unlock_r_p2i in the scope of the returned string
  if (inode2path.count(inode))
    return inode2path[inode].c_str();
  else
    return 0;
}


//------------------------------------------------------------------------------
// Translate from path to inode
//------------------------------------------------------------------------------

unsigned long long
xrd_inode (const char* path)
{
  eos::common::RWMutexReadLock rd_lock(mutex_inode_path);
  unsigned long long ret = 0;

  if (path2inode.count(path))
    ret = path2inode[path];
  return ret;
}


//------------------------------------------------------------------------------
// Store an inode <-> path mapping
//------------------------------------------------------------------------------

void
xrd_store_p2i (unsigned long long inode, const char* path)
{
  eos::common::RWMutexWriteLock wr_lock(mutex_inode_path);
  path2inode[path] = inode;
  inode2path[inode] = path;
}


//------------------------------------------------------------------------------
// Store an inode <-> path mapping given the parent inode
//------------------------------------------------------------------------------

void
xrd_store_child_p2i (unsigned long long inode,
                     unsigned long long childinode,
                     const char* name)
{
  eos::common::RWMutexWriteLock wr_lock(mutex_inode_path);
  std::string fullpath = inode2path[inode];
  std::string sname = name;

  eos_static_debug("parent_inode=%llu, child_inode=%llu, name=%s, fullpath=%s",
                   inode, childinode, name, fullpath.c_str());

  if (sname != ".")
  {
    // we don't need to store this one
    if (sname == "..")
    {
      if (inode == 1)
      {
        fullpath = "/";
      }
      else
      {
        size_t spos = fullpath.find("/");
        size_t bpos = fullpath.rfind("/");

        if ((spos != std::string::npos) && (spos != bpos))
        {
          fullpath.erase(bpos);
        }
      }
    }
    else
    {
      if (*fullpath.rbegin() != '/')
        fullpath += "/";

      fullpath += name;
    }

    eos_static_debug("sname=%s fullpath=%s inode=%llu childinode=%llu ",
                     sname.c_str(), fullpath.c_str(), inode, childinode);
    path2inode[fullpath] = childinode;
    inode2path[childinode] = fullpath;
  }
}


//------------------------------------------------------------------------------
// Delete an inode <-> path mapping given the inode
//------------------------------------------------------------------------------

void
xrd_forget_p2i (unsigned long long inode)
{
  eos::common::RWMutexWriteLock wr_lock(mutex_inode_path);

  if (inode2path.count(inode))
  {
    std::string path = inode2path[inode];

    path2inode.erase(path);
    inode2path.erase(inode);
  }
}



//------------------------------------------------------------------------------
//      ******* Implementation of the directory listing table *******
//------------------------------------------------------------------------------

// Protecting the directory listing table
eos::common::RWMutex mutex_dir2inodelist;

// Dir listing map
google::dense_hash_map<unsigned long long, std::vector<unsigned long long> > dir2inodelist;
google::dense_hash_map<unsigned long long, struct dirbuf> dir2dirbuf;


//------------------------------------------------------------------------------
// Lock read
//------------------------------------------------------------------------------

void
xrd_lock_r_dirview ()
{
  mutex_dir2inodelist.LockRead();
}

//------------------------------------------------------------------------------
// Unlock read
//------------------------------------------------------------------------------

void
xrd_unlock_r_dirview ()
{
  mutex_dir2inodelist.UnLockRead();
}


//------------------------------------------------------------------------------
// Lock write
//------------------------------------------------------------------------------

void
xrd_lock_w_dirview ()
{
  mutex_dir2inodelist.LockWrite();
}


//------------------------------------------------------------------------------
// Unlock write
//------------------------------------------------------------------------------

void
xrd_unlock_w_dirview ()
{
  mutex_dir2inodelist.UnLockWrite();
}


//------------------------------------------------------------------------------
// Create a new entry in the maps for the current inode (directory)
//------------------------------------------------------------------------------

void
xrd_dirview_create (unsigned long long inode)
{
  eos_static_debug("inode=%llu", inode);
  //Obs: path should be attached beforehand into path translation
  eos::common::RWMutexWriteLock vLock(mutex_dir2inodelist);
  dir2inodelist[inode].clear();
  dir2dirbuf[inode].p = 0;
  dir2dirbuf[inode].size = 0;
}


//------------------------------------------------------------------------------
// Delete entry from maps for current inode (directory)
//------------------------------------------------------------------------------

void
xrd_dirview_delete (unsigned long long inode)
{
  eos_static_debug("inode=%llu", inode);
  eos::common::RWMutexWriteLock wr_lock(mutex_dir2inodelist);

  if (dir2inodelist.count(inode))
  {
    if (dir2dirbuf[inode].p)
    {
      free(dir2dirbuf[inode].p);
    }

    dir2dirbuf.erase(inode);
    dir2inodelist[inode].clear();
    dir2inodelist.erase(inode);
  }
}


//------------------------------------------------------------------------------
// Get entry's inode with index 'index' from directory
//------------------------------------------------------------------------------
unsigned long long
xrd_dirview_entry (unsigned long long dirinode,
                   size_t index,
                   int get_lock)
{
  eos_static_debug("dirinode=%llu, index=%zu", dirinode, index);

  if (get_lock) eos::common::RWMutexReadLock rd_lock(mutex_dir2inodelist);

  if ((dir2inodelist.count(dirinode)) &&
      (dir2inodelist[dirinode].size() > index))
    return dir2inodelist[dirinode][index];

  return 0;
}


//------------------------------------------------------------------------------
// Get dirbuf corresponding to inode
//------------------------------------------------------------------------------
struct dirbuf*
xrd_dirview_getbuffer (unsigned long long inode, int get_lock)
{
  if (get_lock) eos::common::RWMutexReadLock rd_lock(mutex_dir2inodelist);

  if (dir2dirbuf.count(inode))
    return &dir2dirbuf[inode];
  else
    return 0;
}


//------------------------------------------------------------------------------
//      ******* Implementation of the FUSE directory cache *******
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Get maximum number of directories in cache
//------------------------------------------------------------------------------

static const unsigned long long
GetMaxCacheSize ()
{
  return 1024;
}

// Protecting the cache entry map
eos::common::RWMutex mutex_fuse_cache;

// Directory cache
google::dense_hash_map<unsigned long long, FuseCacheEntry*> inode2cache;


//------------------------------------------------------------------------------
// Get a cached directory
//------------------------------------------------------------------------------
int
xrd_dir_cache_get (unsigned long long inode,
                   struct timespec mtime,
                   struct dirbuf** b)
{
  int retc = 0;
  FuseCacheEntry* dir = 0;
  eos::common::RWMutexReadLock rd_lock(mutex_fuse_cache);

  if (inode2cache.count(inode) && (dir = inode2cache[inode]))
  {
    struct timespec oldtime = dir->GetModifTime();

    if ((oldtime.tv_sec == mtime.tv_sec) &&
        (oldtime.tv_nsec == mtime.tv_nsec))
    {
      // Dir in cache and valid
      *b = static_cast<struct dirbuf*> (calloc(1, sizeof ( dirbuf)));
      dir->GetDirbuf(*b);
      retc = 1; // found
    }
  }

  return retc;
}


//------------------------------------------------------------------------------
// Forget a cached directory
//------------------------------------------------------------------------------
int
xrd_dir_cache_forget (unsigned long long inode)
{
  eos::common::RWMutexWriteLock wr_lock(mutex_fuse_cache);

  if (inode2cache.count(inode))
  {
    inode2cache.erase(inode);
    return true;
  }
  return false;
}

//------------------------------------------------------------------------------
// Add or update a cache directory entry
//------------------------------------------------------------------------------
void
xrd_dir_cache_sync (unsigned long long inode,
                    int nentries,
                    struct timespec mtime,
                    struct dirbuf* b)
{
  eos::common::RWMutexWriteLock wr_lock(mutex_fuse_cache);
  FuseCacheEntry* dir = 0;

  if ((inode2cache.count(inode)) && (dir = inode2cache[inode]))
  {
    dir->Update(nentries, mtime, b);
  }
  else
  {
    // Add new entry
    if (inode2cache.size() >= GetMaxCacheSize())
    {
      // Size control of the cache
      unsigned long long indx = 0;
      unsigned long long entries_del =
        static_cast<unsigned long long> (0.25 * GetMaxCacheSize());
      google::dense_hash_map<unsigned long long, FuseCacheEntry*>::iterator iter;
      iter = inode2cache.begin();

      while ((indx <= entries_del) && (iter != inode2cache.end()))
      {
        dir = (FuseCacheEntry*) iter->second;
        inode2cache.erase(iter++);
        delete dir;
        indx++;
      }
    }

    dir = new FuseCacheEntry(nentries, mtime, b);
    inode2cache[inode] = dir;
  }

  return;
}


//------------------------------------------------------------------------------
// Get a subentry from a cached directory
//------------------------------------------------------------------------------
int
xrd_dir_cache_get_entry (fuse_req_t req,
                         unsigned long long inode,
                         unsigned long long entry_inode,
                         const char* efullpath)
{
  int retc = 0;
  eos::common::RWMutexReadLock rd_lock(mutex_fuse_cache);
  FuseCacheEntry* dir;

  if ((inode2cache.count(inode)) && (dir = inode2cache[inode]))
  {
    if (dir->IsFilled())
    {
      struct fuse_entry_param e;
      if (dir->GetEntry(entry_inode, e))
      {
        xrd_store_p2i(entry_inode, efullpath);
        fuse_reply_entry(req, &e);
        retc = 1; // found
      }
    }
  }

  return retc;
}


//------------------------------------------------------------------------------
// Add new subentry to a cached directory
//------------------------------------------------------------------------------
void
xrd_dir_cache_add_entry (unsigned long long inode,
                         unsigned long long entry_inode,
                         struct fuse_entry_param* e)
{
  eos::common::RWMutexReadLock rd_lock(mutex_fuse_cache);
  FuseCacheEntry* dir = 0;

  if ((inode2cache.count(inode)) && (dir = inode2cache[inode]))
    dir->AddEntry(entry_inode, e);
}



//------------------------------------------------------------------------------
//      ******* Implementation of the open File Descriptor map *******
//------------------------------------------------------------------------------

// Map used for associating file descriptors with XrdCl::File objects
eos::common::RWMutex rwmutex_fd2fabst;
google::dense_hash_map<int, FileAbstraction*> fd2fabst;

// Map <inode, user> to a file descriptor
google::dense_hash_map<std::string, int> inodexrdlogin2fd;
// Helper function to construct a key in the previous map
std::string get_xrd_login(uid_t uid, gid_t gid, pid_t pid);

// Pool of available file descriptors
int base_fd = 1;
std::queue<int> pool_fd;


//------------------------------------------------------------------------------
// Create artificial file descriptor
//------------------------------------------------------------------------------
int
xrd_generate_fd ()
{
  int retc = -1;

  if (!pool_fd.empty())
  {
    retc = pool_fd.front();
    pool_fd.pop();
  }
  else if (base_fd < INT_MAX)
  {
    base_fd++;
    retc = base_fd;
  }
  else
  {
    eos_static_err("no more file descirptors available.");
    retc = -1;
  }

  return retc;
}


//------------------------------------------------------------------------------
// Add new mapping between fd and raw file object
//------------------------------------------------------------------------------
int
xrd_add_fd2file (LayoutWrapper* raw_file,
                 unsigned long inode,
                 uid_t uid, gid_t gid, pid_t pid ,
                 const char* path="")
{
  eos_static_debug("file raw ptr=%p, inode=%lu, uid=%lu",
                   raw_file, inode, (unsigned long) uid);
  int fd = -1;
  std::ostringstream sstr;
  sstr << inode << ":" << get_xrd_login(uid,gid,pid);

  eos::common::RWMutexWriteLock wr_lock(rwmutex_fd2fabst);
  auto iter_fd = inodexrdlogin2fd.find(sstr.str());

  // If there is already an entry for the current user and the current inode
  // then we return the old fd
  while (!raw_file)
  {
    if (iter_fd != inodexrdlogin2fd.end())
    {
      eos_static_warning("inodeuid mapping exists, just return old fd=%i",
                         iter_fd->second);
      fd = iter_fd->second;
      auto iter_file = fd2fabst.find(fd);

      if (iter_file != fd2fabst.end())
        iter_file->second->IncNumOpen();
      else
      {
        eos_static_err("fd=%i not found in fd2fobj map", fd);
        FileAbstraction* fabst = new FileAbstraction(fd, raw_file, path);
	fd2fabst[fd] = fabst;
      }
    }
    return fd;
  }

  fd = xrd_generate_fd();
  
  if (fd > 0)
  {
    FileAbstraction* fabst = new FileAbstraction(fd, raw_file, path);
    fd2fabst[fd] = fabst;
    inodexrdlogin2fd[sstr.str()] = fd;
  }
  else
  {
    eos_static_err("error while getting file descriptor");
    delete raw_file;
  }

  return fd;
}


//------------------------------------------------------------------------------
// Get the file abstraction object corresponding to the fd
//------------------------------------------------------------------------------
FileAbstraction*
xrd_get_file (int fd)
{
  eos_static_debug("fd=%i", fd);
  eos::common::RWMutexReadLock rd_lock(rwmutex_fd2fabst);
  auto iter = fd2fabst.find(fd);

  if (iter == fd2fabst.end())
  {
    eos_static_err("no file abst for fd=%i", fd);
    return 0;
  }

  iter->second->IncNumRef();
  return iter->second;
}


//------------------------------------------------------------------------------
// Remove entry from mapping
//------------------------------------------------------------------------------
int
xrd_remove_fd2file (int fd, unsigned long inode, uid_t uid, gid_t gid, pid_t pid)
{
  int retc = -1;
  eos_static_debug("fd=%i, inode=%lu", fd, inode);
  eos::common::RWMutexWriteLock wr_lock(rwmutex_fd2fabst);
  auto iter = fd2fabst.find(fd);

  if (iter != fd2fabst.end())
  {
    FileAbstraction* fabst = iter->second;

    if (!fabst->IsInUse())
    {
      eos_static_debug("fd=%i is not in use, remove it", fd);
      LayoutWrapper* raw_file = fabst->GetRawFile();

      if(raw_file->IsOpen())
      {
        retc = raw_file->Close ();
        struct timespec utimes[2];
        const char* path = 0;
        if ((path = fabst->GetUtimes (utimes)))
        {
          // run the utimes command now after the close
          xrd_utimes (path, utimes, uid, gid, pid);
        }
      }
      delete fabst;
      fabst = 0;
      fd2fabst.erase(iter);

      // Remove entry also from the inodeuser2fd
      std::ostringstream sstr;
      sstr << inode << ":" << get_xrd_login(uid,gid,pid);
      auto iter1 = inodexrdlogin2fd.find(sstr.str());

      if (iter1 != inodexrdlogin2fd.end())
        inodexrdlogin2fd.erase(iter1);
      else
      {
	// if a file is repaired during an RW open, the inode can change and we find the fd in a different inode
	// search the map for the filedescriptor and remove it
	for (iter1 = inodexrdlogin2fd.begin(); iter1 != inodexrdlogin2fd.end(); ++iter1)
	{
	  if (iter1->second == fd)
	  {
	    inodexrdlogin2fd.erase(iter1);
	    break;
	  }
	}
      }
      // Return fd to the pool
      pool_fd.push(fd);
    }
    else
    {
      eos_static_debug("fd=%i is still in use, cannot remove", fd);
      // Decrement number of references - so that the last process can
      // properly close the file
      fabst->DecNumRef();
      fabst->DecNumOpen();
    }
  }
  else
    eos_static_warning("fd=%i no long in map, maybe already closed ...", fd);

  return retc;
}


//------------------------------------------------------------------------------
//        ******* Implementation IO Buffer Management *******
//------------------------------------------------------------------------------

// Forward declaration
class IoBuf;

// Protecting the IO buffer map
XrdSysMutex IoBufferLock;

// IO buffer table. Each fuse thread has its own read buffer
std::map<pthread_t, IoBuf> IoBufferMap;


//------------------------------------------------------------------------------
//! Class IoBuf
//------------------------------------------------------------------------------
class IoBuf
{
private:
  void* buffer;
  size_t size;

public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  IoBuf ()
  {
    buffer = 0;
    size = 0;
  }


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual
  ~IoBuf ()
  {
    if (buffer && size) free(buffer);
  }


  //----------------------------------------------------------------------------
  //! Get buffer
  //----------------------------------------------------------------------------
  char*
  GetBuffer ()
  {
    return (char*) buffer;
  }


  //----------------------------------------------------------------------------
  //! Get size of buffer
  //----------------------------------------------------------------------------
  size_t
  GetSize ()
  {
    return size;
  }

  //----------------------------------------------------------------------------
  //! Resize buffer
  //----------------------------------------------------------------------------
  void
  Resize (size_t newsize)
  {
    if (newsize > size)
    {
      size = (newsize < (128 * 1024)) ? 128 * 1024 : newsize;
      buffer = realloc(buffer, size);
    }
  }
};


//------------------------------------------------------------------------------
// Guarantee a buffer for reading of at least 'size' for the specified thread
//------------------------------------------------------------------------------
char*
xrd_attach_rd_buff (pthread_t tid, size_t size)
{
  XrdSysMutexHelper lock(IoBufferLock);
  IoBufferMap[tid].Resize(size);
  return (char*) IoBufferMap[tid].GetBuffer();
}


//------------------------------------------------------------------------------
// Release read buffer corresponding to the thread
//------------------------------------------------------------------------------
void
xrd_release_rd_buff (pthread_t tid)
{
  XrdSysMutexHelper lock(IoBufferLock);
  IoBufferMap.erase(tid);
  return;
}


//------------------------------------------------------------------------------
//             ******* XROOTD connection/authentication functions *******
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Get user name from the uid and change the effective user ID of the thread
//------------------------------------------------------------------------------
const char*
xrd_mapuser (uid_t uid, gid_t gid, pid_t pid, uint8_t authid)
{
  eos_static_debug("uid=%lu gid=%lu pid=%lu",
                   (unsigned long) uid,
                   (unsigned long) gid,
                   (unsigned long) pid);

  XrdOucString sid = "";

  if (uid == 0)
  {
    uid = gid = 2;
  }

  unsigned long long bituser=0;

  // Emergency mapping of too high user ids to nob
  if ( uid > 0xfffff)
  {
    eos_static_err("msg=\"unable to map uid - out of 20-bit range - mapping to "
                   "nobody\" uid=%u", uid);
    uid = 99;
  }
  if ( gid > 0xffff)
  {
    eos_static_err("msg=\"unable to map gid - out of 16-bit range - mapping to "
                   "nobody\" gid=%u", gid);
    gid = 99;
  }

  bituser = (uid & 0xfffff);
  bituser <<= 16;
  bituser |= (gid & 0xffff);
  bituser <<= 6;
  if(use_user_gsiproxy || use_user_krb5cc)
  {
    // if using strong authentication, the 6 bits are used to map different strong ids to the same uid
    // if recoonection is needed, it goes through the authidmanager
    bituser |= (authid & 0x3f);
  }
  else
  {
    // if using the gateway node, the purpose of the reamining 6 bits is just a connection counter to be able to reconnect
    XrdSysMutexHelper cLock(connectionIdMutex);
    if (connectionId)
      bituser |= (connectionId & 0x3f);
  }

  bituser = h_tonll(bituser);

  XrdOucString sb64;
  // WARNING: we support only one endianess flavour by doing this
  eos::common::SymKey::Base64Encode ( (char*) &bituser, 8 , sb64);
  size_t len = sb64.length();
  // Remove the non-informative '=' in the end
  if (len >2)
  {
    sb64.erase(len-1);
    len--;
  }

  // Reduce to 7 b64 letters
  if (len > 7)
    sb64.erase(0, len - 7);

  sid = "*";
  sid += sb64;

  // Encode '/' -> '_', '+' -> '-' to ensure the validity of the XRootD URL
  // if necessary.
  sid.replace('/', '_');
  sid.replace('+', '-');
  eos_static_debug("user-ident=%s", sid.c_str());
  return STRINGSTORE(sid.c_str());
}

//------------------------------------------------------------------------------
// Helper structure to get the xrootd login from uid, gid and authid when using secured authentication
// each user can use up to 64 different ids simultaneously (krb5 and gsi cummulated)
//------------------------------------------------------------------------------
struct map_user
{
  uid_t uid;
  gid_t gid;
  uint8_t authid;
  // first 20 bits for user id
  // 16 following bites for authid
  // 6 following bits for auth id (identity)
  char base64buf[9];
  bool base64computed;
  static const uint16_t sMaxAuthId;
  map_user (uid_t _uid, gid_t _gid, uint8_t _authid) :
      uid (_uid), gid(_gid), authid(_authid), base64computed(false)
  {
  }

  char*
  base64 ()
  {
    if (!base64computed)
    {
      // pid is actually meaningless
      strncpy(base64buf, xrd_mapuser (uid, gid, 0,authid),8);
      base64buf[8]=0;
      base64computed = true;
    }
    return base64buf;
  }
};

const uint16_t map_user::sMaxAuthId = 2^6;

//------------------------------------------------------------------------------
// Class in charge of managing the xroot login (i.e. xroot connection)
// logins are 8 characters long : ABgE73AA23@myrootserver
// it's base 64 , first 6 are userid and 2 lasts are authid
// authid is an idx a pool of identities for the specified user
// if the user comes with a new identity, it's added the pool
// if the identity is already in the pool, the connection is reused
// identity are NEVER removed from the pool
// for a given identity, the SAME conneciton is ALWAYS reused
//------------------------------------------------------------------------------
class AuthIdManager
{
public:
  enum CredType {krb5,krk5,x509};
  struct CredInfo
  {
    CredType type;     // krb5 , krk5 or x509
    std::string lname; // link to credential file
    std::string fname; // credential file
    time_t fmtime;     // credential file mtime
    time_t fctime;     // credential file ctime
    time_t lmtime;     // link to credential file mtime
    time_t lctime;     // link to credential file mtime
    std::string identity; // identity in the credential file
    std::string cachedStrongLogin;
  };

protected:
  friend void xrd_init (); // to allow the sizing of pid2StrongLogin
  // mutex protecting the maps
  RWMutex pMutex;
  // maps (userid,sessionid) -> ( credinfo )
  // several threads (each from different process) might concurrently access this
  std::map< std::pair<uid_t,pid_t> , CredInfo > uidsid2credinfo;
  struct IdenAuthIdEntry {
    std::map<std::string, uint32_t> strongid2authid;
    std::list<uint8_t> freeAuthIdPool;

    IdenAuthIdEntry()
    {
      // initialize the free authentication id pool with all the number from 0 to sMaxAuthId-1
      for(uint8_t i=0;i<map_user::sMaxAuthId;i++) freeAuthIdPool.push_back(i);
    }
  };
  // maps userid -> strongid2authid  : ( identity -> authid ) , identity being krb5:<some_identity> or krk5:<some_fileless_param> or gsi:<some_identity>
  //                freeAuthIdPool   : a list with all the available authid
  // several threads (each from different process) might concurrently access this
  std::map<uid_t, IdenAuthIdEntry > uid2IdenAuthid;
  // maps procid -> xrootd_login
  // only one thread per process will access this (protected by one mutex per process)
  std::vector<std::string> pid2StrongLogin;

  std::string
  symlinkCredentials (const std::string &authMethod, uid_t uid, std::string identity, const std::string &xrdlogin = "")
  {
    std::stringstream ss;
    size_t i = 0;
    // avoid characters that could mess up the link name
    while ((i = identity.find_first_of ("/", i)) != std::string::npos)
      identity[i] = '.';
    ss << "/var/run/eosd/credentials/u" << uid << "_" << identity;
    std::string linkname = ss.str ();
    auto colidx = authMethod.find (':');

    eos_static_debug("authmethod=%s",authMethod.c_str());

    if (xrdlogin.empty ())
    {
      std::string filename = authMethod.substr (colidx + 1, std::string::npos);
      if (filename.empty ()) return "";
      if (linkname != filename)
      {
        unlink (linkname.c_str ()); // remove the previous link first if any
        if (symlink (filename.c_str (), linkname.c_str ()))
        {
          eos_static_err("could not create symlink from %s to %s", filename.c_str (), linkname.c_str ());
          return "";
        }
      }
      return authMethod.substr (0, colidx + 1) + linkname;
    }
    else
    {
      ss.str ("");
      ss << "/var/run/eosd/credentials/xrd" << xrdlogin;
      std::string newlinkname = ss.str ();
      if (linkname != newlinkname)
      {
        unlink (newlinkname.c_str ()); // remove the previous link first if any
        if (symlink (linkname.c_str (), newlinkname.c_str ()))
        {
          eos_static_err("could not create new symlink from %s to %s", linkname.c_str (), newlinkname.c_str ());
          return "";
        }
      }
      return newlinkname;
    }
  }

  bool findCred (CredInfo &credinfo, struct stat &linkstat, struct stat &filestat, uid_t uid, pid_t sid, time_t & sst)
  {
    if (!(use_user_gsiproxy || use_user_krb5cc)) return false;

    bool ret = false;
    char buffer[1024];
    char buffer2[1024];
    const char* format = "/var/run/eosd/credentials/uid%d_sid%d_sst%d.%s";
    // krb5 -> kerberos 5 credential cache file
    // krk5 -> kerberos 5 credential cache not in a file (e.g. KeyRing)
    // x509 -> gsi authentication
    const char* suffixes[5] =
    { "krb5", "krk5", "x509", "krb5", "krk5" };
    CredType credtypes[5] = {krb5, krk5,x509,krb5,krk5};
    int sidx = 1, sn = 2;
    if (!use_user_krb5cc && use_user_gsiproxy)
      (sidx = 2) && (sn = 1);
    else if (use_user_krb5cc && !use_user_gsiproxy)
      (sidx = 0) && (sn = 2);
    else if (tryKrb5First)
      (sidx = 0) && (sn = 3);
    else
      (sidx = 2) && (sn = 3);

    // try all the credential types according to settings and stop as soon as a credetnial is found
    for (int i = sidx; i < sidx + sn; i++)
    {
      snprintf (buffer, 1024, format, (int) uid, (int) sid, (int) sst, suffixes[i]);
      //eos_static_debug("trying to stat %s", buffer);
      if (!lstat (buffer, &linkstat))
      {
        ret = true;
        credinfo.lname = buffer;
        credinfo.lmtime = linkstat.st_mtim.tv_sec;
        credinfo.lctime = linkstat.st_ctim.tv_sec;
        credinfo.type = credtypes[i];
        size_t bsize = readlink (buffer, buffer2, 1024);
        buffer2[bsize] = 0;
        eos_static_debug("found credential link %s for uid %d and sid %d", credinfo.lname.c_str (), (int )uid, (int )sid);
        if(credinfo.type==krk5)
        {
          credinfo.fname = buffer2;
          break; // there is no file to stat in that case
        }
        if (!stat (buffer2, &filestat))
        {
          if (bsize > 0)
          {
            buffer2[bsize] = 0;
            credinfo.fname = buffer2;
            credinfo.fmtime = filestat.st_mtim.tv_sec;
            credinfo.fctime = filestat.st_ctim.tv_sec;
            eos_static_debug("found credential file %s for uid %d and sid %d", credinfo.fname.c_str (), (int )uid, (int )sid);
          }
        }
        else
        {
          eos_static_debug("could not stat file %s for uid %d and sid %d", credinfo.fname.c_str (), (int )uid, (int )sid);
        }
        // we found some credential, we stop searching here
        break;
      }
    }

    if (!ret)
    eos_static_debug("could not find any credential for uid %d and sid %d", (int )uid, (int )sid);

    return ret;
  }

  bool readCred(CredInfo &credinfo)
  {
    bool ret = false;
    eos_static_debug("reading %s credential file %s",credinfo.type==krb5?"krb5":(credinfo.type==krb5?"krk5":"x509"),credinfo.fname.c_str());
    if(credinfo.type==krk5)
    {
      // fileless authentication cannot rely on symlinks to be able to change the cache credential file
      // instead of the identity, we use the keyring information and each has a different xrd login
      credinfo.identity = credinfo.fname;
      ret = true;
    }
    if(credinfo.type==krb5)
    {
      ProcReaderKrb5UserName reader(credinfo.fname);
      if(!reader.ReadUserName(credinfo.identity))
        eos_static_debug("could not read principal in krb5 cc file %s",credinfo.fname.c_str());
      else
        ret = true;
    }
    if(credinfo.type==x509)
    {
      ProcReaderGsiIdentity reader(credinfo.fname);
      if(!reader.ReadIdentity(credinfo.identity))
        eos_static_debug("could not read identity in x509 proxy file %s",credinfo.fname.c_str());
      else
        ret = true;
    }
    return ret;
  }

  bool checkCredSecurity(const struct stat &linkstat, const struct stat &filestat, uid_t uid, CredType credtype)
  {
    //eos_static_debug("linkstat.st_uid=%d  filestat.st_uid=%d  filestat.st_mode=%o  requiredmode=%o",(int)linkstat.st_uid,(int)filestat.st_uid,filestat.st_mode & 0777,reqMode);
    if (
    // check owner ship
    linkstat.st_uid == uid)
    {
      if (credtype == krk5)
        return true;
      else if (filestat.st_uid == uid && (filestat.st_mode & 0077) == 0 // no access to other users/groups
      && (filestat.st_mode & 0400) != 0 // read allowed for the user
          ) return true;
    }

    return false;
  }

  int
  updateProcCache (uid_t uid, gid_t gid, pid_t pid, bool reconnect)
  {
    // when entering this function proccachemutexes[pid] must be write locked
    int errCode;
    char buffer[1024];

    // this is useful even in gateway mode because of the recursive deletion protection
    if ((errCode = proccache_InsertEntry (pid)))
    {
      eos_static_err("updating proc cache information for process %d. Error code is %d", (int )pid, errCode);
      return errCode;
    }

    // check if we are using strong authentication
    if(!(use_user_krb5cc || use_user_gsiproxy))
      return 0;

    // get the startuptime of the process
    time_t processSut;
    proccache_GetStartupTime(pid,&processSut);
    // get the session id
    pid_t sid;
    proccache_GetSid(pid,&sid);
    bool isSessionLeader = (sid==pid);
    // update the proccache of the session leader
    if (!isSessionLeader)
    {
      xrd_lock_w_pcache (sid);
      if ((errCode = proccache_InsertEntry (sid)))
      {
        eos_static_err("updating proc cache information for session leader process %d. Error code is %d", (int )pid, errCode);
        xrd_unlock_w_pcache (sid);
        return errCode;
      }
      xrd_unlock_w_pcache (sid);
    }

    // get the startuptime of the leader of the session
    time_t sessionSut;
    proccache_GetStartupTime(sid,&sessionSut);

    // find the credentials
    CredInfo credinfo;
    struct stat filestat,linkstat;
    if(!findCred(credinfo,linkstat,filestat,uid,sid,sessionSut))
    {
      eos_static_notice("could not find any credential");
      return EACCES;
    }

    // check if the credentials in the credential cache cache are up to date
    // TODO: should we implement a TTL , my guess is NO
    bool sessionInCache = false;
    pMutex.LockRead();
    auto cacheEntry = uidsid2credinfo.find(std::make_pair(uid,sid));
    // skip the cache if reconnecting
    sessionInCache = !reconnect && (cacheEntry!=uidsid2credinfo.end());
    if(sessionInCache)
    {
      sessionInCache = false;
      const CredInfo &ci = cacheEntry->second;
      // we also check ctime to be sure that permission/ownership has not changed
      if(ci.lmtime == credinfo.lmtime
          && ci.lctime == credinfo.lctime )
      {
        if(credinfo.type==krk5) // if this is fileless credential cache, no target file to check
          sessionInCache = true;
        else if(ci.fmtime == credinfo.fmtime
            && ci.fctime == credinfo.fctime
            && ci.fname == credinfo.fname)
          sessionInCache = true;
      }
    }
    pMutex.UnLockRead();

    if(sessionInCache)
    {
      // TODO: could detect from the call to ptoccahce_InsertEntry if the process was changed
      //       then, it would be possible to bypass this part copy, which is probably not the main bottleneck anyway
      // no lock needed as only one thread per process can access this (lock is supposed to be already taken -> beginning of the function)
      eos_static_debug("uid=%d  sid=%d  pid=%d  found stronglogin in cache %s",(int)uid,(int)sid,(int)pid,cacheEntry->second.cachedStrongLogin.c_str());
      pid2StrongLogin[pid] = cacheEntry->second.cachedStrongLogin;
      proccache_GetAuthMethod(sid,buffer,1024);
      proccache_SetAuthMethod(pid,buffer);
      return 0;
    }

    // refresh the credentials in the cache
    // check the credential security
    if(!checkCredSecurity(linkstat,filestat,uid,credinfo.type))
    {
      eos_static_alert("credentials are not safe");
      return EACCES;
    }
    // check the credential security
    if(!readCred(credinfo))
      return EACCES;
    // update authmethods for session leader and current pid
    uint8_t authid;
    std::string sId;
    if(credinfo.type==krb5) sId = "krb5:";
    else if(credinfo.type==krk5) sId = "krk5:";
    else sId = "x509:";

    std::string newauthmeth;
    if(credinfo.type==krk5)
    {
      // don't need to create any symlink in that case
      sId.append(credinfo.fname);
      newauthmeth = sId;
    }
    else
    {
      // binding (uid_identity to the latest credential file received)
      sId.append(credinfo.lname);
      newauthmeth = symlinkCredentials(sId, uid,credinfo.identity);
    }

    if(newauthmeth.empty())
    {
      eos_static_err("error symlinking credential file ");
      return EACCES;
    }
    proccache_SetAuthMethod(pid,newauthmeth.c_str());
    proccache_SetAuthMethod(sid,newauthmeth.c_str());
    // update uid2IdenAuthid
    {
      eos::common::RWMutexWriteLock lock (pMutex);
      // this will create the entry in uid2IdenAuthid if it does not exist already
      auto &uidEntry = uid2IdenAuthid[uid];
      if(credinfo.type==krb5) sId = "krb5:";
      else if(credinfo.type==krk5) sId = "krk5:";
      else sId = "x509:";
      sId.append(credinfo.identity);
      if (!reconnect && uidEntry.strongid2authid.count (sId))
        authid = uidEntry.strongid2authid[sId]; // if this identity already has an authid , use it
      else
      {
        if(uidEntry.freeAuthIdPool.empty())
        {
          eos_static_err("reached maximum number of connections for uid %d. cannot bind identity [%s] to a new xrootd connection! "
              ,(int)uid,sId.c_str());
          return EACCES;
        }
        // get the new connection id
        uint8_t newauthid = uidEntry.freeAuthIdPool.front();
        // remove the new connection from the connections available to be used
        uidEntry.freeAuthIdPool.pop_front();
        // recycle the previsous connection number if reconnecting
        if(reconnect)
        {
          eos_static_debug("dropping authid %d",(int)uidEntry.strongid2authid[sId]);
          uidEntry.freeAuthIdPool.push_back(uidEntry.strongid2authid[sId]);
        }
        // store the new authid
        eos_static_debug("using newauthid %d",(int)newauthid);
        authid = (uidEntry.strongid2authid[sId] = newauthid); // create a new authid if this id is not registered yet
      }
    }
    // update pid2StrongLogin (no lock needed as only one thread per process can access this)
    map_user xrdlogin (uid, gid, authid);
    pid2StrongLogin[pid] = std::string (xrdlogin.base64 ());
    // update uidsid2credinfo
    credinfo.cachedStrongLogin = pid2StrongLogin[pid];
    eos_static_debug("uid=%d  sid=%d  pid=%d  writing stronglogin in cache %s",(int)uid,(int)sid,(int)pid,credinfo.cachedStrongLogin.c_str());
    pMutex.LockWrite();
    uidsid2credinfo[std::make_pair(uid,sid)] = credinfo;
    pMutex.UnLockWrite();

    eos_static_debug("qualifiedidentity [%s] used for pid %d, xrdlogin is %s (%d/%d)", sId.c_str (), (int )pid,
                     pid2StrongLogin[pid].c_str (), (int )uid, (int )authid);

    return errCode;
  }

public:

  inline int
  updateProcCache (uid_t uid, gid_t gid, pid_t pid)
  {
    return updateProcCache (uid, gid, pid,false);
  }

  inline int
  reconnectProcCache (uid_t uid, gid_t gid, pid_t pid)
  {
    return updateProcCache (uid, gid, pid,true);
  }

  std::string
  getXrdLogin (pid_t pid)
  {
    eos::common::RWMutexReadLock lock (proccachemutexes[pid]);
    return pid2StrongLogin[pid];
  }

};

AuthIdManager authidmanager;

int update_proc_cache (uid_t uid, gid_t gid, pid_t pid)
{
  return authidmanager.updateProcCache(uid,gid,pid);
}

std::string get_xrd_login(uid_t uid, gid_t gid, pid_t pid)
{
  return (use_user_krb5cc||use_user_gsiproxy)?authidmanager.getXrdLogin(pid):xrd_mapuser (uid, gid, pid,0);
}

//------------------------------------------------------------------------------
//             ******* XROOTD interface functions *******
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Remove extended attribute
//------------------------------------------------------------------------------
int
xrd_rmxattr (const char* path,
             const char* xattr_name,
             uid_t uid,
             gid_t gid,
             pid_t pid)
{
  eos_static_info("path=%s xattr_name=%s uid=%u pid=%u", path, xattr_name, uid, pid);
  eos::common::Timing rmxattrtiming("rmxattr");
  COMMONTIMING("START", &rmxattrtiming);
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = path;
  request += "?";
  request += "mgm.pcmd=xattr&eos.app=fuse&";
  request += "mgm.subcmd=rm&";
  request += "mgm.xattrname=";
  request += xattr_name;
  arg.FromString(request);

  std::string surl=xrd_user_url(uid, gid, pid);
  if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) surl += '?';
  surl += xrd_strongauth_cgi(pid);

  XrdCl::URL Url(surl.c_str());
  XrdCl::FileSystem fs(Url);

  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile,
                                        arg, response);
  COMMONTIMING("GETPLUGIN", &rmxattrtiming);
  errno = 0;

  if (status.IsOK())
  {
    int retc = 0;
    int items = 0;
    char tag[1024];
    // Parse output
    items = sscanf(response->GetBuffer(), "%s retc=%i", tag, &retc);

    if ((items != 2) || (strcmp(tag, "rmxattr:")))
      errno = ENOENT;
    else
      errno = retc;
  }
  else
  {
    eos_static_err("status is NOT ok : %s",status.ToString().c_str());
    errno = ((status.code == XrdCl::errAuthFailed) ? EPERM : EFAULT);
  }

  COMMONTIMING("END", &rmxattrtiming);

  if (EOS_LOGS_DEBUG)
    rmxattrtiming.Print();

  delete response;
  return errno;
}


//------------------------------------------------------------------------------
// Set extended attribute
//------------------------------------------------------------------------------
int
xrd_setxattr (const char* path,
              const char* xattr_name,
              const char* xattr_value,
              size_t size,
              uid_t uid,
              gid_t gid,
              pid_t pid)
{
  eos_static_info("path=%s xattr_name=%s xattr_value=%s uid=%u pid=%u",
                  path, xattr_name, xattr_value, uid, pid);
  eos::common::Timing setxattrtiming("setxattr");
  COMMONTIMING("START", &setxattrtiming);
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = path;
  request += "?";
  request += "mgm.pcmd=xattr&eos.app=fuse&";
  request += "mgm.subcmd=set&";
  request += "mgm.xattrname=";
  request += xattr_name;

  std::string s_xattr_name = xattr_name;
  if (s_xattr_name.find("&") != std::string::npos)
  {
    // & is a forbidden character in attribute names
    errno = EINVAL;
    return errno;
  }

  request += "&";
  request += "mgm.xattrvalue=";
  request += std::string(xattr_value, size);
  arg.FromString(request);

  std::string surl=xrd_user_url(uid, gid, pid);
  if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) surl += '?';
  surl += xrd_strongauth_cgi(pid);

  XrdCl::URL Url(surl.c_str());
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile,
                                        arg, response);
  COMMONTIMING("GETPLUGIN", &setxattrtiming);
  errno = 0;

  if (status.IsOK())
  {
    int retc = 0;
    int items = 0;
    char tag[1024];
    // Parse output
    items = sscanf(response->GetBuffer(), "%s retc=%i", tag, &retc);

    if ((items != 2) || (strcmp(tag, "setxattr:")))
      errno = ENOENT;
    else
      errno = retc;
  }
  else
  {
	eos_static_err("status is NOT ok : %s",status.ToString().c_str());
	errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;
  }

  COMMONTIMING("END", &setxattrtiming);

  if (EOS_LOGS_DEBUG)
    setxattrtiming.Print();

  delete response;
  return errno;
}


//------------------------------------------------------------------------------
// Read an extended attribute
//------------------------------------------------------------------------------
int
xrd_getxattr (const char* path,
              const char* xattr_name,
              char** xattr_value,
              size_t* size,
              uid_t uid,
              gid_t gid,
              pid_t pid)
{
  eos_static_info("path=%s xattr_name=%s uid=%u pid=%u", path, xattr_name, uid, pid);
  eos::common::Timing getxattrtiming("getxattr");

  COMMONTIMING("START", &getxattrtiming);
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = path;
  request += "?";
  request += "mgm.pcmd=xattr&eos.app=fuse&";
  request += "mgm.subcmd=get&";
  request += "mgm.xattrname=";
  std::string s_xattr_name = xattr_name;
  if (s_xattr_name.find("&") != std::string::npos)
  {
    // & is a forbidden character in attribute names
    errno = EINVAL;
    return errno;
  }
  
  request += xattr_name;
  arg.FromString(request);

  std::string surl=xrd_user_url(uid, gid, pid);
  if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) surl += '?';
  surl += xrd_strongauth_cgi(pid);
  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile,
                                        arg, response);
  COMMONTIMING("GETPLUGIN", &getxattrtiming);
  errno = 0;

  if (status.IsOK())
  {
    int retc = 0;
    int items = 0;
    char tag[1024];
    char rval[4096];
    // Parse output
    items = sscanf(response->GetBuffer(), "%s retc=%i value=%s", tag, &retc, rval);

    if ((items != 3) || (strcmp(tag, "getxattr:")))
      errno = EFAULT;
    else
    {
      if (strcmp(xattr_name, "user.eos.XS") == 0)
      {
        char* ptr = rval;

        for (unsigned int i = 0; i < strlen(rval); i++, ptr++)
        {
          if (*ptr == '_')
            *ptr = ' ';
        }
      }

      *size = strlen(rval);
      *xattr_value = (char*) calloc((*size) + 1, sizeof ( char));
      *xattr_value = strncpy(*xattr_value, rval, *size);
      errno = retc;
    }
  }
  else
  {
	eos_static_err("status is NOT ok : %s",status.ToString().c_str());
	errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;
  }


  COMMONTIMING("END", &getxattrtiming);

  if (EOS_LOGS_DEBUG)
    getxattrtiming.Print();

  delete response;
  return errno;
}


//------------------------------------------------------------------------------
// List extended attributes
//------------------------------------------------------------------------------
int
xrd_listxattr (const char* path,
               char** xattr_list,
               size_t* size,
               uid_t uid,
               gid_t gid,
               pid_t pid)
{
  eos_static_info("path=%s uid=%u pid=%u", path, uid, pid);
  eos::common::Timing listxattrtiming("listxattr");
  COMMONTIMING("START", &listxattrtiming);
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = path;
  request += "?";
  request += "mgm.pcmd=xattr&eos.app=fuse&";
  request += "mgm.subcmd=ls";
  arg.FromString(request);

  std::string surl=xrd_user_url(uid, gid, pid);
  if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) surl += '?';
  surl += xrd_strongauth_cgi(pid);
  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile,
                                        arg, response);
  COMMONTIMING("GETPLUGIN", &listxattrtiming);
  errno = 0;

  if (status.IsOK())
  {
    int retc = 0;
    int items = 0;
    char tag[1024];
    char rval[16384];
    // Parse output
    items = sscanf(response->GetBuffer(), "%s retc=%i %s", tag, &retc, rval);

    if ((items != 3) || (strcmp(tag, "lsxattr:")))
      errno = ENOENT;
    else
    {
      *size = strlen(rval);
      char* ptr = rval;

      for (unsigned int i = 0; i < (*size); i++, ptr++)
      {
        if (*ptr == '&')
          *ptr = '\0';
      }

      *xattr_list = (char*) calloc((*size) + 1, sizeof ( char));
      *xattr_list = (char*) memcpy(*xattr_list, rval, *size);
      errno = retc;
    }
  }
  else
  {
	eos_static_err("status is NOT ok : %s",status.ToString().c_str());
	errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;
  }


  COMMONTIMING("END", &listxattrtiming);

  if (EOS_LOGS_DEBUG)
    listxattrtiming.Print();

  delete response;
  return errno;
}


//------------------------------------------------------------------------------
// Return file attributes. If a field is meaningless or semi-meaningless
// (e.g., st_ino) then it should be set to 0 or given a "reasonable" value.
//------------------------------------------------------------------------------
int
xrd_stat (const char* path,
          struct stat* buf,
          uid_t uid,
          gid_t gid,
          pid_t pid,
          unsigned long inode)
{
  eos_static_info("path=%s, uid=%i, gid=%i inode=%lu",
                  path, (int)uid, (int)gid, inode);
  eos::common::Timing stattiming("xrd_stat");
  off_t file_size = -1;
  struct timespec atime,mtime;
  atime.tv_sec = atime.tv_nsec = mtime.tv_sec= mtime.tv_nsec = 0;
  errno = 0;
  COMMONTIMING("START", &stattiming);

  if (inode)
  {
    // Try to stat via an open file - first find the file descriptor using the
    // inodeuser2fd map and then find the file object using the fd2fabst map.
    // Meanwhile keep the mutex locked for read so that no other thread can
    // delete the file object
    eos_static_debug("path=%s, uid=%lu, inode=%lu",
                     path, (unsigned long) uid, inode);
    eos::common::RWMutexReadLock rd_lock(rwmutex_fd2fabst);
    std::ostringstream sstr;
    sstr << inode << ":" << get_xrd_login(uid,gid,pid);
    google::dense_hash_map<std::string, int>::iterator
      iter_fd = inodexrdlogin2fd.find(sstr.str());

    if (iter_fd != inodexrdlogin2fd.end())
    {
      google::dense_hash_map<int, FileAbstraction*>::iterator
        iter_file = fd2fabst.find(iter_fd->second);

      if (iter_file != fd2fabst.end())
      {
        // Force flush so that we get the real current size through the file obj.
        if (XFC && fuse_cache_write) 
	{
	  iter_file->second->mMutexRW.WriteLock();
          XFC->ForceAllWrites(iter_file->second);
	  iter_file->second->mMutexRW.UnLock();
	}

        struct stat tmp;
        LayoutWrapper* file = iter_file->second->GetRawFile();

        if (!file->Stat(&tmp))
        {
          file_size = tmp.st_size;
          mtime = tmp.st_mtim;
          atime = tmp.st_atim;
          eos_static_debug("fd=%i, size-fd=%lld, raw_file=%p",
                          iter_fd->second, file_size, file);
        }
        else
          eos_static_err("fd=%i stat failed on open file", iter_fd->second);
      }
      else
        eos_static_err("fd=%i not found in file obj map", iter_fd->second);
    }
    else
      eos_static_debug("path=%s not open", path);
  }

  // Do stat using the Fils System object
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = path;
  request += "?";
  request += "mgm.pcmd=stat&eos.app=fuse";
  arg.FromString(request);

  std::string surl=xrd_user_url(uid, gid, pid);
  if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) surl += '?';
  surl += xrd_strongauth_cgi(pid);

  eos_static_debug("stat url is %s",surl.c_str());
  XrdCl::URL Url(surl.c_str());
  XrdCl::FileSystem fs(Url);

  eos_static_debug("arg = %s",arg.ToString().c_str());
  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile,
                                        arg, response);
  COMMONTIMING("GETPLUGIN", &stattiming);

  if (status.IsOK())
  {
    unsigned long long sval[10];
    unsigned long long ival[6];
    char tag[1024];
    tag[0]=0;
    // Parse output
    int items = sscanf(response->GetBuffer(),
                       "%s %llu %llu %llu %llu %llu %llu %llu %llu "
                       "%llu %llu %llu %llu %llu %llu %llu %llu",
                       tag, (unsigned long long*) &sval[0],
                       (unsigned long long*) &sval[1],
                       (unsigned long long*) &sval[2],
                       (unsigned long long*) &sval[3],
                       (unsigned long long*) &sval[4],
                       (unsigned long long*) &sval[5],
                       (unsigned long long*) &sval[6],
                       (unsigned long long*) &sval[7],
                       (unsigned long long*) &sval[8],
                       (unsigned long long*) &sval[9],
                       (unsigned long long*) &ival[0],
                       (unsigned long long*) &ival[1],
                       (unsigned long long*) &ival[2],
                       (unsigned long long*) &ival[3],
                       (unsigned long long*) &ival[4],
                       (unsigned long long*) &ival[5]);

    if ((items != 17) || (strcmp(tag, "stat:")))
    {
      int retc=0;
      items = sscanf(response->GetBuffer(), "%s retc=%i", tag, &retc);
      if ( (!strcmp(tag, "stat:")) && (items == 2) )
	errno = retc;
      else
	errno = EFAULT;
      delete response;
      eos_static_info("path=%s errno=%i tag=%s", path, errno, tag);
      return errno;
    }
    else
    {
      buf->st_dev = (dev_t) sval[0];
      buf->st_ino = (ino_t) sval[1];
      buf->st_mode = (mode_t) sval[2];
      buf->st_nlink = (nlink_t) sval[3];
      buf->st_uid = (uid_t) sval[4];
      buf->st_gid = (gid_t) sval[5];
      buf->st_rdev = (dev_t) sval[6];
      buf->st_size = (off_t) sval[7];
      buf->st_blksize = (blksize_t) sval[8];
      buf->st_blocks = (blkcnt_t) sval[9];
#ifdef __APPLE__
      buf->st_atimespec.tv_sec = (time_t) ival[0];
      buf->st_mtimespec.tv_sec = (time_t) ival[1];
      buf->st_ctimespec.tv_sec = (time_t) ival[2];
      buf->st_atimespec.tv_nsec = (time_t) ival[3];
      buf->st_mtimespec.tv_nsec = (time_t) ival[4];
      buf->st_ctimespec.tv_nsec = (time_t) ival[5];
#else
      buf->st_atime = (time_t) ival[0];
      buf->st_mtime = (time_t) ival[1];
      buf->st_ctime = (time_t) ival[2];
      buf->st_atim.tv_sec = (time_t) ival[0];
      buf->st_mtim.tv_sec = (time_t) ival[1];
      buf->st_ctim.tv_sec = (time_t) ival[2];
      buf->st_atim.tv_nsec = (time_t) ival[3];
      buf->st_mtim.tv_nsec = (time_t) ival[4];
      buf->st_ctim.tv_nsec = (time_t) ival[5];
#endif

      if (S_ISREG(buf->st_mode) && fuse_exec)
        buf->st_mode |= (S_IXUSR | S_IXGRP | S_IXOTH);

      buf->st_mode &= (~S_ISVTX); // clear the vxt bit
      buf->st_mode &= (~S_ISUID); // clear suid
      buf->st_mode &= (~S_ISGID); // clear sgid
      errno = 0;
    }
  }
  else
  {
    eos_static_err("status is NOT ok : %s",status.ToString().c_str());
    errno = (status.code==XrdCl::errAuthFailed)?EPERM:EFAULT;
  }

  // If got size using opened file then return this value
  if ((file_size != -1) && use_localtime_consistency)
  {
    buf->st_size = file_size;
    // if using local time consistency model
    // overwrite mgm mtime only if local mtime is newer
    // WARNING : this is VALID ONLY if
    // - the file is being accessed only from fuse mounts that are synchronized between together
    // OR
    // - the file is being accessed from all types of protocol if they are all synchronized with the eos instance
    if (buf->st_mtim.tv_sec < mtime.tv_sec || ((buf->st_mtim.tv_sec == mtime.tv_sec) && (buf->st_mtim.tv_nsec <= mtime.tv_nsec)))
    {
      buf->st_atim = atime;
      buf->st_mtim = mtime;
      buf->st_atime = buf->st_atim.tv_sec;
      buf->st_mtime = buf->st_mtim.tv_sec;
    }
    else
      eos_static_debug("ALERT: last update on the mgm %lu.%.9lu is more recent that my local update %lu.%.9lu",buf->st_mtim.tv_sec,buf->st_mtim.tv_nsec,mtime.tv_sec,mtime.tv_nsec);
  }

  COMMONTIMING("END", &stattiming);

  if (EOS_LOGS_DEBUG)
    stattiming.Print();

  eos_static_info("path=%s st-size=%llu st-mtim.tv_sec=%llu st-mtim.tv_nsec=%llu errno=%i", path, buf->st_size, buf->st_mtim.tv_sec, buf->st_mtim.tv_nsec, errno);
  delete response;
  return errno;
}


//------------------------------------------------------------------------------
// Return statistics about the filesystem
//------------------------------------------------------------------------------
int
xrd_statfs (const char* path, struct statvfs* stbuf,
            uid_t uid,
            gid_t gid,
            pid_t pid)
{
  eos_static_info("path=%s", path);
  static unsigned long long a1 = 0;
  static unsigned long long a2 = 0;
  static unsigned long long a3 = 0;
  static unsigned long long a4 = 0;
  static XrdSysMutex statmutex;
  static time_t laststat = 0;
  statmutex.Lock();
  errno = 0;

  if ((time(NULL) - laststat) < ((15 + (int) 5.0 * rand() / RAND_MAX)))
  {
    stbuf->f_bsize = 4096;
    stbuf->f_frsize = 4096;
    stbuf->f_blocks = a3 / 4096;
    stbuf->f_bfree = a1 / 4096;
    stbuf->f_bavail = a1 / 4096;
    stbuf->f_files = a4;
    stbuf->f_ffree = a2;
    stbuf->f_fsid = 0xcafe;
    stbuf->f_namemax = 256;
    statmutex.UnLock();
    return errno;
  }

  eos::common::Timing statfstiming("xrd_statfs");
  COMMONTIMING("START", &statfstiming);

  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = path;
  request += "?";
  request += "mgm.pcmd=statvfs&eos.app=fuse&";
  request += "path=";
  request += path;
  arg.FromString(request);

  std::string surl=xrd_user_url(uid, gid, pid);
  if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) surl += '?';
  surl += xrd_strongauth_cgi(pid);
  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile,
                                        arg, response);
  errno = 0;

  if (status.IsOK())
  {
    int retc;
    char tag[1024];

    if (!response->GetBuffer())
    {
      statmutex.UnLock();
      delete response;
      errno = EFAULT;
      return errno;
    }

    // Parse output
    int items = sscanf(response->GetBuffer(),
                       "%s retc=%d f_avail_bytes=%llu f_avail_files=%llu "
                       "f_max_bytes=%llu f_max_files=%llu",
                       tag, &retc, &a1, &a2, &a3, &a4);

    if ((items != 6) || (strcmp(tag, "statvfs:")))
    {
      statmutex.UnLock();
      delete response;
      errno = EFAULT;
      return errno;
    }

    errno = retc;
    laststat = time(NULL);
    statmutex.UnLock();
    stbuf->f_bsize = 4096;
    stbuf->f_frsize = 4096;
    stbuf->f_blocks = a3 / 4096;
    stbuf->f_bfree = a1 / 4096;
    stbuf->f_bavail = a1 / 4096;
    stbuf->f_files = a4;
    stbuf->f_ffree = a2;
  }
  else
  {
    statmutex.UnLock();
    eos_static_err("status is NOT ok : %s",status.ToString().c_str());
    errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;
  }

  COMMONTIMING("END", &statfstiming);

  if (EOS_LOGS_DEBUG)
    statfstiming.Print();

  delete response;
  return errno;
}

//------------------------------------------------------------------------------
// Change permissions for the file
//------------------------------------------------------------------------------
int
xrd_chmod (const char* path,
           mode_t mode,
           uid_t uid,
           gid_t gid,
           pid_t pid)
{
  eos_static_info("path=%s mode=%x uid=%u pid=%u", path, mode, uid, pid);
  eos::common::Timing chmodtiming("xrd_chmod");
  COMMONTIMING("START", &chmodtiming);
  int retc = 0;
  XrdOucString smode;
  smode += (int) mode;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = path;
  request += "?";
  request += "mgm.pcmd=chmod&eos.app=fuse&mode=";
  request += smode.c_str();
  arg.FromString(request);

  std::string surl=xrd_user_url(uid, gid, pid);
  if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) surl += '?';
  surl += xrd_strongauth_cgi(pid);
  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile,
                                        arg, response);
  COMMONTIMING("END", &chmodtiming);
  errno = 0;

  if (EOS_LOGS_DEBUG)
    chmodtiming.Print();

  if (status.IsOK())
  {
    char tag[1024];

    if (!response->GetBuffer())
    {
      delete response;
      errno = EFAULT;
      return errno;
    }

    // Parse output
    int items = sscanf(response->GetBuffer(), "%s retc=%d", tag, &retc);

    if ((items != 2) || (strcmp(tag, "chmod:")))
      errno = EFAULT;
    else
      errno = retc;
  }
  else
  {
	eos_static_err("status is NOT ok : %s",status.ToString().c_str());
	errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;
  }

  delete response;
  return errno;
}

//------------------------------------------------------------------------------
// Postpone utimes to a file close if still open
//------------------------------------------------------------------------------
int
xrd_set_utimes_close(unsigned long long inode,
                    struct timespec* tvp,
                    uid_t uid, gid_t gid, pid_t pid)
{
  // try to attach the utimes call until a referenced filedescriptor on that path is closed
  std::ostringstream sstr;
  sstr << inode << ":" << get_xrd_login(uid,gid,pid);
  {
    eos::common::RWMutexWriteLock wr_lock(rwmutex_fd2fabst);
    auto iter_fd = inodexrdlogin2fd.find(sstr.str());
    if (iter_fd != inodexrdlogin2fd.end())
    {
      google::dense_hash_map<int, FileAbstraction*>::iterator
        iter_file = fd2fabst.find(iter_fd->second);

      if (iter_file != fd2fabst.end())
      {
        iter_file->second->SetUtimes(tvp);
        return 0;
      }
    }
  }
  return 1;
}


//------------------------------------------------------------------------------
// Update the last access time and last modification time
//------------------------------------------------------------------------------
int
xrd_utimes (const char* path,
            struct timespec* tvp,
            uid_t uid,
            gid_t gid,
            pid_t pid)
{
  eos_static_info("path=%s uid=%u pid=%u", path, uid, pid);
  eos::common::Timing utimestiming("xrd_utimes");

  COMMONTIMING("START", &utimestiming);
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = path;
  request += "?";
  request += "mgm.pcmd=utimes&eos.app=fuse&tv1_sec=";
  char lltime[1024];
  sprintf(lltime, "%llu", (unsigned long long) tvp[0].tv_sec);
  request += lltime;
  request += "&tv1_nsec=";
  sprintf(lltime, "%llu", (unsigned long long) tvp[0].tv_nsec);
  request += lltime;
  request += "&tv2_sec=";
  sprintf(lltime, "%llu", (unsigned long long) tvp[1].tv_sec);
  request += lltime;
  request += "&tv2_nsec=";
  sprintf(lltime, "%llu", (unsigned long long) tvp[1].tv_nsec);
  request += lltime;
  eos_static_debug("request: %s",request.c_str());
  arg.FromString(request);

  std::string surl=xrd_user_url(uid, gid, pid);
  if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) surl += '?';
  surl += xrd_strongauth_cgi(pid);
  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile,
                                        arg, response);
  COMMONTIMING("END", &utimestiming);
  errno = 0;

  if (EOS_LOGS_DEBUG)
    utimestiming.Print();

  if (status.IsOK())
  {
    int retc = 0;
    char tag[1024];
    // Parse output
    int items = sscanf(response->GetBuffer(), "%s retc=%d", tag, &retc);

    if ((items != 2) || (strcmp(tag, "utimes:")))
      errno = EFAULT;
    else
      errno = retc;
  }
  else
  {
	eos_static_err("status is NOT ok : %s",status.ToString().c_str());
	errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;
  }

  delete response;
  return errno;
}


//----------------------------------------------------------------------------                                                                                                                
//!                                                                                                                                                                                           
//----------------------------------------------------------------------------                                                                                                                

int 
xrd_symlink(const char* path,
	    const char* link,
	    uid_t uid,
	    gid_t gid,
	    pid_t pid)
{
  eos_static_info("path=%s link=%s uid=%u pid=%u", path, link, uid, pid);
  eos::common::Timing symlinktiming("xrd_symlink");
  COMMONTIMING("START", &symlinktiming);

  int retc = 0;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = path;
  request += "?";
  request += "mgm.pcmd=symlink&eos.app=fuse&target=";
  XrdOucString savelink = link;
  while (savelink.replace("&", "#AND#")){}
  request += savelink.c_str();
  arg.FromString(request);
  std::string surl=xrd_user_url(uid, gid, pid);
  if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) surl += '?';
  surl += xrd_strongauth_cgi(pid);
  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);

  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile,
                                        arg, response);

  COMMONTIMING("STOP", &symlinktiming);
  errno = 0;

  if (EOS_LOGS_DEBUG)
    symlinktiming.Print();

  if (status.IsOK())
  {
    char tag[1024];
    // Parse output
    int items = sscanf(response->GetBuffer(), "%s retc=%d", tag, &retc);

    if (EOS_LOGS_DEBUG)
      fprintf(stderr, "symlink-retc=%d\n", retc);

    if ((items != 2) || (strcmp(tag, "symlink:")))
      errno = EFAULT;
    else
      errno = retc;
  }
  else
  {
	eos_static_err("error=status is NOT ok : %s",status.ToString().c_str());
	errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;
  }

  delete response;
  return errno;
}

//----------------------------------------------------------------------------                                                                                                                
//!                                                                                                                                                                                           
//----------------------------------------------------------------------------                                                                                                                

int 
xrd_readlink(const char* path,
	     char* buf,
	     size_t bufsize,
	     uid_t uid,
	     gid_t gid,
	     pid_t pid)
{
  eos_static_info("path=%s uid=%u pid=%u", path, uid, pid);
  eos::common::Timing readlinktiming("xrd_readlink");
  COMMONTIMING("START", &readlinktiming);
  int retc = 0;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = path;
  request += "?";
  request += "mgm.pcmd=readlink&eos.app=fuse";
  arg.FromString(request);

  std::string surl=xrd_user_url(uid, gid, pid);
  if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) surl += '?';
  surl += xrd_strongauth_cgi(pid);
  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile,
                                        arg, response);
  COMMONTIMING("END", &readlinktiming);
  errno = 0;

  if (EOS_LOGS_DEBUG)
    readlinktiming.Print();

  if (status.IsOK())
  {
    char tag[1024];

    if (!response->GetBuffer())
    {
      delete response;
      errno = EFAULT;
      return errno;
    }

    // Parse output
    int items = sscanf(response->GetBuffer(), "%s retc=%d %*s", tag, &retc);

    if (EOS_LOGS_DEBUG)
      fprintf(stderr, "readlink-retc=%d\n", retc);

    if ((items != 2) || (strcmp(tag, "readlink:")))
      errno = EFAULT;
    else
      errno = retc;

    if (!errno) {
      const char* rs = strchr(response->GetBuffer(),'=');
      if (rs) 
      {
	const char* ss = strchr(rs,' ');
	if (ss) 
	{
	  snprintf(buf,bufsize,"%s", ss+1);
	}
	else
	{
	  errno = EBADE;
	}
      }
      else
      {
	errno = EBADE;
      }
    }
  }
  else
  {
	eos_static_err("status is NOT ok : %s",status.ToString().c_str());
	errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;
  }

  delete response;
  return errno;
}

//------------------------------------------------------------------------------
// It returns -ENOENT if the path doesn't exist, -EACCESS if the requested
// permission isn't available, or 0 for success. Note that it can be called
// on files, directories, or any other object that appears in the filesystem.
//------------------------------------------------------------------------------
int
xrd_access (const char* path,
            int mode,
            uid_t uid,
            gid_t gid,
            pid_t pid
            )
{
  eos_static_info("path=%s mode=%d uid=%u pid=%u", path, mode, uid, pid);
  eos::common::Timing accesstiming("xrd_access");
  COMMONTIMING("START", &accesstiming);

  int retc = 0;
  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  char smode[16];
  snprintf(smode, sizeof (smode) - 1, "%d", mode);
  request = path;
  request += "?";
  request += "mgm.pcmd=access&eos.app=fuse&mode=";
  request += smode;
  arg.FromString(request);

  std::string surl=xrd_user_url(uid, gid, pid);
  if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) surl += '?';
  surl += xrd_strongauth_cgi(pid);
  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);

  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile,
                                        arg, response);

  COMMONTIMING("STOP", &accesstiming);
  errno = 0;

  if (EOS_LOGS_DEBUG)
    accesstiming.Print();

  if (status.IsOK())
  {
    char tag[1024];
    // Parse output
    int items = sscanf(response->GetBuffer(), "%s retc=%d", tag, &retc);

    if (EOS_LOGS_DEBUG)
      fprintf(stderr, "access-retc=%d\n", retc);

    if ((items != 2) || (strcmp(tag, "access:")))
      errno = EFAULT;
    else
      errno = retc;
  }
  else
  {
	eos_static_err("status is NOT ok : %s",status.ToString().c_str());
	errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;
  }

  delete response;
  return errno;
}


//------------------------------------------------------------------------------
// Get list of entries in directory
//------------------------------------------------------------------------------
int
xrd_inodirlist (unsigned long long dirinode,
                const char* path,
                uid_t uid,
                gid_t gid,
                pid_t pid)
{
  eos_static_info("inode=%llu path=%s", dirinode, path);
  eos::common::Timing inodirtiming("xrd_inodirlist");
  COMMONTIMING("START", &inodirtiming);

  int retc = 0;
  char* ptr = 0;
  char* value = 0;
  int doinodirlist = -1;
  std::string request = path;

  // we have to replace '&' in path names with '#AND#'

  size_t a_pos = request.find("mgm.path=/");

  while ( (a_pos = request.find("&", a_pos+1)) != std::string::npos)
  {
    request.erase(a_pos,1);
    request.insert(a_pos,"#AND#");
    a_pos+=4;
  }

  // add the kerberos token
  if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) request += '&';
  request += xrd_strongauth_cgi(pid);

  COMMONTIMING("GETSTSTREAM", &inodirtiming);
  request.insert(0, xrd_user_url(uid, gid, pid));
  XrdCl::File* file = new XrdCl::File();
  XrdCl::XRootDStatus status = file->Open(request.c_str(),
                                          XrdCl::OpenFlags::Flags::Read);
  errno = 0;

  if (!status.IsOK())
  {
    eos_static_err("got an error to request.");
    delete file;
    eos_static_err("error=status is NOT ok : %s",status.ToString().c_str());
    errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;
    return errno;
  }

  // Start to read
  int npages = 1;
  off_t offset = 0;
  unsigned int nbytes = 0;
  value = (char*) malloc(PAGESIZE + 1);
  COMMONTIMING("READSTSTREAM", &inodirtiming);

  status = file->Read(offset, PAGESIZE, value + offset, nbytes);

  while ((status.IsOK()) && (nbytes == PAGESIZE))
  {
    npages++;
    value = (char*) realloc(value, npages * PAGESIZE + 1);
    offset += PAGESIZE;
    status = file->Read(offset, PAGESIZE, value + offset, nbytes);
  }

  if (status.IsOK()) offset += nbytes;

  value[offset] = 0;
  delete file;
  xrd_dirview_create((unsigned long long) dirinode);
  COMMONTIMING("PARSESTSTREAM", &inodirtiming);
  xrd_lock_w_dirview(); // =>

  if (status.IsOK())
  {
    char dirpath[4096];
    unsigned long long inode;
    char tag[128];
    // Parse output
    int items = sscanf(value, "%s retc=%d", tag, &retc);

    if (retc)
    {
      free(value);
      xrd_unlock_w_dirview(); // <=
      xrd_dirview_delete((unsigned long long) dirinode);
      errno = EFAULT;
      return errno;
    }

    if ((items != 2) || (strcmp(tag, "inodirlist:")))
    {
      eos_static_err("got an error(1).");
      free(value);
      xrd_unlock_w_dirview(); // <=
      xrd_dirview_delete((unsigned long long) dirinode);
      errno = EFAULT;
      return errno;
    }

    ptr = strchr(value, ' ');
    if (ptr) ptr = strchr(ptr + 1, ' ');
    char* endptr = value + strlen(value) - 1;

    while ((ptr) && (ptr < endptr))
    {
      int items = sscanf(ptr, "%s %llu", dirpath, &inode);

      if (items != 2)
      {
        eos_static_err("got an error(2).");
        free(value);
        xrd_unlock_w_dirview(); // <=
        xrd_dirview_delete((unsigned long long) dirinode);
        errno = EFAULT;
        return errno;
      }

      XrdOucString whitespacedirpath = dirpath;
      whitespacedirpath.replace("%20", " ");
      whitespacedirpath.replace("%0A", "\n");
      xrd_store_child_p2i(dirinode, inode, whitespacedirpath.c_str());
      dir2inodelist[dirinode].push_back(inode);

      // to the next entries
      if (ptr) ptr = strchr(ptr + 1, ' ');
      if (ptr) ptr = strchr(ptr + 1, ' ');
    }

    doinodirlist = 0;
  }

  xrd_unlock_w_dirview(); // <=
  COMMONTIMING("END", &inodirtiming);

  if (EOS_LOGS_DEBUG)
    //inodirtiming.Print();

  free(value);
  return doinodirlist;
}


//------------------------------------------------------------------------------
// Get directory entries
//------------------------------------------------------------------------------
struct dirent*
xrd_readdir (const char* path_dir, size_t *size,
             uid_t uid,
             gid_t gid,
             pid_t pid)
{
  eos_static_info("path=%s", path_dir);
  struct dirent* dirs = NULL;
  XrdCl::DirectoryList* response = 0;
  XrdCl::DirListFlags::Flags flags = XrdCl::DirListFlags::None;
  string path_str = path_dir;

  std::string surl=xrd_user_url(uid, gid, pid);
  if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) surl += '?';
  surl += xrd_strongauth_cgi(pid);
  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = fs.DirList(path_str, flags, response);

  if (status.IsOK())
  {
    *size = response->GetSize();
    dirs = static_cast<struct dirent*> (calloc(*size, sizeof ( struct dirent)));
    int i = 0;

    for (XrdCl::DirectoryList::ConstIterator iter = response->Begin();
      iter != response->End();
      ++iter)
    {
      XrdCl::DirectoryList::ListEntry* list_entry =
        static_cast<XrdCl::DirectoryList::ListEntry*> (*iter);
      size_t len = list_entry->GetName().length();
      const char* cp = list_entry->GetName().c_str();
      const int dirhdrln = dirs[i].d_name - (char *) &dirs[i];
#if defined(__macos__) || defined(__FreeBSD__)
      dirs[i].d_fileno = i;
      dirs[i].d_type = DT_UNKNOWN;
      dirs[i].d_namlen = len;
#else
      dirs[i].d_ino = i;
      dirs[i].d_off = i*NAME_MAX;
#endif
      dirs[i].d_reclen = len + dirhdrln;
      dirs[i].d_type = DT_UNKNOWN;
      strncpy(dirs[i].d_name, cp, len);
      dirs[i].d_name[len] = '\0';
      i++;
    }

    return dirs;
  }

  *size = 0;
  return NULL;
}


//------------------------------------------------------------------------------
// Create a directory with the given name
//------------------------------------------------------------------------------
int
xrd_mkdir (const char* path,
           mode_t mode,
           uid_t uid,
           gid_t gid,
           pid_t pid,
           struct stat* buf)
{
  eos_static_info("path=%s mode=%d uid=%u pid=%u", path, mode, uid, pid);
  eos::common::Timing mkdirtiming("xrd_mkdir");
  errno = 0;
  COMMONTIMING("START", &mkdirtiming);

  std::string request;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  request = path;
  request += '?';
  request += "mgm.pcmd=mkdir";
  request += "&eos.app=fuse&mode=";
  request += (int) mode;
  arg.FromString(request);

  std::string surl=xrd_user_url(uid, gid, pid);
  if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) surl += '?';
  surl += xrd_strongauth_cgi(pid);
  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile,
                                        arg, response);
  COMMONTIMING("GETPLUGIN", &mkdirtiming);

  if (status.IsOK())
  {
    unsigned long long sval[10];
    unsigned long long ival[6];
    char tag[1024];
    //..........................................................................
    // Parse output
    //..........................................................................
    int items = sscanf(response->GetBuffer(),
                       "%s %llu %llu %llu %llu %llu %llu %llu %llu "
                       "%llu %llu %llu %llu %llu %llu %llu %llu",
                       tag, (unsigned long long*) &sval[0],
                       (unsigned long long*) &sval[1],
                       (unsigned long long*) &sval[2],
                       (unsigned long long*) &sval[3],
                       (unsigned long long*) &sval[4],
                       (unsigned long long*) &sval[5],
                       (unsigned long long*) &sval[6],
                       (unsigned long long*) &sval[7],
                       (unsigned long long*) &sval[8],
                       (unsigned long long*) &sval[9],
                       (unsigned long long*) &ival[0],
                       (unsigned long long*) &ival[1],
                       (unsigned long long*) &ival[2],
                       (unsigned long long*) &ival[3],
                       (unsigned long long*) &ival[4],
                       (unsigned long long*) &ival[5]);

    if ((items != 17) || (strcmp(tag, "mkdir:")))
    {
      int retc = 0;
      char tag[1024];
      // Parse output
      int items = sscanf(response->GetBuffer(), "%s retc=%d", tag, &retc);
      
      if ((items != 2) || (strcmp(tag, "mkdir:")))
	errno = EFAULT;
      else
	errno = retc;
      
      delete response;
      return errno;
    }
    else
    {
      buf->st_dev = (dev_t) sval[0];
      buf->st_ino = (ino_t) sval[1];
      buf->st_mode = (mode_t) sval[2];
      buf->st_nlink = (nlink_t) sval[3];
      buf->st_uid = (uid_t) sval[4];
      buf->st_gid = (gid_t) sval[5];
      buf->st_rdev = (dev_t) sval[6];
      buf->st_size = (off_t) sval[7];
      buf->st_blksize = (blksize_t) sval[8];
      buf->st_blocks = (blkcnt_t) sval[9];
#ifdef __APPLE__
      buf->st_atimespec.tv_sec = (time_t) ival[0];
      buf->st_mtimespec.tv_sec = (time_t) ival[1];
      buf->st_ctimespec.tv_sec = (time_t) ival[2];
      buf->st_atimespec.tv_nsec = (time_t) ival[3];
      buf->st_mtimespec.tv_nsec = (time_t) ival[4];
      buf->st_ctimespec.tv_nsec = (time_t) ival[5];
#else
      buf->st_atime = (time_t) ival[0];
      buf->st_mtime = (time_t) ival[1];
      buf->st_ctime = (time_t) ival[2];
      buf->st_atim.tv_sec = (time_t) ival[0];
      buf->st_mtim.tv_sec = (time_t) ival[1];
      buf->st_ctim.tv_sec = (time_t) ival[2];
      buf->st_atim.tv_nsec = (time_t) ival[3];
      buf->st_mtim.tv_nsec = (time_t) ival[4];
      buf->st_ctim.tv_nsec = (time_t) ival[5];
#endif

      if (S_ISREG(buf->st_mode) && fuse_exec)
        buf->st_mode |= (S_IXUSR | S_IXGRP | S_IXOTH);

      buf->st_mode &= (~S_ISVTX); // clear the vxt bit
      buf->st_mode &= (~S_ISUID); // clear suid
      buf->st_mode &= (~S_ISGID); // clear sgid
      errno = 0;
    }
  }
  else
  {
    eos_static_err("status is NOT ok");
    errno = EFAULT;
  }

  COMMONTIMING("END", &mkdirtiming);

  if (EOS_LOGS_DEBUG)
    mkdirtiming.Print();

  eos_static_debug("path=%s inode=%llu", path, buf->st_ino);
  delete response;
  return errno;
}


//------------------------------------------------------------------------------
// Remove the given directory
//------------------------------------------------------------------------------
int
xrd_rmdir (const char* path, uid_t uid, gid_t gid, pid_t pid)
{
  eos_static_info("path=%s uid=%u pid=%u", path, uid, pid);

  std::string surl=xrd_user_url(uid, gid, pid);
  if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) surl += '?';
  surl += xrd_strongauth_cgi(pid);
  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = fs.RmDir(path);

  if (xrd_error_retc_map(status.errNo))
  {
    if(status.GetErrorMessage().find("Directory not empty")!=std::string::npos)
      errno = ENOTEMPTY;
    return errno;
  }
  else
    return 0;
}


//------------------------------------------------------------------------------
// Map open return codes to errno's
//------------------------------------------------------------------------------
int
xrd_error_retc_map (int retc)
{
  if (retc) errno = retc;
  if (retc == kXR_ArgInvalid)
    errno = EINVAL;

  if (retc == kXR_ArgMissing)
    errno = EINVAL;

  if (retc == kXR_ArgTooLong)
    errno = E2BIG;

  if (retc == kXR_FileNotOpen)
    errno = EBADF;

  if (retc == kXR_FSError)
    errno = EIO;

  if (retc == kXR_InvalidRequest)
    errno = EINVAL;

  if (retc == kXR_IOError)
    errno = EIO;

  if (retc == kXR_NoMemory)
    errno = ENOMEM;

  if (retc == kXR_NoSpace)
    errno = ENOSPC;

  if (retc == kXR_ServerError)
    errno = EIO;

  if (retc == kXR_NotAuthorized)
    errno = EPERM;

  if (retc == kXR_NotFound)
    errno = ENOENT;

  if (retc == kXR_Unsupported)
    errno = ENOTSUP;

  if (retc == kXR_NotFile)
    errno = EISDIR;

  if (retc == kXR_isDirectory)
    errno = EISDIR;

  if (retc == kXR_Cancelled)
    errno = ECANCELED;

  if (retc == kXR_ChkLenErr)
    errno = ERANGE;

  if (retc == kXR_ChkSumErr)
    errno = ERANGE;

  if (retc == kXR_inProgress)
    errno = EAGAIN;

  if (retc)
    return -1;

  return 0;
}

//------------------------------------------------------------------------------
// Open a file
//------------------------------------------------------------------------------
int
xrd_open (const char* path,
          int oflags,
          mode_t mode,
          uid_t uid,
          gid_t gid,
          pid_t pid,
          unsigned long* return_inode)
{
  eos_static_info("path=%s flags=%08x mode=%d uid=%u pid=%u",
                  path, oflags, mode, uid, pid);
  XrdOucString spath = xrd_user_url(uid, gid, pid);
  XrdSfsFileOpenMode flags_sfs = eos::common::LayoutId::MapFlagsPosix2Sfs(oflags);
  struct stat buf;
  bool exists = true;

  eos::common::Timing opentiming("xrd_open");
  COMMONTIMING("START", &opentiming);

  spath += path;
  errno = 0;
  int t0;
  int retc = xrd_add_fd2file(0, *return_inode, uid, gid, pid, path);

  if (retc != -1)
  {
    eos_static_debug("file already opened, return fd=%i", retc);
    return retc;
  }


  if ((t0 = spath.find("/proc/")) != STR_NPOS)
  {
    XrdOucString orig_path=spath;
    // Clean the path
    int t1 = spath.find("//");
    int t2 = spath.find("//", t1 + 2);
    spath.erase(t2 + 2, t0 - t2 - 2);

    while (spath.replace("///", "//")) { };

    // Force a reauthentication to the head node
    if (spath.endswith("/proc/reconnect"))
    {
      if(use_user_gsiproxy || use_user_krb5cc)
      {
        xrd_lock_w_pcache (pid);
        authidmanager.reconnectProcCache(uid,gid,pid);
        xrd_unlock_w_pcache (pid);
      }
      else
      {
        XrdSysMutexHelper cLock(connectionIdMutex);
        connectionId++;
      }
      errno = ECONNABORTED;
      return -1;
    }

    // Return the 'whoami' information in that file
    if (spath.endswith("/proc/whoami"))
    {
      spath.replace("/proc/whoami", "/proc/user/");
      //spath += "?mgm.cmd=whoami&mgm.format=fuse&eos.app=fuse";
      spath += '?';
      spath += xrd_strongauth_cgi(pid);
      if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) spath += '&';
      spath += "mgm.cmd=whoami&mgm.format=fuse&eos.app=fuse";
      LayoutWrapper* file = new LayoutWrapper( new eos::fst::PlainLayout(NULL, 0, NULL, NULL,
                                                         eos::common::LayoutId::kXrdCl) , use_localtime_consistency);

      XrdOucString open_path = get_url_nocgi(spath.c_str());
      XrdOucString open_cgi = get_cgi(spath.c_str());

      if(xrd_stat(open_path.c_str(),&buf,uid,gid,pid,0))
        exists = false;

      retc = file->Open(open_path.c_str(), flags_sfs, mode, open_cgi.c_str(),exists?&buf:NULL);

      if (retc)
      {
        eos_static_err("open failed for %s", spath.c_str());
        return xrd_error_retc_map(errno);
      }
      else
      {
        retc = xrd_add_fd2file(file, *return_inode, uid, gid, pid);
        return retc;
      }
    }

    if (spath.endswith("/proc/who"))
    {
      spath.replace("/proc/who", "/proc/user/");
      //spath += "?mgm.cmd=who&mgm.format=fuse&eos.app=fuse";
      spath += '?';
      spath += xrd_strongauth_cgi(pid);
      if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) spath += '&';
      spath += "mgm.cmd=who&mgm.format=fuse&eos.app=fuse";
      LayoutWrapper* file = new LayoutWrapper( new eos::fst::PlainLayout(NULL, 0, NULL, NULL,
                                                         eos::common::LayoutId::kXrdCl) , use_localtime_consistency);
      XrdOucString open_path = get_url_nocgi(spath.c_str());
      XrdOucString open_cgi = get_cgi(spath.c_str());

      if(xrd_stat(open_path.c_str(),&buf,uid,gid,pid,0))
        exists = false;
      retc = file->Open(open_path.c_str(), flags_sfs, mode, open_cgi.c_str(),exists?&buf:NULL);

      if (retc)
      {
        eos_static_err("open failed for %s", spath.c_str());
        return xrd_error_retc_map(errno);
      }
      else
      {
        retc = xrd_add_fd2file(file, *return_inode, uid, gid,pid);
        return retc;
      }
    }

    if (spath.endswith("/proc/quota"))
    {
      spath.replace("/proc/quota", "/proc/user/");
      //spath += "?mgm.cmd=quota&mgm.subcmd=lsuser&mgm.format=fuse&eos.app=fuse";
      spath += '?';
      spath += xrd_strongauth_cgi(pid);
      if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) spath += '&';
      spath += "mgm.cmd=quota&mgm.subcmd=lsuser&mgm.format=fuse&eos.app=fuse";
      LayoutWrapper* file = new LayoutWrapper( new eos::fst::PlainLayout(NULL, 0, NULL, NULL,
                                                         eos::common::LayoutId::kXrdCl) , use_localtime_consistency);

      XrdOucString open_path = get_url_nocgi(spath.c_str());
      XrdOucString open_cgi = get_cgi(spath.c_str());

      if(xrd_stat(open_path.c_str(),&buf,uid,gid,pid,0))
        exists = false;
      retc = file->Open(open_path.c_str(), flags_sfs, mode, open_cgi.c_str(),exists?&buf:NULL);

      if (retc)
      {
        eos_static_err("open failed for %s", spath.c_str());
        return xrd_error_retc_map(errno);
      }
      else
      {
        retc = xrd_add_fd2file(file, *return_inode, uid,gid,pid);
        return retc;
      }
    }
    spath=orig_path;
  }

  // Try to open file using PIO (parallel io) only in read mode
  if ((!getenv("EOS_FUSE_NOPIO")) && (flags_sfs == SFS_O_RDONLY))
  {
    XrdCl::Buffer arg;
    XrdCl::Buffer* response = 0;
    XrdCl::XRootDStatus status;
    std::string file_path = path;
    size_t spos = file_path.rfind("//");

    if (spos != std::string::npos)
      file_path.erase(0, spos + 1);

    std::string request = file_path;
    request += "?eos.app=fuse&mgm.pcmd=open";
    arg.FromString(request);

    std::string surl=xrd_user_url(uid, gid, pid);
    if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) surl += '?';
    surl += xrd_strongauth_cgi(pid);
    XrdCl::URL Url(surl);
    XrdCl::FileSystem fs(Url);
    status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg, response);

    if (status.IsOK())
    {
      // Parse output
      XrdOucString tag;
      XrdOucString stripePath;
      std::vector<std::string> stripeUrls;

      XrdOucString origResponse = response->GetBuffer();
      XrdOucString stringOpaque = response->GetBuffer();

      // Add the eos.app=fuse tag to all future PIO open requests
      origResponse += "&eos.app=fuse";

      while (stringOpaque.replace("?", "&")) {}

      while (stringOpaque.replace("&&", "&")) {}

      XrdOucEnv* openOpaque = new XrdOucEnv(stringOpaque.c_str());
      char* opaqueInfo = (char*) strstr(origResponse.c_str(), "&mgm.logid");

      if (opaqueInfo)
      {
        opaqueInfo += 1;
        LayoutId::layoutid_t layout = openOpaque->GetInt("mgm.lid");

        for (unsigned int i = 0; i <= eos::common::LayoutId::GetStripeNumber(layout); i++)
        {
          tag = "pio.";
          tag += static_cast<int> (i);
          stripePath = "root://";
          stripePath += openOpaque->Get(tag.c_str());
          stripePath += "/";
          stripePath += file_path.c_str();
          stripeUrls.push_back(stripePath.c_str());
        }

        eos::fst::RaidMetaLayout* file;

        if (LayoutId::GetLayoutType(layout) == LayoutId::kRaidDP)
        {
          file = new eos::fst::RaidDpLayout(NULL, layout, NULL, NULL,
                                            eos::common::LayoutId::kXrdCl);
        }
        else if ((LayoutId::GetLayoutType(layout) == LayoutId::kRaid6) ||
                 (LayoutId::GetLayoutType(layout) == LayoutId::kArchive))
        {
          file = new eos::fst::ReedSLayout(NULL, layout, NULL, NULL,
                                           eos::common::LayoutId::kXrdCl);
        }
        else
        {
          eos_static_warning("warning=no such supported layout for PIO");
          file = 0;
        }

        if (file)
        {
          retc = file->OpenPio(stripeUrls,
                               flags_sfs,
                               mode,
                               opaqueInfo);
          if (retc)
          {
            eos_static_err("failed open for pio red, path=%s", spath.c_str());
            delete file;
            return xrd_error_retc_map(errno);
          }
          else
          {
            if (return_inode)
            {
              // Try to extract the inode from the opaque redirection
              XrdOucEnv RedEnv = file->GetLastUrl().c_str();
              const char* sino = RedEnv.Get("mgm.id");

              if (sino)
                *return_inode = eos::common::FileId::Hex2Fid(sino) << 28;
              else
                *return_inode = 0;

              eos_static_debug("path=%s created inode=%lu", path,
                               (unsigned long)*return_inode);
            }

            retc = xrd_add_fd2file(new LayoutWrapper( file , use_localtime_consistency), *return_inode, uid,gid,pid);
            return retc;
          }
        }
      }
      else
        eos_static_debug("opaque info not what we expected");
    }
    else
      eos_static_err("failed get request for pio read. query was   %s  ,  response was   %s    and   error was    %s",arg.ToString().c_str(),response->ToString().c_str(),status.ToStr().c_str());
  }

  eos_static_debug("the spath is:%s", spath.c_str());
  LayoutWrapper* file = new LayoutWrapper (new eos::fst::PlainLayout(NULL, 0, NULL, NULL,
                                                     eos::common::LayoutId::kXrdCl) , use_localtime_consistency);
  XrdOucString open_cgi = "eos.app=fuse";

  if (oflags & (O_RDWR | O_WRONLY))
  {
      open_cgi += "&eos.bookingsize=0";
  }

  if (do_rdahead)
  {
    open_cgi += "&fst.readahead=true&fst.blocksize=";
    open_cgi += rdahead_window.c_str();
  }

  if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared)
  {
    open_cgi += "&";
    open_cgi += xrd_strongauth_cgi(pid);
  }

  // check if the file already exist
  if(xrd_stat(path,&buf,uid,gid,pid,0))
    exists = false;
  eos_static_debug("open_path=%s, open_cgi=%s, exists=%d", spath.c_str(), open_cgi.c_str(), (int)exists);
  retc = file->Open(spath.c_str(), flags_sfs, mode, open_cgi.c_str(),exists?&buf:NULL);

  if (retc)
  {
    eos_static_err("open failed for %s.", spath.c_str());
    delete file;
    return xrd_error_retc_map(errno);
  }
  else
  {
    if (return_inode)
    {
      // Try to extract the inode from the opaque redirection
      XrdOucEnv RedEnv = file->GetLastUrl().c_str();
      const char* sino = RedEnv.Get("mgm.id");

      ino_t old_ino = return_inode?*return_inode:0;
      ino_t new_ino = sino? (eos::common::FileId::Hex2Fid(sino) << 28): 0;
      if (old_ino && (old_ino != new_ino))
      {
	// an inode of an existing file can be changed during the process of an open due to an auto-repair
	std::ostringstream sstr_old;
	std::ostringstream sstr_new;
	sstr_old << old_ino << ":" << get_xrd_login(uid,gid,pid);
	sstr_new << new_ino << ":" << get_xrd_login(uid,gid,pid);
	{
	  eos::common::RWMutexWriteLock wr_lock(rwmutex_fd2fabst);
	  if (inodexrdlogin2fd.count(sstr_old.str()))
	  {
	    inodexrdlogin2fd[sstr_new.str()] = inodexrdlogin2fd[sstr_old.str()];
	    inodexrdlogin2fd.erase(sstr_old.str());
	  }
	}

	{
	  eos::common::RWMutexWriteLock wr_lock(mutex_inode_path);
	  path2inode.erase(path);
	  inode2path.erase(old_ino);
	  path2inode[path] = new_ino;
	  inode2path[new_ino] = path;
	  eos_static_info("msg=\"inode replaced remotely\" path=%s old-ino=%lu new-ino=%lu", path, old_ino, new_ino);
	}
      }

      *return_inode = new_ino;
      
      eos_static_debug("path=%s opened ino=%lu", path, (unsigned long)*return_inode);
    }

    retc = xrd_add_fd2file(file, *return_inode, uid, gid,pid,path);

    COMMONTIMING("end", &opentiming);
    
    if (EOS_LOGS_DEBUG)
    opentiming.Print();

    return retc;
  }
}


//------------------------------------------------------------------------------
// Release is called when FUSE is completely done with a file; at that point,
// you can free up any temporarily allocated data structures.
//------------------------------------------------------------------------------
int
xrd_close (int fildes, unsigned long inode, uid_t uid, gid_t gid, pid_t pid)
{
  eos_static_info("fd=%d inode=%lu, uid=%i, gid=%i, pid=%i", fildes, inode, uid, gid, pid);
  int ret = -1;
  FileAbstraction* fabst = xrd_get_file(fildes);

  if (!fabst)
  {
    errno = ENOENT;
    return ret;
  }

  if (XFC)
  {
    fabst->mMutexRW.WriteLock();
    XFC->ForceAllWrites(fabst);
    fabst->mMutexRW.UnLock();
  }

  // Close file and remove it from all mappings
  ret = xrd_remove_fd2file(fildes, inode, uid, gid, pid);

  if (ret)
    errno = EIO;

  return ret;
}


//------------------------------------------------------------------------------
// Flush file data to disk
//------------------------------------------------------------------------------
int
xrd_flush (int fd, uid_t uid, gid_t gid, pid_t pid)
{
  int retc = 0;
  eos_static_info("fd=%d ", fd);
  FileAbstraction* fabst = xrd_get_file(fd);

  if (!fabst)
  {
    errno = ENOENT;
    return -1;
  }

  if (XFC && fuse_cache_write)
  {
    fabst->mMutexRW.WriteLock();
    XFC->ForceAllWrites(fabst);
    eos::common::ConcurrentQueue<error_type> err_queue = fabst->GetErrorQueue();
    error_type error;

    if (err_queue.try_pop(error))
    {
      eos_static_info("Extract error from queue");
      retc = error.first;
    }

    auto raw_file = fabst->GetRawFile();
    if(raw_file->IsOpen())
    {
      int retc2 = raw_file->Close ();
      eos_static_debug("temporarily closing file %s  returned  %d",raw_file->GetLastPath().c_str(),retc2);
      if(retc2) retc=retc2;
      struct timespec utimes[2];
      const char* path = 0;
      if ((path = fabst->GetUtimes (utimes)))
      {
        eos_static_debug("CLOSEDEBUG closing file %s open with flag %d and utiming",raw_file->GetOpenPath().c_str(),(int)raw_file->GetOpenFlags());
        // run the utimes command now after the close
        xrd_utimes (path, utimes, uid, gid, pid);
      }
      else
        eos_static_debug("CLOSEDEBUG closing file %s open with flag %d and NOT utiming",raw_file->GetOpenPath().c_str(),(int)raw_file->GetOpenFlags());
    }
    fabst->mMutexRW.UnLock();

  }

  fabst->DecNumRef();
  return retc;
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
xrd_truncate (int fildes, off_t offset)
{
  int ret = -1;
  eos_static_info("fd=%d offset=%llu", fildes, (unsigned long long) offset);
  FileAbstraction* fabst = xrd_get_file(fildes);
  errno = 0;

  if (!fabst)
  {
    errno = ENOENT;
    return ret;
  }

  LayoutWrapper* file = fabst->GetRawFile();

  if (XFC && fuse_cache_write)
  {
    fabst->mMutexRW.WriteLock();
    XFC->ForceAllWrites(fabst);
    ret = file->Truncate(offset);
    fabst->mMutexRW.UnLock();
  }
  else
    ret = file->Truncate(offset);

  fabst->DecNumRef();

  if (ret == -1)
    errno = EIO;

  return ret;
}

//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
xrd_truncate2 (const char *fullpath, unsigned long inode, unsigned long truncsize, uid_t uid, gid_t gid, pid_t pid)
{
  if (inode)
  {
    // Try to truncate via an open file - first find the file descriptor using the
    // inodeuser2fd map and then find the file object using the fd2fabst map.
    // Meanwhile keep the mutex locked for read so that no other thread can
    // delete the file object
    eos_static_debug("path=%s, uid=%lu, inode=%lu",
                     fullpath, (unsigned long) uid, inode);
    eos::common::RWMutexReadLock rd_lock(rwmutex_fd2fabst);
    std::ostringstream sstr;
    sstr << inode << ":" << get_xrd_login(uid,gid,pid);
    google::dense_hash_map<std::string, int>::iterator
      iter_fd = inodexrdlogin2fd.find(sstr.str());

    if (iter_fd != inodexrdlogin2fd.end())
    {
      return xrd_truncate(iter_fd->second,truncsize);
    }
    else
      eos_static_debug("path=%s not open", fullpath);
  }

  int fd, retc = -1;
  unsigned long rinode;

  if ((fd = xrd_open (fullpath, O_WRONLY,
  S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH,
                      uid, gid, pid, &rinode)) > 0)
  {
    retc = xrd_truncate (fd, truncsize);
    xrd_close (fd, rinode, uid, gid, pid);
  }
  else
    retc = errno;

  return retc;
}

//------------------------------------------------------------------------------
// Read from file. Returns the number of bytes transferred, or 0 if offset
// was at or beyond the end of the file
//------------------------------------------------------------------------------
ssize_t
xrd_pread (int fildes,
           void* buf,
           size_t nbyte,
           off_t offset)
{
  eos::common::Timing xpr("xrd_pread");
  COMMONTIMING("start", &xpr);

  eos_static_debug("fd=%d nbytes=%lu offset=%llu",
                   fildes, (unsigned long) nbyte,
                   (unsigned long long) offset);
  ssize_t ret = -1;
  FileAbstraction* fabst = xrd_get_file(fildes);

  if (!fabst)
  {
    errno = ENOENT;
    return ret;
  }

  if (XFC && fuse_cache_write)
  {
    fabst->mMutexRW.WriteLock();
    XFC->ForceAllWrites(fabst);
    LayoutWrapper* file = fabst->GetRawFile();
    ret = file->Read(offset, static_cast<char*> (buf), nbyte, do_rdahead);
    fabst->mMutexRW.UnLock();
  }
  else
  {
    LayoutWrapper* file = fabst->GetRawFile();
    ret = file->Read(offset, static_cast<char*> (buf), nbyte, do_rdahead);
  }

  // Release file reference
  fabst->DecNumRef();
  COMMONTIMING("end", &xpr);

  if (ret == -1)
  {
    eos_static_err("failed read off=%ld, len=%u", offset, nbyte);
    errno = EIO;
  }
  else if ((size_t)ret != nbyte)
  {
    eos_static_warning("read size=%u, returned=%u", nbyte, ret);
  }

  if (EOS_LOGS_DEBUG)
    xpr.Print();

  return ret;
}


//------------------------------------------------------------------------------
// Write to file
//------------------------------------------------------------------------------
ssize_t
xrd_pwrite (int fildes,
            const void* buf,
            size_t nbyte,
            off_t offset)
{
  eos::common::Timing xpw("xrd_pwrite");
  COMMONTIMING("start", &xpw);
  eos_static_debug("fd=%d nbytes=%lu cache=%d cache-w=%d",
                   fildes, (unsigned long) nbyte, XFC ? 1 : 0,
                   fuse_cache_write);
  int64_t ret = -1;
  FileAbstraction* fabst = xrd_get_file(fildes);

  if (!fabst)
  {
    errno = ENOENT;
    return ret;
  }


  if (XFC && fuse_cache_write)
  {
    fabst->mMutexRW.ReadLock();
    XFC->SubmitWrite(fabst, const_cast<void*> (buf), offset, nbyte);
    fabst->mMutexRW.UnLock();
    // to modify the timestamp
    if(use_localtime_consistency)
      fabst->GetRawFile()->Write(0, NULL, -1);
    ret = nbyte;
  }
  else
  {
    LayoutWrapper* file = fabst->GetRawFile();
    ret = file->Write(offset, static_cast<const char*> (buf), nbyte);

    if (ret == -1)
      errno = EIO;
  }

  // Release file reference
  fabst->DecNumRef();
  COMMONTIMING("end", &xpw);

  if (EOS_LOGS_DEBUG)
    xpw.Print();

  return ret;
}


//------------------------------------------------------------------------------
// Flush any dirty information about the file to disk
//------------------------------------------------------------------------------
int
xrd_fsync (int fildes)
{
  eos_static_info("fd=%d", fildes);
  int ret = -1;
  FileAbstraction* fabst = xrd_get_file(fildes);

  if (!fabst)
  {
    errno = ENOENT;
    return ret;
  }

  if (XFC && fuse_cache_write)
  {
    fabst->mMutexRW.WriteLock();
    XFC->ForceAllWrites(fabst);
    fabst->mMutexRW.UnLock();
  }

  LayoutWrapper* file = fabst->GetRawFile();
  ret = file->Sync();

  if (ret)
    errno = EIO;

  // Release file reference
  fabst->DecNumRef();
  return ret;
}


//------------------------------------------------------------------------------
// Remove (delete) the given file, symbolic link, hard link, or special node
//------------------------------------------------------------------------------
int
xrd_unlink (const char* path,
            uid_t uid,
            gid_t gid,
            pid_t pid)
{
  eos_static_info("path=%s uid=%u, pid=%u", path, uid, pid);
  std::string surl=xrd_user_url(uid, gid, pid);
  if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) surl += '?';
  surl += xrd_strongauth_cgi(pid);
  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);
  XrdCl::XRootDStatus status = fs.Rm(path);

  if (xrd_error_retc_map(status.errNo))
    return errno;
  else
    return 0;
}


//------------------------------------------------------------------------------
// Rename file/dir
//------------------------------------------------------------------------------
int
xrd_rename (const char* oldpath,
            const char* newpath,
            uid_t uid,
            gid_t gid,
            pid_t pid)
{
  eos_static_info("oldpath=%s newpath=%s", oldpath, newpath, uid, pid);
  XrdOucString sOldPath = oldpath;
  XrdOucString sNewPath = newpath;

  // XRootd move cannot deal with space in the path names
  sOldPath.replace(" ","#space#");
  sNewPath.replace(" ","#space#");
  std::string surl=xrd_user_url(uid, gid, pid);
  if((use_user_krb5cc||use_user_gsiproxy) && fuse_shared) surl += '?';
  surl += xrd_strongauth_cgi(pid);
  XrdCl::URL Url(surl);
  XrdCl::FileSystem fs(Url);

  XrdCl::XRootDStatus status = fs.Mv(sOldPath.c_str(), sNewPath.c_str());

  if (xrd_error_retc_map(status.errNo))
    return errno;
  else
    return 0;
}

const char* xrd_strongauth_cgi (pid_t pid)
{
  XrdOucString str = "";

  if (fuse_shared && (use_user_krb5cc || use_user_gsiproxy))
  {
    char buffer[256];
    buffer[0]=(char)0;
    proccache_GetAuthMethod(pid,buffer,256);
    std::string authmet(buffer);
    if (authmet.compare (0, 5, "krb5:") == 0)
    {
      str += "xrd.k5ccname=";
      str += (authmet.c_str()+5);
      str += "&xrd.wantprot=krb5";
    }
    else if (authmet.compare (0, 5, "krk5:") == 0)
    {
      str += "xrd.k5ccname=";
      str += (authmet.c_str()+5);
      str += "&xrd.wantprot=krb5";
    }
    else if (authmet.compare (0, 5, "x509:") == 0)
    {
      str += "xrd.gsiusrpxy=";
      str += authmet.c_str()+5;
      str += "&xrd.wantprot=gsi";
    }
    else
    {
      eos_static_err("don't know what to do with qualifiedid [%s]", authmet.c_str ());
      goto bye;
    }
  }

  bye:
  eos_static_debug("pid=%lu sep=%s", (unsigned long ) pid, str.c_str ());
  return STRINGSTORE(str.c_str());
}

//------------------------------------------------------------------------------
// Get a user private physical connection URL like root://<user>@<host>
// - if we are a user private mount we don't need to specify that
//------------------------------------------------------------------------------
const char*
xrd_user_url (uid_t uid, gid_t gid, pid_t pid)
{
  std::string url = "root://";

  if (fuse_shared)
  {
    if (use_user_krb5cc || use_user_gsiproxy)
    {
      url += authidmanager.getXrdLogin(pid);
        url += "@";
    }
    else
    {
      url += xrd_mapuser (uid, gid, pid,0);
      url += "@";
    }
  }

  url += gMgmHost.c_str();
  url += "/";

  eos_static_debug("uid=%lu gid=%lu pid=%lu url=%s",
                   (unsigned long) uid,
                   (unsigned long) gid,
                   (unsigned long) pid, url.c_str());
  return STRINGSTORE(url.c_str());
}


//------------------------------------------------------------------------------
// Cache that holds the mapping from a pid to a time stamp (to see if the cache needs
// to be refreshed and bool to check if the operation needs to be denied.
// variable which is true if this is a top level rm operation and false other-
// wise. It is used by recursive rm commands which belong to the same pid in
// order to decide if his operation is denied or not.
//------------------------------------------------------------------------------
eos::common::RWMutex mMapPidDenyRmMutex;
std::map<pid_t, std::pair<time_t,bool> > mMapPidDenyRm;

//------------------------------------------------------------------------------
// Decide if this is an 'rm -rf' command issued on the toplevel directory or
// anywhere whithin the EOS_FUSE_RMLVL_PROTECT levels from the root directory
//------------------------------------------------------------------------------
int is_toplevel_rm(int pid, char* local_dir)
{
  eos_static_debug("is_toplevel_rm for pid %d and mountpoint %s",pid,local_dir);
  if(rm_level_protect==0)
    return 0;

  time_t psstime;
  if(proccache_GetPsStartTime(pid,&psstime))
    eos_static_err("could not get process start time");

  // Check the cache
  {
    eos::common::RWMutexReadLock rlock (mMapPidDenyRmMutex);
    auto it_map = mMapPidDenyRm.find (pid);

    if (it_map != mMapPidDenyRm.end ())
    {
      eos_static_debug("found an entry in the cache");
      // if the cached denial is up to date, return it
      if (psstime <= it_map->second.first)
      {
        eos_static_debug("found in cache pid=%i, rm_deny=%i", it_map->first, it_map->second.second);
        if (it_map->second.second)
        {
          std::string cmd = gProcCache.GetEntry (pid)->GetArgsStr ();
          eos_static_notice("rejected toplevel recursive deletion command %s", cmd.c_str ());
        }
        return (it_map->second.second ? 1 : 0);
      }
      eos_static_debug("the entry is oudated in cache %d, current %d", (int )it_map->second.first, (int )psstime);
    }
  }

  // create an entry if it does not exist or if it's outdated
  eos_static_debug("no entry found or outdated entry, creating entry with psstime %d",(int)psstime);
  auto entry = std::make_pair(psstime,false);

  // Try to print the command triggering the unlink
  std::ostringstream oss;
  const auto &cmdv = gProcCache.GetEntry(pid)->GetArgsVec();
  std::string cmd = gProcCache.GetEntry(pid)->GetArgsStr();
  std::set<std::string> rm_entries;
  std::set<std::string> rm_opt; // rm command options (long and short)
  char exe[PATH_MAX];
  oss.str("");
  oss.clear();
  oss << "/proc/" << pid << "/exe";
  ssize_t len = readlink(oss.str().c_str(), exe, sizeof(exe) - 1);
  if (len == -1)
  {
    eos_static_err("error while reading cwd for path=%s", oss.str().c_str());
    return 0;
  }
  exe[len] = '\0';
  //std::string rm_cmd = *cmdv.begin();
  std::string rm_cmd = exe;
  std::string token;
  for(auto it=cmdv.begin()+1; it!=cmdv.end();it++)
  {
    token = *it;
    // Long option
    if (token.find("--") == 0)
    {
      token.erase(0, 2);
      rm_opt.insert(token);
    }
    else if (token.find('-') == 0)
    {
      token.erase(0, 1);

      // Short option
      size_t length = token.length();

      for (size_t i = 0; i != length; ++i)
        rm_opt.insert(std::string(&token[i], 1));
    }
    else
      rm_entries.insert(token);
  }

  for (std::set<std::string>::iterator it = rm_opt.begin();
       it != rm_opt.end(); ++it)
  {
    eos_static_debug("rm option:%s", it->c_str());
  }

  // Exit if this is not a recursive removal
  auto fname  = rm_cmd.length()<2?rm_cmd:rm_cmd.substr(rm_cmd.length()-2,2);
  bool isrm = rm_cmd.length()<=2?(fname=="rm"):( fname=="rm" && rm_cmd[rm_cmd.length()-3]=='/');
  if ( !isrm ||
      ( isrm &&
       rm_opt.find("r") == rm_opt.end() &&
       rm_opt.find("recursive") == rm_opt.end()))
  {
    eos_static_debug("%s is not an rm command",rm_cmd.c_str());
    mMapPidDenyRmMutex.LockWrite();
    mMapPidDenyRm[pid] = entry;
    mMapPidDenyRmMutex.UnLockWrite();
    return 0;
  }

  // check that we dealing with the system rm command
  bool skip_relpath = !rm_watch_relpath;
  if( (!skip_relpath) && (rm_cmd!=rm_command) )
  {
    eos_static_warning("using rm command %s different from the system rm command %s : cannot watch recursive deletion on relative paths"
        ,rm_cmd.c_str(),rm_command.c_str());
    skip_relpath = true;
  }

  // get the current working directory
  oss.str("");
  oss.clear();
  oss << "/proc/" << pid << "/cwd";
  char cwd[PATH_MAX];
  len = readlink(oss.str().c_str(), cwd, sizeof(cwd) - 1);
  if (len == -1)
  {
    eos_static_err("error while reading cwd for path=%s", oss.str().c_str());
    return 0;
  }

  cwd[len] = '\0';
  std::string scwd (cwd);

  if (*scwd.rbegin() != '/')
      scwd += '/';

  // we are dealing with an rm command
  {
    std::set<std::string> rm_entries2;
    for (auto it = rm_entries.begin (); it != rm_entries.end (); it++)
    {
      char resolved_path[PATH_MAX];
      auto path2resolve = *it;
      eos_static_debug("path2resolve %s", path2resolve.c_str());
      if(path2resolve[0] != '/')
      {
        if(skip_relpath)
          {
          eos_static_debug("skipping recusive deletion check on command %s on relative path %s because rm command used is likely to chdir"
              ,cmd.c_str(),path2resolve.c_str());
          continue;
          }
        path2resolve = scwd + path2resolve;
      }
      if (myrealpath (path2resolve.c_str(), resolved_path,pid))
      {
        rm_entries2.insert (resolved_path);
        eos_static_debug("path %s resolves to realpath %s", path2resolve.c_str (), resolved_path);
      }
      else
        eos_static_warning("could not resolve path %s for top level recursive deletion protection", path2resolve.c_str ());

    }
    std::swap(rm_entries, rm_entries2);
  }

  // Make sure both the cwd and local mount dir ends with '/'
  std::string mount_dir(local_dir);

  if (*mount_dir.rbegin() != '/')
    mount_dir += '/';

  // First check if the command was launched from a location inside the hierarchy
  // of the local mount point
  eos_static_debug("cwd=%s, mount_dir=%s, skip_relpath=%d", scwd.c_str(), mount_dir.c_str(),skip_relpath?1:0);
  std::string rel_path;
  int level;

  // Detect remove from inside the mount point hierarchy
  if (!skip_relpath && scwd.find(mount_dir) == 0)
  {
    rel_path = scwd.substr(mount_dir.length());
    level = std::count(rel_path.begin(), rel_path.end(), '/') + 1;
    eos_static_debug("rm_int current_lvl=%i, protect_lvl=%i", level, rm_level_protect);

    if (level <= rm_level_protect)
    {
      entry.second = true;
      mMapPidDenyRmMutex.LockWrite();
      mMapPidDenyRm[pid] = entry;
      mMapPidDenyRmMutex.UnLockWrite();
      eos_static_notice("rejected toplevel recursive deletion command %s",cmd.c_str());
      return 1;
    }
  }

  // At this point, absolute path are used.
  // Get the deepness level it reaches inside the EOS
  // mount point so that we can take the right decision
  for (std::set<std::string>::iterator it = rm_entries.begin();
       it != rm_entries.end(); ++it)
  {
      token = *it;

    if (token.find(mount_dir) == 0)
    {
      rel_path = token.substr(mount_dir.length());
      level = std::count(rel_path.begin(), rel_path.end(), '/') + 1;
      eos_static_debug("rm_ext current_lvl=%i, protect_lvl=%i", level,
                      rm_level_protect);

      if (level <= rm_level_protect)
      {
        entry.second = true;
        mMapPidDenyRmMutex.LockWrite();
        mMapPidDenyRm[pid] = entry;
        mMapPidDenyRmMutex.UnLockWrite();
        eos_static_notice("rejected toplevel recursive deletion command %s",cmd.c_str());
        return 1;
      }
    }

    // Another case is when the delete command is issued on a directory higher
    // up in the hierarchy where the mountpoint was done
    if (mount_dir.find(*it) == 0)
    {
      level = 1;

      if (level <= rm_level_protect)
      {
        entry.second = true;
        mMapPidDenyRmMutex.LockWrite();
        mMapPidDenyRm[pid] = entry;
        mMapPidDenyRmMutex.UnLockWrite();
        eos_static_notice("rejected toplevel recursive deletion command %s",cmd.c_str());
        return 1;
      }
    }
  }

  mMapPidDenyRmMutex.LockWrite();
  mMapPidDenyRm[pid] = entry;
  mMapPidDenyRmMutex.UnLockWrite();
  return 0;
}


//------------------------------------------------------------------------------
// Extract the EOS MGM endpoint for connection and also check that the MGM
// daemon is available.
//
// @return 1 if MGM avilable, otherwise 0
//------------------------------------------------------------------------------
int xrd_check_mgm()
{
  std::string address = getenv("EOS_RDRURL");

  if (address == "")
  {
    fprintf(stderr, "error: EOS_RDRURL is not defined so we fall back to "
            "root://localhost:1094// \n");
    address = "root://localhost:1094//";
  }

  XrdCl::URL url(address);

  if (!url.IsValid())
  {
    eos_static_err("URL is not valid: %s", address.c_str());
    return 0;
  }

  // Check MGM is available
  uint16_t timeout = 3;
  XrdCl::FileSystem fs (url);
  XrdCl::XRootDStatus st = fs.Ping(timeout);

  if (!st.IsOK())
  {
    eos_static_err("Unable to contact MGM at address=%s", address.c_str());
    return 0;
  }

  gMgmHost = address.c_str();
  gMgmHost.replace("root://", "");
  return 1;
}

//------------------------------------------------------------------------------
// Init function
//------------------------------------------------------------------------------
void
xrd_init ()
{
  FILE* fstderr;

  // Open log file
  if (getuid())
  {
    fuse_shared = false; //eosfsd
    char logfile[1024];
    if (getenv("EOS_FUSE_LOGFILE")) 
    {
      snprintf(logfile, sizeof ( logfile) - 1, "%s", getenv("EOS_FUSE_LOGFILE"));
    }
    else 
    {
      snprintf(logfile, sizeof ( logfile) - 1, "/tmp/eos-fuse.%d.log", getuid());
    }

    // Running as a user ... we log into /tmp/eos-fuse.$UID.log
    if (!(fstderr = freopen(logfile, "a+", stderr)))
      fprintf(stderr, "error: cannot open log file %s\n", logfile);
    else
      chmod(logfile, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  }
  else
  {
    fuse_shared = true; //eosfsd

    // Running as root ... we log into /var/log/eos/fuse
    std::string log_path="/var/log/eos/fuse/fuse.";
    if (getenv("EOS_FUSE_LOG_PREFIX")) 
    {
      log_path += getenv("EOS_FUSE_LOG_PREFIX");
      log_path += ".log";
    } 
    else
    {
      log_path += "log";
    }
    
    eos::common::Path cPath(log_path.c_str());
    cPath.MakeParentPath(S_IRWXU | S_IRGRP | S_IROTH);

    if (!(fstderr = freopen(cPath.GetPath(), "a+", stderr)))
      fprintf(stderr, "error: cannot open log file %s\n", cPath.GetPath());
    else
      chmod(cPath.GetPath(), S_IRUSR | S_IWUSR);
  }

  setvbuf(fstderr, (char*) NULL, _IONBF, 0);

  // Initialize hashes
  path2inode.set_empty_key("");
  path2inode.set_deleted_key("#__deleted__#");

  inode2path.set_empty_key(0);
  inode2path.set_deleted_key(0xffffffffll);

  dir2inodelist.set_empty_key(0);
  dir2inodelist.set_deleted_key(0xffffffffll);

  dir2dirbuf.set_empty_key(0);
  dir2dirbuf.set_deleted_key(0xffffffffll);

  inode2cache.set_empty_key(0);
  inode2cache.set_deleted_key(0xffffffffll);

  inodexrdlogin2fd.set_empty_key("");
  inodexrdlogin2fd.set_deleted_key("#__deleted__#");

  fd2fabst.set_empty_key(-1);
  fd2fabst.set_deleted_key(-2);

  // Create the root entry
  path2inode["/"] = 1;
  inode2path[1] = "/";
  eos::common::Mapping::VirtualIdentity_t vid;
  eos::common::Mapping::Root(vid);
  eos::common::Logging::Init();
  eos::common::Logging::SetUnit("FUSE@localhost");
  eos::common::Logging::gShortFormat = true;
  XrdOucString fusedebug = getenv("EOS_FUSE_DEBUG");

  // Extract MGM endpoint and check availability
  if (xrd_check_mgm() == 0)
    exit(-1);

  // Get read-ahead configuration
  if (getenv("EOS_FUSE_RDAHEAD") && (!strcmp(getenv("EOS_FUSE_RDAHEAD"), "1")))
  {
    do_rdahead = true;

    if (getenv("EOS_FUSE_RDAHEAD_WINDOW"))
    {
      rdahead_window = getenv("EOS_FUSE_RDAHEAD_WINDOW");

      try
      {
        (void) std::stol(rdahead_window);
      }
      catch (const std::exception& e)
      {
        rdahead_window = "131072"; // default 128
      }
    }
  }

  if (getenv("EOS_FUSE_LOCALTIMECONSISTENT") && (!strcmp(getenv("EOS_FUSE_LOCALTIMECONSISTENT"), "1")))
  {
    use_localtime_consistency = 1;
  }

  if ((getenv("EOS_FUSE_DEBUG")) && (fusedebug != "0"))
  {
    eos::common::Logging::SetLogPriority(LOG_DEBUG);
  }
  else
  {
    if ((getenv("EOS_FUSE_LOGLEVEL")))
      eos::common::Logging::SetLogPriority(atoi(getenv("EOS_FUSE_LOGLEVEL")));
    else
      eos::common::Logging::SetLogPriority(LOG_INFO);
  }

  // Check if we should set files executable
  if (getenv("EOS_FUSE_EXEC") && (!strcmp(getenv("EOS_FUSE_EXEC"), "1")))
    fuse_exec = true;

  // Initialise the XrdFileCache
  fuse_cache_write = false;

  if ((!(getenv("EOS_FUSE_CACHE"))) ||
      (getenv("EOS_FUSE_CACHE") && (!strcmp(getenv("EOS_FUSE_CACHE"), "0"))))
  {
    eos_static_notice("cache=false");
    XFC = NULL;
  }
  else
  {
    if (!getenv("EOS_FUSE_CACHE_SIZE"))
      setenv("EOS_FUSE_CACHE_SIZE", "30000000", 1); // ~300MB

    eos_static_notice("cache=true size=%s cache-write=%s exec=%d",
                      getenv("EOS_FUSE_CACHE_SIZE"),
                      getenv("EOS_FUSE_CACHE_WRITE"), fuse_exec);

    XFC = FuseWriteCache::GetInstance(static_cast<size_t>(atol(getenv("EOS_FUSE_CACHE_SIZE"))));

    if (getenv("EOS_FUSE_CACHE_WRITE") && atoi(getenv("EOS_FUSE_CACHE_WRITE")))
      fuse_cache_write = true;
  }

  // Get the number of levels in the top hierarchy protected agains deletions
  if (!getenv("EOS_FUSE_RMLVL_PROTECT"))
    rm_level_protect = 1;
  else
    rm_level_protect = atoi(getenv("EOS_FUSE_RMLVL_PROTECT"));
  if(rm_level_protect)
  {
    rm_watch_relpath = false;
    char rm_cmd[PATH_MAX];
    FILE *f = popen("`which which` --skip-alias --skip-functions --skip-dot rm","r");
    if(!f)
    {
      eos_static_err("could not run the system wide rm command procedure");
    }
    else if(!fscanf(f,"%s",rm_cmd))
    {
      pclose (f);
      eos_static_err("cannot get rm command to watch");
    }
    else
    {
      pclose (f);
      eos_static_notice("rm command to watch is %s", rm_cmd);
      rm_command = rm_cmd;
      char cmd[PATH_MAX+16];
      sprintf(cmd,"%s --version",rm_cmd);
      f = popen (cmd, "r");
      if(!f)
        eos_static_err("could not run the rm command to watch");
      char *line = NULL;
      size_t len = 0;
      if ( f && getline (&line, &len, f)==-1)
      {
        pclose(f);
        if(f) eos_static_err("could not read rm command version to watch");
      }
      else if (line)
      {
        pclose(f);
        char *lasttoken = strrchr (line, ' ');
        if (lasttoken)
        {
          float rmver;
          if(!sscanf(lasttoken,"%f",&rmver))
            eos_static_err("could not interpret rm command version to watch %s",lasttoken);
          else
          {
            int rmmajv=floor(rmver);
            eos_static_notice("top level recursive deletion command to watch is %s, version is %f, major version is %d",rm_cmd,rmver,rmmajv);
            if(rmmajv>=8)
            {
              rm_watch_relpath = true;
              eos_static_notice("top level recursive deletion CAN watch relative path removals");
            }
            else
              eos_static_warning("top level recursive deletion CANNOT watch relative path removals");
          }
        }
        free (line);
      }
    }
  }

  // Get parameters about strong authentication
  if (getenv("EOS_FUSE_USER_KRB5CC") && (atoi(getenv("EOS_FUSE_USER_KRB5CC"))==1) )
    use_user_krb5cc = true;
  else
    use_user_krb5cc = false;
  if (getenv("EOS_FUSE_USER_GSIPROXY") && (atoi(getenv("EOS_FUSE_USER_GSIPROXY"))==1) )
    use_user_gsiproxy = true;
  else
    use_user_gsiproxy = false;
  if (getenv("EOS_FUSE_USER_KRB5FIRST") && (atoi(getenv("EOS_FUSE_USER_KRB5FIRST"))==1) )
    tryKrb5First= true;
  else
    tryKrb5First = false;

  // get uid and pid specificities of the system
  {
    FILE *f = fopen ("/proc/sys/kernel/pid_max", "r");
    if (f && fscanf (f, "%lu", &pid_max))
      eos_static_notice("pid_max is %llu", pid_max);
    else
    {
      eos_static_err("could not read pid_max in /proc/sys/kernel/pid_max. defaulting to 32767");
      pid_max = 32767;
    }
    if(f) fclose (f);
    f = fopen ("/etc/login.defs", "r");
    char line[4096];
    line[0]='\0';
    uid_max = 0;
    while (f && fgets (line, sizeof(line), f))
    {
      if(line[0]=='#') continue; //commented line on the first character
      auto keyword = strstr(line,"UID_MAX");
      if(!keyword) continue;
      auto comment_tag = strstr(line,"#");
      if(comment_tag && comment_tag<keyword) continue; // commented line with the keyword
      char buffer[4096];
      if(sscanf(line,"%s %lu",buffer,&uid_max)!=2)
      {
	eos_static_err("could not parse line %s in /etc/login.defs",line);
	uid_max = 0;
	continue;
      }
      else
	break;
    }
    if(uid_max)
    {
      eos_static_notice("uid_max is %llu",uid_max);
    }
    else
    {
      eos_static_err("could not read uid_max value in /etc/login.defs. defaulting to 65535");
      uid_max = 65535;
    }
    if (f) fclose (f);
  }
  proccachemutexes.resize(pid_max+1);
  authidmanager.pid2StrongLogin.resize(pid_max+1);

  // Get parameters about strong authentication
  if (getenv("EOS_FUSE_PIDMAP") && (atoi(getenv("EOS_FUSE_PIDMAP"))==1) )
    link_pidmap = true;
  else
    link_pidmap = false;


  eos_static_notice("krb5=%d",use_user_krb5cc?1:0);

  passwdstore = new XrdOucHash<XrdOucString > ();
  stringstore = new XrdOucHash<XrdOucString > ();
}

//------------------------------------------------------------------------------
// this function is just to make it possible to use BSD realpath implementation
//------------------------------------------------------------------------------
size_t strlcat (char *dst, const char *src, size_t siz)
{
  register char *d = dst;
  register const char *s = src;
  register size_t n = siz;
  size_t dlen;

  /* Find the end of dst and adjust bytes left but don't go past end */
  while (n-- != 0 && *d != '\0')
    d++;
  dlen = d - dst;
  n = siz - dlen;

  if (n == 0) return (dlen + strlen (s));
  while (*s != '\0')
  {
    if (n != 1)
    {
      *d++ = *s;
      n--;
    }
    s++;
  }
  *d = '\0';

  return (dlen + (s - src)); /* count does not include NUL */
}

//------------------------------------------------------------------------------
// this function is a workaround to avoid that
// fuse calls itself while using realpath
// that would require to trace back the user process which made the first call
// to fuse. That would involve keeping track of fuse self call to stat
// especially in is_toplevel_rm
//------------------------------------------------------------------------------
int mylstat (const char *__restrict name, struct stat *__restrict __buf, pid_t pid)
{
  std::string path (name);
  if (path.find (local_mount_dir) == 0)
  {
    eos_static_debug("name=%%s\n", name);

    uid_t uid;
    gid_t gid;
    if (proccache_GetFsUidGid (pid, &uid, &gid)) return ESRCH;

    mutex_inode_path.LockRead ();
    unsigned long long ino = path2inode.count (name) ? path2inode[name] : 0;
    mutex_inode_path.UnLockRead ();
    return xrd_stat (name, __buf, uid, gid, pid, ino);
  }
  else
    return lstat (name, __buf);
}

//------------------------------------------------------------------------------
// this is code from taken from BSD implementation
// it was just made ok for C++ compatibility
// and regular lstat was replaced with the above mylstat
//------------------------------------------------------------------------------
char *
myrealpath (const char * __restrict path, char * __restrict resolved, pid_t pid)
{
  struct stat sb;
  char *p, *q, *s;
  size_t left_len, resolved_len;
  unsigned symlinks;
  int m, serrno, slen;
  char left[PATH_MAX], next_token[PATH_MAX], symlink[PATH_MAX];

  if (path == NULL)
  {
    errno = EINVAL;
    return (NULL);
  }
  if (path[0] == '\0')
  {
    errno = ENOENT;
    return (NULL);
  }
  serrno = errno;
  if (resolved == NULL)
  {
    resolved = (char*) malloc (PATH_MAX);
    if (resolved == NULL) return (NULL);
    m = 1;
  }
  else
    m = 0;
  symlinks = 0;
  if (path[0] == '/')
  {
    resolved[0] = '/';
    resolved[1] = '\0';
    if (path[1] == '\0') return (resolved);
    resolved_len = 1;
    left_len = strlcpy (left, path + 1, sizeof(left));
  }
  else
  {
    if (getcwd (resolved, PATH_MAX) == NULL)
    {
      if (m)
        free (resolved);
      else
      {
        resolved[0] = '.';
        resolved[1] = '\0';
      }
      return (NULL);
    }
    resolved_len = strlen (resolved);
    left_len = strlcpy (left, path, sizeof(left));
  }
  if (left_len >= sizeof(left) || resolved_len >= PATH_MAX)
  {
    if (m) free (resolved);
    errno = ENAMETOOLONG;
    return (NULL);
  }

  /*
   * Iterate over path components in `left'.
   */
  while (left_len != 0)
  {
    /*
     * Extract the next path component and adjust `left'
     * and its length.
     */
    p = strchr (left, '/');
    s = p ? p : left + left_len;
    if (s - left >= (int) sizeof(next_token))
    {
      if (m) free (resolved);
      errno = ENAMETOOLONG;
      return (NULL);
    }
    memcpy (next_token, left, s - left);
    next_token[s - left] = '\0';
    left_len -= s - left;
    if (p != NULL) memmove (left, s + 1, left_len + 1);
    if (resolved[resolved_len - 1] != '/')
    {
      if (resolved_len + 1 >= PATH_MAX)
      {
        if (m) free (resolved);
        errno = ENAMETOOLONG;
        return (NULL);
      }
      resolved[resolved_len++] = '/';
      resolved[resolved_len] = '\0';
    }
    if (next_token[0] == '\0')
      continue;
    else if (strcmp (next_token, ".") == 0)
      continue;
    else if (strcmp (next_token, "..") == 0)
    {
      /*
       * Strip the last path component except when we have
       * single "/"
       */
      if (resolved_len > 1)
      {
        resolved[resolved_len - 1] = '\0';
        q = strrchr (resolved, '/') + 1;
        *q = '\0';
        resolved_len = q - resolved;
      }
      continue;
    }

    /*
     * Append the next path component and lstat() it. If
     * lstat() fails we still can return successfully if
     * there are no more path components left.
     */
    resolved_len = strlcat (resolved, next_token, PATH_MAX);
    if (resolved_len >= PATH_MAX)
    {
      if (m) free (resolved);
      errno = ENAMETOOLONG;
      return (NULL);
    }
    if (mylstat (resolved, &sb, pid) != 0)
    {
      if (errno == ENOENT && p == NULL)
      {
        errno = serrno;
        return (resolved);
      }
      if (m) free (resolved);
      return (NULL);
    }
    if (S_ISLNK(sb.st_mode))
    {
      if (symlinks++ > MAXSYMLINKS)
      {
        if (m) free (resolved);
        errno = ELOOP;
        return (NULL);
      }
      slen = readlink (resolved, symlink, sizeof(symlink) - 1);
      if (slen < 0)
      {
        if (m) free (resolved);
        return (NULL);
      }
      symlink[slen] = '\0';
      if (symlink[0] == '/')
      {
        resolved[1] = 0;
        resolved_len = 1;
      }
      else if (resolved_len > 1)
      {
        /* Strip the last path component. */
        resolved[resolved_len - 1] = '\0';
        q = strrchr (resolved, '/') + 1;
        *q = '\0';
        resolved_len = q - resolved;
      }

      /*
       * If there are any path components left, then
       * append them to symlink. The result is placed
       * in `left'.
       */
      if (p != NULL)
      {
        if (symlink[slen - 1] != '/')
        {
          if (slen + 1 >= (int) sizeof(symlink))
          {
            if (m) free (resolved);
            errno = ENAMETOOLONG;
            return (NULL);
          }
          symlink[slen] = '/';
          symlink[slen + 1] = 0;
        }
        left_len = strlcat (symlink, left, sizeof(left));
        if (left_len >= sizeof(left))
        {
          if (m) free (resolved);
          errno = ENAMETOOLONG;
          return (NULL);
        }
      }
      left_len = strlcpy (left, symlink, sizeof(left));
    }
  }

  /*
   * Remove trailing slash except when the resolved pathname
   * is a single "/".
   */
  if (resolved_len > 1 && resolved[resolved_len - 1] == '/') resolved[resolved_len - 1] = '\0';
  return (resolved);
}

