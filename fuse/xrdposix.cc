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
#include <iostream>
#include <libgen.h>
#include <pwd.h>
#include "xrdposix.hh"
#include "XrdCache/XrdFileCache.hh"
#include "XrdCache/FileAbstraction.hh"
#include "XrdPosix/XrdPosixXrootd.hh"
#include "XrdClient/XrdClientEnv.hh"
#include "XrdClient/XrdClient.hh"
#include "XrdClient/XrdClientAdmin.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTable.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdClient/XrdClientConst.hh"

#include "common/Logging.hh"

#include "common/Path.hh"
#include "common/RWMutex.hh"

#include <google/dense_hash_map>
#include <google/sparse_hash_map>
#include <google/sparsehash/densehashtable.h>

#ifndef __macos__
#define OSPAGESIZE 4096
#else
#define OSPAGESIZE 65536
#endif


XrdPosixXrootd posixsingleton;

static XrdFileCache* XFC;

static XrdOucHash<XrdOucString> *passwdstore;
static XrdOucHash<XrdOucString> *stringstore;

XrdSysMutex passwdstoremutex;
XrdSysMutex stringstoremutex;

unsigned long long sim_inode=1; // this is the highest used simulated inode number as it is used by eosfsd (which works only by path but xrdposix caches by inode!) - this variable is protected by the p2i write lock

char*
STRINGSTORE(const char* __charptr__) {
  XrdOucString* yourstring;
  if (!__charptr__ ) return (char*)"";

  if ((yourstring = stringstore->Find(__charptr__))) {
    return ((char*)yourstring->c_str());
  } else {
    XrdOucString* newstring = new XrdOucString(__charptr__);
    stringstoremutex.Lock();
    stringstore->Add(__charptr__,newstring);
    stringstoremutex.UnLock();
    return (char*)newstring->c_str();
  }
}


void xrd_sync_env();
void xrd_ro_env();
void xrd_rw_env();
void xrd_wo_env();

// ---------------------------------------------------------------
// Implementation Translations
// ---------------------------------------------------------------

// protecting the path/inode translation table
eos::common::RWMutex InodePathMutex;

// translate path name to inode
google::dense_hash_map<std::string, unsigned long long> Path2Inode;

// translate inode to path name
google::dense_hash_map<unsigned long long, std::string> Inode2Path;

// translate dir inode to inode list

void           xrd_lock_r_p2i()   { InodePathMutex.LockRead();}
void           xrd_unlock_r_p2i() { InodePathMutex.UnLockRead();}
void           xrd_lock_w_p2i()   { InodePathMutex.LockWrite();}
void           xrd_unlock_w_p2i() { InodePathMutex.UnLockWrite();}

const char* 
xrd_path(unsigned long   long inode)
{
  // translate from inode to path - use xrd_lock_r_p2i/xrd_unlock_r_p2i for thread safety in the scope of the returned string
  if (Inode2Path.count(inode)) 
    return Inode2Path[inode].c_str();
  else 
    return 0;
};

char*
xrd_basename(unsigned long long inode) 
{
  eos::common::RWMutexReadLock vLock(InodePathMutex);
  const char* fname = xrd_path(inode);
  if (fname) {
    std::string spath = fname;
    size_t len = spath.length();
    if (len) {
      if (spath[len-1] == '/') {
	spath.erase(len-1);
      }
    }
    size_t spos = spath.rfind("/");

    if (spos != std::string::npos) {
      spath.erase(0, spos+1);
    }
    return (char*)STRINGSTORE(spath.c_str());
  }
  return 0;
}

unsigned long long 
xrd_inode(const char* path) 
{
  // translate from path to inode - use xrd_lock_r_p2i/xrd_unlock_r_p2i for thread safety in the scope of the returned inode
  if (Path2Inode.count(path)) 
    return Path2Inode[path];
  else
    return 0;
}

void 
xrd_store_p2i(unsigned long long inode, const char* path)
{
  // store an inode/path mapping
  xrd_lock_w_p2i();
  Path2Inode[path] = inode;
  Inode2Path[inode] = path;
  xrd_unlock_w_p2i();
}

unsigned long long
xrd_simulate_p2i(const char* path)
{
  // first try to find the existing mapping
  xrd_lock_r_p2i();
  unsigned long long newinode=xrd_inode(path);
  xrd_unlock_r_p2i();
  // store an inode/path mapping
  if (newinode) {
    return newinode;
  }

  // create a new virtual inode
  xrd_lock_w_p2i();
  if (!newinode) {
    newinode = ++sim_inode;
  }
  Path2Inode[path] = newinode;
  Inode2Path[newinode] = path;
  xrd_unlock_w_p2i();
  return newinode;
}

void 
xrd_store_child_p2i(unsigned long long inode, unsigned long long childinode, const char* name)
{
  // store an inode/path mapping
  xrd_lock_w_p2i();
  std::string fullpath = Inode2Path[inode];
  std::string sname=name;

  if ( (sname != ".") ) {
    // we don't need to store this ones
    if (sname == "..") {
      if (inode == 1) {
	fullpath = "/";
      } else {
	size_t spos = fullpath.rfind("/");
	if (spos != std::string::npos) {
	  fullpath.erase(spos);
	}
      }
    } else {
      fullpath += "/"; fullpath += name;
    }
    fprintf(stderr,"sname=%s fullpath=%s inode=%llu childinode=%llu\n", sname.c_str(),fullpath.c_str(), inode, childinode);
    Path2Inode[fullpath] = childinode;
    Inode2Path[childinode] = fullpath;
  }
  
  xrd_unlock_w_p2i();
}

void       
xrd_forget_p2i(unsigned long long inode)
{
  // forget an inode/path mapping by inode
  xrd_lock_w_p2i();
  if (Inode2Path.count(inode)) {
    std::string path = Inode2Path[inode];
    Path2Inode.erase(path);
    Inode2Path.erase(inode);
  }
  xrd_unlock_w_p2i();
}

void
xrd_forget_p2i(const char* path)
{
  // forget an inode/path mapping by path
  xrd_lock_w_p2i();
  if (Path2Inode.count(path)) {
    unsigned long long inode = Path2Inode[path];
    Path2Inode.erase(path);
    Inode2Path.erase(inode);
  }
  xrd_unlock_w_p2i();
}

// ---------------------------------------------------------------
// Implementation of the directory listing table
// ---------------------------------------------------------------

// protecting the directory listing table
eos::common::RWMutex DirInodeListMutex;

// dir listing map
google::dense_hash_map<unsigned long long, std::vector<unsigned long long> >DirInodeList;
google::dense_hash_map<unsigned long long, struct dirbuf> DirInodeBuffer;

void xrd_lock_r_dirview()   {DirInodeListMutex.LockRead();}
void xrd_unlock_r_dirview() {DirInodeListMutex.UnLockRead();}
void xrd_lock_w_dirview()   {DirInodeListMutex.LockWrite();}
void xrd_unlock_w_dirview() {DirInodeListMutex.UnLockWrite();}

void           
xrd_dirview_create(unsigned long long inode)
{
  // path should be attached beforehand into path translation
  eos::common::RWMutexWriteLock vLock(DirInodeListMutex);
  DirInodeList[inode].clear();
  DirInodeBuffer[inode].p    = 0; 
  DirInodeBuffer[inode].size = 0;
}

void
xrd_dirview_delete(unsigned long long inode)
{
  eos::common::RWMutexWriteLock vLock(DirInodeListMutex);
  if (DirInodeList.count(inode)) {
    if (DirInodeBuffer[inode].p) {
      free(DirInodeBuffer[inode].p);
    }
    DirInodeBuffer.erase(inode);
    DirInodeList[inode].clear();
    DirInodeList.erase(inode);
  }

}

unsigned long long            
xrd_dirview_entry(unsigned long long dirinode, size_t index)
{
  // returns entry with index 'index', should have xrd_lock_dirview in the scope of the call
  if (DirInodeList.count(dirinode) && (DirInodeList[dirinode].size() > index)) 
    return DirInodeList[dirinode][index];
  else
    return 0;
}

struct dirbuf* xrd_dirview_getbuffer(unsigned long long inode)
{
  // returns pointer to dirbuf , should have xrd_lock_dirview in the scope of the call
  return &DirInodeBuffer[inode];
}

// ---------------------------------------------------------------
// Implementation of the FUSE cache entry map
// ---------------------------------------------------------------

// protecting the cache entry map
eos::common::RWMutex FuseCacheMutex;

class FuseCacheEntry {
public:
  FuseCacheEntry() {
  }

  ~FuseCacheEntry() {};

  size_t getSize() { eos::common::RWMutexReadLock vLock(Mutex); return children.size(); }

  void Update() { 
    // update the contents
    eos::common::RWMutexReadLock vLock(Mutex);
  }

private:
  eos::common::RWMutex Mutex;

  std::map<unsigned long long,struct fuse_entry_param> children;
  struct timespec mtime;

};

// inode cache
google::dense_hash_map<unsigned long long, FuseCacheEntry> FuseCache;


int
xrd_dir_cache_get(unsigned long long inode, struct timespec mtime, char *fullpath, struct dirbuf **b)
{
  // create a cached directory
  return 0;
}

int
xrd_dir_cache_get_entry(fuse_req_t req, unsigned long long dir_inode, const char* ifullpath)
{
  // get a cached entry from a cached directory
  return 0;
}
  
void
xrd_dir_cache_add_entry(unsigned long long dir_inode, unsigned long long entry_inode, const char *entry_name, struct fuse_entry_param *e)
{
  // add a new entry to a cached directory
  return ;
}

void 
xrd_dir_cache_sync_entry(unsigned long long dir_inode, char *name, int nentries, struct timespec mtime, struct dirbuf *b)
{
  // update the cache entry inside a cached directory
  return ;
}


// ---------------------------------------------------------------
// Implementation the open File Descriptor map
// ---------------------------------------------------------------

// protecting the open filedescriptor map
XrdSysMutex OpenPosixXrootFdLock;

// open xrootd fd table
class PosixFd {
public:
  PosixFd() {
    fd = 0;
    nuser = 0;
  }
  ~PosixFd() {
  }

  void   setFd(int FD) { fd = FD;Inc();   }
  int    getFd()       { Inc(); return fd;}
  size_t getUser()     { return nuser;    }

  void Inc() { nuser++;}
  void Dec() { if(nuser) nuser--;}

  static std::string Index(unsigned long long inode, uid_t uid) {
    char index[256];
    snprintf(index, sizeof(index)-1,"%llu-%u", inode,uid);
    return index;
  }

private:
  int fd;       // POSIX fd to store
  size_t nuser; // number of users attached to this fd
};

google::dense_hash_map<std::string, PosixFd> OpenPosixXrootdFd; 

void
xrd_add_open_fd(int fd, unsigned long long inode, uid_t uid)
{
  // add fd as an open file descriptor to speed-up mknod
  XrdSysMutexHelper vLock(OpenPosixXrootFdLock);

  OpenPosixXrootdFd[PosixFd::Index(inode,uid)].setFd(fd);
}

int
xrd_get_open_fd(unsigned long long inode, uid_t uid)
{
  // return posix fd for inode - increases 'nuser'
  XrdSysMutexHelper vLock(OpenPosixXrootFdLock);

  return OpenPosixXrootdFd[PosixFd::Index(inode,uid)].getFd();
}

void
xrd_lease_open_fd(unsigned long long inode, uid_t uid)
{
  // release an attached file descriptor
  XrdSysMutexHelper vLock(OpenPosixXrootFdLock);
  OpenPosixXrootdFd[PosixFd::Index(inode,uid)].Dec();
  if(!OpenPosixXrootdFd[PosixFd::Index(inode,uid)].getUser()) {
    OpenPosixXrootdFd.erase(PosixFd::Index(inode,uid));
  }
}

// ---------------------------------------------------------------
// Implementation IO Buffer Management
// ---------------------------------------------------------------

// protecting the IO buffer map
XrdSysMutex IoBufferLock;

class IoBuf {
 private:
  void* buffer;
  size_t size;

 public:
  
  IoBuf() {
    buffer = 0;
    size = 0;
  }

  virtual ~IoBuf() { 
    if (buffer && size) free(buffer);
  }
  char* getBuffer() {return (char*)buffer;}
  size_t getSize() {return size;}
  void resize(size_t newsize) {
    if (newsize > size) {
      size = (newsize<(128*1024))?128*1024:newsize;
      buffer = realloc(buffer,size);
    }
  }
};

// IO buffer table
std::map<int, IoBuf> IoBufferMap;

char*    
xrd_attach_read_buffer(int fd, size_t  size)
{
  // guarantee a buffer for reading of at least 'size' for the specified fd
  XrdSysMutexHelper vlock(IoBufferLock);

  IoBufferMap[fd].resize(size);
  return (char*)IoBufferMap[fd].getBuffer();
}
  
void 
xrd_release_read_buffer(int fd)
{
  // release a read buffer for the specified fd
  XrdSysMutexHelper vlock(IoBufferLock);
  IoBufferMap.erase(fd);
  return;
}



//------------------------------------------------------------------------------
void
xrd_socks4(const char* host, const char* port)
{
  EnvPutString( NAME_SOCKS4HOST, host);
  EnvPutString( NAME_SOCKS4PORT, port);
  XrdPosixXrootd::setEnv(NAME_SOCKS4HOST,host);
  XrdPosixXrootd::setEnv(NAME_SOCKS4PORT,port);
}


//------------------------------------------------------------------------------
void
xrd_ro_env()
{
  eos_static_info("");

  int rahead = 0; 
  int rcsize = 0;

  if (getenv("EOS_FUSE_READAHEADSIZE")) {
    rahead = atoi(getenv("EOS_FUSE_READAHEADSIZE"));
  }
  if (getenv("EOS_FUSE_READCACHESIZE")) {
    rcsize = atoi(getenv("EOS_FUSE_READCACHESIZE"));
  }
  eos_static_info("ra=%d cs=%d", rahead, rcsize);
  XrdPosixXrootd::setEnv(NAME_READAHEADSIZE,rahead);
  XrdPosixXrootd::setEnv(NAME_READCACHESIZE,rcsize);
}


//------------------------------------------------------------------------------
void
xrd_wo_env()
{
  eos_static_info("");

  int rahead = 0;
  int rcsize = 0;

  XrdPosixXrootd::setEnv(NAME_READAHEADSIZE, rahead);
  XrdPosixXrootd::setEnv(NAME_READCACHESIZE, rcsize);  
}

//------------------------------------------------------------------------------
void
xrd_sync_env()
{
  eos_static_info("");
  XrdPosixXrootd::setEnv(NAME_READAHEADSIZE, (long)0);
  XrdPosixXrootd::setEnv(NAME_READCACHESIZE, (long)0);
}

//-------------------------------------------------------------------------------------------------------------------
void xrd_rw_env() {
  int rahead = 0;
  int rcsize = 0;

  XrdPosixXrootd::setEnv(NAME_READAHEADSIZE, rahead);
  XrdPosixXrootd::setEnv(NAME_READCACHESIZE, rcsize);
}


//------------------------------------------------------------------------------
int
xrd_rmxattr(const char *path, const char *xattr_name) 
{
  eos_static_info("path=%s xattr_name=%s", path, xattr_name);
  eos::common::Timing rmxattrtiming("rmxattr");
  TIMING("START", &rmxattrtiming);
  
  char response[4096]; 
  response[0] = 0;
  XrdOucString request;
  request = path;
  request += "?";
  request += "mgm.pcmd=xattr&";
  request += "mgm.subcmd=rm&";
  request += "mgm.xattrname=";
  request +=  xattr_name; 

  long long rmxattr = XrdPosixXrootd::QueryOpaque(request.c_str(), response, 4096);
  TIMING("GETPLUGIN", &rmxattrtiming);
  
  if (rmxattr >= 0) {
    //parse the output
    int retc = 0;
    int items =0;
    char tag[1024];

    items = sscanf(response, "%s retc=%i", tag, &retc);
    if ((items != 2) || (strcmp(tag,"rmxattr:"))) {
      errno = ENOENT;
      return EFAULT;
    }
    else 
      rmxattr = retc;
  }
  else 
    return EFAULT;

  TIMING("END", &rmxattrtiming);
  if (EOS_LOGS_DEBUG) {
    rmxattrtiming.Print();
  }

  return rmxattr;
}


//------------------------------------------------------------------------------
int
xrd_setxattr(const char *path, const char *xattr_name, const char *xattr_value, size_t size) 
{
  eos_static_info("path=%s xattr_name=%s xattr_value=%s", path, xattr_name, xattr_value);
  eos::common::Timing setxattrtiming("setxattr");
  TIMING("START", &setxattrtiming);
  
  char response[4096]; 
  response[0] = 0;
  XrdOucString request;
  request = path;
  request += "?";
  request += "mgm.pcmd=xattr&";
  request += "mgm.subcmd=set&";
  request += "mgm.xattrname=";
  request +=  xattr_name; request += "&";
  request += "mgm.xattrvalue=";
  request += xattr_value;

  long long setxattr = XrdPosixXrootd::QueryOpaque(request.c_str(), response, 4096);
  TIMING("GETPLUGIN", &setxattrtiming);
  
  if (setxattr >= 0) {
    //parse the output
    int retc = 0;
    int items =0;
    char tag[1024];

    items = sscanf(response, "%s retc=%i", tag, &retc);
    if ((items != 2) || (strcmp(tag,"setxattr:"))) {
      errno = ENOENT;
      return EFAULT;
    }
    else
      setxattr = retc;
  }
  else 
    return EFAULT;

  TIMING("END", &setxattrtiming);
  if (EOS_LOGS_DEBUG) {
    setxattrtiming.Print();
  }

  return setxattr;
}


//------------------------------------------------------------------------------
int
xrd_getxattr(const char *path, const char *xattr_name, char **xattr_value, size_t *size) 
{
  eos_static_info("path=%s xattr_name=%s",path,xattr_name);
  eos::common::Timing getxattrtiming("getxattr");
  TIMING("START", &getxattrtiming);
  
  char response[4096]; 
  response[0] = 0;
  XrdOucString request;
  request = path;
  request += "?";
  request += "mgm.pcmd=xattr&";
  request += "mgm.subcmd=get&";
  request += "mgm.xattrname=";
  request +=  xattr_name;

  long long getxattr = XrdPosixXrootd::QueryOpaque(request.c_str(), response, 4096);
  TIMING("GETPLUGIN", &getxattrtiming);
  
  if (getxattr >= 0) {
    //parse the output
    int retc = 0;
    int items =0;
    char tag[1024];
    char rval[4096];
    
    items = sscanf(response, "%s retc=%i value=%s", tag, &retc, rval);
    if ((items != 3) || (strcmp(tag,"getxattr:"))) {
      errno = ENOENT;
      return EFAULT;
    }
    else {
      getxattr = retc;
      if (strcmp(xattr_name, "user.eos.XS") == 0){
        char *ptr = rval;
        for (unsigned int i = 0; i< strlen(rval); i++, ptr++) {
          if (*ptr == '_')
            *ptr = ' ';
        }
      }
      *size = strlen(rval);
      *xattr_value = (char*) calloc((*size) + 1, sizeof(char));
      *xattr_value = strncpy(*xattr_value, rval, *size);
    } 
  }
  else 
    return EFAULT;

  TIMING("END", &getxattrtiming);

  if (EOS_LOGS_DEBUG) {
    getxattrtiming.Print();
  }

  return getxattr;
}


//------------------------------------------------------------------------------
int
xrd_listxattr(const char *path, char **xattr_list, size_t *size) 
{
  eos_static_info("path=%s",path);
  eos::common::Timing listxattrtiming("listxattr");
  TIMING("START", &listxattrtiming);
  
  char response[16384]; 
  response[0] = 0;
  XrdOucString request;
  request = path;
  request += "?";
  request += "mgm.pcmd=xattr&";
  request += "mgm.subcmd=ls";

  long long listxattr = XrdPosixXrootd::QueryOpaque(request.c_str(), response, 16384);
  TIMING("GETPLUGIN", &listxattrtiming);
  if (listxattr >= 0) {
    //parse the output
    int retc = 0;
    int items =0;
    char tag[1024];
    char rval[16384];
    
    items = sscanf(response, "%s retc=%i %s", tag, &retc, rval);
    if ((items != 3) || (strcmp(tag,"lsxattr:"))) {
      errno = ENOENT;
      return EFAULT;
    } else {
      listxattr = retc;
      *size = strlen(rval);
      char *ptr = rval;
      for (unsigned int i = 0; i < (*size); i++, ptr++){
        if (*ptr == '&')
          *ptr = '\0';
      }      
      *xattr_list = (char*) calloc((*size) + 1, sizeof(char));
      *xattr_list = (char*) memcpy(*xattr_list, rval, *size);
    }
  }
  else 
    return EFAULT;

  TIMING("END", &listxattrtiming);
  if (EOS_LOGS_DEBUG) {
    listxattrtiming.Print();
  }

  return listxattr;
}


//------------------------------------------------------------------------------
int
xrd_stat(const char *path, struct stat *buf)
{
  eos_static_info("path=%s", path);
  eos::common::Timing stattiming("xrd_stat");
  TIMING("START",&stattiming);

  char value[4096]; value[0] = 0;;
  XrdOucString request;
  request = path;
  request += "?";
  request += "mgm.pcmd=stat";
  //  request.replace("@","#admin@");

  long long dostat = XrdPosixXrootd::QueryOpaque(request.c_str(), value, 4096);
  TIMING("GETPLUGIN",&stattiming);
  //  fprintf(stderr,"returned %s %lld\n",value, dostat);
  if (dostat >= 0) {
    unsigned long long sval[10];
    unsigned long long ival[6];
    char tag[1024];
    
    // parse the stat output
    int items = sscanf(value,"%s %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",tag, (unsigned long long*)&sval[0],(unsigned long long*)&sval[1],(unsigned long long*)&sval[2],(unsigned long long*)&sval[3],(unsigned long long*)&sval[4],(unsigned long long*)&sval[5],(unsigned long long*)&sval[6],(unsigned long long*)&sval[7],(unsigned long long*)&sval[8],(unsigned long long*)&sval[9],(unsigned long long*)&ival[0],(unsigned long long*)&ival[1],(unsigned long long*)&ival[2], (unsigned long long*)&ival[3], (unsigned long long*)&ival[4], (unsigned long long*)&ival[5]);
    if ((items != 17) || (strcmp(tag,"stat:"))) {
      errno = ENOENT;
      return EFAULT;
    } else {
      buf->st_dev = (dev_t) sval[0];
      buf->st_ino = (ino_t) sval[1];
      buf->st_mode = (mode_t) sval[2];
      buf->st_nlink = (nlink_t) sval[3];
      buf->st_uid = (uid_t) sval[4];
      buf->st_gid = (gid_t) sval[5];
      buf->st_rdev = (dev_t) sval[6];
      buf->st_size = (off_t) sval[7];
      buf->st_blksize = (blksize_t) sval[8];
      buf->st_blocks  = (blkcnt_t) sval[9];
      buf->st_atime = (time_t) ival[0];
      buf->st_mtime = (time_t) ival[1];
      buf->st_ctime = (time_t) ival[2];
      buf->st_atim.tv_sec = (time_t) ival[0];
      buf->st_mtim.tv_sec = (time_t) ival[1];
      buf->st_ctim.tv_sec = (time_t) ival[2];
      buf->st_atim.tv_nsec = (time_t) ival[3];
      buf->st_mtim.tv_nsec = (time_t) ival[4];
      buf->st_ctim.tv_nsec = (time_t) ival[5];
      dostat = 0;
    } 
  }

  TIMING("END",&stattiming);
  if (EOS_LOGS_DEBUG) {
    stattiming.Print();
  }
  
  return dostat;
}


//------------------------------------------------------------------------------
int 
xrd_statfs(const char* url, const char* path, struct statvfs *stbuf) 
{
  eos_static_info("url=%s path=%s", url, path);
  static unsigned long long a1=0;
  static unsigned long long a2=0;
  static unsigned long long a3=0;
  static unsigned long long a4=0;
    
  static XrdSysMutex statmutex;
  static time_t laststat=0;
  statmutex.Lock();
  
  if ( (time(NULL) - laststat) < ( (15 + (int)5.0*rand()/RAND_MAX)) ) {
    stbuf->f_bsize  = 4096;
    stbuf->f_frsize = 4096;
    stbuf->f_blocks = a3 /4096;
    stbuf->f_bfree  = a1 /4096;
    stbuf->f_bavail = a1 /4096;
    stbuf->f_files  = a4;
    stbuf->f_ffree  = a2;
    stbuf->f_fsid     = 0xcafe;
    stbuf->f_namemax  = 256;
    statmutex.UnLock();
    return 0;
  }

  eos::common::Timing statfstiming("xrd_statfs");
  TIMING("START",&statfstiming);

  char value[4096]; value[0] = 0;;
  XrdOucString request;
  request = url;
  request += "?";
  request += "mgm.pcmd=statvfs&";
  request += "path=";
  request += path;

  //  fprintf(stderr,"Query %s\n", request.c_str());
  long long dostatfs = XrdPosixXrootd::QueryOpaque(request.c_str(), value, 4096);
  
  TIMING("END",&statfstiming);
  if (EOS_LOGS_DEBUG) {
    statfstiming.Print();
  }
  
  if (dostatfs>=0) {
    char tag[1024];
    int retc=0;

    if (!value[0]) {
      statmutex.UnLock();
      return -EFAULT;
    }
    // parse the stat output
    int items = sscanf(value,"%s retc=%d f_avail_bytes=%llu f_avail_files=%llu f_max_bytes=%llu f_max_files=%llu",tag, &retc, &a1, &a2, &a3, &a4);
    if ((items != 6) || (strcmp(tag,"statvfs:"))) {
      statmutex.UnLock();
      return -EFAULT;
    }

    laststat = time(NULL);

    statmutex.UnLock();
    stbuf->f_bsize  = 4096;
    stbuf->f_frsize = 4096;
    stbuf->f_blocks = a3 /4096;
    stbuf->f_bfree  = a1 /4096;
    stbuf->f_bavail = a1 /4096;
    stbuf->f_files  = a4;
    stbuf->f_ffree  = a2;

    return retc;
  } else {
    statmutex.UnLock();
    return -EFAULT;
  }
}


//------------------------------------------------------------------------------  
int 
xrd_chmod(const char* path, mode_t mode) 
{
  eos_static_info("path=%s mode=%x", path, mode);
  eos::common::Timing chmodtiming("xrd_chmod");
  TIMING("START",&chmodtiming);

  char value[4096]; value[0] = 0;;
  XrdOucString request;
  request = path;
  request += "?";
  request += "mgm.pcmd=chmod&mode=";
  request += (int)mode;
  //  request.replace("@","#admin@");
  long long dochmod = XrdPosixXrootd::QueryOpaque(request.c_str(), value, 4096);

  TIMING("END",&chmodtiming);
  if (EOS_LOGS_DEBUG) {
    chmodtiming.Print();
  }

  if (dochmod>=0) {
    char tag[1024];
    int retc=0;
    if (!value[0]) {
      return -EFAULT;
    }
    // parse the stat output
    int items = sscanf(value,"%s retc=%d",tag, &retc);
    if ((items != 2) || (strcmp(tag,"chmod:"))) {
      
      return -EFAULT;
    }
    
    return retc;
  } else {
    return -EFAULT;
  }
}

//------------------------------------------------------------------------------
int 
xrd_symlink(const char* url, const char* destpath, const char* sourcepath) 
{
  eos_static_info("url=%s destpath=%s,sourcepath=%s", url, destpath, sourcepath);
  eos::common::Timing symlinktiming("xrd_symlink");
  TIMING("START",&symlinktiming);
  
  char value[4096]; value[0] = 0;;
  XrdOucString request;
  request = url;
  request += "?";
  request += "mgm.pcmd=symlink&linkdest=";
  request += destpath;
  request += "&linksource=";
  request += sourcepath;
  long long dosymlink = XrdPosixXrootd::QueryOpaque(request.c_str(), value, 4096);

  TIMING("END",&symlinktiming);
  if (EOS_LOGS_DEBUG) {
    symlinktiming.Print();
  }
  
  if (dosymlink>=0) {
    char tag[1024];
    int retc;
    // parse the stat output
    int items = sscanf(value,"%s retc=%d",tag, &retc);
    if ((items != 2) || (strcmp(tag,"symlink:"))) {
      
      return -EFAULT;
    }
    
    return retc;
  } else {
    
    return -EFAULT;
  }

}


//------------------------------------------------------------------------------
int 
xrd_link(const char* url, const char* destpath, const char* sourcepath) 
{
  eos_static_info("url=%s destpath=%s sourcepath=%s", url, destpath, sourcepath);
  eos::common::Timing linktiming("xrd_link");
  TIMING("START",&linktiming);
  
  char value[4096]; value[0] = 0;;
  XrdOucString request;
  request = url;
  request += "?";
  request += "mgm.pcmd=link&linkdest=";
  request += destpath;
  request += "&linksource=";
  request += sourcepath;
  long long dolink = XrdPosixXrootd::QueryOpaque(request.c_str(), value, 4096);

  TIMING("END",&linktiming);
  if (EOS_LOGS_DEBUG) {
    linktiming.Print();
  }

  if (dolink>=0) {
    char tag[1024];
    int retc;
    // parse the stat output
    int items = sscanf(value,"%s retc=%d",tag, &retc);
    if ((items != 2) || (strcmp(tag,"link:"))) {
      
      return -EFAULT;
    }
    
    return retc;
  } else {
    
    return -EFAULT;
  }

}


//------------------------------------------------------------------------------
int 
xrd_readlink(const char* path, char* buf, size_t bufsize)
{
  eos_static_info("path=%s", path);
  eos::common::Timing readlinktiming("xrd_readlink");
  TIMING("START",&readlinktiming);
  
  char value[4096]; value[0] = 0;;
  XrdOucString request;
  request = path;
  request += "?";
  request += "mgm.pcmd=readlink";
  long long doreadlink = XrdPosixXrootd::QueryOpaque(request.c_str(), value, 4096);

  TIMING("END",&readlinktiming);
  if (EOS_LOGS_DEBUG) {
    readlinktiming.Print();
  }

  if (doreadlink>=0) {
    char tag[1024];
    char link[4096];
    link[0] = 0;
    //    printf("Readlink gave %s\n",value);
    int retc;
    // parse the stat output
    int items = sscanf(value,"%s retc=%d link=%s",tag, &retc, link);
    if ((items != 3) || (strcmp(tag,"readlink:"))) {
      
      return -EFAULT;
    }

    strncpy(buf,link,(bufsize<OSPAGESIZE)?bufsize:(OSPAGESIZE-1));
    
    return retc;
  } else {
    
    return -EFAULT;
  }
}


//------------------------------------------------------------------------------
int 
xrd_utimes(const char* path, struct timespec *tvp)
{
  eos_static_info("path=%s", path);
  eos::common::Timing utimestiming("xrd_utimes");
  TIMING("START",&utimestiming);

  char value[4096]; value[0] = 0;;
  XrdOucString request;
  request = path;
  request += "?";
  request += "mgm.pcmd=utimes&tv1_sec=";
  char lltime[1024];
  sprintf(lltime,"%llu",(unsigned long long)tvp[0].tv_sec);
  request += lltime;
  request += "&tv1_nsec=";
  sprintf(lltime,"%llu",(unsigned long long)tvp[0].tv_nsec);
  request += lltime;
  request += "&tv2_sec=";
  sprintf(lltime,"%llu",(unsigned long long)tvp[1].tv_sec);
  request += lltime;
  request += "&tv2_nsec=";
  sprintf(lltime,"%llu",(unsigned long long)tvp[1].tv_nsec);
  request += lltime;

  long long doutimes = XrdPosixXrootd::QueryOpaque(request.c_str(), value, 4096);

  TIMING("END",&utimestiming);
  if (EOS_LOGS_DEBUG) {
    utimestiming.Print();
  }

  if (doutimes>=0) {
    char tag[1024];
    int retc;
    // parse the stat output
    int items = sscanf(value,"%s retc=%d",tag, &retc);
    if ((items != 2) || (strcmp(tag,"utimes:"))) {
      errno = EFAULT;
      return -EFAULT;
    }
    
    return retc;
  } else {
    errno = EFAULT;
    return -EFAULT;
  }  
}


//------------------------------------------------------------------------------
int            
xrd_access(const char* path, int mode)
{
  eos_static_info("path=%s mode=%d", path, mode);
  eos::common::Timing accesstiming("xrd_access");
  TIMING("START",&accesstiming);

  char value[4096]; value[0] = 0;;
  XrdOucString request;
  request = path;
  if (getenv("EOS_FUSE_NOACCESS") && (!strcmp(getenv("EOS_FUSE_NOACCESS"),"1"))) {
    return 0;
  }

  request += "?";
  request += "mgm.pcmd=access&mode=";
  request += (int)mode;
  long long doaccess = XrdPosixXrootd::QueryOpaque(request.c_str(), value, 4096);

  TIMING("STOP",&accesstiming);
  if (EOS_LOGS_DEBUG) {
    accesstiming.Print();
  }

  if (doaccess>=0) {
    char tag[1024];
    int retc;
    // parse the stat output
    int items = sscanf(value,"%s retc=%d",tag, &retc);
    if ((items != 2) || (strcmp(tag,"access:"))) {
      errno = EFAULT;
      return -EFAULT;
    }
    fprintf(stderr,"retc=%d\n", retc);
    errno = retc;
    return retc;
  } else {
    errno = EFAULT;
    return -EFAULT;
  }
}


//------------------------------------------------------------------------------
int
xrd_inodirlist(unsigned long long dirinode, const char *path)
{
  eos_static_info("inode=%llu path=%s",dirinode, path);
  eos::common::Timing inodirtiming("xrd_inodirlist");
  TIMING("START",&inodirtiming);

  char* value=0;
  char* ptr=0;
  XrdOucString request;
  request = path;

  TIMING("GETSTSTREAM",&inodirtiming);

  int doinodirlist=-1;
  int retc;
  xrd_sync_env();
  XrdClient* listclient = new XrdClient(request.c_str());
  
  if (!listclient) {
    return EFAULT;
  }

  if (!listclient->Open(0,0,true)) {
    delete listclient;
    return ENOENT;
  }

  // start to read
  value = (char*) malloc(PAGESIZE+1);
  int nbytes = 0;
  int npages=1;
  off_t offset=0;
  TIMING("READSTSTREAM",&inodirtiming);
  while ( (nbytes = listclient->Read(value+offset ,offset,PAGESIZE)) == PAGESIZE) {
    npages++;
    value = (char*) realloc(value,npages*PAGESIZE+1);
    offset += PAGESIZE;
  }
  if (nbytes>=0) offset+= nbytes;
  value[offset] = 0;
  
  delete listclient;
  
  char dirtag[1024];
  sprintf(dirtag,"%llu",dirinode);
  
  xrd_dirview_create( (unsigned long long ) dirinode);

  TIMING("PARSESTSTREAM",&inodirtiming);    

  xrd_lock_w_dirview(); // =>

  if (nbytes>= 0) {
    char dirpath[4096];
    unsigned long long inode;
    char tag[128];
    
    retc = 0;
    
    // parse the stat output
    int items = sscanf(value,"%s retc=%d",tag, &retc);
    if ((items != 2) || (strcmp(tag,"inodirlist:"))) {
      free(value);
      xrd_unlock_w_dirview(); // <=
      xrd_dirview_delete( (unsigned long long ) dirinode);
      return EFAULT;
    }
    ptr = strchr(value,' ');
    if (ptr) ptr = strchr(ptr+1,' ');
    char* endptr = value + strlen(value) -1 ;
    
    while ((ptr) &&(ptr < endptr)) {
      int items = sscanf(ptr,"%s %llu",dirpath,&inode);
      if (items != 2) {
        free(value);
	xrd_unlock_w_dirview(); // <=
	xrd_dirview_delete( (unsigned long long ) dirinode);
        return EFAULT;
      }
      XrdOucString whitespacedirpath = dirpath;
      whitespacedirpath.replace("%20"," ");

      xrd_store_child_p2i(dirinode, inode, whitespacedirpath.c_str());

      DirInodeList[dirinode].push_back(inode);
      // to the next entries
      if (ptr) ptr = strchr(ptr+1,' ');
      if (ptr) ptr = strchr(ptr+1,' ');
      eos_static_info("name=%s inode=%llu",whitespacedirpath.c_str(), inode);
    } 
    doinodirlist = 0;
  }

  xrd_unlock_w_dirview(); // <=
  
  TIMING("END",&inodirtiming);
  if (EOS_LOGS_DEBUG) {
    inodirtiming.Print();
  }
  
  free(value);
  return doinodirlist;
}


//------------------------------------------------------------------------------  
DIR *xrd_opendir(const char *path)

{
  eos_static_info("path=%s",path);
  return XrdPosixXrootd::Opendir(path);
}


//------------------------------------------------------------------------------  
struct dirent *xrd_readdir(DIR *dirp)
{
  eos_static_info("dirp=%llx",(long long) dirp);
  return XrdPosixXrootd::Readdir(dirp);
}


//------------------------------------------------------------------------------  
int xrd_closedir(DIR *dirp)
{
  eos_static_info("dirp=%llx",(long long) dirp);
  return XrdPosixXrootd::Closedir(dirp);
}


//------------------------------------------------------------------------------  
int xrd_mkdir(const char *path, mode_t mode)
{
  eos_static_info("path=%s mode=%d", path, mode);
  return XrdPosixXrootd::Mkdir(path, mode);
}


//------------------------------------------------------------------------------  
int xrd_rmdir(const char *path)
{
  eos_static_info("path=%s", path);
  return XrdPosixXrootd::Rmdir(path);
}


//------------------------------------------------------------------------------
int
xrd_open(const char *path, int oflags, mode_t mode)
{
  eos_static_info("path=%s flags=%d mode=%d", path, oflags, mode);
  XrdOucString spath=path;
  int t0;
  if ((t0=spath.find("/proc/"))!=STR_NPOS) {
    // clean the path
    int t1 = spath.find("//");
    int t2 = spath.find("//", t1+2);
    spath.erase(t2+2,t0-t2-2);
    while (spath.replace("///","//")){};
    // force a reauthentication to the head node
    if (spath.endswith("/proc/reconnect")) {
      XrdClientAdmin* client = new XrdClientAdmin(path);
      if (client) {
        if (client->Connect()) {
          client->GetClientConn()->Disconnect(true);
          errno = ENETRESET;
          return -1;
        }
        delete client;
      }
      errno = ECONNABORTED;
      return -1;
    }
    // return the 'whoami' information in that file
    if (spath.endswith("/proc/whoami")) {
      spath.replace("/proc/whoami","/proc/user/");
      spath += "?mgm.cmd=whoami&mgm.format=fuse";
      //      OpenMutex.Lock();
      xrd_sync_env();
      int retc = XrdPosixXrootd::Open(spath.c_str(), oflags, mode);
      //      OpenMutex.UnLock();
      return retc;
    }

    if (spath.endswith("/proc/who")) {
      spath.replace("/proc/who","/proc/user/");
      spath += "?mgm.cmd=who&mgm.format=fuse";
      //      OpenMutex.Lock();
      xrd_sync_env();
      int retc = XrdPosixXrootd::Open(spath.c_str(), oflags, mode);
      //      OpenMutex.UnLock();
      return retc;
    }

    if (spath.endswith("/proc/quota")) {
      spath.replace("/proc/quota","/proc/user/");
      spath += "?mgm.cmd=quota&mgm.subcmd=ls&mgm.format=fuse";
      //      OpenMutex.Lock();
      xrd_sync_env();
      int retc = XrdPosixXrootd::Open(spath.c_str(), oflags, mode);
      //      OpenMutex.UnLock();
      return retc;
    }
  }

  //  OpenMutex.Lock();
  if (oflags & O_WRONLY) {
    xrd_wo_env();
  } else if (oflags & O_RDWR) {
    xrd_rw_env();
  } else {
    xrd_ro_env();
  }
  int retc = XrdPosixXrootd::Open(path, oflags, mode);
  //  OpenMutex.UnLock();
  return retc;
}


//------------------------------------------------------------------------------
int
xrd_close(int fildes, unsigned long inode)
{
  eos_static_info("fd=%d inode=%lu", fildes, inode);
  if (XFC && inode) {
    XFC->waitFinishWrites(inode);
  }

  return XrdPosixXrootd::Close(fildes);
}


//------------------------------------------------------------------------------
int
xrd_truncate(int fildes, off_t offset, unsigned long inode)
{
  eos_static_info("fd=%d offset=%llu inode=%lu", fildes, (unsigned long long)offset, inode);
  if (XFC && inode) {
    XFC->waitFinishWrites(inode);
  }
  
  return XrdPosixXrootd::Ftruncate(fildes,offset);
}


//------------------------------------------------------------------------------
off_t
xrd_lseek(int fildes, off_t offset, int whence, unsigned long inode)
{
  eos_static_info("fd=%d offset=%llu whence=%d indoe=%lu", fildes, (unsigned long long)offset, whence, inode);    
  if (XFC && inode) {
    XFC->waitFinishWrites(inode);
  }
  return XrdPosixXrootd::Lseek(fildes, (long long)offset, whence);
}


//------------------------------------------------------------------------------
ssize_t
xrd_read(int fildes, void *buf, size_t nbyte, unsigned long inode)
{
  eos_static_info("fd=%d nbytes=%lu inode=%lu", fildes, (unsigned long)nbyte, (unsigned long)inode);
  size_t ret;
  FileAbstraction* fAbst =0;

  if (XFC && fuse_cache_read && inode) {
    fAbst = XFC->getFileObj(inode, true);
    XFC->waitFinishWrites(*fAbst);
    off_t offset = XrdPosixXrootd::Lseek(fildes, 0, SEEK_SET);

    if ((ret = XFC->getRead(*fAbst, buf, offset, nbyte)) != nbyte) {
      ret = XrdPosixXrootd::Read(fildes, buf, nbyte);
      XFC->putRead(*fAbst, fildes, buf, offset, nbyte);
    }
    fAbst->decrementNoReferences();
  } else {
    ret = XrdPosixXrootd::Read(fildes, buf, nbyte);
  }
   
  return ret;
}


//------------------------------------------------------------------------------
ssize_t
xrd_pread(int fildes, void *buf, size_t nbyte, off_t offset, unsigned long inode)
{
  eos::common::Timing xpr("xrd_pread");
  TIMING("start", &xpr);
  
  eos_static_debug("fd=%d nbytes=%lu offset=%llu inode=%lu",fildes, (unsigned long)nbyte, (unsigned long long)offset, (unsigned long) inode);
 
  size_t ret;

  if (XFC && fuse_cache_read && inode) {
    FileAbstraction* fAbst = 0;
    fAbst = XFC->getFileObj(inode, true);
    XFC->waitFinishWrites(*fAbst);
    TIMING("wait writes", &xpr);
    if ((ret = XFC->getRead(*fAbst, buf, offset, nbyte)) != nbyte) {
      TIMING("read in", &xpr);
      eos_static_debug("Block not found in cache: off=%zu, len=%zu", offset, nbyte);
      ret = XrdPosixXrootd::Pread(fildes, buf, nbyte, static_cast<long long>(offset));
      TIMING("read out", &xpr);
      XFC->putRead(*fAbst, fildes, buf, offset, nbyte);
      TIMING("put read", &xpr);
    }
    else {
      eos_static_debug("Block found in cache: off=%zu, len=%zu", offset, nbyte);
      TIMING("block in cache", &xpr);
    }
    fAbst->decrementNoReferences();
  } else {
    ret = XrdPosixXrootd::Pread(fildes, buf, nbyte, static_cast<long long>(offset));
  }

  TIMING("end", &xpr);
  if (EOS_LOGS_DEBUG ) {
    xpr.Print();
  }

  return ret;
}


//------------------------------------------------------------------------------
ssize_t
xrd_write(int fildes, const void *buf, size_t nbyte, unsigned long inode)
{
  eos_static_info("fd=%d nbytes=%lu inode=%lu", fildes, (unsigned long)nbyte, (unsigned long) inode);
  size_t ret;

  if (XFC && fuse_cache_write && inode) {
    off_t offset = XrdPosixXrootd::Lseek(fildes, 0, SEEK_SET);
    XFC->submitWrite(inode, fildes, const_cast<void*>(buf), offset, nbyte);
    ret = nbyte;
  } else {
    ret = XrdPosixXrootd::Write(fildes, buf, nbyte);
  }
  return ret;                  
}


//------------------------------------------------------------------------------
ssize_t
xrd_pwrite(int fildes, const void *buf, size_t nbyte, off_t offset, unsigned long inode)
{
  eos::common::Timing xpw("xrd_pwrite");
  TIMING("start", &xpw);
  
  eos_static_debug("fd=%d nbytes=%lu inode=%lu cache=%d cache-w=%d", fildes, (unsigned long)nbyte, (unsigned long) inode, XFC?1:0, fuse_cache_write);
  size_t ret;

  if (XFC && fuse_cache_write && inode) {
    XFC->submitWrite(inode, fildes, const_cast<void*>(buf), offset, nbyte);
    ret = nbyte;
  } else {
    ret = XrdPosixXrootd::Pwrite(fildes, buf, nbyte, static_cast<long long>(offset));    
  }

  TIMING("end", &xpw);
  if (EOS_LOGS_DEBUG) {
    xpw.Print();
  }

  return ret;                  
}


//------------------------------------------------------------------------------
int
xrd_fsync(int fildes, unsigned long inode)
{
  eos_static_info("fd=%d inode=%lu", fildes, (unsigned long)inode);
  if (XFC && inode) {
    XFC->waitFinishWrites(inode);
  }
  
  return XrdPosixXrootd::Fsync(fildes);
}


//------------------------------------------------------------------------------
int xrd_unlink(const char *path)
{
  eos_static_info("path=%s",path);
  return XrdPosixXrootd::Unlink(path);
}


//------------------------------------------------------------------------------
int xrd_rename(const char *oldpath, const char *newpath)
{
  eos_static_info("oldpath=%s newpath=%s", oldpath, newpath);
  return XrdPosixXrootd::Rename(oldpath, newpath);
}

//------------------------------------------------------------------------------
const char*
xrd_mapuser(uid_t uid)
{
  eos_static_debug("uid=%lu", (unsigned long) uid);
  struct passwd* pw;
  XrdOucString sid = "";
  XrdOucString* spw=NULL;
  sid += (int) (uid);
  passwdstoremutex.Lock();
  if (!(spw = passwdstore->Find(sid.c_str()))) {
    pw = getpwuid(uid);
    if (pw) {
      spw = new XrdOucString(pw->pw_name);
      passwdstore->Add(sid.c_str(),spw,60); 
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
  userproxy  += (int) uid;
  krb5ccname += (int) uid;
  setenv("X509_USER_PROXY",  userproxy.c_str(),1);
  setenv("KRB5CCNAME", krb5ccname.c_str(),1);
  // ----------------------------------------------------------------------------------

  return STRINGSTORE(spw->c_str());
}

//------------------------------------------------------------------------------
const char* xrd_get_dir(DIR* dp, int entry) { return 0;}

void
xrd_init()
{
  FILE* fstderr ;
  // open a log file
  if (getuid()) {
    char logfile[1024];
    snprintf(logfile,sizeof(logfile)-1, "/tmp/eos-fuse.%d.log", getuid());
    // running as a user ... we log into /tmp/eos-fuse.$UID.log

    if (!(fstderr = freopen(logfile,"a+", stderr))) {
      fprintf(stderr,"error: cannot open log file %s\n", logfile);
    }
  } else {
    // running as root ... we log into /var/log/eos/fuse
    eos::common::Path cPath("/var/log/eos/fuse/fuse.log");
    cPath.MakeParentPath(S_IRWXU | S_IRGRP | S_IROTH);
    if (!(fstderr = freopen(cPath.GetPath(),"a+", stderr))) {
      fprintf(stderr,"error: cannot open log file %s\n", cPath.GetPath());
    }
  }

  setvbuf(fstderr, (char*) NULL, _IONBF, 0);

  // initialize hashes
  Path2Inode.set_empty_key("");
  Inode2Path.set_empty_key(0);
  DirInodeList.set_empty_key(0);
  DirInodeBuffer.set_empty_key(0);
  FuseCache.set_empty_key(0);
  OpenPosixXrootdFd.set_empty_key("");

  Path2Inode.set_deleted_key("#__deleted__#");
  Inode2Path.set_deleted_key(0xffffffffll);
  DirInodeList.set_deleted_key(0xffffffffll);
  DirInodeBuffer.set_deleted_key(0xffffffffll);
  FuseCache.set_deleted_key(0xffffffffll);
  OpenPosixXrootdFd.set_deleted_key("#__deleted__#");
  
  // create the root entry
  Path2Inode["/"] = 1;
  Inode2Path[1] = "/";

  eos::common::Mapping::VirtualIdentity_t vid;
  eos::common::Mapping::Root(vid);
  eos::common::Logging::Init();
  eos::common::Logging::SetUnit("FUSE@localhost");
  eos::common::Logging::gShortFormat=true;

  XrdOucString fusedebug = getenv("EOS_FUSE_DEBUG");
  if ((getenv("EOS_FUSE_DEBUG")) && (fusedebug != "0")) {
    eos::common::Logging::SetLogPriority(LOG_DEBUG);
  } else {
    //eos::common::Logging::SetLogPriority(LOG_DEBUG);
    eos::common::Logging::SetLogPriority(LOG_INFO);
  }
  
  XrdPosixXrootd::setEnv(NAME_DATASERVERCONN_TTL,300);
  XrdPosixXrootd::setEnv(NAME_LBSERVERCONN_TTL,3600*24);
  XrdPosixXrootd::setEnv(NAME_REQUESTTIMEOUT,30);
  EnvPutInt("NAME_MAXREDIRECTCOUNT",3);
  EnvPutInt("NAME_RECONNECTWAIT", 10);

  setenv("XRDPOSIX_POPEN","1",1);
  if ((fusedebug.find("posix")!=STR_NPOS)) {
    XrdPosixXrootd::setEnv(NAME_DEBUG,atoi(getenv("EOS_FUSE_DEBUG")));
  }
  
  fuse_cache_read = false;
  fuse_cache_write = false;

  //initialise the XrdFileCache
  if (!(getenv("EOS_FUSE_CACHE"))) {
    eos_static_notice("cache=false");
    XFC = NULL;
  } else {
    if (!getenv("EOS_FUSE_CACHE_SIZE")) {
      setenv("EOS_FUSE_CACHE_SIZE", "30000000", 1);   // ~300MB
    }
    eos_static_notice("cache=true size=%s cache-read=%s, cache-write=%s",getenv("EOS_FUSE_CACHE_SIZE"), getenv("EOS_FUSE_CACHE_READ"), getenv("EOS_FUSE_CACHE_WRITE"));
    XFC = XrdFileCache::getInstance(static_cast<size_t>(atol(getenv("EOS_FUSE_CACHE_SIZE"))));   
    if (getenv("EOS_FUSE_CACHE_READ") && atoi(getenv("EOS_FUSE_CACHE_READ"))) {
      fuse_cache_read = true;
    }
    if (getenv("EOS_FUSE_CACHE_WRITE") && atoi(getenv("EOS_FUSE_CACHE_WRITE"))) {
      fuse_cache_write = true;
    }
  }
  
  passwdstore = new XrdOucHash<XrdOucString> ();
  stringstore = new XrdOucHash<XrdOucString> ();
}
        

