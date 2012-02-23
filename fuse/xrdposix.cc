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
#ifndef __macos__
#define OSPAGESIZE 4096
#else
#define OSPAGESIZE 65536
#endif


XrdPosixXrootd posixsingleton;

static XrdOucHash<XrdOucString> *passwdstore;
static XrdOucHash<XrdOucString> *inodestore;
static XrdOucHash<XrdOucString> *stringstore;

XrdSysMutex passwdstoremutex;
XrdSysMutex inodestoremutex;
XrdSysMutex mknodopenstoremutex;
XrdSysMutex readopenstoremutex;
XrdSysMutex stringstoremutex;

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


#define XWCDEBUG

char* fdbuffermap[65535];

void xrd_sync_env();
void xrd_ro_env();
void xrd_rw_env();
void xrd_wo_env();

/******************************************************************************/
/*                            XrdPosixDirEntry Class                          */
/******************************************************************************/

class XrdPosixDirEntry {
public:
  XrdOucString dname;
  unsigned long long inode;
  XrdPosixDirEntry(const char* name, unsigned long long in) {
    dname = name;
    inode = in;
  }
};

/******************************************************************************/
/*                            XrdPosixDirList Class                           */
/******************************************************************************/


class XrdPosixDirList {
public:
  XrdPosixDirEntry** entrylist;
  int nEntries;
  int nUsed;
  struct dirbuf b;

  XrdPosixDirList() {
    nEntries = 1024;
    nUsed    = 0;
    entrylist = (XrdPosixDirEntry**) malloc(nEntries * sizeof(XrdPosixDirEntry*));
    b.p=0;
    b.size=0;
  }

  XrdPosixDirEntry* GetEntry(int index) {
    if (index < nUsed) {
      return entrylist[index];
    } else {
      return NULL;
    }
  }

  bool Add(const char* name, unsigned long long ino) {
    if (nUsed >= nEntries) {
      nEntries += 1024;
      entrylist = (XrdPosixDirEntry**) realloc(entrylist, nEntries * sizeof(XrdPosixDirEntry*));
    }
    entrylist[nUsed] = new XrdPosixDirEntry(name,ino);
    nUsed++;
    return true;
  }

  ~XrdPosixDirList() {
    for (int i=0 ;i< nUsed; i++) {
      delete entrylist[i];
    }
    delete entrylist;
    if (b.p) {
      free (b.p);
    }
  }
};


/******************************************************************************/
/*                           XrdOpenPosixFile Class                           */
/******************************************************************************/

static XrdOucHash<XrdPosixDirList>  *dirstore;
XrdSysMutex dirmutex;

class XrdOpenPosixFile {
public:
  int fd;
  int nuser;
  uid_t uid;

  XrdOpenPosixFile(int FD){fd=FD;nuser=0;uid=0;};
  XrdOpenPosixFile(int FD,uid_t UID){fd=FD;nuser=0;uid=UID;};

  ~XrdOpenPosixFile(){if ((nuser==0)&&(fd>0)) xrd_close(fd, 0);}
};


/******************************************************************************/
/*                             XrdPosixTiming Class                           */
/******************************************************************************/

static XrdOucHash<XrdOpenPosixFile>  *mknodopenstore;
static XrdOucHash<XrdOpenPosixFile>  *readopenstore;

class XrdPosixTiming {
public:
  struct timeval tv;
  XrdOucString tag;
  XrdOucString maintag;
  XrdPosixTiming* next;
  XrdPosixTiming* ptr;

  XrdPosixTiming(const char* name, struct timeval &i_tv) {
    memcpy(&tv, &i_tv, sizeof(struct timeval));
    tag = name;
    next = NULL;
    ptr  = this;
    tv.tv_sec = 0;
    tv.tv_usec = 0;
  } 
  XrdPosixTiming(const char* i_maintag) {
    tag = "BEGIN";
    next = NULL;
    ptr  = this;
    maintag = i_maintag;
  }

  void Print() {
    char msg[512];

    if (!getenv("EOS_TIMING"))       
      return;

    XrdPosixTiming* p = this->next;
    XrdPosixTiming* n; 

    cerr << std::endl;
    while ((n =p->next)) {

      sprintf(msg,"                                        [%12s] %12s<=>%-12s : %.03f\n",maintag.c_str(),p->tag.c_str(),n->tag.c_str(), (float)((n->tv.tv_sec - p->tv.tv_sec) *1000000 + (n->tv.tv_usec - p->tv.tv_usec))/1000.0);
      cerr << msg;
      p = n;
    }
    n = p;
    p = this->next;
    sprintf(msg,"                                        =%12s= %12s<=>%-12s : %.03f\n",maintag.c_str(),p->tag.c_str(), n->tag.c_str(), (float)((n->tv.tv_sec - p->tv.tv_sec) *1000000 + (n->tv.tv_usec - p->tv.tv_usec))/1000.0);
    cerr << msg;
  }

  virtual ~XrdPosixTiming(){XrdPosixTiming* n = next; if (n) delete n;};
};

#define TIMING(__ID__,__LIST__)                                 \
  do {                                                          \
    struct timeval tp;                                          \
    struct timezone tz;                                         \
    gettimeofday(&tp, &tz);                                     \
    (__LIST__)->ptr->next=new XrdPosixTiming(__ID__,tp);        \
    (__LIST__)->ptr = (__LIST__)->ptr->next;                    \
  } while(0);                                                   


static XrdFileCache* XFC;

XrdSysMutex OpenMutex;

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
  int rahead = 0; //97*1024;
  int rcsize = 0; //512*1024;

  if (!(getenv("EOS_XFC")))
  {
    if (getenv("EOS_READAHEADSIZE")) {
      rahead = atoi(getenv("EOS_READAHEADSIZE"));
    }
    if (getenv("EOS_READCACHESIZE")) {
      rcsize = atoi(getenv("EOS_READCACHESIZE"));
    }
  }
   
  XrdPosixXrootd::setEnv(NAME_READAHEADSIZE,rahead);
  XrdPosixXrootd::setEnv(NAME_READCACHESIZE,rcsize);
}


//------------------------------------------------------------------------------
void
xrd_wo_env()
{
  eos_static_info("");
  int rahead = 0; //97*1024;
  int rcsize = 0; //512*1024;

  if (!(getenv("EOS_XFC")))
  {
    if (getenv("EOS_READAHEADSIZE")) {
      rahead = atoi(getenv("EOS_READAHEADSIZE"));
    }
    if (getenv("EOS_READCACHESIZE")) {
      rcsize = atoi(getenv("EOS_READCACHESIZE"));
    }
  }  
  
  XrdPosixXrootd::setEnv(NAME_READAHEADSIZE, rahead);
  XrdPosixXrootd::setEnv(NAME_READCACHESIZE, rcsize);  
}

//------------------------------------------------------------------------------
void
xrd_sync_env()
{
  XrdPosixXrootd::setEnv(NAME_READAHEADSIZE, (long)0);
  XrdPosixXrootd::setEnv(NAME_READCACHESIZE, (long)0);
}

//-------------------------------------------------------------------------------------------------------------------
void xrd_rw_env() {
  int rahead = 0;
  int rcsize = 0;

  if (!(getenv("EOS_XFC"))) {
    if (getenv("EOS_WRITECACHESIZE")) {
      rcsize = atoi(getenv("EOS_WRITECACHESIZE"));
    }
  }
  
  XrdPosixXrootd::setEnv(NAME_READAHEADSIZE, rahead);
  XrdPosixXrootd::setEnv(NAME_READCACHESIZE, rcsize);
}


//------------------------------------------------------------------------------
int
xrd_rmxattr(const char *path, const char *xattr_name) 
{
  eos_static_info("path=%s xattr_name=%s", path, xattr_name);
  XrdPosixTiming rmxattrtiming("rmxattr");
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
  rmxattrtiming.Print();

  return rmxattr;
}


//------------------------------------------------------------------------------
int
xrd_setxattr(const char *path, const char *xattr_name, const char *xattr_value, size_t size) 
{
  eos_static_info("path=%s xattr_name=%s xattr_value=%s", path, xattr_name, xattr_value);
  XrdPosixTiming setxattrtiming("setxattr");
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
  setxattrtiming.Print();

  return setxattr;
}


//------------------------------------------------------------------------------
int
xrd_getxattr(const char *path, const char *xattr_name, char **xattr_value, size_t *size) 
{
  eos_static_info("path=%s xattr_name=%s",path,xattr_name);
  XrdPosixTiming getxattrtiming("getxattr");
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
  getxattrtiming.Print();

  return getxattr;
}


//------------------------------------------------------------------------------
int
xrd_listxattr(const char *path, char **xattr_list, size_t *size) 
{
  eos_static_info("path=%s",path);
  XrdPosixTiming listxattrtiming("listxattr");
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
  listxattrtiming.Print();

  return listxattr;
}


//------------------------------------------------------------------------------
int
xrd_stat(const char *path, struct stat *buf)
{
  eos_static_info("path=%x", path);
  XrdPosixTiming stattiming("xrd_stat");
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
  stattiming.Print();
  
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

  XrdPosixTiming statfstiming("xrd_statfs");
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
  statfstiming.Print();
  
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
  XrdPosixTiming chmodtiming("xrd_chmod");
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
  chmodtiming.Print();

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
  XrdPosixTiming symlinktiming("xrd_symlink");
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
  symlinktiming.Print();
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
  XrdPosixTiming linktiming("xrd_link");
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
  linktiming.Print();

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
xrd_readlink(const char* path, char* buf, size_t bufsize) {
  eos_static_info("path=%s", path);
  XrdPosixTiming readlinktiming("xrd_readlink");
  TIMING("START",&readlinktiming);
  char value[4096]; value[0] = 0;;
  XrdOucString request;
  request = path;
  request += "?";
  request += "mgm.pcmd=readlink";
  long long doreadlink = XrdPosixXrootd::QueryOpaque(request.c_str(), value, 4096);

  TIMING("END",&readlinktiming);
  readlinktiming.Print();

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
xrd_utimes(const char* path, struct timespec *tvp) {
  eos_static_info("path=%s", path);
  XrdPosixTiming utimestiming("xrd_utimes");
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
  utimestiming.Print();

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
xrd_access(const char* path, int mode) {
  eos_static_info("path=%s mode=%d", path, mode);
  XrdPosixTiming accesstiming("xrd_access");
  TIMING("START",&accesstiming);

  char value[4096]; value[0] = 0;;
  XrdOucString request;
  request = path;
  if (getenv("EOS_NOACCESS") && (!strcmp(getenv("EOS_NOACCESS"),"1"))) {
    return 0;
  }

  request += "?";
  request += "mgm.pcmd=access&mode=";
  request += (int)mode;
  long long doaccess = XrdPosixXrootd::QueryOpaque(request.c_str(), value, 4096);

  TIMING("STOP",&accesstiming);
  accesstiming.Print();

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
  XrdPosixTiming inodirtiming("xrd_inodirlist");
  TIMING("START",&inodirtiming);

  char* value=0;
  char* ptr=0;
  XrdOucString request;
  request = path;

  TIMING("GETSTSTREAM",&inodirtiming);

  int doinodirlist=-1;
  int retc;
  //  OpenMutex.Lock();
  xrd_sync_env();
  XrdClient* listclient = new XrdClient(request.c_str());
  
  if (!listclient) {
    //    OpenMutex.UnLock();
    return EFAULT;
  }

  if (!listclient->Open(0,0,true)) {
    //    OpenMutex.UnLock();
    delete listclient;
    return ENOENT;
  }

  //  OpenMutex.UnLock();

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
  
  XrdPosixDirList* posixdir;
  dirmutex.Lock();
  if ((dirstore->Find(dirtag))) {
    dirstore->Del(dirtag);
  }
  dirmutex.UnLock();
  
  posixdir = new XrdPosixDirList();
  
  TIMING("PARSESTSTREAM",&inodirtiming);    
  if (nbytes>= 0) {
    char dirpath[4096];
    unsigned long long inode;
    char tag[128];
    
    retc = 0;
    
    // parse the stat output
    int items = sscanf(value,"%s retc=%d",tag, &retc);
    if ((items != 2) || (strcmp(tag,"inodirlist:"))) {
      free(value);
      if (posixdir) 
        delete posixdir;
      return EFAULT;
    }
    ptr = strchr(value,' ');
    if (ptr) ptr = strchr(ptr+1,' ');
    char* endptr = value + strlen(value) -1 ;
    
    while ((ptr) &&(ptr < endptr)) {
      int items = sscanf(ptr,"%s %llu",dirpath,&inode);
      if (items != 2) {
        free(value);
        if (posixdir)
          delete posixdir;
        return EFAULT;
      }
      XrdOucString whitespacedirpath = dirpath;
      whitespacedirpath.replace("%20"," ");
      posixdir->Add(whitespacedirpath.c_str(), inode);
      // to the next entries
      if (ptr) ptr = strchr(ptr+1,' ');
      if (ptr) ptr = strchr(ptr+1,' ');
    } 
    doinodirlist = 0;
  }
  
  dirmutex.Lock();
  dirstore->Add(dirtag,posixdir);
  dirmutex.UnLock();
    
  TIMING("END",&inodirtiming);
  inodirtiming.Print();
  free(value);
  return doinodirlist;
}


//------------------------------------------------------------------------------  
DIR *xrd_opendir(const char *path)

{
  eos_static_info("path+%s",path);
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
  printf("Spath is %s\n",spath.c_str());
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
  if (XFC && inode)
    XFC->WaitFinishWrites(inode);
  eos_static_info("wait finished");
  return XrdPosixXrootd::Close(fildes);
}


//------------------------------------------------------------------------------
int
xrd_truncate(int fildes, off_t offset, unsigned long inode)
{
  eos_static_info("fd=%d offset=%llu inode=%lu", fildes, (unsigned long long)offset, inode);
  if (XFC && inode) {
    XFC->WaitFinishWrites(inode);
  }
  
  return XrdPosixXrootd::Ftruncate(fildes,offset);
}


//------------------------------------------------------------------------------
off_t
xrd_lseek(int fildes, off_t offset, int whence, unsigned long inode)
{
  eos_static_info("fd=%d offset=%llu whence=%d indoe=%lu", fildes, (unsigned long long)offset, whence, inode);
  if (XFC && inode) {
    XFC->WaitFinishWrites(inode);
  }
 
  return XrdPosixXrootd::Lseek(fildes, (long long)offset, whence);
}


//------------------------------------------------------------------------------
ssize_t
xrd_read(int fildes, void *buf, size_t nbyte, unsigned long inode)
{
  eos_static_info("fd=%d nbytes=%lu inode=%lu",fildes, (unsigned long)nbyte, (unsigned long)inode);
  size_t ret;
   
  if (XFC && inode)
  {
    XFC->WaitFinishWrites(inode);
    off_t offset = XrdPosixXrootd::Lseek(fildes, 0, SEEK_SET);

    if ((ret = XFC->GetRead(inode, fildes, buf, offset, nbyte)) != nbyte)
    {
      ret = XrdPosixXrootd::Read(fildes, buf, nbyte);
      XFC->PutRead(inode, fildes, buf, offset, nbyte);
    }
  } else {
    ret = XrdPosixXrootd::Read(fildes, buf, nbyte);
  }
  
  return ret;
}


//------------------------------------------------------------------------------
ssize_t
xrd_pread(int fildes, void *buf, size_t nbyte, off_t offset, unsigned long inode)
{
  eos_static_debug("fd=%d nbytes=%lu offset=%llu inode=%lu",fildes, (unsigned long)nbyte, (unsigned long long)offset, (unsigned long) inode);
  size_t ret;

  if (XFC && inode) {
    XFC->WaitFinishWrites(inode);
    if ((ret = XFC->GetRead(inode, fildes, buf, offset, nbyte)) != nbyte)
      {
        fprintf(stdout, "Block not in cache, try to read it now. \n");
        ret = XrdPosixXrootd::Pread(fildes, buf, nbyte, static_cast<long long>(offset));
        fprintf(stdout, "Block not in cache, try to cache it now. \n");
        XFC->PutRead(inode, fildes, buf, offset, nbyte);
      }
  } else {
    ret = XrdPosixXrootd::Pread(fildes, buf, nbyte, static_cast<long long>(offset));
  }
  
  return ret;
}


//------------------------------------------------------------------------------
ssize_t
xrd_write(int fildes, const void *buf, size_t nbyte, unsigned long inode)
{
  eos_static_info("fd=%d nbytes=%lu inode=%lu", fildes, (unsigned long)nbyte, (unsigned  long) inode);
  size_t ret;
  if (XFC && inode) {
    off_t offset = XrdPosixXrootd::Lseek(fildes, 0, SEEK_SET);
    XFC->SubmitWrite(inode, fildes, const_cast<void*>(buf), offset, nbyte);
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
  eos_static_debug("fd=%d nbytes=%lu inode=%lu", fildes, (unsigned long)nbyte, (unsigned long) inode);
  size_t ret;
  if (XFC && inode) {
    XFC->SubmitWrite(inode, fildes, const_cast<void*>(buf), offset, nbyte);
    ret = nbyte;
  } else {
    ret = XrdPosixXrootd::Pwrite(fildes, buf, nbyte, static_cast<long long>(offset));    
  }
  return ret;                  
}


//------------------------------------------------------------------------------
int
xrd_fsync(int fildes, unsigned long inode)
{
  eos_static_info("fd=%d inode=%lu", fildes, (unsigned long)inode);
  if (XFC && inode) {
    XFC->WaitFinishWrites(inode);
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
void
xrd_store_inode(long long inode, const char* name) {
  eos_static_info("inode=%llu name=%s", inode, name);
  XrdOucString* node;
  char nodename[4096];
  sprintf(nodename,"%lld",inode);

  inodestoremutex.Lock();
  if ( (node = inodestore->Find(nodename) ) ) {
    inodestore->Rep(nodename, new XrdOucString(name));
    inodestoremutex.UnLock();
  } else {
    inodestore->Add(nodename, new XrdOucString(name));
    inodestoremutex.UnLock();
  }
}


//------------------------------------------------------------------------------
void
xrd_forget_inode(long long inode) 
{
  eos_static_info("inode=%lld", inode);
  char nodename[4096];
  sprintf(nodename,"%lld",inode);
  
  inodestoremutex.Lock();
  inodestore->Del(nodename);
  inodestoremutex.UnLock();
}


//------------------------------------------------------------------------------
const char*
xrd_get_name_for_inode(long long inode)
{
  eos_static_info("inode=%lld", inode);
  char nodename[4096];
  sprintf(nodename,"%lld",inode);
  inodestoremutex.Lock();
  XrdOucString* node = (inodestore->Find(nodename));
  inodestoremutex.UnLock();
  if (node) 
    return node->c_str();
  else 
    return NULL;
}


//------------------------------------------------------------------------------
const char*
xrd_mapuser(uid_t uid)
{
  eos_static_info("uid=%lu", (unsigned long) uid);
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
int
xrd_inodirlist_entry(unsigned long long dirinode, int index, char** name, unsigned long long *inode)
{
  eos_static_info("indoe=%llu", dirinode);
  char dirtag[1024];
  sprintf(dirtag,"%llu", dirinode);
  XrdPosixDirList* posixdir;
  dirmutex.Lock();
  if ((posixdir = dirstore->Find(dirtag))) {
    dirmutex.UnLock();
    XrdPosixDirEntry* entry;
    entry = posixdir->GetEntry(index);
    if (entry) {
      *name = (char*)entry->dname.c_str();
      *inode = entry->inode;
      return 0;
    }
  }
  dirmutex.UnLock();
  return -1;
}


//------------------------------------------------------------------------------
void
xrd_inodirlist_delete(unsigned long long dirinode)
{
  eos_static_info("inode=%llu", dirinode);
  char dirtag[1024];
  sprintf(dirtag,"%llu",dirinode);
  dirmutex.Lock();
  dirstore->Del(dirtag);
  dirmutex.UnLock();
}


//------------------------------------------------------------------------------
int
xrd_mknodopenfilelist_get(unsigned long long inode)
{
  eos_static_info("inode=%llu", inode);
  char filetag[1024];
  sprintf(filetag,"%llu",inode);
  XrdOpenPosixFile* posixfile;
  mknodopenstoremutex.Lock();
  if ( (posixfile = mknodopenstore->Find(filetag))) {
    posixfile->nuser++;
    mknodopenstoremutex.UnLock();
    return posixfile->fd;
  }
  mknodopenstoremutex.UnLock();
  return -1;
}


//------------------------------------------------------------------------------
int
xrd_mknodopenfilelist_release(int fd,unsigned long long inode)
{
  eos_static_info("fd=%d inode=%llu", fd, inode);
  char filetag[1024];
  sprintf(filetag,"%llu",inode);
  XrdOpenPosixFile* posixfile;
  mknodopenstoremutex.Lock();
  if ( (posixfile = mknodopenstore->Find(filetag))) {
    if (fd == posixfile->fd) {
      mknodopenstore->Del(filetag);
      mknodopenstoremutex.UnLock();
      return 0;
    }
  }
  mknodopenstoremutex.UnLock();
  return -1;
}


//------------------------------------------------------------------------------
int
xrd_mknodopenfilelist_add(int fd, unsigned long long inode)
{
  eos_static_info("fd=%d inode=%llu", fd, inode);
  char filetag[1024];
  sprintf(filetag,"%llu",inode);

  XrdOpenPosixFile* posixfile;
  mknodopenstoremutex.Lock();
  if ( (posixfile = mknodopenstore->Find(filetag)) ) {
    mknodopenstoremutex.UnLock();
    return -1;
  }
  
  posixfile = new XrdOpenPosixFile(fd);
  mknodopenstore->Add(filetag,posixfile,60);
  mknodopenstoremutex.UnLock();
  return 0;
}


//------------------------------------------------------------------------------
int
xrd_readopenfilelist_get(unsigned long long inode, uid_t uid)
{
  eos_static_info("inode=%llu uid=%lu", inode, (unsigned long) uid);
  char filetag[1024];
  sprintf(filetag,"%u-%llu",uid,inode);
  XrdOpenPosixFile* posixfile;
  readopenstoremutex.Lock();
  if ( (posixfile = readopenstore->Find(filetag))) {
    posixfile->nuser++;
    readopenstoremutex.UnLock();
    return posixfile->fd;
  }
  readopenstoremutex.UnLock();
  return -1;
}


//------------------------------------------------------------------------------
int
xrd_readopenfilelist_lease(unsigned long long inode, uid_t uid)
{
  eos_static_info("inode=%llu uid=%u", inode, (unsigned long) uid);
  char filetag[1024];
  sprintf(filetag,"%u%llu",uid, inode);
  XrdOpenPosixFile* posixfile;
  readopenstoremutex.Lock();
  if ( (posixfile = readopenstore->Find(filetag))) {
    posixfile->nuser--;
    readopenstoremutex.UnLock();
    return posixfile->fd;
  }
  readopenstoremutex.UnLock();
  return -1;
}


//------------------------------------------------------------------------------
int
xrd_readopenfilelist_add(int fd, unsigned long long inode, uid_t uid, double readopentime)
{
  eos_static_info("fd=%d inode=%llu uid=%lu readopentime=%.02f", fd, inode, (unsigned long)uid, readopentime);
  char filetag[1024];
  sprintf(filetag,"%u-%llu",uid,inode);
  
  XrdOpenPosixFile* posixfile;
  readopenstoremutex.Lock();
  if ( (posixfile = readopenstore->Find(filetag)) ) {
    readopenstoremutex.UnLock();
    return -1;
  }
  
  posixfile = new XrdOpenPosixFile(fd, uid);
  readopenstore->Add(filetag,posixfile,(time_t)readopentime);
  readopenstoremutex.UnLock();
  return 0;
}


//------------------------------------------------------------------------------
struct dirbuf*
xrd_inodirlist_getbuffer(unsigned long long dirinode)
{
  eos_static_info("inode=%llu",dirinode);
  char dirtag[1024];
  sprintf(dirtag,"%llu",dirinode);
  XrdPosixDirList* posixdir;
  dirmutex.Lock();
  if ((posixdir = dirstore->Find(dirtag))) {
    dirmutex.UnLock();
    return (&(posixdir->b));
  } else {
    dirmutex.UnLock();
    return NULL;
  }
}


//------------------------------------------------------------------------------
const char* xrd_get_dir(DIR* dp, int entry) { return 0;}


#define MAX_NUM_NODES 63 /* max number of data nodes in a cluster */
//------------------------------------------------------------------------------
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

  eos::common::Mapping::VirtualIdentity_t vid;
  eos::common::Mapping::Root(vid);
  eos::common::Logging::Init();
  eos::common::Logging::SetUnit("FUSE@localhost");
  eos::common::Logging::SetLogPriority(LOG_INFO);

  memset(fdbuffermap,0,sizeof(fdbuffermap));

  XrdPosixXrootd::setEnv(NAME_DATASERVERCONN_TTL,300);
  XrdPosixXrootd::setEnv(NAME_LBSERVERCONN_TTL,3600*24);
  XrdPosixXrootd::setEnv(NAME_REQUESTTIMEOUT,30);
  EnvPutInt("NAME_MAXREDIRECTCOUNT",3);
  EnvPutInt("NAME_RECONNECTWAIT", 10);

  setenv("XRDPOSIX_POPEN","1",1);
  if (getenv("EOS_DEBUG")) {
    XrdPosixXrootd::setEnv(NAME_DEBUG,atoi(getenv("EOS_DEBUG")));
  }
  
  //uncomment this to enable XrdCachFile
  setenv("EOS_XFC", "1", 1);
  setenv("EOS_XFC_SIZE", "200000000", 1);   // ~200MB

  //initialise the XrdFileCache
  if (!(getenv("EOS_XFC"))) {
    eos_static_notice("File Cache not initialized.");
    XFC = NULL;
  } else {
    eos_static_notice("File Cache enabled with size %s",getenv("EOS_XFC_SIZE"));
    XFC = XrdFileCache::Instance(static_cast<size_t>(atol(getenv("EOS_XFC_SIZE"))));   
  }
  
  passwdstore = new XrdOucHash<XrdOucString> ();
  inodestore  = new XrdOucHash<XrdOucString> ();
  stringstore = new XrdOucHash<XrdOucString> ();
  dirstore    = new XrdOucHash<XrdPosixDirList> (); 
  mknodopenstore = new XrdOucHash<XrdOpenPosixFile> ();
  readopenstore = new XrdOucHash<XrdOpenPosixFile> ();
}
        

