//------------------------------------------------------------------------------
//! @file eosfuse.hh
//! @author Andreas-Joachim Peters CERN
//! @brief EOS C++ Fuse low-level implementation 
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#ifndef FUSE_EOSFUSE_HH_
#define FUSE_EOSFUSE_HH_

#include "stat/Stat.hh"
#include "md/md.hh"
#include "cap/cap.hh"
#include "data/data.hh"
#include "backend/backend.hh"
#include "kv/kv.hh"
#include "llfusexx.hh"

#include <string.h>
#include <string>
#include <thread>

class EosFuse : public llfusexx::FuseBase<EosFuse>
{
public:

  static EosFuse&
  instance()
  {
    static EosFuse i;
    return i;
  }

  EosFuse();
  virtual ~EosFuse();

  int run(int argc, char* argv[], void *userdata);

  static void init(void *userdata, struct fuse_conn_info *conn);

  static void destroy(void *userdata);

  static void getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);

  static void setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
                      int to_set, struct fuse_file_info *fi);

  static void
  lookup(fuse_req_t req, fuse_ino_t parent, const char *name);

  static void opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);

  static void readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                      struct fuse_file_info *fi);

  static void releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);

  static void statfs(fuse_req_t req, fuse_ino_t ino);

  static void mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
                    mode_t mode, dev_t rdev);

  static void mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
                    mode_t mode);

  static void rm(fuse_req_t req, fuse_ino_t parent, const char *name);

  static void unlink(fuse_req_t req, fuse_ino_t parent, const char *name);

  static void rmdir(fuse_req_t req, fuse_ino_t parent, const char *name);

#ifdef _FUSE3
  static void rename(fuse_req_t req, fuse_ino_t parent, const char *name,
                     fuse_ino_t newparent, const char *newname, unsigned int flags);
#else
  static void rename(fuse_req_t req, fuse_ino_t parent, const char *name,
                     fuse_ino_t newparent, const char *newname);
#endif

  static void access(fuse_req_t req, fuse_ino_t ino, int mask);

  static void open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info * fi);

  static void create(fuse_req_t req, fuse_ino_t parent, const char *name,
                     mode_t mode, struct fuse_file_info *fi);

  static void read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                   struct fuse_file_info * fi);

  static void write(fuse_req_t req, fuse_ino_t ino, const char *buf,
                    size_t size, off_t off, struct fuse_file_info * fi);

  static void release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info * fi);

  static void fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                    struct fuse_file_info * fi);

  static void forget(fuse_req_t req, fuse_ino_t ino, unsigned long nlookup);

  static void flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info * fi);

#ifdef __APPLE__
  static void getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                       size_t size, uint32_t position);
#else
  static void getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                       size_t size);
#endif

#ifdef __APPLE__
  static void setxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                       const char *value, size_t size, int flags, uint32_t position);
#else
  static void setxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                       const char *value, size_t size, int flags);
#endif

  static void listxattr(fuse_req_t req, fuse_ino_t ino, size_t size);

  static void removexattr(fuse_req_t req, fuse_ino_t ino, const char *name);

  static void readlink(fuse_req_t req, fuse_ino_t ino);

  static void
  symlink(fuse_req_t req, const char *link, fuse_ino_t parent, const char *name);

  static void getlk(fuse_req_t req, fuse_ino_t ino,
                    struct fuse_file_info *fi, struct flock *lock);

  static void setlk(fuse_req_t req, fuse_ino_t ino,
                    struct fuse_file_info *fi,
                    struct flock *lock, int sleep) ;
  metad mds;
  data datas;
  cap caps;
  backend mdbackend;

  static EosFuse& Instance()
  {
    return *sEosFuse;
  }

  fuse_session* Session()
  {
    return fusesession;
  }

  fuse_chan* Channel()
  {
    return fusechan;
  }

  typedef struct cfg
  {
    std::string name;
    std::string hostport;
    std::string remotemountdir;
    std::string localmountdir;
    std::string statfilesuffix;
    std::string statfilepath;
    std::string mdcachehost;
    int mdcacheport;
    std::string mqtargethost;
    std::string mqidentity;
    std::string mqname;
    std::string clienthost;
    std::string clientuuid;

    typedef struct options
    {
      int debug;
      int lowleveldebug;
      int debuglevel;
      int libfusethreads;
      int foreground;
      int md_kernelcache;
      int data_kernelcache;
      int mkdir_is_sync;
      int create_is_sync;
    } options_t;
    options_t options;
  } cfg_t;

  cfg_t& Config()
  {
    return config;
  }

private:

  struct fuse_id
  {
    uid_t uid;
    gid_t gid;
    pid_t pid;

    fuse_id(fuse_req_t req)
    {
      uid = fuse_req_ctx(req)->uid;
      gid = fuse_req_ctx(req)->gid;
      pid = fuse_req_ctx(req)->pid;
    }
  } ;

  static std::string dump(fuse_id id,
                          fuse_ino_t ino,
                          struct fuse_file_info *fi,
                          int rc,
                          std::string name="")
  {
    char s[1024];
    char ebuf[1024];
    ebuf[0]=0;
    if (strerror_r(rc, ebuf, sizeof (ebuf)))
    {
      snprintf(ebuf, sizeof (ebuf), "???");
    }

    snprintf(s, 1024, "rc=%02d uid=%05d gid=%05d pid=%05d ino=%08lx fh=%08lx name=%s",
             rc,
             id.uid,
             id.gid,
             id.pid,
             ino,
             fi ? fi->fh : 0,
             name.c_str());
    return s;
  }

  cfg_t config;

  kv mKV;

  Stat fusestat;

  Stat& getFuseStat()
  {
    return fusestat;
  }

  metad::mdstat& getMdStat()
  {
    return mds.stats();
  }

  static EosFuse* sEosFuse;

  struct fuse_session* fusesession;
  struct fuse_chan* fusechan;

  std::thread tDumpStatistic;
  std::thread tStatCirculate;
  std::thread tMetaCacheFlush;
  std::thread tMetaCommunicate;
  std::thread tCapFlush;

  static void DumpStatistic();
  static void StatCirculate();

} ;

#endif /* FUSE_EOSFUSE_HH_ */
