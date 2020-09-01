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

#include "misc/MacOSXHelper.hh"
#include "common/AssistedThread.hh"
#include "common/LinuxTotalMem.hh"
#include "common/Murmur3.hh"

#include "stat/Stat.hh"
#include "md/md.hh"
#include "cap/cap.hh"
#include "data/data.hh"
#include "backend/backend.hh"
#include "kv/kv.hh"
#include "kv/RedisKV.hh"
#include "llfusexx.hh"
#include "auth/CredentialFinder.hh"
#include "misc/Track.hh"
#include "misc/FuseId.hh"
#include "misc/stringTS.hh"
#include "submount/SubMount.hh"
#include <set>
#include <signal.h>
#include <string.h>
#include <string>
#include <thread>

#include <google/dense_hash_set>
#include <google/dense_hash_map>

// PROTOBUF protocol version announced via heartbeats and attached to URLs by the backend
#define FUSEPROTOCOLVERSION eos::fusex::heartbeat::PROTOCOLV4

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

  std::string UsageGet();
  std::string UsageSet();
  std::string UsageMount();
  std::string UsageHelp();

  int run(int argc, char* argv[], void* userdata);

  static void umounthandler(int sig, siginfo_t* si, void* unused);

  static void init(void* userdata, struct fuse_conn_info* conn);

  static void destroy(void* userdata);

  static void getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);

  static void setattr(fuse_req_t req, fuse_ino_t ino, struct stat* attr,
                      int to_set, struct fuse_file_info* fi);

  static void
  lookup(fuse_req_t req, fuse_ino_t parent, const char* name);

  static int listdir(fuse_req_t req, fuse_ino_t ino, metad::shared_md& md);

  static void opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);

  static void readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                      struct fuse_file_info* fi);

  static void releasedir(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info* fi);

  static void statfs(fuse_req_t req, fuse_ino_t ino);

  static void mknod(fuse_req_t req, fuse_ino_t parent, const char* name,
                    mode_t mode, dev_t rdev);

  static void mkdir(fuse_req_t req, fuse_ino_t parent, const char* name,
                    mode_t mode);

  static void rm(fuse_req_t req, fuse_ino_t parent, const char* name);

  static void unlink(fuse_req_t req, fuse_ino_t parent, const char* name);

  static void rmdir(fuse_req_t req, fuse_ino_t parent, const char* name);

#ifdef _FUSE3
  static void rename(fuse_req_t req, fuse_ino_t parent, const char* name,
                     fuse_ino_t newparent, const char* newname, unsigned int flags);
#else
  static void rename(fuse_req_t req, fuse_ino_t parent, const char* name,
                     fuse_ino_t newparent, const char* newname);
#endif

  static void access(fuse_req_t req, fuse_ino_t ino, int mask);

  static void open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);

  static void create(fuse_req_t req, fuse_ino_t parent, const char* name,
                     mode_t mode, struct fuse_file_info* fi);

  static void read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                   struct fuse_file_info* fi);

  static void write(fuse_req_t req, fuse_ino_t ino, const char* buf,
                    size_t size, off_t off, struct fuse_file_info* fi);

  static void release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);

  static void fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                    struct fuse_file_info* fi);

  static void forget(fuse_req_t req, fuse_ino_t ino, unsigned long nlookup);

  static void flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi);

#ifdef __APPLE__
  static void getxattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                       size_t size, uint32_t position);
#else
  static void getxattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                       size_t size);
#endif

#ifdef __APPLE__
  static void setxattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                       const char* value, size_t size, int flags, uint32_t position);
#else
  static void setxattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                       const char* value, size_t size, int flags);
#endif

  static void listxattr(fuse_req_t req, fuse_ino_t ino, size_t size);

  static void removexattr(fuse_req_t req, fuse_ino_t ino, const char* name);

  static void readlink(fuse_req_t req, fuse_ino_t ino);

  static void
  link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t parent, const char* newname);

  static void
  symlink(fuse_req_t req, const char* link, fuse_ino_t parent, const char* name);

  static void getlk(fuse_req_t req, fuse_ino_t ino,
                    struct fuse_file_info* fi, struct flock* lock);

  static void setlk(fuse_req_t req, fuse_ino_t ino,
                    struct fuse_file_info* fi,
                    struct flock* lock, int sleep);

#if ( FUSE_VERSION > 28 )
  static void flock(fuse_req_t req, fuse_ino_t ino,
		    struct fuse_file_info *fi, int op);
#endif

  metad mds;
  ::data datas;
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

  std::string Prefix(std::string path);

  typedef struct cfg
  {
    std::string name;
    std::string hostport;
    std::string remotemountdir;
    std::string localmountdir;
    std::string statfilesuffix;
    std::string statfilepath;
    std::string logfilepath;
    std::string mdcachedir;
    std::string mdcachedir_unlink;
    std::string mqtargethost;
    std::string mqidentity;
    std::string mqname;
    std::string clienthost;
    std::string clientuuid;
    std::string ssskeytab;
    std::string appname;

    typedef struct options
    {
      int debug;
      int debuglevel;
      int libfusethreads;
      int foreground;
      int automounted;
      int md_kernelcache;
      int enable_backtrace;
      double md_kernelcache_enoent_timeout;
      double md_backend_timeout;
      double md_backend_put_timeout;
      int data_kernelcache;
      int rename_is_sync;
      int rmdir_is_sync;
      int global_flush;

      int flush_wait_umount;
      int flush_wait_open;
      enum eFLUSH_WAIT_OPEN
      {
        kWAIT_FLUSH_NEVER = 0, // if a file is updated/created - flush will not wait to open it
        kWAIT_FLUSH_ON_UPDATE = 1, // if a file is updated - flush will wait to open it
        kWAIT_FLUSH_ON_CREATE = 2 // if a file is created - flush will wait to open it
      };

      size_t flush_wait_open_size;

      int global_locking;
      uint64_t fdlimit;
      int rm_rf_protect_levels;
      int rm_rf_bulk;
      int show_tree_size;
      int free_md_asap;
      int cpu_core_affinity;
      mode_t overlay_mode;
      mode_t x_ok;
      int no_xattr;
      int no_eos_xattr_listing;
      int no_hardlinks;
      uint32_t nocache_graceperiod;
      int leasetime;
      int write_size_flush_interval;
      int submounts;
      int inmemory_inodes;
      bool flock;
      bool hide_versions;
      std::vector<std::string> no_fsync_suffixes;
      std::vector<std::string> nowait_flush_executables;
    } options_t;

    typedef struct recovery
    {
      int read;
      int write;
      int read_open;
      int write_open;
      int read_open_noserver;
      int write_open_noserver;
      size_t read_open_noserver_retrywindow;
      size_t write_open_noserver_retrywindow;
    } recovery_t;

    typedef struct fuzzing
    {
      size_t open_async_submit;
      size_t open_async_return;
      size_t read_async_return;
      bool open_async_submit_fatal;
      bool open_async_return_fatal;
    } fuzzing_t;

    typedef struct inlining
    {
      uint64_t max_size;
      std::string default_compressor;
    } inlining_t;

    recovery_t recovery;
    options_t options;
    inlining_t inliner;
    fuzzing_t fuzzing;

    CredentialConfig auth;
  } cfg_t;

  cfg_t& Config()
  {
    return config;
  }

  Track& Tracker()
  {
    return tracker;
  }

  SubMount& Mounter()
  {
    return mounter;
  }

  static std::string dump(fuse_id id,
                          fuse_ino_t ino,
                          struct fuse_file_info* fi,
                          int rc,
                          std::string name = "")
  {
    char s[1024];
    char ebuf[1024];
    ebuf[0] = 0;

    if (strerror_r(rc, ebuf, sizeof(ebuf))) {
      snprintf(ebuf, sizeof(ebuf), "???");
    }

    snprintf(s, 1024,
             "rc=%02d uid=%05d gid=%05d pid=%05d ino=%#lx fh=%#lx name=%s",
             rc,
             id.uid,
             id.gid,
             id.pid,
             ino,
             fi ? fi->fh : 0,
             name.c_str());
    return s;
  }

  typedef struct opendir_fh
  {
    typedef std::vector<std::string> ChildSet;
    typedef std::map <std::string, uint64_t> ChildMap;

    opendir_fh()
    {
      pmd_mtime.tv_sec = pmd_mtime.tv_nsec = 0;
    }

    metad::shared_md md;

    ChildSet readdir_items;
    ChildMap pmd_children;

    struct reply_buf
    {
      char blob[65536];
      char* ptr;
      off_t size;

      reply_buf()
      {
        reset();
      }

      void reset() {
        ptr = blob;
        size = 0;
      }

      char* buffer()
      {
        return blob;
      }
    };

    struct timespec pmd_mtime;
    struct reply_buf b;

    XrdSysMutex items_lock;
  } opendir_t;

  static int readdir_filler(fuse_req_t req, opendir_t* md,
                            mode_t&pmd_mode, uint64_t&pmd_id);

  void getHbStat(eos::fusex::statistics&);

  kv* getKV()
  {

    return mKV.get();
  }

  cap& getCap()
  {

    return caps;
  }

  void cleanup(fuse_ino_t ino)
  {

    return mds.cleanup(ino);
  }

  void TrackMgm(const std::string& lasturl);

  stringTS statsout;

  int truncateLogFile()
  {
    if (!fstderr) {
      return 0;
    }

    fflush(fstderr);
    return ftruncate(fileno(fstderr), (off_t) 0);
  }

  size_t sizeLogFile()
  {
    struct stat buf;
    if (!fstderr) {
      return 0;
    }

    if (!fstat(fileno(fstderr), &buf)) {
      return buf.st_size;
    } else {

      return 0;
    }
  }

  void shrinkLogFile()
  {
    if (!fstderr) {
      return ;
    }
    const size_t maxsize = 4*1024ll*1024ll*1024ll; // 4G
    if ( sizeLogFile() > maxsize) {
      ftruncate(fileno(fstderr), maxsize/2);
      eos_static_crit("logfile has been truncated back to %lu bytes - exceeded %lu bytes", maxsize/2, maxsize);
    }
  }

  bool Trace() { return mTrace; }
  void SetTrace(bool t) { mTrace = t;}

protected:

private:

  static bool isRecursiveRm(fuse_req_t req, bool forced = false,
                            bool notverbose = false);

  bool mTrace;

  Track tracker;

  SubMount mounter;

  cfg_t config;

  stringTS lastMgmHostPort;

  std::unique_ptr<kv> mKV;
  Stat fusestat;

  FILE* fstderr;

  Stat& getFuseStat()
  {

    return fusestat;
  }

  metad::mdstat& getMdStat()
  {
    return mds.stats();
  }

  eos::common::LinuxTotalMem meminfo;
  XrdSysMutex statsoutmutex;

  static EosFuse* sEosFuse;

  struct fuse_session* fusesession;
  struct fuse_chan* fusechan;


  AssistedThread tDumpStatistic;
  AssistedThread tStatCirculate;
  AssistedThread tMetaCacheFlush;
  AssistedThread tMetaSizeFlush;
  AssistedThread tMetaStackFree;
  AssistedThread tMetaCommunicate;
  AssistedThread tCapFlush;

  void DumpStatistic(ThreadAssistant& assistant);
  void StatCirculate(ThreadAssistant& assistant);
};

#endif /* FUSE_EOSFUSE_HH_ */
