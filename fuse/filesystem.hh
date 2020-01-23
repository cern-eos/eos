//------------------------------------------------------------------------------
//! @file filesystem.hh
//! @author Andreas-Joachim Peters, Geoffray Adde, Elvin Sindrilaru CERN
//! @brief remote IO filesystem implementation
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

#ifndef FUSE_FILESYSTEM_HH_
#define FUSE_FILESYSTEM_HH_

#include "llfusexx.hh"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
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
#include "common/Logging.hh"
#include "common/Path.hh"
#include "common/RWMutex.hh"
#include "common/SymKeys.hh"
#include <google/dense_hash_map>
#include <google/sparse_hash_map>
#include <google/sparsehash/densehashtable.h>
#include "FuseCacheEntry.hh"
#include "fst/layout/LayoutPlugin.hh"
#include "fst/layout/PlainLayout.hh"
#include "fst/layout/RaidDpLayout.hh"
#include "fst/layout/ReedSLayout.hh"
#include "FuseCache/FuseWriteCache.hh"
#include "FuseCache/FileAbstraction.hh"
#include "FuseCache/LayoutWrapper.hh"
#include "AuthIdManager.hh"

#define sMaxAuthId (2^6)
#define N_OPEN_MUTEXES_NBITS 12
#define N_OPEN_MUTEXES (1 << N_OPEN_MUTEXES_NBITS)
#define PAGESIZE 128 * 1024

// Sometimes, XRootd gives a NULL responses on some calls, this is a bug.
// When it happens we retry.
extern int xrootd_nullresponsebug_retrycount;
// Sometimes, XRootd gives a NULL responses on some calls, this is a bug.
// When it happens we sleep between attempts.
extern int xrootd_nullresponsebug_retrysleep;

class fuse_filesystem
{
public:

  fuse_filesystem();

  virtual ~fuse_filesystem();

  typedef std::vector<unsigned long long> dirlist;

  void
  setMountPoint(const std::string& md)
  {
    mount_dir = md;
  }

  const char*
  getMountPoint()
  {
    return mount_dir.c_str();
  }

  typedef struct fd_user_info {
    unsigned long long fd;
    uid_t uid;
    gid_t gid;
    gid_t pid;
    long long ino;
  } fd_user_info;

  void setMaxWbInMemorySize(uint64_t size)
  {
    max_wb_in_memory_size = size;
  }

  uint64_t getMaxWbInMemorySize() const
  {
    return max_wb_in_memory_size;
  }

  //----------------------------------------------------------------------------
  // Lock
  //----------------------------------------------------------------------------
  void
  lock_r_pcache(pid_t pid);

  void
  lock_w_pcache(pid_t pid);

  //----------------------------------------------------------------------------
  // Unlock
  //----------------------------------------------------------------------------
  void
  unlock_r_pcache(pid_t pid);

  void
  unlock_w_pcache(pid_t pid);

  //----------------------------------------------------------------------------
  //                ******* Path translation *******
  //----------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  //! Read lock for path or inode translation
  //----------------------------------------------------------------------------
  void lock_r_p2i();


  //----------------------------------------------------------------------------
  //! Read unlock after path or inode translation
  //----------------------------------------------------------------------------
  void unlock_r_p2i();

  //----------------------------------------------------------------------------
  //! Translate from inode to path
  //----------------------------------------------------------------------------
  const char* path(unsigned long long inode);

  //----------------------------------------------------------------------------
  //! Return the basename of a file
  //----------------------------------------------------------------------------
  std::string base_name(unsigned long long inode);

  //----------------------------------------------------------------------------
  //! Translate from path to inode
  //----------------------------------------------------------------------------
  unsigned long long inode(const char* path);

  //----------------------------------------------------------------------------
  //! Get URL without CGI
  //----------------------------------------------------------------------------
  XrdOucString
  get_url_nocgi(const char* url);

  //----------------------------------------------------------------------------
  //! Store an inode/path mapping
  //----------------------------------------------------------------------------
  void store_p2i(unsigned long long inode, const char* path);

  //----------------------------------------------------------------------------
  //! Store an inode/path mapping starting from the parent:
  //! inode + child inode + child base name
  //----------------------------------------------------------------------------
  void store_child_p2i(unsigned long long inode,
                       unsigned long long childinode,
                       const char* name);

  //----------------------------------------------------------------------------
  //! Forget an inode/path mapping by inode
  //----------------------------------------------------------------------------
  void forget_p2i(unsigned long long inode);

  //----------------------------------------------------------------------------
  //! redirect inode to a new inode
  //----------------------------------------------------------------------------
  void redirect_p2i(unsigned long long inode, unsigned long long new_inode);

  //----------------------------------------------------------------------------
  //! Redirect inode to latest version of an inode
  //----------------------------------------------------------------------------
  unsigned long long redirect_i2i(unsigned long long inode);

  //----------------------------------------------------------------------------
  //! Replace all names with a given prefix
  //----------------------------------------------------------------------------
  void
  replace_prefix(const char* oldprefix, const char* newprefix);

  //----------------------------------------------------------------------------
  //                ******* Path computation *******
  //----------------------------------------------------------------------------
  inline void
  getPath(std::string& out,
          std::string& prefix,
          const char* name)
  {
    out = "/";
    out += prefix;
    out += name;
  }

  //----------------------------------------------------------------------------
  //! Store an inode/mtime pair
  //----------------------------------------------------------------------------
  void store_i2mtime(unsigned long long inode, timespec ts);

  //----------------------------------------------------------------------------
  //! Store and test inode/mtime pair
  //----------------------------------------------------------------------------
  bool store_open_i2mtime(unsigned long long inode);

  inline void
  getPPath(std::string& out,
           std::string& prefix,
           const char* parent,
           const char* name)
  {
    out = "/";
    out += prefix;
    out += parent;
    out += "/";
    out += name;
  }

  inline void
  getUrl(std::string& out,
         std::string& user,
         std::string& hostport,
         std::string& prefix,
         const char* parent,
         const char* name)
  {
    out = "root://";
    out += user;
    out += "@";
    out += hostport;
    out += "//";
    out += prefix;
    out += parent;
    out += "/";
    out += name;
  }

  inline void
  getParentUrl(
    std::string& out,
    std::string& user,
    std::string& hostport,
    std::string& prefix,
    std::string& parent)
  {
    out = "root://";
    out += user;
    out += "@";
    out += hostport;
    out += "//";
    out += prefix;
    out += parent;
  }

  static inline bool checkpathname(const char* pathname)
  {
    static const std::vector<char> forbidden = {'?'};

    for (const char* c = pathname; *c != 0; c++)
      for (size_t i = 0; i < forbidden.size(); i++)
        if (*c == forbidden[i]) {
          return false;
        }

    return true;
  }

  inline std::string safePath(const char* unsafe)
  {
    if (encode_pathname) {
      return eos::common::StringConversion::curl_escaped(unsafe);
    } else {
      return unsafe;
    }
  }

  //----------------------------------------------------------------------------
  //                ******* IO buffers *******
  //----------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  //! Guarantee a buffer for reading of at least 'size' for the specified thread
  //!
  //! @param tid thread id
  //! @param size size of the read buffer
  //!
  //! @return pointer to buffer region
  //!
  //----------------------------------------------------------------------------
  char* attach_rd_buff(pthread_t tid, size_t size);

  //----------------------------------------------------------------------------
  //              ******* POSIX opened file descriptors *******
  //----------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  //! Create an artificial file descriptor
  //----------------------------------------------------------------------------
  int generate_fd();

  //----------------------------------------------------------------------------
  //! Add new inodeuser to fd mapping used when doing stat through the file obj.
  //!
  //! @param inode inode number
  //! @param uid user uid
  //! @param fd file descriptor
  //----------------------------------------------------------------------------
  void add_inodeuser_fd(unsigned long long inode, uid_t uid, gid_t gid, pid_t pid,
                        int fd);

  //----------------------------------------------------------------------------
  //! Add new mapping between fd and raw file object
  //----------------------------------------------------------------------------
  int
  add_fd2file(LayoutWrapper* raw_file,
              unsigned long long inode,
              uid_t uid, gid_t gid, pid_t pid,
              bool isROfd,
              const char* path = "",
              bool mknod = false);

  //----------------------------------------------------------------------------
  //! Force pending rw open to happen (in case of lazy open)
  //----------------------------------------------------------------------------
  int
  force_rwopen(unsigned long long inode, uid_t uid, gid_t gid, pid_t pid);

  //----------------------------------------------------------------------------
  //! Get the file abstraction object corresponding to the fd
  //----------------------------------------------------------------------------
  std::shared_ptr<FileAbstraction>
  get_file(int fd, bool* isRW = NULL, bool forceRWtoo = false);


  //----------------------------------------------------------------------------
  //! Remove file descriptor from mapping
  //----------------------------------------------------------------------------
  int remove_fd2file(int fd, unsigned long long inode, uid_t uid, gid_t gid,
                     pid_t pid);

  int
  get_open_idx(const unsigned long long& inode);

  //----------------------------------------------------------------------------
  //              ******* FUSE Directory Cache *******
  //----------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  //!
  //! Get a cached directory
  //!
  //! @param inode inode value of the directory to be cached
  //! @param mtime modification time
  //! @param ctime modification time
  //! @param b dirbuf structure
  //!
  //! @return true if found, otherwise false
  //!
  //----------------------------------------------------------------------------
  int dir_cache_get(unsigned long long inode,
                    struct timespec mtime,
                    struct timespec ctime,
                    struct dirbuf** b);

  //----------------------------------------------------------------------------
  //!
  //! Forget a cached directory
  //!
  //! @param inode inode value of the directory to be forgotten
  //! @return true if found, otherwise false
  //!
  //----------------------------------------------------------------------------
  int dir_cache_forget(unsigned long long inode);

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
  int dir_cache_get_entry(fuse_req_t req,
                          unsigned long long inode,
                          unsigned long long einode,
                          const char* ifullpath,
                          struct stat* overwrite_stat = 0);

  //----------------------------------------------------------------------------
  //! Add new subentry to a cached directory
  //!
  //! @param inode directory inode
  //! @param entry_inode subentry inode
  //! @param e fuse_entry_param structure
  //!
  //----------------------------------------------------------------------------
  void dir_cache_add_entry(unsigned long long inode,
                           unsigned long long entry_inode,
                           struct fuse_entry_param* e);

  //----------------------------------------------------------------------------
  //! Add or update a cache directory entry
  //!
  //! @param inode directory inode value
  //! @param nentries number of entries in the current directory
  //! @param mtime modifcation time
  //! @param b dirbuf structure
  //!
  //----------------------------------------------------------------------------
  void dir_cache_sync(unsigned long long inode,
                      int nentries,
                      struct timespec mtime,
                      struct timespec ctime,
                      struct dirbuf* b,
                      long lifetimens);

  //----------------------------------------------------------------------------
  //! Update stat information of an entry
  //!
  //! @param entry_inode
  //! @param buf stat info
  //----------------------------------------------------------------------------
  bool dir_cache_update_entry(unsigned long long entry_inode,
                              struct stat* buf);


  //----------------------------------------------------------------------------
  //              ******* XROOT interfacing ********
  //----------------------------------------------------------------------------

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int stat(const char* path, struct stat* buf, uid_t uid, gid_t gid, pid_t pid,
           unsigned long long inode, bool onlysizemtime = false);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int statfs(const char* path, struct statvfs* stbuf, uid_t uid, gid_t gid,
             pid_t pid);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int getxattr(const char* path, const char* xattr_name, char** xattr_value,
               size_t* size, uid_t uid, gid_t gid, pid_t pid);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int listxattr(const char* path, char** xattr_list, size_t* size, uid_t uid,
                gid_t gid, pid_t pid);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int setxattr(const char* path, const char* xattr_name, const char* xattr_value,
               size_t size, uid_t uid, gid_t gid, pid_t pid);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int rmxattr(const char* path, const char* xattr_name, uid_t uid, gid_t gid,
              pid_t pid);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  struct dirent* readdir(const char* path_dir, size_t* size, uid_t uid,
                         gid_t gid, pid_t pid);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int mkdir(const char* path, mode_t mode, uid_t uid, gid_t gid, pid_t pid,
            struct stat* buf);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int rmdir(const char* path, uid_t uid, gid_t gid, pid_t pid);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int open(const char* pathname, int flags, mode_t mode, uid_t uid, gid_t gid,
           pid_t pid, unsigned long long* return_inode, bool mknod = false);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int truncate(int fildes, off_t offset);
  int truncate2(const char* fullpath, unsigned long long inode,
                unsigned long truncsize, uid_t uid, gid_t gid, pid_t pid);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  ssize_t pread(int fildes, void* buf, size_t nbyte, off_t offset);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int utimes_from_fabst(std::shared_ptr<FileAbstraction> fabst,
                        unsigned long long inode, uid_t uid, gid_t gid, pid_t pid);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int close(int fildes, unsigned long long inode, uid_t uid, gid_t gid, pid_t pid);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int flush(int fd, uid_t uid, gid_t gid, pid_t pid);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  ssize_t pwrite(int fildes, const void* buf, size_t nbyte, off_t offset);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int fsync(int fildes);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int unlink(const char* path, uid_t uid, gid_t gid, pid_t pid,
             unsigned long long inode);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int rename(const char* oldpath, const char* newpath, uid_t uid, gid_t gid,
             pid_t pid);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int chmod(const char* path, mode_t mode, uid_t uid, gid_t gid, pid_t pid);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int symlink(const char* path, const char* newpath, uid_t uid, gid_t gid,
              pid_t pid);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int readlink(const char* path, char* buf, size_t bufsize, uid_t uid,
               gid_t gid, pid_t pid);


  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int access(const char* path, int mode, uid_t uid, gid_t gid, pid_t pid);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int utimes_if_open(unsigned long long inode, struct timespec* tvp,
                     uid_t uid, gid_t gid, pid_t pid);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int utimes(const char* path, struct timespec* tvp, uid_t uid, gid_t gid,
             pid_t pid);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int inodirlist(unsigned long long dirinode, const char* path, uid_t uid,
                 gid_t gid, pid_t pid, dirlist& dlist,
                 struct fuse_entry_param** stats, size_t* nstats);

  //----------------------------------------------------------------------------
  //! Do user mapping
  //----------------------------------------------------------------------------
  const char* mapuser(uid_t uid, gid_t gid, pid_t pid, uint8_t authid);

  //----------------------------------------------------------------------------
  //! updates the proccache entry for the given pid (only if needed)
  //! the proccache entry contains the environment, the command line
  //! the fsuid, the fsgid amd if kerberos is used the krb5ccname and the krb5login
  //! used in it
  //----------------------------------------------------------------------------
  int update_proc_cache(uid_t uid, gid_t gid, pid_t pid);

  //----------------------------------------------------------------------------
  //! Create the cgi argument to be added to the url to use the kerberos cc file
  //!   for the given pid. e.g. xrd.k5ccname=<krb5login>
  //----------------------------------------------------------------------------
  std::string strongauth_cgi(uid_t uid, gid_t gid, pid_t pid);

  //----------------------------------------------------------------------------
  //! Create an URL
  //! - with a user private physical channel e.g. root://<uid-gid>@<host> if
  //! krb5 is not used
  //! - with kerberos authentication if used e.g. root://<krb5login>@<host>
  //----------------------------------------------------------------------------
  std::string user_url(uid_t uid, gid_t gid, pid_t pid);

  //----------------------------------------------------------------------------
  //! Return the CGI of an URL
  //----------------------------------------------------------------------------
  const char* get_cgi(const char* url);

  //----------------------------------------------------------------------------
  //! Check if rm command was issued on one of the top level directories
  //!
  //! @param pid process id
  //! @param local_dir local mount point directory
  //!
  //! @return 0 if false, otherwise 1
  //----------------------------------------------------------------------------
  int is_toplevel_rm(pid_t pid, const char* local_dir);

  //----------------------------------------------------------------------------
  //! Get the configured overlay mode
  mode_t get_mode_overlay()
  {
    return mode_overlay;
  }

  //----------------------------------------------------------------------------
  //! Initialisation function
  //----------------------------------------------------------------------------
  bool initlogging();
  bool init(int argc, char* argv[], void* userdata,
            std::map<std::string, std::string>* features);
  bool check_mgm(std::map<std::string, std::string>* features);

  void log(const char* level, const char* msg);
  void log_settings();

  typedef struct _meta {

    _meta()
    {
      openr = openw = 0;
      mInUse.SetBlockedStackTracing(false);
    }

    RWMutexR mInUse;
    XrdSysMutex mlocker;
    size_t openr;
    size_t openw;
  } meta_t;

  class Track
  {
  public:

    Track() { }

    ~Track() { }

    void
    assure(unsigned long long ino)
    {
      XrdSysMutexHelper l(iMutex);
      iNodes[ino] = std::make_shared<meta_t>();
    }

    void
    forget(unsigned long long ino)
    {
      XrdSysMutexHelper l(iMutex);
      iNodes.erase(ino);
    }

    std::shared_ptr<meta_t>
    Attach(unsigned long long ino, bool exclusive = false)
    {
      std::shared_ptr<meta_t> m;
      {
        XrdSysMutexHelper l(iMutex);

        if (!iNodes.count(ino)) {
          iNodes[ino] = std::make_shared<meta_t>();
        }

        m = iNodes[ino];
      }

      if (exclusive) {
        m->mInUse.LockWrite();
      } else {
        m->mInUse.LockRead();
      }

      return m;
    }

    void
    Detach(std::shared_ptr<meta_t> m)
    {
      m->mInUse.UnLockRead();
    }

    class Monitor
    {
    public:

      Monitor(const char* caller, Track& tracker, unsigned long long ino,
              bool exclusive = false)
      {
        eos_static_debug("trylock caller=%s self=%lld in=%llu exclusive=%d", caller,
                         thread_id(), ino, exclusive);
        this->me = tracker.Attach(ino, exclusive);
        this->ino = ino;
        this->caller = caller;
        this->exclusive = exclusive;
        eos_static_debug("locked  caller=%s self=%lld in=%llu exclusive=%d obj=%llx",
                         caller, thread_id(), ino, exclusive,
                         &(*(this->me)));
      }

      ~Monitor()
      {
        eos_static_debug("unlock  caller=%s self=%lld in=%llu exclusive=%d", caller,
                         thread_id(), ino, exclusive);

        if (exclusive) {
          me->mInUse.UnLockWrite();
        } else {
          me->mInUse.UnLockRead();
        }

        eos_static_debug("unlocked  caller=%s self=%lld in=%llu exclusive=%d", caller,
                         thread_id(), ino, exclusive);
      }
    private:
      std::shared_ptr<meta_t> me;
      bool exclusive;
      unsigned long long ino;
      const char* caller;
    };

  private:
    XrdSysMutex iMutex;
    std::map<unsigned long long, std::shared_ptr<meta_t> > iNodes;
  };

  Track iTrack;

  void setPrefix(std::string& prefix)
  {
    mPrefix = prefix;
  }

  bool getInlineRepair() const
  {
    return inline_repair;
  }

  uint64_t getMaxInlineRepairSize() const
  {
    return max_inline_repair_size;
  }

  pthread_t tCacheCleanup;

private:
  uint64_t pid_max;
  uint64_t uid_max;

  //! Indicates if mapping between pid and strong authentication is symlinked in
  //! /var/run/eosd/credentials/pidXXX
  bool link_pidmap;
  //! Stores all configuration related to credential handling
  CredentialConfig credConfig;
  //! Indicates if lazy openning of the file should be used for files open in RO
  bool lazy_open_ro;
  //! Indicates if lazy openning of the file should be used for files open in RW
  bool lazy_open_rw;
  //! Indicates if async open should be used (this used only in coordination
  //! with lazy_open)
  bool async_open;
  //! Indicates if lazy openning is disabled because the server does not support it
  bool lazy_open_disabled;
  //! Indicate if we should try to repair broken files for wrinting inlined
  //! in the open
  bool inline_repair;
  off_t max_inline_repair_size; ///< Define maximum inline repair size
  bool do_rdahead; ///< true if readahead is to be enabled, otherwise false
  std::string rdahead_window; ///< size of the readahead window
  int rm_level_protect; ///< number of levels in hierarchy protected against rm -rf
  //! Full path of the system rm command (e.g. "/bin/rm" )
  std::string rm_command;
  //! Indicates if the system rm command changes it CWD making relative path
  //! expansion impossible
  bool rm_watch_relpath;
  int fuse_cache_write; ///< 0 if fuse cache write disabled, otherwise 1
  bool fuse_exec; ///< indicates if files should be make exectuble
  bool fuse_shared; ///< Indicates if this is eosd = true or eosfsd = false
  //! Time period where files are considered owned locally e.g. remote
  //! modifications are not reflected locally.
  int creator_cap_lifetime;
  //! Max temporary write-back cache per file size in bytes
  int file_write_back_cache_size;
  bool encode_pathname; ///< Indicates if filename should be encoded
  //! Indicate if we show atomic entries, version, backup files etc.
  bool hide_special_files;
  //! Show all sys.* and emulated user.eos attributes when listing xattributes
  bool show_eos_attributes;
  mode_t mode_overlay; ///< mask which is or'ed into the retrieved mode
  //! Maximum size of in-memory wb cache structures
  uint64_t max_wb_in_memory_size;
  //! host name of the FUSE contact point
  XrdOucString gMgmHost;

  //----------------------------------------------------------------------------
  //             ******* Implementation Translations *******
  //----------------------------------------------------------------------------
  //! Protecting the path/inode translation table
  eos::common::RWMutexR mutex_inode_path;

  //! Mapping path name to inode
  google::dense_hash_map<std::string, unsigned long long> path2inode;

  //! Mapping inode to path name
  std::map<unsigned long long, std::string> inode2path;

  std::map<unsigned long long, struct timespec> inode2mtime;
  std::map<unsigned long long, struct timespec> inode2mtime_open;

  //! Prefix (duplicated from upstream object)
  std::string mPrefix;

  //------------------------------------------------------------------------------
  //      ******* Implementation of the directory listing table *******
  //------------------------------------------------------------------------------
  //! Protecting the directory listing table
  eos::common::RWMutexR mutex_dir2inodelist;

  //! Dir listing map
  std::map<unsigned long long, std::vector<unsigned long long> > dir2inodelist;
  std::map<unsigned long long, struct dirbuf> dir2dirbuf;

  //------------------------------------------------------------------------------
  //      ******* Implementation of the FUSE directory cache *******
  //------------------------------------------------------------------------------

  //------------------------------------------------------------------------------
  // Get maximum number of directories in cache
  //------------------------------------------------------------------------------
  const unsigned long long
  GetMaxCacheSize()
  {
    return 1024;
  }

  //! Protecting the cache entry map
  eos::common::RWMutexR mutex_fuse_cache;

  //! Directory cache
  std::map<unsigned long long, FuseCacheEntry*> inode2cache;

  //! Parent cache
  std::map<unsigned long long, unsigned long long> inode2parent;

  //------------------------------------------------------------------------------
  //      ******* Implementation of the open File Descriptor map *******
  //------------------------------------------------------------------------------

  //! Map used for associating file descriptors with XrdCl::File objects
  eos::common::RWMutexR rwmutex_fd2fabst;
  google::dense_hash_map<int, shared_ptr<FileAbstraction>> fd2fabst;

  //! Counting write open of inodes
  eos::common::RWMutexR rwmutex_inodeopenw;
  std::map<unsigned long long, int> inodeopenw;

  //! The count is >0 for RW and <0 for RO
  google::dense_hash_map<int, int> fd2count;
  eos::common::RWMutexR openmutexes[N_OPEN_MUTEXES];

  //! Map <inode, user> to a set of file descriptors - used only in the stat
  //! method  note : the set of file descriptors for one key point to the
  //! same fabst
  google::dense_hash_map<std::string, std::set<int> > inodexrdlogin2fds;

  //! Helper function to construct a key in the previous map
  std::string
  get_login(uid_t uid, gid_t gid, pid_t pid);

  //! Pool of available file descriptors
  int base_fd;
  std::queue<int> pool_fd;

  //! WB Cache Cleanup Thread
  static void* CacheCleanup(void* pp);

  //------------------------------------------------------------------------------
  //        ******* Implementation IO Buffer Management *******
  //------------------------------------------------------------------------------

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
    IoBuf()
    {
      buffer = 0;
      size = 0;
    }

    //----------------------------------------------------------------------------
    //! Destructor
    //----------------------------------------------------------------------------
    virtual
    ~IoBuf()
    {
      if (buffer && size) {
        free(buffer);
      }
    }

    //----------------------------------------------------------------------------
    //! Get buffer
    //----------------------------------------------------------------------------
    char*
    GetBuffer()
    {
      return (char*) buffer;
    }

    //----------------------------------------------------------------------------
    //! Get size of buffer
    //----------------------------------------------------------------------------
    size_t
    GetSize()
    {
      return size;
    }

    //----------------------------------------------------------------------------
    //! Resize buffer
    //----------------------------------------------------------------------------
    void
    Resize(size_t newsize)
    {
      if (newsize > size) {
        size = (newsize < (128 * 1024)) ? 128 * 1024 : newsize;
        char* new_buffer = (char*)realloc(buffer, size);

        if (new_buffer == nullptr) {
          std::abort();
        }

        buffer = new_buffer;
      }
    }
  };

  //! Protecting the IO buffer map
  XrdSysMutex IoBufferLock;

  //! IO buffer table. Each fuse thread has its own read buffer
  std::map<pthread_t, IoBuf> IoBufferMap;
  AuthIdManager authidmanager;

  //------------------------------------------------------------------------------
  // Cache that holds the mapping from a pid to a time stamp (to see if the cache needs
  // to be refreshed and bool to check if the operation needs to be denied.
  // variable which is true if this is a top level rm operation and false other-
  // wise. It is used by recursive rm commands which belong to the same pid in
  // order to decide if his operation is denied or not.
  //------------------------------------------------------------------------------
  eos::common::RWMutexR mMapPidDenyRmMutex;
  std::map<pid_t, std::pair<time_t, bool> > mMapPidDenyRm;

  FuseWriteCache* XFC;

  int
  mylstat(const char* __restrict name, struct stat* __restrict __buf, pid_t pid);

  char*
  myrealpath(const char* __restrict path, char* __restrict resolved, pid_t pid);

  bool get_features(const std::string& url,
                    std::map<std::string, std::string>* features);

  std::string mount_dir;
};
#endif
