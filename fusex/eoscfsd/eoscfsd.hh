//! @file eoscfsd.hh
//! @author Andreas-Joachim Peters CERN
//! @brief EOS ClientFS C++ Fuse low-level implementation
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#pragma once
#define FUSE_USE_VERSION 35
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

// C includes
#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <ftw.h>
#include <fuse3/fuse_lowlevel.h>
#include <inttypes.h>
#include <string.h>
#include <sys/file.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <sys/xattr.h>
#include <sys/mount.h>
#include <sys/fsuid.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <limits.h>
#include <sys/ptrace.h>
// C++ includes
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <list>
#include <mutex>
#include <fstream>
#include <thread>
#include <iomanip>
#include <atomic>
#include <deque>
#include "cfslogin.hh"
#include "cfsquota.hh"
#include "cfsrecycle.hh"
#include "cfsvattr.hh"
#include "cfskey.hh"
#include "stat/Stat.hh"
#include "common/AssistedThread.hh"
#include "common/LRU.hh"
using namespace std;

/*----------------------------------------------------------------------------*/

/* We are re-using pointers to our `struct sfs_inode` and `struct
   sfs_dirp` elements as inodes and file handles. This means that we
   must be able to store pointer a pointer in both a fuse_ino_t
   variable and a uint64_t variable (used for file handles). */
static_assert(sizeof(fuse_ino_t) >= sizeof(void*),
              "void* must fit into fuse_ino_t");
static_assert(sizeof(fuse_ino_t) >= sizeof(uint64_t),
              "fuse_ino_t must be at least 64 bits");


/* Forward declarations */
struct Inode;
static Inode& get_inode(fuse_ino_t ino);
static void forget_one(fuse_ino_t ino, uint64_t n);

// Uniquely identifies a file in the source directory tree. This could
// be simplified to just ino_t since we require the source directory
// not to contain any mountpoints. This hasn't been done yet in case
// we need to reconsider this constraint (but relaxing this would have
// the drawback that we can no longer re-use inode numbers, and thus
// readdir() would need to do a full lookup() in order to report the
// right inode number).
typedef std::pair<ino_t, dev_t> SrcId;

// Define a hash function for SrcId
namespace std
{
template<>
struct hash<SrcId> {
  size_t operator()(const SrcId& id) const
  {
    return hash<ino_t> {}(id.first) ^ hash<dev_t> {}(id.second);
  }
};
}

// Maps files in the source directory tree to inodes
typedef std::unordered_map<SrcId, Inode> InodeMap;

typedef struct forgetentry {
  forgetentry()
  {
    parent = 0;
    tst = 0;
  }
  forgetentry(ino_t p, std::string n) : parent(p), name(n)
  {
    tst = time(NULL);
  };
  ino_t parent;
  std::string name;
  time_t tst;
} forgetentry_t;

typedef std::deque<shared_ptr<forgetentry_t>> ForgetQueue;
typedef  std::pair<uid_t, gid_t> userid_t;
typedef std::map<int, userid_t> OpenFds;


struct Inode {
  int fd {-1};
  dev_t src_dev {0};
  ino_t src_ino {0};
  int generation {0};
  uint64_t nopen {0};
  uint64_t nlookup {0};
  std::mutex m;

  // Delete copy constructor and assignments. We could implement
  // move if we need it.
  Inode() = default;
  Inode(const Inode&) = delete;
  Inode(Inode&& inode) = delete;
  Inode& operator=(Inode&& inode) = delete;
  Inode& operator=(const Inode&) = delete;

  ~Inode()
  {
    if (fd > 0) {
      close(fd);
    }
  }
};

class Fs
{
public:
  void LevelFDs(ThreadAssistant& assistant);

  Fs();
  virtual ~Fs();
  void Run();
  // Must be acquired *after* any Inode.m locks.
  std::mutex mutex;
  InodeMap inodes; // protected by mutex
  ForgetQueue forgetq;
  std::atomic<size_t> forgetq_size;

  std::mutex openFdsMutex;
  OpenFds openFds;

  Inode root;
  double timeout;
  bool debug;
  bool recyclebin;
  std::string source;
  std::string name;
  std::string mount;
  std::string logpath;
  std::string jsonpath;
  std::string keyresource;
  std::string keyfile;
  time_t starttime;
  size_t blocksize;
  dev_t src_dev;
  bool nosplice;
  bool nocache;
  bool dropcache;
  bool foreground;
  size_t idletime;
  cfsquota quota;
  cfsrecycle recycle;
  std::string k5domain;
  FILE* fstderr;

  Stat& getFuseStat()
  {
    return fusestat;
  }

  struct fuse_session* se;
private:
  AssistedThread tFdLeveler;
  AssistedThread tDumpStatistic;
  AssistedThread tStatCirculate;
  std::string umount_system_line;
  void DumpStatistic(ThreadAssistant& assistant);
  void StatCirculate(ThreadAssistant& assistant);
  Stat fusestat;
};

static Fs fs{};


#define FUSE_BUF_COPY_FLAGS                      \
        (fs.nosplice ?                           \
            FUSE_BUF_NO_SPLICE :                 \
            static_cast<fuse_buf_copy_flags>(0))


static Inode& get_inode(fuse_ino_t ino)
{
  if (ino == FUSE_ROOT_ID) {
    return fs.root;
  }

  Inode* inode = reinterpret_cast<Inode*>(ino);

  if (inode->fd == -1) {
    cerr << "INTERNAL ERROR: Unknown inode " << ino << endl;
    abort();
  }

  return *inode;
}


static int get_fs_fd(fuse_ino_t ino)
{
  int fd = get_inode(ino).fd;
  return fd;
}


struct DirHandle {
  DIR* dp {nullptr};
  off_t offset;

  DirHandle() = default;
  DirHandle(const DirHandle&) = delete;
  DirHandle& operator=(const DirHandle&) = delete;

  ~DirHandle()
  {
    if (dp) {
      closedir(dp);
    }
  }
};

static void print_usage(char* prog_name)
{
  cout << "Usage: " << prog_name << " --help\n"
       << "       " << prog_name << " [options] <mountpoint> [<name>]\n";
  cout << "options:\n";
  cout << "         -d    --debug       Enable filesystem debug messages\n";
  cout << "               --debug-fuse  Enable libfuse debug messages\n";
  cout << "         -h    --help        Print help\n";
  cout << "               --nosplice    Do not use splice(2) to transfer data\n";
  cout << "         -s    --single      Run single-threaded\n";
  cout << "         -f    --foreground  Run in foreground\n";
  cout << "         -r    --recycle     Run with recycling bin\n";
  cout << "         -e    --embedded    Use an embedded key\n";
}

static std::set<std::string> parse_options(int argc, char** argv)
{
  std::set<std::string> options;
  std::string mountpath = "";
  std::string mountname = "";

  for (int i = 1 ; i < argc; ++i) {
    std::string args = argv[i];

    if (args == std::string("-o")) {
      i++;
      continue;
    } else {
      if (args.substr(0, 2) == std::string("-o")) {
        continue;
      }
    }

    if (args.substr(0, 1) == "-") {
      if ((args == "-h") || (args == "--help")) {
        print_usage(argv[0]);
        exit(0);
      }

      if ((args == "--debug") || (args == "-d")) {
        options.insert("debug");
      } else if ((args == "--debug-fuse")) {
        options.insert("debug-fuse");
      } else if ((args == "--nosplice")) {
        options.insert("nosplice");
      } else if ((args == "--single") || (args == "-s")) {
        options.insert("single");
      } else if ((args == "-f") || (args == "--foreground")) {
        options.insert("foreground");
      } else if ((args == "-r") || (args == "--recycle")) {
        options.insert("recycle");
      } else if ((args == "-e") || (args == "--embedded")) {
        options.insert("embedded");
      } else {
        print_usage(argv[0]);
        exit(0);
      }
    } else {
      if (mountpath.empty()) {
        mountpath = args;
      } else {
        if (mountname.empty()) {
          mountname = args;
        } else {
          print_usage(argv[0]);
          exit(-1);
        }
      }
    }
  }

  if (mountpath.empty()) {
    print_usage(argv[0]);
    exit(-1);
  }

  fs.debug = options.count("debug") != 0;
  fs.nosplice = options.count("nosplice") != 0;
  fs.recyclebin = options.count("recycle") != 0;
  fs.mount = mountpath;
  fs.name = mountname;
  fs.foreground = options.count("foreground") != 0;

  if (options.count("embedded")) {
    fs.keyresource = "";
  }

  return options;
}


static void maximize_fd_limit()
{
  struct rlimit lim {};
  auto res = getrlimit(RLIMIT_NOFILE, &lim);

  if (res != 0) {
    warn("WARNING: getrlimit() failed with");
    return;
  }

  lim.rlim_cur = lim.rlim_max;
  res = setrlimit(RLIMIT_NOFILE, &lim);

  if (res != 0) {
    warn("WARNING: setrlimit() failed with");
  }
}

static void maximize_priority()
{
  if (setpriority(PRIO_PROCESS, getpid(), -PRIO_MAX / 2) < 0) {
    fprintf(stderr,
            "error: failed to renice this process '%u', to maximum priority '%d'\n",
            getpid(), -PRIO_MAX / 2);
  }
}


class FsID
{
public:
  FsID(fuse_req_t req)
  {
    //fprintf(stderr,"username=%s exec=%s\n", cfslogin::name(req).c_str(), cfslogin::executable(req).c_str());
    uid_t myuid = 99;
    gid_t mygid = 99;
    name = cfslogin::translate(req, myuid, mygid);
    setfsuid(myuid);
    setfsgid(mygid);
    uid = myuid;
    gid = mygid;
  }
  FsID(uid_t u, gid_t g) : uid(u), gid(g)
  {
    setfsuid(uid);
    setfsgid(gid);
  }
  virtual ~FsID()
  {
    setfsuid(0);
    setfsuid(0);
  }

  uid_t getUid() const
  {
    return uid;
  }
  gid_t getGid() const
  {
    return gid;
  }
  std::string getName() const
  {
    return name;
  }

private:
  uid_t uid;
  gid_t gid;
  std::string name;
};

