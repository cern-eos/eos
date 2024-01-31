//! @file eoscfsd.cc
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
 ***********************************************************************/

#include "eoscfsd.hh"
#include "obfuscate.hh"
#include "common/Untraceable.hh"
#include "common/Path.hh"
#include "common/StacktraceHere.hh"
#include "common/AssistedThread.hh"
#include "common/Logging.hh"
#include "common/Timing.hh"
#include "common/LinuxMemConsumption.hh"
#include "common/LinuxTotalMem.hh"
#include "common/LinuxStat.hh"
#include <stdio.h>
#include <json/json.h>

using namespace std;



static void
/* -------------------------------------------------------------------------- */
umount()
/* -------------------------------------------------------------------------- */
{
  static char systemline[4096];
  snprintf(systemline, sizeof(systemline),
           "umount -fl %s >& /dev/null; fusermount -u -z %s >& /dev/null",
           fs.source.c_str(),
           fs.mount.c_str()
          );
  system(systemline);
  fprintf(stderr, "# cleanup: old mounts\n");
}

static void sfs_init(void* userdata, fuse_conn_info* conn)
{
  (void)userdata;

  if (conn->capable & FUSE_CAP_EXPORT_SUPPORT) {
    conn->want |= FUSE_CAP_EXPORT_SUPPORT;
  }

  if (fs.timeout && conn->capable & FUSE_CAP_WRITEBACK_CACHE) {
    conn->want |= FUSE_CAP_WRITEBACK_CACHE;
  }

  if (conn->capable & FUSE_CAP_FLOCK_LOCKS) {
    conn->want |= FUSE_CAP_FLOCK_LOCKS;
  }

  if (fs.nosplice) {
    // FUSE_CAP_SPLICE_READ is enabled in libfuse3 by default,
    // see do_init() in in fuse_lowlevel.c
    // Just unset both, in case FUSE_CAP_SPLICE_WRITE would also get enabled
    // by detault.
    conn->want &= ~FUSE_CAP_SPLICE_READ;
    conn->want &= ~FUSE_CAP_SPLICE_WRITE;
  } else {
    if (conn->capable & FUSE_CAP_SPLICE_WRITE) {
      conn->want |= FUSE_CAP_SPLICE_WRITE;
    }

    if (conn->capable & FUSE_CAP_SPLICE_READ) {
      conn->want |= FUSE_CAP_SPLICE_READ;
    }
  }
}


static void sfs_getattr(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi)
{
  eos::common::Timing timing("getattr");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("getattr");
  ADD_CFSD_STAT("getattr", req);
  //  FsID fsid(req);
  (void)fi;
  Inode& inode = get_inode(ino);
  struct stat attr;
  auto res = fstatat(inode.fd, "", &attr,
                     AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW);

  if (res == -1) {
    fuse_reply_err(req, errno);
    return;
  }

  fuse_reply_attr(req, &attr, fs.timeout);
  CFSD_TIMING_END("getattr");
  COMMONTIMING("_stop_", &timing);
}


static void do_setattr(fuse_req_t req, fuse_ino_t ino, struct stat* attr,
                       int valid, struct fuse_file_info* fi)
{
  FsID fsid(req);
  Inode& inode = get_inode(ino);
  int ifd = inode.fd;
  int res;

  if (valid & FUSE_SET_ATTR_MODE) {
    if (fi) {
      res = fchmod(fi->fh, attr->st_mode);
    } else {
      char procname[64];
      sprintf(procname, "/proc/self/fd/%i", ifd);
      res = chmod(procname, attr->st_mode);
    }

    if (res == -1) {
      goto out_err;
    }
  }

  if (valid & (FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID)) {
    uid_t uid = (valid & FUSE_SET_ATTR_UID) ? attr->st_uid : static_cast<uid_t>(-1);
    gid_t gid = (valid & FUSE_SET_ATTR_GID) ? attr->st_gid : static_cast<gid_t>(-1);
    res = fchownat(ifd, "", uid, gid, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW);

    if (res == -1) {
      goto out_err;
    }
  }

  if (valid & FUSE_SET_ATTR_SIZE) {
    if (fi) {
      res = ftruncate(fi->fh, attr->st_size);
    } else {
      char procname[64];
      sprintf(procname, "/proc/self/fd/%i", ifd);
      res = truncate(procname, attr->st_size);
    }

    if (res == -1) {
      goto out_err;
    }
  }

  if (valid & (FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME)) {
    struct timespec tv[2];
    tv[0].tv_sec = 0;
    tv[1].tv_sec = 0;
    tv[0].tv_nsec = UTIME_OMIT;
    tv[1].tv_nsec = UTIME_OMIT;

    if (valid & FUSE_SET_ATTR_ATIME_NOW) {
      tv[0].tv_nsec = UTIME_NOW;
    } else if (valid & FUSE_SET_ATTR_ATIME) {
      tv[0] = attr->st_atim;
    }

    if (valid & FUSE_SET_ATTR_MTIME_NOW) {
      tv[1].tv_nsec = UTIME_NOW;
    } else if (valid & FUSE_SET_ATTR_MTIME) {
      tv[1] = attr->st_mtim;
    }

    if (fi) {
      res = futimens(fi->fh, tv);
    } else {
      char procname[64];
      sprintf(procname, "/proc/self/fd/%i", ifd);
      res = utimensat(AT_FDCWD, procname, tv, 0);
    }

    if (res == -1) {
      goto out_err;
    }
  }

  return sfs_getattr(req, ino, fi);
out_err:
  fuse_reply_err(req, errno);
}


static void sfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat* attr,
                        int valid, fuse_file_info* fi)
{
  eos::common::Timing timing("setattr");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("setattr");
  ADD_CFSD_STAT("setattr", req);
  (void) ino;
  do_setattr(req, ino, attr, valid, fi);
  CFSD_TIMING_END("setattr");
  COMMONTIMING("_stop_", &timing);
}


static int do_lookup(fuse_ino_t parent, const char* name,
                     fuse_entry_param* e)
{
  if (fs.debug)
    cerr << "DEBUG: lookup(): name=" << name
         << ", parent=" << parent << endl;

  memset(e, 0, sizeof(*e));
  e->attr_timeout = fs.timeout;
  e->entry_timeout = fs.timeout;
  auto newfd = openat(get_fs_fd(parent), name, O_PATH | O_NOFOLLOW);

  if (newfd == -1) {
    return errno;
  }

  auto res = fstatat(newfd, "", &e->attr, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW);

  if (res == -1) {
    auto saveerr = errno;
    close(newfd);

    if (fs.debug) {
      cerr << "DEBUG: lookup(): fstatat failed" << endl;
    }

    return saveerr;
  }

  if (0 && (e->attr.st_dev != fs.src_dev)) {
    cerr << "WARNING: Mountpoints in the source directory tree will be hidden." <<
         endl;
    return ENOTSUP;
  } else if (e->attr.st_ino == FUSE_ROOT_ID) {
    cerr << "ERROR: Source directory tree must not include inode "
         << FUSE_ROOT_ID << endl;
    return EIO;
  }

  SrcId id {e->attr.st_ino, e->attr.st_dev};
  unique_lock<mutex> fs_lock {fs.mutex};
  Inode* inode_p;

  try {
    inode_p = &fs.inodes[id];
  } catch (std::bad_alloc&) {
    return ENOMEM;
  }

  e->ino = reinterpret_cast<fuse_ino_t>(inode_p);
  Inode& inode {*inode_p};
  e->generation = inode.generation;

  if (inode.fd == -ENOENT) { // found unlinked inode
    if (fs.debug)
      cerr << "DEBUG: lookup(): inode " << e->attr.st_ino
           << " recycled; generation=" << inode.generation << endl;

    /* fallthrough to new inode but keep existing inode.nlookup */
  }

  if (inode.fd > 0) { // found existing inode
    fs_lock.unlock();

    if (fs.debug)
      cerr << "DEBUG: lookup(): inode " << e->attr.st_ino
           << " (userspace) already known; fd = " << inode.fd << endl;

    if (strcmp(name, ".")) {
      lock_guard<mutex> g {inode.m};
      inode.nlookup++;

      if (fs.debug)
        cerr << "DEBUG:" << __func__ << ":" << __LINE__ << " "
             <<  "inode " << inode.src_ino
             << " count " << inode.nlookup << endl;
    }

    close(newfd);
  } else { // no existing inode
    /* This is just here to make Helgrind happy. It violates the
       lock ordering requirement (inode.m must be acquired before
       fs.mutex), but this is of no consequence because at this
       point no other thread has access to the inode mutex */
    unique_lock<mutex> g {inode.m};
    inode.src_ino = e->attr.st_ino;
    inode.src_dev = e->attr.st_dev;
    inode.nlookup++;

    if (fs.debug)
      cerr << "DEBUG:" << __func__ << ":" << __LINE__ << " "
           <<  "inode " << inode.src_ino
           << " count " << inode.nlookup << endl;

    inode.fd = newfd;

    if ((S_ISDIR(e->attr.st_mode))) {
      std::shared_ptr fe = std::make_shared<forgetentry_t>(parent, name);
      fs.forgetq.push_back(fe);
      fs.forgetq_size++;
      fs_lock.unlock();
    }

    if (fs.debug)
      cerr << "DEBUG: lookup(): created userspace inode " << e->attr.st_ino
           << "; fd = " << inode.fd << endl;
  }

  return 0;
}


static void sfs_lookup(fuse_req_t req, fuse_ino_t parent, const char* name)
{
  eos::common::Timing timing("lookup");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("lookup");
  ADD_CFSD_STAT("lookup", req);
  fuse_entry_param e {};
  auto err = do_lookup(parent, name, &e);

  if (err == ENOENT) {
    e.attr_timeout = fs.timeout;
    e.entry_timeout = fs.timeout;
    e.ino = e.attr.st_ino = 0;
    fuse_reply_entry(req, &e);
  } else if (err) {
    if (err == ENFILE || err == EMFILE) {
      cerr << "ERROR: Reached maximum number of file descriptors." << endl;
    }

    fuse_reply_err(req, err);
  } else {
    fuse_reply_entry(req, &e);
  }

  CFSD_TIMING_END("lookup");
  COMMONTIMING("_stop_", &timing);
}


static void mknod_symlink(fuse_req_t req, fuse_ino_t parent,
                          const char* name, mode_t mode, dev_t rdev,
                          const char* link)
{
  FsID fsid(req);
  int res;
  Inode& inode_p = get_inode(parent);
  auto saverr = ENOMEM;

  if (S_ISDIR(mode)) {
    res = mkdirat(inode_p.fd, name, mode);
  } else if (S_ISLNK(mode)) {
    res = symlinkat(link, inode_p.fd, name);
  } else {
    res = mknodat(inode_p.fd, name, mode, rdev);
  }

  saverr = errno;

  if (res == -1) {
    goto out;
  }

  fuse_entry_param e;
  saverr = do_lookup(parent, name, &e);

  if (saverr) {
    goto out;
  }

  fuse_reply_entry(req, &e);
  return;
out:

  if (saverr == ENFILE || saverr == EMFILE) {
    cerr << "ERROR: Reached maximum number of file descriptors." << endl;
  }

  fuse_reply_err(req, saverr);
}


static void sfs_mknod(fuse_req_t req, fuse_ino_t parent, const char* name,
                      mode_t mode, dev_t rdev)
{
  eos::common::Timing timing("mknod");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("mknod");
  ADD_CFSD_STAT("mknod", req);
  mknod_symlink(req, parent, name, mode, rdev, nullptr);
  CFSD_TIMING_END("mknod");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char* name,
                      mode_t mode)
{
  eos::common::Timing timing("mkdir");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("mkdir");
  ADD_CFSD_STAT("mkdir", req);
  mknod_symlink(req, parent, name, S_IFDIR | mode, 0, nullptr);
  CFSD_TIMING_END("mkdir");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_symlink(fuse_req_t req, const char* link, fuse_ino_t parent,
                        const char* name)
{
  eos::common::Timing timing("symlink");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("symlink");
  ADD_CFSD_STAT("symlink", req);
  mknod_symlink(req, parent, name, S_IFLNK, 0, link);
  CFSD_TIMING_END("symlink");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t parent,
                     const char* name)
{
  eos::common::Timing timing("link");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("link");
  ADD_CFSD_STAT("link", req);
  FsID fsid(req);
  Inode& inode = get_inode(ino);
  Inode& inode_p = get_inode(parent);
  fuse_entry_param e {};
  e.attr_timeout = fs.timeout;
  e.entry_timeout = fs.timeout;
  char procname[64];
  sprintf(procname, "/proc/self/fd/%i", inode.fd);
  auto res = linkat(AT_FDCWD, procname, inode_p.fd, name, AT_SYMLINK_FOLLOW);

  if (res == -1) {
    fuse_reply_err(req, errno);
    return;
  }

  res = fstatat(inode.fd, "", &e.attr, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW);

  if (res == -1) {
    fuse_reply_err(req, errno);
    return;
  }

  e.ino = reinterpret_cast<fuse_ino_t>(&inode);
  {
    lock_guard<mutex> g {inode.m};
    inode.nlookup++;

    if (fs.debug)
      cerr << "DEBUG:" << __func__ << ":" << __LINE__ << " "
           <<  "inode " << inode.src_ino
           << " count " << inode.nlookup << endl;
  }
  fuse_reply_entry(req, &e);
  CFSD_TIMING_END("link");
  COMMONTIMING("_stop_", &timing);
  return;
}


static void sfs_rmdir(fuse_req_t req, fuse_ino_t parent, const char* name)
{
  eos::common::Timing timing("rmdir");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("rmdir");
  ADD_CFSD_STAT("rmdir", req);
  FsID fsid(req);
  Inode& inode_p = get_inode(parent);
  lock_guard<mutex> g {inode_p.m};
  auto res =
    (fs.recyclebin &&
     fs.recycle.shouldRecycle(fsid.getUid(), parent, inode_p.fd, name)) ?
    fs.recycle.moveBin(fsid.getUid(), parent, inode_p.fd, name) :
    unlinkat(inode_p.fd, name, AT_REMOVEDIR);
  fuse_reply_err(req, res == -1 ? errno : 0);
  CFSD_TIMING_END("rmdir");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_rename(fuse_req_t req, fuse_ino_t parent, const char* name,
                       fuse_ino_t newparent, const char* newname,
                       unsigned int flags)
{
  eos::common::Timing timing("rename");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("rename");
  ADD_CFSD_STAT("rename", req);
  FsID fsid(req);
  Inode& inode_p = get_inode(parent);
  Inode& inode_np = get_inode(newparent);

  if (flags) {
    fuse_reply_err(req, EINVAL);
    return;
  }

  auto res = renameat(inode_p.fd, name, inode_np.fd, newname);
  fuse_reply_err(req, res == -1 ? errno : 0);
  CFSD_TIMING_END("rename");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_unlink(fuse_req_t req, fuse_ino_t parent, const char* name)
{
  eos::common::Timing timing("unlink");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("unlink");
  ADD_CFSD_STAT("unlink", req);
  FsID fsid(req);
  Inode& inode_p = get_inode(parent);

  // Release inode.fd before last unlink like nfsd EXPORT_OP_CLOSE_BEFORE_UNLINK
  // to test reused inode numbers.
  // Skip this when inode has an open file and when writeback cache is enabled.
  if (!fs.timeout) {
    fuse_entry_param e;
    auto err = do_lookup(parent, name, &e);

    if (err) {
      fuse_reply_err(req, err);
      return;
    }

    if (e.attr.st_nlink == 1) {
      Inode& inode = get_inode(e.ino);
      lock_guard<mutex> g {inode.m};

      if (inode.fd > 0 && !inode.nopen) {
        if (fs.debug)
          cerr << "DEBUG: unlink: release inode " << e.attr.st_ino
               << "; fd=" << inode.fd << endl;

        {
          lock_guard<mutex> g_fs {fs.mutex};
          close(inode.fd);
          inode.fd = -ENOENT;
          inode.generation++;
        }
      }
    }

    // decrease the ref which lookup above had increased
    forget_one(e.ino, 1);
  }

  auto res =
    (fs.recyclebin &&
     fs.recycle.shouldRecycle(fsid.getUid(), parent, inode_p.fd, name)) ?
    fs.recycle.moveBin(fsid.getUid(), parent, inode_p.fd, name) :
    unlinkat(inode_p.fd, name, 0);
  fuse_reply_err(req, res == -1 ? errno : 0);
  CFSD_TIMING_END("unlink");
  COMMONTIMING("_stop_", &timing);
}


static void forget_one(fuse_ino_t ino, uint64_t n)
{
  Inode& inode = get_inode(ino);
  unique_lock<mutex> l {inode.m};

  if (n > inode.nlookup) {
    cerr << "INTERNAL ERROR: Negative lookup count for inode "
         << inode.src_ino << endl;
    abort();
  }

  inode.nlookup -= n;

  if (fs.debug)
    cerr << "DEBUG:" << __func__ << ":" << __LINE__ << " "
         <<  "inode " << inode.src_ino
         << " count " << inode.nlookup << endl;

  if (!inode.nlookup) {
    if (fs.debug) {
      cerr << "DEBUG: forget: cleaning up inode " << inode.src_ino << endl;
    }

    {
      lock_guard<mutex> g_fs {fs.mutex};
      l.unlock();
      fs.inodes.erase({inode.src_ino, inode.src_dev});
    }
  } else if (fs.debug)
    cerr << "DEBUG: forget: inode " << inode.src_ino
         << " lookup count now " << inode.nlookup << endl;
}

static void sfs_forget(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup)
{
  eos::common::Timing timing("forget");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("forget");
  ADD_CFSD_STAT("forget", req);
  forget_one(ino, nlookup);
  fuse_reply_none(req);
  CFSD_TIMING_END("forget");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_forget_multi(fuse_req_t req, size_t count,
                             fuse_forget_data* forgets)
{
  eos::common::Timing timing("forgetmulti");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("forgetmulti");
  ADD_CFSD_STAT("forgetmulti", req);

  for (size_t i = 0; i < count; i++) {
    forget_one(forgets[i].ino, forgets[i].nlookup);
  }

  fuse_reply_none(req);
  CFSD_TIMING_END("forgetmulti");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_readlink(fuse_req_t req, fuse_ino_t ino)
{
  eos::common::Timing timing("readlink");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("readlink");
  ADD_CFSD_STAT("readlink", req);
  FsID fsid(req);
  Inode& inode = get_inode(ino);
  char buf[PATH_MAX + 1];
  auto res = readlinkat(inode.fd, "", buf, sizeof(buf));

  if (res == -1) {
    fuse_reply_err(req, errno);
  } else if (res == sizeof(buf)) {
    fuse_reply_err(req, ENAMETOOLONG);
  } else {
    buf[res] = '\0';
    fuse_reply_readlink(req, buf);
  }

  CFSD_TIMING_END("readlink");
  COMMONTIMING("_stop_", &timing);
}

static DirHandle* get_dir_handle(fuse_file_info* fi)
{
  return reinterpret_cast<DirHandle*>(fi->fh);
}


static void sfs_opendir(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi)
{
  eos::common::Timing timing("opendir");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("opendir");
  ADD_CFSD_STAT("opendir", req);
  FsID fsid(req);
  Inode& inode = get_inode(ino);
  auto d = new (nothrow) DirHandle;

  if (d == nullptr) {
    fuse_reply_err(req, ENOMEM);
    return;
  }

  // Make Helgrind happy - it can't know that there's an implicit
  // synchronization due to the fact that other threads cannot
  // access d until we've called fuse_reply_*.
  lock_guard<mutex> g {inode.m};
  auto fd = openat(inode.fd, ".", O_RDONLY);

  if (fd == -1) {
    goto out_errno;
  }

  // On success, dir stream takes ownership of fd, so we
  // do not have to close it.
  d->dp = fdopendir(fd);

  if (d->dp == nullptr) {
    goto out_errno;
  }

  d->offset = 0;
  fi->fh = reinterpret_cast<uint64_t>(d);

  if (fs.timeout) {
    fi->keep_cache = 1;
    fi->cache_readdir = 1;
  }

  fuse_reply_open(req, fi);
  return;
out_errno:
  auto error = errno;
  delete d;

  if (error == ENFILE || error == EMFILE) {
    cerr << "ERROR: Reached maximum number of file descriptors." << endl;
  }

  fuse_reply_err(req, error);
  CFSD_TIMING_END("opendir");
  COMMONTIMING("_stop_", &timing);
}


static bool is_dot_or_dotdot(const char* name)
{
  return name[0] == '.' &&
         (name[1] == '\0' || (name[1] == '.' && name[2] == '\0'));
}


static void do_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                       off_t offset, fuse_file_info* fi, int plus)
{
  FsID fsid(req);
  auto d = get_dir_handle(fi);
  Inode& inode = get_inode(ino);
  lock_guard<mutex> g {inode.m};
  char* p;
  auto rem = size;
  int err = 0, count = 0;

  if (fs.debug)
    cerr << "DEBUG: readdir(): started with offset "
         << offset << endl;

  auto buf = new (nothrow) char[size];

  if (!buf) {
    fuse_reply_err(req, ENOMEM);
    return;
  }

  p = buf;

  if (offset != d->offset) {
    if (fs.debug) {
      cerr << "DEBUG: readdir(): seeking to " << offset << endl;
    }

    seekdir(d->dp, offset);
    d->offset = offset;
  }

  while (1) {
    struct dirent* entry;
    errno = 0;
    entry = readdir(d->dp);

    if (!entry) {
      if (errno) {
        err = errno;

        if (fs.debug) {
          warn("DEBUG: readdir(): readdir failed with");
        }

        goto error;
      }

      break; // End of stream
    }

    d->offset = entry->d_off;
    fuse_entry_param e{};
    size_t entsize;

    if (plus) {
      err = do_lookup(ino, entry->d_name, &e);

      if (err) {
        goto error;
      }

      entsize = fuse_add_direntry_plus(req, p, rem, entry->d_name, &e, entry->d_off);

      if (entsize > rem) {
        if (fs.debug) {
          cerr << "DEBUG: readdir(): buffer full, returning data. " << endl;
        }

        forget_one(e.ino, 1);
        break;
      }
    } else {
      e.attr.st_ino = entry->d_ino;
      e.attr.st_mode = entry->d_type << 12;
      entsize = fuse_add_direntry(req, p, rem, entry->d_name, &e.attr, entry->d_off);

      if (!is_dot_or_dotdot(entry->d_name)) {
        unique_lock<mutex> fs_lock {fs.mutex};
        std::shared_ptr fe = std::make_shared<forgetentry_t>(ino, entry->d_name);
        fs.forgetq.push_back(fe);
        fs.forgetq_size++;
      }

      if (entsize > rem) {
        if (fs.debug) {
          cerr << "DEBUG: readdir(): buffer full, returning data. " << endl;
        }

        break;
      }
    }

    p += entsize;
    rem -= entsize;
    count++;

    if (fs.debug) {
      cerr << "DEBUG: readdir(): added to buffer: " << entry->d_name
           << ", ino " << e.attr.st_ino << ", offset " << entry->d_off << endl;
    }
  }

  err = 0;
error:

  // If there's an error, we can only signal it if we haven't stored
  // any entries yet - otherwise we'd end up with wrong lookup
  // counts for the entries that are already in the buffer. So we
  // return what we've collected until that point.
  if (err && rem == size) {
    if (err == ENFILE || err == EMFILE) {
      cerr << "ERROR: Reached maximum number of file descriptors." << endl;
    }

    fuse_reply_err(req, err);
  } else {
    if (fs.debug)
      cerr << "DEBUG: readdir(): returning " << count
           << " entries, curr offset " << d->offset << endl;

    fuse_reply_buf(req, buf, size - rem);
  }

  delete[] buf;
  return;
}


static void sfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                        off_t offset, fuse_file_info* fi)
{
  eos::common::Timing timing("readdir");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("readdir");
  ADD_CFSD_STAT("readdir", req);
  // operation logging is done in readdir to reduce code duplication
  do_readdir(req, ino, size, offset, fi, 0);
  CFSD_TIMING_END("readdir");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_readdirplus(fuse_req_t req, fuse_ino_t ino, size_t size,
                            off_t offset, fuse_file_info* fi)
{
  eos::common::Timing timing("readdirplus");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("readdirplus");
  ADD_CFSD_STAT("readdirplus", req);
  // operation logging is done in readdir to reduce code duplication
  do_readdir(req, ino, size, offset, fi, 1);
  CFSD_TIMING_END("readdirplus");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_releasedir(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi)
{
  eos::common::Timing timing("releasedir");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("releasedir");
  ADD_CFSD_STAT("releasedir", req);
  (void) ino;
  auto d = get_dir_handle(fi);
  delete d;
  fuse_reply_err(req, 0);
  CFSD_TIMING_END("releasedir");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_create(fuse_req_t req, fuse_ino_t parent, const char* name,
                       mode_t mode, fuse_file_info* fi)
{
  eos::common::Timing timing("create");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("create");
  ADD_CFSD_STAT("create", req);
  FsID fsid(req);
  Inode& inode_p = get_inode(parent);
  auto fd = openat(inode_p.fd, name,
                   (fi->flags | O_CREAT) & ~O_NOFOLLOW, mode);

  if (fd == -1) {
    auto err = errno;

    if (err == ENFILE || err == EMFILE) {
      cerr << "ERROR: Reached maximum number of file descriptors." << endl;
    }

    fuse_reply_err(req, err);
    return;
  }

  {
    lock_guard<mutex> g {fs.openFdsMutex};
    fs.openFds[fd].first  = fsid.getUid();
    fs.openFds[fd].second = fsid.getGid();
  }

  fi->fh = fd;
  fuse_entry_param e;
  auto err = do_lookup(parent, name, &e);

  if (err) {
    if (err == ENFILE || err == EMFILE) {
      cerr << "ERROR: Reached maximum number of file descriptors." << endl;
    }

    fuse_reply_err(req, err);
    return;
  }

  Inode& inode = get_inode(e.ino);
  lock_guard<mutex> g {inode.m};
  inode.nopen++;
  fuse_reply_create(req, &e, fi);
  CFSD_TIMING_END("create");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync,
                         fuse_file_info* fi)
{
  eos::common::Timing timing("fsyncdir");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("fsyncdir");
  ADD_CFSD_STAT("fsyncdir", req);
  (void) ino;
  int res;
  int fd = dirfd(get_dir_handle(fi)->dp);

  if (datasync) {
    res = fdatasync(fd);
  } else {
    res = fsync(fd);
  }

  fuse_reply_err(req, res == -1 ? errno : 0);
  CFSD_TIMING_END("fsyncdir");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_open(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi)
{
  eos::common::Timing timing("open");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("open");
  ADD_CFSD_STAT("open", req);
  FsID fsid(req);
  Inode& inode = get_inode(ino);

  /* With writeback cache, kernel may send read requests even
     when userspace opened write-only */
  if (fs.timeout && (fi->flags & O_ACCMODE) == O_WRONLY) {
    fi->flags &= ~O_ACCMODE;
    fi->flags |= O_RDWR;
  }

  /* With writeback cache, O_APPEND is handled by the kernel.  This
     breaks atomicity (since the file may change in the underlying
     filesystem, so that the kernel's idea of the end of the file
     isn't accurate anymore). However, no process should modify the
     file in the underlying filesystem once it has been read, so
     this is not a problem. */
  if (fs.timeout && fi->flags & O_APPEND) {
    fi->flags &= ~O_APPEND;
  }

  /* Unfortunately we cannot use inode.fd, because this was opened
     with O_PATH (so it doesn't allow read/write access). */
  char buf[64];
  sprintf(buf, "/proc/self/fd/%i", inode.fd);
  auto fd = open(buf, fi->flags & ~O_NOFOLLOW);

  if (fd == -1) {
    auto err = errno;

    if (err == ENFILE || err == EMFILE) {
      cerr << "ERROR: Reached maximum number of file descriptors." << endl;
    }

    fuse_reply_err(req, err);
    return;
  }

  {
    lock_guard<mutex> g {fs.openFdsMutex};
    fs.openFds[fd].first  = fsid.getUid();
    fs.openFds[fd].second = fsid.getGid();
  }

  lock_guard<mutex> g {inode.m};
  inode.nopen++;
  fi->keep_cache = (fs.timeout != 0);
#if ( FUSE_MINOR_VERSION > 10 )
  fi->noflush = (fs.timeout == 0 && (fi->flags & O_ACCMODE) == O_RDONLY);
#endif
  fi->fh = fd;
  fuse_reply_open(req, fi);
  CFSD_TIMING_END("fsyncdir");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_release(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi)
{
  eos::common::Timing timing("release");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("release");
  ADD_CFSD_STAT("release", req);
  {
    lock_guard<mutex> g {fs.openFdsMutex};
    fs.openFds.erase(fi->fh);
  }
  Inode& inode = get_inode(ino);
  lock_guard<mutex> g {inode.m};
  inode.nopen--;
  close(fi->fh);
  fuse_reply_err(req, 0);
  CFSD_TIMING_END("release");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_flush(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi)
{
  eos::common::Timing timing("flush");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("flush");
  ADD_CFSD_STAT("flush", req);
  FsID fsid(req);
  (void) ino;
  auto res = close(dup(fi->fh));
  fuse_reply_err(req, res == -1 ? errno : 0);
  CFSD_TIMING_END("flush");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                      fuse_file_info* fi)
{
  eos::common::Timing timing("fsync");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("fsync");
  ADD_CFSD_STAT("fsync", req);
  FsID fsid(req);
  (void) ino;
  int res;

  if (datasync) {
    res = fdatasync(fi->fh);
  } else {
    res = fsync(fi->fh);
  }

  fuse_reply_err(req, res == -1 ? errno : 0);
  CFSD_TIMING_END("fsync");
  COMMONTIMING("_stop_", &timing);
}


static void do_read(fuse_req_t req, size_t size, off_t off, fuse_file_info* fi)
{
  fuse_bufvec buf = FUSE_BUFVEC_INIT(size);
  buf.buf[0].flags = static_cast<fuse_buf_flags>(
                       FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK);
  buf.buf[0].fd = fi->fh;
  buf.buf[0].pos = off;
  fuse_reply_data(req, &buf, FUSE_BUF_COPY_FLAGS);
  ADD_CFSD_IO_STAT("rbytes", size);
}

static void sfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                     fuse_file_info* fi)
{
  eos::common::Timing timing("read");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("read");
  ADD_CFSD_STAT("read", req);
  (void) ino;
  do_read(req, size, off, fi);
  CFSD_TIMING_END("read");
  COMMONTIMING("_stop_", &timing);
}


static void do_write_buf(fuse_req_t req, size_t size, off_t off,
                         fuse_bufvec* in_buf, fuse_file_info* fi)
{
  uid_t uid = 99;
  gid_t gid = 99;
  {
    lock_guard<mutex> g {fs.openFdsMutex};
    uid = fs.openFds[fi->fh].first;
    gid = fs.openFds[fi->fh].second;
  }

  if (!fs.quota.hasquota(uid, gid)) {
    fuse_reply_err(req, EDQUOT);
    return;
  }

  fuse_bufvec out_buf = FUSE_BUFVEC_INIT(size);
  out_buf.buf[0].flags = static_cast<fuse_buf_flags>(
                           FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK);
  out_buf.buf[0].fd = fi->fh;
  out_buf.buf[0].pos = off;
  auto res = fuse_buf_copy(&out_buf, in_buf, FUSE_BUF_COPY_FLAGS);

  if (res < 0) {
    fuse_reply_err(req, -res);
  } else {
    fuse_reply_write(req, (size_t)res);
    ADD_CFSD_IO_STAT("wbytes", res);
  }
}


static void sfs_write_buf(fuse_req_t req, fuse_ino_t ino, fuse_bufvec* in_buf,
                          off_t off, fuse_file_info* fi)
{
  eos::common::Timing timing("write");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("write");
  ADD_CFSD_STAT("write", req);
  (void) ino;
  auto size {fuse_buf_size(in_buf)};
  do_write_buf(req, size, off, in_buf, fi);
  CFSD_TIMING_END("write");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_statfs(fuse_req_t req, fuse_ino_t ino)
{
  eos::common::Timing timing("statfs");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("statfs");
  ADD_CFSD_STAT("statfs", req);
  FsID fsid(req);
  struct statvfs stbuf;
  auto res = fstatvfs(get_fs_fd(ino), &stbuf);

  if (res == -1) {
    fuse_reply_err(req, errno);
  } else {
    fuse_reply_statfs(req, &stbuf);
  }

  CFSD_TIMING_END("statfs");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_fallocate(fuse_req_t req, fuse_ino_t ino, int mode,
                          off_t offset, off_t length, fuse_file_info* fi)
{
  eos::common::Timing timing("fallocate");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("fallocate");
  ADD_CFSD_STAT("fallocate", req);
  (void) ino;

  if (mode) {
    fuse_reply_err(req, EOPNOTSUPP);
    return;
  }

  auto err = posix_fallocate(fi->fh, offset, length);
  fuse_reply_err(req, err);
  CFSD_TIMING_END("fallocate");
  COMMONTIMING("_stop_", &timing);
}

static void sfs_flock(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi,
                      int op)
{
  eos::common::Timing timing("flock");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("flock");
  ADD_CFSD_STAT("flock", req);
  (void) ino;
  auto res = flock(fi->fh, op);
  fuse_reply_err(req, res == -1 ? errno : 0);
  CFSD_TIMING_END("flock");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_getxattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                         size_t size)
{
  eos::common::Timing timing("getxattr");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("getxattr");
  ADD_CFSD_STAT("getxattr", req);
  FsID fsid(req);
  std::string vattr = cfsvattr::vattr(name, fsid.getName(), fsid.getUid(),
                                      fsid.getGid(), fs.quota.hasquota(fsid.getUid(), fsid.getGid()));

  if (!vattr.empty()) {
    if (size && vattr.size() > size) {
      fuse_reply_err(req, ERANGE);
    } else {
      if (size == 0) {
        fuse_reply_xattr(req, vattr.size());
      } else {
        fuse_reply_buf(req, vattr.c_str(), vattr.size());
      }
    }

    return;
  }

  char* value = nullptr;
  Inode& inode = get_inode(ino);
  ssize_t ret;
  int saverr;
  char procname[64];
  sprintf(procname, "/proc/self/fd/%i", inode.fd);

  if (size) {
    value = new (nothrow) char[size];

    if (value == nullptr) {
      saverr = ENOMEM;
      goto out;
    }

    ret = getxattr(procname, name, value, size);

    if (ret == -1) {
      goto out_err;
    }

    saverr = 0;

    if (ret == 0) {
      goto out;
    }

    fuse_reply_buf(req, value, ret);
  } else {
    ret = getxattr(procname, name, nullptr, 0);

    if (ret == -1) {
      goto out_err;
    }

    fuse_reply_xattr(req, ret);
  }

  CFSD_TIMING_END("getxattr");
  COMMONTIMING("_stop_", &timing);
out_free:
  delete[] value;
  return;
out_err:
  saverr = errno;
out:
  fuse_reply_err(req, saverr);
  goto out_free;
}


static void sfs_listxattr(fuse_req_t req, fuse_ino_t ino, size_t size)
{
  eos::common::Timing timing("listxattr");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("listxattr");
  ADD_CFSD_STAT("listxxattr", req);
  FsID fsid(req);
  char* value = nullptr;
  Inode& inode = get_inode(ino);
  ssize_t ret;
  int saverr;
  char procname[64];
  sprintf(procname, "/proc/self/fd/%i", inode.fd);

  if (size) {
    value = new (nothrow) char[size];

    if (value == nullptr) {
      saverr = ENOMEM;
      goto out;
    }

    ret = listxattr(procname, value, size);

    if (ret == -1) {
      goto out_err;
    }

    saverr = 0;

    if (ret == 0) {
      goto out;
    }

    fuse_reply_buf(req, value, ret);
  } else {
    ret = listxattr(procname, nullptr, 0);

    if (ret == -1) {
      goto out_err;
    }

    fuse_reply_xattr(req, ret);
  }

  CFSD_TIMING_END("listxattr");
  COMMONTIMING("_stop_", &timing);
out_free:
  delete[] value;
  return;
out_err:
  saverr = errno;
out:
  fuse_reply_err(req, saverr);
  goto out_free;
}


static void sfs_setxattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                         const char* value, size_t size, int flags)
{
  eos::common::Timing timing("setxattr");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("setxattr");
  ADD_CFSD_STAT("setxattr", req);
  FsID fsid(req);
  Inode& inode = get_inode(ino);
  ssize_t ret;
  int saverr;
  char procname[64];
  sprintf(procname, "/proc/self/fd/%i", inode.fd);
  ret = setxattr(procname, name, value, size, flags);
  saverr = ret == -1 ? errno : 0;
  fuse_reply_err(req, saverr);
  CFSD_TIMING_END("setxattr");
  COMMONTIMING("_stop_", &timing);
}


static void sfs_removexattr(fuse_req_t req, fuse_ino_t ino, const char* name)
{
  eos::common::Timing timing("removexattr");
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  CFSD_TIMING_BEGIN("removexattr");
  ADD_CFSD_STAT("removexattr", req);
  FsID fsid(req);
  char procname[64];
  Inode& inode = get_inode(ino);
  ssize_t ret;
  int saverr;
  sprintf(procname, "/proc/self/fd/%i", inode.fd);
  ret = removexattr(procname, name);
  saverr = ret == -1 ? errno : 0;
  fuse_reply_err(req, saverr);
  CFSD_TIMING_END("removexattr");
  COMMONTIMING("_stop_", &timing);
}

static void assign_operations(fuse_lowlevel_ops& sfs_oper)
{
  sfs_oper.init = sfs_init;
  sfs_oper.lookup = sfs_lookup;
  sfs_oper.mkdir = sfs_mkdir;
  sfs_oper.mknod = sfs_mknod;
  sfs_oper.symlink = sfs_symlink;
  sfs_oper.link = sfs_link;
  sfs_oper.unlink = sfs_unlink;
  sfs_oper.rmdir = sfs_rmdir;
  sfs_oper.rename = sfs_rename;
  sfs_oper.forget = sfs_forget;
  sfs_oper.forget_multi = sfs_forget_multi;
  sfs_oper.getattr = sfs_getattr;
  sfs_oper.setattr = sfs_setattr;
  sfs_oper.readlink = sfs_readlink;
  sfs_oper.opendir = sfs_opendir;
  sfs_oper.readdir = sfs_readdir;
  sfs_oper.readdirplus = sfs_readdirplus;
  sfs_oper.releasedir = sfs_releasedir;
  sfs_oper.fsyncdir = sfs_fsyncdir;
  sfs_oper.create = sfs_create;
  sfs_oper.open = sfs_open;
  sfs_oper.release = sfs_release;
  sfs_oper.flush = sfs_flush;
  sfs_oper.fsync = sfs_fsync;
  sfs_oper.read = sfs_read;
  sfs_oper.write_buf = sfs_write_buf;
  sfs_oper.statfs = sfs_statfs;
  sfs_oper.fallocate = sfs_fallocate;
  sfs_oper.flock = sfs_flock;
  sfs_oper.setxattr = sfs_setxattr;
  sfs_oper.getxattr = sfs_getxattr;
  sfs_oper.listxattr = sfs_listxattr;
  sfs_oper.removexattr = sfs_removexattr;
}


std::string prepare(std::string input, std::string keylocation)
{
  std::string key;
  eos::common::StringConversion::LoadFileIntoString(keylocation.c_str(), key);
  struct stat buf;

  if (::stat(keylocation.c_str(), &buf)) {
    fprintf(stderr, "error: %s not accessible!\n", keylocation.c_str());
    exit(-1);
  }

#include "keychange.hh"

  if (buf.st_uid || ((buf.st_mode & 0x1ff) != S_IRUSR)) {
    fprintf(stderr,
            "error: %sdoes not have correct ownership (root) or 400 permission! [%d/%d/%x/%x]\n",
            keylocation.c_str(), buf.st_uid, buf.st_mode, buf.st_mode, S_IRUSR);
    exit(-1);
  }

  std::string shakey = eos::common::SymKey::HexSha256(key);
  XrdOucString in = input.c_str();
  XrdOucString out;
  eos::common::SymKey::SymmetricStringDecrypt(in, out, (char*)shakey.c_str());
  return out.c_str();
}

int execute(std::string& scmd)
{
  return system(scmd.c_str());
}

void
Fs::LevelFDs(ThreadAssistant& assistant)
{
  while (true) {
    assistant.wait_for(std::chrono::milliseconds(1000));
    {
      ForgetQueue forget;
      {
        const std::lock_guard<std::mutex> g_fs(this->mutex);

        if (fs.dropcache) {
          if (fs.inodes.size() > (128 * 1024)) {
            fprintf(stderr, "# inodes:%lu\n", fs.inodes.size());
            fprintf(stderr, "# flushing DENTRY cache\n");
            ofstream mdflush;
            mdflush.open("/proc/sys/vm/drop_caches");
            mdflush << "2" << std::endl;
            mdflush.close();
          }
        } else {
          time_t now = time(NULL);

          //    fprintf(stderr,"%lu %lu %lu %lu\n", fs.forgetq_size.load(),
          //      fs.forgetq_size.load()?fs.forgetq.front()->tst:0, fs.idletime, now);
          while ((fs.forgetq_size > 4096) ||
                 (fs.forgetq_size && ((time_t)(fs.forgetq.front()->tst + fs.idletime) < now))) {
            forget.push_back(fs.forgetq.front());
            //      fprintf(stderr,"forgetting %x:%s\n", fs.forgetq.front()->parent, fs.forgetq.front()->name.c_str());
            fs.forgetq.pop_front();
            forgetq_size--;
            //      fprintf(stderr,"forgetq: %lu\n", forgetq_size.load());
          }
        }
      }

      for (auto it : forget) {
        fuse_lowlevel_notify_inval_entry(fs.se,
                                         it->parent, it->name.c_str(), it->name.length());
        //fprintf(stderr,"forgot %x:%s\n", it->parent, it->name.c_str());
      }
    }

    if (assistant.terminationRequested()) {
      break;
    }
  }
}

void
Fs::StatCirculate(ThreadAssistant& assistant)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("started stat circulate thread");
  fusestat.Circulate(assistant);
}

void
Fs::DumpStatistic(ThreadAssistant& assistant)
{
  eos::common::LinuxTotalMem meminfo;

  while (!assistant.terminationRequested()) {
    Json::StyledWriter jsonwriter;
    Json::Value jsonstats{};
    meminfo.update();
    eos::common::LinuxStat::linux_stat_t osstat;
#ifndef __APPLE__
    eos::common::LinuxMemConsumption::linux_mem_t mem;

    if (!eos::common::LinuxMemConsumption::GetMemoryFootprint(mem)) {
      eos_static_err("failed to get the MEM usage information");
    }

    if (!eos::common::LinuxStat::GetStat(osstat)) {
      eos_static_err("failed to get the OS usage information");
    }

#endif
    eos_static_debug("dumping statistics");
    {
      //fuse counter
      fusestat.PrintOutTotalJson(jsonstats);
      {
        unsigned long long rbytes, wbytes = 0;
        unsigned long nops = 0;
        float total_rbytes, total_wbytes = 0;
        int sum = 0;
        unsigned long totalram, freeram, loads0 = 0;
        {
          std::lock_guard<std::mutex> g{getFuseStat().Mutex};
          rbytes = this->getFuseStat().GetTotal("rbytes");
          wbytes = this->getFuseStat().GetTotal("wbytes");
          nops = this->getFuseStat().GetOps();
          total_rbytes = this->getFuseStat().GetTotalAvg5("rbytes") / 1000.0 / 1000.0;
          total_wbytes = this->getFuseStat().GetTotalAvg5("wbytes") / 1000.0 / 1000.0;
          sum = (int) this->getFuseStat().GetTotalAvg5(":sum");
          {
            std::lock_guard<std::mutex> lock(meminfo.mutex());
            totalram = meminfo.getref().totalram;
            freeram = meminfo.getref().freeram;
            loads0 = meminfo.getref().loads[0];
          }
          {
            //os stats
            Json::Value stats {};
            std::string s1;
            std::string s2;
            stats["threads"]             = (Json::LargestUInt) osstat.threads;
            stats["vsize"]               =
              eos::common::StringConversion::GetReadableSizeString(s1, osstat.vsize, "b");
            stats["rss"]                 =
              eos::common::StringConversion::GetReadableSizeString(s2, osstat.rss, "b");
            stats["pid"]                 = (Json::UInt) getpid();
            stats["version"]             = VERSION;
            stats["fuseversion"]         = FUSE_USE_VERSION;
            stats["starttime"]           = (Json::LargestUInt) starttime;
            stats["uptime"]              = (Json::LargestUInt) time(NULL) - starttime;
            stats["total-mem"]           = (Json::LargestUInt) totalram;
            stats["free-mem"]            = (Json::LargestUInt) freeram;
            stats["load"]                = (Json::LargestUInt) loads0;
            stats["total-rbytes"]        = (Json::LargestUInt) rbytes;
            stats["total-wbytes"]        = (Json::LargestUInt) wbytes;
            stats["total-io-ops"]        = (Json::LargestUInt) nops;
            stats["read-mb/s"]           = (double) total_rbytes;
            stats["write-mb/s"]          = (double) total_wbytes;
            stats["iops"]                = sum;
            stats["forgetq"]             = (Json::LargestUInt) fs.forgetq_size.load();
            {
              std::unique_lock<std::mutex> fs_lock {fs.mutex};
              stats["inodes"]              = (Json::LargestUInt)fs.inodes.size();
            }
            jsonstats["stats"] = stats;
          }
        }
      }
    }
    std::string tmpjsonfile = fs.jsonpath + "~";
    std::ofstream dumpjsonfile(tmpjsonfile);
    {
      // atomic re-write+replace
      dumpjsonfile << jsonwriter.write(jsonstats);

      if (::chmod(tmpjsonfile.c_str(), S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) {
        fprintf(stderr, "error: failed to chmod <json> stats file to 644 '%s'\n",
                tmpjsonfile.c_str());
      }

      if (::rename(tmpjsonfile.c_str(), fs.jsonpath.c_str())) {
        fprintf(stderr,
                "error: failed to rename <json> stats file from temporary to final name '%s'=>'%s'\n",
                tmpjsonfile.c_str(),
                fs.jsonpath.c_str());
      }
    }
    assistant.wait_for(std::chrono::seconds(1));
  }
}

Fs::Fs()
{
  dropcache = false;
  idletime = 60;
  fusestat.Add("getattr", 0, 0, 0);
  fusestat.Add("setattr", 0, 0, 0);
  fusestat.Add("setattr:chown", 0, 0, 0);
  fusestat.Add("setattr:chmod", 0, 0, 0);
  fusestat.Add("setattr:utimes", 0, 0, 0);
  fusestat.Add("setattr:truncate", 0, 0, 0);
  fusestat.Add("lookup", 0, 0, 0);
  fusestat.Add("opendir", 0, 0, 0);
  fusestat.Add("readdir", 0, 0, 0);
  fusestat.Add("readdirplus", 0, 0, 0);
  fusestat.Add("releasedir", 0, 0, 0);
  fusestat.Add("fsyncdir", 0, 0, 0);
  fusestat.Add("statfs", 0, 0, 0);
  fusestat.Add("mknod", 0, 0, 0);
  fusestat.Add("mkdir", 0, 0, 0);
  fusestat.Add("rm", 0, 0, 0);
  fusestat.Add("unlink", 0, 0, 0);
  fusestat.Add("rmdir", 0, 0, 0);
  fusestat.Add("rename", 0, 0, 0);
  fusestat.Add("access", 0, 0, 0);
  fusestat.Add("open", 0, 0, 0);
  fusestat.Add("create", 0, 0, 0);
  fusestat.Add("read", 0, 0, 0);
  fusestat.Add("write", 0, 0, 0);
  fusestat.Add("release", 0, 0, 0);
  fusestat.Add("fsync", 0, 0, 0);
  fusestat.Add("fallocate", 0, 0, 0);
  fusestat.Add("flock", 0, 0, 0);
  fusestat.Add("forget", 0, 0, 0);
  fusestat.Add("forgetmulti", 0, 0, 0);
  fusestat.Add("flush", 0, 0, 0);
  fusestat.Add("getxattr", 0, 0, 0);
  fusestat.Add("setxattr", 0, 0, 0);
  fusestat.Add("listxattr", 0, 0, 0);
  fusestat.Add("removexattr", 0, 0, 0);
  fusestat.Add("readlink", 0, 0, 0);
  fusestat.Add("symlink", 0, 0, 0);
  fusestat.Add("link", 0, 0, 0);
  fusestat.Add(__SUM__TOTAL__, 0, 0, 0);
}

void
Fs::Run()
{
  tFdLeveler.reset(&Fs::LevelFDs, this);
  tDumpStatistic.reset(&Fs::DumpStatistic, this);
  tStatCirculate.reset(&Fs::StatCirculate, this);
}

Fs::~Fs()
{
  tFdLeveler.join();
  tDumpStatistic.join();
  tStatCirculate.join();
}

int main(int argc, char* argv[])
{
  // Parse command line options
  auto options = parse_options(argc, argv);
  // We need an fd for every dentry in our the filesystem that the
  // kernel knows about. This is way more than most processes need,
  // so try to get rid of any resource softlimit.
  // Initialize filesystem root
  fs.root.fd = -1;
  fs.root.nlookup = 9999;
  fs.timeout = 0;
  fs.source = "/@eoscfsd/";

  if (fs.name.empty()) {
    fs.name = "default";
  }

  fs.keyresource = "cernhome-server.cern.ch/";
  fs.keyresource += fs.name;
  fs.keyresource += ".key";
  fs.keyfile = "cfsd.key";
  fs.starttime = time(NULL);
  // cernhome mount
  system("mkdir -p /@eoscfsd/");
  struct stat stat;
  auto ret = lstat(fs.source.c_str(), &stat);

  if (ret == -1) {
    err(1, "ERROR: failed to stat source (\"%s\")", fs.source.c_str());
  }

  if (!S_ISDIR(stat.st_mode)) {
    errx(1, "ERROR: source is not a directory");
  }

  fs.src_dev = stat.st_dev;
  // Initialize fuse
  fuse_args args = FUSE_ARGS_INIT(0, nullptr);
  std::string nameopt = "fsname=";
  nameopt += fs.name;
  nameopt += ",allow_other,subtype=eoscfs";

  for (auto i : options) {
    fprintf(stderr, "options: %s\n", i.c_str());
  }

  if (fuse_opt_add_arg(&args, argv[0]) ||
      fuse_opt_add_arg(&args, "-o") ||
      fuse_opt_add_arg(&args, nameopt.c_str()) ||
      (options.count("debug-fuse") && fuse_opt_add_arg(&args, "-odebug"))) {
    errx(3, "ERROR: Out of memory");
  }

  fuse_lowlevel_ops sfs_oper {};
  assign_operations(sfs_oper);
  // Setup credential cache
  CredentialConfig cconfig;
  cconfig.fuse_shared = true;
  cconfig.use_user_krb5cc = true;
  cconfig.use_user_oauth2 = false;
  cconfig.use_user_unix = false;
  cconfig.ignore_containerization = true;
  cconfig.use_user_gsiproxy = false;
  cconfig.use_user_sss = false;
  cconfig.credentialStore = "/var/cache/eos/cfsd/credential-store/";
  cconfig.tryKrb5First = true;
  cconfig.environ_deadlock_timeout = 100;
  cconfig.forknoexec_heuristic = true;
  Json::Value root;

  try {
    std::ifstream file("/etc/eos/cfsd/eoscfsd.conf");
    file >> root;
  } catch (...) {
    fprintf(stderr,
            "# warning: couldn't/didn't parse /etc/eos/cfsd/eoscfsd.conf\n");
  }

  std::string cmd;
  std::string scmd;
  fs.k5domain = "CERN.CH";

  if (root.isMember("auth") && root["auth"].isMember("k5domain")) {
    fs.k5domain = root["auth"]["k5domain"].asString();
  }

  fs.k5domain.insert(0, "@");
  fprintf(stderr, "info: kerberos domain is '%s'n", fs.k5domain.c_str());

  if (root.isMember(fs.name) && root[fs.name].isMember("server")) {
    if (root[fs.name]["server"].asString().empty()) {
      // compiled in mount-script
      fs.keyresource = "";
      fs.keyfile = fs.name;
      fs.keyfile += ".key";
    } else {
      // fetch mount script
      fs.keyresource = root[fs.name]["server"].asString();
      fs.keyresource += "/";
      fs.keyresource += fs.name;
      fs.keyresource += ".key";
      fs.keyfile = fs.name;
      fs.keyfile += ".key";
    }
  }

  std::string keyfile = "/etc/eos/cfsd/" + fs.keyfile;
  //  fs.dcache.setMaxSize(4096);
  fs.se = fuse_session_new(&args, &sfs_oper, sizeof(sfs_oper), &fs);

  if (fs.se == nullptr) {
    goto err_out1;
  }

  if (fuse_set_signal_handlers(fs.se) != 0) {
    goto err_out2;
  }

  // umount us
  umount();
  // Don't apply umask, use modes exactly as specified
  umask(0);
  // Mount and run main loop
  struct fuse_loop_config loop_config;
  loop_config.clone_fd = 1;
  loop_config.max_idle_threads = 10;

  if (fuse_session_mount(fs.se, fs.mount.c_str()) != 0) {
    goto err_out3;
  }

  fprintf(stderr, "# unsharing\n");

  // unshare mount namespace
  if (unshare(CLONE_NEWNS)) {
    fprintf(stderr, "warning: failed to unshare mount namespace errno=%d\n", errno);
  }

  fprintf(stderr, "# re-mounting\n");

  if (mount("none", "/", NULL, MS_REC | MS_PRIVATE, NULL)) {
    fprintf(stderr, "warning: failed none mount / - errno=%d\n", errno);
  }

  fprintf(stderr, "# mounting %s\n", fs.name.c_str());
#include "overlay.hh"

  if (!fs.keyresource.empty()) {
    // fetch mount instruction remote
    cmd = cfskey::get(fs.keyresource);
  } else {
    // this will be provided by overlay.hh
  }

  fs.logpath = "/var/log/eos/cfsd/";
  fs.logpath += fs.name;
  fs.jsonpath = fs.logpath;
  fs.logpath += ".log";
  fs.jsonpath += ".json";
  struct stat buf1;
  struct stat buf2;
  fprintf(stderr, "# mounting backends ...\n");
  ::stat(fs.source.c_str(), &buf1);
  pid_t child;

  if (!(child = fork())) {
    eos::common::Untraceable();
    scmd = prepare(cmd, keyfile);
    execute(scmd);
    exit(0);
  } else {
    time_t a_time = time(NULL);

    do {
      ::stat(fs.source.c_str(), &buf2);

      if (buf1.st_ino != buf2.st_ino) {
        kill(child, 9);
        break;
      }

      time_t b_time = time(NULL);

      if ((b_time - a_time) > 10) {
        kill(child, 9);
        fprintf(stderr, "error: internal mount failed\n");
        exit(-1);
      }
    } while (1);
  }

  fprintf(stderr, "# backends mounted ...");
  system("ls -la /@eoscfsd/");
  fs.root.fd = open(fs.source.c_str(), O_PATH);

  if (fs.root.fd == -1) {
    err(1, "ERROR: open(\"%s\", O_PATH)", fs.source.c_str());
  }

  if (fuse_daemonize(fs.foreground) != -1) {
    eos::common::Logging::GetInstance().SetUnit("FUSE@eoscfsd");
    eos::common::Logging::GetInstance().gShortFormat = true;
    eos::common::Logging::GetInstance().SetIndexSize(512);

    if (fs.debug) {
      eos::common::Logging::GetInstance().SetLogPriority(LOG_DEBUG);
    } else {
      eos::common::Logging::GetInstance().SetLogPriority(LOG_WARNING);
    }

    // start threads now
    fs.Run();

    if (!fs.foreground) {
      FILE* fstderr = 0;
      eos::common::Path cPath(fs.logpath.c_str());
      cPath.MakeParentPath(S_IRWXU | S_IRGRP | S_IROTH | S_IXGRP | S_IXOTH);

      if (!(fstderr = freopen(cPath.GetPath(), "a+", stderr))) {
        fprintf(stderr, "error: cannot open log file %s\n", cPath.GetPath());
        exit(-1);
      } else if (::chmod(cPath.GetPath(), S_IRUSR | S_IWUSR)) {
        fprintf(stderr, "error: failed to chmod %s\n", cPath.GetPath());
        exit(-1);
      }
    }

    eos_static_warning("********************************************************************************");
    eos_static_warning("eoscfsd started version %s - FUSE protocol version %d",
                       VERSION, FUSE_USE_VERSION);
    cfslogin::initializeProcessCache(cconfig);
    maximize_fd_limit();
    maximize_priority();

    if (options.count("single")) {
      ret = fuse_session_loop(fs.se);
    } else {
      ret = fuse_session_loop_mt(fs.se, &loop_config);
    }

    eos_static_warning("eoscfsd stopped version %s - FUSE protocol version %d",
                       VERSION, FUSE_USE_VERSION);
    eos_static_warning("********************************************************************************");
    fuse_session_unmount(fs.se);
err_out3:
    fuse_remove_signal_handlers(fs.se);
err_out2:
    fuse_session_destroy(fs.se);
err_out1:
    fuse_opt_free_args(&args);
    return ret ? 1 : 0;
  } else {
    fprintf(stderr, "error: failed to daemonize\n");
    exit(errno ? errno : -1);
  }
}


