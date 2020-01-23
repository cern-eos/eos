//------------------------------------------------------------------------------
//! @file eosfuse.cc
//! @author Andreas-Joachim Peters, Geoffray Adde, Elvin Sindrilaru CERN
//! @brief EOS C++ Fuse low-level implementation
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

#include "eosfuse.hh"
#include "MacOSXHelper.hh"

#include <string>
#include <map>
#include <iostream>
#include <sstream>
#include <memory>
#include <algorithm>
#include <dirent.h>
#include <stdint.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include "common/XattrCompat.hh"
#include "common/Timing.hh"
#include <XrdCl/XrdClDefaultEnv.hh>

#define _FILE_OFFSET_BITS 64

#ifdef __APPLE__
#define UPDATEPROCCACHE \
  do {} while (0)

#else
#define UPDATEPROCCACHE \
  do { \
    int errCode; \
    if( (errCode=me.fs().update_proc_cache(fuse_req_ctx(req)->uid,fuse_req_ctx(req)->gid,fuse_req_ctx(req)->pid)) )\
    { \
      fuse_reply_err (req, errCode); \
      return; \
    } \
  } while (0)

#endif

EosFuse::EosFuse()
{
// set default values
  config.entrycachetime = 10.0;
  config.attrcachetime = 10.0;
  config.neg_entrycachetime = 30.0;
  config.readopentime = 5.0;
  config.cap_creator_lifetime = 30;
  config.kernel_cache = 0;
  config.direct_io = 0;
  config.no_access = 0;
}

EosFuse::~EosFuse() { }

int
EosFuse::run(int argc, char* argv[], void* userdata)
{
  eos_static_debug("");
  EosFuse& me = instance();
  struct fuse_chan* ch;
  int err = -1;
  char* epos;
  char* spos;
  char* local_mount_dir;
  char mounthostport[4096];
  char mountprefix[4096];
  int i;

  if (getenv("EOS_FUSE_ENTRY_CACHE_TIME")) {
    me.config.entrycachetime = strtod(getenv("EOS_FUSE_ENTRY_CACHE_TIME"), 0);
  }

  if (getenv("EOS_FUSE_ATTR_CACHE_TIME")) {
    me.config.attrcachetime = strtod(getenv("EOS_FUSE_ATTR_CACHE_TIME"), 0);
  }

  if (getenv("EOS_FUSE_NEG_ENTRY_CACHE_TIME")) {
    me.config.neg_entrycachetime = strtod(getenv("EOS_FUSE_NEG_ENTRY_CACHE_TIME"),
                                          0);
  }

  if ((getenv("EOS_FUSE_KERNELCACHE")) &&
      (!strcmp(getenv("EOS_FUSE_KERNELCACHE"), "1"))) {
    me.config.kernel_cache = 1;
  }

  if (((!getenv("EOS_FUSE_NOACCESS")) ||
       (!strcmp(getenv("EOS_FUSE_NOACCESS"), "1")))) {
    me.config.no_access = 1;
  }

  if ((getenv("EOS_FUSE_DIRECTIO")) &&
      (!strcmp(getenv("EOS_FUSE_DIRECTIO"), "1"))) {
    me.config.direct_io = 1;
  }

  if ((getenv("EOS_FUSE_SYNC")) && (!strcmp(getenv("EOS_FUSE_SYNC"), "1"))) {
    me.config.is_sync = 1;
  } else {
    me.config.is_sync = 0;
  }

  if (getenv("EOS_FUSE_MAX_WB_INMEMORY_SIZE")) {
    me.fs().setMaxWbInMemorySize(strtoull(getenv("EOS_FUSE_MAX_WB_INMEMORY_SIZE"),
                                          0, 10));
  }

  char rdr[4096];
  char url[4096];
  rdr[0] = 0;
  char* cstr = getenv("EOS_RDRURL");

  if (cstr && (strlen(cstr) < 4096)) {
    snprintf(rdr, 4096, "%s", cstr);
  }

  for (i = 0; i < argc; i++) {
    if ((spos = strstr(argv[i], "url=root://"))) {
      size_t os = spos - argv[i];
      argv[i] = strdup(argv[i]);
      argv[i][os - 1] = 0;
      snprintf(rdr, 4096, "%s", spos + 4);
      snprintf(url, 4096, "%s", spos + 4);

      if ((epos = strstr(rdr + 7, "//"))) {
        if ((epos + 2 - rdr) < 4096) {
          rdr[epos + 2 - rdr] = 0;
        }
      }
    }
  }

  if (!rdr[0]) {
    fprintf(stderr, "error: EOS_RDRURL is not defined or add "
            "root://<host>// to the options argument\n");
    exit(-1);
  }

  if (strchr(rdr, '@')) {
    fprintf(stderr, "error: EOS_RDRURL or url option contains user "
            "specification '@' - forbidden\n");
    exit(-1);
  }

  setenv("EOS_RDRURL", rdr, 1);
  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
  // Move the mounthostport starting with the host name
  char* pmounthostport = 0;
  char* smountprefix = 0;
  pmounthostport = strstr(url, "root://");
#ifndef __APPLE__

  if (::access("/bin/fusermount", X_OK)) {
    fprintf(stderr, "error: /bin/fusermount is not executable for you!\n");
    exit(-1);
  }

#endif

  if (!pmounthostport) {
    fprintf(stderr, "error: EOS_RDRURL or url option is not valid\n");
    exit(-1);
  }

  pmounthostport += 7;

  if (!(smountprefix = strstr(pmounthostport, "//"))) {
    fprintf(stderr, "error: EOS_RDRURL or url option is not valid\n");
    exit(-1);
  } else {
    strncpy(mounthostport, pmounthostport, smountprefix - pmounthostport);
    *smountprefix = 0;
    smountprefix++;
    smountprefix++;
    size_t sz = std::min(strlen(smountprefix), (size_t)4095);
    strncpy(mountprefix, smountprefix, sz);
    mountprefix[sz] = '\0';

    while (mountprefix[strlen(mountprefix) - 1] == '/') {
      mountprefix[strlen(mountprefix) - 1] = '\0';
    }
  }

  if (getuid() <= DAEMONUID) {
    setenv("KRB5CCNAME", "FILE:/dev/null", 1);
    setenv("X509_USER_PROXY", "/dev/null", 1);
  }

  if (!me.fs().check_mgm(NULL)) {
    me.fs().initlogging();
    eos_static_crit("failed to contact configured mgm");
    return 1;
  }

  if ((fuse_parse_cmdline(&args, &local_mount_dir, NULL,
                          &me.config.isdebug) != -1) &&
      ((ch = fuse_mount(local_mount_dir, &args)) != NULL) &&
#ifdef __APPLE__
      (fuse_daemonize(1) != -1)
#else
      (fuse_daemonize(me.config.foreground) != -1)
#endif
     ) {
    me.config.isdebug = 0;

    if (getenv("EOS_FUSE_LOWLEVEL_DEBUG") &&
        (!strcmp(getenv("EOS_FUSE_LOWLEVEL_DEBUG"), "1"))) {
      me.config.isdebug = 1;
    }

    if (me.config.isdebug) {
      XrdCl::DefaultEnv::SetLogLevel("Dump");
      setenv("XRD_LOGLEVEL", "Dump", 1);
    } else {
      // disable backward stacktraces long standing mutexes unless in debug mode
      setenv("EOS_DISABLE_BACKWARD_STACKTRACE", "1", 1);
    }

    me.config.mount_point = local_mount_dir;
    me.config.mountprefix = mountprefix;
    me.config.mounthostport = mounthostport;
    me.fs().setMountPoint(me.config.mount_point);
    me.fs().setPrefix(me.config.mountprefix);
    std::map<std::string, std::string> features;

    try {
      if (!me.fs().init(argc, argv, userdata, &features)) {
        return 1;
      }
    } catch (const std::length_error& e) {
      fprintf(stderr, "error: failed to insert into google map\n");
      return 1;
    }

    me.config.encode_pathname = features.find("eos.encodepath") != features.end();
    me.config.lazy_open = (features.find("eos.lazyopen") != features.end()) ? true :
                          false;
    eos_static_warning("********************************************************************************");
    eos_static_warning("eosd started version %s - FUSE protocol version %d",
                       VERSION, FUSE_USE_VERSION);
    eos_static_warning("eos-instance-url       := %s", getenv("EOS_RDRURL"));
    eos_static_warning("encode-pathname        := %s",
                       me.config.encode_pathname ? "true" : "false");
    eos_static_warning("lazy-open@server       := %s",
                       me.config.lazy_open ? "true" : "false");
    eos_static_warning("inline-repair          := %s max-size=%llu",
                       me.fs().getInlineRepair() ? "true" : "false", me.fs().getMaxInlineRepairSize());
    eos_static_warning("multi-threading        := %s", (getenv("EOS_FUSE_NO_MT") &&
                       (!strcmp(getenv("EOS_FUSE_NO_MT"), "1"))) ? "false" : "true");
    eos_static_warning("kernel-cache           := %s",
                       me.config.kernel_cache ? "true" : "false");
    eos_static_warning("direct-io              := %s",
                       me.config.direct_io ? "true" : "false");
    eos_static_warning("no-access              := %s",
                       me.config.no_access ? "true" : "false");
    eos_static_warning("fsync                  := %s",
                       me.config.is_sync ? "sync" : "async");
    eos_static_warning("attr-cache-timeout     := %.02f seconds",
                       me.config.attrcachetime);
    eos_static_warning("entry-cache-timeout    := %.02f seconds",
                       me.config.entrycachetime);
    eos_static_warning("negative-entry-timeout := %.02f seconds",
                       me.config.neg_entrycachetime);
    me.fs().log_settings();
    struct fuse_session* se;
    se = fuse_lowlevel_new(&args,
                           &(get_operations()),
                           sizeof(operations), NULL);

    if ((se != NULL)) {
      if (fuse_set_signal_handlers(se) != -1) {
        fuse_session_add_chan(se, ch);

        if (getenv("EOS_FUSE_NO_MT") &&
            (!strcmp(getenv("EOS_FUSE_NO_MT"), "1"))) {
          err = fuse_session_loop(se);
        } else {
          err = fuse_session_loop_mt(se);
        }

        fuse_remove_signal_handlers(se);
        fuse_session_remove_chan(ch);
      }

      fuse_session_destroy(se);
    }

    XrdSysThread::Cancel(me.fs().tCacheCleanup);
    XrdSysThread::Join(me.fs().tCacheCleanup, NULL);
    fuse_unmount(local_mount_dir, ch);
  }

  return err ? 1 : 0;
}

void
EosFuse::init(void* userdata, struct fuse_conn_info* conn)
{
  eos_static_debug("");
}

void
EosFuse::destroy(void* userdata)
{
  eos_static_debug("");
// empty
}

void
EosFuse::dirbuf_add(fuse_req_t req,
                    struct dirbuf* b,
                    const char* name,
                    fuse_ino_t ino,
                    const struct stat* s)
{
  struct stat stbuf;
  size_t oldsize = b->size;
  memset(&stbuf, 0, sizeof(stbuf));
  stbuf.st_ino = ino;
  b->size += fuse_add_direntry(req, NULL, 0, name, s, 0);
  b->p = (char*) realloc(b->p, b->size);
  fuse_add_direntry(req, b->p + oldsize, b->size - oldsize, name,
                    &stbuf, b->size);
}

int
EosFuse::reply_buf_limited(fuse_req_t req,
                           const char* buf,
                           size_t bufsize,
                           off_t off,
                           size_t maxsize)
{
  if ((ssize_t) off < (ssize_t) bufsize) {
    return fuse_reply_buf(req, buf + off, std::min((size_t)(bufsize - off),
                          maxsize));
  } else {
    return fuse_reply_buf(req, NULL, 0);
  }
}

void
EosFuse::getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EosFuse& me = instance();
// re-resolve the inode
  ino = me.fs().redirect_i2i(ino);
  struct stat stbuf;
  memset(&stbuf, 0, sizeof(struct stat));
  std::string fullpath;
  UPDATEPROCCACHE;
  me.fs().lock_r_p2i();   // =>
  const char* name = me.fs().path((unsigned long long) ino);

  if (!name || !checkpathname(name)) {
    fuse_reply_err(req, ENOENT);
    me.fs().unlock_r_p2i();   // <=
    return;
  }

  me.fs().getPath(fullpath, me.config.mountprefix, name);
  me.fs().unlock_r_p2i();   // <=
  eos_static_debug("inode=%lld path=%s",
                   (long long) ino, fullpath.c_str());
  int retc = me.fs().stat(fullpath.c_str(), &stbuf, fuse_req_ctx(req)->uid,
                          fuse_req_ctx(req)->gid, fuse_req_ctx(req)->pid, ino);

  if (!retc) {
    eos_static_info("attr-reply %lld %u %u %ld.%ld %ld.%ld",
                    (long long) stbuf.st_ino, stbuf.st_uid, stbuf.st_gid,
                    (long) stbuf.ATIMESPEC.tv_sec, (long) stbuf.ATIMESPEC.tv_nsec,
                    (long) stbuf.MTIMESPEC.tv_sec, (long) stbuf.MTIMESPEC.tv_nsec);
    fuse_reply_attr(req, &stbuf, me.config.attrcachetime);
    me.fs().store_i2mtime(stbuf.st_ino, stbuf.MTIMESPEC);
    eos_static_debug("mode=%x timeout=%.02f\n", stbuf.st_mode,
                     me.config.attrcachetime);
  } else {
    if (ino == 1) {
      // we always return a directory stat for the mount point for autofs
      memset(&stbuf, 0, sizeof(stbuf));
      stbuf.st_mode |= S_IFDIR | S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
      stbuf.st_uid = 0;
      stbuf.st_gid = 0;
      stbuf.st_rdev = 0;
      stbuf.st_size = 4096;
      stbuf.st_blksize = 4096;
      stbuf.st_nlink = 1;
      stbuf.st_ino = 1;
      stbuf.st_dev = 0;
      fuse_reply_attr(req, &stbuf, 0);
    } else {
      fuse_reply_err(req, retc);
    }
  }

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::setattr(fuse_req_t req, fuse_ino_t ino, struct stat* attr, int to_set,
                 struct fuse_file_info* fi)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EosFuse& me = instance();
// re-resolve the inode
  ino = me.fs().redirect_i2i(ino);
  int retc = 0;
  std::string fullpath;
  UPDATEPROCCACHE;
  me.fs().lock_r_p2i();   // =>
  const char* name = me.fs().path((unsigned long long) ino);

  if (!name || !checkpathname(name)) {
    fuse_reply_err(req, ENOENT);
    me.fs().unlock_r_p2i();   // <=
    return;
  }

  me.fs().getPath(fullpath, me.config.mountprefix, name);
  me.fs().unlock_r_p2i();   // <=

  if (to_set & FUSE_SET_ATTR_MODE) {
    struct stat newattr;
    newattr.st_mode = attr->st_mode;
    eos_static_debug("set attr mode ino=%lld", (long long) ino);
    retc = me.fs().chmod(fullpath.c_str(), newattr.st_mode, fuse_req_ctx(req)->uid,
                         fuse_req_ctx(req)->gid, fuse_req_ctx(req)->pid);
  }

  if ((to_set & FUSE_SET_ATTR_UID) && (to_set & FUSE_SET_ATTR_GID)) {
    eos_static_debug("set attr uid  ino=%lld", (long long) ino);
  }

  if (to_set & FUSE_SET_ATTR_SIZE) {
    retc = me.fs().truncate2(fullpath.c_str(), ino, attr->st_size,
                             fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid, fuse_req_ctx(req)->pid);
  }

  if ((to_set & FUSE_SET_ATTR_ATIME) && (to_set & FUSE_SET_ATTR_MTIME)) {
    struct timespec tvp[2];
    tvp[0].tv_sec = attr->ATIMESPEC.tv_sec;
    tvp[0].tv_nsec = attr->ATIMESPEC.tv_nsec;
    tvp[1].tv_sec = attr->MTIMESPEC.tv_sec;
    tvp[1].tv_nsec = attr->MTIMESPEC.tv_nsec;
    eos_static_debug("set attr time ino=%lld atime=%ld mtime=%ld mtime.nsec=%ld",
                     (long long) ino, (long) attr->ATIMESPEC.tv_sec, (long) attr->MTIMESPEC.tv_sec,
                     (long) attr->MTIMESPEC.tv_nsec);

    if ((retc = me.fs().utimes_if_open(ino,
                                       tvp,
                                       fuse_req_ctx(req)->uid,
                                       fuse_req_ctx(req)->gid,
                                       fuse_req_ctx(req)->pid))) {
      retc = me.fs().utimes(fullpath.c_str(), tvp,
                            fuse_req_ctx(req)->uid,
                            fuse_req_ctx(req)->gid,
                            fuse_req_ctx(req)->pid);
    }
  }

  eos_static_debug("return code =%d", retc);
  struct stat newattr;
  memset(&newattr, 0, sizeof(struct stat));

  if (!retc) {
    retc = me.fs().stat(fullpath.c_str(), &newattr, fuse_req_ctx(req)->uid,
                        fuse_req_ctx(req)->gid, fuse_req_ctx(req)->pid, 0/*ino*/);

    if ((to_set & FUSE_SET_ATTR_ATIME) && (to_set & FUSE_SET_ATTR_MTIME)) {
      // return what was set by the client
      newattr.ATIMESPEC.tv_sec = attr->ATIMESPEC.tv_sec;
      newattr.ATIMESPEC.tv_nsec = attr->ATIMESPEC.tv_nsec;
      newattr.MTIMESPEC.tv_sec = attr->MTIMESPEC.tv_sec;
      newattr.MTIMESPEC.tv_nsec = attr->MTIMESPEC.tv_nsec;
      newattr.st_ino = ino;
      me.fs().store_i2mtime(ino, attr->MTIMESPEC);
      eos_static_debug("set attr ino=%lld atime=%ld atime.nsec=%ld mtime=%ld mtime.nsec=%ld",
                       (long long) ino, (long) newattr.ATIMESPEC.tv_sec,
                       (long) newattr.ATIMESPEC.tv_nsec, (long) newattr.MTIMESPEC.tv_sec,
                       (long) newattr.MTIMESPEC.tv_nsec);
    }

    // the stat above bypasses the local consistency cache
    off_t csize = LayoutWrapper::CacheAuthSize(ino);

    if (csize > 0) {
      newattr.st_size = csize;
    }

    if (to_set & FUSE_SET_ATTR_SIZE) {
      newattr.st_size = attr->st_size;
    }

    if (!retc) {
      eos_static_info("attr-reply %lld %u %u %ld.%ld %ld.%ld",
                      (long long) newattr.st_ino, newattr.st_uid, newattr.st_gid,
                      (long) newattr.ATIMESPEC.tv_sec, (long) newattr.ATIMESPEC.tv_nsec,
                      (long) newattr.MTIMESPEC.tv_sec, (long) newattr.MTIMESPEC.tv_nsec);
      fuse_reply_attr(req, &newattr, me.config.attrcachetime);
      eos_static_debug("mode=%x timeout=%.02f\n", newattr.st_mode,
                       me.config.attrcachetime);
    } else {
      fuse_reply_err(req, errno);
    }
  } else {
    fuse_reply_err(req, errno);
  }

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::lookup(fuse_req_t req, fuse_ino_t parent, const char* name)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EosFuse& me = instance();
  int entry_found = 0;
  unsigned long long entry_inode;
  const char* parentpath = NULL;
  std::string fullpath;
  char ifullpath[16384];
  UPDATEPROCCACHE;
  eos_static_debug("name=%s, ino_parent=%llu",
                   name, (unsigned long long) parent);
  me.fs().lock_r_p2i();   // =>
  parentpath = me.fs().path((unsigned long long) parent);

  if (!parentpath || !checkpathname(name)) {
    eos_static_err("pathname=%s checkpathname=%d", parentpath, checkpathname(name));
    fuse_reply_err(req, ENOENT);
    me.fs().unlock_r_p2i();   // <=
    return;
  }

  if (name[0] == '/') {
    sprintf(ifullpath, "%s%s", parentpath, name);
  } else {
    if ((strlen(parentpath) == 1) && (parentpath[0] == '/')) {
      sprintf(ifullpath, "/%s", name);
    } else {
      sprintf(ifullpath, "%s/%s", parentpath, name);
    }
  }

  me.fs().getPath(fullpath, me.config.mountprefix, ifullpath);
  me.fs().unlock_r_p2i();   // <=
  eos_static_debug("parent=%lld path=%s uid=%d",
                   (long long) parent, fullpath.c_str(), fuse_req_ctx(req)->uid);
  entry_inode = me.fs().inode(ifullpath);
  eos_static_debug("entry_found = %lli %s", entry_inode, ifullpath);

  if (entry_inode && (LayoutWrapper::CacheAuthSize(entry_inode) == -1)) {
    struct fuse_entry_param e;
    memset(&e, 0, sizeof(e));
    int rc = me.fs().stat(fullpath.c_str(), &e.attr, fuse_req_ctx(req)->uid,
                          fuse_req_ctx(req)->gid, fuse_req_ctx(req)->pid, entry_inode, true);
    entry_found = me.fs().dir_cache_get_entry(req, parent, entry_inode, ifullpath,
                  rc ? 0 : &e.attr);
    eos_static_debug("subentry_found = %i", entry_found);
  }

  if (!entry_found) {
    struct fuse_entry_param e;
    memset(&e, 0, sizeof(e));
    e.attr_timeout = me.config.attrcachetime;
    e.entry_timeout = me.config.entrycachetime;
    int retc = me.fs().stat(fullpath.c_str(), &e.attr, fuse_req_ctx(req)->uid,
                            fuse_req_ctx(req)->gid, fuse_req_ctx(req)->pid, entry_inode);

    if (!retc) {
      eos_static_debug("storeinode=%lld path=%s",
                       (long long) e.attr.st_ino, ifullpath);
      e.ino = e.attr.st_ino;
      me.fs().store_p2i(e.attr.st_ino, ifullpath);
      eos_static_notice("attr-reply %lld %u %u %ld.%ld %ld.%ld",
                        (long long) e.attr.st_ino, e.attr.st_uid, e.attr.st_gid,
                        (long) e.attr.ATIMESPEC.tv_sec, (long) e.attr.ATIMESPEC.tv_nsec,
                        (long) e.attr.MTIMESPEC.tv_sec, (long) e.attr.MTIMESPEC.tv_nsec);
      fuse_reply_entry(req, &e);
      eos_static_debug("mode=%x timeout=%.02f\n", e.attr.st_mode, e.attr_timeout);
      me.fs().dir_cache_add_entry(parent, e.attr.st_ino, &e);
      me.fs().store_i2mtime(e.attr.st_ino, e.attr.MTIMESPEC);
    } else {
      // Add entry as a negative stat cache entry
      e.ino = 0;
      e.attr_timeout = me.config.neg_entrycachetime;
      e.entry_timeout = me.config.neg_entrycachetime;
      fuse_reply_entry(req, &e);
      eos_static_debug("mode=%x timeout=%.02f\n", e.attr.st_mode, e.attr_timeout);
    }
  }

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EosFuse& me = instance();
  // concurrency monitor
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, ino);
  std::string dirfullpath;
  char fullpath[16384];
  char* name = 0;
  int retc = 0;
  int dir_status = 0;
  size_t cnt = 0;
  struct dirbuf b;
  struct dirbuf* fh_buf;
  struct stat attr {};
  b.size = 0;
  b.p = 0;
  UPDATEPROCCACHE;
  me.fs().lock_r_p2i();   // =>
  const char* tmpname = me.fs().path((unsigned long long) ino);

  if (tmpname) {
    name = strdup(tmpname);
  }

  me.fs().unlock_r_p2i();   // <=

  if (!name || !checkpathname(name)) {
    fuse_reply_err(req, ENOENT);
    free(name);
    return;
  }

  me.fs().getPath(dirfullpath, me.config.mountprefix, name);

  if (me.config.encode_pathname) {
    sprintf(fullpath, "/proc/user/?mgm.cmd=fuse&"
            "mgm.subcmd=inodirlist&eos.encodepath=1&mgm.statentries=1&mgm.path=%s"
            , me.fs().safePath((("/" + me.config.mountprefix) + name).c_str()).c_str());
  } else {
    sprintf(fullpath, "/proc/user/?mgm.cmd=fuse&"
            "mgm.subcmd=inodirlist&mgm.statentries=1&mgm.path=/%s%s",
            me.config.mountprefix.c_str(), name);
  }

  eos_static_debug("inode=%lld path=%s",
                   (long long) ino, dirfullpath.c_str());

  if (me.config.no_access) {
    // if ACCESS is disabled we have to make sure that we can actually read this directory if we are not 'root'
    if (me.fs().access(dirfullpath.c_str(),
                       R_OK | X_OK,
                       fuse_req_ctx(req)->uid,
                       fuse_req_ctx(req)->gid,
                       fuse_req_ctx(req)->pid)) {
      eos_static_err("no access to %s", dirfullpath.c_str());
      fuse_reply_err(req, errno);
      free(name);
      return;
    }
  }

  // No dirview entry, try to use the directory cache
  if ((retc = me.fs().stat(dirfullpath.c_str(), &attr, fuse_req_ctx(req)->uid,
                           fuse_req_ctx(req)->gid, fuse_req_ctx(req)->pid, ino))) {
    eos_static_err("could not stat %s", dirfullpath.c_str());
    fuse_reply_err(req, errno);
    free(name);
    return;
  }

  dir_status = me.fs().dir_cache_get(ino, attr.MTIMESPEC, attr.CTIMESPEC,
                                     &fh_buf);

  if (!dir_status) {
    // Dir not in cache or invalid, fall-back to normal reading
    struct fuse_entry_param* entriesstats = NULL;
    fuse_filesystem::dirlist dlist;
    size_t nstats = 0;
    me.fs().inodirlist((unsigned long long) ino, fullpath, fuse_req_ctx(req)->uid,
                       fuse_req_ctx(req)->gid, fuse_req_ctx(req)->pid, dlist,
                       &entriesstats, &nstats);
    unsigned long long in;

    for (cnt = 0; cnt < dlist.size(); cnt++) {
      in = dlist[cnt];
      std::string bname = me.fs().base_name(in);

      if (cnt == 0) {
        // this is the '.' directory
        bname = ".";
      } else if (cnt == 1) {
        // this is the '..' directory
        bname = "..";
      }

      if (bname.length()) {
        struct stat* buf = NULL;

        if (entriesstats && entriesstats[cnt].attr.st_ino > 0) {
          buf = &entriesstats[cnt].attr;
        }

        dirbuf_add(req, &b, bname.c_str(), (fuse_ino_t) in, buf);
      } else {
        eos_static_err("failed for inode=%llu", in);
      }
    }

    // Add directory to cache or update it
    me.fs().dir_cache_sync(ino, cnt, attr.MTIMESPEC, attr.CTIMESPEC, &b,
                           me.config.attrcachetime * 1000000000l);
    // duplicate the dirbuf response and store in the file handle
    fh_buf = (struct dirbuf*) malloc(sizeof(dirbuf));
    fh_buf->p = (char*) calloc(b.size, sizeof(char));
    fh_buf->p = (char*) memcpy(fh_buf->p, b.p, b.size);
    fh_buf->size = b.size;
    fi->fh = (uint64_t) fh_buf;

    if (entriesstats) {
      // Add the stat to the cache
      for (size_t i = 2; i < nstats; i++) { // the two first ones are . and ..
        entriesstats[i].attr_timeout = me.config.attrcachetime;
        entriesstats[i].entry_timeout = me.config.entrycachetime;
        me.fs().dir_cache_add_entry(ino, entriesstats[i].attr.st_ino, entriesstats + i);
        eos_static_debug("add_entry  %lu  %lu", entriesstats[i].ino,
                         entriesstats[i].attr.st_ino);
      }

      free(entriesstats);
      free(b.p);
    }
  } else {
    fi->fh = (uint64_t) fh_buf;
  }

  free(name);
  fuse_reply_open(req, fi);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                 struct fuse_file_info* fi)
{
  eos::common::Timing timing(__func__);
  EosFuse& me = instance();
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, ino);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");

  if (!fi->fh) {
    fuse_reply_err(req, ENXIO);
    return;
  }

  struct dirbuf* b = (struct dirbuf*)(fi->fh);

  eos_static_debug("return size=%lld ptr=%lld",
                   (long long) b->size, (long long) b->p);

  reply_buf_limited(req, b->p, b->size, off, size);

  COMMONTIMING("_stop_", &timing);

  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi)
{
  eos::common::Timing timing(__func__);
  EosFuse& me = instance();
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, ino, true);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");

  if (!fi->fh) {
    fuse_reply_err(req, ENXIO);
    return;
  }

  struct dirbuf* b = (struct dirbuf*)(fi->fh);

  if (b->p) {
    free(b->p);
  }

  if (fi->fh) {
    free(b);
  }

  fuse_reply_err(req, 0);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::statfs(fuse_req_t req, fuse_ino_t ino)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EosFuse& me = instance();
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, ino);
  int res = 0;
  char* path = NULL;
  struct statvfs svfs, svfs2;
  UPDATEPROCCACHE;
  me.fs().lock_r_p2i();   // =>
  const char* tmppath = me.fs().path((unsigned long long) ino);

  if (tmppath) {
    path = strdup(tmppath);
  }

  me.fs().unlock_r_p2i();   // <=

  if (!path) {
    svfs.f_bsize = 128 * 1024;
    svfs.f_blocks = 1000000000ll;
    svfs.f_bfree = 1000000000ll;
    svfs.f_bavail = 1000000000ll;
    svfs.f_files = 1000000;
    svfs.f_ffree = 1000000;
    fuse_reply_statfs(req, &svfs);
    return;
  }

  std::string rootpath = "/" + me.config.mountprefix + path;
  res = me.fs().statfs(rootpath.c_str(), &svfs2, fuse_req_ctx(req)->uid,
                       fuse_req_ctx(req)->gid, fuse_req_ctx(req)->pid);
  free(path);

  if (res) {
    svfs.f_bsize = 128 * 1024;
    svfs.f_blocks = 1000000000ll;
    svfs.f_bfree = 1000000000ll;
    svfs.f_bavail = 1000000000ll;
    svfs.f_files = 1000000;
    svfs.f_ffree = 1000000;
    fuse_reply_statfs(req, &svfs);
  } else {
    fuse_reply_statfs(req, &svfs2);
  }

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::mkdir(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EosFuse& me = instance();
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, parent);
  std::string parentpath;
  std::string fullpath;
  UPDATEPROCCACHE;
  me.fs().lock_r_p2i();   // =>
  const char* tmp = me.fs().path((unsigned long long) parent);

  if (!tmp || !checkpathname(name)) {
    fuse_reply_err(req, ENOENT);
    me.fs().unlock_r_p2i();   // <=
    return;
  }

  parentpath = tmp;
  std::string ifullpath;
  ifullpath = parentpath + "/" + name;
  fullpath = "/" + me.config.mountprefix + parentpath + "/" + name;
  me.fs().unlock_r_p2i();   // <=
  eos_static_debug("path=%s", fullpath.c_str());
  struct fuse_entry_param e;
  memset(&e, 0, sizeof(e));
  e.attr_timeout = me.config.attrcachetime;
  e.entry_timeout = me.config.entrycachetime;
  int retc = me.fs().mkdir(fullpath.c_str(),
                           mode,
                           fuse_req_ctx(req)->uid,
                           fuse_req_ctx(req)->gid,
                           fuse_req_ctx(req)->pid,
                           &e.attr);

  if (!retc) {
    e.ino = e.attr.st_ino;
    me.fs().store_p2i((unsigned long long) e.attr.st_ino, ifullpath.c_str());
    const char* ptr = strrchr(parentpath.c_str(), (int)('/'));

    if (ptr) {
      size_t sz = 16384;
      char gparent[sz];
      int num = (int)(ptr - parentpath.c_str());

      if (num) {
        strncpy(gparent, parentpath.c_str(), num);
        gparent[num] = '\0';
        ptr = strrchr(gparent, (int)('/'));

        if (ptr && ptr != gparent) {
          num = (int)(ptr - gparent);
          strncpy(gparent, parentpath.c_str(), num);
          parentpath[num] = '\0';
          size_t len = std::min(sz, parentpath.length());
          strncpy(gparent, parentpath.c_str(), len);
          gparent[len] = '\0';
        }
      } else {
        strcpy(gparent, "/\0");
      }

      unsigned long long ino_gparent = me.fs().inode(gparent);
      me.fs().dir_cache_forget(ino_gparent);
    }

    fuse_reply_entry(req, &e);
    eos_static_debug("mode=%x timeout=%.02f\n", e.attr.st_mode, e.attr_timeout);
  } else {
    fuse_reply_err(req, errno);
  }

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::unlink(fuse_req_t req, fuse_ino_t parent, const char* name)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EosFuse& me = instance();
  fuse_filesystem::Track::Monitor pmon(__func__, me.fs().iTrack, parent);
  const char* parentpath = 0;
  std::string fullpath;
  char ifullpath[16384];
  unsigned long long ino;
  UPDATEPROCCACHE;
#ifndef __APPLE__

  if (me.fs().is_toplevel_rm(fuse_req_ctx(req)->pid,
                             me.config.mount_point.c_str()) == 1) {
    fuse_reply_err(req, EPERM);
    return;
  }

#endif
  me.fs().lock_r_p2i();   // =>
  parentpath = me.fs().path((unsigned long long) parent);

  if (!parentpath || !checkpathname(name)) {
    fuse_reply_err(req, ENOENT);
    me.fs().unlock_r_p2i();   // <=
    return;
  }

  me.fs().getPPath(fullpath, me.config.mountprefix, parentpath, name);

  if (name[0] == '/') {
    sprintf(ifullpath, "%s%s", parentpath, name);
  } else {
    if ((strlen(parentpath) == 1) && (parentpath[0] == '/')) {
      sprintf(ifullpath, "/%s", name);
    } else {
      sprintf(ifullpath, "%s/%s", parentpath, name);
    }
  }

  ino = me.fs().inode(ifullpath);
  me.fs().unlock_r_p2i();   // <=
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, ino, true);
  eos_static_debug("path=%s ipath=%s inode=%llu", fullpath.c_str(), ifullpath,
                   ino);
  me.fs().dir_cache_forget(parent);
  int retc = me.fs().unlink(fullpath.c_str(), fuse_req_ctx(req)->uid,
                            fuse_req_ctx(req)->gid, fuse_req_ctx(req)->pid, ino);

  if (!retc) {
    me.fs().forget_p2i((unsigned long long) ino);
    fuse_reply_buf(req, NULL, 0);
  } else {
    fuse_reply_err(req, errno);
  }

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::rmdir(fuse_req_t req, fuse_ino_t parent, const char* name)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EosFuse& me = instance();
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, parent);
  const char* parentpath = NULL;
  std::string fullpath;
  char ifullpath[16384];
  unsigned long long ino;
  UPDATEPROCCACHE;

  if (me.fs().is_toplevel_rm(fuse_req_ctx(req)->pid,
                             me.config.mount_point.c_str()) == 1) {
    fuse_reply_err(req, EPERM);
    return;
  }

  me.fs().lock_r_p2i();   // =>
  parentpath = me.fs().path((unsigned long long) parent);

  if (!parentpath || !checkpathname(name)) {
    fuse_reply_err(req, ENOENT);
    me.fs().unlock_r_p2i();   // <=
    return;
  }

  me.fs().getPPath(fullpath, me.config.mountprefix, parentpath, name);
  me.fs().unlock_r_p2i();   // <=
  eos_static_debug("path=%s", fullpath.c_str());
  int retc = me.fs().rmdir(fullpath.c_str(),
                           fuse_req_ctx(req)->uid,
                           fuse_req_ctx(req)->gid,
                           fuse_req_ctx(req)->pid);

  if ((strlen(parentpath) == 1) && (parentpath[0] == '/')) {
    sprintf(ifullpath, "/%s", name);
  } else {
    sprintf(ifullpath, "%s/%s", parentpath, name);
  }

  ino = me.fs().inode(ifullpath);
  me.fs().dir_cache_forget((unsigned long long) parent);

  if (!retc) {
    if (ino) {
      me.fs().forget_p2i((unsigned long long) ino);
    }

    fuse_reply_err(req, 0);
  } else {
    if (errno == ENOSYS) {
      fuse_reply_err(req, ENOTEMPTY);
    } else {
      fuse_reply_err(req, errno);
    }
  }

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

#ifdef _FUSE3
void
EosFuse::rename(fuse_req_t req, fuse_ino_t parent, const char* name,
                fuse_ino_t newparent, const char* newname, unsigned int flags)
#else
void
EosFuse::rename(fuse_req_t req, fuse_ino_t parent, const char* name,
                fuse_ino_t newparent, const char* newname)
#endif
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("oldparent=%llu newparent=%llu oldname=%s newname=%s",
                   (unsigned long long)parent, (unsigned long long)newparent, name, newname);
  EosFuse& me = instance();
  fuse_filesystem::Track::Monitor monp(__func__, me.fs().iTrack, parent);
  fuse_filesystem::Track::Monitor monn(__func__, me.fs().iTrack, newparent);
  const char* parentpath = NULL;
  const char* newparentpath = NULL;
  std::string fullpath;
  std::string newfullpath;
  char iparentpath[16384];
  char ipath[16384];
  UPDATEPROCCACHE;
  me.fs().lock_r_p2i();   // =>
  parentpath = me.fs().path((unsigned long long) parent);

  if (!parentpath || !checkpathname(name)) {
    fuse_reply_err(req, ENOENT);
    me.fs().unlock_r_p2i();   // <=
    return;
  }

  newparentpath = me.fs().path((unsigned long long) newparent);

  if (!newparentpath || !checkpathname(newname)) {
    fuse_reply_err(req, ENOENT);
    me.fs().unlock_r_p2i();   // <=
    return;
  }

  me.fs().getPPath(fullpath, me.config.mountprefix, parentpath, name);
  me.fs().getPPath(newfullpath, me.config.mountprefix, newparentpath, newname);
  sprintf(ipath, "%s/%s", parentpath, name);
  sprintf(iparentpath, "%s/%s", newparentpath, newname);
  me.fs().unlock_r_p2i();   // <=
  struct stat stbuf;
  int retcold = me.fs().stat(fullpath.c_str(), &stbuf, fuse_req_ctx(req)->uid,
                             fuse_req_ctx(req)->gid, fuse_req_ctx(req)->pid, 0);
  eos_static_debug("path=%s newpath=%s inode=%llu op=%llu np=%llu [%d]",
                   fullpath.c_str(), newfullpath.c_str(), (unsigned long long) stbuf.st_ino,
                   (unsigned long long) parent, (unsigned long long) newparent, retcold);
  int retc = 0;
  fuse_filesystem::Track::Monitor mone(__func__, me.fs().iTrack, stbuf.st_ino, true);
  retc = me.fs().rename(fullpath.c_str(), newfullpath.c_str(),
                        fuse_req_ctx(req)->uid,
                        fuse_req_ctx(req)->gid, fuse_req_ctx(req)->pid);

  if (!retc) {
    if (!retcold) {
      eos_static_debug("forgetting inode=%llu storing as %s",
                       (unsigned long long) stbuf.st_ino, iparentpath);
      me.fs().dir_cache_forget((unsigned long long) parent);

      if (parent != newparent) {
        me.fs().dir_cache_forget((unsigned long long) newparent);
      }

      me.fs().forget_p2i((unsigned long long) stbuf.st_ino);
      me.fs().store_p2i((unsigned long long) stbuf.st_ino, iparentpath);

      // if a directory is renamed we have to replace the whole map by prefix
      if (S_ISDIR(stbuf.st_mode)) {
        // if a directory is renamed we have to replace the whole map by prefix
        me.fs().replace_prefix(ipath, iparentpath);
      }
    }

    fuse_reply_err(req, 0);
  } else {
    fuse_reply_err(req, errno);
  }

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::access(fuse_req_t req, fuse_ino_t ino, int mask)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EosFuse& me = instance();
// re-resolve the inode
  ino = me.fs().redirect_i2i(ino);
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, ino);
  std::string fullpath;
  const char* name = NULL;
  UPDATEPROCCACHE;
  me.fs().lock_r_p2i();   // =>
  name = me.fs().path((unsigned long long) ino);

  if (!name || !checkpathname(name)) {
    fuse_reply_err(req, ENOENT);
    me.fs().unlock_r_p2i();   // <=
    return;
  }

  me.fs().getPath(fullpath, me.config.mountprefix, name);
  me.fs().unlock_r_p2i();   // <=
  eos_static_debug("inode=%lld path=%s",
                   (long long) ino, fullpath.c_str());

  if ((getenv("EOS_FUSE_NOACCESS")) &&
      (!strcmp(getenv("EOS_FUSE_NOACCESS"), "1"))) {
    fuse_reply_err(req, 0);
    return;
  }

// this is useful only if krb5 is not enabled
  uid_t fsuid = fuse_req_ctx(req)->uid;
  gid_t fsgid = fuse_req_ctx(req)->gid;
  gProcCache(fuse_req_ctx(req)->pid).GetFsUidGid(fuse_req_ctx(req)->pid, fsuid,
      fsgid);
  int retc = me.fs().access(fullpath.c_str(), mask, fsuid,
                            fsgid, fuse_req_ctx(req)->pid);

  if (!retc) {
    fuse_reply_err(req, 0);
  } else {
    fuse_reply_err(req, errno);
  }

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EosFuse& me = instance();
// re-resolve the inode
  ino = me.fs().redirect_i2i(ino);
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, ino, true);
  int res;
  mode_t mode = 0;
  std::string fullpath;
  const char* name = NULL;
  UPDATEPROCCACHE;
  me.fs().lock_r_p2i();   // =>
  name = me.fs().path((unsigned long long) ino);

  if (!name || !checkpathname(name)) {
    fuse_reply_err(req, ENOENT);
    me.fs().unlock_r_p2i();   // <=
    return;
  }

  me.fs().getPath(fullpath, me.config.mountprefix, name);
  me.fs().unlock_r_p2i();   // <=

  if (fi->flags & (O_RDWR | O_WRONLY | O_CREAT)) {
    mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  }

  unsigned long long rino = ino;
// Do open
  res = me.fs().open(fullpath.c_str(), fi->flags, mode, fuse_req_ctx(req)->uid,
                     fuse_req_ctx(req)->gid, fuse_req_ctx(req)->pid, &rino);
  eos_static_debug("inode=%lld path=%s res=%d",
                   (long long) ino, fullpath.c_str(), res);

  if (rino != (unsigned long long) ino) {
    // this indicates a repaired file
    eos_static_notice("migrating inode=%llu to inode=%llu after repair", ino, rino);
    me.fs().redirect_p2i(ino, rino);
  }

  if (res == -1) {
    fuse_reply_err(req, errno);
    return;
  }

  fuse_filesystem::fd_user_info* info = (fuse_filesystem::fd_user_info*) calloc(1,
                                   sizeof(fuse_filesystem::fd_user_info));
  info->fd = res;
  info->uid = fuse_req_ctx(req)->uid;
  info->gid = fuse_req_ctx(req)->gid;
  info->pid = fuse_req_ctx(req)->pid;
  fi->fh = (uint64_t) info;

  if (me.config.kernel_cache) {
    // TODO: this should be improved
    if (strstr(fullpath.c_str(), "/proc/")) {
      fi->keep_cache = 0;
    } else {
      // If we created this file, we keep our cache
      if (LayoutWrapper::CacheAuthSize(ino) >= 0) {
        fi->keep_cache = 1;
      } else {
        fi->keep_cache = me.fs().store_open_i2mtime(ino);
      }

      eos_static_debug("ino=%lx keep-cache=%d", ino, fi->keep_cache);
    }
  } else {
    fi->keep_cache = 0;
  }

  if (me.config.direct_io) {
    fi->direct_io = 1;
  } else {
    fi->direct_io = 0;
  }

  fuse_reply_open(req, fi);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::mknod(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode,
               dev_t rdev)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");

  if (!S_ISREG(mode)) {
    // we only imnplement files
    fuse_reply_err(req, ENOSYS);
    return;
  }

  struct fuse_file_info fi;

  return create(req, parent, name, mode | S_IFBLK, &fi);
}

void
EosFuse::create(fuse_req_t req, fuse_ino_t parent, const char* name,
                mode_t mode, struct fuse_file_info* fi)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EosFuse& me = instance();
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, parent, true);
  int res;
  unsigned long long rinode = 0;
  bool mknod = false;

  if (mode & S_IFBLK) {
    mode &= (~S_IFBLK);
    mknod = true;
  }

  if (S_ISREG(mode)) {
    const char* parentpath = NULL;
    std::string fullpath;
    char ifullpath[16384];
    UPDATEPROCCACHE;
    me.fs().lock_r_p2i();   // =>
    parentpath = me.fs().path((unsigned long long) parent);

    if (!parentpath || !checkpathname(parentpath) || !checkpathname(name)) {
      fuse_reply_err(req, ENOENT);
      me.fs().unlock_r_p2i();   // <=
      return;
    }

    me.fs().getPPath(fullpath, me.config.mountprefix, parentpath, name);

    if ((strlen(parentpath) == 1) && (parentpath[0] == '/')) {
      sprintf(ifullpath, "/%s", name);
    } else {
      sprintf(ifullpath, "%s/%s", parentpath, name);
    }

#ifdef __APPLE__
    // OSX calls several creates and we have to do the EEXIST check here
    eos_static_info("apple check");

    if (me.fs().inode(ifullpath)) {
      eos_static_info("apple check - EEXIST");
      fuse_reply_err(req, EEXIST);
      me.fs().unlock_r_p2i();   // <=
      return;
    }

#endif
    me.fs().unlock_r_p2i();   // <=
    eos_static_debug("parent=%lld path=%s uid=%d",
                     (long long) parent, fullpath.c_str(), fuse_req_ctx(req)->uid);
    res = me.fs().open(fullpath.c_str(), O_CREAT | O_EXCL | O_RDWR,
                       mode,
                       fuse_req_ctx(req)->uid,
                       fuse_req_ctx(req)->gid,
                       fuse_req_ctx(req)->pid,
                       &rinode,
                       mknod);

    if (res == -1) {
      fuse_reply_err(req, errno);
      return;
    }

    // Update file information structure
    fuse_filesystem::fd_user_info* info = (fuse_filesystem::fd_user_info*) calloc(1,
                                     sizeof(fuse_filesystem::fd_user_info));
    info->fd = res;
    info->uid = fuse_req_ctx(req)->uid;
    info->gid = fuse_req_ctx(req)->gid;
    info->pid = fuse_req_ctx(req)->pid;
    fi->fh = (uint64_t) info;
    struct fuse_entry_param e;
    memset(&e, 0, sizeof(e));
    //   e.attr_timeout = me.config.attrcachetime;
    e.attr_timeout = 0 ;
    e.entry_timeout = me.config.entrycachetime;
    e.ino = rinode;
    e.attr.st_mode = S_IFREG | mode | me.fs().get_mode_overlay();
    e.attr.st_uid = fuse_req_ctx(req)->uid;
    e.attr.st_gid = fuse_req_ctx(req)->gid;
    e.attr.st_dev = 0;
    e.attr.st_atime = e.attr.st_mtime = e.attr.st_ctime = time(NULL);
    eos_static_debug("update inode=%llu", __FUNCTION__, (unsigned long long) e.ino);

    if (!rinode) {
      me.fs().close(res, 0, fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid,
                    fuse_req_ctx(req)->pid);
      fuse_reply_err(req, EPROTO);
      return;
    } else {
      me.fs().store_p2i((unsigned long long) e.ino, ifullpath);
      eos_static_debug("storeinode=%lld path=%s",
                       (long long) e.ino, ifullpath);

      if (me.config.kernel_cache) {
        // TODO: this should be improved
        if (strstr(fullpath.c_str(), "/proc/")) {
          fi->keep_cache = 0;
        } else {
          fi->keep_cache = 1;
        }
      } else {
        fi->keep_cache = 0;
      }

      if (me.config.direct_io) {
        fi->direct_io = 1;
      } else {
        fi->direct_io = 0;
      }

      if (mknod) {
        fuse_reply_entry(req, &e);
      } else {
        fuse_reply_create(req, &e, fi);
      }

      eos_static_debug("mode=%x timeout=%.02f\n", e.attr.st_mode, e.attr_timeout);
      COMMONTIMING("_stop_", &timing);
      eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
      return;
    }
  }

  fuse_reply_err(req, EINVAL);
}

void
EosFuse::read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
              struct fuse_file_info* fi)
{
  eos_static_debug("");
  EosFuse& me = instance();
  eos_static_debug("inode=%llu size=%li off=%llu",
                   (unsigned long long) ino, size, (unsigned long long) off);

  if (fi && fi->fh) {
    fuse_filesystem::fd_user_info* info = (fuse_filesystem::fd_user_info*) fi->fh;
    char* buf = me.fs().attach_rd_buff(thread_id(), size);
    eos_static_debug("inode=%lld size=%lld off=%lld buf=%lld fh=%lld",
                     (long long) ino, (long long) size,
                     (long long) off, (long long) buf, (long long) info->fd);
    int res = me.fs().pread(info->fd, buf, size, off);

    if (res == -1) {
      if (errno == ENOSYS) {
        errno = EIO;
      }

      fuse_reply_err(req, errno);
      return;
    }

    fuse_reply_buf(req, buf, res);
  } else {
    fuse_reply_err(req, ENXIO);
  }

  return;
}

void
EosFuse::write(fuse_req_t req, fuse_ino_t ino, const char* buf, size_t size,
               off_t off, struct fuse_file_info* fi)
{
  eos_static_debug("");
  EosFuse& me = instance();
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, ino, true);

  if (fi && fi->fh) {
    fuse_filesystem::fd_user_info* info = (fuse_filesystem::fd_user_info*) fi->fh;
    eos_static_debug("inode=%lld size=%lld off=%lld buf=%lld fh=%lld",
                     (long long) ino, (long long) size,
                     (long long) off, (long long) buf, (long long) info->fd);
    int res = me.fs().pwrite(info->fd, buf, size, off);

    if (res == -1) {
      if (errno == ENOSYS) {
        errno = EIO;
      }

      fuse_reply_err(req, errno);
      return;
    }

    fuse_reply_write(req, res);
  } else {
    fuse_reply_err(req, ENXIO);
  }

  return;
}

void
EosFuse::release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EosFuse& me = instance();
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, ino, true);
  errno = 0;

  if (fi && fi->fh) {
    fuse_filesystem::fd_user_info* info = (fuse_filesystem::fd_user_info*) fi->fh;
    int fd = info->fd;
    eos_static_debug("inode=%lld fh=%lld",
                     (long long) ino, (long long) fd);
    eos_static_debug("try to close file fd=%llu", info->fd);
    me.fs().close(info->fd, ino, info->uid, info->gid, info->pid);
    // Free memory
    free(info);
    fi->fh = 0;
    unsigned long long new_inode;

    // evt. call the inode migration procedure in the cache and lookup tables
    if ((new_inode = LayoutWrapper::CacheRestore(ino))) {
      eos_static_notice("migrating inode=%llu to inode=%llu after restore", ino,
                        new_inode);
      me.fs().redirect_p2i(ino, new_inode);
    }
  }

  fuse_reply_err(req, errno);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
               struct fuse_file_info* fi)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EosFuse& me = instance();

  if (!me.config.is_sync) {
    fuse_reply_err(req, 0);
    return;
  }

// concurrency monitor
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, ino);

  if (fi && fi->fh) {
    fuse_filesystem::fd_user_info* info = (fuse_filesystem::fd_user_info*) fi->fh;
    eos_static_debug("inode=%lld fh=%lld",
                     (long long) ino, (long long) info->fd);

    if (me.fs().fsync(info->fd)) {
      fuse_reply_err(req, errno);
      return;
    }
  }

  fuse_reply_err(req, 0);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::forget(fuse_req_t req, fuse_ino_t ino, unsigned long nlookup)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EosFuse& me = instance();
  eos_static_debug("inode=%lld",
                   (long long) ino);
  me.fs().iTrack.forget(ino);
  me.fs().forget_p2i((unsigned long long) ino);
  fuse_reply_none(req);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EosFuse& me = instance();
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, ino, true);
  errno = 0;

  if (fi && fi->fh) {
    fuse_filesystem::fd_user_info* info = (fuse_filesystem::fd_user_info*) fi->fh;
    int err_flush = me.fs().flush(info->fd, fuse_req_ctx(req)->uid,
                                  fuse_req_ctx(req)->gid, fuse_req_ctx(req)->pid);

    if (err_flush) {
      errno = EIO;
    }
  }

  fuse_reply_err(req, errno);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f errno=%d", __FUNCTION__, timing.RealTime(),
                    errno);
}

#ifdef __APPLE__
void EosFuse::getxattr(fuse_req_t req, fuse_ino_t ino, const char* xattr_name,
                       size_t size, uint32_t position)
#else
void
EosFuse::getxattr(fuse_req_t req, fuse_ino_t ino, const char* xattr_name,
                  size_t size)
#endif
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");

  if (getenv("EOS_FUSE_XATTR_ENOSYS")) {
    if ((!strcmp(xattr_name, "system.posix_acl_access")) ||
        (!strcmp(xattr_name, "system.posix_acl_default") ||
         (!strcmp(xattr_name, "security.capability")))) {
      // Filter out specific requests to increase performance
      fuse_reply_err(req, ENOSYS);
      return;
    }
  }

  XrdOucString xa = xattr_name;

// exclude security attributes
  if (xa.beginswith("security.")) {
    fuse_reply_err(req, ENOATTR);
    return;
  }

  if (xa.beginswith("system.posix_acl")) {
    fuse_reply_err(req, ENOATTR);
    return;
  }

  EosFuse& me = instance();
// re-resolve the inode
  ino = me.fs().redirect_i2i(ino);
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, ino);
  int retc = 0;
  size_t init_size = size;
  std::string fullpath;
  const char* name = NULL;
  UPDATEPROCCACHE;
  me.fs().lock_r_p2i();   // =>
  name = me.fs().path((unsigned long long) ino);

  if (!name || !checkpathname(name)) {
    fuse_reply_err(req, ENOENT);
    me.fs().unlock_r_p2i();   // <=
    return;
  }

  me.fs().getPath(fullpath, me.config.mountprefix, name);
  me.fs().unlock_r_p2i();   // <=
  eos_static_debug("inode=%lld path=%s",
                   (long long) ino, fullpath.c_str());
  char* xattr_value = NULL;
  retc = me.fs().getxattr(fullpath.c_str(), xattr_name, &xattr_value, &size,
                          fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid, fuse_req_ctx(req)->pid);

  if (retc) {
    fuse_reply_err(req, ENOATTR);
  } else {
    if (init_size) {
      if (init_size < size) {
        fuse_reply_err(req, ERANGE);
      } else {
        fuse_reply_buf(req, xattr_value, size);
      }
    } else {
      fuse_reply_xattr(req, size);
    }
  }

  if (xattr_value) {
    free(xattr_value);
  }

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

#ifdef __APPLE__
void EosFuse::setxattr(fuse_req_t req, fuse_ino_t ino, const char* xattr_name,
                       const char* xattr_value, size_t size, int flags, uint32_t position)
#else

void
EosFuse::setxattr(fuse_req_t req, fuse_ino_t ino, const char* xattr_name,
                  const char* xattr_value, size_t size, int flags)
#endif
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  XrdOucString xa = xattr_name;

// exclude security attributes
  if (xa.beginswith("security.")) {
    fuse_reply_err(req, 0);
    return;
  }

  if (xa.beginswith("system.posix_acl")) {
    fuse_reply_err(req, 0);
    return;
  }

#ifdef __APPLE__

  if (xa.beginswith("com.apple")) {
    fuse_reply_err(req, 0);
    return;
  }

#endif
  EosFuse& me = instance();
// re-resolve the inode
  ino = me.fs().redirect_i2i(ino);
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, ino, true);
  int retc = 0;
  std::string fullpath;
  const char* name = NULL;
  UPDATEPROCCACHE;
  me.fs().lock_r_p2i();   // =>
  name = me.fs().path((unsigned long long) ino);

  if (!name || !checkpathname(name)) {
    fuse_reply_err(req, ENOENT);
    me.fs().unlock_r_p2i();   // <=
    return;
  }

  me.fs().getPath(fullpath, me.config.mountprefix, name);
  me.fs().unlock_r_p2i();   // <=
  eos_static_debug("inode=%lld path=%s",
                   (long long) ino, fullpath.c_str());
  retc = me.fs().setxattr(fullpath.c_str(), xattr_name, xattr_value, size,
                          fuse_req_ctx(req)->uid, fuse_req_ctx(req)->gid, fuse_req_ctx(req)->pid);
  eos_static_debug("setxattr_retc=%i", retc);
  fuse_reply_err(req, retc);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::listxattr(fuse_req_t req, fuse_ino_t ino, size_t size)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EosFuse& me = instance();
// re-resolve the inode
  ino = me.fs().redirect_i2i(ino);
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, ino);
  int retc = 0;
  size_t init_size = size;
  std::string fullpath;
  const char* name = NULL;
  UPDATEPROCCACHE;
  me.fs().lock_r_p2i();   // =>
  name = me.fs().path((unsigned long long) ino);

  if (!name || !checkpathname(name)) {
    fuse_reply_err(req, ENOENT);
    me.fs().unlock_r_p2i();   // <=
    return;
  }

  me.fs().getPath(fullpath, me.config.mountprefix, name);
  me.fs().unlock_r_p2i();   // <=
  eos_static_debug("inode=%lld path=%s",
                   (long long) ino, fullpath.c_str());
  char* xattr_list = NULL;
  retc = me.fs().listxattr(fullpath.c_str(), &xattr_list, &size,
                           fuse_req_ctx(req)->uid,
                           fuse_req_ctx(req)->gid, fuse_req_ctx(req)->pid);

  if (retc) {
    fuse_reply_err(req, retc);
  } else {
    if (init_size) {
      if (init_size < size) {
        fuse_reply_err(req, ERANGE);
      } else {
        fuse_reply_buf(req, xattr_list, size);
      }
    } else {
      fuse_reply_xattr(req, size);
    }
  }

  if (xattr_list) {
    free(xattr_list);
  }

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::removexattr(fuse_req_t req, fuse_ino_t ino, const char* xattr_name)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  XrdOucString xa = xattr_name;

// exclude security attributes
  if (xa.beginswith("security.")) {
    fuse_reply_err(req, 0);
    return;
  }

  if (xa.beginswith("system.posix_acl")) {
    fuse_reply_err(req, 0);
    return;
  }

  EosFuse& me = instance();
// re-resolve the inode
  ino = me.fs().redirect_i2i(ino);
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, ino);
  int retc = 0;
  std::string fullpath;
  const char* name = NULL;
  UPDATEPROCCACHE;
  me.fs().lock_r_p2i();   // =>
  name = me.fs().path((unsigned long long) ino);

  if (!name || !checkpathname(name)) {
    fuse_reply_err(req, ENOENT);
    me.fs().unlock_r_p2i();   // <=
    return;
  }

  me.fs().getPath(fullpath, me.config.mountprefix, name);
  me.fs().unlock_r_p2i();   // <=
  eos_static_debug("inode=%lld path=%s",
                   (long long) ino, fullpath.c_str());
  retc = me.fs().rmxattr(fullpath.c_str(), xattr_name, fuse_req_ctx(req)->uid,
                         fuse_req_ctx(req)->gid, fuse_req_ctx(req)->pid);
  fuse_reply_err(req, retc);
  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::readlink(fuse_req_t req, fuse_ino_t ino)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EosFuse& me = instance();
// re-resolve the inode
  ino = me.fs().redirect_i2i(ino);
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, ino);
  std::string fullpath;
  const char* name = NULL;
  UPDATEPROCCACHE;
  me.fs().lock_r_p2i();   // =>
  name = me.fs().path((unsigned long long) ino);

  if (!name || !checkpathname(name)) {
    fuse_reply_err(req, ENOENT);
    me.fs().unlock_r_p2i();   // <=
    return;
  }

  me.fs().getPath(fullpath, me.config.mountprefix, name);
  me.fs().unlock_r_p2i();   // <=
  char linkbuffer[8912];
  int retc = me.fs().readlink(fullpath.c_str(), linkbuffer, sizeof(linkbuffer),
                              fuse_req_ctx(req)->uid,
                              fuse_req_ctx(req)->gid,
                              fuse_req_ctx(req)->pid);

  if (!retc) {
    fuse_reply_readlink(req, linkbuffer);
    return;
  } else {
    fuse_reply_err(req, errno);
    return;
  }

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}

void
EosFuse::symlink(fuse_req_t req, const char* link, fuse_ino_t parent,
                 const char* name)
{
  eos::common::Timing timing(__func__);
  COMMONTIMING("_start_", &timing);
  eos_static_debug("");
  EosFuse& me = instance();
  fuse_filesystem::Track::Monitor mon(__func__, me.fs().iTrack, parent);
  const char* parentpath = NULL;
  char partialpath[16384];
  std::string fullpath;
  char ifullpath[16384];
  UPDATEPROCCACHE;
  me.fs().lock_r_p2i();   // =>
  parentpath = me.fs().path((unsigned long long) parent);

  if (!parentpath || !checkpathname(parentpath) || !checkpathname(name)) {
    fuse_reply_err(req, ENOENT);
    me.fs().unlock_r_p2i();   // <=
    return;
  }

  sprintf(partialpath, "/%s%s/%s", me.config.mountprefix.c_str(), parentpath,
          name);
  me.fs().getPPath(fullpath, me.config.mountprefix, parentpath, name);

  if ((strlen(parentpath) == 1) && (parentpath[0] == '/')) {
    sprintf(ifullpath, "/%s", name);
  } else {
    sprintf(ifullpath, "%s/%s", parentpath, name);
  }

  me.fs().unlock_r_p2i();   // <=
  eos_static_debug("path=%s link=%s", fullpath.c_str(), link);
  int retc = me.fs().symlink(fullpath.c_str(),
                             link,
                             fuse_req_ctx(req)->uid,
                             fuse_req_ctx(req)->gid,
                             fuse_req_ctx(req)->pid);

  if (!retc) {
    struct fuse_entry_param e;
    memset(&e, 0, sizeof(e));
    e.attr_timeout = me.config.attrcachetime;
    e.entry_timeout = me.config.entrycachetime;
    int retc = me.fs().stat(fullpath.c_str(), &e.attr,
                            fuse_req_ctx(req)->uid,
                            fuse_req_ctx(req)->gid,
                            fuse_req_ctx(req)->pid, 0);

    if (!retc) {
      eos_static_debug("storeinode=%lld path=%s", (long long) e.attr.st_ino,
                       ifullpath);
      e.ino = e.attr.st_ino;
      me.fs().store_p2i((unsigned long long) e.attr.st_ino, ifullpath);
      fuse_reply_entry(req, &e);
      eos_static_debug("mode=%x timeout=%.02f\n", e.attr.st_mode, e.attr_timeout);
      return;
    } else {
      fuse_reply_err(req, errno);
      return;
    }
  } else {
    fuse_reply_err(req, errno);
    return;
  }

  COMMONTIMING("_stop_", &timing);
  eos_static_notice("RT %-16s %.04f", __FUNCTION__, timing.RealTime());
}
