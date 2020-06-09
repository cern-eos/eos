//------------------------------------------------------------------------------
//! @file md.cc
//! @author Andreas-Joachim Peters CERN
//! @brief meta data handling class
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

#include "eosfuse.hh"
#include "md/md.hh"
#include "kv/kv.hh"
#include "cap/cap.hh"
#include "md/kernelcache.hh"
#include "misc/MacOSXHelper.hh"
#include "misc/longstring.hh"
#include "common/Logging.hh"
#include "common/Path.hh"
#include "common/StackTrace.hh"
#include "common/StringConversion.hh"
#include "common/Path.hh"
#include <iostream>
#include <sstream>
#include <vector>
#include <thread>
#include <memory>
#include <functional>
#include <assert.h>
#include <google/protobuf/util/json_util.h>

/* -------------------------------------------------------------------------- */
metad::metad() : mdflush(0), mdqueue_max_backlog(1000),
  z_ctx(0), z_socket(0)
{
  // make a mapping for inode 1, it is re-loaded afterwards in init '/'
  {
    inomap.insert(1, 1);
  }
  shared_md md = std::make_shared<mdx>(1);
  md->set_nlink(1);
  md->set_mode(S_IRWXU | S_IRWXG | S_IRWXO | S_IFDIR);
  md->set_name(":root:");
  md->set_pid(1);
  stat.inodes_inc();
  stat.inodes_ever_inc();
  set_is_visible(0);
  mdbackend = 0;
  mdmap.insertTS(1, md);
}

/* -------------------------------------------------------------------------- */
metad::~metad()
{
  if (z_socket) {
    delete z_socket;
  }

  if (z_ctx) {
    delete z_ctx;
  }
}

/* -------------------------------------------------------------------------- */
void
metad::init(backend* _mdbackend)
{
  mdbackend = _mdbackend;
  std::string mdstream;
  // load the root node
  fuse_req_t req = 0;
  XrdSysMutexHelper mLock(mdmap);
  update(req, mdmap[1], "", true);
  mdmap.init(EosFuse::Instance().getKV());
  dentrymessaging = false;
  writesizeflush = false;
  appname = false;
  mdquery = false;
  serverversion = "<unkown>";
}

/* -------------------------------------------------------------------------- */
int
metad::connect(std::string zmqtarget, std::string zmqidentity,
               std::string zmqname, std::string zmqclienthost,
               std::string zmqclientuuid)
{
  set_zmq_wants_to_connect(1);
  std::lock_guard<std::mutex> connectionMutex(zmq_socket_mutex);

  if (z_socket && z_socket->connected() && (zmqtarget != zmq_target)) {
    // delete the exinsting ZMQ connection
    delete z_socket;
    delete z_ctx;
  }

  if (zmqtarget.length()) {
    zmq_target = zmqtarget;
  }

  if (zmqidentity.length()) {
    zmq_identity = zmqidentity;
  }

  if (zmqname.length()) {
    zmq_name = zmqname;
  }

  if (zmqclienthost.length()) {
    zmq_clienthost = zmqclienthost;
  }

  if (zmqclientuuid.length()) {
    zmq_clientuuid = zmqclientuuid;
  }

  eos_static_info("metad connect %s as %s %d",
                  zmq_target.c_str(), zmq_identity.c_str(), zmq_identity.length());
  z_ctx = new zmq::context_t(1);
  z_socket = new zmq::socket_t(*z_ctx, ZMQ_DEALER);
  z_socket->setsockopt(ZMQ_IDENTITY, zmq_identity.c_str(), zmq_identity.length()
                      );

  while (1) {
    try {
      z_socket->connect(zmq_target);
      int linger = 0;
      z_socket->setsockopt( ZMQ_LINGER, &linger, sizeof(linger));
      eos_static_notice("connected to %s", zmq_target.c_str());
      break;
    } catch (zmq::error_t& e) {
      if (e.num() != EINTR) {
        eos_static_err("msg=\"%s\" rc=%d", e.what(), e.num());
        return e.num();
      }

      eos_static_err("msg=\"%s\" rc=%d", e.what(), e.num());
    }
  }

  if (zmqclientuuid.length()) {
    mdbackend->set_clientuuid(zmq_clientuuid);
  }

  set_zmq_wants_to_connect(0);
  return 0;
}

/* -------------------------------------------------------------------------- */
metad::shared_md
metad::lookup(fuse_req_t req, fuse_ino_t parent, const char* name)
{
  eos_static_info("ino=%#lx name=%s", parent, name);
  // --------------------------------------------------
  // STEP 1 : retrieve the required parent MD
  // --------------------------------------------------
  shared_md pmd = get(req, parent, "", false);
  shared_md md;

  if (pmd->id() == parent) {
    XrdSysMutexHelper mLock(pmd->Locker());
    fuse_ino_t inode = 0; // inode referenced by parent + name

    // self lookup required for NFS exports
    if (!strcmp(name, ".")) {
      return pmd;
    }

    // parent lookup required for NFS exports
    if (!strcmp(name, "..")) {
      pmd->Locker().UnLock();
      shared_md ppmd = get(req, pmd->pid(), "", false);
      pmd->Locker().Lock();
      return ppmd;
    }

    // --------------------------------------------------
    // STEP 2: check if we hold a cap for that directory
    // --------------------------------------------------
    if (pmd->cap_count() && !pmd->needs_refresh()) {
      // --------------------------------------------------
      // if we have a cap and we listed this directory, we trust the child information
      // --------------------------------------------------
      if (pmd->local_children().count(
				      eos::common::StringConversion::EncodeInvalidUTF8(name))) {
        inode = pmd->local_children().at(
					 eos::common::StringConversion::EncodeInvalidUTF8(name));
      } else {
	if (pmd->local_enoent().count(name)) {
          md = std::make_shared<mdx>();
          md->set_err(ENOENT);
          return md;
	}
        // if we are still having the creator MD record, we can be sure, that we know everything about this directory
        if (pmd->creator() ||
            (pmd->type() == pmd->MDLS)) {
          // no entry - TODO return a NULLMD object instead of creating it all the time
          md = std::make_shared<mdx>();
          md->set_err(pmd->err());
          return md;
        }

        if (pmd->get_todelete().count(eos::common::StringConversion::EncodeInvalidUTF8(
                                        name))) {
          // if this has been deleted, we just say this
          md = std::make_shared<mdx>();
          md->set_err(pmd->err());

          if (EOS_LOGS_DEBUG) {
            eos_static_debug("in deletion list %016lx name=%s", pmd->id(), name);
          }

          return md;
        }
      }
    } else {
      // --------------------------------------------------
      // if we don't have a cap, get will result in an MGM call anyway
      // --------------------------------------------------
    }

    // --------------------------------------------------
    // try to get the meta data record
    // --------------------------------------------------
    pmd->Locker().UnLock();
    md = get(req, inode, "", false, pmd, name);

    if (md) {
      md->Locker().Lock();
      std::string fullpath = pmd->fullpath();

      if (fullpath.back() != '/') {
        fullpath += "/";
      }

      fullpath += name;
      md->set_fullpath(fullpath.c_str());
      md->Locker().UnLock();
      pmd->Locker().Lock();
    } else {
      md = std::make_shared<mdx>();
      md->set_err(ENOENT);
    }
  } else {
    // --------------------------------------------------
    // no md available
    // --------------------------------------------------
    md = std::make_shared<mdx>();
    md->set_err(pmd->err());
  }

  return md;
}

/* -------------------------------------------------------------------------- */
int
metad::forget(fuse_req_t req, fuse_ino_t ino, int nlookup)
{
  shared_md md;
  uint64_t pino = 0;

  if (!mdmap.retrieveTS(ino, md)) {
    return ENOENT;
  }

  {
    XrdSysMutexHelper mLock(md->Locker());

    if (!md->id()) {
      return EAGAIN;
    }

    if (EOS_LOGS_DEBUG) {
      eos_static_debug("count=%d(-%d) - ino=%#lx", md->lookup_is(), nlookup, ino);
    }

    if (!md->lookup_dec(nlookup)) {
      eos_static_debug("count=%d(-%d) - ino=%#lx", md->lookup_is(), nlookup, ino);
      return EAGAIN;
    }

    pino = md->pid();
  }

  if (has_flush(ino)) {
    eos_static_debug("flush - ino=%016x", ino);
    return 0;
  }

  if ((pino > 1) && (ino != pino)) {
    // this does not make sense for the mount directory (inode 1)
    shared_md pmd;

    if (!mdmap.retrieveTS(pino, pmd)) {
      return ENOENT;
    }

    if (pmd->cap_count()) {
      eos_static_debug("caps %d - ino=%016x", pmd->cap_count(), ino);
      return 0;
    }

    if (pmd->opendir_is()) {
      eos_static_debug("opendir %d - ino=%016x", pmd->opendir_is(), ino);
      return 0;
    }
  } else {
    // we don't remove the mount point
    return 0;
  }

  if (EOS_LOGS_DEBUG) {
    XrdSysMutexHelper mLock(md->Locker());
    eos_static_debug("delete md object - ino=%#lx name=%s", ino,
                     md->name().c_str());
  }

  if (mdmap.eraseTS(ino)) {
    stat.inodes_dec();
  }

  // - we currently don't forget old mappings, because it creates race conditions with overlaying caps
  //  PUTMEBACK-LATER: inomap.erase_bwd(ino);
  return 0;
}

/* -------------------------------------------------------------------------- */
void
metad::mdx::convert(struct fuse_entry_param& e, double lifetime)
{
  const char* k_mdino = "sys.eos.mdino";
  const char* k_fifo = "sys.eos.fifo";
  auto attrMap = attr();
  e.ino = id();
  e.attr.st_dev = 0;
  e.attr.st_ino = id();
  e.attr.st_mode = mode();
  e.attr.st_nlink = nlink();

  if (attrMap.count(k_mdino)) {
    uint64_t mdino = std::stoull(attrMap[k_mdino]);
    uint64_t local_ino = EosFuse::Instance().mds.inomap.forward(mdino);
    shared_md tmd = EosFuse::Instance().mds.getlocal(NULL, local_ino);

    if (!tmd->id()) {
      local_ino = mdino;
      e.attr.st_nlink = 2;
      eos_static_err("converting hard-link %s target inode %#lx remote %#lx not in cache, nlink set to %d",
                     name().c_str(), local_ino, mdino, e.attr.st_nlink);
    } else {
      if (EOS_LOGS_DEBUG) {
        eos_static_debug("hlnk convert name=%s id=%#lx target local_ino=%#lx nlink0=",
                         name().c_str(), id(), local_ino, tmd->nlink());
      }

      e.attr.st_nlink = tmd->nlink();
    }

    e.ino = e.attr.st_ino = local_ino;
  }

  if (attrMap.count(k_fifo)) {
    e.attr.st_mode &= !S_IFREG;
    e.attr.st_mode |= S_IFIFO;
  }

  e.attr.st_uid = uid();
  e.attr.st_gid = gid();
  e.attr.st_rdev = 0;
  e.attr.st_size = size();
  e.attr.st_blksize = 4096;
  e.attr.st_blocks = (e.attr.st_size + 511) / 512;
  e.attr.st_atime = atime();
  e.attr.st_mtime = mtime();
  e.attr.st_ctime = ctime();
  e.attr.ATIMESPEC.tv_sec = atime();
  e.attr.ATIMESPEC.tv_nsec = atime_ns();
  e.attr.MTIMESPEC.tv_sec = mtime();
  e.attr.MTIMESPEC.tv_nsec = mtime_ns();
  e.attr.CTIMESPEC.tv_sec = ctime();
  e.attr.CTIMESPEC.tv_nsec = ctime_ns();

  if (EosFuse::Instance().Config().options.md_kernelcache) {
    e.attr_timeout = lifetime;
    e.entry_timeout = (lifetime > 30) ? 30 : lifetime;
  } else {
    e.attr_timeout = 0;
    e.entry_timeout = 0;
  }

  if (EosFuse::Instance().Config().options.overlay_mode) {
    e.attr.st_mode |= EosFuse::Instance().Config().options.overlay_mode;
  }

  if (S_ISDIR(e.attr.st_mode)) {
    if (!EosFuse::Instance().Config().options.show_tree_size) {
      // show 4kb directory size
      e.attr.st_size = 4096;
      e.attr.st_blocks = (e.attr.st_size + 511) / 512;
    }

    // we mask this bits for the moment
    e.attr.st_mode &= (~S_ISGID);
    e.attr.st_mode &= (~S_ISUID);
  }

  if (S_ISDIR(e.attr.st_mode)) {
    if (!EosFuse::Instance().Config().options.show_tree_size) {
      // show 4kb directory size
      e.attr.st_size = 4096;
    }

    // we mask this bits for the moment
    e.attr.st_mode &= (~S_ISGID);
    e.attr.st_mode &= (~S_ISUID);
  }

  if (S_ISLNK(e.attr.st_mode)) {
    e.attr.st_size = target().size();
  }

  e.generation = 1;
}

/* -------------------------------------------------------------------------- */
std::string
metad::mdx::dump()
{
  char sout[16384];
  snprintf(sout, sizeof(sout),
           "ino=%#lx dev=%#lx mode=%#o nlink=%u uid=%05u gid=%05u rdev=%#lx "
           "size=%llu bsize=%lu blocks=%llu atime=%lu.%lu mtime=%lu.%lu ctime=%lu.%lu",
           (unsigned long) id(), (unsigned long)0, (unsigned int) mode(),
           (unsigned int) nlink(),
           (unsigned int) uid(), (unsigned int) gid(), (unsigned long)0,
           (unsigned long long) size(), (unsigned long) 4096,
           (unsigned long long) size() / 512,
           (unsigned long) atime(), (unsigned long) atime_ns(),
           (unsigned long) mtime(), (unsigned long) mtime_ns(),
           (unsigned long) ctime(), (unsigned long) ctime_ns());
  return sout;
}

/* -------------------------------------------------------------------------- */
std::string
metad::mdx::dump(struct fuse_entry_param& e)
{
  char sout[16384];
  snprintf(sout, sizeof(sout),
           "ino=%#lx dev=%#lx mode=%#o nlink=%u uid=%05u gid=%05u rdev=%#lx "
           "size=%llu bsize=%lu blocks=%llu atime=%lu.%lu mtime=%lu.%lu ctime=%lu.%lu "
           "attr-timeout=%lu entry-timeout=%lu",
           (unsigned long) e.attr.st_ino, (unsigned long) e.attr.st_dev,
           (unsigned int) e.attr.st_mode, (unsigned int) e.attr.st_nlink,
           (unsigned int) e.attr.st_uid, (unsigned int) e.attr.st_gid,
           (unsigned long) e.attr.st_rdev,
           (unsigned long long) e.attr.st_size, (unsigned long) e.attr.st_blksize,
           (unsigned long long) e.attr.st_blocks,
           (unsigned long) e.attr.ATIMESPEC.tv_sec,
           (unsigned long) e.attr.ATIMESPEC.tv_nsec,
           (unsigned long) e.attr.MTIMESPEC.tv_sec,
           (unsigned long) e.attr.MTIMESPEC.tv_nsec,
           (unsigned long) e.attr.CTIMESPEC.tv_sec,
           (unsigned long) e.attr.CTIMESPEC.tv_nsec,
           (unsigned long) e.attr_timeout,
           (unsigned long) e.entry_timeout);
  return sout;
}

/* -------------------------------------------------------------------------- */
bool
metad::map_children_to_local(shared_md pmd)
{
  bool ret = true;
  // map a remote listing to a local one
  std::set<std::string> names;
  std::vector<std::string> names_to_delete;

  // we always merge remote contents, for changes our cap will be dropped
  for (auto map = pmd->children().begin(); map != pmd->children().end(); ++map) {
    if (EOS_LOGS_DEBUG) {
      eos_static_debug("translate %s [%#lx]",
                       eos::common::StringConversion::EncodeInvalidUTF8(map->first).c_str(),
                       map->second);
    }

    uint64_t remote_ino = map->second;
    uint64_t local_ino = inomap.forward(remote_ino);

    if (EosFuse::Instance().Config().options.hide_versions && EosFuse::Instance().mds.supports_hideversion()) {
      // check for version prefixes
      if ( map->first.substr(0, strlen(EOS_COMMON_PATH_VERSION_FILE_PREFIX)) == EOS_COMMON_PATH_VERSION_FILE_PREFIX ) {
	// check if there is actually a 'babysitting' reference file for this version, if now we display it!
	std::string nvfile = map->first.substr( strlen(EOS_COMMON_PATH_VERSION_FILE_PREFIX ));
	if (pmd->children().count(nvfile)) {
	  continue;
	}
      }
    }

    // skip entries we already know, if we don't have the mapping we have forgotten already this one
    if (pmd->local_children().count(
          eos::common::StringConversion::EncodeInvalidUTF8(map->first)) && local_ino) {
      continue;
    }

    // skip entries which are the deletion list
    if (pmd->get_todelete().count(eos::common::StringConversion::EncodeInvalidUTF8(
                                    map->first))) {
      continue;
    }

    shared_md md;

    if (!mdmap.retrieveTS(local_ino, md)) {
      local_ino = remote_ino;
      inomap.insert(remote_ino, local_ino);
      stat.inodes_inc();
      stat.inodes_ever_inc();
      md = std::make_shared<mdx>();
      mdmap.insertTS(local_ino, md);
    }

    if (EOS_LOGS_DEBUG)
      eos_static_debug("store-lookup r-ino %016lx <=> l-ino %016lx", remote_ino,
                       local_ino);

    pmd->local_children()[eos::common::StringConversion::EncodeInvalidUTF8(
                            map->first)] = local_ino;
  }

  if (EOS_LOGS_DEBUG) {
    for (auto map = pmd->local_children().begin();
         map != pmd->local_children().end(); ++map) {
      eos_static_debug("listing: %s [%#lx]", map->first.c_str(), map->second);
    }
  }

  pmd->set_nchildren(pmd->local_children().size());
  pmd->mutable_children()->clear();
  return ret;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::wait_deleted(fuse_req_t req,
                    fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  shared_md md;

  if (mdmap.retrieveTS(ino, md)) {
    if (md && md->id()) {
      while (1) {
        // wait that the deletion entry is leaving the flush queue
        mdflush.Lock();

        if (mdqueue.count(md->id())) {
          mdflush.UnLock();
          eos_static_notice("waiting for deletion entry to be synced upstream ino=%#lx",
                            md->id());
          std::this_thread::sleep_for(std::chrono::microseconds(500));
        } else {
          mdflush.UnLock();
          break;
        }
      }
    }
  }
}

/* -------------------------------------------------------------------------- */
metad::shared_md
/* -------------------------------------------------------------------------- */
metad::getlocal(fuse_req_t req,
                fuse_ino_t ino)
{
  eos_static_info("ino=%1llx", ino);
  shared_md md;

  if (!mdmap.retrieveTS(ino, md)) {
    md = std::make_shared<mdx>();
    md->set_err(ENOENT);
  }

  return md;
}

/* -------------------------------------------------------------------------- */
metad::shared_md
/* -------------------------------------------------------------------------- */
metad::get(fuse_req_t req,
           fuse_ino_t ino,
           std::string authid,
           bool listing,
           shared_md pmd,
           const char* name,
           bool readdir)
{
  eos_static_info("ino=%#lx pino=%#lx name=%s listing=%d", ino,
                  pmd ? pmd->id() : 0, name, listing);
  shared_md md;

  if (ino) {
    if (!mdmap.retrieveTS(ino, md)) {
      md = std::make_shared<mdx>();
      md->set_md_ino(inomap.backward(ino));
    } else {
      if (ino != 1) {
        // we need this to refetch a hard link target which was removed server side
        md->set_md_ino(ino);
      }
    }

    if (EOS_LOGS_DEBUG) {
      eos_static_debug("MD:\n%s", (!md) ? "<empty>" : dump_md(md).c_str());
    }
  } else {
    // -------------------------------------------------------------------------
    // this happens if we get asked for a child, which was never listed before
    // -------------------------------------------------------------------------
    md = std::make_shared<mdx>();
  }

  if (!md || !md->id()) {
    // -------------------------------------------------------------------------
    // there is no local meta data available, this can only be found upstream
    // -------------------------------------------------------------------------
  } else {
    // -------------------------------------------------------------------------
    // there is local meta data, we have to decide if we can 'trust' it, or we
    // need to refresh it from upstream  - TODO !
    // -------------------------------------------------------------------------
    if (readdir && !listing) {
      eos_static_info("returning opendir(readdir) entry");
      return md;
    }

    if (pmd && (pmd->cap_count() || pmd->creator()) && !pmd->needs_refresh()) {
      eos_static_info("returning cap entry");
      return md;
    } else {
      eos_static_info("pmd=%#lx cap-cnt=%d", pmd ? pmd->id() : 0,
                      pmd ? pmd->cap_count() : 0);
      uint64_t md_pid = 0;
      mode_t md_mode = 0;
      {
        XrdSysMutexHelper mLock(md->Locker());

        if (((!listing) || (listing && md->type() == md->MDLS)) && md->md_ino() &&
            md->cap_count() && !md->needs_refresh()) {
          eos_static_info("returning cap entry via parent lookup cap-count=%d",
                          md->cap_count());

          if (EOS_LOGS_DEBUG) {
            eos_static_debug("MD:\n%s", dump_md(md, false).c_str());
          }

          return md;
        }

        md_pid = md->pid();
        md_mode = md->mode();
      }

      if (!S_ISDIR(md_mode)) {
        // files are covered by the CAP of the parent, so if there is a cap
        // on the parent we can return this entry right away
        if (mdmap.retrieveTS(md_pid, pmd)) {
          if (pmd && pmd->id() && pmd->cap_count() && !md->needs_refresh()) {
            return md;
          }
        }
      }
    }

    XrdSysMutexHelper mLock(md->Locker());

    if ((md->id() != 1) && !md->pid() && !md->needs_refresh()) {
      // this must have been generated locally, we return this entry
      eos_static_info("returning generated entry");

      if (EOS_LOGS_DEBUG) {
        eos_static_debug("MD:\n%s", dump_md(md, false).c_str());
      }

      return md;
    }
  }

  // ---------------------------------------------------------------------------
  // we will get meta data from upstream
  // ---------------------------------------------------------------------------
  int rc = 0; // response code to a backend getMD call
  std::vector<eos::fusex::container> contv; // response container
  int thecase = 0;

  if (ino == 1)
    // -------------------------------------------------------------------------
    // CASE 1: root mount
    // -------------------------------------------------------------------------
  {
    thecase = 1;
    // -------------------------------------------------------------------------
    // the root inode is the only one we get by full path, all the others
    // go by parent-ino + name or inode
    // -------------------------------------------------------------------------
    std::string root_path = "/";
    // request the root meta data
    rc = mdbackend->getMD(req, root_path, contv, listing, authid);
    // set ourselfs as parent of root since we might mount
    // a remote directory != '/'
    md->set_pid(1);
  } else if (!ino)
    // -------------------------------------------------------------------------
    // CASE 2: by remote parent inode + name
    // -------------------------------------------------------------------------
  {
    thecase = 2;

    if (pmd) {
      // prevent resyning when we have deletions pending
      /*while (1)
      {
        XrdSysMutexHelper mdLock(pmd->Locker());
        if (pmd->WaitSync(1))
        {
          if (pmd->get_todelete().size())
            continue;

          break;
        }
      }
       */
      pmd->Locker().Lock();
      uint64_t pmd_ino = pmd->md_ino();
      pmd->Locker().UnLock();

      if (pmd_ino) {
        rc = mdbackend->getMD(req, pmd_ino, name, contv, listing, authid);
      } else {
        rc = ENOENT;
      }
    } else {
      rc = ENOENT;
    }
  } else
    // -------------------------------------------------------------------------
    // CASE 3: by remote inode
    // -------------------------------------------------------------------------
  {
    thecase = 3;
    XrdSysMutexHelper mLock(md->Locker());

    if (md->md_ino()) {
      /*
      // prevent resyncing when we have deletions pending
      while (1)
      {
        XrdSysMutexHelper mdLock(md->Locker());
        if (md->WaitSync(1))
        {
          if (md->get_todelete().size())
            continue;

          break;
        }
      }
       */
      eos_static_info("ino=%016lx type=%d", md->md_ino(), md->type());
      rc = mdbackend->getMD(req, md->md_ino(), listing ? ((md->type() != md->MDLS)
                            ? 0 : md->clock()) : md->clock(),
                            contv, listing, authid);
    } else {
      if (md->id()) {
        // that can be a locally created entry which is not yet upstream
        rc = 0;

        if (EOS_LOGS_DEBUG) {
          eos_static_debug("MD:\n%s", dump_md(md).c_str());
        }

        return md;
      } else {
        rc = ENOENT;
      }
    }
  }

  if (!rc) {
    // -------------------------------------------------------------------------
    // we need to store all response data and eventually create missing
    // hierarchical entries
    // -------------------------------------------------------------------------
    eos_static_debug("apply vector=%d", contv.size());

    for (auto it = contv.begin(); it != contv.end(); ++it) {
      if (it->ref_inode_()) {
        if (ino) {
          // the response contains the remote inode according to the request
          inomap.insert(it->ref_inode_(), ino);
        }

        uint64_t l_ino;

        // store the retrieved meta data blob
        if (!(l_ino = apply(req, *it, listing))) {
          eos_static_crit("msg=\"failed to apply response\"");
        } else {
          ino = l_ino;
        }
      } else {
        // we didn't get the md back
      }
    }

    // if the md record was returned, it is accessible after the apply function
    // attached it. We should also attach to the parent to be able to add
    // a not yet published child entry at the parent.
    mdmap.retrieveWithParentTS(ino, md, pmd);
    eos_static_info("ino=%08llx pino=%08llx name=%s listing=%d", ino,
                    pmd ? pmd->id() : 0, name, listing);

    switch (thecase) {
    case 1:
      // nothing to do
      break;

    case 2: {
      // we make sure, that the meta data record is attached to the local parent
      if (pmd && pmd->id()) {
        std::string encname = eos::common::StringConversion::EncodeInvalidUTF8(
                                md->name());

	XrdSysMutexHelper mLock(pmd->Locker());

        if (!pmd->local_children().count(encname) &&
            !pmd->get_todelete().count(encname) &&
            !md->deleted()) {
          eos_static_info("attaching %s [%#lx] to %s [%#lx]",
                          encname.c_str(), md->id(),
                          pmd->name().c_str(), pmd->id());
          // persist this hierarchical dependency
          pmd->local_children()[eos::common::StringConversion::EncodeInvalidUTF8(
                                  md->name())] = md->id();
          update(req, pmd, "", true);
        }
      }

      break;
    }

    case 3:
      break;
    }
  }

  if (rc) {
    shared_md md = std::make_shared<mdx>();
    md->set_err(rc);

    if (EOS_LOGS_DEBUG) {
      eos_static_debug("MD:\n%s", dump_md(md).c_str());
    }

    return md;
  }

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("MD:\n%s", dump_md(md).c_str());
  }

  return md;
}

/* -------------------------------------------------------------------------- */
uint64_t
metad::insert(fuse_req_t req, metad::shared_md md, std::string authid)
{
  {
    if (EOS_LOGS_DEBUG) {
      eos_static_debug("%s", dump_md(md, false).c_str());
    }

    mdmap.insertTS(md->id(), md);
  }
  return md->id();
}

/* -------------------------------------------------------------------------- */
int
metad::wait_flush(fuse_req_t req, metad::shared_md md)
{
  // logic to wait for a completion of request
  md->Locker().UnLock();

  while (1) {
    if (md->WaitSync(1)) {
      if (has_flush(md->id())) {
	// if a deletion was issued, OP state is md->RM not md->NONE hence we would never leave this loop
	continue;
      }

      break;
    }
  }

  eos_static_info("waited for sync rc=%d bw=%#lx", md->err(),
                  inomap.backward(md->id()));

  if (!inomap.backward(md->id())) {
    md->Locker().Lock();
    return md->err();
  } else {
    md->Locker().Lock();
    return 0;
  }
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
metad::has_flush(fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  bool in_flush = false;
  mdflush.Lock();

  if (mdqueue.count(ino)) {
    in_flush = true;
  }

  mdflush.UnLock();
  return in_flush;
}

/* -------------------------------------------------------------------------- */
void
metad::update(fuse_req_t req, shared_md md, std::string authid,
              bool localstore)
{
  mdflush.Lock();
  stat.inodes_backlog_store(mdqueue.size());

  if (!localstore) {
    // only updates initiated from FUSE limited,
    // server response updates pass
    while (mdqueue.size() == mdqueue_max_backlog) {
      mdflush.WaitMS(25);
    }
  }

  flushentry fe(md->id(), authid, localstore ? mdx::LSTORE : mdx::UPDATE, req);
  fe.bind();
  mdqueue[md->id()]++;
  mdflushqueue.push_back(fe);
  eos_static_info("added ino=%#lx flushentry=%s queue-size=%u local-store=%d",
                  md->id(), flushentry::dump(fe).c_str(), mdqueue.size(), localstore);
  mdflush.Signal();
  mdflush.UnLock();
}

/* -------------------------------------------------------------------------- */
void
metad::add(fuse_req_t req, metad::shared_md pmd, metad::shared_md md,
           std::string authid, bool localstore)
{
  using eos::common::StringConversion;
  // this is called with a lock on the md object
  stat.inodes_inc();
  stat.inodes_ever_inc();
  uint64_t pid = 0;
  uint64_t id = 0;

  if (EOS_LOGS_DEBUG)
    eos_static_debug("child=%s parent=%s inode=%016lx authid=%s localstore=%d",
                     md->name().c_str(),
                     pmd->name().c_str(), md->id(), authid.c_str(), localstore);

  // avoid lock-order violation
  md->Locker().UnLock();
  {
    XrdSysMutexHelper mLock(pmd->Locker());

    if (!pmd->local_children().count(StringConversion::EncodeInvalidUTF8(
                                       md->name()))) {
      pmd->set_nchildren(pmd->nchildren() + 1);
    }

    pmd->local_children()[StringConversion::EncodeInvalidUTF8(
                            md->name())] = md->id();
    pmd->set_nlink(1);
    pmd->get_todelete().erase(StringConversion::EncodeInvalidUTF8(md->name()));
    pid = pmd->id();
  }
  md->Locker().Lock();
  {
    // store the local and remote parent inode
    md->set_pid(pmd->id());
    md->set_md_pino(pmd->md_ino());
    id = md->id();
  }
  mdflush.Lock();
  stat.inodes_backlog_store(mdqueue.size());

  if (!localstore) {
    while (mdqueue.size() == mdqueue_max_backlog) {
      mdflush.WaitMS(25);
    }

    flushentry fe(id, authid, mdx::ADD, req);
    fe.bind();
    mdqueue[id]++;
    mdflushqueue.push_back(fe);
  }

  flushentry fep(pid, authid, mdx::LSTORE, req);
  fep.bind();
  mdqueue[pid]++;
  mdflushqueue.push_back(fep);
  mdflush.Signal();
  mdflush.UnLock();
}

/* -------------------------------------------------------------------------- */
int
metad::add_sync(fuse_req_t req, shared_md pmd, shared_md md, std::string authid)
{
  // this is called with a lock on the md object
  int rc = 0;
  // store the local and remote parent inode
  XrdSysMutexHelper mLockParent(pmd->Locker());
  md->set_pid(pmd->id());
  md->set_md_pino(pmd->md_ino());
  mLockParent.UnLock();
  mdx::md_op op = mdx::ADD;

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("metacache::sync ino=%016lx authid=%s op=%d", md->id(),
                     authid.c_str(), (int) op);
  }

  md->set_operation(md->SET);
  eos_static_info("metacache::sync backend::putMD - start");

  while (1) {
    // wait that the parent is leaving the mdqueue
    mdflush.Lock();

    if (mdqueue.count(pmd->id())) {
      mdflush.UnLock();
      eos_static_info("waiting for parent directory to be synced upstream parent-ino= %#lx ino=%#lx",
                      pmd->id(), md->id());
      std::this_thread::sleep_for(std::chrono::microseconds(500));
    } else {
      mdflush.UnLock();
      break;
    }
  }

  // push to backend
  if ((rc = mdbackend->putMD(req, &(*md), authid, &(md->Locker())))) {
    eos_static_err("metad::add_sync backend::putMD failed rc=%d", rc);
    // in this case we always clean this MD record to force a refresh
    inomap.erase_bwd(md->id());
    md->setop_none();
    md->set_err(rc);

    if (md->id()) {
      if (mdmap.eraseTS(md->id())) {
        stat.inodes_dec();
        stat.inodes_ever_inc();
      }
    }

    return rc;
  } else {
    md->set_id(md->md_ino());
    inomap.insert(md->md_ino(), md->id());
    md->setop_none();
  }

  eos_static_info("metad::add_sync backend::putMD - stop");
  std::string mdstream;
  std::string md_name = md->name();
  md->SerializeToString(&mdstream);
  stat.inodes_inc();
  stat.inodes_ever_inc();

  if (EOS_LOGS_DEBUG)
    eos_static_debug("child=%s parent=%s inode=%016lx authid=%s",
                     md->name().c_str(),
                     pmd->name().c_str(), md->id(), authid.c_str());

  // avoid lock-order violation
  md->Locker().UnLock();
  {
    XrdSysMutexHelper mLock(pmd->Locker());

    if (!pmd->local_children().count(
          eos::common::StringConversion::EncodeInvalidUTF8(md_name))) {
      pmd->set_nchildren(pmd->nchildren() + 1);
    }

    pmd->local_children()[eos::common::StringConversion::EncodeInvalidUTF8(
                            md_name)] = md->id();
    pmd->set_nlink(1);
    pmd->get_todelete().erase(eos::common::StringConversion::EncodeInvalidUTF8(
                                md_name));
  }
  md->Locker().Lock();
  mdflush.Lock();
  stat.inodes_backlog_store(mdqueue.size());

  while (mdqueue.size() == mdqueue_max_backlog) {
    mdflush.WaitMS(25);
  }

  flushentry fep(pmd->id(), authid, mdx::LSTORE, req);
  fep.bind();
  mdqueue[pmd->id()]++;
  mdflushqueue.push_back(fep);
  mdflush.Signal();
  mdflush.UnLock();
  return 0;
}

/* -------------------------------------------------------------------------- */
int
metad::begin_flush(fuse_req_t req, shared_md emd, std::string authid)
{
  shared_md md = std::make_shared<mdx>();
  md->set_operation(md->BEGINFLUSH);
  int rc = 0;

  if (!emd->md_ino()) {
    //TODO wait for the remote inode to be known
  }

  md->set_md_ino(emd->md_ino());

  if ((rc = mdbackend->putMD(req, &(*md), authid, 0))) {
    eos_static_err("metad::begin_flush backend::putMD failed rc=%d", rc);
  }

  return rc;
}

/* -------------------------------------------------------------------------- */
int
metad::end_flush(fuse_req_t req, shared_md emd, std::string authid)
{
  shared_md md = std::make_shared<mdx>();
  md->set_operation(md->ENDFLUSH);
  int rc = 0;

  if (!emd->md_ino()) {
    //TODO wait for the remote inode to be known
  }

  md->set_md_ino(emd->md_ino());

  if ((rc = mdbackend->putMD(req, &(*md), authid, 0))) {
    eos_static_err("metad::begin_flush backend::putMD failed rc=%d", rc);
  }

  return rc;
}

/* -------------------------------------------------------------------------- */
void
metad::remove(fuse_req_t req, metad::shared_md pmd, metad::shared_md md,
              std::string authid,
              bool upstream)
{
  // this is called with the md object locked
  if (EOS_LOGS_DEBUG)
    eos_static_debug("child=%s parent=%s inode=%#lx upstreaqm=%d",
                     md->name().c_str(),
                     pmd->name().c_str(), md->id(), upstream);

  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts);

  if (!md->deleted()) {
    md->lookup_inc();
    stat.inodes_deleted_inc();
    stat.inodes_deleted_ever_inc();
  }

  md->set_mtime(ts.tv_sec);
  md->set_mtime_ns(ts.tv_nsec);
  md->setop_delete();

  if ( EosFuse::Instance().Config().options.hide_versions && EosFuse::Instance().mds.supports_hideversion() ) {
    // indicate the MGM to remove also all versions
    md->set_opflags(eos::fusex::md::DELETEVERSIONS);
  }


  std::string name = md->name();
  // avoid lock order violation
  md->Locker().UnLock();
  {
    XrdSysMutexHelper mLock(pmd->Locker());
    pmd->local_children().erase(eos::common::StringConversion::EncodeInvalidUTF8(
                                  name));
    pmd->set_nchildren(pmd->nchildren() - 1);
    pmd->get_todelete()[eos::common::StringConversion::EncodeInvalidUTF8(
                          name)] = md->id();
    pmd->set_mtime(ts.tv_sec);
    pmd->set_mtime_ns(ts.tv_nsec);
  }
  md->Locker().Lock();

  if (!upstream) {
    return;
  }

  flushentry fe(md->id(), authid, mdx::RM, req);
  fe.bind();
  flushentry fep(pmd->id(), authid, mdx::LSTORE, req);
  fep.bind();
  mdflush.Lock();

  while (mdqueue.size() == mdqueue_max_backlog) {
    mdflush.WaitMS(25);
  }

  mdqueue[pmd->id()]++;
  mdflushqueue.push_back(fep);
  mdqueue[md->id()]++;
  mdflushqueue.push_back(fe);
  stat.inodes_backlog_store(mdqueue.size());
  mdflush.Signal();
  mdflush.UnLock();
}

/* -------------------------------------------------------------------------- */
void
metad::mv(fuse_req_t req, shared_md p1md, shared_md p2md, shared_md md,
          std::string newname, std::string authid1, std::string authid2)
{
  if (EOS_LOGS_DEBUG)
    eos_static_debug("child=%s new-name=%s parent=%s newparent=%s inode=%016lx",
                     md->name().c_str(),
                     newname.c_str(),
                     p1md->name().c_str(), p2md->name().c_str(), md->id());

  XrdSysMutexHelper mLock(md->Locker());
  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts);

  if (p1md->id() != p2md->id()) {
    // move between directories. We need to run an expensive algorithm to
    // determine the correct lock order, but a rename should be rather uncommon,
    // anyway.
    MdLocker locker(p1md, p2md, determineLockOrder(p1md, p2md));
    std::string oldname = md->name();

    if (!p2md->local_children().count(
          eos::common::StringConversion::EncodeInvalidUTF8(newname))) {
      p2md->set_nchildren(p2md->nchildren() + 1);
    }

    p2md->local_children()[eos::common::StringConversion::EncodeInvalidUTF8(
                             newname)] = md->id();
    p1md->local_children().erase(eos::common::StringConversion::EncodeInvalidUTF8(
                                   md->name()));
    p1md->set_nchildren(p1md->nchildren() - 1);
    p1md->set_mtime(ts.tv_sec);
    p1md->set_mtime_ns(ts.tv_nsec);
    p1md->clear_pmtime();
    p1md->clear_pmtime_ns();
    p1md->set_ctime(ts.tv_sec);
    p1md->set_ctime_ns(ts.tv_nsec);
    p2md->set_mtime(ts.tv_sec);
    p2md->set_mtime_ns(ts.tv_nsec);
    p2md->clear_pmtime();
    p2md->clear_pmtime_ns();
    p2md->set_ctime(ts.tv_sec);
    p2md->set_ctime_ns(ts.tv_nsec);
    md->set_name(newname);
    md->set_pid(p2md->id());
    md->set_md_pino(p2md->md_ino());
    p1md->get_todelete()[eos::common::StringConversion::EncodeInvalidUTF8(
                           oldname)] = 0; //md->id(); // make it known as deleted
    p2md->get_todelete().erase(eos::common::StringConversion::EncodeInvalidUTF8(
                                 newname)); // the new target is not deleted anymore
    p2md->local_enoent().erase(newname); // remove a possible enoent entry
    md->setop_update();
    p1md->setop_update();
    p2md->setop_update();
  } else {
    // move within directory
    XrdSysMutexHelper m1Lock(p1md->Locker());

    if (p2md->local_children().count(
          eos::common::StringConversion::EncodeInvalidUTF8(newname))) {
      p2md->set_nchildren(p2md->nchildren() - 1);
    }

    p2md->local_children()[eos::common::StringConversion::EncodeInvalidUTF8(
                             newname)] = md->id();
    p1md->local_children().erase(eos::common::StringConversion::EncodeInvalidUTF8(
                                   md->name()));
    p1md->get_todelete()[eos::common::StringConversion::EncodeInvalidUTF8(
                           md->name())] = md->id(); // make it known as deleted
    p2md->get_todelete().erase(eos::common::StringConversion::EncodeInvalidUTF8(
                                 newname)); // the new target is not deleted anymore
    p2md->local_enoent().erase(newname); // remove a possible enoent entry
    md->set_name(newname);
    md->setop_update();

    p1md->set_mtime(ts.tv_sec);
    p1md->set_mtime_ns(ts.tv_nsec);
    p1md->clear_pmtime();
    p1md->clear_pmtime_ns();
    p1md->set_ctime(ts.tv_sec);
    p1md->set_ctime_ns(ts.tv_nsec);
    p1md->setop_update();
  }

  md->clear_pmtime();
  md->clear_pmtime_ns();
  md->set_ctime(ts.tv_sec);
  md->set_ctime_ns(ts.tv_nsec);
  md->set_mv_authid(authid1); // store also the source authid
  mdflush.Lock();

  while (mdqueue.size() == mdqueue_max_backlog) {
    mdflush.WaitMS(25);
  }

  flushentry fe1(p1md->id(), authid1, mdx::UPDATE, req);
  fe1.bind();
  mdqueue[p1md->id()]++;
  mdflushqueue.push_back(fe1);

  if (p1md->id() != p2md->id()) {
    flushentry fe2(p2md->id(), authid2, mdx::UPDATE, req);
    fe2.bind();
    mdqueue[p2md->id()]++;
    mdflushqueue.push_back(fe2);
  }

  flushentry fe(md->id(), authid2, mdx::UPDATE, req);
  fe.bind();
  mdqueue[md->id()]++;
  mdflushqueue.push_back(fe);
  stat.inodes_backlog_store(mdqueue.size());
  mdflush.Signal();
  mdflush.UnLock();
}

/* -------------------------------------------------------------------------- */
int
metad::rmrf(fuse_req_t req, shared_md md)
{
  int rc = mdbackend->rmRf(req, &(*md));
  return rc;
}

/* -------------------------------------------------------------------------- */
std::string
metad::dump_md(shared_md md, bool lock)
{
  if (!(md)) {
    return "";
  }

  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string jsonstring;

  if (lock) {
    md->Locker().Lock();
  }

  google::protobuf::util::MessageToJsonString(*((eos::fusex::md*)(&(*md))),
      &jsonstring, options);
  char capcnt[16];
  snprintf(capcnt, sizeof(capcnt), "%d", md->cap_count());
  jsonstring += "\nlocal-children: {\n";

  for (auto it = md->local_children().begin();
       it != md->local_children().end(); ++it) {
    char buff[32];
    jsonstring += "\"";
    jsonstring += it->first;
    jsonstring += "\" : ";
    jsonstring += longstring::to_decimal(it->second, buff);

    if (it == md->local_children().end()) {
      break;
    } else {
      jsonstring += "\",";
    }
  }

  jsonstring += "}\n";
  jsonstring += "\nto-delete: {\n";

  for (auto it = md->get_todelete().begin();
       it != md->get_todelete().end(); ++it) {
    jsonstring += "\"";
    jsonstring += it->first.c_str();

    if (it == md->get_todelete().end()) {
      jsonstring += "\"";
      break;
    } else {
      jsonstring += "\",";
    }
  }

  jsonstring += "}\n";
  jsonstring += "\nenoent: {\n";

  for (auto it = md->local_enoent().begin();
       it != md->local_enoent().end(); ++it) {
    jsonstring += "\"";
    jsonstring += *it;

    if (it == md->local_enoent().end()) {
      jsonstring += "\"";
      break;
    } else {
      jsonstring += "\",";
    }
  }

  jsonstring += "}\n";
  jsonstring += "\ncap-cnt: ";
  jsonstring += capcnt;
  jsonstring += "\nlru-prev: ";
  jsonstring += std::to_string(md->lru_prev());
  jsonstring += "\nlru_next: ";
  jsonstring += std::to_string(md->lru_next());
  jsonstring += "\n";
  jsonstring += "\nrefresh: ";
  jsonstring += md->needs_refresh() ? "true" : "false";
  jsonstring += "\n";

  if (lock) {
    md->Locker().UnLock();
  }

  return jsonstring;
}

/* -------------------------------------------------------------------------- */
std::string
metad::dump_md(eos::fusex::md& md)
{
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string jsonstring;
  google::protobuf::util::MessageToJsonString(md, &jsonstring, options);
  return jsonstring;
}

/* -------------------------------------------------------------------------- */
std::string
metad::dump_container(eos::fusex::container& cont)
{
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string jsonstring;
  google::protobuf::util::MessageToJsonString(cont, &jsonstring, options);
  return jsonstring;
}

/* -------------------------------------------------------------------------- */
int
metad::getlk(fuse_req_t req, shared_md md, struct flock* lock)
{
  XrdSysMutexHelper locker(md->Locker());
  // fill lock request structure
  md->mutable_flock()->set_pid(fuse_req_ctx(req)->pid);
  md->mutable_flock()->set_len(lock->l_len);
  md->mutable_flock()->set_start(lock->l_start);
  md->set_operation(md->GETLK);

  switch (lock->l_type) {
  case F_RDLCK:
    md->mutable_flock()->set_type(eos::fusex::lock::RDLCK);
    break;

  case F_WRLCK:
    md->mutable_flock()->set_type(eos::fusex::lock::WRLCK);
    break;

  case F_UNLCK:
    md->mutable_flock()->set_type(eos::fusex::lock::UNLCK);
    break;

  default:
    return EINVAL;
  }

  // do sync upstream lock call
  int rc = mdbackend->doLock(req, *md, &(md->Locker()));

  // digest the response
  if (!rc) {
    // store the md->flock response into the flock structure
    lock->l_pid = md->flock().pid();
    lock->l_len = md->flock().len();
    lock->l_start = md->flock().start();
    lock->l_whence = SEEK_SET;

    switch (md->flock().type()) {
    case eos::fusex::lock::RDLCK:
      lock->l_type = F_RDLCK;
      break;

    case eos::fusex::lock::WRLCK:
      lock->l_type = F_WRLCK;
      break;

    case eos::fusex::lock::UNLCK:
      lock->l_type = F_UNLCK;
      break;

    default:
      rc = md->flock().err_no();
    }
  } else {
    rc = EAGAIN;
  }

  // clean the lock structure;
  md->clear_flock();
  return rc;
}

/* -------------------------------------------------------------------------- */
int
metad::setlk(fuse_req_t req, shared_md md, struct flock* lock, int sleep)
{
  XrdSysMutexHelper locker(md->Locker());
  // fill lock request structure
  md->mutable_flock()->set_pid(fuse_req_ctx(req)->pid);
  md->mutable_flock()->set_len(lock->l_len);
  md->mutable_flock()->set_start(lock->l_start);

  if (sleep) {
    md->set_operation(md->SETLKW);
  } else {
    md->set_operation(md->SETLK);
  }

  switch (lock->l_type) {
  case F_RDLCK:
    md->mutable_flock()->set_type(eos::fusex::lock::RDLCK);
    break;

  case F_WRLCK:
    md->mutable_flock()->set_type(eos::fusex::lock::WRLCK);
    break;

  case F_UNLCK:
    md->mutable_flock()->set_type(eos::fusex::lock::UNLCK);
    break;

  default:
    return EINVAL;
  }

  bool backend_call = true;

  if (lock->l_type == F_UNLCK) {
    backend_call = false;

    // check that we have actually a lock for that before doing an upstream call
    for (auto it = md->LockTable().begin(); it != md->LockTable().end(); ++it) {
      if (it->l_pid == (pid_t) md->flock().pid()) {
        backend_call = true;
      }
    }
  }

  // do sync upstream lock call
  int rc = 0;

  if (backend_call) {
    rc = mdbackend->doLock(req, *md, &(md->Locker()));
  }

  // digest the response
  if (!rc) {
    rc = md->flock().err_no();
  } else {
    rc = EAGAIN;
  }

  if (!rc) {
    // store in the lock table - unlocking done during flush
    if (lock->l_type != F_UNLCK) {
      md->LockTable().push_back(*lock);
    } else {
      // remove from LockTable - not the most efficient
      auto it = md->LockTable().begin();

      while (it != md->LockTable().end()) {
        if (it->l_pid == (pid_t) md->flock().pid()) {
          it = md->LockTable().erase(it);
        } else {
          it++;
        }
      }
    }
  }

  // clean the lock structure;
  md->clear_flock();
  return rc;
}

/* -------------------------------------------------------------------------- */
int
metad::statvfs(fuse_req_t req, struct statvfs* svfs)
{
  return mdbackend->statvfs(req, svfs);
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::cleanup(shared_md md)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("id=%16x", md->id());
  std::vector<std::string> inval_entry_name;
  std::vector<fuse_ino_t> inval_files;
  std::vector<fuse_ino_t> inval_dirs;

  for (auto it = md->local_children().begin();
       it != md->local_children().end(); ++it) {
    shared_md cmd;

    if (mdmap.retrieveTS(it->second, cmd)) {
      // XrdSysMutexHelper cmLock(cmd->Locker());
      bool in_flush = has_flush(it->second);

      if (!S_ISDIR(cmd->mode())) {
        if (!in_flush && !EosFuse::Instance().datas.has(cmd->id())) {
          // clean-only entries, which are not in the flush queue and not open
          inval_files.push_back(it->second);
          cmd->force_refresh();
        }
      }

      if (!dentrymessaging) {
        // if the server does not provide a dentry invalidation message
        inval_entry_name.push_back(it->first);
      } else {
        // if the server provides a dentry invalidation message
        // files and directories never get an inval_entry call, only when we see an explicit deletion or a negative cache entry needs cleanup
      }
    }
  }

  for (auto it : md->local_enoent()) {
    inval_entry_name.push_back(it);
  }

  md->local_enoent().clear();
  md->Locker().UnLock();

  if (EosFuse::Instance().Config().options.md_kernelcache) {
    for (auto it = inval_entry_name.begin(); it != inval_entry_name.end(); ++it) {
      kernelcache::inval_entry(md->id(), *it);
    }
  }

  md->Locker().Lock();
  md->set_type(md->MD);
  md->set_creator(false);
  md->cap_count_reset();
  md->set_nchildren(md->local_children().size());
  md->get_todelete().clear();
  md->setop_none();     /* so that wait_flush() returns */
  md->Locker().UnLock();

  if ((EosFuse::Instance().Config().options.data_kernelcache) ||
      (EosFuse::Instance().Config().options.md_kernelcache)) {
    for (auto it = inval_files.begin(); it != inval_files.end(); ++it) {
      forget(0, *it, 0);
    }
  }
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::cleanup(fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  shared_md md;

  if (mdmap.retrieveTS(ino, md)) {
    md->Locker().Lock();
    return cleanup(md);
  }
}

/* -------------------------------------------------------------------------- */
uint64_t
metad::apply(fuse_req_t req, eos::fusex::container& cont, bool listing)
{
  // apply receives either a single MD record or a parent MD + all children MD
  // we have to make sure that the modification of children is atomic in the parent object
  shared_md md;
  shared_md pmd;

  if (EOS_LOGS_DEBUG) {
    eos_static_debug(dump_container(cont).c_str());
  }

  if (cont.type() == cont.MD) {
    uint64_t md_ino = cont.md_().md_ino();
    uint64_t md_pino = cont.md_().md_pino();
    uint64_t ino = inomap.forward(md_ino);
    bool is_new = false;
    {
      // Create a new md object, if none is found in the cache
      if (!mdmap.retrieveTS(ino, md)) {
        is_new = true;
        md = std::make_shared<mdx>();
      }

      md->Locker().Lock();

      if (EOS_LOGS_DEBUG) {
        eos_static_debug("%s op=%d deleted=%d", md->dump().c_str(), md->getop(),
                         md->deleted());
      }

      if (md->deleted()) {
        md->Locker().UnLock();
        return ino;
      }
    }
    uint64_t p_ino = inomap.forward(md_pino);

    if (!p_ino) {
      p_ino = md_pino;
      // it might happen that we don't know yet anything about this parent
      inomap.insert(md_pino, p_ino);
      eos_static_debug("msg=\"creating lookup entry for parent inode\" md-pino=%016lx pino=%016lx md-ino=%016lx ino=%016lx",
                       md_pino, p_ino, md_ino, ino);
    }

    if (is_new) {
      // in this case we need to create a new one
      md->set_id(md_ino);
      uint64_t new_ino = insert(req, md, md->authid());
      ino = new_ino;
    }

    if (!S_ISDIR(md->mode())) {
      // if its a file we need to have a look at parent cap-count, so we get the parent md
      md->Locker().UnLock();
      mdmap.retrieveTS(p_ino, pmd);
      md->Locker().Lock();
    }

    {
      if (!has_flush(ino)) {
        md->CopyFrom(cont.md_());

	shared_md d_md = EosFuse::Instance().datas.retrieve_wr_md(ino);
	if (d_md) {
          // see if this file is open for write, because in that case
          // we have to keep the local size information and modification times
          md->set_size(d_md->size());
          md->set_mtime(d_md->mtime());
          md->set_mtime_ns(d_md->mtime_ns());
        }
      } else {
        eos_static_warning("deferring MD overwrite local-ino=%016lx remote-ino=%016lx ",
                           (long) ino, (long) md_ino);
      }

      md->set_nchildren(md->local_children().size());

      if (EOS_LOGS_DEBUG) {
        eos_static_debug("store md for local-ino=%016lx remote-ino=%016lx -",
                         (long) ino, (long) md_ino);
        eos_static_debug("%s", md->dump().c_str());
      }
    }

    md->set_pid(p_ino);
    md->set_id(ino);
    md->clear_refresh();
    eos_static_info("store local pino=%016lx for %016lx", md->pid(), md->id());
    inomap.insert(md_ino, ino);
    md->Locker().UnLock();

    if (is_new) {
      XrdSysMutexHelper mLock(mdmap);
      mdmap[ino] = md;
      stat.inodes_inc();
      stat.inodes_ever_inc();
    }

    return ino;
  } else if (cont.type() == cont.MDMAP) {
    uint64_t p_ino = inomap.forward(cont.ref_inode_());

    for (auto map = cont.md_map_().md_map_().begin();
         map != cont.md_map_().md_map_().end(); ++map) {
      // loop over the map of meta data objects
      uint64_t ino = inomap.forward(map->first);
      eos::fusex::cap cap_received;
      cap_received.set_id(0);

      if (EOS_LOGS_DEBUG) {
        eos_static_debug("remote-ino=%016lx local-ino=%016lx", (long) map->first, ino);
      }

      if (mdmap.retrieveTS(ino, md)) {
        // this is an already known inode
        eos_static_debug("lock mdmap");
        {
          bool child = false;

          if (map->first != cont.ref_inode_()) {
            child = true;

            if (!S_ISDIR(map->second.mode())) {
              shared_md child_pmd;

              if (mdmap.retrieveTS(p_ino, child_pmd)) {
                if (cap_received.id()) {
                  // store cap
                  EosFuse::Instance().getCap().store(req, cap_received);
                  md->cap_inc();
                }

                // don't overwrite md to be flushed
                if (has_flush(ino)) {
                  continue;
                }
              }
            }

            md->Locker().Lock();
          } else {
            md->Locker().Lock();
            pmd = md;

            if (EOS_LOGS_DEBUG) {
              eos_static_debug("lock pmd ino=%#lx", pmd->id());
            }
          }

          if (map->second.has_capability()) {
            // extract any new capability
            cap_received = map->second.capability();
          }

          if (child) {
            eos_static_debug("case 1 %s", md->name().c_str());
            eos::fusex::md::TYPE mdtype = md->type();
            size_t local_size = md->size();
            uint64_t local_mtime = md->mtime();
            uint64_t local_mtime_ns = md->mtime_ns();

	    md->CopyFrom(map->second);
	    md->clear_refresh();

	    shared_md d_md = EosFuse::Instance().datas.retrieve_wr_md(ino);
	    if (d_md) {
	      // see if this file is open for write, because in that case
	      // we have to keep the local size information and modification times
	      md->set_size(d_md->size());
	      md->set_mtime(d_md->mtime());
	      md->set_mtime_ns(d_md->mtime_ns());
	    } else {
	      if (has_flush(ino)) {
		md->set_size(local_size);
		md->set_mtime(local_mtime);
		md->set_mtime_ns(local_mtime_ns);
	      }
	    }
	    md->set_nchildren(md->local_children().size());
	    // if this object was a listing type, keep that
	    md->set_type(mdtype);
          } else {
            // we have to overlay the listing
            std::map<std::string, uint64_t> todelete;
            mdflush.Lock();

            if (!mdqueue.count(md->id())) {
              eos_static_debug("case 2 %s id %#lx", md->name().c_str(), md->id());
              mdflush.UnLock();
              todelete = md->get_todelete();
              // overwrite local meta data with remote state
              md->CopyFrom(map->second);
              md->get_todelete() = todelete;
              md->set_type(md->MD);
              md->set_nchildren(md->local_children().size());
            } else {
              eos_static_debug("case 3 %s children=%d listing=%d", md->name().c_str(),
                               map->second.children().size(), listing);
              mdflush.UnLock();
              todelete = md->get_todelete();
              // copy only the listing
              md->mutable_children()->clear();

              for (auto it = map->second.children().begin();
                   it != map->second.children().end(); ++it) {
                (*md->mutable_children())[eos::common::StringConversion::EncodeInvalidUTF8(
                                            it->first)] = it->second;
              }

              // keep the listing
              md->get_todelete() = todelete;
              md->set_type(md->MD);
              md->set_nchildren(md->local_children().size());
            }
          }

          md->clear_capability();
          md->set_id(ino);
          p_ino = inomap.forward(md->md_pino());
          md->set_pid(p_ino);
          eos_static_info("store remote-ino=%016lx local pino=%016lx for %016lx",
                          md-> md_pino(), md->pid(), md->id());

          for (auto it = md->get_todelete().begin(); it != md->get_todelete().end();
               ++it) {
            eos_static_info("%016lx to-delete=%s", md->id(), it->first.c_str());
          }

          if (EOS_LOGS_DEBUG) {
            eos_static_debug("store md for local-ino=%08ld remote-ino=%016lx type=%d -",
                             (long) ino, (long) map->first, md->type());
            eos_static_debug("%s", md->dump().c_str());
          }

          md->Locker().UnLock();

          if (!child) {
            if (EOS_LOGS_DEBUG) {
              eos_static_debug("cap count %d\n", pmd->cap_count());
            }

            if (!pmd->cap_count()) {
              if (EOS_LOGS_DEBUG) {
                eos_static_debug("clearing out %0016lx", pmd->id());
              }

              // we don't wipe meta-data children which we still have to flush or have open
              // if they have to be deleted, there is an explicit callback arriving for deletion
              XrdSysMutexHelper scope_lock(pmd->Locker());
              std::vector<std::string> clear_children;

              for (auto it = pmd->local_children().begin() ;
                   it != pmd->local_children().end(); ++it) {
                bool in_flush = has_flush(it->second);
                bool is_attached = EosFuse::Instance().datas.has(it->second);

                if (!in_flush && !is_attached) {
                  clear_children.push_back(it->first);
                }
              }

              for (auto it = clear_children.begin(); it != clear_children.end(); ++it) {
                pmd->local_children().erase(*it);
              }

              pmd->get_todelete().clear();
            }
          }

          if (cap_received.id()) {
            // store cap
            EosFuse::Instance().getCap().store(req, cap_received);
            md->cap_inc();
          }
        }
      } else {
        // this is a new inode we don't know yet
        md = std::make_shared<mdx>();

        if (map->second.has_capability()) {
          // extract any new capability
          cap_received = map->second.capability();
        }

        *md = map->second;
        md->clear_capability();
        md->clear_refresh();

        if ((!pmd) && (map->first == cont.ref_inode_())) {
          pmd = md;
          md->set_type(pmd->MD);
        }

        uint64_t new_ino = 0;
        new_ino = inomap.forward(md->md_ino());
        md->set_id(new_ino);
        insert(req, md, md->authid());

        if (!listing) {
          p_ino = inomap.forward(md->md_pino());
        }

        md->set_pid(p_ino);
        eos_static_info("store local pino=%016lx for %016lx", md->pid(), md->id());
        inomap.insert(map->first, new_ino);
        {
          mdmap.insertTS(new_ino, md);
          stat.inodes_inc();
          stat.inodes_ever_inc();
        }

        if ((pmd == md)) {
          if (EOS_LOGS_DEBUG) {
            eos_static_debug("cap count %d\n", pmd->cap_count());
          }

          if (!pmd->cap_count()) {
            if (EOS_LOGS_DEBUG) {
              eos_static_debug("clearing out %0016lx", pmd->id());
            }

            XrdSysMutexHelper scope_lock(pmd->Locker());
            pmd->local_children().clear();
            pmd->get_todelete().clear();
          }
        }

        if (cap_received.id()) {
          // store cap
          EosFuse::Instance().getCap().store(req, cap_received);
          md->cap_inc();
        }

        if (EOS_LOGS_DEBUG) {
          eos_static_debug("store md for local-ino=%016lx remote-ino=%016lx type=%d -",
                           (long) new_ino, (long) map->first, md->type());
        }

        if (EOS_LOGS_DEBUG) {
          eos_static_debug("%s", md->dump().c_str());
        }
      }
    }

    if (pmd) {
      pmd->Locker().Lock();
    }

    if (pmd && listing) {
      bool ret = false;

      if (!(ret = map_children_to_local(pmd))) {
        eos_static_err("local mapping has failed %d", ret);
        assert(0);
      }

      if (EOS_LOGS_DEBUG)
        for (auto map = pmd->local_children().begin();
             map != pmd->local_children().end(); ++map) {
          eos_static_debug("listing: %s [%#lx]", map->first.c_str(), map->second);
        }

      // now flag as a complete listing
      pmd->set_type(pmd->MDLS);
    }

    if (pmd) {
      pmd->Locker().UnLock();
    }
  }

  if (pmd) {
    return pmd->id();
  } else {
    return 0;
  }
}

/* -------------------------------------------------------------------------- */
void
metad::mdcflush(ThreadAssistant& assistant)
{
  uint64_t lastflushid = 0;

  while (!assistant.terminationRequested()) {
    {
      mdflush.Lock();

      if (mdqueue.count(lastflushid)) {
        // remove entries from the mdqueue, if their ref count is 0
        if (!mdqueue[lastflushid]) {
          mdqueue.erase(lastflushid);
        }
      }

      stat.inodes_backlog_store(mdqueue.size());

      while (mdqueue.size() == 0) {
        // TODO(gbitzes): Fix this, so we don't need to poll. Have ThreadAssistant
        // accept callbacks for when termination is requested, so we can wake up
        // any condvar.
        mdflush.Wait(1);

        if (assistant.terminationRequested()) {
          mdflush.UnLock();
          return;
        }
      }

      // TODO: add an optimzation to merge requests in the queue
      auto it = mdflushqueue.begin();
      uint64_t ino = it->id();
      std::string authid = it->authid();
      fuse_id f_id = it->get_fuse_id();
      mdx::md_op op = it->op();
      lastflushid = ino;
      eos_static_info("metacache::flush ino=%#lx flushqueue-size=%u", ino,
                      mdflushqueue.size());
      eos_static_info("metacache::flush %s", flushentry::dump(*it).c_str());
      mdflushqueue.erase(it);
      mdqueue[ino]--;
      mdflush.UnLock();

      if (assistant.terminationRequested()) {
        return;
      }

      if (EOS_LOGS_DEBUG) {
        eos_static_debug("metacache::flush ino=%016lx authid=%s op=%d", ino,
                         authid.c_str(), (int) op);
      }

      {
        shared_md md;

        if (!mdmap.retrieveTS(ino, md)) {
          eos_static_crit("metacache::flush failed to retrieve ino=%016lx", ino);
          continue;
        }

        eos_static_info("metacache::flush ino=%016lx", (unsigned long long) ino);

        if (op != metad::mdx::LSTORE) {
          XrdSysMutexHelper mdLock(md->Locker());

          if (!md->md_pino()) {
            // when creating objects locally faster than pushed upstream
            // we might not know the remote parent id when we insert a local
            // creation request
            shared_md pmd;

            if (mdmap.retrieveTS(md->pid(), pmd)) {
              // TODO: check if we need to lock pmd? But then we have to enforce
              // locking order child -> parent
              uint64_t md_pino = pmd->md_ino();
              eos_static_info("metacache::flush providing parent inode %016lx to %016lx",
                              md->id(), md_pino);
              md->set_md_pino(md_pino);
            } else {
              eos_static_crit("metacache::flush ino=%016lx parent remote inode not known",
                              (unsigned long long) ino);
            }
          }
        }

        if (md->id()) {
          uint64_t removeentry = 0;
          {
            md->Locker().Lock();
            int rc = 0;

            if (op == metad::mdx::RM) {
              md->set_operation(md->DELETE);
            } else {
              md->set_operation(md->SET);
            }

            if (((op == metad::mdx::ADD) ||
                 (op == metad::mdx::UPDATE) ||
                 (op == metad::mdx::RM)) &&
                md->id() != 1) {
              eos_static_info("metacache::flush backend::putMD - start");
              eos::fusex::md::TYPE mdtype = md->type();
              md->set_type(md->MD);

              // push to backend
              if ((rc = mdbackend->putMD(f_id, &(*md), authid, &(md->Locker())))) {
                eos_static_err("metacache::flush backend::putMD failed rc=%d", rc);
                // we just set an error code
                //! inomap.erase_bwd(md->id());
                //! removeentry=md->id();
                md->set_err(rc);
              } else {
                inomap.insert(md->md_ino(), md->id());
              }

              if (md->getop() != md->RM) {
                md->setop_none();
                md->clear_mv_authid();
              }

              md->set_type(mdtype);
              md->Signal();
              eos_static_info("metacache::flush backend::putMD - stop");
            }

            if ((op == metad::mdx::ADD) || (op == metad::mdx::UPDATE) ||
                (op == metad::mdx::LSTORE)) {
              // TODO: local MD store is now disabled - delete this code
              //! std::string mdstream;
              //! md->SerializeToString(&mdstream);
              //! EosFuse::Instance().getKV()->put(ino, mdstream);
              md->Locker().UnLock();
            } else {
              md->Locker().UnLock();

              if (op == metad::mdx::RM) {
                // this step is coupled to the forget function, since we cannot
                // forget an entry if we didn't process the outstanding KV changes
                stat.inodes_deleted_dec();

                if (EOS_LOGS_DEBUG) {
                  eos_static_debug("count=%d(-%d) - ino=%#lx", md->lookup_is(), 1, ino);
                }

                XrdSysMutexHelper mLock(md->Locker());

                if (md->lookup_dec(1)) {
                  // forget this inode
                  removeentry = ino;
                }
              }
            }
          }

          if (removeentry) {
            shared_md pmd;

            if (EOS_LOGS_DEBUG) {
              eos_static_debug("delete md object - ino=%#lx", removeentry);
            }

            {
              if (EOS_LOGS_DEBUG) {
                eos_static_debug("calling forget function %#lx", removeentry);
              }

              forget(0, removeentry, 0);
            }

            {
              if (pmd) {
                XrdSysMutexHelper mmLock(pmd->Locker());
                // we don't remote entries from the local deletion list because there could be
                // a race condition of a thread doing MDLS overwriting the locally deleted entry
                pmd->get_todelete().erase(eos::common::StringConversion::EncodeInvalidUTF8(
                                            md->name()));
                pmd->Signal();
              }
            }
          }
        }
      }
    }
  }
}

/* -------------------------------------------------------------------------- */
void
metad::mdsizeflush(ThreadAssistant& assistant)
{
  // TODO: implement MGM size updates while writing files
  while (!assistant.terminationRequested()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(128));
  }

  return;
}


/* -------------------------------------------------------------------------- */
void
metad::mdstackfree(ThreadAssistant& assistant)
{
  size_t cnt = 0;
  int max_inodes = EosFuse::Instance().Config().options.inmemory_inodes;

  while (!assistant.terminationRequested()) {
    cnt++;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // do this ~every 64 seconds
    if (!(cnt % 128)) {
      EosFuse::Instance().Tracker().clean();
    }

    // do this ~every 128 seconds
    if (!(cnt % 256)) {
      XrdSysMutexHelper mLock(mdmap);

      for (auto it = mdmap.begin(); it != mdmap.end();) {
        if (!it->second) {
          it++;
          continue;
        }

        // if the parent is gone, we can remove the child
        if ((!mdmap.count(it->second->pid())) &&
            (!S_ISDIR(it->second->mode()) || it->second->deleted())) {
          eos_static_debug("removing orphaned inode from mdmap ino=%#lx path=%s",
                           it->first, it->second->fullpath().c_str());
          mdmap.lru_remove(it->first);
          it = mdmap.erase(it);
          stat.inodes_dec();
        } else {
          if (it->second->deleted()) {
            if ( (!has_flush(it->first)) &&
		 (!EosFuse::Instance().datas.has(it->first)) ) {
              eos_static_debug("removing deleted inode from mdmap ino=%#lx path=%s",
                               it->first, it->second->fullpath().c_str());
              mdmap.lru_remove(it->first);
              it = mdmap.erase(it);
              stat.inodes_dec();
            } else {
              it++;
            }
          } else {
            it++;
          }
        }
      }
    }

    if (!EosFuse::Instance().Config().mdcachedir.empty()) {
      // level the inodes stored in memory and eventually swap out into kv store
      int swap_out_inodes = 0 ;
      size_t filled = 0;
      size_t empty = 0;
      {
        XrdSysMutexHelper mLock(mdmap);

        for (auto it = mdmap.begin(); it != mdmap.end(); ++it) {
          if (it->second) {
            filled++;
          } else {
            empty++;
          }
        }
      }

      do {
        swap_out_inodes = mdmap.sizeTS() - max_inodes -
                          EosFuse::Instance().mds.stats().inodes_stacked();

        if (swap_out_inodes > 0) {
          eos_static_info("swap-out %d inodes", swap_out_inodes);
          // grab the last lru inode and swap out
          mdmap.Lock();
          mdmap.lru_dump();
          uint64_t inode_to_swap = mdmap.lru_oldest();

          if (!inode_to_swap) {
            // nothing in the lru list anymore
            mdmap.UnLock();
            break;
          }

          if (mdmap.count(inode_to_swap)) {
            shared_md md = mdmap[inode_to_swap];

            if ((md.use_count() > 2) ||
                (md && md->LockTable().size())) {
              eos_static_info("swap-out skipping referenced ino=%#llx ref-count=%lu\n",
                              inode_to_swap,
                              md.use_count());

              if (md) {
                mdmap.lru_update(inode_to_swap, md);
              }

              mdmap.UnLock();
              continue;
            }

            mdmap.lru_remove(inode_to_swap);

            if (md) {
              eos_static_info("swap-out lru-removed ino=%#llx oldest=%#llx", inode_to_swap,
                              mdmap.lru_oldest());
	      mdmap.lru_remove(inode_to_swap);
              mdmap[inode_to_swap] = 0;

              if (mdmap.swap_out(md)) {
                eos_static_err("swap-out failed for ino=%#llx", inode_to_swap);
              }
            }
          } else {
            mdmap.lru_remove(inode_to_swap);
          }

          mdmap.UnLock();
        }
      } while ((swap_out_inodes > 0) &&
               (!assistant.terminationRequested()));
    }
  }

  return;
}

/* -------------------------------------------------------------------------- */
bool
metad::determineLockOrder(shared_md md1, shared_md md2)
{
  // Determine lock order of _two_ md objects, which is not as trivial as it
  // might seem:
  //
  // Children are _always_ locked before their parents!
  // If and only if two md's are not related as in parent and child, we decide the
  // order based on increasing inodes.
  //
  // Example 1: /a/b/c and /a/ -> /a/b/c locked first, as it's a child of /a/
  // Example 2: /a/b/c and /a/b/d -> Decision based on increasing inode.
  //
  // This procedure is very expensive.. we should simplify if possible..
  md1->Locker().Lock();
  fuse_ino_t inode1 = md1->id();
  md1->Locker().UnLock();
  md2->Locker().Lock();
  fuse_ino_t inode2 = md2->id();
  md2->Locker().UnLock();

  if (isChild(md1, inode2)) {
    return true;
  }

  if (isChild(md2, inode1)) {
    return false;
  }

  // Determine based on increasing inode.
  return inode1 < inode2;
}

/* -------------------------------------------------------------------------- */
bool
metad::isChild(shared_md potentialChild, fuse_ino_t parentId)
{
  XrdSysMutexHelper helper(potentialChild->Locker());

  if (potentialChild->id() == 1 || potentialChild->id() == 0) {
    return false;
  }

  if (potentialChild->id() == parentId) {
    return true;
  }

  shared_md pmd;

  if (!mdmap.retrieveTS(potentialChild->pid(), pmd)) {
    eos_static_warning("could not lookup parent ino=%d of %d when determining lock order..",
                       potentialChild->pid(), potentialChild->id());
    return false;
  }

  helper.UnLock();
  return isChild(pmd, parentId);
}

/* -------------------------------------------------------------------------- */
int
metad::calculateDepth(shared_md md)
{
  if (md->id() == 1 || md->id() == 0) {
    return 1;
  }

  fuse_ino_t pino = md->pid();

  if (pino == 1 || pino == 0) {
    return 2;
  }

  shared_md pmd;

  if (!mdmap.retrieveTS(pino, pmd)) {
    eos_static_warning("could not lookup parent ino=%d of %d when calculating depth..",
                       pino, md->id());
    return -1;
  }

  XrdSysMutexHelper mmLock(pmd->Locker());
  return calculateDepth(pmd) + 1;
}

/* -------------------------------------------------------------------------- */
std::string
metad::calculateLocalPath(shared_md md)
{
  std::string lpath = "/" + md->name();

  if (md->id() == 1 || md->id() == 0) {
    return "/";
  }

  fuse_ino_t pino = md->pid();

  if (pino == 1 || pino == 0) {
    lpath = "/";
    lpath += md->name();
    return lpath;
  }

  shared_md pmd;

  if (!mdmap.retrieveTS(pino, pmd)) {
    eos_static_warning("could not lookup parent ino=%d of %d when calculating depth..",
                       pino, md->id());
    return "";
  }

  XrdSysMutexHelper mmLock(pmd->Locker());
  return calculateLocalPath(pmd) + lpath;
}

/* -------------------------------------------------------------------------- */
void
metad::mdcommunicate(ThreadAssistant& assistant)
{
  std::string sendlog = "";
  std::string stacktrace = "";
  eos::fusex::container hb;
  hb.mutable_heartbeat_()->set_name(zmq_name);
  hb.mutable_heartbeat_()->set_host(zmq_clienthost);
  hb.mutable_heartbeat_()->set_uuid(zmq_clientuuid);
  hb.mutable_heartbeat_()->set_version(VERSION);
  hb.mutable_heartbeat_()->set_protversion(FUSEPROTOCOLVERSION);
  hb.mutable_heartbeat_()->set_pid((int32_t) getpid());
  hb.mutable_heartbeat_()->set_starttime(time(NULL));
  hb.mutable_heartbeat_()->set_leasetime(
    EosFuse::Instance().Config().options.leasetime);
  hb.mutable_heartbeat_()->set_mount(EosFuse::Instance().Config().localmountdir);
  hb.mutable_heartbeat_()->set_automounted(
    EosFuse::Instance().Config().options.automounted);
  hb.set_type(hb.HEARTBEAT);
  eos::fusex::response rsp;
  size_t cnt = 0;
  int interval = 10;
  bool shutdown = false;
  bool first = true;

  while (!assistant.terminationRequested() || shutdown == false) {
    try {
      std::unique_lock<std::mutex> connectionMutex(zmq_socket_mutex);
      eos_static_debug("");
      zmq::pollitem_t items[] = {
        {static_cast<void*>(*z_socket), 0, ZMQ_POLLIN, 0}
      };
      struct timespec ts;
      eos::common::Timing::GetTimeSpec(ts);

      do {
        // if there is a ZMQ reconnection to be done we release the ZQM socket mutex
        if (zmq_wants_to_connect()) {
          connectionMutex.unlock();
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          continue;
        }

        if (first) {
          // we want to see the first hearteat directly after startup
          first = false;
          break;
        }

        // 10 milliseconds
        zmq_poll(items, 1, 10);

        if (assistant.terminationRequested()) {
          shutdown = true;
          EosFuse::Instance().caps.reset();
          eos_static_notice("sending shutdown heartbeat message");
          hb.mutable_heartbeat_()->set_shutdown(true);
          break;
        }

        if (items[0].revents & ZMQ_POLLIN) {
          int rc;
          int64_t more = 0;
          size_t more_size = sizeof(more);
          zmq_msg_t message;
          rc = zmq_msg_init(&message);

          if (rc) {
            rc = 0;
          }

          do {
            int size = zmq_msg_recv(&message, static_cast<void*>(*z_socket), 0);
            size = size;
            zmq_getsockopt(static_cast<void*>(*z_socket), ZMQ_RCVMORE, &more, &more_size);
          } while (more);

          std::string s((const char*) zmq_msg_data(&message), zmq_msg_size(&message));
          rsp.Clear();

          if (rsp.ParseFromString(s)) {
            if (rsp.type() == rsp.EVICT) {
              eos_static_crit("evict message from MD server - instruction: %s",
                              rsp.evict_().reason().c_str());

              if (rsp.evict_().reason().find("setlog") != std::string::npos) {
                if (rsp.evict_().reason().find("debug") != std::string::npos) {
                  eos::common::Logging::GetInstance().SetLogPriority(LOG_DEBUG);
                }

                if (rsp.evict_().reason().find("info") != std::string::npos) {
                  eos::common::Logging::GetInstance().SetLogPriority(LOG_INFO);
                }

                if (rsp.evict_().reason().find("error") != std::string::npos) {
                  eos::common::Logging::GetInstance().SetLogPriority(LOG_ERR);
                }

                if (rsp.evict_().reason().find("notice") != std::string::npos) {
                  eos::common::Logging::GetInstance().SetLogPriority(LOG_NOTICE);
                }

                if (rsp.evict_().reason().find("warning") != std::string::npos) {
                  eos::common::Logging::GetInstance().SetLogPriority(LOG_WARNING);
                }

                if (rsp.evict_().reason().find("crit") != std::string::npos) {
                  eos::common::Logging::GetInstance().SetLogPriority(LOG_CRIT);
                }
              } else  {
                if (rsp.evict_().reason().find("stacktrace") != std::string::npos) {
                  std::string stacktrace_file = EosFuse::Instance().Config().logfilepath;
                  stacktrace_file += ".strace";
                  eos::common::StackTrace::GdbTrace(0, getpid(), "thread apply all bt",
                                                    stacktrace_file.c_str(), &stacktrace);
                  hb.mutable_heartbeat_()->set_trace(stacktrace);
                } else {
                  if (rsp.evict_().reason().find("sendlog") != std::string::npos) {
		    std::string refs;
		    XrdCl::Proxy::WriteAsyncHandler::DumpReferences(refs);
		    eos_static_warning("\n%s\n", refs.c_str());

                    sendlog = "";
                    int logtagindex =
                      eos::common::Logging::GetInstance().GetPriorityByString("debug");

                    for (int j = 0; j <= logtagindex; j++) {
                      for (int i = 1; i <= 512; i++) {
                        std::string logline;
                        eos::common::Logging::GetInstance().gMutex.Lock();
                        const char* log = eos::common::Logging::GetInstance().gLogMemory[j][
                                            (eos::common::Logging::GetInstance().gLogCircularIndex[j] - i +
                                             eos::common::Logging::GetInstance().gCircularIndexSize) %
                                            eos::common::Logging::GetInstance().gCircularIndexSize].c_str();

                        if (log) {
                          logline = log;
                        }

                        eos::common::Logging::GetInstance().gMutex.UnLock();

                        if (logline.length()) {
                          sendlog += logline;
                          sendlog += "\n";
                        }
                      }
                    }

                    hb.mutable_heartbeat_()->set_log(sendlog);
                  } else {
                    if (rsp.evict_().reason().find("resetbuffer") != std::string::npos) {
                      eos_static_warning("MGM asked us to reset the buffer in flight");
                      XrdCl::Proxy::sWrBufferManager.reset();
                      XrdCl::Proxy::sRaBufferManager.reset();
                    } else if (rsp.evict_().reason().find("log2big") != std::string::npos) {
                      // we were asked to truncate our logfile
                      EosFuse::Instance().truncateLogFile();
                    } else {
                      // suicide
                      if (rsp.evict_().reason().find("abort") != std::string::npos) {
                        kill(getpid(), SIGABRT);
                      } else {
                        kill(getpid(), SIGTERM);
                      }

                      pause();
                    }
                  }
                }
              }
            }

            if (rsp.type() == rsp.DROPCAPS) {
              eos_static_notice("MGM asked us to drop all known caps");
              // a newly started MGM requests this as a response to the first heartbeat
              EosFuse::Instance().caps.reset();
            }

            if (rsp.type() == rsp.CONFIG) {
              if (rsp.config_().hbrate()) {
                eos_static_warning("MGM asked us to set our heartbeat interval to %d seconds, %s dentry-messaging, %s writesizeflush, %s appname, %s mdquery versions %s and server-version=%s",
                                   rsp.config_().hbrate(),
                                   rsp.config_().dentrymessaging() ? "enable" : "disable",
                                   rsp.config_().writesizeflush() ?  "enable" : "disable",
                                   rsp.config_().appname() ? "accepts" : "rejects",
                                   rsp.config_().mdquery() ? "accepts" : "rejects",
				   rsp.config_().hideversion() ? "hidden" : "visible",
                                   rsp.config_().serverversion().c_str());
                interval = (int) rsp.config_().hbrate();
                XrdSysMutexHelper cLock(EosFuse::Instance().mds.ConfigMutex);
                EosFuse::Instance().mds.dentrymessaging = rsp.config_().dentrymessaging();
                EosFuse::Instance().mds.writesizeflush = rsp.config_().writesizeflush();
                EosFuse::Instance().mds.appname = rsp.config_().appname();
                EosFuse::Instance().mds.mdquery = rsp.config_().mdquery();
		EosFuse::Instance().mds.hideversion = rsp.config_().hideversion();

                if (rsp.config_().serverversion().length()) {
                  EosFuse::Instance().mds.serverversion = rsp.config_().serverversion();
                }
              }
            }

            if (rsp.type() == rsp.DENTRY) {
              uint64_t md_ino = rsp.dentry_().md_ino();
              std::string authid = rsp.dentry_().authid();
              std::string name = rsp.dentry_().name();
              uint64_t ino = inomap.forward(md_ino);

              if (rsp.dentry_().type() == rsp.dentry_().ADD) {
              } else if (rsp.dentry_().type() == rsp.dentry_().REMOVE) {
                eos_static_notice("remove-dentry: remote-ino=%#lx ino=%#lx clientid=%s authid=%s name=%s",
                                  md_ino, ino, rsp.lease_().clientid().c_str(), authid.c_str(), name.c_str());

                // remove directory entry
                if (EosFuse::Instance().Config().options.md_kernelcache) {
                  kernelcache::inval_entry(ino, name);
                }

                shared_md pmd;

                if (ino && mdmap.retrieveTS(ino, pmd)) {
                  XrdSysMutexHelper mLock(pmd->Locker());

                  if (pmd->local_children().count(
                        eos::common::StringConversion::EncodeInvalidUTF8(name))) {
                    pmd->local_children().erase(eos::common::StringConversion::EncodeInvalidUTF8(
                                                  name));
                    pmd->get_todelete().erase(eos::common::StringConversion::EncodeInvalidUTF8(
                                                name));
                    pmd->set_nchildren(pmd->nchildren() - 1);
                  }
                }
              }
            }

            if (rsp.type() == rsp.REFRESH) {
              uint64_t md_ino = rsp.refresh_().md_ino();
              uint64_t ino = inomap.forward(md_ino);
              mode_t mode = 0;
              eos_static_notice("refresh-dentry: remote-ino=%#lx ino=%#lx",
                                md_ino, ino);
              shared_md md;

              // force meta data refresh
              if (ino && mdmap.retrieveTS(ino, md)) {
                XrdSysMutexHelper mLock(md->Locker());
                md->force_refresh();
                mode = md->mode();
              }

              if (EOS_LOGS_DEBUG) {
                eos_static_debug("%s", dump_md(md).c_str());
              }

              if (EosFuse::Instance().Config().options.md_kernelcache) {
                eos_static_info("invalidate metadata cache for ino=%#lx", ino);
                kernelcache::inval_inode(ino, S_ISDIR(mode) ? false : true);
              }
            }

            if (rsp.type() == rsp.LEASE) {
              uint64_t md_ino = rsp.lease_().md_ino();
              std::string authid = rsp.lease_().authid();
              uint64_t ino = inomap.forward(md_ino);
              eos_static_notice("lease: remote-ino=%#lx ino=%#lx clientid=%s authid=%s",
                                md_ino, ino, rsp.lease_().clientid().c_str(), authid.c_str());
              shared_md check_md;

              if (ino && mdmap.retrieveTS(ino, check_md)) {
                std::string capid = cap::capx::capid(ino, rsp.lease_().clientid());

                // wait that the inode is flushed out of the mdqueue
                do {
                  mdflush.Lock();

                  if (mdqueue.count(ino)) {
                    mdflush.UnLock();
                    eos_static_info("lease: delaying cap-release remote-ino=%#lx ino=%#lx clientid=%s authid=%s",
                                    md_ino, ino, rsp.lease_().clientid().c_str(), authid.c_str());
                    std::this_thread::sleep_for(std::chrono::milliseconds(25));

                    if (assistant.terminationRequested()) {
                      return;
                    }
                  } else {
                    mdflush.UnLock();
                    break;
                  }
                } while (1);

                eos_static_debug("");
                fuse_ino_t ino = EosFuse::Instance().getCap().forget(capid);
                shared_md md;

                if (mdmap.retrieveTS(ino, md)) {
                  md->Locker().Lock();
                }

                // invalidate children
                if (md) {
                  if (md->id()) {
                    // force an update of the metadata with next access
                    eos_static_info("md=%16x", md->id());
                    cleanup(md);

                    if (EOS_LOGS_DEBUG) {
                      eos_static_debug("%s", dump_md(md).c_str());
                    }
                  } else {
                    md->Locker().UnLock();
                  }
                }
              } else {
                // there might have been several caps and the first has wiped already the MD,
                // still we want to remove the cap entry
                std::string capid = cap::capx::capid(ino, rsp.lease_().clientid());
                eos_static_debug("");
                EosFuse::Instance().getCap().forget(capid);
              }
            }

            if (rsp.type() == rsp.CAP) {
              std::string clientid = rsp.cap_().clientid();
              uint64_t ino = inomap.forward(rsp.cap_().id());
              cap::shared_cap cap = EosFuse::Instance().caps.get(ino, clientid);
              eos_static_notice("cap-update: cap-id=%#lx %s", rsp.cap_().id(),
                                cap->dump().c_str());

              if (cap->id()) {
                EosFuse::Instance().caps.update_quota(cap, rsp.cap_()._quota());
                eos_static_notice("cap-update: cap-id=%#lx %s", rsp.cap_().id(),
                                  cap->dump().c_str());
              }
            }

            if (rsp.type() == rsp.MD) {
              fuse_req_t req;
              memset(&req, 0, sizeof(fuse_req_t));
              uint64_t md_ino = rsp.md_().md_ino();
              std::string authid = rsp.md_().authid();
              uint64_t ino = inomap.forward(md_ino);
              eos_static_notice("md-update: remote-ino=%#lx ino=%#lx authid=%s",
                                md_ino, ino, authid.c_str());
              // we get this when a file update/flush appeared
              shared_md md;
              int64_t bookingsize = 0;
              uint64_t pino = 0;
              mode_t mode = 0;
              std::string md_clientid;
              std::string old_name;

              if (mdmap.retrieveTS(ino, md)) {
                eos_static_notice("md-update: (existing) remote-ino=%#lx ino=%#lx authid=%s",
                                  md_ino, ino, authid.c_str());

                // updated file MD
                if (EOS_LOGS_DEBUG) {
                  eos_static_debug("%s op=%d", md->dump().c_str(), md->getop());
                }

                md->Locker().Lock();
                bookingsize = rsp.md_().size() - md->size();
                md_clientid = rsp.md_().clientid();
                eos_static_info("md-update: %s %s", md->name().c_str(),
                                rsp.md_().name().c_str());

                // check if this implies a rename
                if (md->name() != rsp.md_().name()) {
                  old_name = rsp.md_().name();
                }

                // verify that this record is newer than
                if (rsp.md_().clock() >= md->clock()) {
                  eos_static_info("overwriting clock MD %#lx => %#lx", md->clock(),
                                  rsp.md_().clock());
                  *md = rsp.md_();
                  md->set_creator(false);
                  md->set_bc_time(time(NULL));
                } else {
                  eos_static_warning("keeping clock MD %#lx => %#lx", md->clock(),
                                     rsp.md_().clock());
                }

                md->clear_clientid();
                pino = inomap.forward(md->md_pino());
                md->set_id(ino);
                md->set_pid(pino);
                mode = md->mode();

                if (EOS_LOGS_DEBUG) {
                  eos_static_debug("%s op=%d", md->dump().c_str(), md->getop());
                }

                // update the local store
                update(req, md, authid, true);
                std::string name = md->name();
                md->Locker().UnLock();
                // adjust local quota
                cap::shared_cap cap = EosFuse::Instance().caps.get(pino, md_clientid);

                if (cap->id()) {
                  if (bookingsize >= 0) {
                    EosFuse::Instance().caps.book_volume(cap, (uint64_t) bookingsize);
                  } else {
                    EosFuse::Instance().caps.free_volume(cap, (uint64_t) - bookingsize);
                  }

                  EosFuse::instance().caps.book_inode(cap);
                } else {
                  eos_static_debug("missing quota node for pino=%#lx and clientid=%s",
                                   pino, md->clientid().c_str());
                }

                // possibly invalidate kernel cache
                if (EosFuse::Instance().Config().options.md_kernelcache ||
                    EosFuse::Instance().Config().options.data_kernelcache) {
                  eos_static_info("invalidate data cache for ino=%#lx", ino);
                  kernelcache::inval_inode(ino, S_ISDIR(mode) ? false : true);
                }

                if (EosFuse::Instance().Config().options.md_kernelcache && old_name.length()) {
                  eos_static_info("invalidate previous name for ino=%#lx old-name=%s", ino,
                                  old_name.c_str());
                  kernelcache::inval_entry(pino, old_name.c_str());
                  kernelcache::inval_inode(pino, false);
                }

                if (S_ISREG(mode)) {
                  // invalidate local disk cache
                  EosFuse::Instance().datas.invalidate_cache(ino);
                  eos_static_info("invalidate local disk cache for ino=%#lx", ino);
                }
              } else {
                eos_static_info("md-update: (new) remote-ino=%#lx ino=%#lx authid=%s",
                                md_ino, ino, authid.c_str());
                // new file
                md = std::make_shared<mdx>();
                *md = rsp.md_();
                md->set_id(md_ino);
                insert(req, md, authid);
                uint64_t md_pino = md->md_pino();
                std::string md_clientid = md->clientid();
                uint64_t md_size = md->size();
                md->Locker().Lock();
                uint64_t pino = inomap.forward(md_pino);
                shared_md pmd;

                if (pino && mdmap.retrieveTS(pino, pmd)) {
                  if (md->pt_mtime()) {
                    pmd->set_mtime(md->pt_mtime());
                    pmd->set_mtime_ns(md->pt_mtime_ns());
                  }

                  md->clear_pt_mtime();
                  md->clear_pt_mtime_ns();
                  inomap.insert(md->md_ino(), md->id());
                  add(0, pmd, md, authid, true);
                  update(req, pmd, authid, true);
                  // adjust local quota
                  cap::shared_cap cap = EosFuse::Instance().caps.get(pino, md_clientid);

                  if (cap->id()) {
                    EosFuse::Instance().caps.book_volume(cap, md_size);
                    EosFuse::instance().caps.book_inode(cap);
                  } else {
                    eos_static_debug("missing quota node for pino=%#llx and clientid=%s",
                                     pino, md->clientid().c_str());
                  }

                  md->Locker().UnLock();

                  // possibly invalidate kernel cache for parent
                  if (EosFuse::Instance().Config().options.md_kernelcache) {
                    eos_static_info("invalidate md cache for ino=%016lx", pino);
                    kernelcache::inval_entry(pino, md->name());
                    kernelcache::inval_inode(pino, false);
		    XrdSysMutexHelper mLock(pmd->Locker());
                    pmd->local_enoent().erase(md->name());
                  }
                } else {
                  eos_static_err("missing parent mapping pino=%16x for ino%16x",
                                 md_pino,
                                 md_ino);
                  md->Locker().UnLock();
                }
              }
            }
          } else {
            eos_static_err("unable to parse message");
          }

          zmq_msg_close(&message);
        }

        // leave the loop to send a heartbeat after the given interval
        if ((eos::common::Timing::GetCoarseAgeInNs(&ts,
             0) >= (interval * 1000000000ll))) {
          break;
        }
      } while (1);

      eos_static_debug("send");
      // prepare a heart-beat message
      struct timespec tsnow;
      eos::common::Timing::GetTimeSpec(tsnow);
      hb.mutable_heartbeat_()->set_clock(tsnow.tv_sec);
      hb.mutable_heartbeat_()->set_clock_ns(tsnow.tv_nsec);

      if (!(cnt % (60 / interval))) {
        // we send a statistics update every 60 heartbeats
        EosFuse::Instance().getHbStat((*hb.mutable_statistics_()));
        std::string blocker;
        hb.mutable_statistics_()->set_blockedms(
          EosFuse::Instance().Tracker().blocked_ms(blocker));
        hb.mutable_statistics_()->set_blockedfunc(blocker);
      } else {
        hb.clear_statistics_();
      }

      {
        // add caps to be revoked
        XrdSysMutexHelper rLock(EosFuse::Instance().getCap().get_revocationLock());
        // clear the hb map
        hb.mutable_heartbeat_()->mutable_authrevocation()->clear();
        auto rmap = hb.mutable_heartbeat_()->mutable_authrevocation();
        cap::revocation_set_t& revocationset =
          EosFuse::Instance().getCap().get_revocationmap();
        size_t n_revocations = 0;

        for (auto it = revocationset.begin(); it != revocationset.end();) {
          (*rmap)[*it] = 0;
          eos_static_notice("cap-revocation: authid=%s", it->c_str());
          it = revocationset.erase(it);
          n_revocations++;

          if (n_revocations > 32 * 1024) {
            eos_static_notice("stopped revocations after 32k entries");
            break;
          }
        }

        eos_static_debug("cap-revocation: map-size=%u",
                         revocationset.size());
      }

      std::string hbstream;
      hb.SerializeToString(&hbstream);
      z_socket->send(hbstream.c_str(), hbstream.length());

      if (!is_visible()) {
        set_is_visible(1);
      }

      hb.mutable_heartbeat_()->clear_log();
      hb.mutable_heartbeat_()->clear_trace();
    } catch (std::exception& e) {
      eos_static_err("catched exception %s", e.what());
    }

    cnt++;
  }
}

/* -------------------------------------------------------------------------- */
void
metad::vmap::insert(fuse_ino_t a, fuse_ino_t b)
{
  // weonly store ino=1 mappings
  if ((a != 1) &&
      (b != 1)) {
    return;
  }

  eos_static_info("inserting %llx <=> %llx", a, b);
  XrdSysMutexHelper mLock(mMutex);

  if (fwd_map.count(a) && fwd_map[a] == b) {
    return;
  }

  if (bwd_map.count(b)) {
    fwd_map.erase(bwd_map[b]);
  }

  fwd_map[a] = b;
  bwd_map[b] = a;
}

/* -------------------------------------------------------------------------- */
std::string
metad::vmap::dump()
{
  //XrdSysMutexHelper mLock(this);
  std::string sout;
  char stime[1024];
  snprintf(stime, sizeof(stime), "%lu this=%llx forward=%lu backward=%lu",
           time(NULL), (unsigned long long) this, fwd_map.size(), bwd_map.size());
  sout += stime;
  sout += "\n";

  for (auto it = fwd_map.begin(); it != fwd_map.end(); it++) {
    char out[1024];
    snprintf(out, sizeof(out), "%16lx => %16lx\n", it->first, it->second);
    sout += out;
  }

  for (auto it = bwd_map.begin(); it != bwd_map.end(); it++) {
    char out[1024];
    snprintf(out, sizeof(out), "%16lx <= %16lx\n", it->first, it->second);
    sout += out;
  }

  sout += "end\n";
  return sout;
}

/* -------------------------------------------------------------------------- */
void
metad::vmap::erase_fwd(fuse_ino_t lookup)
{
  XrdSysMutexHelper mLock(mMutex);

  if (fwd_map.count(lookup)) {
    bwd_map.erase(fwd_map[lookup]);
  }

  fwd_map.erase(lookup);
}

/* -------------------------------------------------------------------------- */
void
metad::vmap::erase_bwd(fuse_ino_t lookup)
{
  XrdSysMutexHelper mLock(mMutex);

  if (bwd_map.count(lookup)) {
    fwd_map.erase(bwd_map[lookup]);
  }

  bwd_map.erase(lookup);
}

/* -------------------------------------------------------------------------- */
fuse_ino_t
metad::vmap::forward(fuse_ino_t lookup)
{
  XrdSysMutexHelper mLock(mMutex);
  auto it = fwd_map.find(lookup);
  fuse_ino_t ino = (it == fwd_map.end()) ? 0 : it->second;

  if (!ino) {
    return lookup;
  }

  return ino;
}

/* -------------------------------------------------------------------------- */
fuse_ino_t
metad::vmap::backward(fuse_ino_t lookup)
{
  XrdSysMutexHelper mLock(mMutex);
  auto it = bwd_map.find(lookup);
  return (it == bwd_map.end()) ? lookup : it->second;
}


/* -------------------------------------------------------------------------- */
size_t
metad::pmap::sizeTS()
{
  XrdSysMutexHelper mLock(this);
  return size();
}

/* -------------------------------------------------------------------------- */
bool
metad::pmap::retrieveOrCreateTS(fuse_ino_t ino, shared_md& ret)
{
  XrdSysMutexHelper mLock(this);

  if (this->retrieve(ino, ret)) {
    return false;
  }

  ret = std::make_shared<mdx>();

  if (ino) {
    (*this)[ino] = ret;
  }

  return true;
}

/* -------------------------------------------------------------------------- */
bool
metad::pmap::retrieveTS(fuse_ino_t ino, shared_md& ret)
{
  XrdSysMutexHelper mLock(this);
  return this->retrieve(ino, ret);
}

/* -------------------------------------------------------------------------- */
bool
metad::pmap::retrieve(fuse_ino_t ino, shared_md& ret)
{
  auto it = this->find(ino);

  if (it == this->end()) {
    if (!ret) {
      ret = std::make_shared<mdx>();
      ret->set_err(ENOENT);
    }

    return false;
  }

  ret = it->second;
  eos_static_debug("retc=%x", (bool)(ret));

  if (!ret) {
    ret = std::make_shared<mdx>();

    // swap-in this inode
    if (swap_in(ino, ret)) {
      eos_static_crit("failed to swap-in ino=%#llx", ino);
      return false;
    }

    // attach the new object
    (*this)[ino] = ret;
    // add to the lru list
    lru_add(ino, ret);
  }

  // update lru entry whenever we retrieve something
  lru_update(ino, ret);
  return true;
}

/* -------------------------------------------------------------------------- */
uint64_t
metad::pmap::lru_oldest() const
{
  return lru_last;
}

/* -------------------------------------------------------------------------- */
void
metad::pmap::lru_add(fuse_ino_t ino, shared_md md)
{
  if (ino == 1) {
    return;
  }

  md->set_lru_prev(lru_first);
  md->set_lru_next(0);

  // lru list insert with outside lock handling
  if (this->count(lru_first)) {
    if ((*this)[lru_first]) {
      // connect the new inode to the head of the lru list
      (*this)[lru_first]->set_lru_next(ino);
    } else {
      // points to swapped-out entry
      lru_last = ino;
    }
  }

  lru_first = ino;

  if (!lru_last) {
    lru_last = ino;
  }

  eos_static_info("ino=%#llx first=%#llx last=%#llx prev=%llx next=%#llx", ino,
                  lru_first, lru_last, md->lru_prev(), md->lru_next());
}

/* -------------------------------------------------------------------------- */
void
metad::pmap::lru_remove(fuse_ino_t ino)
{
  if (ino == 1) {
    return;
  }

  uint64_t prev = 0;
  uint64_t next = 0;

  if (EOS_LOGS_DEBUG)
    eos_static_debug("ino=%#llx first=%#llx last=%#llx", ino,
                     lru_first, lru_last);

  // lru list handling with outside lock handling
  if (this->count(ino)) {
    shared_md smd = (*this)[ino];

    if (smd) {
      prev = (*this)[ino]->lru_prev();
      next = (*this)[ino]->lru_next();

      if (this->count(prev) && (*this)[prev]) {
        (*this)[prev]->set_lru_next(next);
      } else {
        // this is the tail of the LRU list
        lru_last = next;
      }

      if (this->count(next) && (*this)[next]) {
        (*this)[next]->set_lru_prev(prev);
      } else {
        // this is the head of the LRU list
        lru_first = prev;
      }
    }

    if (EOS_LOGS_DEBUG) {
      eos_static_debug("last:%#llx => %#llx (prev=%#llx)", lru_last, next, prev);
    }
  }

  if (EOS_LOGS_DEBUG)
    eos_static_debug("ino=%#llx first=%#llx last=%#llx prev=%#llx next=%#llx", ino,
                     lru_first, lru_last, prev, next);
}

/* -------------------------------------------------------------------------- */
void
metad::pmap::lru_update(fuse_ino_t ino, shared_md md)
{
  if (ino == 1) {
    return;
  }

  if (lru_first == ino) {
    return;
  }

  if (EOS_LOGS_DEBUG)
    eos_static_debug("ino=%#llx first=%#llx last=%#llx", ino,
                     lru_first, lru_last);

  // move an lru item to the head of the list
  uint64_t prev = md->lru_prev();
  uint64_t next = md->lru_next();

  if (this->count(prev) && (*this)[prev]) {
    (*this)[prev]->set_lru_next(next);
  } else {
    if (next) {
      lru_last = next;
    } else {
      lru_last = ino;
    }
  }

  if (this->count(next) && (*this)[next]) {
    (*this)[next]->set_lru_prev(prev);
  }

  if (this->count(lru_first) && (*this)[lru_first]) {
    (*this)[lru_first]->set_lru_next(ino);
    md->set_lru_prev(lru_first);
    md->set_lru_next(0);
    lru_first = ino;
  }

  if (EOS_LOGS_DEBUG)
    eos_static_debug("ino=%#llx first=%#llx last=%#llx prev=%#llx next=%#llx", ino,
                     lru_first, lru_last, prev, next);
}

/* -------------------------------------------------------------------------- */
void
metad::pmap::lru_dump()
{
  if (!EOS_LOGS_DEBUG) {
    return;
  }

  uint64_t start = lru_first;
  std::stringstream ss;

  do {
    if (this->count(start)) {
      shared_md md = (*this)[start];
      ss << start << "[" << md->lru_next() << ".." << md->lru_prev() << "]" <<
         std::endl;

      if (start == md->lru_prev()) {
        eos_static_crit("corruption in list");
        break;
      }

      start = md->lru_prev();
    } else {
      start = 0;
    }
  } while (start);

  eos_static_debug("%s", ss.str().c_str());
  eos_static_debug("first=%#llx last=%#llx",
                   lru_first, lru_last);
}

/* -------------------------------------------------------------------------- */
int
metad::mdx::state_serialize(std::string& mdsstream)
{
  eos::fusex::md_state state;
  // serialize in-memory state into md_state and then into string
  state.set_op(op);
  state.set_lookup_cnt(lookup_cnt);
  state.set_cap_cnt(cap_cnt);
  state.set_opendir_cnt(opendir_cnt);
  state.set_lock_remote(lock_remote);
  state.set_refresh(refresh);
  state.set_rmrf(rmrf);

  for (auto it = todelete.begin(); it != todelete.end() ; ++it) {
    (*(state.mutable_todelete()))[it->first] = it->second;
  }

  for (auto it = _local_children.begin(); it != _local_children.end() ; ++it) {
    (*(state.mutable_children()))[it->first] = it->second;
  }

  for (auto it = _local_enoent.begin(); it != _local_enoent.end(); ++it) {
    (*(state.mutable_enoent())) [*it] = 0;
  }

  if (!state.SerializeToString(&mdsstream)) {
    return EFAULT;
  }

  return 0;
}


/* -------------------------------------------------------------------------- */
int
metad::mdx::state_deserialize(std::string& mdsstream)
{
  eos::fusex::md_state state;

  // deserialize in-memory state from string
  if (!state.ParseFromString(mdsstream)) {
    return EFAULT;
  }

  op = (metad::mdx::md_op)state.op();
  lookup_cnt = state.lookup_cnt();
  cap_cnt = state.cap_cnt();
  opendir_cnt = state.opendir_cnt();
  lock_remote = state.lock_remote();
  refresh = state.refresh();
  rmrf = state.rmrf();

  for (auto it = state.todelete().begin(); it != state.todelete().end(); ++it) {
    todelete[it->first] = it->second;
  }

  for (auto it = state.children().begin(); it != state.children().end(); ++it) {
    _local_children[it->first] = it->second;
  }

  for (auto it = state.enoent().begin(); it != state.enoent().end(); ++it) {
    _local_enoent.insert(it->first);
  }

  return 0;
}

/* -------------------------------------------------------------------------- */
int
metad::pmap::swap_out(shared_md md)
{
  // serialize an in-memory md object into the kv store
  std::string mdstream;
  std::string mdsstream;

  if (!md->SerializeToString(&mdstream)) {
    return EFAULT;
  }

  if (md->state_serialize(mdsstream)) {
    return EFAULT;
  }

  if (store) {
    std::string md_key = "md.";
    md_key += std::to_string(md->id());

    if (store->put(md_key, mdstream)) {
      return EIO;
    }

    std::string md_state_key = "mds.";
    md_state_key += std::to_string(md->id());

    if (store->put(md_state_key, mdsstream)) {
      return EIO;
    }
  }

  EosFuse::Instance().mds.stats().inodes_stacked_inc();
  return 0;
}

/* -------------------------------------------------------------------------- */
int
metad::pmap::swap_in(fuse_ino_t ino, shared_md md)
{
  // deserialize an in-memory md object from the kv store
  std::string mdstream;
  std::string mdsstream;

  if (store) {
    std::string md_key = "md.";
    md_key += std::to_string(ino);

    if (store->get(md_key, mdstream)) {
      return EIO;
    }

    if (!md->ParseFromString(mdstream)) {
      return EFAULT;
    }

    std::string md_state_key = "mds.";
    md_state_key += std::to_string(ino);

    if (store->get(md_state_key, mdsstream)) {
      return EIO;
    }

    if (md->state_deserialize(mdsstream)) {
      return EFAULT;
    }
  }

  EosFuse::Instance().mds.stats().inodes_stacked_dec();
  return 0;
}

/* -------------------------------------------------------------------------- */
int
metad::pmap::swap_rm(fuse_ino_t ino)
{
  // delete from the external KV store
  if (store) {
    std::string md_key = "md.";
    md_key += std::to_string(ino);

    if (store->erase(md_key)) {
      return EIO;
    }

    std::string md_state_key = "mds.";
    md_state_key += std::to_string(ino);

    if (store->erase(md_state_key)) {
      return EIO;
    }
  }

  return 0;
}


/* -------------------------------------------------------------------------- */
void
metad::pmap::insertTS(fuse_ino_t ino, shared_md& md)
{
  XrdSysMutexHelper mLock(this);
  bool exists = this->count(ino);
  (*this)[ino] = md;
  // lru list handling

  if (!exists) {
    lru_add(ino, md);
  }

  lru_dump();
}

/* -------------------------------------------------------------------------- */
bool
metad::pmap::eraseTS(fuse_ino_t ino)
{
  XrdSysMutexHelper mLock(this);
  // lru list handling
  lru_remove(ino);
  bool exists = false;
  auto it = this->find(ino);

  if ((it != this->end()) && it->first) {
    exists = true;
  }

  if (exists && !it->second) {
    // deletion of a stacked inode has to be accounted for
    EosFuse::Instance().mds.stats().inodes_stacked_dec();
  }

  if (exists) {
    this->erase(it);
  }

  swap_rm(ino); // ignore return code
  return exists;
}

/* -------------------------------------------------------------------------- */
void
metad::pmap::retrieveWithParentTS(fuse_ino_t ino, shared_md& md, shared_md& pmd)
{
  // Atomically retrieve md objects for an inode, and its parent.
  while (true) {
    // In this particular case, we need to first lock mdmap, and then
    // md.. The following algorithm is meant to avoid deadlocks with code
    // which locks md first, and then mdmap.
    md.reset();
    pmd.reset();
    XrdSysMutexHelper mLock(this);

    if (!retrieve(ino, md)) {
      return; // ino not there, nothing to do
    }

    // md has been found. Can we lock it?
    if (md->Locker().CondLock()) {
      // Success!
      retrieve(md->pid(), pmd);
      md->Locker().UnLock();
      return;
    }

    // Nope, unlock mdmap and try again.
    mLock.UnLock();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}
