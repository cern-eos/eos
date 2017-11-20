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
#include "common/Logging.hh"
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
/* -------------------------------------------------------------------------- */
{
  // make a mapping for inode 1, it is re-loaded afterwards in init '/'


  {
    inomap.insert(1, 1);
  }

  shared_md md = std::make_shared<mdx>(1);
  md->set_nlink(1);
  md->set_mode( S_IRWXU | S_IRWXG | S_IRWXO | S_IFDIR);
  md->set_name(":root:");
  md->set_pid(1);
  stat.inodes_inc();
  stat.inodes_ever_inc();
  mdbackend = 0;

  mdmap.insertTS(1, md);
}

/* -------------------------------------------------------------------------- */
metad::~metad()
/* -------------------------------------------------------------------------- */
{
  if (z_socket)
    delete z_socket;
  if (z_ctx)
    delete z_ctx;

}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::init(backend* _mdbackend)
/* -------------------------------------------------------------------------- */
{
  mdbackend = _mdbackend;

  std::string mdstream;
  // load the root node

  fuse_req_t req = 0;
  XrdSysMutexHelper mLock(mdmap);
  update(req, mdmap[1], "", true);

  next_ino.init(EosFuse::Instance().getKV());
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
metad::connect(std::string zmqtarget, std::string zmqidentity, std::string zmqname, std::string zmqclienthost, std::string zmqclientuuid)
/* -------------------------------------------------------------------------- */
{
  if (z_socket && z_socket->connected() && (zmqtarget != zmq_target))
  {
    // TODO:
  }

  if (zmqtarget.length())
  {
    zmq_target = zmqtarget;
  }

  if (zmqidentity.length())
  {
    zmq_identity = zmqidentity;
  }

  if (zmqname.length())
  {
    zmq_name = zmqname;
  }

  if (zmqclienthost.length())
  {
    zmq_clienthost = zmqclienthost;
  }

  if (zmqclientuuid.length())
  {
    zmq_clientuuid = zmqclientuuid;
  }

  eos_static_info("metad connect %s as %s %d",
                  zmq_target.c_str(), zmq_identity.c_str(), zmq_identity.length());



  z_ctx = new zmq::context_t(1);
  z_socket = new zmq::socket_t(*z_ctx, ZMQ_DEALER);
  z_socket->setsockopt(ZMQ_IDENTITY, zmq_identity.c_str(), zmq_identity.length()
                       );
  while (1)
  {
    try
    {
      z_socket->connect(zmq_target);
      eos_static_notice("connected to %s", zmq_target.c_str());
      break;
    }
    catch (zmq::error_t& e)
    {
      if (e.num() != EINTR)
      {
        eos_static_err("msg=\"%s\" rc=%d", e.what(), e.num());
        return e.num();
      }
      eos_static_err("msg=\"%s\" rc=%d", e.what(), e.num());
    }
  }

  mdbackend->set_clientuuid(zmq_clientuuid);

  return 0;
}

/* -------------------------------------------------------------------------- */
metad::shared_md
/* -------------------------------------------------------------------------- */
metad::lookup(fuse_req_t req,
              fuse_ino_t parent,
              const char* name)
/* -------------------------------------------------------------------------- */
{
  eos_static_info("ino=%08llx name=%s", parent, name);

  // --------------------------------------------------
  // STEP 1 : retrieve the required parent MD
  // --------------------------------------------------
  shared_md pmd = get(req, parent, "", false);
  shared_md md;

  if (pmd->id() == parent)
  {
    XrdSysMutexHelper mLock(pmd->Locker());
    fuse_ino_t inode = 0; // inode referenced by parent + name

    // self lookup required for NFS exports
    if (!strcmp(name, "."))
    {
      return pmd;
    }

    // parent lookup required for NFS exports
    if (!strcmp(name, ".."))
    {
      pmd->Locker().UnLock();
      shared_md ppmd = get(req, pmd->pid(), "", false);
      pmd->Locker().Lock();
      return ppmd;
    }

    // --------------------------------------------------
    // STEP 2: check if we hold a cap for that directory
    // --------------------------------------------------
    if (pmd->cap_count())
    {
      // --------------------------------------------------
      // if we have a cap and we listed this directory, we trust the child information
      // --------------------------------------------------

      if (pmd->children().count(name))
      {
        inode = pmd->children().at(name);
      }
      else
      {
	// if we are still having the creator MD record, we can be sure, that we know everything about this directory
	if (pmd->creator() ||
	    (pmd->type() == pmd->MDLS))
        {
          // no entry - TODO return a NULLMD object instead of creating it all the time
          md = std::make_shared<mdx>();
          md->set_err(pmd->err());
          return md;
        }
        if (pmd->get_todelete().count(name))
        {
          // if this has been deleted, we just say this
          md = std::make_shared<mdx>();
          md->set_err(pmd->err());
          return md;
        }
      }
    }
    else
    {
      // --------------------------------------------------
      // if we don't have a cap, get will result in an MGM call anyway
      // --------------------------------------------------
    }
    // --------------------------------------------------
    // try to get the meta data record
    // --------------------------------------------------
    mLock.UnLock();
    md = get(req, inode, "", false, pmd, name);
  }
  else
  {
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
/* -------------------------------------------------------------------------- */
metad::forget(fuse_req_t req,
              fuse_ino_t ino,
              int nlookup)
/* -------------------------------------------------------------------------- */
{
  shared_md md;
  if (!mdmap.retrieveTS(ino, md))
  {
    return ENOENT;
  }

  XrdSysMutexHelper mLock(md->Locker());

  if (!md->id())
  {
    return EAGAIN;
  }

  eos_static_debug("count=%d(-%d) - ino=%016x", md->lookup_is(), nlookup, ino);

  if (!md->lookup_dec(nlookup))
  {
    return EAGAIN;
  }

  shared_md pmd;
  if (!mdmap.retrieveTS(md->pid(), pmd))
  {
    return ENOENT;
  }

  if (!md->deleted())
    return 0;

  eos_static_info("delete md object - ino=%016x name=%s", ino, md->name().c_str());

  mdmap.eraseTS(ino);
  stat.inodes_dec();
  return 0;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::mdx::convert(struct fuse_entry_param &e)
/* -------------------------------------------------------------------------- */
{
  e.ino = id();
  e.attr.st_dev=0;
  e.attr.st_ino=id();
  e.attr.st_mode=mode();
  e.attr.st_nlink=nlink() + 1;
  e.attr.st_uid=uid();
  e.attr.st_gid=gid();
  e.attr.st_rdev=0;
  e.attr.st_size=size();
  e.attr.st_blksize=4096;
  e.attr.st_blocks=(e.attr.st_size + 511) / 512;
  e.attr.st_atime=atime();
  e.attr.st_mtime=mtime();
  e.attr.st_ctime=ctime();
  e.attr.ATIMESPEC.tv_sec=atime();
  e.attr.ATIMESPEC.tv_nsec=atime_ns();
  e.attr.MTIMESPEC.tv_sec=mtime();
  e.attr.MTIMESPEC.tv_nsec=mtime_ns();
  e.attr.CTIMESPEC.tv_sec=ctime();
  e.attr.CTIMESPEC.tv_nsec=ctime_ns();
  if (EosFuse::Instance().Config().options.md_kernelcache)
  {
    e.attr_timeout=180.0;
    e.entry_timeout=180.0;
  }
  else
  {
    e.attr_timeout=0;
    e.entry_timeout=0;
  }
  if (EosFuse::Instance().Config().options.overlay_mode)
  {
    e.attr.st_mode |= EosFuse::Instance().Config().options.overlay_mode;
  }

  e.generation = 1;
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
metad::mdx::dump()
/* -------------------------------------------------------------------------- */
{
  char sout[16384];
  snprintf(sout, sizeof (sout),
           "ino=%016lx dev=%08x mode=%08x nlink=%08x uid=%05d gid=%05d rdev=%08x "
           "size=%llu bsize=%u blocks=%llu atime=%lu.%lu mtime=%lu.%lu ctime=%lu.%lu",
           (unsigned long) id(), 0, (unsigned int) mode(), (unsigned int) nlink(),
           (int) uid(), (int) gid(), 0,
           (unsigned long long) size(), (unsigned int) 4096, (unsigned long long) size() / 512,
           (unsigned long) atime(), (unsigned long) atime_ns(),
           (unsigned long) mtime(), (unsigned long) mtime_ns(),
           (unsigned long) ctime(),
           (unsigned long) ctime_ns()
           );
  return sout;
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
metad::mdx::dump(struct fuse_entry_param &e)
/* -------------------------------------------------------------------------- */
{
  char sout[16384];
  snprintf(sout, sizeof (sout),
           "ino=%016lx dev=%08x mode=%08x nlink=%08x uid=%05d gid=%05d rdev=%08x "
           "size=%llu bsize=%u blocks=%llu atime=%lu.%lu mtime=%lu.%lu ctime=%lu.%lu "
           "attr-timeout=%llu entry-timeout=%llu",
           (unsigned long) e.attr.st_ino, (unsigned int) e.attr.st_dev,
           (unsigned int) e.attr.st_mode, (unsigned int) e.attr.st_nlink,
           (int) e.attr.st_uid, (int) e.attr.st_gid, (unsigned int) e.attr.st_rdev,
           (unsigned long long) e.attr.st_size, (unsigned int) e.attr.st_blksize,
           (unsigned long long) e.attr.st_blocks,
           (unsigned long) e.attr.ATIMESPEC.tv_sec, (unsigned long) e.attr.ATIMESPEC.tv_nsec,
           (unsigned long) e.attr.MTIMESPEC.tv_sec, (unsigned long) e.attr.MTIMESPEC.tv_nsec,
           (unsigned long) e.attr.CTIMESPEC.tv_sec, (unsigned long) e.attr.CTIMESPEC.tv_nsec,
           (unsigned long long) e.attr_timeout, (unsigned long long) e.entry_timeout
           );
  return sout;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
metad::map_children_to_local(shared_md pmd)
/* -------------------------------------------------------------------------- */
{
  //  XrdSysMutexHelper pLock(pmd->Locker());
  bool ret = true;
  // exchange the remote inode map with the local used inode map
  std::set<std::string> names ;
  std::vector<std::string> names_to_delete;

  for (auto map = pmd->children().begin(); map != pmd->children().end(); ++map)
  {
    eos_static_debug("translate %s [%lx]", map->first.c_str(), map->second);
    if (pmd->get_todelete().count(map->first))
    {
      eos_static_debug("masking %s", map->first.c_str());
      names_to_delete.push_back(map->first);
      //      pmd->set_nchildren(pmd->nchildren()-1);
    }
    else
    {
      names.insert(map->first);
    }
  }

  for (auto it=names.begin(); it != names.end(); ++it)
  {
    uint64_t remote_ino = (*pmd->mutable_children())[*it];
    uint64_t local_ino = inomap.forward(remote_ino);

    if (!local_ino)
    {
      local_ino = next_ino.inc();
      inomap.insert(remote_ino, local_ino);
      stat.inodes_inc();
      stat.inodes_ever_inc();
      shared_md md = std::make_shared<mdx>();
      mdmap.insertTS(local_ino, md);
    }

    eos_static_debug("store-lookup r-ino %016lx <=> l-ino %016lx", remote_ino,
                     local_ino);
    (*pmd->mutable_children())[*it] = local_ino;
  }

  for (size_t i=0; i < names_to_delete.size(); ++i)
  {
    pmd->mutable_children()->erase(names_to_delete[i]);
  }

  for (auto map = pmd->get_childrentomap().begin(); map != pmd->get_childrentomap().end(); ++map)
  {
    (*pmd->mutable_children())[map->first] = map->second;
  }


  if (EOS_LOGS_DEBUG)
  {
    for (auto map = pmd->children().begin(); map != pmd->children().end(); ++map)
    {
      eos_static_debug("listing: %s [%lx]", map->first.c_str(), map->second);
    }
  }

  pmd->set_nchildren(pmd->children().size());
  return ret;
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
           bool readdir
           )
/* -------------------------------------------------------------------------- */
{
  eos_static_info("ino=%1llx pino=%16lx name=%s listing=%d", ino, pmd ? pmd->id() : 0, name, listing);
  shared_md md;
  if (ino)
  {
    {
      XrdSysMutexHelper mLock(mdmap);

      // the inode is known, we try to get that one
      mdmap.retrieve(ino, md);
    }
    if ( EOS_LOGS_DEBUG )
      eos_static_debug("MD:\n%s", (!md) ? "<empty>" : dump_md(md).c_str());
  }
  else
  {
    // -------------------------------------------------------------------------
    // this happens if we get asked for a child, which was never listed before
    // -------------------------------------------------------------------------
    md = std::make_shared<mdx>();
  }

  if (!md || !md->id())
  {
    // -------------------------------------------------------------------------
    // there is no local meta data available, this can only be found upstream
    // -------------------------------------------------------------------------
  }
  else
  {
    // -------------------------------------------------------------------------
    // there is local meta data, we have to decide if we can 'trust' it, or we
    // need to refresh it from upstream  - TODO !
    // -------------------------------------------------------------------------
    if (readdir)
    {
      eos_static_info("returning opendir(readdir) entry");
      return md;
    }
    if (pmd && (pmd->cap_count() || pmd->creator()))
    {
      eos_static_info("returning cap entry");
      return md;
    }
    else
    {
      eos_static_info("pmd=%x cap-cnt=%d", pmd ? pmd->id() : 0, pmd ? pmd->cap_count() : 0);
      uint64_t md_pid = 0;
      mode_t md_mode = 0;
      {
        XrdSysMutexHelper mLock(md->Locker());
        if ( ( (!listing) || (listing && md->type() == md->MDLS) ) && md->md_ino() && md->cap_count())
        {
          eos_static_info("returning cap entry via parent lookup cap-count=%d", md->cap_count());
          if ( EOS_LOGS_DEBUG )
            eos_static_debug("MD:\n%s", dump_md(md, false).c_str());
          return md;
        }
        md_pid = md->pid();
        md_mode = md->mode();
      }

      if (!S_ISDIR(md_mode))
      {
        // files are covered by the CAP of the parent, so if there is a cap
        // on the parent we can return this entry right away
        if (mdmap.retrieveTS(md_pid, pmd))
        {
          if (pmd && pmd->id() && pmd->cap_count())
          {
            return md;
          }
        }
      }
    }

    XrdSysMutexHelper mLock(md->Locker());
    if (!md->pid())
    {
      // this must have been generated locally, we return this entry
      eos_static_info("returning generated entry");
      if ( EOS_LOGS_DEBUG )
        eos_static_debug("MD:\n%s", dump_md(md, false).c_str());
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
    std::string root_path="/";
    // request the root meta data
    rc = mdbackend->getMD(req, root_path, contv, authid);
    // set ourselfs as parent of root since we might mount
    // a remote directory != '/'

    md->set_pid(1);

    // mark this as a listing request
    listing = true;
  }
  else if (!ino)
    // -------------------------------------------------------------------------
    // CASE 2: by remote parent inode + name
    // -------------------------------------------------------------------------
  {
    thecase = 2;
    if (pmd)
    {
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


      if (pmd_ino)
      {
        rc = mdbackend->getMD(req, pmd_ino, name, contv, listing, authid);
      }
      else
      {
        rc = ENOENT;
      }
    }
    else
    {
      rc = ENOENT;
    }
  }
  else
    // -------------------------------------------------------------------------
    // CASE 3: by remote inode
    // -------------------------------------------------------------------------
  {
    thecase = 3;
    XrdSysMutexHelper mLock(md->Locker());
    if (md->md_ino())
    {
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
      rc = mdbackend->getMD(req, md->md_ino(), listing ? ( (md->type() != md->MDLS) ? 0 : md->clock()) : md->clock(), contv, listing, authid);
    }
    else
    {
      if (md->id())
      {
        // that can be a locally created entry which is not yet upstream
        rc = 0;
        if ( EOS_LOGS_DEBUG )
          eos_static_debug("MD:\n%s", dump_md(md).c_str());
        return md;
      }
      else
      {
        rc = ENOENT;
      }
    }
  }

  if (!rc)
  {
    // -------------------------------------------------------------------------
    // we need to store all response data and eventually create missing
    // hierarchical entries
    // -------------------------------------------------------------------------

    eos_static_debug("apply vector=%d", contv.size());

    for (auto it=contv.begin(); it != contv.end(); ++it)
    {
      if (it->ref_inode_())
      {
        if (ino)
        {
          // the response contains the remote inode according to the request
          inomap.insert(it->ref_inode_(), ino);
        }

        uint64_t l_ino;

        // store the retrieved meta data blob
        if (!(l_ino = apply(req, *it, listing)))
        {
          eos_static_crit("msg=\"failed to apply response\"");
        }
        else
        {
          ino = l_ino;
        }
      }
      else
      {
        // we didn't get the md back
      }
    }


    {
      XrdSysMutexHelper mLock(mdmap);
      if (mdmap.retrieve(ino, md))
      {
        // if the md record was returned, it is accessible after the apply function
        // attached it. We should also attach to the parent to be able to add
        // a not yet published child entry at the parent.

        mdmap.retrieve(md->pid(), pmd);
      }
    }

    eos_static_info("ino=%08llx pino=%08llx name=%s listing=%d", ino, pmd ? pmd->id() : 0, name, listing);

    switch (thecase) {
    case 1:
      // nothing to do
      break;
    case 2:
    {
      // we make sure, that the meta data record is attached to the local parent
      if (pmd->id())
      {
        if (!pmd->children().count(md->name()) && !md->deleted())
        {
          eos_static_info("attaching %s [%lx] to %s [%lx]", md->name().c_str(), md->id(), pmd->name().c_str(), pmd->id());
          // persist this hierarchical dependency
          (*pmd->mutable_children())[md->name()] = md->id();
          update(req, pmd, "", true);
        }
      }
      break;
    }
    case 3:
      break;
    }
  }


  if (rc)
  {
    shared_md md = std::make_shared<mdx>();
    md->set_err(rc);

    if ( EOS_LOGS_DEBUG )
      eos_static_debug("MD:\n%s", dump_md(md).c_str());

    return md;
  }

  if ( EOS_LOGS_DEBUG )
    eos_static_debug("MD:\n%s", dump_md(md).c_str());

  return md;
}

/* -------------------------------------------------------------------------- */
uint64_t
/* -------------------------------------------------------------------------- */
metad::insert(fuse_req_t req,
              metad::shared_md md,
              std::string authid)
/* -------------------------------------------------------------------------- */
{
  uint64_t newinode = 0;
  {
    newinode = next_ino.inc();
    md->set_id(newinode);
    if (EOS_LOGS_DEBUG)
      eos_static_debug("%s", dump_md(md, false).c_str());
    mdmap.insertTS(newinode, md);
  }

  return newinode;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
metad::wait_flush(fuse_req_t req,
                  metad::shared_md md)
/* -------------------------------------------------------------------------- */
{
  // logic to wait for a completion of request
  md->Locker().UnLock();
  while (1)
  {
    if (md->WaitSync(1))
    {
      if (md->getop() != md->NONE)
        continue;

      break;
    }
  }
  eos_static_info("waited for sync rc=%d bw=%lx", md->err(), inomap.backward(md->id()));
  if (!inomap.backward(md->id()))
  {
    md->Locker().Lock();
    return md->err();
  }
  else
  {
    md->Locker().Lock();
    return 0;
  }
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::update(fuse_req_t req,
              shared_md md,
              std::string authid,
              bool localstore)
/* -------------------------------------------------------------------------- */
{
  mdflush.Lock();
  stat.inodes_backlog_store(mdqueue.size());

  if ( !localstore )
  {
    // only updates initiated from FUSE limited,
    // server response updates pass

    while (mdqueue.size() == mdqueue_max_backlog)
      mdflush.WaitMS(25);
  }

  flushentry fe(md->id(), authid, localstore ? mdx::LSTORE : mdx::UPDATE, req);

  mdqueue[md->id()]++;
  mdflushqueue.push_back(fe);

  eos_static_info("added ino=%lx flushentry=%s queue-size=%u local-store=%d", md->id(), flushentry::dump(fe).c_str(), mdqueue.size(), localstore);

  mdflush.Signal();
  mdflush.UnLock();
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::add(fuse_req_t req, metad::shared_md pmd, metad::shared_md md, std::string authid,
           bool localstore)
/* -------------------------------------------------------------------------- */
{
  // this is called with a lock on the md object
  stat.inodes_inc();
  stat.inodes_ever_inc();

  uint64_t pid=0;
  uint64_t id=0;

  auto map = pmd->mutable_children();
  eos_static_debug("child=%s parent=%s inode=%016lx authid=%s localstore=%d", md->name().c_str(),
                   pmd->name().c_str(), md->id(), authid.c_str(), localstore);
  {
    XrdSysMutexHelper mLock(pmd->Locker());
    (*map)[md->name()] = md->id();
    pmd->set_nlink(1);
    pmd->set_nchildren(pmd->nchildren() + 1);
    pmd->get_todelete().erase(md->name());
    pid = pmd->id();
  }

  {
    // store the local and remote parent inode
    md->set_pid(pmd->id());
    md->set_md_pino(pmd->md_ino());
    id = md->id();
  }

  mdflush.Lock();

  stat.inodes_backlog_store(mdqueue.size());

  if (!localstore)
  {
    while (mdqueue.size() == mdqueue_max_backlog)
      mdflush.WaitMS(25);

    flushentry fe(id, authid, mdx::ADD, req);
    mdqueue[id]++;
    mdflushqueue.push_back(fe);
  }

  flushentry fep(pid, authid, mdx::LSTORE, req);
  mdqueue[pid]++;
  mdflushqueue.push_back(fep);

  mdflush.Signal();
  mdflush.UnLock();
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
metad::add_sync(fuse_req_t req, shared_md pmd, shared_md md, std::string authid)
/* -------------------------------------------------------------------------- */
{
  // this is called with a lock on the md object
  int rc = 0;
  // store the local and remote parent inode

  XrdSysMutexHelper mLockParent(pmd->Locker());
  md->set_pid(pmd->id());
  md->set_md_pino(pmd->md_ino());
  mLockParent.UnLock();

  mdx::md_op op = mdx::ADD;

  eos_static_debug("metacache::sync ino=%016lx authid=%s op=%d", md->id(), authid.c_str(), (int) op);

  md->set_operation(md->SET);
  eos_static_info("metacache::sync backend::putMD - start");

  while (1)
  {
    // wait that the parent is leaving the mdqueue
    mdflush.Lock();
    if (mdqueue.count(pmd->id()))
    {
      mdflush.UnLock();
      eos_static_info("waiting for parent directory to be synced upstream parent-ino= %lx ino=%lx",
                      md->id(), pmd->id());
      XrdSysTimer delay;
      delay.Wait(25);
    }
    else
    {
      mdflush.UnLock();
      break;
    }
  }

  // push to backend
  if ((rc = mdbackend->putMD(req, &(*md), authid, &(md->Locker()))))
  {
    eos_static_err("metad::add_sync backend::putMD failed rc=%d", rc);
    // ---------------------------------------------------------------
    // in this case we always clean this MD record to force a refresh
    // ---------------------------------------------------------------
    inomap.erase_bwd(md->id());
    md->setop_none();
    md->set_err(rc);
    {
      mdmap.eraseTS(md->id());
      stat.inodes_dec();
      stat.inodes_ever_inc();
    }
    return rc;
  }
  else
  {
    inomap.insert(md->md_ino(), md->id());
    md->setop_none();
  }
  eos_static_info("metad::add_sync backend::putMD - stop");

  std::string mdstream;
  md->SerializeToString(&mdstream);
  EosFuse::Instance().getKV()->put(md->id(), mdstream);

  stat.inodes_inc();
  stat.inodes_ever_inc();

  auto map = pmd->mutable_children();

  eos_static_debug("child=%s parent=%s inode=%016lx authid=%s", md->name().c_str(),
                   pmd->name().c_str(), md->id(), authid.c_str());
  {
    XrdSysMutexHelper mLock(pmd->Locker());
    (*map)[md->name()] = md->id();
    pmd->set_nlink(1);
    pmd->set_nchildren(pmd->nchildren() + 1);
    pmd->get_todelete().erase(md->name());
  }

  mdflush.Lock();

  stat.inodes_backlog_store(mdqueue.size());

  while (mdqueue.size() == mdqueue_max_backlog)
    mdflush.WaitMS(25);

  flushentry fep(pmd->id(), authid, mdx::LSTORE, req);
  mdqueue[pmd->id()]++;
  mdflushqueue.push_back(fep);

  mdflush.Signal();
  mdflush.UnLock();
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
metad::begin_flush(fuse_req_t req, shared_md emd, std::string authid)
/* -------------------------------------------------------------------------- */
{
  shared_md md = std::make_shared<mdx>();
  md->set_operation(md->BEGINFLUSH);

  int rc = 0;
  if (!emd->md_ino())
  {
    //TODO wait for the remote inode to be known
  }

  md->set_md_ino(emd->md_ino());

  if ((rc = mdbackend->putMD(req, &(*md), authid, 0)))
  {
    eos_static_err("metad::begin_flush backend::putMD failed rc=%d", rc);
  }
  return rc;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
metad::end_flush(fuse_req_t req, shared_md emd, std::string authid)
/* -------------------------------------------------------------------------- */
{
  shared_md md = std::make_shared<mdx>();
  md->set_operation(md->ENDFLUSH);

  int rc = 0;

  if (!emd->md_ino())
  {
    //TODO wait for the remote inode to be known
  }

  md->set_md_ino(emd->md_ino());

  if ((rc = mdbackend->putMD(req, &(*md), authid, 0)))
  {
    eos_static_err("metad::begin_flush backend::putMD failed rc=%d", rc);
  }
  return rc;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::remove(fuse_req_t req, metad::shared_md pmd, metad::shared_md md, std::string authid,
              bool upstream)
/* -------------------------------------------------------------------------- */
{
  // this is called with the md object locked
  auto map = pmd->mutable_children();
  eos_static_debug("child=%s parent=%s inode=%016lx", md->name().c_str(),
                   pmd->name().c_str(), md->id());

  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts);

  if (!md->deleted())
  {
    md->lookup_inc();
    stat.inodes_deleted_inc();
    stat.inodes_deleted_ever_inc();
  }

  md->set_mtime(ts.tv_sec);
  md->set_mtime_ns(ts.tv_nsec);

  md->setop_delete();

  std::string name = md->name();
  // avoid lock order violation
  md->Locker().UnLock();
  {
    XrdSysMutexHelper mLock(pmd->Locker());
    (*map).erase(name);
    pmd->set_nchildren(pmd->nchildren() - 1);
    pmd->get_todelete().insert(name);
    pmd->set_mtime(ts.tv_sec);
    pmd->set_mtime_ns(ts.tv_nsec);
  }

  md->Locker().Lock();

  if (!upstream)
    return ;

  flushentry fe(md->id(), authid, mdx::RM, req);
  flushentry fep(pmd->id(), authid, mdx::LSTORE, req);

  mdflush.Lock();

  while (mdqueue.size() == mdqueue_max_backlog)
    mdflush.WaitMS(25);

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
/* -------------------------------------------------------------------------- */
metad::mv(fuse_req_t req, shared_md p1md, shared_md p2md, shared_md md, std::string newname,
          std::string authid1, std::string authid2)
/* -------------------------------------------------------------------------- */
{
  auto map1 = p1md->mutable_children();
  auto map2 = p2md->mutable_children();

  eos_static_debug("child=%s new-name=%s parent=%s newparent=%s inode=%016lx", md->name().c_str(),
                   newname.c_str(),
                   p1md->name().c_str(), p2md->name().c_str(), md->id());

  XrdSysMutexHelper mLock(md->Locker());

  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts);

  if (p1md->id() != p2md->id())
  {
    // move between directories.
    // Similarly to EosFuse::rename, we lock in order of increasing inode.
    XrdSysMutex *first = nullptr;
    XrdSysMutex *second = nullptr;

    if(p1md->id() < p2md->id()) {
      first = &p1md->Locker();
      second = &p2md->Locker();
    }
    else {
      first = &p2md->Locker();
      second = &p1md->Locker();
    }

    XrdSysMutexHelper m1Lock(first);
    XrdSysMutexHelper m2Lock(second);
    (*map2)[newname] = md->id();
    (*map1).erase(md->name());
    p1md->set_nchildren(p1md->nchildren() - 1);
    p2md->set_nchildren(p2md->nchildren() + 1);
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
    p1md->get_todelete().insert(md->name()); // make it known as deleted
    p2md->get_todelete().erase(newname); // the new target is not deleted anymore
  }
  else
  {
    // move within directory
    XrdSysMutexHelper m1Lock(p1md->Locker());
    (*map2)[newname] = md->id();
    (*map1).erase(md->name());
    p1md->get_todelete().insert(md->name()); // make it known as deleted
    p2md->get_todelete().erase(newname); // the new target is not deleted anymore
    md->set_name(newname);

  }

  md->clear_pmtime();
  md->clear_pmtime_ns();
  md->set_ctime(ts.tv_sec);
  md->set_ctime_ns(ts.tv_nsec);

  mdflush.Lock();

  while (mdqueue.size() == mdqueue_max_backlog)
    mdflush.WaitMS(25);

  flushentry fe1(p1md->id(), authid1, mdx::UPDATE, req);
  mdqueue[p1md->id()]++;
  mdflushqueue.push_back(fe1);

  if (p1md->id() != p2md->id())
  {
    flushentry fe2(p2md->id(), authid2, mdx::UPDATE, req);
    mdqueue[p2md->id()]++;
    mdflushqueue.push_back(fe2);
  }
  flushentry fe(md->id(), authid2, mdx::UPDATE, req);

  mdqueue[md->id()]++;
  mdflushqueue.push_back(fe);
  stat.inodes_backlog_store(mdqueue.size());

  mdflush.Signal();
  mdflush.UnLock();
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
metad::dump_md(shared_md md, bool lock)
/* -------------------------------------------------------------------------- */
{
  if (! (md) )
    return "";

  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string jsonstring;
  if (lock)
    md->Locker().Lock();
  google::protobuf::util::MessageToJsonString( *((eos::fusex::md*)(&(*md))), &jsonstring, options);
  char capcnt[16];
  snprintf(capcnt, sizeof (capcnt), "%d", md->cap_count());
  jsonstring += "\ncap-cnt:";
  jsonstring += capcnt;
  if (lock)
    md->Locker().UnLock();
  return jsonstring;
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
metad::dump_md(eos::fusex::md & md)
/* -------------------------------------------------------------------------- */
{
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string jsonstring;
  google::protobuf::util::MessageToJsonString( md, &jsonstring, options);
  return jsonstring;
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
metad::dump_container(eos::fusex::container & cont)
/* -------------------------------------------------------------------------- */
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
/* -------------------------------------------------------------------------- */
metad::getlk(fuse_req_t req, shared_md md, struct flock * lock)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper locker(md->Locker());

  // fill lock request structure
  md->mutable_flock()->set_pid(fuse_req_ctx(req)->pid);
  md->mutable_flock()->set_len(lock->l_len);
  md->mutable_flock()->set_start(lock->l_start);

  md->set_operation(md->GETLK);

  switch (lock->l_type) {
  case F_RDLCK: md->mutable_flock()->set_type(eos::fusex::lock::RDLCK);
    break;
  case F_WRLCK: md->mutable_flock()->set_type(eos::fusex::lock::WRLCK);
    break;
  case F_UNLCK: md->mutable_flock()->set_type(eos::fusex::lock::UNLCK);
    break;
  default:
    return EINVAL;
  }

  // do sync upstream lock call
  int rc = mdbackend->doLock(req, *md, &(md->Locker()));

  // digest the response
  if (!rc)
  {
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
  }
  else
  {
    rc = EAGAIN;
  }

  // clean the lock structure;
  md->clear_flock();

  return rc;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
metad::setlk(fuse_req_t req, shared_md md, struct flock* lock, int sleep)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper locker(md->Locker());

  // fill lock request structure
  md->mutable_flock()->set_pid(fuse_req_ctx(req)->pid);
  md->mutable_flock()->set_len(lock->l_len);
  md->mutable_flock()->set_start(lock->l_start);


  if (sleep)
  {
    md->set_operation(md->SETLKW);
  }
  else
  {
    md->set_operation(md->SETLK);
  }

  switch (lock->l_type) {
  case F_RDLCK: md->mutable_flock()->set_type(eos::fusex::lock::RDLCK);
    break;
  case F_WRLCK: md->mutable_flock()->set_type(eos::fusex::lock::WRLCK);
    break;
  case F_UNLCK: md->mutable_flock()->set_type(eos::fusex::lock::UNLCK);
    break;
  default:
    return EINVAL;
  }

  bool backend_call = true;

  if (lock->l_type == F_UNLCK)
  {
    backend_call =false;
    // check that we have actually a lock for that before doing an upstream call
    for (auto it=md->LockTable().begin(); it!=md->LockTable().end();++it)
    {
      if ( it->l_pid == (pid_t)md->flock().pid())
      {
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
  if (!rc)
  {
    rc = md->flock().err_no();
  }
  else
  {
    rc = EAGAIN;
  }

  if (!rc)
  {
    // store in the lock table - unlocking done during flush
    if (lock->l_type != F_UNLCK)
    {
      md->LockTable().push_back(*lock);
    }
    else
    {
      // remove from LockTable - not the most efficient
      auto it = md->LockTable().begin();
      while(it != md->LockTable().end())
      {
	if( it->l_pid == (pid_t)md->flock().pid() )
	{
	  it = md->LockTable().erase(it);
	}
	else
	  it++;
      }
    }
  }
  // clean the lock structure;
  md->clear_flock();

  return rc;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
metad::statvfs(fuse_req_t req, struct statvfs * svfs)
/* -------------------------------------------------------------------------- */
{
  return mdbackend->statvfs(req, svfs);
}

/* -------------------------------------------------------------------------- */
uint64_t
/* -------------------------------------------------------------------------- */
metad::apply(fuse_req_t req, eos::fusex::container & cont, bool listing)
/* -------------------------------------------------------------------------- */
{
  // apply receives either a single MD record or a parent MD + all children MD
  // we have to make sure that the modification of children is atomic in the parent object

  shared_md md;
  shared_md pmd;
  eos_static_debug(dump_container(cont).c_str());

  if (cont.type() == cont.MD)
  {
    uint64_t md_ino = cont.md_().md_ino();
    uint64_t ino = inomap.forward(md_ino);

    bool is_new = false;
    {
      // Create a new md object, if none is found in the cache
      is_new = mdmap.retrieveOrCreateTS(ino, md);
      md->Locker().Lock();

      if (EOS_LOGS_DEBUG)
        eos_static_debug("%s op=%d deleted=%d", md->dump().c_str(), md->getop(), md->deleted());
      if (md->deleted())
      {
	md->Locker().UnLock();
        return 0;
      }
    }

    if (is_new) {
      if (!ino)
      {
	// in this case we need to create a new one
	uint64_t new_ino = insert(req, md, md->authid());
	ino = new_ino;
	is_new = true;
      }
    }

    uint64_t p_ino = inomap.forward(md->md_pino());
    if (!p_ino)
    {
      eos_static_crit("msg=\"missing lookup entry for inode\" ino=%016lx", ino);
    }
    assert(p_ino != 0);

    if (!S_ISDIR(md->mode()))
    {
      // if its a file we need to have a look at parent cap-count, so we get the parent md
      md->Locker().UnLock();
      mdmap.retrieveTS(p_ino, pmd);
      md->Locker().Lock();
    }

    {
      if ( !pmd || (((!S_ISDIR(md->mode())) && !pmd->cap_count())) ||
          (!md->cap_count()) )
      {
        // don't wipe capp'ed meta-data, local state has to be used, since we get remote-invalidated in case we are out-of-date
        *md = cont.md_();

        eos_static_debug("store md for local-ino=%016lx remote-ino=%016lx -", (long) ino, (long) md_ino);
        if (EOS_LOGS_DEBUG)
          eos_static_debug("%s", md->dump().c_str());
      }
    }

    md->set_pid(p_ino);
    md->set_id(ino);

    inomap.insert(md_ino, ino);

    //md->get_todelete().clear();
    eos_static_info("store local pino=%016lx for %016lx", md->pid(), md->id());
    update(req, md, "", true);
    md->Locker().UnLock();
    if (is_new)
    {
      XrdSysMutexHelper mLock(mdmap);
      mdmap[ino] = md;
      stat.inodes_inc();
      stat.inodes_ever_inc();
    }
    return ino;
  }
  else if (cont.type() == cont.MDMAP)
  {
    uint64_t p_ino = inomap.forward(cont.ref_inode_());

    for (auto map = cont.md_map_().md_map_().begin(); map != cont.md_map_().md_map_().end(); ++map)
    {
      // loop over the map of meta data objects

      uint64_t ino = inomap.forward(map->first);

      eos::fusex::cap cap_received;
      cap_received.set_id(0);

      eos_static_debug("remote-ino=%016lx local-ino=%016lx", (long) map->first, ino);

      if (mdmap.retrieveTS(ino, md))
      {
        // this is an already known inode
        eos_static_debug("lock mdmap");

        {
          bool child=false;
          if (map->first != cont.ref_inode_())
          {
            child = true;

            if (!S_ISDIR(map->second.mode()))
            {
	      shared_md child_pmd;
              if (mdmap.retrieveTS(p_ino, child_pmd))
              {
                if ( child_pmd->cap_count() )
                {
                  // if we have a cap for the parent of a file, we don't need to get an
                  // update, we receive it via an asynchronous broadcast
                  eos_static_debug("skipping md for file child local-ino=%016x remote-ino=%016lx",
                                   ino, map->first);
                  // but eventually refresh the cap
                  if (map->second.has_capability())
                  {
                    // store cap
                    EosFuse::Instance().getCap().store(req, cap_received);
                    md->cap_inc();
                  }
                  // don't modify existing local meta-data
                  continue;
                }
                else
                {
                  if ( pmd && pmd->id() && pmd->cap_count())
                  {
                    eos_static_debug("skipping md for dir child local-ino=%016x remote-ino=%016lx",
                                     ino, map->first);
                    // but eventually refresh the cap
                    if (map->second.has_capability())
                    {
                      // store cap
                      EosFuse::Instance().getCap().store(req, cap_received);
                      md->cap_inc();
                    }
                    // don't modify existing local meta-data
                    continue;
                  }
                }
              }
            }
            md->Locker().Lock();
          }
          else
          {
            md->Locker().Lock();
            pmd = md;
          }

          if (map->second.has_capability())
          {
            // extract any new capability
            cap_received = map->second.capability();
          }

          if (child)
          {
            int children = md->nchildren();
            *md = map->second;
            md->set_nchildren(children);
          }
          else
          {
            // we have to overlay the listing
            std::set<std::string> todelete = md->get_todelete();
            md->get_childrentomap().clear();
            for (auto it=md->children().begin(); it != md->children().end(); ++it)
            {
              // add the current state to the childrenmap
              md->get_childrentomap()[it->first] = it->second;
            }

            mdflush.Lock();
            if (!mdqueue.count(md->id()))
            {
              mdflush.UnLock();
              // overwrite local meta data with remote state
              *md = map->second;
              md->get_todelete() = todelete;
            }
            else
            {
              mdflush.UnLock();
              // keep the listing
              md->mutable_children()->clear();
              for (auto it=map->second.children().begin(); it != map->second.children().end(); ++it)
              {
                (*md->mutable_children())[it->first] = it->second;
              }
              md->get_todelete() = todelete;
            }
          }

          md->clear_capability();
          md->set_id(ino);

          p_ino = inomap.forward(md->md_pino());

          md->set_pid(p_ino);
          eos_static_info("store remote-ino=%016lx local pino=%016lx for %016lx", md-> md_pino(), md->pid(), md->id());
          for (auto it=md->get_todelete().begin(); it != md->get_todelete().end(); ++it)
          {
            eos_static_info("%016lx to-delete=%s", md->id(), it->c_str());
          }
          // push only into the local KV cache - md was retrieved from upstream

          if (map->first != cont.ref_inode_())
            update(req, md, "", true);

          eos_static_debug("store md for local-ino=%08ld remote-ino=%016lx type=%d -",
                           (long) ino, (long) map->first, md->type());
          if (EOS_LOGS_DEBUG)
            eos_static_debug("%s", md->dump().c_str());

          md->Locker().UnLock();

          if (cap_received.id())
          {
            // store cap
            EosFuse::Instance().getCap().store(req, cap_received);
            md->cap_inc();
            if (md->cap_count() == 1)
            {
              eos_static_info("clearing all children of ino=%16dx", md->id());
              // we got a full refresh from upstream, the local contents=remote contents for the map_chilren function

              md->get_childrentomap().clear();
              md->get_todelete().clear();
            }
            //eos_static_err("increase cap counter for ino=%lu", ino);
          }
        }
      }
      else
      {
        // this is a new inode we don't know yet
        md = std::make_shared<mdx>();
        if (map->second.has_capability())
        {
          // extract any new capability
          cap_received = map->second.capability();
        }
        *md = map->second;
        md->clear_capability();

        if (!pmd)
        {
          pmd = md;
        }

        uint64_t new_ino = 0;
	if (! (new_ino = inomap.forward(md->md_ino())) )
	{
	  // if the mapping was in the local KV, we know the mapping, but actually the md record is new in the mdmap
	  new_ino = insert(req, md, md->authid());
	}

        md->set_id(new_ino);

        md->set_id(new_ino);
        if (!listing)
        {
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

        update(req, md, md->authid(), true);

        if (cap_received.id())
        {
          // store cap
          EosFuse::Instance().getCap().store(req, cap_received);
          md->cap_inc();
          //          eos_static_err("increase cap counter for ino=%lu", new_ino);
        }
        eos_static_debug("store md for local-ino=%016lx remote-ino=%016lx type=%d -", (long) new_ino, (long) map->first, md->type());
        if (EOS_LOGS_DEBUG)
          eos_static_debug("%s", md->dump().c_str());
      }
    }

    if (pmd) pmd->Locker().Lock();

    if (pmd && listing)
    {
      bool ret = false;
      if (! (ret = map_children_to_local(pmd)))
      {
        eos_static_err("local mapping has failed %d", ret);
        assert(0);
      }

      for (auto map = pmd->children().begin(); map != pmd->children().end(); ++map)
      {
        eos_static_debug("listing: %s [%lx]", map->first.c_str(), map->second);
      }
    }
    if (pmd)
    {
      // store the parent now, after all children are inserted
      update(req, pmd, "", true);
    }

    if (pmd) pmd->Locker().UnLock();
  }
  else
  {
    return 0;
  }

  if (pmd)
    return pmd->id();

  else
    return 0;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::mdcflush(ThreadAssistant &assistant)
/* -------------------------------------------------------------------------- */
{
  uint64_t lastflushid=0;
  while (!assistant.terminationRequested())
  {
    {
      mdflush.Lock();

      if (mdqueue.count(lastflushid))
      {
        // remove entries from the mdqueue, if their ref count is 0
        if (!mdqueue[lastflushid])
        {
          mdqueue.erase(lastflushid);
        }
      }

      stat.inodes_backlog_store(mdqueue.size());

      while (mdqueue.size() == 0)
      {
        // TODO(gbitzes): Fix this, so we don't need to poll. Have ThreadAssistant
        // accept callbacks for when termination is requested, so we can wake up
        // any condvar.
        mdflush.Wait(1);
        if (assistant.terminationRequested())
	{
	  mdflush.UnLock();
	  return;
	}
      }

      // TODO: add an optimzation to merge requests in the queue
      auto it= mdflushqueue.begin();

      uint64_t ino = it->id();
      std::string authid = it->authid();
      fuse_id f_id = it->get_fuse_id();
      mdx::md_op op = it->op();


      lastflushid = ino;

      eos_static_info("metacache::flush ino=%lx flushqueue-size=%u", ino, mdflushqueue.size());
      eos_static_info("metacache::flush %s", flushentry::dump(*it).c_str());

      mdflushqueue.erase(it);
      mdqueue[ino]--;
      mdflush.UnLock();

      if (assistant.terminationRequested())
      {
        return;
      }

      eos_static_debug("metacache::flush ino=%016lx authid=%s op=%d", ino, authid.c_str(), (int) op);
      {

        shared_md md;

        if (!mdmap.retrieveTS(ino, md))
        {
          continue;
        }

        eos_static_info("metacache::flush ino=%016lx", (unsigned long long) ino);

        if (op != metad::mdx::LSTORE)
        {
          XrdSysMutexHelper mdLock(md->Locker());
          if (!md->md_pino())
          {
            // when creating objects locally faster than pushed upstream
            // we might not know the remote parent id when we insert a local
            // creation request

            shared_md pmd;
            if (mdmap.retrieveTS(md->pid(), pmd))
            {
              // TODO: check if we need to lock pmd? But then we have to enforce
              // locking order child -> parent
              uint64_t md_pino = pmd->md_ino();
              eos_static_crit("metacache::flush providing parent inode %016lx to %016lx", md->id(), md_pino);
              md->set_md_pino(md_pino);
            }
            else
            {
              eos_static_crit("metacache::flush ino=%016lx parent remote inode not known", (unsigned long long) ino);
            }
          }
        }
        if (md->id())
        {
          uint64_t removeentry=0;
          {
            md->Locker().Lock();

            int rc = 0;

            if (op == metad::mdx::RM)
              md->set_operation(md->DELETE);
            else
              md->set_operation(md->SET);

            if ((op != metad::mdx::RM) && md->deleted())
            {
              // if the md was deleted in the meanwhile does not need to
              // push it remote, since the response creates a race condition
              md->Locker().UnLock();
              continue;
            }

            if (((op == metad::mdx::ADD) ||
                (op == metad::mdx::UPDATE) ||
                (op == metad::mdx::RM)) &&
                md->id() != 1)
            {
              eos_static_info("metacache::flush backend::putMD - start");

              md->set_type(md->MD);

              // push to backend
              if ((rc = mdbackend->putMD(f_id, &(*md), authid, &(md->Locker()))))
              {
                eos_static_err("metacache::flush backend::putMD failed rc=%d", rc);
                // ---------------------------------------------------------------
                // in this case we always clean this MD record to force a refresh
                // ---------------------------------------------------------------
                inomap.erase_bwd(md->id());
                //removeentry=md->id();
                md->set_err(rc);
              }
              else
              {
                inomap.insert(md->md_ino(), md->id());
              }

              if (md->getop() != md->RM)
              {
                md->setop_none();
              }
              md->Signal();
              eos_static_info("metacache::flush backend::putMD - stop");
            }

            if ( (op == metad::mdx::ADD) || (op == metad::mdx::UPDATE) || (op == metad::mdx::LSTORE) )
            {
              std::string mdstream;
              md->SerializeToString(&mdstream);
              md->Locker().UnLock();
              EosFuse::Instance().getKV()->put(ino, mdstream);
            }
            else
            {
              md->Locker().UnLock();
              if (op == metad::mdx::RM)
              {
                EosFuse::Instance().getKV()->erase(ino);
                // this step is coupled to the forget function, since we cannot
                // forget an entry if we didn't process the outstanding KV changes
                stat.inodes_deleted_dec();
                eos_static_debug("count=%d(-%d) - ino=%016x", md->lookup_is(), 1, ino);
                XrdSysMutexHelper mLock(md->Locker());
                if (md->lookup_dec(1))
                {
                  // forget this inode
                  removeentry = ino;
                }
              }
            }
          }
          if (removeentry)
          {
            shared_md pmd;
            eos_static_debug("delete md object - ino=%016x", removeentry);
            {
              XrdSysMutexHelper mmLock(mdmap);
              mdmap.retrieve(md->pid(), pmd);
              mdmap.erase(removeentry);
              stat.inodes_dec();
            }
            {
              if (pmd)
              {

                XrdSysMutexHelper mmLock(pmd->Locker());
                //		pmd->get_todelete().erase(md->name());
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
int
/* -------------------------------------------------------------------------- */
metad::calculateDepth(shared_md md)
/* -------------------------------------------------------------------------- */
{
  if(md->id() == 1 || md->id() == 0) {
    return 1;
  }

  fuse_ino_t pino = md->pid();
  if(pino == 1 || pino == 0) {
    return 2;
  }

  shared_md pmd;
  if(!mdmap.retrieveTS(pino, pmd)) {
    eos_static_warning("could not lookup parent ino=%d of %d when calculating depth..", pino, md->id());
    return -1;
  }

  XrdSysMutexHelper mmLock(pmd->Locker());
  return calculateDepth(pmd) + 1;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::mdcommunicate(ThreadAssistant &assistant)
/* -------------------------------------------------------------------------- */
{
  eos::fusex::container hb;
  hb.mutable_heartbeat_()->set_name(zmq_name);
  hb.mutable_heartbeat_()->set_host(zmq_clienthost);
  hb.mutable_heartbeat_()->set_uuid(zmq_clientuuid);
  hb.mutable_heartbeat_()->set_version(VERSION);
  hb.mutable_heartbeat_()->set_protversion(hb.heartbeat_().PROTOCOLV2);
  hb.mutable_heartbeat_()->set_pid((int32_t) getpid());
  hb.mutable_heartbeat_()->set_starttime(time(NULL));
  hb.set_type(hb.HEARTBEAT);

  eos::fusex::response rsp;
  size_t cnt=0;
  int interval=1;

  while (!assistant.terminationRequested())
  {

    try
    {
      eos_static_debug("");

      zmq::pollitem_t items[] ={
        {static_cast<void*> (*z_socket), 0, ZMQ_POLLIN, 0}
      };

      for (int i = 0; i < 100 * interval; ++i)
      {
        //eos_static_debug("poll %d", i );
        // 10 milliseconds
        zmq_poll(items, 1, 10);

        if (assistant.terminationRequested())
        {
          return;
        }

        if (items[0].revents & ZMQ_POLLIN)
        {
          int rc;
          int64_t more;
          size_t more_size = sizeof (more);
          zmq_msg_t message;
          rc = zmq_msg_init (&message);

          do
          {
            //eos_static_debug("0MQ receive");
            int size = zmq_msg_recv (&message, static_cast<void*> (*z_socket), 0);
            //int size = z_socket.recv (&message, 0);
            size=size;
            //eos_static_debug("0MQ size=%d", size);
            zmq_getsockopt (static_cast<void*> (*z_socket), ZMQ_RCVMORE, &more, &more_size);
          }
          while (more);

          std::string s((const char*) zmq_msg_data(&message), zmq_msg_size(&message));

          rsp.Clear();

          if (rsp.ParseFromString(s))
          {
            if (rsp.type() == rsp.EVICT)
            {
              eos_static_crit("evicted from MD server - reason: %s",
                              rsp.evict_().reason().c_str());

              // suicide
              kill(getpid(), SIGINT);
              pause();
            }

	    if (rsp.type() == rsp.DROPCAPS)
	    {
	      eos_static_notice("MGM asked us to drop all known caps");
	      // a newly started MGM requests this as a response to the first heartbeat
	      EosFuse::Instance().caps.reset();
	    }

	    if (rsp.type() == rsp.CONFIG)
	    {
	      if (rsp.config_().hbrate())
	      {
		eos_static_notice("MGM asked us to set our heartbeat interval to %d seconds", rsp.config_().hbrate());
		interval = (int) rsp.config_().hbrate();
	      }
	    }


            if (rsp.type() == rsp.LEASE)
            {

              uint64_t md_ino = rsp.lease_().md_ino();
              std::string authid = rsp.lease_().authid();

              uint64_t ino = inomap.forward(md_ino);
              eos_static_info("lease: remote-ino=%lx ino=%lx clientid=%s authid=%s",
                              md_ino, ino, rsp.lease_().clientid().c_str(), authid.c_str());

	      shared_md check_md;
	      if (ino && mdmap.retrieveTS(ino, check_md)) {
                std::string capid = cap::capx::capid(ino, rsp.lease_().clientid());

                // wait that the inode is flushed out of the mdqueue
                do
                {
                  mdflush.Lock();
                  if (mdqueue.count(ino))
                  {
                    mdflush.UnLock();
                    eos_static_info("lease: delaying cap-release remote-ino=%lx ino=%lx clientid=%s authid=%s",
                                    md_ino, ino, rsp.lease_().clientid().c_str(), authid.c_str());
                    XrdSysTimer delay;
                    delay.Wait(25);
                  }
                  else
                  {
                    mdflush.UnLock();
                    break;
                  }
                }
                while (1);
                fuse_ino_t ino = EosFuse::Instance().getCap().forget(capid);
                {
                  shared_md md;
                  {
                    XrdSysMutexHelper mmLock(mdmap);
                    if (mdmap.count(ino))
                    {
                      md = mdmap[ino];
                      md->Locker().Lock();
                    }
                  }

                  // invalidate children
                  if (md && md->id())
                  {
                    eos_static_info("md=%16x", md->id());
                    std::map<std::string, uint64_t> children_copy;
                    if (EosFuse::Instance().Config().options.md_kernelcache)
                    {
                      for (auto it = md->children().begin(); it != md->children().end(); ++it)
                      {
                        children_copy[it->first] = it->second;
                      }
                    }

                    md->Locker().UnLock();
                    md->cap_count_reset();
		    md->set_creator(false);
                    eos_static_debug("%s", dump_md(md).c_str());

                    if (EosFuse::Instance().Config().options.md_kernelcache)
                    {
                      for (auto it = children_copy.begin(); it != children_copy.end(); ++it)
                      {
                        shared_md child_md;
                        if (mdmap.retrieveTS(it->second, child_md))
                        {
                          // we know this inode
                          mode_t mode;
                          {
                            XrdSysMutexHelper mLock(md->Locker());
                            mode = md->mode();
                          }
                          // avoid any locks while doing this
                          if ( EosFuse::Instance().Config().options.data_kernelcache )
                          {
                            if (!S_ISDIR(mode))
                            {
                              kernelcache::inval_inode(it->second, true);
                            }
                            else
                            {
                              kernelcache::inval_inode(it->second, false);
                            }
                            // drop the entry stats
                            kernelcache::inval_entry(ino, it->first);
                          }
                        }
                      }
                      eos_static_info("invalidated direct children ino=%016lx cap-cnt=%d", ino, md->cap_count());
                    }
                  }
                }
              }
            }
            if (rsp.type() == rsp.MD)
            {
              fuse_req_t req;
              memset(&req, 0, sizeof (fuse_req_t));
              uint64_t md_ino = rsp.md_().md_ino();
              std::string authid = rsp.md_().authid();
              uint64_t ino = inomap.forward(md_ino);

              eos_static_info("md-update: remote-ino=%lx ino=%lx authid=%s",
                              md_ino, ino, authid.c_str());
              // we get this when a file update/flush appeared
              shared_md md;
              {
                bool create = true;
                int64_t bookingsize=0;
                uint64_t pino = 0;
                uint64_t ino = 0;
                mode_t mode = 0;
                std::string md_clientid;

                {
                  // MD update logic
                  {
                    if (ino && mdmap.retrieveTS(ino, md))
                    {
                      // updated file MD
                      if (EOS_LOGS_DEBUG)
                        eos_static_debug("%s op=%d", md->dump().c_str(), md->getop());
                      md->Locker().Lock();
                      bookingsize = rsp.md_().size() - md->size();
                      md_clientid = rsp.md_().clientid();
                      *md = rsp.md_();
                      md->clear_clientid();
                      pino = md->pid();
                      ino = md->id();
                      mode = md->mode();
                      if (EOS_LOGS_DEBUG)
                        eos_static_debug("%s op=%d", md->dump().c_str(), md->getop());

                      // update the local store
                      update(req, md, authid, true);
                      create = false;

                      md->Locker().UnLock();
                    }
                  }
                  if (!create)
                  {
                    // adjust local quota
                    cap::shared_cap cap = EosFuse::Instance().caps.get(pino, md_clientid);
                    if (cap->id())
                    {
                      if (bookingsize >= 0)
                      {
                        EosFuse::Instance().caps.book_volume(cap, (uint64_t) bookingsize);
                      }
                      else
                      {
                        EosFuse::Instance().caps.free_volume(cap, (uint64_t) - bookingsize);
                      }
                      EosFuse::instance().caps.book_inode(cap);
                    }
                    else
                    {
                      eos_static_err("missing quota node for pino=%16x and clientid=%s",
                                     pino, md->clientid().c_str());
                    }
                    // possibly invalidate kernel cache
                    if (EosFuse::Instance().Config().options.data_kernelcache)
                    {
                      eos_static_info("invalidate data cache for ino=%016lx", ino);
                      kernelcache::inval_inode(ino, S_ISDIR(mode) ? false : true);
                    }
                    if (S_ISREG(mode))
                    {
                      // invalidate local disk cache
                      EosFuse::Instance().datas.invalidate_cache(ino);
                      eos_static_info("invalidate local disk cache for ino=%016lx", ino);
                    }
                  }
                }

                if (create)
                {
                  // new file
                  md = md = std::make_shared<mdx>();
                  *md = rsp.md_();
                  uint64_t new_ino = insert(req, md , authid);
                  uint64_t md_pino = md->md_pino();
                  std::string md_clientid= md->clientid();
                  uint64_t md_size = md->size();

                  // add to mdmap
                  mdmap[new_ino] = md;
                  // add to parent
                  uint64_t pino = inomap.forward(md_pino);

		  shared_md pmd;

                  if (pino && mdmap.retrieveTS(pino,pmd))
                  {
		    if (md->pt_mtime())
                    {
		      pmd->set_mtime(md->pt_mtime());
		      pmd->set_mtime_ns(md->pt_mtime_ns());
		    }

		    md->clear_pt_mtime();
		    md->clear_pt_mtime_ns();
		    add(0, pmd, md, authid, true);
		    update(req, pmd, authid, true);

                    // adjust local quota
                    cap::shared_cap cap = EosFuse::Instance().caps.get(pino, md_clientid);
                    if (cap->id())
                    {
                      EosFuse::Instance().caps.book_volume(cap, md_size);
                      EosFuse::instance().caps.book_inode(cap);
                    }
                    else
                    {
                      eos_static_err("missing quota node for pino=%16x and clientid=%s",
                                     pino, md->clientid().c_str());
                    }
                  }
                  else
                  {
                    eos_static_err("missing parent mapping pino=%16x for ino%16x",
                                   md_pino,
                                   md_ino);
                  }
                }
              }
            }
          }
          else
          {
            eos_static_err("unable to parse message");
          }
          rc = rc;
          zmq_msg_close(&message);
        }
      }
      //eos_static_debug("send");

      // prepare a heart-beat message
      struct timespec tsnow;
      eos::common::Timing::GetTimeSpec(tsnow);
      hb.mutable_heartbeat_()->set_clock(tsnow.tv_sec);
      hb.mutable_heartbeat_()->set_clock_ns(tsnow.tv_nsec);

      if (!(cnt % 60))
      {
        // we send a statistics update every 60 heartbeats
        EosFuse::Instance().getHbStat((*hb.mutable_statistics_()));
      }
      else
      {
        hb.clear_statistics_();
      }

      {
        // add caps to be extended
        XrdSysMutexHelper eLock(EosFuse::Instance().getCap().get_extensionLock());
        auto map = hb.mutable_heartbeat_()->mutable_authextension();
        cap::extension_map_t extmap = EosFuse::Instance().getCap().get_extensionmap();

        for (auto it = extmap.begin(); it != extmap.end(); ++it)
        {
          (*map)[it->first] = it->second;
          eos_static_info("cap-extension: authid=%s delta=%u", it->first.c_str(),
                          it->second);
          ;
        }
        extmap.clear();
        eos_static_info("cap-extension: map-size=%u", extmap.size());
      }


      std::string hbstream;
      hb.SerializeToString(&hbstream);

      z_socket->send(hbstream.c_str(), hbstream.length());
    }
    catch (std::exception &e)
    {

      eos_static_err("catched exception %s", e.what());
    }
    cnt++;
  }
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::vmap::insert(fuse_ino_t a, fuse_ino_t b)
/* -------------------------------------------------------------------------- */
{
  eos_static_info("inserting %llx <=> %llx", a, b);
  //fprintf(stderr, "inserting %llx => %llx\n", a, b);

  XrdSysMutexHelper mLock(mMutex);

  if (fwd_map.count(a) && fwd_map[a] == b)
    return;

  if (bwd_map.count(b))
  {
    fwd_map.erase(bwd_map[b]);
  }
  fwd_map[a]=b;
  bwd_map[b]=a;

  uint64_t a64 = a;
  uint64_t b64 = b;
  if (a != 1 && EosFuse::Instance().getKV()->put(a64, b64, "l"))
  {

    throw std::runtime_error("REDIS backend failure - nextinode");
  }
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
metad::vmap::dump()
/* -------------------------------------------------------------------------- */
{
  //XrdSysMutexHelper mLock(this);
  std::string sout;
  char stime[1024];
  snprintf(stime, sizeof (stime), "%lu this=%llx forward=%lu backward=%lu", time(NULL), (unsigned long long) this, fwd_map.size(), bwd_map.size());
  sout += stime;
  sout += "\n";

  for (auto it = fwd_map.begin(); it != fwd_map.end(); it++)
  {
    char out[1024];
    snprintf(out, sizeof (out), "%16lx => %16lx\n", it->first, it->second);
    sout += out;
  }

  for (auto it = bwd_map.begin(); it != bwd_map.end(); it++)
  {

    char out[1024];
    snprintf(out, sizeof (out), "%16lx <= %16lx\n", it->first, it->second);
    sout += out;
  }
  sout += "end\n";
  return sout;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::vmap::erase_fwd(fuse_ino_t lookup)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(mMutex);
  if (fwd_map.count(lookup))
  {

    bwd_map.erase(fwd_map[lookup]);
  }
  fwd_map.erase(lookup);
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::vmap::erase_bwd(fuse_ino_t lookup)
{
  XrdSysMutexHelper mLock(mMutex);
  if (bwd_map.count(lookup))
  {

    fwd_map.erase(bwd_map[lookup]);
  }
  bwd_map.erase(lookup);
}

/* -------------------------------------------------------------------------- */
fuse_ino_t
/* -------------------------------------------------------------------------- */
metad::vmap::forward(fuse_ino_t lookup)
{
  XrdSysMutexHelper mLock(mMutex);

  fuse_ino_t ino = fwd_map[lookup];

  if (!ino)
  {
    uint64_t a64=lookup;
    uint64_t b64;
    if (EosFuse::Instance().getKV()->get(a64, b64, "l"))

    if (EosFuse::Instance().getKV()->get(a64, b64, "l")) {
      bwd_map[b64] = a64;
      return ino;
    }
    else
    {

      fwd_map[a64]=b64;
      bwd_map[b64]=a64;
      ino = b64;
    }
  }
  return ino;
}

/* -------------------------------------------------------------------------- */
fuse_ino_t
/* -------------------------------------------------------------------------- */
metad::vmap::backward(fuse_ino_t lookup)
{
  XrdSysMutexHelper mLock(mMutex);
  return bwd_map[lookup];
}
