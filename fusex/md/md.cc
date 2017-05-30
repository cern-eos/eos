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

std::string metad::vnode_gen::cInodeKey = "nextinode";

/* -------------------------------------------------------------------------- */
metad::metad() : mdflush(0), mdqueue_max_backlog(1000),
z_ctx(1), z_socket(z_ctx, ZMQ_DEALER)
/* -------------------------------------------------------------------------- */
{
  // make a mapping for inode 1, it is re-loaded afterwards in init '/'


  {
    inomap.insert(1, 1);
  }
  mdmap[1] = std::make_shared<mdx>(1);
  XrdSysMutexHelper mLock(mdmap[1]->Locker());
  mdmap[1]->set_nlink(1);
  mdmap[1]->set_mode( S_IRWXU | S_IRWXG | S_IRWXO | S_IFDIR);
  mdmap[1]->set_name(":root:");
  mdmap[1]->set_pid(1);
  stat.inodes_inc();
  stat.inodes_ever_inc();
  mdbackend = 0;
}

/* -------------------------------------------------------------------------- */
metad::~metad()
/* -------------------------------------------------------------------------- */
{

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

  shared_md root_md = load_from_kv(1);
  if (root_md->id() != 1)
  {
    fuse_req_t req = 0;
    XrdSysMutexHelper mLock(mdmap);
    update(req, mdmap[1], "", true);
  }
  next_ino.init();
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
metad::connect(std::string zmqtarget, std::string zmqidentity, std::string zmqname, std::string zmqclienthost, std::string zmqclientuuid)
/* -------------------------------------------------------------------------- */
{
  if (z_socket.connected() && (zmqtarget != zmq_target))
  {
    // when the target changes we disconnect first from the old one
    //zmq_disconnect(*z_socket, zmq_target);
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

  z_socket.setsockopt(ZMQ_IDENTITY, zmq_identity.c_str(), zmq_identity.length()
                      );
  try
  {
    z_socket.connect(zmq_target);
    eos_static_notice("connected to %s", zmq_target.c_str());
  }
  catch (zmq::error_t& e)
  {
    eos_static_err("msg=\"%s\" rc=%d", e.what(), e.num());
    return e.num();
  }

  mdbackend->set_clientuuid(zmq_clientuuid);

  return 0;
}

/* -------------------------------------------------------------------------- */
//metad::shared_md
/* -------------------------------------------------------------------------- */
//metad::lookupold(fuse_req_t req,
//                 fuse_ino_t parent,
//                 const char* name)
/* -------------------------------------------------------------------------- */
/*{
  eos_static_info("ino=%08llx name=%s", parent, name);
  shared_md pmd = get(req, parent);

  if (pmd->i  d() == parent)
  {
    std::string sname=name;

    if (pmd->reqid())
    {
      if (pmd->children().count(sname))
      {
        fuse_ino_t name_ino = pmd->children().at(sname);
        eos_static_info("name=%s => %08llx", name, name_ino);
        return get(req, name_ino, false, pmd->md_ino(), name);
      }
    }
    else
    {
      eos_static_info("name=%s => unknown", name);
      return get(req, 0, false, pmd->md_ino(), name, parent);
    }
  }
  return  std::make_shared<mdx>();
}
 */

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
  shared_md pmd = get(req, parent, false);
  shared_md md;

  if (pmd->id() == parent)
  {
    fuse_ino_t inode = 0; // inode referenced by parent + name
    // --------------------------------------------------
    // STEP 2: check if we hold a cap for that directory
    // --------------------------------------------------
    if (pmd->cap_count())
    {
      // --------------------------------------------------
      // if we have a cap amd we listed this directory, we trust the child information  
      // --------------------------------------------------
      if (pmd->children().count(name))
      {
        inode = pmd->children().at(name);
      }
      else
      {
        if (pmd->type() == pmd->MDLS)
        {
          // no entry - TODO return a NULLMD object instead of creating it all the time
          md = std::make_shared<mdx>();
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
    md = get(req, inode, false, pmd, name);
  }
  else
  {
    // --------------------------------------------------
    // no md available
    // --------------------------------------------------
    md = std::make_shared<mdx>();
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
  XrdSysMutexHelper mLock(mdmap);
  if (mdmap.count(ino))
  {
    metad::shared_md md = mdmap[ino];
    if (md->id())
    {
      XrdSysMutexHelper mLock(md->Locker());
      if (md->lookup_dec(nlookup))
      {
        // forget this inode
        mdmap.erase(ino);
        stat.inodes_dec();
        return 0;
      }
    }
    return EAGAIN;
  }
  return ENOENT;
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
  if (EosFuse::Instance().Config().options.kernelcache)
  {
    e.attr_timeout=3600.0;
    e.entry_timeout=3600.0;
  }
  else
  {
    e.attr_timeout=0;
    e.entry_timeout=0;
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
           "ino=%08lx dev=%08x mode=%08x nlink=%08x uid=%05d gid=%05d rdev=%08x "
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
           "ino=%08lx dev=%08x mode=%08x nlink=%08x uid=%05d gid=%05d rdev=%08x "
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
metad::shared_md
/* -------------------------------------------------------------------------- */
metad::load_from_kv(fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  // mdmap is locked when this is called !
  std::string mdstream;
  shared_md md = std::make_shared<mdx>();
  if (!kv::Instance().get(ino, mdstream))
  {
    if (!md->ParseFromString(mdstream))
    {
      eos_static_err("msg=\"GPB parsing failed\" inode=%08lx", ino);
    }
    else
    {
      eos_static_debug("msg=\"GPB parsed inode\" inode=%08lx", ino);
    }
    mdmap[ino] = md;

    if (md->md_ino() && ino)
    {
      inomap.insert(md->md_ino(), ino);
    }

    for (auto it = md->children().begin(); it != md->children().end(); ++it)
    {
      eos_static_info("adding child %s ino=%08lx", it->first.c_str(), it->second);
      if (mdmap.count(it->second))
        continue;

      shared_md cmd = std::make_shared<mdx>();
      if (!kv::Instance().get(it->second, mdstream))
      {
        if (!cmd->ParseFromString(mdstream))
        {
          eos_static_err("msg=\"GPB parsing failed\" inode=%08lx", it->second);
        }
        else
        {
          eos_static_debug("msg=\"GPB parsed inode\" inode=%08lx", it->second);
        }
        mdmap[it->second] = cmd;
        if (cmd->md_ino() && ino)
        {
          inomap.insert(cmd->md_ino(), it->second);
        }
      }
    }
    stat.inodes_inc();
    stat.inodes_ever_inc();
  }
  else
  {
    eos_static_debug("msg=\"no entry in kv store\" inode=%08lx", ino);
  }
  return md;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
metad::map_children_to_local(shared_md pmd)
/* -------------------------------------------------------------------------- */
{
  bool ret = true;
  // exchange the remote inode map with the local used inode map
  std::vector<std::string> names ;

  for (auto map = pmd->children().begin(); map != pmd->children().end(); ++map)
  {
    eos_static_debug("translate %s [%lx]", map->first.c_str(), map->second);
    names.push_back(map->first);
  }

  for (size_t i=0; i < names.size(); ++i)
  {
    uint64_t remote_ino = (*pmd->mutable_children())[names[i]];
    uint64_t local_ino = inomap.forward(remote_ino);

    if (!local_ino)
    {
      local_ino = next_ino.inc();
      inomap.insert(remote_ino, local_ino);
      shared_md md = std::make_shared<mdx>();
      XrdSysMutexHelper mLock(mdmap);
      mdmap[local_ino] = md;
    }
    eos_static_debug("store-lookup r-ino %08x <=> l-ino %08x", remote_ino, local_ino);
    (*pmd->mutable_children())[names[i]] = local_ino;
  }


  for (auto map = pmd->children().begin(); map != pmd->children().end(); ++map)
  {
    eos_static_debug("listing: %s [%lx]", map->first.c_str(), map->second);
  }

  return ret;
}

/* -------------------------------------------------------------------------- */
metad::shared_md
/* -------------------------------------------------------------------------- */
metad::get(fuse_req_t req,
           fuse_ino_t ino,
           bool listing,
           shared_md pmd,
           const char* name,
           bool readdir
           )
/* -------------------------------------------------------------------------- */
{
  eos_static_info("ino=%08llx pino=%08llx name=%s listing=%d", ino, pmd ? pmd->id() : 0, name, listing);
  shared_md md;
  bool loaded = false;

  if (ino)
  {
    // the inode is known, we try to get that one
    XrdSysMutexHelper mLock(mdmap);
    if (mdmap.count(ino))
    {
      // -----------------------------------------------------------------------
      // first we attach to an existing record
      // -----------------------------------------------------------------------
      md = mdmap[ino];
      eos_static_debug("MD:\n%s", dump_md(md).c_str());
    }
    else
    {
      // -----------------------------------------------------------------------
      // if there is none we load the current cached md from the kv store
      // which also loads all available child meta data
      // -----------------------------------------------------------------------

      md = load_from_kv(ino);
      eos_static_info("loaded from kv ino=%08llx remote-ino=%08llx", md->id(), md->md_ino());
      loaded = true;
    }
  }
  else
  {
    // -------------------------------------------------------------------------
    // this happens if we get asked for a child, which was never listed before
    // -------------------------------------------------------------------------
    md = std::make_shared<mdx>();
  }

  if (!md->id() || loaded)
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
    if (pmd && pmd->cap_count())
    {
      eos_static_info("returning cap entry");
      return md;
    }
    else
    {
      eos_static_info("pmd=%x cap-cnt=%d", pmd ? pmd->id() : 0, pmd ? pmd->cap_count() : 0);
      if ( ( (!listing) || (listing && md->type() == md->MDLS) ) && md->md_ino() && md->cap_count())
      {
        eos_static_info("returning cap entry via parent lookup cap-count=%d", md->cap_count());
        eos_static_debug("MD:\n%s", dump_md(md).c_str());
        return md;
      }

      if (!S_ISDIR(md->mode()))
      {
        // files are covered by the CAP of the parent, so if there is a cap 
        // on the parent we can return this entry right away
        shared_md pmd = mdmap[md->pid()];
        if (pmd && pmd->id())
        {
          if (pmd->cap_count())
          {
            return md;
          }
        }
      }
    }

    if (!md->pid() && !md->deleted())
    {
      /*
       if ( (md->getop() == md->ADD) ||
           (md->getop() == md->LSTORE) )
       */
      {
        // this must have been generated locally, we return this entry
        eos_static_info("returning generated entry");
        eos_static_debug("MD:\n%s", dump_md(md).c_str());
        return md;
      }
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
    rc = mdbackend->getMD(req, root_path, contv);
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
      XrdSysMutexHelper mdLock(pmd->Locker());
      if (pmd->md_ino())
      {
        rc = mdbackend->getMD(req, pmd->md_ino(), name, contv, listing);
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
    if (md->md_ino())
    {
      eos_static_info("ino=%08lx type=%d", md->md_ino(), md->type());
      rc = mdbackend->getMD(req, md->md_ino(), listing ? ( (md->type() != md->MDLS) ? 0 : md->clock()) : md->clock(), contv, listing);
    }
    else
    {
      if (md->id() && !loaded)
      {
        // that can be a locally created entry which is not yet upstream
        rc = 0;
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

    //    md->Locker().Lock();
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
    //    md->Locker().UnLock();



    {
      XrdSysMutexHelper mLock(mdmap);
      if (mdmap.count(ino))
      {
        // if the md record was returned, it is accessible after the apply function
        // attached it. We should also attach to the parent to be able to add
        // a not yet published child entry at the parent.
        md = mdmap[ino];
        if (mdmap.count(md->pid()))
        {
          pmd = mdmap[md->pid()];
        }
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
        if (!pmd->children().count(md->name()))
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
  eos_static_debug("MD:\n%s", dump_md(md).c_str());

  if (rc)
  {
    return std::make_shared<mdx>();
  }
  return md;
}

/* -------------------------------------------------------------------------- */
//metad::shared_md
/* -------------------------------------------------------------------------- */
//metad::getold(fuse_req_t req,
//              fuse_ino_t ino,
//              bool listing,
//              fuse_ino_t remote_pino,
//              const char* name,
//              fuse_ino_t pino)
/* -------------------------------------------------------------------------- */
/*
{
  shared_md md;
  {
    XrdSysMutexHelper mLock(mdmap);
    if (mdmap.count(ino))
    {
      md = mdmap[ino];
    }
    else
    {
      // -----------------------------------------------------------------------
      // the first thing is to try to load a parent directory and all its 
      // children from the local KV store
      // -----------------------------------------------------------------------

      md = load_from_kv(ino);
    }
  }

  // ---------------------------------------------------------------------------
  // this variable indicates if we can stay with the local cached information
  // ---------------------------------------------------------------------------
  bool remote_refresh = false;

  uint64_t remote_ino = 0;

  // ---------------------------------------------------------------------------
  // if this is a get from opendir, we have to fetch all the children, if we 
  // never requested this MD but got it as a result to a parent query
  // ---------------------------------------------------------------------------
  {
    XrdSysMutexHelper mdLock(md->Locker());
    remote_ino = md->md_ino();
    if (listing && !md->reqid())
    {
      eos_static_debug("forcing remote-refresh - not yet listed");
      remote_refresh = true;
    }

    if (!md->cap_count())
    {
      eos_static_debug("forcing remote-refresh - no cap");
      remote_refresh = true;
    }
  }


  if (remote_ino && !remote_refresh)
  {
    inomap.insert(remote_ino, ino);
    eos_static_debug("return unrefreshed MD %lx<=>%lx", remote_ino, ino);
    return md;
  }
  else
  {
    if (remote_ino)
      inomap.insert(remote_ino, ino);
    eos_static_debug("refreshing MD %lx<=>%lx", remote_ino, ino);
  }
  // the root case is special because we don't know the remote inode and have
  // to query by path

  bool getroot=false;
  {
    // verify if the MD is (still) valid
    if ( (ino == 1) && (!remote_ino))
    {
      getroot = true;
    }
  }

  std::vector<eos::fusex::container> contv;
  int rc=0;

  remote_ino=0;

  if (getroot)
  {
    // retrieve the root node by path
    std::string root_path="/";
    // request the root metadata
    rc = mdbackend->getMD(req, root_path, contv);
  }
  else
  {
    // retrieve meta data by inode
    {
      remote_ino = inomap.backward(ino);
      if (remote_ino)
      {
        eos_static_debug("get by inode ino=%lx", remote_ino);
        rc = mdbackend->getMD(req, remote_ino, md->clock(), contv, listing);
      }
      else
      {
        eos_static_debug("by parent inode/name pino=%lx name=%s", remote_pino, name);
        if (remote_pino)
          rc = mdbackend->getMD(req, remote_pino, name, contv, listing);
        else
          rc = ENOENT;
      }
    }
  }

  if (!rc)
  {
    md->Locker().Lock();
    for (auto it=contv.begin(); it != contv.end(); ++it)
    {
      if (it->ref_inode_())
      {
        if (getroot) inomap.insert(it->ref_inode_(), 1);

        // store the retrieved meta data blob
        apply(req, *it, listing, pmd);
      }
      else
      {
        if (getroot)
          eos_static_crit("msg=\"could not retrieve meta-data for root inode 1");
        else
          eos_static_crit("msg=\"could not retrieve meta-data for inode=%16lx",
                          remote_ino);
      }
    }
    if (getroot) md->set_pid(1);
    if (listing)
      md->set_reqid((uint64_t) req);
    md->Locker().UnLock();
  }

  {
    XrdSysMutexHelper mLock(mdmap);
    if (pino)
    {
      eos_static_info("get by ino/name searching pino=%lx", pino);
      // get by parent inode/name
      if (mdmap.count(pino))
      {
        eos_static_info("get by ino/name found pino=%lx", pino);
        shared_md pmd = mdmap[pino];
        if (pmd->children().count(name))
        {
          ino = pmd->children().at(name);
          eos_static_info("get by ino/name found pino=%lx child-ino=%lx", pino, ino);
        }
        else
        {
          eos_static_info("get by ino/name found pino=%lx no child", pino);
        }
      }
    }
    if (ino)
    {
      // get by inode
      if (mdmap.count(ino))
      {
        eos_static_info("get by ino/name assinging ino md", ino);
        md = mdmap[ino];
      }
    }
  }
  return md;
}
 */

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
    XrdSysMutexHelper mLock(mdmap);
    newinode = next_ino.inc();
    md->set_id(newinode);
    mdmap[newinode]=md;
  }

  /*
  mdflush.Lock();

  
   // the add command pushes it to the queue
  if ( md->getop() != metad::mdx::LSTORE )
  {

    while (mdqueue.size() == mdqueue_max_backlog)
      mdflush.WaitMS(25);
  }

  mdqueue[newinode] = authid;
  mdflush.Signal();
  mdflush.UnLock();
   */
  return newinode;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
metad::wait_flush(fuse_req_t req,
                  metad::shared_md md)
/* -------------------------------------------------------------------------- */
{
  // logic to make creation synchronous
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
    return md->err();
  }
  else
  {
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

  flushentry fe(authid, localstore ? mdx::LSTORE : mdx::UPDATE);

  mdqueue[md->id()].push_back(fe);

  eos_static_info("added ino=%lx authid=%s queue-size=%u local-store=%d", md->id(), flushentry::dump(mdqueue[md->id()]).c_str(), mdqueue.size(), localstore);

  mdflush.Signal();
  mdflush.UnLock();
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::add(metad::shared_md pmd, metad::shared_md md, std::string authid)
/* -------------------------------------------------------------------------- */
{
  stat.inodes_inc();
  stat.inodes_ever_inc();

  uint64_t pid=0;
  uint64_t id=0;

  auto map = pmd->mutable_children();
  eos_static_debug("child=%s parent=%s inode=%08lx authid=%s", md->name().c_str(),
                   pmd->name().c_str(), md->id(), authid.c_str());
  {
    XrdSysMutexHelper mLock(pmd->Locker());
    (*map)[md->name()] = md->id();
    pmd->set_nlink(1);
    pmd->set_nchildren(pmd->nchildren() + 1);
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

  while (mdqueue.size() == mdqueue_max_backlog)
    mdflush.WaitMS(25);

  flushentry fe(authid, mdx::ADD);
  mdqueue[id].push_back(fe);

  flushentry fep(authid, mdx::LSTORE);
  mdqueue[pid].push_back(fep);

  mdflush.Signal();
  mdflush.UnLock();
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::remove(metad::shared_md pmd, metad::shared_md md, std::string authid,
              bool upstream)
/* -------------------------------------------------------------------------- */
{
  auto map = pmd->mutable_children();
  eos_static_debug("child=%s parent=%s inode=%08lx", md->name().c_str(),
                   pmd->name().c_str(), md->id());
  {
    XrdSysMutexHelper mLock(pmd->Locker());
    (*map).erase(md->name());
    pmd->set_nchildren(pmd->nchildren() - 1);
  }

  if (!md->deleted())
  {
    md->lookup_inc();
    stat.inodes_deleted_inc();
    stat.inodes_deleted_ever_inc();
  }

  md->setop_delete();

  if (!upstream)
    return ;

  flushentry fe(authid, mdx::RM);
  flushentry fep(authid, mdx::LSTORE);

  mdflush.Lock();

  while (mdqueue.size() == mdqueue_max_backlog)
    mdflush.WaitMS(25);

  mdqueue[pmd->id()].push_back(fep);
  mdqueue[md->id()].push_back(fe);

  stat.inodes_backlog_store(mdqueue.size());

  mdflush.Signal();
  mdflush.UnLock();

}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::mv(shared_md p1md, shared_md p2md, shared_md md, std::string newname,
          std::string authid1, std::string authid2)
/* -------------------------------------------------------------------------- */
{
  auto map1 = p1md->mutable_children();
  auto map2 = p2md->mutable_children();

  eos_static_debug("child=%s parent=%s newparent=%s inode=%08lx", md->name().c_str(),
                   p1md->name().c_str(), p2md->name().c_str(), md->id());

  XrdSysMutexHelper mLock(md->Locker());

  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts);

  if (p1md->id() != p2md->id())
  {
    // move between directories
    XrdSysMutexHelper m1Lock(p1md->Locker());
    XrdSysMutexHelper m2Lock(p2md->Locker());
    (*map2)[newname] = md->id();
    (*map1).erase(md->name());
    p1md->set_nchildren(p1md->nchildren() - 1);
    p2md->set_nchildren(p2md->nchildren() + 1);
    p1md->set_mtime(ts.tv_sec);
    p1md->set_mtime_ns(ts.tv_sec);
    p1md->set_ctime(ts.tv_sec);
    p1md->set_ctime_ns(ts.tv_nsec);
    p2md->set_mtime(ts.tv_sec);
    p2md->set_mtime_ns(ts.tv_sec);
    p2md->set_ctime(ts.tv_sec);
    p2md->set_ctime_ns(ts.tv_nsec);
    md->set_name(newname);
    md->set_pid(p2md->id());
    md->set_md_pino(p2md->md_ino());
  }
  else
  {
    // move within directory
    XrdSysMutexHelper m1Lock(p1md->Locker());
    (*map2)[newname] = md->id();
    (*map1).erase(md->name());
    md->set_name(newname);
  }

  md->set_mtime(ts.tv_sec);
  md->set_mtime_ns(ts.tv_sec);
  md->set_ctime(ts.tv_sec);
  md->set_ctime_ns(ts.tv_nsec);

  mdflush.Lock();

  while (mdqueue.size() == mdqueue_max_backlog)
    mdflush.WaitMS(25);

  flushentry fe1(authid1, mdx::UPDATE);
  mdqueue[p1md->id()].push_back(fe1);

  if (p1md->id() != p2md->id())
  {
    flushentry fe2(authid2, mdx::UPDATE);
    mdqueue[p2md->id()].push_back(fe2);
  }
  flushentry fe(authid2, mdx::UPDATE);

  mdqueue[md->id()].push_back(fe);

  stat.inodes_backlog_store(mdqueue.size());

  mdflush.Signal();
  mdflush.UnLock();
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
metad::dump_md(shared_md md)
/* -------------------------------------------------------------------------- */
{
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string jsonstring;
  google::protobuf::util::MessageToJsonString( *((eos::fusex::md*)(&(*md))), &jsonstring, options);
  return jsonstring;
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
metad::dump_md(eos::fusex::md& md)
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
      return EINVAL;
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

  // do sync upstream lock call
  int rc = mdbackend->doLock(req, *md, &(md->Locker()));

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
  }
  // clean the lock structure;
  md->clear_flock();

  return rc;
}

/* -------------------------------------------------------------------------- */
uint64_t
/* -------------------------------------------------------------------------- */
metad::apply(fuse_req_t req, eos::fusex::container & cont, bool listing)
/* -------------------------------------------------------------------------- */
{
  shared_md md;
  shared_md pmd;

  bool unlock_pmd = false;

  eos_static_debug(dump_container(cont).c_str());

  switch (cont.type()) {

  case cont.MD:
  {
    uint64_t md_ino = cont.md_().md_ino();
    uint64_t ino = inomap.forward(md_ino);
    if (ino)
    {
      XrdSysMutexHelper mLock(mdmap);
      if (mdmap.count(ino))
      {
        md = mdmap[ino];
      }
      else
      {
        std::string mdstream;
        md = std::make_shared<mdx>();
        mdmap[ino] = md;
      }
      {
        *md = cont.md_();
        eos_static_debug("store md for local-ino=%08lx remote-ino=%08lx -", (long) ino, (long) md_ino);
        eos_static_debug("%s", md->dump().c_str());
      }
      uint64_t p_ino = inomap.forward(md->md_pino());
      if (!p_ino)
      {
        eos_static_crit("msg=\"missing lookup entry for inode\" ino=%08lx", ino);
      }
      assert(p_ino != 0);
      md->set_pid(p_ino);
      md->set_id(ino);
      eos_static_info("store local pino=%08lx for %08x", md->pid(), md->id());
      update(req, md, "", true);
      return ino;
    }
    else
    {
      eos_static_crit("msg=\"no local inode\" remote-ino=%lx", md_ino);
      return 0;
    }
  }
    break;

  case cont.MDMAP:
  {
    uint64_t p_ino = inomap.forward(cont.ref_inode_());

    for (auto map = cont.md_map_().md_map_().begin(); map != cont.md_map_().md_map_().end(); ++map)
    {
      // loop over the map of meta data objects

      uint64_t ino = inomap.forward(map->first);

      eos::fusex::cap cap_received;
      cap_received.set_id(0);

      eos_static_debug("remote-ino=%08lx local-ino=%08lx", (long) map->first, ino);

      if (ino)
      {
        // this is an already known inode
        eos_static_debug("lock mdmap");
        bool mdexist = false;
        {
          XrdSysMutexHelper mLock(mdmap);
          if (mdmap.count(ino))
          {
            md = mdmap[ino];
            mdexist = true;
          }
          else
          {
            md = std::make_shared<mdx>();
            mdmap[ino] = md;
            mdexist = true;
          }
        }
        if (mdexist)
        {
          {
            if (map->first != cont.ref_inode_())
            {
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

            *md = map->second;
            md->clear_capability();

            md->set_id(ino);
            /*
            
            if (!listing)
            {
              p_ino = inomap.forward(md->md_pino());
            }
         
             */
            p_ino = inomap.forward(md->md_pino());

            md->set_pid(p_ino);
            eos_static_info("store remote-ino=%08lx local pino=%08lx for %08x", md->md_pino(), md->pid(), md->id());
            // push only into the local KV cache - md was retrieved from upstream

            if (map->first != cont.ref_inode_())
              update(req, md, "", true);

            eos_static_debug("store md for local-ino=%08ld remote-ino=%08lx type=%d -", (long) ino, (long) map->first, md->type());
            eos_static_debug("%s", md->dump().c_str());

            if (map->first != cont.ref_inode_())
            {
              md->Locker().UnLock();
            }
            else
            {
              md->Locker().UnLock();
            }
            if (cap_received.id())
            {
              // store cap
              cap::Instance().store(req, cap_received);
              md->cap_inc();
              //eos_static_err("increase cap counter for ino=%lu", ino);
            }
          }
        }
        else
        {
          eos_static_crit("msg=\"inconsistent mapping found - md_inode->v_inode exists but no MD entry\" md_ino=%08x v_ino=%08x",
                          map->first, ino);
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
          //	  pmd->Locker().Lock();
          unlock_pmd = true;
        }

        uint64_t new_ino = insert(req, md, md->authid());

        md->set_id(new_ino);
        if (!listing)
        {
          p_ino = inomap.forward(md->md_pino());
        }
        md->set_pid(p_ino);
        eos_static_info("store local pino=%08lx for %08x", md->pid(), md->id());

        inomap.insert(map->first, new_ino);

        {
          XrdSysMutexHelper mLock(mdmap);
          mdmap[new_ino] = md;
        }

        update(req, md, md->authid(), true);

        if (cap_received.id())
        {
          // store cap
          cap::Instance().store(req, cap_received);
          md->cap_inc();
          //          eos_static_err("increase cap counter for ino=%lu", new_ino);
        }
        eos_static_debug("store md for local-ino=%08x remote-ino=%08x type=%d -", (long) new_ino, (long) map->first, md->type());
        eos_static_debug("%s", md->dump().c_str());
      }
    }

    if (pmd && listing)
    {
      bool ret = false;
      if (! (ret = map_children_to_local(pmd)))
      {
        eos_static_err("local mapping has failed %d", ret);
        assert(0);
      }
      if (unlock_pmd)
      {
        //      pmd->Locker().UnLock();
      }

      for (auto map = pmd->children().begin(); map != pmd->children().end(); ++map)
      {
        eos_static_debug("listing: %s [%lx]", map->first.c_str(), map->second);
      }
    }
    if (pmd)
    {
      // store the parent now, after all children are inservted
      update(req, pmd, "", true);
    }
  }

    break;
  default:
    return 0;
    break;
  }

  if (pmd)
    return pmd->id();
  else
    return 0;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::mdcflush()
/* -------------------------------------------------------------------------- */
{
  while (1)
  {
    {
      mdflush.Lock();

      stat.inodes_backlog_store(mdqueue.size());

      while (mdqueue.size() == 0)
        mdflush.Wait();

      auto it= mdqueue.begin();
      uint64_t ino = it->first;

      // take the flush set for a given inode and merge them
      flushentry_set_t flushset = flushentry::merge(it->second);

      eos_static_info("metacache::flush ino=%lx queue-size=%u", ino, mdqueue.size());
      eos_static_info("metacache::flush %s", flushentry::dump(flushset).c_str());

      mdqueue.erase(it);
      mdflush.UnLock();

      for (auto fit = flushset.begin(); fit != flushset.end(); ++fit)
      {
        flushentry entry = *fit;
        std::string authid = entry.authid();

        mdx::md_op op = entry.op();
        eos_static_debug("metacache::flush ino=%08lx authid=%s op=%d", ino, authid.c_str(), (int) op);
        {
          shared_md md;
          {
            XrdSysMutexHelper mLock(mdmap);

            if (should_terminate())
              return;

            if (mdmap.count(ino))
            {
              eos_static_info("metacache::flush ino=%08lx", (unsigned long long) ino);

              md = mdmap[ino];
              if (!md->md_pino())
              {
                // when creating objects locally faster than pushed upstream
                // we might not know the remote parent id when we insert a local
                // creation request
                if (mdmap.count(md->pid()))
                {
                  uint64_t md_pino = mdmap[md->pid()]->md_ino();
                  if (md_pino)
                  {
                    eos_static_crit("metacache::flush providing parent inode %08lx to %08lx", md->id(), md_pino);
                    md->set_md_pino(md_pino);
                  }
                  else
                  {
                    eos_static_crit("metacache::flush ino=%08lx parent remote inode not known", (unsigned long long) ino);
                  }
                }
              }
            }
            else
            {
              continue;
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

              if (((op == metad::mdx::ADD) ||
                  (op == metad::mdx::UPDATE) ||
                  (op == metad::mdx::RM)) &&
                  md->id() != 1)
              {
                eos_static_info("metacache::flush backend::putMD - start");

                // push to backend
                if ((rc = mdbackend->putMD(&(*md), authid, &(md->Locker()))))
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
                md->setop_none();
                
                md->Signal();
                eos_static_info("metacache::flush backend::putMD - stop");
              }

              if ( (op == metad::mdx::ADD) || (op == metad::mdx::UPDATE) || (op == metad::mdx::LSTORE) )
              {
                std::string mdstream;
                md->SerializeToString(&mdstream);
                md->Locker().UnLock();
                kv::Instance().put(ino, mdstream);
              }
              else
              {
                md->Locker().UnLock();
                if (op == metad::mdx::RM)
                {
                  kv::Instance().erase(ino);
                  // this step is coupled to the forget function, since we cannot
                  // forget an entry if we didn't process the outstanding KV changes
                  stat.inodes_deleted_dec();
                  if (md->lookup_dec(1))
                  {
                    // forget this inode
                    removeentry = ino;
                    stat.inodes_dec();
                  }
                }
              }
            }
            if (removeentry)
            {
              XrdSysMutexHelper mmLock(mdmap);
              mdmap.erase(removeentry);
            }
          }
        }
      }
    }
  }
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::mdcommunicate()
/* -------------------------------------------------------------------------- */
{
  eos::fusex::container hb;
  hb.mutable_heartbeat_()->set_name(zmq_name);
  hb.mutable_heartbeat_()->set_host(zmq_clienthost);
  hb.mutable_heartbeat_()->set_uuid(zmq_clientuuid);
  hb.mutable_heartbeat_()->set_version(VERSION);
  hb.mutable_heartbeat_()->set_pid((int32_t) getpid());
  hb.mutable_heartbeat_()->set_starttime(time(NULL));
  hb.set_type(hb.HEARTBEAT);

  eos::fusex::response rsp;

  while (1)
  {
    if (should_terminate())
      return;

    try
    {
      eos_static_debug("");

      zmq::pollitem_t items[] ={
        {z_socket, 0, ZMQ_POLLIN, 0}
      };

      for (int i = 0; i < 100; ++i)
      {
        //eos_static_debug("poll %d", i );
        // 10 milliseconds
        zmq_poll(items, 1, 10);

        if (should_terminate())
          return;

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
            int size = zmq_msg_recv (&message, z_socket, 0);
            size=size;
            //eos_static_debug("0MQ size=%d", size);

            rc = zmq_getsockopt(z_socket, ZMQ_RCVMORE, &more, &more_size);
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
              wait();
            }
            if (rsp.type() == rsp.LEASE)
            {

              uint64_t md_ino = rsp.lease_().md_ino();
              std::string authid = rsp.lease_().authid();

              uint64_t ino = inomap.forward(md_ino);
              eos_static_info("lease: remote-ino=%lx ino=%lx clientid=%s authid=%s",
                              md_ino, ino, rsp.lease_().clientid().c_str(), authid.c_str());
              if (ino)
              {
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
                fuse_ino_t ino = cap::Instance().forget(capid);
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
                    if (EosFuse::Instance().Config().options.kernelcache)
                    {
                      eos_static_info("invalidate direct children ino=%08lx", ino);
                      for (auto it = md->children().begin(); it != md->children().end(); ++it)
                      {
                        eos_static_info("invalidate child ino=%08lx", it->second);
                        kernelcache::inval_inode(it->second);
                        kernelcache::inval_entry(ino, it->first);
                        //mdmap.erase(it->second);
                      }
                      eos_static_info("invalidated direct children ino=%08lx cap-cnt=%d", ino, md->cap_count());
                    }
                    md->Locker().UnLock();
                    md->cap_count_reset();
                    //XrdSysMutexHelper mmLock(mdmap);
                    //mdmap.erase(ino);
                  }
                }
              }
            }
          }
          else
          {
            eos_static_err("unable to parse message");
          }
          rc=rc;
          zmq_msg_close(&message);
        }
      }
      //eos_static_debug("send");

      // prepare a heart-beat message
      struct timespec tsnow;
      eos::common::Timing::GetTimeSpec(tsnow);
      hb.mutable_heartbeat_()->set_clock(tsnow.tv_sec);
      hb.mutable_heartbeat_()->set_clock_ns(tsnow.tv_nsec);

      std::string hbstream;
      hb.SerializeToString(&hbstream);

      z_socket.send(hbstream.c_str(), hbstream.length());
    }
    catch (std::exception &e)
    {
      eos_static_err("catched exception %s", e.what());
    }
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
  if (a != 1 && kv::Instance().put(a64, b64, "l"))
  {
    throw std::runtime_error("REDIS backend failure - nextinode");
  }


  //eos_static_info("%s", dump().c_str());

  /*
  fprintf(stderr, "============================================\n");
  for (auto it = fwd_map.begin(); it != fwd_map.end(); it++)
  {
    fprintf(stderr, "%16lx <=> %16lx\n", it->first, it->second);
  }
  fprintf(stderr, "============================================\n");
   */
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
    if (kv::Instance().get(a64, b64, "l"))
    {
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
