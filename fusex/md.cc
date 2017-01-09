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

#include "md.hh"
#include "kv.hh"
#include "MacOSXHelper.hh"
#include "common/Logging.hh"

#include <iostream>
#include <sstream>


std::string metad::vnode_gen::cInodeKey = "nextinode";

/* -------------------------------------------------------------------------- */
metad::metad() : mdflush(0), mdqueue_max_backlog(1000)
/* -------------------------------------------------------------------------- */
{
  // make a mapping for inode 1, it is re-loaded afterwards in init '/'
  {
    XrdSysMutexHelper mLock(inomap);
    inomap[1]=1;
  }
  mdmap[1] = std::make_shared<mdx>(1);
  XrdSysMutexHelper mLock(mdmap[1]->Locker());
  mdmap[1]->set_nlink(2);
  mdmap[1]->set_mode( S_IRWXU | S_IRWXG | S_IRWXO | S_IFDIR);
  mdmap[1]->set_name(":root:");
  stat.inodes_inc();
  stat.inodes_ever_inc();
}

/* -------------------------------------------------------------------------- */
metad::~metad()
/* -------------------------------------------------------------------------- */
{

}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::init()
/* -------------------------------------------------------------------------- */
{
  std::string mdstream;
  // load the root node

  if (!kv::Instance().get((uint64_t) 1, mdstream))
  {
    if (!mdmap[1]->ParseFromString(mdstream))
    {
      eos_static_err("msg=\"GPB parsing failed\" inode=%08lx", 1);
    }
    else
    {
      eos_static_debug("msg=\"GPB parsed root inode\"");
    }
  }
  else
  {
    fuse_req_t req = 0;
    XrdSysMutexHelper mLock(mdmap);
    update(req, mdmap[1]);
  }
  next_ino.init();
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
  shared_md pmd = get(req, parent);
  if (pmd->id() == parent)
  {
    std::string sname=name;

    if (pmd->children().count(sname))
    {
      fuse_ino_t name_ino = pmd->children().at(sname);
      return get(req, name_ino);
    }
  }
  return  std::make_shared<mdx>();
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
  e.attr.st_nlink=nlink() + 2;
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
  e.attr_timeout=0.0;
  e.entry_timeout=0.0;
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
metad::get(fuse_req_t req,
           fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(mdmap);
  if (mdmap.count(ino))
  {
    shared_md md = mdmap[ino];
    return md;
  }
  else
  {
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
      stat.inodes_inc();
      stat.inodes_ever_inc();
    }
    return md;
  }
}

/* -------------------------------------------------------------------------- */
uint64_t
/* -------------------------------------------------------------------------- */
metad::insert(fuse_req_t req,
              metad::shared_md md)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(mdmap);
  uint64_t newinode = next_ino.inc();
  md->set_id(newinode);
  mdmap[newinode]=md;

  mdflush.Lock();

  stat.inodes_backlog_store(mdqueue.size());

  while (mdqueue.size() == mdqueue_max_backlog)
    mdflush.Wait();
  mdqueue.insert(md->id());
  mdflush.Signal();
  mdflush.UnLock();

  return newinode;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::update(fuse_req_t req,
              shared_md md)
/* -------------------------------------------------------------------------- */
{
  mdflush.Lock();
  stat.inodes_backlog_store(mdqueue.size());

  while (mdqueue.size() == mdqueue_max_backlog)
    mdflush.Wait();


  mdqueue.insert(md->id());

  mdflush.Signal();
  mdflush.UnLock();
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::add(metad::shared_md pmd, metad::shared_md md)
/* -------------------------------------------------------------------------- */
{
  stat.inodes_inc();
  stat.inodes_ever_inc();

  auto map = pmd->mutable_children();
  eos_static_debug("child=%s parent=%s inode=%08lx", md->name().c_str(),
                   pmd->name().c_str(), md->id());
  {
    XrdSysMutexHelper mLock(pmd->Locker());
    (*map)[md->name()] = md->id();
    pmd->set_nlink(pmd->nlink() + 1);
  }

  mdflush.Lock();

  while (mdqueue.size() == mdqueue_max_backlog)
    mdflush.Wait();

  mdqueue.insert(pmd->id());

  mdflush.Signal();
  mdflush.UnLock();
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::remove(metad::shared_md pmd, metad::shared_md md)
/* -------------------------------------------------------------------------- */
{
  auto map = pmd->mutable_children();
  eos_static_debug("child=%s parent=%s inode=%08lx", md->name().c_str(),
                   pmd->name().c_str(), md->id());
  {
    XrdSysMutexHelper mLock(pmd->Locker());
    (*map).erase(md->name());
    pmd->set_nlink(pmd->nlink() - 1);
  }

  if (!md->deleted())
  {
    md->lookup_inc();
    stat.inodes_deleted_inc();
    stat.inodes_deleted_ever_inc();
  }

  md->setop_delete();

  mdflush.Lock();

  while (mdqueue.size() == mdqueue_max_backlog)
    mdflush.Wait();

  mdqueue.insert(pmd->id());
  mdqueue.insert(md->id());

  stat.inodes_backlog_store(mdqueue.size());

  mdflush.Signal();
  mdflush.UnLock();

}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
metad::mv(shared_md p1md, shared_md p2md, shared_md md, std::string newname)
/* -------------------------------------------------------------------------- */
{
  auto map1 = p1md->mutable_children();
  auto map2 = p2md->mutable_children();

  eos_static_debug("child=%s parent=%s newparent=%s inode=%08lx", md->name().c_str(),
                   p1md->name().c_str(), p2md->name().c_str(), md->id());

  XrdSysMutexHelper mLock(md->Locker());

  if (p1md->id() != p2md->id())
  {
    // move between directories
    XrdSysMutexHelper m1Lock(p1md->Locker());
    XrdSysMutexHelper m2Lock(p2md->Locker());
    (*map2)[newname] = md->id();
    (*map1).erase(md->name());
    p1md->set_nlink(p1md->nlink() - 1);
    p2md->set_nlink(p2md->nlink() + 1);
    md->set_name(newname);
  }
  else
  {
    // move within directory
    XrdSysMutexHelper m1Lock(p1md->Locker());
    (*map2)[newname] = md->id();
    (*map1).erase(md->name());
    md->set_name(newname);
  }

  mdflush.Lock();

  while (mdqueue.size() == mdqueue_max_backlog)
    mdflush.Wait();

  mdqueue.insert(p1md->id());

  if (p1md->id() != p2md->id())
  {
    mdqueue.insert(p2md->id());
  }

  mdqueue.insert(md->id());

  stat.inodes_backlog_store(mdqueue.size());

  mdflush.Signal();
  mdflush.UnLock();
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
      uint64_t ino = *it;
      mdqueue.erase(it);
      mdflush.UnLock();
      {
        XrdSysMutexHelper mLock(mdmap);
        if (mdmap.count(ino))
        {
          eos_static_info("metacache::flush ino=%08lx", (unsigned long long) ino);

          shared_md md = mdmap[ino];
          metad::mdx::md_op op = md->getop();

          if (op == metad::mdx::ADD)
          {
            std::string mdstream;
            md->SerializeToString(&mdstream);
            kv::Instance().put(ino, mdstream);
          }
          else
          {
            if (op == metad::mdx::DELETE)
            {
              kv::Instance().erase(ino);
              // this step is coupled to the forget function, since we cannot
              // forget an entry if we didn't process the outstanding KV changes
              stat.inodes_deleted_dec();
              if (md->lookup_dec(1))
              {
                // forget this inode
                mdmap.erase(ino);
                stat.inodes_dec();
              }
            }
          }
        }
      }
    }
  }
}
