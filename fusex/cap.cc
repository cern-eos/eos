//------------------------------------------------------------------------------
//! @file cap.cc
//! @author Andreas-Joachim Peters CERN
//! @brief cap data handling class
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

#include "cap.hh"
#include "MacOSXHelper.hh"
#include "common/Logging.hh"

cap* cap::sCAP=0;

/* -------------------------------------------------------------------------- */
cap::cap()
/* -------------------------------------------------------------------------- */
{
  sCAP=this;
}

/* -------------------------------------------------------------------------- */
cap::~cap()
/* -------------------------------------------------------------------------- */
{
  sCAP=0;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
cap::init(backend* _mdbackend, metad* _metad)
/* -------------------------------------------------------------------------- */
{
  mdbackend = _mdbackend;
  mds = _metad;
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
cap::capx::dump()
/* -------------------------------------------------------------------------- */
{
  char sout[16384];
  snprintf(sout, sizeof (sout),
           "id=%lx mode=%x vtime=%lu.%lu uid=%u gid=%u client-id=%s auth-id=%s errc=%d",
           id(), mode(), vtime(), vtime_ns(), uid(), gid(), clientid().c_str(), authid().c_str(), errc()
           );
  return sout;
}

/* -------------------------------------------------------------------------- */
cap::shared_cap
/* ----------------------------------------------------------- -------------- */
cap::get(fuse_req_t req,
         fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  std::string cid = cap::capx::capid(req, ino);

  eos_static_debug("inode=%08lx cap-id=%s", ino, cid.c_str());

  XrdSysMutexHelper mLock(capmap);
  if (capmap.count(cid))
  {
    shared_cap cap = capmap[cid];
    return cap;
  }
  else
  {
    shared_cap cap = std::make_shared<capx>();
    cap->set_clientid(cid);
    cap->set_authid(cid);
    cap->set_clientuuid(mds->get_clientuuid());
    cap->set_id(ino);
    cap->set_uid(fuse_req_ctx(req)->uid);
    cap->set_gid(fuse_req_ctx(req)->gid);
    cap->set_vtime(0);
    cap->set_vtime_ns(0);
    capmap[cid] = cap;
    mds->increase_cap(ino);
    return cap;
  }
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
cap::store(fuse_req_t req,
           eos::fusex::cap icap)
/* -------------------------------------------------------------------------- */
{
  std::string cid = cap::capx::capid(req, icap.id());

  uint64_t id = mds->vmaps().forward(icap.id());

  XrdSysMutexHelper mLock(capmap);
  if (capmap.count(cid))
  {
    shared_cap cap = capmap[cid];
    *cap = icap;
    cap->set_id(id);
  }
  else
  {
    shared_cap cap = std::make_shared<capx>();
    cap->set_clientid(cid);
    cap->set_id(id);
    *cap = icap;
    capmap[cid] = cap;
    mds->increase_cap(icap.id());
  }
  eos_static_debug("store inode=[l:%lx r:%lx] capid=%s cap: %s", icap.id(), id, cid.c_str(),
                   capmap[cid]->dump().c_str());
}

/* -------------------------------------------------------------------------- */
cap::shared_cap
/* -------------------------------------------------------------------------- */
cap::acquire(fuse_req_t req,
             fuse_ino_t ino,
             mode_t mode
             )
/* -------------------------------------------------------------------------- */
{
  std::string cid = cap::capx::capid(req, ino);
  eos_static_debug("inode=%08lx cap-id=%s mode=%x", ino, cid.c_str(), mode);

  shared_cap cap = get(req, ino);

  if (!cap->valid())
  {
    refresh(req, cap);
  }

  if (!cap->satisfy(mode) || !cap->valid())
  {
    cap->set_errc(EPERM);
  }
  else
  {
    cap->set_errc(0);
  }

  eos_static_debug("%s", cap->dump().c_str());
  return cap;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
cap::refresh(fuse_req_t req, shared_cap cap)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("inode=%08lx cap-id=%s", cap->id(), cap->clientid().c_str());

  // retrieve cap from upstream
  std::vector<eos::fusex::container> contv;
  int rc=0;
  uint64_t remote_ino = mds->vmaps().backward(cap->id());
  rc = mdbackend->getCAP(req, remote_ino, contv);
  if (!rc)
  {
    // decode the cap
    for (auto it=contv.begin(); it != contv.end(); ++it)
    {
      switch (it->type()) {
      case eos::fusex::container::CAP:
      {
        uint64_t id = mds->vmaps().forward(it->cap_().id());
        XrdSysMutexHelper mLock(cap->Locker());
        // check if the cap received matches what we think about local mapping
        if (cap->id() == id)
        {
          eos_static_debug("correct cap received for inode=%08x", cap->id());
          // great
          *cap = it->cap_();
          cap->set_id(id);
        }
        else
        {
          eos_static_debug("wrong cap received for inode=%08x", cap->id());
          // that is a fatal logical error
          rc = ENXIO;
        }
        break;
      }
      default:
        eos_static_err("msg=\"wrong content type received\" type=%d",
                       it->type());
      }
    }
    return;
  }
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
cap::capx::satisfy(mode_t mode)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(Locker());

  if ( ((mode & this->mode())) == mode)
  {
    eos_static_debug("inode=%08lx cap-id=%s mode=%x test-mode=%x satisfy=true",
                     id(), clientid().c_str(), this->mode(), mode);
    return true;
  }

  eos_static_debug("inode=%08lx cap-id=%s mode=%x test-mode=%x satisfy=false",
                   id(), clientid().c_str(), this->mode(), mode);
  return false;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
cap::capx::valid(bool debug)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(Locker());
  struct timespec ts;
  ts.tv_sec = vtime();
  ts.tv_nsec = vtime_ns();

  if (eos::common::Timing::GetCoarseAgeInNs(&ts, 0) < 0)
  {
    if (debug)
      eos_static_debug("inode=%08lx cap-id=%s now=%lu vtime=%lu valid=true",
                       id(), clientid().c_str(), time(NULL), vtime());
    return true;
  }
  else
  {
    if (debug)
      eos_static_debug("inode=%08lx cap-id=%s now=%lu vtime=%lu valid=false",


                       id(), clientid().c_str(), time(NULL), vtime());
    return false;
  }
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
cap::capflush()
/* -------------------------------------------------------------------------- */
{
  while (1)
  {
    {
      cmap capdelmap;

      capmap.Lock();
      for (auto it = capmap.begin(); it != capmap.end(); ++it)
      {
        // make a list of caps to timeout
        if (!it->second->valid(false))
        {
          capdelmap[it->first]=it->second;
          eos_static_debug("expire %s", it->second->dump().c_str());
          mds->decrease_cap(it->second->id());
        }
      }
      for (auto it = capdelmap.begin(); it != capdelmap.end(); ++it)
      {
        // remove the expired caps
        capmap.erase(it->first);
      }
      capmap.UnLock();
      XrdSysTimer sleeper;
      sleeper.Wait(1000);
    }
  }
}
