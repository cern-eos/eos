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

#include "cap/cap.hh"
#include "eosfuse.hh"
#include "md/kernelcache.hh"
#include "misc/MacOSXHelper.hh"
#include "misc/fusexrdlogin.hh"
#include "common/Logging.hh"
#include "common/Timing.hh"

/* -------------------------------------------------------------------------- */
cap::cap()
/* -------------------------------------------------------------------------- */
{
  mdbackend = 0;
  mds = 0;
}

/* -------------------------------------------------------------------------- */
cap::~cap()
/* -------------------------------------------------------------------------- */
{
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
cap::capx::dump(bool dense)
/* -------------------------------------------------------------------------- */
{
  char sout[16384];

  if (dense) {
    snprintf(sout, sizeof(sout),
             "i=%08lx m=%x c=%s",
             id(), mode(), clientid().c_str()
            );
  } else {
    snprintf(sout, sizeof(sout),
             "id=%#lx mode=%#x vtime=%lu.%lu u=%u g=%u cid=%s auth-id=%s errc=%d maxs=%lu q-node=%16lx ino=%lu vol=%lu",
             id(), mode(), vtime(), vtime_ns(), uid(), gid(), clientid().c_str(),
             authid().c_str(), errc(),
             max_file_size(),
             _quota().quota_inode(),
             _quota().inode_quota(),
             _quota().volume_quota()
            );
  }

  return sout;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
cap::reset()
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(capmap);
  XrdSysMutexHelper rLock(revocationLock);

  for (auto it : capmap) {
    revocationset.insert(it.second->authid());
  }

  capmap.clear();
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
cap::capx::capid(fuse_req_t req, fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  char sid[256];
  std::string login = fusexrdlogin::xrd_login(req);
  snprintf(sid, sizeof(sid),
           "%lx:%u:%u:%s@%s:%s",
           ino,
           fuse_req_ctx(req)->uid,
           fuse_req_ctx(req)->gid,
           login.c_str(),
           EosFuse::Instance().Config().clienthost.c_str(),
           EosFuse::Instance().Config().name.c_str()
          );
  return sid;
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
cap::capx::capid(fuse_ino_t ino, std::string clientid)
/* -------------------------------------------------------------------------- */
{
  char sid[256];
  snprintf(sid, sizeof(sid),
           "%lx:%s",
           ino,
           clientid.c_str()
          );
  return sid;
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
cap::capx::getclientid(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  char sid[256];
  std::string login = fusexrdlogin::xrd_login(req);
  snprintf(sid, sizeof(sid),
           "%u:%u:%s@%s:%s",
           fuse_req_ctx(req)->uid,
           fuse_req_ctx(req)->gid,
           login.c_str(),
           EosFuse::Instance().Config().clienthost.c_str(),
           EosFuse::Instance().Config().name.c_str()
          );
  return sid;
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
cap::ls()
/* -------------------------------------------------------------------------- */
{
  std::string listing;
  XrdSysMutexHelper mLock(capmap);

  for (auto it = capmap.begin(); it != capmap.end(); ++it) {
    listing += it->second->dump(false);
    listing += "\n";
  }

  if (listing.size() > (64 * 1000)) {
    listing.resize((64 * 1000));
    listing += "\n... (truncated) ...\n";
  }

  char csize[32];
  snprintf(csize, sizeof(csize), "# [ %lu caps ]\n", capmap.size());
  listing += csize;
  return listing;
}

/* -------------------------------------------------------------------------- */
cap::shared_cap
/* ----------------------------------------------------------- -------------- */
cap::get(fuse_req_t req,
         fuse_ino_t ino,
         bool lock)
/* -------------------------------------------------------------------------- */
{
  std::string cid = cap::capx::capid(req, ino);
  std::string clientid = cap::capx::getclientid(req);
  eos_static_debug("inode=%08lx cap-id=%s", ino, cid.c_str());
  XrdSysMutexHelper mLock(capmap);

  if (capmap.count(cid)) {
    shared_cap cap = capmap[cid];
    return cap;
  } else {
    shared_cap cap = std::make_shared<capx>();
    cap->set_clientid(clientid);
    cap->set_authid("");
    cap->set_clientuuid(mds->get_clientuuid());
    cap->set_id(ino);
    cap->set_uid(fuse_req_ctx(req)->uid);
    cap->set_gid(fuse_req_ctx(req)->gid);
    cap->set_vtime(0);
    cap->set_vtime_ns(0);
    capmap[cid] = cap;
    return cap;
  }
}

/* -------------------------------------------------------------------------- */
cap::shared_cap
/* -------------------------------------------------------------------------- */
cap::get(fuse_ino_t ino, std::string clientid)
/* -------------------------------------------------------------------------- */
{
  std::string cid = cap::capx::capid(ino, clientid);
  eos_static_debug("inode=%08lx cap-id=%s", ino, cid.c_str());
  XrdSysMutexHelper mLock(capmap);

  if (capmap.count(cid)) {
    shared_cap cap = capmap[cid];
    return cap;
  } else {
    shared_cap cap = std::make_shared<capx>();
    cap->set_id(0);
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
  std::string clientid = cap::capx::getclientid(req);
  uint64_t id = mds->vmaps().forward(icap.id());
  std::string cid = cap::capx::capid(req, id); // cid uses the local inode
  XrdSysMutexHelper mLock(capmap);

  if (capmap.count(cid)) {
    shared_cap cap = capmap[cid];
    *cap = icap;
    cap->set_id(id);
  } else {
    shared_cap cap = std::make_shared<capx>();
    cap->set_clientid(clientid);
    *cap = icap;
    cap->set_id(id);
    capmap[cid] = cap;
  }

  eos_static_debug("store inode=[r:%lx l:%lx] capid=%s cap: %s", icap.id(), id,
                   cid.c_str(),
                   capmap[cid]->dump().c_str());
}

/* -------------------------------------------------------------------------- */
fuse_ino_t
/* -------------------------------------------------------------------------- */
cap::forget(const std::string& cid)
/* -------------------------------------------------------------------------- */
{
  fuse_ino_t inode = 0;
  {
    XrdSysMutexHelper mLock(capmap);

    if (capmap.count(cid)) {
      eos_static_debug("forget capid=%s cap: %s", cid.c_str(),
                       capmap[cid]->dump().c_str());
      shared_cap cap = capmap[cid];
      inode = cap->id();
      capmap.erase(cid);
      XrdSysMutexHelper rLock(revocationLock);
      revocationset.insert(cap->authid());
    } else {
      eos_static_debug("forget capid=%s cap: ENOENT", cid.c_str());
    }
  }

  if (inode) {
    if (EosFuse::Instance().Config().options.md_kernelcache) {
      kernelcache::inval_inode(inode, false);
    }
  }

  return inode;
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
cap::imply(shared_cap cap,
           std::string imply_authid,
           mode_t mode,
           fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  shared_cap implied_cap = std::make_shared<capx>();
  *implied_cap = *cap;
  implied_cap->set_authid(imply_authid);
  implied_cap->set_id(ino);
  implied_cap->set_vtime(cap->vtime() +
                         EosFuse::Instance().Config().options.leasetime);
  std::string clientid = cap->clientid();
  std::string cid = capx::capid(ino, clientid);
  XrdSysMutexHelper mLock(capmap);
  // TODO: deal with the influence of mode to the cap itself
  capmap[cid] = implied_cap;
  return cid;
}

/* -------------------------------------------------------------------------- */
cap::shared_cap
/* -------------------------------------------------------------------------- */
cap::acquire(fuse_req_t req,
             fuse_ino_t ino,
             mode_t mode,
             bool lock
            )
/* -------------------------------------------------------------------------- */
{
  // the parent of 1 might be 0
  if (!ino) { ino = 1; }

  std::string cid = cap::capx::capid(req, ino);
  eos_static_debug("inode=%08lx cap-id=%s mode=%x", ino, cid.c_str(), mode);
  shared_cap cap = get(req, ino);
  bool try_attach = false;
  // avoid we create the same cap concurrently
  {
    XrdSysMutexHelper cLock(cap->Locker());

    if (!cap->valid()) {
      if (refresh(req, cap)) {
        cap->set_errc(errno ? errno : EIO);
        return cap;
      }

      try_attach = true;
    }

    if (!cap->satisfy(mode) || !cap->valid()) {
      cap->set_errc(EPERM);
    } else {
      cap->set_errc(0);
    }

    eos_static_debug("%s", cap->dump().c_str());
  }
  XrdSysMutexHelper mLock(capmap);
  XrdSysMutexHelper mLock2(cap->Locker());

  if (try_attach) {
    if (!capmap.count(cid)) {
      capmap[cid] = cap;
      cap->set_id(ino);
    }
  }

  // stamp latest time of use
  cap->use();
  return cap;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
cap::refresh(fuse_req_t req, shared_cap cap)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("inode=%08lx cap-id=%s", cap->id(), cap->clientid().c_str());
  // retrieve cap from upstream
  std::vector<eos::fusex::container> contv;
  int rc = 0;
  uint64_t remote_ino = mds->vmaps().backward(cap->id());
  // measure the call duration
  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts, true);

  do {
    rc = mdbackend->getCAP(req, remote_ino, contv);

    if (!rc) {
      // decode the cap
      for (auto it = contv.begin(); it != contv.end(); ++it) {
        switch (it->type()) {
        case eos::fusex::container::CAP: {
          uint64_t id = mds->vmaps().forward(it->cap_().id());

          //XrdSysMutexHelper mLock(cap->Locker());
          // check if the cap received matches what we think about local mapping
          if (cap->id() == id) {
            eos_static_debug("correct cap received for inode=%#lx", cap->id());
            // great
            *cap = it->cap_();
            cap->set_id(id);
          } else {
            eos_static_debug("wrong cap received for inode=%#lx", cap->id());
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

      return rc;
    } else {
      if (errno != EPERM) {
	eos_static_err("GETCAP failed with errno=%d for inode=%16x", errno, cap->id());
      }

      if (errno != EL2NSYNC) {
        return rc;
      }

      // if there is a time synchronization error reported we check if the call just took long to execute
      // 2 seconds is the maximum allowed roundtrip/out-of-sync time applied by the MGM
      uint64_t ns_lag;

      if ((ns_lag = eos::common::Timing::GetCoarseAgeInNs(&ts, 0)) < 2000000000) {
        eos_static_err("GETCAP finished during the allowed 2s round-trip time - our clock seems to be out of sync with the MGM!");
        return EL2NSYNC;
      } else {
        float backoff = round(10 * random() / (double) RAND_MAX);
        eos_static_warning("GETCAP exceeded 2s (%.02fs) round-trip time for inode=%16x - backing of for %.02f seconds, then retry!",
                           ns_lag / 1000000000.0,
                           cap->id(),
                           backoff);
        std::this_thread::sleep_for(std::chrono::seconds((int) backoff));
      }
    }
  } while (1);
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
cap::capx::satisfy(mode_t mode)
/* -------------------------------------------------------------------------- */
{
  //XrdSysMutexHelper mLock(Locker());
  if (((mode & this->mode())) == mode) {
    eos_static_debug("inode=%08lx client-id=%s mode=%x test-mode=%x satisfy=true",
                     id(), clientid().c_str(), this->mode(), mode);
    return true;
  }

  eos_static_debug("inode=%08lx client-id=%s mode=%x test-mode=%x satisfy=false",
                   id(), clientid().c_str(), this->mode(), mode);
  return false;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
cap::capx::valid(bool debug)
/* -------------------------------------------------------------------------- */
{
  struct timespec ts;
  ts.tv_sec = vtime();
  ts.tv_nsec = vtime_ns();

  if (eos::common::Timing::GetCoarseAgeInNs(&ts, 0) < 0) {
    if (debug)
      eos_static_debug("inode=%08lx client-id=%s now=%lu vtime=%lu valid=true",
                       id(), clientid().c_str(), time(NULL), vtime());

    return true;
  } else {
    if (debug)
      eos_static_debug("inode=%08lx client-id=%s now=%lu vtime=%lu valid=false",
                       id(), clientid().c_str(), time(NULL), vtime());

    return false;
  }
}

/* -------------------------------------------------------------------------- */
double
/* -------------------------------------------------------------------------- */
cap::capx::lifetime()
/* -------------------------------------------------------------------------- */
{
  struct timespec ts;
  ts.tv_sec = vtime();
  ts.tv_nsec = vtime_ns();
  double lifetime = -1.0 * (eos::common::Timing::GetCoarseAgeInNs(&ts,
                            0)) / 1000000000.0;
  eos_static_debug("inode=%08lx client-id=%s lifetime=%.02f",
                   id(), clientid().c_str(), lifetime);

  if (lifetime < 0) {
    lifetime = 0.000000001;
  }

  return lifetime;
}


/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
cap::capx::invalidate()
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper cLock(Locker());
  set_vtime(0);
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
cap::capflush(ThreadAssistant& assistant)
/* -------------------------------------------------------------------------- */
{
  while (!assistant.terminationRequested()) {
    {
      cmap capdelmap;
      cinodes capdelinodes;

      cmap flushcaps;

      // avoid keeping two mutexes
      {
	XrdSysMutexHelper capLock(capmap);
	flushcaps = capmap;
      }

      for (auto it = flushcaps.begin(); it != flushcaps.end(); ++it) {
        XrdSysMutexHelper cLock(it->second->Locker());

        // make a list of caps to timeout
        if (!it->second->valid(false)) {
          capdelmap[it->first] = it->second;

	  if (EOS_LOGS_DEBUG) {
	    eos_static_debug("expire %s", it->second->dump().c_str());
	  }

          mds->decrease_cap(it->second->id());
          capdelinodes.insert(it->second->id());
        }
      }

      {
	XrdSysMutexHelper capLock(capmap);
	for (auto it = capdelmap.begin(); it != capdelmap.end(); ++it) {
	  // remove the expired or invalidated by delete caps
	  capmap.erase(it->first);
	}
      }

      for (auto it = capdelinodes.begin(); it != capdelinodes.end(); ++it) {
        kernelcache::inval_inode(*it, false);
        // retrieve the md object and if there is no cap reference remove all child files
        EosFuse::Instance().cleanup(*it);
      }

      assistant.wait_for(std::chrono::seconds(5));
    }
  }
}

/* -------------------------------------------------------------------------- */
cap::shared_quota
/* -------------------------------------------------------------------------- */
cap::qmap::get(shared_cap cap)
{
  XrdSysMutexHelper mLock(this);
  uint64_t ino = cap->_quota().quota_inode();
  char sqid[128];
  snprintf(sqid, sizeof(sqid), "%u:%u:%16lx",
           cap->uid(),
           cap->gid(),
           ino);
  std::string qid = sqid;

  // quota information is shared per uid/gid/quota_inode triple
  if (this->count(qid)) {
    shared_quota quota = (*this)[qid];
    if (!quota->writer()) {
      eos_static_notice("updating qnode=%s volume=%lu inodes=%lu", 
			sqid, quota->volume_quota(), quota->inode_quota());
      // if there is no open file on that quota node, we can refresh from remo
      *quota = cap->_quota();
    }
    (*this)[qid] = quota;
    return quota;
  } else {
    shared_quota quota = std::make_shared<quotax>();
    *quota = cap->_quota();
    (*this)[qid] = quota;
    return quota;
  }
}
