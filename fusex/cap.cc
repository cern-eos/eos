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

/* -------------------------------------------------------------------------- */
cap::cap()
/* -------------------------------------------------------------------------- */
{
}

/* -------------------------------------------------------------------------- */
cap::~cap()
/* -------------------------------------------------------------------------- */
{

}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
cap::capx::dump()
/* -------------------------------------------------------------------------- */
{
  char sout[16384];
  snprintf(sout, sizeof (sout),
           " ");
  return sout;
}

/* -------------------------------------------------------------------------- */
cap::shared_cap
/* -------------------------------------------------------------------------- */
cap::get(fuse_req_t req,
         fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  std::string cid = cap::capx::capid(req, ino);

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
    cap->set_id(ino);
    cap->set_uid(fuse_req_ctx(req)->uid);
    cap->set_gid(fuse_req_ctx(req)->gid);
    return cap;
  }
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
  shared_cap cap = get(req, ino);

  if (!cap->satisfy(mode) ||
      !cap->valid())
  {
    cap->renew(mode);
  }

  return cap;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
cap::capx::satisfy(mode_t mode)
/* -------------------------------------------------------------------------- */
{
  if ( ((mode & this->mode()) && mode ) == mode)
    return true;
  return false;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
cap::capx::valid()
/* -------------------------------------------------------------------------- */
{
  struct timespec ts;
  ts.tv_sec = vtime();
  ts.tv_nsec = vtime_ns();

  if (eos::common::Timing::GetCoarseAgeInNs(&ts, 0) < 0)
    return true;
  else
    return false;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
cap::capx::renew(mode_t mode)
/* -------------------------------------------------------------------------- */
{
  // TODOD: call upstream to make sure, now fake for 5s 
  struct timespec ts;
  eos::common::Timing::GetTimeSpec (ts, true);
  set_vtime(ts.tv_sec + 5);
  set_vtime_ns(ts.tv_nsec);
  set_mode(mode);
}
