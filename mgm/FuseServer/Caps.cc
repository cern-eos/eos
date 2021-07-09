//------------------------------------------------------------------------------
// File: FuseServer/Caps.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include <string>
#include <cstdlib>

#include "mgm/FuseServer/Caps.hh"
#include <thread>
#include <regex>

#include "common/Logging.hh"
#include "common/Timing.hh"

#include "mgm/ZMQ.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Stat.hh"

#include "namespace/interface/IView.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
FuseServer::Caps::Store(const eos::fusex::cap& ecap,
                        eos::common::VirtualIdentity* vid)
{
  gOFS->MgmStats.Add("Eosxd::int::Store", 0, 0, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::Store");
  std::lock_guard lg(mtx);
  eos_static_info("id=%lx clientid=%s authid=%s",
                  ecap.id(),
                  ecap.clientid().c_str(),
                  ecap.authid().c_str());

  // register this clientid to a given client uuid
  ClientIds()[ecap.clientuuid()].insert(ecap.clientid());

  // avoid to have multiple time entries for the same cap
  if (auto kv = mCaps.find(ecap.authid());
      kv != mCaps.end()) {
    shared_cap cap = kv->second;
   
    if ((*cap)()->id() != ecap.id()) {
      eos_static_info("got inode change for %s from %x to %x",
                      ecap.authid().c_str(), (*cap)()->id(), ecap.id());
      Remove(cap);
    }
  }

  mTimeOrderedCap.emplace(std::make_pair(ecap.vtime(),ecap.authid()));

  mClientCaps[ecap.clientid()].insert(ecap.authid());
  mClientInoCaps[ecap.clientid()][ecap.id()].insert(ecap.authid());
  shared_cap cap = std::make_shared<capx>();
  *cap = ecap;
  cap->set_vid(vid);
  mCaps[ecap.authid()] = cap;
  mInodeCaps[ecap.id()].insert(ecap.authid());
  EXEC_TIMING_END("Eosxd::int::Store");
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool
FuseServer::Caps::Imply(uint64_t md_ino,
                        FuseServer::Caps::authid_t authid,
                        FuseServer::Caps::authid_t implied_authid)
{
  eos_static_info("id=%lx authid=%s implied-authid=%s",
                  md_ino,
                  authid.c_str(),
                  implied_authid.c_str());
  shared_cap implied_cap = std::make_shared<capx>();
  shared_cap cap = GetTS(authid);

  if ((cap == nullptr) || !(*cap)()->id() || !implied_authid.length()) {
    return false;
  }

  *implied_cap = *cap;
  (*implied_cap)()->set_authid(implied_authid);
  (*implied_cap)()->set_id(md_ino);
  implied_cap->set_vid(cap->vid());
  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts, true);

  {
    size_t leasetime = 0;
    {
      eos::common::RWMutexReadLock lLock(gOFS->zMQ->gFuseServer.Client());
      leasetime = gOFS->zMQ->gFuseServer.Client().leasetime((*cap)()->clientuuid());
    }
    (*implied_cap)()->set_vtime(ts.tv_sec + (leasetime ? leasetime : 300));
    (*implied_cap)()->set_vtime_ns(ts.tv_nsec);
    // fill the three views on caps
    std::lock_guard lg(mtx);
    mTimeOrderedCap.insert(std::pair<time_t, authid_t>((*implied_cap)()->vtime(),
                           implied_authid));
    mClientCaps[(*cap)()->clientid()].insert(implied_authid);
    mClientInoCaps[(*cap)()->clientid()][(*cap)()->id()].insert(implied_authid);
    mCaps[implied_authid] = implied_cap;
    mInodeCaps[md_ino].insert(implied_authid);
  }
  return true;
}

//------------------------------------------------------------------------------
// Get shared capability - one needs to hold (at least) the read lock
//------------------------------------------------------------------------------
FuseServer::Caps::shared_cap
FuseServer::Caps::Get(const FuseServer::Caps::authid_t& id, bool make_default)
{
  if (auto kv = mCaps.find(id);
      kv != mCaps.end()) {
    return kv->second;
  }
  return make_default ? std::make_shared<capx>() : nullptr;
}

//------------------------------------------------------------------------------
// Get Broadcast Caps
//----------------------------------------------------------------------------
std::vector<std::shared_ptr<eos::mgm::FuseServer::Caps::capx>>
FuseServer::Caps::GetBroadcastCapsTS(uint64_t id,
                                     shared_cap refcap,
                                     const eos::fusex::md* mdptr,
                                     bool suppress,
                                     std::string suppress_stat_tag)
{
  std::vector<shared_cap> bccaps;
  std::vector<authid_t> auth_ids;
  size_t n_suppressed {0};
  regex_t regex;

  {
    std::lock_guard lg(mtx);
    auto ids = mInodeCaps.find(id);
    if (ids == mInodeCaps.end()) {
      return bccaps;
    }

    auth_ids.reserve(ids->second.size());
    std::copy(ids->second.begin(),
              ids->second.end(),
              std::back_inserter(auth_ids));
  }

  if (suppress) {
    // audience check
    int audience = gOFS->zMQ->gFuseServer.Client().BroadCastMaxAudience();
    std::string match =
      gOFS->zMQ->gFuseServer.Client().BroadCastAudienceSuppressMatch();

    if (audience && ((auth_ids.size() > (size_t)audience))) {
      if (regcomp(&regex, match.c_str(), REG_ICASE | REG_EXTENDED | REG_NOSUB)) {
        suppress = false;
        eos_static_err("msg=\"broadcast audience suppress match not valid regex\" regex=\"%s\"",
                       match.c_str());
      }
    } else {
      suppress = false;
    }
  }

  eos_static_debug("id=%lx mInodeCaps.count=1", id);
  for (const auto& it: auth_ids) {
    // TODO: do we need to debug log mCaps.found
    // eos_static_debug("mCaps.found=1")
    shared_cap cap = GetTS(it);
    if (!(*cap)()->id()) {
      continue;
    }

    if ((*refcap)() && mdptr) {
      // skip our own cap!
      if ((*cap)()->authid() == mdptr->authid()) {
        continue;
      }

      // skip identical client mounts!
      if ((*cap)()->clientuuid() == (*refcap)()->clientuuid()) {
        continue;
      }

      // skip same source
      if ((*cap)()->clientuuid() == mdptr->clientuuid()) {
        continue;
      }
    }

    if (suppress) {
      if (regexec(&regex, (*cap)()->clientid().c_str(), 0, NULL, 0) != REG_NOMATCH) {
        n_suppressed++;
        continue;
      }
    }

    bccaps.emplace_back(std::move(cap));
  }

  if (n_suppressed && !suppress_stat_tag.empty()) {
    gOFS->MgmStats.Add(suppress_stat_tag.c_str(), 0, 0, n_suppressed);
  }

  return bccaps;
}

//------------------------------------------------------------------------------
// Broadcast release for id from external
//----------------------------------------------------------------------------
int
FuseServer::Caps::BroadcastReleaseFromExternal(uint64_t id)

{
  gOFS->MgmStats.Add("Eosxd::int::BcReleaseExt", 0, 0, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::BcReleaseExt");

  auto bccaps = GetBroadcastCapsTS(id);

  for (auto it : bccaps) {
    eos_static_debug("ReleaseCAP id %#lx clientid %s", (*it)()->clientid().c_str());
    gOFS->zMQ->gFuseServer.Client().ReleaseCAP((uint64_t)(*it)()->id(),
                                               (*it)()->clientuuid(),
                                               (*it)()->clientid());
    errno = 0 ; // seems that ZMQ function might set errno
  }

  EXEC_TIMING_END("Eosxd::int::BcReleaseExt");
  return 0;
}

/*----------------------------------------------------------------------------*/
int
FuseServer::Caps::BroadcastRefreshFromExternal(uint64_t id, uint64_t pid)
/*----------------------------------------------------------------------------*/
{
  gOFS->MgmStats.Add("Eosxd::int::BcRefreshExt", 0, 0, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::BcRefreshExt");
  // broad-cast refresh for a given inode
  eos_static_info("id=%lx pid=%lx", id, pid);
  auto bccaps = GetBroadcastCapsTS(pid, nullptr, nullptr, true, "Eosxd::int::BcRefreshExtSup");

  for (auto it : bccaps) {
    gOFS->zMQ->gFuseServer.Client().RefreshEntry((uint64_t) id,
                                                 (*it)()->clientuuid(),
                                                 (*it)()->clientid());
    errno = 0 ; // seems that ZMQ function might set errno
  }

  EXEC_TIMING_END("Eosxd::int::BcRefreshExt");
  return 0;
}

int
FuseServer::Caps::BroadcastRelease(const eos::fusex::md& md)
{
  gOFS->MgmStats.Add("Eosxd::int::BcRelease", 0, 0, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::BcRelease");
  FuseServer::Caps::shared_cap refcap = GetTS(md.authid());
  eos_static_info("id=%lx/%lx clientid=%s clientuuid=%s authid=%s",
                  (*refcap)()->id(),
                  md.md_pino(),
                  (*refcap)()->clientid().c_str(),
                  (*refcap)()->clientuuid().c_str(),
                  (*refcap)()->authid().c_str());
  uint64_t md_pino = (*refcap)()->id();

  if (!md_pino) {
    md_pino = md.md_pino();
  }

  auto bccaps = GetBroadcastCapsTS(md_pino, refcap, &md);

  for (auto it : bccaps) {
    gOFS->zMQ->gFuseServer.Client().ReleaseCAP((uint64_t) (*it)()->id(),
                                               (*it)()->clientuuid(),
                                               (*it)()->clientid());
    errno = 0 ;
  }

  EXEC_TIMING_END("Eosxd::int::BcRelease");
  return 0;
}

/*----------------------------------------------------------------------------*/
int
FuseServer::Caps::BroadcastDeletionFromExternal(uint64_t id,
    const std::string& name)
/*----------------------------------------------------------------------------*/
{
  gOFS->MgmStats.Add("Eosxd::int::BcDeletionExt", 0, 0, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::BcDeletionExt");
  eos_static_info("id=%lx name=%s", id, name.c_str());
  // broad-cast deletion for a given name in a container
  auto bccaps = GetBroadcastCapsTS(id);

  for (auto it : bccaps) {
    gOFS->zMQ->gFuseServer.Client().DeleteEntry((uint64_t) (*it)()->id(),
                                                (*it)()->clientuuid(),
                                                (*it)()->clientid(),
        name);
    errno = 0 ; // seems that ZMQ function might set errno
  }

  EXEC_TIMING_END("Eosxd::int::BcDeletionExt");
  return 0;
}

/*----------------------------------------------------------------------------*/
int
FuseServer::Caps::BroadcastDeletion(uint64_t id, const eos::fusex::md& md,
                                    const std::string& name)
/*----------------------------------------------------------------------------*/
{
  gOFS->MgmStats.Add("Eosxd::int::BcDeletion", 0, 0, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::BcDeletion");
  eos_static_info("id=%lx name=%s", id, name.c_str());
  FuseServer::Caps::shared_cap refcap = GetTS(md.authid());
  auto bccaps = GetBroadcastCapsTS((*refcap)()->id(), refcap, &md);

  for (auto it : bccaps) {
    gOFS->zMQ->gFuseServer.Client().DeleteEntry((uint64_t) (*it)()->id(),
                                                (*it)()->clientuuid(),
                                                (*it)()->clientid(),
                                                name);
    errno = 0;
  }

  EXEC_TIMING_END("Eosxd::int::BcDeletion");
  return 0;
}

/*----------------------------------------------------------------------------*/
int
FuseServer::Caps::BroadcastRefresh(uint64_t inode,
                                   const eos::fusex::md& md,
                                   uint64_t parent_inode)
/*----------------------------------------------------------------------------*/
{
  gOFS->MgmStats.Add("Eosxd::int::BcRefresh", 0, 0, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::BcRefresh");
  eos_static_info("id=%lx parent=%lx", inode, parent_inode);
  size_t n_suppressed = 0;
  std::vector<authid_t> auth_ids;
  FuseServer::Caps::shared_cap refcap {nullptr};

  {
    std::lock_guard lg(mtx);
    refcap = Get(md.authid(), false);

    auto kv = mInodeCaps.find(parent_inode);

    if (kv == mInodeCaps.end()) {
      EXEC_TIMING_END("Eosxd::int::BcRefresh");
      return 0; // nothing to process here
    }

    auth_ids.reserve(kv->second.size());
    std::copy(kv->second.begin(),
              kv->second.end(),
              std::back_inserter(auth_ids));
  }

  bool suppress_audience = false;
  regex_t regex;
  // audience check
  int audience = gOFS->zMQ->gFuseServer.Client().BroadCastMaxAudience();
  std::string match =
    gOFS->zMQ->gFuseServer.Client().BroadCastAudienceSuppressMatch();

  if (audience && ((auth_ids.size() > (size_t)audience))) {
    suppress_audience = true;

    if (regcomp(&regex, match.c_str(), REG_ICASE | REG_EXTENDED | REG_NOSUB)) {
      suppress_audience = false;
      eos_static_err("msg=\"broadcast audience suppress match not valid regex\" regex=\"%s\"",
                     match.c_str());
    }
  }

  for (const auto& it: auth_ids) {
    shared_cap cap = GetTS(it);
    // avoid processing if the cap doesn't exist
    if (!(*cap)()->id()) {
      continue;
    }

    // skip identical client mounts!
    if (refcap && (*cap)()->clientuuid() == (*refcap)()->clientuuid()) {
      continue;
    }

    // skip same source
    if ((*cap)()->clientuuid() == md.clientuuid()) {
      continue;
    }

    if (suppress_audience) {
      if (regexec(&regex, (*cap)()->clientid().c_str(), 0, NULL, 0) != REG_NOMATCH) {
        n_suppressed++;
        continue;
      }
    }

    gOFS->zMQ->gFuseServer.Client().RefreshEntry((uint64_t) inode,
                                                 (*cap)()->clientuuid(),
                                                 (*cap)()->clientid());
    errno = 0;
  }

  if (n_suppressed) {
    gOFS->MgmStats.Add("Eosxd::int::BcRefreshSup", 0, 0, n_suppressed);
  }

  EXEC_TIMING_END("Eosxd::int::BcRefresh");
  return 0;
}


int
FuseServer::Caps::BroadcastCap(shared_cap cap)
{
  if (cap && (*cap)()->id()) {
    (void) gOFS->zMQ->gFuseServer.Client().SendCAP(cap);
  }

  return -1;
}

int
FuseServer::Caps::BroadcastMD(const eos::fusex::md& md,
                              uint64_t md_ino,
                              uint64_t md_pino,
                              uint64_t clock,
                              struct timespec& p_mtime)
{
  gOFS->MgmStats.Add("Eosxd::int::BcMD", 0, 0, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::BcMD");
  size_t n_suppressed = 0;
  std::vector<shared_cap> bccaps;
  std::unordered_set<std::string> clients_sent;
  std::vector<authid_t> auth_ids;
  FuseServer::Caps::shared_cap refcap {nullptr};

  {
    std::lock_guard lg(mtx);
    refcap = Get(md.authid(), false);
    if (refcap == nullptr) {
      EXEC_TIMING_END("Eosxd::int::BcMD");
      return 0;
    }

    if (refcap == nullptr) {
      EXEC_TIMING_END("Eosxd::int::BcMD");
      return 0;
    }

    auto kv = mInodeCaps.find(md_pino);

    if (kv == mInodeCaps.end()) {
      EXEC_TIMING_END("Eosxd::int::BcMD");
      return 0; // nothing to process here
    }

    auth_ids.reserve(kv->second.size());
    std::copy(kv->second.begin(),
              kv->second.end(),
              std::back_inserter(auth_ids));
  }

  if (refcap != nullptr) {
    eos_static_info("id=%lx/%lx clientid=%s clientuuid=%s authid=%s",
                    (*refcap)()->id(), md_pino, (*refcap)()->clientid().c_str(),
                    (*refcap)()->clientuuid().c_str(), (*refcap)()->authid().c_str());
  }

  bool suppress_audience = false;
  regex_t regex;
  // audience check
  int audience = gOFS->zMQ->gFuseServer.Client().BroadCastMaxAudience();
  std::string match =
    gOFS->zMQ->gFuseServer.Client().BroadCastAudienceSuppressMatch();

  if (audience && (auth_ids.size() > (size_t) audience)) {
    suppress_audience = true;

    if (regcomp(&regex, match.c_str(), REG_ICASE | REG_EXTENDED | REG_NOSUB)) {
      suppress_audience = false;
      eos_static_err("msg=\"broadcast audience suppress match not valid regex\" regex=\"%s\"",
                     match.c_str());
    }
  }

  for (const auto& it: auth_ids) {
    shared_cap cap = GetTS(it, false);
    // avoid processing if the cap doesn't exist
    if (!cap) {
      continue;
    }
    
    // avoid processing if the cap doesn't exist or to a sent client
    if (!(*cap)()->id() || clients_sent.count((*cap)()->clientuuid())) {
      continue;
    }

    // skip identical client mounts, the have it anyway!
    if (refcap && (*cap)()->clientuuid() == (*refcap)()->clientuuid()) {
      continue;
    }

    // skip same source
    if ((*cap)()->clientuuid() == md.clientuuid()) {
      continue;
    }

    if (suppress_audience) {
      if (regexec(&regex, (*cap)()->clientid().c_str(), 0, NULL, 0) != REG_NOMATCH) {
        n_suppressed++;
        continue;
      }
    }

    eos_static_debug("id=%lx clientid=%s clientuuid=%s authid=%s",
                    (*cap)()->id(),
                    (*cap)()->clientid().c_str(),
                    (*cap)()->clientuuid().c_str(),
                    (*cap)()->authid().c_str());

    // make sure we sent the update only once to each client, eveh if this
    // one has many caps
    clients_sent.emplace((*cap)()->clientuuid());
    gOFS->zMQ->gFuseServer.Client().SendMD(md,
                                           (*cap)()->clientuuid(),
                                           (*cap)()->clientid(),
                                           md_ino,
                                           md_pino,
                                           clock,
                                           p_mtime);
    errno = 0; // avoid errno clobbering from ZMQ
  }

  if (n_suppressed) {
    gOFS->MgmStats.Add("Eosxd::int::BcMDSup", 0, 0, n_suppressed);
  }

  EXEC_TIMING_END("Eosxd::int::BcMD");
  return 0;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
std::string
FuseServer::Caps::Print(const std::string& option,
                        const std::string& filter)
{
  std::string out;
  std::string astring;
  uint64_t now = (uint64_t) time(NULL);
  eos::common::RWMutexReadLock lock;

  if (option == "p") {
    lock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
  }

  eos_static_info("option=%s string=%s", option.c_str(), filter.c_str());
  regex_t regex;

  if (filter.size() &&
      regcomp(&regex, filter.c_str(), REG_ICASE | REG_EXTENDED | REG_NOSUB)) {
    out = "error: illegal regular expression ;";
    out += filter.c_str();
    out += "'\n";
    return out;
  }

  if (option == "t") {
    std::lock_guard lg(mtx);
    // print by time order
    for (auto it = mTimeOrderedCap.begin(); it != mTimeOrderedCap.end();) {
      if (!mCaps.count(it->second)) {
        it = mTimeOrderedCap.erase(it);
        continue;
      }

      char ahex[256];
      shared_cap cap = mCaps[it->second];
      snprintf(ahex, sizeof(ahex), "%016lx", (unsigned long) (*cap)()->id());
      std::string match = "";
      match += "# i:";
      match += ahex;
      match += " a:";
      match += (*cap)()->authid();
      match += " c:";
      match += (*cap)()->clientid();
      match += " u:";
      match += (*cap)()->clientuuid();
      match += " m:";
      snprintf(ahex, sizeof(ahex), "%08lx", (unsigned long) (*cap)()->mode());
      match += ahex;
      match += " v:";

      if (((*cap)()->vtime() - now) >  0) {
        match += eos::common::StringConversion::GetSizeString(astring,
                                                              (unsigned long long) (*cap)()->vtime() - now);
      } else {
        match += eos::common::StringConversion::GetSizeString(astring,
                                                              (unsigned long long) 0);
      }

      match += "\n";

      if (filter.size() &&
          (regexec(&regex, match.c_str(), 0, NULL, 0) == REG_NOMATCH)) {
        it++;
        continue;
      }

      out += match.c_str();
      ++it;
    }
  }

  if (option == "i") {
    // print by inode
    for (auto it = mInodeCaps.begin(); it != mInodeCaps.end(); ++it) {
      char ahex[256];
      snprintf(ahex, sizeof(ahex), "%016lx", (unsigned long) it->first);

      if (filter.size() && (regexec(&regex, ahex, 0, NULL, 0) == REG_NOMATCH)) {
        continue;
      }

      out += "# i:";
      out += ahex;
      out += "\n";

      for (auto sit = it->second.begin(); sit != it->second.end(); ++sit) {
        out += "___ a:";
        out += *sit;

        if (!mCaps.count(*sit)) {
          out += " c:<unfound> u:<unfound> m:<unfound> v:<unfound>\n";
        } else {
          shared_cap cap = mCaps[*sit];
          out += " c:";
          out += (*cap)()->clientid();
          out += " u:";
          out += (*cap)()->clientuuid();
          out += " m:";
          snprintf(ahex, sizeof(ahex), "%016lx", (unsigned long) (*cap)()->mode());
          out += ahex;
          out += " v:";
          out += eos::common::StringConversion::GetSizeString(astring,
                                                              (unsigned long long) (*cap)()->vtime() - now);
          out += "\n";
        }
      }
    }
  }

  if (option == "p") {
    // print by inode
    for (auto it = mInodeCaps.begin(); it != mInodeCaps.end(); ++it) {
      std::string spath;

      try {
        if (eos::common::FileId::IsFileInode(it->first)) {
          std::shared_ptr<eos::IFileMD> fmd =
            gOFS->eosFileService->getFileMD(eos::common::FileId::InodeToFid(it->first));
          spath = "f:";
          spath += gOFS->eosView->getUri(fmd.get());
        } else {
          std::shared_ptr<eos::IContainerMD> cmd =
            gOFS->eosDirectoryService->getContainerMD(it->first);
          spath = "d:";
          spath += gOFS->eosView->getUri(cmd.get());
        }
      } catch (eos::MDException& e) {
        spath = "<unknown>";
      }

      if (filter.size() &&
          (regexec(&regex, spath.c_str(), 0, NULL, 0) == REG_NOMATCH)) {
        continue;
      }

      char apath[1024];
      out += "# ";
      snprintf(apath, sizeof(apath), "%-80s", spath.c_str());
      out += apath;
      out += "\n";

      for (auto sit = it->second.begin(); sit != it->second.end(); ++sit) {
        out += "___ a:";
        out += *sit;

        if (!mCaps.count(*sit)) {
          out += " c:<unfound> u:<unfound> m:<unfound> v:<unfound>\n";
        } else {
          shared_cap cap = mCaps[*sit];
          out += " c:";
          out += (*cap)()->clientid();
          out += " u:";
          out += (*cap)()->clientuuid();
          out += " m:";
          char ahex[20];
          snprintf(ahex, sizeof(ahex), "%016lx", (unsigned long) (*cap)()->mode());
          out += ahex;
          out += " v:";
          out += eos::common::StringConversion::GetSizeString(astring,
                                                              (unsigned long long) (*cap)()->vtime() - now);
          out += "\n";
        }
      }
    }
  }

  return out;
}

//------------------------------------------------------------------------------
// Delete capabilities corresponding to an inode
//------------------------------------------------------------------------------
int
FuseServer::Caps::Delete(uint64_t md_ino)
{
  // Hash functor used for storing client_set_t::iterator objects in an
  // unordered set
  struct iter_client_set_hash {
    size_t operator()(client_set_t::iterator it) const
    {
      return std::hash<std::string>()(it->first);
    }
  };
  std::unordered_set<client_set_t::iterator, iter_client_set_hash>
  to_del_client_caps;

  std::lock_guard lg(mtx);
  const auto it_inode_caps = mInodeCaps.find(md_ino);

  if (it_inode_caps == mInodeCaps.end()) {
    return ENONET;
  }

  const authid_set_t& set_authid = it_inode_caps->second;

  for (auto it_client_caps = mClientCaps.begin();
       it_client_caps != mClientCaps.end(); ++it_client_caps) {
    for (const auto& authid : set_authid) {
      // erase authid from the client set
      it_client_caps->second.erase(authid);

      if (it_client_caps->second.empty()) {
        to_del_client_caps.insert(it_client_caps);
      }
    }
  }

  for (auto& it_elem : to_del_client_caps) {
    mClientCaps.erase(it_elem);
  }

  for (const auto& authid : set_authid) {
    const auto it_caps = mCaps.find(authid);

    if (it_caps != mCaps.end()) {
      const std::string client_id = (*it_caps->second)()->clientid();
      auto it_cli_inocaps = mClientInoCaps.find(client_id);

      if (it_cli_inocaps != mClientInoCaps.end()) {
        it_cli_inocaps->second.erase(md_ino);

        if (it_cli_inocaps->second.size() == 0) {
          mClientInoCaps.erase(it_cli_inocaps);
        }
      }

      mCaps.erase(it_caps);
    }
  }

  mInodeCaps.erase(it_inode_caps);
  return 0;
}

EOSMGMNAMESPACE_END
