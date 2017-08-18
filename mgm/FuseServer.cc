// ----------------------------------------------------------------------
// File: FuseServer.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

/*----------------------------------------------------------------------------*/

#include "mgm/FuseServer.hh"
#include "mgm/Acl.hh"
#include "mgm/Policy.hh"
#include <thread>
#include <regex.h>
#include "common/Logging.hh"
#include "XrdMgmOfs.hh"

EOSMGMNAMESPACE_BEGIN

#define D_OK 8     // delete
#define M_OK 16    // chmod
#define C_OK 32    // chown
#define SA_OK 64   // set xattr
#define U_OK 128   // can update

/*----------------------------------------------------------------------------*/
FuseServer::FuseServer()
{
  eos_static_info("msg=\"starting fuse server\"");
  std::thread monitorthread(&FuseServer::Clients::MonitorHeartBeat, &(this->mClients));
  monitorthread.detach();
  std::thread capthread(&FuseServer::MonitorCaps, this);
  capthread.detach();
}

/*----------------------------------------------------------------------------*/
FuseServer::~FuseServer()
/*----------------------------------------------------------------------------*/
{
  shutdown();
}

/*----------------------------------------------------------------------------*/
void
FuseServer::shutdown()
/*----------------------------------------------------------------------------*/
{
  Clients().terminate();
  terminate();
}

std::string
FuseServer::dump_message(const google::protobuf::Message & message)
{
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string jsonstring;
  google::protobuf::util::MessageToJsonString( message, &jsonstring, options);
  return jsonstring;
}

/*----------------------------------------------------------------------------*/
void
FuseServer::Clients::MonitorHeartBeat()
/*----------------------------------------------------------------------------*/
{
  XrdSysTimer sleeper;
  eos_static_info("msg=\"starting fusex heart beat thread\"");

  while (1)
  {
    client_uuid_t evictmap;
    {
      XrdSysMutexHelper lLock(this);

      struct timespec tsnow;
      eos::common::Timing::GetTimeSpec(tsnow);
      for (auto it = map().begin(); it != map().end(); ++it)
      {
        double last_heartbeat = tsnow.tv_sec - it->second.heartbeat().clock() + (((int64_t) tsnow.tv_nsec - (int64_t) it->second.heartbeat().clock_ns())*1.0 / 1000000000.0);
        if (last_heartbeat > mHeartBeatWindow)
        {
          if (last_heartbeat > mHeartBeatEvictWindow)
          {
            evictmap[it->second.heartbeat().uuid()] = it->first;
            it->second.set_state(Client::EVICTED);
            gOFS->zMQ->gFuseServer.Locks().dropLocks(it->first);
          }
          else
          {
            it->second.set_state(Client::OFFLINE);
          }
        }
        else
        {
          it->second.set_state(Client::ONLINE);
        }
      }
      // delete clients to be evicted
      for (auto it = evictmap.begin(); it != evictmap.end(); ++it)
      {
        mMap.erase(it->second);
        mUUIDView.erase(it->first);
      }
    }
    sleeper.Snooze(1);

    if (should_terminate())
      break;
  }
  return ;
}

/*----------------------------------------------------------------------------*/
bool
FuseServer::Clients::Dispatch(const std::string identity,
                              const eos::fusex::heartbeat& hb)
/*----------------------------------------------------------------------------*/
{
  bool rc=true;
  XrdSysMutexHelper lLock(this);
  if (this->map().count(identity))
    rc = false;

  (this->map())[identity].heartbeat() = hb;
  (this->uuidview())[hb.uuid()] = identity;

  return rc;
}

/*----------------------------------------------------------------------------*/
void
FuseServer::MonitorCaps()
/*----------------------------------------------------------------------------*/
{
  XrdSysTimer sleeper;
  eos_static_info("msg=\"starting fusex monitor caps thread\"");

  while (1)
  {
    do
    {
      if ( Cap().expire() )
      {
        Cap().pop();
      }
      else
      {
        break;
      }
    }
    while (1);

    sleeper.Snooze(1);

    if (should_terminate())
      break;
  }
  return ;
}

/*----------------------------------------------------------------------------*/
void
FuseServer::Print(std::string& out, std::string options, bool monitoring)
/*----------------------------------------------------------------------------*/
{


  if (options.find("c"))
  {
    Client().Print(out, "", monitoring);
  }
}

/*----------------------------------------------------------------------------*/
void
FuseServer::Clients::Print(std::string& out, std::string options, bool monitoring)
/*----------------------------------------------------------------------------*/
{
  XrdSysMutexHelper lLock(this);

  struct timespec tsnow;
  eos::common::Timing::GetTimeSpec(tsnow);
  for (auto it = this->map().begin(); it != this->map().end(); ++it)
  {
    char formatline[4096];
    if (!monitoring)
    {
      snprintf(formatline, sizeof (formatline), "client : %-8s %32s %-8s %-8s %s %.02f %.02f %36s \n",
               it->second.heartbeat().name().c_str(),
               it->second.heartbeat().host().c_str(),
               it->second.heartbeat().version().c_str(),
               it->second.status[it->second.state()],
               eos::common::Timing::utctime(it->second.heartbeat().starttime()).c_str(),
               tsnow.tv_sec - it->second.heartbeat().clock() +
               (((int64_t) tsnow.tv_nsec -
               (int64_t) it->second.heartbeat().clock_ns())*1.0 / 1000000000.0),
               it->second.heartbeat().delta()*1000,
               it->second.heartbeat().uuid().c_str()
               );

      out += formatline;

      std::map<uint64_t, std::set < pid_t>> rlocks;
      std::map<uint64_t, std::set < pid_t>> wlocks;

      gOFS->zMQ->gFuseServer.Locks().lsLocks(it->first, rlocks, wlocks);


      for (auto rit = rlocks.begin(); rit != rlocks.end(); ++rit)
      {
        if (rit->second.size())
        {
          out += "\n";
          snprintf(formatline, sizeof (formatline), "      t:rlock i:%016lx", rit->first);
          out += formatline;
          std::string pidlocks;
          for (auto pit = rit->second.begin(); pit != rit->second.end(); ++pit)
          {
            if (pidlocks.length())
              pidlocks += ",";
            pidlocks += *pit;
          }
          out += pidlocks;
        }
      }

      for (auto wit = wlocks.begin(); wit != wlocks.end(); ++wit)
      {
        if (wit->second.size())
        {
          out += "\n";
          snprintf(formatline, sizeof (formatline), "      t:wlock i:%016lx", wit->first);
          out += formatline;
          std::string pidlocks;
          for (auto pit = wit->second.begin(); pit != wit->second.end(); ++pit)
          {
            if (pidlocks.length())
              pidlocks += ",";
            pidlocks += *pit;
          }
          out += pidlocks;
        }
      }
    }
    else
    {
      snprintf(formatline, sizeof (formatline) - 1, "_");
    }
  }
}

/*----------------------------------------------------------------------------*/
int
FuseServer::Clients::Evict(std::string& uuid, std::string reason)
/*----------------------------------------------------------------------------*/
{
  // prepare eviction message 
  eos::fusex::response rsp;
  rsp.set_type(rsp.EVICT);
  rsp.mutable_evict_()->set_reason(reason);

  std::string rspstream;
  rsp.SerializeToString(&rspstream);

  XrdSysMutexHelper lLock(this);

  if (!mUUIDView.count(uuid))
  {
    return ENOENT;
  }

  std::string id = mUUIDView[uuid];

  eos_static_info("msg=\"evicting client\" uuid=%s name=%s",
                  uuid.c_str(), id.c_str());

  gOFS->zMQ->task->reply(id, rspstream);
  return 0;
}

/*----------------------------------------------------------------------------*/
int
FuseServer::Clients::Dropcaps(const std::string& uuid, std::string& out)
/*----------------------------------------------------------------------------*/
{
  XrdSysMutexHelper lock(gOFS->zMQ->gFuseServer.Cap());

  std::string id = mUUIDView[uuid];
  out += " dropping caps of '";
  out += uuid;
  out += "' : ";

  if (!mUUIDView.count(uuid))
  {
    return ENOENT;
  }

  for (auto it = gOFS->zMQ->gFuseServer.Cap().InodeCaps().begin();
       it != gOFS->zMQ->gFuseServer.Cap().InodeCaps().end(); ++it)
  {
    std::set<FuseServer::Caps::shared_cap> cap2delete;

    for (auto sit = it->second.begin(); sit != it->second.end(); ++sit)
    {
      if (gOFS->zMQ->gFuseServer.Cap().GetCaps().count(*sit))
      {
        FuseServer::Caps::shared_cap cap = gOFS->zMQ->gFuseServer.Cap().GetCaps()[*sit];
        if (cap->clientuuid() == uuid)
        {
          cap2delete.insert(cap);
          out += "\n ";
          char ahex[20];
          snprintf(ahex, sizeof (ahex), "%016lx", (unsigned long) cap->id());
          std::string match = "";
          match += "# i:";
          match += ahex;
          match += " a:";
          match += cap->authid();
          out += match;
        }
      }
    }

    for (auto scap = cap2delete.begin(); scap != cap2delete.end(); ++scap)
    {
      gOFS->zMQ->gFuseServer.Client().ReleaseCAP((uint64_t) (*scap)->id(),
                                                 (*scap)->clientuuid(),
                                                 (*scap)->clientid());

      eos_static_info("erasing %llx %s %s", (*scap)->id(), (*scap)->clientid().c_str(), (*scap)->authid().c_str());
      // erase cap by auth id
      gOFS->zMQ->gFuseServer.Cap().GetCaps().erase((*scap)->authid());
      // erase cap by inode
      gOFS->zMQ->gFuseServer.Cap().InodeCaps()[(*scap)->id()].erase((*scap)->authid());
      if (!gOFS->zMQ->gFuseServer.Cap().InodeCaps()[(*scap)->id()].size())
        gOFS->zMQ->gFuseServer.Cap().InodeCaps().erase((*scap)->id());

      gOFS->zMQ->gFuseServer.Cap().ClientCaps()[(*scap)->clientid()].erase((*scap)->authid());
      if (!gOFS->zMQ->gFuseServer.Cap().ClientCaps()[(*scap)->clientid()].size())
      {
        gOFS->zMQ->gFuseServer.Cap().ClientCaps().erase( (*scap)->clientid() );
      }
      gOFS->zMQ->gFuseServer.Cap().ClientCaps()[(*scap)->clientid()].insert((*scap)->authid());
    }
    if (!cap2delete.size())
    {
      out += " <no caps held>\n";
    }
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
int
FuseServer::Clients::ReleaseCAP(uint64_t md_ino,
                                const std::string& uuid,
                                const std::string& clientid
                                )
/*----------------------------------------------------------------------------*/
{
  // prepare eviction message 
  eos::fusex::response rsp;
  rsp.set_type(rsp.LEASE);
  rsp.mutable_lease_()->set_type(eos::fusex::lease::RELEASECAP);
  rsp.mutable_lease_()->set_md_ino(md_ino);
  rsp.mutable_lease_()->set_clientid(clientid);

  std::string rspstream;
  rsp.SerializeToString(&rspstream);

  XrdSysMutexHelper lLock(this);

  if (!mUUIDView.count(uuid))
  {
    return ENOENT;
  }

  std::string id = mUUIDView[uuid];

  eos_static_info("msg=\"asking cap release\" uuid=%s clientid=%s id=%lx",
                  uuid.c_str(), clientid.c_str(), md_ino);

  gOFS->zMQ->task->reply(id, rspstream);
  return 0;
}

/*----------------------------------------------------------------------------*/
int
FuseServer::Clients::SendMD( const eos::fusex::md &md,
			     const std::string& uuid,
			     const std::string& clientid
			     )
/*----------------------------------------------------------------------------*/
{
  // prepare update message 
  eos::fusex::response rsp;
  rsp.set_type(rsp.MD);
  *(rsp.mutable_md_()) = md; 
  rsp.mutable_md_()->set_type(eos::fusex::md::MD);

  std::string rspstream;
  rsp.SerializeToString(&rspstream);

  XrdSysMutexHelper lLock(this);

  if (!mUUIDView.count(uuid))
  {
    return ENOENT;
  }

  std::string id = mUUIDView[uuid];

  eos_static_info("msg=\"sending md update\" uuid=%s clientid=%s id=%lx",
                  uuid.c_str(), clientid.c_str(), md.md_ino());

  gOFS->zMQ->task->reply(id, rspstream);
  return 0;
}

/*----------------------------------------------------------------------------*/
void
FuseServer::Clients::HandleStatistics(const std::string identity, const eos::fusex::statistics& stats)
/*----------------------------------------------------------------------------*/
{
  eos_static_debug("");
}

/*----------------------------------------------------------------------------*/
void
FuseServer::Caps::Store(const eos::fusex::cap &ecap, eos::common::Mapping::VirtualIdentity* vid)
/*----------------------------------------------------------------------------*/
{
  XrdSysMutexHelper lock(this);

  eos_static_info("id=%lx clientid=%s authid=%s",
                  ecap.id(),
                  ecap.clientid().c_str(),
                  ecap.authid().c_str());

  // fill the three views on caps
  mTimeOrderedCap.push_back(ecap.authid());
  mClientCaps[ecap.clientid()].insert(ecap.authid());

  shared_cap cap = std::make_shared<capx>();
  *cap = ecap;
  cap->set_vid(vid);
  mCaps[ecap.authid()] = cap;
  mInodeCaps[ecap.id()].insert(ecap.authid());
}

/*----------------------------------------------------------------------------*/
bool
FuseServer::Caps::Imply(uint64_t md_ino,
                        FuseServer::Caps::authid_t authid,
                        FuseServer::Caps::authid_t implied_authid)
/*----------------------------------------------------------------------------*/
{
  eos_static_info("id=%lx authid=%s implied-authid=%s",
                  md_ino,
                  authid.c_str(),
                  implied_authid.c_str());

  shared_cap implied_cap = std::make_shared<capx>();
  shared_cap cap = Get(authid);

  if (!cap->id() || !implied_authid.length())
  {
    return false;
  }

  *implied_cap = *cap;

  implied_cap->set_authid(implied_authid);
  implied_cap->set_id(md_ino);
  implied_cap->set_vid(cap->vid());


  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts, true);
  implied_cap->set_vtime(ts.tv_sec + 300);
  implied_cap->set_vtime_ns(ts.tv_nsec);

  // fill the three views on caps
  mTimeOrderedCap.push_back(implied_authid);
  mClientCaps[cap->clientid()].insert(implied_authid);

  mCaps[implied_authid] = implied_cap;
  mInodeCaps[md_ino].insert(implied_authid);

  return true;
}

/*----------------------------------------------------------------------------*/
FuseServer::Caps::shared_cap
FuseServer::Caps::Get(FuseServer::Caps::authid_t id)
/*----------------------------------------------------------------------------*/
{
  XrdSysMutexHelper lock(this);

  if (mCaps.count(id))
  {
    return mCaps[id];
  }
  else
  {
    return std::make_shared<capx>();
  }
}

int
FuseServer::Caps::BroadcastReleaseFromExternal(uint64_t id)
{
  // broad-cast release for a given inode
  XrdSysMutexHelper lock(this);

  eos_static_info("id=%lx ",
                  id);

  if (mInodeCaps.count(id))
  {
    std::set<std::string> deletioncaps;

    for (auto it = mInodeCaps[id].begin();
         it != mInodeCaps[id].end(); ++it)
    {
      shared_cap cap;
      // loop over all caps for that inode
      if (mCaps.count(*it))
        cap = mCaps[*it];
      else
        continue;

      if (cap->id())
      {
        deletioncaps.insert(*it);
        gOFS->zMQ->gFuseServer.Client().ReleaseCAP((uint64_t) cap->id(),
                                                   cap->clientuuid(),
                                                   cap->clientid());
        errno = 0 ; // seems that ZMQ function might set errno
      }
    }

    for (auto it = deletioncaps.begin(); it != deletioncaps.end(); ++it)
    {
      eos_static_info("auto-remove-cap authid=%s", it->c_str());
      mInodeCaps[id].erase(*it);
    }
  }
  return 0;
}

int
FuseServer::Caps::BroadcastRelease(const eos::fusex::md &md)
{
  FuseServer::Caps::shared_cap refcap = Get(md.authid());

  XrdSysMutexHelper lock(this);

  eos_static_info("id=%lx clientid=%s clientuuid=%s authid=%s",
                  refcap->id(),
                  refcap->clientid().c_str(),
                  refcap->clientuuid().c_str(),
                  refcap->authid().c_str());

  if (mInodeCaps.count(refcap->id()))
  {

    std::set<std::string> deletioncaps;

    for (auto it = mInodeCaps[refcap->id()].begin();
         it != mInodeCaps[refcap->id()].end(); ++it)
    {
      shared_cap cap;
      // loop over all caps for that inode
      if (mCaps.count(*it))
        cap = mCaps[*it];
      else
        continue;

      // skip our own cap!
      if (cap->authid() == refcap->authid())
        continue;

      // skip identical client mounts!
      if (cap->clientid() == refcap->clientid())
        continue;

      if (cap->id())
      {
        deletioncaps.insert(*it);
        gOFS->zMQ->gFuseServer.Client().ReleaseCAP((uint64_t) cap->id(),
                                                   cap->clientuuid(),
                                                   cap->clientid());
      }
    }

    for (auto it = deletioncaps.begin(); it != deletioncaps.end(); ++it)
    {
      eos_static_info("auto-remove-cap authid=%s", it->c_str());
      mInodeCaps[refcap->id()].erase(*it);
    }
  }
  return 0;
}

int
FuseServer::Caps::BroadcastMD(const eos::fusex::md &md)
{
  FuseServer::Caps::shared_cap refcap = Get(md.authid());

  XrdSysMutexHelper lock(this);

  eos_static_info("id=%lx clientid=%s clientuuid=%s authid=%s",
                  refcap->id(),
                  refcap->clientid().c_str(),
                  refcap->clientuuid().c_str(),
                  refcap->authid().c_str());

  if (mInodeCaps.count(refcap->id()))
  {
    for (auto it = mInodeCaps[refcap->id()].begin();
         it != mInodeCaps[refcap->id()].end(); ++it)
    {
      shared_cap cap;
      // loop over all caps for that inode
      if (mCaps.count(*it))
        cap = mCaps[*it];
      else
        continue;

      // skip our own cap!
      if (cap->authid() == refcap->authid())
        continue;

      // skip identical client mounts, the have it anyway!
      if (cap->clientid() == refcap->clientid())
        continue;

      if (cap->id())
      {
        gOFS->zMQ->gFuseServer.Client().SendMD(md,
					       cap->clientuuid(),
					       cap->clientid());
      }
    }
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
std::string
FuseServer::Caps::Print(std::string option, std::string filter)
/*----------------------------------------------------------------------------*/
{
  std::string out;
  std::string astring;
  uint64_t now = (uint64_t) time(NULL);
  XrdSysMutexHelper lock(this);

  eos_static_info("option=%s string=%s", option.c_str(), filter.c_str());
  regex_t regex;

  if (filter.size() && regcomp(&regex, filter.c_str(), REG_ICASE | REG_EXTENDED | REG_NOSUB))
  {
    out = "error: illegal regular expression ;";
    out += filter.c_str();
    out += "'\n";
    return out;
  }

  if (option == "t")
  {
    // print by time order
    for (auto it = mTimeOrderedCap.begin(); it != mTimeOrderedCap.end(); ++it)
    {
      if (!mCaps.count(*it))
        continue;

      char ahex[256];
      shared_cap cap = mCaps[*it];

      snprintf(ahex, sizeof (ahex), "%016lx", (unsigned long) cap->id());
      std::string match = "";
      match += "# i:";
      match += ahex;
      match += " a:";
      match += cap->authid();
      match += " c:";
      match += cap->clientid();
      match += " u:";
      match += cap->clientuuid();
      match += " m:";
      snprintf(ahex, sizeof (ahex), "%08lx", (unsigned long) cap->mode());
      match += ahex;
      match += " v:";
      match += eos::common::StringConversion::GetSizeString(astring, (unsigned long long) cap->vtime() - now);
      match += "\n";

      if (filter.size() && (regexec(&regex, match.c_str(), 0, NULL, 0) == REG_NOMATCH))
        continue;
      out += match.c_str();
    }
  }
  if (option == "i")
  {
    // print by inode
    for (auto it = mInodeCaps.begin(); it != mInodeCaps.end(); ++it)
    {
      char ahex[256];
      snprintf(ahex, sizeof (ahex), "%016lx", (unsigned long) it->first);
      if (filter.size() && (regexec(&regex, ahex, 0, NULL, 0) == REG_NOMATCH))
        continue;

      out += "# i:";
      out += ahex;
      out += "\n";
      for (auto sit = it->second.begin(); sit != it->second.end(); ++sit)
      {
        out += "___ a:";
        out += *sit;
        if (!mCaps.count(*sit))
        {

          out += " c:<unfound> u:<unfound> m:<unfound> v:<unfound>\n";
        }
        else
        {
          shared_cap cap = mCaps[*sit];
          out += " c:";
          out += cap->clientid();
          out += " u:";
          out += cap->clientuuid();
          out += " m:";
          snprintf(ahex, sizeof (ahex), "%016lx", (unsigned long) cap->mode());
          out += ahex;
          out += " v:";
          out += eos::common::StringConversion::GetSizeString(astring, (unsigned long long) cap->vtime() - now);
          out += "\n";
        }
      }
    }
  }

  if (option == "p")
  {
    // print by inode
    for (auto it = mInodeCaps.begin(); it != mInodeCaps.end(); ++it)
    {
      std::string spath;
      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
      try
      {
        if (eos::common::FileId::IsFileInode(it->first))
        {
          eos::FileMD* fmd =
                  gOFS->eosFileService->getFileMD(eos::common::FileId::InodeToFid(it->first));
          spath = "f:";
          spath += gOFS->eosView->getUri(fmd);
        }
        else
        {
          eos::ContainerMD* cmd =
                  gOFS->eosDirectoryService->getContainerMD(it->first);
          spath = "d:";
          spath += gOFS->eosView->getUri(cmd);
        }
      }
      catch (eos::MDException &e)
      {
        spath = "<unknown>";
      }

      if (filter.size() && (regexec(&regex, spath.c_str(), 0, NULL, 0) == REG_NOMATCH))
        continue;

      char apath[1024];
      out += "# ";
      snprintf(apath, sizeof (apath), "%-80s", spath.c_str());
      out += apath;
      out += "\n";
      for (auto sit = it->second.begin(); sit != it->second.end(); ++sit)
      {
        out += "___ a:";
        out += *sit;
        if (!mCaps.count(*sit))
        {

          out += " c:<unfound> u:<unfound> m:<unfound> v:<unfound>\n";
        }
        else
        {
          shared_cap cap = mCaps[*sit];
          out += " c:";
          out += cap->clientid();
          out += " u:";
          out += cap->clientuuid();
          out += " m:";
          char ahex[20];
          snprintf(ahex, sizeof (ahex), "%016lx", (unsigned long) cap->mode());
          out += ahex;
          out += " v:";
          out += eos::common::StringConversion::GetSizeString(astring, (unsigned long long) cap->vtime() - now);
          out += "\n";
        }
      }
    }
  }
  return out;
}

/*----------------------------------------------------------------------------*/
LockTracker&
/*----------------------------------------------------------------------------*/
FuseServer::Lock::getLocks(uint64_t id)
/*----------------------------------------------------------------------------*/
{
  // make sure you have this object locked
  return lockmap[id];
}

/*----------------------------------------------------------------------------*/
void
/*----------------------------------------------------------------------------*/
FuseServer::Lock::purgeLocks()
/*----------------------------------------------------------------------------*/
{
  XrdSysMutexHelper lock(this);
  std::set<uint64_t>purgeset;

  for (auto it=lockmap.begin(); it != lockmap.end(); ++it)
  {
    if (!it->second.inuse())
    {
      purgeset.insert(it->first);
    }
  }
  for (auto it=purgeset.begin(); it != purgeset.end(); ++it)
  {
    lockmap.erase(*it);
  }
}

/*----------------------------------------------------------------------------*/
int
/*----------------------------------------------------------------------------*/
FuseServer::Lock::dropLocks(uint64_t id, pid_t pid)
/*----------------------------------------------------------------------------*/
{
  eos_static_info("id=%llu pid=%u", id, pid);

  // drop locks for a given inode/pid pair
  int retc = 0;
  {
    XrdSysMutexHelper lock(this);
    if (lockmap.count(id))
    {
      lockmap[id].removelk(pid);
      retc = 0;
    }
    else
    {
      retc = ENOENT;
    }
  }
  purgeLocks();
  return retc;
}

/*----------------------------------------------------------------------------*/
int
/*----------------------------------------------------------------------------*/
FuseServer::Lock::dropLocks(const std::string& owner)
/*----------------------------------------------------------------------------*/
{
  eos_static_info("owner=%s", owner.c_str());

  // drop locks for a given owner
  int retc = 0;
  {
    XrdSysMutexHelper lock(this);
    for (auto it=lockmap.begin(); it != lockmap.end(); ++it)
    {
      it->second.removelk(owner);
    }
  }
  purgeLocks();
  return retc;
}

/*----------------------------------------------------------------------------*/
int
/*----------------------------------------------------------------------------*/
FuseServer::Lock::lsLocks(const std::string& owner,
                          std::map<uint64_t, std::set<pid_t>> rlocks,
                          std::map<uint64_t, std::set<pid_t>> wlocks)
/*----------------------------------------------------------------------------*/
{
  int retc = 0;
  {
    for (auto it=lockmap.begin(); it != lockmap.end(); ++it)
    {
      std::set<pid_t> rlk = it->second.getrlks(owner);
      std::set<pid_t> wlk = it->second.getwlks(owner);
      rlocks[it->first].insert(rlk.begin(), rlk.end());
      wlocks[it->first].insert(wlk.begin(), wlk.end());
    }
  }
  return retc;
}

/*----------------------------------------------------------------------------*/
bool
FuseServer::FillContainerMD(uint64_t id, eos::fusex::md & dir)
/*----------------------------------------------------------------------------*/
{
  eos::ContainerMD* cmd = 0;
  eos::ContainerMD::ctime_t ctime;
  eos::ContainerMD::ctime_t mtime;
  uint64_t clock = 0;

  eos_static_debug("container-id=%llx", id);

  try
  {
    cmd = gOFS->eosDirectoryService->getContainerMD(id, &clock);
    cmd->getCTime(ctime);
    cmd->getMTime(mtime);
    dir.set_md_ino(id);
    dir.set_md_pino(cmd->getParentId());
    dir.set_ctime(ctime.tv_sec);
    dir.set_ctime_ns(ctime.tv_nsec);
    dir.set_mtime(mtime.tv_sec);
    dir.set_mtime_ns(mtime.tv_nsec);


    dir.set_atime(mtime.tv_sec);
    dir.set_atime_ns(mtime.tv_nsec);
    dir.set_size(cmd->getTreeSize());
    dir.set_uid(cmd->getCUid());
    dir.set_gid(cmd->getCGid());
    dir.set_mode(cmd->getMode());

    // TODO: no hardlinks
    dir.set_nlink(1);
    dir.set_name(cmd->getName());

    for ( eos::ContainerMD::XAttrMap::iterator it = cmd->attributesBegin();
         it != cmd->attributesEnd(); ++it)
    {
      (*dir.mutable_attr())[it->first] = it->second;
      if ( (it->first) == "eos.btime")
      {
        std::string key, val;
        eos::common::StringConversion::SplitKeyValue(it->second, key, val, ".");
        dir.set_btime(strtoul(key.c_str(), 0, 10));
        dir.set_btime_ns(strtoul(val.c_str(), 0, 10));
      }
    }

    dir.set_nchildren(cmd->getNumContainers() + cmd->getNumFiles());

    if (dir.operation() == dir.LS)
    {
      for ( eos::ContainerMD::FileMap::iterator it = cmd->filesBegin();
           it != cmd->filesEnd() ; ++it)
      {
        (*dir.mutable_children())[it->first] = eos::common::FileId::FidToInode(it->second->getId());
      }

      for ( eos::ContainerMD::ContainerMap::iterator it = cmd->containersBegin();
           it != cmd->containersEnd(); ++it)
      {
        (*dir.mutable_children())[it->first] = it->second->getId();
      }
      // indicate that this MD record contains children information
      dir.set_type(dir.MDLS);
    }
    else
    {
      // indicate that this MD record contains only MD but no children information
      eos_static_debug("setting md type");
      dir.set_type(dir.MD);
    }
    dir.set_clock(clock);
    dir.clear_err();
    return true;
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
    eos_static_debug("caught exception %d %s\n", e.getErrno(), e.getMessage().str().c_str());
    dir.set_err(errno);
    return false;
  }
}

/*----------------------------------------------------------------------------*/
bool
FuseServer::FillFileMD(uint64_t inode, eos::fusex::md & file)
/*----------------------------------------------------------------------------*/
{
  // fills file meta data by inode number

  eos::FileMD* fmd = 0;
  eos::FileMD::ctime_t ctime;
  eos::FileMD::ctime_t mtime;
  uint64_t clock=0;

  eos_static_debug("file-inode=%llx file-id=%llx", inode, eos::common::FileId::InodeToFid(inode));

  try
  {
    fmd = gOFS->eosFileService->getFileMD(eos::common::FileId::InodeToFid(inode), &clock);
    eos_static_info("clock=%llx", clock);
    fmd->getCTime(ctime);
    fmd->getMTime(mtime);
    file.set_md_ino(inode);
    file.set_md_pino(fmd->getContainerId());
    file.set_ctime(ctime.tv_sec);
    file.set_ctime_ns(ctime.tv_nsec);
    file.set_mtime(mtime.tv_sec);
    file.set_mtime_ns(mtime.tv_nsec);

    file.set_btime(ctime.tv_sec);
    file.set_btime_ns(ctime.tv_nsec);
    file.set_atime(mtime.tv_sec);
    file.set_atime_ns(mtime.tv_nsec);
    file.set_size(fmd->getSize());
    file.set_uid(fmd->getCUid());
    file.set_gid(fmd->getCGid());
    if (fmd->isLink())
    {
      file.set_mode(fmd->getFlags() | S_IFLNK );
      file.set_target(fmd->getLink());
    }
    else
    {
      file.set_mode(fmd->getFlags() | S_IFREG );
    }
    // TODO: no hardlinks
    file.set_nlink(1);
    file.set_name(fmd->getName());
    file.set_clock(clock);

    for ( eos::FileMD::XAttrMap::iterator it = fmd->attributesBegin();
         it != fmd->attributesEnd(); ++it)
    {
      (*file.mutable_attr())[it->first] = it->second;
      if ( (it->first) == "sys.eos.btime")
      {
        std::string key, val;
        eos::common::StringConversion::SplitKeyValue(it->second, key, val, ".");
        file.set_btime(strtoul(key.c_str(), 0, 10));
        file.set_btime_ns(strtoul(val.c_str(), 0, 10));
      }
    }

    file.clear_err();

    return true;
  }
  catch (eos::MDException &e)
  {

    errno = e.getErrno();
    eos_static_debug("caught exception %d %s\n", e.getErrno(), e.getMessage().str().c_str());
    file.set_err(errno);
    return false;
  }
}

/*----------------------------------------------------------------------------*/
bool
FuseServer::FillContainerCAP(uint64_t id,
                             eos::fusex::md& dir,
                             eos::common::Mapping::VirtualIdentity * vid,
                             std::string reuse_uuid)
/*----------------------------------------------------------------------------*/
{
  dir.mutable_capability()->set_id(id);

  eos_static_debug("container-id=%llx", id);

  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts, true);
  dir.mutable_capability()->set_vtime(ts.tv_sec + 300);
  dir.mutable_capability()->set_vtime_ns(ts.tv_nsec);

  mode_t mode = S_IFDIR;

  // define the permissions
  if ( (vid->uid == 0) )
  {
    // grant all permissions
    dir.mutable_capability()->set_mode(0xff | S_IFDIR);
  }
  else
  {
    if (vid->sudoer)
    {
      mode |= C_OK | M_OK | U_OK | W_OK | D_OK | SA_OK ; // chown + chmod permission + all the rest
    }

    if (vid->uid == (uid_t) dir.uid())
    {
      if (dir.mode() & S_IRUSR)
        mode |= R_OK;

      if (dir.mode() & S_IWUSR)
        mode |= U_OK | W_OK | D_OK | SA_OK | M_OK;

      if (dir.mode() & S_IXUSR)
        mode |= X_OK;
    }

    if (vid->gid == (gid_t) dir.gid())
    {
      if (dir.mode() & S_IRGRP)
        mode |= R_OK;

      if (dir.mode() & S_IWGRP)
      {
        mode |= U_OK | W_OK | D_OK | SA_OK | M_OK;
      }
      if (dir.mode() & S_IXGRP)
        mode |= X_OK;
    }

    if (dir.mode() & S_IROTH)
      mode |= R_OK;

    if (dir.mode() & S_IWOTH)
    {
      mode |= U_OK | W_OK | D_OK | SA_OK | M_OK;
    }

    if (dir.mode() & S_IXOTH)
      mode |= X_OK;

    // look at ACLs
    std::string sysacl = (*(dir.mutable_attr()))["sys.acl"];
    std::string useracl =(*(dir.mutable_attr()))["user.acl"];

    if (sysacl.length() || useracl.length())
    {
      bool evaluseracl = dir.attr().count("sys.eval.useracl") ? true : false;

      Acl acl;
      acl.Set(sysacl,
              useracl,
              *vid,
              evaluseracl);

      if (acl.IsMutable())
      {
        if (acl.CanRead())
          mode |= R_OK;

        if (acl.CanWrite() || acl.CanWriteOnce())
          mode |= W_OK;

        if (acl.CanBrowse())
          mode |= X_OK;

        if (acl.CanChmod())
          mode |= M_OK;

        if (acl.CanNotChmod())
          mode &= ~M_OK;

        if (acl.CanChown())
          mode |= C_OK;

        if (acl.CanUpdate())
          mode |= U_OK;

        if (acl.CanNotDelete())
          mode &= ~D_OK;

      }
    }
    dir.mutable_capability()->set_mode(mode);
  }

  std::string ownerauth=(*(dir.mutable_attr()))["sys.owner.auth"];

  // define new target owner
  if (ownerauth.length())
  {
    if (ownerauth == "*")
    {
      // sticky ownership for everybody
      dir.mutable_capability()->set_uid(dir.uid());
      dir.mutable_capability()->set_gid(dir.gid());
    }
    else
    {
      ownerauth += ",";
      std::string ownerkey = vid->prot.c_str();
      ownerkey += ":";
      if (vid->prot == "gsi")
      {
        ownerkey += vid->dn.c_str();
      }
      else
      {
        ownerkey += vid->uid_string.c_str();
      }
      if ((ownerauth.find(ownerkey)) != std::string::npos)
      {
        // sticky ownership for this authentication 
        dir.mutable_capability()->set_uid(dir.uid());
        dir.mutable_capability()->set_gid(dir.gid());
      }
      else
      {
        // no sticky ownership for this authentication
        dir.mutable_capability()->set_uid(vid->uid);
        dir.mutable_capability()->set_gid(vid->gid);
      }
    }
  }
  else
  {
    // no sticky ownership
    dir.mutable_capability()->set_uid(vid->uid);
    dir.mutable_capability()->set_gid(vid->gid);
  }
  dir.mutable_capability()->set_authid(reuse_uuid.length() ?
                                       reuse_uuid : eos::common::StringConversion::random_uuidstring());
  dir.mutable_capability()->set_clientid(dir.clientid());
  dir.mutable_capability()->set_clientuuid(dir.clientuuid());
  Cap().Store(dir.capability(), vid);
  return true;
}

/*----------------------------------------------------------------------------*/
FuseServer::Caps::shared_cap
FuseServer::ValidateCAP(const eos::fusex::md& md, mode_t mode)
{
  FuseServer::Caps::shared_cap cap = Cap().Get(md.authid());
  // no cap - go away
  if (!cap->id())
  {
    eos_static_err("no cap for authid=%s", md.authid().c_str());
    return 0;
  }

  // wrong cap - go away
  if ((cap->id() != md.md_ino()) && (cap->id() != md.md_pino()))
  {
    eos_static_err("wrong cap for authid=%s cap-id=%lx md-ino=%lx md-pino=%lx",
                   md.authid().c_str(),
                   md.md_ino(),
                   md.md_pino()
                   );
    return 0;
  }

  eos_static_debug("cap-mode=%x mode=%x", cap->mode(), mode);

  if ( (cap->mode() & mode ) == mode )
  {
    uint64_t now = (uint64_t) time(NULL);

    // leave some margin for revoking
    if (cap->vtime() <= (now + 10))
    {
      // cap expired !
      return 0;
    }
    return cap;
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
uint64_t
FuseServer::InodeFromCAP(const eos::fusex::md & md)
{
  FuseServer::Caps::shared_cap cap = Cap().Get(md.authid());
  // no cap - go away
  if (!cap)
  {
    eos_static_debug("no cap for authid=%s", md.authid().c_str());
    return 0;
  }
  else
  {
    eos_static_debug("authid=%s cap-ino=%lx", md.authid().c_str(), cap->id());
  }
  return cap->id();
}

/*----------------------------------------------------------------------------*/
std::string
FuseServer::Header(const std::string & response)
{

  char hex[8];
  sprintf(hex, "%08x", (int) response.length());
  return std::string( "[" ) + hex + std::string ( "]" );
}

/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
int
FuseServer::HandleMD(const std::string &id,
                     const eos::fusex::md& md,
                     std::string* response,
                     uint64_t* clock,
                     eos::common::Mapping::VirtualIdentity * vid)
/*----------------------------------------------------------------------------*/
{
  std::string ops;
  switch (md.operation()) {
  case md.GET: ops = "GET";
    break;
  case md.SET: ops = "SET";
    break;
  case md.DELETE: ops = "DELETE";
    break;
  case md.GETCAP: ops = "GETCAP";
    break;
  case md.LS: ops = "LS";
    break;
  case md.GETLK: ops = "GETLK";
    break;
  case md.SETLK: ops = "SETLK";
    break;
  default:
    ops = "UNKNOWN";
  }

  eos_static_info("ino=%lx operation=%s cid=%s cuuid=%s", (long) md.md_ino(),
                  ops.c_str(),
                  md.clientid().c_str(), md.clientuuid().c_str());


  if ( EOS_LOGS_DEBUG )
  {
    std::string mdout = dump_message(md);
    eos_static_debug("\n%s\n", mdout.c_str());
  }

  if ( (md.operation() == md.GET) || (md.operation() == md.LS) )
  {
    if (clock)
      *clock = 0 ;

    eos::fusex::container cont;

    eos::common::RWMutexReadLock(gOFS->eosViewRWMutex);

    if (!eos::common::FileId::IsFileInode(md.md_ino()) )
    {
      eos_static_info("ino=%lx get-dir", (long) md.md_ino());
      cont.set_type(cont.MDMAP);
      cont.set_ref_inode_(md.md_ino());

      eos::fusex::md_map* mdmap = cont.mutable_md_map_();
      // create the parent entry;
      auto parent = mdmap->mutable_md_map_();

      (*parent)[md.md_ino()].set_md_ino(md.md_ino());
      (*parent)[md.md_ino()].set_clientuuid(md.clientuuid());
      (*parent)[md.md_ino()].set_clientid(md.clientid());
      if (md.operation() == md.LS)
      {
        (*parent)[md.md_ino()].set_operation(md.LS);
      }

      size_t n_attached=1;

      // retrieve directory meta data
      if (FillContainerMD(md.md_ino(), (*parent)[md.md_ino()] ))
      {
        // refresh the cap with the same authid
        FillContainerCAP(md.md_ino(), (*parent)[md.md_ino()], vid,
                         md.authid());
        // store clock
        if (clock)
          *clock = (*parent)[md.md_ino()].clock();

        if ( md.operation() == md.LS)
        {
          // attach children
          auto map = (*parent)[md.md_ino()].children();
          auto it = map.begin();
          for ( ; it != map.end(); ++it)
          {
            // this is a map by inode 
            (*parent)[it->second].set_md_ino(it->second);
            auto child_md = &((*parent)[it->second]);

            if (eos::common::FileId::IsFileInode(it->second))
            {
              // this is a file
              FillFileMD(it->second, *child_md);
            }
            else
            {
              // we don't fill the LS information for the children, just the MD
              child_md->set_operation(md.GET);
              child_md->set_clientuuid(md.clientuuid());
              child_md->set_clientid(md.clientid());
              // this is a directory
              FillContainerMD(it->second, *child_md);
              // get the capability
              FillContainerCAP(it->second, *child_md, vid);
              child_md->clear_operation();
            }
          }
          n_attached ++;

          if (n_attached >= 128)
          {
            std::string rspstream;
            cont.SerializeToString(&rspstream);

            if (!response)
            {
              // send parent + first 128 children
              gOFS->zMQ->task->reply(id, rspstream);
            }
            else
            {
              *response+=Header(rspstream);
              response->append(rspstream.c_str(), rspstream.size());
            }

            n_attached = 0;
            cont.Clear();
          }
        }
        if ( EOS_LOGS_DEBUG )
        {
          std::string mdout = dump_message(*mdmap);
          eos_static_debug("\n%s\n", mdout.c_str());
        }
      }
      (*parent)[md.md_ino()].clear_operation();

      if (n_attached)
      {
        // send left-over children
        std::string rspstream;
        cont.SerializeToString(&rspstream);

        if (!response)
        {
          gOFS->zMQ->task->reply(id, rspstream);
        }
        else
        {
          *response += Header(rspstream);
          response->append(rspstream.c_str(), rspstream.size());
        }
      }
    }
    else
    {
      eos_static_info("ino=%lx get-file/link", (long) md.md_ino());
      cont.set_type(cont.MD);
      cont.set_ref_inode_(md.md_ino());

      FillFileMD(md.md_ino(), (*cont.mutable_md_()));
      std::string rspstream;
      cont.SerializeToString(&rspstream);

      // store clock
      if (clock)
        *clock = cont.md_().clock();

      if (!response)
      {
        // send file meta data
        gOFS->zMQ->task->reply(id, rspstream);
      }
      else
      {
        *response += Header(rspstream);
        *response += rspstream;
      }
    }
    return 0;
  }
  if (md.operation() == md.SET)
  {
    uint64_t md_pino=md.md_pino();

    if (!md_pino)
    {
      // -----------------------------------------------------------------------
      // this can be a creation with an implied capability and the remote inode
      // of the parent directory
      // was not yet send back to the creating client
      // -----------------------------------------------------------------------
      md_pino = InodeFromCAP(md);
    }
    if (!ValidateCAP(md, W_OK | SA_OK))
    {
      return EPERM;
    }

    enum set_type
    {
      CREATE, UPDATE, RENAME, MOVE
    } ;

    set_type op;
    uint64_t md_ino=0;
    bool exclusive = false;

    if (md.type() == md.EXCL)
    {
      exclusive = true;
    }


    if (S_ISDIR(md.mode()))
    {
      eos_static_info("ino=%lx pin=%lx authid=%s set-dir", (long) md.md_ino(),
                      (long) md.md_pino(),
                      md.authid().c_str());

      eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
      eos::ContainerMD* cmd = 0;
      eos::ContainerMD* pcmd = 0;
      eos::ContainerMD* cpcmd = 0;

      try
      {
        if (md.md_ino() && exclusive)
        {
          return EEXIST;
        }

        if (md.md_ino())
        {
          if (md.implied_authid().length())
          {
            // this is a create on top of an existing inode
            return EEXIST;
          }


          op = UPDATE;
          // dir update
          cmd = gOFS->eosDirectoryService->getContainerMD(md.md_ino());
          pcmd = gOFS->eosDirectoryService->getContainerMD(md.md_pino());
          if (cmd->getParentId() != md.md_pino())
          {
            // this indicates a directory move
            op = MOVE;
            eos_static_info("moving %lx => %lx", cmd->getParentId(), md.md_pino());
            cpcmd = gOFS->eosDirectoryService->getContainerMD(cmd->getParentId());
            cpcmd->removeContainer(cmd->getName());
            gOFS->eosView->updateContainerStore(cpcmd);
            cmd->setName(md.name());
            pcmd->addContainer(cmd);
            gOFS->eosView->updateContainerStore(pcmd);
          }

          if (cmd->getName() != md.name())
          {
            // this indicates a directory rename
            op = RENAME;
            eos_static_info("rename %s=>%s", cmd->getName().c_str(),
                            md.name().c_str());
            gOFS->eosView->renameContainer(cmd, md.name());
          }

          md_ino = md.md_ino();
          eos_static_info("ino=%lx pino=%lx cpino=%lx update-dir",
                          (long) md.md_ino(),
                          (long) md.md_pino(), (long) cmd->getParentId());
        }
        else
        {
          // dir creation
          op = CREATE;
          pcmd = gOFS->eosDirectoryService->getContainerMD(md.md_pino());

          if (exclusive && pcmd->findContainer( md.name() ))
          {
            // O_EXCL set on creation - 
            return EEXIST;
          }

          cmd = gOFS->eosDirectoryService->createContainer();
          cmd->setName( md.name() );
          md_ino = cmd->getId();
          pcmd->addContainer(cmd);
          eos_static_info("ino=%lx pino=%lx md-ino=%lx create-dir",
                          (long) md.md_ino(),
                          (long) md.md_pino(),
                          md_ino);

          if (!Cap().Imply(md_ino, md.authid(), md.implied_authid()))
          {
            eos_static_err("imply failed for new inode %lx", md_ino);
          }
        }

        cmd->setName(md.name());
        cmd->setCUid(md.uid());
        cmd->setCGid(md.gid());
        cmd->setMode(md.mode());
        eos::ContainerMD::ctime_t ctime;
        eos::ContainerMD::ctime_t mtime;
        ctime.tv_sec = md.ctime();
        ctime.tv_nsec = md.ctime_ns();
        mtime.tv_sec = md.mtime();
        mtime.tv_nsec = md.mtime_ns();
        cmd->setCTime(ctime);
        cmd->setMTime(mtime);
        cmd->clearAttributes();
        for (auto map = md.attr().begin(); map != md.attr().end(); ++map)
        {
          cmd->setAttribute(map->first, map->second);
        }

        if (op == CREATE)
        {
          // store the birth time as an extended attribute
          char btime[256];
          snprintf(btime, sizeof (btime), "%lu.%lu", md.btime(), md.btime_ns());
          cmd->setAttribute("sys.eos.btime", btime);
        }

        if (op != UPDATE && md.pmtime())
        {
          // store the new modification time for the parent
          eos::ContainerMD::ctime_t pmtime;
          pmtime.tv_sec = md.pmtime();
          pmtime.tv_nsec = md.pmtime_ns();
          pcmd->setMTime(pmtime);
          gOFS->eosDirectoryService->updateStore(pcmd);
          pcmd->notifyMTimeChange( gOFS->eosDirectoryService );
        }
        
        gOFS->eosDirectoryService->updateStore(cmd);

        eos::fusex::response resp;
        resp.set_type(resp.ACK);
        resp.mutable_ack_()->set_code(resp.ack_().OK);
        resp.mutable_ack_()->set_transactionid(md.reqid());
        resp.mutable_ack_()->set_md_ino(md_ino);
        resp.SerializeToString(response);

        // broadcast this update around

        switch ( op ) {
        case UPDATE:
        case CREATE:
        case RENAME:
        case MOVE:
          Cap().BroadcastRelease(md);
          break;
        }
      }
      catch ( eos::MDException &e )
      {
        eos_static_info("ino=%lx err-no=%d err-msg=%s", (long) md.md_ino(),
                        e.getErrno(),
                        e.getMessage().str().c_str());
        eos::fusex::response resp;
        resp.set_type(resp.ACK);
        resp.mutable_ack_()->set_code(resp.ack_().PERMANENT_FAILURE);
        resp.mutable_ack_()->set_err_no(e.getErrno());
        resp.mutable_ack_()->set_err_msg(e.getMessage().str().c_str());
        resp.mutable_ack_()->set_transactionid(md.reqid());
        resp.SerializeToString(response);
      }
      return 0;
    }

    if (S_ISREG(md.mode()))
    {
      eos_static_info("ino=%lx pin=%lx authid=%s file", (long) md.md_ino(),
                      (long) md.md_pino(),
                      md.authid().c_str());

      eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
      eos::FileMD* fmd = 0;
      eos::ContainerMD* pcmd = 0;
      eos::ContainerMD* cpcmd = 0;


      uint64_t fid = eos::common::FileId::InodeToFid (md.md_ino());
      md_ino = md.md_ino();

      try
      {
        if (md.md_ino() && exclusive)
        {
          return EEXIST;
        }

        if (md_ino)
        {
          // file update
          op = UPDATE;

          // dir update
          fmd = gOFS->eosFileService->getFileMD(fid);
          pcmd = gOFS->eosDirectoryService->getContainerMD(md.md_pino());
          if (fmd->getContainerId() != md.md_pino())
          {
            // this indicates a file move
            op = MOVE;
            eos_static_info("moving %lx => %lx", fmd->getContainerId(), md.md_pino());
            cpcmd = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
            cpcmd->removeFile(fmd->getName());
            gOFS->eosView->updateContainerStore(cpcmd);
            fmd->setName(md.name());
            pcmd->addFile(fmd);
            gOFS->eosView->updateContainerStore(pcmd);
          }

          if (fmd->getName() != md.name())
          {
            // this indicates a file rename
            op = RENAME;
            eos_static_info("rename %s=>%s", fmd->getName().c_str(), md.name().c_str());
            gOFS->eosView->renameFile(fmd, md.name());
          }
          eos_static_info("fid=%lx ino=%lx pino=%lx cpino=%lx update-file",
                          (long) fid,
                          (long) md.md_ino(),
                          (long) md.md_pino(), (long) fmd->getContainerId());
        }
        else
        {
          // file creation
          op = CREATE;
          pcmd = gOFS->eosDirectoryService->getContainerMD(md.md_pino());

          if (exclusive && pcmd->findContainer( md.name() ))
          {
            // O_EXCL set on creation - 
            return EEXIST;
          }

          unsigned long layoutId = 0;
          unsigned long forcedFsId = 0;
          long forcedGroup = 0;
          XrdOucString space;
          eos::ContainerMD::XAttrMap attrmap = pcmd->getAttributeMap();
          XrdOucEnv env;

          // retrieve the layout
          Policy::GetLayoutAndSpace("fusex",
                                    attrmap,
                                    *vid,
                                    layoutId,
                                    space,
                                    env,
                                    forcedFsId,
                                    forcedGroup);



          fmd = gOFS->eosFileService->createFile();
          fmd->setName( md.name() );
          fmd->setLayoutId( layoutId );
          md_ino = eos::common::FileId::FidToInode(fmd->getId());
          pcmd->addFile(fmd);
          eos_static_info("ino=%lx pino=%lx md-ino=%lx create-file", (long) md.md_ino(), (long) md.md_pino(), md_ino);
        }

        fmd->setName(md.name());
        fmd->setCUid(md.uid());
        fmd->setCGid(md.gid());
        fmd->setSize(md.size());

        // for the moment we store 9 bits here
        fmd->setFlags( md.mode() & (S_IRWXU | S_IRWXG | S_IRWXO) );
        eos::FileMD::ctime_t ctime;
        eos::FileMD::ctime_t mtime;
        ctime.tv_sec = md.ctime();
        ctime.tv_nsec = md.ctime_ns();
        mtime.tv_sec = md.mtime();
        mtime.tv_nsec = md.mtime_ns();
        fmd->setCTime(ctime);
        fmd->setMTime(mtime);
        fmd->clearAttributes();
        for (auto map = md.attr().begin(); map != md.attr().end(); ++map)
        {
          fmd->setAttribute(map->first, map->second);
        }

        // store the birth time as an extended attribute
        char btime[256];
        snprintf(btime, sizeof (btime), "%lu.%lu", md.btime(), md.btime_ns());
        fmd->setAttribute("sys.eos.btime", btime);

        gOFS->eosFileService->updateStore(fmd);

        eos::fusex::response resp;
        resp.set_type(resp.ACK);
        resp.mutable_ack_()->set_code(resp.ack_().OK);
        resp.mutable_ack_()->set_transactionid(md.reqid());
        resp.mutable_ack_()->set_md_ino(md_ino);
        resp.SerializeToString(response);

	// broadcast this update around
	switch ( op ) {
	case UPDATE:
	case CREATE:
	case RENAME:
	case MOVE:
	  Cap().BroadcastMD(md);
	  break;
	}
      }
      catch ( eos::MDException &e )
      {
        eos_static_info("ino=%lx err-no=%d err-msg=%s", (long) md.md_ino(),
                        e.getErrno(),
                        e.getMessage().str().c_str());
        eos::fusex::response resp;
        resp.set_type(resp.ACK);
        resp.mutable_ack_()->set_code(resp.ack_().PERMANENT_FAILURE);
        resp.mutable_ack_()->set_err_no(e.getErrno());
        resp.mutable_ack_()->set_err_msg(e.getMessage().str().c_str());
        resp.mutable_ack_()->set_transactionid(md.reqid());
        resp.SerializeToString(response);
      }
      return 0;
    }

    if (S_ISLNK(md.mode()))
    {
      eos_static_info("ino=%lx set-link", (long) md.md_ino());

      eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);

      eos::FileMD* fmd = 0;
      eos::ContainerMD* pcmd = 0;

      try 
      {
	// link creation
	op = CREATE;
	pcmd = gOFS->eosDirectoryService->getContainerMD(md.md_pino());
	
	if ( pcmd->findContainer( md.name() ))
	{
	  // O_EXCL set on creation - 
	  return EEXIST;
	}
	
	fmd = gOFS->eosFileService->createFile();
	
	fmd->setName( md.name() );
	fmd->setLink( md.target() );
	fmd->setLayoutId( 0 );
	md_ino = eos::common::FileId::FidToInode(fmd->getId());
	pcmd->addFile(fmd);
	eos_static_info("ino=%lx pino=%lx md-ino=%lx create-link", (long) md.md_ino(), (long) md.md_pino(), md_ino);
	
	fmd->setCUid(md.uid());
	fmd->setCGid(md.gid());
	fmd->setSize( 1 );
	
	fmd->setFlags( md.mode() & (S_IRWXU | S_IRWXG | S_IRWXO) );
	
	eos::FileMD::ctime_t ctime;
	eos::FileMD::ctime_t mtime;
	ctime.tv_sec = md.ctime();
	ctime.tv_nsec = md.ctime_ns();
	mtime.tv_sec = md.mtime();
	mtime.tv_nsec = md.mtime_ns();
	fmd->setCTime(ctime);
	fmd->setMTime(mtime);
	fmd->clearAttributes();
	
	// store the birth time as an extended attribute
	char btime[256];
	snprintf(btime, sizeof (btime), "%lu.%lu", md.btime(), md.btime_ns());
	fmd->setAttribute("sys.eos.btime", btime);
	
	gOFS->eosFileService->updateStore(fmd);
	
	eos::fusex::response resp;
	resp.set_type(resp.ACK);
	resp.mutable_ack_()->set_code(resp.ack_().OK);
	resp.mutable_ack_()->set_transactionid(md.reqid());
	resp.mutable_ack_()->set_md_ino(md_ino);
	resp.SerializeToString(response);

	Cap().BroadcastRelease(md);
      }
      catch ( eos::MDException &e )
      {
        eos_static_info("ino=%lx err-no=%d err-msg=%s", (long) md.md_ino(),
                        e.getErrno(),
                        e.getMessage().str().c_str());
        eos::fusex::response resp;
        resp.set_type(resp.ACK);
        resp.mutable_ack_()->set_code(resp.ack_().PERMANENT_FAILURE);
        resp.mutable_ack_()->set_err_no(e.getErrno());
        resp.mutable_ack_()->set_err_msg(e.getMessage().str().c_str());
        resp.mutable_ack_()->set_transactionid(md.reqid());
        resp.SerializeToString(response);
      }
      return 0;
    }
  }

  if (md.operation() == md.DELETE)
  {
    if (!ValidateCAP(md, D_OK))
    {
      eos_static_err("ino=%lx delete has wrong cap");
      return EPERM;
    }

    eos::fusex::response resp;
    resp.set_type(resp.ACK);

    eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
    eos::ContainerMD* cmd = 0;
    eos::ContainerMD* pcmd = 0;
    eos::FileMD* fmd = 0;
    try
    {
      pcmd = gOFS->eosDirectoryService->getContainerMD(md.md_pino());
      if (S_ISDIR(md.mode()))
        cmd = gOFS->eosDirectoryService->getContainerMD(md.md_ino());
      else
        fmd = gOFS->eosFileService->getFileMD(eos::common::FileId::InodeToFid(md.md_ino()));


      if (S_ISDIR(md.mode()))
      {
        // check if this directory is empty
        if (cmd->getNumContainers() || cmd->getNumFiles())
        {
          eos::fusex::response resp;
          resp.set_type(resp.ACK);
          resp.mutable_ack_()->set_code(resp.ack_().PERMANENT_FAILURE);
          resp.mutable_ack_()->set_err_no(ENOTEMPTY);
          resp.mutable_ack_()->set_err_msg("directory not empty");
          resp.mutable_ack_()->set_transactionid(md.reqid());
          resp.SerializeToString(response);
          return 0;
        }
        eos_static_info("ino=%lx delete-dir", (long) md.md_ino());
        pcmd->removeContainer(cmd->getName());
        gOFS->eosDirectoryService->removeContainer(cmd);
        pcmd->notifyMTimeChange( gOFS->eosDirectoryService );
        resp.mutable_ack_()->set_code(resp.ack_().OK);
        resp.mutable_ack_()->set_transactionid(md.reqid());
        resp.SerializeToString(response);

        Cap().BroadcastRelease(md);

        return 0;
      }
      if (S_ISREG(md.mode()))
      {
        eos_static_info("ino=%lx delete-file", (long) md.md_ino());
        pcmd->removeFile(fmd->getName());
        fmd->setContainerId(0);
        fmd->unlinkAllLocations();
        gOFS->eosFileService->updateStore(fmd);
        pcmd->notifyMTimeChange( gOFS->eosDirectoryService );
        resp.mutable_ack_()->set_code(resp.ack_().OK);
        resp.mutable_ack_()->set_transactionid(md.reqid());
        resp.SerializeToString(response);

        Cap().BroadcastRelease(md);
        return 0;
      }
      if (S_ISLNK(md.mode()))
      {
        eos_static_info("ino=%lx delete-link", (long) md.md_ino());
        pcmd->removeFile(fmd->getName());
        fmd->setContainerId(0);
        fmd->unlinkAllLocations();
        gOFS->eosFileService->updateStore(fmd);
        pcmd->notifyMTimeChange( gOFS->eosDirectoryService );
        resp.mutable_ack_()->set_code(resp.ack_().OK);
        resp.mutable_ack_()->set_transactionid(md.reqid());
        resp.SerializeToString(response);

        Cap().BroadcastRelease(md);
        return 0;
      }
    }
    catch ( eos::MDException &e )
    {
      resp.mutable_ack_()->set_code(resp.ack_().PERMANENT_FAILURE);
      resp.mutable_ack_()->set_err_no(e.getErrno());
      resp.mutable_ack_()->set_err_msg(e.getMessage().str().c_str());
      resp.mutable_ack_()->set_transactionid(md.reqid());
      resp.SerializeToString(response);
      eos_static_info("ino=%lx err-no=%d err-msg=%s", (long) md.md_ino(),
                      e.getErrno(),
                      e.getMessage().str().c_str());
      return 0;
    }
  }

  if (md.operation() == md.GETCAP)
  {
    eos::fusex::container cont;
    cont.set_type(cont.CAP);

    eos::fusex::md lmd;
    // get the meta data
    FillContainerMD((uint64_t) md.md_ino(), lmd);

    lmd.set_clientuuid(md.clientuuid());
    lmd.set_clientid(md.clientid());
    // get the capability
    FillContainerCAP(md.md_ino(), lmd, vid);

    // this cap only provides the permissions, but it is not a cap which 
    // synchronized the meta data atomically, the client marks a cap locally
    // if he synchronized the contents with it

    *(cont.mutable_cap_()) = lmd.capability();

    std::string rspstream;
    cont.SerializeToString(&rspstream);
    *response+=Header(rspstream);
    response->append(rspstream.c_str(), rspstream.size());

    eos_static_info("cap-issued: id=%lx mode=%x vtime=%lu.%lu uid=%u gid=%u client-id=%s auth-id=%s errc=%d",
                    cont.cap_().id(), cont.cap_().mode(), cont.cap_().vtime(), cont.cap_().vtime_ns(), cont.cap_().uid(), cont.cap_().gid(), cont.cap_().clientid().c_str(), cont.cap_().authid().c_str(), cont.cap_().errc());
    return 0;
  }

  if (md.operation() == md.GETLK)
  {
    eos::fusex::response resp;
    resp.set_type(resp.LOCK);

    struct flock lock;

    Locks().getLocks(md.md_ino()).getlk((pid_t) md.flock().pid(), &lock);
    resp.mutable_lock_()->set_len(lock.l_len);
    resp.mutable_lock_()->set_start(lock.l_start);
    resp.mutable_lock_()->set_pid(lock.l_pid);

    switch (lock.l_type) {
    case F_RDLCK:
      resp.mutable_lock_()->set_type(md.flock().RDLCK);
      break;
    case F_WRLCK:
      resp.mutable_lock_()->set_type(md.flock().WRLCK);
      break;
    case F_UNLCK:
      resp.mutable_lock_()->set_type(md.flock().UNLCK);
      break;
    }
  }

  if ( (md.operation() == md.SETLK) ||
      (md.operation() == md.SETLKW) )
  {
    eos::fusex::response resp;
    resp.set_type(resp.LOCK);

    int sleep = 0;

    if ( md.operation() == md.SETLKW )
      sleep = 1;

    struct flock lock;
    lock.l_len = md.flock().len();
    lock.l_start = md.flock().start();
    lock.l_pid = md.flock().pid();

    switch (md.flock().type()) {

    case eos::fusex::lock::RDLCK:
      lock.l_type = F_RDLCK;
      break;
    case eos::fusex::lock::WRLCK:
      lock.l_type = F_WRLCK;
      break;
    case eos::fusex::lock::UNLCK:
      lock.l_type = F_UNLCK;
    default:
      resp.mutable_lock_()->set_err_no(EAGAIN);
      resp.SerializeToString(response);
      return 0;
      break;
    }

    if (Locks().getLocks(md.md_ino()).setlk(md.flock().pid(), &lock, sleep, md.clientid()))
    {
      // lock ok!
      resp.mutable_lock_()->set_err_no(0);
    }
    else
    {
      // lock is busy
      resp.mutable_lock_()->set_err_no(EAGAIN);
    }
    resp.SerializeToString(response);
    return 0;
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
void
FuseServer::HandleDir(const std::string &identity, const eos::fusex::dir & dir)
/*----------------------------------------------------------------------------*/
{
  eos_static_debug("");
}

EOSMGMNAMESPACE_END
