//------------------------------------------------------------------------------
// File: FuseServer/Server.cc
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
#include <thread>
#include <regex>

#include <google/protobuf/util/json_util.h>

#include "mgm/FuseServer/Server.hh"

#include "mgm/Acl.hh"
#include "mgm/Policy.hh"
#include "mgm/Quota.hh"
#include "mgm/Recycle.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsFile.hh"
#include "mgm/ZMQ.hh"
#include "mgm/Stat.hh"
#include "mgm/tracker/ReplicationTracker.hh"

#include "namespace/interface/IView.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/ContainerIterators.hh"
#include "namespace/Prefetcher.hh"

#include "common/Logging.hh"
#include "common/Path.hh"

EOSFUSESERVERNAMESPACE_BEGIN

#define D_OK 8     // delete
#define M_OK 16    // chmod
#define C_OK 32    // chown
#define SA_OK 64   // set xattr
#define U_OK 128   // can update
#define SU_OK 256  // set utime

#define k_mdino  XrdMgmOfsFile::k_mdino
#define k_nlink  XrdMgmOfsFile::k_nlink


USE_EOSFUSESERVERNAMESPACE

const char* Server::cident = "fxserver";


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------

Server::Server()
{
  SetLogId(logId, "fxserver");
  c_max_children = getenv("EOS_MGM_FUSEX_MAX_CHILDREN") ? strtoull(
                     getenv("EOS_MGM_FUSEX_MAX_CHILDREN"), 0, 10) : 131072;

  if (!c_max_children) {
    c_max_children = 131072;
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

Server::~Server()
{
  shutdown();
}

//------------------------------------------------------------------------------
// Start method
//------------------------------------------------------------------------------

void
Server::start()
{
  eos_static_info("msg=\"starting fuse server\" max-children=%llu",
                  c_max_children);
  std::thread monitorthread(&FuseServer::Clients::MonitorHeartBeat,
                            &(this->mClients));
  monitorthread.detach();
  std::thread capthread(&Server::MonitorCaps, this);
  capthread.detach();
}

//------------------------------------------------------------------------------
// Shutdown method
//------------------------------------------------------------------------------

void
Server::shutdown()
{
  Clients().terminate();
  terminate();
}

//------------------------------------------------------------------------------
// Dump message contents as json string
//------------------------------------------------------------------------------

std::string
Server::dump_message(const google::protobuf::Message& message)
{
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string jsonstring;
  google::protobuf::util::MessageToJsonString(message, &jsonstring, options);
  return jsonstring;
}


//------------------------------------------------------------------------------
// Expire caps and update quota information
//------------------------------------------------------------------------------

void
Server::MonitorCaps() noexcept
{
  eos_static_info("msg=\"starting fusex monitor caps thread\"");
  std::map<FuseServer::Caps::authid_t, time_t> outofquota;
  uint64_t noquota = std::numeric_limits<long>::max() / 2;
  size_t cnt = 0;

  while (1) {
    EXEC_TIMING_BEGIN("Eosxd::int::MonitorCaps");

    // expire caps
    do {
      if (Cap().expire()) {
        Cap().pop();
      } else {
        break;
      }
    } while (1);

    time_t now = time(NULL);

    if (!(cnt % Clients().QuotaCheckInterval())) {
      // check quota nodes every mQuotaCheckInterval iterations
      typedef struct quotainfo {

        quotainfo(uid_t _uid, gid_t _gid, uint64_t _qid) : uid(_uid), gid(_gid),
          qid(_qid)
        {
        }

        quotainfo() : uid(0), gid(0), qid(0)
        {
        }
        uid_t uid;
        gid_t gid;
        uint64_t qid;
        std::vector<std::string> authids;

        std::string id()
        {
          char sid[64];
          snprintf(sid, sizeof(sid), "%u:%u:%lu", uid, gid, qid);
          return sid;
        }
      } quotainfo_t;
      std::map<std::string, quotainfo_t> qmap;
      {
        eos::common::RWMutexReadLock lLock(Cap());

        if (EOS_LOGS_DEBUG) {
          eos_static_debug("looping over caps n=%d", Cap().GetCaps().size());
        }

        std::map<FuseServer::Caps::authid_t, FuseServer::Caps::shared_cap>& allcaps =
          Cap().GetCaps();

        for (auto it = allcaps.begin(); it != allcaps.end(); ++it) {
          if (EOS_LOGS_DEBUG) {
            eos_static_debug("cap q-node %lx", it->second->_quota().quota_inode());
          }

          // if we find a cap with 'noquota' contents, we just ignore this one
          if (it->second->_quota().inode_quota() == noquota) {
            continue;
          }

          if (it->second->_quota().quota_inode()) {
            quotainfo_t qi(it->second->uid(), it->second->gid(),
                           it->second->_quota().quota_inode());

            // skip if we did this already ...
            if (qmap.count(qi.id())) {
              qmap[qi.id()].authids.push_back(it->second->authid());
            } else {
              qmap[qi.id()] = qi;
              qmap[qi.id()].authids.push_back(it->second->authid());
            }
          }
        }
      }

      for (auto it = qmap.begin(); it != qmap.end(); ++it) {
        eos::IContainerMD::id_t qino_id = it->second.qid;

        if (EOS_LOGS_DEBUG) {
          eos_static_debug("checking qino=%d", qino_id);
        }

        long long avail_bytes = 0;
        long long avail_files = 0;

        if (!Quota::QuotaBySpace(qino_id, it->second.uid, it->second.gid,
                                 avail_files, avail_bytes)) {
          for (auto auit = it->second.authids.begin();
               auit != it->second.authids.end(); ++auit) {
            if (EOS_LOGS_DEBUG)
              eos_static_debug("checking qino=%d files=%ld bytes=%ld authid=%s",
                               qino_id, avail_files, avail_bytes, auit->c_str());

            if (((!avail_files || !avail_bytes) && (!outofquota.count(*auit))) ||
                // first time out of quota
                ((avail_files && avail_bytes) &&
                 (outofquota.count(*auit)))) { // first time back to quota
              // send the changed quota information via a cap update
              FuseServer::Caps::shared_cap cap;
              {
                eos::common::RWMutexReadLock lLock(Cap());

                if (auto kv = Cap().GetCaps().find(*auit);
                    kv != Cap().GetCaps().end()) {
                  cap = kv->second;
                }
              }

              if (cap) {
                cap->mutable__quota()->set_inode_quota(avail_files);
                cap->mutable__quota()->set_volume_quota(avail_bytes);
                // send this cap (again)
                Cap().BroadcastCap(cap);
              }

              // mark to not send this again unless the quota status changes
              if (!avail_files || !avail_bytes) {
                outofquota[*auit] = now;
              } else {
                outofquota.erase(*auit);
              }
            }
          }
        }
      }

      // expire some old out of quota entries
      for (auto it = outofquota.begin(); it != outofquota.end();) {
        if (((it->second) + 3600) < now) {
          auto erase_it = it++;
          outofquota.erase(erase_it);
        } else {
          it++;
        }
      }
    }

    EXEC_TIMING_END("Eosxd::int::MonitorCaps");
    std::this_thread::sleep_for(std::chrono::seconds(1));

    if (should_terminate()) {
      break;
    }

    cnt++;

    if (gOFS) {
      gOFS->MgmStats.Add("Eosxd::int::MonitorCaps", 0, 0, 1);
    }
  }
}

//------------------------------------------------------------------------------
// Print cleints
//------------------------------------------------------------------------------

void
Server::Print(std::string& out, std::string options)
{
  if (
    (options.find("m") != std::string::npos) ||
    (options.find("l") != std::string::npos) ||
    (options.find("k") != std::string::npos) ||
    !options.length()) {
    Client().Print(out, options);
  }

  if (options.find("f") != std::string::npos) {
    std::string flushout;
    gOFS->zMQ->gFuseServer.Flushs().Print(flushout);
    out += flushout;
  }
}

//------------------------------------------------------------------------------
// Fill container meta-data object
//------------------------------------------------------------------------------

int
Server::FillContainerMD(uint64_t id, eos::fusex::md& dir,
                        eos::common::VirtualIdentity& vid)
{
  gOFS->MgmStats.Add("Eosxd::int::FillContainerMD", vid.uid, vid.gid, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::FillContainerMD");
  std::shared_ptr<eos::IContainerMD> cmd;
  eos::IContainerMD::ctime_t ctime;
  eos::IContainerMD::ctime_t mtime;
  eos::IContainerMD::ctime_t tmtime;
  uint64_t clock = 0;

  if (EOS_LOGS_DEBUG) {
    eos_debug("container-id=%llx", id);
  }

  eos::common::RWMutexReadLock rd_ns_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                          __LINE__, __FILE__);
  try {
    cmd = gOFS->eosDirectoryService->getContainerMD(id, &clock);
    rd_ns_lock.Release();
    cmd->getCTime(ctime);
    cmd->getMTime(mtime);
    cmd->getTMTime(tmtime);
    std::string fullpath = gOFS->eosView->getUri(cmd.get());
    dir.set_md_ino(id);
    dir.set_md_pino(cmd->getParentId());
    dir.set_ctime(ctime.tv_sec);
    dir.set_ctime_ns(ctime.tv_nsec);
    dir.set_mtime(mtime.tv_sec);
    dir.set_mtime_ns(mtime.tv_nsec);
    dir.set_ttime(tmtime.tv_sec);
    dir.set_ttime_ns(tmtime.tv_nsec);
    dir.set_atime(mtime.tv_sec);
    dir.set_atime_ns(mtime.tv_nsec);
    dir.set_size(cmd->getTreeSize());
    dir.set_uid(cmd->getCUid());
    dir.set_gid(cmd->getCGid());
    dir.set_mode(cmd->getMode());
    // @todo (apeters): no hardlinks
    dir.set_nlink(2);
    dir.set_name(cmd->getName());
    dir.set_fullpath(fullpath);
    eos::IFileMD::XAttrMap xattrs = cmd->getAttributes();

    for (const auto& elem : xattrs) {
      if ((elem.first) == "sys.vtrace") {
        continue;
      }

      if ((elem.first) == "sys.utrace") {
        continue;
      }

      (*dir.mutable_attr())[elem.first] = elem.second;

      if ((elem.first) == "sys.eos.btime") {
        std::string key, val;
        eos::common::StringConversion::SplitKeyValue(elem.second, key, val, ".");
        dir.set_btime(strtoul(key.c_str(), 0, 10));
        dir.set_btime_ns(strtoul(val.c_str(), 0, 10));
      }
    }

    dir.set_nchildren(cmd->getNumContainers() + cmd->getNumFiles());

    if (dir.operation() == dir.LS) {
      // we put a hard-coded listing limit for service protection
      if (vid.app != "fuse::restic") {
        // no restrictions for restic backups
        if ((uint64_t)dir.nchildren() > c_max_children) {
          // xrootd does not handle E2BIG ... sigh
          return ENAMETOOLONG;
        }
      }

      for (auto it = eos::FileMapIterator(cmd); it.valid(); it.next()) {
        std::string key = eos::common::StringConversion::EncodeInvalidUTF8(it.key());
        (*dir.mutable_children())[key] =
          eos::common::FileId::FidToInode(it.value());
      }

      for (auto it = ContainerMapIterator(cmd); it.valid(); it.next()) {
        std::string key = eos::common::StringConversion::EncodeInvalidUTF8(it.key());
        (*dir.mutable_children())[key] = it.value();
      }

      // indicate that this MD record contains children information
      dir.set_type(dir.MDLS);
    } else {
      // indicate that this MD record contains only MD but no children information
      if (EOS_LOGS_DEBUG) {
        eos_debug("setting md type");
      }

      dir.set_type(dir.MD);
    }

    dir.set_clock(clock);
    dir.clear_err();
    EXEC_TIMING_END("Eosxd::int::FillContainerMD");
    return 0;
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_err("caught exception %d %s\n", e.getErrno(),
            e.getMessage().str().c_str());
    dir.set_err(errno);
    return errno;
  }
}

//------------------------------------------------------------------------------
// Fill file meta-data object
//------------------------------------------------------------------------------

bool
Server::FillFileMD(uint64_t inode, eos::fusex::md& file,
                   eos::common::VirtualIdentity& vid)
{
  gOFS->MgmStats.Add("Eosxd::int::FillFileMD", vid.uid, vid.gid, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::FillFileMD");
  // fills file meta data by inode number
  std::shared_ptr<eos::IFileMD> fmd, gmd;
  eos::IFileMD::ctime_t ctime;
  eos::IFileMD::ctime_t mtime;
  uint64_t clock = 0;

  if (EOS_LOGS_DEBUG) eos_debug("file-inode=%llx file-id=%llx", inode,
                                  eos::common::FileId::InodeToFid(inode));

  eos::common::RWMutexReadLock rd_ns_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                          __LINE__, __FILE__);
  try {
    bool has_mdino = false;
    fmd = gOFS->eosFileService->getFileMD(eos::common::FileId::InodeToFid(inode),
                                          &clock);
    eos_debug("clock=%llx", clock);
    file.set_name(fmd->getName());
    gmd = fmd;
    rd_ns_lock.Release();

    if (fmd->hasAttribute(k_mdino)) {
      has_mdino = true;
      uint64_t mdino = std::stoull(fmd->getAttribute(k_mdino));
      fmd = gOFS->eosFileService->getFileMD(eos::common::FileId::InodeToFid(mdino),
                                            &clock);
      eos_debug("hlnk switched from %s to file %s (%#llx)",
                gmd->getName().c_str(), fmd->getName().c_str(), mdino);
    }

    /* fmd = link target file, gmd = link file */
    fmd->getCTime(ctime);
    fmd->getMTime(mtime);
    file.set_md_ino(eos::common::FileId::FidToInode(gmd->getId()));
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

    if (fmd->isLink()) {
      file.set_mode(fmd->getFlags() | S_IFLNK);
      file.set_target(fmd->getLink());
    } else {
      file.set_mode(fmd->getFlags() | S_IFREG);
    }

    /* hardlinks */
    int nlink = 1;

    if (fmd->hasAttribute(k_nlink)) {
      nlink = std::stoi(fmd->getAttribute(k_nlink)) + 1;

      if (EOS_LOGS_DEBUG) {
        eos_debug("hlnk %s (%#lx) nlink %d", file.name().c_str(), fmd->getId(),
                  nlink);
      }
    }

    file.set_nlink(nlink);
    file.set_clock(clock);
    eos::IFileMD::XAttrMap xattrs = fmd->getAttributes();

    for (const auto& elem : xattrs) {
      if (has_mdino && ((elem.first) == k_nlink)) {
        continue;
      }

      if ((elem.first) == "sys.vtrace") {
        continue;
      }

      if ((elem.first) == "sys.utrace") {
        continue;
      }

      (*file.mutable_attr())[elem.first] = elem.second;

      if ((elem.first) == "sys.eos.btime") {
        std::string key, val;
        eos::common::StringConversion::SplitKeyValue(elem.second, key, val, ".");
        file.set_btime(strtoul(key.c_str(), 0, 10));
        file.set_btime_ns(strtoul(val.c_str(), 0, 10));
      }
    }

    if (has_mdino) {
      (*file.mutable_attr())[k_mdino] = gmd->getAttribute(k_mdino);
    }

    file.clear_err();
    EXEC_TIMING_END("Eosxd::int::FillFileMD");
    return true;
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_err("caught exception %d %s\n", e.getErrno(),
            e.getMessage().str().c_str());
    file.set_err(errno);
    return false;
  }
}

//------------------------------------------------------------------------------
// Fill container capability
//------------------------------------------------------------------------------

bool
Server::FillContainerCAP(uint64_t id,
                         eos::fusex::md& dir,
                         eos::common::VirtualIdentity& vid,
                         std::string reuse_uuid,
                         bool issue_only_one)
{
  gOFS->MgmStats.Add("Eosxd::int::FillContainerCAP", vid.uid, vid.gid, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::FillContainerCAP");
  Caps::authid_set_t duplicated_caps;
  eos_info("ino=%#lx client=%s only-once=%d", id, dir.clientid().c_str(),
           issue_only_one);

  if (issue_only_one) {
    if (EOS_LOGS_DEBUG) {
      eos_debug("checking for id=%s", dir.clientid().c_str());
    }

    // check if the client has already a cap, in case yes, we don't return a new
    // one
    eos::common::RWMutexReadLock lLock(Cap());

    if (Cap().ClientInoCaps().count(dir.clientid())) {
      if (Cap().ClientInoCaps()[dir.clientid()].count(id)) {
        return true;
      }
    }
  } else {
    // avoid to pile-up caps for the same client, delete previous ones
    eos::common::RWMutexReadLock lLock(Cap());

    if (Cap().ClientInoCaps().count(dir.clientid())) {
      if (Cap().ClientInoCaps()[dir.clientid()].count(id)) {
        for (auto it = Cap().ClientInoCaps()[dir.clientid()][id].begin();
             it != Cap().ClientInoCaps()[dir.clientid()][id].end(); ++it) {
          if (*it != reuse_uuid) {
            duplicated_caps.insert(*it);
          }
        }
      }
    }
  }

  dir.mutable_capability()->set_id(id);

  if (EOS_LOGS_DEBUG) {
    eos_debug("container-id=%#llx vid.sudoer %d dir.uid %u name %s", id, vid.sudoer,
              (uid_t) dir.uid(), dir.name().c_str());
  }

  struct timespec ts;

  eos::common::Timing::GetTimeSpec(ts, true);

  size_t leasetime = 0;

  {
    eos::common::RWMutexReadLock lLock(gOFS->zMQ->gFuseServer.Client());
    leasetime = gOFS->zMQ->gFuseServer.Client().leasetime(dir.clientuuid());
    eos_debug("checking client %s leastime=%d", dir.clientid().c_str(),
              leasetime);
  }

  dir.mutable_capability()->set_vtime(ts.tv_sec + (leasetime ? leasetime : 300));
  dir.mutable_capability()->set_vtime_ns(ts.tv_nsec);
  std::string sysmask = (*(dir.mutable_attr()))["sys.mask"];
  long mask = 0777;

  if (sysmask.length()) {
    mask &= strtol(sysmask.c_str(), 0, 8);
  }

  mode_t mode = dir.mode() & S_IFDIR;

  // define the permissions
  if (vid.uid == 0) {
    // grant all permissions
    dir.mutable_capability()->set_mode(0xff | mode);
  } else {
    if (vid.sudoer) {
      mode |= C_OK | M_OK | U_OK | W_OK | D_OK | SA_OK | SU_OK
              ; // chown + chmod permission + all the rest
    }

    if (vid.uid == (uid_t) dir.uid()) {
      // we don't apply a mask if we are the owner
      if (dir.mode() & S_IRUSR) {
        mode |= R_OK | M_OK | SU_OK;
      }

      if (dir.mode() & S_IWUSR) {
        mode |= U_OK | W_OK | D_OK | SA_OK | M_OK | SU_OK;
      }

      if (dir.mode() & mask & S_IXUSR) {
        mode |= X_OK;
      }
    }

    if (vid.gid == (gid_t) dir.gid()) {
      // we apply a mask if we are in the same group
      if (dir.mode() & mask & S_IRGRP) {
        mode |= R_OK;
      }

      if (dir.mode() & mask & S_IWGRP) {
        mode |= U_OK | W_OK | D_OK | SA_OK | M_OK | SU_OK;
      }

      if (dir.mode() & mask & S_IXGRP) {
        mode |= X_OK;
      }
    }

    // we apply a mask if we are matching other permissions
    if (dir.mode() & mask & S_IROTH) {
      mode |= R_OK;
    }

    if (dir.mode() & mask & S_IWOTH) {
      mode |= U_OK | W_OK | D_OK | SA_OK | M_OK | SU_OK;
    }

    if (dir.mode() & mask & S_IXOTH) {
      mode |= X_OK;
    }

    // look at ACLs
    std::string sysacl = (*(dir.mutable_attr()))["sys.acl"];
    std::string useracl = (*(dir.mutable_attr()))["user.acl"];
    std::string shareacl = (*(dir.mutable_attr()))["share.acl"];

    if (EOS_LOGS_DEBUG) {
      eos_debug("name='%s' sysacl='%s' useracl='%s' shareacl='%s' count(sys.eval.useracl)=%d",
                dir.name().c_str(), sysacl.c_str(), useracl.c_str(), shareacl.c_str(),
                dir.attr().count("sys.eval.useracl"));
    }

    if (sysacl.length() || useracl.length(), shareacl.length()) {
      bool evaluseracl = (!S_ISDIR(dir.mode())) ||
                         dir.attr().count("sys.eval.useracl") > 0;
      Acl acl(sysacl,
              useracl,
	      shareacl,
              vid,
              evaluseracl);

      if (EOS_LOGS_DEBUG)
        eos_debug("cap id=%lld name %s evaluseracl %d CanRead %d CanWrite %d CanChmod %d CanChown %d CanUpdate %d CanNotDelete %d",
                  id, dir.name().c_str(), evaluseracl, acl.CanRead(), acl.CanWrite(),
                  acl.CanChmod(), acl.CanChown(),
                  acl.CanUpdate(), acl.CanNotDelete());

      if (acl.IsMutable()) {
        if (acl.CanRead()) {
          mode |= R_OK;
        } else if (acl.CanNotRead()) { /* denials override mode bits */
          mode &= ~R_OK;
        }

        if (acl.CanWrite() || acl.CanWriteOnce()) {
          mode |= W_OK | SA_OK | D_OK | M_OK;
        } else if (acl.CanNotWrite()) { /* denials override mode bits */
          mode &= ~(W_OK | SA_OK | D_OK | M_OK);
        }

        if (acl.CanBrowse()) {
          mode |= X_OK;
        } else if (acl.CanNotBrowse()) {/* denials override mode bits */
          mode &= ~X_OK;
        }

        if (acl.CanNotChmod()) {
          mode &= ~M_OK;
        }

        if (acl.CanChmod()) {
          mode |= M_OK;
        }

        if (acl.CanChown()) {
          mode |= C_OK;
        }

        if (acl.CanUpdate()) {
          mode |= U_OK | SA_OK;
        }

        // the owner can always delete
        if ((vid.uid != (uid_t) dir.uid()) && acl.CanNotDelete()) {
          mode &= ~D_OK;
        }
      }
    }

    if (!gOFS->allow_public_access(dir.fullpath().c_str(), vid)) {
      mode = dir.mode() & S_IFDIR;
      mode |= X_OK;
    }

    dir.mutable_capability()->set_mode(mode);
  }

  std::string ownerauth = (*(dir.mutable_attr()))["sys.owner.auth"];

  // define new target owner
  if (ownerauth.length()) {
    if (ownerauth == "*") {
      // sticky ownership for everybody
      dir.mutable_capability()->set_uid(dir.uid());
      dir.mutable_capability()->set_gid(dir.gid());
    } else {
      ownerauth += ",";
      std::string ownerkey = vid.prot.c_str();
      std::string prot = vid.prot.c_str();
      ownerkey += ":";

      if (prot == "gsi") {
        ownerkey += vid.dn.c_str();
      } else {
        ownerkey += vid.uid_string.c_str();
      }

      if ((ownerauth.find(ownerkey)) != std::string::npos) {
        // sticky ownership for this authentication
        dir.mutable_capability()->set_uid(dir.uid());
        dir.mutable_capability()->set_gid(dir.gid());
      } else {
        // no sticky ownership for this authentication
        dir.mutable_capability()->set_uid(vid.uid);
        dir.mutable_capability()->set_gid(vid.gid);
      }
    }
  } else {
    // no sticky ownership
    dir.mutable_capability()->set_uid(vid.uid);
    dir.mutable_capability()->set_gid(vid.gid);
  }

  dir.mutable_capability()->set_authid(reuse_uuid.length() ?
                                       reuse_uuid : eos::common::StringConversion::random_uuidstring());
  dir.mutable_capability()->set_clientid(dir.clientid());
  dir.mutable_capability()->set_clientuuid(dir.clientuuid());

  // max-filesize settings
  if (dir.attr().count("sys.forced.maxsize")) {
    // dynamic upper file size limit per file
    dir.mutable_capability()->set_max_file_size(strtoull((*
        (dir.mutable_attr()))["sys.forced.maxsize"].c_str(), 0, 10));
  } else {
    // hard-coded upper file size limit per file
    dir.mutable_capability()->set_max_file_size(512ll * 1024ll * 1024ll *
        1024ll); // 512 GB
  }

  std::string space = "default";
  {
    // add quota information
    if (dir.attr().count("sys.forced.space")) {
      space = (*(dir.mutable_attr()))["sys.forced.space"];
    } else {
      if (dir.attr().count("user.forced.space")) {
        space = (*(dir.mutable_attr()))["user.forced.space"];
      }
    }

    // Check if quota is enabled for the current space
    bool has_quota = false;
    long long avail_bytes = 0;
    long long avail_files = 0;
    eos::IContainerMD::id_t quota_inode = 0;

    if (eos::mgm::FsView::gFsView.IsQuotaEnabled(space)) {
      if (!Quota::QuotaByPath(dir.fullpath().c_str(), dir.capability().uid(),
                              dir.capability().gid(), avail_files, avail_bytes,
                              quota_inode)) {
        has_quota = true;
      }
    } else {
      avail_files = std::numeric_limits<long>::max() / 2;
      avail_bytes = std::numeric_limits<long>::max() / 2;
      has_quota = true;
    }

    dir.mutable_capability()->mutable__quota()->set_inode_quota(avail_files);
    dir.mutable_capability()->mutable__quota()->set_volume_quota(avail_bytes);
    dir.mutable_capability()->mutable__quota()->set_quota_inode(quota_inode);

    if (!has_quota) {
      dir.mutable_capability()->mutable__quota()->clear_inode_quota();
      dir.mutable_capability()->mutable__quota()->clear_volume_quota();
      dir.mutable_capability()->mutable__quota()->clear_quota_inode();
    }
  }
  EXEC_TIMING_END("Eosxd::int::FillContainerCAP");
  Cap().Store(dir.capability(), &vid);

  if (duplicated_caps.size()) {
    eos::common::RWMutexWriteLock lLock(Cap());

    for (auto it = duplicated_caps.begin(); it != duplicated_caps.end(); ++it) {
      eos_static_debug("removing duplicated cap %s\n", it->c_str());
      Caps::shared_cap cap = Cap().Get(*it);
      Cap().Remove(cap);
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Validate access persmissions based on a given capability
//------------------------------------------------------------------------------

FuseServer::Caps::shared_cap
Server::ValidateCAP(const eos::fusex::md& md, mode_t mode,
                    eos::common::VirtualIdentity& vid)
{
  errno = 0;
  FuseServer::Caps::shared_cap cap = Cap().GetTS(md.authid());

  // no cap - go away
  if (!cap->id()) {
    eos_static_err("no cap for authid=%s", md.authid().c_str());
    errno = ENOENT;
    return 0;
  }

  // wrong cap - go away
  if ((cap->id() != md.md_ino()) && (cap->id() != md.md_pino())) {
    eos_static_err("wrong cap for authid=%s cap-id=%lx md-ino=%lx md-pino=%lx",
                   md.authid().c_str(), md.md_ino(), md.md_pino());
    errno = EINVAL;
    return 0;
  }

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("cap-mode=%x mode=%x", cap->mode(), mode);
  }

  if ((cap->mode() & mode) == mode) {
    uint64_t now = (uint64_t) time(NULL);

    // leave some margin for revoking
    if (cap->vtime() <= (now + 60)) {
      // cap expired !
      errno = ETIMEDOUT;
      return 0;
    }

    return cap;
  }

  errno = EPERM;
  return 0;
}

//------------------------------------------------------------------------------
// Extract inode from capability
//------------------------------------------------------------------------------

uint64_t
Server::InodeFromCAP(const eos::fusex::md& md)
{
  FuseServer::Caps::shared_cap cap = Cap().GetTS(md.authid());

  // no cap - go away
  if (!cap) {
    if (EOS_LOGS_DEBUG) {
      eos_static_debug("no cap for authid=%s", md.authid().c_str());
    }

    return 0;
  } else {
    if (EOS_LOGS_DEBUG) {
      eos_static_debug("authid=%s cap-ino=%lx", md.authid().c_str(), cap->id());
    }
  }

  return cap->id();
}

//------------------------------------------------------------------------------
// Create a response header string
//------------------------------------------------------------------------------

std::string
Server::Header(const std::string& response)
{
  char hex[9];
  sprintf(hex, "%08x", (int) response.length());
  return std::string("[") + hex + std::string("]");
}

//------------------------------------------------------------------------------
// Validate permissions froa given meta-data object
//------------------------------------------------------------------------------

bool
Server::ValidatePERM(const eos::fusex::md& md, const std::string& mode,
                     eos::common::VirtualIdentity& vid,
                     bool take_lock)
{
  gOFS->MgmStats.Add("Eosxd::int::ValidatePERM", vid.uid, vid.gid, 1);
  EXEC_TIMING_BEGIN("Eosxd::int::ValidatePERM");
  // -------------------------------------------------------------------------------------------------------------
  // - when an MGM was restarted it does not know anymore any client CAPs, but we can fallback to validate
  //   permissions on the fly again
  // -------------------------------------------------------------------------------------------------------------
  eos_info("mode=%s", mode.c_str());
  std::string path;
  shared_ptr<eos::IContainerMD> cmd;
  uint64_t clock = 0;
  bool r_ok = false;
  bool w_ok = false;
  bool x_ok = false;
  bool d_ok = false;
  eos::common::RWMutexReadLock rd_ns_lock;

  if (take_lock) {
    rd_ns_lock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
  }

  try {
    if (S_ISDIR(md.mode())) {
      cmd = gOFS->eosDirectoryService->getContainerMD(md.md_pino(), &clock);
    } else {
      cmd = gOFS->eosDirectoryService->getContainerMD(md.md_pino(), &clock);
    }

    path = gOFS->eosView->getUri(cmd.get());
    // for performance reasons we implement a seperate access control check here, because
    // we want to avoid another id=path translation and unlock lock of the namespace
    eos::IContainerMD::XAttrMap attrmap = cmd->getAttributes();

    if (cmd->access(vid.uid, vid.gid, R_OK)) {
      r_ok = true;
    }

    if (cmd->access(vid.uid, vid.gid, W_OK)) {
      w_ok = true;
      d_ok = true;
    }

    if (cmd->access(vid.uid, vid.gid, X_OK)) {
      x_ok = true;
    }

    // ACL and permission check
    Acl acl(attrmap, vid);
    eos_info("acl=%d r=%d w=%d wo=%d x=%d egroup=%d mutable=%d",
             acl.HasAcl(), acl.CanRead(), acl.CanWrite(), acl.CanWriteOnce(),
             acl.HasAcl(), acl.CanRead(), acl.CanWrite(), acl.CanWriteOnce(),
             acl.CanBrowse(), acl.HasEgroup(), acl.IsMutable());

    // browse permission by ACL
    if (acl.HasAcl()) {
      if (acl.CanWrite()) {
        w_ok = true;
        d_ok = true;
      }

      // write-once excludes updates, also denials
      if (acl.CanNotWrite() || acl.CanWriteOnce()) {
        w_ok = false;
      }

      // deletion might be overwritten/forbidden
      if (acl.CanNotDelete()) {
        d_ok = false;
      }

      // the r/x are added to the posix permissions already set
      if (acl.CanRead()) {
        r_ok |= true;
      }

      if (acl.CanBrowse()) {
        x_ok |= true;
      }

      if (!acl.IsMutable()) {
        w_ok = d_ok = false;
      }
    }
  } catch (eos::MDException& e) {
    eos_err("failed to get directory inode ino=%16x", md.md_pino());
    return false;
  }

  std::string accperm;
  accperm = "R";

  if (r_ok) {
    accperm += "R";
  }

  if (w_ok) {
    accperm += "WCKNV";
  }

  if (d_ok) {
    accperm += "D";
  }

  EXEC_TIMING_END("Eosxd::int::ValidatePERM");

  if (accperm.find(mode) != std::string::npos) {
    eos_info("allow access to ino=%16x request-mode=%s granted-mode=%s",
             md.md_pino(),
             mode.c_str(),
             accperm.c_str()
            );
    return true;
  } else {
    eos_err("reject access to ino=%16x request-mode=%s granted-mode=%s",
            md.md_pino(),
            mode.c_str(),
            accperm.c_str()
           );
    return false;
  }
}

//------------------------------------------------------------------------------
// Prefetch meta-data according to request type
//------------------------------------------------------------------------------

void
Server::prefetchMD(const eos::fusex::md& md)
{
  if (md.operation() == md.GET) {
    Prefetcher::prefetchInodeAndWait(gOFS->eosView, md.md_ino());
  } else if (md.operation() == md.LS) {
    Prefetcher::prefetchInodeWithChildrenAndWait(gOFS->eosView, md.md_ino());
  } else if (md.operation() == md.DELETE) {
    Prefetcher::prefetchInodeWithChildrenAndWait(gOFS->eosView, md.md_pino());

    if (S_ISDIR(md.mode())) {
      Prefetcher::prefetchInodeWithChildrenAndWait(gOFS->eosView, md.md_ino());
    }
  } else if (md.operation() == md.GETCAP) {
    Prefetcher::prefetchInodeAndWait(gOFS->eosView, md.md_ino());
  }
}

//------------------------------------------------------------------------------
// Mark beginning of a flush operation
//------------------------------------------------------------------------------

int
Server::OpBeginFlush(const std::string& id,
                     const eos::fusex::md& md,
                     eos::common::VirtualIdentity& vid,
                     std::string* response,
                     uint64_t* clock)
{
  gOFS->MgmStats.Add("Eosxd::ext::BEGINFLUSH", vid.uid, vid.gid, 1);
  EXEC_TIMING_BEGIN("Eosxd::ext::BEGINFLUSH");
  // this is a flush begin/end indicator
  Flushs().beginFlush(md.md_ino(), md.clientuuid());
  eos::fusex::response resp;
  resp.set_type(resp.NONE);
  resp.SerializeToString(response);
  EXEC_TIMING_END("Eosxd::ext::BEGINFLUSH");
  return 0;
}

//------------------------------------------------------------------------------
// Mark end of a flush operation
//---------------------------------------------------------------------------------*/

int
Server::OpEndFlush(const std::string& id,
                   const eos::fusex::md& md,
                   eos::common::VirtualIdentity& vid,
                   std::string* response,
                   uint64_t* clock)
{
  gOFS->MgmStats.Add("Eosxd::ext::ENDFLUSH", vid.uid, vid.gid, 1);
  EXEC_TIMING_BEGIN("Eosxd::ext::ENDFLUSH");
  Flushs().endFlush(md.md_ino(), md.clientuuid());
  eos::fusex::response resp;
  resp.set_type(resp.NONE);
  resp.SerializeToString(response);
  EXEC_TIMING_END("Eosxd::ext::ENDFLUSH");
  return 0;
}

//------------------------------------------------------------------------------
// Server a meta-data GET or LS operation
//------------------------------------------------------------------------------

int
Server::OpGetLs(const std::string& id,
                const eos::fusex::md& md,
                eos::common::VirtualIdentity& vid,
                std::string* response,
                uint64_t* clock)
{
  if (clock) {
    *clock = 0;
  }

  eos::fusex::container cont;

  if (!eos::common::FileId::IsFileInode(md.md_ino())) {
    eos_info("ino=%lx get-dir", (long) md.md_ino());
    cont.set_type(cont.MDMAP);
    cont.set_ref_inode_(md.md_ino());
    eos::fusex::md_map* mdmap = cont.mutable_md_map_();
    // create the parent entry;
    auto parent = mdmap->mutable_md_map_();
    (*parent)[md.md_ino()].set_md_ino(md.md_ino());
    (*parent)[md.md_ino()].set_clientuuid(md.clientuuid());
    (*parent)[md.md_ino()].set_clientid(md.clientid());
    EXEC_TIMING_BEGIN((md.operation() == md.LS) ? "Eosxd::ext::LS" :
                      "Eosxd::ext::GET");

    if (md.operation() == md.LS) {
      gOFS->MgmStats.Add("Eosxd::ext::LS", vid.uid, vid.gid, 1);
      (*parent)[md.md_ino()].set_operation(md.LS);
    } else {
      gOFS->MgmStats.Add("Eosxd::ext::GET", vid.uid, vid.gid, 1);
    }

    size_t n_attached = 1;
    int retc = 0;

    // retrieve directory meta data
    if (!(retc = FillContainerMD(md.md_ino(), (*parent)[md.md_ino()], vid))) {
      // refresh the cap with the same authid
      FillContainerCAP(md.md_ino(), (*parent)[md.md_ino()], vid,
                       md.authid());

      // store clock
      if (clock) {
        *clock = (*parent)[md.md_ino()].clock();
      }

      if (md.operation() == md.LS) {
        // attach children
        auto map = (*parent)[md.md_ino()].children();
        auto it = map.begin();
        size_t n_caps = 0;
        gOFS->MgmStats.Add("Eosxd::ext::LS-Entry", vid.uid, vid.gid, map.size());

        for (; it != map.end(); ++it) {
          // this is a map by inode
          (*parent)[it->second].set_md_ino(it->second);
          auto child_md = &((*parent)[it->second]);

          if (eos::common::FileId::IsFileInode(it->second)) {
            // this is a file
            FillFileMD(it->second, *child_md, vid);
          } else {
            // we don't fill the LS information for the children, just the MD
            child_md->set_operation(md.GET);
            child_md->set_clientuuid(md.clientuuid());
            child_md->set_clientid(md.clientid());
            FillContainerMD(it->second, *child_md, vid);

            if (n_caps < 16) {
              // skip hidden directories
              if (it->first.substr(0, 1) == ".") {
                // add maximum 16 caps for a listing
                FillContainerCAP(it->second, *child_md, vid, "", true);
                n_caps++;
              }
            }

            child_md->clear_operation();
          }
        }

        n_attached++;

        if (n_attached >= 128) {
          std::string rspstream;
          cont.SerializeToString(&rspstream);

          if (!response) {
            // send parent + first 128 children
            gOFS->zMQ->mTask->reply(id, rspstream);
          } else {
            *response += Header(rspstream);
            response->append(rspstream.c_str(), rspstream.size());
          }

          n_attached = 0;
          cont.Clear();
        }
      }

      if (EOS_LOGS_DEBUG) {
        std::string mdout = dump_message(*mdmap);
        eos_debug("\n%s\n", mdout.c_str());
      }
    } else {
      eos_err("ino=%lx errc=%d", (long) md.md_ino(),
              retc);
      return retc;
    }

    (*parent)[md.md_ino()].clear_operation();

    if (n_attached) {
      // send left-over children
      std::string rspstream;
      cont.SerializeToString(&rspstream);

      if (!response) {
        gOFS->zMQ->mTask->reply(id, rspstream);
      } else {
        *response += Header(rspstream);
        response->append(rspstream.c_str(), rspstream.size());
      }
    }

    EXEC_TIMING_END((md.operation() == md.LS) ? "Eosxd::ext::LS" :
                    "Eosxd::ext::GET");
  } else {
    EXEC_TIMING_BEGIN("Eosxd::ext::GET");
    eos_info("ino=%lx get-file/link", (long) md.md_ino());
    cont.set_type(cont.MD);
    cont.set_ref_inode_(md.md_ino());
    (*cont.mutable_md_()).set_clientuuid(md.clientuuid());
    (*cont.mutable_md_()).set_clientid(md.clientid());
    FillFileMD(md.md_ino(), (*cont.mutable_md_()), vid);

    if (md.attr().count("user.acl") > 0) { /* File has its own ACL */
      if (EOS_LOGS_DEBUG) {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true;
        options.always_print_primitive_fields = true;
        std::string jsonstring;
        google::protobuf::util::MessageToJsonString(cont, &jsonstring, options);
        eos_static_debug("MD GET file-cap ino %#x %s", md.md_ino(), jsonstring.c_str());
      }

      FillContainerCAP(md.md_ino(), (*cont.mutable_md_()), vid, md.authid());

      if (EOS_LOGS_DEBUG)
        eos_info("file-cap issued: id=%lx mode=%x vtime=%lu.%lu uid=%u gid=%u "
                 "client-id=%s auth-id=%s errc=%d", cont.cap_().id(), cont.cap_().mode(),
                 cont.cap_().vtime(),
                 cont.cap_().vtime_ns(), cont.cap_().uid(), cont.cap_().gid(),
                 cont.cap_().clientid().c_str(), cont.cap_().authid().c_str(),
                 cont.cap_().errc());
    }

    std::string rspstream;
    cont.SerializeToString(&rspstream);

    // store clock
    if (clock) {
      *clock = cont.md_().clock();
    }

    if (!response) {
      // send file meta data
      gOFS->zMQ->mTask->reply(id, rspstream);
    } else {
      *response += Header(rspstream);
      *response += rspstream;
    }

    EXEC_TIMING_END("Eosxd::ext::GET");
  }

  return 0;
}



//------------------------------------------------------------------------------
// Server a meta-data SET operation
//------------------------------------------------------------------------------

/*----------------------------------------------------------------------------*/

int
Server::OpSet(const std::string& id,
              const eos::fusex::md& md,
              eos::common::VirtualIdentity& vid,
              std::string* response,
              uint64_t* clock)
{
  gOFS->MgmStats.Add("Eosxd::ext::SET", vid.uid, vid.gid, 1);

  if (!ValidateCAP(md, W_OK | SA_OK, vid)) {
    std::string perm = "W";

    // a CAP might have gone or timed out, let's check again the permissions
    if (((errno == ENOENT) ||
         (errno == EINVAL) ||
         (errno == ETIMEDOUT)) &&
        ValidatePERM(md, perm, vid)) {
      // this can pass on ... permissions are fine
    } else {
      return EPERM;
    }
  }

  if (S_ISDIR(md.mode())) {
    return OpSetDirectory(id, md, vid, response, clock);
  } else if (S_ISREG(md.mode()) || S_ISFIFO(md.mode())) {
    return OpSetFile(id, md, vid, response, clock);
  } else if (S_ISLNK(md.mode())) {
    return OpSetLink(id, md, vid, response, clock);
  }

  return EINVAL;
}



//------------------------------------------------------------------------------
// Server a meta-data SET operation
//------------------------------------------------------------------------------

/*----------------------------------------------------------------------------*/

int
Server::OpSetDirectory(const std::string& id,
                       const eos::fusex::md& md,
                       eos::common::VirtualIdentity& vid,
                       std::string* response,
                       uint64_t* clock)
{
  gOFS->MgmStats.Add("Eosxd::ext::SETDIR", vid.uid, vid.gid, 1);
  EXEC_TIMING_BEGIN("Eosxd::ext::SETDIR");
  uint64_t md_pino = md.md_pino();

  if (!md_pino) {
    // -----------------------------------------------------------------------
    // this can be a creation with an implied capability and the remote inode
    // of the parent directory
    // was not yet send back to the creating client
    // -----------------------------------------------------------------------
    md_pino = InodeFromCAP(md);
  }

  enum set_type {
    CREATE, UPDATE, RENAME, MOVE
  };
  set_type op;
  uint64_t md_ino = 0;
  bool exclusive = false;

  if (md.type() == md.EXCL) {
    exclusive = true;
  }

  eos_info("ino=%lx pin=%lx authid=%s set-dir", (long) md.md_ino(),
           (long) md.md_pino(),
           md.authid().c_str());
  std::shared_ptr<eos::IContainerMD> cmd;
  std::shared_ptr<eos::IContainerMD> pcmd;
  std::shared_ptr<eos::IContainerMD> cpcmd;
  eos::fusex::md mv_md;
  mode_t sgid_mode = 0;
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                     __FILE__);

  try {
    if (md.md_ino() && exclusive) {
      eos_err("ino=%lx exists", (long) md.md_ino());
      return EEXIST;
    }

    if (md.md_ino()) {
      if (md.implied_authid().length()) {
        // this is a create on top of an existing inode
        eos_err("ino=%lx exists implied=%s", (long) md.md_ino(),
                md.implied_authid().c_str());
        return EEXIST;
      }

      op = UPDATE;
      // dir update
      cmd = gOFS->eosDirectoryService->getContainerMD(md.md_ino());
      pcmd = gOFS->eosDirectoryService->getContainerMD(md.md_pino());

      if (!cmd && md.md_ino()) {
        // directory existed but has been deleted
        throw_mdexception(ENOENT, "No such directory : " << md.md_ino());
      }

      if (cmd->getParentId() != md.md_pino()) {
        // this indicates a directory move
        {
          // we have to check that we have write permission on the source parent
          eos::fusex::md source_md;
          source_md.set_md_pino(cmd->getParentId());
          source_md.set_mode(S_IFDIR);
          std::string perm = "W";

          if (!ValidatePERM(source_md, perm, vid, false)) {
            eos_err("source-ino=%lx no write permission on source directory to do mv ino=%lx",
                    cmd->getParentId(),
                    md.md_ino());
            return EPERM;
          }
        }
        op = MOVE;
        // create a broadcast md object with the authid of the source directory,
        // the target is the standard authid for notification
        mv_md.set_authid(md.mv_authid());
        // If the destination exists, we have to remove it if it's empty
        std::shared_ptr<eos::IContainerMD> exist_target_cmd = pcmd->findContainer(
              md.name());
        unsigned long long tree_size = cmd->getTreeSize();

        if (exist_target_cmd) {
          if (exist_target_cmd->getNumFiles() + exist_target_cmd->getNumContainers()) {
            // Fatal error we have to fail that rename
            eos_err("msg=\"failed move, destination exists and not empty\""
                    " name=%s cxid=%08llx", md.name().c_str(), md.md_ino());
            return ENOTEMPTY;
          }

          try {
            // Remove it via the directory service
            eos_info("msg=\"mv delete empty destination\" name=%s cxid=%08llx",
                     md.name().c_str(), md.md_ino());
            pcmd->removeContainer(md.name());
            gOFS->eosDirectoryService->removeContainer(exist_target_cmd.get());
          }  catch (eos::MDException& e) {
            eos_crit("msg=\"got an exception while trying to remove a container"
                     " which we saw before\" name=%s cxid=%08llx",
                     md.name().c_str(), md.md_ino());
          }
        }

        eos_info("msg=\"mv detach source from parent\" moving %lx => %lx",
                 cmd->getParentId(), md.md_pino());
        cpcmd = gOFS->eosDirectoryService->getContainerMD(cmd->getParentId());
        cpcmd->removeContainer(cmd->getName());

        if (gOFS->eosContainerAccounting) {
          gOFS->eosContainerAccounting->RemoveTree(cpcmd.get(), tree_size);
        }

        gOFS->eosView->updateContainerStore(cpcmd.get());
        cmd->setName(md.name());
        pcmd->addContainer(cmd.get());

        if (gOFS->eosContainerAccounting) {
          gOFS->eosContainerAccounting->AddTree(pcmd.get(), tree_size);
        }

        gOFS->eosView->updateContainerStore(pcmd.get());
      }

      if (cmd->getName() != md.name()) {
        // this indicates a directory rename
        op = RENAME;
        eos_info("rename %s=>%s", cmd->getName().c_str(),
                 md.name().c_str());
        gOFS->eosView->renameContainer(cmd.get(), md.name());
      }

      if (cmd->getCUid() != (uid_t)md.uid() /* a chown */ && !vid.sudoer &&
          (uid_t)md.uid() != vid.uid) {
        /* chown is under control of container sys.acl only, if a vanilla user chowns to other than themselves */
        Acl acl;
        eos::IContainerMD::XAttrMap attrmap = cmd->getAttributes();

        if (EOS_LOGS_DEBUG) {
          eos_debug("sysacl '%s' useracl '%s' evaluseracl %d (ignored)",
                    attrmap["sys.acl"].c_str(), attrmap["user.acl"].c_str(),
                    attrmap.count("sys.eval.useracl"));
        }

        acl.SetFromAttrMap(attrmap, vid, NULL, true /* sysacl-only */);

        if (!acl.CanChown()) {
          return EPERM;
        }
      }

      if (pcmd->getMode() & S_ISGID) {
        sgid_mode = S_ISGID;
      }

      md_ino = md.md_ino();
      eos_info("ino=%lx pino=%lx cpino=%lx update-dir",
               (long) md.md_ino(),
               (long) md.md_pino(), (long) cmd->getParentId());
    } else {
      // dir creation
      op = CREATE;
      pcmd = gOFS->eosDirectoryService->getContainerMD(md.md_pino());

      if (md.name().substr(0, strlen(EOS_COMMON_PATH_ATOMIC_FILE_PREFIX)) ==
          EOS_COMMON_PATH_ATOMIC_FILE_PREFIX) {
        eos_err("ino=%lx name=%s atomic path is forbidden as a directory name");
        return EPERM;
      }

      if (exclusive && pcmd->findContainer(md.name())) {
        // O_EXCL set on creation -
        eos_err("ino=%lx name=%s exists", md.md_pino(), md.name().c_str());
        return EEXIST;
      }

      eos::IContainerMD::XAttrMap xattrs = pcmd->getAttributes();
      // test to verify this is the culprit of failing all eosxd system tests in the CI
      // if ( (md.attr().find("user.acl") != md.attr().end()) && (xattrs.find("sys.eval.useracl") == xattrs.end()) ) {
      // return EPERM;
      // }
      cmd = gOFS->eosDirectoryService->createContainer(0);
      cmd->setName(md.name());
      md_ino = cmd->getId();
      pcmd->addContainer(cmd.get());
      eos_info("ino=%lx pino=%lx md-ino=%lx create-dir",
               (long) md.md_ino(),
               (long) md.md_pino(),
               md_ino);

      if (!Cap().Imply(md_ino, md.authid(), md.implied_authid())) {
        eos_err("imply failed for new inode %lx", md_ino);
      }

      // parent attribute inheritance

      for (const auto& elem : xattrs) {
        cmd->setAttribute(elem.first, elem.second);
      }

      sgid_mode = S_ISGID;
    }

    cmd->setName(md.name());
    cmd->setCUid(md.uid());
    cmd->setCGid(md.gid());
    // @todo (apeters): is sgid_mode still needed?
    cmd->setMode(md.mode() | sgid_mode);
    eos::IContainerMD::ctime_t ctime;
    eos::IContainerMD::ctime_t mtime;
    eos::IContainerMD::ctime_t pmtime;
    ctime.tv_sec = md.ctime();
    ctime.tv_nsec = md.ctime_ns();
    mtime.tv_sec = md.mtime();
    mtime.tv_nsec = md.mtime_ns();
    pmtime.tv_sec = mtime.tv_sec;
    pmtime.tv_nsec = mtime.tv_nsec;
    cmd->setCTime(ctime);
    cmd->setMTime(mtime);
    // propagate mtime changes
    cmd->notifyMTimeChange(gOFS->eosDirectoryService);

    for (auto it = md.attr().begin(); it != md.attr().end(); ++it) {
      if ((it->first.substr(0, 3) != "sys") ||
          (it->first == "sys.eos.btime")) {
        cmd->setAttribute(it->first, it->second);
      }
    }

    size_t numAttr = cmd->numAttributes();

    if (op != CREATE &&
        numAttr != md.attr().size()) { /* an attribute got removed */
      eos::IContainerMD::XAttrMap cmap = cmd->getAttributes();

      for (auto it = cmap.begin(); it != cmap.end(); ++it) {
        if (md.attr().find(it->first) == md.attr().end()) {
          eos_debug("attr %s=%s has been removed", it->first.c_str(),
                    it->second.c_str());
          cmd->removeAttribute(it->first);
          /* if ((--numAttr) == md.attr().size()) break;   would be possible - under a lock! */
        }
      }
    }

    if (op == CREATE) {
      // store the birth time as an extended attribute
      char btime[256];
      snprintf(btime, sizeof(btime), "%lu.%lu", md.btime(), md.btime_ns());
      cmd->setAttribute("sys.eos.btime", btime);
      cmd->setAttribute("sys.vtrace", vid.getTrace());
    }

    if (op != UPDATE && md.pmtime()) {
      // store the new modification time for the parent
      pmtime.tv_sec = md.pmtime();
      pmtime.tv_nsec = md.pmtime_ns();
      pcmd->setMTime(pmtime);
      gOFS->eosDirectoryService->updateStore(pcmd.get());
      pcmd->notifyMTimeChange(gOFS->eosDirectoryService);
    }

    gOFS->eosDirectoryService->updateStore(cmd.get());
    // release the namespace lock before seralization/broadcasting
    lock.Release();
    eos::fusex::response resp;
    resp.set_type(resp.ACK);
    resp.mutable_ack_()->set_code(resp.ack_().OK);
    resp.mutable_ack_()->set_transactionid(md.reqid());
    resp.mutable_ack_()->set_md_ino(md_ino);
    resp.SerializeToString(response);
    uint64_t clock = 0;

    switch (op) {
    case MOVE:
      gOFS->MgmStats.Add("Eosxd::ext::MV", vid.uid, vid.gid, 1);
      break;

    case UPDATE:
      gOFS->MgmStats.Add("Eosxd::ext::UPDATE", vid.uid, vid.gid, 1);
      break;

    case CREATE:
      gOFS->MgmStats.Add("Eosxd::ext::MKDIR", vid.uid, vid.gid, 1);
      break;

    case RENAME:
      gOFS->MgmStats.Add("Eosxd::ext::RENAME", vid.uid, vid.gid, 1);
      break;
    }

    // broadcast this update around
    switch (op) {
    case CREATE:
      Cap().BroadcastMD(md, md_ino, md.md_pino(), clock, pmtime);
      break;

    case MOVE:
      Cap().BroadcastRelease(mv_md);

    case UPDATE:
    case RENAME:
      Cap().BroadcastRelease(md);
      Cap().BroadcastRefresh(md.md_ino(), md, md.md_pino());
      break;
    }
  } catch (eos::MDException& e) {
    eos_err("ino=%lx err-no=%d err-msg=%s", (long) md.md_ino(),
            e.getErrno(), e.getMessage().str().c_str());
    eos::fusex::response resp;
    resp.set_type(resp.ACK);
    resp.mutable_ack_()->set_code(resp.ack_().PERMANENT_FAILURE);
    resp.mutable_ack_()->set_err_no(e.getErrno());
    resp.mutable_ack_()->set_err_msg(e.getMessage().str().c_str());
    resp.mutable_ack_()->set_transactionid(md.reqid());
    resp.SerializeToString(response);
  }

  EXEC_TIMING_END("Eosxd::ext::SETDIR");
  return 0;
}


//------------------------------------------------------------------------------
// Server a meta-data SET operation
//------------------------------------------------------------------------------

/*----------------------------------------------------------------------------*/
bool
Server::CheckRecycleBinOrVersion(std::shared_ptr<eos::IFileMD> fmd)
{
  std::string path = gOFS->eosView->getUri(fmd.get());
  return (Recycle::InRecycleBin(path) || eos::common::Path::IsVersion(path));
}


//------------------------------------------------------------------------------
// Server a meta-data SET operation
//------------------------------------------------------------------------------

/*----------------------------------------------------------------------------*/
int
Server::OpSetFile(const std::string& id,
                  const eos::fusex::md& md,
                  eos::common::VirtualIdentity& vid,
                  std::string* response,
                  uint64_t* clock)
{
  gOFS->MgmStats.Add("Eosxd::ext::SETFILE", vid.uid, vid.gid, 1);
  EXEC_TIMING_BEGIN("Eosxd::ext::SETFILE");
  enum set_type {
    CREATE, UPDATE, RENAME, MOVE
  };
  set_type op;
  uint64_t md_ino = 0;
  bool exclusive = false;

  if (md.type() == md.EXCL) {
    exclusive = true;
  }

  eos_info("ino=%lx pin=%lx authid=%s file", (long) md.md_ino(),
           (long) md.md_pino(),
           md.authid().c_str());
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                     __FILE__);
  std::shared_ptr<eos::IFileMD> fmd;
  std::shared_ptr<eos::IFileMD> ofmd;
  std::shared_ptr<eos::IContainerMD> pcmd;
  std::shared_ptr<eos::IContainerMD> cpcmd;
  uint64_t fid = eos::common::FileId::InodeToFid(md.md_ino());
  md_ino = md.md_ino();
  uint64_t md_pino = md.md_pino();
  bool recycleOrVersioned = false;

  try {
    uint64_t clock = 0;
    pcmd = gOFS->eosDirectoryService->getContainerMD(md.md_pino());

    if (md_ino && exclusive) {
      return EEXIST;
    }

    if (md_ino) {
      fs_rd_lock.Release();
      // file update
      op = UPDATE;
      // dir update
      fmd = gOFS->eosFileService->getFileMD(fid);

      if (!fmd && md_ino) {
        // file existed but has been deleted
        throw_mdexception(ENOENT, "No such file : " << md_ino);
      }

      if (EOS_LOGS_DEBUG) eos_debug("updating %s => %s ",
                                      fmd->getName().c_str(),
                                      md.name().c_str());

      if (fmd->getContainerId() != md.md_pino()) {
        recycleOrVersioned = CheckRecycleBinOrVersion(fmd);
      }

      if (!recycleOrVersioned) {
        if (fmd->getContainerId() != md.md_pino()) {
          // this indicates a file move
          op = MOVE;
          bool hasVersion = false;

          if (EOS_LOGS_DEBUG) {
            eos_debug("moving %lx => %lx", fmd->getContainerId(), md.md_pino());
          }

          eos::common::Path oPath(gOFS->eosView->getUri(fmd.get()).c_str());
          std::string vdir = EOS_COMMON_PATH_VERSION_FILE_PREFIX;
          vdir += oPath.GetName();
          cpcmd = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());

          if (cpcmd->findContainer(vdir)) {
            eos_static_info("%s has version", vdir.c_str());
            hasVersion = true;
          }

          cpcmd->removeFile(fmd->getName());
          cpcmd = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
          gOFS->eosView->updateContainerStore(cpcmd.get());
          fmd->setName(md.name());
          ofmd = pcmd->findFile(md.name());

          if (ofmd) {
            // the target might exist, so we remove it
            if (EOS_LOGS_DEBUG) {
              eos_debug("removing previous file in move %s", md.name().c_str());
            }

            eos::IContainerMD::XAttrMap attrmap = pcmd->getAttributes();
            // check if there is versioning to be done
            int versioning = 0;

            if (attrmap.count("user.fusex.rename.version")) {
              if (attrmap.count("sys.versioning")) {
                versioning = std::stoi(attrmap["sys.versioning"]);
              } else {
                if (attrmap.count("user.versioning")) {
                  versioning = std::stoi(attrmap["user.versioning"]);
                }
              }
            }

            bool try_recycle = true;
            bool created_version = false;

            // create a version before replacing
            if (versioning && !hasVersion) {
              XrdOucErrInfo error;
              lock.Release();
              eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();

              if (gOFS->Version(ofmd->getId(), error, rootvid, versioning)) {
                try_recycle = true;
              } else {
                try_recycle = false;
                created_version = true;
              }

              lock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
            } else {
              // recycle bin - not for hardlinked files or hardlinks!
              if ((try_recycle &&
                   (attrmap.count(Recycle::gRecyclingAttribute) || hasVersion)) ||
                  ofmd->hasAttribute(k_mdino) || ofmd->hasAttribute(k_nlink)) {
                // translate to a path name and call the complex deletion function
                // this is vulnerable to a hard to trigger race conditions
                std::string fullpath = gOFS->eosView->getUri(ofmd.get());
                gOFS->WriteRecycleRecord(ofmd);
                lock.Release();
                XrdOucErrInfo error;
                (void) gOFS->_rem(fullpath.c_str(), error, vid, "",
                                  false, false, false, true, false);
                lock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);;
              } else {
                if (!created_version) {
                  // no recycle bin, no version
                  try {
                    XrdOucErrInfo error;

                    if (XrdMgmOfsFile::create_cow(XrdMgmOfsFile::cowDelete, pcmd, ofmd, vid,
                                                  error) == -1) {
                      pcmd->removeFile(md.name());
                      // unlink the existing file
                      ofmd->setContainerId(0);
                      ofmd->unlinkAllLocations();
                    }

                    eos::IQuotaNode* quotanode = gOFS->eosView->getQuotaNode(pcmd.get());

                    // free previous quota
                    if (quotanode) {
                      quotanode->removeFile(ofmd.get());
                    }

                    gOFS->eosFileService->updateStore(ofmd.get());
                  } catch (eos::MDException& e) {
                  }
                }
              }
            }
          }

          pcmd->addFile(fmd.get());
          gOFS->eosView->updateFileStore(fmd.get());
          gOFS->eosView->updateContainerStore(pcmd.get());

          if (hasVersion) {
            eos::common::Path nPath(gOFS->eosView->getUri(fmd.get()).c_str());
            lock.Release();
            XrdOucErrInfo error;

            if (gOFS->_rename(oPath.GetVersionDirectory(), nPath.GetVersionDirectory(),
                              error, vid, "", "", false, false, false)) {
              eos_err("failed to rename version directory '%s'=>'%s'",
                      oPath.GetVersionDirectory(), nPath.GetVersionDirectory());
            }

            lock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
          }
        } else {
          if (fmd->getName() != md.name()) {
            // this indicates a file rename
            op = RENAME;
            bool hasVersion = false;
            ofmd = pcmd->findFile(md.name());

            if (EOS_LOGS_DEBUG) eos_debug("rename %s [%lx] => %s [%lx]",
                                            fmd->getName().c_str(), fid,
                                            md.name().c_str(),
                                            ofmd ? ofmd->getId() : 0);

            eos::common::Path oPath(gOFS->eosView->getUri(fmd.get()).c_str());
            std::string vdir = EOS_COMMON_PATH_VERSION_FILE_PREFIX;
            vdir += oPath.GetName();

            if (pcmd->findContainer(vdir)) {
              hasVersion = true;
            }

            if (EOS_LOGS_DEBUG) {
              eos_debug("v=%s version=%d exists=%d", vdir.c_str(), hasVersion, ofmd ? 1 : 0);
            }

            if (ofmd) {
              // the target might exist, so we remove it
              if (EOS_LOGS_DEBUG) {
                eos_debug("removing previous file in update %s", md.name().c_str());
              }

              eos::IContainerMD::XAttrMap attrmap = pcmd->getAttributes();
              // check if there is versioning to be done
              int versioning = 0;

              if (attrmap.count("user.fusex.rename.version")) {
                if (attrmap.count("sys.versioning")) {
                  versioning = std::stoi(attrmap["sys.versioning"]);
                } else {
                  if (attrmap.count("user.versioning")) {
                    versioning = std::stoi(attrmap["user.versioning"]);
                  }
                }
              }

              bool try_recycle = true;
              bool created_version = false;

              // create a version before replacing
              if (versioning && !hasVersion) {
                XrdOucErrInfo error;
                lock.Release();
                eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();

                if (gOFS->Version(ofmd->getId(), error, rootvid, versioning)) {
                  try_recycle = true;
                } else {
                  try_recycle = false;
                  created_version = true;
                }

                if (EOS_LOGS_DEBUG) {
                  eos_debug("tried versioning - try_recycle=%d", try_recycle);
                }

                lock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
              }

              // recycle bin - not for hardlinked files or hardlinks !
              if ((try_recycle &&
                   (attrmap.count(Recycle::gRecyclingAttribute) || hasVersion)) ||
                  ofmd->hasAttribute(k_mdino) || ofmd->hasAttribute(k_nlink)) {
                // translate to a path name and call the complex deletion function
                // this is vulnerable to a hard to trigger race conditions
                std::string fullpath = gOFS->eosView->getUri(ofmd.get());
                gOFS->WriteRecycleRecord(ofmd);
                lock.Release();
                XrdOucErrInfo error;
                (void) gOFS->_rem(fullpath.c_str(), error, vid, "", false, false,
                                  false, true, false);
                lock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
              } else {
                if (!created_version) {
                  try {
                    XrdOucErrInfo error;

                    if (XrdMgmOfsFile::create_cow(XrdMgmOfsFile::cowDelete, pcmd, ofmd, vid,
                                                  error) == -1) {
                      pcmd->removeFile(md.name());
                      // unlink the existing file
                      ofmd->setContainerId(0);
                      ofmd->unlinkAllLocations();
                    }

                    eos::IQuotaNode* quotanode = gOFS->eosView->getQuotaNode(pcmd.get());

                    // free previous quota
                    if (quotanode) {
                      quotanode->removeFile(ofmd.get());
                    }

                    gOFS->eosFileService->updateStore(ofmd.get());
                  } catch (eos::MDException& e) {
                  }
                }
              }
            }

            gOFS->eosView->renameFile(fmd.get(), md.name());

            if (hasVersion) {
              eos::common::Path nPath(gOFS->eosView->getUri(fmd.get()).c_str());
              lock.Release();
              XrdOucErrInfo error;

              if (gOFS->_rename(oPath.GetVersionDirectory(), nPath.GetVersionDirectory(),
                                error, vid, "", "", false, false, false)) {
                eos_err("failed to rename version directory '%s'=>'%s'\n",
                        oPath.GetVersionDirectory(), nPath.GetVersionDirectory());
              }

              lock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
            }
          }
        }
      }

      if (EOS_LOGS_DEBUG) {
        eos_debug("vid.sudoer %d vid.uid %u md.uid() %u fmd->getCUid() %u", vid.sudoer,
                  vid.uid, (uid_t)md.uid(), fmd->getCUid());
      }

      if (fmd->getCUid() != (uid_t)md.uid() /* a chown */ && !vid.sudoer &&
          (uid_t)md.uid() != vid.uid) {
        /* chown is under control of container sys.acl only, if a vanilla user chowns to other than themselves */
        Acl acl;
        eos::IContainerMD::XAttrMap attrmap = pcmd->getAttributes();

        if (EOS_LOGS_DEBUG) {
          eos_debug("sysacl '%s' useracl '%s' (ignored) evaluseracl %d",
                    attrmap["sys.acl"].c_str(), attrmap["user.acl"].c_str(),
                    attrmap.count("sys.eval.useracl"));
        }

        acl.SetFromAttrMap(attrmap, vid, NULL, true /* sysacl-only */);

        if (!acl.CanChown()) {
          return EPERM;
        }
      }

      eos_info("fid=%08llx ino=%lx pino=%lx cpino=%lx update-file",
               (long) fid,
               (long) md.md_ino(),
               (long) md.md_pino(), (long) fmd->getContainerId());
    } else if (strncmp(md.target().c_str(), "////hlnk",
                       8) == 0) {  /* hard link creation */
      fs_rd_lock.Release();
      uint64_t tgt_md_ino = atoll(md.target().c_str() + 8);

      if (pcmd->findContainer(md.name())) {
        return EEXIST;
      }

      /* fmd is the target file corresponding to tgt_fid, gmd the file corresponding to new name */
      fmd = gOFS->eosFileService->getFileMD(eos::common::FileId::InodeToFid(
                                              tgt_md_ino));
      std::shared_ptr<eos::IFileMD> gmd = gOFS->eosFileService->createFile(0);
      int nlink;
      nlink = (fmd->hasAttribute(k_nlink)) ? std::stoi(fmd->getAttribute(
                k_nlink)) + 1 : 1;

      if (EOS_LOGS_DEBUG) {
        eos_debug("hlnk fid=%08llx target name %s nlink %d create hard link %s",
                  (long)fid, fmd->getName().c_str(), nlink, md.name().c_str());
      }

      fmd->setAttribute(k_nlink, std::to_string(nlink));
      gOFS->eosFileService->updateStore(fmd.get());
      gmd->setAttribute(k_mdino, std::to_string(tgt_md_ino));
      gmd->setName(md.name());

      if (EOS_LOGS_DEBUG)
        eos_debug("hlnk %s mdino %s %s nlink %s",
                  gmd->getName().c_str(), gmd->getAttribute(k_mdino).c_str(),
                  fmd->getName().c_str(), fmd->getAttribute(k_nlink).c_str());

      pcmd->addFile(gmd.get());
      gOFS->eosFileService->updateStore(gmd.get());
      gOFS->eosView->updateContainerStore(pcmd.get());
      eos::fusex::response resp;
      resp.set_type(resp.ACK);
      resp.mutable_ack_()->set_code(resp.ack_().OK);
      resp.mutable_ack_()->set_transactionid(md.reqid());
      resp.mutable_ack_()->set_md_ino(eos::common::FileId::FidToInode(gmd->getId()));
      // prepare to broadcast the new hardlink around, need to create an md object with the hardlink
      eos::fusex::md g_md;
      uint64_t g_ino = eos::common::FileId::FidToInode(gmd->getId());
      lock.Release();
      Prefetcher::prefetchInodeAndWait(gOFS->eosView, g_ino);
      FillFileMD(g_ino, g_md, vid);
      // release the namespace lock before serialization/broadcasting
      resp.SerializeToString(response);
      struct timespec pt_mtime;
      pt_mtime.tv_sec = md.mtime();
      pt_mtime.tv_nsec = md.mtime_ns();
      gOFS->eosDirectoryService->updateStore(pcmd.get());
      uint64_t clock = 0;
      Cap().BroadcastMD(md, tgt_md_ino, md_pino, clock, pt_mtime);
      Cap().BroadcastMD(g_md, g_ino, md_pino, clock, pt_mtime);
      return 0;
    } else {
      // file creation
      op = CREATE;

      if (md.name().substr(0, strlen(EOS_COMMON_PATH_ATOMIC_FILE_PREFIX)) ==
          EOS_COMMON_PATH_ATOMIC_FILE_PREFIX) {
        eos_err("name=%s atomic path is forbidden as a filename",
                md.name().c_str());
        return EPERM;
      }

      if (pcmd->findContainer(
            md.name())) {
        return EEXIST;
      }

      unsigned long layoutId = 0;
      unsigned long forcedFsId = 0;
      long forcedGroup = 0;
      XrdOucString space;
      eos::IContainerMD::XAttrMap attrmap = pcmd->getAttributes();
      XrdOucEnv env;
      // retrieve the layout
      Policy::GetLayoutAndSpace("fusex", attrmap, vid, layoutId, space, env,
                                forcedFsId, forcedGroup, false);
      fs_rd_lock.Release();

      if (eos::mgm::FsView::gFsView.IsQuotaEnabled(space.c_str())) {
        // check inode quota here
        long long avail_bytes = 0;
        long long avail_files = 0;

        try {
          eos::IQuotaNode* quotanode = gOFS->eosView->getQuotaNode(pcmd.get());

          if (quotanode) {
            if (!Quota::QuotaBySpace(quotanode->getId(),
                                     vid.uid,
                                     vid.gid,
                                     avail_files,
                                     avail_bytes)) {
              if (!avail_files) {
                eos_err("name=%s out-of-inode-quota uid=%u gid=%u",
                        md.name().c_str(),
                        vid.uid,
                        vid.gid);
                return EDQUOT;
              }
            }
          }
        } catch (eos::MDException& e) {
        }
      }

      fmd = gOFS->eosFileService->createFile(0);
      fmd->setName(md.name());
      fmd->setLayoutId(layoutId);
      md_ino = eos::common::FileId::FidToInode(fmd->getId());
      pcmd->addFile(fmd.get());
      eos_info("ino=%lx pino=%lx md-ino=%lx create-file", (long) md_ino,
               (long) md.md_pino(), md_ino);
      // store the birth time as an extended attribute
      char btime[256];
      snprintf(btime, sizeof(btime), "%lu.%lu", md.btime(), md.btime_ns());
      fmd->setAttribute("sys.eos.btime", btime);
      fmd->setAttribute("sys.vtrace", vid.getTrace());
    }

    if (!recycleOrVersioned) {
      fmd->setName(md.name());
    }

    fmd->setCUid(md.uid());
    fmd->setCGid(md.gid());
    {
      try {
        eos::IQuotaNode* quotanode = gOFS->eosView->getQuotaNode(pcmd.get());

        // free previous quota
        if (quotanode) {
          if (op != CREATE) {
            quotanode->removeFile(fmd.get());
          }

          fmd->setSize(md.size());
          quotanode->addFile(fmd.get());
        } else {
          fmd->setSize(md.size());
        }
      } catch (eos::MDException& e) {
        fmd->setSize(md.size());
      }
    }
    // for the moment we store 9 bits here
    fmd->setFlags(md.mode() & (S_IRWXU | S_IRWXG | S_IRWXO));
    eos::IFileMD::ctime_t ctime;
    eos::IFileMD::ctime_t mtime;
    ctime.tv_sec = md.ctime();
    ctime.tv_nsec = md.ctime_ns();
    mtime.tv_sec = md.mtime();
    mtime.tv_nsec = md.mtime_ns();
    fmd->setCTime(ctime);
    fmd->setMTime(mtime);
    replaceNonSysAttributes(fmd, md);
    struct timespec pt_mtime;

    if (op != UPDATE) {
      // update the mtime
      pcmd->setMTime(mtime);
      pt_mtime.tv_sec = mtime.tv_sec;
      pt_mtime.tv_nsec = mtime.tv_nsec;
    } else {
      pt_mtime.tv_sec = pt_mtime.tv_nsec = 0;
    }

    gOFS->eosFileService->updateStore(fmd.get());

    if (op != UPDATE) {
      // update the mtime
      gOFS->eosDirectoryService->updateStore(pcmd.get());
    }

    // retrieve the clock
    fmd = gOFS->eosFileService->getFileMD(eos::common::FileId::InodeToFid(md_ino),
                                          &clock);

    if (op == CREATE) {
      gOFS->mReplicationTracker->Create(fmd);
    }

    eos_info("ino=%llx clock=%llx", md_ino, clock);
    // release the namespace lock before serialization/broadcasting
    lock.Release();
    eos::fusex::response resp;
    resp.set_type(resp.ACK);
    resp.mutable_ack_()->set_code(resp.ack_().OK);
    resp.mutable_ack_()->set_transactionid(md.reqid());
    resp.mutable_ack_()->set_md_ino(md_ino);
    resp.SerializeToString(response);

    switch (op) {
    case MOVE:
      gOFS->MgmStats.Add("Eosxd::ext::MV", vid.uid, vid.gid, 1);
      break;

    case UPDATE:
      gOFS->MgmStats.Add("Eosxd::ext::UPDATE", vid.uid, vid.gid, 1);
      break;

    case CREATE:
      gOFS->MgmStats.Add("Eosxd::ext::CREATE", vid.uid, vid.gid, 1);
      break;

    case RENAME:
      gOFS->MgmStats.Add("Eosxd::ext::RENAME", vid.uid, vid.gid, 1);
      break;
    }

    // broadcast this update around
    switch (op) {
    case UPDATE:
    case CREATE:
    case RENAME:
    case MOVE:
      Cap().BroadcastMD(md, md_ino, md_pino, clock, pt_mtime);
      break;
    }
  } catch (eos::MDException& e) {
    eos_err("ino=%lx err-no=%d err-msg=%s", (long) md.md_ino(),
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

  EXEC_TIMING_END("Eosxd::ext::SETFILE");
  return 0;
}


//------------------------------------------------------------------------------
// Server a meta-data SET operation
//------------------------------------------------------------------------------/*----------------------------------------------------------------------------*/

int
Server::OpSetLink(const std::string& id,
                  const eos::fusex::md& md,
                  eos::common::VirtualIdentity& vid,
                  std::string* response,
                  uint64_t* clock)
{
  gOFS->MgmStats.Add("Eosxd::ext::SETLNK", vid.uid, vid.gid, 1);
  EXEC_TIMING_BEGIN("Eosxd::ext::SETLNK");
  enum set_type {
    CREATE, UPDATE, RENAME, MOVE
  };
  set_type op;
  uint64_t md_ino = 0;
  bool exclusive = false;

  if (md.type() == md.EXCL) {
    exclusive = true;
  }

  eos_info("ino=%#lx %s", (long) md.md_ino(),
           md.name().c_str());
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                     __FILE__);
  std::shared_ptr<eos::IFileMD> fmd;
  std::shared_ptr<eos::IFileMD> ofmd;
  std::shared_ptr<eos::IContainerMD> pcmd;
  std::shared_ptr<eos::IContainerMD> opcmd;
  uint64_t md_pino = md.md_pino();

  try {
    // link creation/update
    pcmd = gOFS->eosDirectoryService->getContainerMD(md.md_pino());
    fmd = md.md_ino() ? gOFS->eosFileService->getFileMD(
            eos::common::FileId::InodeToFid(md.md_ino())) : 0;

    if (!fmd && md.md_ino()) {
      // file existed but has been deleted
      throw_mdexception(ENOENT, "No such file : " << md_ino);
    }

    if (fmd && exclusive) {
      return EEXIST;
    }

    if (fmd) {
      // link MD update
      op = UPDATE;

      if (fmd->getContainerId() != md.md_pino()) {
        op = MOVE;
        eos_info("op=MOVE ino=%#lx %s=>%s", (long) md.md_ino(), fmd->getName().c_str(),
                 md.name().c_str());
        opcmd = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
        // remove symlink from current parent
        opcmd->removeFile(fmd->getName());
        gOFS->eosView->updateContainerStore(opcmd.get());
        // update the name
        fmd->setName(md.name());
        // check if target exists
        ofmd = pcmd->findFile(md.name());

        if (ofmd) {
          // remove the target - no recycle bin for symlinks
          try {
            XrdOucErrInfo error;
            pcmd->removeFile(md.name());
            // unlink the existing file
            ofmd->setContainerId(0);
            ofmd->unlinkAllLocations();
            eos::IQuotaNode* quotanode = gOFS->eosView->getQuotaNode(pcmd.get());

            // free previous quota
            if (quotanode) {
              quotanode->removeFile(ofmd.get());
            }

            gOFS->eosFileService->updateStore(ofmd.get());
            gOFS->eosView->updateContainerStore(opcmd.get());
          } catch (eos::MDException& e) {
          }
        }

        eos::IQuotaNode* quotanode = gOFS->eosView->getQuotaNode(opcmd.get());

        // free quota in old parent, will be added to new parent
        if (quotanode) {
          quotanode->removeFile(fmd.get());
        }
      } else {
        if (fmd->getName() != md.name()) {
          op = RENAME;
          eos_info("op=RENAME ino=%#lx %s=>%s", (long) md.md_ino(),
                   fmd->getName().c_str(), md.name().c_str());
          // check if target exists
          ofmd = pcmd->findFile(md.name());

          if (ofmd) {
            // remove the target - no recycle bin for symlink rename
            try {
              XrdOucErrInfo error;
              pcmd->removeFile(md.name());
              // unlink the existing file
              ofmd->setContainerId(0);
              ofmd->unlinkAllLocations();
              eos::IQuotaNode* quotanode = gOFS->eosView->getQuotaNode(pcmd.get());

              // free previous quota
              if (quotanode) {
                quotanode->removeFile(ofmd.get());
              }

              gOFS->eosFileService->updateStore(ofmd.get());
            } catch (eos::MDException& e) {
            }
          }

          // call the rename function
          gOFS->eosView->renameFile(fmd.get(), md.name());
        }
      }
    } else {
      op = CREATE;
      eos_info("op=CREATE ino=%#lx %s", (long) md.md_ino(), md.name().c_str());

      if (md.name().substr(0, strlen(EOS_COMMON_PATH_ATOMIC_FILE_PREFIX)) ==
          EOS_COMMON_PATH_ATOMIC_FILE_PREFIX) {
        eos_err("ino=%lx name=%s atomic path is forbidden as a link/fifo name");
        return EPERM;
      }

      fmd = pcmd->findFile(md.name());

      if (fmd && exclusive) {
        return EEXIST;
      }

      fmd = gOFS->eosFileService->createFile(0);
    }

    switch (op) {
    case MOVE:
      gOFS->MgmStats.Add("Eosxd::ext::MV", vid.uid, vid.gid, 1);
      break;

    case UPDATE:
      gOFS->MgmStats.Add("Eosxd::ext::UPDATE", vid.uid, vid.gid, 1);
      break;

    case CREATE:
      gOFS->MgmStats.Add("Eosxd::ext::CREATELNK", vid.uid, vid.gid, 1);
      break;

    case RENAME:
      gOFS->MgmStats.Add("Eosxd::ext::RENAME", vid.uid, vid.gid, 1);
      break;
    }

    fmd->setName(md.name());
    fmd->setLink(md.target());
    fmd->setLayoutId(0);
    md_ino = eos::common::FileId::FidToInode(fmd->getId());
    eos_info("ino=%lx pino=%lx md-ino=%lx create-link", (long) md.md_ino(),
             (long) md.md_pino(), md_ino);
    fmd->setCUid(md.uid());
    fmd->setCGid(md.gid());
    fmd->setSize(md.target().length());
    fmd->setFlags(md.mode() & (S_IRWXU | S_IRWXG | S_IRWXO));
    eos::IFileMD::ctime_t ctime;
    eos::IFileMD::ctime_t mtime;
    ctime.tv_sec = md.ctime();
    ctime.tv_nsec = md.ctime_ns();
    mtime.tv_sec = md.mtime();
    mtime.tv_nsec = md.mtime_ns();
    fmd->setCTime(ctime);
    fmd->setMTime(mtime);
    replaceNonSysAttributes(fmd, md);

    if ((op == CREATE) || (op == MOVE)) {
      pcmd->addFile(fmd.get());
      eos::IQuotaNode* quotanode = gOFS->eosView->getQuotaNode(pcmd.get());

      // add inode quota
      if (quotanode) {
        quotanode->addFile(fmd.get());
      }
    }

    if (op == CREATE) {
      // store the birth time as an extended attribute
      char btime[256];
      snprintf(btime, sizeof(btime), "%lu.%lu", md.btime(), md.btime_ns());
      fmd->setAttribute("sys.eos.btime", btime);
      fmd->setAttribute("sys.vtrace", vid.getTrace());
    }

    struct timespec pt_mtime;

    pcmd->setMTime(ctime);

    pt_mtime.tv_sec = ctime.tv_sec;

    pt_mtime.tv_nsec = ctime.tv_nsec;

    gOFS->eosFileService->updateStore(fmd.get());

    gOFS->eosDirectoryService->updateStore(pcmd.get());

    // release the namespace lock before serialization/broadcasting
    lock.Release();

    eos::fusex::response resp;

    resp.set_type(resp.ACK);

    resp.mutable_ack_()->set_code(resp.ack_().OK);

    resp.mutable_ack_()->set_transactionid(md.reqid());

    resp.mutable_ack_()->set_md_ino(md_ino);

    resp.SerializeToString(response);

    uint64_t bclock = 0;

    Cap().BroadcastMD(md, md_ino, md_pino, bclock, pt_mtime);
  } catch (eos::MDException& e) {
    eos_err("ino=%lx err-no=%d err-msg=%s", (long) md.md_ino(),
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

  EXEC_TIMING_END("Eosxd::ext::SETLNK");
  return 0;
}


//------------------------------------------------------------------------------
// Serve a meta-data DELETE operation
//-------------------------------------------------------------------------------*/

int
Server::OpDelete(const std::string& id,
                 const eos::fusex::md& md,
                 eos::common::VirtualIdentity& vid,
                 std::string* response,
                 uint64_t* clock)
{
  gOFS->MgmStats.Add("Eosxd::ext::RM", vid.uid, vid.gid, 1);

  if (!ValidateCAP(md, D_OK, vid)) {
    std::string perm = "D";

    // a CAP might have gone or timedout, let's check again the permissions
    if (((errno == ENOENT) ||
         (errno == EINVAL) ||
         (errno == ETIMEDOUT)) &&
        ValidatePERM(md, perm, vid)) {
      // this can pass on ... permissions are fine
    } else {
      eos_err("ino=%lx delete has wrong cap");
      return EPERM;
    }
  }

  if (S_ISDIR(md.mode())) {
    return OpDeleteDirectory(id, md, vid, response, clock);
  } else if (S_ISREG(md.mode()) || S_ISFIFO(md.mode())) {
    return OpDeleteFile(id, md, vid, response, clock);
  } else if (S_ISLNK(md.mode())) {
    return OpDeleteLink(id, md, vid, response, clock);
  }

  return EINVAL;
}

//------------------------------------------------------------------------------
// Serve a meta-data DELETE operation
//-------------------------------------------------------------------------------*/

int
Server::OpDeleteDirectory(const std::string& id,
                          const eos::fusex::md& md,
                          eos::common::VirtualIdentity& vid,
                          std::string* response,
                          uint64_t* clock)
{
  gOFS->MgmStats.Add("Eosxd::ext::RMDIR", vid.uid, vid.gid, 1);
  EXEC_TIMING_BEGIN("Eosxd::ext::RMDIR");
  eos::fusex::response resp;
  resp.set_type(resp.ACK);
  std::shared_ptr<eos::IContainerMD> cmd;
  std::shared_ptr<eos::IContainerMD> pcmd;
  std::shared_ptr<eos::IFileMD> fmd;
  eos::IFileMD::ctime_t mtime;
  mtime.tv_sec = md.mtime();
  mtime.tv_nsec = md.mtime_ns();
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                     __FILE__);

  try {
    pcmd = gOFS->eosDirectoryService->getContainerMD(md.md_pino());

    if (S_ISDIR(md.mode())) {
      cmd = gOFS->eosDirectoryService->getContainerMD(md.md_ino());
    } else {
      fmd = gOFS->eosFileService->getFileMD(eos::common::FileId::InodeToFid(
                                              md.md_ino()));
    }

    if (!cmd) {
      // directory does not exist
      throw_mdexception(ENOENT, "No such directory : " << md.md_ino());
    }

    pcmd->setMTime(mtime);

    // check if this directory is empty
    if (cmd->getNumContainers() || cmd->getNumFiles()) {
      eos::fusex::response resp;
      resp.set_type(resp.ACK);
      resp.mutable_ack_()->set_code(resp.ack_().PERMANENT_FAILURE);
      resp.mutable_ack_()->set_err_no(ENOTEMPTY);
      resp.mutable_ack_()->set_err_msg("directory not empty");
      resp.mutable_ack_()->set_transactionid(md.reqid());
      lock.Release();
      resp.SerializeToString(response);
    } else {
      eos_info("ino=%lx delete-dir", (long) md.md_ino());
      pcmd->removeContainer(cmd->getName());
      gOFS->eosDirectoryService->removeContainer(cmd.get());
      gOFS->eosDirectoryService->updateStore(pcmd.get());
      pcmd->notifyMTimeChange(gOFS->eosDirectoryService);
      // release the namespace lock before serialization/broadcasting
      lock.Release();
      resp.mutable_ack_()->set_code(resp.ack_().OK);
      resp.mutable_ack_()->set_transactionid(md.reqid());
      resp.SerializeToString(response);
      Cap().BroadcastRelease(md);
      Cap().BroadcastDeletion(pcmd->getId(), md, cmd->getName());
      Cap().BroadcastRefresh(pcmd->getId(), md, pcmd->getParentId());
      Cap().Delete(md.md_ino());
    }
  } catch (eos::MDException& e) {
    resp.mutable_ack_()->set_code(resp.ack_().PERMANENT_FAILURE);
    resp.mutable_ack_()->set_err_no(e.getErrno());
    resp.mutable_ack_()->set_err_msg(e.getMessage().str().c_str());
    resp.mutable_ack_()->set_transactionid(md.reqid());
    resp.SerializeToString(response);
    eos_err("ino=%lx err-no=%d err-msg=%s", (long) md.md_ino(),
            e.getErrno(),
            e.getMessage().str().c_str());
  }

  EXEC_TIMING_END("Eosxd::ext::RMDIR");
  return 0;
}

//------------------------------------------------------------------------------
// Serve a meta-data DELETE operation
//-------------------------------------------------------------------------------*/

int
Server::OpDeleteFile(const std::string& id,
                     const eos::fusex::md& md,
                     eos::common::VirtualIdentity& vid,
                     std::string* response,
                     uint64_t* clock)
{
  gOFS->MgmStats.Add("Eosxd::ext::DELETE", vid.uid, vid.gid, 1);
  EXEC_TIMING_BEGIN("Eosxd::ext::DELETE");

  if (!ValidateCAP(md, D_OK, vid)) {
    std::string perm = "D";

    // a CAP might have gone or timedout, let's check again the permissions
    if (((errno == ENOENT) ||
         (errno == EINVAL) ||
         (errno == ETIMEDOUT)) &&
        ValidatePERM(md, perm, vid)) {
      // this can pass on ... permissions are fine
    } else {
      eos_err("ino=%lx delete has wrong cap");
      return EPERM;
    }
  }

  eos::fusex::response resp;
  resp.set_type(resp.ACK);
  std::shared_ptr<eos::IContainerMD> cmd;
  std::shared_ptr<eos::IContainerMD> pcmd;
  std::shared_ptr<eos::IFileMD> fmd;
  eos::IFileMD::ctime_t mtime;
  mtime.tv_sec = md.mtime();
  mtime.tv_nsec = md.mtime_ns();
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                     __FILE__);

  try {
    pcmd = gOFS->eosDirectoryService->getContainerMD(md.md_pino());

    if (S_ISDIR(md.mode())) {
      cmd = gOFS->eosDirectoryService->getContainerMD(md.md_ino());
    } else {
      fmd = gOFS->eosFileService->getFileMD(eos::common::FileId::InodeToFid(
                                              md.md_ino()));
    }

    if (!fmd) {
      // file does not exist
      throw_mdexception(ENOENT, "No such file : " << md.md_ino());
    }

    pcmd->setMTime(mtime);
    eos_info("ino=%lx delete-file", (long) md.md_ino());
    eos::IContainerMD::XAttrMap attrmap = pcmd->getAttributes();
    // this is a client hiding versions, force the version cleanup
    bool version_cleanup = (md.opflags() == eos::fusex::md::DELETEVERSIONS);

    // recycle bin - not for hardlinked files or hardlinks!
    if (
      (version_cleanup || attrmap.count(Recycle::gRecyclingAttribute)) &&
      (!fmd->hasAttribute(k_mdino)) && (!fmd->hasAttribute(k_nlink))) {
      // translate to a path name and call the complex deletion function
      // this is vulnerable to a hard to trigger race conditions
      std::string fullpath = gOFS->eosView->getUri(fmd.get());
      gOFS->WriteRecycleRecord(fmd);
      lock.Release();
      XrdOucErrInfo error;
      (void) gOFS->_rem(fullpath.c_str(), error, vid, "",
                        false,  // not simulated
                        false, // delete versions as well
                        !attrmap.count(Recycle::gRecyclingAttribute)
                        ,  // indicate if recycle bin is disabled
                        true,  // don't enforce quota
                        false // don't broadcast
                       );
      lock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
    } else {
      try {
        // handle quota
        eos::IQuotaNode* quotanode = gOFS->eosView->getQuotaNode(pcmd.get());

        if (quotanode) {
          quotanode->removeFile(fmd.get());
        }
      } catch (eos::MDException& e) {
      }

      bool doDelete = true;
      uint64_t tgt_md_ino;

      if (fmd->hasAttribute(k_mdino)) {
        /* this is a hard link, decrease reference count on underlying file */
        tgt_md_ino = std::stoull(fmd->getAttribute(k_mdino));
        uint64_t clock;
        /* gmd = the file holding the inode */
        std::shared_ptr<eos::IFileMD> gmd = gOFS->eosFileService->getFileMD(
                                              eos::common::FileId::InodeToFid(tgt_md_ino), &clock);
        long nlink = std::stol(gmd->getAttribute(k_nlink)) - 1;

        if (nlink) {
          gmd->setAttribute(k_nlink, std::to_string(nlink));
        } else {
          gmd->removeAttribute(k_nlink);
        }

        gOFS->eosFileService->updateStore(gmd.get());
        eos_info("hlnk nlink update on %s for %s now %ld",
                 gmd->getName().c_str(), fmd->getName().c_str(), nlink);

        if (nlink <= 0) {
          if (gmd->getName().substr(0, 13) == "...eos.ino...") {
            eos_info("hlnk unlink target %s for %s nlink %ld",
                     gmd->getName().c_str(), fmd->getName().c_str(), nlink);
            XrdOucErrInfo error;

            if (XrdMgmOfsFile::create_cow(XrdMgmOfsFile::cowDelete, pcmd, gmd, vid,
                                          error) == -1) {
              pcmd->removeFile(gmd->getName());
              gmd->unlinkAllLocations();
              gmd->setContainerId(0);
            }

            gOFS->eosFileService->updateStore(gmd.get());
          }
        }
      } else if (fmd->hasAttribute(k_nlink)) {
        /* this is a genuine file, potentially with hard links */
        tgt_md_ino = eos::common::FileId::FidToInode(fmd->getId());
        long nlink = std::stol(fmd->getAttribute(k_nlink));

        if (nlink > 0) {
          // hard links exist, just rename the file so the inode does not disappear
          char nameBuf[256];
          snprintf(nameBuf, sizeof(nameBuf), "...eos.ino...%lx", tgt_md_ino);
          std::string tmpName = nameBuf;
          fmd->setAttribute(k_nlink, std::to_string(nlink));
          eos_info("hlnk unlink rename %s=>%s new nlink %d",
                   fmd->getName().c_str(), tmpName.c_str(), nlink);
          pcmd->removeFile(tmpName);            // if the target exists, remove it!
          gOFS->eosView->renameFile(fmd.get(), tmpName);
          doDelete = false;
        } else {
          eos_info("hlnk nlink %ld for %s, will be deleted",
                   nlink, fmd->getName().c_str());
        }
      }

      if (doDelete) {       /* delete, but clone first if needed */
        XrdOucErrInfo error;
        int rc = XrdMgmOfsFile::create_cow(XrdMgmOfsFile::cowDelete, pcmd, fmd, vid,
                                           error);

        if (rc == -1) {     /* usual outcome: no cloning */
          pcmd->removeFile(fmd->getName());
          fmd->setContainerId(0);
          fmd->unlinkAllLocations();
        }

        gOFS->WriteRmRecord(fmd);
      }

      gOFS->eosFileService->updateStore(fmd.get());
      gOFS->eosDirectoryService->updateStore(pcmd.get());
      pcmd->notifyMTimeChange(gOFS->eosDirectoryService);
    }

    // release the namespace lock before serialization/broadcasting
    lock.Release();
    resp.mutable_ack_()->set_code(resp.ack_().OK);
    resp.mutable_ack_()->set_transactionid(md.reqid());
    resp.SerializeToString(response);
    Cap().BroadcastRelease(md);
    Cap().BroadcastDeletion(pcmd->getId(), md, md.name());
    Cap().BroadcastRefresh(pcmd->getId(), md, pcmd->getParentId());
    Cap().Delete(md.md_ino());
  } catch (eos::MDException& e) {
    resp.mutable_ack_()->set_code(resp.ack_().PERMANENT_FAILURE);
    resp.mutable_ack_()->set_err_no(e.getErrno());
    resp.mutable_ack_()->set_err_msg(e.getMessage().str().c_str());
    resp.mutable_ack_()->set_transactionid(md.reqid());
    resp.SerializeToString(response);
    eos_err("ino=%lx err-no=%d err-msg=%s", (long) md.md_ino(),
            e.getErrno(),
            e.getMessage().str().c_str());
  }

  EXEC_TIMING_END("Eosxd::ext::DELETE");
  return 0;
}

//------------------------------------------------------------------------------
// Serve a meta-data DELETE operation
//-------------------------------------------------------------------------------*/

int
Server::OpDeleteLink(const std::string& id,
                     const eos::fusex::md& md,
                     eos::common::VirtualIdentity& vid,
                     std::string* response,
                     uint64_t* clock)
{
  gOFS->MgmStats.Add("Eosxd::ext::DELETELNK", vid.uid, vid.gid, 1);
  EXEC_TIMING_BEGIN("Eosxd::ext::DELETELNK");

  if (!ValidateCAP(md, D_OK, vid)) {
    std::string perm = "D";

    // a CAP might have gone or timedout, let's check again the permissions
    if (((errno == ENOENT) ||
         (errno == EINVAL) ||
         (errno == ETIMEDOUT)) &&
        ValidatePERM(md, perm, vid)) {
      // this can pass on ... permissions are fine
    } else {
      eos_err("ino=%lx delete has wrong cap");
      return EPERM;
    }
  }

  eos::fusex::response resp;
  resp.set_type(resp.ACK);
  std::shared_ptr<eos::IContainerMD> pcmd;
  std::shared_ptr<eos::IFileMD> fmd;
  eos::IFileMD::ctime_t mtime;
  mtime.tv_sec = md.mtime();
  mtime.tv_nsec = md.mtime_ns();
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                     __FILE__);

  try {
    pcmd = gOFS->eosDirectoryService->getContainerMD(md.md_pino());
    fmd = gOFS->eosFileService->getFileMD(eos::common::FileId::InodeToFid(
                                            md.md_ino()));

    if (!fmd) {
      // no link
      throw_mdexception(ENOENT, "No such link : " << md.md_ino());
    }

    pcmd->setMTime(mtime);
    eos_info("ino=%lx delete-link", (long) md.md_ino());
    gOFS->eosView->removeFile(fmd.get());
    eos::IQuotaNode* quotanode = gOFS->eosView->getQuotaNode(pcmd.get());

    // free previous quota
    if (quotanode) {
      quotanode->removeFile(fmd.get());
    }

    gOFS->eosDirectoryService->updateStore(pcmd.get());
    pcmd->notifyMTimeChange(gOFS->eosDirectoryService);
    // release the namespace lock before serialization/broadcasting
    lock.Release();
    resp.mutable_ack_()->set_code(resp.ack_().OK);
    resp.mutable_ack_()->set_transactionid(md.reqid());
    resp.SerializeToString(response);
    Cap().BroadcastRelease(md);
    Cap().BroadcastDeletion(pcmd->getId(), md, md.name());
    Cap().BroadcastRefresh(pcmd->getId(), md, pcmd->getParentId());
    Cap().Delete(md.md_ino());
  } catch (eos::MDException& e) {
    resp.mutable_ack_()->set_code(resp.ack_().PERMANENT_FAILURE);
    resp.mutable_ack_()->set_err_no(e.getErrno());
    resp.mutable_ack_()->set_err_msg(e.getMessage().str().c_str());
    resp.mutable_ack_()->set_transactionid(md.reqid());
    resp.SerializeToString(response);
    eos_err("ino=%lx err-no=%d err-msg=%s", (long) md.md_ino(),
            e.getErrno(),
            e.getMessage().str().c_str());
  }

  EXEC_TIMING_END("Eosxd::ext::DELETELNK");
  return 0;
}
//------------------------------------------------------------------------------
// Server a meta-data GETCAP operation
//------------------------------------------------------------------------------

int
Server::OpGetCap(const std::string& id,
                 const eos::fusex::md& md,
                 eos::common::VirtualIdentity& vid,
                 std::string* response,
                 uint64_t* clock)
{
  gOFS->MgmStats.Add("Eosxd::ext::GETCAP", vid.uid, vid.gid, 1);
  EXEC_TIMING_BEGIN("Eosxd::ext::GETCAP");
  eos::fusex::container cont;
  cont.set_type(cont.CAP);
  eos::fusex::md lmd;
  {
    // get the meta data
    if (eos::common::FileId::IsFileInode(md.md_ino())) {
      FillFileMD((uint64_t) md.md_ino(), lmd, vid);
    } else {
      FillContainerMD((uint64_t) md.md_ino(), lmd, vid);
    }

    lmd.set_clientuuid(md.clientuuid());
    lmd.set_clientid(md.clientid());
    // get the capability
    FillContainerCAP(md.md_ino(), lmd, vid, "");
  }
  // this cap only provides the permissions, but it is not a cap which
  // synchronized the meta data atomically, the client marks a cap locally
  // if he synchronized the contents with it
  *(cont.mutable_cap_()) = lmd.capability();
  std::string rspstream;
  cont.SerializeToString(&rspstream);
  *response += Header(rspstream);
  response->append(rspstream.c_str(), rspstream.size());
  eos_info("cap-issued: id=%lx mode=%x vtime=%lu.%lu uid=%u gid=%u "
           "client-id=%s auth-id=%s errc=%d",
           cont.cap_().id(), cont.cap_().mode(), cont.cap_().vtime(),
           cont.cap_().vtime_ns(), cont.cap_().uid(), cont.cap_().gid(),
           cont.cap_().clientid().c_str(), cont.cap_().authid().c_str(),
           cont.cap_().errc());
  EXEC_TIMING_END("Eosxd::ext::GETCAP");
  return 0;
}

//------------------------------------------------------------------------------
// Server a meta-data file lock GET status operation
//------------------------------------------------------------------------------

int
Server::OpGetLock(const std::string& id,
                  const eos::fusex::md& md,
                  eos::common::VirtualIdentity& vid,
                  std::string* response,
                  uint64_t* clock)
{
  gOFS->MgmStats.Add("Eosxd::ext::GETLK", vid.uid, vid.gid, 1);
  EXEC_TIMING_BEGIN("Eosxd::ext::GETLK");
  eos::fusex::response resp;
  resp.set_type(resp.LOCK);
  struct flock lock;
  Locks().getLocks(md.md_ino())->getlk((pid_t) md.flock().pid(), &lock);
  resp.mutable_lock_()->set_len(lock.l_len);
  resp.mutable_lock_()->set_start(lock.l_start);
  resp.mutable_lock_()->set_pid(lock.l_pid);
  eos_info("getlk: ino=%016lx start=%lu len=%ld pid=%u type=%d",
           md.md_ino(),
           lock.l_start,
           lock.l_len,
           lock.l_pid,
           lock.l_type);

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

  EXEC_TIMING_END("Eosxd::ext::GETLK");
  return 0;
}

//------------------------------------------------------------------------------
// Server a meta-data file lock SET operation
//------------------------------------------------------------------------------

int
Server::OpSetLock(const std::string& id,
                  const eos::fusex::md& md,
                  eos::common::VirtualIdentity& vid,
                  std::string* response,
                  uint64_t* clock)
{
  EXEC_TIMING_BEGIN((md.operation() == md.SETLKW) ? "Eosxd::ext::SETLKW" :
                    "Eosxd::ext::SETLK");
  eos::fusex::response resp;
  resp.set_type(resp.LOCK);
  int sleep = 0;

  if (md.operation() == md.SETLKW) {
    gOFS->MgmStats.Add("Eosxd::ext::SETLKW", vid.uid, vid.gid, 1);
    sleep = 1;
  } else {
    gOFS->MgmStats.Add("Eosxd::ext::SETLK", vid.uid, vid.gid, 1);
  }

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
    break;

  default:
    resp.mutable_lock_()->set_err_no(EAGAIN);
    resp.SerializeToString(response);
    return 0;
    break;
  }

  if (lock.l_len == 0) {
    // the infinite lock is represented by -1 in the locking class implementation
    lock.l_len = -1;
  }

  eos_info("setlk: ino=%016lx start=%lu len=%ld pid=%u type=%d",
           md.md_ino(),
           lock.l_start,
           lock.l_len,
           lock.l_pid,
           lock.l_type);

  if (Locks().getLocks(md.md_ino())->setlk(md.flock().pid(), &lock, sleep,
      md.clientuuid())) {
    // lock ok!
    resp.mutable_lock_()->set_err_no(0);
  } else {
    // lock is busy
    resp.mutable_lock_()->set_err_no(EAGAIN);
  }

  resp.SerializeToString(response);
  EXEC_TIMING_END((md.operation() == md.SETLKW) ? "Eosxd::ext::SETLKW" :
                  "Eosxd::ext::SETLK");
  return 0;
}

//------------------------------------------------------------------------------
// Dispatch meta-data requests
//------------------------------------------------------------------------------

int
Server::HandleMD(const std::string& id,
                 const eos::fusex::md& md,
                 eos::common::VirtualIdentity& vid,
                 std::string* response,
                 uint64_t* clock)
{
  std::string ops;
  int op_type = md.operation();

  if (op_type == md.GET) {
    ops = "GET";
  } else if (op_type == md.SET) {
    ops = "SET";
  } else if (op_type == md.DELETE) {
    ops = "DELETE";
  } else if (op_type == md.GETCAP) {
    ops = "GETCAP";
  } else if (op_type == md.LS) {
    ops = "LS";
  } else if (op_type == md.GETLK) {
    ops = "GETLK";
  } else if (op_type == md.SETLK) {
    ops = "SETLK";
  } else if (op_type == md.SETLKW) {
    ops = "SETLKW";
  } else if (op_type == md.BEGINFLUSH) {
    ops = "BEGINFLUSH";
  } else if (op_type == md.ENDFLUSH) {
    ops = "ENDFLUSH";
  } else {
    ops = "UNKNOWN";
  }

  std::string op_class = "none";

  if (S_ISDIR(md.mode())) {
    op_class = "dir";
  } else if (S_ISREG(md.mode())) {
    op_class = "file";
  } else if (S_ISFIFO(md.mode())) {
    op_class = "fifo";
  } else if (S_ISLNK(md.mode())) {
    op_class = "link";
  }

  eos_info("ino=%016lx operation=%s type=%s name=%s pino=%016lx cid=%s cuuid=%s",
           (long) md.md_ino(),
           ops.c_str(),
           op_class.c_str(),
           md.name().c_str(),
           md.md_pino(),
           md.clientid().c_str(), md.clientuuid().c_str());

  if (EOS_LOGS_DEBUG) {
    std::string mdout = dump_message(md);
    eos_debug("\n%s\n", mdout.c_str());
  }

  // depending on the operation, prefetch into the namespace cache all
  // metadata entries we'll need to service this request, _before_ acquiring
  // the global namespace lock.
  prefetchMD(md);

  switch (md.operation()) {
  case eos::fusex::md::OP::md_OP_BEGINFLUSH:
    return OpBeginFlush(id, md, vid, response, clock);

  case eos::fusex::md::OP::md_OP_ENDFLUSH:
    return OpEndFlush(id, md, vid, response, clock);

  case eos::fusex::md::OP::md_OP_GET:
  case eos::fusex::md::OP::md_OP_LS:
    return OpGetLs(id, md, vid, response, clock);

  case eos::fusex::md::OP::md_OP_SET:
    return OpSet(id, md, vid, response, clock);

  case eos::fusex::md::OP::md_OP_DELETE:
    return OpDelete(id, md, vid, response, clock);

  case eos::fusex::md::OP::md_OP_GETCAP:
    return OpGetCap(id, md, vid, response, clock);

  case eos::fusex::md::OP::md_OP_GETLK:
    return OpGetLock(id, md, vid, response, clock);

  case eos::fusex::md::OP::md_OP_SETLK:
  case eos::fusex::md::OP::md_OP_SETLKW:
    return OpSetLock(id, md, vid, response, clock);

  default:
    break;
  }

  return 0;
}

//----------------------------------------------------------------------------
// Replaces the file's non-system attributes with client-supplied ones.
//----------------------------------------------------------------------------

void
Server::replaceNonSysAttributes(const std::shared_ptr<eos::IFileMD>& fmd,
                                const eos::fusex::md& md)
{
  eos::IFileMD::XAttrMap xattrs = fmd->getAttributes();

  // Remove all non-system attributes
  for (const auto& attr : xattrs) {
    if ((attr.first.substr(0, 3) != "sys")) {
      fmd->removeAttribute(attr.first);
    }
  }

  // Register non-system client-supplied attributes
  for (const auto& attr : md.attr()) {
    if ((attr.first.substr(0, 3) != "sys")) {
      fmd->setAttribute(attr.first, attr.second);
    }
  }
}

EOSFUSESERVERNAMESPACE_END
