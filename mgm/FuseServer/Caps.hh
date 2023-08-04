// ----------------------------------------------------------------------
// File: FuseServer/Caps.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

#pragma once


#include <thread>
#include <map>
#include <unordered_map>

#include "mgm/Namespace.hh"
#include "mgm/fusex.pb.h"

#include "common/Mapping.hh"
#include "common/Timing.hh"
#include "common/Logging.hh"
#include "common/RWMutex.hh"

EOSFUSESERVERNAMESPACE_BEGIN

//----------------------------------------------------------------------------
//! Class Caps
//----------------------------------------------------------------------------
class Caps
{
  friend class FuseServer;
public:
  class capx
  {
  public:

    capx() = default;

    virtual ~capx() = default;

    capx& operator=(eos::fusex::cap other)
    {
      proto = other;
      return *this;
    }

    void set_vid(eos::common::VirtualIdentity* vid)
    {
      mVid = *vid;
    }

    eos::common::VirtualIdentity* vid()
    {
      return &mVid;
    }

    eos::fusex::cap* operator()() {
      return &proto;
    }

  private:
    eos::common::VirtualIdentity mVid;
    eos::fusex::cap proto;
  };

  typedef std::shared_ptr<capx> shared_cap;

  Caps() = default;

  virtual ~Caps() = default;

  typedef std::string authid_t;
  typedef std::string clientid_t;
  typedef std::string client_uuid_t;
  typedef std::unordered_set<clientid_t> clientid_set_t;
  typedef std::unordered_map<client_uuid_t, clientid_set_t> client_ids_t;
  typedef std::pair<uint64_t, authid_t> ino_authid_t;
  typedef std::unordered_set<authid_t> authid_set_t;
  typedef std::unordered_map<uint64_t, authid_set_t> ino_map_t;
  typedef std::unordered_set<uint64_t> ino_set_t;
  typedef std::unordered_map<uint64_t, authid_set_t> notify_set_t; // inode=>set(authid_t)
  typedef std::unordered_map<clientid_t, authid_set_t> client_set_t;
  typedef std::unordered_map<clientid_t, ino_map_t> client_ino_map_t;


  ssize_t ncaps()
  {
    std::lock_guard lg(mtx);
    return mTimeOrderedCap.size();
  }

  void pop()
  {
    std::lock_guard lg(mtx);

    if (!mTimeOrderedCap.empty()) {
      mTimeOrderedCap.erase(mTimeOrderedCap.begin());
    }
  }

  bool expire()
  {
    std::lock_guard lg(mtx);
    authid_t id;
    time_t idtime = 0;

    if (!mTimeOrderedCap.empty()) {
      id = mTimeOrderedCap.begin()->second;
      idtime = mTimeOrderedCap.begin()->first;
    } else {
      return false;
    }

    // TODO: C++17 - move initialization inside if
    auto it = mCaps.find(id);
    if (it != mCaps.end()) {
      shared_cap cap = it->second;
      uint64_t now = (uint64_t) time(NULL);

      if (((*cap)()->vtime() + 10) <= now) {
        return Remove(cap);
      } else {
        if ((idtime + 10) <= now) {
          return true;
        } else {
          return false;
        }
      }
    }

    return true;
  }

  void Store(const eos::fusex::cap& cap,
             eos::common::VirtualIdentity* vid);


  bool Imply(uint64_t md_ino,
             authid_t authid,
             authid_t implied_authid);

  void dropCaps(const std::string& uuid)
  {
    eos_static_info("drop client caps: %s", uuid.c_str());

    std::vector<shared_cap> deleteme;
    {
      std::lock_guard lg(mtx);

      for (auto it=mCaps.begin(); it!=mCaps.end(); ++it) {
        if ((*it->second)()->clientuuid() == uuid) {
          deleteme.push_back(it->second);
        }
      }
    }
    {
      for (auto it=deleteme.begin(); it!=deleteme.end(); ++it) {
        std::lock_guard lg(mtx);
        shared_cap cap = *it;
        Remove(*it);
      }
    }

    // cleanup by client ids
    {
      std::lock_guard lg(mtx);
      auto uuid_iter = mClientIds.find(uuid);
      if (uuid_iter != mClientIds.end()) {
        for (auto it = uuid_iter->second.begin(); it != uuid_iter->second.end(); ++it) {
          mClientCaps.erase(*it);
          mClientInoCaps.erase(*it);
        }
        mClientIds.erase(uuid_iter);
      }
    }
  }

  template <typename... Args>
  bool RemoveTS(Args&&... args)
  {
    std::lock_guard lg(mtx);
    return Remove(std::forward<Args>(args)...);
  }

  bool Remove(shared_cap cap)
  {
    // you have to have a write lock for the caps
    bool rc = mCaps.erase((*cap)()->authid());

    mInodeCaps[(*cap)()->id()].erase((*cap)()->authid());

    if (!mInodeCaps[(*cap)()->id()].size()) {
      mInodeCaps.erase((*cap)()->id());
    }

    mClientInoCaps[(*cap)()->clientid()][(*cap)()->id()].erase((*cap)()->authid());

    if (!mClientInoCaps[(*cap)()->clientid()][(*cap)()->id()].size()) {
      mClientInoCaps[(*cap)()->clientid()].erase((*cap)()->id());

      if (!mClientInoCaps[(*cap)()->clientid()].size()) {
	mClientInoCaps.erase((*cap)()->clientid());
      }
    }

    mClientCaps[(*cap)()->clientid()].erase((*cap)()->authid());

    if (mClientCaps[(*cap)()->clientid()].size() == 0) {
      mClientCaps.erase((*cap)()->clientid());
    }
    return rc;
  }

  int Delete(uint64_t id);

  shared_cap Get(const authid_t& id, bool make_default=true);

  template <typename... Args>
  auto GetTS(Args&&... args) {
    std::lock_guard lg(mtx);
    return Get(std::forward<Args>(args)...);
  }

  const capx* GetRaw(const authid_t& id);

  int BroadcastCap(shared_cap cap);


  int BroadcastDeletion(uint64_t inode,
                        const eos::fusex::md& md,
                        const std::string& name,
			struct timespec& p_mtime);

  int BroadcastRefresh(uint64_t
                       inode,
                       const eos::fusex::md& md,
                       uint64_t
                       parent_inode,
		       bool notprot5 = false
		       ); // broad cast triggered by fuse network

  int BroadcastRefreshFromExternal(uint64_t
                                   inode,
                                   uint64_t
                                   parent_inode,
				   bool notprot5 = false
				   ); // broad cast triggered non-fuse network

  int BroadcastDeletionFromExternal(uint64_t inode,
                                    const std::string& name,
				    struct timespec& p_mtime);

  int BroadcastMD(const eos::fusex::md& md,
                  uint64_t md_ino,
                  uint64_t md_pino,
                  uint64_t clock,
                  struct timespec& p_mtime
                 ); // broad cast changed md around
  std::string Print(const std::string& option,
                    const std::string& filter);

  const auto& GetCaps() const
  {
    return mCaps;
  }

  auto GetAllCaps() {

    std::vector<shared_cap> results;

    std::lock_guard lg(mtx);
    for (const auto& kv: mCaps) {
      results.push_back(kv.second);
    }
    return results;
  }

  bool HasCap(authid_t authid)
  {
    return (this->mCaps.count(authid) ? true : false);
  }

  bool HasInodeId(const std::string& client_id,
                  uint64_t id)
  {
    std::lock_guard lg(mtx);
    if (auto kv = mClientInoCaps.find(client_id);
        kv != mClientInoCaps.end()) {
      return kv->second.count(id) > 0;
    }
    return false;
  }

  authid_set_t GetInodeCapAuthIds(const std::string& client_id,
                                  uint64_t id)
  {
    authid_set_t results;
    std::lock_guard lg(mtx);
    if (auto kv = mClientInoCaps.find(client_id);
        kv != mClientInoCaps.end()) {
      if (auto auth_ids = kv->second.find(id);
          auth_ids != kv->second.end()) {
        std::copy(auth_ids->second.begin(),
                  auth_ids->second.end(),
                  std::inserter(results, results.begin()));
      }
    }
    return results;
  }

  notify_set_t& InodeCaps()
  {
    return mInodeCaps;
  }

  client_set_t& ClientCaps()
  {
    return mClientCaps;
  }

  client_ino_map_t& ClientInoCaps()
  {
    return mClientInoCaps;
  }

  client_ids_t& ClientIds()
  {
    return mClientIds;
  }

  std::string Dump() {
    std::string s;
    std::lock_guard lg(mtx);
    s = std::to_string(mTimeOrderedCap.size()) + " c: " + std::to_string(mCaps.size()) + " cc: "
      + std::to_string(mClientCaps.size()) + " cic: " + std::to_string(mClientInoCaps.size()) + " ic: "
      + std::to_string(mInodeCaps.size());
    return s;
  }

  // Given a pid, return a vector of shared caps matching this
  // if a reference cap and mdptr are given, these ids are excluded
  std::vector<shared_cap> GetBroadcastCapsTS(uint64_t pid,
                                             shared_cap refcap = nullptr,
                                             const eos::fusex::md* mdptr = nullptr,
                                             bool suppress=false,
                                             std::string suppress_stat_tag="");



protected:

  std::mutex mtx;
  // a time ordered multimap pointing to caps
  std::multimap< time_t, authid_t > mTimeOrderedCap;
  // authid=>cap lookup map
  std::unordered_map<authid_t, shared_cap> mCaps;
  // clientid=>list of authid
  client_set_t mClientCaps;
  // clientid=>list of inodes
  client_ino_map_t mClientInoCaps;
  // inode=>authid_t
  notify_set_t mInodeCaps;
  // uuid=>set of clientid
  client_ids_t mClientIds;
};

EOSFUSESERVERNAMESPACE_END
