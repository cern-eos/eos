// ----------------------------------------------------------------------
// File: FuseServer.hh
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

#pragma once

#include "mgm/Namespace.hh"
#include <thread>
#include <vector>
#include <zmq.hpp>
#include <unistd.h>
#include <map>
#include <atomic>
#include <deque>
#include "mgm/fusex.pb.h"
#include "mgm/fuse-locks/LockTracker.hh"
#include "common/Mapping.hh"
#include "common/Timing.hh"
#include "common/Logging.hh"
#include "namespace/interface/IFileMD.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <google/protobuf/util/json_util.h>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class FuseServer
//------------------------------------------------------------------------------
class FuseServer : public eos::common::LogId

{
public:
  FuseServer();

  ~FuseServer();

  void start();
  void shutdown();

  std::string dump_message(const google::protobuf::Message& message);

  //----------------------------------------------------------------------------
  //! Class Caps
  //----------------------------------------------------------------------------
  class Caps : public eos::common::RWMutex
  {
    friend class FuseServer;
  public:
    class capx : public eos::fusex::cap
    {
    public:

      capx() = default;

      virtual ~capx() = default;

      capx& operator=(eos::fusex::cap other)
      {
        (*((eos::fusex::cap*)(this))) = other;
        return *this;
      }

      void set_vid(eos::common::Mapping::VirtualIdentity* vid)
      {
        mVid = *vid;
      }

      eos::common::Mapping::VirtualIdentity* vid()
      {
        return &mVid;
      }

    private:
      eos::common::Mapping::VirtualIdentity mVid;
    };

    typedef std::shared_ptr<capx> shared_cap;

    Caps() = default;

    virtual ~Caps() = default;

    typedef std::string authid_t;
    typedef std::string clientid_t;
    typedef std::pair<uint64_t, authid_t> ino_authid_t;
    typedef std::set<authid_t> authid_set_t;
    typedef std::map<uint64_t, authid_set_t> ino_map_t;
    typedef std::set<uint64_t> ino_set_t;
    typedef std::map<uint64_t, authid_set_t> notify_set_t; // inode=>set(authid_t)
    typedef std::map<clientid_t, authid_set_t> client_set_t;
    typedef std::map<clientid_t, ino_map_t> client_ino_map_t;


    ssize_t ncaps()
    {
      eos::common::RWMutexReadLock lock(*this);
      return mTimeOrderedCap.size();
    }

    void pop()
    {
      eos::common::RWMutexWriteLock lock(*this);

      if (!mTimeOrderedCap.empty()) {
        mTimeOrderedCap.erase(mTimeOrderedCap.begin());
      }
    }

    bool expire()
    {
      eos::common::RWMutexWriteLock lock(*this);
      authid_t id;

      if (!mTimeOrderedCap.empty()) {
        id = mTimeOrderedCap.begin()->second;
      } else {
        return false;
      }

      if (mCaps.count(id)) {
        shared_cap cap = mCaps[id];
        uint64_t now = (uint64_t) time(NULL);

        if ((cap->vtime() + 10) <= now) {
          mCaps.erase(id);
          mInodeCaps[cap->id()].erase(id);

          if (!mInodeCaps[cap->id()].size()) {
            mInodeCaps.erase(cap->id());
          }

          return true;
        } else {
          return false;
        }
      }

      return true;
    }

    void Store(const eos::fusex::cap& cap,
               eos::common::Mapping::VirtualIdentity* vid);


    bool Imply(uint64_t md_ino,
               authid_t authid,
               authid_t implied_authid);

    bool Remove(shared_cap cap)
    {
      // you have to have a write lock for the caps
      if (mCaps.count(cap->authid())) {
        mCaps.erase(cap->authid());
        mInodeCaps[cap->id()].erase(cap->authid());

        if (!mInodeCaps[cap->id()].size()) {
          mInodeCaps.erase(cap->id());
        }

        mClientInoCaps[cap->clientid()][cap->id()].erase(cap->authid());

        if (!mClientInoCaps[cap->clientid()][cap->id()].size()) {
          mClientInoCaps[cap->clientid()].erase(cap->id());

          if (!mClientInoCaps[cap->clientid()].size()) {
            mClientInoCaps.erase(cap->clientid());
          }
        }

        return true;
      } else {
        return false;
      }
    }

    int Delete(uint64_t id);

    shared_cap GetTS(authid_t id);
    shared_cap Get(authid_t id);

    int BroadcastCap(shared_cap cap);
    int BroadcastRelease(const eos::fusex::md&
                         md); // broad cast triggered by fuse network

    int BroadcastDeletion(uint64_t inode,
                          const eos::fusex::md& md,
                          const std::string& name);

    int BroadcastRefresh(uint64_t
                         inode,
                         const eos::fusex::md& md,
                         uint64_t
                         parent_inode); // broad cast triggered by fuse network


    int BroadcastReleaseFromExternal(uint64_t
                                     inode); // broad cast triggered non-fuse network


    int BroadcastRefreshFromExternal(uint64_t
                                     inode,
                                     uint64_t
                                     parent_inode); // broad cast triggered non-fuse network

    int BroadcastDeletionFromExternal(uint64_t inode,
                                      const std::string& name);

    int BroadcastMD(const eos::fusex::md& md,
                    uint64_t md_ino,
                    uint64_t md_pino,
                    uint64_t clock,
                    struct timespec& p_mtime
                   ); // broad cast changed md around
    std::string Print(std::string option, std::string filter);

    std::map<authid_t, shared_cap>& GetCaps()
    {
      return mCaps;
    }

    bool HasCap(authid_t authid)
    {
      return (this->mCaps.count(authid) ? true : false);
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

  protected:
    // a time ordered multimap pointing to caps
    std::multimap< time_t, authid_t > mTimeOrderedCap;
    // authid=>cap lookup map
    std::map<authid_t, shared_cap> mCaps;
    // clientid=>list of authid
    client_set_t mClientCaps;
    // clientid=>list of inodes
    client_ino_map_t mClientInoCaps;
    // inode=>authid_t
    notify_set_t mInodeCaps;
  };

  //----------------------------------------------------------------------------
  //! Class Clients
  //----------------------------------------------------------------------------

  class Clients : public eos::common::RWMutex
  {
  public:
    Clients():
      mHeartBeatWindow(15), mHeartBeatOfflineWindow(30),
      mHeartBeatRemoveWindow(120), mHeartBeatInterval(10),
      mQuotaCheckInterval(10)
    {}

    virtual ~Clients() = default;

    void ClientStats(size_t& nclients, size_t& active_clients, size_t& locked_clients );

    bool
    Dispatch(const std::string identity, eos::fusex::heartbeat& hb);
    void Print(std::string& out, std::string options = "");
    void HandleStatistics(const std::string identity,
                          const eos::fusex::statistics& stats);

    class Client
    {
    public:

      Client() : mState(PENDING) {}

      virtual ~Client() = default;

      eos::fusex::heartbeat& heartbeat()
      {
        return heartbeat_;
      }

      eos::fusex::statistics& statistics()
      {
        return statistics_;
      }

      enum status_t {
        PENDING, EVICTED, OFFLINE, VOLATILE, ONLINE
      };

      const char* const status [6] {
        "pending",
        "evicted",
        "offline",
        "volatile",
        "online",
        0
      };

      void set_state(status_t s)
      {
        mState = s;
      }

      void tag_opstime() {
	eos::common::Timing::GetTimeSpec(ops_time, true);
      }

      bool validate_opstime ( const struct timespec &ref_time, uint64_t age ) const {
	// return true if the last operations time is older than age compared to ref_time
	if (eos::common::Timing::GetCoarseAgeInNs(&ops_time, &ref_time) / 1000000000.0 > age) {
	  return true;
	} else {
	  return false;
	}
      }

      uint64_t get_opstime_sec() const { return ops_time.tv_sec; }
      uint64_t get_opstime_nsec() const { return ops_time.tv_nsec; }

      inline status_t state() const
      {
        return mState;
      }

    private:
      eos::fusex::heartbeat heartbeat_;
      eos::fusex::statistics statistics_;
      struct timespec ops_time;

      status_t mState;

      // inode, pid lock map
      std::map<uint64_t, std::set < pid_t>> mLockPidMap;
    } ;

    typedef std::map<std::string, Client> client_map_t;
    typedef std::map<std::string, std::string> client_uuid_t;


    ssize_t nclients()
    {
      eos::common::RWMutexReadLock lock(*this);
      return mMap.size();
    }

    client_map_t& map()
    {
      return mMap;
    }

    client_uuid_t& uuidview()
    {
      return mUUIDView;
    }

    size_t leasetime(const std::string& uuid);

    void
    MonitorHeartBeat();

    // check if threads should terminate
    bool should_terminate()
    {
      return terminate_.load();
    }

    // indicate to terminate
    void terminate()
    {
      terminate_.store(true, std::memory_order_seq_cst);
    }

    // evict a client by force
    int Evict(std::string& uuid, std::string reason, std::vector<std::string>* evicted_out=0);

    // release CAPs
    int ReleaseCAP(uint64_t id,
                   const std::string& uuid,
                   const std::string& clientid);

    // delete entry
    int DeleteEntry(uint64_t id,
                    const std::string& uuid,
                    const std::string& clientid,
                    const std::string& name);

    // refresh entry
    int RefreshEntry(uint64_t id,
                     const std::string& uuid,
                     const std::string& clientid);

    // send MD after update
    int SendMD(const eos::fusex::md& md,
               const std::string& uuid,
               const std::string& clientid,
               uint64_t md_ino,
               uint64_t md_pino,
               uint64_t clock,
               struct timespec& p_mtime
              );

    // broadcast a new cap
    int SendCAP(FuseServer::Caps::shared_cap cap);

    // drop caps of a given client
    int Dropcaps(const std::string& uuid, std::string& out);

    // broad cast triggered by heartbeat function
    int BroadcastDropAllCaps(const std::string& identity,
                             eos::fusex::heartbeat& hb);

    // broad cast new heartbeat interval
    int BroadcastConfig(const std::string& identity, eos::fusex::config& cfg);

    // change the clients heartbeat interval
    int SetHeartbeatInterval(int interval);

    // change the quote node check interval
    int SetQuotaCheckInterval(int interval);

    // get heartbeat interval setting
    int HeartbeatInterval() const
    {
      return mHeartBeatInterval;
    }

    // get quota check interval setting
    int QuotaCheckInterval() const
    {
      return mQuotaCheckInterval;
    }

    // to defer an operation based on client versions
    bool DeferClient(std::string clienversion, std::string minimum_allowed_version);

  private:
    // lookup client full id to heart beat
    client_map_t mMap;
    // lookup client uuid to full id
    client_uuid_t mUUIDView;
    // heartbeat window in seconds
    float mHeartBeatWindow;
    // heartbeat window when to remove entries
    float mHeartBeatOfflineWindow;

    // heartbeat window when client entries get removed
    float mHeartBeatRemoveWindow;

    // client heartbeat interval
    int mHeartBeatInterval;

    // quota check interval
    int mQuotaCheckInterval;

    std::atomic<bool> terminate_;
  };


  //----------------------------------------------------------------------------
  //! Class Lock
  //----------------------------------------------------------------------------

  class Lock : XrdSysMutex
  {
  public:

    Lock() = default;

    virtual ~Lock() = default;

    typedef shared_ptr<LockTracker> shared_locktracker;

    typedef std::map<uint64_t, shared_locktracker > lockmap_t;

    shared_locktracker getLocks(uint64_t id);

    void purgeLocks();

    int dropLocks(uint64_t id, pid_t pid);

    int dropLocks(const std::string& owner);

    int lsLocks(const std::string& owner,
                std::map<uint64_t, std::set<pid_t>>& rlocks,
                std::map<uint64_t, std::set<pid_t>>& wlocks);
  private:
    lockmap_t lockmap;
  };

  //----------------------------------------------------------------------------
  //! Class Flush
  //----------------------------------------------------------------------------

  class Flush : XrdSysMutex
  {
    // essentially a map containing clients which currently flush a file
  public:

    static constexpr int cFlushWindow = 60;

    Flush() = default;

    virtual ~Flush() = default;

    void beginFlush(uint64_t id, std::string client);

    void endFlush(uint64_t id, std::string client);

    bool hasFlush(uint64_t id);

    bool validateFlush(uint64_t id);

    void expireFlush();

    void Print(std::string& out);

  private:

    typedef struct flush_info {

      flush_info() : client(""), nref(0)
      {
        ftime.tv_sec = 0;
        ftime.tv_nsec = 0;
      }

      flush_info(std::string _client) : client(_client)
      {
        eos::common::Timing::GetTimeSpec(ftime);
        ftime.tv_sec += cFlushWindow;
        ftime.tv_nsec = 0;
        nref = 0;
      }

      void Add(struct flush_info l)
      {
        ftime = l.ftime;
        nref++;
      }

      bool Remove(struct flush_info l)
      {
        nref--;

        if (nref > 0) {
          return false;
        }

        return true;
      }

      std::string client;
      struct timespec ftime;
      ssize_t nref;
    } flush_info_t;

    std::map<uint64_t, std::map<std::string, flush_info_t> > flushmap;
  };

  Clients& Client()
  {
    return mClients;
  }

  Caps& Cap()
  {
    return mCaps;
  }

  Lock& Locks()
  {
    return mLocks;
  }

  Flush& Flushs()
  {
    return mFlushs;
  }

  void Print(std::string& out, std::string options = "");

  int FillContainerMD(uint64_t id, eos::fusex::md& dir,
                      eos::common::Mapping::VirtualIdentity& vid);
  bool FillFileMD(uint64_t id, eos::fusex::md& file,
                  eos::common::Mapping::VirtualIdentity& vid);
  bool FillContainerCAP(uint64_t id, eos::fusex::md& md,
                        eos::common::Mapping::VirtualIdentity& vid,
                        std::string reuse_uuid = "",
                        bool issue_only_one = false);

  Caps::shared_cap ValidateCAP(const eos::fusex::md& md, mode_t mode,
                               eos::common::Mapping::VirtualIdentity& vid);
  bool ValidatePERM(const eos::fusex::md& md, const std::string& mode,
                    eos::common::Mapping::VirtualIdentity& vid,
                    bool lock = true);

  uint64_t InodeFromCAP(const eos::fusex::md&);

  std::string Header(const std::string& response); // reply a sync-response header

  int HandleMD(const std::string& identity,
               const eos::fusex::md& md,
               eos::common::Mapping::VirtualIdentity& vid,
               std::string* response = 0,
               uint64_t* clock = 0);

  void prefetchMD(const eos::fusex::md& md);


  void
  MonitorCaps() noexcept;

  bool should_terminate()
  {
    return terminate_.load();
  } // check if threads should terminate

  void terminate()
  {
    terminate_.store(true, std::memory_order_seq_cst);
  } // indicate to terminate

  static const char* cident;

protected:
  Clients mClients;
  Caps mCaps;
  Lock mLocks;
  Flush mFlushs;

private:
  std::atomic<bool> terminate_;

  //----------------------------------------------------------------------------
  //! Replaces the file's non-system attributes with client-supplied ones.
  //!
  //! @param fmd file metadata object
  //! @param md client supplied metadata
  //----------------------------------------------------------------------------
  void replaceNonSysAttributes(const std::shared_ptr<eos::IFileMD>& fmd,
                               const eos::fusex::md& md);
};


EOSMGMNAMESPACE_END
