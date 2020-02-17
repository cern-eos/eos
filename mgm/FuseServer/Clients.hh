// ----------------------------------------------------------------------
// File: FuseServer/Clients.hh
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

#include "mgm/Namespace.hh"
#include "mgm/FuseServer/Caps.hh"
#include "mgm/fusex.pb.h"
#include "common/Timing.hh"
#include "common/Logging.hh"




EOSFUSESERVERNAMESPACE_BEGIN

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

    // get broadcast max audience
    int BroadCastMaxAudience() const 
    {
      return mMaxBroadCastAudience;
    }

    // get broadcast audience match
    std::string BroadCastAudienceSuppressMatch() const
    {
      return mMaxbroadCastAudienceMatch;
    }

    // to defer an operation based on client versions
    bool DeferClient(std::string clienversion, std::string minimum_allowed_version);

    // set max audience before suppression
    void SetBroadCastMaxAudience(int size);

    // set max audience client match to be suppressed
    void SetBroadCastAudienceSuppressMatch(const std::string& match);

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

    // max audience before suppression
    int mMaxBroadCastAudience;

    // match string for hosts which get suppressed
    std::string mMaxbroadCastAudienceMatch;

    std::atomic<bool> terminate_;
  };


EOSFUSESERVERNAMESPACE_END
