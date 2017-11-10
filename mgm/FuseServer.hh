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

#ifndef __EOSMGM_FUSESERVER__HH__
#define __EOSMGM_FUSESERVER__HH__

/*----------------------------------------------------------------------------*/
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
#include "XrdSys/XrdSysPthread.hh"
#include <google/protobuf/util/json_util.h>

/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class FuseServer
{
public:
  FuseServer();

  ~FuseServer ();

  void shutdown();

  std::string dump_message(const google::protobuf::Message& message);

  class Clients : public XrdSysMutex
  {
  public:

    Clients() : mHeartBeatWindow(15), mHeartBeatOfflineWindow(60) , mHeartBeatRemoveWindow(900), mHeartBeatInterval(1)
    {
    }

    virtual ~Clients()
    {
    }

    bool
    Dispatch(const std::string identity, eos::fusex::heartbeat & hb);
    void Print(std::string& out, std::string options="", bool monitoring=false);
    void HandleStatistics(const std::string identity,
                          const eos::fusex::statistics& stats);

    class Client
    {
    public:

      Client() : mState(PENDING)
      {
      }

      virtual ~Client()
      {
      }

      eos::fusex::heartbeat& heartbeat()
      {
        return heartbeat_;
      }

      eos::fusex::statistics& statistics()
      {
        return statistics_;
      }

      enum status_t
      {
        PENDING, EVICTED, OFFLINE, VOLATILE, ONLINE
      } ;

      const char* const status [6]{
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

      status_t state()
      {
        return mState;
      }

    private:
      eos::fusex::heartbeat heartbeat_;
      eos::fusex::statistics statistics_;
      status_t mState;

      // inode, pid lock map
      std::map<uint64_t, std::set < pid_t>> mLockPidMap;
    } ;

    typedef std::map<std::string, Client> client_map_t;
    typedef std::map<std::string, std::string> client_uuid_t;

    client_map_t & map()
    {
      return mMap;
    }

    client_uuid_t & uuidview()
    {
      return mUUIDView;
    }

    void
    MonitorHeartBeat();

    bool should_terminate()
    {
      return terminate_.load();
    } // check if threads should terminate 

    void terminate()
    {
      terminate_.store(true, std::memory_order_seq_cst);
    } // indicate to terminate

    // evict a client by force
    int Evict(std::string& uuid, std::string reason);

    // release CAPs
    int ReleaseCAP(uint64_t id,
                   const std::string& uuid,
                   const std::string& clientid
                   );

    // send MD after update
    int SendMD( const eos::fusex::md &md,
               const std::string& uuid,
               const std::string& clientid,
               uint64_t md_ino,
               uint64_t md_pino,
               struct timespec& p_mtime
               );

    // drop caps of a given client
    int Dropcaps(const std::string& uuid, std::string& out);

    // broad cast triggered by heartbeat function
    int BroadcastDropAllCaps(const std::string& identity, eos::fusex::heartbeat& hb); 

    // broad cast new heartbeat interval
    int BroadcastConfig(const std::string& identity, eos::fusex::config& cfg);

    // change the clients heartbeat interval
    int SetHeartbeatInterval(int interval);

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

    std::atomic<bool> terminate_;
  } ;

  class Caps : public XrdSysMutex
  {
    friend class FuseServer;

  public:

    class capx : public eos::fusex::cap
    {
    public:

      capx()
      {
      }

      virtual ~capx()
      {
      }

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
    } ;

    typedef std::shared_ptr<capx> shared_cap;

    Caps()
    {
    }

    virtual ~Caps()
    {
    }

    typedef std::string authid_t;
    typedef std::string clientid_t;
    typedef std::pair<uint64_t, authid_t> ino_authid_t;
    typedef std::set<authid_t> authid_set_t;
    typedef std::set<uint64_t> ino_set_t;
    typedef std::map<uint64_t, authid_set_t> notify_set_t; // inode=>set(authid_t)
    typedef std::map<clientid_t, authid_set_t> client_set_t;
    typedef std::map<clientid_t, ino_set_t> client_ino_set_t;

    void pop()
    {
      XrdSysMutexHelper lock(this);
      mTimeOrderedCap.pop_front();
    }

    bool expire()
    {
      XrdSysMutexHelper lock(this);
      authid_t id;
      if (!mTimeOrderedCap.empty())
      {
        id = mTimeOrderedCap.front();
      }
      else
        return false;

      if (mCaps.count(id))
      {
        shared_cap cap = mCaps[id];
        uint64_t now = (uint64_t) time(NULL);

        // leave some margin for revoking
        if (cap->vtime() <= (now + 10))
        {
          mCaps.erase(id);
          mInodeCaps[cap->id()].erase(id);
          if (!mInodeCaps[cap->id()].size())
            mInodeCaps.erase(cap->id());
          return true;
        }
        else
        {
          return false;
        }
      }
      return true;
    }

    void Store(const eos::fusex::cap &cap,
               eos::common::Mapping::VirtualIdentity* vid);


    bool Imply(uint64_t md_ino,
               authid_t authid,
               authid_t implied_authid);


    int Delete(uint64_t id);

    shared_cap Get(authid_t id);

    int BroadcastRelease(const eos::fusex::md &md); // broad cast triggered by fuse network
    int BroadcastReleaseFromExternal(uint64_t inode); // broad cast triggered non-fuse network
    int BroadcastMD(const eos::fusex::md &md,
                    uint64_t md_ino,
                    uint64_t md_pino,
                    struct timespec& p_mtime
                    ); // broad cast changed md around
    std::string Print(std::string option, std::string filter);

    std::map<authid_t, shared_cap>& GetCaps()
    {
      return mCaps;
    }

    notify_set_t& InodeCaps()
    {
      return mInodeCaps;
    }

    client_set_t& ClientCaps()
    {
      return mClientCaps;
    }

    client_ino_set_t& ClientInoCaps()
    {
      return mClientInoCaps;
    }

  protected:
    // -------------------------------------------------------------------------
    // a time ordered list pointing to caps
    // -------------------------------------------------------------------------
    std::deque< authid_t > mTimeOrderedCap;
    // -------------------------------------------------------------------------
    // authid=>cap lookup map
    // -------------------------------------------------------------------------
    std::map<authid_t, shared_cap> mCaps;
    // -------------------------------------------------------------------------
    // clientid=>list of authid 
    // -------------------------------------------------------------------------
    client_set_t mClientCaps;

    // -------------------------------------------------------------------------
    // clientid=>list of inodes 
    // -------------------------------------------------------------------------
    client_ino_set_t mClientInoCaps;

    // -------------------------------------------------------------------------
    // inode=>authid_t 
    // -------------------------------------------------------------------------
    notify_set_t mInodeCaps;
  } ;

  class Lock : XrdSysMutex
  {
  public:

    Lock()
    {
    }

    virtual ~Lock()
    {
    }

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

  } ;

  class Flush : XrdSysMutex
  {
    // essentially a map containing clients which currently flush a file
  public:

    static const int cFlushWindow=60;

    Flush()
    {
    }

    virtual ~Flush()
    {
    }

    void beginFlush(uint64_t id, std::string client);

    void endFlush(uint64_t id, std::string client);

    bool hasFlush(uint64_t id);

    bool validateFlush(uint64_t id);

    void expireFlush();

    void Print(std::string& out);

  private:

    typedef struct flush_info
    {

      flush_info() : client("") , nref(0)
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
        if (nref > 0)
          return false;
        return true;
      }

      std::string client;
      struct timespec ftime;
      ssize_t nref;
    } flush_info_t;

    std::map<uint64_t, std::map<std::string, flush_info_t> > flushmap;
  } ;

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

  void Print(std::string& out, std::string options="", bool monitoring=false);

  bool FillContainerMD(uint64_t id, eos::fusex::md& dir);
  bool FillFileMD(uint64_t id, eos::fusex::md& file);
  bool FillContainerCAP(uint64_t id, eos::fusex::md& md,
                        eos::common::Mapping::VirtualIdentity* vid,
                        std::string reuse_uuid = "",
                        bool issue_only_one=false);

  Caps::shared_cap ValidateCAP(const eos::fusex::md& md, mode_t mode);
  bool ValidatePERM(const eos::fusex::md& md, const std::string& mode, eos::common::Mapping::VirtualIdentity* vid);

  uint64_t InodeFromCAP(const eos::fusex::md&);

  void HandleDir(const std::string& identity, const eos::fusex::dir& dir);

  std::string Header(const std::string& response); // reply a sync-response header

  int HandleMD(const std::string& identity,
               const eos::fusex::md& md,
               std::string* response = 0,
               uint64_t* clock = 0,
               eos::common::Mapping::VirtualIdentity* vid = 0);

  void
  MonitorCaps();

  bool should_terminate()
  {

    return terminate_.load();
  } // check if threads should terminate 

  void terminate()
  {
    terminate_.store(true, std::memory_order_seq_cst);
  } // indicate to terminate

protected:

  Clients mClients;
  Caps mCaps;
  Lock mLocks;
  Flush mFlushs;

private:

  std::thread* mMonitorThread;
  std::thread* mCapThread;
  std::atomic<bool> terminate_;
} ;

EOSMGMNAMESPACE_END

#endif
