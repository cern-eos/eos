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

    Clients() : mHeartBeatWindow(15), mHeartBeatEvictWindow(60)
    {
    }

    virtual ~Clients()
    {
    }

    bool
    Dispatch(const std::string identity, const eos::fusex::heartbeat & hb);
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
      status_t mState;
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

  private:
    // lookup client full id to heart beat
    client_map_t mMap;

    // lookup client uuid to full id
    client_uuid_t mUUIDView;

    // heartbeat window in seconds
    float mHeartBeatWindow;

    // heartbeat window when to remove entires
    float mHeartBeatEvictWindow;

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
          fprintf(stderr, "releasing cap %s", cap->authid().c_str());
          mCaps.erase(id);
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

    shared_cap Get(authid_t id);

    std::string Print(std::string option);

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
    std::map<clientid_t, authid_set_t> mClientCaps;

  } ;

  Clients& Client()
  {

    return mClients;
  }

  Caps& Cap()
  {

    return mCaps;
  }

  void Print(std::string& out, std::string options="", bool monitoring=false);

  bool FillContainerMD(uint64_t id, eos::fusex::md& dir);
  bool FillFileMD(uint64_t id, eos::fusex::md& file);
  bool FillContainerCAP(uint64_t id, eos::fusex::md& md, eos::common::Mapping::VirtualIdentity* vid);
  Caps::shared_cap ValidateCAP(const eos::fusex::md&, mode_t mode);


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


private:

  std::thread* mMonitorThread;
  std::thread* mCapThread;
  std::atomic<bool> terminate_;
} ;

EOSMGMNAMESPACE_END

#endif
