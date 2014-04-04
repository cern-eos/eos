// ----------------------------------------------------------------------
// File: FsView.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#ifndef __EOSMGM_FSVIEW__HH__
#define __EOSMGM_FSVIEW__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Balancer.hh"
#include "mgm/GroupBalancer.hh"
#include "mgm/GeoBalancer.hh"
#include "mgm/Converter.hh"
#include "mgm/Namespace.hh"
#include "mgm/FileSystem.hh"
#include "common/RWMutex.hh"
#include "common/SymKeys.hh"
#include "common/Logging.hh"
#include "common/GlobalConfig.hh"
#include "common/TransferQueue.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#ifndef __APPLE__
#include <sys/vfs.h>
#else
#include <sys/param.h>
#include <sys/mount.h>
#endif

#include <map>
#include <set>

#ifndef EOSMGMFSVIEWTEST
#include "mgm/ConfigEngine.hh"
#endif
/*----------------------------------------------------------------------------*/
/**
 * @file FsView.hh
 * 
 * @brief Class representing the cluster configuration of EOS 
 * 
 * There are three views on EOS filesystems by space,group and by node.
 */

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
/**
 * @brief Class representing a grouped set of filesystems 
 * 
 */
/*----------------------------------------------------------------------------*/
class BaseView : public std::set<eos::common::FileSystem::fsid_t>
{
private:
  /// last heartbeat time
  time_t mHeartBeat;

  /// last heartbeat time as string
  std::string mHeartBeatString;

  /// time since last heartbeat as string
  std::string mHeartBeatDeltaString;

  /// status of the base view (meaning depends on inheritor)
  std::string mStatus;

  /// size of the base object (meaning depends on inheritor)
  std::string mSize;

  /// number of items in queue (meaning depends on inheritor)
  size_t mInQueue;

public:
  /// name of the base view
  std::string mName;

  /// type of the base view
  std::string mType;

  // ---------------------------------------------------------------------------
  /**
   * @brief Constructor
   * 
   */
  // ---------------------------------------------------------------------------

  BaseView ()
  {
    mStatus = "unknown";
    mHeartBeat = 0;
    mInQueue = 0;
  }

  // ---------------------------------------------------------------------------
  /**
   * @brief Destructor
   * 
   */
  // ---------------------------------------------------------------------------

  virtual
  ~BaseView () { };

  // ---------------------------------------------------------------------------
  /**
   * @brief Return the configuration queue prefix
   * @return return the configuration prefix
   */
  // ---------------------------------------------------------------------------

  virtual const char*
  GetConfigQueuePrefix ()
  {
    return "";
  }

  // ---------------------------------------------------------------------------
  // Print the view contents
  // ---------------------------------------------------------------------------
  void Print (std::string &out, std::string headerformat, std::string listformat);

  // ---------------------------------------------------------------------------
  // Return a member variable in the view
  // ---------------------------------------------------------------------------
  virtual std::string GetMember (std::string member);

  // ---------------------------------------------------------------------------
  // Set a member variable in a view
  virtual bool SetConfigMember (std::string key,
                                string value,
                                bool create = false,
                                std::string broadcastqueue = "",
                                bool isstatus = false);

  // ---------------------------------------------------------------------------
  // Return a configuration member
  // ---------------------------------------------------------------------------
  virtual std::string GetConfigMember (std::string key);

  // ---------------------------------------------------------------------------
  // Return all configuration keys
  // ---------------------------------------------------------------------------
  bool GetConfigKeys (std::vector<std::string> &keys);

  // ---------------------------------------------------------------------------
  /**
   * @brief Set the heartbeat time
   * @param hb heart beat time to set
   */
  // ---------------------------------------------------------------------------

  void
  SetHeartBeat (time_t hb)
  {
    mHeartBeat = hb;
  }

  // ---------------------------------------------------------------------------
  /**
   * 
   * @brief Set the status 
   * @param status status to set
   */
  // ---------------------------------------------------------------------------

  void
  SetStatus (const char* status)
  {
    mStatus = status;
  }

  // ---------------------------------------------------------------------------
  /**
   * @brief Return the status 
   */
  // ---------------------------------------------------------------------------

  const char*
  GetStatus ()
  {
    return mStatus.c_str();
  }

  // ---------------------------------------------------------------------------
  /**
   * @brief Get the heart beat time
   */
  // ---------------------------------------------------------------------------

  time_t
  GetHeartBeat ()
  {
    return mHeartBeat;
  }

  // ---------------------------------------------------------------------------
  /**
   * @brief Set the queue size
   * @param iq size to set a s queue size variable
   */
  // ---------------------------------------------------------------------------

  void
  SetInQueue (size_t iq)
  {
    mInQueue = iq;
  }

  // calculates the sum of <param> as long long
  long long SumLongLong (const char* param, bool lock = true);

  // calculates the sum of <param> as double
  double SumDouble (const char* param, bool lock = true);

  // calculates the average of <param> as double
  double AverageDouble (const char* param, bool lock = true);

  // calculates the maximum deviation from the average in a group
  double MaxDeviation (const char* param, bool lock = true);

  // calculates the standard deviation of <param> as double
  double SigmaDouble (const char* param, bool lock = true);
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Class describing a space (set of filesystems)
 * 
 */

/*----------------------------------------------------------------------------*/
class FsSpace : public BaseView
{
public:
#ifndef EOSMGMFSVIEWTEST
  /// threaded object supervising space balancing 
  Balancer* mBalancer;

  ///threaded object running layout conversion jobs
  Converter* mConverter;

  /// threaded object running group balancing
  GroupBalancer* mGroupBalancer;

  /// threaded object running geotag balancing
  GeoBalancer* mGeoBalancer;

#endif

  /// this variable is set when a configuration get's loaded to avoid overwriting of the loaded values by default values
  static bool gDisableDefaults;

  // ---------------------------------------------------------------------------
  /**
   * @brief Consructor
   * @param name name of the space to construct
   */
  // ---------------------------------------------------------------------------

  FsSpace (const char* name)
  {
    mName = name;
    mType = "spaceview";
#ifndef EOSMGMFSVIEWTEST
    mBalancer = new Balancer(name);
    mConverter = new Converter(name);
    mGroupBalancer = new GroupBalancer(name);
    mGeoBalancer = new GeoBalancer(name);

    if (!gDisableDefaults)
    {
      // -----------------------------------------------------------------------
      // set default balancing variables
      // -----------------------------------------------------------------------

      // disable balancing by default
      if (GetConfigMember("balancer") == "")
        SetConfigMember("balancer", "off", true, "/eos/*/mgm");
      // set deviation treshold
      if (GetConfigMember("balancer.threshold") == "")
        SetConfigMember("balancer.threshold", "20", true, "/eos/*/mgm");
      // set balancing rate per balancing stream
      if (GetConfigMember("balancer.node.rate") == "")
        SetConfigMember("balancer.node.rate", "25", true, "/eos/*/mgm");
      // set parallel balancing streams per node
      if (GetConfigMember("balancer.node.ntx") == "")
        SetConfigMember("balancer.node.ntx", "2", true, "/eos/*/mgm");
      // set drain rate per drain stream
      if (GetConfigMember("drain.node.rate") == "")
        SetConfigMember("drainer.node.rate", "25", true, "/eos/*/mgm");
      // set parallel draining streams per node
      if (GetConfigMember("drainer.node.ntx") == "")
        SetConfigMember("drainer.node.ntx", "2", true, "/eos/*/mgm");
      // set the grace period before drain start on opserror to 1 day
      if (GetConfigMember("graceperiod") == "")
        SetConfigMember("graceperiod", "86400", true, "/eos/*/mgm");
      // set the time for a drain by default to 1 day
      if (GetConfigMember("drainperiod") == "")
        SetConfigMember("drainperiod", "86400", true, "/eos/*/mgm");
      // set the scan interval by default to 1 week
      if (GetConfigMember("scaninterval") == "")
        SetConfigMember("scaninterval", "604800", true, "/eos/*/mgm");
      // disable quota by default
      if (GetConfigMember("quota") == "")
        SetConfigMember("quota", "off", true, "/eos/*/mgm");
      // set the group modulo to 0
      if (GetConfigMember("groupmod") == "")
        SetConfigMember("groupmod", "0", true, "/eos/*/mgm");
      // set the group size to 0
      if (GetConfigMember("groupsize") == "")
        SetConfigMember("groupsize", "0", true, "/eos/*/mgm");
      // disable converter by default
      if (GetConfigMember("converter") == "")
        SetConfigMember("converter", "off", true, "/eos/*/mgm");
      // set two converter streams by default
      if (GetConfigMember("converter.ntx") == "")
        SetConfigMember("converter.ntx", "2", true, "/eos/*/mgm");
      if (GetConfigMember("groupbalancer") == "")
        SetConfigMember("groupbalancer", "off", true, "/eos/*/mgm");
      // set the groupbalancer max number of scheduled files by default
      if (GetConfigMember("groupbalancer.ntx") == "")
        SetConfigMember("groupbalancer.ntx", "10", true, "/eos/*/mgm");
      // set the groupbalancer threshold by default
      if (GetConfigMember("groupbalancer.threshold") == "")
        SetConfigMember("groupbalancer.threshold", "5", true, "/eos/*/mgm");
      if (GetConfigMember("geotagbalancer") == "")
        SetConfigMember("geotagbalancer", "off", true, "/eos/*/mgm");
      // set the geotagbalancer max number of scheduled files by default
      if (GetConfigMember("geotagbalancer.ntx") == "")
        SetConfigMember("geotagbalancer.ntx", "10", true, "/eos/*/mgm");
      // set the geotagbalancer threshold by default
      if (GetConfigMember("geotagbalancer.threshold") == "")
        SetConfigMember("geotagbalancer.threshold", "5", true, "/eos/*/mgm");
      // disable lru by default
      if (GetConfigMember("lru") == "")
        SetConfigMember("converter", "off", true, "/eos/*/mgm");
      // set one week lru interval by default
      if (GetConfigMember("lru.interval") == "604800")
        SetConfigMember("converter.ntx", "2", true, "/eos/*/mgm");
    }

#endif

  }

  // ---------------------------------------------------------------------------
  /**
   * @brief stop function stopping threads before destruction
   */
  // ---------------------------------------------------------------------------

  void
  Stop ()
  {
#ifndef EOSMGMFSVIEWTEST
    if (mBalancer)
      mBalancer->Stop();
    if (mConverter)
      mConverter->Stop();
    if (mGroupBalancer)
      mGroupBalancer->Stop();
    if (mGeoBalancer)
      mGeoBalancer->Stop();
#endif
  }


  // ---------------------------------------------------------------------------
  /**
   * @brief Destructor
   */
  // ---------------------------------------------------------------------------

  virtual
  ~FsSpace ()
  {

#ifndef EOSMGMFSVIEWTEST
    if (mBalancer) delete mBalancer;
    if (mConverter) delete mConverter;
    if (mGroupBalancer) delete mGroupBalancer;
    if (mGeoBalancer) delete mGeoBalancer;
    mBalancer = 0;
    mConverter = 0;
    mGroupBalancer = 0;
    mGeoBalancer = 0;
#endif
  };

  /// configuration queue prefix 
  static std::string gConfigQueuePrefix;

  // ---------------------------------------------------------------------------
  //! return the configuration queue prefix (virtual method)
  // ---------------------------------------------------------------------------

  virtual const char*
  GetConfigQueuePrefix ()
  {
    return gConfigQueuePrefix.c_str();
  }

  // ---------------------------------------------------------------------------
  //! return the configuration queeu prefix
  // ---------------------------------------------------------------------------

  static const char*
  sGetConfigQueuePrefix ()
  {
    return gConfigQueuePrefix.c_str();
  }

  // ---------------------------------------------------------------------------
  // Apply the default space parameters 
  // ---------------------------------------------------------------------------
  bool ApplySpaceDefaultParameters (eos::mgm::FileSystem* fs, bool force = false);
  
  // ---------------------------------------------------------------------------
  // Reset the Drain state
  // ---------------------------------------------------------------------------
  void ResetDraining();
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Class describing a group (set of filesystems)
 */

/*----------------------------------------------------------------------------*/
class FsGroup : public BaseView
{
  friend class FsView;

protected:
  /// Index of the described group (normally 0,1,2,3...)
  unsigned int mIndex;

public:

  // ---------------------------------------------------------------------------
  /**
   * @brief Constructor
   * @param name name of the group e.g. 'default.0'
   * 
   */
  // ---------------------------------------------------------------------------

  FsGroup (const char* name)
  {
    mName = name;
    mType = "groupview";
    mIndex = 0;
  }

#ifdef EOSMGMFSVIEWTEST

  // ---------------------------------------------------------------------------
  /**
   * @brief Destructor 
   */
  // ---------------------------------------------------------------------------

  virtual
  ~FsGroup () { };
#else
  virtual ~FsGroup ();
#endif

  // ---------------------------------------------------------------------------
  /**
   * @brief Return index of the group
   * @return index
   */
  // ---------------------------------------------------------------------------

  unsigned int
  GetIndex ()
  {
    return mIndex;
  }

  /// configuration queue prefix
  static std::string gConfigQueuePrefix;

  // ---------------------------------------------------------------------------
  /**
   * @brief Return the configuration queue prefix (virtual function)
   */
  // ---------------------------------------------------------------------------

  virtual const char*
  GetConfigQueuePrefix ()
  {
    return gConfigQueuePrefix.c_str();
  }

  // ---------------------------------------------------------------------------
  /**
   * @brief Return the configuration queue prefix
   */
  // ---------------------------------------------------------------------------

  static const char*
  sGetConfigQueuePrefix ()
  {
    return gConfigQueuePrefix.c_str();
  }
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Class describing a group (set of filesystems)
 */

/*----------------------------------------------------------------------------*/
class FsNode : public BaseView
{
public:

  /// Name of the responsible manager
  static std::string gManagerId;

  // Return a member variable
  virtual std::string GetMember (std::string name);

  /// Gateway Transfer Queue
  eos::common::TransferQueue* mGwQueue;

  // ---------------------------------------------------------------------------
  /** 
   * @brief Constructor
   * @param name nodeview name 
   */
  // ---------------------------------------------------------------------------

  FsNode (const char* name)
  {
    mName = name;
    mType = "nodesview";
    std::string n = mName.c_str();
    n += "/gw";
    mGwQueue = new eos::common::TransferQueue(mName.c_str(), n.c_str(), "txq", (eos::common::FileSystem*)0, eos::common::GlobalConfig::gConfig.SOM(), false);
  }

  // ---------------------------------------------------------------------------
  // Destructor
  // ---------------------------------------------------------------------------
  virtual ~FsNode ();

  // ---------------------------------------------------------------------------
  /**
   * @brief Set the configuration default values for a node
   * 
   */
  // ---------------------------------------------------------------------------

  void
  SetNodeConfigDefault ()
  {
#ifndef EOSMGMFSVIEWTEST
    // define the manager ID
    if (!(GetConfigMember("manager").length()))
    {
      SetConfigMember("manager", gManagerId, true, mName.c_str(), true);
    }
    // by default set 2 balancing streams per node
    if (!(GetConfigMember("stat.balance.ntx").length()))
    {
      SetConfigMember("stat.balance.ntx", "2", true, mName.c_str(), true);
    }
    // by default set 25 MB/s stream balancing rate
    if (!(GetConfigMember("stat.balance.rate").length()))
    {
      SetConfigMember("stat.balance.rate", "25", true, mName.c_str(), true);
    }
    // set the default sym key from the sym key store
    eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
    // store the sym key as configuration member
    if (!(GetConfigMember("symkey").length()))
    {
      SetConfigMember("symkey", symkey->GetKey64(), true, mName.c_str(), true);
    }
    // set the default debug level to notice
    if (!(GetConfigMember("debug.level").length()))
    {
      SetConfigMember("debug.level", "info", true, mName.c_str(), true);
    }
    // set by default as no transfer gateway
    if ((GetConfigMember("txgw") != "on") && (GetConfigMember("txgw") != "off"))
    {
      SetConfigMember("txgw", "off", true, mName.c_str(), true);
    }

    // set by default 10 transfers per gateway node
    if ((strtol(GetConfigMember("gw.ntx").c_str(), 0, 10) == 0) ||
        (strtol(GetConfigMember("gw.ntx").c_str(), 0, 10) == LONG_MAX))
    {
      SetConfigMember("gw.ntx", "10", true, mName.c_str(), true);
    }

    // set by default the gateway stream transfer speed to 120 Mb/s
    if ((strtol(GetConfigMember("gw.rate").c_str(), 0, 10) == 0) ||
        (strtol(GetConfigMember("gw.rate").c_str(), 0, 10) == LONG_MAX))
    {
      SetConfigMember("gw.rate", "120", true, mName.c_str(), true);
    }

    // set by default the MGM domain e.g. same geographical position as the MGM
    if (!(GetConfigMember("domain").length()))
    {
      SetConfigMember("domain", "MGM", true, mName.c_str(), true);
    }
#endif
  }

  /// configuration queue prefix
  static std::string gConfigQueuePrefix;

  // ---------------------------------------------------------------------------
  /**
   * @brief Return the configuration queue prefix (virtual function)
   */
  // ---------------------------------------------------------------------------

  virtual const char*
  GetConfigQueuePrefix ()
  {
    return gConfigQueuePrefix.c_str();
  }

  // ---------------------------------------------------------------------------
  /**
   * @brief Return the configuration queue prefix
   */
  // ---------------------------------------------------------------------------

  static const char*
  sGetConfigQueuePrefix ()
  {
    return gConfigQueuePrefix.c_str();
  }
};

/*----------------------------------------------------------------------------*/
/**
 * @brief Class describing an EOS pool including views
 */

/*----------------------------------------------------------------------------*/
class FsView : public eos::common::LogId
{
private:

  /// next free filesystem ID if a new one has to be registered
  eos::common::FileSystem::fsid_t NextFsId;

  /// map translating a file system ID to a unique ID
  std::map<eos::common::FileSystem::fsid_t, std::string> Fs2UuidMap;

  /// map translating a unique ID to a filesystem ID
  std::map<std::string, eos::common::FileSystem::fsid_t> Uuid2FsMap;

  /// MGM configuration queue name
  std::string MgmConfigQueueName;

public:

#ifndef EOSMGMFSVIEWTEST
  static ConfigEngine* ConfEngine;
#endif

  // this adds or modifies a filesystem
  bool Register (FileSystem* fs);

  // move a filesystem to another group
  bool MoveGroup (FileSystem* fs, std::string group);

  // store the filesystem configuration into the config engine
  // should be called whenever a filesystem wide parameters is changed
  void StoreFsConfig (FileSystem* fs);

  // remove a filesystem
  bool UnRegister (FileSystem* fs);

  // check's if a queue+path exists already
  bool ExistsQueue (std::string queue, std::string queuepath);

  // add or modify an fst node
  bool RegisterNode (const char* nodequeue);

  // remove a node
  bool UnRegisterNode (const char* nodequeue);

  // remoev all nodes
  void UnRegisterNodes ();

  // add or modify a space
  bool RegisterSpace (const char* spacename);

  // remove a space
  bool UnRegisterSpace (const char* spacename);

  // add or modify a group
  bool RegisterGroup (const char* groupname);

  // remove a group
  bool UnRegisterGroup (const char* groupname);

  /// Mutex protecting all ...View variables
  eos::common::RWMutex ViewMutex;
  /// Mutex protecting all ...Map variables
  eos::common::RWMutex MapMutex;

  /// Map translating a space name to a set of group objects
  std::map<std::string, std::set<FsGroup*> > mSpaceGroupView;

  /// Map translating a space name to a space view object
  std::map<std::string, FsSpace* > mSpaceView;

  /// Map translating a group name to a group view object
  std::map<std::string, FsGroup* > mGroupView;

  /// Map translating a node name to a node view object
  std::map<std::string, FsNode* > mNodeView;

  /// Map translating a filesystem ID to a file system object
  std::map<eos::common::FileSystem::fsid_t, FileSystem*> mIdView;

  /// Map translating a filesystem object pointer to a filesystem ID
  std::map<FileSystem*, eos::common::FileSystem::fsid_t> mFileSystemView;

  /// Mutex protecting the set of gateway nodes mGwNodes
  eos::common::RWMutex GwMutex;

  /// Set containing all nodes which are usable as a gateway machine
  std::set<std::string> mGwNodes;

  // ---------------------------------------------------------------------------
  // find filesystem by queue path
  // ---------------------------------------------------------------------------
  FileSystem* FindByQueuePath (std::string &queuepath);

  // ---------------------------------------------------------------------------
  // create a filesystem mapping
  // ---------------------------------------------------------------------------
  eos::common::FileSystem::fsid_t CreateMapping (std::string fsuuid);

  // ---------------------------------------------------------------------------
  // provide a filesystem mapping
  // ---------------------------------------------------------------------------
  bool ProvideMapping (std::string fsuuid, eos::common::FileSystem::fsid_t fsid);

  // ---------------------------------------------------------------------------
  // get a filesystem mapping by unique ID
  // ---------------------------------------------------------------------------
  eos::common::FileSystem::fsid_t GetMapping (std::string fsuuid);

  // ---------------------------------------------------------------------------
  // check for an exinsting mapping by filesystem id
  // ---------------------------------------------------------------------------

  bool
  HasMapping (eos::common::FileSystem::fsid_t fsid)
  {
    return (Fs2UuidMap.count(fsid) > 0) ? true : false;
  }

  // ---------------------------------------------------------------------------
  // remove a mapping providing filesystem ID and unique ID
  // ---------------------------------------------------------------------------
  bool RemoveMapping (eos::common::FileSystem::fsid_t fsid, std::string fsuuid);\
  
  // ---------------------------------------------------------------------------
  // remove a mapping providing filesystem ID
  // ---------------------------------------------------------------------------
  bool RemoveMapping (eos::common::FileSystem::fsid_t fsid);

  // ---------------------------------------------------------------------------
  // Print views (space,group,nodes)
  void PrintSpaces (std::string &out, std::string headerformat, std::string listformat, const char* selection = 0);
  void PrintGroups (std::string &out, std::string headerformat, std::string listformat, const char* selection = 0);
  void PrintNodes (std::string &out, std::string headerformat, std::string listformat, const char* selection = 0);

  // ---------------------------------------------------------------------------
  // Return printout formats
  // ---------------------------------------------------------------------------
  static std::string GetNodeFormat (std::string option);
  static std::string GetGroupFormat (std::string option);
  static std::string GetSpaceFormat (std::string option);
  static std::string GetFileSystemFormat (std::string option);

  void Reset (); // clears all mappings and filesystem objects obtaining locks

  /// Thread ID of the heartbeat thread
  pthread_t hbthread;

  // static thread startup function
  static void* StaticHeartBeatCheck (void*);

  // thread loop function checking heartbeats
  void* HeartBeatCheck ();

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------

  FsView ()
  {
    MgmConfigQueueName = "";

#ifndef EOSMGMFSVIEWTEST
    ConfEngine = 0;
#endif
    XrdSysThread::Run(&hbthread,
                      FsView::StaticHeartBeatCheck,
                      static_cast<void *> (this),
                      XRDSYSTHREAD_HOLD,
                      "HeartBeat Thread");
  }

  // ---------------------------------------------------------------------------
  //! Stop the heart beat thread
  // ---------------------------------------------------------------------------

  void
  StopHeartBeat ()
  {
    if (hbthread)
    {
      XrdSysThread::Cancel(hbthread);
      XrdSysThread::Join(hbthread, 0);
      hbthread = 0;
    }
  }

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------

  virtual
  ~FsView ()
  {
    StopHeartBeat();
  };

  void
  SetConfigQueues (
                   const char* mgmconfigqueue,
                   const char* nodeconfigqueue,
                   const char* groupconfigqueue,
                   const char* spaceconfigqueue
                   )
  {
    FsSpace::gConfigQueuePrefix = spaceconfigqueue;
    FsGroup::gConfigQueuePrefix = groupconfigqueue;
    FsNode::gConfigQueuePrefix = nodeconfigqueue;
    MgmConfigQueueName = mgmconfigqueue;
  }

#ifndef EOSMGMFSVIEWTEST

  // ---------------------------------------------------------------------------
  //! Set the configuration engine object
  // ---------------------------------------------------------------------------

  void
  SetConfigEngine (ConfigEngine* engine)
  {
    ConfEngine = engine;
  }

  // ---------------------------------------------------------------------------
  // Apply all filesystem configuration key-val pair
  // ---------------------------------------------------------------------------
  bool ApplyFsConfig (const char* key, std::string &val);

  // ---------------------------------------------------------------------------
  // Apply a global configuration key-val pair
  // ---------------------------------------------------------------------------
  bool ApplyGlobalConfig (const char* key, std::string &val);

  // ---------------------------------------------------------------------------
  // Set a global configuration key-val pair
  // ---------------------------------------------------------------------------
  bool SetGlobalConfig (std::string key, std::string value);

  // ---------------------------------------------------------------------------
  // Get a global configuration value
  // ---------------------------------------------------------------------------
  std::string GetGlobalConfig (std::string key);

#endif

  // ---------------------------------------------------------------------------
  // Set the next available filesystem ID
  // ---------------------------------------------------------------------------
  void SetNextFsId (eos::common::FileSystem::fsid_t fsid);

  /// static singleton object hosting the filesystem view object
  static FsView gFsView;
};

EOSMGMNAMESPACE_END

#endif
