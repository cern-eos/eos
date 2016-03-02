//------------------------------------------------------------------------------
// File: FsView.hh
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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
#include <cfloat>
#include "XrdOuc/XrdOucString.hh"
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
//------------------------------------------------------------------------------
//! @file FsView.hh
//! @brief Class representing the cluster configuration of EOS
//! There are three views on EOS filesystems by space, group and node.
//------------------------------------------------------------------------------
EOSMGMNAMESPACE_BEGIN

struct GeoTreeNode;
struct GeoTreeLeaf;
struct GeoTreeElement;
class GeoTree;

//------------------------------------------------------------------------------
//! Base class representing any element in a GeoTree
//------------------------------------------------------------------------------
struct GeoTreeElement
{
  //! Pointer to father node in the tree
  GeoTreeElement* mFather;

  //! Tag token for example BBB in AA::BBB:CC
  std::string mTagToken;

  //! Full geo tag for example AA::BBB:CC in AA::BBB:CC
  std::string mFullTag;

  //! An auxiliary numbering to run the aggregator
  mutable size_t mId;
  typedef eos::common::FileSystem::fsid_t fsid_t;

  //! All the FileSystems attached to this node of the tree
  std::set<fsid_t> mFsIds;

  /// Map geoTreeTag -> son branches
  std::map<std::string , GeoTreeElement*> mSons;

  ~GeoTreeElement();
};

//------------------------------------------------------------------------------
//! @brief A helper class to order branches in a GeoTree in the proper display
//! order.
//------------------------------------------------------------------------------
class GeoTreeNodeOrderHelper
{
 public:
  bool operator()(const GeoTreeElement* const& left,
                  const GeoTreeElement* const& right) const
  {
    return (left->mFullTag > right->mFullTag);
  }
};

//------------------------------------------------------------------------------
//! Base class representing a functor to compute statistics along a GeoTree
//------------------------------------------------------------------------------
class GeoTreeAggregator
{
 public:
  virtual ~GeoTreeAggregator()
  {};

  // Initialize the aggregator
  virtual bool init(const std::vector<std::string>& geotags,
                    const std::vector<size_t>& depthLevelsIndexes) = 0;

  // Aggregate the leaves at the last level of the tree
  virtual bool aggregateLeaves(const std::set<eos::common::FileSystem::fsid_t>&
                               leaves,
                               const size_t& idx) = 0;

  //----------------------------------------------------------------------------
  // Aggregate the nodes at intermediate levels
  // WARNING target node might be part of the nodes to aggregate.
  // Carefull before overwriting the target node.
  //----------------------------------------------------------------------------
  virtual bool aggregateNodes(const std::map<std::string , GeoTreeElement*>&
                              nodes,
                              const size_t& idx, bool includeSelf = false) = 0;

  // Aggregate the leaves any level of the tree
  virtual bool deepAggregate(const std::set<eos::common::FileSystem::fsid_t>&
                             leaves,
                             const size_t& idx) = 0;

  // Aggregate the leaves and the nodes at any level of the tree
  virtual bool aggregateLeavesAndNodes(
      const std::set<eos::common::FileSystem::fsid_t>& leaves,
      const std::map<std::string , GeoTreeElement*>& nodes,
      const size_t& idx)
  {
    return (leaves.empty() ? true : aggregateLeaves(leaves, idx))
        && (nodes.empty() ? true : aggregateNodes(nodes, idx, !leaves.empty()));
  }
};

//------------------------------------------------------------------------------
//! Class representing a tree-structured set of fsids
//------------------------------------------------------------------------------
class GeoTree
{
  typedef eos::common::FileSystem::fsid_t fsid_t;
  typedef GeoTreeElement tElement;
  typedef GeoTreeLeaf tLeaf;
  typedef GeoTreeNode tNode;

  //! The root branch of the tree
  tElement* pRoot;

  //! All the elements of the tree collected by depth
  std::vector<std::set<tElement*, GeoTreeNodeOrderHelper > > pLevels;

  //! All the leaves of the tree
  std::map<fsid_t, tElement*> pLeaves;

  //----------------------------------------------------------------------------
  //! Get the geotag of FileSystem
  //----------------------------------------------------------------------------
  std::string getGeoTag(const fsid_t& fs) const;

 public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  GeoTree();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~GeoTree();

  //----------------------------------------------------------------------------
  //! Insert a FileSystem into the tree
  //----------------------------------------------------------------------------
  bool insert(const fsid_t& fs);

  //----------------------------------------------------------------------------
  //! Remove a FileSystem from the tree
  //----------------------------------------------------------------------------
  bool erase(const fsid_t& fs);

  //----------------------------------------------------------------------------
  //! Get the geotag at which the fs is stored if found
  //----------------------------------------------------------------------------
  bool getGeoTagInTree(const fsid_t& fs , std::string& geoTag);

  //----------------------------------------------------------------------------
  //! Number of FileSystems in the tree
  //----------------------------------------------------------------------------
  size_t size() const;

  //----------------------------------------------------------------------------
  //! STL const_iterator class
  //!
  //! Only the leaves are iterated in alphabetical order of their geotag
  //----------------------------------------------------------------------------
  class const_iterator: public
  std::iterator<std::bidirectional_iterator_tag, fsid_t>
  {
    friend class GeoTree;
    std::map<fsid_t, tElement*>::const_iterator mIt;
   public:
    const_iterator operator++(int);
    const_iterator operator--(int);
    const_iterator operator++();
    const_iterator operator--();
    const eos::common::FileSystem::fsid_t& operator*() const;
    operator const eos::common::FileSystem::fsid_t* () const;
    const const_iterator& operator= (const const_iterator& it);
  };

  const_iterator begin() const;
  const_iterator cbegin() const;
  const_iterator end() const;
  const_iterator cend() const;
  const_iterator find(const fsid_t& fsid) const;

  //----------------------------------------------------------------------------
  //! Run an aggregator through the tree
  //----------------------------------------------------------------------------
  bool runAggregator(GeoTreeAggregator* aggregator) const;

  //----------------------------------------------------------------------------
  //! Run an aggregator through the tree
  //----------------------------------------------------------------------------
  bool runDeepAggregator(GeoTreeAggregator* aggregator)
  {
    // loop over the last level of Aggregate and call AggregateDeepLeaves
    // loop from end-1 to beginning in mLevels and call AggregateDeppNodes
    // NOT IMPLEMENTED
    return false;
  }

  //----------------------------------------------------------------------------
  //! Recursive debug helper function to display the tree
  //----------------------------------------------------------------------------
  char* dumpTree(char* buffer, GeoTreeElement* el,
                 std::string fullgeotag = "") const;

  //----------------------------------------------------------------------------
  //! Debug helper function to display the leaves in the tree
  //----------------------------------------------------------------------------
  char* dumpLeaves(char* buffer) const;

  //----------------------------------------------------------------------------
  //! Debug helper function to display the elements of the tree sorted by levels
  //----------------------------------------------------------------------------
  char* dumpLevels(char* buffer) const;

  //----------------------------------------------------------------------------
  //! Debug helper function to display all the content of the tree
  //----------------------------------------------------------------------------
  char* dump(char* buffer) const;
};

//------------------------------------------------------------------------------
//! Class representing a grouped set of filesystems
//------------------------------------------------------------------------------
class BaseView : public GeoTree
{
 private:
  //! Last heartbeat time
  time_t mHeartBeat;

  // Last heartbeat time as string
  std::string mHeartBeatString;

  //! Time since last heartbeat as string
  std::string mHeartBeatDeltaString;

  //! Status of the base view (meaning depends on inheritor)
  std::string mStatus;

  //! Size of the base object (meaning depends on inheritor)
  std::string mSize;

  //! Number of items in queue (meaning depends on inheritor)
  size_t mInQueue;

 public:

  std::string mName; ///< Name of the base view
  std::string mType; ///< type of the base view

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  BaseView()
  {
    mStatus = "unknown";
    mHeartBeat = 0;
    mInQueue = 0;
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~BaseView() {};

  //----------------------------------------------------------------------------
  //! Return the configuration queue prefix
  //!
  //! @return return the configuration prefix
  //----------------------------------------------------------------------------
  virtual const char* GetConfigQueuePrefix()
  {
    return "";
  }

  //----------------------------------------------------------------------------
  //! Print the view contents
  //----------------------------------------------------------------------------
  void Print(std::string& out, std::string headerformat, std::string listformat,
             unsigned outdepth, std::vector<std::string>& selections);

  //----------------------------------------------------------------------------
  //! Return a member variable in the view
  //----------------------------------------------------------------------------
  virtual std::string GetMember(std::string member);

  //----------------------------------------------------------------------------
  //! Set a member variable in a view
  //----------------------------------------------------------------------------
  virtual bool SetConfigMember(std::string key, string value,
                               bool create = false,
                               std::string broadcastqueue = "",
                               bool isstatus = false);

  //----------------------------------------------------------------------------
  //! Return a configuration member
  //----------------------------------------------------------------------------
  virtual std::string GetConfigMember(std::string key);

  //----------------------------------------------------------------------------
  //! Return all configuration keys
  //----------------------------------------------------------------------------
  bool GetConfigKeys(std::vector<std::string>& keys);

  //----------------------------------------------------------------------------
  //! Set the heartbeat time
  //! @param hb heart beat time to set
  //----------------------------------------------------------------------------
  void SetHeartBeat(time_t hb)
  {
    mHeartBeat = hb;
  }

  //----------------------------------------------------------------------------
  //! Set the status
  //! @param status status to set
  //----------------------------------------------------------------------------
  void SetStatus(const char* status)
  {
    mStatus = status;
  }

  //----------------------------------------------------------------------------
  //! Return the status
  //----------------------------------------------------------------------------
  const char* GetStatus()
  {
    return mStatus.c_str();
  }

  //----------------------------------------------------------------------------
  //! Get the heartbeat time
  //----------------------------------------------------------------------------
  time_t GetHeartBeat()
  {
    return mHeartBeat;
  }

  //----------------------------------------------------------------------------
  //! Set the queue size
  //! @param iq size to set a s queue size variable
  //----------------------------------------------------------------------------
  void SetInQueue(size_t iq)
  {
    mInQueue = iq;
  }

  //----------------------------------------------------------------------------
  //! Calculate the sum of <param> as long long
  //----------------------------------------------------------------------------
  long long SumLongLong(const char* param, bool lock = true,
                        const std::set<eos::common::FileSystem::fsid_t>* subset = NULL);

  //----------------------------------------------------------------------------
  //! Calculate the sum of <param> as double
  //----------------------------------------------------------------------------
  double SumDouble(const char* param, bool lock = true,
                   const std::set<eos::common::FileSystem::fsid_t>* subset = NULL);

  //----------------------------------------------------------------------------
  //! Calculates the average of <param> as double
  //----------------------------------------------------------------------------
  double AverageDouble(const char* param, bool lock = true,
                       const std::set<eos::common::FileSystem::fsid_t>* subset = NULL);

  //----------------------------------------------------------------------------
  //! Calculates the maximum deviation from the average in a group
  //---------------------------------------------------------------------------
  double MaxAbsDeviation(const char* param, bool lock = true,
                         const std::set<eos::common::FileSystem::fsid_t>* subset = NULL);

  double MaxDeviation(const char* param, bool lock = true,
                      const std::set<eos::common::FileSystem::fsid_t>* subset = NULL);

  double MinDeviation(const char* param, bool lock = true,
                      const std::set<eos::common::FileSystem::fsid_t>* subset = NULL);

  //----------------------------------------------------------------------------
  //! Calculates the standard deviation of <param> as double
  //----------------------------------------------------------------------------
  double SigmaDouble(const char* param, bool lock = true,
                     const std::set<eos::common::FileSystem::fsid_t>* subset = NULL);

  //----------------------------------------------------------------------------
  //! Calculates the number of fsid considered for average
  //----------------------------------------------------------------------------
  long long
  ConsiderCount(bool lock,
                const std::set<eos::common::FileSystem::fsid_t>* subset);

  //----------------------------------------------------------------------------
  //! Calculate the number of fsid regardless of being considered for
  //! averages or not.
  //----------------------------------------------------------------------------
  long long TotalCount(bool lock,
                       const std::set<eos::common::FileSystem::fsid_t>* subset);
};

//------------------------------------------------------------------------------
//! Class describing a space (set of filesystems)
//------------------------------------------------------------------------------
class FsSpace : public BaseView
{
 public:
#ifndef EOSMGMFSVIEWTEST
  Balancer* mBalancer; ///< Threaded object supervising space balancing
  Converter* mConverter; ///< Threaded object running layout conversion jobs
  GroupBalancer* mGroupBalancer; ///< Threaded object running group balancing
  GeoBalancer* mGeoBalancer; ///< Threaded object running geotag balancing

#endif

  //! Set when a configuration gets loaded to avoid overwriting of the loaded
  //! values by default values
  static bool gDisableDefaults;

  //----------------------------------------------------------------------------
  //! Constructor
  //! @param name name of the space to construct
  //----------------------------------------------------------------------------
  FsSpace(const char* name)
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
      // Set default balancing variables
      // Disable balancing by default
      if (GetConfigMember("balancer") == "")
        SetConfigMember("balancer", "off", true, "/eos/*/mgm");

      // Set deviation treshold
      if (GetConfigMember("balancer.threshold") == "")
        SetConfigMember("balancer.threshold", "20", true, "/eos/*/mgm");

      // Set balancing rate per balancing stream
      if (GetConfigMember("balancer.node.rate") == "")
        SetConfigMember("balancer.node.rate", "25", true, "/eos/*/mgm");

      // Set parallel balancing streams per node
      if (GetConfigMember("balancer.node.ntx") == "")
        SetConfigMember("balancer.node.ntx", "2", true, "/eos/*/mgm");

      // Set drain rate per drain stream
      if (GetConfigMember("drain.node.rate") == "")
        SetConfigMember("drainer.node.rate", "25", true, "/eos/*/mgm");

      // Set parallel draining streams per node
      if (GetConfigMember("drainer.node.ntx") == "")
        SetConfigMember("drainer.node.ntx", "2", true, "/eos/*/mgm");

      // Set the grace period before drain start on opserror to 1 day
      if (GetConfigMember("graceperiod") == "")
        SetConfigMember("graceperiod", "86400", true, "/eos/*/mgm");

      // Set the time for a drain by default to 1 day
      if (GetConfigMember("drainperiod") == "")
        SetConfigMember("drainperiod", "86400", true, "/eos/*/mgm");

      // Set the scan interval by default to 1 week
      if (GetConfigMember("scaninterval") == "")
        SetConfigMember("scaninterval", "604800", true, "/eos/*/mgm");

      // Disable quota by default
      if (GetConfigMember("quota") == "")
        SetConfigMember("quota", "off", true, "/eos/*/mgm");

      // Set the group modulo to 0
      if (GetConfigMember("groupmod") == "")
        SetConfigMember("groupmod", "0", true, "/eos/*/mgm");

      // Set the group size to 0
      if (GetConfigMember("groupsize") == "")
        SetConfigMember("groupsize", "0", true, "/eos/*/mgm");

      // Disable converter by default
      if (GetConfigMember("converter") == "")
        SetConfigMember("converter", "off", true, "/eos/*/mgm");

      // Set two converter streams by default
      if (GetConfigMember("converter.ntx") == "")
        SetConfigMember("converter.ntx", "2", true, "/eos/*/mgm");

      if (GetConfigMember("groupbalancer") == "")
        SetConfigMember("groupbalancer", "off", true, "/eos/*/mgm");

      // Set the groupbalancer max number of scheduled files by default
      if (GetConfigMember("groupbalancer.ntx") == "")
        SetConfigMember("groupbalancer.ntx", "10", true, "/eos/*/mgm");

      // Set the groupbalancer threshold by default
      if (GetConfigMember("groupbalancer.threshold") == "")
        SetConfigMember("groupbalancer.threshold", "5", true, "/eos/*/mgm");

      if (GetConfigMember("geotagbalancer") == "")
        SetConfigMember("geotagbalancer", "off", true, "/eos/*/mgm");

      // Set the geotagbalancer max number of scheduled files by default
      if (GetConfigMember("geotagbalancer.ntx") == "")
        SetConfigMember("geotagbalancer.ntx", "10", true, "/eos/*/mgm");

      // Set the geotagbalancer threshold by default
      if (GetConfigMember("geotagbalancer.threshold") == "")
        SetConfigMember("geotagbalancer.threshold", "5", true, "/eos/*/mgm");

      // Disable lru by default
      if (GetConfigMember("lru") == "")
        SetConfigMember("converter", "off", true, "/eos/*/mgm");

      // Set one week lru interval by default
      if (GetConfigMember("lru.interval") == "604800")
        SetConfigMember("converter.ntx", "2", true, "/eos/*/mgm");
    }

#endif
  }

  //----------------------------------------------------------------------------
  //! Stop function stopping threads before destruction
  //----------------------------------------------------------------------------
  void Stop()
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

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FsSpace()
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


  static std::string gConfigQueuePrefix; ///<  Configuration queue prefix

  //----------------------------------------------------------------------------
  //! Get the configuration queue prefix
  //----------------------------------------------------------------------------
  virtual const char* GetConfigQueuePrefix()
  {
    return gConfigQueuePrefix.c_str();
  }

  //----------------------------------------------------------------------------
  //! Get the configuration queeu prefix
  //----------------------------------------------------------------------------
  static const char* sGetConfigQueuePrefix()
  {
    return gConfigQueuePrefix.c_str();
  }

  //----------------------------------------------------------------------------
  //! Apply the default space parameters
  //----------------------------------------------------------------------------
  bool ApplySpaceDefaultParameters(eos::mgm::FileSystem* fs, bool force = false);

  //----------------------------------------------------------------------------
  // Reset the Drain state
  //----------------------------------------------------------------------------
  void ResetDraining();
};

//------------------------------------------------------------------------------
//! Class describing a group (set of filesystems)
//------------------------------------------------------------------------------
class FsGroup : public BaseView
{
  friend class FsView;

 protected:
  //! Index of the described group (normally 0,1,2,3...)
  unsigned int mIndex;

 public:

  //----------------------------------------------------------------------------
  //! Constructor
  //! @param name name of the group e.g. 'default.0'
  //----------------------------------------------------------------------------
  FsGroup(const char* name)
  {
    mName = name;
    mType = "groupview";
    mIndex = 0;
  }

#ifdef EOSMGMFSVIEWTEST

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FsGroup() {};
#else
  virtual ~FsGroup();
#endif

  //----------------------------------------------------------------------------
  //! Return index of the group
  //----------------------------------------------------------------------------
  unsigned int GetIndex()
  {
    return mIndex;
  }

  static std::string gConfigQueuePrefix; ///< Configuration queue prefix

  //----------------------------------------------------------------------------
  //! Return the configuration queue prefix (virtual function)
  //----------------------------------------------------------------------------
  virtual const char* GetConfigQueuePrefix()
  {
    return gConfigQueuePrefix.c_str();
  }

  //----------------------------------------------------------------------------
  //! Return the configuration queue prefix
  //----------------------------------------------------------------------------
  static const char* sGetConfigQueuePrefix()
  {
    return gConfigQueuePrefix.c_str();
  }
};

//------------------------------------------------------------------------------
//! Class describing a group (set of filesystems)
//------------------------------------------------------------------------------
class FsNode : public BaseView
{
 public:

  static std::string gManagerId; ///< Name of the responsible manager
  eos::common::TransferQueue* mGwQueue; ///< Gateway transfer queue

  //----------------------------------------------------------------------------
  //! Constructor
  //! @param name nodeview name
  //----------------------------------------------------------------------------
  FsNode(const char* name)
  {
    mName = name;
    mType = "nodesview";
    std::string n = mName.c_str();
    n += "/gw";
    mGwQueue = new eos::common::TransferQueue(
        mName.c_str(), n.c_str(), "txq", (eos::common::FileSystem*) 0,
        eos::common::GlobalConfig::gConfig.SOM(), false);
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FsNode();

  //----------------------------------------------------------------------------
  //! Return a member variable
  //----------------------------------------------------------------------------
  virtual std::string GetMember(std::string name);

  //----------------------------------------------------------------------------
  //! Set the configuration default values for a node
  //----------------------------------------------------------------------------
  void SetNodeConfigDefault()
  {
#ifndef EOSMGMFSVIEWTEST

    // Define the manager ID
    if (!(GetConfigMember("manager").length()))
    {
      SetConfigMember("manager", gManagerId, true, mName.c_str(), true);
    }

    // By default set 2 balancing streams per node
    if (!(GetConfigMember("stat.balance.ntx").length()))
    {
      SetConfigMember("stat.balance.ntx", "2", true, mName.c_str(), true);
    }

    // By default set 25 MB/s stream balancing rate
    if (!(GetConfigMember("stat.balance.rate").length()))
    {
      SetConfigMember("stat.balance.rate", "25", true, mName.c_str(), true);
    }

    // Set the default sym key from the sym key store
    eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

    // Store the sym key as configuration member
    if (!(GetConfigMember("symkey").length()))
    {
      SetConfigMember("symkey", symkey->GetKey64(), true, mName.c_str(), true);
    }

    // Set the default debug level to notice
    if (!(GetConfigMember("debug.level").length()))
    {
      SetConfigMember("debug.level", "info", true, mName.c_str(), true);
    }

    // Set by default as no transfer gateway
    if ((GetConfigMember("txgw") != "on") && (GetConfigMember("txgw") != "off"))
    {
      SetConfigMember("txgw", "off", true, mName.c_str(), true);
    }

    // Set by default 10 transfers per gateway node
    if ((strtol(GetConfigMember("gw.ntx").c_str(), 0, 10) == 0) ||
        (strtol(GetConfigMember("gw.ntx").c_str(), 0, 10) == LONG_MAX))
    {
      SetConfigMember("gw.ntx", "10", true, mName.c_str(), true);
    }

    // Set by default the gateway stream transfer speed to 120 Mb/s
    if ((strtol(GetConfigMember("gw.rate").c_str(), 0, 10) == 0) ||
        (strtol(GetConfigMember("gw.rate").c_str(), 0, 10) == LONG_MAX))
    {
      SetConfigMember("gw.rate", "120", true, mName.c_str(), true);
    }

    // Set by default the MGM domain e.g. same geographical position as the MGM
    if (!(GetConfigMember("domain").length()))
    {
      SetConfigMember("domain", "MGM", true, mName.c_str(), true);
    }

#endif
  }

  static std::string gConfigQueuePrefix; ///< Configuration queue prefix

  //----------------------------------------------------------------------------
  //! Return the configuration queue prefix (virtual function)
  //----------------------------------------------------------------------------
  virtual const char* GetConfigQueuePrefix()
  {
    return gConfigQueuePrefix.c_str();
  }

  //----------------------------------------------------------------------------
  //! Return the configuration queue prefix
  //----------------------------------------------------------------------------
  static const char* sGetConfigQueuePrefix()
  {
    return gConfigQueuePrefix.c_str();
  }
};

//------------------------------------------------------------------------------
//! Class describing an EOS pool including views
//------------------------------------------------------------------------------
class FsView : public eos::common::LogId
{
 private:

  //! Next free filesystem ID if a new one has to be registered
  eos::common::FileSystem::fsid_t NextFsId;

  //! Map translating a file system ID to a unique ID
  std::map<eos::common::FileSystem::fsid_t, std::string> Fs2UuidMap;

  //! Map translating a unique ID to a filesystem ID
  std::map<std::string, eos::common::FileSystem::fsid_t> Uuid2FsMap;
  std::string MgmConfigQueueName; ///< MGM configuration queue name

 public:

#ifndef EOSMGMFSVIEWTEST
  static ConfigEngine* ConfEngine;
#endif

  //----------------------------------------------------------------------------
  //! Add or modify a filesystem
  //----------------------------------------------------------------------------
  bool Register(FileSystem* fs, bool registerInGeoTreeEngine = true);

  //----------------------------------------------------------------------------
  //! Move a filesystem to another group
  //----------------------------------------------------------------------------
  bool MoveGroup(FileSystem* fs, std::string group);

  //----------------------------------------------------------------------------
  //! Store the filesystem configuration into the config engine. Should be
  //! called whenever a filesystem wide parameters is changed
  //----------------------------------------------------------------------------
  void StoreFsConfig(FileSystem* fs);

  //----------------------------------------------------------------------------
  //! Remove a filesystem
  //----------------------------------------------------------------------------
  bool UnRegister(FileSystem* fs, bool unregisterInGeoTreeEngine = true);

  //----------------------------------------------------------------------------
  //! Check's if a queue+path exists already
  //----------------------------------------------------------------------------
  bool ExistsQueue(std::string queue, std::string queuepath);

  //----------------------------------------------------------------------------
  //! Add or modify an fst node
  //----------------------------------------------------------------------------
  bool RegisterNode(const char* nodequeue);

  //----------------------------------------------------------------------------
  //! Remove a node
  //----------------------------------------------------------------------------
  bool UnRegisterNode(const char* nodequeue);

  //----------------------------------------------------------------------------
  //! Remove all nodes
  //----------------------------------------------------------------------------
  void UnRegisterNodes();

  //----------------------------------------------------------------------------
  //! Add or modify a space
  //----------------------------------------------------------------------------
  bool RegisterSpace(const char* spacename);

  //----------------------------------------------------------------------------
  //! Remove a space
  //----------------------------------------------------------------------------
  bool UnRegisterSpace(const char* spacename);

  //----------------------------------------------------------------------------
  //! Add or modify a group
  //----------------------------------------------------------------------------
  bool RegisterGroup(const char* groupname);

  //----------------------------------------------------------------------------
  //! Remove a group
  //----------------------------------------------------------------------------
  bool UnRegisterGroup(const char* groupname);

  //! Mutex protecting all ...View variables
  eos::common::RWMutex ViewMutex;
  //! Mutex protecting all ...Map variables
  eos::common::RWMutex MapMutex;

  //! Map translating a space name to a set of group objects
  std::map<std::string, std::set<FsGroup*> > mSpaceGroupView;

  //! Map translating a space name to a space view object
  std::map<std::string, FsSpace* > mSpaceView;

  //! Map translating a group name to a group view object
  std::map<std::string, FsGroup* > mGroupView;

  //! Map translating a node name to a node view object
  std::map<std::string, FsNode* > mNodeView;

  //! Map translating a filesystem ID to a file system object
  std::map<eos::common::FileSystem::fsid_t, FileSystem*> mIdView;

  //! Map translating a filesystem object pointer to a filesystem ID
  std::map<FileSystem*, eos::common::FileSystem::fsid_t> mFileSystemView;

  //! Mutex protecting the set of gateway nodes mGwNodes
  eos::common::RWMutex GwMutex;

  //! Set containing all nodes which are usable as a gateway machine
  std::set<std::string> mGwNodes;

  //----------------------------------------------------------------------------
  //! Check if quota is enabled for space
  //!
  //! @param space space name
  //!
  //! @return true if quota enabled for space, otherwise false
  //! @warning needs to be called with a read-lock on the MapMutex
  //----------------------------------------------------------------------------
  bool IsQuotaEnabled(const std::string& space);

  //----------------------------------------------------------------------------
  //! Find filesystem by queue path
  //----------------------------------------------------------------------------
  FileSystem* FindByQueuePath(std::string& queuepath);

  //----------------------------------------------------------------------------
  //! Create a filesystem mapping
  //----------------------------------------------------------------------------
  eos::common::FileSystem::fsid_t CreateMapping(std::string fsuuid);

  //----------------------------------------------------------------------------
  //! Provide a filesystem mapping
  //----------------------------------------------------------------------------
  bool ProvideMapping(std::string fsuuid, eos::common::FileSystem::fsid_t fsid);

  //----------------------------------------------------------------------------
  //! Get a filesystem mapping by unique ID
  //----------------------------------------------------------------------------
  eos::common::FileSystem::fsid_t GetMapping(std::string fsuuid);

  //----------------------------------------------------------------------------
  //! Check for an existing mapping by filesystem id
  //----------------------------------------------------------------------------
  bool HasMapping(eos::common::FileSystem::fsid_t fsid)
  {
    return (Fs2UuidMap.count(fsid) > 0) ? true : false;
  }

  //----------------------------------------------------------------------------
  //! Remove a mapping providing filesystem ID and unique ID
  //----------------------------------------------------------------------------
  bool RemoveMapping(eos::common::FileSystem::fsid_t fsid, std::string fsuuid);

  //----------------------------------------------------------------------------
  //! Remove a mapping providing filesystem ID
  //----------------------------------------------------------------------------
  bool RemoveMapping(eos::common::FileSystem::fsid_t fsid);

  //----------------------------------------------------------------------------
  //! Print views (space,group,nodes)
  //----------------------------------------------------------------------------
  void PrintGroups(std::string& out, std::string headerformat,
                   std::string listformat, unsigned int geodepth = 0,
                   const char* selection = 0);

  void PrintNodes(std::string& out, std::string headerformat,
                  std::string listformat, unsigned int geodepth = 0,
                  const char* selection = 0);

  void PrintSpaces(std::string& out, std::string headerformat,
                   std::string listformat, unsigned int geodepth = 0,
                   const char* selection = 0);

  //----------------------------------------------------------------------------
  //! Return printout formats
  //----------------------------------------------------------------------------
  static std::string GetNodeFormat(std::string option);
  static std::string GetGroupFormat(std::string option);
  static std::string GetSpaceFormat(std::string option);
  static std::string GetFileSystemFormat(std::string option);

  //----------------------------------------------------------------------------
  //! Clear all mappings and filesystem objects obtaining locks
  //----------------------------------------------------------------------------
  void Reset();

  pthread_t hbthread; ///< Thread ID of the heartbeat thread

  //----------------------------------------------------------------------------
  //! Static thread startup function
  //----------------------------------------------------------------------------
  static void* StaticHeartBeatCheck(void*);

  //----------------------------------------------------------------------------
  //! Thread loop function checking heartbeats
  //----------------------------------------------------------------------------
  void* HeartBeatCheck();

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FsView()
  {
    MgmConfigQueueName = "";
#ifndef EOSMGMFSVIEWTEST
    ConfEngine = 0;
#endif
    XrdSysThread::Run(&hbthread, FsView::StaticHeartBeatCheck,
                      static_cast<void*>(this), XRDSYSTHREAD_HOLD,
                      "HeartBeat Thread");
  };

  //----------------------------------------------------------------------------
  //! Stop the heartbeat thread
  //----------------------------------------------------------------------------
  void StopHeartBeat()
  {
    if (hbthread)
    {
      XrdSysThread::Cancel(hbthread);
      XrdSysThread::Join(hbthread, 0);
      hbthread = 0;
    }
  };

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FsView()
  {
    StopHeartBeat();
  };

  //----------------------------------------------------------------------------
  //! Set config queues
  //----------------------------------------------------------------------------
  void SetConfigQueues(const char* mgmconfigqueue,
                       const char* nodeconfigqueue,
                       const char* groupconfigqueue,
                       const char* spaceconfigqueue)
  {
    FsSpace::gConfigQueuePrefix = spaceconfigqueue;
    FsGroup::gConfigQueuePrefix = groupconfigqueue;
    FsNode::gConfigQueuePrefix = nodeconfigqueue;
    MgmConfigQueueName = mgmconfigqueue;
  }

#ifndef EOSMGMFSVIEWTEST

  //----------------------------------------------------------------------------
  //! Set the configuration engine object
  //----------------------------------------------------------------------------
  void SetConfigEngine(ConfigEngine* engine)
  {
    ConfEngine = engine;
  }

  //----------------------------------------------------------------------------
  //! Apply all filesystem configuration key-val pair
  //----------------------------------------------------------------------------
  bool ApplyFsConfig(const char* key, std::string& val);

  //----------------------------------------------------------------------------
  //! Apply a global configuration key-val pair
  //----------------------------------------------------------------------------
  bool ApplyGlobalConfig(const char* key, std::string& val);

  //----------------------------------------------------------------------------
  //! Set a global configuration key-val pair
  //----------------------------------------------------------------------------
  bool SetGlobalConfig(std::string key, std::string value);

  //----------------------------------------------------------------------------
  //! Get a global configuration value
  //----------------------------------------------------------------------------
  std::string GetGlobalConfig(std::string key);

#endif

  //----------------------------------------------------------------------------
  //! Set the next available filesystem ID
  //----------------------------------------------------------------------------
  void SetNextFsId(eos::common::FileSystem::fsid_t fsid);

  //! Static singleton object hosting the filesystem view object
  static FsView gFsView;
};

//------------------------------------------------------------------------------
//! Aggregator implementation to compute double precision statistics
//! Statistics are sum, average, std dev, min dev, max dev, max abs dev
//! It calls the underlying unstructured versions of the statistics computation
//! DoubleSum, DoubleAverage, DoubleStdDev, ...
//------------------------------------------------------------------------------
class DoubleAggregator : public GeoTreeAggregator
{
  typedef eos::common::FileSystem::fsid_t fsid_t;

  //! Name of the parameter for which the statistics is to be computed
  std::string pParam;
  std::vector<double> pSums; ///< Sums at the elements of the tree
  std::vector<double> pMeans; ///< Averages at the elements of the tree
  std::vector<double> pMaxDevs; ///< Max deviations at the elements of the tree
  std::vector<double> pMinDevs; ///< Min deviations at the elements of the tree
  std::vector<double>
  pMaxAbsDevs; ///< Min abs. deviations at the elements of the tree
  std::vector<double> pStdDevs; ///< Std. deviations at the elements of the tree
  //! Number of entries considered in the statistics at the elements of the tree
  std::vector<long long> pNb;
  BaseView* pView; ///< The base view ordering the statistics
  //! End index (excluded) of each depth level in the statistics vectors
  std::vector<size_t> pDepthLevelsIndexes;
  std::vector<std::string> pGeoTags; ///< Full geotags at the elements of the tree
 public:

  //----------------------------------------------------------------------------
  //! Get the sums at each tree element
  //----------------------------------------------------------------------------
  const std::vector<double>* getSums() const;

  //----------------------------------------------------------------------------
  //! Get the averages at each tree element
  //----------------------------------------------------------------------------
  const std::vector<double>* getMeans() const;

  //----------------------------------------------------------------------------
  //! Get the max absolute deviations at each tree element
  //----------------------------------------------------------------------------
  const std::vector<double>* getMaxAbsDevs() const;

  //----------------------------------------------------------------------------
  //! Get the standard deviations at each tree element
  //----------------------------------------------------------------------------
  const std::vector<double>* getStdDevs() const;

  //----------------------------------------------------------------------------
  //! Get the full geotags at each tree element
  //----------------------------------------------------------------------------
  const std::vector<std::string>* getGeoTags() const;

  //----------------------------------------------------------------------------
  //! Get the end index (excluded) for a given depth level
  //----------------------------------------------------------------------------
  size_t getEndIndex(int depth = -1) const;

  //----------------------------------------------------------------------------
  //! Constructor given the name of the parameter to compute the statistics for
  //----------------------------------------------------------------------------
  DoubleAggregator(const char* param);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~DoubleAggregator();

  //----------------------------------------------------------------------------
  //! Set the view ordering the statistics. Needs to be set before running
  //! the aggregator.
  //----------------------------------------------------------------------------
  void setView(BaseView* view);

  virtual bool init(const std::vector<std::string>& geotags,
                    const std::vector<size_t>& depthLevelsIndexes);

  virtual bool aggregateLeaves(
      const std::set<eos::common::FileSystem::fsid_t>& leaves, const size_t& idx);

  virtual bool aggregateNodes(
      const std::map<std::string , GeoTreeElement*>& nodes,
      const size_t& idx, bool includeSelf = false);

  virtual bool deepAggregate(
      const std::set<eos::common::FileSystem::fsid_t>& leaves, const size_t& idx);
};

//------------------------------------------------------------------------------
//! Aggregator implementation to compute long long integer statistics
//!
//! Statistics is only sum
//! It calls the underlying unstructured versions of the statistics computation
//! LongLongSum, so it benefits from the same filtering and special cases
//! implemented there.
//------------------------------------------------------------------------------
class LongLongAggregator : public GeoTreeAggregator
{
  typedef eos::common::FileSystem::fsid_t fsid_t;

  //! Name of the parameter for which the statistics are to be computed
  std::string pParam;
  std::vector<long long> pSums; ///< Sums at the elements of the tree
  //! End index (excluded) of each depth level in the statistics vectors
  std::vector<size_t> pDepthLevelsIndexes;
  std::vector<std::string> pGeoTags; ///< Full geotags at the elements of the tree
  BaseView* pView; ///< The base view ordering the statistics

 public:

  //----------------------------------------------------------------------------
  //! Constructor given the name of the parameter to compute the statistics for
  //----------------------------------------------------------------------------
  LongLongAggregator(const char* param);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~LongLongAggregator();

  //----------------------------------------------------------------------------
  //! Set the view ordering the statistics. Needs to be set before running
  //! the aggregator.
  //----------------------------------------------------------------------------
  void setView(BaseView* view);

  //----------------------------------------------------------------------------
  //! Get the sums at each tree element
  //----------------------------------------------------------------------------
  const std::vector<long long>* getSums() const;

  //----------------------------------------------------------------------------
  //! Get the full geotags at each tree element
  //----------------------------------------------------------------------------
  const std::vector<std::string>* getGeoTags() const;

  //----------------------------------------------------------------------------
  //! Get the end index (excluded) for a given depth level
  //----------------------------------------------------------------------------
  size_t getEndIndex(int depth = -1) const;

  virtual bool init(const std::vector<std::string>& geotags,
                    const std::vector<size_t>& depthLevelsIndexes);

  virtual bool aggregateLeaves(
      const std::set<eos::common::FileSystem::fsid_t>& leaves, const size_t& idx);

  virtual bool aggregateNodes(
      const std::map<std::string , GeoTreeElement*>& nodes, const size_t& idx,
      bool includeSelf = false);

  virtual bool deepAggregate(
      const std::set<eos::common::FileSystem::fsid_t>& leaves, const size_t& idx);
};

EOSMGMNAMESPACE_END

#endif
