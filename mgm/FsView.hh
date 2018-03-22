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

#include "mgm/Namespace.hh"
#include "mgm/FileSystem.hh"
#include "mgm/IConfigEngine.hh"
#include "common/RWMutex.hh"
#include "common/SymKeys.hh"
#include "common/Logging.hh"
#include "common/GlobalConfig.hh"
#include "common/TransferQueue.hh"
#ifndef __APPLE__
#include <sys/vfs.h>
#else
#include <sys/param.h>
#include <sys/mount.h>
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
class TableFormatterBase;
class Balancer;
class GroupBalancer;
class GeoBalancer;
class Converter;

//------------------------------------------------------------------------------
//! Base class representing any element in a GeoTree
//------------------------------------------------------------------------------
struct GeoTreeElement {
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
  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~GeoTreeAggregator() = default;

  // Initialize the aggregator
  virtual bool init(const std::vector<std::string>& geotags,
                    const std::vector<size_t>& depthLevelsIndexes) = 0;

  // Aggregate the leaves at the last level of the tree
  virtual bool aggregateLeaves(const std::set<eos::common::FileSystem::fsid_t>&
                               leaves, const size_t& idx) = 0;

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
  //! Insert a file system into the tree
  //!
  //! @parma fs file system id
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool insert(const fsid_t& fs);

  //----------------------------------------------------------------------------
  //! Remove a file system from the tree
  //!
  //! @parma fs file system id
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool erase(const fsid_t& fs);

  //----------------------------------------------------------------------------
  //! Get the geotag at which the fs is stored if found
  //!
  //! @param fs file system id
  //! @param geoTag returned geotag if fs found
  //!
  // @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool getGeoTagInTree(const fsid_t& fs , std::string& geoTag);

  //----------------------------------------------------------------------------
  //! Get number of file systems in the tree
  //----------------------------------------------------------------------------
  size_t size() const;

  //----------------------------------------------------------------------------
  //! Run an aggregator through the tree
  //! @note At any depth level, the aggregator is fed ONLY with the data of
  //! the ONE deeper level in the tree
  //!
  //! @param aggregator the aggregator to be run
  //!
  //! @return true if successful, otherwise false
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
  //!
  //! @param buffer output buffer
  //! @param el the tree element to start the display from
  //! @param fullgeotag the full geotag of the element
  //!
  //! @return buffer output
  //----------------------------------------------------------------------------
  char* dumpTree(char* buffer, GeoTreeElement* el,
                 std::string fullgeotag = "") const;

  //----------------------------------------------------------------------------
  //! Debug helper function to display the leaves in the tree
  //!
  //! @param buffer output buffer
  //----------------------------------------------------------------------------
  char* dumpLeaves(char* buffer) const;

  //----------------------------------------------------------------------------
  //! Debug helper function to display the elements of the tree sorted by levels
  //!
  //! @param buffer output buffer
  //----------------------------------------------------------------------------
  char* dumpLevels(char* buffer) const;

  //----------------------------------------------------------------------------
  //! Debug helper function to display all the content of the tree
  //!
  //! @param buffer output buffer
  //----------------------------------------------------------------------------
  char* dump(char* buffer) const;

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
    const_iterator() = default;
    const_iterator(std::map<fsid_t, tElement*>::const_iterator it):
      mIt(it) {}
    ~const_iterator() = default;
    const_iterator operator++(int);
    const_iterator operator--(int);
    const_iterator& operator++();
    const_iterator& operator--();
    const eos::common::FileSystem::fsid_t& operator*() const;
    operator const eos::common::FileSystem::fsid_t* () const;
    const const_iterator& operator= (const const_iterator& it);
  };

  const_iterator begin() const;
  const_iterator cbegin() const;
  const_iterator end() const;
  const_iterator cend() const;
  const_iterator find(const fsid_t& fsid) const;

private:
  //----------------------------------------------------------------------------
  //! Get file system geotag
  //!
  //! @param fs file system id
  //!
  //! @return geotag
  //----------------------------------------------------------------------------
  std::string getGeoTag(const fsid_t& fs) const;

  tElement* pRoot;   ///< The root branch of the tree
  //! All the elements of the tree collected by depth
  std::vector<std::set<tElement*, GeoTreeNodeOrderHelper > > pLevels;
  std::map<fsid_t, tElement*> pLeaves; ///< All the leaves of the tree
};

//------------------------------------------------------------------------------
//! Class representing a grouped set of filesystems
//------------------------------------------------------------------------------
class BaseView : public GeoTree
{
public:
  std::string mName; ///< Name of the base view
  std::string mType; ///< type of the base view

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  BaseView():
    mHeartBeat(0), mStatus("unknown"), mInQueue(0)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~BaseView() = default;

  //----------------------------------------------------------------------------
  //! Return the configuration queue prefix
  //!
  //! @return return the configuration prefix
  //----------------------------------------------------------------------------
  virtual const char* GetConfigQueuePrefix() const
  {
    return "";
  }

  //----------------------------------------------------------------------------
  //! Return a member variable in the view
  //----------------------------------------------------------------------------
  virtual std::string GetMember(const std::string& member) const;

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
  //! Print the view contents
  //!
  //! @param table table info
  //! @param table_format format for table from MGM side
  //! @param table_mq_format format for table from MQ side
  //! @param outdepth ouput depth for geoscheduling
  //! @param filter view filter
  //----------------------------------------------------------------------------
  void Print(TableFormatterBase& table, std::string table_format,
             const std::string& table_mq_format, unsigned outdepth,
             const std::string& filter = "");

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
  //! Get the heartbeat timestamp
  //----------------------------------------------------------------------------
  time_t GetHeartBeat()
  {
    return mHeartBeat;
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
  //! Set the queue size
  //!
  //! @param iq size to set a s queue size variable
  //----------------------------------------------------------------------------
  void SetInQueue(size_t iq)
  {
    mInQueue = iq;
  }

  //----------------------------------------------------------------------------
  //! Calculate the sum of <param> as long long
  //----------------------------------------------------------------------------
  long long
  SumLongLong(const char* param, bool lock = true,
              const std::set<eos::common::FileSystem::fsid_t>* subset = nullptr);

  //----------------------------------------------------------------------------
  //! Calculate the sum of <param> as double
  //----------------------------------------------------------------------------
  double
  SumDouble(const char* param, bool lock = true,
            const std::set<eos::common::FileSystem::fsid_t>* subset = nullptr);

  //----------------------------------------------------------------------------
  //! Calculates the average of <param> as double
  //----------------------------------------------------------------------------
  double
  AverageDouble(const char* param, bool lock = true,
                const std::set<eos::common::FileSystem::fsid_t>* subset = nullptr);

  //----------------------------------------------------------------------------
  //! Calculates the maximum deviation from the average in a group
  //---------------------------------------------------------------------------
  double
  MaxAbsDeviation(const char* param, bool lock = true,
                  const std::set<eos::common::FileSystem::fsid_t>* subset = nullptr);

  double
  MaxDeviation(const char* param, bool lock = true,
               const std::set<eos::common::FileSystem::fsid_t>* subset = nullptr);

  double
  MinDeviation(const char* param, bool lock = true,
               const std::set<eos::common::FileSystem::fsid_t>* subset = nullptr);

  //----------------------------------------------------------------------------
  //! Calculates the standard deviation of <param> as double
  //----------------------------------------------------------------------------
  double
  SigmaDouble(const char* param, bool lock = true,
              const std::set<eos::common::FileSystem::fsid_t>* subset = nullptr);

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

private:
  time_t mHeartBeat; ///< Last heartbeat time
  std::string mStatus; ///< Status (meaning depends on inheritor)
  std::string mSize; ///< Size of base object (meaning depends on inheritor)
  size_t mInQueue; ///< Number of items in queue(meaning depends on inheritor)
};

//------------------------------------------------------------------------------
//! Class FsSpace describing a space (set of filesystems)
//------------------------------------------------------------------------------
class FsSpace: public BaseView
{
public:
  //! Set when a configuration gets loaded to avoid overwriting of the loaded
  //! values by default values
  static bool gDisableDefaults;
  static std::string gConfigQueuePrefix; ///<  Configuration queue prefix

  //----------------------------------------------------------------------------
  //! Get the configuration queeu prefix
  //----------------------------------------------------------------------------
  static const char* sGetConfigQueuePrefix()
  {
    return gConfigQueuePrefix.c_str();
  }

  Balancer* mBalancer; ///< Threaded object supervising space balancing
  Converter* mConverter; ///< Threaded object running layout conversion jobs
  GroupBalancer* mGroupBalancer; ///< Threaded object running group balancing
  GeoBalancer* mGeoBalancer; ///< Threaded object running geotag balancing

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param name name of the space to construct
  //----------------------------------------------------------------------------
  FsSpace(const char* name);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FsSpace();

  //----------------------------------------------------------------------------
  //! Stop function stopping threads before destruction
  //----------------------------------------------------------------------------
  void Stop();

  //----------------------------------------------------------------------------
  //! Synchronously join threads before destruction
  //----------------------------------------------------------------------------
  void Join();

  //----------------------------------------------------------------------------
  //! Get the configuration queue prefix
  //----------------------------------------------------------------------------
  virtual const char* GetConfigQueuePrefix() const
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
//! Class FsGroup describing a group (set of filesystems)
//------------------------------------------------------------------------------
class FsGroup : public BaseView
{
  friend class FsView;
public:
  static std::string gConfigQueuePrefix; ///< Configuration queue prefix

  //----------------------------------------------------------------------------
  //! Return the configuration queue prefix
  //----------------------------------------------------------------------------
  static const char* sGetConfigQueuePrefix()
  {
    return gConfigQueuePrefix.c_str();
  }

  //----------------------------------------------------------------------------
  //! Constructor
  //! @param name name of the group e.g. 'default.0'
  //----------------------------------------------------------------------------
  FsGroup(const char* name):
    mIndex(0)
  {
    mName = name;
    mType = "groupview";
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FsGroup() = default;

  //----------------------------------------------------------------------------
  //! Return index of the group
  //----------------------------------------------------------------------------
  unsigned int GetIndex()
  {
    return mIndex;
  }

  //----------------------------------------------------------------------------
  //! Return the configuration queue prefix (virtual function)
  //----------------------------------------------------------------------------
  virtual const char* GetConfigQueuePrefix() const
  {
    return gConfigQueuePrefix.c_str();
  }

protected:
  unsigned int mIndex; ///< Group index i.e 0,1,2,3 ...
};

//------------------------------------------------------------------------------
//! Class FsNode describing a node (set of filesystems)
//------------------------------------------------------------------------------
class FsNode : public BaseView
{
public:
  static std::string gManagerId; ///< Name of the responsible manager
  eos::common::TransferQueue* mGwQueue; ///< Gateway transfer queue

  //----------------------------------------------------------------------------
  //! Snapshoting
  //----------------------------------------------------------------------------
  bool SnapShotHost(FileSystem::host_snapshot_t& host, bool dolock);

  //----------------------------------------------------------------------------
  //! Check heartbeat
  //!
  //! @param fs file system info
  //!
  //! @return true if successful, othewise false
  //----------------------------------------------------------------------------
  bool HasHeartBeat(eos::common::FileSystem::host_snapshot_t& fs);

  //----------------------------------------------------------------------------
  //! Get active status
  //----------------------------------------------------------------------------
  eos::common::FileSystem::fsactive_t GetActiveStatus();

  //----------------------------------------------------------------------------
  //! Set active status
  //----------------------------------------------------------------------------
  bool SetActiveStatus(eos::common::FileSystem::fsactive_t active);

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param name nodeview name
  //----------------------------------------------------------------------------
  FsNode(const char* name)
  {
    mName = name;
    mType = "nodesview";
    SetConfigMember("stat.hostport", GetMember("hostport"), true, mName.c_str(),
                    false);
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
  virtual std::string GetMember(const std::string& name) const;

  //----------------------------------------------------------------------------
  //! Set the configuration default values for a node
  //----------------------------------------------------------------------------
  void SetNodeConfigDefault()
  {
    // Define the manager ID
    if (!(GetConfigMember("manager").length())) {
      SetConfigMember("manager", gManagerId, true, mName.c_str(), true);
    }

    // By default set 2 balancing streams per node
    if (!(GetConfigMember("stat.balance.ntx").length())) {
      SetConfigMember("stat.balance.ntx", "2", true, mName.c_str(), true);
    }

    // By default set 25 MB/s stream balancing rate
    if (!(GetConfigMember("stat.balance.rate").length())) {
      SetConfigMember("stat.balance.rate", "25", true, mName.c_str(), true);
    }

    // Set the default sym key from the sym key store
    eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

    // Store the sym key as configuration member
    if (!(GetConfigMember("symkey").length())) {
      SetConfigMember("symkey", symkey->GetKey64(), true, mName.c_str(), true);
    }

    // Set the default debug level to notice
    if (!(GetConfigMember("debug.level").length())) {
      SetConfigMember("debug.level", "info", true, mName.c_str(), true);
    }

    // Set by default as no transfer gateway
    if ((GetConfigMember("txgw") != "on") && (GetConfigMember("txgw") != "off")) {
      SetConfigMember("txgw", "off", true, mName.c_str(), true);
    }

    // set by default 10 transfers per gateway node
    if ((strtol(GetConfigMember("gw.ntx").c_str(), 0, 10) == 0) ||
        (strtol(GetConfigMember("gw.ntx").c_str(), 0, 10) == LONG_MAX)) {
      SetConfigMember("gw.ntx", "10", true, mName.c_str(), true);
    }

    // Set by default the gateway stream transfer speed to 120 Mb/s
    if ((strtol(GetConfigMember("gw.rate").c_str(), 0, 10) == 0) ||
        (strtol(GetConfigMember("gw.rate").c_str(), 0, 10) == LONG_MAX)) {
      SetConfigMember("gw.rate", "120", true, mName.c_str(), true);
    }

    // Set by default the MGM domain e.g. same geographical position as the MGM
    if (!(GetConfigMember("domain").length())) {
      SetConfigMember("domain", "MGM", true, mName.c_str(), true);
    }
  }

  static std::string gConfigQueuePrefix; ///< Configuration queue prefix

  //----------------------------------------------------------------------------
  //! Return the configuration queue prefix (virtual function)
  //----------------------------------------------------------------------------
  virtual const char* GetConfigQueuePrefix() const
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
  pthread_t hbthread; ///< Thread ID of the heartbeat thread
  bool mIsHeartbeatOn; ///< True if heartbeat thread is running
  //! Next free filesystem ID if a new one has to be registered
  eos::common::FileSystem::fsid_t NextFsId;
  //! Mutex protecting all ...Map variables
  eos::common::RWMutex MapMutex;
  //! Map translating a file system ID to a unique ID
  std::map<eos::common::FileSystem::fsid_t, std::string> Fs2UuidMap;
  //! Map translating a unique ID to a filesystem ID
  std::map<std::string, eos::common::FileSystem::fsid_t> Uuid2FsMap;
  std::string MgmConfigQueueName; ///< MGM configuration queue name

public:
  static IConfigEngine* sConfEngine;

  //----------------------------------------------------------------------------
  //! Add or modify a filesystem
  //!
  //! @param fs filesystem to register
  //! @parma registerInGeoTreeEngine
  //!
  //! @return true if done, otherwise false
  //----------------------------------------------------------------------------
  bool Register(FileSystem* fs, bool registerInGeoTreeEngine = true);

  //----------------------------------------------------------------------------
  //! Move a filesystem to another group
  //!
  //! @param fs filesystem object to move
  //! @param group target group
  //!
  //! @return true if moved otherwise false
  //----------------------------------------------------------------------------
  bool MoveGroup(FileSystem* fs, std::string group);

  //----------------------------------------------------------------------------
  //! Store the filesystem configuration into the config engine. Should be
  //! called whenever a filesystem wide parameters is changed.
  //!
  //! @param fs file sytem object
  //----------------------------------------------------------------------------
  void StoreFsConfig(FileSystem* fs);

  //----------------------------------------------------------------------------
  //! Remove a filesystem
  //!
  //! @param fs file system object
  //! @param unregisterInGeoTreeEngine'
  //!
  //! @return true if successful, otherwise false
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
  eos::common::RWMutexR ViewMutex;

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
  //! @warning needs to be called with a read-lock on the ViewMutex
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
  void PrintGroups(std::string& out, const std::string& headerformat,
                   const std::string& listformat, unsigned int geodepth = 0,
                   const char* selection = 0);

  void PrintNodes(std::string& out, const std::string& headerformat,
                  const std::string& listformat, unsigned int geodepth = 0,
                  const char* selection = 0);

  void PrintSpaces(std::string& out, const std::string& headerformat,
                   const std::string& listformat, unsigned int geodepth = 0,
                   const char* selection = 0, const std::string&  filter = "");

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
  //!
  //! @param start_heartbeat control wheather heartbeat thread is started - for
  //!                        testing purposes
  //----------------------------------------------------------------------------
  FsView(bool start_heartbeat = true):
    hbthread(), mIsHeartbeatOn(false), NextFsId(0)
  {
    MgmConfigQueueName = "";
    sConfEngine = nullptr;

    if (start_heartbeat) {
      mIsHeartbeatOn = true;
      XrdSysThread::Run(&hbthread, FsView::StaticHeartBeatCheck,
                        static_cast<void*>(this), XRDSYSTHREAD_HOLD,
                        "HeartBeat Thread");
    }
  }

  //----------------------------------------------------------------------------
  //! Stop the heartbeat thread
  //----------------------------------------------------------------------------
  void StopHeartBeat()
  {
    if (mIsHeartbeatOn) {
      XrdSysThread::Cancel(hbthread);
      XrdSysThread::Join(hbthread, 0);
    }
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FsView()
  {
    StopHeartBeat();
  }

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

  //----------------------------------------------------------------------------
  //! Set the configuration engine object
  //----------------------------------------------------------------------------
  void SetConfigEngine(IConfigEngine* engine)
  {
    sConfEngine = engine;
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
