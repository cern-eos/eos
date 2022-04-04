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
#include "mgm/utils/FilesystemUuidMapper.hh"
#include "mgm/utils/FileSystemRegistry.hh"
#include "common/RWMutex.hh"
#include "common/SymKeys.hh"
#include "common/Logging.hh"
#include "common/Locators.hh"
#include "common/InstanceName.hh"
#include "common/AssistedThread.hh"
#ifndef __APPLE__
#include <sys/vfs.h>
#else
#include <sys/param.h>
#include <sys/mount.h>
#endif

namespace eos::common
{
class TransferQueue;
}

//------------------------------------------------------------------------------
//! @file FsView.hh
//! @brief Class representing the cluster configuration of EOS
//! There are three views on EOS filesystems by space, group and node.
//------------------------------------------------------------------------------
EOSMGMNAMESPACE_BEGIN

struct GeoTreeElement;
class GeoTree;
class TableFormatterBase;
class Balancer;
class GroupBalancer;
class GroupDrainer;
class GeoBalancer;
class Converter;
class IConfigEngine;

//------------------------------------------------------------------------------
//! Check if given heartbeat timestamp is recent enough
//------------------------------------------------------------------------------
inline bool isHeartbeatRecent(time_t heartbeatTime)
{
  time_t now = time(NULL);

  if ((now - heartbeatTime) < 60) {
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
//! Base class representing any element in a GeoTree
//------------------------------------------------------------------------------
struct GeoTreeElement {

  //------------------------------------------------------------------------------
  //! Destructor
  //------------------------------------------------------------------------------
  ~GeoTreeElement();

  //! Pointer to father node in the tree
  GeoTreeElement* mFather;
  //! Tag token for example BBB in AA::BBB:CC
  std::string mTagToken;
  //! Full geo tag for example AA::BBB:CC in AA::BBB:CC
  std::string mFullTag;
  //! An auxiliary numbering to run the aggregator
  mutable size_t mId;
  //! All the FileSystems attached to this node of the tree
  std::set<eos::common::FileSystem::fsid_t> mFsIds;
  //! Map geoTreeTag -> son branches
  std::map<std::string , GeoTreeElement*> mSons;
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
  // Careful before overwriting the target node.
  //----------------------------------------------------------------------------
  virtual bool aggregateNodes(const std::map<std::string , GeoTreeElement*>&
                              nodes,
                              const size_t& idx, bool includeSelf = false) = 0;

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
  //! STL const_iterator class
  //!
  //! Only the leaves are iterated in alphabetical order of their geotag
  //----------------------------------------------------------------------------
  class const_iterator: public
    std::iterator<std::bidirectional_iterator_tag, fsid_t>
  {
    friend class GeoTree;
    using ContainerT = std::map<fsid_t, GeoTreeElement*>;
    ContainerT::const_iterator mIt; ///< Iterator inside the container
    ContainerT* mCont; ///< Pointer to original container
  public:
    //--------------------------------------------------------------------------
    //! Default constructor
    //--------------------------------------------------------------------------
    const_iterator() = default;

    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param iterator inside map
    //--------------------------------------------------------------------------
    const_iterator(ContainerT::const_iterator it, ContainerT& cont):
      mIt(it), mCont(&cont) {}

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    ~const_iterator() = default;

    //--------------------------------------------------------------------------
    //! Copy assignment operator
    //--------------------------------------------------------------------------
    const_iterator& operator= (const const_iterator& it);

    //--------------------------------------------------------------------------
    //! Copy constructor
    //--------------------------------------------------------------------------
    const_iterator(const const_iterator& it)
    {
      *this = it;
    }

    //--------------------------------------------------------------------------
    //! Pre-increment
    //--------------------------------------------------------------------------
    const_iterator& operator++();

    //--------------------------------------------------------------------------
    //! Post-increment
    //--------------------------------------------------------------------------
    const_iterator operator++(int);

    //--------------------------------------------------------------------------
    //! Pre-decrement
    //--------------------------------------------------------------------------
    const_iterator& operator--();

    //--------------------------------------------------------------------------
    //! Post-decrement
    //--------------------------------------------------------------------------
    const_iterator operator--(int);

    //--------------------------------------------------------------------------
    //! Indirection operator
    //--------------------------------------------------------------------------
    const eos::common::FileSystem::fsid_t& operator*() const;

    //--------------------------------------------------------------------------
    // Inequality operator
    //--------------------------------------------------------------------------
    inline bool operator !=(const const_iterator& rhs) const
    {
      return mIt != rhs.mIt;
    }

    //--------------------------------------------------------------------------
    // Equality operator
    //--------------------------------------------------------------------------
    inline bool operator ==(const const_iterator& rhs) const
    {
      return mIt == rhs.mIt;
    }
  };

  //----------------------------------------------------------------------------
  // begin()
  //----------------------------------------------------------------------------
  inline const_iterator begin() const
  {
    const_iterator it(pLeaves.begin(), pLeaves);
    return it;
  }

  //----------------------------------------------------------------------------
  // cbegin()
  //----------------------------------------------------------------------------
  inline const_iterator cbegin() const
  {
    return begin();
  }

  //----------------------------------------------------------------------------
  // end()
  //----------------------------------------------------------------------------
  inline const_iterator end() const
  {
    const_iterator it(pLeaves.end(), pLeaves);
    return it;
  }

  //----------------------------------------------------------------------------
  // cend()
  //----------------------------------------------------------------------------
  inline const_iterator cend() const
  {
    return end();
  }

private:
  //----------------------------------------------------------------------------
  //! Get file system geotag
  //!
  //! @param fs file system id
  //!
  //! @return geotag
  //----------------------------------------------------------------------------
  std::string getGeoTag(const fsid_t& fs) const;

  GeoTreeElement* pRoot;   ///< The root branch of the tree
  //! All the elements of the tree collected by depth
  std::vector<std::set<GeoTreeElement*, GeoTreeNodeOrderHelper > > pLevels;
  mutable std::map<fsid_t, GeoTreeElement*>
  pLeaves; ///< All the leaves of the tree
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
  BaseView(const common::SharedHashLocator& locator):
    mLocator(locator), mHeartBeat(0), mStatus("unknown"), mInQueue(0)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~BaseView() = default;

  //----------------------------------------------------------------------------
  //! Return a member variable in the view
  //----------------------------------------------------------------------------
  virtual std::string GetMember(const std::string& member) const;

  //----------------------------------------------------------------------------
  //! Return a configuration member
  //----------------------------------------------------------------------------
  virtual std::string GetConfigMember(std::string key) const;

  //----------------------------------------------------------------------------
  //! Delete a configuration member
  //----------------------------------------------------------------------------
  virtual bool DeleteConfigMember(std::string key) const;

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
             const std::string& filter = "", const bool dont_color = false);

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
  void SetStatus(const std::string& status)
  {
    mStatus = status;
  }

  //----------------------------------------------------------------------------
  //! Return the status
  //----------------------------------------------------------------------------
  const std::string GetStatus() const
  {
    return mStatus;
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
  //! Should the provided fsid participate in statistics calculations?
  //! Yes, if:
  //! - The filesystem exists (duh)
  //! - The filesystem is at-least-RO, booted and online
  //!
  //! Call with fsview lock at-least-read locked.
  //----------------------------------------------------------------------------
  bool ShouldConsiderForStatistics(FileSystem* fs);

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
  //! Set a member variable in a view
  //----------------------------------------------------------------------------
  bool SetConfigMember(std::string key, string value,
                       bool isstatus = false);

protected:

  common::SharedHashLocator mLocator; ///< Locator for shared hash
  std::atomic<time_t> mHeartBeat; ///< Last heartbeat time

private:
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
  static std::atomic<bool> gDisableDefaults;
  static std::string gConfigQueuePrefix; ///<  Configuration queue prefix

  Balancer* mBalancer; ///< Threaded object supervising space balancing
  Converter* mConverter; ///< Threaded object running layout conversion jobs
  GroupBalancer* mGroupBalancer; ///< Threaded object running group balancing
  GeoBalancer* mGeoBalancer; ///< Threaded object running geotag balancing
  std::unique_ptr<GroupDrainer> mGroupDrainer; ///< Threaded object running group drainer
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
  //----------------------------------------------------------------------------
  //! Constructor
  //! @param name name of the group e.g. 'default.0'
  //----------------------------------------------------------------------------
  FsGroup(const char* name)
    : BaseView(common::SharedHashLocator::makeForGroup(name)),
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

protected:
  unsigned int mIndex; ///< Group index i.e 0,1,2,3 ...
};

//------------------------------------------------------------------------------
//! Class FsNode describing a node (set of filesystems)
//------------------------------------------------------------------------------
class FsNode : public BaseView
{
public:
  eos::common::TransferQueue* mGwQueue; ///< Gateway transfer queue

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param name nodeview name
  //----------------------------------------------------------------------------
  explicit FsNode(const char* name);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FsNode();

  //----------------------------------------------------------------------------
  //! Return a member variable
  //----------------------------------------------------------------------------
  virtual std::string GetMember(const std::string& name) const override;

  //----------------------------------------------------------------------------
  //! Get active status
  //----------------------------------------------------------------------------
  eos::common::ActiveStatus GetActiveStatus();

  //----------------------------------------------------------------------------
  //! Set active status
  //----------------------------------------------------------------------------
  bool SetActiveStatus(eos::common::ActiveStatus active);

  //----------------------------------------------------------------------------
  //! Set the configuration default values for a node
  //----------------------------------------------------------------------------
  void SetNodeConfigDefault();

  //----------------------------------------------------------------------------
  //! Check if node has a recent enough heartbeat
  //----------------------------------------------------------------------------
  bool HasHeartbeat() const;
};


//------------------------------------------------------------------------------
//! Class ConfigResetMonitor - reset the current configuration engine object
//! used by the FsView to null during construction and put it back to the
//! initial value during destruction.
//------------------------------------------------------------------------------
class ConfigResetMonitor final
{
public:
  //------------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------------
  ConfigResetMonitor();
  //------------------------------------------------------------------------------
  //! Destructor
  //------------------------------------------------------------------------------
  ~ConfigResetMonitor();

private:
  IConfigEngine* mOrigConfEngine; ///< Initial config engine object
};


//------------------------------------------------------------------------------
//! Class describing an EOS pool including views
//------------------------------------------------------------------------------
class FsView : public eos::common::LogId
{
  friend class ConfigResetMonitor;
  // @todo (esindril): this is just for the call in SetConfigMember when
  // accessing mConfigEngine. Should be refactored.
  friend class BaseView;
public:
  //! Static singleton object hosting the filesystem view object
  static FsView gFsView;

  //----------------------------------------------------------------------------
  //! Return printout formats
  //----------------------------------------------------------------------------
  static std::string GetNodeFormat(std::string option);
  static std::string GetGroupFormat(std::string option);
  static std::string GetSpaceFormat(std::string option);
  static std::string GetFileSystemFormat(std::string option);

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param start_heartbeat control whether heartbeat thread is started - for
  //!                        testing purposes
  //----------------------------------------------------------------------------
  FsView() : mConfigEngine(nullptr)
  {
    mHeartBeatThread.reset(&FsView::HeartBeatCheck, this);
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FsView()
  {
    StopHeartBeat();
  }

  //----------------------------------------------------------------------------
  //! Add or modify a filesystem
  //!
  //! @param fs filesystem to register
  //! @parma registerInGeoTreeEngine
  //!
  //! @return true if done, otherwise false
  //----------------------------------------------------------------------------
  bool Register(FileSystem* fs, const common::FileSystemCoreParams& coreParams,
                bool registerInGeoTreeEngine = true);

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
  //! Move a filesystem to another node
  //!
  //! @param fs filesystem object to move
  //! @param target node
  //!
  //! @return true if moved otherwise false
  //----------------------------------------------------------------------------
  bool MoveNode(FileSystem* fs, std::string node);

  //----------------------------------------------------------------------------
  //! Store the filesystem configuration into the config engine. Should be
  //! called whenever a filesystem wide parameters is changed.
  //!
  //! @param fs file system object
  //! @param save_config mark if the config should be saved or not
  //! @note this requires at least the read lock on the gFsView.ViewMutex
  //----------------------------------------------------------------------------
  void StoreFsConfig(FileSystem* fs, bool save_config = true);

  //----------------------------------------------------------------------------
  //! Remove a filesystem
  //!
  //! @param fs file system object
  //! @param unreg_from_geo_tree if true unregister from GeoTree
  //! @param notify_fst if true delete the shared hash object corresponding
  //!        to the current file system
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool UnRegister(FileSystem* fs, bool unreg_from_geo_tree = true,
                  bool notify_fst = false);

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
  mutable eos::common::RWMutexR ViewMutex;

  //! Map translating a space name to a set of group objects
  std::map<std::string, std::set<FsGroup*> > mSpaceGroupView;

  //! Map translating a space name to a space view object
  std::map<std::string, FsSpace* > mSpaceView;

  //! Map translating a group name to a group view object
  std::map<std::string, FsGroup* > mGroupView;

  //! Map translating a node name to a node view object
  std::map<std::string, FsNode* > mNodeView;

  //! Map translating a filesystem ID to a file system object
  FileSystemRegistry mIdView;

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
    return mFilesystemMapper.hasFsid(fsid);
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
                   const char* selection = 0, bool dont_color = false);

  void PrintNodes(std::string& out, const std::string& headerformat,
                  const std::string& listformat, unsigned int geodepth = 0,
                  const char* selection = 0, bool dont_color = false);

  void PrintSpaces(std::string& out, const std::string& headerformat,
                   const std::string& listformat, unsigned int geodepth = 0,
                   const char* selection = 0, const std::string&  filter = "",
                   bool dont_color = false);

  //----------------------------------------------------------------------------
  //! Clear all mappings and filesystem objects obtaining locks
  //----------------------------------------------------------------------------
  void Reset();

  //----------------------------------------------------------------------------
  //! Clear all maps and delete all filesystem/group/space objects
  //----------------------------------------------------------------------------
  void Clear();

  //----------------------------------------------------------------------------
  //! Thread loop function checking heartbeats
  //----------------------------------------------------------------------------
  void HeartBeatCheck(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  //! Stop the heartbeat thread
  //----------------------------------------------------------------------------
  void StopHeartBeat()
  {
    mHeartBeatThread.join();
  }

  //----------------------------------------------------------------------------
  //! Set the configuration engine object
  //----------------------------------------------------------------------------
  void SetConfigEngine(IConfigEngine* engine)
  {
    mConfigEngine = engine;
  }

  //----------------------------------------------------------------------------
  //! Apply all filesystem configuration key-val pair
  //!
  //! @param key fs configuration key
  //! @param val fs configuration to be applied
  //! @param first_unregister if true then unregister the file system before
  //!        applying any of the changes. This is needed for slave MGMs when
  //!        following changes from the master MGM. [default false]
  //----------------------------------------------------------------------------
  bool ApplyFsConfig(const char* key, const std::string& val,
                     bool first_unregister = false);

  //----------------------------------------------------------------------------
  //! Apply a global configuration key-val pair
  //----------------------------------------------------------------------------
  bool ApplyGlobalConfig(const char* key, std::string& val);

  //----------------------------------------------------------------------------
  //! Set a global configuration key-val pair
  //----------------------------------------------------------------------------
  virtual bool SetGlobalConfig(const std::string& key, const std::string& value);

  //----------------------------------------------------------------------------
  //! Set a global configuration key-val pair given as boolean
  //----------------------------------------------------------------------------
  bool SetGlobalConfig(const std::string& key, bool value)
  {
    return SetGlobalConfig(key, (value ? std::string("true") :
                                 std::string("false")));
  }

  //----------------------------------------------------------------------------
  //! Get a global configuration value
  //----------------------------------------------------------------------------
  virtual std::string GetGlobalConfig(const std::string& key);

  //----------------------------------------------------------------------------
  //! Get a global configuration value as boolean
  //----------------------------------------------------------------------------
  bool GetBoolGlobalConfig(const std::string& key)
  {
    return (GetGlobalConfig(key) == "true");
  }

  //----------------------------------------------------------------------------
  //! Broadcast new manager id to all the FST nodes
  //!
  //! @param master_id master identity <hostname>:<port>
  //----------------------------------------------------------------------------
  void BroadcastMasterId(const std::string master_id);

  //----------------------------------------------------------------------------
  //! Get number of filesystems registered
  //----------------------------------------------------------------------------
  inline size_t GetNumFileSystems() const
  {
    eos::common::RWMutexReadLock fs_rd_lock(ViewMutex);
    return mIdView.size();
  }

  //----------------------------------------------------------------------------
  //! Physical bytes available
  //----------------------------------------------------------------------------
  bool UnderNominalQuota(const std::string& space, bool isroot = false);

  //----------------------------------------------------------------------------
  //! Collect all endpoints (<hostname>:<port>) matching the given queue or
  //! pattern
  //!
  //! @return set of matching endpoints
  //----------------------------------------------------------------------------
  std::set<std::string> CollectEndpoints(const std::string& queue) const;

  //----------------------------------------------------------------------------
  //! Re-apply drain status for file systems to re-trigger draining. This is
  //! needed in case the drain engine is stopped and then restatred the fs-es
  //! which where in draning mode are not reactivated so we need to go through
  //! them and reapply the drain status so that draining is activated.
  //----------------------------------------------------------------------------
  void ReapplyDrainStatus();

private:
  IConfigEngine* mConfigEngine;
  AssistedThread mHeartBeatThread; ///< Thread monitoring heart-beats
  //! Object to map between fsid <-> uuid
  FilesystemUuidMapper mFilesystemMapper;

  std::map<std::string, std::pair<bool, time_t>> mUsageOk;
  XrdSysMutex mUsageMutex;
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
};

EOSMGMNAMESPACE_END

#endif
