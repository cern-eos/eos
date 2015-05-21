//------------------------------------------------------------------------------
//! @file IQuota.hh
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2015 CERN/Switzerland                                  *
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

#ifndef __EOS_NS_IQUOTA_HH__
#define __EOS_NS_IQUOTA_HH__

#include "namespace/Namespace.hh"
#include <map>

EOSNSNAMESPACE_BEGIN

//! Forward declrations
class IContainerMD;
class IFileMD;
class IQuotaStats;

//------------------------------------------------------------------------------
//! Placeholder for space occupancy statistics of an accounting node
//------------------------------------------------------------------------------
class IQuotaNode
{
 public:

  struct UsageInfo
  {
    UsageInfo(): space(0), physicalSpace(0), files(0) {}
    UsageInfo& operator += (const UsageInfo& other)
    {
      space         += other.space;
      physicalSpace += other.physicalSpace;
      files         += other.files;
      return *this;
    }
    uint64_t space;
    uint64_t physicalSpace;
    uint64_t files;
  };

  typedef std::map<uid_t, UsageInfo> UserMap;
  typedef std::map<gid_t, UsageInfo> GroupMap;

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  IQuotaNode(IQuotaStats* quotaStats): pQuotaStats(quotaStats) {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~IQuotaNode() {};

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  virtual uint64_t getUsedSpaceByUser(uid_t uid) = 0;

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given group
  //----------------------------------------------------------------------------
  virtual uint64_t getUsedSpaceByGroup(gid_t gid) = 0;

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  virtual uint64_t getPhysicalSpaceByUser(uid_t uid) = 0;

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given group
  //----------------------------------------------------------------------------
  virtual uint64_t getPhysicalSpaceByGroup(gid_t gid) = 0;

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  virtual uint64_t getNumFilesByUser(uid_t uid) = 0;

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given group
  //----------------------------------------------------------------------------
  virtual uint64_t getNumFilesByGroup(gid_t gid) = 0;

  //----------------------------------------------------------------------------
  //! Change the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  virtual void changeSpaceUser(uid_t uid, int64_t delta) = 0;

  //----------------------------------------------------------------------------
  //! Change the amount of space occpied by the given group
  //----------------------------------------------------------------------------
  virtual void changeSpaceGroup(gid_t gid, int64_t delta) = 0;

  //----------------------------------------------------------------------------
  //! Change the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  virtual void changePhysicalSpaceUser(uid_t uid, int64_t delta) = 0;

  //----------------------------------------------------------------------------
  //! Change the amount of space occpied by the given group
  //----------------------------------------------------------------------------
  virtual void changePhysicalSpaceGroup(gid_t gid, int64_t delta) = 0;

  //----------------------------------------------------------------------------
  //! Change the number of files owned by the given user
  //----------------------------------------------------------------------------
  virtual uint64_t changeNumFilesUser(uid_t uid, uint64_t delta) = 0;

  //----------------------------------------------------------------------------
  //! Change the number of files owned by the given group
  //----------------------------------------------------------------------------
  virtual uint64_t changeNumFilesGroup(gid_t gid, uint64_t delta) = 0;

  //----------------------------------------------------------------------------
  //! Iterate over the usage of known users
  //----------------------------------------------------------------------------
  UserMap::iterator userUsageBegin()
  {
    return pUserUsage.begin();
  }

  UserMap::iterator userUsageEnd()
  {
    return pUserUsage.end();
  }

  UserMap::const_iterator userUsageBegin() const
  {
    return pUserUsage.begin();
  }

  UserMap::const_iterator userUsageEnd() const
  {
    return pUserUsage.end();
  }

  //----------------------------------------------------------------------------
  //! Iterate over the usage of known groups
  //----------------------------------------------------------------------------
  GroupMap::iterator groupUsageBegin()
  {
    return pGroupUsage.begin();
  }

  GroupMap::iterator groupUsageEnd()
  {
    return pGroupUsage.end();
  }

  const GroupMap::const_iterator groupUsageBegin() const
  {
    return pGroupUsage.begin();
  }

  const GroupMap::const_iterator groupUsageEnd() const
  {
    return pGroupUsage.end();
  }

  //----------------------------------------------------------------------------
  //! Account a new file, adjust the size using the size mapping function
  //----------------------------------------------------------------------------
  virtual void addFile(const IFileMD* file) = 0;

  //----------------------------------------------------------------------------
  //! Remove a file, adjust the size using the size mapping function
  //----------------------------------------------------------------------------
  virtual void removeFile(const IFileMD* file) = 0;

  //----------------------------------------------------------------------------
  //! Meld in another quota node
  //----------------------------------------------------------------------------
  virtual void meld(const IQuotaNode* node) = 0;

 protected:
  UserMap      pUserUsage;
  GroupMap     pGroupUsage;
  IQuotaStats* pQuotaStats;
};

//----------------------------------------------------------------------------
//! Manager of the quota nodes
//----------------------------------------------------------------------------
class IQuotaStats
{
 public:
  //----------------------------------------------------------------------------
  // Type definitions
  //----------------------------------------------------------------------------
  typedef uint64_t (*SizeMapper)(const IFileMD* file);
  typedef std::map<IContainerMD::id_t, IQuotaNode*> NodeMap;

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  IQuotaStats(): pSizeMapper(0) {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~IQuotaStats() {};

  //----------------------------------------------------------------------------
  //! Get a quota node associated to the container id
  //----------------------------------------------------------------------------
  virtual IQuotaNode* getQuotaNode(IContainerMD::id_t nodeId) = 0;

  //----------------------------------------------------------------------------
  //! Register a new quota node
  //----------------------------------------------------------------------------
  virtual IQuotaNode* registerNewNode(IContainerMD::id_t nodeId) = 0;

  //----------------------------------------------------------------------------
  //! Remove quota node
  //----------------------------------------------------------------------------
  virtual void removeNode(IContainerMD::id_t nodeId) = 0;

  //----------------------------------------------------------------------------
  //! Register a mapping function used to calculate the physical
  //! space that the file occuppies (replicas, striping and so on)
  //----------------------------------------------------------------------------
  void registerSizeMapper(SizeMapper sizeMapper)
  {
    pSizeMapper = sizeMapper;
  }

  //----------------------------------------------------------------------------
  //! Calculate the physical size the file occupies
  //----------------------------------------------------------------------------
  uint64_t getPhysicalSize(const IFileMD* file)
  {
    if (!pSizeMapper)
    {
      MDException e;
      e.getMessage() << "No size mapping function registered" << std::endl;
      throw (e);
    }

    return (*pSizeMapper)(file);
  }

  //----------------------------------------------------------------------------
  //! Iterate over the quota nodes
  //----------------------------------------------------------------------------
  NodeMap::iterator nodesBegin()
  {
    return pNodeMap.begin();
  }

  NodeMap::iterator nodesEnd()
  {
    return pNodeMap.end();
  }

  const NodeMap::const_iterator nodesBegin() const
  {
    return pNodeMap.begin();
  }

  const NodeMap::const_iterator nodesEnd() const
  {
    return pNodeMap.end();
  }

 protected:
  SizeMapper pSizeMapper;
  NodeMap    pNodeMap;
};

EOSNSNAMESPACE_END

#endif // __EOS_NS_IQUOTA_HH__
