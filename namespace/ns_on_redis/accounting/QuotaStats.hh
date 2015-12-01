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

//------------------------------------------------------------------------------
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch.
//! @brief Quota accounting on top of Redis
//------------------------------------------------------------------------------

#ifndef EOS_NS_QUOTA_STATS_HH
#define EOS_NS_QUOTA_STATS_HH

#include "namespace/Namespace.hh"
#include "namespace/interface/IQuota.hh"
#include "namespace/ns_on_redis/RedisClient.hh"
#include <map>

EOSNSNAMESPACE_BEGIN

//! Forward declaration
class QuotaStats;

//------------------------------------------------------------------------------
//! Placeholder for space occupancy statistics of an accounting node
//!
//! TODO: add description about the format of the data saved in Redis
//------------------------------------------------------------------------------
class QuotaNode: public IQuotaNode
{
 public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  QuotaNode(IQuotaStats* quotaStats, IContainerMD::id_t nodeId);

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  uint64_t getUsedSpaceByUser(uid_t uid);

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given group
  //----------------------------------------------------------------------------
  uint64_t getUsedSpaceByGroup(gid_t gid);

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  uint64_t getPhysicalSpaceByUser(uid_t uid);

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given group
  //----------------------------------------------------------------------------
  uint64_t getPhysicalSpaceByGroup(gid_t gid);

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  uint64_t getNumFilesByUser(uid_t uid);

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given group
  //----------------------------------------------------------------------------
  uint64_t getNumFilesByGroup(gid_t gid);

  //----------------------------------------------------------------------------
  //! Account a new file, adjust the size using the size mapping function
  //----------------------------------------------------------------------------
  void addFile(const IFileMD* file);

  //----------------------------------------------------------------------------
  //! Remove a file, adjust the size using the size mapping function
  //----------------------------------------------------------------------------
  void removeFile(const IFileMD* file);

  //----------------------------------------------------------------------------
  //! Meld in another quota node
  //----------------------------------------------------------------------------
  void meld(const IQuotaNode* node);

  //----------------------------------------------------------------------------
  //! Get current uid qutoa key
  //----------------------------------------------------------------------------
  std::string getUidKey() const
  {
    return pQuotaUidKey;
  }

  //----------------------------------------------------------------------------
  //! Get current uid qutoa key
  //----------------------------------------------------------------------------
  std::string getGidKey() const
  {
    return pQuotaGidKey;
  }

  //----------------------------------------------------------------------------
  //! Get the set of all quota node ids. The quota node id corresponds to the
  //! container id.
  //!
  //! @return set of quota node ids
  //----------------------------------------------------------------------------
  std::set<std::string> getAllIds();

  //----------------------------------------------------------------------------
  //! Get the set of uids for which information is stored in the current quota
  //! node.
  //!
  //! @return vector of uids
  //----------------------------------------------------------------------------
  std::vector<unsigned long> getUids();

  //----------------------------------------------------------------------------
  //! Get the set of gids for which information is stored in the current quota
  //! node.
  //!
  //! @return vector of gids
  //----------------------------------------------------------------------------
  std::vector<unsigned long> getGids();

private:

  //----------------------------------------------------------------------------
  //! Get all uid fields for current quota node
  //!
  //! @return vector of all uid fileds for current quota node
  //----------------------------------------------------------------------------
  std::vector<std::string> getAllUidFields();

  //----------------------------------------------------------------------------
  //! Get all gid fields for current quota node
  //!
  //! @return vector of all gid fileds for current quota node
  //----------------------------------------------------------------------------
  std::vector<std::string> getAllGidFields();

  //! Quota quota node uid hash key e.g. quota_node:id_t:uid
  std::string pQuotaUidKey;
  //! Quota quota node gid hash key e.g. quota_node:id_t:gid
  std::string pQuotaGidKey;

  static const std::string sSpaceTag; ///< Tag for space quota
  static const std::string sPhysicalSpaceTag; ///< Tag for physical space quota
  static const std::string sFilesTag; ///< Tag for number of files quota
};

//----------------------------------------------------------------------------
//! Manager of the quota nodes
//!
//! TODO: add description about the format of the data saved in Redis
//----------------------------------------------------------------------------
class QuotaStats: public IQuotaStats
{
 public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  QuotaStats();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~QuotaStats();

  //----------------------------------------------------------------------------
  //! Get a quota node associated to the container id
  //----------------------------------------------------------------------------
  IQuotaNode* getQuotaNode(IContainerMD::id_t node_id);

  //----------------------------------------------------------------------------
  //! Register a new quota node
  //----------------------------------------------------------------------------
  IQuotaNode* registerNewNode(IContainerMD::id_t node_id);

  //----------------------------------------------------------------------------
  //! Remove quota node
  //----------------------------------------------------------------------------
  void removeNode(IContainerMD::id_t node_id);

  //----------------------------------------------------------------------------
  //! Get the set of all quota node ids. The quota node id corresponds to the
  //! container id.
  //!
  //! @return set of quota node ids
  //----------------------------------------------------------------------------
  std::set<std::string> getAllIds();

  static const std::string sQuotaPrefix; ///< Quota node prefix
  static const std::string sSetQuotaIds; ///< Set of quota node ids

private:

  redox::Redox* pRedox; ///< Redix client
  std::map<IContainerMD::id_t, IQuotaNode*> pNodeMap; ///< Map of quota nodes
};

EOSNSNAMESPACE_END

#endif // EOS_NS_QUOTA_STATS_HH
