/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

EOSNSNAMESPACE_BEGIN

//! Forward declaration
class QuotaStats;

//------------------------------------------------------------------------------
//! QuotaNode class which keeps track of user/group volume/inode use
//!
//! The class accounts the volume/inodes used by each user/group in the
//! corresponding container. Each such object saves two HMAPs in the Redis
//! instance using the following convention:
//!
//! 1. id_t:quota_hmap_id - this is the HMAP key, where id_t is the id of the
//!    corresponding container. It contains only information about the uids
//!    of the uses who have written to the container.
//!
//!    { uid1:space          --> val1,
//!      uid1:physical_space --> val2,
//!      uid1:files          --> val3,
//!      ...
//!      uidn:files          --> val3n }
//!
//! 2. id_t:quota_hmap_gid - the same for group ids
//!
//!   { gid1:space          --> val1,
//!     gid1:physical_space --> val2,
//!     gid1:files          --> val3,
//!     ...
//!     gidm:files          --> val3m}
//!
//! Besides these, we also save the ids of all the containers that are also
//! quota nodes in a set structure called "quota_set_ids".
//------------------------------------------------------------------------------
class QuotaNode : public IQuotaNode
{
  friend class ConvertContainerMDSvc;
  friend class ConvertFileMDSvc;

public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  QuotaNode(IQuotaStats* quotaStats, IContainerMD::id_t node_id);

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
  std::string
  getUidKey() const
  {
    return pQuotaUidKey;
  }

  //----------------------------------------------------------------------------
  //! Get current uid qutoa key
  //----------------------------------------------------------------------------
  std::string
  getGidKey() const
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
  std::vector<uint64_t> getUids();

  //----------------------------------------------------------------------------
  //! Get the set of gids for which information is stored in the current quota
  //! node.
  //!
  //! @return vector of gids
  //----------------------------------------------------------------------------
  std::vector<uint64_t> getGids();

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

  redox::Redox* pRedox; ///< Redis client
  //! Quota quota node uid hash key e.g. quota_node:id_t:uid
  std::string pQuotaUidKey;
  //! Quota quota node gid hash key e.g. quota_node:id_t:gid
  std::string pQuotaGidKey;
  redox::RedoxHash pUidMap; ///< Redox hmap for uids
  redox::RedoxHash pGidMap; ///< Redox hmap for gids
  static const std::string sSpaceTag;         ///< Tag for space quota
  static const std::string sPhysicalSpaceTag; ///< Tag for physical space quota
  static const std::string sFilesTag;         ///< Tag for number of files quota
};

//----------------------------------------------------------------------------
//! Manager of the quota nodes
//!
//! The informatio about the exists quota nodes which in this class is stored
//! in the pNodeMap is also saved in redis as a HSET holding the container
//! ids for the corresponding quota nodes. The key name of the set in the
//! Redis instance needs to be unique i.e the sSetQuotaIds static variable.
//----------------------------------------------------------------------------
class QuotaStats : public IQuotaStats
{
  friend class QuotaNode;
  friend class ConvertContainerMDSvc;
  friend class ConvertFileMDSvc;

public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  QuotaStats(const std::map<std::string, std::string>& config);

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

private:
  static const std::string sSetQuotaIds;     ///< Ket of quota node ids set
  static const std::string sQuotaUidsSuffix; ///< Quota hmap of uids suffix
  static const std::string sQuotaGidsSuffix; ///< Quota hmap of gids suffix
  std::map<IContainerMD::id_t, IQuotaNode*> pNodeMap; ///< Map of quota nodes
  redox::Redox* pRedox;                      ///< Redix client
  redox::RedoxSet pIdsSet; ///< Set of quota node ids
};

EOSNSNAMESPACE_END

#endif // EOS_NS_QUOTA_STATS_HH
