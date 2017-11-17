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

#pragma once
#include "namespace/Namespace.hh"
#include "namespace/interface/IQuota.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"


EOSNSNAMESPACE_BEGIN

//! Forward declaration
class QuotaStats;
class MetadataFlusher;

//------------------------------------------------------------------------------
//! QuotaNode class which keeps track of user/group volume/inode use
//!
//! The class accounts the volume/inodes used by each user/group in the
//! corresponding container. Each such object saves two HMAPs in the Redis
//! instance using the following convention:
//!
//! 1. quota::id:map_uid - this is the HMAP key, where id is the id of the
//!    corresponding container. It contains only information about the uids
//!    of the users who have written to the container.
//!
//!    { uid1:logical_size   --> val1,
//!      uid1:physical_size  --> val2,
//!      uid1:files          --> val3,
//!      ...
//!      uidn:files          --> val3n }
//!
//! 2. quota:id_t:map_gid - the same for group ids
//!
//!   { gid1:logical_size   --> val1,
//!     gid1:physical_size  --> val2,
//!     gid1:files          --> val3,
//!     ...
//!     gidm:files          --> val3m }
//------------------------------------------------------------------------------
class QuotaNode : public IQuotaNode
{
  friend class ConvertContainerMDSvc;
  friend class ConvertFileMDSvc;

public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param quotaStats quota stats object
  //! @param node_id quota node id
  //----------------------------------------------------------------------------
  QuotaNode(IQuotaStats* quotaStats, IContainerMD::id_t node_id);

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  uint64_t getUsedSpaceByUser(uid_t uid) override;

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given group
  //----------------------------------------------------------------------------
  uint64_t getUsedSpaceByGroup(gid_t gid) override;

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  uint64_t getPhysicalSpaceByUser(uid_t uid) override;

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given group
  //----------------------------------------------------------------------------
  uint64_t getPhysicalSpaceByGroup(gid_t gid) override;

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given user
  //----------------------------------------------------------------------------
  uint64_t getNumFilesByUser(uid_t uid) override;

  //----------------------------------------------------------------------------
  //! Get the amount of space occupied by the given group
  //----------------------------------------------------------------------------
  uint64_t getNumFilesByGroup(gid_t gid) override;

  //----------------------------------------------------------------------------
  //! Account a new file, adjust the size using the size mapping function
  //----------------------------------------------------------------------------
  void addFile(const IFileMD* file) override;

  //----------------------------------------------------------------------------
  //! Remove a file, adjust the size using the size mapping function
  //----------------------------------------------------------------------------
  void removeFile(const IFileMD* file) override;

  //----------------------------------------------------------------------------
  //! Meld in another quota node
  //----------------------------------------------------------------------------
  void meld(const IQuotaNode* node) override;

  //----------------------------------------------------------------------------
  //! Get the set of uids for which information is stored in the current quota
  //! node.
  //!
  //! @return vector of uids
  //----------------------------------------------------------------------------
  std::unordered_set<uint64_t> getUids() override;

  //----------------------------------------------------------------------------
  //! Get the set of gids for which information is stored in the current quota
  //! node.
  //!
  //! @return vector of gids
  //----------------------------------------------------------------------------
  std::unordered_set<uint64_t> getGids() override;

  //----------------------------------------------------------------------------
  //! Update with information from the backend
  //----------------------------------------------------------------------------
  void updateFromBackend();

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
  qclient::QHash pUidMap; ///< Backend map for uids
  qclient::QHash pGidMap; ///< Backend map for gids
  qclient::AsyncHandler pAh; ///< Async handler for qclient requests
  qclient::QClient* pQcl; ///< Backend client from QuotaStats
  MetadataFlusher* pFlusher; ///< Metadata flusher object from QuotaStats
  UserMap pUserUsage;
  GroupMap pGroupUsage;
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
  QuotaStats();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~QuotaStats();

  //----------------------------------------------------------------------------
  //! Configure the quota service
  //----------------------------------------------------------------------------
  virtual void configure(const std::map<std::string, std::string>& config)
  override;

  //----------------------------------------------------------------------------
  //! Get a quota node associated to the container id
  //----------------------------------------------------------------------------
  IQuotaNode* getQuotaNode(IContainerMD::id_t node_id) override;

  //----------------------------------------------------------------------------
  //! Register a new quota node
  //----------------------------------------------------------------------------
  IQuotaNode* registerNewNode(IContainerMD::id_t node_id) override;

  //----------------------------------------------------------------------------
  //! Remove quota node
  //----------------------------------------------------------------------------
  void removeNode(IContainerMD::id_t node_id) override;

  //----------------------------------------------------------------------------
  //! Get the set of all quota node ids. The quota node id corresponds to the
  //! container id.
  //!
  //! @return set of quota node ids
  //----------------------------------------------------------------------------
  std::unordered_set<IContainerMD::id_t> getAllIds() override;

private:
  //----------------------------------------------------------------------------
  //! Get quota node uid map key
  //!
  //! @param sid container id
  //!
  //! @return map key
  //----------------------------------------------------------------------------
  static std::string KeyQuotaUidMap(const std::string& sid);

  //----------------------------------------------------------------------------
  //! Get quota node gid map key
  //!
  //! @param sid container id
  //!
  //! @return map key
  //----------------------------------------------------------------------------
  static std::string KeyQuotaGidMap(const std::string& sid);

  //----------------------------------------------------------------------------
  //! Parse quota id from string
  //!
  //! @param inpurt input string in the form: <prefix>:id:<suffix>
  //! @param id quota node id
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  static bool ParseQuotaId(const std::string& input, IContainerMD::id_t& id);

  std::map<IContainerMD::id_t, IQuotaNode*> pNodeMap; ///< Map of quota nodes
  qclient::QClient* pQcl; ///< Backend client
  MetadataFlusher* pFlusher; ///< Metadata flusher object
};

EOSNSNAMESPACE_END
