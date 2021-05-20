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

namespace qclient
{
class QClient;
}

EOSNSNAMESPACE_BEGIN

//! Forward declaration
class QuarkQuotaStats;
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
class QuarkQuotaNode : public IQuotaNode
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
  QuarkQuotaNode(IQuotaStats* quotaStats, IContainerMD::id_t node_id);

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
  //! Update with information from the backend
  //----------------------------------------------------------------------------
  void updateFromBackend();

  //----------------------------------------------------------------------------
  //! Replace underlying QuotaNodeCore object.
  //----------------------------------------------------------------------------
  void replaceCore(const QuotaNodeCore& updated) override;

  //----------------------------------------------------------------------------
  //! Partial update of underlying QuotaNodeCore object.
  //----------------------------------------------------------------------------
  void updateCore(const QuotaNodeCore &updated) override;

private:
  //! Quota quota node uid hash key e.g. quota_node:id:uid
  std::string pQuotaUidKey;
  //! Quota quota node gid hash key e.g. quota_node:id:gid
  std::string pQuotaGidKey;
  qclient::QClient* pQcl; ///< Backend client from QuotaStats
  MetadataFlusher* pFlusher; ///< Metadata flusher object from QuotaStats
};

//------------------------------------------------------------------------------
//! Manager of the quota nodes
//------------------------------------------------------------------------------
class QuarkQuotaStats : public IQuotaStats
{
  friend class QuarkQuotaNode;
  friend class ConvertContainerMDSvc;
  friend class ConvertFileMDSvc;

public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  QuarkQuotaStats(qclient::QClient *qcl, MetadataFlusher *flusher);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~QuarkQuotaStats();

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

  std::map<IContainerMD::id_t, std::unique_ptr<IQuotaNode>> pNodeMap; ///< Map of quota nodes
  qclient::QClient* pQcl; ///< Backend client
  MetadataFlusher* pFlusher; ///< Metadata flusher object
};

EOSNSNAMESPACE_END
