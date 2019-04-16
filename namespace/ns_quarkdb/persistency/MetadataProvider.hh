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
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Asynchronous metadata retrieval from QDB, with caching support.
//------------------------------------------------------------------------------

#pragma once
#include "namespace/interface/Identifiers.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/ns_quarkdb/persistency/MetadataProviderShard.hh"
#include "namespace/Namespace.hh"
#include <folly/futures/Future.h>
#include <folly/futures/FutureSplitter.h>

namespace folly
{
class Executor;
}

EOSNSNAMESPACE_BEGIN

class IContainerMDSvc;
class IFileMDSvc;
class QdbContactDetails;

//------------------------------------------------------------------------------
//! Class MetadataProvider
//------------------------------------------------------------------------------
class MetadataProvider
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  MetadataProvider(const QdbContactDetails& contactDetails, IContainerMDSvc* contsvc,
                   IFileMDSvc* filemvc);

  //----------------------------------------------------------------------------
  //! Retrieve ContainerMD by ID
  //----------------------------------------------------------------------------
  folly::Future<IContainerMDPtr> retrieveContainerMD(ContainerIdentifier id);

  //----------------------------------------------------------------------------
  //! Retrieve FileMD by ID
  //----------------------------------------------------------------------------
  folly::Future<IFileMDPtr> retrieveFileMD(FileIdentifier id);

  //----------------------------------------------------------------------------
  //! Check if a FileMD exists with the given id
  //----------------------------------------------------------------------------
  folly::Future<bool> hasFileMD(FileIdentifier id);

  //----------------------------------------------------------------------------
  //! Insert newly created item into the cache
  //----------------------------------------------------------------------------
  void insertFileMD(FileIdentifier id, IFileMDPtr item);

  //----------------------------------------------------------------------------
  //! Insert newly created item into the cache
  //----------------------------------------------------------------------------
  void insertContainerMD(ContainerIdentifier id, IContainerMDPtr item);

  //----------------------------------------------------------------------------
  //! Change file cache size
  //----------------------------------------------------------------------------
  void setFileMDCacheNum(uint64_t max_num);

  //----------------------------------------------------------------------------
  //! Change container cache size
  //----------------------------------------------------------------------------
  void setContainerMDCacheNum(uint64_t max_num);

  //----------------------------------------------------------------------------
  //! Get file cache statistics
  //----------------------------------------------------------------------------
  CacheStatistics getFileMDCacheStats();

  //----------------------------------------------------------------------------
  //! Get container cache statistics
  //----------------------------------------------------------------------------
  CacheStatistics getContainerMDCacheStats();

private:
  //----------------------------------------------------------------------------
  //! Pick shard based on FileIdentifier
  //----------------------------------------------------------------------------
  MetadataProviderShard* pickShard(FileIdentifier id);

  //----------------------------------------------------------------------------
  //! Pick shard based on ContainerIdentifier
  //----------------------------------------------------------------------------
  MetadataProviderShard* pickShard(ContainerIdentifier id);


  static constexpr size_t kShards = 16;

  //----------------------------------------------------------------------------
  //! CAUTION: The folly Executor must outlive qclient! If a continuation is
  //! attached to a qclient-provided future, but the executor has been
  //! destroyed, qclient will segfault when fulfilling the
  //! corresponding promise.
  //!
  //! The order of these two members is very important - the executor must be
  //! first.
  //----------------------------------------------------------------------------
  std::unique_ptr<folly::Executor> mExecutor;
  std::vector<std::unique_ptr<qclient::QClient>> mQcl;

  std::vector<std::unique_ptr<MetadataProviderShard>> mShards;
};

EOSNSNAMESPACE_END
