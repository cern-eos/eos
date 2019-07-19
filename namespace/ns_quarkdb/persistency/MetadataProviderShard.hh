/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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
#include "namespace/Namespace.hh"
#include "namespace/ns_quarkdb/LRU.hh"
#include "namespace/interface/Misc.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/ns_quarkdb/ContainerMD.hh"
#include <qclient/QClient.hh>
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
class MetadataProviderShard
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  MetadataProviderShard(qclient::QClient *qcl,
    IContainerMDSvc* contsvc, IFileMDSvc* filemvc, folly::Executor *exec);

  //----------------------------------------------------------------------------
  //! Retrieve ContainerMD by ID
  //----------------------------------------------------------------------------
  folly::Future<IContainerMDPtr> retrieveContainerMD(ContainerIdentifier id);

  //----------------------------------------------------------------------------
  //! Retrieve FileMD by ID
  //----------------------------------------------------------------------------
  folly::Future<IFileMDPtr> retrieveFileMD(FileIdentifier id);

  //----------------------------------------------------------------------------
  //! Drop cached FileID - return true if found
  //----------------------------------------------------------------------------
  bool dropCachedFileID(FileIdentifier id);

  //----------------------------------------------------------------------------
  //! Drop cached ContainerID - return true if found
  //----------------------------------------------------------------------------
  bool dropCachedContainerID(ContainerIdentifier id);

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
  //! Turn an incoming FileMDProto into FileMD, removing from the inFlight
  //! staging area, and inserting into the cache
  //----------------------------------------------------------------------------
  IFileMDPtr processIncomingFileMdProto(FileIdentifier id,
                                        eos::ns::FileMdProto proto);

  //----------------------------------------------------------------------------
  //! Turn a (ContainerMDProto, FileMap, ContainerMap) triplet into a
  //! ContainerMDPtr and insert into the cache
  //----------------------------------------------------------------------------
  IContainerMDPtr processIncomingContainerMD(ContainerIdentifier id,
      std::tuple <
      eos::ns::ContainerMdProto,
      IContainerMD::FileMap,
      IContainerMD::ContainerMap
      > tup);

  qclient::QClient *mQcl; // no ownership
  IContainerMDSvc* mContSvc; // no ownership
  IFileMDSvc* mFileSvc; // no ownership
  std::mutex mMutex;
  std::map<ContainerIdentifier,
      folly::FutureSplitter<IContainerMDPtr>> mInFlightContainers;
  std::map<FileIdentifier, folly::FutureSplitter<IFileMDPtr>> mInFlightFiles;
  LRU<ContainerIdentifier, IContainerMD> mContainerCache;
  LRU<FileIdentifier, IFileMD> mFileCache;
  folly::Executor *mExecutor; // no ownership
};

EOSNSNAMESPACE_END
