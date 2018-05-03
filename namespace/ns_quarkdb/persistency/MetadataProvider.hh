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
#include "namespace/Namespace.hh"
#include "namespace/ns_quarkdb/LRU.hh"
#include "namespace/interface/Misc.hh"
#include <qclient/QClient.hh>

#include <folly/futures/Future.h>
#include <folly/futures/FutureSplitter.h>

namespace folly {
  class Executor;
}

EOSNSNAMESPACE_BEGIN

class IContainerMDSvc;
class IFileMDSvc;

//------------------------------------------------------------------------------
//! Class MetadataProvider
//------------------------------------------------------------------------------
class MetadataProvider
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  MetadataProvider(qclient::QClient &qcl, IContainerMDSvc *contsvc, IFileMDSvc *filemvc);

  //----------------------------------------------------------------------------
  //! Retrieve ContainerMD by ID.
  //----------------------------------------------------------------------------
  folly::Future<IContainerMDPtr> retrieveContainerMD(ContainerIdentifier id);

  //----------------------------------------------------------------------------
  //! Retrieve FileMD by ID.
  //----------------------------------------------------------------------------
  folly::Future<IFileMDPtr> retrieveFileMD(FileIdentifier id);

  //----------------------------------------------------------------------------
  //! Insert newly created item into the cache.
  //----------------------------------------------------------------------------
  void insertFileMD(FileIdentifier id, IFileMDPtr item);

  //----------------------------------------------------------------------------
  //! Insert newly created item into the cache.
  //----------------------------------------------------------------------------
  void insertContainerMD(ContainerIdentifier id, IContainerMDPtr item);

  //----------------------------------------------------------------------------
  //! Change file cache size.
  //----------------------------------------------------------------------------
  void setFileMDCacheSize(uint64_t size);

  //----------------------------------------------------------------------------
  //! Change container cache size.
  //----------------------------------------------------------------------------
  void setContainerMDCacheSize(uint64_t size);

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
  //! staging area, and inserting into the cache.
  //----------------------------------------------------------------------------
  IFileMDPtr processIncomingFileMdProto(FileIdentifier id, eos::ns::FileMdProto proto);

  //----------------------------------------------------------------------------
  //! Turn a (ContainerMDProto, FileMap, ContainerMap) triplet into a
  //! ContainerMDPtr, and insert into the cache.
  //----------------------------------------------------------------------------
  IContainerMDPtr processIncomingContainerMD(ContainerIdentifier id,
    std::tuple<
      eos::ns::ContainerMdProto,
      IContainerMD::FileMap,
      IContainerMD::ContainerMap
    >
  );

  qclient::QClient &mQcl;
  IContainerMDSvc *mContSvc;
  IFileMDSvc *mFileSvc;

  std::mutex mMutex;
  std::map<ContainerIdentifier, folly::FutureSplitter<IContainerMDPtr>> mInFlightContainers;
  std::map<FileIdentifier, folly::FutureSplitter<IFileMDPtr>> mInFlightFiles;

  LRU<ContainerIdentifier, IContainerMD> mContainerCache;
  LRU<FileIdentifier, IFileMD> mFileCache;

  std::unique_ptr<folly::Executor> mExecutor;

};

EOSNSNAMESPACE_END
