/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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
//! @brief Metadata prefetching engine
//------------------------------------------------------------------------------

#pragma once
#include "namespace/Namespace.hh"
#include "namespace/interface/IFileMD.hh"
#include <folly/futures/Future.h>

EOSNSNAMESPACE_BEGIN

class IContainerMDSvc;
class IFileMDSvc;

class Prefetcher {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Prefetcher(IFileMDSvc *file_svc, IContainerMDSvc *cont_svc);

  //----------------------------------------------------------------------------
  //! Declare an intent to access FileMD with the given id soon
  //----------------------------------------------------------------------------
  void stageFileMD(id_t id);

  //----------------------------------------------------------------------------
  //! Declare an intent to access ContainerMD with the given id soon
  //----------------------------------------------------------------------------
  void stageContainerMD(id_t id);

  //----------------------------------------------------------------------------
  //! Wait until all staged requests have been loaded in cache.
  //----------------------------------------------------------------------------
  void wait();

private:
  IFileMDSvc      *pFileMDSvc;
  IContainerMDSvc *pContainerMDSvc;

  std::vector<folly::Future<IFileMDPtr>> mFileMDs;
  std::vector<folly::Future<IContainerMDPtr>> mContainerMDs;
};

EOSNSNAMESPACE_END
