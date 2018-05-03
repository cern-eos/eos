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
class IView;

class Prefetcher {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Prefetcher(IView *view);

  //----------------------------------------------------------------------------
  //! Declare an intent to access FileMD with the given id soon
  //----------------------------------------------------------------------------
  void stageFileMD(IFileMD::id_t id);

  //----------------------------------------------------------------------------
  //! Declare an intent to access FileMD with the given path soon
  //----------------------------------------------------------------------------
  void stageFileMD(const std::string &path, bool follow);

  //----------------------------------------------------------------------------
  //! Declare an intent to access ContainerMD with the given id soon
  //----------------------------------------------------------------------------
  void stageContainerMD(IContainerMD::id_t id);

  //----------------------------------------------------------------------------
  //! Declare an intent to access ContainerMD with the given path soon
  //----------------------------------------------------------------------------
  void stageContainerMD(const std::string &path, bool follow);

  //----------------------------------------------------------------------------
  //! Wait until all staged requests have been loaded in cache.
  //----------------------------------------------------------------------------
  void wait();

  //----------------------------------------------------------------------------
  //! Prefetch FileMD and wait
  //----------------------------------------------------------------------------
  static void prefetchFileMDAndWait(IView *view, const std::string &path, bool follow = true);
  static void prefetchFileMDAndWait(IView *view, IFileMD::id_t id);

  //------------------------------------------------------------------------------
  //! Prefetch ContainerMD and wait
  //------------------------------------------------------------------------------
  static void prefetchContainerMDAndWait(IView *view, const std::string &path, bool follow = true);

private:
  IView           *pView;
  IFileMDSvc      *pFileMDSvc;
  IContainerMDSvc *pContainerMDSvc;

  std::vector<folly::Future<IFileMDPtr>> mFileMDs;
  std::vector<folly::Future<IContainerMDPtr>> mContainerMDs;
};

EOSNSNAMESPACE_END
