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

#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/ContainerIterators.hh"
#include "namespace/Prefetcher.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Prefetcher::Prefetcher(IView *view) {
  pView = view;
  pFileMDSvc = pView->getFileMDSvc();
  pContainerMDSvc = pView->getContainerMDSvc();
}

//------------------------------------------------------------------------------
// Declare an intent to access FileMD with the given id soon
//------------------------------------------------------------------------------
void Prefetcher::stageFileMD(IFileMD::id_t id) {
  if(pView->inMemory()) return;
  mFileMDs.emplace_back(pFileMDSvc->getFileMDFut(id));
}

//----------------------------------------------------------------------------
// Declare an intent to access FileMD with the given path soon
//----------------------------------------------------------------------------
void Prefetcher::stageFileMD(const std::string &path, bool follow) {
  if(pView->inMemory()) return;
  mFileMDs.emplace_back(pView->getFileFut(path, follow));
}

//------------------------------------------------------------------------------
// Declare an intent to access ContainerMD with the given id soon
//------------------------------------------------------------------------------
void Prefetcher::stageContainerMD(IContainerMD::id_t id) {
  if(pView->inMemory()) return;
  mContainerMDs.emplace_back(pContainerMDSvc->getContainerMDFut(id));
}

//----------------------------------------------------------------------------
// Declare an intent to access ContainerMD with the given path soon
//----------------------------------------------------------------------------
void Prefetcher::stageContainerMD(const std::string &path, bool follow) {
  if(pView->inMemory()) return;
  mContainerMDs.emplace_back(pView->getContainerFut(path, follow));
}

//------------------------------------------------------------------------------
// Wait until all staged requests have been loaded in cache.
//------------------------------------------------------------------------------
void Prefetcher::wait() {
  if(pView->inMemory()) return;
  for(size_t i = 0; i < mFileMDs.size(); i++) {
    mFileMDs[i].wait();
  }

  for(size_t i = 0; i < mContainerMDs.size(); i++) {
    mContainerMDs[i].wait();
  }
}

//------------------------------------------------------------------------------
//! Prefetch FileMD by path and wait
//------------------------------------------------------------------------------
void Prefetcher::prefetchFileMDAndWait(IView *view, const std::string &path, bool follow) {
  Prefetcher prefetcher(view);
  prefetcher.stageFileMD(path, follow);
  prefetcher.wait();
}

//------------------------------------------------------------------------------
//! Prefetch FileMD by id and wait
//------------------------------------------------------------------------------
void Prefetcher::prefetchFileMDAndWait(IView *view, IFileMD::id_t id) {
  Prefetcher prefetcher(view);
  prefetcher.stageFileMD(id);
  prefetcher.wait();
}


//------------------------------------------------------------------------------
//! Prefetch ContainerMD and wait
//------------------------------------------------------------------------------
void Prefetcher::prefetchContainerMDAndWait(IView *view, const std::string &path, bool follow) {
  Prefetcher prefetcher(view);
  prefetcher.stageContainerMD(path, follow);
  prefetcher.wait();
}

//------------------------------------------------------------------------------
//! Prefetch ContainerMD, along with all its children, and wait
//------------------------------------------------------------------------------
void Prefetcher::prefetchContainerMDWithChildrenAndWait(IView *view, const std::string &path, bool follow) {
  if(view->inMemory()) return;

  folly::Future<IContainerMDPtr> fut = view->getContainerFut(path, follow);
  fut.wait();

  if(fut.hasException()) return;

  IContainerMDPtr cmd = fut.get();
  Prefetcher prefetcher(view);

  for (auto dit = eos::ContainerMapIterator(cmd); dit.valid(); dit.next()) {
    std::string fpath = SSTR(path << dit.key() << "/");
    prefetcher.stageContainerMD(fpath, true);
  }

  for (auto dit = eos::FileMapIterator(cmd); dit.valid(); dit.next()) {
    std::string fpath = SSTR(path << dit.key() << "/");
    prefetcher.stageFileMD(fpath, true);
  }

  prefetcher.wait();
}

EOSNSNAMESPACE_END
