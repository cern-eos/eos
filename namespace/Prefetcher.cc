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
#include "namespace/interface/IFsView.hh"
#include "namespace/interface/ContainerIterators.hh"
#include "namespace/Prefetcher.hh"
#include "common/FileId.hh"
#include "common/Logging.hh"

using std::placeholders::_1;

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Prefetcher::Prefetcher(IView* view)
{
  pView = view;
  pFileMDSvc = pView->getFileMDSvc();
  pContainerMDSvc = pView->getContainerMDSvc();
}

//------------------------------------------------------------------------------
// Declare an intent to access FileMD with the given id soon
//------------------------------------------------------------------------------
void Prefetcher::stageFileMD(IFileMD::id_t id)
{
  if (pView->inMemory()) {
    return;
  }

  mFileMDs.emplace_back(pFileMDSvc->getFileMDFut(id));
}

//------------------------------------------------------------------------------
// Prefetch Uri of IFileMDPtr
//------------------------------------------------------------------------------
folly::Future<std::string> Prefetcher::prefetchFileUri(IFileMDPtr file) {
  if(file) {
    return this->pView->getUriFut(file->getIdentifier());
  }

  return "";
}

//------------------------------------------------------------------------------
// Prefetch Uri of IContainerMDPtr
//------------------------------------------------------------------------------
folly::Future<std::string> Prefetcher::prefetchContUri(IContainerMDPtr cont) {
  if(cont) {
    return this->pView->getUriFut(cont->getIdentifier());
  }

  return "";
}

//------------------------------------------------------------------------------
// Declare an intent to access FileMD with the given id soon, along with
// its parents
//------------------------------------------------------------------------------
void Prefetcher::stageFileMDWithParents(IFileMD::id_t id)
{
  if (pView->inMemory()) {
    return;
  }

  folly::Future<IFileMDPtr> fut = pFileMDSvc->getFileMDFut(id);
  mUris.emplace_back(pFileMDSvc->getFileMDFut(id).thenValue(std::bind(&Prefetcher::prefetchFileUri, this, _1)));
}

//------------------------------------------------------------------------------
// Declare an intent to access ContainerMD with the given id soon, along with
// its parents
//------------------------------------------------------------------------------
void Prefetcher::stageContainerMDWithParents(IContainerMD::id_t id)
{
  if (pView->inMemory()) {
    return;
  }

  folly::Future<IContainerMDPtr> fut = pContainerMDSvc->getContainerMDFut(id);
  mUris.emplace_back(pContainerMDSvc->getContainerMDFut(id).thenValue(std::bind(&Prefetcher::prefetchContUri, this, _1)));
}

//----------------------------------------------------------------------------
// Declare an intent to access FileMD with the given path soon
//----------------------------------------------------------------------------
void Prefetcher::stageFileMD(const std::string& path, bool follow)
{
  if (pView->inMemory()) {
    return;
  }

  try {
    mFileMDs.emplace_back(pView->getFileFut(path, follow));
  }
  catch(MDException &exc) {
    eos_static_warning("Exception in Prefetcher while looking up FileMD path %s: %s, benign race condition?", path.c_str(), exc.getMessage().str().c_str());
  }
}

//------------------------------------------------------------------------------
// Declare an intent to access ContainerMD with the given id soon
//------------------------------------------------------------------------------
void Prefetcher::stageContainerMD(IContainerMD::id_t id)
{
  if (pView->inMemory()) {
    return;
  }

  mContainerMDs.emplace_back(pContainerMDSvc->getContainerMDFut(id));
}

//----------------------------------------------------------------------------
// Declare an intent to access ContainerMD with the given path soon
//----------------------------------------------------------------------------
void Prefetcher::stageContainerMD(const std::string& path, bool follow)
{
  if (pView->inMemory()) {
    return;
  }

  try {
    mContainerMDs.emplace_back(pView->getContainerFut(path, follow));
  }
  catch(MDException &exc) {
    eos_static_warning("Exception in Prefetcher while looking up ContainerMD path %s: %s, benign race condition?", path.c_str(), exc.getMessage().str().c_str());
  }
}

//------------------------------------------------------------------------------
//! Declare an intent to access the given path soon. We don't know if there's
//! a file, or directory there.
//------------------------------------------------------------------------------
void Prefetcher::stageItem(const std::string& path, bool follow)
{
  if (pView->inMemory()) {
    return;
  }

  mItems.emplace_back(pView->getItem(path, follow));
}

//------------------------------------------------------------------------------
// Wait until all staged requests have been loaded in cache.
//------------------------------------------------------------------------------
void Prefetcher::wait()
{
  if (pView->inMemory()) {
    return;
  }

  for (size_t i = 0; i < mFileMDs.size(); i++) {
    mFileMDs[i].wait();
  }

  for (size_t i = 0; i < mContainerMDs.size(); i++) {
    mContainerMDs[i].wait();
  }

  for (size_t i = 0; i < mUris.size(); i++) {
    mUris[i].wait();
  }
}

//------------------------------------------------------------------------------
// Prefetch FileMD by path and wait
//------------------------------------------------------------------------------
void Prefetcher::prefetchFileMDAndWait(IView* view, const std::string& path,
                                       bool follow)
{
  Prefetcher prefetcher(view);
  prefetcher.stageFileMD(path, follow);
  prefetcher.wait();
}

//------------------------------------------------------------------------------
// Prefetch FileMD by id and wait
//------------------------------------------------------------------------------
void Prefetcher::prefetchFileMDAndWait(IView* view, IFileMD::id_t id)
{
  Prefetcher prefetcher(view);
  prefetcher.stageFileMD(id);
  prefetcher.wait();
}


//------------------------------------------------------------------------------
// Prefetch ContainerMD and wait
//------------------------------------------------------------------------------
void Prefetcher::prefetchContainerMDAndWait(IView* view,
    const std::string& path, bool follow)
{
  Prefetcher prefetcher(view);
  prefetcher.stageContainerMD(path, follow);
  prefetcher.wait();
}

//------------------------------------------------------------------------------
// Prefetch ContainerMD and wait
//------------------------------------------------------------------------------
void Prefetcher::prefetchContainerMDAndWait(IView* view, IContainerMD::id_t id)
{
  Prefetcher prefetcher(view);
  prefetcher.stageContainerMD(id);
  prefetcher.wait();
}


//------------------------------------------------------------------------------
// Prefetch item and wait. We don't know if there's a file, or container under
// that path.
//------------------------------------------------------------------------------
void Prefetcher::prefetchItemAndWait(IView* view, const std::string& path,
                                     bool follow)
{
  Prefetcher prefetcher(view);
  prefetcher.stageItem(path, follow);
  prefetcher.wait();
}

//------------------------------------------------------------------------------
// Prefetch ContainerMD, along with all its children, and wait
//------------------------------------------------------------------------------
void Prefetcher::prefetchContainerMDWithChildrenAndWait(IView* view,
    const std::string& path, bool follow, bool onlyDirs, bool limitresult, uint64_t dir_limit, uint64_t file_limit)
{
  if (view->inMemory()) {
    return;
  }

  folly::Future<IContainerMDPtr> fut = view->getContainerFut(path, follow);
  fut.wait();

  if (fut.hasException()) {
    return;
  }

  IContainerMDPtr cmd = std::move(fut).get();

  if(std::chrono::steady_clock::now() - cmd->getLastPrefetch() <= std::chrono::minutes(10)) {
    return;
  }

  Prefetcher prefetcher(view);
  std::vector<std::string> paths;

  if (limitresult) {
    uint64_t dirsfound=0;
    for (auto dit = eos::ContainerMapIterator(cmd); dit.valid() && dirsfound<dir_limit; dit.next(),dirsfound++) {
      paths.emplace_back(SSTR(path << "/" << dit.key()));
    }
  } else {
    for (auto dit = eos::ContainerMapIterator(cmd); dit.valid(); dit.next()) {
      paths.emplace_back(SSTR(path << "/" << dit.key()));
    }
  }

  for (size_t i = 0; i < paths.size(); i++) {
    prefetcher.stageContainerMD(paths[i], true);
  }

  paths.clear();

  if(!onlyDirs) {

    if (limitresult) {
      uint64_t filesfound = 0;
      for (auto dit = eos::FileMapIterator(cmd); dit.valid() && filesfound<file_limit; dit.next(),filesfound++) {
        paths.emplace_back(SSTR(path << "/" << dit.key()));
      }
    } else {
      for (auto dit = eos::FileMapIterator(cmd); dit.valid(); dit.next()) {
        paths.emplace_back(SSTR(path << "/" << dit.key()));
      }
    }

    for (size_t i = 0; i < paths.size(); i++) {
      prefetcher.stageFileMD(paths[i], true);
    }
  }

  prefetcher.wait();
  cmd->setLastPrefetch(std::chrono::steady_clock::now());
}

//------------------------------------------------------------------------------
// Prefetch inode metadata, automatically detect if it's a file or directory
//------------------------------------------------------------------------------
void Prefetcher::prefetchInodeAndWait(IView* view, uint64_t ino)
{
  if(view->inMemory() || ino == 0) {
    return;
  }

  if(eos::common::FileId::IsFileInode(ino)) {
    prefetchFileMDAndWait(view, eos::common::FileId::InodeToFid(ino));
  }
  else {
    prefetchContainerMDAndWait(view, ino);
  }
}

//------------------------------------------------------------------------------
// Prefetch inode metadata with all children (if any), automatically detect if
// it's a file or directory
//------------------------------------------------------------------------------
void Prefetcher::prefetchInodeWithChildrenAndWait(IView* view, uint64_t ino)
{
  if(view->inMemory() || ino == 0) {
    return;
  }

  if(eos::common::FileId::IsFileInode(ino)) {
    prefetchFileMDAndWait(view, eos::common::FileId::InodeToFid(ino));
  }
  else {
    prefetchContainerMDWithChildrenAndWait(view, ino);
  }
}

//------------------------------------------------------------------------------
// Prefetch ContainerMD, along with all its children, and wait
//------------------------------------------------------------------------------
void Prefetcher::prefetchContainerMDWithChildrenAndWait(IView* view,
    IContainerMD::id_t id, bool onlyDirs, bool limitresults, uint64_t dir_limit, uint64_t file_limit)
{
  if (view->inMemory()) {
    return;
  }

  folly::Future<IContainerMDPtr> fut =
    view->getContainerMDSvc()->getContainerMDFut(id);
  fut.wait();

  if (fut.hasException()) {
    return;
  }

  IContainerMDPtr cmd = std::move(fut).get();

  if(std::chrono::steady_clock::now() - cmd->getLastPrefetch() <= std::chrono::minutes(10)) {
    return;
  }

  Prefetcher prefetcher(view);
  std::vector<std::string> paths;

  if (limitresults) {
    uint64_t dirsfound=0;
    for (auto dit = eos::ContainerMapIterator(cmd); dit.valid() && dirsfound<dir_limit; dit.next(),dirsfound++) {
      prefetcher.stageContainerMD(dit.value());
    }
  } else {
    for (auto dit = eos::ContainerMapIterator(cmd); dit.valid(); dit.next()) {
      prefetcher.stageContainerMD(dit.value());
    }
  }

  if(!onlyDirs) {
    if (limitresults) {
      uint64_t filesfound=0;
      for (auto dit = eos::FileMapIterator(cmd); dit.valid() && filesfound<file_limit; dit.next(),filesfound++) {
        prefetcher.stageFileMD(dit.value());
      }
    } else {
      for (auto dit = eos::FileMapIterator(cmd); dit.valid(); dit.next()) {
        prefetcher.stageFileMD(dit.value());
      }
    }
  }

  prefetcher.wait();
  cmd->setLastPrefetch(std::chrono::steady_clock::now());
}

//------------------------------------------------------------------------------
// Prefetch FileMD inode, along with all its parents, and wait
//------------------------------------------------------------------------------
void Prefetcher::prefetchFileMDWithParentsAndWait(IView* view,
    IFileMD::id_t id)
{
  if (view->inMemory()) {
    return;
  }

  Prefetcher prefetcher(view);
  prefetcher.stageFileMDWithParents(id);
  prefetcher.wait();
}

//------------------------------------------------------------------------------
// Prefetch ContainerMD inode, along with all its parents, and wait
//------------------------------------------------------------------------------
void Prefetcher::prefetchContainerMDWithParentsAndWait(IView* view,
    IContainerMD::id_t id)
{
  if (view->inMemory()) {
    return;
  }

  Prefetcher prefetcher(view);
  prefetcher.stageContainerMDWithParents(id);
  prefetcher.wait();
}

//------------------------------------------------------------------------------
// Prefetch FileList for the given filesystem ID
//------------------------------------------------------------------------------
void Prefetcher::prefetchFilesystemFileListAndWait(IView* view, IFsView* fsview,
    IFileMD::location_t location)
{
  if (view->inMemory()) {
    return;
  }

  auto it = fsview->getFileList(location);
}

//----------------------------------------------------------------------------
// Prefetch unlinked FileList for the given filesystem ID
//----------------------------------------------------------------------------
void Prefetcher::prefetchFilesystemUnlinkedFileListAndWait(IView* view,
    IFsView* fsview, IFileMD::location_t location)
{
  if (view->inMemory()) {
    return;
  }

  auto it = fsview->getUnlinkedFileList(location);
}

//----------------------------------------------------------------------------
// Prefetch unlinked FileList for the given filesystem ID, along with all
// contained FileMDs.
//----------------------------------------------------------------------------
void Prefetcher::prefetchFilesystemUnlinkedFileListWithFileMDsAndWait(
  IView* view, IFsView* fsview, IFileMD::location_t location)
{
  if (view->inMemory()) {
    return;
  }

  Prefetcher prefetcher(view);

  for (auto it = fsview->getUnlinkedFileList(location); it &&
       it->valid(); it->next()) {
    prefetcher.stageFileMD(it->getElement());
  }

  prefetcher.wait();
}

//------------------------------------------------------------------------------
// Prefetch FileList for the given filesystem ID, along with all contained
// FileMDs.
//------------------------------------------------------------------------------
void Prefetcher::prefetchFilesystemFileListWithFileMDsAndWait(IView* view,
    IFsView* fsview, IFileMD::location_t location)
{
  if (view->inMemory()) {
    return;
  }

  Prefetcher prefetcher(view);

  for (auto it = fsview->getFileList(location); it && it->valid(); it->next()) {
    prefetcher.stageFileMD(it->getElement());
  }

  prefetcher.wait();
}

//------------------------------------------------------------------------------
// Prefetch FileList for the given filesystem ID, along with all contained
// FileMDs, and all parents of those.
//------------------------------------------------------------------------------
void Prefetcher::prefetchFilesystemFileListWithFileMDsAndParentsAndWait(
  IView* view, IFsView* fsview, IFileMD::location_t location)
{
  if (view->inMemory()) {
    return;
  }

  Prefetcher prefetcher(view);

  for (auto it = fsview->getFileList(location); it && it->valid(); it->next()) {
    prefetcher.stageFileMDWithParents(it->getElement());
  }

  prefetcher.wait();
}


EOSNSNAMESPACE_END
