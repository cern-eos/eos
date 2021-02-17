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
class IFsView;

class Prefetcher
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Prefetcher(IView* view);

  //----------------------------------------------------------------------------
  //! Declare an intent to access FileMD with the given id soon
  //----------------------------------------------------------------------------
  void stageFileMD(IFileMD::id_t id);

  //----------------------------------------------------------------------------
  //! Declare an intent to access FileMD with the given id soon, along with
  //! its parents
  //----------------------------------------------------------------------------
  void stageFileMDWithParents(IFileMD::id_t id);

  //----------------------------------------------------------------------------
  //! Declare an intent to access FileMD with the given id soon, along with
  //! its parents
  //----------------------------------------------------------------------------
  void stageContainerMDWithParents(IContainerMD::id_t id);

  //----------------------------------------------------------------------------
  //! Declare an intent to access FileMD with the given path soon
  //----------------------------------------------------------------------------
  void stageFileMD(const std::string& path, bool follow);

  //----------------------------------------------------------------------------
  //! Declare an intent to access ContainerMD with the given id soon
  //----------------------------------------------------------------------------
  void stageContainerMD(IContainerMD::id_t id);

  //----------------------------------------------------------------------------
  //! Declare an intent to access ContainerMD with the given path soon
  //----------------------------------------------------------------------------
  void stageContainerMD(const std::string& path, bool follow);

  //------------------------------------------------------------------------------
  //! Prefetch item and wait. We don't know if there's a file, or container
  //! under that path.
  //------------------------------------------------------------------------------
  void stageItem(const std::string& path, bool follow);

  //----------------------------------------------------------------------------
  //! Wait until all staged requests have been loaded in cache.
  //----------------------------------------------------------------------------
  void wait();

  //----------------------------------------------------------------------------
  //! Prefetch FileMD and wait
  //----------------------------------------------------------------------------
  static void prefetchFileMDAndWait(IView* view, const std::string& path,
                                    bool follow = true);
  static void prefetchFileMDAndWait(IView* view, IFileMD::id_t id);

  //------------------------------------------------------------------------------
  //! Prefetch ContainerMD and wait
  //------------------------------------------------------------------------------
  static void prefetchContainerMDAndWait(IView* view, const std::string& path,
                                         bool follow = true);
  static void prefetchContainerMDAndWait(IView* view, IContainerMD::id_t id);

  //------------------------------------------------------------------------------
  //! Prefetch item and wait
  //------------------------------------------------------------------------------
  static void prefetchItemAndWait(IView* view, const std::string& path,
                                  bool follow = true);

  //----------------------------------------------------------------------------
  //! Prefetch ContainerMD with children and wait
  //----------------------------------------------------------------------------
  static void
  prefetchContainerMDWithChildrenAndWait(IView* view, const std::string& path,
                                         bool follow = true, bool onlyDirs = false,
                                         bool limitresult = false,
                                         uint64_t dir_limit = -1,
                                         uint64_t file_limit = -1);

  static void
  prefetchContainerMDWithChildrenAndWait(IView* view, IContainerMD::id_t id,
                                         bool onlyDirs = false,
                                         bool limitresult = false,
                                         uint64_t dir_limit = -1,
                                         uint64_t file_limit = -1);

  //----------------------------------------------------------------------------
  //! Prefetch FileMD inode, along with all its parents, and wait
  //----------------------------------------------------------------------------
  static void prefetchFileMDWithParentsAndWait(IView* view, IFileMD::id_t id);

  //----------------------------------------------------------------------------
  //! Prefetch ContainerMD inode, along with all its parents, and wait
  //----------------------------------------------------------------------------
  static void prefetchContainerMDWithParentsAndWait(IView* view,
      IFileMD::id_t id);

  //----------------------------------------------------------------------------
  //! Prefetch inode metadata, automatically detect if it's a file or directory
  //----------------------------------------------------------------------------
  static void prefetchInodeAndWait(IView* view, uint64_t ino);

  //----------------------------------------------------------------------------
  //! Prefetch inode metadata with all children (if any), automatically detect
  //! if it's a file or directory
  //----------------------------------------------------------------------------
  static void prefetchInodeWithChildrenAndWait(IView* view, uint64_t ino);

  //----------------------------------------------------------------------------
  //! Prefetch FileList for the given filesystem ID
  //----------------------------------------------------------------------------
  static void prefetchFilesystemFileListAndWait(IView* view, IFsView* fsview,
      IFileMD::location_t location);

  //----------------------------------------------------------------------------
  //! Prefetch unlinked FileList for the given filesystem ID
  //----------------------------------------------------------------------------
  static void prefetchFilesystemUnlinkedFileListAndWait(IView* view,
      IFsView* fsview, IFileMD::location_t location);

  //----------------------------------------------------------------------------
  //! Prefetch unlinked FileList for the given filesystem ID, along with all
  //! contained FileMDs.
  //----------------------------------------------------------------------------
  static void prefetchFilesystemUnlinkedFileListWithFileMDsAndWait(IView* view,
      IFsView* fsview, IFileMD::location_t location);

  //----------------------------------------------------------------------------
  //! Prefetch FileList for the given filesystem ID, along with all contained
  //! FileMDs.
  //----------------------------------------------------------------------------
  static void prefetchFilesystemFileListWithFileMDsAndWait(IView* view,
      IFsView* fsview, IFileMD::location_t location);

  //----------------------------------------------------------------------------
  //! Prefetch FileList for the given filesystem ID, along with all contained
  //! FileMDs, and all parents of those.
  //----------------------------------------------------------------------------
  static void prefetchFilesystemFileListWithFileMDsAndParentsAndWait(IView* view,
      IFsView* fsview, IFileMD::location_t location);

private:
  //----------------------------------------------------------------------------
  //! Prefetch Uri of IFileMDPtr
  //----------------------------------------------------------------------------
  folly::Future<std::string> prefetchFileUri(IFileMDPtr file);

  //----------------------------------------------------------------------------
  //! Prefetch Uri of IContainerMDPtr
  //----------------------------------------------------------------------------
  folly::Future<std::string> prefetchContUri(IContainerMDPtr cont);

  IView*           pView;
  IFileMDSvc*      pFileMDSvc;
  IContainerMDSvc* pContainerMDSvc;

  std::vector<folly::Future<IFileMDPtr>> mFileMDs;
  std::vector<folly::Future<IContainerMDPtr>> mContainerMDs;
  std::vector<folly::Future<FileOrContainerMD>> mItems;
  std::vector<folly::Future<std::string>> mUris;
};

EOSNSNAMESPACE_END
