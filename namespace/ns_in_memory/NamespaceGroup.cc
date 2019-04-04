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
//! @brief Class representing the file metadata
//------------------------------------------------------------------------------

#include "namespace/ns_in_memory/NamespaceGroup.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/ns_in_memory/views/HierarchicalView.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Constructor
//------------------------------------------------------------------------------
InMemNamespaceGroup::InMemNamespaceGroup() {}

//------------------------------------------------------------------------------
//! Destructor
//------------------------------------------------------------------------------
InMemNamespaceGroup::~InMemNamespaceGroup() {}

//----------------------------------------------------------------------------
// Initialize with the given configuration - must be called before any
// other function, and right after construction.
//
// Initialization may fail - in such case, "false" will be returned, and
// "err" will be filled out.
//----------------------------------------------------------------------------
bool InMemNamespaceGroup::initialize(const std::map<std::string, std::string> &config, std::string &err) {
  return true;
}

//------------------------------------------------------------------------------
// Provide file service
//------------------------------------------------------------------------------
IFileMDSvc* InMemNamespaceGroup::getFileService() {
  std::lock_guard<std::mutex> lock(mMutex);

  if(!mFileService) {
    mFileService.reset(new ChangeLogFileMDSvc());
  }

  return mFileService.get();
}

//------------------------------------------------------------------------------
// Provide container service
//------------------------------------------------------------------------------
IContainerMDSvc* InMemNamespaceGroup::getContainerService() {
  std::lock_guard<std::mutex> lock(mMutex);

  if(!mContainerService) {
    mContainerService.reset(new ChangeLogContainerMDSvc());
  }

  return mContainerService.get();
}

//------------------------------------------------------------------------------
// Provide hierarchical view
//------------------------------------------------------------------------------
IView* InMemNamespaceGroup::getHierarchicalView() {
  std::lock_guard<std::mutex> lock(mMutex);

  if(!mHierarchicalView) {
    mHierarchicalView.reset(new HierarchicalView());
  }

  return mHierarchicalView.get();
}

//------------------------------------------------------------------------------
//! Provide filesystem view
//------------------------------------------------------------------------------
IFsView* InMemNamespaceGroup::getFilesystemView() {
  std::lock_guard<std::mutex> lock(mMutex);

  if(!mFilesystemView) {
    mFilesystemView.reset(new FileSystemView());
  }

  return mFilesystemView.get();
}



EOSNSNAMESPACE_END
