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

#include "namespace/ns_quarkdb/NamespaceGroup.hh"
#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "namespace/ns_quarkdb/views/HierarchicalView.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QuarkNamespaceGroup::QuarkNamespaceGroup() {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
QuarkNamespaceGroup::~QuarkNamespaceGroup() {}

//----------------------------------------------------------------------------
// Initialize with the given configuration - must be called before any
// other function, and right after construction.
//
// Initialization may fail - in such case, "false" will be returned, and
// "err" will be filled out.
//----------------------------------------------------------------------------
bool QuarkNamespaceGroup::initialize(const std::map<std::string, std::string> &config, std::string &err) {
  return true;
}

//----------------------------------------------------------------------------
// Provide file service
//----------------------------------------------------------------------------
IFileMDSvc* QuarkNamespaceGroup::getFileService() {
  std::lock_guard<std::mutex> lock(mMutex);

  if(!mFileService) {
    mFileService.reset(new QuarkFileMDSvc());
  }

  return mFileService.get();
}

//------------------------------------------------------------------------------
// Provide container service
//------------------------------------------------------------------------------
IContainerMDSvc* QuarkNamespaceGroup::getContainerService() {
  std::lock_guard<std::mutex> lock(mMutex);

  if(!mContainerService) {
    mContainerService.reset(new QuarkContainerMDSvc());
  }

  return mContainerService.get();
}

//------------------------------------------------------------------------------
// Provide hieararchical view
//------------------------------------------------------------------------------
IView* QuarkNamespaceGroup::getHierarchicalView() {
  std::lock_guard<std::mutex> lock(mMutex);

  if(!mHierarchicalView) {
    mHierarchicalView.reset(new QuarkHierarchicalView());
  }

  return mHierarchicalView.get();
}


EOSNSNAMESPACE_END

