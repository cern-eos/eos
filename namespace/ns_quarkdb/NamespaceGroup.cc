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
#include "namespace/ns_quarkdb/accounting/FileSystemView.hh"
#include "namespace/ns_quarkdb/accounting/SyncTimeAccounting.hh"
#include "namespace/ns_quarkdb/accounting/QuotaStats.hh"
#include "namespace/ns_quarkdb/accounting/ContainerAccounting.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QuarkNamespaceGroup::QuarkNamespaceGroup() {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
QuarkNamespaceGroup::~QuarkNamespaceGroup() {
  mSyncAccounting.reset();
  mContainerAccounting.reset();
  mQuotaStats.reset();
  mFilesystemView.reset();
  mHierarchicalView.reset();
  mFileService.reset();
  mContainerService.reset();
}

//----------------------------------------------------------------------------
// Initialize with the given configuration - must be called before any
// other function, and right after construction.
//
// Initialization may fail - in such case, "false" will be returned, and
// "err" will be filled out.
//----------------------------------------------------------------------------
bool QuarkNamespaceGroup::initialize(eos::common::RWMutex* nsMtx, const std::map<std::string, std::string> &config, std::string &err) {
  mNsMutex = nsMtx;
  return true;
}

//------------------------------------------------------------------------------
// Initialize file and container services
//------------------------------------------------------------------------------
void QuarkNamespaceGroup::initializeFileAndContainerServices() {
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if(!mFileService) {
    mFileService.reset(new QuarkFileMDSvc());
  }

  if(!mContainerService) {
    mContainerService.reset(new QuarkContainerMDSvc());
  }

  mContainerService->setFileMDService(mFileService.get());
  mFileService->setContMDService(mContainerService.get());
}

//----------------------------------------------------------------------------
// Provide file service
//----------------------------------------------------------------------------
IFileMDSvc* QuarkNamespaceGroup::getFileService() {
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if(!mFileService) {
    initializeFileAndContainerServices();
  }

  return mFileService.get();
}

//------------------------------------------------------------------------------
// Provide container service
//------------------------------------------------------------------------------
IContainerMDSvc* QuarkNamespaceGroup::getContainerService() {
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if(!mContainerService) {
    initializeFileAndContainerServices();
  }

  return mContainerService.get();
}

//------------------------------------------------------------------------------
// Provide hieararchical view
//------------------------------------------------------------------------------
IView* QuarkNamespaceGroup::getHierarchicalView() {
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if(!mHierarchicalView) {
    mHierarchicalView.reset(new QuarkHierarchicalView());
    mHierarchicalView->setFileMDSvc(getFileService());
    mHierarchicalView->setContainerMDSvc(getContainerService());
  }

  return mHierarchicalView.get();
}

//----------------------------------------------------------------------------
// Provide filesystem view
//----------------------------------------------------------------------------
IFsView* QuarkNamespaceGroup::getFilesystemView() {
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if(!mFilesystemView) {
    mFilesystemView.reset(new QuarkFileSystemView());
  }

  return mFilesystemView.get();
}

//------------------------------------------------------------------------------
// Provide container accounting view
//------------------------------------------------------------------------------
IFileMDChangeListener* QuarkNamespaceGroup::getContainerAccountingView() {
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if(!mContainerAccounting) {
    mContainerAccounting.reset(new QuarkContainerAccounting(getContainerService(), mNsMutex));
  }

  return mContainerAccounting.get();
}

//------------------------------------------------------------------------------
// Provide sync time accounting view
//------------------------------------------------------------------------------
IContainerMDChangeListener* QuarkNamespaceGroup::getSyncTimeAccountingView() {
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if(!mSyncAccounting) {
    mSyncAccounting.reset(new QuarkSyncTimeAccounting(getContainerService(), mNsMutex));
  }

  return mSyncAccounting.get();
}

//------------------------------------------------------------------------------
// Provide quota stats
//------------------------------------------------------------------------------
IQuotaStats* QuarkNamespaceGroup::getQuotaStats() {
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if(!mQuotaStats) {
    mQuotaStats.reset(new QuarkQuotaStats());
  }

  return mQuotaStats.get();
}


EOSNSNAMESPACE_END

