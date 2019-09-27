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
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "namespace/ns_quarkdb/views/HierarchicalView.hh"
#include "namespace/ns_quarkdb/accounting/FileSystemView.hh"
#include "namespace/ns_quarkdb/accounting/SyncTimeAccounting.hh"
#include "namespace/ns_quarkdb/accounting/QuotaStats.hh"
#include "namespace/ns_quarkdb/accounting/ContainerAccounting.hh"
#include "namespace/ns_quarkdb/CacheRefreshListener.hh"
#include "namespace/ns_quarkdb/VersionEnforcement.hh"
#include <folly/executors/IOThreadPoolExecutor.h>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QuarkNamespaceGroup::QuarkNamespaceGroup()
{
  mExecutor.reset(new folly::IOThreadPoolExecutor(48));
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
QuarkNamespaceGroup::~QuarkNamespaceGroup()
{
  mCacheRefreshListener.reset();
  mSyncAccounting.reset();
  mContainerAccounting.reset();
  mFilesystemView.reset();
  mHierarchicalView.reset();
  mFileService.reset();
  mContainerService.reset();
  mMetadataFlusher.reset();
  mQuotaFlusher.reset();
  mQClient.reset();
  mExecutor.reset();
}

//------------------------------------------------------------------------------
// Initialize with the given configuration - must be called before any
// other function, and right after construction.
//
// Initialization may fail - in such case, "false" will be returned, and
// "err" will be filled out.
//------------------------------------------------------------------------------
bool QuarkNamespaceGroup::initialize(eos::common::RWMutex* nsMtx,
                                     const std::map<std::string, std::string>& config,
                                     std::string& err)
{
  mNsMutex = nsMtx;
  // Mandatory configuration option: queue_path
  auto it = config.find("queue_path");

  if (it == config.end()) {
    err = "configuration key queue_path not found!";
    return false;
  }

  queuePath = it->second;
  // Mandatory configuration option: qdb_cluster
  it = config.find("qdb_cluster");

  if (it == config.end()) {
    err = "configuration key qdb_cluster not found!";
    return false;
  }

  if (!contactDetails.members.parse(it->second)) {
    err = "could not parse qdb_cluster!";
    return false;
  }

  // Optional configuration option: qdb_password
  it = config.find("qdb_password");

  if (it != config.end()) {
    contactDetails.password = it->second;
  }

  // Mandatory configuration: qdb_flusher_md
  it = config.find("qdb_flusher_md");

  if (it == config.end()) {
    err = "configuration key qdb_flusher_md not found!";
    return false;
  }

  flusherMDTag = it->second;
  // Mandatory configuration: qdb_flusher_quota
  it = config.find("qdb_flusher_quota");

  if (it == config.end()) {
    err = "configuration key qdb_flusher_quota not found!";
    return false;
  }

  flusherQuotaTag = it->second;

  if(!enforceQuarkDBVersion(getQClient())) {
    err = "QuarkDB is either down, or running an outdated version.";
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Initialize file and container services
//------------------------------------------------------------------------------
void QuarkNamespaceGroup::initializeFileAndContainerServices()
{
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if (!mFileService) {
    mFileService.reset(new QuarkFileMDSvc(getQClient(), getMetadataFlusher()));
  }

  if (!mContainerService) {
    mContainerService.reset(new QuarkContainerMDSvc(getQClient(),
                            getMetadataFlusher()));
  }

  mContainerService->setFileMDService(mFileService.get());
  mFileService->setContMDService(mContainerService.get());
}

//------------------------------------------------------------------------------
// Provide file service
//------------------------------------------------------------------------------
IFileMDSvc* QuarkNamespaceGroup::getFileService()
{
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if (!mFileService) {
    initializeFileAndContainerServices();
  }

  return mFileService.get();
}

//------------------------------------------------------------------------------
// Provide container service
//------------------------------------------------------------------------------
IContainerMDSvc* QuarkNamespaceGroup::getContainerService()
{
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if (!mContainerService) {
    initializeFileAndContainerServices();
  }

  return mContainerService.get();
}

//------------------------------------------------------------------------------
// Provide hieararchical view
//------------------------------------------------------------------------------
IView* QuarkNamespaceGroup::getHierarchicalView()
{
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if (!mHierarchicalView) {
    mHierarchicalView.reset(new QuarkHierarchicalView(getQClient(),
                            getQuotaFlusher()));
    mHierarchicalView->setFileMDSvc(getFileService());
    mHierarchicalView->setContainerMDSvc(getContainerService());
  }

  return mHierarchicalView.get();
}

//------------------------------------------------------------------------------
// Provide filesystem view
//------------------------------------------------------------------------------
IFsView* QuarkNamespaceGroup::getFilesystemView()
{
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if (!mFilesystemView) {
    mFilesystemView.reset(new QuarkFileSystemView(getQClient(),
                          getMetadataFlusher()));
    getFileService()->addChangeListener(mFilesystemView.get());
  }

  return mFilesystemView.get();
}

//------------------------------------------------------------------------------
// Provide container accounting view
//------------------------------------------------------------------------------
IFileMDChangeListener* QuarkNamespaceGroup::getContainerAccountingView()
{
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if (!mContainerAccounting) {
    mContainerAccounting.reset(new QuarkContainerAccounting(getContainerService(),
                               mNsMutex));
    getFileService()->addChangeListener(mContainerAccounting.get());
    getContainerService()->setContainerAccounting(mContainerAccounting.get());
  }

  return mContainerAccounting.get();
}

//------------------------------------------------------------------------------
// Provide sync time accounting view
//------------------------------------------------------------------------------
IContainerMDChangeListener* QuarkNamespaceGroup::getSyncTimeAccountingView()
{
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if (!mSyncAccounting) {
    mSyncAccounting.reset(new QuarkSyncTimeAccounting(getContainerService(),
                          mNsMutex));
    getContainerService()->addChangeListener(mSyncAccounting.get());
  }

  return mSyncAccounting.get();
}

//------------------------------------------------------------------------------
// Provide quota stats
//------------------------------------------------------------------------------
IQuotaStats* QuarkNamespaceGroup::getQuotaStats()
{
  return getHierarchicalView()->getQuotaStats();
}

//------------------------------------------------------------------------------
// Get metadata flusher
//------------------------------------------------------------------------------
MetadataFlusher* QuarkNamespaceGroup::getMetadataFlusher()
{
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if (!mMetadataFlusher) {
    std::string path = SSTR(queuePath << "/" << flusherMDTag);
    mMetadataFlusher.reset(new MetadataFlusher(path, contactDetails));
  }

  return mMetadataFlusher.get();
}

//------------------------------------------------------------------------------
// Get quota flusher
//------------------------------------------------------------------------------
MetadataFlusher* QuarkNamespaceGroup::getQuotaFlusher()
{
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if (!mQuotaFlusher) {
    std::string path = SSTR(queuePath << "/" << flusherQuotaTag);
    mQuotaFlusher.reset(new MetadataFlusher(path, contactDetails));
  }

  return mQuotaFlusher.get();
}

//------------------------------------------------------------------------------
// Get generic qclient object for light-weight tasks
//------------------------------------------------------------------------------
qclient::QClient* QuarkNamespaceGroup::getQClient()
{
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if (!mQClient) {
    mQClient = std::make_unique<qclient::QClient>(contactDetails.members,
               contactDetails.constructOptions());
  }

  return mQClient.get();
}

//------------------------------------------------------------------------------
// Get folly executor
//------------------------------------------------------------------------------
folly::Executor* QuarkNamespaceGroup::getExecutor()
{
  std::lock_guard<std::recursive_mutex> lock(mMutex);
  return mExecutor.get();
}

//------------------------------------------------------------------------------
// Start cache refresh listener
//------------------------------------------------------------------------------
void QuarkNamespaceGroup::startCacheRefreshListener() {
  std::lock_guard<std::recursive_mutex> lock(mMutex);

  if(!mCacheRefreshListener) {
    mCacheRefreshListener.reset(new CacheRefreshListener(contactDetails, mFileService->getMetadataProvider()));
  }
}

EOSNSNAMESPACE_END

