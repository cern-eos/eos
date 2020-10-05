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
//! @brief  Class to hold ownership of all QuarkDB-namespace objects.
//------------------------------------------------------------------------------

#pragma once
#include "namespace/Namespace.hh"
#include "namespace/interface/INamespaceGroup.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "namespace/ns_quarkdb/QClPerformance.hh"
#include <mutex>
#include <memory>

namespace folly
{
class Executor;
}

namespace qclient
{
class QClient;
}

EOSNSNAMESPACE_BEGIN

class QuarkContainerMDSvc;
class QuarkFileMDSvc;
class QuarkHierarchicalView;
class QuarkFileSystemView;
class QuarkContainerAccounting;
class QuarkSyncTimeAccounting;
class QuarkQuotaStats;
class MetadataFlusher;
class CacheRefreshListener;

//------------------------------------------------------------------------------
//! Class to hold ownership of all QuarkDB-namespace objects.
//------------------------------------------------------------------------------
class QuarkNamespaceGroup : public INamespaceGroup
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  QuarkNamespaceGroup();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~QuarkNamespaceGroup();

  //----------------------------------------------------------------------------
  //! Initialize with the given configuration - must be called before any
  //! other function, and right after construction.
  //!
  //! Initialization may fail - in such case, "false" will be returned, and
  //! "err" will be filled out.
  //----------------------------------------------------------------------------
  virtual bool initialize(eos::common::RWMutex* nsMtx, const
                          std::map<std::string, std::string>& config, std::string& err) override final;

  //----------------------------------------------------------------------------
  //! Provide file service
  //----------------------------------------------------------------------------
  virtual IFileMDSvc* getFileService() override final;

  //----------------------------------------------------------------------------
  //! Provide container service
  //----------------------------------------------------------------------------
  virtual IContainerMDSvc* getContainerService() override final;

  //----------------------------------------------------------------------------
  //! Provide hieararchical view
  //----------------------------------------------------------------------------
  virtual IView* getHierarchicalView() override final;

  //----------------------------------------------------------------------------
  //! Provide filesystem view
  //----------------------------------------------------------------------------
  virtual IFsView* getFilesystemView() override final;

  //----------------------------------------------------------------------------
  //! Provide container accounting view
  //----------------------------------------------------------------------------
  virtual IFileMDChangeListener* getContainerAccountingView() override final;

  //----------------------------------------------------------------------------
  //! Provide sync time accounting view
  //----------------------------------------------------------------------------
  virtual IContainerMDChangeListener* getSyncTimeAccountingView() override final;

  //----------------------------------------------------------------------------
  //! Provide quota stats
  //----------------------------------------------------------------------------
  virtual IQuotaStats* getQuotaStats() override final;

  //----------------------------------------------------------------------------
  //! Is this in-memory namespace?
  //----------------------------------------------------------------------------
  virtual bool isInMemory() override final
  {
    return false;
  }

  //----------------------------------------------------------------------------
  //! Get metadata flusher
  //----------------------------------------------------------------------------
  MetadataFlusher* getMetadataFlusher();

  //----------------------------------------------------------------------------
  //! Get quota flusher
  //----------------------------------------------------------------------------
  MetadataFlusher* getQuotaFlusher();

  //----------------------------------------------------------------------------
  //! Get quota flusher
  //----------------------------------------------------------------------------
  std::shared_ptr<QClPerfMonitor> getPerformanceMonitor();

  //----------------------------------------------------------------------------
  //! Get generic qclient object for light-weight tasks
  //----------------------------------------------------------------------------
  qclient::QClient* getQClient();

  //----------------------------------------------------------------------------
  //! Get folly executor
  //----------------------------------------------------------------------------
  folly::Executor* getExecutor();

  //----------------------------------------------------------------------------
  //! Start cache refresh listener
  //----------------------------------------------------------------------------
  void startCacheRefreshListener() override final;

private:
  //----------------------------------------------------------------------------
  // Configuration
  //----------------------------------------------------------------------------
  QdbContactDetails contactDetails; //< QDB cluster contact details
  std::string queuePath;            //< Namespace queue path
  std::string flusherMDTag;         //< Tag for MD flusher
  std::string flusherQuotaTag;      //< Tag for quota flusher

  //----------------------------------------------------------------------------
  // Initialize file and container services
  //----------------------------------------------------------------------------
  void initializeFileAndContainerServices();

  std::recursive_mutex mMutex;

  //----------------------------------------------------------------------------
  //! CAUTION: The folly Executor must outlive qclient! If a continuation is
  //! attached to a qclient-provided future, but the executor has been
  //! destroyed, qclient will segfault when fulfilling the corresponding
  //! promise.
  //!
  //! Once qclient is destroyed however, any pending promises will break, and
  //! the executor can then be deleted safely.
  //----------------------------------------------------------------------------
  std::unique_ptr<folly::Executor> mExecutor;

  std::unique_ptr<MetadataFlusher> mMetadataFlusher;  //< Flusher for metadata
  std::unique_ptr<MetadataFlusher> mQuotaFlusher;     //< Flusher for quota

  std::unique_ptr<qclient::QClient> mQClient;         //< Main qclient object
  //< used for generic tasks

  std::unique_ptr<QuarkContainerMDSvc> mContainerService;
  std::unique_ptr<QuarkFileMDSvc> mFileService;
  std::unique_ptr<QuarkHierarchicalView> mHierarchicalView;
  std::unique_ptr<QuarkFileSystemView> mFilesystemView;
  std::unique_ptr<QuarkContainerAccounting> mContainerAccounting;
  std::unique_ptr<QuarkSyncTimeAccounting> mSyncAccounting;
  std::unique_ptr<CacheRefreshListener> mCacheRefreshListener;
  std::shared_ptr<QClPerfMonitor> mPerfMonitor; ///< QCl performance monitor
};


EOSNSNAMESPACE_END
