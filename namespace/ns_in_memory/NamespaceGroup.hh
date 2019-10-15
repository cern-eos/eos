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
// author: Georgios Bitzes <georgios.bitzes@cern.ch>
// desc:   Class to hold ownership of all in-memory-namespace objects.
//------------------------------------------------------------------------------

#ifndef EOS_NS_IN_MEMORY_NAMESPACE_GROUP_HH
#define EOS_NS_IN_MEMORY_NAMESPACE_GROUP_HH

#include "namespace/Namespace.hh"
#include "namespace/interface/INamespaceGroup.hh"
#include <memory>
#include <mutex>

EOSNSNAMESPACE_BEGIN

class IFileMDChangeListener;
class ChangeLogFileMDSvc;
class ChangeLogContainerMDSvc;
class HierarchicalView;
class FileSystemView;
class SyncTimeAccounting;
class QuotaStats;
class ContainerAccounting;

class InMemNamespaceGroup : public INamespaceGroup {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  InMemNamespaceGroup();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~InMemNamespaceGroup();

  //----------------------------------------------------------------------------
  //! Initialize with the given configuration - must be called before any
  //! other function, and right after construction.
  //!
  //! Initialization may fail - in such case, "false" will be returned, and
  //! "err" will be filled out.
  //----------------------------------------------------------------------------
  virtual bool initialize(eos::common::RWMutex* nsMtx, const
    std::map<std::string, std::string> &config, std::string &err) override final;

  //----------------------------------------------------------------------------
  //! Provide container service
  //----------------------------------------------------------------------------
  virtual IContainerMDSvc* getContainerService() override final;

  //----------------------------------------------------------------------------
  //! Provide file service
  //----------------------------------------------------------------------------
  virtual IFileMDSvc* getFileService() override final;

  //----------------------------------------------------------------------------
  //! Provide hierarchical view
  //----------------------------------------------------------------------------
  virtual IView* getHierarchicalView() override final;

  //----------------------------------------------------------------------------
  //! Provide filesystem view
  //----------------------------------------------------------------------------
  virtual IFsView* getFilesystemView() override final;

  //----------------------------------------------------------------------------
  //! Provide sync time accounting view
  //----------------------------------------------------------------------------
  virtual IContainerMDChangeListener* getSyncTimeAccountingView() override final;

  //----------------------------------------------------------------------------
  //! Provide container accounting view
  //----------------------------------------------------------------------------
  virtual IFileMDChangeListener* getContainerAccountingView() override final;

  //----------------------------------------------------------------------------
  //! Provide quota stats
  //----------------------------------------------------------------------------
  virtual IQuotaStats* getQuotaStats() override final;

  //----------------------------------------------------------------------------
  //! Is this in-memory namespace?
  //----------------------------------------------------------------------------
  virtual bool isInMemory() override final {
    return true;
  }

  //----------------------------------------------------------------------------
  //! Start cache refresh listener - no-op for in-memory namespace
  //----------------------------------------------------------------------------
  virtual void startCacheRefreshListener() override final {}


private:
  //----------------------------------------------------------------------------
  //! Initialize file and container services
  //----------------------------------------------------------------------------
  void initializeFileAndContainerServices();


  std::recursive_mutex mMutex;

  std::unique_ptr<ChangeLogFileMDSvc> mFileService;
  std::unique_ptr<ChangeLogContainerMDSvc> mContainerService;
  std::unique_ptr<HierarchicalView> mHierarchicalView;
  std::unique_ptr<FileSystemView> mFilesystemView;
  std::unique_ptr<SyncTimeAccounting> mSyncAccounting;
  std::unique_ptr<ContainerAccounting> mContainerAccounting;


};

EOSNSNAMESPACE_END

#endif
