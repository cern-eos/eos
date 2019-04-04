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
#include <mutex>
#include <memory>

EOSNSNAMESPACE_BEGIN

class QuarkContainerMDSvc;
class QuarkFileMDSvc;
class QuarkHierarchicalView;
class QuarkFileSystemView;
class QuarkContainerAccounting;
class QuarkSyncTimeAccounting;
class QuarkQuotaStats;

//------------------------------------------------------------------------------
//! Class to hold ownership of all QuarkDB-namespace objects.
//------------------------------------------------------------------------------
class QuarkNamespaceGroup : public INamespaceGroup {
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
    std::map<std::string, std::string> &config, std::string &err) override final;

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

private:
  std::recursive_mutex mMutex;

  std::unique_ptr<QuarkContainerMDSvc> mContainerService;
  std::unique_ptr<QuarkFileMDSvc> mFileService;
  std::unique_ptr<QuarkHierarchicalView> mHierarchicalView;
  std::unique_ptr<QuarkFileSystemView> mFilesystemView;
  std::unique_ptr<QuarkContainerAccounting> mContainerAccounting;
  std::unique_ptr<QuarkSyncTimeAccounting> mSyncAccounting;
  std::unique_ptr<QuarkQuotaStats> mQuotaStats;

};


EOSNSNAMESPACE_END
