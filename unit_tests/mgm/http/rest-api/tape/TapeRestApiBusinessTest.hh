//------------------------------------------------------------------------------
//! @file TapeRestApiBusinessTest.hh
//! @author Cedric Caffy - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#ifndef EOS_TAPERESTAPIBUSINESSTEST_HH
#define EOS_TAPERESTAPIBUSINESSTEST_HH

#include "InMemoryBulkRequestDAO.hh"
#include "mgm/bulk-request/MockPrepareMgmFSInterface.hh"
#include "mgm/bulk-request/prepare/manager/BulkRequestPrepareManager.hh"
#include "mgm/bulk-request/prepare/manager/PrepareManager.hh"
#include "mgm/bulk-request/prepare/query-prepare/QueryPrepareResult.hh"
#include "mgm/http/rest-api/business/tape/TapeRestApiBusiness.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <XrdSys/XrdSysError.hh>
#include <memory>
#include <vector>

USE_EOSBULKNAMESPACE;
USE_EOSMGMRESTNAMESPACE;

EOSMGMRESTNAMESPACE_BEGIN

class FailingBulkRequestPrepareManager : public bulk::BulkRequestPrepareManager
{
public:
  explicit FailingBulkRequestPrepareManager(const std::string& errorMsg)
    : bulk::BulkRequestPrepareManager(
        std::make_unique<testing::NiceMock<bulk::MockPrepareMgmFSInterface>>()),
      mErrorMsg(errorMsg) {}

  int prepare(XrdSfsPrep& pargs, XrdOucErrInfo& error,
              const common::VirtualIdentity* vid) override
  {
    (void)pargs;
    (void)vid;
    error.setErrInfo(1, mErrorMsg.c_str());
    return SFS_ERROR;
  }

private:
  std::string mErrorMsg;
};

class UnfinishedQueryPrepareManager : public bulk::PrepareManager
{
public:
  UnfinishedQueryPrepareManager()
    : bulk::PrepareManager(std::make_unique<testing::NiceMock<bulk::MockPrepareMgmFSInterface>>()) {}

  std::unique_ptr<bulk::QueryPrepareResult> queryPrepare(
    XrdSfsPrep& pargs, XrdOucErrInfo& error,
    const common::VirtualIdentity* vidClient) override
  {
    (void)pargs;
    (void)error;
    (void)vidClient;
    return std::make_unique<bulk::QueryPrepareResult>();
  }
};

class TestableTapeRestApiBusiness : public TapeRestApiBusiness
{
public:
  void setBulkRequestBusiness(std::shared_ptr<bulk::BulkRequestBusiness> business)
  {
    mBulkRequestBusiness = std::move(business);
  }

  void setPrepareManager(std::unique_ptr<bulk::PrepareManager> prepareManager)
  {
    mPrepareManager = std::move(prepareManager);
  }

  void setBulkRequestPrepareManager(
    std::unique_ptr<bulk::BulkRequestPrepareManager> prepareManager)
  {
    mBulkRequestPrepareManager = std::move(prepareManager);
  }

protected:
  std::shared_ptr<bulk::BulkRequestBusiness> createBulkRequestBusiness() override
  {
    return mBulkRequestBusiness;
  }

  std::unique_ptr<bulk::PrepareManager> createPrepareManager() override
  {
    return std::move(mPrepareManager);
  }

  std::unique_ptr<bulk::BulkRequestPrepareManager> createBulkRequestPrepareManager()
  override
  {
    return std::move(mBulkRequestPrepareManager);
  }

private:
  std::shared_ptr<bulk::BulkRequestBusiness> mBulkRequestBusiness;
  std::unique_ptr<bulk::PrepareManager> mPrepareManager;
  std::unique_ptr<bulk::BulkRequestPrepareManager> mBulkRequestPrepareManager;
};

EOSMGMRESTNAMESPACE_END

class TapeRestApiBusinessTest : public ::testing::Test
{
protected:
  static void SetUpTestSuite()
  {
    mSysErr = new XrdSysError(nullptr, "tape-rest-api-business-test");
    mOfs = new XrdMgmOfs(mSysErr);
    gOFS = mOfs;
  }

  static void TearDownTestSuite()
  {
    delete mOfs;
    delete mSysErr;
    gOFS = nullptr;
  }

  void SetUp() override
  {
    mStore = std::make_shared<bulk::InMemoryBulkRequestDAO::StageStore>();
    mDaoFactory = std::make_unique<bulk::InMemoryDAOFactory>(mStore);
    mBulkRequestBusiness = std::make_shared<bulk::BulkRequestBusiness>(
                             std::move(mDaoFactory));
    mBusiness = std::make_unique<eos::mgm::rest::TestableTapeRestApiBusiness>();
    mBusiness->setBulkRequestBusiness(mBulkRequestBusiness);
    mIssuer.uid = 1000;
    mOtherUser.uid = 2000;
  }

  void addStageRequest(const std::string& requestId, const std::string& path,
                       const eos::common::VirtualIdentity& issuer)
  {
    addStageRequestFiles(requestId, {path}, issuer);
  }

  void addStageRequestFiles(const std::string& requestId,
                            const std::vector<std::string>& paths,
                            const eos::common::VirtualIdentity& issuer)
  {
    auto request = std::make_unique<bulk::StageBulkRequest>(requestId, issuer);

    for (const auto& path : paths) {
      request->addFile(std::make_unique<bulk::File>(path));
    }

    bulk::InMemoryBulkRequestDAO dao(mStore);
    dao.addStageRequest(std::move(request));
  }

  void addStageRequestWithFileError(const std::string& requestId,
                                    const std::string& path,
                                    const std::string& error,
                                    const eos::common::VirtualIdentity& issuer)
  {
    auto request = std::make_unique<bulk::StageBulkRequest>(requestId, issuer);
    auto file = std::make_unique<bulk::File>(path);
    file->setError(error);
    request->addFile(std::move(file));
    bulk::InMemoryBulkRequestDAO dao(mStore);
    dao.addStageRequest(std::move(request));
  }

  bool stageRequestExists(const std::string& requestId) const
  {
    bulk::InMemoryBulkRequestDAO dao(mStore);
    return dao.exists(requestId, bulk::BulkRequest::PREPARE_STAGE);
  }

  static void setupCancelPrepareMock(bulk::MockPrepareMgmFSInterface& mockFs)
  {
    using ::testing::_;
    using ::testing::Invoke;
    using ::testing::Return;

    ON_CALL(mockFs, _exists(_, _, _, _, _, _))
    .WillByDefault(Invoke(bulk::MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA));
    ON_CALL(mockFs, _attr_ls(_, _, _, _, _, _))
    .WillByDefault(Invoke(bulk::MockPrepareMgmFSInterface::_ATTR_LS_ABORT_PREPARE_LAMBDA));
    ON_CALL(mockFs, _access(_, _, _, _, _)).WillByDefault(Return(SFS_OK));
    ON_CALL(mockFs, FSctl(_, _, _, _)).WillByDefault(Return(SFS_OK));
    ON_CALL(mockFs, get_logId()).WillByDefault(Return("log"));
    ON_CALL(mockFs, get_host()).WillByDefault(Return("host"));
    ON_CALL(mockFs, writeEosReportRecord(_)).WillByDefault(Return());
  }

  static void setupStagePrepareMock(bulk::MockPrepareMgmFSInterface& mockFs)
  {
    using ::testing::_;
    using ::testing::Invoke;
    using ::testing::Return;

    ON_CALL(mockFs, _exists(_, _, _, _, _, _))
    .WillByDefault(Invoke(bulk::MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA));
    ON_CALL(mockFs, _attr_ls(_, _, _, _, _, _))
    .WillByDefault(Invoke(bulk::MockPrepareMgmFSInterface::_ATTR_LS_STAGE_PREPARE_LAMBDA));
    ON_CALL(mockFs, _access(_, _, _, _, _)).WillByDefault(Return(SFS_OK));
    ON_CALL(mockFs, FSctl(_, _, _, _)).WillByDefault(Return(SFS_OK));
    ON_CALL(mockFs, getReqIdMaxCount()).WillByDefault(Return(64));
    ON_CALL(mockFs, get_logId()).WillByDefault(Return("log"));
    ON_CALL(mockFs, get_host()).WillByDefault(Return("host"));
    ON_CALL(mockFs, writeEosReportRecord(_)).WillByDefault(Return());
  }

  static void setupEvictPrepareMock(bulk::MockPrepareMgmFSInterface& mockFs)
  {
    using ::testing::_;
    using ::testing::Invoke;
    using ::testing::Return;

    ON_CALL(mockFs, _exists(_, _, _, _, _, _))
    .WillByDefault(Invoke(bulk::MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA));
    ON_CALL(mockFs, _attr_ls(_, _, _, _, _, _))
    .WillByDefault(Invoke(bulk::MockPrepareMgmFSInterface::_ATTR_LS_EVICT_PREPARE_LAMBDA));
    ON_CALL(mockFs, _access(_, _, _, _, _)).WillByDefault(Return(SFS_OK));
    ON_CALL(mockFs, FSctl(_, _, _, _)).WillByDefault(Return(SFS_OK));
    ON_CALL(mockFs, get_logId()).WillByDefault(Return("log"));
    ON_CALL(mockFs, get_host()).WillByDefault(Return("host"));
    ON_CALL(mockFs, writeEosReportRecord(_)).WillByDefault(Return());
  }

  std::unique_ptr<bulk::PrepareManager> makePrepareManager(
    std::unique_ptr<bulk::MockPrepareMgmFSInterface> mockFs)
  {
    return std::make_unique<bulk::PrepareManager>(std::move(mockFs));
  }

  std::unique_ptr<bulk::BulkRequestPrepareManager> makeBulkRequestPrepareManager(
    std::unique_ptr<bulk::MockPrepareMgmFSInterface> mockFs)
  {
    auto prepareManager =
      std::make_unique<bulk::BulkRequestPrepareManager>(std::move(mockFs));
    prepareManager->setBulkRequestBusiness(mBulkRequestBusiness);
    return prepareManager;
  }

  static XrdSysError* mSysErr;
  static XrdMgmOfs* mOfs;
  std::shared_ptr<bulk::InMemoryBulkRequestDAO::StageStore> mStore;
  std::unique_ptr<bulk::InMemoryDAOFactory> mDaoFactory;
  std::shared_ptr<bulk::BulkRequestBusiness> mBulkRequestBusiness;
  std::unique_ptr<eos::mgm::rest::TestableTapeRestApiBusiness> mBusiness;
  eos::common::VirtualIdentity mIssuer;
  eos::common::VirtualIdentity mOtherUser;
};

XrdSysError* TapeRestApiBusinessTest::mSysErr = nullptr;
XrdMgmOfs* TapeRestApiBusinessTest::mOfs = nullptr;

#endif // EOS_TAPERESTAPIBUSINESSTEST_HH
