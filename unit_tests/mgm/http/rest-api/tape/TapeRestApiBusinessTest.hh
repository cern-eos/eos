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
#include "mgm/http/rest-api/business/tape/TapeRestApiBusiness.hh"
#include "mgm/bulk-request/prepare/manager/PrepareManager.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include <gtest/gtest.h>
#include <XrdSys/XrdSysError.hh>
#include <memory>

USE_EOSBULKNAMESPACE;
USE_EOSMGMRESTNAMESPACE;

EOSMGMRESTNAMESPACE_BEGIN

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

protected:
  std::shared_ptr<bulk::BulkRequestBusiness> createBulkRequestBusiness() override
  {
    return mBulkRequestBusiness;
  }

  std::unique_ptr<bulk::PrepareManager> createPrepareManager() override
  {
    return std::move(mPrepareManager);
  }

private:
  std::shared_ptr<bulk::BulkRequestBusiness> mBulkRequestBusiness;
  std::unique_ptr<bulk::PrepareManager> mPrepareManager;
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
    auto request = std::make_unique<bulk::StageBulkRequest>(requestId, issuer);
    request->addFile(std::make_unique<bulk::File>(path));
    bulk::InMemoryBulkRequestDAO dao(mStore);
    dao.addStageRequest(std::move(request));
  }

  std::unique_ptr<bulk::PrepareManager> makePrepareManager(
    std::unique_ptr<bulk::MockPrepareMgmFSInterface> mockFs)
  {
    return std::make_unique<bulk::PrepareManager>(std::move(mockFs));
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
