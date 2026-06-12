//------------------------------------------------------------------------------
//! @file TapeRestApiBusinessTest.cc
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

#include "TapeRestApiBusinessTest.hh"
#include "mgm/bulk-request/MockPrepareMgmFSInterface.hh"
#include "common/Constants.hh"
#include "mgm/http/rest-api/exception/Exceptions.hh"
#include "mgm/http/rest-api/model/tape/stage/PathsModel.hh"
#include <gmock/gmock.h>

using namespace eos::mgm::bulk;
using ::testing::Invoke;
using ::testing::NiceMock;

TEST_F(TapeRestApiBusinessTest, getStageBulkRequestThrowsWhenRequestMissing)
{
  ASSERT_THROW(mBusiness->getStageBulkRequest("missing-id", &mIssuer),
               ObjectNotFoundException);
}

TEST_F(TapeRestApiBusinessTest, getStageBulkRequestForbiddenForNonIssuer)
{
  const std::string requestId = "req-forbidden";
  addStageRequest(requestId, "/eos/user/file.txt", mIssuer);
  ASSERT_THROW(mBusiness->getStageBulkRequest(requestId, &mOtherUser),
               ForbiddenException);
}

TEST_F(TapeRestApiBusinessTest, getStageBulkRequestInProgressUsesOnDiskField)
{
  const std::string requestId = "req-in-progress";
  const std::string path = "/eos/user/staging.txt";
  addStageRequest(requestId, path, mIssuer);

  auto mockFsPtr = std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface>& mockFs = *mockFsPtr;
  EXPECT_CALL(mockFs, _exists(_, _, _, _, _, _))
  .WillRepeatedly(Invoke(MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA));
  EXPECT_CALL(mockFs, _stat(_, _, _, _, _, _, _, _))
  .WillRepeatedly(Invoke(MockPrepareMgmFSInterface::_STAT_FILE_ON_TAPE_ONLY));
  EXPECT_CALL(mockFs, _attr_ls(_, _, _, _, _, _))
  .WillRepeatedly(Invoke(
                     [](const char*, XrdOucErrInfo&, const eos::common::VirtualIdentity&,
                        const char*, eos::IContainerMD::XAttrMap& map, bool)
  {
    map[eos::common::RETRIEVE_REQID_ATTR_NAME] = requestId;
    map[eos::common::RETRIEVE_ERROR_ATTR_NAME] = "";
    map[eos::common::ARCHIVE_ERROR_ATTR_NAME] = "";
    return SFS_OK;
  }));
  EXPECT_CALL(mockFs, _access(_, _, _, _, _))
  .WillRepeatedly(Invoke(MockPrepareMgmFSInterface::_ACCESS_FILE_PREPARE_PERMISSION_LAMBDA));

  mBusiness->setPrepareManager(makePrepareManager(std::move(mockFsPtr)));
  auto response = mBusiness->getStageBulkRequest(requestId, &mIssuer);
  ASSERT_EQ(1u, response->getFiles().size());
  const auto& file = *response->getFiles().front();
  ASSERT_EQ(path, file.mPath);
  ASSERT_TRUE(file.mOnDisk.has_value());
  ASSERT_FALSE(*file.mOnDisk);
  ASSERT_FALSE(file.mState.has_value());
}

TEST_F(TapeRestApiBusinessTest, getStageBulkRequestTerminalSetsCompletedState)
{
  const std::string requestId = "req-terminal";
  const std::string path = "/eos/user/online.txt";
  addStageRequest(requestId, path, mIssuer);

  auto mockFsPtr = std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface>& mockFs = *mockFsPtr;
  EXPECT_CALL(mockFs, _exists(_, _, _, _, _, _))
  .WillRepeatedly(Invoke(MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA));
  EXPECT_CALL(mockFs, _stat(_, _, _, _, _, _, _, _))
  .WillRepeatedly(Invoke(MockPrepareMgmFSInterface::_STAT_FILE_ON_DISK_ONLY));
  EXPECT_CALL(mockFs, _attr_ls(_, _, _, _, _, _))
  .WillRepeatedly(Invoke(MockPrepareMgmFSInterface::_ATTR_LS_QUERY_PREPARE_NO_ERROR_LAMBDA));
  EXPECT_CALL(mockFs, _access(_, _, _, _, _))
  .WillRepeatedly(Invoke(MockPrepareMgmFSInterface::_ACCESS_FILE_PREPARE_PERMISSION_LAMBDA));

  mBusiness->setPrepareManager(makePrepareManager(std::move(mockFsPtr)));
  auto response = mBusiness->getStageBulkRequest(requestId, &mIssuer);
  ASSERT_EQ(1u, response->getFiles().size());
  const auto& file = *response->getFiles().front();
  ASSERT_TRUE(file.mState.has_value());
  ASSERT_EQ("COMPLETED", *file.mState);
  ASSERT_TRUE(file.mStartedAt.has_value());
  ASSERT_TRUE(file.mFinishedAt.has_value());
  ASSERT_FALSE(file.mOnDisk.has_value());
}

TEST_F(TapeRestApiBusinessTest, cancelStageBulkRequestThrowsWhenFileNotInRequest)
{
  const std::string requestId = "req-cancel";
  addStageRequest(requestId, "/eos/user/in-request.txt", mIssuer);
  PathsModel paths;
  paths.addFile("/eos/user/not-in-request.txt");
  ASSERT_THROW(mBusiness->cancelStageBulkRequest(requestId, &paths, &mIssuer),
               FileDoesNotBelongToBulkRequestException);
}
