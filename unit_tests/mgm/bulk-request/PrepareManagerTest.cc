//------------------------------------------------------------------------------
//! @file PrepareManagerTest.cc
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

#include "unit_tests/mgm/bulk-request/PrepareManagerTest.hh"
#include "common/Logging.hh"
#include "mgm/bulk-request/business/BulkRequestBusiness.hh"
#include "mgm/bulk-request/prepare/manager/BulkRequestPrepareManager.hh"
#include "mgm/bulk-request/prepare/manager/PrepareManager.hh"
#include "unit_tests/mgm/FakeSciTokensAuthz.hh"
#include "unit_tests/mgm/bulk-request/MockPrepareMgmFSInterface.hh"
#include "unit_tests/mgm/http/rest-api/tape/InMemoryBulkRequestDAO.hh"
#include <cerrno>
#include <functional>
#include <pwd.h>

using ::testing::_;
using ::testing::AllOf;
using ::testing::AnyNumber;
using ::testing::Contains;
using ::testing::HasSubstr;
using ::testing::Invoke;
using ::testing::MatchesRegex;
using ::testing::NiceMock;
using ::testing::Return;
using ::testing::StrEq;

USE_EOSBULKNAMESPACE


//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(PrepareManagerTest, PrepareUtilsPrepareOptsToString)
{
  using namespace eos::mgm;
  {
    const int opts = Prep_PRTY0;
    ASSERT_EQ("PRTY0", PrepareUtils::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_PRTY1;
    ASSERT_EQ("PRTY1", PrepareUtils::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_PRTY2;
    ASSERT_EQ("PRTY2", PrepareUtils::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_PRTY3;
    ASSERT_EQ("PRTY3", PrepareUtils::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_SENDAOK;
    ASSERT_EQ("PRTY0,SENDAOK", PrepareUtils::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_SENDERR;
    ASSERT_EQ("PRTY0,SENDERR", PrepareUtils::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_SENDACK;
    ASSERT_EQ("PRTY0,SENDACK", PrepareUtils::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_WMODE;
    ASSERT_EQ("PRTY0,WMODE", PrepareUtils::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_STAGE;
    ASSERT_EQ("PRTY0,STAGE", PrepareUtils::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_COLOC;
    ASSERT_EQ("PRTY0,COLOC", PrepareUtils::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_FRESH;
    ASSERT_EQ("PRTY0,FRESH", PrepareUtils::prepareOptsToString(opts));
  }
#if (XrdMajorVNUM(XrdVNUMBER) == 4 && XrdMinorVNUM(XrdVNUMBER) >= 10) || XrdMajorVNUM(XrdVNUMBER) >= 5
  {
    const int opts = Prep_CANCEL;
    ASSERT_EQ("PRTY0,CANCEL", PrepareUtils::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_QUERY;
    ASSERT_EQ("PRTY0,QUERY", PrepareUtils::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_EVICT;
    ASSERT_EQ("PRTY0,EVICT", PrepareUtils::prepareOptsToString(opts));
  }
#endif
}

TEST_F(PrepareManagerTest, pargsWrapperTest)
{
  PrepareArgumentsWrapper pargs("reqid", Prep_CANCEL);

  for (int i = 0; i < 10; ++i) {
    pargs.addFile(std::to_string(i), std::to_string(i));
  }

  ASSERT_EQ(10, pargs.getNbFiles());
  ASSERT_NE(nullptr, pargs.getPrepareArguments());
}

TEST_F(PrepareManagerTest, stagePrepareFilesWorkflow)
{
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(
                                     nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(
                                      nbFiles);
  // Add twice the same file to verify that the prepare workflow will be triggered
  // for duplicated path
  paths.push_back("a");
  paths.push_back("b");
  paths.push_back("a");
  oinfos.push_back("");
  oinfos.push_back("");
  oinfos.push_back("");
  nbFiles += 3;
  std::unique_ptr<MockPrepareMgmFSInterface> mgmOfsPtr =
    std::make_unique<MockPrepareMgmFSInterface>();
  MockPrepareMgmFSInterface& mgmOfs = *mgmOfsPtr;
  //addStats should be called only two times
  EXPECT_CALL(mgmOfs, addStats).Times(2);
  //isTapeEnabled should not be called as we are in the case where everything is fine
  EXPECT_CALL(mgmOfs, isTapeEnabled).Times(0);
  // Set default value for getReqIdMaxCount
  EXPECT_CALL(mgmOfs, getReqIdMaxCount()).Times(nbFiles).WillRepeatedly(Return(64));
  //As everything is fine, no Emsg should be called
  EXPECT_CALL(mgmOfs, Emsg).Times(0);
  //Everything is fine, all the files exist
  ON_CALL(mgmOfs, _exists(_, _, _, _, _, _)).WillByDefault(Invoke(
        MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA));
  //the _exists method should be called for all files
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(nbFiles);
  ON_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _))
  .WillByDefault(Invoke(
                   MockPrepareMgmFSInterface::_ATTR_LS_STAGE_PREPARE_LAMBDA
                 ));
  EXPECT_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _)).Times(2 * nbFiles);
  EXPECT_CALL(mgmOfs, Emsg).Times(0);
  EXPECT_CALL(mgmOfs, _access).Times(nbFiles);
  EXPECT_CALL(mgmOfs, FSctl).Times(nbFiles);
  EXPECT_CALL(mgmOfs, get_logId()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, get_host()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, writeEosReportRecord(MatchesRegex(
                MockPrepareMgmFSInterface::EOS_REPORT_STR_FORMAT))).Times(nbFiles);
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId", Prep_STAGE, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()), *error,
                              client.getClient());
  ASSERT_EQ(SFS_DATA, retPrepare);
}

TEST_F(PrepareManagerTest, stagePrepareFileWithNoPath)
{
  //prepare stage should be idempotent https://its.cern.ch/jira/projects/EOS/issues/EOS-4739
  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr =
        std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface>& mgmOfs = *mgmOfsPtr;
  //No path exist, but Emsg should not be called
  EXPECT_CALL(mgmOfs, Emsg).Times(0);
  //No path are set, no mgmOfs method should be called
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(0);
  EXPECT_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _)).Times(0);
  EXPECT_CALL(mgmOfs, _access).Times(0);
  EXPECT_CALL(mgmOfs, FSctl).Times(0);
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId", Prep_STAGE, {}, {});
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()), *error,
                              client.getClient());
  //The prepare manager returns SFS_ERROR
  ASSERT_EQ(SFS_ERROR, retPrepare);
}

TEST_F(PrepareManagerTest, stagePrepareAllFilesDoNotExist)
{
  /**
   * If all files do not exist, the prepare should not succeed
   */
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(
                                     nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(
                                      nbFiles);
  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr =
        std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface>& mgmOfs = *mgmOfsPtr;
  //One file does not exist, Emsg should be called once
  EXPECT_CALL(mgmOfs, Emsg).Times(nbFiles);
  ON_CALL(mgmOfs, _exists(_, _, _, _, _, _)).WillByDefault(Invoke(
        MockPrepareMgmFSInterface::_EXISTS_VID_FILE_DOES_NOT_EXIST_LAMBDA));
  //The current behaviour is that the prepare logic returns an error if at least one file does not exist.
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(3);
  EXPECT_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _)).Times(0);
  EXPECT_CALL(mgmOfs, _access).Times(0);
  EXPECT_CALL(mgmOfs, FSctl).Times(0);
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId", Prep_STAGE, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()), *error,
                              client.getClient());
  ASSERT_EQ(SFS_ERROR, retPrepare);
}

TEST_F(PrepareManagerTest, stagePrepareOneFileDoNotExistReturnsSfsData)
{
  /**
   * If all files do not exist, the prepare should succeed
   * Prepare is now idempotent (https://its.cern.ch/jira/projects/EOS/issues/EOS-4739)
   */
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(
                                     nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(
                                      nbFiles);
  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr =
        std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface>& mgmOfs = *mgmOfsPtr;
  //isTapeEnabled should not be called
  EXPECT_CALL(mgmOfs, isTapeEnabled).Times(0);
  // Set default value for getReqIdMaxCount
  EXPECT_CALL(mgmOfs, getReqIdMaxCount()).Times(nbFiles - 1).WillRepeatedly(Return(64));
  //One file does not exist, Emsg should be called once
  EXPECT_CALL(mgmOfs, Emsg).Times(1);
  //Exist will first return true for the existing file, then return false,
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(nbFiles).WillOnce(
    Invoke(MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA)
  ).WillOnce(Invoke(
               MockPrepareMgmFSInterface::_EXISTS_VID_FILE_DOES_NOT_EXIST_LAMBDA)
            ).WillRepeatedly(
              Invoke(MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA)
            );
  //Attr ls should work for the files that exist
  EXPECT_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _)).Times(2 * (nbFiles - 1))
  .WillRepeatedly(Invoke(
                    MockPrepareMgmFSInterface::_ATTR_LS_STAGE_PREPARE_LAMBDA
                  ));
  EXPECT_CALL(mgmOfs, _access).Times(nbFiles - 1);
  EXPECT_CALL(mgmOfs, FSctl).Times(nbFiles - 1);
  EXPECT_CALL(mgmOfs, get_logId()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, get_host()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, writeEosReportRecord(MatchesRegex(
                MockPrepareMgmFSInterface::EOS_REPORT_STR_FORMAT))).Times(nbFiles);
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId", Prep_STAGE, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()), *error,
                              client.getClient());
  //We failed the second file, the prepare is a success
  ASSERT_EQ(SFS_DATA, retPrepare);
}

TEST_F(PrepareManagerTest, abortPrepareFilesWorkflow)
{
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(
                                     nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(
                                      nbFiles);
  std::unique_ptr<MockPrepareMgmFSInterface> mgmOfsPtr =
    std::make_unique<MockPrepareMgmFSInterface>();
  MockPrepareMgmFSInterface& mgmOfs = *mgmOfsPtr;
  //addStats should be called only two times
  EXPECT_CALL(mgmOfs, addStats).Times(nbFiles - 1);
  //isTapeEnabled should not be called as we are in the case where everything is fine
  EXPECT_CALL(mgmOfs, isTapeEnabled).Times(0);
  // Set default value for getReqIdMaxCount
  EXPECT_CALL(mgmOfs, getReqIdMaxCount()).Times(0);
  //As everything is fine, no Emsg should be called
  EXPECT_CALL(mgmOfs, Emsg).Times(0);
  //Everything is fine, all the files exist
  ON_CALL(mgmOfs, _exists(_, _, _, _, _, _)).WillByDefault(Invoke(
        MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA));
  //the _exists method should be called for all files
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(nbFiles);
  ON_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _))
  .WillByDefault(Invoke(
                   MockPrepareMgmFSInterface::_ATTR_LS_ABORT_PREPARE_LAMBDA
                 ));
  EXPECT_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _)).Times(nbFiles);
  EXPECT_CALL(mgmOfs, Emsg).Times(0);
  EXPECT_CALL(mgmOfs, _access).Times(nbFiles);
  EXPECT_CALL(mgmOfs, FSctl).Times(nbFiles);
  EXPECT_CALL(mgmOfs, get_logId()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, get_host()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, writeEosReportRecord(MatchesRegex(
                MockPrepareMgmFSInterface::EOS_REPORT_STR_FORMAT))).Times(nbFiles);
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId", Prep_CANCEL, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()), *error,
                              client.getClient());
  //Abort prepare returns SFS_OK
  ASSERT_EQ(SFS_OK, retPrepare);
}

TEST_F(PrepareManagerTest, abortPrepareOneFileExistsOthersDoNotExist)
{
  /**
   * If one file does not exist, the prepare abort should fail
   */
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(
                                     nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(
                                      nbFiles);
  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr =
        std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface>& mgmOfs = *mgmOfsPtr;
  //isTapeEnabled should not be called
  EXPECT_CALL(mgmOfs, isTapeEnabled).Times(0);
  // Set default value for getReqIdMaxCount
  EXPECT_CALL(mgmOfs, getReqIdMaxCount()).Times(0);
  EXPECT_CALL(mgmOfs, Emsg).Times(nbFiles - 1);
  //Exist will first return true for the existing file, then return false
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(nbFiles).WillOnce(Invoke(
        MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA)).WillRepeatedly(
          Invoke(
            MockPrepareMgmFSInterface::_EXISTS_VID_FILE_DOES_NOT_EXIST_LAMBDA));
  //Attr ls should work for the file that exists
  EXPECT_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _)).Times(1)
  .WillRepeatedly(Invoke(
                    MockPrepareMgmFSInterface::_ATTR_LS_ABORT_PREPARE_LAMBDA
                  ));
  EXPECT_CALL(mgmOfs, _access).Times(1);
  EXPECT_CALL(mgmOfs, FSctl).Times(1);
  EXPECT_CALL(mgmOfs, get_logId()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, get_host()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, writeEosReportRecord(MatchesRegex(
                MockPrepareMgmFSInterface::EOS_REPORT_STR_FORMAT))).Times(nbFiles);
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId", Prep_CANCEL, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()), *error,
                              client.getClient());
  ASSERT_EQ(SFS_ERROR, retPrepare);
}

TEST_F(PrepareManagerTest, evictPrepareFilesWorkflow)
{
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(
                                     nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(
                                      nbFiles);
  std::unique_ptr<MockPrepareMgmFSInterface> mgmOfsPtr =
    std::make_unique<MockPrepareMgmFSInterface>();
  MockPrepareMgmFSInterface& mgmOfs = *mgmOfsPtr;
  //addStats should be called only two times
  EXPECT_CALL(mgmOfs, addStats).Times(2);
  //isTapeEnabled should not be called as we are in the case where everything is fine
  EXPECT_CALL(mgmOfs, isTapeEnabled).Times(0);
  // Set default value for getReqIdMaxCount
  EXPECT_CALL(mgmOfs, getReqIdMaxCount()).Times(0);
  //As everything is fine, no Emsg should be called
  EXPECT_CALL(mgmOfs, Emsg).Times(0);
  //Everything is fine, all the files exist
  ON_CALL(mgmOfs, _exists(_, _, _, _, _, _)).WillByDefault(Invoke(
        MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA));
  //the _exists method should be called for all files
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(nbFiles);
  ON_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _))
  .WillByDefault(Invoke(
                   MockPrepareMgmFSInterface::_ATTR_LS_EVICT_PREPARE_LAMBDA
                 ));
  EXPECT_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _)).Times(nbFiles);
  EXPECT_CALL(mgmOfs, Emsg).Times(0);
  EXPECT_CALL(mgmOfs, _access).Times(nbFiles);
  EXPECT_CALL(mgmOfs, FSctl).Times(nbFiles);
  EXPECT_CALL(mgmOfs, get_logId()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, get_host()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, writeEosReportRecord(MatchesRegex(
                MockPrepareMgmFSInterface::EOS_REPORT_STR_FORMAT))).Times(nbFiles);
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId", Prep_EVICT, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()), *error,
                              client.getClient());
  //Evict prepare returns SFS_OK
  ASSERT_EQ(SFS_OK, retPrepare);
}

TEST_F(PrepareManagerTest, evictPrepareOneFileExistsOthersDoNotExist)
{
  /**
   * If one file does not exist, the prepare evict should succeed
   * Prepare is now idempotent (https://its.cern.ch/jira/projects/EOS/issues/EOS-4739)
   */
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(
                                     nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(
                                      nbFiles);
  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr =
        std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface>& mgmOfs = *mgmOfsPtr;
  //isTapeEnabled should not be called
  EXPECT_CALL(mgmOfs, isTapeEnabled).Times(0);
  // Set default value for getReqIdMaxCount
  EXPECT_CALL(mgmOfs, getReqIdMaxCount()).Times(0);
  EXPECT_CALL(mgmOfs, Emsg).Times(nbFiles - 1);
  //Exist will first return true for the existing file, then return false
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(nbFiles).WillOnce(Invoke(
        MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA)).WillRepeatedly(
          Invoke(
            MockPrepareMgmFSInterface::_EXISTS_VID_FILE_DOES_NOT_EXIST_LAMBDA));
  //Attr ls should work for the files that exist
  EXPECT_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _)).Times(1)
  .WillRepeatedly(Invoke(
                    MockPrepareMgmFSInterface::_ATTR_LS_EVICT_PREPARE_LAMBDA
                  ));
  EXPECT_CALL(mgmOfs, _access).Times(1);
  EXPECT_CALL(mgmOfs, FSctl).Times(1);
  EXPECT_CALL(mgmOfs, get_logId()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, get_host()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, writeEosReportRecord(MatchesRegex(
                MockPrepareMgmFSInterface::EOS_REPORT_STR_FORMAT))).Times(nbFiles);
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId", Prep_EVICT, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()), *error,
                              client.getClient());
  ASSERT_EQ(SFS_ERROR, retPrepare);
}

TEST_F(PrepareManagerTest, queryPrepare)
{
  int nbFiles = 2;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(
                                     nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(
                                      nbFiles);
  //Add twice the same file to verify that query prepare will return the result twice for the same file
  paths.push_back("a");
  //One file in the middle to test that the order will be preserved
  paths.push_back("b");
  paths.push_back("a");
  oinfos.push_back("");
  oinfos.push_back("");
  oinfos.push_back("");
  nbFiles += 3;
  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr =
        std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface>& mgmOfs = *mgmOfsPtr;
  //Exist will first return true for the first file, then return false
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(nbFiles).WillOnce(Invoke(
        MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA)).WillRepeatedly(
          Invoke(
            MockPrepareMgmFSInterface::_EXISTS_VID_FILE_DOES_NOT_EXIST_LAMBDA));
  //stat with one file on disk, on file on disk and tape
  EXPECT_CALL(mgmOfs, _stat(_, _, _, _, _, _, _, _)).Times(1).WillOnce(Invoke(
        MockPrepareMgmFSInterface::_STAT_FILE_ON_DISK_AND_TAPE));
  //Attr ls should work for the files that exist
  EXPECT_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _)).Times(1)
  .WillRepeatedly(Invoke(
                    MockPrepareMgmFSInterface::_ATTR_LS_STAGE_PREPARE_LAMBDA
                  ));
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  std::string requestId = "testReqId";
  PrepareArgumentsWrapper pargs(requestId, Prep_QUERY, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  std::unique_ptr<eos::mgm::bulk::QueryPrepareResult> retQueryPrepare =
    pm.queryPrepare(*(pargs.getPrepareArguments()), *error, client.getClient());
  const auto& response = retQueryPrepare->getResponse();
  ASSERT_EQ(requestId, response->request_id);
  const auto& existingFile = response->responses.front();
  ASSERT_TRUE(existingFile.is_online);
  ASSERT_TRUE(existingFile.is_on_tape);
  ASSERT_TRUE(existingFile.is_exists);
  ASSERT_EQ(paths.front(), existingFile.path);
  const auto& notExistingFile = response->responses.back();
  ASSERT_FALSE(notExistingFile.is_online);
  ASSERT_FALSE(notExistingFile.is_on_tape);
  ASSERT_FALSE(notExistingFile.is_exists);
  ASSERT_EQ("USER ERROR: file does not exist or is not accessible to you",
            notExistingFile.error_text);
  ASSERT_FALSE(notExistingFile.can_show_locality.has_value());
  ASSERT_EQ(paths.back(), notExistingFile.path);

  //Test that the files are returned in the same order as they were submitted by the client
  for (int i = 0; i < nbFiles; ++i) {
    ASSERT_EQ(paths[i], retQueryPrepare->getResponse()->responses[i].path);
  }

  ASSERT_EQ(SFS_DATA, retQueryPrepare->getReturnCode());
}

TEST_F(PrepareManagerTest, queryPrepareFileDoesNotExist)
{
  int nbFiles = 1;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(
                                     nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(
                                      nbFiles);
  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr =
        std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface>& mgmOfs = *mgmOfsPtr;
  //First file does not exist
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(nbFiles).WillRepeatedly(
    Invoke(
      MockPrepareMgmFSInterface::_EXISTS_VID_FILE_DOES_NOT_EXIST_LAMBDA)
  );
  EXPECT_CALL(mgmOfs, _stat(_, _, _, _, _, _, _, _)).Times(0);
  EXPECT_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _)).Times(0);
  EXPECT_CALL(mgmOfs, _access(_, _, _, _, _)).Times(0);
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  std::string requestId = "testReqId";
  PrepareArgumentsWrapper pargs(requestId, Prep_QUERY, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  std::unique_ptr<eos::mgm::bulk::QueryPrepareResult> retQueryPrepare =
    pm.queryPrepare(*(pargs.getPrepareArguments()), *error, client.getClient());
  const auto& response = retQueryPrepare->getResponse();
  ASSERT_EQ(requestId, response->request_id);
  const auto& fileDoesNotExist = response->responses[0];
  ASSERT_FALSE(fileDoesNotExist.is_online);
  ASSERT_FALSE(fileDoesNotExist.is_on_tape);
  ASSERT_FALSE(fileDoesNotExist.is_exists);
  ASSERT_FALSE(fileDoesNotExist.is_reqid_present);
  ASSERT_FALSE(fileDoesNotExist.is_requested);
  ASSERT_FALSE(fileDoesNotExist.can_show_locality.has_value());
  ASSERT_FALSE(fileDoesNotExist.error_text.empty());
  ASSERT_TRUE(fileDoesNotExist.request_time.empty());
  ASSERT_EQ(paths[0], fileDoesNotExist.path);
}

TEST_F(PrepareManagerTest, queryPrepareFileStatFails)
{
  int nbFiles = 1;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(
                                     nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(
                                      nbFiles);
  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr =
        std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface>& mgmOfs = *mgmOfsPtr;
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(nbFiles).WillRepeatedly(
    Invoke(
      MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA)
  );
  EXPECT_CALL(mgmOfs, _stat(_, _, _, _, _, _, _, _)).Times(nbFiles).
  WillRepeatedly(
    Invoke(
      //Stat fails for one file
      MockPrepareMgmFSInterface::_STAT_ERROR
    ));
  EXPECT_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _)).Times(0);
  EXPECT_CALL(mgmOfs, _access(_, _, _, _, _)).Times(0);
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  std::string requestId = "testReqId";
  PrepareArgumentsWrapper pargs(requestId, Prep_QUERY, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  std::unique_ptr<eos::mgm::bulk::QueryPrepareResult> retQueryPrepare =
    pm.queryPrepare(*(pargs.getPrepareArguments()), *error, client.getClient());
  const auto& response = retQueryPrepare->getResponse();
  ASSERT_EQ(requestId, response->request_id);
  const auto& file = response->responses[0];
  ASSERT_FALSE(file.is_online);
  ASSERT_FALSE(file.is_on_tape);
  ASSERT_TRUE(file.is_exists);
  ASSERT_FALSE(file.is_reqid_present);
  ASSERT_FALSE(file.is_requested);
  ASSERT_FALSE(file.can_show_locality.has_value());
  ASSERT_EQ(MockPrepareMgmFSInterface::ERROR_STAT_STR, file.error_text);
  ASSERT_TRUE(file.request_time.empty());
  ASSERT_EQ(paths[0], file.path);
}

TEST_F(PrepareManagerTest, queryPrepareFileOnTapeRetrieveError)
{
  int nbFiles = 1;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(
                                     nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(
                                      nbFiles);
  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr =
        std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface>& mgmOfs = *mgmOfsPtr;
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(nbFiles).WillRepeatedly(
    Invoke(
      MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA)
  );
  EXPECT_CALL(mgmOfs, _stat(_, _, _, _, _, _, _, _)).WillRepeatedly(
    Invoke(
      //File could not be recalled
      MockPrepareMgmFSInterface::_STAT_FILE_ON_TAPE_ONLY
    ));
  EXPECT_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _)).Times(nbFiles).WillRepeatedly(
                                 Invoke(
                                   MockPrepareMgmFSInterface::_ATTR_LS_RETRIEVE_ERROR_LAMBDA
                                 ));
  EXPECT_CALL(mgmOfs, _access(_, _, _, _, _)).Times(nbFiles);
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  std::string requestId = MockPrepareMgmFSInterface::RETRIEVE_REQ_ID;
  PrepareArgumentsWrapper pargs(requestId, Prep_QUERY, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  std::unique_ptr<eos::mgm::bulk::QueryPrepareResult> retQueryPrepare =
    pm.queryPrepare(*(pargs.getPrepareArguments()), *error, client.getClient());
  const auto& response = retQueryPrepare->getResponse();
  ASSERT_EQ(requestId, response->request_id);
  const auto& file = response->responses[0];
  ASSERT_FALSE(file.is_online);
  ASSERT_TRUE(file.is_on_tape);
  ASSERT_TRUE(file.is_exists);
  ASSERT_TRUE(file.is_reqid_present);
  ASSERT_TRUE(file.is_requested);
  ASSERT_TRUE(file.can_show_locality.value_or(false));
  ASSERT_EQ(MockPrepareMgmFSInterface::ERROR_RETRIEVE_STR, file.error_text);
  ASSERT_EQ(MockPrepareMgmFSInterface::RETRIEVE_REQ_TIME, file.request_time);
  ASSERT_EQ(paths[0], file.path);
}

TEST_F(PrepareManagerTest, queryPrepareFileOnDiskArchiveError)
{
  int nbFiles = 1;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(
                                     nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(
                                      nbFiles);
  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr =
        std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface>& mgmOfs = *mgmOfsPtr;
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(nbFiles).WillRepeatedly(
    Invoke(
      MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA)
  );
  EXPECT_CALL(mgmOfs, _stat(_, _, _, _, _, _, _, _)).WillRepeatedly(
    Invoke(
      //File could not be recalled
      MockPrepareMgmFSInterface::_STAT_FILE_ON_DISK_ONLY
    ));
  EXPECT_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _)).Times(nbFiles).WillRepeatedly(
                                 Invoke(
                                   MockPrepareMgmFSInterface::_ATTR_LS_ARCHIVE_ERROR_LAMBDA
                                 ));
  EXPECT_CALL(mgmOfs, _access(_, _, _, _, _)).Times(nbFiles);
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  std::string requestId = "testReqId";
  PrepareArgumentsWrapper pargs(requestId, Prep_QUERY, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  std::unique_ptr<eos::mgm::bulk::QueryPrepareResult> retQueryPrepare =
    pm.queryPrepare(*(pargs.getPrepareArguments()), *error, client.getClient());
  const auto& response = retQueryPrepare->getResponse();
  ASSERT_EQ(requestId, response->request_id);
  const auto& file = response->responses[0];
  ASSERT_TRUE(file.is_online);
  ASSERT_FALSE(file.is_on_tape);
  ASSERT_TRUE(file.is_exists);
  ASSERT_FALSE(file.is_reqid_present);
  ASSERT_FALSE(file.is_requested);
  ASSERT_TRUE(file.can_show_locality.value_or(false));
  ASSERT_EQ(MockPrepareMgmFSInterface::ERROR_ARCHIVE_STR, file.error_text);
  ASSERT_TRUE(file.request_time.empty());
  ASSERT_EQ(paths[0], file.path);
}

TEST_F(PrepareManagerTest, queryPrepareFileNoPreparePermissionOnDirectory)
{
  int nbFiles = 1;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(
                                     nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(
                                      nbFiles);
  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr =
        std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface>& mgmOfs = *mgmOfsPtr;
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(nbFiles).WillRepeatedly(
    Invoke(
      MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA)
  );
  EXPECT_CALL(mgmOfs, _stat(_, _, _, _, _, _, _, _)).WillRepeatedly(
    Invoke(
      //File could not be recalled
      MockPrepareMgmFSInterface::_STAT_FILE_ON_TAPE_ONLY
    ));
  EXPECT_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _)).Times(nbFiles).WillRepeatedly(
                                 Invoke(
                                   //Set all possible errors, we want to see the "USER ERROR: you don't have prepare permission" error message in all cases.
                                   MockPrepareMgmFSInterface::_ATTR_LS_ARCHIVE_RETRIEVE_ERROR_LAMBDA
                                 ));
  EXPECT_CALL(mgmOfs, _access(_, _, _, _, _)).Times(nbFiles).WillRepeatedly(
    Invoke(
      MockPrepareMgmFSInterface::_ACCESS_FILE_NO_PREPARE_PERMISSION_LAMBDA
    )
  );
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  std::string requestId = "testReqId";
  PrepareArgumentsWrapper pargs(requestId, Prep_QUERY, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  std::unique_ptr<eos::mgm::bulk::QueryPrepareResult> retQueryPrepare =
    pm.queryPrepare(*(pargs.getPrepareArguments()), *error, client.getClient());
  const auto& response = retQueryPrepare->getResponse();
  ASSERT_EQ(requestId, response->request_id);
  const auto& file = response->responses[0];
  ASSERT_FALSE(file.is_online);
  ASSERT_TRUE(file.is_on_tape);
  ASSERT_TRUE(file.is_exists);
  ASSERT_FALSE(file.is_reqid_present);
  ASSERT_FALSE(file.is_requested);
  ASSERT_FALSE(file.can_show_locality.value_or(false));
  ASSERT_FALSE(file.error_text.empty());
  ASSERT_EQ("USER ERROR: you don't have prepare permission", file.error_text);
  ASSERT_TRUE(file.request_time.empty());
  ASSERT_EQ(paths[0], file.path);
}

//------------------------------------------------------------------------------
// The tape REST API queries the files of a request with its token, one file
// at a time. The client's entity lives as long as its connection: a token that
// does not validate must not give the user of an earlier token.
//------------------------------------------------------------------------------
class PrepareManagerTokenTest : public PrepareManagerTest {
protected:
  void
  SetUp() override
  {
    PrepareManagerTest::SetUp();
    mUids.swap(eos::common::Mapping::gVirtualUidMap);
    mGids.swap(eos::common::Mapping::gVirtualGidMap);
    // Map the https clients to the local user of their name, like
    // "eos vid enable https"
    eos::common::Mapping::gVirtualUidMap["https:\"<pwd>\":uid"] = 0;
    eos::common::Mapping::gVirtualGidMap["https:\"<pwd>\":gid"] = 0;
  }

  void
  TearDown() override
  {
    mUids.swap(eos::common::Mapping::gVirtualUidMap);
    mGids.swap(eos::common::Mapping::gVirtualGidMap);
    PrepareManagerTest::TearDown();
  }

  eos::common::Mapping::VirtualUserMap_t mUids;
  eos::common::Mapping::VirtualGroupMap_t mGids;
};

TEST_F(PrepareManagerTokenTest, queryPrepareInvalidTokenIgnoresTheUserOfAnEarlierToken)
{
  const struct passwd* pw = getpwnam("daemon");
  ASSERT_NE(nullptr, pw);
  const uid_t tokenUid = pw->pw_uid;
  int nbFiles = 2;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(nbFiles);
  FakeSciTokensAuthz authz("valid-token", "", "daemon");
  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr =
      std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface>& mgmOfs = *mgmOfsPtr;
  EXPECT_CALL(mgmOfs, getTokenHandler()).WillRepeatedly(Return(&authz));
  // Only the user of the token sees the files
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _))
      .WillRepeatedly(
          Invoke([tokenUid](const char* path, XrdSfsFileExistence& file_exists,
                            XrdOucErrInfo& error, eos::common::VirtualIdentity& vid,
                            const char* opaque, bool take_lock) {
            const bool visible = (vid.uid == tokenUid);
            file_exists = visible ? XrdSfsFileExistIsFile : XrdSfsFileExistNo;
            return visible ? SFS_OK : SFS_ERROR;
          }));
  EXPECT_CALL(mgmOfs, _stat(_, _, _, _, _, _, _, _))
      .WillRepeatedly(Invoke(MockPrepareMgmFSInterface::_STAT_FILE_ON_DISK_AND_TAPE));
  EXPECT_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _))
      .WillRepeatedly(
          Invoke(MockPrepareMgmFSInterface::_ATTR_LS_QUERY_PREPARE_NO_ERROR_LAMBDA));
  EXPECT_CALL(mgmOfs, _access(_, _, _, _, _))
      .WillRepeatedly(
          Invoke(MockPrepareMgmFSInterface::_ACCESS_FILE_PREPARE_PERMISSION_LAMBDA));
  // A client without a name of its own: without a token, it is nobody
  XrdSecEntity client("https");
  std::string tident = "client.1:2@client.example.org";
  client.tident = tident.c_str();
  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  // A query on the client's connection, with the token of its Authorization
  // header
  auto query = [&](const std::string& auth) {
    eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Nobody();
    vid.deferredClientPtr = &client;
    vid.deferredAuth = auth;
    PrepareArgumentsWrapper pargs("testReqId", Prep_QUERY, paths, oinfos);
    ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
    return pm.queryPrepare(*(pargs.getPrepareArguments()), *errorWrapper.getError(),
                           &vid);
  };
  std::unique_ptr<eos::mgm::bulk::QueryPrepareResult> valid = query("Bearer valid-token");
  ASSERT_EQ(SFS_DATA, valid->getReturnCode());
  ASSERT_EQ(paths.size(), valid->getResponse()->responses.size());

  for (const auto& file : valid->getResponse()->responses) {
    ASSERT_TRUE(file.is_exists) << file.path;
    EXPECT_TRUE(file.can_show_locality.value_or(false)) << file.path;
    EXPECT_EQ("", file.error_text) << file.path;
  }

  // Without the user of the earlier token, the token authorizes none of the
  // files, so the query is refused
  std::unique_ptr<eos::mgm::bulk::QueryPrepareResult> invalid =
      query("Bearer invalid-token");
  EXPECT_EQ(SFS_ERROR, invalid->getReturnCode());
  EXPECT_FALSE(invalid->hasQueryPrepareFinished());
  EXPECT_TRUE(invalid->getResponse()->responses.empty());
}

//------------------------------------------------------------------------------
// A change through the tape REST API (stage, cancel, release, DELETE) checks
// its token file by file. When the token authorizes none of the files, the
// change is refused with EACCES before any file is looked at and before
// anything is saved. Every other per-file error keeps the request, with the
// error of each file.
//------------------------------------------------------------------------------
class PrepareManagerTokenChangeTest : public PrepareManagerTokenTest {
protected:
  void
  SetUp() override
  {
    PrepareManagerTokenTest::SetUp();
    const struct passwd* pw = getpwnam("daemon");
    ASSERT_NE(nullptr, pw);
    mTokenUid = pw->pw_uid;
    mTokenGid = pw->pw_gid;
    mStore = std::make_shared<InMemoryBulkRequestDAO::StageStore>();
    mBulkRequestBusiness = std::make_shared<BulkRequestBusiness>(
        std::make_unique<InMemoryDAOFactory>(mStore));
    mClient.tident = mTident.c_str();
  }

  // Only the user of the token sees the files, if they exist. Each file is in
  // a directory with a stage workflow, where it may be prepared.
  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>>
  makeMgmOfs(XrdAccAuthorize& authz, const bool filesExist = true)
  {
    auto mgmOfs = std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
    const uid_t tokenUid = mTokenUid;
    ON_CALL(*mgmOfs, getTokenHandler()).WillByDefault(Return(&authz));
    ON_CALL(*mgmOfs, getReqIdMaxCount()).WillByDefault(Return(64));
    // Like XrdMgmOfs::Emsg, which puts the error code in the error object
    ON_CALL(*mgmOfs, Emsg(_, _, _, _, _))
        .WillByDefault(Invoke([](const char* pfx, XrdOucErrInfo& einfo, int ecode,
                                 const char* op, const char* target) {
          einfo.setErrInfo(ecode, op);
          return SFS_ERROR;
        }));
    ON_CALL(*mgmOfs, _exists(_, _, _, _, _, _))
        .WillByDefault(Invoke([tokenUid, filesExist](
                                  const char* path, XrdSfsFileExistence& file_exists,
                                  XrdOucErrInfo& error, eos::common::VirtualIdentity& vid,
                                  const char* opaque, bool take_lock) {
          const bool visible = filesExist && (vid.uid == tokenUid);
          file_exists = visible ? XrdSfsFileExistIsFile : XrdSfsFileExistNo;
          return visible ? SFS_OK : SFS_ERROR;
        }));
    ON_CALL(*mgmOfs, _attr_ls(_, _, _, _, _, _))
        .WillByDefault(Invoke(MockPrepareMgmFSInterface::_ATTR_LS_STAGE_PREPARE_LAMBDA));
    ON_CALL(*mgmOfs, _access(_, _, _, _, _))
        .WillByDefault(
            Invoke(MockPrepareMgmFSInterface::_ACCESS_FILE_PREPARE_PERMISSION_LAMBDA));
    ON_CALL(*mgmOfs, FSctl(_, _, _, _)).WillByDefault(Return(SFS_DATA));
    return mgmOfs;
  }

  // The identity of a tape REST request with a WLCG token: nobody until the
  // token is checked for each file
  eos::common::VirtualIdentity
  tokenVid(const std::string& token)
  {
    eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Nobody();
    vid.prot = "https";
    vid.tident = mTident.c_str();
    vid.deferredClientPtr = &mClient;
    vid.deferredAuth = token;
    return vid;
  }

  // The identity of a tape REST request with the client's own credentials,
  // e.g. an X.509 certificate of the user of the token
  eos::common::VirtualIdentity
  userVid()
  {
    eos::common::VirtualIdentity vid;
    vid.uid = mTokenUid;
    vid.gid = mTokenGid;
    vid.allowed_uids = {mTokenUid};
    vid.allowed_gids = {mTokenGid};
    vid.name = "daemon";
    vid.prot = "https";
    vid.tident = mTident.c_str();
    return vid;
  }

  int
  stage(BulkRequestPrepareManager& pm, const std::vector<std::string>& paths,
        const eos::common::VirtualIdentity& vid, XrdOucErrInfo& error)
  {
    PrepareArgumentsWrapper pargs("testReqId", Prep_STAGE, paths,
                                  PrepareManagerTest::generateEmptyOinfos(paths.size()));
    pm.setBulkRequestBusiness(mBulkRequestBusiness);
    return pm.prepare(*(pargs.getPrepareArguments()), error, &vid);
  }

  // The error of the file with this path in a stage request, or "none" if it
  // has none
  static std::string
  fileError(const StageBulkRequest& request, const std::string& path)
  {
    const auto files = request.getFilesMap();
    const auto it = files->find(path);

    if (it == files->end()) {
      return "not in the request";
    }

    return it->second->getError().value_or("none");
  }

  // The lines that the MGM log gets at error level while run() runs
  static std::vector<std::string>
  errorLinesOf(const std::function<void()>& run)
  {
    auto& logging = eos::common::Logging::GetInstance();
    const int oldMask = logging.gLogMask;
    logging.gLogMask = oldMask | LOG_MASK(LOG_ERR);
    logging.gMutex.Lock();
    const unsigned long first = logging.gLogCircularIndex[LOG_ERR];
    logging.gMutex.UnLock();
    run();
    logging.gLogMask = oldMask;
    std::vector<std::string> lines;
    logging.gMutex.Lock();
    const unsigned long last = logging.gLogCircularIndex[LOG_ERR];

    for (unsigned long i = first; i < last; ++i) {
      const char* text =
          logging.gLogMemory[LOG_ERR][i % logging.gCircularIndexSize].c_str();
      lines.emplace_back(text ? text : "");
    }

    logging.gMutex.UnLock();
    return lines;
  }

  uid_t mTokenUid = 0;
  gid_t mTokenGid = 0;
  // A client without a name of its own: without a token, it is nobody
  XrdSecEntity mClient{"https"};
  std::string mTident = "client.1:2@client.example.org";
  std::shared_ptr<InMemoryBulkRequestDAO::StageStore> mStore;
  std::shared_ptr<BulkRequestBusiness> mBulkRequestBusiness;
};

TEST_F(PrepareManagerTokenChangeTest, stageTokenAuthorizingNoFileIsRefused)
{
  // The token is valid, but none of the files is in its scope
  FakeSciTokensAuthz authz("valid-token", "/eos/elsewhere/", "daemon");
  auto mgmOfsPtr = makeMgmOfs(authz);
  auto& mgmOfs = *mgmOfsPtr;
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(0);
  EXPECT_CALL(mgmOfs, FSctl(_, _, _, _)).Times(0);
  BulkRequestPrepareManager pm(std::move(mgmOfsPtr));
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo& error = *errorWrapper.getError();
  ASSERT_EQ(SFS_ERROR, stage(pm, PrepareManagerTest::generateDefaultPaths(2),
                             tokenVid("Bearer valid-token"), error));
  EXPECT_EQ(EACCES, error.getErrInfo());
  EXPECT_TRUE(mStore->empty());
}

TEST_F(PrepareManagerTokenChangeTest, stageTokenMappedToNobodyIsRefused)
{
  // The token is accepted for every file, but its user is nobody
  FakeSciTokensAuthz authz("valid-token", "", "nobody");
  auto mgmOfsPtr = makeMgmOfs(authz);
  auto& mgmOfs = *mgmOfsPtr;
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(0);
  EXPECT_CALL(mgmOfs, FSctl(_, _, _, _)).Times(0);
  BulkRequestPrepareManager pm(std::move(mgmOfsPtr));
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo& error = *errorWrapper.getError();
  ASSERT_EQ(SFS_ERROR, stage(pm, PrepareManagerTest::generateDefaultPaths(2),
                             tokenVid("Bearer valid-token"), error));
  EXPECT_EQ(EACCES, error.getErrInfo());
  EXPECT_TRUE(mStore->empty());
}

TEST_F(PrepareManagerTokenChangeTest, stageTokenAuthorizingOneFileKeepsTheRequest)
{
  // path1 is in the scope of the token, path2 is not
  FakeSciTokensAuthz authz("valid-token", "path1", "daemon");
  auto mgmOfsPtr = makeMgmOfs(authz);
  auto& mgmOfs = *mgmOfsPtr;
  EXPECT_CALL(mgmOfs, FSctl(_, _, _, _)).Times(1);
  BulkRequestPrepareManager pm(std::move(mgmOfsPtr));
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo& error = *errorWrapper.getError();
  ASSERT_EQ(SFS_DATA,
            stage(pm, {"path1", "path2"}, tokenVid("Bearer valid-token"), error));
  ASSERT_EQ(1u, mStore->size());
  const StageBulkRequest& request = *mStore->begin()->second;
  ASSERT_EQ(2u, request.getFilesMap()->size());
  EXPECT_EQ("none", fileError(request, "path1"));
  EXPECT_NE("none", fileError(request, "path2"));
}

TEST_F(PrepareManagerTokenChangeTest, stageTokenAuthorizingMissingFilesKeepsTheRequest)
{
  // The token authorizes every file, but none of them exists
  FakeSciTokensAuthz authz("valid-token", "", "daemon");
  auto mgmOfsPtr = makeMgmOfs(authz, false);
  auto& mgmOfs = *mgmOfsPtr;
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(2);
  EXPECT_CALL(mgmOfs, FSctl(_, _, _, _)).Times(0);
  BulkRequestPrepareManager pm(std::move(mgmOfsPtr));
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo& error = *errorWrapper.getError();
  ASSERT_EQ(SFS_DATA, stage(pm, PrepareManagerTest::generateDefaultPaths(2),
                            tokenVid("Bearer valid-token"), error));
  ASSERT_EQ(1u, mStore->size());
  const StageBulkRequest& request = *mStore->begin()->second;
  ASSERT_EQ(2u, request.getFilesMap()->size());

  for (const auto& path : PrepareManagerTest::generateDefaultPaths(2)) {
    EXPECT_NE("none", fileError(request, path)) << path;
  }
}

TEST_F(PrepareManagerTokenChangeTest, stageWithoutTokenWhoseFilesAllFailKeepsTheRequest)
{
  // The client's own identity, not a token, maps every file. None of them
  // exists.
  FakeSciTokensAuthz authz("valid-token", "", "daemon");
  auto mgmOfsPtr = makeMgmOfs(authz, false);
  auto& mgmOfs = *mgmOfsPtr;
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(2);
  EXPECT_CALL(mgmOfs, FSctl(_, _, _, _)).Times(0);
  BulkRequestPrepareManager pm(std::move(mgmOfsPtr));
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo& error = *errorWrapper.getError();
  ASSERT_EQ(SFS_DATA,
            stage(pm, PrepareManagerTest::generateDefaultPaths(2), userVid(), error));
  ASSERT_EQ(1u, mStore->size());
  const StageBulkRequest& request = *mStore->begin()->second;
  ASSERT_EQ(2u, request.getFilesMap()->size());

  for (const auto& path : PrepareManagerTest::generateDefaultPaths(2)) {
    EXPECT_NE("none", fileError(request, path)) << path;
  }
}

TEST_F(PrepareManagerTokenChangeTest,
       stageTokenAuthorizingNoFileWritesAReportRecordForEachFile)
{
  // The token is valid, but none of the files is in its scope
  FakeSciTokensAuthz authz("valid-token", "/eos/elsewhere/", "daemon");
  auto mgmOfsPtr = makeMgmOfs(authz);
  auto& mgmOfs = *mgmOfsPtr;
  const std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(2);
  const std::string nobodyUid = std::to_string(eos::common::VirtualIdentity::kNobodyUid);

  // As for a file that is not prepared: who asked, and why it was not prepared
  for (const auto& path : paths) {
    EXPECT_CALL(
        mgmOfs,
        writeEosReportRecord(
            AllOf(HasSubstr("&path=" + path + "&"), HasSubstr("&ruid=" + nobodyUid + "&"),
                  HasSubstr("&senttowfe=false&"), HasSubstr("&successful=false&"),
                  HasSubstr("&error=prepare - the token authorizes none of the files"),
                  HasSubstr("sec.app=tape_prepare"))))
        .Times(1);
  }

  BulkRequestPrepareManager pm(std::move(mgmOfsPtr));
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo& error = *errorWrapper.getError();
  ASSERT_EQ(SFS_ERROR, stage(pm, paths, tokenVid("Bearer valid-token"), error));
  EXPECT_EQ(EACCES, error.getErrInfo());
  EXPECT_TRUE(mStore->empty());
}

TEST_F(PrepareManagerTokenChangeTest, stageTokenAuthorizingNoFileLogsTheClient)
{
  // The token is valid, but none of the files is in its scope
  FakeSciTokensAuthz authz("valid-token", "/eos/elsewhere/", "daemon");
  BulkRequestPrepareManager pm(makeMgmOfs(authz));
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo& error = *errorWrapper.getError();
  int retCode = SFS_OK;
  const std::vector<std::string> lines = errorLinesOf([&]() {
    retCode = stage(pm, PrepareManagerTest::generateDefaultPaths(2),
                    tokenVid("Bearer valid-token"), error);
  });
  ASSERT_EQ(SFS_ERROR, retCode);
  // One error line tells who was refused, for what and for how many files
  EXPECT_THAT(
      lines,
      Contains(AllOf(
          HasSubstr("msg=\"the token authorizes none of the files, request refused\""),
          HasSubstr("event=\"sync::prepare\""), HasSubstr("client=\"" + mTident + "\""),
          HasSubstr("nfiles=2"))));
}

//------------------------------------------------------------------------------
// A query through the tape REST API (archiveinfo, stage status) checks its
// token file by file, like a change. When the token authorizes none of the
// files, the query is refused with EACCES before any file is looked at.
// Otherwise every file gets its answer, with an error for each file the token
// does not authorize.
//------------------------------------------------------------------------------
class PrepareManagerTokenQueryTest : public PrepareManagerTokenChangeTest {
protected:
  // As for a change, and each file is on disk and on tape
  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>>
  makeQueryMgmOfs(XrdAccAuthorize& authz, const bool filesExist = true)
  {
    auto mgmOfs = makeMgmOfs(authz, filesExist);
    ON_CALL(*mgmOfs, _stat(_, _, _, _, _, _, _, _))
        .WillByDefault(Invoke(MockPrepareMgmFSInterface::_STAT_FILE_ON_DISK_AND_TAPE));
    return mgmOfs;
  }

  std::unique_ptr<QueryPrepareResult>
  query(PrepareManager& pm, const std::vector<std::string>& paths,
        const eos::common::VirtualIdentity& vid, XrdOucErrInfo& error)
  {
    PrepareArgumentsWrapper pargs("testReqId", Prep_QUERY, paths,
                                  PrepareManagerTest::generateEmptyOinfos(paths.size()));
    return pm.queryPrepare(*(pargs.getPrepareArguments()), error, &vid);
  }

  // The error of the answer for this path, or "none" if it has none
  static std::string
  answerError(const QueryPrepareResult& result, const std::string& path)
  {
    for (const auto& file : result.getResponse()->responses) {
      if (file.path == path) {
        return file.error_text.empty() ? "none" : file.error_text;
      }
    }

    return "no answer";
  }
};

TEST_F(PrepareManagerTokenQueryTest, queryTokenAuthorizingNoFileIsRefused)
{
  // The token is valid, but none of the files is in its scope
  FakeSciTokensAuthz authz("valid-token", "/eos/elsewhere/", "daemon");
  auto mgmOfsPtr = makeQueryMgmOfs(authz);
  auto& mgmOfs = *mgmOfsPtr;
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(0);
  EXPECT_CALL(mgmOfs, _stat(_, _, _, _, _, _, _, _)).Times(0);
  PrepareManager pm(std::move(mgmOfsPtr));
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo& error = *errorWrapper.getError();
  auto result = query(pm, PrepareManagerTest::generateDefaultPaths(2),
                      tokenVid("Bearer valid-token"), error);
  EXPECT_EQ(SFS_ERROR, result->getReturnCode());
  EXPECT_FALSE(result->hasQueryPrepareFinished());
  EXPECT_EQ(EACCES, error.getErrInfo());
  EXPECT_TRUE(result->getResponse()->responses.empty());
}

TEST_F(PrepareManagerTokenQueryTest, queryTokenMappedToNobodyIsRefused)
{
  // The token is accepted for every file, but its user is nobody
  FakeSciTokensAuthz authz("valid-token", "", "nobody");
  auto mgmOfsPtr = makeQueryMgmOfs(authz);
  auto& mgmOfs = *mgmOfsPtr;
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(0);
  EXPECT_CALL(mgmOfs, _stat(_, _, _, _, _, _, _, _)).Times(0);
  PrepareManager pm(std::move(mgmOfsPtr));
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo& error = *errorWrapper.getError();
  auto result = query(pm, PrepareManagerTest::generateDefaultPaths(2),
                      tokenVid("Bearer valid-token"), error);
  EXPECT_EQ(SFS_ERROR, result->getReturnCode());
  EXPECT_FALSE(result->hasQueryPrepareFinished());
  EXPECT_EQ(EACCES, error.getErrInfo());
  EXPECT_TRUE(result->getResponse()->responses.empty());
}

TEST_F(PrepareManagerTokenQueryTest, queryTokenAuthorizingOneFileAnswersEveryFile)
{
  // path1 is in the scope of the token, path2 is not
  FakeSciTokensAuthz authz("valid-token", "path1", "daemon");
  PrepareManager pm(makeQueryMgmOfs(authz));
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo& error = *errorWrapper.getError();
  auto result = query(pm, {"path1", "path2"}, tokenVid("Bearer valid-token"), error);
  ASSERT_EQ(SFS_DATA, result->getReturnCode());
  ASSERT_EQ(2u, result->getResponse()->responses.size());
  EXPECT_EQ("none", answerError(*result, "path1"));
  EXPECT_EQ("USER ERROR: file does not exist or is not accessible to you",
            answerError(*result, "path2"));
}

TEST_F(PrepareManagerTokenQueryTest, queryTokenAuthorizingMissingFilesAnswersEveryFile)
{
  // The token authorizes every file, but none of them exists
  FakeSciTokensAuthz authz("valid-token", "", "daemon");
  PrepareManager pm(makeQueryMgmOfs(authz, false));
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo& error = *errorWrapper.getError();
  auto result = query(pm, PrepareManagerTest::generateDefaultPaths(2),
                      tokenVid("Bearer valid-token"), error);
  ASSERT_EQ(SFS_DATA, result->getReturnCode());
  ASSERT_EQ(2u, result->getResponse()->responses.size());

  for (const auto& path : PrepareManagerTest::generateDefaultPaths(2)) {
    EXPECT_EQ("USER ERROR: file does not exist or is not accessible to you",
              answerError(*result, path))
        << path;
  }
}

TEST_F(PrepareManagerTokenQueryTest, queryWithoutTokenWhoseFilesAllFailAnswersEveryFile)
{
  // The client's own identity, not a token, maps every file. None of them
  // exists.
  FakeSciTokensAuthz authz("valid-token", "", "daemon");
  PrepareManager pm(makeQueryMgmOfs(authz, false));
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo& error = *errorWrapper.getError();
  auto result = query(pm, PrepareManagerTest::generateDefaultPaths(2), userVid(), error);
  ASSERT_EQ(SFS_DATA, result->getReturnCode());
  ASSERT_EQ(2u, result->getResponse()->responses.size());

  for (const auto& path : PrepareManagerTest::generateDefaultPaths(2)) {
    EXPECT_EQ("USER ERROR: file does not exist or is not accessible to you",
              answerError(*result, path))
        << path;
  }
}

TEST_F(PrepareManagerTokenQueryTest, queryTokenAuthorizingNoFileCountsInTheStats)
{
  // The token is valid, but none of the files is in its scope
  FakeSciTokensAuthz authz("valid-token", "/eos/elsewhere/", "daemon");
  auto mgmOfsPtr = makeQueryMgmOfs(authz);
  auto& mgmOfs = *mgmOfsPtr;
  // A refused query counts in QueryPrepare (eos ns stat), as any other query
  EXPECT_CALL(mgmOfs, addStats(_, _, _, _)).Times(AnyNumber());
  EXPECT_CALL(mgmOfs, addStats(StrEq("QueryPrepare"), _, _, 2)).Times(1);
  PrepareManager pm(std::move(mgmOfsPtr));
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo& error = *errorWrapper.getError();
  auto result = query(pm, PrepareManagerTest::generateDefaultPaths(2),
                      tokenVid("Bearer valid-token"), error);
  EXPECT_EQ(SFS_ERROR, result->getReturnCode());
  EXPECT_EQ(EACCES, error.getErrInfo());
}

TEST_F(PrepareManagerTokenQueryTest, queryTokenAuthorizingNoFileLogsTheClient)
{
  // The token is valid, but none of the files is in its scope
  FakeSciTokensAuthz authz("valid-token", "/eos/elsewhere/", "daemon");
  PrepareManager pm(makeQueryMgmOfs(authz));
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo& error = *errorWrapper.getError();
  std::unique_ptr<QueryPrepareResult> result;
  const std::vector<std::string> lines = errorLinesOf([&]() {
    result = query(pm, PrepareManagerTest::generateDefaultPaths(2),
                   tokenVid("Bearer valid-token"), error);
  });
  ASSERT_EQ(SFS_ERROR, result->getReturnCode());
  // One error line tells who was refused, for which request and for how many
  // files
  EXPECT_THAT(
      lines,
      Contains(AllOf(
          HasSubstr("msg=\"the token authorizes none of the files, query refused\""),
          HasSubstr("reqid=\"testReqId\""), HasSubstr("client=\"" + mTident + "\""),
          HasSubstr("nfiles=2"))));
}
