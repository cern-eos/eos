//------------------------------------------------------------------------------
//! @file BulkRequestPrepareManagerTest.cc
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
#include "unit_tests/mgm/bulk-request/MockPrepareMgmFSInterface.hh"
#include "mgm/bulk-request/prepare/manager/BulkRequestPrepareManager.hh"
#include "mgm/bulk-request/BulkRequestFactory.hh"
#include "mgm/Namespace.hh"
#include "common/VirtualIdentity.hh"

TEST_F(BulkRequestPrepareManagerTest, bulkRequestTest)
{
  USE_EOSBULKNAMESPACE
  std::unique_ptr<StageBulkRequest> request =
    BulkRequestFactory::createStageBulkRequest("requestId",
        eos::common::VirtualIdentity::Root());
  //Let's add 10 files
  uint8_t nbFiles = 10;

  for (uint8_t i = 0; i < nbFiles; ++i) {
    std::stringstream ss;
    ss << "path" << i;
    std::unique_ptr<File> fileToAdd = std::make_unique<File>(ss.str());
    request->addFile(std::move(fileToAdd));
  }

  ASSERT_EQ(nbFiles, request->getFiles()->size());
}

using ::testing::MatchesRegex;

TEST_F(BulkRequestPrepareManagerTest, stagePrepareFilesWorkflow)
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
  // EOS-CTA reporter
  EXPECT_CALL(mgmOfs, get_logId()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, get_host()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, writeEosReportRecord(MatchesRegex(
                MockPrepareMgmFSInterface::EOS_REPORT_STR_FORMAT))).Times(nbFiles);
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId", Prep_STAGE, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::BulkRequestPrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()), *error,
                              client.getClient());
  ASSERT_EQ(nbFiles, pm.getBulkRequest()->getFiles()->size());
  ASSERT_EQ(SFS_DATA, retPrepare);
}

TEST_F(BulkRequestPrepareManagerTest, stagePrepareFileWithNoPath)
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
  eos::mgm::bulk::BulkRequestPrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()), *error,
                              client.getClient());
  //The bulk-request is created, 0 files are supposed to be there
  ASSERT_EQ(0, pm.getBulkRequest()->getFiles()->size());
  //The prepare manager returns SFS_DATA
  ASSERT_EQ(SFS_DATA, retPrepare);
}

TEST_F(BulkRequestPrepareManagerTest, stagePrepareFileWithEmptyStringPaths)
{
  //prepare stage should be idempotent https://its.cern.ch/jira/projects/EOS/issues/EOS-4739
  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr =
        std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface>& mgmOfs = *mgmOfsPtr;
  //No path exist, but Emsg should not be called
  EXPECT_CALL(mgmOfs, Emsg).Times(1);
  //No path are set, no mgmOfs method should be called
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(0);
  EXPECT_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _)).Times(0);
  EXPECT_CALL(mgmOfs, _access).Times(0);
  EXPECT_CALL(mgmOfs, FSctl).Times(0);
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId", Prep_STAGE, {""}, {""});
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::BulkRequestPrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()), *error,
                              client.getClient());
  //The bulk-request is created, 0 files are supposed to be there
  ASSERT_EQ(0, pm.getBulkRequest()->getFiles()->size());
  //The prepare manager returns SFS_DATA
  ASSERT_EQ(SFS_DATA, retPrepare);
}

TEST_F(BulkRequestPrepareManagerTest, stagePrepareAllFilesDoNotExist)
{
  /**
   * If all files do not exist, the prepare should succeed
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
  eos::mgm::bulk::BulkRequestPrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()), *error,
                              client.getClient());
  auto bulkRequest = pm.getBulkRequest();
  ASSERT_EQ(3, bulkRequest->getFiles()->size());
  ASSERT_EQ(3, bulkRequest->getAllFilesInError()->size());
  auto filesInError = bulkRequest->getAllFilesInError();

  for (auto fileInError : *filesInError) {
    ASSERT_EQ(0,
              fileInError.getError().value().find("prepare - file does not exist or is not accessible to you"));
  }

  ASSERT_EQ(SFS_DATA, retPrepare);
}

TEST_F(BulkRequestPrepareManagerTest,
       stagePrepareOneFileDoNotExistReturnsSfsData)
{
  /**
   * If one file does not exist, the prepare should succeed
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
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId", Prep_STAGE, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::BulkRequestPrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()), *error,
                              client.getClient());
  //All the files should be in the bulk-request, even the one that does not exist
  auto bulkRequest = pm.getBulkRequest();
  const auto& bulkReqPaths = bulkRequest->getFiles();
  ASSERT_EQ(nbFiles, bulkReqPaths->size());

  for (uint8_t i = 0; i < bulkReqPaths->size(); ++i) {
    ASSERT_EQ(paths[i], (*bulkReqPaths)[i]->getPath());
  }

  //Prepare is a success
  ASSERT_EQ(SFS_DATA, retPrepare);
}

TEST_F(BulkRequestPrepareManagerTest, stagePrepareNoPreparePermission)
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
  //As there is no prepare permission, Emsg should be called  for each file
  EXPECT_CALL(mgmOfs, Emsg).Times(nbFiles);
  //Everything is fine, all the files exist
  ON_CALL(mgmOfs, _exists(_, _, _, _, _, _)).WillByDefault(Invoke(
        MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA));
  //the _exists method should be called for all files
  EXPECT_CALL(mgmOfs, _exists(_, _, _, _, _, _)).Times(nbFiles);
  ON_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _))
  .WillByDefault(Invoke(
                   MockPrepareMgmFSInterface::_ATTR_LS_STAGE_PREPARE_LAMBDA
                 ));
  EXPECT_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _)).Times(nbFiles);
  //Access should
  EXPECT_CALL(mgmOfs, _access).Times(nbFiles).WillRepeatedly(Return(SFS_ERROR));
  EXPECT_CALL(mgmOfs, FSctl).Times(0);
  // EOS-CTA reporter
  EXPECT_CALL(mgmOfs, get_logId()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, get_host()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, writeEosReportRecord(MatchesRegex(
                MockPrepareMgmFSInterface::EOS_REPORT_STR_FORMAT))).Times(nbFiles);
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId", Prep_STAGE, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::BulkRequestPrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()), *error,
                              client.getClient());
  ASSERT_EQ(nbFiles, pm.getBulkRequest()->getFiles()->size());
  ASSERT_EQ(SFS_DATA, retPrepare);
}

TEST_F(BulkRequestPrepareManagerTest, abortPrepareFilesWorkflow)
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
                   MockPrepareMgmFSInterface::_ATTR_LS_ABORT_PREPARE_LAMBDA
                 ));
  EXPECT_CALL(mgmOfs, _attr_ls(_, _, _, _, _, _)).Times(nbFiles);
  EXPECT_CALL(mgmOfs, Emsg).Times(0);
  EXPECT_CALL(mgmOfs, _access).Times(nbFiles);
  EXPECT_CALL(mgmOfs, FSctl).Times(nbFiles);
  // EOS-CTA reporter
  EXPECT_CALL(mgmOfs, get_logId()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, get_host()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, writeEosReportRecord(MatchesRegex(
                MockPrepareMgmFSInterface::EOS_REPORT_STR_FORMAT))).Times(nbFiles);
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId", Prep_CANCEL, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::BulkRequestPrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()), *error,
                              client.getClient());
  //Abort prepare generates a bulk-request
  ASSERT_NE(nullptr, pm.getBulkRequest());
  //Abort prepare returns SFS_OK
  ASSERT_EQ(SFS_OK, retPrepare);
}

TEST_F(BulkRequestPrepareManagerTest, abortPrepareOnFileExistsOtherDoNotExist)
{
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
  //One file does not exist, but as we are idempotent, no error should be returned
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
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId", Prep_CANCEL, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::BulkRequestPrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()), *error,
                              client.getClient());
  ASSERT_EQ(SFS_OK, retPrepare);
}

TEST_F(BulkRequestPrepareManagerTest, evictPrepareFilesWorkflow)
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
  // EOS-CTA reporter
  EXPECT_CALL(mgmOfs, get_logId()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, get_host()).Times(nbFiles);
  EXPECT_CALL(mgmOfs, writeEosReportRecord(MatchesRegex(
                MockPrepareMgmFSInterface::EOS_REPORT_STR_FORMAT))).Times(nbFiles);
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId", Prep_EVICT, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::BulkRequestPrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()), *error,
                              client.getClient());
  //Evict prepare does not generate a bulk-request, so the bulk-request should be equal to nullptr
  std::unique_ptr<BulkRequest> bulkRequest = pm.getBulkRequest();
  ASSERT_EQ(nullptr, bulkRequest);
  //Evict prepare returns SFS_OK
  ASSERT_EQ(SFS_OK, retPrepare);
}

TEST_F(BulkRequestPrepareManagerTest, evictPrepareOneFileExistsOtherDoNotExist)
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
  //One file does not exist, Emsg should not be called as we are idempotent
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
  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId", Prep_EVICT, paths, oinfos);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo* error = errorWrapper.getError();
  eos::mgm::bulk::BulkRequestPrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()), *error,
                              client.getClient());
  ASSERT_EQ(SFS_OK, retPrepare);
}
