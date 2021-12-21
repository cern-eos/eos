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

#include "unit_tests/mgm/bulk-request/MockPrepareMgmFSInterface.hh"
#include "unit_tests/mgm/bulk-request/PrepareManagerTest.hh"
#include "mgm/bulk-request/prepare/manager/PrepareManager.hh"

using ::testing::Return;
using ::testing::_;
using ::testing::Invoke;
using ::testing::NiceMock;

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

TEST_F(PrepareManagerTest,stagePrepareFilesWorkflow){
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(nbFiles);

  std::unique_ptr<MockPrepareMgmFSInterface> mgmOfsPtr = std::make_unique<MockPrepareMgmFSInterface>();
  MockPrepareMgmFSInterface & mgmOfs = *mgmOfsPtr;
  //addStats should be called only two times
  EXPECT_CALL(mgmOfs,addStats).Times(2);
  //isTapeEnabled should not be called as we are in the case where everything is fine
  EXPECT_CALL(mgmOfs,isTapeEnabled).Times(0);
  //As everything is fine, no Emsg should be called
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  //Everything is fine, all the files exist
  ON_CALL(mgmOfs,_exists(_,_,_,_,_,_)).WillByDefault(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA));
  //the _exists method should be called for all files
  EXPECT_CALL(mgmOfs,_exists(_,_,_,_,_,_)).Times(nbFiles);
  ON_CALL(mgmOfs, _attr_ls(_,_,_,_,_,_,_))
      .WillByDefault(Invoke(
          MockPrepareMgmFSInterface::_ATTR_LS_STAGE_PREPARE_LAMBDA
      ));
  EXPECT_CALL(mgmOfs, _attr_ls(_,_,_,_,_,_,_)).Times(nbFiles);
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  EXPECT_CALL(mgmOfs,_access).Times(nbFiles);
  EXPECT_CALL(mgmOfs,FSctl).Times(nbFiles);

  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId",Prep_STAGE,oinfos,paths);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo * error = errorWrapper.getError();

  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()),*error,client.getClient());

  ASSERT_EQ(SFS_DATA,retPrepare);
}

TEST_F(PrepareManagerTest,stagePrepareFileWithNoPath){
  //prepare stage should be idempotent https://its.cern.ch/jira/projects/EOS/issues/EOS-4739
  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr = std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface> & mgmOfs = *mgmOfsPtr;
  //No path exist, but Emsg should not be called
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  //No path are set, no mgmOfs method should be called
  EXPECT_CALL(mgmOfs,_exists(_,_,_,_,_,_)).Times(0);
  EXPECT_CALL(mgmOfs, _attr_ls(_,_,_,_,_,_,_)).Times(0);
  EXPECT_CALL(mgmOfs,_access).Times(0);
  EXPECT_CALL(mgmOfs,FSctl).Times(0);

  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId",Prep_STAGE, {""},{""});
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo * error = errorWrapper.getError();

  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()),*error,client.getClient());

  //The prepare manager returns SFS_DATA
  ASSERT_EQ(SFS_DATA,retPrepare);
}

TEST_F(PrepareManagerTest,stagePrepareAllFilesDoNotExist){
  /**
   * If all files do not exist, the prepare should succeed
   * prepare is now idempotent (https://its.cern.ch/jira/projects/EOS/issues/EOS-4739)
   */
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(nbFiles);

  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr = std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface> & mgmOfs = *mgmOfsPtr;
  //One file does not exist, Emsg should be called once
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  ON_CALL(mgmOfs,_exists(_,_,_,_,_,_)).WillByDefault(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_VID_FILE_DOES_NOT_EXIST_LAMBDA));
  //The current behaviour is that the prepare logic returns an error if at least one file does not exist.
  EXPECT_CALL(mgmOfs,_exists(_,_,_,_,_,_)).Times(3);
  EXPECT_CALL(mgmOfs, _attr_ls(_,_,_,_,_,_,_)).Times(0);
  EXPECT_CALL(mgmOfs,_access).Times(0);
  EXPECT_CALL(mgmOfs,FSctl).Times(0);

  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId",Prep_STAGE,oinfos,paths);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo * error = errorWrapper.getError();

  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()),*error,client.getClient());

  ASSERT_EQ(SFS_DATA,retPrepare);
}

TEST_F(PrepareManagerTest,stagePrepareOneFileDoNotExistReturnsSfsData){
  /**
   * If all files do not exist, the prepare should succeed
   * Prepare is now idempotent (https://its.cern.ch/jira/projects/EOS/issues/EOS-4739)
   */
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(nbFiles);

  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr = std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface> & mgmOfs = *mgmOfsPtr;
  //isTapeEnabled should not be called
  EXPECT_CALL(mgmOfs,isTapeEnabled).Times(0);
  //One file does not exist, Emsg should be called once
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  //Exist will first return true for the existing file, then return false,
  EXPECT_CALL(mgmOfs,_exists(_,_,_,_,_,_)).Times(nbFiles).WillOnce(
    Invoke(MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA)
  ).WillOnce(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_VID_FILE_DOES_NOT_EXIST_LAMBDA)
  ).WillRepeatedly(
    Invoke(MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA)
  );
  //Attr ls should work for the files that exist
  EXPECT_CALL(mgmOfs, _attr_ls(_,_,_,_,_,_,_)).Times(nbFiles - 1)
      .WillRepeatedly(Invoke(
          MockPrepareMgmFSInterface::_ATTR_LS_STAGE_PREPARE_LAMBDA
      ));
  EXPECT_CALL(mgmOfs,_access).Times(nbFiles - 1);
  EXPECT_CALL(mgmOfs,FSctl).Times(nbFiles - 1);

  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId",Prep_STAGE,oinfos,paths);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo * error = errorWrapper.getError();

  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));

  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()),*error,client.getClient());

  //We failed the second file, the prepare is a success
  ASSERT_EQ(SFS_DATA,retPrepare);
}

TEST_F(PrepareManagerTest,abortPrepareFilesWorkflow){
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(nbFiles);

  std::unique_ptr<MockPrepareMgmFSInterface> mgmOfsPtr = std::make_unique<MockPrepareMgmFSInterface>();
  MockPrepareMgmFSInterface & mgmOfs = *mgmOfsPtr;
  //addStats should be called only two times
  EXPECT_CALL(mgmOfs,addStats).Times(2);
  //isTapeEnabled should not be called as we are in the case where everything is fine
  EXPECT_CALL(mgmOfs,isTapeEnabled).Times(0);
  //As everything is fine, no Emsg should be called
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  //Everything is fine, all the files exist
  ON_CALL(mgmOfs,_exists(_,_,_,_,_,_)).WillByDefault(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA));
  //the _exists method should be called for all files
  EXPECT_CALL(mgmOfs,_exists(_,_,_,_,_,_)).Times(nbFiles);
  ON_CALL(mgmOfs, _attr_ls(_,_,_,_,_,_,_))
      .WillByDefault(Invoke(
        MockPrepareMgmFSInterface::_ATTR_LS_ABORT_PREPARE_LAMBDA
      ));
  EXPECT_CALL(mgmOfs, _attr_ls(_,_,_,_,_,_,_)).Times(nbFiles);
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  EXPECT_CALL(mgmOfs,_access).Times(nbFiles);
  EXPECT_CALL(mgmOfs,FSctl).Times(nbFiles);

  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId",Prep_CANCEL,oinfos,paths);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo * error = errorWrapper.getError();

  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()),*error,client.getClient());
  //Abort prepare returns SFS_OK
  ASSERT_EQ(SFS_OK,retPrepare);
}

TEST_F(PrepareManagerTest,abortPrepareOneFileDoesNotExist){
  /**
   * If one file does not exist, the prepare abort should succeed
   * Prepare is now idempotent (https://its.cern.ch/jira/projects/EOS/issues/EOS-4739)
   */
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(nbFiles);

  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr = std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface> & mgmOfs = *mgmOfsPtr;
  //isTapeEnabled should not be called
  EXPECT_CALL(mgmOfs,isTapeEnabled).Times(0);
  //One file does not exist, but as we are idempotent, no error should be returned
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  //Exist will first return true for the existing file, then return false
  EXPECT_CALL(mgmOfs,_exists(_,_,_,_,_,_)).Times(nbFiles).WillOnce(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA)).WillRepeatedly(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_VID_FILE_DOES_NOT_EXIST_LAMBDA));
  //Attr ls should work for the file that exists
  EXPECT_CALL(mgmOfs, _attr_ls(_,_,_,_,_,_,_)).Times(1)
      .WillRepeatedly(Invoke(
          MockPrepareMgmFSInterface::_ATTR_LS_ABORT_PREPARE_LAMBDA
      ));
  EXPECT_CALL(mgmOfs,_access).Times(1);
  EXPECT_CALL(mgmOfs,FSctl).Times(1);

  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId",Prep_CANCEL,oinfos,paths);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo * error = errorWrapper.getError();

  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));

  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()),*error,client.getClient());
  ASSERT_EQ(SFS_OK,retPrepare);
}

TEST_F(PrepareManagerTest,evictPrepareFilesWorkflow){
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(nbFiles);

  std::unique_ptr<MockPrepareMgmFSInterface> mgmOfsPtr = std::make_unique<MockPrepareMgmFSInterface>();
  MockPrepareMgmFSInterface & mgmOfs = *mgmOfsPtr;
  //addStats should be called only two times
  EXPECT_CALL(mgmOfs,addStats).Times(2);
  //isTapeEnabled should not be called as we are in the case where everything is fine
  EXPECT_CALL(mgmOfs,isTapeEnabled).Times(0);
  //As everything is fine, no Emsg should be called
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  //Everything is fine, all the files exist
  ON_CALL(mgmOfs,_exists(_,_,_,_,_,_)).WillByDefault(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA));
  //the _exists method should be called for all files
  EXPECT_CALL(mgmOfs,_exists(_,_,_,_,_,_)).Times(nbFiles);
  ON_CALL(mgmOfs, _attr_ls(_,_,_,_,_,_,_))
      .WillByDefault(Invoke(
          MockPrepareMgmFSInterface::_ATTR_LS_EVICT_PREPARE_LAMBDA
      ));
  EXPECT_CALL(mgmOfs, _attr_ls(_,_,_,_,_,_,_)).Times(nbFiles);
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  EXPECT_CALL(mgmOfs,_access).Times(nbFiles);
  EXPECT_CALL(mgmOfs,FSctl).Times(nbFiles);

  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId",Prep_EVICT,oinfos,paths);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo * error = errorWrapper.getError();

  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()),*error,client.getClient());
  //Evict prepare returns SFS_OK
  ASSERT_EQ(SFS_OK,retPrepare);
}

TEST_F(PrepareManagerTest,evictPrepareOneFileDoesNotExist){
  /**
   * If one file does not exist, the prepare evict should succeed
   * Prepare is now idempotent (https://its.cern.ch/jira/projects/EOS/issues/EOS-4739)
   */
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(nbFiles);

  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr = std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface> & mgmOfs = *mgmOfsPtr;
  //isTapeEnabled should not be called
  EXPECT_CALL(mgmOfs,isTapeEnabled).Times(0);
  //One file does not exist, Emsg should not be called as we are idempotent
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  //Exist will first return true for the existing file, then return false
  EXPECT_CALL(mgmOfs,_exists(_,_,_,_,_,_)).Times(nbFiles).WillOnce(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA)).WillRepeatedly(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_VID_FILE_DOES_NOT_EXIST_LAMBDA));
  //Attr ls should work for the files that exist
  EXPECT_CALL(mgmOfs, _attr_ls(_,_,_,_,_,_,_)).Times(1)
      .WillRepeatedly(Invoke(
          MockPrepareMgmFSInterface::_ATTR_LS_EVICT_PREPARE_LAMBDA
      ));
  EXPECT_CALL(mgmOfs,_access).Times(1);
  EXPECT_CALL(mgmOfs,FSctl).Times(1);

  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId",Prep_EVICT,oinfos,paths);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo * error = errorWrapper.getError();

  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));

  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()),*error,client.getClient());
  ASSERT_EQ(SFS_OK,retPrepare);
}

TEST_F(PrepareManagerTest,queryPrepare){
  int nbFiles = 2;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(nbFiles);

  std::unique_ptr<NiceMock<MockPrepareMgmFSInterface>> mgmOfsPtr = std::make_unique<NiceMock<MockPrepareMgmFSInterface>>();
  NiceMock<MockPrepareMgmFSInterface> & mgmOfs = *mgmOfsPtr;
  //Exist will first return true for the first file, then return false
  EXPECT_CALL(mgmOfs,_exists(_,_,_,_,_,_)).Times(nbFiles).WillOnce(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_VID_FILE_EXISTS_LAMBDA)).WillRepeatedly(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_VID_FILE_DOES_NOT_EXIST_LAMBDA));
  //stat with one file on disk, on file on disk and tape
  EXPECT_CALL(mgmOfs,_stat(_,_,_,_,_,_,_,_)).Times(1).WillOnce(Invoke(
      MockPrepareMgmFSInterface::_STAT_FILE_ON_DISK_AND_TAPE));
  //Attr ls should work for the files that exist
  EXPECT_CALL(mgmOfs, _attr_ls(_,_,_,_,_,_,_)).Times(1)
      .WillRepeatedly(Invoke(
          MockPrepareMgmFSInterface::_ATTR_LS_STAGE_PREPARE_LAMBDA
      ));

  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  std::string requestId = "testReqId";
  PrepareArgumentsWrapper pargs(requestId,Prep_QUERY,oinfos,paths);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo * error = errorWrapper.getError();

  eos::mgm::bulk::PrepareManager pm(std::move(mgmOfsPtr));

  std::unique_ptr<eos::mgm::bulk::QueryPrepareResult> retQueryPrepare = pm.queryPrepare(*(pargs.getPrepareArguments()),*error,client.getClient());
  const auto & response = retQueryPrepare->getResponse();
  ASSERT_EQ(requestId,response->request_id);
  const auto & existingFile = response->responses.front();
  ASSERT_TRUE(existingFile.is_online);
  ASSERT_TRUE(existingFile.is_on_tape);
  ASSERT_TRUE(existingFile.is_exists);
  ASSERT_EQ(paths.front(),existingFile.path);

  const auto & notExistingFile = response->responses.back();

  ASSERT_FALSE(notExistingFile.is_online);
  ASSERT_FALSE(notExistingFile.is_on_tape);
  ASSERT_FALSE(notExistingFile.is_exists);
  ASSERT_EQ("file does not exist or is not accessible to you",notExistingFile.error_text);
  ASSERT_EQ(paths.back(),notExistingFile.path);

  ASSERT_EQ(SFS_DATA,retQueryPrepare->getReturnCode());
}