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
#include "mgm/bulk-request/prepare/BulkRequestPrepareManager.hh"

TEST_F(BulkRequestPrepareManagerTest,stagePrepareFilesWorkflow){
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(nbFiles);

  MockPrepareMgmFSInterface mgmOfs;
  //addStats should be called only two times
  EXPECT_CALL(mgmOfs,addStats).Times(2);
  //isTapeEnabled should not be called as we are in the case where everything is fine
  EXPECT_CALL(mgmOfs,isTapeEnabled).Times(0);
  //As everything is fine, no Emsg should be called
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  //Everything is fine, all the files exist
  ON_CALL(mgmOfs,_exists(_,_,_,_,_)).WillByDefault(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_FILE_EXISTS_LAMBDA));
  //the _exists method should be called for all files
  EXPECT_CALL(mgmOfs,_exists(_,_,_,_,_)).Times(nbFiles);
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

  eos::mgm::bulk::BulkRequestPrepareManager pm(mgmOfs);
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()),*error,client.getClient());

  ASSERT_EQ(nbFiles,pm.getBulkRequest()->getPaths().size());
  ASSERT_EQ(SFS_DATA,retPrepare);
}

TEST_F(BulkRequestPrepareManagerTest,stagePrepareFileWithNoPath){
  //prepare stage should be idempotent https://its.cern.ch/jira/projects/EOS/issues/EOS-4739
  NiceMock<MockPrepareMgmFSInterface> mgmOfs;
  //No path exist, but Emsg should not be called
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  //No path are set, no mgmOfs method should be called
  EXPECT_CALL(mgmOfs,_exists(_,_,_,_,_)).Times(0);
  EXPECT_CALL(mgmOfs, _attr_ls(_,_,_,_,_,_,_)).Times(0);
  EXPECT_CALL(mgmOfs,_access).Times(0);
  EXPECT_CALL(mgmOfs,FSctl).Times(0);

  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId",Prep_STAGE, {""},{""});
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo * error = errorWrapper.getError();

  eos::mgm::bulk::BulkRequestPrepareManager pm(mgmOfs);
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()),*error,client.getClient());

  //The bulk-request is created, 0 files are supposed to be there
  ASSERT_EQ(0,pm.getBulkRequest()->getPaths().size());
  //The prepare manager returns SFS_DATA
  ASSERT_EQ(SFS_DATA,retPrepare);
}

TEST_F(BulkRequestPrepareManagerTest,stagePrepareAllFilesDoNotExist){
  /**
   * If all files do not exist, the prepare should succeed
   * prepare is now idempotent (https://its.cern.ch/jira/projects/EOS/issues/EOS-4739)
   */
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(nbFiles);

  NiceMock<MockPrepareMgmFSInterface> mgmOfs;
  //One file does not exist, Emsg should be called once
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  ON_CALL(mgmOfs,_exists(_,_,_,_,_)).WillByDefault(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_FILE_DOES_NOT_EXIST_LAMBDA));
  //The current behaviour is that the prepare logic returns an error if at least one file does not exist.
  EXPECT_CALL(mgmOfs,_exists(_,_,_,_,_)).Times(3);
  EXPECT_CALL(mgmOfs, _attr_ls(_,_,_,_,_,_,_)).Times(0);
  EXPECT_CALL(mgmOfs,_access).Times(0);
  EXPECT_CALL(mgmOfs,FSctl).Times(0);

  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId",Prep_STAGE,oinfos,paths);
  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo * error = errorWrapper.getError();

  eos::mgm::bulk::BulkRequestPrepareManager pm(mgmOfs);
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()),*error,client.getClient());

  //For the future, even if the files do not exist, they have to be in the bulk-request.
  ASSERT_EQ(3,pm.getBulkRequest()->getPaths().size());
  ASSERT_EQ(SFS_DATA,retPrepare);
}

TEST_F(BulkRequestPrepareManagerTest,stagePrepareOneFileDoNotExistReturnsSfsData){
  /**
   * If all files do not exist, the prepare should succeed
   * Prepare is now idempotent (https://its.cern.ch/jira/projects/EOS/issues/EOS-4739)
   */
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(nbFiles);

  NiceMock<MockPrepareMgmFSInterface> mgmOfs;
  //isTapeEnabled should not be called
  EXPECT_CALL(mgmOfs,isTapeEnabled).Times(0);
  //One file does not exist, Emsg should be called once
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  //Exist will first return true for the existing file, then return false,
  EXPECT_CALL(mgmOfs,_exists(_,_,_,_,_)).Times(nbFiles).WillOnce(
      Invoke(MockPrepareMgmFSInterface::_EXISTS_FILE_EXISTS_LAMBDA)
  ).WillOnce(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_FILE_DOES_NOT_EXIST_LAMBDA)
  ).WillRepeatedly(
      Invoke(MockPrepareMgmFSInterface::_EXISTS_FILE_EXISTS_LAMBDA)
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

  eos::mgm::bulk::BulkRequestPrepareManager pm(mgmOfs);

  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()),*error,client.getClient());

  //The existing files are in the bulk-request
  std::set<std::string> bulkReqPaths = pm.getBulkRequest()->getPaths();
  ASSERT_EQ(nbFiles,bulkReqPaths.size());
  auto bulkReqPathsItor = bulkReqPaths.begin();
  int i = 0;
  while(bulkReqPathsItor != bulkReqPaths.end()){
    //All the files should be in the bulk-request, even the one that does not exist
    ASSERT_EQ(paths.at(i),*bulkReqPathsItor);
    i++;
    bulkReqPathsItor++;
  }
  //We failed the second file, the prepare is a success
  ASSERT_EQ(SFS_DATA,retPrepare);
}

TEST_F(BulkRequestPrepareManagerTest,abortPrepareFilesWorkflow){
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(nbFiles);

  MockPrepareMgmFSInterface mgmOfs;
  //addStats should be called only two times
  EXPECT_CALL(mgmOfs,addStats).Times(2);
  //isTapeEnabled should not be called as we are in the case where everything is fine
  EXPECT_CALL(mgmOfs,isTapeEnabled).Times(0);
  //As everything is fine, no Emsg should be called
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  //Everything is fine, all the files exist
  ON_CALL(mgmOfs,_exists(_,_,_,_,_)).WillByDefault(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_FILE_EXISTS_LAMBDA));
  //the _exists method should be called for all files
  EXPECT_CALL(mgmOfs,_exists(_,_,_,_,_)).Times(nbFiles);
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

  eos::mgm::bulk::BulkRequestPrepareManager pm(mgmOfs);
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()),*error,client.getClient());
  //Abort prepare does not generate a bulk-request, so the bulk-request should be equal to nullptr
  ASSERT_EQ(nullptr,pm.getBulkRequest());
  //Abort prepare returns SFS_OK
  ASSERT_EQ(SFS_OK,retPrepare);
}

TEST_F(BulkRequestPrepareManagerTest,abortPrepareOneFileDoesNotExist){
  /**
   * If one file does not exist, the prepare abort should succeed
   * Prepare is now idempotent (https://its.cern.ch/jira/projects/EOS/issues/EOS-4739)
   */
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(nbFiles);

  NiceMock<MockPrepareMgmFSInterface> mgmOfs;
  //isTapeEnabled should not be called
  EXPECT_CALL(mgmOfs,isTapeEnabled).Times(0);
  //One file does not exist, but as we are idempotent, no error should be returned
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  //Exist will first return true for the existing file, then return false
  EXPECT_CALL(mgmOfs,_exists(_,_,_,_,_)).Times(nbFiles).WillOnce(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_FILE_EXISTS_LAMBDA)).WillRepeatedly(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_FILE_DOES_NOT_EXIST_LAMBDA));
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

  eos::mgm::bulk::BulkRequestPrepareManager pm(mgmOfs);

  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()),*error,client.getClient());
  ASSERT_EQ(SFS_OK,retPrepare);
}

TEST_F(BulkRequestPrepareManagerTest,evictPrepareFilesWorkflow){
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(nbFiles);

  MockPrepareMgmFSInterface mgmOfs;
  //addStats should be called only two times
  EXPECT_CALL(mgmOfs,addStats).Times(2);
  //isTapeEnabled should not be called as we are in the case where everything is fine
  EXPECT_CALL(mgmOfs,isTapeEnabled).Times(0);
  //As everything is fine, no Emsg should be called
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  //Everything is fine, all the files exist
  ON_CALL(mgmOfs,_exists(_,_,_,_,_)).WillByDefault(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_FILE_EXISTS_LAMBDA));
  //the _exists method should be called for all files
  EXPECT_CALL(mgmOfs,_exists(_,_,_,_,_)).Times(nbFiles);
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

  eos::mgm::bulk::BulkRequestPrepareManager pm(mgmOfs);
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()),*error,client.getClient());
  //Evict prepare does not generate a bulk-request, so the bulk-request should be equal to nullptr
  ASSERT_EQ(nullptr,pm.getBulkRequest());
  //Evict prepare returns SFS_OK
  ASSERT_EQ(SFS_OK,retPrepare);
}

TEST_F(BulkRequestPrepareManagerTest,evictPrepareOneFileDoesNotExist){
  /**
   * If one file does not exist, the prepare evict should succeed
   * Prepare is now idempotent (https://its.cern.ch/jira/projects/EOS/issues/EOS-4739)
   */
  int nbFiles = 3;
  std::vector<std::string> paths = PrepareManagerTest::generateDefaultPaths(nbFiles);
  std::vector<std::string> oinfos = PrepareManagerTest::generateEmptyOinfos(nbFiles);

  NiceMock<MockPrepareMgmFSInterface> mgmOfs;
  //isTapeEnabled should not be called
  EXPECT_CALL(mgmOfs,isTapeEnabled).Times(0);
  //One file does not exist, Emsg should not be called as we are idempotent
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  //Exist will first return true for the existing file, then return false
  EXPECT_CALL(mgmOfs,_exists(_,_,_,_,_)).Times(nbFiles).WillOnce(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_FILE_EXISTS_LAMBDA)).WillRepeatedly(Invoke(
      MockPrepareMgmFSInterface::_EXISTS_FILE_DOES_NOT_EXIST_LAMBDA));
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

  eos::mgm::bulk::BulkRequestPrepareManager pm(mgmOfs);

  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()),*error,client.getClient());
  ASSERT_EQ(SFS_OK,retPrepare);
}