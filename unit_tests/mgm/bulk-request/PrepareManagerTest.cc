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

#include "mgm/XrdMgmOfs.hh"

#include <XrdVersion.hh>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <mgm/bulk-request/prepare/PrepareManager.hh>
#include "unit_tests/mgm/bulk-request/MockPrepareMgmFSInterface.hh"
#include "xrootd/XrdOuc/XrdOucTList.hh"
#include "auth_plugin/ProtoUtils.hh"
#include "mgm/bulk-request/prepare/PrepareUtils.hh"

/**
 * RAII class to hold the XrdSecEntity client pointer
 * Avoids memory leaks for valgrind
 */
class ClientWrapper {
public:
  ClientWrapper(const std::string &prot, const std::string & name, const std::string & host, const std::string & tident){
    eos::auth::XrdSecEntityProto clientProto;
    clientProto.set_prot(prot);
    clientProto.set_name(name);
    clientProto.set_host(host);
    clientProto.set_tident(tident);
    mClient = eos::auth::utils::GetXrdSecEntity(clientProto);
  }
  ~ClientWrapper(){
      eos::auth::utils::DeleteXrdSecEntity(mClient);
  }
  XrdSecEntity * getClient() { return mClient; }
private:
  XrdSecEntity * mClient;
};

/**
 * RAII class to hold the XrdSfsPrep prepare arguments pointer
 * Avoids memory leaks for valgrind
 */
class PrepareArgumentsWrapper{
public:
  PrepareArgumentsWrapper(const std::string & reqid, const int opts, const std::vector<std::string> & oinfos, const std::vector<std::string> & paths){
    eos::auth::XrdSfsPrepProto pargsProto;
    pargsProto.set_reqid("testReqid");
    pargsProto.set_opts(Prep_STAGE);
    for(auto & oinfo: oinfos){
      pargsProto.add_oinfo(oinfo);
    }
    for(auto & path: paths){
      pargsProto.add_paths(path);
    }
    mPargs = eos::auth::utils::GetXrdSfsPrep(pargsProto);
  }
  ~PrepareArgumentsWrapper(){
    eos::auth::utils::DeleteXrdSfsPrep(mPargs);
  }
  XrdSfsPrep * getPrepareArguments() {
    return mPargs;
  }
private:
  XrdSfsPrep * mPargs;
};

/**
 * RAII class to hold the XrdOucErrInfo object related to errors.
 * Avoids memory leaks for valgrind
 */
class ErrorWrapper{
public:
  ErrorWrapper(const std::string user, const int code, const std::string & message){
    eos::auth::XrdOucErrInfoProto errorProto;
    errorProto.set_user(user);
    errorProto.set_code(code);
    errorProto.set_message(message);
    mError = eos::auth::utils::GetXrdOucErrInfo(errorProto);
  }

  XrdOucErrInfo * getError(){
    return mError;
  }

  ~ErrorWrapper(){
    if(mError != nullptr){
      delete mError;
      mError = nullptr;
    }
  }
private:
  XrdOucErrInfo * mError;
};

class PrepareManagerTest : public ::testing::Test {
protected:

  virtual void SetUp() {
    eos::common::Mapping::Init();
  }

  virtual void TearDown() {
  }

  static ClientWrapper getDefaultClient() {
    return ClientWrapper("krb5","clientName","localhost","clientTident");
  }

  static ErrorWrapper getDefaultError() {
    return ErrorWrapper("",0,"");
  }
};

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

TEST_F(PrepareManagerTest,stagePrepareThreeFilesWorkflow){
  using ::testing::Return;
  using ::testing::_;
  using ::testing::Invoke;

  int nbFiles = 3;
  std::vector<std::string> paths;
  for(int i = 0; i < nbFiles; i++){
    std::stringstream ss;
    ss << "path" << i + 1;
    paths.push_back(ss.str());
  }

  std::vector<std::string> oinfos;
  for(int i = 0; i < nbFiles; i++){
    oinfos.push_back("");
  }

  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId",Prep_STAGE,oinfos,paths);

  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo * error = errorWrapper.getError();

  MockPrepareMgmFSInterface mgmOfs;
  //addStats should be called only two times
  EXPECT_CALL(mgmOfs,addStats).Times(2);
  //isTapeEnabled should not be called as we are in the case where everything is fine
  EXPECT_CALL(mgmOfs,isTapeEnabled).Times(0);
  //As everything is fine, no Emsg should be called
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  //Everything is fine, all the files exist
  ON_CALL(mgmOfs,_exists(_,_,_,_,_)).WillByDefault(Invoke(
    [](const char* path, XrdSfsFileExistence& file_exists, XrdOucErrInfo& error, const XrdSecEntity* client, const char* ininfo){
      file_exists = XrdSfsFileExistIsFile;
      return SFS_OK;
    }
  ));
  //the _exists method should be called 3 times as we have three paths
  EXPECT_CALL(mgmOfs,_exists(_,_,_,_,_)).Times(nbFiles);
  ON_CALL(mgmOfs, _attr_ls(_,_,_,_,_,_,_))
      .WillByDefault(Invoke(
          [](const char* path, XrdOucErrInfo& out_error, const eos::common::VirtualIdentity& vid, const char* opaque, eos::IContainerMD::XAttrMap& map, bool take_lock, bool links)
          {
            map["sys.workflow.sync::prepare"] = "";
            return 0;
          }));
  EXPECT_CALL(mgmOfs, _attr_ls(_,_,_,_,_,_,_)).Times(nbFiles);
  EXPECT_CALL(mgmOfs,Emsg).Times(0);
  EXPECT_CALL(mgmOfs,_access).Times(nbFiles);
  EXPECT_CALL(mgmOfs,FSctl).Times(nbFiles);
  eos::mgm::PrepareManager pm(mgmOfs);
  int retPrepare = pm.prepare(*(pargs.getPrepareArguments()),*error,client.getClient());

  ASSERT_EQ(nbFiles,pm.getBulkRequest()->getPaths().size());
  ASSERT_EQ(SFS_DATA,retPrepare);
}

/*TEST_F(PrepareManagerTest,stagePrepareAllFilesDoNotExist){
  using ::testing::Return;
  using ::testing::_;
  using ::testing::Invoke;

  int nbFiles = 3;
  std::vector<std::string> paths;
  for(int i = 0; i < nbFiles; i++){
    std::stringstream ss;
    ss << "path" << i + 1;
    paths.push_back(ss.str());
  }

  std::vector<std::string> oinfos;
  for(int i = 0; i < nbFiles; i++){
    oinfos.push_back("");
  }

  ClientWrapper client = PrepareManagerTest::getDefaultClient();
  PrepareArgumentsWrapper pargs("testReqId",Prep_STAGE,oinfos,paths);

  ErrorWrapper errorWrapper = PrepareManagerTest::getDefaultError();
  XrdOucErrInfo * error = errorWrapper.getError();
}*/