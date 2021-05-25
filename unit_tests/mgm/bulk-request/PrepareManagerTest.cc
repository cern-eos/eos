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
#include <xrootd/XrdOuc/XrdOucErrInfo.hh>
#include <xrootd/XrdSfs/XrdSfsInterface.hh>
#include <xrootd/XrdSec/XrdSecEntity.hh>
#include <gtest/gtest.h>
#include <mgm/bulk-request/prepare/PrepareManager.hh>
#include "unit_tests/mgm/bulk-request/MockPrepareMgmFSInterface.hh"
#include "xrootd/XrdOuc/XrdOucTList.hh"
#include "common/utils/XrdUtils.hh"
#include "auth_plugin/ProtoUtils.hh"

class PrepareManagerTest : public ::testing::Test {
protected:

  virtual void SetUp() {
    eos::common::Mapping::Init();
  }

  virtual void TearDown() {
  }

  static XrdOucTList * generateListOfPaths(){
    return new XrdOucTList();
  }
};

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(PrepareManagerTest, prepareOptsToString)
{
  using namespace eos::mgm;

  {
    const int opts = Prep_PRTY0;
    ASSERT_EQ("PRTY0", XrdMgmOfs::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_PRTY1;
    ASSERT_EQ("PRTY1", XrdMgmOfs::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_PRTY2;
    ASSERT_EQ("PRTY2", XrdMgmOfs::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_PRTY3;
    ASSERT_EQ("PRTY3", XrdMgmOfs::prepareOptsToString(opts));
  }

  {
    const int opts = Prep_SENDAOK;
    ASSERT_EQ("PRTY0,SENDAOK", XrdMgmOfs::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_SENDERR;
    ASSERT_EQ("PRTY0,SENDERR", XrdMgmOfs::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_SENDACK;
    ASSERT_EQ("PRTY0,SENDACK", XrdMgmOfs::prepareOptsToString(opts));
  }

  {
    const int opts = Prep_WMODE;
    ASSERT_EQ("PRTY0,WMODE", XrdMgmOfs::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_STAGE;
    ASSERT_EQ("PRTY0,STAGE", XrdMgmOfs::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_COLOC;
    ASSERT_EQ("PRTY0,COLOC", XrdMgmOfs::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_FRESH;
    ASSERT_EQ("PRTY0,FRESH", XrdMgmOfs::prepareOptsToString(opts));
  }
#if (XrdMajorVNUM(XrdVNUMBER) == 4 && XrdMinorVNUM(XrdVNUMBER) >= 10) || XrdMajorVNUM(XrdVNUMBER) >= 5
  {
    const int opts = Prep_CANCEL;
    ASSERT_EQ("PRTY0,CANCEL", XrdMgmOfs::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_QUERY;
    ASSERT_EQ("PRTY0,QUERY", XrdMgmOfs::prepareOptsToString(opts));
  }
  {
    const int opts = Prep_EVICT;
    ASSERT_EQ("PRTY0,EVICT", XrdMgmOfs::prepareOptsToString(opts));
  }
#endif
}

TEST_F(PrepareManagerTest,prepareStageWorkflow){
  eos::auth::XrdSecEntityProto clientProto;
  clientProto.set_prot("krb5");
  clientProto.set_name("clientName");
  clientProto.set_host("localhost");

  clientProto.set_tident("clientTident");

  XrdSecEntity * client = eos::auth::utils::GetXrdSecEntity(clientProto);

  eos::auth::XrdSfsPrepProto pargsProto;
  pargsProto.add_paths("testPath");
  pargsProto.set_reqid("testReqid");
  pargsProto.set_opts(32);
  pargsProto.add_oinfo("");

  XrdSfsPrep * pargs = eos::auth::utils::GetXrdSfsPrep(pargsProto);

  eos::auth::XrdOucErrInfoProto errorProto;
  XrdOucErrInfo * error = eos::auth::utils::GetXrdOucErrInfo(errorProto);

  MockPrepareMgmFSInterface mgmOfs;
  eos::mgm::PrepareManager pm(mgmOfs);
  pm.prepare(*pargs,*error,client);

  eos::auth::utils::DeleteXrdSecEntity(client);
  //TODO: do the eos::auth::utils::DeleteXrdSfsPrep(XrdSfsPrep *& pargs);

  /*XrdSfsPrep pargs;
  pargs.paths = new XrdOucTList("Test");
  pargs.oinfo = nullptr;
  std::string reqid = "coucou";
  pargs.reqid = strdup(reqid.c_str());
  pargs.opts = 32;
  std::string clientHost = "localhost";
  std::string clientName = "clientName";

  ASSERT_EQ(1,eos::common::XrdUtils::countNbElementsInXrdOucTList(pargs.paths));
  delete pargs.paths;*/
  //XrdOucErrInfo error;
  //XrdSecEntity client;
  //pm.prepare(pargs,error,&client);
}