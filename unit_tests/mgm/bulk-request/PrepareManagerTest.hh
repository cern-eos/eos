//------------------------------------------------------------------------------
//! @file PrepareManagerTest.hh
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

#ifndef EOS_PREPAREMANAGERTEST_HH
#define EOS_PREPAREMANAGERTEST_HH

#include "mgm/XrdMgmOfs.hh"

#include <XrdVersion.hh>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "mgm/bulk-request/prepare/PrepareUtils.hh"
#include "auth_plugin/ProtoUtils.hh"
#include "mgm/bulk-request/utils/PrepareArgumentsWrapper.hh"

using ::testing::Return;
using ::testing::_;
using ::testing::Invoke;
using ::testing::NiceMock;

USE_EOSBULKNAMESPACE

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

/**
 * Test suite class to test the PrepareManager
 */
class PrepareManagerTest : public ::testing::Test {
protected:

  virtual void SetUp() {
    eos::common::Mapping::Init();
  }

  virtual void TearDown() {
    eos::common::Mapping::Reset();
  }

  static ClientWrapper getDefaultClient() {
    return ClientWrapper("krb5","clientName","localhost","clientTident");
  }

  static ErrorWrapper getDefaultError() {
    return ErrorWrapper("",0,"");
  }

  static std::vector<std::string> generateDefaultPaths(const uint64_t nbFiles){
    std::vector<std::string> paths;
    for(uint64_t i = 0; i < nbFiles; ++i){
      std::stringstream ss;
      ss << "path" << i + 1;
      paths.push_back(ss.str());
    }
    return paths;
  }

  static std::vector<std::string> generateEmptyOinfos(const uint64_t nbFiles){
    std::vector<std::string> oinfos;
    for(uint64_t i = 0; i < nbFiles; ++i){
      oinfos.push_back("");
    }
    return oinfos;
  }
};

/**
 * Test suite class to test the BulkRequestPrepareManager
 */
class BulkRequestPrepareManagerTest : public PrepareManagerTest{

};


#endif // EOS_PREPAREMANAGERTEST_HH
