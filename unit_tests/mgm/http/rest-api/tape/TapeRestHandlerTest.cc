//------------------------------------------------------------------------------
//! @file TapeRestHandlerTest.cc
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

#include "MockTapeRestApiBusiness.hh"
#include "RestApiTestSupport.hh"
#include "mgm/http/rest-api/exception/Exceptions.hh"
#include "mgm/http/rest-api/handler/tape/TapeRestHandler.hh"
#include "mgm/http/rest-api/model/tape/stage/GetStageBulkRequestResponseModel.hh"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <memory>

using namespace eos::mgm::rest;
using eos::mgm::rest::test::createHttpRequest;
using eos::mgm::rest::test::createTapeRestApiConfig;
using eos::mgm::rest::test::parseResponseJson;
using ::testing::_;
using ::testing::Return;
using ::testing::Throw;

class TapeRestHandlerTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    mConfig = createTapeRestApiConfig();
    mMockBusiness = std::make_shared<MockTapeRestApiBusiness>();
    mHandler = std::make_unique<TapeRestHandler>(mConfig.get(), mMockBusiness);
    mVid = eos::common::VirtualIdentity();
  }

  std::unique_ptr<TapeRestApiConfig> mConfig;
  std::shared_ptr<MockTapeRestApiBusiness> mMockBusiness;
  std::unique_ptr<TapeRestHandler> mHandler;
  eos::common::VirtualIdentity mVid;
};

TEST_F(TapeRestHandlerTest, isRestRequestReturnsTrueWhenFullyConfigured)
{
  std::string errorMsg;
  EXPECT_TRUE(mHandler->isRestRequest("/api/v1/stage/", errorMsg));
  EXPECT_TRUE(errorMsg.empty());
}

TEST_F(TapeRestHandlerTest, isRestRequestReturnsFalseWhenNotActivated)
{
  mConfig->setActivated(false);
  std::string errorMsg;
  EXPECT_FALSE(mHandler->isRestRequest("/api/v1/stage/", errorMsg));
  EXPECT_FALSE(errorMsg.empty());
}

TEST_F(TapeRestHandlerTest, isRestRequestReturnsFalseWhenSiteNameMissing)
{
  mConfig->setSiteName("");
  std::string errorMsg;
  EXPECT_FALSE(mHandler->isRestRequest("/api/v1/stage/", errorMsg));
  EXPECT_NE(std::string::npos, errorMsg.find("sitename"));
}

TEST_F(TapeRestHandlerTest, isRestRequestReturnsFalseWhenTapeDisabled)
{
  mConfig->setTapeEnabled(false);
  std::string errorMsg;
  EXPECT_FALSE(mHandler->isRestRequest("/api/v1/stage/", errorMsg));
  EXPECT_NE(std::string::npos, errorMsg.find("tapeenabled"));
}

TEST_F(TapeRestHandlerTest, handleRequestDispatchesGetStage404)
{
  EXPECT_CALL(*mMockBusiness, getStageBulkRequest("missing-id", _))
  .WillOnce(Throw(ObjectNotFoundException("not found")));

  auto request = createHttpRequest("GET", "/api/v1/stage/missing-id");
  std::unique_ptr<common::HttpResponse> response(
    mHandler->handleRequest(request.get(), &mVid));
  ASSERT_EQ(common::HttpResponse::NOT_FOUND, response->GetResponseCode());
}

TEST_F(TapeRestHandlerTest, handleRequestDispatchesGetStage403ForForbiddenIssuer)
{
  EXPECT_CALL(*mMockBusiness, getStageBulkRequest("req-id", _))
  .WillOnce(Throw(ForbiddenException("You are not allowed to get this bulk-request")));

  auto request = createHttpRequest("GET", "/api/v1/stage/req-id");
  std::unique_ptr<common::HttpResponse> response(
    mHandler->handleRequest(request.get(), &mVid));
  ASSERT_EQ(common::HttpResponse::FORBIDDEN, response->GetResponseCode());
  const Json::Value root = parseResponseJson(response.get());
  ASSERT_EQ("Forbidden", root["title"].asString());
}

TEST_F(TapeRestHandlerTest, handleRequestReturns405ForUnsupportedMethod)
{
  auto request = createHttpRequest("PUT", "/api/v1/stage/");
  std::unique_ptr<common::HttpResponse> response(
    mHandler->handleRequest(request.get(), &mVid));
  ASSERT_EQ(common::HttpResponse::METHOD_NOT_ALLOWED, response->GetResponseCode());
}

TEST_F(TapeRestHandlerTest, handleRequestDispatchesGetStage200)
{
  auto responseModel = std::make_shared<GetStageBulkRequestResponseModel>();
  responseModel->setId("req-id");
  responseModel->setCreatedAt(1646305430);
  responseModel->setStartedAt(1646305456);
  EXPECT_CALL(*mMockBusiness, getStageBulkRequest("req-id", _))
  .WillOnce(Return(responseModel));

  auto request = createHttpRequest("GET", "/api/v1/stage/req-id");
  std::unique_ptr<common::HttpResponse> response(
    mHandler->handleRequest(request.get(), &mVid));
  ASSERT_EQ(common::HttpResponse::OK, response->GetResponseCode());
  const Json::Value root = parseResponseJson(response.get());
  ASSERT_EQ("req-id", root["id"].asString());
}
