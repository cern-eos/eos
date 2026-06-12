//------------------------------------------------------------------------------
//! @file TapeActionsTest.cc
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
#include "mgm/bulk-request/prepare/StageBulkRequest.hh"
#include "mgm/bulk-request/response/QueryPrepareResponse.hh"
#include "mgm/http/rest-api/action/tape/TapeActions.hh"
#include "mgm/http/rest-api/exception/Exceptions.hh"
#include "mgm/http/rest-api/handler/tape/TapeRestHandler.hh"
#include "mgm/http/rest-api/json/tape/TapeJsonifiers.hh"
#include "mgm/http/rest-api/json/tape/TapeModelBuilders.hh"
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

class TapeActionsTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    mConfig = createTapeRestApiConfig();
    mTapeRestHandler = std::make_unique<TapeRestHandler>(mConfig.get());
    mMockBusiness = std::make_shared<MockTapeRestApiBusiness>();
    mVid = eos::common::VirtualIdentity();
  }

  std::unique_ptr<TapeRestApiConfig> mConfig;
  std::unique_ptr<TapeRestHandler> mTapeRestHandler;
  std::shared_ptr<MockTapeRestApiBusiness> mMockBusiness;
  eos::common::VirtualIdentity mVid;
};

TEST_F(TapeActionsTest, createStageReturns201WithLocationHeader)
{
  const std::string requestId = "11111111-2222-3333-4444-555555555555";
  eos::common::VirtualIdentity issuer;
  issuer.uid = 1000;
  auto stageRequest = std::make_shared<eos::mgm::bulk::StageBulkRequest>(requestId,
                      issuer);
  EXPECT_CALL(*mMockBusiness, createStageBulkRequest(_, _))
  .WillOnce(Return(stageRequest));

  CreateStageBulkRequest action("/api/v1/stage/", common::HttpHandler::Methods::POST,
                                mMockBusiness,
                                std::make_shared<CreateStageRequestModelBuilder>("test-site"),
                                std::make_shared<CreatedStageBulkRequestJsonifier>(),
                                mTapeRestHandler.get());
  const std::string body = R"({"files":[{"path":"/eos/user/file.txt"}]})";
  auto request = createHttpRequest("POST", "/api/v1/stage/", body);
  std::unique_ptr<common::HttpResponse> response(action.run(request.get(), &mVid));
  ASSERT_EQ(common::HttpResponse::CREATED, response->GetResponseCode());
  const auto& headers = response->GetHeaders();
  ASSERT_EQ(1u, headers.count("Location"));
  EXPECT_EQ("https://tape.example.org:1234/api/v1/stage/" + requestId,
            headers.at("Location"));
  const Json::Value root = parseResponseJson(response.get());
  ASSERT_EQ(requestId, root["requestId"].asString());
}

TEST_F(TapeActionsTest, getStageReturns404WhenRequestMissing)
{
  EXPECT_CALL(*mMockBusiness, getStageBulkRequest("missing-id", _))
  .WillOnce(Throw(ObjectNotFoundException("not found")));

  GetStageBulkRequest action("/api/v1/stage/{id}", common::HttpHandler::Methods::GET,
                             mMockBusiness, std::make_shared<GetStageBulkRequestJsonifier>());
  auto request = createHttpRequest("GET", "/api/v1/stage/missing-id");
  std::unique_ptr<common::HttpResponse> response(action.run(request.get(), &mVid));
  ASSERT_EQ(common::HttpResponse::NOT_FOUND, response->GetResponseCode());
}

TEST_F(TapeActionsTest, cancelStageReturns400WhenFileMissingFromRequest)
{
  EXPECT_CALL(*mMockBusiness, cancelStageBulkRequest("req-id", _, _))
  .WillOnce(Throw(FileDoesNotBelongToBulkRequestException("file not in request")));

  CancelStageBulkRequest action("/api/v1/stage/{id}/cancel",
                                common::HttpHandler::Methods::POST, mMockBusiness,
                                std::make_shared<PathsModelBuilder>());
  const std::string body = R"({"paths":["/eos/user/other.txt"]})";
  auto request = createHttpRequest("POST", "/api/v1/stage/req-id/cancel", body);
  std::unique_ptr<common::HttpResponse> response(action.run(request.get(), &mVid));
  ASSERT_EQ(common::HttpResponse::BAD_REQUEST, response->GetResponseCode());
  const Json::Value root = parseResponseJson(response.get());
  ASSERT_EQ("File missing from stage request", root["title"].asString());
}

TEST_F(TapeActionsTest, deleteStageReturns200WithEmptyBody)
{
  EXPECT_CALL(*mMockBusiness, deleteStageBulkRequest("req-id", _));

  DeleteStageBulkRequest action("/api/v1/stage/{id}",
                                common::HttpHandler::Methods::DELETE, mMockBusiness);
  auto request = createHttpRequest("DELETE", "/api/v1/stage/req-id");
  std::unique_ptr<common::HttpResponse> response(action.run(request.get(), &mVid));
  ASSERT_EQ(common::HttpResponse::OK, response->GetResponseCode());
  EXPECT_TRUE(response->GetBody().empty());
}

TEST_F(TapeActionsTest, releaseReturns200WithEmptyBody)
{
  EXPECT_CALL(*mMockBusiness, releasePaths(_, _));

  CreateReleaseBulkRequest action("/api/v1/stage/{id}/release",
                                  common::HttpHandler::Methods::POST, mMockBusiness,
                                  std::make_shared<PathsModelBuilder>());
  const std::string body = R"({"paths":["/eos/user/file.txt"]})";
  auto request = createHttpRequest("POST", "/api/v1/stage/req-id/release", body);
  std::unique_ptr<common::HttpResponse> response(action.run(request.get(), &mVid));
  ASSERT_EQ(common::HttpResponse::OK, response->GetResponseCode());
  EXPECT_TRUE(response->GetBody().empty());
}

TEST_F(TapeActionsTest, getArchiveInfoReturns200WithArrayBody)
{
  auto queryResponse = std::make_shared<eos::mgm::bulk::QueryPrepareResponse>();
  eos::mgm::bulk::QueryPrepareFileResponse file("/eos/user/file.txt");
  file.is_exists = true;
  file.is_online = true;
  file.is_on_tape = true;
  queryResponse->responses.push_back(file);
  EXPECT_CALL(*mMockBusiness, getFileInfo(_, _)).WillOnce(Return(queryResponse));

  GetArchiveInfo action("/api/v1/archiveinfo/", common::HttpHandler::Methods::POST,
                      mMockBusiness, std::make_shared<PathsModelBuilder>(),
                      std::make_shared<GetArchiveInfoResponseJsonifier>());
  const std::string body = R"({"paths":["/eos/user/file.txt"]})";
  auto request = createHttpRequest("POST", "/api/v1/archiveinfo/", body);
  std::unique_ptr<common::HttpResponse> response(action.run(request.get(), &mVid));
  ASSERT_EQ(common::HttpResponse::OK, response->GetResponseCode());
  const Json::Value root = parseResponseJson(response.get());
  ASSERT_TRUE(root.isArray());
  ASSERT_EQ(1u, root.size());
  ASSERT_EQ("/eos/user/file.txt", root[0]["path"].asString());
  ASSERT_EQ("DISK_AND_TAPE", root[0]["locality"].asString());
}
