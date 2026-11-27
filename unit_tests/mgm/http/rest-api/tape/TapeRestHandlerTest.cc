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
#include "mgm/bulk-request/prepare/StageBulkRequest.hh"
#include "mgm/bulk-request/response/QueryPrepareResponse.hh"
#include "mgm/http/rest-api/exception/Exceptions.hh"
#include "mgm/http/rest-api/handler/tape/TapeRestHandler.hh"
#include "mgm/http/rest-api/model/tape/stage/GetStageBulkRequestResponseModel.hh"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <memory>

using namespace eos::mgm::rest;
using eos::mgm::rest::test::assertEmptyOkResponse;
using eos::mgm::rest::test::createAuthenticatedVid;
using eos::mgm::rest::test::createHttpRequest;
using eos::mgm::rest::test::createTapeRestApiConfig;
using eos::mgm::rest::test::parseResponseJson;
using ::testing::_;
using ::testing::Return;
using ::testing::Throw;

namespace {
const std::vector<std::string> kPathsWithDotComponents = {"/eos/in/../out/f",
                                                          "/eos/in/./f"};
}

class TapeRestHandlerTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    mConfig = createTapeRestApiConfig();
    mMockBusiness = std::make_shared<MockTapeRestApiBusiness>();
    mHandler = std::make_unique<TapeRestHandler>(mConfig.get(), mMockBusiness);
    mVid = createAuthenticatedVid();
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
  std::unique_ptr<eos::common::HttpResponse> response(
    mHandler->handleRequest(request.get(), &mVid));
  ASSERT_EQ(eos::common::HttpResponse::NOT_FOUND, response->GetResponseCode());
}

TEST_F(TapeRestHandlerTest, handleRequestDispatchesGetStage403ForForbiddenIssuer)
{
  EXPECT_CALL(*mMockBusiness, getStageBulkRequest("req-id", _))
  .WillOnce(Throw(ForbiddenException("You are not allowed to get this bulk-request")));

  auto request = createHttpRequest("GET", "/api/v1/stage/req-id");
  std::unique_ptr<eos::common::HttpResponse> response(
    mHandler->handleRequest(request.get(), &mVid));
  ASSERT_EQ(eos::common::HttpResponse::FORBIDDEN, response->GetResponseCode());
  const Json::Value root = parseResponseJson(response.get());
  ASSERT_EQ("Forbidden", root["title"].asString());
}

TEST_F(TapeRestHandlerTest, handleRequestReturns405ForUnsupportedMethod)
{
  auto request = createHttpRequest("PUT", "/api/v1/stage/");
  std::unique_ptr<eos::common::HttpResponse> response(
    mHandler->handleRequest(request.get(), &mVid));
  ASSERT_EQ(eos::common::HttpResponse::METHOD_NOT_ALLOWED, response->GetResponseCode());
}

namespace {
struct TapeRoute {
  std::string method;
  std::string url;
  std::string body;
};

//! The six routes of the tape REST API, each with a body it accepts
const std::vector<TapeRoute> kTapeRoutes = {
    {"POST", "/api/v1/stage/", R"({"files":[{"path":"/eos/user/file.txt"}]})"},
    {"GET", "/api/v1/stage/req-id", ""},
    {"DELETE", "/api/v1/stage/req-id", ""},
    {"POST", "/api/v1/stage/req-id/cancel", R"({"paths":["/eos/user/file.txt"]})"},
    {"POST", "/api/v1/archiveinfo/", R"({"paths":["/eos/user/file.txt"]})"},
    {"POST", "/api/v1/release/req-id", R"({"paths":["/eos/user/file.txt"]})"},
};

//! Expect no call to the business layer. A call returns valid objects anyway,
//! so that the test fails on the expectation instead of crashing.
void
expectNoBusinessCall(MockTapeRestApiBusiness& business,
                     const eos::common::VirtualIdentity& issuer)
{
  ON_CALL(business, createStageBulkRequest(_, _))
      .WillByDefault(
          Return(std::make_shared<eos::mgm::bulk::StageBulkRequest>("req-id", issuer)));
  ON_CALL(business, getStageBulkRequest(_, _))
      .WillByDefault(Return(std::make_shared<GetStageBulkRequestResponseModel>()));
  ON_CALL(business, getFileInfo(_, _))
      .WillByDefault(Return(std::make_shared<eos::mgm::bulk::QueryPrepareResponse>()));
  EXPECT_CALL(business, createStageBulkRequest(_, _)).Times(0);
  EXPECT_CALL(business, getStageBulkRequest(_, _)).Times(0);
  EXPECT_CALL(business, deleteStageBulkRequest(_, _)).Times(0);
  EXPECT_CALL(business, cancelStageBulkRequest(_, _, _)).Times(0);
  EXPECT_CALL(business, getFileInfo(_, _)).Times(0);
  EXPECT_CALL(business, releasePaths(_, _)).Times(0);
}
} // namespace

TEST_F(TapeRestHandlerTest, handleRequestAnswers401WithoutCredentials)
{
  expectNoBusinessCall(*mMockBusiness, mVid);
  const eos::common::VirtualIdentity nobody;

  for (const auto& route : kTapeRoutes) {
    SCOPED_TRACE(route.method + " " + route.url);
    auto request = createHttpRequest(route.method, route.url, route.body);
    std::unique_ptr<eos::common::HttpResponse> response(
        mHandler->handleRequest(request.get(), &nobody));
    ASSERT_NE(nullptr, response);
    EXPECT_EQ(eos::common::HttpResponse::UNAUTHORIZED, response->GetResponseCode());
    const auto& headers = response->GetHeaders();
    const auto challenge = headers.find("WWW-Authenticate");
    ASSERT_NE(headers.end(), challenge);
    EXPECT_EQ("Bearer", challenge->second);
    EXPECT_EQ("Unauthorized", parseResponseJson(response.get())["title"].asString());
    EXPECT_NE(std::string::npos,
              parseResponseJson(response.get())["detail"].asString().find("nobody"));
  }
}

TEST_F(TapeRestHandlerTest, handleRequestChecksCredentialsBeforeTheBody)
{
  expectNoBusinessCall(*mMockBusiness, mVid);
  const eos::common::VirtualIdentity nobody;
  auto request = createHttpRequest("POST", "/api/v1/stage/", "{ not json");
  std::unique_ptr<eos::common::HttpResponse> response(
      mHandler->handleRequest(request.get(), &nobody));
  ASSERT_EQ(eos::common::HttpResponse::UNAUTHORIZED, response->GetResponseCode());
}

TEST_F(TapeRestHandlerTest, handleRequestDispatchesNobodyWithADeferredToken)
{
  auto responseModel = std::make_shared<GetStageBulkRequestResponseModel>();
  responseModel->setId("req-id");
  responseModel->setCreatedAt(1646305430);
  responseModel->setStartedAt(1646305456);
  EXPECT_CALL(*mMockBusiness, getStageBulkRequest("req-id", _))
      .WillOnce(Return(responseModel));
  // A WLCG token is checked file by file later, so the client is still nobody
  eos::common::VirtualIdentity vid;
  vid.deferredAuth = "Bearer header.payload.signature";
  auto request = createHttpRequest("GET", "/api/v1/stage/req-id");
  std::unique_ptr<eos::common::HttpResponse> response(
      mHandler->handleRequest(request.get(), &vid));
  ASSERT_EQ(eos::common::HttpResponse::OK, response->GetResponseCode());
}

namespace {
const std::string kToken = "header.payload.signature";

//! The query that HttpServer passes on for a request with a token in its URL
const std::vector<std::string> kQueriesWithATokenInTheUrl = {
    "&authz=" + kToken + "&eos.app=http", "&authz=Bearer%20" + kToken + "&eos.app=http"};

void
expectTokenInTheUrlRefused(eos::common::HttpResponse* response)
{
  ASSERT_NE(nullptr, response);
  EXPECT_EQ(eos::common::HttpResponse::BAD_REQUEST, response->GetResponseCode());
  const Json::Value root = parseResponseJson(response);
  EXPECT_EQ("Bad request", root["title"].asString());
  EXPECT_NE(std::string::npos, root["detail"].asString().find("Authorization header"));
}
} // namespace

// The tape REST API takes a token only in the Authorization header. A request
// with a token in its URL gets 400, whatever identity the token gave.
TEST_F(TapeRestHandlerTest, handleRequestAnswers400ToATokenInTheUrl)
{
  expectNoBusinessCall(*mMockBusiness, mVid);
  const eos::common::VirtualIdentity nobody;
  const std::vector<std::pair<std::string, const eos::common::VirtualIdentity*>> vids = {
      {"nobody", &nobody}, {"a user", &mVid}};

  for (const auto& [label, vid] : vids) {
    for (const auto& query : kQueriesWithATokenInTheUrl) {
      for (const auto& route : kTapeRoutes) {
        SCOPED_TRACE(route.method + " " + route.url + " " + query + ", mapped to " +
                     label);
        auto request = createHttpRequest(route.method, route.url, route.body, query);
        std::unique_ptr<eos::common::HttpResponse> response(
            mHandler->handleRequest(request.get(), vid));
        expectTokenInTheUrlRefused(response.get());
      }
    }
  }
}

TEST_F(TapeRestHandlerTest, handleRequestAnswers400ToATokenInTheUrlWithAnEmptyHeader)
{
  expectNoBusinessCall(*mMockBusiness, mVid);

  for (const auto& query : kQueriesWithATokenInTheUrl) {
    SCOPED_TRACE(query);
    auto request = createHttpRequest("GET", "/api/v1/stage/req-id", "", query,
                                     {{"authorization", ""}});
    std::unique_ptr<eos::common::HttpResponse> response(
        mHandler->handleRequest(request.get(), &mVid));
    expectTokenInTheUrlRefused(response.get());
  }
}

// HttpServer passes the token of the Authorization header on as "authz" in the
// query: a WLCG token checked file by file later, or a token that mapped
TEST_F(TapeRestHandlerTest, handleRequestDispatchesATokenInTheAuthorizationHeader)
{
  auto responseModel = std::make_shared<GetStageBulkRequestResponseModel>();
  responseModel->setId("req-id");
  responseModel->setCreatedAt(1646305430);
  responseModel->setStartedAt(1646305456);
  EXPECT_CALL(*mMockBusiness, getStageBulkRequest("req-id", _))
      .Times(2)
      .WillRepeatedly(Return(responseModel));
  eos::common::VirtualIdentity deferred;
  deferred.deferredAuth = "Bearer " + kToken;
  const std::vector<std::pair<std::string, const eos::common::VirtualIdentity*>> vids = {
      {"a deferred token", &deferred}, {"a user", &mVid}};

  for (const auto& [label, vid] : vids) {
    SCOPED_TRACE(label);
    auto request = createHttpRequest("GET", "/api/v1/stage/req-id", "",
                                     "&authz=Bearer%20" + kToken + "&eos.app=http",
                                     {{"authorization", "Bearer " + kToken}});
    std::unique_ptr<eos::common::HttpResponse> response(
        mHandler->handleRequest(request.get(), vid));
    ASSERT_NE(nullptr, response);
    EXPECT_EQ(eos::common::HttpResponse::OK, response->GetResponseCode());
  }
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
  std::unique_ptr<eos::common::HttpResponse> response(
    mHandler->handleRequest(request.get(), &mVid));
  ASSERT_EQ(eos::common::HttpResponse::OK, response->GetResponseCode());
  const Json::Value root = parseResponseJson(response.get());
  ASSERT_EQ("req-id", root["id"].asString());
}

TEST_F(TapeRestHandlerTest, handleRequestDispatchesDeleteStage200Empty)
{
  EXPECT_CALL(*mMockBusiness, deleteStageBulkRequest("req-id", _));

  auto request = createHttpRequest("DELETE", "/api/v1/stage/req-id");
  std::unique_ptr<eos::common::HttpResponse> response(
    mHandler->handleRequest(request.get(), &mVid));
  assertEmptyOkResponse(response.get());
}

TEST_F(TapeRestHandlerTest, handleRequestDispatchesCancelStage200Empty)
{
  EXPECT_CALL(*mMockBusiness, cancelStageBulkRequest("req-id", _, _));

  const std::string body = R"({"paths":["/eos/user/file.txt"]})";
  auto request = createHttpRequest("POST", "/api/v1/stage/req-id/cancel", body);
  std::unique_ptr<eos::common::HttpResponse> response(
    mHandler->handleRequest(request.get(), &mVid));
  assertEmptyOkResponse(response.get());
}

TEST_F(TapeRestHandlerTest, handleRequestDispatchesRelease200Empty)
{
  EXPECT_CALL(*mMockBusiness, releasePaths(_, _));

  const std::string body = R"({"paths":["/eos/user/file.txt"]})";
  auto request = createHttpRequest("POST", "/api/v1/release/req-id", body);
  std::unique_ptr<eos::common::HttpResponse> response(
    mHandler->handleRequest(request.get(), &mVid));
  assertEmptyOkResponse(response.get());
}

// Each route that takes paths refuses a path with a "." or ".." component
// before it reaches the business layer.
TEST_F(TapeRestHandlerTest, handleRequestRefusesDotComponentsInStage)
{
  auto stageRequest = std::make_shared<eos::mgm::bulk::StageBulkRequest>("req-id", mVid);
  ON_CALL(*mMockBusiness, createStageBulkRequest(_, _))
      .WillByDefault(Return(stageRequest));
  EXPECT_CALL(*mMockBusiness, createStageBulkRequest(_, _)).Times(0);

  for (const auto& path : kPathsWithDotComponents) {
    const std::string body = R"({"files":[{"path":")" + path + R"("}]})";
    auto request = createHttpRequest("POST", "/api/v1/stage/", body);
    std::unique_ptr<eos::common::HttpResponse> response(
        mHandler->handleRequest(request.get(), &mVid));
    ASSERT_EQ(eos::common::HttpResponse::BAD_REQUEST, response->GetResponseCode())
        << path;
    ASSERT_EQ("JSON Validation error",
              parseResponseJson(response.get())["title"].asString());
  }
}

TEST_F(TapeRestHandlerTest, handleRequestRefusesDotComponentsInArchiveInfo)
{
  ON_CALL(*mMockBusiness, getFileInfo(_, _))
      .WillByDefault(Return(std::make_shared<eos::mgm::bulk::QueryPrepareResponse>()));
  EXPECT_CALL(*mMockBusiness, getFileInfo(_, _)).Times(0);

  for (const auto& path : kPathsWithDotComponents) {
    const std::string body = R"({"paths":[")" + path + R"("]})";
    auto request = createHttpRequest("POST", "/api/v1/archiveinfo/", body);
    std::unique_ptr<eos::common::HttpResponse> response(
        mHandler->handleRequest(request.get(), &mVid));
    ASSERT_EQ(eos::common::HttpResponse::BAD_REQUEST, response->GetResponseCode())
        << path;
    ASSERT_EQ("JSON Validation error",
              parseResponseJson(response.get())["title"].asString());
  }
}

TEST_F(TapeRestHandlerTest, handleRequestRefusesDotComponentsInRelease)
{
  EXPECT_CALL(*mMockBusiness, releasePaths(_, _)).Times(0);

  for (const auto& path : kPathsWithDotComponents) {
    const std::string body = R"({"paths":[")" + path + R"("]})";
    auto request = createHttpRequest("POST", "/api/v1/release/req-id", body);
    std::unique_ptr<eos::common::HttpResponse> response(
        mHandler->handleRequest(request.get(), &mVid));
    ASSERT_EQ(eos::common::HttpResponse::BAD_REQUEST, response->GetResponseCode())
        << path;
    ASSERT_EQ("JSON Validation error",
              parseResponseJson(response.get())["title"].asString());
  }
}

TEST_F(TapeRestHandlerTest, handleRequestRefusesDotComponentsInCancel)
{
  EXPECT_CALL(*mMockBusiness, cancelStageBulkRequest(_, _, _)).Times(0);

  for (const auto& path : kPathsWithDotComponents) {
    const std::string body = R"({"paths":[")" + path + R"("]})";
    auto request = createHttpRequest("POST", "/api/v1/stage/req-id/cancel", body);
    std::unique_ptr<eos::common::HttpResponse> response(
        mHandler->handleRequest(request.get(), &mVid));
    ASSERT_EQ(eos::common::HttpResponse::BAD_REQUEST, response->GetResponseCode())
        << path;
    ASSERT_EQ("JSON Validation error",
              parseResponseJson(response.get())["title"].asString());
  }
}
