//------------------------------------------------------------------------------
//! @file WellKnownHandlerTest.cc
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

#include "RestApiTestSupport.hh"
#include "mgm/http/rest-api/handler/wellknown/WellKnownHandler.hh"
#include "mgm/http/rest-api/manager/RestApiManager.hh"
#include <gtest/gtest.h>
#include <map>

using namespace eos::mgm::rest;
using eos::mgm::rest::test::createHttpRequest;
using eos::mgm::rest::test::parseResponseJson;

class WellKnownHandlerTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    mManager = std::make_unique<RestApiManager>();
    auto* config = mManager->getTapeRestApiConfig();
    config->setSiteName("cern-prod-tape-atlas");
    config->setHostAlias("tape-api.example.org");
    config->setXrdHttpPort(1234);
    config->setActivated(true);
    config->setTapeEnabled(true);
    mHandler = std::make_unique<WellKnownHandler>(mManager->getWellKnownAccessURL(),
                mManager.get());
    mVid = eos::common::VirtualIdentity();
  }

  std::unique_ptr<RestApiManager> mManager;
  std::unique_ptr<WellKnownHandler> mHandler;
  eos::common::VirtualIdentity mVid;
};

TEST_F(WellKnownHandlerTest, getWellKnownTapeRestApiReturnsDiscoveryDocument)
{
  auto request = createHttpRequest("GET", "/.well-known/wlcg-tape-rest-api");
  std::unique_ptr<common::HttpResponse> response(
    mHandler->handleRequest(request.get(), &mVid));
  ASSERT_EQ(common::HttpResponse::OK, response->GetResponseCode());
  const Json::Value root = parseResponseJson(response.get());
  ASSERT_EQ("cern-prod-tape-atlas", root["sitename"].asString());
  ASSERT_TRUE(root["endpoints"].isArray());
  ASSERT_GE(root["endpoints"].size(), 1u);
  ASSERT_EQ("https://tape-api.example.org:1234/api/v1",
            root["endpoints"][0]["uri"].asString());
  ASSERT_EQ("v1", root["endpoints"][0]["version"].asString());
}

TEST_F(WellKnownHandlerTest, postWellKnownTapeRestApiReturns405)
{
  auto request = createHttpRequest("POST", "/.well-known/wlcg-tape-rest-api");
  std::unique_ptr<common::HttpResponse> response(
    mHandler->handleRequest(request.get(), &mVid));
  ASSERT_EQ(common::HttpResponse::METHOD_NOT_ALLOWED, response->GetResponseCode());
  const Json::Value root = parseResponseJson(response.get());
  ASSERT_EQ("Method not allowed", root["title"].asString());
}

TEST_F(WellKnownHandlerTest, getWellKnownAcceptsGetWithoutRequestBody)
{
  auto request = createHttpRequest("GET", "/.well-known/wlcg-tape-rest-api");
  std::unique_ptr<common::HttpResponse> response(
    mHandler->handleRequest(request.get(), &mVid));
  ASSERT_EQ(common::HttpResponse::OK, response->GetResponseCode());
  EXPECT_FALSE(response->GetBody().empty());
}

TEST_F(WellKnownHandlerTest, getWellKnownIgnoresRequestBodyOnGet)
{
  auto request = createHttpRequest("GET", "/.well-known/wlcg-tape-rest-api",
                                   R"({"unexpected":"payload"})");
  std::unique_ptr<common::HttpResponse> response(
    mHandler->handleRequest(request.get(), &mVid));
  ASSERT_EQ(common::HttpResponse::OK, response->GetResponseCode());
  const Json::Value root = parseResponseJson(response.get());
  ASSERT_EQ("cern-prod-tape-atlas", root["sitename"].asString());
}

TEST_F(WellKnownHandlerTest, getWellKnownReflectsConfigEndpointMapping)
{
  std::map<std::string, std::string> endpointMap = {
    {"v1", "https://custom.example.org:9999/api/v1"}
  };
  mManager->getTapeRestApiConfig()->setEndpointToUrlMapping(endpointMap);
  mHandler = std::make_unique<WellKnownHandler>(mManager->getWellKnownAccessURL(),
                mManager.get());

  auto request = createHttpRequest("GET", "/.well-known/wlcg-tape-rest-api");
  std::unique_ptr<common::HttpResponse> response(
    mHandler->handleRequest(request.get(), &mVid));
  const Json::Value root = parseResponseJson(response.get());
  ASSERT_EQ("https://custom.example.org:9999/api/v1",
            root["endpoints"][0]["uri"].asString());
}

TEST_F(WellKnownHandlerTest, getWellKnownReturns500WhenTapeApiDisabled)
{
  mManager->getTapeRestApiConfig()->setTapeEnabled(false);
  mHandler = std::make_unique<WellKnownHandler>(mManager->getWellKnownAccessURL(),
                mManager.get());

  auto request = createHttpRequest("GET", "/.well-known/wlcg-tape-rest-api");
  std::unique_ptr<common::HttpResponse> response(
    mHandler->handleRequest(request.get(), &mVid));
  ASSERT_EQ(common::HttpResponse::INTERNAL_SERVER_ERROR, response->GetResponseCode());
  const Json::Value root = parseResponseJson(response.get());
  ASSERT_EQ("Internal server error", root["title"].asString());
  EXPECT_NE(std::string::npos, root["detail"].asString().find("tapeenabled"));
}
