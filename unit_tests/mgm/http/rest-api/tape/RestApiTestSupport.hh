//------------------------------------------------------------------------------
//! @file RestApiTestSupport.hh
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

#ifndef EOS_RESTAPITESTSUPPORT_HH
#define EOS_RESTAPITESTSUPPORT_HH

#include "mgm/http/rest-api/config/tape/TapeRestApiConfig.hh"
#include "common/http/HttpRequest.hh"
#include "common/http/HttpResponse.hh"
#include <json/json.h>
#include <gtest/gtest.h>
#include <memory>
#include <string>

namespace eos::mgm::rest::test {

inline std::unique_ptr<eos::common::HttpRequest> createHttpRequest(
  const std::string& method, const std::string& url, const std::string& body = "")
{
  eos::common::HttpRequest::HeaderMap headers;
  size_t dataSize = body.size();
  eos::common::HttpRequest::HeaderMap cookies;
  return std::make_unique<eos::common::HttpRequest>(headers, method, url, body, "",
      &dataSize, cookies);
}

inline Json::Value parseResponseJson(eos::common::HttpResponse* response)
{
  Json::Value root;
  Json::Reader reader;
  const std::string body = response->GetBody();
  EXPECT_TRUE(reader.parse(body, root)) << reader.getFormattedErrorMessages();
  return root;
}

inline void assertEmptyOkResponse(eos::common::HttpResponse* response)
{
  ASSERT_NE(nullptr, response);
  ASSERT_EQ(eos::common::HttpResponse::OK, response->GetResponseCode());
  EXPECT_TRUE(response->GetBody().empty());
}

inline std::unique_ptr<eos::mgm::rest::TapeRestApiConfig> createTapeRestApiConfig(
  const std::string& accessURL = "/api/")
{
  auto config = std::make_unique<eos::mgm::rest::TapeRestApiConfig>(accessURL);
  config->setSiteName("test-site");
  config->setHostAlias("tape.example.org");
  config->setXrdHttpPort(1234);
  config->setActivated(true);
  config->setTapeEnabled(true);
  config->setStageEnabled(true);
  return config;
}

} // namespace eos::mgm::rest::test

#endif // EOS_RESTAPITESTSUPPORT_HH
