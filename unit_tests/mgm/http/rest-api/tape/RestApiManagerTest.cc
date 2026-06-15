//------------------------------------------------------------------------------
//! @file RestApiManagerTest.cc
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

#include "mgm/http/rest-api/handler/tape/TapeRestHandler.hh"
#include "mgm/http/rest-api/handler/wellknown/WellKnownHandler.hh"
#include "mgm/http/rest-api/manager/RestApiManager.hh"
#include <gtest/gtest.h>
#include <memory>

using namespace eos::mgm::rest;

class RestApiManagerTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    mManager = std::make_unique<RestApiManager>();
    auto* config = mManager->getTapeRestApiConfig();
    config->setSiteName("test-site");
    config->setHostAlias("tape.example.org");
    config->setXrdHttpPort(1234);
    config->setActivated(true);
    config->setTapeEnabled(true);
  }

  std::unique_ptr<RestApiManager> mManager;
};

TEST_F(RestApiManagerTest, getRestHandlerReturnsTapeHandlerForApiUrl)
{
  auto handler = mManager->getRestHandler("/api/v1/stage/");
  ASSERT_NE(nullptr, handler);
  EXPECT_NE(nullptr, dynamic_cast<TapeRestHandler*>(handler.get()));
}

TEST_F(RestApiManagerTest, getRestHandlerReturnsWellKnownHandler)
{
  auto handler = mManager->getRestHandler("/.well-known/wlcg-tape-rest-api");
  ASSERT_NE(nullptr, handler);
  EXPECT_NE(nullptr, dynamic_cast<WellKnownHandler*>(handler.get()));
}

TEST_F(RestApiManagerTest, getRestHandlerReturnsNullptrForUnknownUrl)
{
  EXPECT_EQ(nullptr, mManager->getRestHandler("/unknown/path/"));
}

TEST_F(RestApiManagerTest, isRestRequestReturnsTrueForConfiguredTapeApi)
{
  EXPECT_TRUE(mManager->isRestRequest("/api/v1/stage/"));
}

TEST_F(RestApiManagerTest, isRestRequestReturnsFalseWhenTapeApiDisabled)
{
  mManager->getTapeRestApiConfig()->setActivated(false);
  EXPECT_FALSE(mManager->isRestRequest("/api/v1/stage/"));
}

TEST_F(RestApiManagerTest, isRestRequestReturnsFalseForUnknownUrl)
{
  EXPECT_FALSE(mManager->isRestRequest("/unknown/path/"));
}

TEST_F(RestApiManagerTest, getWellKnownAccessURLMatchesExpectedPrefix)
{
  EXPECT_EQ("/.well-known/", mManager->getWellKnownAccessURL());
}
