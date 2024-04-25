//------------------------------------------------------------------------------
// File: HttpServerTests.cc
// Author: Elvin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#include "gtest/gtest.h"
#include <XrdOuc/XrdOucEnv.hh>
#define IN_TEST_HARNESS
#include "mgm/http/HttpServer.hh"
#undef IN_TEST_HARNESS

//------------------------------------------------------------------------------
// Test parsing for HTTP requests where path might contain opaque data which
// represents the authorization token or the HTTP headers include this info
//------------------------------------------------------------------------------
TEST(HttpServer, ParsePathAndToken)
{
  using eos::mgm::HttpServer;
  std::string path;
  std::unique_ptr<XrdOucEnv> env_opaque;
  std::map<std::string, std::string> norm_hdrs;
  ASSERT_FALSE(HttpServer::BuildPathAndEnvOpaque(norm_hdrs, path, env_opaque));
  ASSERT_TRUE(path.empty());
  ASSERT_EQ(nullptr, env_opaque);
  norm_hdrs = {{"dummy", "test"}};
  ASSERT_FALSE(HttpServer::BuildPathAndEnvOpaque(norm_hdrs, path, env_opaque));
  ASSERT_TRUE(path.empty());
  ASSERT_EQ(nullptr, env_opaque);
  norm_hdrs = {{"xrd-http-fullresource", "/eos/dev/file.dat"}};
  ASSERT_TRUE(HttpServer::BuildPathAndEnvOpaque(norm_hdrs, path, env_opaque));
  ASSERT_FALSE(path.empty());
  ASSERT_TRUE(env_opaque != nullptr);
  ASSERT_EQ(nullptr, env_opaque->Get("authz"));
  // Authorization appeneded as opaque information
  norm_hdrs = {{"xrd-http-fullresource", "/eos/dev/file1.dat?authz=deadbeef"}};
  ASSERT_TRUE(HttpServer::BuildPathAndEnvOpaque(norm_hdrs, path, env_opaque));
  EXPECT_EQ("/eos/dev/file1.dat", path);
  ASSERT_STREQ("deadbeef", env_opaque->Get("authz"));
  // Authorization appened as part of the http headers
  norm_hdrs = {{"xrd-http-fullresource", "/eos/dev/file2.dat"},
    {"authorization", "dabadaba"}
  };
  ASSERT_TRUE(HttpServer::BuildPathAndEnvOpaque(norm_hdrs, path, env_opaque));
  EXPECT_EQ("/eos/dev/file2.dat", path);
  ASSERT_STREQ("dabadaba", env_opaque->Get("authz"));
  // Fail if both http header and opaque info with authorization present
  norm_hdrs = {{"xrd-http-fullresource", "/eos/dev/file3.dat?authz=abbaabba"},
    {"authorization", "dabadaba"}
  };
  ASSERT_FALSE(HttpServer::BuildPathAndEnvOpaque(norm_hdrs, path, env_opaque));
  // Authorization appeneded as opaque information plus extra opaque data
  norm_hdrs = {{"xrd-http-fullresource", "/eos/dev/file4.dat?authz=deadbeef&test=dummy"}};
  ASSERT_TRUE(HttpServer::BuildPathAndEnvOpaque(norm_hdrs, path, env_opaque));
  EXPECT_EQ("/eos/dev/file4.dat", path);
  ASSERT_STREQ("deadbeef", env_opaque->Get("authz"));
  ASSERT_STREQ("dummy", env_opaque->Get("test"));
}
