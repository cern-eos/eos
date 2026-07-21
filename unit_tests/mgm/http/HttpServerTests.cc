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
  ASSERT_STREQ("http",env_opaque->Get("eos.app"));
  // eos.app provided by client via opaque infos, should either be http or http/xyz
  norm_hdrs = {{"xrd-http-fullresource", "/eos/dev/file4.dat?authz=deadbeef&test=dummy&eos.app=wizz"}};
  ASSERT_TRUE(HttpServer::BuildPathAndEnvOpaque(norm_hdrs, path, env_opaque));
  ASSERT_STREQ("http/wizz", env_opaque->Get("eos.app"));
  // eos.app provided by client via opaque infos, should either be http or http/xyz
  norm_hdrs = {{"xrd-http-fullresource", "/eos/dev/file4.dat?eos.app=test&authz=deadbeef&test=dummy&eos.app=wizz"}};
  ASSERT_TRUE(HttpServer::BuildPathAndEnvOpaque(norm_hdrs, path, env_opaque));
  ASSERT_STREQ("http/wizz", env_opaque->Get("eos.app"));
}

//------------------------------------------------------------------------------
// Count the number of occurrences of a needle inside the given string
//------------------------------------------------------------------------------
static size_t
CountOccurrences(const std::string& haystack, const std::string& needle)
{
  size_t count = 0;
  size_t pos = 0;

  while ((pos = haystack.find(needle, pos)) != std::string::npos) {
    ++count;
    pos += needle.length();
  }

  return count;
}

//------------------------------------------------------------------------------
// Test the parsing of requests as they are really handed over by XrdHttp i.e
// with both the "xrd-http-fullresource" (url-encoded) and the
// "xrd-http-query" (already percent-decoded) headers set. The two carry the
// very same query, therefore only one of them must end up in the opaque info.
//------------------------------------------------------------------------------
TEST(HttpServer, ParseAuthzFromOpaque)
{
  using eos::mgm::HttpServer;
  std::string path;
  std::unique_ptr<XrdOucEnv> env_opaque;
  std::map<std::string, std::string> norm_hdrs;
  // Token given as "?authz=Bearer%20<token>" - XrdHttp decodes the query so
  // the bearer prefix must be re-encoded for the authz plugins to see it
  norm_hdrs = {{"xrd-http-fullresource", "/eos/dev/file.dat?authz=Bearer%20MDAxMg"},
               {"xrd-http-query", "&authz=Bearer MDAxMg"}};
  ASSERT_TRUE(HttpServer::BuildPathAndEnvOpaque(norm_hdrs, path, env_opaque));
  EXPECT_EQ("/eos/dev/file.dat", path);
  ASSERT_STREQ("Bearer%20MDAxMg", env_opaque->Get("authz"));
  // Clients working around the issue by double-encoding the percent sign must
  // keep on working - their decoded value is already in the expected form
  norm_hdrs = {{"xrd-http-fullresource", "/eos/dev/file.dat?authz=Bearer%2520MDAxMg"},
               {"xrd-http-query", "&authz=Bearer%20MDAxMg"}};
  ASSERT_TRUE(HttpServer::BuildPathAndEnvOpaque(norm_hdrs, path, env_opaque));
  ASSERT_STREQ("Bearer%20MDAxMg", env_opaque->Get("authz"));
  // A bare token without any bearer prefix is left untouched
  norm_hdrs = {{"xrd-http-fullresource", "/eos/dev/file.dat?authz=zteos64:abc"},
               {"xrd-http-query", "&authz=zteos64:abc"}};
  ASSERT_TRUE(HttpServer::BuildPathAndEnvOpaque(norm_hdrs, path, env_opaque));
  ASSERT_STREQ("zteos64:abc", env_opaque->Get("authz"));
  // No key must appear twice in the resulting opaque info and the non-authz
  // values keep the decoded form that XrdHttp handed over
  norm_hdrs = {
      {"xrd-http-fullresource", "/eos/dev/file.dat?a=b%20c&authz=Bearer%20MDAxMg"},
      {"xrd-http-query", "&a=b c&authz=Bearer MDAxMg"}};
  ASSERT_TRUE(HttpServer::BuildPathAndEnvOpaque(norm_hdrs, path, env_opaque));
  ASSERT_STREQ("b c", env_opaque->Get("a"));
  ASSERT_STREQ("Bearer%20MDAxMg", env_opaque->Get("authz"));
  int envlen = 0;
  std::string env_str = env_opaque->Env(envlen);
  ASSERT_EQ(1, CountOccurrences(env_str, "authz="));
  ASSERT_EQ(1, CountOccurrences(env_str, "a=b c"));
  // Authorization header with a query that carries no authz info
  norm_hdrs = {{"xrd-http-fullresource", "/eos/dev/file.dat?test=dummy"},
               {"xrd-http-query", "&test=dummy"},
               {"authorization", "Bearer MDAxMg"}};
  ASSERT_TRUE(HttpServer::BuildPathAndEnvOpaque(norm_hdrs, path, env_opaque));
  ASSERT_STREQ("Bearer%20MDAxMg", env_opaque->Get("authz"));
  ASSERT_STREQ("dummy", env_opaque->Get("test"));
  // Conflicting authorization info is still detected
  norm_hdrs = {{"xrd-http-fullresource", "/eos/dev/file.dat?authz=Bearer%20MDAxMg"},
               {"xrd-http-query", "&authz=Bearer MDAxMg"},
               {"authorization", "Bearer MDAxMg"}};
  ASSERT_FALSE(HttpServer::BuildPathAndEnvOpaque(norm_hdrs, path, env_opaque));
  // The eos.app handling is not disturbed by the new opaque assembly
  norm_hdrs = {{"xrd-http-fullresource", "/eos/dev/file.dat?authz=deadbeef&test=dummy"},
               {"xrd-http-query", "&authz=deadbeef&test=dummy"}};
  ASSERT_TRUE(HttpServer::BuildPathAndEnvOpaque(norm_hdrs, path, env_opaque));
  ASSERT_STREQ("http", env_opaque->Get("eos.app"));
  norm_hdrs = {{"xrd-http-fullresource", "/eos/dev/file.dat?authz=deadbeef&eos.app=wizz"},
               {"xrd-http-query", "&authz=deadbeef&eos.app=wizz"}};
  ASSERT_TRUE(HttpServer::BuildPathAndEnvOpaque(norm_hdrs, path, env_opaque));
  ASSERT_STREQ("http/wizz", env_opaque->Get("eos.app"));
}

static std::map<std::string, std::string> opaqueToNormalizedOpaque = {
    {"", ""},
    {"authz=Bearer tok", "authz=Bearer%20tok"},
    {"&authz=Bearer tok", "&authz=Bearer%20tok"},
    {"a=1&authz=Bearer tok&b=2", "a=1&authz=Bearer%20tok&b=2"},
    {"authz=Bearer tok&authz=Bearer tok2", "authz=Bearer%20tok&authz=Bearer%20tok2"},
    // Already encoded or without any bearer prefix, nothing to do
    {"authz=Bearer%20tok", "authz=Bearer%20tok"},
    {"authz=zteos64:abc", "authz=zteos64:abc"},
    {"authz=Bearertok", "authz=Bearertok"},
    // Keys that merely end with "authz" must not be touched
    {"xauthz=Bearer tok", "xauthz=Bearer tok"},
    {"eos.authz=Bearer tok", "eos.authz=Bearer tok"}};

TEST(HttpServer, NormalizeBearerAuthz)
{
  for (const auto& [opaque, expected] : opaqueToNormalizedOpaque) {
    std::string normalized = opaque;
    eos::mgm::HttpServer::NormalizeBearerAuthz(normalized);
    ASSERT_EQ(expected, normalized);
  }
}

static std::map<std::string,std::pair<std::string,std::string>> fullPathToPathAndOpaque = {
    {"",{"",""}},
    {"/eos/file.dat",{"/eos/file.dat",""}},
    {"/eos/file.dat?",{"/eos/file.dat",""}},
    {"/eos/file.dat?testopaque=1",{"/eos/file.dat","testopaque=1"}},
    {"/eos/file.dat?testopaque=1&authz=qwerty&test=2",{"/eos/file.dat","testopaque=1&authz=qwerty&test=2"}},
};

TEST(HttpServer, ExtractPathAndOpaque) {
  for(const auto & [fullpath, pathAndOpaquePair]: fullPathToPathAndOpaque) {
    std::string extractedPath;
    std::string extractedOpaque;
    eos::mgm::HttpServer::extractPathAndOpaque(fullpath,extractedPath, extractedOpaque);
    ASSERT_EQ(pathAndOpaquePair.first,extractedPath);
    ASSERT_EQ(pathAndOpaquePair.second,extractedOpaque);
  }
}

static std::map<std::string,std::string> fullPathToOpaque = {
    {"",""},
    {"/eos/lhcb/test/?eos.ruid=0","eos.ruid=0"},
    {"/eos/lhcb/",""},
    {"/eos/file.dat?",""},
    {"/eos/lhcb/passwd.txt?eos.test=0&oss.test=18&test=3","eos.test=0&oss.test=18&test=3"},
    {"/eos/lhcb/passwd.txt?authz=azerty&eos.test=0&oss.test=18&test=3","eos.test=0&oss.test=18&test=3"},
    {"/eos/lhcb/passwd.txt?eos.test=0&oss.test=18&authz=azerty&test=3","eos.test=0&oss.test=18&test=3"},
    {"/eos/lhcb/passwd.txt?eos.test=0&oss.test=18&test=3&authz=azerty","eos.test=0&oss.test=18&test=3"}
};

TEST(HttpServer, ExtractOpaqueWithoutAuthz) {
  for (const auto & [fullpath,opaque]: fullPathToOpaque) {
    std::string extractedOpaque;
    eos::mgm::HttpServer::extractOpaqueWithoutAuthz(fullpath,extractedOpaque);
    ASSERT_EQ(opaque,extractedOpaque);
  }
}
