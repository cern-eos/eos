//------------------------------------------------------------------------------
// File: WFEClientTests.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "common/WFEClient.hh"
#include <cstdlib>
#include <cstring>
#include <gtest/gtest.h>
#include <unistd.h>

//------------------------------------------------------------------------------
// The JWT sent to the CTA frontend must not show up in the logs, not even at
// debug level
//------------------------------------------------------------------------------
TEST(WFEGrpcClient, TokenNotLogged)
{
  using namespace eos::common;
  const std::string token =
      jwt::create().set_issuer("eos-unit-test").sign(jwt::algorithm::hs256{"secret"});
  char path[] = "/tmp/eos-wfe-jwt-XXXXXX";
  int fd = mkstemp(path);
  ASSERT_NE(fd, -1);
  ASSERT_EQ(write(fd, token.c_str(), token.size()), (ssize_t)token.size());
  close(fd);
  // Nothing listens on port 1, the call fails after the token is attached
  WFEGrpcClient client("127.0.0.1:1", std::nullopt, path, false);
  cta::xrd::Request request;
  cta::xrd::Response response;
  request.mutable_notification()->mutable_wf()->set_event(cta::eos::Workflow::CLOSEW);
  Logging& logging = Logging::GetInstance();
  const int old_priority = logging.gPriorityLevel;
  logging.SetLogPriority(LOG_DEBUG);
  EXPECT_ANY_THROW(client.send(request, response));
  logging.SetLogPriority(old_priority);
  unlink(path);
  XrdSysMutexHelper lock(logging.gMutex);

  for (const auto& lines : logging.gLogMemory) {
    for (const auto& line : lines) {
      // Unused slots of the log ring are empty and have a null c_str()
      const char* text = line.c_str();
      ASSERT_TRUE(text == nullptr || strstr(text, token.c_str()) == nullptr) << text;
    }
  }
}
