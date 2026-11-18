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
#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace {
class FakeCtaFrontend : public cta::xrd::CtaRpc::Service {
  grpc::Status
  Archive(grpc::ServerContext*, const cta::xrd::Request*, cta::xrd::Response*) override
  {
    return grpc::Status::OK;
  }
};
} // namespace

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
  logging.gMutex.Lock();

  for (const auto& lines : logging.gLogMemory) {
    for (const auto& line : lines) {
      // Unused slots of the log ring are empty and have a null c_str()
      const char* text = line.c_str();
      EXPECT_TRUE(text == nullptr || strstr(text, token.c_str()) == nullptr) << text;
    }
  }

  logging.gMutex.UnLock();
}

//------------------------------------------------------------------------------
// A restarted CTA frontend resets the old connection. The client must
// reconnect and retry at once, not fail the request.
//------------------------------------------------------------------------------
TEST(WFEGrpcClient, RetryAfterConnectionReset)
{
  const std::string token =
      jwt::create().set_issuer("eos-unit-test").sign(jwt::algorithm::hs256{"secret"});
  char path[] = "/tmp/eos-wfe-jwt-XXXXXX";
  int fd = mkstemp(path);
  ASSERT_NE(fd, -1);
  ASSERT_EQ(write(fd, token.c_str(), token.size()), (ssize_t)token.size());
  close(fd);
  // This listener resets the first connection, then the real frontend
  // takes over the same port
  int lfd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(lfd, 0);
  int one = 1;
  setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  socklen_t len = sizeof(addr);
  ASSERT_EQ(0, bind(lfd, (sockaddr*)&addr, len));
  ASSERT_EQ(0, listen(lfd, 1));
  ASSERT_EQ(0, getsockname(lfd, (sockaddr*)&addr, &len));
  const std::string endpoint = "127.0.0.1:" + std::to_string(ntohs(addr.sin_port));
  FakeCtaFrontend service;
  std::unique_ptr<grpc::Server> server;
  std::thread restarter([&]() {
    int cfd = accept(lfd, nullptr, nullptr);
    char buf[256];
    (void)!read(cfd, buf, sizeof(buf));
    close(lfd);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(endpoint, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    server = builder.BuildAndStart();
    linger lg{1, 0};
    setsockopt(cfd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
    close(cfd);
  });
  WFEGrpcClient client(endpoint, std::nullopt, path, false);
  cta::xrd::Request request;
  cta::xrd::Response response;
  request.mutable_notification()->mutable_wf()->set_event(cta::eos::Workflow::CLOSEW);
  cta::xrd::Response::ResponseType rsp = cta::xrd::Response::RSP_INVALID;
  EXPECT_NO_THROW(rsp = client.send(request, response));
  EXPECT_EQ(cta::xrd::Response::RSP_SUCCESS, rsp);
  restarter.join();
  ASSERT_TRUE(server);
  server->Shutdown();
  unlink(path);
}
