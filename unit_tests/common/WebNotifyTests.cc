//------------------------------------------------------------------------------
// File: WebNotifyTests.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#include "Namespace.hh"
#include "common/WebNotify.hh"
#include "gtest/gtest.h"
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <cstring>
#include <chrono>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

class SimpleTCPServer
{
public:
  explicit SimpleTCPServer(int port) : port_(port), ready_(false),
    running_(false)
  {
    Start();
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  ~SimpleTCPServer()
  {
    Shutdown();

    if (server_thread_.joinable()) {
      server_thread_.join();  // Avoid std::terminate()
    }
  }

  void Start()
  {
    if (running_) {
      return;  // Don't start twice
    }

    running_ = true;
    server_thread_ = std::thread(&SimpleTCPServer::RunServer, this);
  }

  std::vector<char> GetMessage()
  {
    std::unique_lock<std::mutex> lock(msg_mutex_);
    cv_.wait(lock, [&ready = ready_] { return ready.load(); });
    return message_;
  }

private:
  void RunServer()
  {
    int server_fd, client_fd;
    struct sockaddr_in address {};
    socklen_t addrlen = sizeof(address);
    const int buffer_size = 4096;
    char buffer[buffer_size];
    server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd < 0) {
      perror("Socket failed");
      return;
    }

    server_fd_ = server_fd;  // Save for shutdown
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port_);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
      perror("Bind failed");
      close(server_fd);
      return;
    }

    if (listen(server_fd, 1) < 0) {
      perror("Listen failed");
      close(server_fd);
      return;
    }

    std::cout << "Server listening on port " << port_ << "\n";
    client_fd = accept(server_fd, (struct sockaddr*)&address, &addrlen);

    if (client_fd < 0) {
      if (running_) {
        perror("Accept failed");
      }

      close(server_fd);
      return;
    }

    std::vector<char> local_msg;
    ssize_t bytes_read;

    while ((bytes_read = read(client_fd, buffer, buffer_size)) > 0) {
      local_msg.insert(local_msg.end(), buffer, buffer + bytes_read);
      break;
    }

    {
      std::lock_guard<std::mutex> lock(msg_mutex_);
      message_ = std::move(local_msg);
      ready_ = true;
    }

    close(client_fd);
    close(server_fd);
    running_ = false;
    cv_.notify_all();
  }

  void Shutdown()
  {
    if (running_) {
      running_ = false;

      if (server_fd_ != -1) {
        shutdown(server_fd_, SHUT_RDWR);
        close(server_fd_);
        server_fd_ = -1;
      }

      if (server_thread_.joinable()) {
        server_thread_.join();
      }
    }
  }

  int port_;
  std::thread server_thread_;
  std::vector<char> message_;
  std::mutex msg_mutex_;
  std::condition_variable cv_;
  std::atomic<bool> ready_;
  std::atomic<bool> running_;
  int server_fd_ = -1;
};

std::string to_visible_string(const std::vector<char>& in)
{
  std::string out;
  char buf[5];

  for (unsigned char c : in)
    out += (c == '\n') ? "\\n" :
           (c == '\r') ? "\\r" :
           (c == '\t') ? "\\t" :
           (c == '\\') ? "\\\\" :
           (c == '\"') ? "\\\"" :
           std::isprint(c) ? std::string(1, c) :
           (std::snprintf(buf, sizeof(buf), "\\x%02x", c), buf);

  return out;
}


EOSCOMMONNAMESPACE_BEGIN

TEST(WebNotifyTimeoutTests, HttpPostNotification_TimesOut)
{
  WebNotify notifier;
  std::string url =
    "http://10.255.255.1:12345"; // Non-routable IP (used for timeout testing)
  std::string message = "{\"event\":\"timeout_test\"}";
  EXPECT_FALSE(notifier.sendHttpPostNotification(url, message, 250));
}


TEST(WebNotifyTimeoutTests, HttpPostNotification_OK)
{
  WebNotify notifier;
  std::string url = "http://localhost:12345";
  std::string message = "{\"event\":\"ok_test\"}";
  SimpleTCPServer server(12345);
  EXPECT_FALSE(notifier.sendHttpPostNotification(url, message, 250));
  auto response_message =
    server.GetMessage();  // Blocks until message is received
  std::cerr << to_visible_string(response_message) << std::endl;
  EXPECT_EQ("POST / HTTP/1.1\r\nHost: localhost:12345\r\nAccept: */*\r\nContent-Type: application/json\r\nContent-Length: 19\r\n\r\n{\"event\":\"ok_test\"}",
            std::string(response_message.begin(), response_message.end()));
}

TEST(WebNotifyTimeoutTests, GrpcNotification_TimesOut)
{
  WebNotify notifier;
  std::string target = "10.255.255.1:50051"; // Unreachable IP
  std::string message = "gRPC timeout check";
  EXPECT_FALSE(notifier.sendGrpcNotification(target, message, 250));
}

TEST(WebNotifyTimeoutTests, GrpcNotification_Ok)
{
  WebNotify notifier;
  std::string target = "localhost:12345";
  std::string message = "gRPC timeout check";
  SimpleTCPServer server(12345);
  EXPECT_FALSE(notifier.sendGrpcNotification(target, message, 250));
}

TEST(WebNotifyTimeoutTests, QClientNotification_TimesOut)
{
  WebNotify notifier;
  std::string target = "10.255.255.1"; // Unreachable IP
  int port = 50051;
  std::string message = "QCLient timeout check";
  std::string channel = "Notification";
  EXPECT_FALSE(notifier.sendQClientNotification(target, port, channel, message,
               250));
}

TEST(WebNotifyTimeoutTests, QClientNotification_Ok)
{
  WebNotify notifier;
  std::string target = "localhost";
  int port = 12345;
  std::string message = "QClient timeout check";
  std::string channel = "Notification";
  SimpleTCPServer server(12345);
  EXPECT_FALSE(notifier.sendQClientNotification(target, port, channel, message,
               250));
  auto response_message =
    server.GetMessage();  // Blocks until message is received
  std::cerr << to_visible_string(response_message) << std::endl;
  EXPECT_EQ("*2\r\n$4\r\nPING\r\n$33\r\nqclient-connection-initialization\r\n",
            std::string(response_message.begin(), response_message.end()));
}

TEST(WebNotifyTimeoutTests, ActiveMQNotification_TimesOut)
{
  WebNotify notifier;
  std::string brokerURI = "tcp://10.255.255.1:61616"; // Unreachable
  std::string queue = "timeout_test";
  std::string message = "ActiveMQ timeout check";
  EXPECT_FALSE(notifier.sendActiveMQNotification(brokerURI, queue, message, 250));
}

TEST(WebNotifyTimeoutTests, ActiveMQNotification_Ok)
{
  WebNotify notifier;
  std::string brokerURI = "tcp://localhost:12345";
  std::string queue = "timeout_test";
  std::string message = "ActiveMQ timeout check";
  SimpleTCPServer server(12345);  // Non-blocking
  EXPECT_FALSE(notifier.sendActiveMQNotification(brokerURI, queue, message, 250));
  auto response_message =
    server.GetMessage();  // Blocks until message is received
  auto containsActiveMQ = [](const std::vector<char>& response) {
    const std::string target = "ActiveMQ";
    std::string sresponse(response.begin(), response.end());
    return (sresponse.find(target) != std::string::npos);
  };
  EXPECT_TRUE(containsActiveMQ(response_message));
}


EOSCOMMONNAMESPACE_END
