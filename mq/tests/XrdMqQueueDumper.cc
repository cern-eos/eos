// ----------------------------------------------------------------------
// File: XrdMqQueueDumper.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#include "mq/XrdMqClient.hh"
#include <stdio.h>
#include <chrono>

int main(int argc, char* argv[])
{
  uint64_t max_dumps = 0ull;
  uint64_t max_timeout = 0ull;
  uint64_t ms_sleep = 1000;
  bool debug = false;
  std::chrono::time_point<std::chrono::steady_clock> deadline;

  if ((argc < 2) || (argc > 6)) {
    std::cerr << "Usage: " << argv[0] << " <broker_url>/<queue> num_dumps "
              << " sleep_between_dumps_ms max_timeout_sec debug" << std::endl;
    exit(-1);
  }

  if (argc >= 3) {
    max_dumps = std::stoll(argv[2]);
  }

  if (argc >= 4) {
    ms_sleep = std::stoll(argv[3]);
  }

  if (argc >= 5) {
    max_timeout = std::stoull(argv[4]);

    if (max_timeout) {
      deadline = std::chrono::steady_clock::now() +
                 std::chrono::seconds(max_timeout);
    }
  }

  if (argc >= 6) {
    debug = std::stoll(argv[5]) ? true : false;
  }

  XrdOucString broker = argv[1];

  if (!broker.beginswith("root://")) {
    std::cerr << "error: <borkerurl> must have the following format "
              << "root://host[:port]/<queue>" << std::endl;
    exit(-1);
  }

  XrdMqClient mqc;

  if (!mqc.AddBroker(broker.c_str())) {
    std::cerr << "error: failed to add broker " << broker.c_str() << std::endl;
    exit(-1);
  }

  mqc.Subscribe();
  XrdMqMessage message("");
  message.Configure(0); // Creates a logger object for the message
  uint64_t dumped = 0ull;

  while (true) {
    std::unique_ptr<XrdMqMessage> new_msg {mqc.RecvMessage()};

    if (new_msg) {
      ++dumped;

      if (!debug) {
        std::cout << "info: msg #" << dumped << " contents: "
                  << new_msg->GetBody() << std::endl;
      } else {
        std::cout << "info: " << dumped << "/" << max_dumps << ", msg size:"
                  << strlen(new_msg->GetBody()) << std::endl;
      }
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(ms_sleep));
    }

    // Exit after max_dumps messages
    if (max_dumps && (dumped >= max_dumps)) {
      exit(0);
    }

    // Exist if deadline given and expired
    if (max_timeout && (std::chrono::steady_clock::now() > deadline)) {
      exit(ETIME);
    }
  }
}
