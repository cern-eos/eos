// ----------------------------------------------------------------------
// File: XrdMqQueueFeeder.cc
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
  uint64_t max_feeds = 0ull;
  uint64_t num_feeds = 0ull;
  uint64_t ms_sleep = 0ull;
  uint64_t msg_size = 10;

  if ((argc < 2) || (argc > 5)) {
    std::cerr << "Usage: " << argv[0] << " <broker_url>/<queue> num_feeds "
              << " ms_sleep_between_feeds msg_size" << std::endl;
    exit(-1);
  }

  if (argc >= 3) {
    max_feeds = std::stoll(argv[2]);
  }

  if (argc >= 4) {
    ms_sleep = strtoll(argv[3], 0, 10);
  }

  if (argc >= 5) {
    msg_size = strtoll(argv[4], 0, 10);
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

  XrdOucString queue = broker;
  int apos = broker.find("//");

  if (apos == STR_NPOS) {
    std::cerr << "error: <borkerurl> must have the following format "
              << "root://host[:port]/<queue>" << std::endl;
    exit(-1);
  }

  int bpos = broker.find("/", apos + 2);

  if (bpos == STR_NPOS) {
    std::cerr << "error: <borkerurl> must have the following format "
              << "root://host[:port]/<queue>" << std::endl;
    exit(-1);
  }

  queue.erase(0, bpos + 1);
  std::cout << "info: feeding into queue: " << queue.c_str() << std::endl;
  mqc.SetDefaultReceiverQueue(queue.c_str());
  XrdMqMessage message("HelloDumper");
  message.Configure(0); // Creates a logger object for the message
  std::string body;
  uint64_t successful_feeds = 0ull;

  for (uint64_t i = 0; i < msg_size; ++i) {
    body += "a";
  }

  while (true) {
    message.NewId();
    message.kMessageHeader.kDescription = "Hello Dumper ";
    message.kMessageHeader.kDescription += (int)num_feeds;
    message.SetBody(body.c_str());
    ++num_feeds;

    if (!(mqc << message)) {
      std::cerr << "error: failed to send msg #" << num_feeds  << std::endl;
    } else {
      std::cout << "info: feeding msg #" << num_feeds << std::endl;
      ++successful_feeds;
    }

    // Exit after max_feeds messages
    if (max_feeds && (num_feeds >= max_feeds)) {
      std::cout << "info: successfully sent " << successful_feeds
                << "/" << num_feeds << " feeds" << std::endl;
      exit(0);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(ms_sleep));
  }
}
