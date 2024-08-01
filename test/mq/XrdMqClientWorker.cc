//------------------------------------------------------------------------------
// File: XrdMqClientWorker.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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
#include "mq/XrdMqTiming.hh"
#include <stdio.h>

int main(int argc, char* argv[])
{
  if (argc != 2) {
    std::cerr << "error: at least one argument neeeds to be provided"
              << std::endl;
    exit(-1);
  }

  XrdOucString myid = "root://localhost:1097//eos/";
  myid += argv[1];
  myid += "/worker";
  XrdMqClient mqc;

  if (!mqc.AddBroker(myid.c_str())) {
    std::cerr << "error: failed to add broker " << myid.c_str() << std::endl;
    exit(-1);
  }

  mqc.Subscribe();
  mqc.SetDefaultReceiverQueue("/eos/*/master");
  XrdMqMessage message("Msg for master");
  message.Configure();
  message.Encode();
  XrdMqTiming mq("send");
  TIMING("START", &mq);
  uint64_t count = 0ull;

  while (true) {
    message.NewId();
    message.kMessageHeader.kDescription = "Hello Master Test";
    (mqc << message);
    std::unique_ptr<XrdMqMessage> new_msg {mqc.RecvMessage()};

    if (new_msg) {
      new_msg->Print();
      std::string expected = "Hello Worker Test " + std::to_string(count);

      if (new_msg->kMessageHeader.kDescription != expected.c_str()) {
        std::cerr << "expected: " << expected << " received: " <<
                  new_msg->kMessageHeader.kDescription << std::endl;
        std::terminate();
      }

      ++count;
    }

    do {
      new_msg.reset(mqc.RecvFromInternalBuffer());

      if (new_msg == nullptr) {
        break;
      } else {
        new_msg->Print();
        std::string expected = "Hello Worker Test " + std::to_string(count);

        if (new_msg->kMessageHeader.kDescription != expected.c_str()) {
          std::cerr << "expected: " << expected << " received: " <<
                    new_msg->kMessageHeader.kDescription << std::endl;
          std::terminate();
        }

        ++count;
      }
    } while (true);
  }

  TIMING("SEND+RECV", &mq);
  mq.Print();
}
