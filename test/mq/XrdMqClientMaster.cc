//------------------------------------------------------------------------------
// File: XrdMqClientMaster.cc
//------------------------------------------------------------------------------o

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
#include <chrono>

int main(int argc, char* argv[])
{
  uint64_t num_loops = 1000;

  if (argc == 2) {
    num_loops = std::stoi(argv[1]);
  }

  XrdMqClient mqc;

  if (!mqc.AddBroker("root://localhost:1097//eos/localhost/master", true, true)) {
    std::cerr << "error: failed to add broker" << std::endl;
    exit(-1);
  }

  mqc.Subscribe();
  mqc.SetDefaultReceiverQueue("/eos/*/worker");
  XrdMqMessage message("Hello Worker");
  message.Configure();
  message.Encode();
  message.Print();
  XrdMqTiming mq("send");
  TIMING("START", &mq);

  do {
    for (uint64_t i = 0; i < num_loops; ++i) {
      message.NewId();
      message.kMessageHeader.kDescription = "Hello Worker Test ";
      message.kMessageHeader.kDescription += (int)i;
      (mqc << message);

      for (int j = 0; j < 10; j++) {
        std::unique_ptr<XrdMqMessage> new_msg {mqc.RecvMessage()};

        if (new_msg == nullptr) {
          std::this_thread::sleep_for(std::chrono::seconds(2));
          continue;
        }

        if ((new_msg->kMessageHeader.kType == XrdMqMessageHeader::kStatusMessage) ||
            (new_msg->kMessageHeader.kType == XrdMqMessageHeader::kQueryMessage)) {
          std::unique_ptr<XrdAdvisoryMqMessage> adv_msg {
            XrdAdvisoryMqMessage::Create(new_msg->GetMessageBuffer())};
          // adv_msg->Print();
        } else {
          new_msg->Print();

          if (new_msg->kMessageHeader.kDescription != "Hello Master Test") {
            std::terminate();
          }
        }

        do {
          new_msg.reset(mqc.RecvFromInternalBuffer());

          if (new_msg == nullptr) {
            break;
          } else {
            if ((new_msg->kMessageHeader.kType == XrdMqMessageHeader::kStatusMessage) ||
                (new_msg->kMessageHeader.kType == XrdMqMessageHeader::kQueryMessage)) {
              std::unique_ptr<XrdAdvisoryMqMessage> adv_msg {
                XrdAdvisoryMqMessage::Create(new_msg->GetMessageBuffer())};
              // adv_msg->Print();
            } else {
              new_msg->Print();

              if (new_msg->kMessageHeader.kDescription != "Hello Master Test") {
                std::terminate();
              }
            }
          }
        } while (true);
      }
    }
  } while (true);

  TIMING("SEND+RECV", &mq);
  mq.Print();
}
