// ----------------------------------------------------------------------
// File: XrdMqClientTest.cc
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
#include "mq/XrdMqTiming.hh"
#include <XrdSys/XrdSysLogger.hh>
#include <stdio.h>

int main(int argc, char* argv[])
{
  uint64_t num_loops = 1000;

  if (argc == 2) {
    num_loops = std::stoi(argv[1]);
  }

  XrdMqMessage::Logger = new XrdSysLogger();
  XrdMqMessage::Eroute.logger(XrdMqMessage::Logger);
  XrdMqClient mqc;
  std::string broker_url = "root://localhost:1097//xmessage/";

  if (!mqc.AddBroker(broker_url.c_str())) {
    std::cerr << "error: failed to add broker " << broker_url << std::endl;
    exit(-1);
  }

  if (mqc.AddBroker("root://localhost:1097//xmessage/")) {
    std::cerr << "error: added twice the same broker " << broker_url << std::endl;
    exit(-1);
  }

  mqc.Subscribe();
  mqc.SetDefaultReceiverQueue("/xmessage/*");
  XrdMqMessage message("TestMessage");
  message.Print();
  XrdMqTiming mq("send");
  TIMING("START", &mq);

  for (uint64_t i = 0; i < num_loops; ++i) {
    message.NewId();
    message.kMessageHeader.kDescription = "Test";
    message.kMessageHeader.kDescription += (int)i;
    (mqc << message);
    std::unique_ptr<XrdMqMessage> newmessage {mqc.RecvMessage()};

    if (newmessage) {
      if (i == 0ull)  {
        newmessage->Print();
      }
    }
  }

  TIMING("SEND+RECV", &mq);
  mq.Print();
}
