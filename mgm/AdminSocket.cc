//------------------------------------------------------------------------------
//! @file AdminSocket.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "mgm/AdminSocket.hh"
#include "mgm/proc/IProcCommand.hh"
#include "mgm/proc/ProcInterface.hh"

EOSMGMNAMESPACE_BEGIN

void
AdminSocket::Run(ThreadAssistant& assistant) noexcept
{
  zmq::context_t context(1);
  zmq::socket_t socket(context, ZMQ_REP);
  socket.bind(mSocket.c_str());
  zmq::pollitem_t items[] = {
    {static_cast<void*>(socket), 0, ZMQ_POLLIN, 0}
  };

  while (!assistant.terminationRequested()) {
    zmq::message_t request;
    // poll for work
    zmq_poll(items, 1, 100);

    if (items[0].revents & ZMQ_POLLIN) {
      socket.recv(&request);
      std::string input((char*)request.data(), request.size());
      std::string info = input.substr(input.find("?") + 1);
      input.erase(input.find("?"));
      std::cerr << "serving path: " << input << " cgi: " << info << std::endl;
      eos::common::VirtualIdentity root_vid = eos::common::VirtualIdentity::Root();
      std::unique_ptr<IProcCommand> proccmd =
        ProcInterface::GetProcCommand("adminsocket@localhost", root_vid, input.c_str(),
                                      info.c_str(), "adminsocket");
      size_t size = 0;
      std::string result;

      if (proccmd) {
        XrdOucErrInfo error;
        (void) proccmd->open(input.c_str() , info.c_str() , root_vid, &error);
        struct stat buf;
        proccmd->stat(&buf);
        size = buf.st_size;
        zmq::message_t reply(size);
        proccmd->read(0, (char*)reply.data(), size);
        proccmd->close();
        socket.send(reply);
      } else {
        zmq::message_t reply(0);
        memcpy(reply.data(), result.c_str(), size);
        socket.send(reply);
      }
    }
  }
}
EOSMGMNAMESPACE_END
