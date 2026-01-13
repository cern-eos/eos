//------------------------------------------------------------------------------
//! @file AdminSocket.hh
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

#pragma once
#include "common/AssistedThread.hh"
#include "common/Logging.hh"
#include "mgm/Namespace.hh"
#include <zmq.hpp>
#include <sys/types.h>
#include <string>

EOSMGMNAMESPACE_BEGIN

class AdminSocket {
public:
  AdminSocket() {
  }
  
  AdminSocket(const std::string& path){
    mSocket = "ipc://";
    mSocket += path;
    eos_static_info("socket-path=%s", mSocket.c_str());
    mThread.reset(&AdminSocket::Run, this);
  }
  
  void Run(ThreadAssistant& assistant) noexcept;

  virtual ~AdminSocket(){
    mThread.join();
  }
private:
  AssistedThread mThread;
  std::string mSocket;
};


EOSMGMNAMESPACE_END
