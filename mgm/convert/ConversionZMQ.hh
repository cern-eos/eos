//------------------------------------------------------------------------------
// File: ConversionZMQ.hh
// Author: Andreas-Joachim Peers - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 20229 CERN/Switzerland                                  *
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
#include "mgm/Namespace.hh"

#include <unistd.h>
#include <stdio.h>
#include <time.h>
#include <signal.h>
#include <vector>
#include <sys/types.h>
#include <sys/wait.h>
#include <zmq.hpp>
#include <string>
#include <iostream>
#include <atomic>
#include <mutex>

EOSMGMNAMESPACE_BEGIN

class ConversionZMQ  {
public:
  ConversionZMQ(int parallelism=16, int basesocket=6001, bool tpc=true);
  virtual ~ConversionZMQ();
  
  bool RunServer();
  void StopServer();
  bool SetupClients();

  int Send(std::string& msg);

  std::string Exec(const std::string& input);

private:
  // robin counter to select client socket
  std::atomic<size_t> mCnt;
  // pids of forked server
  std::vector<pid_t> mPids;
  // client contexts
  std::vector<zmq::context_t*> mContext;
  // client sockets
  std::vector<zmq::socket_t*> mSocket;
  // client mutex
  std::vector<std::mutex*> mMutex;
  // client busy
  std::vector<bool> mBusy;
  pid_t mPid;
  int mParallelism;
  int mBasesocket;
  bool mTpc;
};

EOSMGMNAMESPACE_END
