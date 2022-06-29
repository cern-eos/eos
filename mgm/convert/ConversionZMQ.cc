//------------------------------------------------------------------------------
// File: ConversionZMQ.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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



#include<chrono>
#include<thread>
#include<setjmp.h>
#include "ConversionZMQ.hh"
#include "common/StringConversion.hh"

EOSMGMNAMESPACE_BEGIN

ConversionZMQ::ConversionZMQ(int parallelism, int basesocket, bool tpc) : mCnt(0), mParallelism(parallelism) , mBasesocket(basesocket) , mTpc(tpc)  {
  // nothing yet
}


ConversionZMQ::~ConversionZMQ()
{
  StopServer();

  for ( int i = 0 ; i < mParallelism; i++ ) {
    delete mSocket[i];
    delete mContext[i];
    delete mMutex[i];
  }
}

bool 
ConversionZMQ::RunServer()
{
  mPid = getpid();
  for (int i = 0 ; i< mParallelism; ++i) {
    pid_t pid;
    if ( !(pid = fork()) ) {
      // forked process goes here
      zmq::context_t context (1);
      zmq::socket_t socket (context, ZMQ_REP);
      std::string zmqaddr = "tcp://*:" + std::to_string(mBasesocket + i);
      socket.bind (zmqaddr);
      while (true) {
	zmq::message_t request;
	//  Wait for next request from client
	socket.recv (&request);
	std::string input((char*)request.data(), request.size());
	std::string src = Exec(input);
	//  Send reply back to client
	zmq::message_t reply (src.length()+1);
	memcpy ((void *) reply.data (), (void*)src.c_str(), src.length()+1);
	socket.send (reply);
	if (kill(mPid,0)) {
	  fprintf(stderr,"# ConversionZMQ::kill parent disappeared - exiting ...\n");
	  _exit(-1);
	}
      }
      _exit(0);
    } else {
      // track the child processes
      mPids.push_back(pid);
    }
  }

  // test that all server are up
  for (int i = 0 ; i< mParallelism; ++i) {
    if (kill(mPids[i],0)) {
      return false;
    }
  }
  return true;
}

bool 
ConversionZMQ::SetupClients()
{
  try {
    for (size_t i= 0 ; i< mPids.size(); ++i) {
      // create connections
      zmq::context_t* ctx = new zmq::context_t(1);
      zmq::socket_t * socket = new zmq::socket_t(*ctx, ZMQ_REQ);
      mContext.push_back(ctx);
      mSocket.push_back(socket);
      mBusy.push_back(false);
      mMutex.push_back(new std::mutex());
      std::string zmqaddr = "tcp://localhost:" + std::to_string(mBasesocket + i);
      mSocket[i]->connect(zmqaddr);
    }
  } catch (...) {
    return false;
  }
  return true;
}

void ConversionZMQ::StopServer() 
{
  for (size_t i = 0 ; i< mPids.size(); i++) {
    kill(mPids[i],9);
    wait(NULL);
  }
}


int ConversionZMQ::Send(std::string& msg)
{
  // increment robin counter
  size_t loops=0;
  do {
    mCnt++;
    loops++;
    if (!(loops%mParallelism)) {
      // snooze a bit everytime we checked out all slots
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    size_t s = mCnt%mParallelism;
    {
      std::unique_lock<std::mutex> lock(*mMutex[s]);
      if (mBusy[s]) {
	continue;
      } else {
	mBusy[s] = true;
      }
    }
    zmq::message_t request (msg.c_str(), msg.size());
    mSocket[s]->send (request);
    //  Get the reply.
    zmq::message_t reply;
    mSocket[s]->recv (&reply);
    std::string response((char*)reply.data(),reply.size());
    {
      std::unique_lock<std::mutex> lock(*mMutex[s]);
      mBusy[s]=false;
    }
    return std::atoi(response.c_str());
  } while(1);
}



std::string
ConversionZMQ::Exec(const std::string& input)
{
  size_t timeout;
  std::string environment;
  std::string url1;
  std::string url2;
  std::vector<std::string> tokens;
  eos::common::StringConversion::EmptyTokenize(input, tokens, "|");
  if (tokens.size()== 4) {
    timeout = atoi(tokens[0].c_str());
    if (!timeout) {
      timeout = 7200;
    }
    environment = tokens[1];
    url1 = tokens[2];
    url2 = tokens[3];
    pid_t child=0;
    int rc = 0;
    time_t s_time = time(NULL);
    if (!(child=fork())) {
      tokens.clear();
      // extract the environment
      eos::common::StringConversion::EmptyTokenize(environment, tokens, " ");
      char *envp[tokens.size()+1];
      for (size_t i=0; i< tokens.size(); ++i) {
	envp[i] = (char*)tokens[i].c_str();
      }
      envp[tokens.size()]=NULL;

      char *args[mTpc?7:6];
      args[0] = "/opt/eos/xrootd/bin/xrdcp";
      args[1] = (char*)"-f";
      args[2] = (char*)"-N";
      if (mTpc) {
	args[3] = (char*)"--tpc only";
	args[4] = (char*)url1.c_str();
	args[5] = (char*)url2.c_str();
	args[6] = NULL;
      } else {
	args[3] = (char*)url1.c_str();
	args[4] = (char*)url2.c_str();
	args[5] = NULL;
      }
      
      int rc = execve("/opt/eos/xrootd/bin/xrdcp", args, envp);
      // the program disappears here
      fprintf(stderr,"# ConversionZMQ: failed to run xrdcp with %d [%d]\n", rc, errno);
      _exit(rc);
    }
    int status;
    while(1) {
      pid_t pid;
      if ( (pid = waitpid(child, &status, WNOHANG)) == 0) {
	time_t age = time(NULL) -s_time;
	if (age > timeout) {
	  fprintf(stderr,"# ConversionZMQ: timeout occured after %u seconds]\n", age);
	  kill(child,9);
	  return std::to_string(ETIMEDOUT);
	}
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	continue;
      }  else {
	break;
      }
    }
    rc = WEXITSTATUS(status);
    return std::to_string(rc);
  } else {
    return std::to_string(EINVAL);
  }
  return std::to_string(-1);
}

EOSMGMNAMESPACE_END
