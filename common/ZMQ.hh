// ----------------------------------------------------------------------
// File: ZMQ.hh
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

#ifndef __EOSCOMMON_ZMQ__HH__
#define __EOSCOMMON_ZMQ__HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysTimer.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#ifdef HAVE_ZMQ
#include <zmq.hpp>
#else
#include "common/zmq.hpp"
#endif
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class ZMQ : public eos::common::LogId {
private:
  std::string bindUrl;  // bind URL to listen for messages
  pthread_t tid;        // thread ID of listener thread
  bool zombie;          // indicator for zombie state
  eos::common::LogId    ThreadLogId;

public:

  ZMQ() {zombie = true;}
  ZMQ(const char* URL);
  virtual ~ZMQ();

  virtual bool IsZombie() { return zombie; }

  virtual void Listen();
  virtual void Process(zmq::socket_t &socket, zmq::message_t &request);
  
  // listener thread startup                                                                                                                                                                         
  static void* Start(void*);

};

EOSCOMMONNAMESPACE_END

#endif
