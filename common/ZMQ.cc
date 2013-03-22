// ----------------------------------------------------------------------
// File: ZMQ.cc
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

/*----------------------------------------------------------------------------*/

#ifdef HAVE_ZMQ
#include "common/ZMQ.hh"

EOSCOMMONNAMESPACE_BEGIN

void*
ZMQ::Start (void *pp)
{
  ((ZMQ*) pp)->Listen();
  return 0;
}

/*----------------------------------------------------------------------------*/
ZMQ::ZMQ (const char* url)
{
  eos::common::LogId();
  tid = 0;
  bindUrl = url;
  int rc = 0;

  if ((rc = XrdSysThread::Run(&tid, ZMQ::Start, static_cast<void *> (this),
                              XRDSYSTHREAD_HOLD, "ZMQ Receiver")))
  {
    eos_thread_err("unable to create zmq thread");
    zombie = true;
  }
  else
  {
    zombie = false;
    eos_thread_info("started ZMQ thread");
  }
}

/*----------------------------------------------------------------------------*/
ZMQ::~ZMQ ()
{
  if (tid && (!zombie))
  {
    XrdSysThread::Cancel(tid);
    XrdSysThread::Join(tid, 0);
  }
}

/*----------------------------------------------------------------------------*/
void
ZMQ::Listen ()
{
  zmq::context_t context(1);
  zmq::socket_t socket(context, ZMQ_REP);
  socket.bind(bindUrl.c_str());

  while (1)
  {
    zmq::message_t request;

    socket.recv(&request);
    XrdSysTimer sleeper;
    sleeper.Snooze(1);

    Process(socket, request);

    XrdSysThread::SetCancelOn();
    XrdSysThread::CancelPoint();
  }
}

/*----------------------------------------------------------------------------*/
void
ZMQ::Process (zmq::socket_t &socket, zmq::message_t &request)
{
  // send reply 
  zmq::message_t reply(5);
  memcpy((void*) reply.data(), "World", 5);
  socket.send(reply);
}

EOSCOMMONNAMESPACE_END

#endif
