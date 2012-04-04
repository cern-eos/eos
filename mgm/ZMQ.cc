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

#ifdef HAVE_ZMQ
/*----------------------------------------------------------------------------*/

#include "mgm/ZMQ.hh"

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
ZMQ::ZMQ(const char* URL) : eos::common::ZMQ(URL) 
{
}


/*----------------------------------------------------------------------------*/
void
ZMQ::Process(zmq::socket_t &socket, zmq::message_t &request) 
{
  // send reply 
  zmq::message_t reply(5);
  memcpy((void*) reply.data(), "World", 5);
  socket.send(reply);
}

EOSMGMNAMESPACE_END
#endif

