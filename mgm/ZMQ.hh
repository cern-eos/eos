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

#ifndef __EOSMGM_ZMQ__HH__
#define __EOSMGM_ZMQ__HH__

#ifdef HAVE_ZMQ
/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/ZMQ.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class ZMQ : public eos::common::ZMQ {
public:
  ZMQ(const char* URL);
  ~ZMQ() {};
  virtual void Process(zmq::socket_t &socket, zmq::message_t &request); // we implement the MGM processing here
};

EOSMGMNAMESPACE_END

#endif
#endif
