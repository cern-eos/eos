// ----------------------------------------------------------------------
// File: Messaging.hh
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

#ifndef __EOSMGM_MESSAGING__HH__
#define __EOSMGM_MESSAGING__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "mq/XrdMqMessaging.hh"
#include "mq/XrdMqSharedObject.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class Messaging : public XrdMqMessaging, public eos::common::LogId
{
public:
  // we have to clone the base class constructore otherwise we cannot run inside valgrind                                                                                                            
  Messaging (const char* url, const char* defaultreceiverqueue, bool advisorystatus = false, bool advisoryquery = false, XrdMqSharedObjectManager* som = 0);

  virtual
  ~Messaging () { }

  virtual bool Update (XrdAdvisoryMqMessage* advmsg);
  virtual void Listen ();
  virtual void Process (XrdMqMessage* newmessage);

  // listener thread startup                                                                                                                                                                         
  static void* Start (void*);
};

EOSMGMNAMESPACE_END

#endif
