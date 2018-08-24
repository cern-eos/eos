//------------------------------------------------------------------------------
// File: XrdMqMessaging.hh
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

#ifndef __XRDMQ_MESSAGING_HH__
#define __XRDMQ_MESSAGING_HH__

#include "mq/XrdMqClient.hh"
#include "mq/XrdMqSharedObject.hh"

//------------------------------------------------------------------------------
//! Class XrdMqMessaging
//------------------------------------------------------------------------------
class XrdMqMessaging
{
public:
  static XrdMqClient gMessageClient;
  static void* Start(void* pp);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdMqMessaging():
    mIsZombie(false), mSom(nullptr), mThreadId(0)
  {}

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdMqMessaging(const char* url, const char* defaultreceiverqueue,
                 bool advisorystatus = false, bool advisoryquery = false,
                 XrdMqSharedObjectManager* som = 0);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdMqMessaging();

  virtual void Listen();

  virtual bool StartListenerThread();

  virtual void StopListener();

  //----------------------------------------------------------------------------
  //! Broadcast message and collect responses
  //!
  //! @param broadcastresponsequeue
  //! @param broadcasttargetqueue
  //! @param msgbody message which is broadcasted
  //! @param reponses collected reponses
  //! @param waittime timeout in seconds before we attempt to collect responses
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool BroadCastAndCollect(XrdOucString broadcastresponsequeue,
                           XrdOucString broadcasttargetqueues,
                           XrdOucString& msgbody, XrdOucString& responses,
                           unsigned long waittime = 5);

  //----------------------------------------------------------------------------
  //! Check if listener thread is zombie
  //----------------------------------------------------------------------------
  inline bool IsZombie()
  {
    return mIsZombie;
  }

protected:
  std::atomic<bool> mIsZombie;
  XrdMqSharedObjectManager* mSom;
  pthread_t mThreadId;
};

#endif
