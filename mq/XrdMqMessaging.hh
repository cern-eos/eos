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

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdMqMessaging():
    mIsZombie(false), mSom(nullptr)
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

  virtual void Listen(ThreadAssistant& assistant) noexcept;

  virtual bool StartListenerThread();

  virtual void StopListener();

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
  AssistedThread mThread; ///< Listener thread
};

#endif
