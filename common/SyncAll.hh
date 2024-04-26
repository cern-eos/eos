//------------------------------------------------------------------------------
// File: SyncAll.hh
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/ASwitzerland                                  *
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

#ifndef __EOSCOMMON__CLOEXEC__HH
#define __EOSCOMMON__CLOEXEC__HH

#include <XrdOuc/XrdOucString.hh>
#include <XrdOuc/XrdOucTrace.hh>
#include "common/Namespace.hh"
#include <fcntl.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Static class to sync(+close) all file descriptors
//!
//! Example
//! eos::common::SyncAll::All();
//------------------------------------------------------------------------------
class SyncAll
{
public:
  static void All()
  {
    for (size_t i = getdtablesize(); i-- > 3;) {
      fsync(i);
    }
  }

  static void AllandClose()
  {
    for (size_t i = getdtablesize(); i-- > 3;) {
      fsync(i);
      close(i);
    }
  }

  static void AllandCloseFileSocks()
  {
    for (size_t i = getdtablesize(); i-- > 3;) {
      int v;
      socklen_t vs = sizeof(v);
      if(!fsync(i) || !getsockopt(i, SOL_SOCKET, SO_TYPE, &v, &vs))
        close(i);
    }
  }
};

EOSCOMMONNAMESPACE_END
#endif
