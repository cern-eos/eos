// ----------------------------------------------------------------------
// File: StringStore.hh
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

#ifndef __EOSCOMMON_STRINGSTORE__
#define __EOSCOMMON_STRINGSTORE__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class StringStore {
private:
  static XrdSysMutex StringMutex;
public:
  static XrdOucHash<XrdOucString> theStore;
  static char* Store(const char* charstring , int lifetime=0);

  StringStore() {};
  ~StringStore() {};
};

EOSCOMMONNAMESPACE_END

#endif
