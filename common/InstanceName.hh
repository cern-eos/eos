//------------------------------------------------------------------------------
// File: InstanceName.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#ifndef EOS_COMMON_INSTANCE_NAME_HH
#define EOS_COMMON_INSTANCE_NAME_HH

#include "common/Namespace.hh"
#include <string>
#include <shared_mutex>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Simple class to hold the name of the running EOS instance - initialized
//! during startup.
//!
//! Do not use before initialization, and do not initialize twice.
//------------------------------------------------------------------------------
class InstanceName {
public:

  //----------------------------------------------------------------------------
  //! Set eos instance name - call this only once
  //----------------------------------------------------------------------------
  static void set(const std::string &name);

  //----------------------------------------------------------------------------
  //! Get eos instance name - do not call before getInstanceName
  //----------------------------------------------------------------------------
  static std::string get();

  //----------------------------------------------------------------------------
  //! Get MGM global config queue
  //----------------------------------------------------------------------------
  static std::string getGlobalMgmConfigQueue();

  //----------------------------------------------------------------------------
  //! Has the instance name been set?
  //----------------------------------------------------------------------------
  static bool empty();

  //----------------------------------------------------------------------------
  //! Clear stored instance name - used in unit tests
  //----------------------------------------------------------------------------
  static void clear();

private:
  static std::string mInstanceName;
  static std::shared_timed_mutex mMutex;
};

EOSCOMMONNAMESPACE_END


#endif
