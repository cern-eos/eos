//------------------------------------------------------------------------------
// File: InstanceName.cc
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

#include "common/InstanceName.hh"
#include "common/Logging.hh"
#include "common/Assert.hh"

EOSCOMMONNAMESPACE_BEGIN

eos::common::RWMutex InstanceName::mMutex;
std::string InstanceName::mInstanceName;

//------------------------------------------------------------------------------
// Set eos instance name - call this only once
//------------------------------------------------------------------------------
void InstanceName::set(const std::string& name)
{
  eos::common::RWMutexWriteLock wr_lock(mMutex);
  eos_static_info("Setting global instance name => %s", name.c_str());
  eos_assert(mInstanceName.empty());
  eos_assert(!name.empty());
  mInstanceName = name;
}

//------------------------------------------------------------------------------
// Get eos instance name
//------------------------------------------------------------------------------
std::string InstanceName::get()
{
  eos::common::RWMutexReadLock rd_lock(mMutex);
  eos_assert(!mInstanceName.empty());
  return mInstanceName;
}

//------------------------------------------------------------------------------
// Get MGM global config queue
//------------------------------------------------------------------------------
std::string InstanceName::getGlobalMgmConfigQueue()
{
  return SSTR("/config/" << InstanceName::get() << "/mgm/");
}

//------------------------------------------------------------------------------
// Has the instance name been set?
//------------------------------------------------------------------------------
bool InstanceName::empty()
{
  eos::common::RWMutexReadLock rd_lock(mMutex);
  return mInstanceName.empty();
}

//------------------------------------------------------------------------------
// Clear stored instance name - used in unit tests
//------------------------------------------------------------------------------
void InstanceName::clear()
{
  eos::common::RWMutexWriteLock wr_lock(mMutex);
  mInstanceName.clear();
}

EOSCOMMONNAMESPACE_END
