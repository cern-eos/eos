//------------------------------------------------------------------------------
// File: Locators.cc
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

#include "common/Namespace.hh"
#include "common/Locators.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "common/InstanceName.hh"

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor: Pass the EOS instance name, BaseView type, and name.
//
// Once we drop the MQ entirely, the instance name can be removed.
//------------------------------------------------------------------------------
SharedHashLocator::SharedHashLocator(const std::string &instanceName, Type type,
    const std::string &name)
: mInstanceName(instanceName), mType(type), mName(name) {

  switch(type) {
    case Type::kSpace: {
      mMqSharedHashPath = SSTR("/config/" << instanceName << "/space/" << name);
      mBroadcastQueue = "/eos/*/mgm";
      break;
    }
    case Type::kGroup: {
      mMqSharedHashPath = SSTR("/config/" << instanceName << "/group/" << name);
      mBroadcastQueue = "/eos/*/mgm";
      break;
    }
    case Type::kNode: {
      std::string hostPort = eos::common::StringConversion::GetHostPortFromQueue(name.c_str()).c_str();
      mMqSharedHashPath = SSTR("/config/" << instanceName << "/node/" << hostPort);
      mBroadcastQueue = SSTR("/eos/" << hostPort << "/fst");
      break;
    }
  }
}

//------------------------------------------------------------------------------
//! Constructor: Same as above, but auto-discover instance name.
//------------------------------------------------------------------------------
SharedHashLocator::SharedHashLocator(Type type, const std::string &name)
: SharedHashLocator(InstanceName::get(), type, name) {}

//------------------------------------------------------------------------------
// Get "config queue" for shared hash
//------------------------------------------------------------------------------
std::string SharedHashLocator::getConfigQueue() const {
  return mMqSharedHashPath;
}

//------------------------------------------------------------------------------
// Get "broadcast queue" for shared hash
//------------------------------------------------------------------------------
std::string SharedHashLocator::getBroadcastQueue() const {
  return mBroadcastQueue;
}


EOSCOMMONNAMESPACE_END
