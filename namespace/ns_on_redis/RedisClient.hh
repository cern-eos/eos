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

//------------------------------------------------------------------------------
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch.
//! @brief Redis client singleton
//------------------------------------------------------------------------------

#ifndef __EOS_NS_REDIS_CLIENT_HH__
#define __EOS_NS_REDIS_CLIENT_HH__

#include "namespace/Namespace.hh"
#include "redox.hpp"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Singleton Redis client class used throughout the namespace implementation
//------------------------------------------------------------------------------
class RedisClient
{
public:

  //----------------------------------------------------------------------------
  //~ Get instance
  //----------------------------------------------------------------------------
  static redox::Redox* getInstance();

private:

  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  RedisClient() {};

  static redox::Redox* sInstance; ///< Singleton RedisClient instance
  static std::string sRedisHost; ///< Redis instance host
  static int sRedisPort; ///< Redis instance port
};

EOSNSNAMESPACE_END

#endif //__EOS_NS_REDIS_CLIENT_HH__
