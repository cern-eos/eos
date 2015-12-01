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

#include "namespace/ns_on_redis/RedisClient.hh"

EOSNSNAMESPACE_BEGIN

// Static variables
redox::Redox* RedisClient::sInstance = nullptr;
std::string RedisClient::sRedisHost = "localhost";
int RedisClient::sRedisPort = 6380;

//------------------------------------------------------------------------------
// Get instance
//------------------------------------------------------------------------------
redox::Redox*
RedisClient::getInstance()
{
  if (!sInstance)
  {
    sInstance = new redox::Redox();
    // TODO: catch exceptions
    try
    {
      sInstance->connect(sRedisHost, sRedisPort);
    }
    catch (...)
    {
      std::cerr << "ERROR: Failed to connect to Redis instance" << std::endl;
      throw;
    }
  }

  return sInstance;
}

EOSNSNAMESPACE_END
