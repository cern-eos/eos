/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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
#include "redox/redoxSet.hpp"
#include "redox/redoxHash.hpp"
#include <atomic>
#include <map>
#include <mutex>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Singleton Redis client class used throughout the namespace implementation
//------------------------------------------------------------------------------
class RedisClient
{
public:
  //----------------------------------------------------------------------------
  //! Initialize
  //----------------------------------------------------------------------------
  static void Initialize() noexcept;

  //----------------------------------------------------------------------------
  //! Finalize
  //----------------------------------------------------------------------------
  static void Finalize();

  //----------------------------------------------------------------------------
  //! Get client for a particular Redis instance
  //!
  //! @param host Redis host
  //! @param port Redis port
  //!
  //! @return Redis client object
  //----------------------------------------------------------------------------
  static redox::Redox* getInstance(const std::string& host = "",
                                   uint32_t port = 0);

private:
  //! Redis client for the default case
  static std::atomic<redox::Redox*> sRedoxClient;
  static std::string sRedisHost; ///< Redis instance host
  static int sRedisPort;         ///< Redis instance port
  //! Map between Redis instance and Redox client
  static std::map<std::string, redox::Redox*> pMapClients;
  static std::mutex pMutexMap; ///< Mutex to protect the access to the map
};

EOSNSNAMESPACE_END

#endif //__EOS_NS_REDIS_CLIENT_HH__
