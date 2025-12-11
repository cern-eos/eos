//------------------------------------------------------------------------------
// File: LocalHash.hh
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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

#pragma once
#include "common/mq/Namespace.hh"
#include "qclient/shared/SharedHash.hh"
#include "qclient/shared/PersistentSharedHash.hh"
#include "qclient/shared/TransientSharedHash.hh"
#include "qclient/Reply.hh"
#include <map>
#include <string>
#include <mutex>

//! Forward declarations
namespace qclient
{
class UpdateBatch;
}

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Hash that stores the key values locally
//------------------------------------------------------------------------------
class LocalHash: public qclient::SharedHash
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  LocalHash(const std::string& key);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~LocalHash() = default;

  //----------------------------------------------------------------------------
  //! Set value
  //----------------------------------------------------------------------------
  std::future<qclient::redisReplyPtr>
  set(const qclient::UpdateBatch& batch) override;

  //----------------------------------------------------------------------------
  //! Get value
  //----------------------------------------------------------------------------
  bool get(const std::string& key, std::string& value) const override;

  //----------------------------------------------------------------------------
  //! Get a list of values, returns a map of kv pairs of found values, expects
  //! empty map as the out param, returns true if all the values have been found
  //!
  //! @param keys vector of string keys
  //! @param out empty map, which will be populated
  //!
  //! @return true if all keys were found, false otherwise or in case of
  //! non empty map
  //----------------------------------------------------------------------------
  bool get(const std::vector<std::string>& keys,
           std::map<std::string, std::string>& out) const override;

  //----------------------------------------------------------------------------
  //! Get the set of keys in the current hash
  //!
  //! @return set of keys in the hash, or empty if none
  //----------------------------------------------------------------------------
  std::vector<std::string> getKeys() const override;

  //----------------------------------------------------------------------------
  //! Get contents of the hash
  //!
  //! @return map of the key value pairs
  //----------------------------------------------------------------------------
  std::map<std::string, std::string> getContents() const override;

private:
  std::string mKey;
  mutable std::mutex mMutex;
  std::map<std::string, std::string> mMap;
};

EOSMQNAMESPACE_END
