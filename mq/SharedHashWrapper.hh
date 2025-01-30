// ----------------------------------------------------------------------
// File: SharedHashWrapper.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

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

#ifndef EOS_MQ_SHARED_HASH_WRAPPER_HH
#define EOS_MQ_SHARED_HASH_WRAPPER_HH

#include "mq/Namespace.hh"
#include "common/Locators.hh"
#include "common/RWMutex.hh"
#include <string>
#include <vector>
#include <map>
#include <memory>

namespace qclient
{
class UpdateBatch;
class SharedHash;
class SharedHashSubscription;
}

class XrdMqSharedHash;
class XrdMqSharedObjectManager;

EOSMQNAMESPACE_BEGIN

class MessagingRealm;

//------------------------------------------------------------------------------
//! Compatibility class for shared hashes - work in progress.
//------------------------------------------------------------------------------
class SharedHashWrapper
{
public:

  //----------------------------------------------------------------------------
  //! Update batch object
  //----------------------------------------------------------------------------
  class Batch
  {
  public:
    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    Batch() {}

    //--------------------------------------------------------------------------
    //! Set value, detect based on prefix whether it should be durable,
    //! transient, or local
    //--------------------------------------------------------------------------
    void Set(const std::string& key, const std::string& value);

    //--------------------------------------------------------------------------
    //! Set durable value
    //--------------------------------------------------------------------------
    void SetDurable(const std::string& key, const std::string& value);

    //--------------------------------------------------------------------------
    //! Set transient value
    //--------------------------------------------------------------------------
    void SetTransient(const std::string& key, const std::string& value);

    //--------------------------------------------------------------------------
    //! Set local value
    //--------------------------------------------------------------------------
    void SetLocal(const std::string& key, const std::string& value);

  private:
    friend class SharedHashWrapper;
    std::map<std::string, std::string> mDurableUpdates;
    std::map<std::string, std::string> mTransientUpdates;
    std::map<std::string, std::string> mLocalUpdates;
  };

  //----------------------------------------------------------------------------
  //! Delete a shared hash, without creating an object first
  //!
  //! @param realm messaging realm instance
  //! @param locator hash locator
  //! @param delete_from_qdb by default true and triggers the deletion of the
  //!        SharedHash object from QDB
  //----------------------------------------------------------------------------
  static bool deleteHash(mq::MessagingRealm* realm,
                         const common::SharedHashLocator& locator,
                         bool delete_from_qdb = true);

  //----------------------------------------------------------------------------
  //! "Constructor" for global MGM hash
  //----------------------------------------------------------------------------
  static SharedHashWrapper makeGlobalMgmHash(mq::MessagingRealm* realm);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  SharedHashWrapper(mq::MessagingRealm* realm,
                    const common::SharedHashLocator& locator,
                    bool takeLock = true, bool create = true);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~SharedHashWrapper();

  //----------------------------------------------------------------------------
  //! Release any interal locks - DO NOT use this object any further
  //----------------------------------------------------------------------------
  void releaseLocks();

  //----------------------------------------------------------------------------
  //! Set key-value pair
  //----------------------------------------------------------------------------
  bool set(const std::string& key, const std::string& value,
           bool broadcast = true);

  //----------------------------------------------------------------------------
  //! Set key-value batch
  //----------------------------------------------------------------------------
  bool set(const Batch& batch);

  //----------------------------------------------------------------------------
  //! Query the given key
  //----------------------------------------------------------------------------
  std::string get(const std::string& key);

  //----------------------------------------------------------------------------
  //! Query the given key - convert to long long automatically
  //----------------------------------------------------------------------------
  long long getLongLong(const std::string& key);

  //----------------------------------------------------------------------------
  //! Query the given key - convert to double automatically
  //----------------------------------------------------------------------------
  double getDouble(const std::string& key);

  //----------------------------------------------------------------------------
  //! Query the given key, return if retrieval successful
  //----------------------------------------------------------------------------
  bool get(const std::string& key, std::string& value);

  //----------------------------------------------------------------------------
  //! Query the given key, return if retrieval successful
  //----------------------------------------------------------------------------
  bool get(const std::vector<std::string>& keys,
           std::map<std::string, std::string>& value);

  //----------------------------------------------------------------------------
  //! Delete the given key
  //----------------------------------------------------------------------------
  bool del(const std::string& key, bool broadcast = true);

  //----------------------------------------------------------------------------
  //! Get all keys in hash
  //----------------------------------------------------------------------------
  bool getKeys(std::vector<std::string>& out);

  //----------------------------------------------------------------------------
  //! Get all hash contents as a map
  //----------------------------------------------------------------------------
  bool getContents(std::map<std::string, std::string>& out);

  //----------------------------------------------------------------------------
  //! Subscribe for updates from the underlying hash
  //----------------------------------------------------------------------------
  std::unique_ptr<qclient::SharedHashSubscription> subscribe();

private:
  XrdMqSharedObjectManager* mSom;
  common::SharedHashLocator mLocator;
  common::RWMutexReadLock mReadLock;
  XrdMqSharedHash* mHash {nullptr};
  std::shared_ptr<qclient::SharedHash> mSharedHash;
};

EOSMQNAMESPACE_END

#endif
