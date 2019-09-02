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

class XrdMqSharedHash;
class XrdMqSharedObjectManager;

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Compatibility class for shared hashes - work in progress.
//------------------------------------------------------------------------------
class SharedHashWrapper {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  SharedHashWrapper(const common::SharedHashLocator &locator, bool takeLock = true);

  //----------------------------------------------------------------------------
  //! "Constructor" for global MGM hash
  //----------------------------------------------------------------------------
  static SharedHashWrapper makeGlobalMgmHash();

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
  bool set(const std::string &key, const std::string &value, bool broadcast = true);

  //----------------------------------------------------------------------------
  //! Query the given key
  //----------------------------------------------------------------------------
  std::string get(const std::string &key);

  //----------------------------------------------------------------------------
  //! Query the given key, return if retrieval successful
  //----------------------------------------------------------------------------
  bool get(const std::string &key, std::string &value);

  //----------------------------------------------------------------------------
  //! Delete the given key
  //----------------------------------------------------------------------------
  bool del(const std::string &key, bool broadcast = true);

  //----------------------------------------------------------------------------
  //! Get all keys in hash
  //----------------------------------------------------------------------------
  bool getKeys(std::vector<std::string> &out);

  //----------------------------------------------------------------------------
  //! Initialize, set shared manager.
  //! Call this function before using any SharedHashWrapper!
  //----------------------------------------------------------------------------
  static void initialize(XrdMqSharedObjectManager *som);

private:
  common::SharedHashLocator mLocator;
  common::RWMutexReadLock mReadLock;
  XrdMqSharedHash *mHash;

  static XrdMqSharedObjectManager *mSom;
};

EOSMQNAMESPACE_END

#endif

