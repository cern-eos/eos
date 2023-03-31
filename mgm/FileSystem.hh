// ----------------------------------------------------------------------
// File: FileSystem.hh
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

#pragma once
#include "common/FileSystem.hh"
#include "mq/MessagingRealm.hh"
#include "mgm/Namespace.hh"

//! Forward declarations
namespace eos
{
namespace mq
{
class MessagingRealm;
}
}

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class representing a filesystem on the MGM
//------------------------------------------------------------------------------
class FileSystem : public eos::common::FileSystem
{
public:
  //! Tag for saving number of running balance transfers in hash
  static const std::string sNumBalanceTxTag;

  //----------------------------------------------------------------------------
  //! Check if this is a drain transition i.e. enables or disabled draining
  //!
  //! @param new_status new configuration status to be set
  //! @param new_status new configuration status to be set
  //!
  //! @return 1 if draining enabled, -1 if draining disabled, 2 if draining
  //!         should be restarted, 0 if not a drain transition
  //----------------------------------------------------------------------------
  static
  int IsDrainTransition(const eos::common::ConfigStatus old_status,
                        const eos::common::ConfigStatus new_status);

  //----------------------------------------------------------------------------
  //! @brief Constructor
  //!
  //! @param queuepath describing a filesystem like /eos/<host:port>/fst/data01/
  //! @param queue associated to a filesystem like /eos/<host:port>/fst
  //! @param som external shared object manager object
  //----------------------------------------------------------------------------
  FileSystem(const common::FileSystemLocator& locator,
             mq::MessagingRealm* msr) :
    eos::common::FileSystem(locator, msr)
  {}

  //----------------------------------------------------------------------------
  //! Destructor - needs to kill any on-going drain jobs
  //----------------------------------------------------------------------------
  virtual ~FileSystem() = default;

  //----------------------------------------------------------------------------
  //! @brief Get the current broadcasting setting
  //!
  //! @return true if broadcasting otherwise false
  //----------------------------------------------------------------------------
  bool ShouldBroadCast();

  //----------------------------------------------------------------------------
  //! Set the configuration status of a file system. This can be used to trigger
  //! the draining.
  //! @note Must be called with a lock on FsView::ViewMutex
  //!
  //! @param status file system status
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetConfigStatus(eos::common::ConfigStatus status);

  //----------------------------------------------------------------------------
  //! Set a 'key' describing the filesystem
  //! @note Must be called with a lock on FsView::ViewMutex
  //!
  //! @param key key to set
  //! @param str value of the key
  //! @param broadcast if true broadcast the change around
  //!
  //! @return true if successful otherwise false
  //----------------------------------------------------------------------------
  bool SetString(const char* key, const char* str, bool broadcast = true);

  //----------------------------------------------------------------------------
  //! Increment number of running balancing transfers
  //----------------------------------------------------------------------------
  void IncrementBalanceTx();

  //----------------------------------------------------------------------------
  //! Decrement number of running balancing transfers
  //----------------------------------------------------------------------------
  void DecrementBalanceTx();

private:
  //! Number of running balance transfers
  std::atomic<uint64_t> mNumBalanceTx {0};
};

EOSMGMNAMESPACE_END
