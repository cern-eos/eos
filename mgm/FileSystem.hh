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
#include "common/Logging.hh"
#include "mgm/Namespace.hh"
#include "mq/FsChangeListener.hh"

//! Forward declarations
namespace eos
{
namespace mq
{
class MessagingRealm;
}
}

namespace qclient
{
class SharedHashUpdate;
}

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class representing a filesystem on the MGM
//------------------------------------------------------------------------------
class FileSystem : public eos::common::FileSystem, public eos::common::LogId
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
  //! Constructor
  //!
  //! @param locator file system locator
  //! @param msr messaging realm
  //----------------------------------------------------------------------------
  FileSystem(const common::FileSystemLocator& locator, mq::MessagingRealm* msr);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FileSystem();

  //----------------------------------------------------------------------------
  //! Attach file system change listener
  //!
  //! @param fs_listener file system change listener object
  //! @param interests set of keys which are of interest for the listener
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool AttachFsListener(std::shared_ptr<eos::mq::FsChangeListener> fs_listener,
                        const std::set<std::string>& interests);

  //----------------------------------------------------------------------------
  //! Detach file system change listener
  //!
  //! @param fs_listener file system change listener object
  //! @param interests set of interests from which to detach
  //!
  //! @param return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool DetachFsListener(std::shared_ptr<eos::mq::FsChangeListener> fs_listener,
                        const std::set<std::string>& interests);

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
  static const std::string sGeotagTag;
  static const std::string sErrcTag;
  //! Number of running balance transfers
  std::atomic<uint64_t> mNumBalanceTx {0};
  //! Subscription to underlying shared hash notifications
  std::unique_ptr<qclient::SharedHashSubscription> mSubscription;
  //! Map of interests to file system change notifiers
  std::map<std::string, std::set<std::shared_ptr<eos::mq::FsChangeListener>>>
  mMapListeners;
  //! Mutex protecting the listener's map
  eos::common::RWMutex mRWMutex;

  //----------------------------------------------------------------------------
  //! Process shared hash update
  //!
  //! @param upd shared hash update
  //----------------------------------------------------------------------------
  void ProcessUpdateCb(qclient::SharedHashUpdate&& upd);

  //----------------------------------------------------------------------------
  //! Notify file system change listeners interested in the given update
  //!
  //! @param upd shared hash update
  //----------------------------------------------------------------------------
  void NotifyFsListener(qclient::SharedHashUpdate&& upd);

  //----------------------------------------------------------------------------
  //! Register with interested listeners - this called when a new object is
  //! created and there are already existing FS listeners in the system
  //----------------------------------------------------------------------------
  void RegisterWithExistingListeners();

  //----------------------------------------------------------------------------
  //! Unregister from all the listeners
  //----------------------------------------------------------------------------
  void UnregisterFromListeners();
};

EOSMGMNAMESPACE_END
