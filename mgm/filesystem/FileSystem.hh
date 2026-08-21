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
  //! Apply a legacy configuration status. This is the compatibility verb: the
  //! one word rolls three orthogonal settings into one, and this undoes that,
  //! writing the permission mask, the lifecycle and the drain request. It is
  //! meant for the command layer, which is where the legacy vocabulary still
  //! arrives from; everything internal sets the key it actually means.
  //! @note Must be called with a lock on FsView::ViewMutex
  //!
  //! @param status legacy file system status
  //! @param status_comment comment describing the change, nullptr leaves the
  //!        currently stored comment untouched, an empty value removes the key
  //! @param wait if true wait for the update to be acknowledged by QuarkDB
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetLegacyConfigStatus(eos::common::ConfigStatus status,
                             const std::string* status_comment = nullptr,
                             bool wait = true);

  //----------------------------------------------------------------------------
  //! Set the operations this file system accepts. Writes the authoritative
  //! mask and the legacy configstatus derived from it as one durable batch, so
  //! the published status can never drift from the mask.
  //!
  //! Unlike SetLegacyConfigStatus this has no drain side effect and leaves the
  //! lifecycle alone - starting or stopping a drain is SetDrainRequested.
  //! @note Must be called with a lock on FsView::ViewMutex
  //!
  //! @param ops operations the file system should accept
  //! @param status_comment comment describing the change, nullptr leaves the
  //!        currently stored comment untouched
  //! @param wait if true wait for the update to be acknowledged by QuarkDB
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetSchedOps(eos::common::FsOpMask ops, const std::string* status_comment = nullptr,
                   bool wait = true);

  //----------------------------------------------------------------------------
  //! Request or stop draining of this file system. The request is stored in
  //! its own durable key so that it survives a master failover without having
  //! to be inferred from a configuration status.
  //! @note Must be called with a lock on FsView::ViewMutex
  //!
  //! @param requested true to start draining, false to stop it
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetDrainRequested(bool requested);

  //----------------------------------------------------------------------------
  //! Set the lifecycle of this file system on its own, leaving the permission
  //! mask and the published legacy status alone. Only for a caller that means
  //! exactly that and nothing else - marking a file system removable ahead of
  //! a move, say; anything changing what the file system accepts goes through
  //! SetSchedOps or SetLegacyConfigStatus, which keep the three keys in step.
  //! @note Must be called with a lock on FsView::ViewMutex
  //!
  //! @param lifecycle new lifecycle value
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetLifecycle(eos::common::FsLifecycle lifecycle);

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
  //! Store the permission mask, the lifecycle and the legacy status derived
  //! from the two as one durable batch, so an observer never catches a mask
  //! and a lifecycle that disagree. Implementation shared by SetSchedOps and
  //! SetLegacyConfigStatus, which differ only in where the lifecycle comes
  //! from.
  //!
  //! @param ops operations the file system should accept
  //! @param lifecycle lifecycle to store alongside them
  //! @param status_comment comment describing the change, nullptr leaves the
  //!        currently stored comment untouched
  //! @param wait if true wait for the update to be acknowledged by QuarkDB
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool DoSetSchedOps(eos::common::FsOpMask ops, eos::common::FsLifecycle lifecycle,
                     const std::string* status_comment, bool wait);

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
