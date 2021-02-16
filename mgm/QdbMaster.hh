//------------------------------------------------------------------------------
//! @file QdbMaster.hh
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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
//! @brief Master interface for Qdb implementation
//------------------------------------------------------------------------------
#pragma once
#include "mgm/IMaster.hh"
#include "common/AssistedThread.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"

//! Forward declaration
namespace qclient
{
class QClient;
}

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class IMaster
//------------------------------------------------------------------------------
class QdbMaster: public IMaster
{
public:
  static std::string sLeaseKey;

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param qdb_info contact details for QDB cluster
  //! @param host_port hostname:port of the current mgm
  //----------------------------------------------------------------------------
  QdbMaster(const eos::QdbContactDetails& qdb_info,
            const std::string& host_port);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~QdbMaster();

  //----------------------------------------------------------------------------
  //! Copy constructor
  //----------------------------------------------------------------------------
  QdbMaster(const QdbMaster& other) = delete;

  //----------------------------------------------------------------------------
  //! Copy assignment operator
  //----------------------------------------------------------------------------
  QdbMaster& operator=(const QdbMaster&) = delete;

  //----------------------------------------------------------------------------
  //! Init method to determine the current master/slave state
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool Init() override;

  //----------------------------------------------------------------------------
  //! Boot namespace
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool BootNamespace() override;

  //----------------------------------------------------------------------------
  //! Apply configuration setting
  //!
  //! @param stdOut output string
  //! @param stdErr output error string
  //! @param transition_type transition type
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ApplyMasterConfig(std::string& stdOut, std::string& stdErr,
                         Transition::Type transitiontype) override;

  //----------------------------------------------------------------------------
  //! Check if we are the master host
  //!
  //! @return true if master, otherwise false
  //----------------------------------------------------------------------------
  bool IsMaster() override
  {
    return mIsMaster;
  }

  //----------------------------------------------------------------------------
  //! Check if remove master is OK
  //!
  //! @return true if OK, otherwise false
  //----------------------------------------------------------------------------
  bool IsRemoteMasterOk() const override;

  //----------------------------------------------------------------------------
  //! Get current master hostname
  //----------------------------------------------------------------------------
  const std::string GetMasterId() const override
  {
    std::unique_lock<std::mutex> lock(mMutexId);
    return mMasterIdentity;
  }

  //----------------------------------------------------------------------------
  //! Set the new master hostname
  //!
  //! @param hostname new master hostname
  //! @param port new master port, default 1094
  //! @param err_msg error message
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetMasterId(const std::string& hostname, int port,
                   std::string& err_msg) override;

  //----------------------------------------------------------------------------
  //! Return a delay time for balancing & draining since after a transition
  //! we don't know the maps of already scheduled ID's and we have to make
  //! sure not to reissue a transfer too early!
  //----------------------------------------------------------------------------
  size_t GetServiceDelay() override
  {
    // @todo (esindril): this needs to be properly implemented
    return 0;
  }

  //----------------------------------------------------------------------------
  //! Get master log
  //----------------------------------------------------------------------------
  void GetLog(std::string& stdOut) override
  {
    stdOut = mLog;
  }

  //----------------------------------------------------------------------------
  //! Show the current master/slave run configuration (used by ns stat)
  //!
  //! @return string describing the status
  //----------------------------------------------------------------------------
  std::string PrintOut() override;

private:

  //----------------------------------------------------------------------------
  //! Update the current master id
  //!
  //! @param master_id current master id
  //----------------------------------------------------------------------------
  inline void UpdateMasterId(const std::string& master_id)
  {
    std::unique_lock<std::mutex> lock(mMutexId);
    mMasterIdentity = master_id;
  }

  //----------------------------------------------------------------------------
  //! Method supervising the master/slave status
  //!
  //! @param assistant thread executing the method
  //----------------------------------------------------------------------------
  void Supervisor(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  //! Try to acquire lease
  //!
  //! @param validity_msec validity in milliseconds of the lease
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool AcquireLease(uint64_t validity_msec = 0ull);

  //----------------------------------------------------------------------------
  //! Try to acquire lease with delay. If the mAcquireDelay timestamp is set
  //! then we skip trying to acquire the lease until the delay has expired.
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool AcquireLeaseWithDelay();

  //----------------------------------------------------------------------------
  //! Release lease
  //----------------------------------------------------------------------------
  void ReleaseLease();

  //----------------------------------------------------------------------------
  //! Get the identity of the current lease holder
  //!
  //! @return identity string or empty string if noone holds the leas
  //----------------------------------------------------------------------------
  std::string GetLeaseHolder();

  //----------------------------------------------------------------------------
  //! Slave to master transition
  //----------------------------------------------------------------------------
  void SlaveToMaster();

  //----------------------------------------------------------------------------
  //! Master to slave transition
  //----------------------------------------------------------------------------
  void MasterToSlave();

  //----------------------------------------------------------------------------
  //! Disable namespace caching
  //----------------------------------------------------------------------------
  void DisableNsCaching();

  //----------------------------------------------------------------------------
  //! Enable namespace caching with default values
  //----------------------------------------------------------------------------
  void EnableNsCaching();

  //----------------------------------------------------------------------------
  //! Configure QDB lease timeouts/validity
  //!
  //! @param master_init_lease lease timeout used during a transition
  //----------------------------------------------------------------------------
  void ConfigureTimeouts(uint64_t& master_init_lease);

  std::atomic<bool> mOneOff; ///< Flag to mark that supervisor ran once
  std::string mIdentity; ///< MGM identity hostname:port
  mutable std::mutex mMutexId; ///< Mutex for the master identity
  std::string mMasterIdentity; ///< Current master host
  std::atomic<bool> mIsMaster; ///< Mark if current instance is master
  std::atomic<bool> mConfigLoaded; ///< Mark if configuration is loaded
  ///! Timepoint until when to delay the acquiring of the lease - so that we
  ///! give the chance to other MGMs to become masters
  std::atomic<time_t> mAcquireDelay;
  AssistedThread mThread; ///< Supervisor thread updating master/slave state
  std::unique_ptr<qclient::QClient>
  mQcl; ///< qclient for talking to the QDB cluster
  //! Time for which a lease is aquired
  std::chrono::milliseconds mLeaseValidity {10000};
};

EOSMGMNAMESPACE_END
