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
  static std::chrono::milliseconds sLeaseTimeout;

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param qdb_info contact details for QDB cluster
  //! @param host_port hostname:port of the current mgm
  //----------------------------------------------------------------------------
  QdbMaster(const eos::QdbContactDetails& qdb_info,
            const std::string& host_port);

  //----------------------------------------------------------------------------
  //! Transition types
  //----------------------------------------------------------------------------
  ~QdbMaster() = default;

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
    std::unique_lock<std::mutex> lock(mMutedId);
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
  bool SetManagerId(const std::string& hostname, int port,
                    std::string& err_msg) override;

  //----------------------------------------------------------------------------
  //! Return a delay time for balancing & draining since after a transition
  //! we don't know the maps of already scheduled ID's and we have to make
  //! sure not to reissue a transfer too early!
  //----------------------------------------------------------------------------
  size_t GetServiceDelay() override
  {
    // @todo (esindril): this needs to be properly implemented
    return 60;
  }

  //----------------------------------------------------------------------------
  //! Get master log
  //----------------------------------------------------------------------------
  void GetLog(std::string& stdOut) override
  {
    stdOut = mLog;
  }

private:

  //----------------------------------------------------------------------------
  //! Method supervising the master/slave status
  //!
  //! @param assistant thread executing the method
  //----------------------------------------------------------------------------
  void Supervisor(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  //! Try to acquire lease
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool AcquireLease();

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

  std::string mIdentity; ///< MGM identity hostname:port
  mutable std::mutex mMutedId; ///< Mutex for the master identity
  std::string mMasterIdentity; ///< Current master host
  std::atomic<bool> mIsMaster; ///< Mark if current instance is master
  std::atomic<bool> mConfigLoaded; ///< Mark if configuration is loaded
  AssistedThread mThread; ///< Supervisor thread updating master/slave state
  qclient::QClient* mQcl; ///< qclient for talking to the QDB cluster
};

EOSMGMNAMESPACE_END
