//------------------------------------------------------------------------------
// File: GroupBalancer.hh
// Author: Joaquim Rocha - CERN
//------------------------------------------------------------------------------

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
#include "mgm/Namespace.hh"
#include "common/FileId.hh"
#include "common/AssistedThread.hh"
#include "common/RWMutex.hh"
#include <vector>
#include <string>
#include <cstring>
#include <ctime>
#include <map>
#include <unordered_set>
#include "mgm/groupbalancer/BalancerEngineTypes.hh"

EOSMGMNAMESPACE_BEGIN

class FsGroup;
class FsSpace;
static constexpr uint64_t GROUPBALANCER_MIN_FILE_SIZE = 1ULL << 30;
static constexpr uint64_t GROUPBALANCER_MAX_FILE_SIZE = 16ULL << 30;
static constexpr int GROUPBALANCER_FILE_ATTEMPTS = 50;
using eos::mgm::group_balancer::GroupSizeInfo;

namespace group_balancer {
class BalancerEngine;
} // group_balancer

//------------------------------------------------------------------------------
//! @brief Class running the balancing among groups
//! For it to work, the Converter also needs to be enabled.
//------------------------------------------------------------------------------
class GroupBalancer
{
public:
  //----------------------------------------------------------------------------
  //! Constructor (per space)
  //
  //! @param spacename name of the associated space
  //----------------------------------------------------------------------------
  GroupBalancer(const char* spacename);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~GroupBalancer();

  //----------------------------------------------------------------------------
  //! Stop group balancing thread
  //----------------------------------------------------------------------------
  void Stop();

  //----------------------------------------------------------------------------
  // Service implementation e.g. eternal conversion loop running third-party
  // conversion
  //----------------------------------------------------------------------------
  void GroupBalance(ThreadAssistant& assistant) noexcept;


  struct Config {
    bool is_enabled;
    bool is_conv_enabled;
    int num_tx;
    uint64_t mMinFileSize; ///< Min size of files to be picked
    uint64_t mMaxFileSize; ///< Max size of files to be picked
    uint64_t file_attempts;
    group_balancer::BalancerEngineT engine_type;

    Config(): is_enabled(true), is_conv_enabled(true), num_tx(0),
      mMinFileSize(GROUPBALANCER_MIN_FILE_SIZE),
      mMaxFileSize(GROUPBALANCER_MAX_FILE_SIZE),
      engine_type(group_balancer::BalancerEngineT::stddev)
    {}
  };

  struct FileInfo {
    eos::common::FileId::fileid_t fid;
    std::string filename;
    uint64_t filesize;

    FileInfo() = default;
    FileInfo(eos::common::FileId::fileid_t _fid,
             std::string&& _fname, uint64_t _fsize) : fid(_fid),
      filename(std::move(_fname)),
      filesize(_fsize)
    {}

    // Check if both fid && filename are set, 0 size is valid
    operator bool() const
    {
      return fid != 0  && !filename.empty();
    }
  };

  //----------------------------------------------------------------------------
  //! Apply configuration stored at the space level
  //!
  //! @param space the space to configure
  //! @param cfg the GroupBalancer::Config struct
  //!
  //! @return boolean based on valid configuration
  //----------------------------------------------------------------------------
  bool Configure(FsSpace* const space, Config& cfg);

  std::string Status(bool detail = false, bool monitoring = false) const;

  static bool is_valid_engine(std::string_view engine_name);


  //----------------------------------------------------------------------------
  //! Ask the engine to reconfigure itself
  //! NOTE: Internally this is done by setting an atomic reconfigure flag
  //! While technically due to conf being already synchronised internally with a
  //! mutex, even with relaxed memory ordering we'll see the changes, but a stronger
  //! release/acquire semantic is to ensure that we don't wait on the conf mutex
  //! If you're changing this please make sure to change the corresponding acquire
  //! call in the GroupBalance routine.
  //----------------------------------------------------------------------------
  inline void reconfigure()
  {
    mDoConfigUpdate.store(true, std::memory_order_release);
  }

private:
  AssistedThread mThread; ///< Thread scheduling jobs
  std::string mSpaceName; ///< Attached space name
  Config mCfg;

  std::atomic<bool> mDoConfigUpdate {true};

  mutable eos::common::RWMutexW mEngineMtx;
  std::unique_ptr<group_balancer::BalancerEngine> mEngine;

  /// last time the groups' real used space was checked
  time_t mLastCheck;
  //! Scheduled transfers (maps fid to path in proc)
  std::map<eos::common::FileId::fileid_t, std::string> mTransfers;
  group_balancer::engine_conf_t mEngineConf;
  //----------------------------------------------------------------------------
  //! Produces a file conversion path to be placed in the proc directory taking
  //! into account the given group and also returns its size
  //!
  //! @param fid the file ID
  //! @param group the group to which the file will be transferred
  //! @param size return address for the size of the file
  //!
  //! @return name of the proc transfer file
  //----------------------------------------------------------------------------
  static std::string
  getFileProcTransferNameAndSize(eos::common::FileId::fileid_t fid,
                                 FsGroup* group, uint64_t* size);

  //----------------------------------------------------------------------------
  //! Chooses a random file ID from a random filesystem in the given group
  //!
  //! @param group the group from which the file id will be chosen
  //!
  //! @return the chosen file ID
  //----------------------------------------------------------------------------
  eos::common::FileId::fileid_t chooseFidFromGroup(FsGroup* group);

  //----------------------------------------------------------------------------
  //! Chooses a random file from a random filesystem in the given group, but makes
  //! a few attempts to pick a file within the configured size limits.
  //! @param from_group the group from which the file id will be chosen
  //! @param to_group the group to which file will be moved to
  //! @param no of attempts (default :50)
  //!
  //! @return FileInfo for the chosen file
  //----------------------------------------------------------------------------
  GroupBalancer::FileInfo chooseFileFromGroup(FsGroup* from_group,
                                              FsGroup* to_group, int attempts);

  void prepareTransfers(int nrTransfers);

  //----------------------------------------------------------------------------
  //! Picks two groups (source and target) randomly and schedule a file ID
  //! to be transferred
  //----------------------------------------------------------------------------
  void prepareTransfer(void);

  //----------------------------------------------------------------------------
  //! Creates the conversion file in proc for the file ID, from the given
  //! sourceGroup, to the targetGroup (and updates the cache structures)
  //!
  //! @param file_info, the FileInfo struct of the file to be transferred
  //! @param sourceGroup the group where the file is currently located
  //! @param targetGroup the group to which the file is will be transferred
  //----------------------------------------------------------------------------
  void scheduleTransfer(const FileInfo& file_info,
                        FsGroup* sourceGroup, FsGroup* targetGroup);

  //----------------------------------------------------------------------------
  //! Check if the sizes cache should be updated (based on the time passed since
  //! they were last updated)
  //!
  //! @return whether the cache expired or not
  //----------------------------------------------------------------------------
  bool cacheExpired(void);

  //----------------------------------------------------------------------------
  //! For each entry in mTransfers, check if the file was transfered and remove
  //! it from the list
  //----------------------------------------------------------------------------
  void UpdateTransferList(void);
};

EOSMGMNAMESPACE_END
