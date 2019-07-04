//------------------------------------------------------------------------------
// File: Fsck.hh
// Author: Andreas-Joachim Peters - CERN
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
#include "common/FileSystem.hh"
#include "common/FileId.hh"
#include "common/AssistedThread.hh"
#include <sys/types.h>
#include <string>
#include <stdarg.h>
#include <map>
#include <set>

//------------------------------------------------------------------------------
//! @file Fsck.hh
//! @brief Class aggregating FSCK statistics and repair functionality
//------------------------------------------------------------------------------
EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! @brief Class implementing the EOS filesystem check.
//!
//! When the FSCK thread is enabled it collects on a regular interval the
//! FSCK results broadcasted by all FST nodes into a central view.
//! The FSCK interface offers a 'report' and a 'repair' utility allowing to
//! inspect and to actively try to run repair to fix inconsistencies.
//------------------------------------------------------------------------------
class Fsck
{
public:
  //! Key used in the configuration engine to store the enable status
  static const char* gFsckEnabled;
  //! Key used in the configuration engine to store the check interval
  static const char* gFsckInterval;

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Fsck();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~Fsck();

  //----------------------------------------------------------------------------
  //! Start the collection thread
  //!
  //! @param interval check interval in minutes
  //----------------------------------------------------------------------------
  bool Start(int interval = 0);

  //----------------------------------------------------------------------------
  //! Stop the collection thread
  //----------------------------------------------------------------------------
  bool Stop(bool store = true);

  //----------------------------------------------------------------------------
  //! Print function to display FSCK results
  //!
  //! @param out string holding the output to be displayed
  //----------------------------------------------------------------------------
  void PrintOut(std::string& out) const;

  //----------------------------------------------------------------------------
  //! Apply configuration options to the fsck mechanism
  //!
  //! @param key key to be modified
  //! @param value value
  //!
  //! @param return true if configuration change applied successfully, otherwise
  //!         false
  //----------------------------------------------------------------------------
  bool Config(const std::string& key, const std::string& value);

  //----------------------------------------------------------------------------
  //! Method to create a report
  //!
  //! @param output output string
  //! @param tags set of tags for which the report should be generated
  //! @param display_per_fs if true then display information per file system
  //! @param display_fid if true then display file identifiers
  //! @param display_lfn if true then display logical file name
  //! @param display_json if true then dispaly info in json format
  //! @param display_help if true then display help info about the tags
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool Report(std::string& output, const std::set<std::string> tags,
              bool display_per_fs,  bool display_fid, bool display_lfn,
              bool display_json, bool display_help);

  //----------------------------------------------------------------------------
  //! Method ot issue a repair action
  //!
  //! @param out output of the repair action
  //! @param options set of error types to be repaired
  //!
  //! @return true if successful, otherwise false and out contains the error
  //!         message
  //----------------------------------------------------------------------------
  bool Repair(std::string& out, const std::set<std::string>& options);

  //----------------------------------------------------------------------------
  //! Clear the in-memory log
  //----------------------------------------------------------------------------
  void ClearLog();

  //----------------------------------------------------------------------------
  //! Write a log message to the in-memory log
  //!
  //! @param msg variable length list of printf like format string and args
  //----------------------------------------------------------------------------
  void Log(const char* msg, ...) const;

  //----------------------------------------------------------------------------
  //! Apply the FSCK configuration stored in the configuration engine
  //----------------------------------------------------------------------------
  void ApplyFsckConfig();

  //----------------------------------------------------------------------------
  //! Store the FSCK configuration to the configuration engine
  //----------------------------------------------------------------------------
  bool StoreFsckConfig();

  //----------------------------------------------------------------------------
  //! FSCK thread loop function
  //----------------------------------------------------------------------------
  void Check(ThreadAssistant& assistant) noexcept;

private:
  std::atomic<bool> mShowDarkFiles; ///< Flag to display dark files
  mutable XrdOucString mLog; ///< In-memory FSCK log
  mutable XrdSysMutex mLogMutex; ///< Mutex protecting the in-memory log
  XrdOucString mEnabled; ///< True if collection thread is active
  int mInterval; ///< Interval in min between two FSCK collection loops
  AssistedThread mThread; ///< Collection thread id
  bool mRunning; ///< True if collection thread is currently running
  mutable XrdSysMutex eMutex; ///< Mutex protecting all eX... map objects
  //! Error detail map storing "<error-name>=><fsid>=>[fid1,fid2,fid3...]"
  std::map<std::string,
      std::map<eos::common::FileSystem::fsid_t,
      std::set <eos::common::FileId::fileid_t> > > eFsMap;
  //! Error summary map storing "<error-name>"=>[fid1,fid2,fid3...]"
  std::map<std::string, std::set <eos::common::FileId::fileid_t> > eMap;
  std::map<std::string, unsigned long long > eCount;
  //! Unavailable filesystems map
  std::map<eos::common::FileSystem::fsid_t, unsigned long long > eFsUnavail;
  //! Dark filesystem map - filesystems referenced by a file but not configured
  //! in the filesystem view
  std::map<eos::common::FileSystem::fsid_t, unsigned long long > eFsDark;
  time_t eTimeStamp; ///< Timestamp of collection

  //----------------------------------------------------------------------------
  //! Reset all collected errors in the error map
  //----------------------------------------------------------------------------
  void ResetErrorMaps();

  //----------------------------------------------------------------------------
  //! Account for offline replicas due to unavailable file systems
  //! ie. rep_offline
  //----------------------------------------------------------------------------
  void AccountOfflineReplicas();

  //----------------------------------------------------------------------------
  //! Print offline replicas summary
  //----------------------------------------------------------------------------
  void PrintOfflineReplicas() const;

  //----------------------------------------------------------------------------
  //! Account for file with no replicas ie. zero_replica
  //----------------------------------------------------------------------------
  void AccountNoReplicaFiles();

  //----------------------------------------------------------------------------
  //! Account for offline files or files that require replica adjustments
  //! i.e. file_offline and adjust_replica
  //----------------------------------------------------------------------------
  void AccountOfflineFiles();

  //----------------------------------------------------------------------------
  //! Print summary of the different type of errors collected so far and their
  //! corresponding counters
  //----------------------------------------------------------------------------
  void PrintErrorsSummary() const;

  //----------------------------------------------------------------------------
  //! Account for "dark" file entries i.e. file system ids which have file
  //! entries in the namespace view but have no configured file system in the
  //! FsView.
  //----------------------------------------------------------------------------
  void AccountDarkFiles();
};

EOSMGMNAMESPACE_END
