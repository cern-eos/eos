//------------------------------------------------------------------------------
//! @file Fsck.hh
//! @author Andreas-Joachim Peters/Elvin Sindrilaru - CERN
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
#include "common/ThreadPool.hh"
#include "mgm/IdTrackerWithValidity.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/QClient.hh"
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
class Fsck: public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Fsck();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~Fsck()
  {
    Stop();
  }

  //----------------------------------------------------------------------------
  //! Stop all fsck related threads and activities
  //----------------------------------------------------------------------------
  void Stop();

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
  //! @param msg optional message in case of failure
  //!
  //! @param return true if configuration change applied successfully, otherwise
  //!         false
  //----------------------------------------------------------------------------
  bool Config(const std::string& key, const std::string& value, std::string& msg);

  //----------------------------------------------------------------------------
  //! Method to create a report
  //!
  //! @param output output string
  //! @param tags set of tags for which the report should be generated
  //! @param display_per_fs if true then display information per file system
  //! @param display_fxid if true then display file identifiers
  //! @param display_lfn if true then display logical file name
  //! @param display_json if true then display info in json format
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool Report(std::string& output, const std::set<std::string> tags,
              bool display_per_fs,  bool display_fxid, bool display_lfn,
              bool display_json);

  //----------------------------------------------------------------------------
  //! Publish newly collected error information
  //----------------------------------------------------------------------------
  void PublishLogs();

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
  //! Method collecting errors from the FSTS
  //!
  //! @param assistant thread doing the job
  //----------------------------------------------------------------------------
  void CollectErrs(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  //! Method submitting fsck repair jobs to the thread pool
  //!
  //! @param assistant thread doing the job
  //----------------------------------------------------------------------------
  void RepairErrs(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  //! Try to repair a given entry
  //!
  //! @param fid file identifier
  //! @param fsid_err explicit file system id to check
  //! @param err_type type of error on the explicit file system
  //! @param async if true then submit the job to the repair thread if it's
  //!        enabled
  //! @param err_msg output message
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool RepairEntry(eos::IFileMD::id_t fid,
                   eos::common::FileSystem::fsid_t fsid_err,
                   std::string err_type, bool async, std::string& out_msg);

  //----------------------------------------------------------------------------
  //! Set max size of thread pool used for fsck repair jobs
  //!
  //! @param max max value
  //----------------------------------------------------------------------------
  inline void SetMaxThreadPoolSize(uint64_t max)
  {
    mThreadPool.SetMaxThreads(max);
  }

  //----------------------------------------------------------------------------
  //! Get thread pool info
  //!
  //! @return string summary for the thread pool
  //----------------------------------------------------------------------------
  inline std::string GetThreadPoolInfo() const
  {
    return mThreadPool.GetInfo();
  }

private:
  //! Key used in the configuration engine to store the fsck config
  static const std::string sFsckKey;
  //! Key used to store the status of the collector thread in the config
  static const std::string sCollectKey;
  //! Key used to store the collection interval in the config
  static const std::string sCollectIntervalKey;
  //! Key used to store the status of the repair thread in the config
  static const std::string sRepairKey;

  std::atomic<bool> mShowOffline; ///< Flag to display offline files/replicas
  std::atomic<bool> mShowNoReplica; ///< Flag to display no replica files
  std::atomic<bool> mShowDarkFiles; ///< Flag to display dark files
  std::atomic<bool> mStartProcessing; ///< Notification flag for repair thread
  std::atomic<bool> mCollectEnabled; ///< Mark if the err collection is enabled
  std::atomic<bool> mRepairEnabled; ///< Mark if the repair thread is enabled
  std::atomic<bool> mCollectRunning; ///< Mark fi collector is running
  std::atomic<bool> mRepairRunning; ///< Mark fi collector is running
  mutable std::string mLog, mTmpLog; ///< In-memory fsck log
  mutable XrdSysMutex mLogMutex; ///< Mutex protecting the in-memory log
  ///< Interval between FSCK collection loops
  std::chrono::seconds mCollectInterval;
  mutable eos::common::RWMutex mErrMutex; ///< Mutex protecting all map obj
  //! Error detail map storing "<error-name>=><fsid>=>[fid1,fid2,fid3...]"
  std::map<std::string,
      std::map<eos::common::FileSystem::fsid_t,
      std::set <eos::common::FileId::fileid_t> > > eFsMap;
  //! Unavailable filesystems map
  std::map<eos::common::FileSystem::fsid_t, unsigned long long > eFsUnavail;
  //! Dark filesystem map - filesystems referenced by a file but not configured
  //! in the filesystem view
  std::map<eos::common::FileSystem::fsid_t, unsigned long long > eFsDark;
  time_t eTimeStamp; ///< Timestamp of collection
  uint64_t mMaxQueuedJobs {(uint64_t)1e3}; ///< Max number of queued jobs (1k)
  uint32_t mMaxThreadPoolSize {20}; ///< Max number of threads in the pool
  eos::common::ThreadPool mThreadPool; ///< Thread pool for fsck repair jobs
  AssistedThread
  mRepairThread; ///< Thread repair submitting jobs to the thread pool
  AssistedThread mCollectorThread; ///< Thread collecting errors
  std::shared_ptr<qclient::QClient> mQcl; ///< QClient object for metadata


  //----------------------------------------------------------------------------
  //! Query for fsck responses
  //!
  //! @return string with the fsck replied from all the FSTs
  //----------------------------------------------------------------------------
  std::string QueryFsck();

  //----------------------------------------------------------------------------
  //! Create report in JSON format
  //!
  //! @param output output string
  //! @param tags set of tags for which the report should be generated
  //! @param display_per_fs if true then display information per file system
  //! @param display_fxid if true then display file identifiers
  //! @param display_lfn if true then display logical file name
  //----------------------------------------------------------------------------
  void ReportJsonFormat(std::ostringstream& output,
                        const std::set<std::string> tags,
                        bool display_per_fs, bool display_fxid,
                        bool display_lfn) const;

  //----------------------------------------------------------------------------
  //! Create report in monitor format
  //!
  //! @param output output string
  //! @param tags set of tags for which the report should be generated
  //! @param display_per_fs if true then display information per file system
  //! @param display_fxid if true then display file identifiers
  //! @param display_lfn if true then display logical file name
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  void ReportMonitorFormat(std::ostringstream& output,
                           const std::set<std::string> tags,
                           bool display_per_fs, bool display_fxid,
                           bool display_lfn) const;

  //----------------------------------------------------------------------------
  //! Get the require format for the given file identifier. Empty if no format
  //! requested
  //!
  //! @param fid file identifier
  //! @param display_fxid if true display the hex format
  //! @param display_lfn if true then display the URI if available
  //!
  //! @return string representation
  //----------------------------------------------------------------------------
  std::string GetFidFormat(eos::IFileMD::id_t fid, bool display_fxid, bool
                           display_lfn) const;

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
