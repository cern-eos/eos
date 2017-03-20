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

#ifndef __EOSMGM_FSCK__HH__
#define __EOSMGM_FSCK__HH__

#include "mgm/Namespace.hh"
#include "mgm/FsView.hh"
#include "common/Logging.hh"
#include "common/FileId.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <google/sparse_hash_map>
#include <google/sparse_hash_set>
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
//! When the FSCK thread is enabled it collects in a regular interval the
//! FSCK results broadcasted by all FST nodes into a central view.
//!
//! The FSCK interface offers a 'report' and a 'repair' utility allowing to
//! inspect and to actively try to run repair commands to fix inconsistencies.
//------------------------------------------------------------------------------
class Fsck
{
public:
  //! Key used in the configuration engine to store the enable status
  static const char* gFsckEnabled;
  //! Key used in the configuration engine to store the check interval
  static const char* gFsckInterval;

  //----------------------------------------------------------------------------
  //! Static thread startup function
  //----------------------------------------------------------------------------
  static void* StaticCheck(void*);

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
  //! FSCK interface usage output
  //----------------------------------------------------------------------------
  bool Usage(XrdOucString& out, XrdOucString& err);

  //----------------------------------------------------------------------------
  //! Print function to display FSCK results
  //----------------------------------------------------------------------------
  void PrintOut(XrdOucString& out, XrdOucString option = "");

  //----------------------------------------------------------------------------
  //! Method to create a report
  //----------------------------------------------------------------------------
  bool Report(XrdOucString& out, XrdOucString& err, XrdOucString option = "",
              XrdOucString selection = "");

  //----------------------------------------------------------------------------
  //! Method ot issue a repair action
  //!
  //! @param out return of the action output
  //! @param err return of STDERR
  //! @param option selection of repair action (see code or command help)
  //----------------------------------------------------------------------------
  bool Repair(XrdOucString& out, XrdOucString& err, XrdOucString option = "");

  //----------------------------------------------------------------------------
  //! Clear the in-memory log
  //----------------------------------------------------------------------------
  void ClearLog();

  //----------------------------------------------------------------------------
  //! Write a log message to the in-memory log
  //!
  //! @param overwrite if true overwrites the last message
  //!@param msg variable length list of printf like format string and args
  //----------------------------------------------------------------------------
  void Log(bool overwrite, const char* msg, ...);

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
  void* Check();

private:
  XrdOucString mLog; ///< In-memory FSCK log
  XrdSysMutex mLogMutex; ///< Mutex protecting the in-memory log
  XrdOucString mEnabled; ///< True if collection thread is active
  int mInterval; ///< Interval in min between two FSCK collection loops
  pthread_t mThread; ///< Collection thread id
  bool mRunning; ///< True if collection thread is currently running
  XrdSysMutex eMutex; ///< Mutex protecting all eX... map objects
  //! Error detail map storing "<error-name>=><fsid>=>[fid1,fid2,fid3...]"
  std::map<std::string,
      std::map<eos::common::FileSystem::fsid_t,
      std::set <eos::common::FileId::fileid_t> > > eFsMap;
  //! Error summary map storing "<error-name>"=>[fid1,fid2,fid3...]"
  std::map<std::string, std::set <eos::common::FileId::fileid_t> > eMap;
  std::map<std::string, unsigned long long > eCount;
  //! Unavailable filesystems map
  std::map<eos::common::FileSystem::fsid_t, unsigned long long > eFsUnavail;
  //! Dark filesystem map - filesystems referenced by a file bu not configured
  //! in the filesystem view
  std::map<eos::common::FileSystem::fsid_t, unsigned long long > eFsDark;
  time_t eTimeStamp; ///< Timestamp of collection

  //----------------------------------------------------------------------------
  //! Reset all collected errors in the error map
  //----------------------------------------------------------------------------
  void ResetErrorMaps();
};

EOSMGMNAMESPACE_END

#endif
