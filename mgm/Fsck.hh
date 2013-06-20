// ----------------------------------------------------------------------
// File: Fsck.hh
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

#ifndef __EOSMGM_FSCK__HH__
#define __EOSMGM_FSCK__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "mgm/FsView.hh"
#include "common/Logging.hh"
#include "common/FileId.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <google/sparse_hash_map>
#include <google/sparse_hash_set>
#include <sys/types.h>
#include <string>
#include <stdarg.h>
#include <map>
#include <set>

/*----------------------------------------------------------------------------*/
/**
 * @file Fsck.hh
 * 
 * @brief Class aggregating FSCK statistics and repair functionality 
 * 
 */
/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
/**
 * @brief Class implementing the EOS filesystem check.
 * 
 * When the FSCK thread is enabled it collects in a regular interval the 
 * FSCK results broadcasted by all FST nodes into a central view.
 * 
 * The FSCK interface offers a 'report' and a 'repair' utility allowing to 
 * inspect and to actively try to run repair commands to fix inconsistencies.
 */
/*----------------------------------------------------------------------------*/
class Fsck
{
private:

  /// in-memory FSCK log
  XrdOucString mLog;
  
  /// mutex protecting the in-memory log
  XrdSysMutex mLogMutex;

  /// flag indicating if the collection thread is active
  XrdOucString mEnabled;
  
  /// interval between two FSCK collection loops
  int mInterval;

  /// thread-id of the collection thread
  pthread_t mThread;
  
  /// indicator if the collection thread is currently running
  bool mRunning;

  /// mutex protecting all eX... map objects
  XrdSysMutex eMutex; 

  /// error detail  map storing "<error-name>=><fsid>=>[fid1,fid2,fid3...]"
  std::map<std::string, std::map<eos::common::FileSystem::fsid_t, std::set <eos::common::FileId::fileid_t> > > eFsMap;

  /// error summary map storing "<error-name>"=>[fid1,fid2,fid3...]"
  std::map<std::string, std::set <eos::common::FileId::fileid_t> > eMap;
  std::map<std::string, unsigned long long > eCount;

  /// unavailable filesystems map
  std::map<eos::common::FileSystem::fsid_t, unsigned long long > eFsUnavail;

  /// dark filesystem map - dark filesystems are filesystems referenced by 
  /// a file but they are currently not configured in the filesystem view
  std::map<eos::common::FileSystem::fsid_t, unsigned long long > eFsDark;

  // timestamp of collection
  time_t eTimeStamp;

  // ---------------------------------------------------------------------------
  /**
   * @brief reset all collected errors in the error map
   *  
  */
  // ---------------------------------------------------------------------------
  void
  ResetErrorMaps ()
  {
    XrdSysMutexHelper lock (eMutex);
    eFsMap.clear ();
    eMap.clear ();
    eCount.clear ();
    eFsUnavail.clear ();
    eFsDark.clear ();
    eTimeStamp = time (NULL);
  }

public:
  /// configuration key used in the configuration engine to store the enable 
  /// status
  static const char* gFsckEnabled;
  
  /// configuration key used in the configuration engine to store the interval
  static const char* gFsckInterval;

  // Constructor
  Fsck ();
  
  // Destructor
  ~Fsck ();

  // Start the collection thread
  bool Start (int interval = 0);
  
  // Stop the collection thread
  bool Stop ();

  // FSCK interface usage output
  bool Usage (XrdOucString &out, XrdOucString &err);

  // Print function to display FSCK results
  void PrintOut (XrdOucString &out, XrdOucString option = "");
  
  // Method to create a report
  bool Report (XrdOucString &out, XrdOucString &err, XrdOucString option = "", XrdOucString selection = "");
  
  // Method ot issue an repair action
  bool Repair (XrdOucString &out, XrdOucString &err, XrdOucString option = "");

  // Clear the in-memory log
  void ClearLog ();
  
  // Write a log message to the in-memory log
  void Log (bool overwrite, const char* msg, ...);

  // Apply the FSCK configuration stored in the configuration engine
  void ApplyFsckConfig ();
  
  // Store the FSCK configuration to the configuration engine
  bool StoreFsckConfig ();

  // static thread startup function
  static void* StaticCheck (void*);
  
  // FSCK thread loop function
  void* Check ();
};

EOSMGMNAMESPACE_END

#endif
