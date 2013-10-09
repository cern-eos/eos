// ----------------------------------------------------------------------
// File: FmdHandler.hh
// Author: Geoffray Adde - CERN
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

/**
 * @file   FmdHandler.hh
 * 
 * @brief  Structure holding the File Meta Data
 * 
 * 
 */

#ifndef __EOSFST_FMDHANDLER_HH__
#define __EOSFST_FMDHANDLER_HH__


/*----------------------------------------------------------------------------*/
#include "common/SymKeys.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include "common/LayoutId.hh"
#include "fst/FmdClient.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class FmdHandler : public eos::fst::FmdClient
{
public:
  XrdOucString DBDir; //< path to the directory with the SQLITE DBs
  eos::common::RWMutex Mutex; //< Mutex protecting the Fmd handler

  // ---------------------------------------------------------------------------
  //! Return's the syncing flag (if we sync, all files on disk are flagge as orphans until the MGM meta data has been verified and when this flag is set, we don't report orphans!
  // ---------------------------------------------------------------------------

  virtual bool
  IsSyncing (eos::common::FileSystem::fsid_t fsid)
  {
    return isSyncing[fsid];
  }

  // ---------------------------------------------------------------------------
  //! Return's the dirty flag indicating a non-clean shutdown
  // ---------------------------------------------------------------------------

  virtual bool
  IsDirty (eos::common::FileSystem::fsid_t fsid)
  {
    return isDirty[fsid];
  }

  // ---------------------------------------------------------------------------
  //! Set the stay dirty flag indicating a non completed bootup
  // ---------------------------------------------------------------------------

  virtual void
  StayDirty (eos::common::FileSystem::fsid_t fsid, bool dirty)
  {
    stayDirty[fsid] = dirty;
  }

  // ---------------------------------------------------------------------------
  //! Define a DB file for a filesystem id
  // ---------------------------------------------------------------------------
  virtual bool SetDBFile (const char* dbfile, int fsid, XrdOucString option = "")=0;

  // ---------------------------------------------------------------------------
  //! Shutdown a DB for a filesystem
  // ---------------------------------------------------------------------------
  virtual bool ShutdownDB (eos::common::FileSystem::fsid_t fsid)=0;

  // ---------------------------------------------------------------------------
  //! Read all Fmd entries from a DB file
  // ---------------------------------------------------------------------------
  //virtual bool ReadDBFile (eos::common::FileSystem::fsid_t, XrdOucString option = "")=0;

  // ---------------------------------------------------------------------------
  //! Trim a DB file
  // ---------------------------------------------------------------------------
  //virtual bool TrimDBFile (eos::common::FileSystem::fsid_t fsid, XrdOucString option = "")=0;
  virtual bool TrimDB ()=0;

  // the meta data handling functions

  // ---------------------------------------------------------------------------
  //! attach or create a fmd record
  // ---------------------------------------------------------------------------
  virtual FmdHelper* GetFmd (eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid, uid_t uid, gid_t gid, eos::common::LayoutId::layoutid_t layoutid, bool isRW = false, bool force = false)=0;

  // ---------------------------------------------------------------------------
  //! Delete an fmd record
  // ---------------------------------------------------------------------------
  virtual bool DeleteFmd (eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid)=0;

  // ---------------------------------------------------------------------------
  //! Commit a modified fmd record
  // ---------------------------------------------------------------------------
  virtual bool Commit (FmdHelper* fmd, bool lockit = true)=0;

  // ---------------------------------------------------------------------------
  //! Commit a modified fmd record without locks and change of modification time
  // ---------------------------------------------------------------------------
  //virtual bool CommitFromMemory (eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid)=0;

  // ---------------------------------------------------------------------------
  //! Reset Disk Information
  // ---------------------------------------------------------------------------
  virtual bool ResetDiskInformation (eos::common::FileSystem::fsid_t fsid)=0;

  // ---------------------------------------------------------------------------
  //! Reset Mgm Information
  // ---------------------------------------------------------------------------
  virtual bool ResetMgmInformation (eos::common::FileSystem::fsid_t fsid)=0;

  // ---------------------------------------------------------------------------
  //! Update fmd from disk contents
  // ---------------------------------------------------------------------------
  virtual bool UpdateFromDisk (eos::common::FileSystem::fsid_t fsid, eos::common::FileId::fileid_t fid, unsigned long long disksize, std::string diskchecksum, unsigned long checktime, bool filecxerror, bool blockcxerror, bool flaglayouterror)=0;


  // ---------------------------------------------------------------------------
  //! Update fmd from mgm contents
  // ---------------------------------------------------------------------------
  virtual bool UpdateFromMgm (eos::common::FileSystem::fsid_t fsid, eos::common::FileId::fileid_t fid, eos::common::FileId::fileid_t cid, eos::common::LayoutId::layoutid_t lid, unsigned long long mgmsize, std::string mgmchecksum, uid_t uid, gid_t gid, unsigned long long ctime, unsigned long long ctime_ns, unsigned long long mtime, unsigned long long mtime_ns, int layouterror, std::string locations)=0;

  // ---------------------------------------------------------------------------
  //! Resync File meta data found under path
  // ---------------------------------------------------------------------------
  virtual bool ResyncAllDisk (const char* path, eos::common::FileSystem::fsid_t fsid, bool flaglayouterror)=0;

  // ---------------------------------------------------------------------------
  //! Resync a single entry from Disk
  // ---------------------------------------------------------------------------
  virtual bool ResyncDisk (const char* fstpath, eos::common::FileSystem::fsid_t fsid, bool flaglayouterror)=0;

  // ---------------------------------------------------------------------------
  //! Resync a single entry from Mgm
  // ---------------------------------------------------------------------------
  virtual bool ResyncMgm (eos::common::FileSystem::fsid_t fsid, eos::common::FileId::fileid_t fid, const char* manager)=0;

  // ---------------------------------------------------------------------------
  //! Resync all entries from Mgm
  // ---------------------------------------------------------------------------
  virtual bool ResyncAllMgm (eos::common::FileSystem::fsid_t fsid, const char* manager)=0;

//  // ---------------------------------------------------------------------------
//  //! Query list of fids
//  // ---------------------------------------------------------------------------
//  virtual size_t Query (eos::common::FileSystem::fsid_t fsid, std::string query, std::vector<eos::common::FileId::fileid_t> &fidvector)=0;

  // ---------------------------------------------------------------------------
  //! GetIncosistencyStatistics
  // ---------------------------------------------------------------------------
  virtual bool GetInconsistencyStatistics (eos::common::FileSystem::fsid_t fsid, std::map<std::string, size_t> &statistics, std::map<std::string, std::set < eos::common::FileId::fileid_t> > &fidset)=0;

  // ---------------------------------------------------------------------------
  //! Initialize the changelog hash
  // ---------------------------------------------------------------------------
  virtual void  Reset (eos::common::FileSystem::fsid_t fsid)=0;

  // ---------------------------------------------------------------------------
  //! Initialize the SQL DB
  // ---------------------------------------------------------------------------
  virtual bool ResetDB (eos::common::FileSystem::fsid_t fsid)=0;

  // ---------------------------------------------------------------------------
  //! Comparison function for modification times
  // ---------------------------------------------------------------------------
  static int CompareMtime (const void* a, const void *b);

  // that is all we need for meta data handling

  // ---------------------------------------------------------------------------
  //! Create a new changelog filename in 'dir' (the fsid suffix is not added!)
  // ---------------------------------------------------------------------------

  virtual const char*
  CreateDBFileName (const char* cldir, XrdOucString &clname)
  {
    clname = cldir;
    clname += "/";
    clname += "fmd";
    return clname.c_str();
  }

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------

  FmdHandler ()
  {
    //SetLogId("CommonFmdDbMapHandler");
  }

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------

  virtual ~FmdHandler ()
  {
  }

protected:
  std::map<eos::common::FileSystem::fsid_t, std::string> DBfilename;

  std::map<eos::common::FileSystem::fsid_t, bool> isDirty;
  std::map<eos::common::FileSystem::fsid_t, bool> stayDirty;

  std::map<eos::common::FileSystem::fsid_t, bool> isSyncing;
};

EOSFSTNAMESPACE_END

#endif
