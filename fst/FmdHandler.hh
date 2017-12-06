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

#ifndef __EOSFST_FMDHANDLER_HH__
#define __EOSFST_FMDHANDLER_HH__

#include "common/SymKeys.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include "common/LayoutId.hh"
#include "fst/FmdClient.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class FmdHandler
//------------------------------------------------------------------------------
class FmdHandler : public eos::fst::FmdClient
{
public:
  //----------------------------------------------------------------------------
  //! Comparison function for modification times
  //!
  //! @param a pointer to a filestat struct
  //! @param b pointer to a filestat struct
  //!
  //! @return difference between the two modification times within the
  //! filestat struct
  //----------------------------------------------------------------------------
  static int CompareMtime(const void* a, const void* b)
  {
    struct filestat {
      struct stat buf;
      char filename[1024];
    };
    return ((((struct filestat*) b)->buf.st_mtime) -
            ((struct filestat*) a)->buf.st_mtime);
  }

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FmdHandler()
  {
    //SetLogId("CommonFmdDbMapHandler");
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FmdHandler() = default;

  //----------------------------------------------------------------------------
  //! Create a new changelog filename in 'dir' (the fsid suffix is not added!)
  //----------------------------------------------------------------------------
  virtual const char*
  CreateDBFileName(const char* cldir, XrdOucString& clname)
  {
    clname = cldir;
    clname += "/";
    clname += "fmd";
    return clname.c_str();
  }

  //----------------------------------------------------------------------------
  //! Define a DB file for a filesystem id
  //----------------------------------------------------------------------------
  virtual bool SetDBFile(const char* dbfile, int fsid,
                         XrdOucString option = "") = 0;

  //----------------------------------------------------------------------------
  //! Return's the syncing flag (if we sync, all files on disk are flagge as
  //! orphans until the MGM meta data has been verified and when this flag is
  //! set, we don't report orphans!
  //----------------------------------------------------------------------------
  virtual bool
  IsSyncing(eos::common::FileSystem::fsid_t fsid)
  {
    return isSyncing[fsid];
  }

  //----------------------------------------------------------------------------
  //! Return's the dirty flag indicating a non-clean shutdown
  //----------------------------------------------------------------------------
  virtual bool
  IsDirty(eos::common::FileSystem::fsid_t fsid)
  {
    return isDirty[fsid];
  }

  //----------------------------------------------------------------------------
  //! Set the stay dirty flag indicating a non completed bootup
  //----------------------------------------------------------------------------
  virtual void
  StayDirty(eos::common::FileSystem::fsid_t fsid, bool dirty)
  {
    stayDirty[fsid] = dirty;
  }

  //----------------------------------------------------------------------------
  //! Shutdown a DB for a filesystem
  //----------------------------------------------------------------------------
  virtual bool ShutdownDB(eos::common::FileSystem::fsid_t fsid) = 0;

  //----------------------------------------------------------------------------
  //! Trim a DB file
  //----------------------------------------------------------------------------
  virtual bool TrimDB() = 0;

  // the meta data handling functions

  //----------------------------------------------------------------------------
  //! Attach or create a Fmd record
  //----------------------------------------------------------------------------
  virtual FmdHelper* GetFmd(eos::common::FileId::fileid_t fid,
                            eos::common::FileSystem::fsid_t fsid,
                            uid_t uid, gid_t gid,
                            eos::common::LayoutId::layoutid_t layoutid,
                            bool isRW = false, bool force = false) = 0;

  //----------------------------------------------------------------------------
  //! Delete an fmd record
  //----------------------------------------------------------------------------
  virtual bool DeleteFmd(eos::common::FileId::fileid_t fid,
                         eos::common::FileSystem::fsid_t fsid) = 0;

  //----------------------------------------------------------------------------
  //! Commit a modified fmd record
  //----------------------------------------------------------------------------
  virtual bool Commit(FmdHelper* fmd, bool lockit = true) = 0;

  //----------------------------------------------------------------------------
  //! Reset disk information
  //!
  //! @param fsid file system id
  //!
  //! @return true if information has been reset successfully
  //----------------------------------------------------------------------------
  virtual bool ResetDiskInformation(eos::common::FileSystem::fsid_t fsid) = 0;

  //----------------------------------------------------------------------------
  //! Reset mgm information
  //!
  //! @param fsid file system id
  //!
  //! return true if information has been reset successfully
  //----------------------------------------------------------------------------
  virtual bool ResetMgmInformation(eos::common::FileSystem::fsid_t fsid) = 0;

  //----------------------------------------------------------------------------
  //! Update fmd from disk contents
  //----------------------------------------------------------------------------
  virtual bool UpdateFromDisk(eos::common::FileSystem::fsid_t fsid,
                              eos::common::FileId::fileid_t fid,
                              unsigned long long disksize,
                              std::string diskchecksum,
                              unsigned long checktime, bool filecxerror,
                              bool blockcxerror, bool flaglayouterror) = 0;

  //----------------------------------------------------------------------------
  //! Update fmd from mgm contents
  //----------------------------------------------------------------------------
  virtual bool UpdateFromMgm(eos::common::FileSystem::fsid_t fsid,
                             eos::common::FileId::fileid_t fid,
                             eos::common::FileId::fileid_t cid,
                             eos::common::LayoutId::layoutid_t lid,
                             unsigned long long mgmsize,
                             std::string mgmchecksum,
                             uid_t uid, gid_t gid,
                             unsigned long long ctime,
                             unsigned long long ctime_ns,
                             unsigned long long mtime,
                             unsigned long long mtime_ns,
                             int layouterror, std::string locations) = 0;

  //----------------------------------------------------------------------------
  //! Resync all Fmd info of files found under path
  //----------------------------------------------------------------------------
  virtual bool ResyncAllDisk(const char* path,
                             eos::common::FileSystem::fsid_t fsid,
                             bool flaglayouterror) = 0;

  //----------------------------------------------------------------------------
  //! Resync a single entry from disk
  //----------------------------------------------------------------------------
  virtual bool ResyncDisk(const char* fstpath,
                          eos::common::FileSystem::fsid_t fsid,
                          bool flaglayouterror,
                          bool callautorepair) = 0;

  //----------------------------------------------------------------------------
  //! Resync a single entry from Mgm
  //----------------------------------------------------------------------------
  virtual bool ResyncMgm(eos::common::FileSystem::fsid_t fsid,
                         eos::common::FileId::fileid_t fid,
                         const char* manager) = 0;

  //----------------------------------------------------------------------------
  //! Resync all entries from Mgm
  //----------------------------------------------------------------------------
  virtual bool ResyncAllMgm(eos::common::FileSystem::fsid_t fsid,
                            const char* manager) = 0;

  //----------------------------------------------------------------------------
  //! GetIncosistencyStatistics
  //----------------------------------------------------------------------------
  virtual bool
  GetInconsistencyStatistics(eos::common::FileSystem::fsid_t fsid,
                             std::map<std::string, size_t>& statistics,
                             std::map<std::string,
                             std::set<eos::common::FileId::fileid_t>>& fidset) = 0;

  //----------------------------------------------------------------------------
  //! Initialize the DB
  //----------------------------------------------------------------------------
  virtual bool ResetDB(eos::common::FileSystem::fsid_t fsid) = 0;

  XrdOucString DBDir; //< path to the directory with the DBs
  eos::common::RWMutex Mutex; //< Mutex protecting the Fmd handler

protected:
  std::map<eos::common::FileSystem::fsid_t, std::string> DBfilename;
  std::map<eos::common::FileSystem::fsid_t, bool> isDirty;
  std::map<eos::common::FileSystem::fsid_t, bool> stayDirty;
  std::map<eos::common::FileSystem::fsid_t, bool> isSyncing;
};

EOSFSTNAMESPACE_END

#endif
