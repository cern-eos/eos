// ----------------------------------------------------------------------
// File: FmdHelper.hh
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

/**
 * @file   FmdHelper.hh
 * 
 * @brief  Classes for FST File Meta Data Handling.
 * 
 * 
 */

#ifndef __EOSFST_FmdLEVELDB_HH__
#define __EOSFST_FmdLEVELDB_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "common/Logging.hh"
#include "common/SymKeys.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include "common/LayoutId.hh"
#include "common/DbMap.hh"
#include "fst/FmdHandler.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
// this is needed because of some openssl definition conflict!
#undef des_set_key
#include <google/dense_hash_map>
#include <google/sparse_hash_map>
#include <google/sparsehash/densehashtable.h>
#include <sys/time.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <zlib.h>
#include <openssl/sha.h>

#ifdef __APPLE__
#define ECOMM 70
#endif

EOSFSTNAMESPACE_BEGIN

// ---------------------------------------------------------------------------
//! Class handling many Fmd changelog files at a time
// ---------------------------------------------------------------------------

class FmdDbMapHandler : public FmdHandler
{
public:
  typedef std::vector<std::map< std::string, XrdOucString > > qr_result_t;

  XrdOucString DBDir; //< path to the directory with the SQLITE DBs
  eos::common::RWMutex Mutex;//< Mutex protecting the Fmd handler

  // ---------------------------------------------------------------------------
  //! Define a DB file for a filesystem id
  // ---------------------------------------------------------------------------
  virtual bool SetDBFile (const char* dbfile, int fsid, XrdOucString option = "");

  // ---------------------------------------------------------------------------
  //! Shutdown a DB for a filesystem
  // ---------------------------------------------------------------------------
  virtual bool ShutdownDB (eos::common::FileSystem::fsid_t fsid);

  // ---------------------------------------------------------------------------
  //! Read all Fmd entries from a DB file
  // ---------------------------------------------------------------------------
  //virtual bool ReadDBFile (eos::common::FileSystem::fsid_t, XrdOucString option = "");

  // ---------------------------------------------------------------------------
  //! Trim a DB file
  // ---------------------------------------------------------------------------
  virtual bool TrimDBFile (eos::common::FileSystem::fsid_t fsid, XrdOucString option = "");

  // the meta data handling functions

  // ---------------------------------------------------------------------------
  //! attach or create a fmd record
  // ---------------------------------------------------------------------------
  virtual FmdHelper* GetFmd (eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid, uid_t uid, gid_t gid, eos::common::LayoutId::layoutid_t layoutid, bool isRW = false, bool force = false);

  // ---------------------------------------------------------------------------
  //! Delete an fmd record
  // ---------------------------------------------------------------------------
  virtual bool DeleteFmd (eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid);

  inline bool ExistFmd(eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid)
  {
    eos::common::DbMap::Tval val;
    if(!dbmap.count(fsid))
    return false;
    bool retval=dbmap[fsid]->get(eos::common::Slice((const char*)&fid,sizeof(fid)),&val);
    //eos_warning("ExistFmd fid=%lu fsid=%u result=%d",fid,fsid,retval);
    return retval;
  }
  inline Fmd RetrieveFmd(eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid)
  {
    eos::common::DbMap::Tval val;
    dbmap[fsid]->get(eos::common::Slice((const char*)&fid,sizeof(fid)),&val);
    Fmd retval;
    retval.ParseFromString(val.value);
    std::stringstream ss;
    //eos_warning("RetrieveFmd fid=%lu fsid=%u getfid=%lu getfsid=%u",fid,fsid,retval.fid(),retval.fsid());
    return retval;
  }
  inline bool PutFmd(eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid, const Fmd &fmd)
  {
    std::string sval;
    fmd.SerializePartialToString(&sval);
    //eos_warning("RetrieveFmd fid=%lu fsid=%u setfid=%lu setfsid=%u",fid,fsid,fmd.fid(),fmd.fsid());
    return dbmap[fsid]->set(eos::common::Slice((const char*)&fid,sizeof(fid)),sval,"") == 0;
  }

  // ---------------------------------------------------------------------------
  //! Commit a modified fmd record
  // ---------------------------------------------------------------------------
  virtual bool Commit (FmdHelper* fmd, bool lockit = true);

  // ---------------------------------------------------------------------------
  //! Commit a modified fmd record without locks and change of modification time
  // ---------------------------------------------------------------------------
  //virtual bool CommitFromMemory (eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid);

  // ---------------------------------------------------------------------------
  //! Reset Disk Information
  // ---------------------------------------------------------------------------
  virtual bool ResetDiskInformation (eos::common::FileSystem::fsid_t fsid);

  // ---------------------------------------------------------------------------
  //! Reset Mgm Information
  // ---------------------------------------------------------------------------
  virtual bool ResetMgmInformation (eos::common::FileSystem::fsid_t fsid);

  // ---------------------------------------------------------------------------
  //! Update fmd from disk contents
  // ---------------------------------------------------------------------------
  virtual bool UpdateFromDisk (eos::common::FileSystem::fsid_t fsid, eos::common::FileId::fileid_t fid, unsigned long long disksize, std::string diskchecksum, unsigned long checktime, bool filecxerror, bool blockcxerror, bool flaglayouterror);

  // ---------------------------------------------------------------------------
  //! Update fmd from mgm contents
  // ---------------------------------------------------------------------------
  virtual bool UpdateFromMgm (eos::common::FileSystem::fsid_t fsid, eos::common::FileId::fileid_t fid, eos::common::FileId::fileid_t cid, eos::common::LayoutId::layoutid_t lid, unsigned long long mgmsize, std::string mgmchecksum, uid_t uid, gid_t gid, unsigned long long ctime, unsigned long long ctime_ns, unsigned long long mtime, unsigned long long mtime_ns, int layouterror, std::string locations);

  // ---------------------------------------------------------------------------
  //! Resync File meta data found under path
  // ---------------------------------------------------------------------------
  virtual bool ResyncAllDisk (const char* path, eos::common::FileSystem::fsid_t fsid, bool flaglayouterror);

  // ---------------------------------------------------------------------------
  //! Resync a single entry from Disk
  // ---------------------------------------------------------------------------
  virtual bool ResyncDisk (const char* fstpath, eos::common::FileSystem::fsid_t fsid, bool flaglayouterror);

  // ---------------------------------------------------------------------------
  //! Resync a single entry from Mgm
  // ---------------------------------------------------------------------------
  virtual bool ResyncMgm (eos::common::FileSystem::fsid_t fsid, eos::common::FileId::fileid_t fid, const char* manager);

  // ---------------------------------------------------------------------------
  //! Resync all entries from Mgm
  // ---------------------------------------------------------------------------
  virtual bool ResyncAllMgm (eos::common::FileSystem::fsid_t fsid, const char* manager);

  // ---------------------------------------------------------------------------
  //! Query list of fids
  // ---------------------------------------------------------------------------
  virtual size_t Query (eos::common::FileSystem::fsid_t fsid, std::string query, std::vector<eos::common::FileId::fileid_t> &fidvector);

  // ---------------------------------------------------------------------------
  //! GetIncosistencyStatistics
  // ---------------------------------------------------------------------------
  virtual bool GetInconsistencyStatistics (eos::common::FileSystem::fsid_t fsid, std::map<std::string, size_t> &statistics, std::map<std::string, std::set < eos::common::FileId::fileid_t> > &fidset);

  // ---------------------------------------------------------------------------
  //! Initialize the changelog hash
  // ---------------------------------------------------------------------------

  virtual void
  Reset (eos::common::FileSystem::fsid_t fsid)
  {
    // you need to lock the RWMutex Mutex before calling this
    FmdHelperMap[fsid].clear();
  }

  // ---------------------------------------------------------------------------
  //! Initialize the SQL DB
  // ---------------------------------------------------------------------------
  virtual bool ResetDB (eos::common::FileSystem::fsid_t fsid);
  virtual bool TrimDB ();

  // ---------------------------------------------------------------------------
  //! Comparison function for modification times
  // ---------------------------------------------------------------------------
  static int CompareMtime (const void* a, const void *b);

  // that is all we need for meta data handling

  // ---------------------------------------------------------------------------
  //! Hash map pointing from fid to offset in changelog file
  // ---------------------------------------------------------------------------
  google::sparse_hash_map<eos::common::FileSystem::fsid_t, google::dense_hash_map<unsigned long long, struct Fmd > > FmdHelperMap;

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------

  FmdDbMapHandler ()
  {
    SetLogId("CommonFmdDbMapHandler");
#ifndef EOS_SQLITE_DBMAP
    lvdboption.CacheSizeMb=0;
    lvdboption.BloomFilterNbits=0;
#endif
  }

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------

  virtual ~FmdDbMapHandler ()
  {
    Shutdown();
  }

  // ---------------------------------------------------------------------------
  //! Shutdown
  // ---------------------------------------------------------------------------

  void
  Shutdown ()
  {
    // detach all opened db's
    std::map<eos::common::FileSystem::fsid_t, eos::common::DbMap*>::const_iterator it;
    for (it = dbmap.begin(); it != dbmap.end(); it++)
    {
      ShutdownDB(it->first);
    }

    {
      // remove all
      eos::common::RWMutexWriteLock lock(Mutex);
      dbmap.clear();
    }
  }

  // ---------------------------------------------------------------------------
  //! Shutdown
  // ---------------------------------------------------------------------------

#ifndef EOS_SQLITE_DBMAP
  void
  SetLevelDbCacheMb ( const unsigned &sizemb )
  {
    eos_info("setting up LevelDb cache size %u MB",sizemb);
    lvdboption.CacheSizeMb = sizemb;
  }

  void
  SetLevelDbBloomLength ( const unsigned &bloomlength )
  {
    eos_info("setting up LevelDb bloom filter length %u bit",bloomlength);
    lvdboption.BloomFilterNbits = bloomlength;
  }
#endif

  // ---------------------------------------------------------------------------
  //! Hash map pointing from fid to offset in changelog file
  // ---------------------------------------------------------------------------
  google::sparse_hash_map<eos::common::FileSystem::fsid_t, eos::common::DbMap* > FmdMap;

private:
  std::map<eos::common::FileSystem::fsid_t, eos::common::DbMap*> dbmap;
#ifndef EOS_SQLITE_DBMAP
  eos::common::LvDbDbMapInterface::Option lvdboption;
#endif
  std::map<eos::common::FileSystem::fsid_t, std::string> DBfilename;
};

// ---------------------------------------------------------------------------
extern FmdDbMapHandler gFmdDbMapHandler;

EOSFSTNAMESPACE_END

#endif
