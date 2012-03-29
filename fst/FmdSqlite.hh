// ----------------------------------------------------------------------
// File: FmdSqlite.hh
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
 * @file   FmdSqlite.hh
 * 
 * @brief  Classes for FST File Meta Data Handling.
 * 
 * 
 */

#ifndef __EOSFST_FMDSQLITE_HH__
#define __EOSFST_FMDSQLITE_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "common/Logging.hh"
#include "common/SymKeys.hh"
#include "common/ClientAdmin.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include "common/LayoutId.hh"
#include "common/sqlite/sqlite3.h"
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
//! Class implementing file meta data
// ---------------------------------------------------------------------------
class FmdSqlite : public eos::common::LogId {

public:
  // ---------------------------------------------------------------------------
  //! In-memory entry struct
  // ---------------------------------------------------------------------------
  struct FMD {
    eos::common::FileId::fileid_t fid;       //< fileid
    eos::common::FileId::fileid_t cid;       //< container id (e.g. directory id)
    eos::common::FileSystem::fsid_t fsid;//< filesystem id
    unsigned long ctime;          //< creation time 
    unsigned long ctime_ns;       //< ns of creation time
    unsigned long mtime;          //< modification time | deletion time
    unsigned long mtime_ns;       //< ns of modification time
    unsigned long atime;          //< access time
    unsigned long atime_ns;       //< ns of access time 
    unsigned long checktime;      //< time of last checksum scan
    unsigned long long size;      //< size              - 0xfffffff1ULL means it is still undefined
    unsigned long long disksize;  //< size on disk      - 0xfffffff1ULL means it is still undefined
    unsigned long long mgmsize;   //< size on the MGM   - 0xfffffff1ULL means it is still undefined
    std::string checksum;         //< checksum in hex representation
    std::string diskchecksum;     //< checksum in hex representation
    std::string mgmchecksum;      //< checksum in hex representation
    eos::common::LayoutId::layoutid_t lid ;    //< layout id
    uid_t uid;                    //< user  id
    gid_t gid;                    //< roup id
    std::string name;             //< name
    std::string container;        //< container
    int filecxerror;              //< indicator for file checksum error
    int blockcxerror;             //< indicator for block checksum error
    int layouterror;              //< indicator for resync errors e.g. the mgm layout information is inconsistent e.g. only 1 of 2 replicas exist
    std::string locations;        //< fsid list with locations e.g. 1,2,3,4,10

    // Copy assignment operator
    FMD &operator = ( const FMD &fmd ) {
      fid      = fmd.fid;
      fsid     = fmd.fsid;
      cid      = fmd.cid;
      ctime    = fmd.ctime;
      ctime_ns = fmd.ctime_ns;
      mtime    = fmd.mtime;
      mtime_ns = fmd.mtime_ns;
      atime    = fmd.atime;
      atime_ns = fmd.atime_ns;
      checktime= fmd.checktime;
      size     = fmd.size;
      disksize = fmd.disksize;
      mgmsize  = fmd.mgmsize;
      checksum = fmd.checksum;
      diskchecksum = fmd.diskchecksum;
      mgmchecksum  = fmd.mgmchecksum;
      lid      = fmd.lid;
      uid      = fmd.uid;
      gid      = fmd.gid;
      name     = fmd.name;
      container    = fmd.container;
      filecxerror  = fmd.filecxerror;
      blockcxerror = fmd.blockcxerror;
      layouterror  = fmd.layouterror;
      locations    = fmd.locations;
      return *this;
    }
  };

  // ---------------------------------------------------------------------------
  //! File Metadata object 
  struct FMD fMd;
  // ---------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  //! File meta data object replication function (copy constructor)
  // ---------------------------------------------------------------------------

  void Replicate(struct FMD &fmd) {
    fMd = fmd ;
  }

  // ---------------------------------------------------------------------------
  //! Compute layout error
  // ---------------------------------------------------------------------------

  static int LayoutError(eos::common::FileSystem::fsid_t fsid, eos::common::LayoutId::layoutid_t lid,  std::string locations) {
    if (lid == 0) {
      // an orphans has not lid at the MGM e.g. lid=0
      return eos::common::LayoutId::kOrphan;
    }

    int lerror = 0;
    std::vector<std::string> location_vector;
    std::set<eos::common::FileSystem::fsid_t> location_set;
    eos::common::StringConversion::Tokenize(locations, location_vector,",");

    for (size_t i=0; i< location_vector.size(); i++) {
      if (location_vector[i].length()) {
	// unlinked locates have a '!' infront of the fsid
	if (location_vector[i][0] == '!') {
	  location_set.insert(strtoul(location_vector[i].c_str()+1,0,10));
	} else {
	  location_set.insert(strtoul(location_vector[i].c_str(),0,10));
	}
      }
    }
    size_t nstripes = eos::common::LayoutId::GetStripeNumber(lid)+1;
    if (nstripes != location_vector.size()) {
      lerror |= eos::common::LayoutId::kReplicaWrong;
    }
    if (! location_set.count(fsid)) {
      lerror |= eos::common::LayoutId::kUnregistered;
    }
    return lerror;
  }

  // ---------------------------------------------------------------------------
  //! File meta data object reset function
  // ---------------------------------------------------------------------------

  static void Reset(struct FMD &fmd) {
    fmd.fid   = 0;
    fmd.cid   = 0;
    fmd.ctime = 0;
    fmd.ctime_ns = 0;
    fmd.mtime = 0;
    fmd.mtime_ns = 0;
    fmd.atime = 0;
    fmd.atime_ns = 0;
    fmd.checktime = 0;
    fmd.size     = 0xfffffff1ULL;
    fmd.disksize = 0xfffffff1ULL;
    fmd.mgmsize  = 0xfffffff1ULL;
    fmd.checksum = "";
    fmd.diskchecksum = "";
    fmd.mgmchecksum = "";
    fmd.lid   = 0;
    fmd.uid   = 0;
    fmd.gid   = 0;
    fmd.name  = "";
    fmd.container = "";
    fmd.filecxerror = 0;
    fmd.blockcxerror = 0;
    fmd.layouterror = 0;
    fmd.locations = "";
  }

  // ---------------------------------------------------------------------------
  //! Dump FMD
  // ---------------------------------------------------------------------------
  static void Dump(struct FMD* fmd);
  
  // ---------------------------------------------------------------------------
  //! Convert FMD into env representation
  // ---------------------------------------------------------------------------
  XrdOucEnv* FmdSqliteToEnv();

  // ---------------------------------------------------------------------------
  //! Convert an FST env representation into FMD
  // ---------------------------------------------------------------------------
  static bool EnvFstToFmdSqlite(XrdOucEnv &env, struct FmdSqlite::FMD &fmd);

  // ---------------------------------------------------------------------------
  //! Convert an MGM env representation into FMD
  // ---------------------------------------------------------------------------
  static bool EnvMgmToFmdSqlite(XrdOucEnv &env, struct FmdSqlite::FMD &fmd);

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  FmdSqlite(int fid=0, int fsid=0) {Reset(fMd); LogId();fMd.fid=fid; fMd.fsid=fsid;};
  
  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~FmdSqlite() {};
};

// ---------------------------------------------------------------------------
//! Class handling many FMD changelog files at a time
// ---------------------------------------------------------------------------

class FmdSqliteHandler : public eos::common::LogId {
  typedef std::vector<std::map< std::string, std::string > > qr_result_t;
private:
  bool isOpen;
  qr_result_t Qr;
  std::map<eos::common::FileSystem::fsid_t, sqlite3*> DB;
  std::map<eos::common::FileSystem::fsid_t, std::string> DBfilename;


  char* ErrMsg;
  static int CallBack(void * object, int argc, char **argv, char **ColName);
  std::map<eos::common::FileSystem::fsid_t, bool> isDirty;


public:
  XrdOucString DBDir;            //< path to the directory with the SQLITE DBs
  eos::common::RWMutex Mutex;                 //< Mutex protecting the FMD handler


  std::map<eos::common::FileSystem::fsid_t, sqlite3*> *GetDB() {return &DB;}


  // ---------------------------------------------------------------------------
  //! Return's the dirty flag indicating a non-clean shutdown
  // ---------------------------------------------------------------------------
  bool IsDirty(eos::common::FileSystem::fsid_t fsid) { return isDirty[fsid];}

  // ---------------------------------------------------------------------------
  //! Define a DB file for a filesystem id
  // ---------------------------------------------------------------------------
  bool SetDBFile(const char* dbfile, int fsid, XrdOucString option="") ;

  // ---------------------------------------------------------------------------
  //! Shutdown a DB for a filesystem
  // ---------------------------------------------------------------------------
  bool ShutdownDB(eos::common::FileSystem::fsid_t fsid);

  // ---------------------------------------------------------------------------
  //! Read all FMD entries from a DB file
  // ---------------------------------------------------------------------------
  bool ReadDBFile(eos::common::FileSystem::fsid_t, XrdOucString option="");

  // ---------------------------------------------------------------------------
  //! Trim a DB file
  // ---------------------------------------------------------------------------
  bool TrimDBFile(eos::common::FileSystem::fsid_t fsid, XrdOucString option="");

  // the meta data handling functions
  
  // ---------------------------------------------------------------------------
  //! attach or create a fmd record
  // ---------------------------------------------------------------------------
  FmdSqlite* GetFmd(eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid, uid_t uid, gid_t gid, eos::common::LayoutId::layoutid_t layoutid, bool isRW=false, bool force=false);

  // ---------------------------------------------------------------------------
  //! Delete an fmd record
  // ---------------------------------------------------------------------------
  bool DeleteFmd(eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid);

  // ---------------------------------------------------------------------------
  //! Commit a modified fmd record
  // ---------------------------------------------------------------------------
  bool Commit(FmdSqlite* fmd);

  // ---------------------------------------------------------------------------
  //! Commit a modified fmd record without locks and change of modification time
  // ---------------------------------------------------------------------------
  bool CommitFromMemory(eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid);

  // ---------------------------------------------------------------------------
  //! Reset Disk Information
  // ---------------------------------------------------------------------------
  bool ResetDiskInformation(eos::common::FileSystem::fsid_t fsid);

  // ---------------------------------------------------------------------------
  //! Reset Mgm Information
  // ---------------------------------------------------------------------------
  bool ResetMgmInformation(eos::common::FileSystem::fsid_t fsid);

  // ---------------------------------------------------------------------------
  //! Update fmd from disk contents
  // ---------------------------------------------------------------------------
  bool UpdateFromDisk(eos::common::FileSystem::fsid_t fsid, eos::common::FileId::fileid_t fid, unsigned long long disksize, std::string diskchecksum, unsigned long checktime, bool filecxerror, bool blockcxerror, bool flaglayouterror);


  // ---------------------------------------------------------------------------
  //! Update fmd from mgm contents
  // ---------------------------------------------------------------------------
  bool UpdateFromMgm(eos::common::FileSystem::fsid_t fsid, eos::common::FileId::fileid_t fid, eos::common::FileId::fileid_t cid, eos::common::LayoutId::layoutid_t lid, unsigned long long mgmsize, std::string mgmchecksum, std::string name, std::string container, uid_t uid, gid_t gid, unsigned long long ctime, unsigned long long ctime_ns, unsigned long long mtime, unsigned long long mtime_ns, int layouterror, std::string locations);

  // ---------------------------------------------------------------------------
  //! Resync File meta data found under path
  // ---------------------------------------------------------------------------
  bool ResyncDisk(const char* path, eos::common::FileSystem::fsid_t fsid, bool flaglayouterror);

  // ---------------------------------------------------------------------------
  //! Resync a single entry from Mgm
  // ---------------------------------------------------------------------------
  bool ResyncMgm(eos::common::FileSystem::fsid_t fsid, eos::common::FileId::fileid_t fid, const char* manager);

  // ---------------------------------------------------------------------------
  //! Resync all entries from Mgm
  // ---------------------------------------------------------------------------
  bool ResyncAllMgm(eos::common::FileSystem::fsid_t fsid, const char* manager);
  
  // ---------------------------------------------------------------------------
  //! Query list of fids
  // ---------------------------------------------------------------------------
  size_t Query(eos::common::FileSystem::fsid_t fsid, std::string query, std::vector<eos::common::FileId::fileid_t> &fidvector);

  // ---------------------------------------------------------------------------
  //! GetIncosistencyStatistics
  // ---------------------------------------------------------------------------
  bool GetInconsistencyStatistics(eos::common::FileSystem::fsid_t fsid, std::map<std::string, size_t> &statistics, std::map<std::string , std::set < eos::common::FileId::fileid_t> > &fidset);

  // ---------------------------------------------------------------------------
  //! Initialize the changelog hash
  // ---------------------------------------------------------------------------
  void Reset(eos::common::FileSystem::fsid_t fsid) {
    // you need to lock the RWMutex Mutex before calling this
    FmdSqliteMap[fsid].clear();
  }
    
  // ---------------------------------------------------------------------------
  //! Comparison function for modification times
  // ---------------------------------------------------------------------------
  static int CompareMtime(const void* a, const void *b);

  // that is all we need for meta data handling

  // ---------------------------------------------------------------------------
  //! Hash map pointing from fid to offset in changelog file
  // ---------------------------------------------------------------------------
  google::sparse_hash_map<eos::common::FileSystem::fsid_t, google::dense_hash_map<unsigned long long, struct FmdSqlite::FMD > > FmdSqliteMap;

  // ---------------------------------------------------------------------------
  //! Create a new changelog filename in 'dir' (the fsid suffix is not added!)
  // ---------------------------------------------------------------------------
  const char* CreateDBFileName(const char* cldir, XrdOucString &clname) {
    clname = cldir; clname += "/"; clname += "fmd";
    return clname.c_str();
  }

  // ---------------------------------------------------------------------------
  //! Retrieve FMD from a remote machine
  // ---------------------------------------------------------------------------
  int GetRemoteFmdSqlite(const char* manager, const char* shexfid, const char* sfsid, struct FmdSqlite::FMD &fmd);
  int GetMgmFmdSqlite(const char* manager, eos::common::FileId::fileid_t fid, struct FmdSqlite::FMD &fmd);

  int GetRemoteAttribute(const char* manager, const char* key, const char* path, XrdOucString& attribute);

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  FmdSqliteHandler() {
    SetLogId("CommonFmdSqliteHandler");
  }

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~FmdSqliteHandler() {
    // clean-up all open DB handles
    std::map<eos::common::FileSystem::fsid_t, sqlite3*>::const_iterator it;
    for (it = DB.begin(); it != DB.end(); it++) {
      ShutdownDB(it->first);
    }
  }
};


// ---------------------------------------------------------------------------
extern FmdSqliteHandler gFmdSqliteHandler;

EOSFSTNAMESPACE_END

#endif
