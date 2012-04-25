// ----------------------------------------------------------------------
// File: FmdSqlite.cc
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

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "common/FileId.hh"
#include "common/Path.hh"
#include "common/Attr.hh"
#include "fst/FmdSqlite.hh"
#include "fst/checksum/ChecksumPlugins.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <stdio.h>
#include <sys/mman.h>
#include <fts.h>
#include <iostream>
#include <fstream>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
FmdSqliteHandler gFmdSqliteHandler; //< static 
/*----------------------------------------------------------------------------*/


/*----------------------------------------------------------------------------*/
/** 
 * Callback function for SQLITE3 calls
 * 
 * @param see sqlite manual
 * 
 * @return fills qr_result vector
 */
/*----------------------------------------------------------------------------*/
int
FmdSqliteHandler::CallBack(void *object, int argc, char **argv, char **ColName)
{
  FmdSqliteHandler* tx = (FmdSqliteHandler*) object;
  int i=tx->Qr.size();
  tx->Qr.resize(i+1);
  for (int k=0; k< argc; k++) {
    tx->Qr[i][ColName[k]] = argv[k]?argv[k]:"";
  }
  return 0;
}


/*----------------------------------------------------------------------------*/
/** 
 * Dump an FMD record to stderr
 * 
 * @param fmd handle to the FMD struct
 */
/*----------------------------------------------------------------------------*/
void
FmdSqlite::Dump(struct FMD* fmd) {
  fprintf(stderr,"%08llx %06llu %04lu %010lu %010lu %010lu %010lu %010lu %010lu %010lu %08llu %08llu %08llu %s %s %s %03lu %05u %05u %32s %s\n",
          fmd->fid,
          fmd->cid,
          (unsigned long)fmd->fsid,
          fmd->ctime,
          fmd->ctime_ns,
          fmd->mtime,
          fmd->mtime_ns,
          fmd->atime,
          fmd->atime_ns,
          fmd->checktime,
          fmd->size,
          fmd->disksize,
          fmd->mgmsize,
          fmd->checksum.c_str(),
	  fmd->diskchecksum.c_str(),
	  fmd->mgmchecksum.c_str(),
          fmd->lid,
          fmd->uid,
          fmd->gid,
          fmd->name.c_str(),
          fmd->container.c_str());
}

/*----------------------------------------------------------------------------*/
/** 
 * Set a new DB file for a filesystem id.
 * 
 * @param dbfilename path to the sqlite db file
 * @param fsid filesystem id identified by this file
 * @param option  - not used.
 * 
 * @return true if successfull false if failed
 */
/*----------------------------------------------------------------------------*/
bool
FmdSqliteHandler::SetDBFile(const char* dbfileprefix, int fsid, XrdOucString option) 
{
  eos_debug("");

  bool isattached = false;
  {
    // we first check if we have already this DB open - in this case we first do a shutdown
    eos::common::RWMutexReadLock lock(Mutex);
    if (DB.count(fsid)) {
      isattached = true;
    }
  }

  if (isattached) {
    ShutdownDB(fsid);
  }

  eos::common::RWMutexWriteLock lock(Mutex);

  //! -when we successfully attach to an SQLITE3 DB we set the mode to S_IRUSR
  //! -when we shutdown the daemon clean we set the mode back to S_IRWXU
  //! -when we attach and the mode is S_IRUSR we know that the DB has not been shutdown properly and we set a 'dirty' flag to force a full resynchronization

  if (!FmdSqliteMap.count(fsid)) {
    FmdSqliteMap[fsid].set_empty_key(0xffffffffeULL);
    FmdSqliteMap[fsid].set_deleted_key(0xffffffffULL);
  }

  char fsDBFileName[1024];
  sprintf(fsDBFileName,"%s.%04d.sql", dbfileprefix,fsid);
  eos_info("SQLITE DB is now %s\n", fsDBFileName);

  // store the DB file name
  DBfilename[fsid] = fsDBFileName;

  // check the mode of the DB
  struct stat buf;
  int src=0;
  if ((src=stat(fsDBFileName, &buf)) || ((buf.st_mode&S_IRWXU)!= S_IRWXU) ) {
    isDirty[fsid] = true;
    eos_warning("setting sqlite3 file dirty - unclean shutdown detected");
    if (!src) {
      if (chmod(DBfilename[fsid].c_str(),S_IRWXU)) {
	eos_crit("failed to switch the sqlite3 database file mode to S_IRWXU errno=%d",errno);
      }
    }
  } else {
    isDirty[fsid] = false;
  }

  // create the SQLITE DB
  if ((sqlite3_open(fsDBFileName,&DB[fsid]) == SQLITE_OK)) {
    XrdOucString createtable = "CREATE TABLE if not exists fst ( fid integer PRIMARY KEY, cid integer, fsid integer, ctime integer, ctime_ns integer, mtime integer, mtime_ns integer, atime integer, atime_ns integer, checktime integer, size integer, disksize integer, mgmsize integer, checksum varchar(32), diskchecksum varchar(32), mgmchecksum varchar(32), lid integer, uid integer, gid integer, name varchar(1024), container varchar(1024), filecxerror integer, blockcxerror integer, layouterror integer, locations varchar(128))";
    if ((sqlite3_exec(DB[fsid],createtable.c_str(), CallBack, this, &ErrMsg))) {
      eos_err("unable to create <fst> table - msg=%s\n",ErrMsg);
      return false;
    }
  } else {
    eos_err("failed to open sqlite3 database file %s - msg=%s\n", fsDBFileName, sqlite3_errmsg(DB[fsid]));
    return false;
  }

  // set the mode to S_IRUSR
  if (chmod(fsDBFileName,S_IRUSR)) {
    eos_crit("failed to switch the sqlite3 database file mode to S_IRUSR errno=%d",errno);
  }

  return ReadDBFile(fsid, option);
}

/*----------------------------------------------------------------------------*/
/** 
 * Shutdown an open DB file
 * 
 * @param fsid filesystem id identifier
 * 
 * @return true if successfull false if failed
 */
/*----------------------------------------------------------------------------*/
bool
FmdSqliteHandler::ShutdownDB(eos::common::FileSystem::fsid_t fsid)
{
  eos::common::RWMutexWriteLock lock(Mutex);
  eos_info("SQLITE DB shutdown for fsid=%lu\n", (unsigned long)fsid);
  if (DB.count(fsid)) {
    if (DBfilename.count(fsid)) {
      // set the mode back to S_IRWXU
      if (chmod(DBfilename[fsid].c_str(),S_IRWXU)) {
	eos_crit("failed to switch the sqlite3 database file to S_IRWXU errno=%d", errno);
      }
    }
    if ( (sqlite3_close(DB[fsid]) ==  SQLITE_OK) ) {
      DB.erase(fsid);
      DBfilename.erase(fsid);
      return true;
    }
    DB.erase(fsid);
    DBfilename.erase(fsid);
  }
  return false;
}



/*----------------------------------------------------------------------------*/
/** 
 * Comparison function for modification times
 * 
 * @param a pointer to a filestat struct
 * @param b pointer to a filestat struct
 * 
 * @return difference between the two modification times within the filestat struct
 */
/*----------------------------------------------------------------------------*/
int 
FmdSqliteHandler::CompareMtime(const void* a, const void *b) {
  struct filestat {
    struct stat buf;
    char filename[1024];
  };
  return ( (((struct filestat*)b)->buf.st_mtime) - ((struct filestat*)a)->buf.st_mtime);
}


/*----------------------------------------------------------------------------*/
/** 
 * Read the contents of the DB file into the memory hash
 * 
 * @param fsid filesystem id to read
 * @param option - not used
 * 
 * @return true if all records read and valid otherwise false
 */
/*----------------------------------------------------------------------------*/
bool FmdSqliteHandler::ReadDBFile(eos::common::FileSystem::fsid_t fsid, XrdOucString option) 
{
  // needs the write-lock on gFmdHandler::Mutex
  eos_debug("");

  struct timeval tv1,tv2;
  struct timezone tz;

  gettimeofday(&tv1,&tz);

  Reset(fsid);
  FmdSqliteMap.resize(0);
  
  gettimeofday(&tv2,&tz);
  
  Qr.clear();

  XrdOucString query="";
  query = "select * from fst";
  
  if ((sqlite3_exec(DB[fsid],query.c_str(), CallBack, this, &ErrMsg))) {
    eos_err("unable to query - msg=%s\n",ErrMsg);
    return false;
  }
  
  qr_result_t::const_iterator it;
  eos_info("Preloading %lu files into the memory hash", Qr.size());
  for (size_t i = 0; i< Qr.size(); i++) {
    eos::common::FileId::fileid_t fid = strtoull(Qr[i]["fid"].c_str(),0,10);
    FmdSqliteMap[fsid][fid].fid      = fid;
    FmdSqliteMap[fsid][fid].fsid     = fsid;
    FmdSqliteMap[fsid][fid].cid      = strtoull(Qr[i]["cid"].c_str(),0,10);
    FmdSqliteMap[fsid][fid].ctime    = strtoul(Qr[i]["ctime"].c_str(),0,10);
    FmdSqliteMap[fsid][fid].ctime_ns = strtoul(Qr[i]["ctime_ns"].c_str(),0,10);
    FmdSqliteMap[fsid][fid].mtime    = strtoul(Qr[i]["mtime"].c_str(),0,10);
    FmdSqliteMap[fsid][fid].mtime_ns = strtoul(Qr[i]["mtime_ns"].c_str(),0,10);
    FmdSqliteMap[fsid][fid].atime    = strtoul(Qr[i]["atime"].c_str(),0,10);
    FmdSqliteMap[fsid][fid].atime_ns = strtoul(Qr[i]["atime_ns"].c_str(),0,10);
    FmdSqliteMap[fsid][fid].checktime= strtoul(Qr[i]["checktime"].c_str(),0,10);
    FmdSqliteMap[fsid][fid].size     = strtoull(Qr[i]["size"].c_str(),0,10);
    FmdSqliteMap[fsid][fid].disksize = strtoull(Qr[i]["disksize"].c_str(),0,10);
    FmdSqliteMap[fsid][fid].mgmsize  = strtoull(Qr[i]["mgmsize"].c_str(),0,10);
    FmdSqliteMap[fsid][fid].checksum     = Qr[i]["checksum"];
    FmdSqliteMap[fsid][fid].diskchecksum = Qr[i]["diskchecksum"];
    FmdSqliteMap[fsid][fid].mgmchecksum  = Qr[i]["mgmchecksum"];
    FmdSqliteMap[fsid][fid].lid      = strtoul(Qr[i]["lid"].c_str(),0,10);
    FmdSqliteMap[fsid][fid].uid      = strtoul(Qr[i]["uid"].c_str(),0,10);
    FmdSqliteMap[fsid][fid].gid      = strtoul(Qr[i]["gid"].c_str(),0,10);
    FmdSqliteMap[fsid][fid].name     = Qr[i]["name"];
    FmdSqliteMap[fsid][fid].container = Qr[i]["container"];
    FmdSqliteMap[fsid][fid].filecxerror = atoi(Qr[i]["filecxerror"].c_str());
    FmdSqliteMap[fsid][fid].blockcxerror = atoi(Qr[i]["blockcxerror"].c_str());
    FmdSqliteMap[fsid][fid].layouterror = atoi(Qr[i]["layouterror"].c_str());
    FmdSqliteMap[fsid][fid].locations = Qr[i]["locations"].c_str();
  }
  
  return true;
}

/*----------------------------------------------------------------------------*/
/** 
 * Return or Create an FMD struct for the given file/filesystem id for user uid/gid and layout layoutid
 * 
 * @param fid file id
 * @param fsid filesystem id
 * @param uid user id of the caller
 * @param gid group id of the caller
 * @param layoutid layout id used to store during creation
 * @param isRW indicates if we create a not existing FMD 
 * 
 * @return pointer to FMD struct if successfull otherwise 0
 */
/*----------------------------------------------------------------------------*/
FmdSqlite*
FmdSqliteHandler::GetFmd(eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid, uid_t uid, gid_t gid, eos::common::LayoutId::layoutid_t layoutid, bool isRW, bool force) 
{

  eos_info("fid=%08llx fsid=%lu", fid, fsid);
  Mutex.LockRead();

  if (DB.count(fsid)) {
    if ( FmdSqliteMap.count(fsid) && FmdSqliteMap[fsid].count(fid)) {
      // this is to read an existing entry
      FmdSqlite* fmd = new FmdSqlite();
      if (!fmd) {
	Mutex.UnLockRead();
	return 0;
      }
      
      // make a copy of the current record
      fmd->Replicate(FmdSqliteMap[fsid][fid]);
      
      if ( fmd->fMd.fid != fid) {
        // fatal this is somehow a wrong record!
        eos_crit("unable to get fmd for fid %llu on fs %lu - file id mismatch in meta data block (%llu)", fid, fsid, fmd->fMd.fid);
        delete fmd;
	Mutex.UnLockRead();
        return 0;
      }
      
      if ( fmd->fMd.fsid != fsid) {
        // fatal this is somehow a wrong record!
        eos_crit("unable to get fmd for fid %llu on fs %lu - filesystem id mismatch in meta data block (%llu)", fid, fsid, fmd->fMd.fsid);
        delete fmd;
	Mutex.UnLockRead();
        return 0;
      }

      // the force flag allows to retrieve 'any' value even with inconsistencies as needed by ResyncAllMgm
      if (!force) {
	// if we have a mismatch between the mgm/disk and 'ref' value in size,  we don't return the FMD record
	if ( (fmd->fMd.disksize && (fmd->fMd.disksize != fmd->fMd.size)) ||
	     (fmd->fMd.mgmsize &&  (fmd->fMd.mgmsize  != fmd->fMd.size)) ) {
	  eos_crit("msg=\"size mismatch disk/mgm vs memory\" fid=%08llx fsid=%lu size=%llu disksize=%llu mgmsize=%llu", fid, fsid, fmd->fMd.size, fmd->fMd.disksize, fmd->fMd.mgmsize);
	  
	  delete fmd;
	  Mutex.UnLockRead();
	  return 0;
	}
	   
	// if we have a mismatch between the mgm/disk and 'ref' value in checksum, we don't return the FMD record
	if ( (fmd->fMd.diskchecksum.length() && (fmd->fMd.diskchecksum != fmd->fMd.checksum)) ||
	     (fmd->fMd.mgmchecksum.length() &&  (fmd->fMd.mgmchecksum  != fmd->fMd.checksum)) ) {
	  eos_crit("msg=\"checksum mismatch disk/mgm vs memory\" fid=%08llx fsid=%lu checksum=%s diskchecksum=%s mgmchecksum=%s", fid, fsid, fmd->fMd.checksum.c_str(), fmd->fMd.diskchecksum.c_str(), fmd->fMd.mgmchecksum.c_str());
	  
	  delete fmd;
	  Mutex.UnLockRead();
        return 0;
	}
      }
      
      // return the new entry
      Mutex.UnLockRead();
      return fmd;
    }
    
    if (isRW) {
      // make a new record

      struct timeval tv;
      struct timezone tz;

      gettimeofday(&tv, &tz);

      Mutex.UnLockRead(); // <--
      Mutex.LockWrite();  // -->

      FmdSqliteMap[fsid][fid].uid = uid;
      FmdSqliteMap[fsid][fid].gid = gid;
      FmdSqliteMap[fsid][fid].lid = layoutid;
      FmdSqliteMap[fsid][fid].fsid= fsid; 
      FmdSqliteMap[fsid][fid].fid = fid; 

      FmdSqliteMap[fsid][fid].ctime = FmdSqliteMap[fsid][fid].mtime = FmdSqliteMap[fsid][fid].atime = tv.tv_sec;
      FmdSqliteMap[fsid][fid].ctime_ns = FmdSqliteMap[fsid][fid].mtime_ns = FmdSqliteMap[fsid][fid].atime_ns = tv.tv_usec*1000;
      
      FmdSqlite* fmd = new FmdSqlite(fid,fsid);
      if (!fmd) return 0;
      
      // make a copy of the current record
      fmd->Replicate(FmdSqliteMap[fsid][fid]);

      Mutex.UnLockWrite(); // <--      
      if (Commit(fmd)) {
	eos_debug("returning meta data block for fid %d on fs %d", fid, fsid);
	// return the mmaped meta data block

        return fmd;
      } else {
        eos_crit("unable to write new block for fid %d on fs %d - no changelog db open for writing", fid, fsid);
        delete fmd;
        return 0;
      }
    } else {
      eos_warning("unable to get fmd for fid %llu on fs %lu - record not found", fid, fsid);
      Mutex.UnLockRead();
      return 0;
    }
  } else {
    eos_crit("unable to get fmd for fid %llu on fs %lu - there is no changelog file open for that file system id", fid, fsid);
    Mutex.UnLockRead();
    return 0;
  }
}


/*----------------------------------------------------------------------------*/
/** 
 * Delete a record associated with file id fid on filesystem fsid
 * 
 * @param fid file id
 * @param fsid filesystem id
 * 
 * @return true if deleted, false if it does not exist
 */
/*----------------------------------------------------------------------------*/
bool
FmdSqliteHandler::DeleteFmd(eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid) 
{
  bool rc = true;
  eos_static_info("");
  eos::common::RWMutexWriteLock lock(Mutex);
  // erase the hash entry
  if (FmdSqliteMap.count(fsid) && FmdSqliteMap[fsid].count(fid)) {
    // delete in the in-memory hash
    FmdSqliteMap[fsid].erase(fid);

    char deleteentry[16384];
    snprintf(deleteentry,sizeof(deleteentry),"delete from fst where fid=%llu",fid);


    Qr.clear();

    // delete in the local DB
    if ((sqlite3_exec(DB[fsid],deleteentry, CallBack, this, &ErrMsg))) {
      eos_err("unable to delete fid=%08llx from fst table - msg=%s\n",fid, ErrMsg);
      rc = false;
    } else {
      rc = true;
    }
  } else {
    rc = false;
  }
  return rc;
}


/*----------------------------------------------------------------------------*/
/** 
 * Commit FMD to the DB file
 * 
 * @param fmd pointer to FMD
 * 
 * @return true if record has been commited
 */
/*----------------------------------------------------------------------------*/
bool
FmdSqliteHandler::Commit(FmdSqlite* fmd)
{
  if (!fmd)
    return false;

  int fsid = fmd->fMd.fsid;
  int fid  = fmd->fMd.fid;

  struct timeval tv;
  struct timezone tz;
  
  gettimeofday(&tv, &tz);
  fmd->fMd.mtime = fmd->fMd.atime = tv.tv_sec;
  fmd->fMd.mtime_ns = fmd->fMd.atime_ns = tv.tv_usec * 1000;

  eos::common::RWMutexWriteLock lock(Mutex);

  if (FmdSqliteMap.count(fsid)) {
    // update in-memory
    FmdSqliteMap[fsid][fid] = fmd->fMd;
    return CommitFromMemory(fid,fsid);
  } else {
    eos_crit("no sqlite DB open for fsid=%llu", fsid);
  }

  return false;
}


/*----------------------------------------------------------------------------*/
/** 
 * Commit FMD to the DB file without locking and modification time changes
 * 
 * @param fmd pointer to FMD
 * 
 * @return true if record has been commited
 */
/*----------------------------------------------------------------------------*/
bool
FmdSqliteHandler::CommitFromMemory(eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid)
{
  if ( (!FmdSqliteMap.count(fsid)) || (!FmdSqliteMap[fsid].count(fid)) ) {
    return false;
  }
  char insertentry[16384];
  snprintf(insertentry,sizeof(insertentry),"insert or replace into fst(fid,fsid,cid,ctime,ctime_ns,mtime,mtime_ns,atime,atime_ns,checktime,size,disksize,mgmsize,checksum,diskchecksum,mgmchecksum, lid,uid,gid,name,container,filecxerror,blockcxerror,layouterror,locations) values ('%llu','%lu','%llu','%lu','%lu','%lu','%lu','%lu','%lu','%lu','%llu','%llu','%llu','%s','%s','%s','%lu','%u','%u','%s','%s','%d','%d','%d','%s')",
	   FmdSqliteMap[fsid][fid].fid,
	   (unsigned long)FmdSqliteMap[fsid][fid].fsid,
	   FmdSqliteMap[fsid][fid].cid,
	   FmdSqliteMap[fsid][fid].ctime,
	   FmdSqliteMap[fsid][fid].ctime_ns,
	   FmdSqliteMap[fsid][fid].mtime,
	   FmdSqliteMap[fsid][fid].mtime_ns,
	   FmdSqliteMap[fsid][fid].atime,
	   FmdSqliteMap[fsid][fid].atime_ns,
	   FmdSqliteMap[fsid][fid].checktime,
	   FmdSqliteMap[fsid][fid].size,
	   FmdSqliteMap[fsid][fid].disksize,
	   FmdSqliteMap[fsid][fid].mgmsize,
	   FmdSqliteMap[fsid][fid].checksum.c_str(),
	   FmdSqliteMap[fsid][fid].diskchecksum.c_str(),
	   FmdSqliteMap[fsid][fid].mgmchecksum.c_str(),
	   FmdSqliteMap[fsid][fid].lid,
	   FmdSqliteMap[fsid][fid].uid,
	   FmdSqliteMap[fsid][fid].gid,
	   FmdSqliteMap[fsid][fid].name.c_str(),
	   FmdSqliteMap[fsid][fid].container.c_str(),
	   FmdSqliteMap[fsid][fid].filecxerror,
	   FmdSqliteMap[fsid][fid].blockcxerror,
	   FmdSqliteMap[fsid][fid].layouterror,
	   FmdSqliteMap[fsid][fid].locations.c_str());
  
  if ((sqlite3_exec(DB[fsid],insertentry, CallBack, this, &ErrMsg))) {
    eos_err("unable to update fsid=%lu fid=%08llx in fst table - msg=%s\n",fsid, fid,ErrMsg);
    return false;
  }
  
  return true;
}

/*----------------------------------------------------------------------------*/
/** 
 * Update disk metadata 
 * 
 * @param fsid file system id
 * @param fid  file id to update
 * @param disksize size of the file on disk
 * @param diskchecksum checksum of the file on disk
 * @param checktime time of the last check of that file
 * @param filecxerror indicator for file checksum error
 * @param blockcxerror inidicator for block checksum error
 * 
 * @return true if record has been commited
 */
/*----------------------------------------------------------------------------*/
bool
FmdSqliteHandler::UpdateFromDisk(eos::common::FileSystem::fsid_t fsid, eos::common::FileId::fileid_t fid, unsigned long long disksize, std::string diskchecksum, unsigned long checktime, bool filecxerror, bool blockcxerror, bool flaglayouterror)
{
  eos::common::RWMutexWriteLock lock(Mutex);
  
  eos_info("fsid=%lu fid=%08llx disksize=%llu diskchecksum=%s checktime=%llu fcxerror=%d bcxerror=%d flaglayouterror=%d", fsid, fid, disksize, diskchecksum.c_str(), checktime, filecxerror, blockcxerror, flaglayouterror);
	
  if (!fid) {
    eos_info("skipping to insert a file with fid 0");
    return false;    
  }

  if (FmdSqliteMap.count(fsid)) {
    // update in-memory
    FmdSqliteMap[fsid][fid].disksize = disksize;
    // fix the reference value from disk
    FmdSqliteMap[fsid][fid].size = disksize;
    FmdSqliteMap[fsid][fid].checksum = diskchecksum;
    FmdSqliteMap[fsid][fid].fid  = fid;
    FmdSqliteMap[fsid][fid].fsid = fsid;
    FmdSqliteMap[fsid][fid].diskchecksum = diskchecksum;
    FmdSqliteMap[fsid][fid].checktime = checktime;
    FmdSqliteMap[fsid][fid].filecxerror = filecxerror;
    FmdSqliteMap[fsid][fid].blockcxerror = blockcxerror;
    if (flaglayouterror) {
      // if the mgm sync is run afterwards, every disk file is by construction an orphan, until it is synced from the mgm
      FmdSqliteMap[fsid][fid].layouterror = eos::common::LayoutId::kOrphan;
    }
    return CommitFromMemory(fid,fsid);
  } else {
    eos_crit("no sqlite DB open for fsid=%llu", fsid);
    return false;
  }

}

/*----------------------------------------------------------------------------*/
/** 
 * Update mgm metadata 
 * 
 * @param fsid file system id
 * @param fid  file id to update
 * @param cid  container id
 * @param lid  layout id
 * @param mgmsize size of the file in the mgm namespace
 * @param mgmchecksum checksum of the file in the mgm namespace
 * @param name file name
 * @param container container/dir name
 * 
 * @return true if record has been commited
 */
/*----------------------------------------------------------------------------*/
bool
FmdSqliteHandler::UpdateFromMgm(eos::common::FileSystem::fsid_t fsid, eos::common::FileId::fileid_t fid, eos::common::FileId::fileid_t cid, eos::common::LayoutId::layoutid_t lid, unsigned long long mgmsize, std::string mgmchecksum, std::string name, std::string container, uid_t uid, gid_t gid, unsigned long long ctime, unsigned long long ctime_ns, unsigned long long mtime, unsigned long long mtime_ns, int layouterror, std::string locations)
{
  eos::common::RWMutexWriteLock lock(Mutex);
  
  eos_info("fsid=%lu fid=%08llx cid=%llu lid=%lx mgmsize=%llu mgmchecksum=%s name=%s container=%s", fsid, fid, cid, lid, mgmsize, mgmchecksum.c_str(), name.c_str(), container.c_str());

  if (!fid) {
    eos_info("skipping to insert a file with fid 0");
    return false;    
  }
		
  if (FmdSqliteMap.count(fsid)) {
    // update in-memory
    FmdSqliteMap[fsid][fid].mgmsize = mgmsize;
    FmdSqliteMap[fsid][fid].size = mgmsize;
    FmdSqliteMap[fsid][fid].checksum = mgmchecksum;
    FmdSqliteMap[fsid][fid].mgmchecksum = mgmchecksum;
    FmdSqliteMap[fsid][fid].cid = cid;
    FmdSqliteMap[fsid][fid].lid = lid;
    FmdSqliteMap[fsid][fid].uid = uid;
    FmdSqliteMap[fsid][fid].gid = gid;
    FmdSqliteMap[fsid][fid].ctime = ctime;
    FmdSqliteMap[fsid][fid].ctime_ns = ctime_ns;
    FmdSqliteMap[fsid][fid].mtime = mtime;
    FmdSqliteMap[fsid][fid].mtime_ns = mtime_ns;
    FmdSqliteMap[fsid][fid].name = name;
    FmdSqliteMap[fsid][fid].container = container;
    FmdSqliteMap[fsid][fid].layouterror = layouterror;
    FmdSqliteMap[fsid][fid].locations = locations;
    
    // truncate the checksum to the right string length
    FmdSqliteMap[fsid][fid].mgmchecksum.erase(eos::common::LayoutId::GetChecksumLen(lid)*2);
    FmdSqliteMap[fsid][fid].checksum.erase(eos::common::LayoutId::GetChecksumLen(lid)*2);
    return CommitFromMemory(fid,fsid);
  } else {
    eos_crit("no sqlite DB open for fsid=%llu", fsid);
    return false;
  }
}

/*----------------------------------------------------------------------------*/
/** 
 * Reset disk information
 * 
 * @param fsid file system id
 * 
 * @return true if information has been reset successfully
 */
/*----------------------------------------------------------------------------*/
bool
FmdSqliteHandler::ResetDiskInformation(eos::common::FileSystem::fsid_t fsid)
{
  eos::common::RWMutexWriteLock lock(Mutex);
  
  if (FmdSqliteMap.count(fsid)) {
    google::dense_hash_map<unsigned long long, struct FmdSqlite::FMD >::iterator it;
    for (it = FmdSqliteMap[fsid].begin(); it != FmdSqliteMap[fsid].end(); it++) {
      // update in-memory
      it->second.disksize = 0xfffffff1ULL;
      it->second.diskchecksum = "";
      it->second.checktime = 0;
      it->second.filecxerror = -1;
      it->second.blockcxerror = -1;
    }

    // update SQLITE DB
    char updateentry[16384];
    snprintf(updateentry,sizeof(updateentry),"update fst set disksize=1152921504606846961,diskchecksum='',checktime=0,filecxerror=-1,blockcxerror=-1 where 1");
    
    if ((sqlite3_exec(DB[fsid],updateentry, CallBack, this, &ErrMsg))) {
      eos_err("unable to update fsid=%lu - msg=%s\n",fsid,ErrMsg);
      return false;
    }
  } else {
    eos_crit("no sqlite DB open for fsid=%llu", fsid);
    return false;
  }
  return true;
}

/*----------------------------------------------------------------------------*/
/** 
 * Reset mgm information
 * 
 * @param fsid file system id
 * 
 * @return true if information has been reset successfully
 */
/*----------------------------------------------------------------------------*/
bool
FmdSqliteHandler::ResetMgmInformation(eos::common::FileSystem::fsid_t fsid)
{
  eos::common::RWMutexWriteLock lock(Mutex);
  
  if (FmdSqliteMap.count(fsid)) {
    google::dense_hash_map<unsigned long long, struct FmdSqlite::FMD >::iterator it;
    for (it = FmdSqliteMap[fsid].begin(); it != FmdSqliteMap[fsid].end(); it++) {
      // update in-memory
      it->second.mgmsize = 0xfffffff1ULL;
      it->second.mgmchecksum = "";
      it->second.locations="";
    }

    // update SQLITE DB
    char updateentry[16384];
    snprintf(updateentry,sizeof(updateentry),"update fst set mgmsize=1152921504606846961,mgmchecksum='',locations='' where 1");
    
    if ((sqlite3_exec(DB[fsid],updateentry, CallBack, this, &ErrMsg))) {
      eos_err("unable to update fsid=%lu - msg=%s\n",fsid,ErrMsg);
      return false;
    }
  } else {
    eos_crit("no sqlite DB open for fsid=%llu", fsid);
    return false;
  }
  return true;
}


/*----------------------------------------------------------------------------*/
/** 
 * Resync a single entry from disk
 * 
 * @param path to the stored file on disk
 * @param fsid filesystem id
 * 
 * @return true if successfull
 */
/*----------------------------------------------------------------------------*/
bool 
FmdSqliteHandler::ResyncDisk(const char* path, eos::common::FileSystem::fsid_t fsid, bool flaglayouterror)
{
  bool retc=true;
  eos::common::Path cPath(path);
  eos::common::FileId::fileid_t fid = eos::common::FileId::Hex2Fid(cPath.GetName());
  off_t disksize=0;
  if (fid) {
    eos::common::Attr *attr = eos::common::Attr::OpenAttr(path);
    if (attr) {
      struct stat buf;
      if ((!stat(path, &buf)) && S_ISREG(buf.st_mode)) {
	std::string checksumType,checksumStamp, filecxError, blockcxError;
	std::string diskchecksum="";
	char checksumVal[SHA_DIGEST_LENGTH];
	size_t checksumLen = 0;
	
	unsigned long checktime=0;
	// got the file size
	disksize = buf.st_size;
	memset(checksumVal, 0, sizeof(checksumVal));
	checksumLen = SHA_DIGEST_LENGTH;
	if (!attr->Get("user.eos.checksum", checksumVal, checksumLen)) {
	  checksumLen = 0;
	}
	
	checksumType    = attr->Get("user.eos.checksumtype");
	checksumStamp   = attr->Get("user.eos.timestamp");
	filecxError     = attr->Get("user.eos.filecxerror");
	blockcxError    = attr->Get("user.eos.blockcxerror");
	
	checktime = (strtoull(checksumStamp.c_str(),0,10)/1000000);
	if (checksumLen) {
	  // retrieve a checksum object to get the hex representation
	  XrdOucString envstring = "eos.layout.checksum="; envstring += checksumType.c_str();
	  XrdOucEnv env(envstring.c_str());
	  int checksumtype = eos::common::LayoutId::GetChecksumFromEnv(env);
	  eos::common::LayoutId::layoutid_t layoutid = eos::common::LayoutId::GetId(eos::common::LayoutId::kPlain, checksumtype);		  
	  eos::fst::CheckSum *checksum = eos::fst::ChecksumPlugins::GetChecksumObject(layoutid, false);
	  
	  if (checksum) {
	    if (checksum->SetBinChecksum(checksumVal, checksumLen)) {
	      diskchecksum = checksum->GetHexChecksum();
	    }
	    delete checksum;
	  }
	}
	
	// now update the SQLITE DB
	if (!UpdateFromDisk(fsid,fid, disksize, diskchecksum, checktime, (filecxError =="1")?1:0, (blockcxError == "1")?1:0,flaglayouterror)) {
	  eos_err("failed to update SQLITE DB for fsid=%lu fid=%08llx", fsid, fid);
	  retc = false;
	} 
      }
      delete attr;
    }
  } else {
    eos_debug("would convert %s (%s) to fid 0", cPath.GetName(), path);
    retc = false;;
  }
  return retc;
}

/*----------------------------------------------------------------------------*/
/** 
 * Resync files under path into SQLITE DB
 * 
 * @param path path to scan
 * @param fsid file system id
 * 
 * @return true if successfull
 */
/*----------------------------------------------------------------------------*/
bool
FmdSqliteHandler::ResyncAllDisk(const char* path, eos::common::FileSystem::fsid_t fsid, bool flaglayouterror)
{
  char **paths = (char**) calloc(2, sizeof(char*));
  paths[0] = (char*) path;
  paths[1] = 0;
  if (!paths) {
    return false;
  }

  if (flaglayouterror) {
    isSyncing[fsid] = true;
  }

  if (!ResetDiskInformation(fsid)) {
    eos_err("failed to reset the disk information before resyncing");
    return false;
  } 
  // scan all the files
  FTS *tree = fts_open(paths, FTS_NOCHDIR, 0);
  
  if (!tree){
    eos_err("fts_open failed");
    free(paths);
    return false;
  }
  
  FTSENT *node;
  while ((node = fts_read(tree))) {
    if (node->fts_level > 0 && node->fts_name[0] == '.') {
      fts_set(tree, node, FTS_SKIP);
    } else {
      if (node->fts_info == FTS_F) {
        XrdOucString filePath = node->fts_accpath;
	eos_info("file=%s", filePath.c_str());
        if (!filePath.matches("*.xsmap")){
	  ResyncDisk(filePath.c_str(), fsid, flaglayouterror);
        }
      }
    }    
  }
  if (fts_close(tree)){
    eos_err("fts_close failed");
    free(paths);
    return false;
  } 

  free(paths);
  return true;
}

/*----------------------------------------------------------------------------*/
/** 
 * Resync meta data from MGM into SQLITE DB
 * 
 * @param fsid filesystem id
 * @param fid  file id
 * 
 * @return true if successfull
 */
/*----------------------------------------------------------------------------*/
bool
FmdSqliteHandler::ResyncMgm(eos::common::FileSystem::fsid_t fsid, eos::common::FileId::fileid_t fid, const char* manager)
{

  struct FmdSqlite::FMD fMd;
  FmdSqlite::Reset(fMd);
  int rc=0;
  if ( (!(rc=GetMgmFmdSqlite(manager, fid, fMd))) || 
       (rc == ENODATA)) {
    if (rc == ENODATA) {
      eos_warning("no such file on MGM for fid=%llu", fMd.fid);
    }

    // get our stored one
    FmdSqlite* fmd = GetFmd(fMd.fid, fsid, fMd.uid, fMd.gid, fMd.lid, true, true);
    if (fmd) {
      // check if we have an layout error
      fMd.layouterror = FmdSqlite::LayoutError(fsid,fMd.lid, fMd.locations);
      
      if (!UpdateFromMgm(fsid, fMd.fid, fMd.cid, fMd.lid, fMd.mgmsize, fMd.mgmchecksum, fMd.name, fMd.container, fMd.uid,fMd.gid, fMd.ctime, fMd.ctime_ns, fMd.mtime, fMd.mtime_ns, fMd.layouterror, fMd.locations)) {
	eos_err("failed to update fmd for fid=%08llx", fMd.fid);
	return false;
      }
    } else {
      eos_err("failed to get/create fmd for fid=%08llx", fMd.fid);
      return false;
    }
  } else {
    eos_err("failed to retrieve MGM fmd for fid=%08llx", fMd.fid);
    return false;
  }  
  
  return true;
}

/*----------------------------------------------------------------------------*/
/** 
 * Resync all meta data from MGM into SQLITE DB
 * 
 * @param fsid filesystem id
 * 
 * @return true if successfull
 */
/*----------------------------------------------------------------------------*/
bool
FmdSqliteHandler::ResyncAllMgm(eos::common::FileSystem::fsid_t fsid, const char* manager)
{

  if (!ResetMgmInformation(fsid)) {
    eos_err("failed to reset the mgm information before resyncing");
    return false;
  } 
  
  XrdOucString consolestring = "/proc/admin/?&mgm.format=fuse&mgm.cmd=fs&mgm.subcmd=dumpmd&mgm.dumpmd.storetime=1&mgm.dumpmd.option=m&mgm.fsid=";
  consolestring += (int) fsid;
  XrdOucString url = "root://"; url += manager; url += "//"; url += consolestring;

  // we run an external command and parse the output
  char* tmpfile = tempnam("/tmp/","efstd");
  XrdOucString cmd = "env XrdSecPROTOCOL=sss xrdcp -s \""; cmd += url; cmd+="\" "; cmd += tmpfile;
  int rc = system(cmd.c_str());
  if (WEXITSTATUS(rc)) {
    eos_err("%s returned %d", cmd.c_str(), WEXITSTATUS(rc));
    return false;
  } else {
    eos_debug("%s executed successfully", cmd.c_str());
  }
  
  // parse the result
  std::ifstream inFile(tmpfile);
  std::string dumpentry;

  while (std::getline(inFile, dumpentry)) {
    eos_debug("line=%s", dumpentry.c_str());
    XrdOucEnv* env = new XrdOucEnv(dumpentry.c_str());
    if (env) {
      struct FmdSqlite::FMD fMd;
      FmdSqlite::Reset(fMd);
      if (FmdSqlite::EnvMgmToFmdSqlite(*env, fMd)) {
	// get our stored one
	FmdSqlite* fmd = GetFmd(fMd.fid, fsid, fMd.uid, fMd.gid, fMd.lid, true, true);

	fMd.layouterror = FmdSqlite::LayoutError(fsid,fMd.lid, fMd.locations);
	
	if (fmd) {
	  if (!UpdateFromMgm(fsid, fMd.fid, fMd.cid, fMd.lid, fMd.mgmsize, fMd.mgmchecksum, fMd.name, fMd.container, fMd.uid,fMd.gid, fMd.ctime, fMd.ctime_ns, fMd.mtime, fMd.mtime_ns, fMd.layouterror,fMd.locations)) {
	    eos_err("failed to update fmd %s", dumpentry.c_str());
	  }
	} else {
	  eos_err("failed to get/create fmd %s", dumpentry.c_str());
	}
      } else {
	eos_err("failed to convert %s", dumpentry.c_str());
      }
    }    
  }

  isSyncing[fsid] = false;  

  // remove the temporary file
  unlink(tmpfile);
  free(tmpfile);
  return true;
}

/*----------------------------------------------------------------------------*/
/** 
 * Query vector of fids
 * 
 * @param fsid file system id
 * @param query SLQ statement which is placed in a call like 'select fid from fst where <query>'
 * @param fidvector Vector where the matching fid's are filled in
 * @return number of fid's returned in the vector
 */
/*----------------------------------------------------------------------------*/
size_t
FmdSqliteHandler::Query(eos::common::FileSystem::fsid_t fsid, std::string query, std::vector<eos::common::FileId::fileid_t> &fidvector)
{
  eos::common::RWMutexReadLock lock(Mutex);
  if (DB.count(fsid)) {
    Qr.clear();
    
    std::string selectstring="";
    selectstring = "select fid from fst where ";
    selectstring += query.c_str();

    if ((sqlite3_exec(DB[fsid],selectstring.c_str(), CallBack, this, &ErrMsg))) {
      eos_err("unable to query - msg=%s\n",ErrMsg);
      return 0;
    }
    
    qr_result_t::const_iterator it;
    eos_info("Query returned %lu fids", Qr.size());
    for (size_t i = 0; i< Qr.size(); i++) {
      eos::common::FileId::fileid_t fid = strtoull(Qr[i]["fid"].c_str(),0,10);
      fidvector.push_back(fid);
    }
    return fidvector.size();
  } else {
    eos_err("no SQL DB open for fsid=%lu", fsid);
    return 0;
  }
}

/*----------------------------------------------------------------------------*/
/** 
 * GetInconsistencyStatistics
 * 
 * @param fsid file system id
 * @param statistics output map with counters for each statistics field
 * @param fileset output map with sets for each statistics field
 * @return always true
 */
/*----------------------------------------------------------------------------*/
bool
FmdSqliteHandler::GetInconsistencyStatistics(eos::common::FileSystem::fsid_t fsid, std::map<std::string, size_t> &statistics,std::map<std::string , std::set < eos::common::FileId::fileid_t> > &fidset )
{
  eos::common::RWMutexReadLock lock(Mutex);

  if (!FmdSqliteMap.count(fsid)) 
    return false;

  // query in-memory
  google::dense_hash_map<unsigned long long, struct FmdSqlite::FMD>::const_iterator it;
  statistics["mem_n"]  = 0;         // number of files in SQLITE DB 
  
  statistics["d_sync_n"] = 0;       // number of synced files from disk
  statistics["m_sync_n"] = 0;       // number of synced files from MGM server
  
  statistics["d_mem_sz_diff"] = 0;  // number of files with disk and reference size mismatch
  statistics["m_mem_sz_diff"] = 0;  // number of files with MGM and reference size mismatch
  
  statistics["d_cx_diff"] = 0;      // number of files with disk and reference checksum mismatch
  statistics["m_cx_diff"] = 0;      // number of files with MGM and reference checksum mismatch

  statistics["orphans_n"] = 0;      // number of orphaned replicas
  statistics["unreg_n"]    = 0;     // number of unregistered replicas
  statistics["rep_diff_n"] = 0;     // number of files with replica number mismatch

  fidset["m_sync_n"].clear();       // file set's for the same items as above
  fidset["m_mem_sz_diff"].clear();
  fidset["mem_n"].clear(); 
  fidset["d_sync_n"].clear();
  fidset["d_mem_sz_diff"].clear();
  
  fidset["m_cx_diff"].clear();
  fidset["d_cx_diff"].clear();
  
  fidset["orphans_n"].clear();
  fidset["unreg_n"].clear();
  fidset["rep_diff_n"].clear();
  
  if (!IsSyncing(fsid)) {
    // we report values only when we are not in the sync phase from disk/mgm
    for (it = FmdSqliteMap[fsid].begin(); it != FmdSqliteMap[fsid].end(); it++) {
      if (it->second.layouterror) {
	if (it->second.layouterror & eos::common::LayoutId::kOrphan) {
	  statistics["orphans_n"]++;
	  fidset["orphans_n"].insert(it->second.fid);
	}
	if (it->second.layouterror & eos::common::LayoutId::kUnregistered) {
	  statistics["unreg_n"]++;
	  fidset["unreg_n"].insert(it->second.fid);
	}
	if (it->second.layouterror & eos::common::LayoutId::kReplicaWrong) {
	  statistics["rep_diff_n"]++;
	  fidset["rep_diff_n"].insert(it->second.fid);
	}
      }
      
      if (it->second.mgmsize != 0xfffffff1ULL) {
	statistics["m_sync_n"]++;
	fidset["m_sync_n"].insert(it->second.fid);
	if (it->second.size != 0xfffffff1ULL) {
	  if (it->second.size != it->second.mgmsize) {
	    statistics["m_mem_sz_diff"]++;
	    fidset["m_mem_sz_diff"].insert(it->second.fid);
	  }
	}
      }
      
      if (!it->second.layouterror) {
	if (it->second.diskchecksum.length() && (it->second.diskchecksum != it->second.checksum) ) {
	  statistics["d_cx_diff"]++;
	  fidset["d_cx_diff"].insert(it->second.fid);
	}
	
	if (it->second.mgmchecksum.length() && (it->second.mgmchecksum != it->second.checksum) ) {
	  statistics["m_cx_diff"]++;
	  fidset["m_cx_diff"].insert(it->second.fid);
	}
      }
      
      statistics["mem_n"]++;
      fidset["mem_n"].insert(it->second.fid);
      
      if (it->second.disksize != 0xfffffff1ULL) {
	statistics["d_sync_n"]++;
	fidset["d_sync_n"].insert(it->second.fid);
	if (it->second.size != 0xfffffff1ULL) {
	  if (it->second.size != it->second.disksize) {
	    statistics["d_mem_sz_diff"]++;
	    fidset["d_mem_sz_diff"].insert(it->second.fid);
	  }
	}
      }
    }
  }
  
  return true;
}

/*----------------------------------------------------------------------------*/
/** 
 * Trim the SQLITE DB for a given filesystem id
 * 
 * @param fsid file system id
 * @param option - not used
 * 
 * @return true if successful otherwise false
 */
/*----------------------------------------------------------------------------*/
bool
FmdSqliteHandler::TrimDBFile(eos::common::FileSystem::fsid_t fsid, XrdOucString option) {

  if (!DB.count(fsid)) {
    eos_err("unable to trim DB for fsid=%lu - DB not open", fsid);
    return false;
  }
 
  // cleanup/compact the DB file
  if (sqlite3_exec(DB[fsid],"VACUUM;", 0, 0, &ErrMsg) != SQLITE_OK) {
    eos_err("unable to run VACCUM - msg=%s", ErrMsg);
    return false;
  }

  return true;
}

/*----------------------------------------------------------------------------*/
/** 
 * Convert a FMD struct into an env representation
 * 
 * 
 * @return env representation
 */
/*----------------------------------------------------------------------------*/
XrdOucEnv*
FmdSqlite::FmdSqliteToEnv() 
{
  char serialized[1024*64];
  sprintf(serialized,"id=%llu&cid=%llu&ctime=%lu&ctime_ns=%lu&mtime=%lu&mtime_ns=%lu&size=%llu&checksum=%s&lid=%lu&uid=%u&gid=%u&name=%s&container=%s",fMd.fid,fMd.cid,fMd.ctime,fMd.ctime_ns,fMd.mtime,fMd.mtime_ns,fMd.size, fMd.checksum.c_str(),fMd.lid,fMd.uid,fMd.gid,fMd.name.c_str(),fMd.container.c_str());
  return new XrdOucEnv(serialized);
};

/*----------------------------------------------------------------------------*/
/** 
 * Convert an FST env representation to an FMD struct
 * 
 * @param env env representation
 * @param fmd reference to FMD struct
 * 
 * @return true if successful otherwise false
 */
/*----------------------------------------------------------------------------*/
bool 
FmdSqlite::EnvFstToFmdSqlite(XrdOucEnv &env, struct FmdSqlite::FMD &fmd)
{
  // check that all tags are present
  if ( !env.Get("id") ||
       !env.Get("cid") ||
       !env.Get("ctime") ||
       !env.Get("ctime_ns") ||
       !env.Get("mtime") ||
       !env.Get("mtime_ns") ||
       !env.Get("size") ||
       !env.Get("lid") ||
       !env.Get("uid") ||
       !env.Get("gid") ||
       !env.Get("name") )

    return false;
  
  fmd.fid             = strtoull(env.Get("id"),0,10);
  fmd.cid             = strtoull(env.Get("cid"),0,10);
  fmd.ctime           = strtoul(env.Get("ctime"),0,10);
  fmd.ctime_ns        = strtoul(env.Get("ctime_ns"),0,10);
  fmd.mtime           = strtoul(env.Get("mtime"),0,10);
  fmd.mtime_ns        = strtoul(env.Get("mtime_ns"),0,10);
  fmd.size            = strtoull(env.Get("size"),0,10);
  fmd.lid             = strtoul(env.Get("lid"),0,10);
  fmd.uid             = (uid_t) strtoul(env.Get("uid"),0,10);
  fmd.gid             = (gid_t) strtoul(env.Get("gid"),0,10);
  if (env.Get("name")) {
    fmd.name = env.Get("name");
  } else {
    fmd.name ="";
  }

  if (env.Get("container")) {
    fmd.container = env.Get("container");
  } else {
    fmd.container="";
  }

  if (env.Get("checksum")) {
    fmd.checksum =env.Get("checksum");
  } else {
    fmd.checksum="";
  }

  return true;
}

/*----------------------------------------------------------------------------*/
/** 
 * Convert an FST env representation to an FMD struct
 * 
 * @param env env representation
 * @param fmd reference to FMD struct
 * 
 * @return true if successful otherwise false
 */
/*----------------------------------------------------------------------------*/
bool 
FmdSqlite::EnvMgmToFmdSqlite(XrdOucEnv &env, struct FmdSqlite::FMD &fmd)
{
  // check that all tags are present
  if ( !env.Get("id") ||
       !env.Get("cid") ||
       !env.Get("location") ||
       !env.Get("ctime") ||
       !env.Get("ctime_ns") ||
       !env.Get("mtime") ||
       !env.Get("mtime_ns") ||
       !env.Get("size") ||
       !env.Get("checksum") ||
       !env.Get("lid") ||
       !env.Get("uid") ||
       !env.Get("gid") ||
       !env.Get("name") ||
       !env.Get("container"))
    return false;
  
  fmd.fid             = strtoull(env.Get("id"),0,10);
  fmd.cid             = strtoull(env.Get("cid"),0,10);
  fmd.ctime           = strtoul(env.Get("ctime"),0,10);
  fmd.ctime_ns        = strtoul(env.Get("ctime_ns"),0,10);
  fmd.mtime           = strtoul(env.Get("mtime"),0,10);
  fmd.mtime_ns        = strtoul(env.Get("mtime_ns"),0,10);
  fmd.mgmsize            = strtoull(env.Get("size"),0,10);
  fmd.lid             = strtoul(env.Get("lid"),0,10);
  fmd.uid             = (uid_t) strtoul(env.Get("uid"),0,10);
  fmd.gid             = (gid_t) strtoul(env.Get("gid"),0,10);
  fmd.name            = env.Get("name");
  fmd.container       = env.Get("container");
  fmd.mgmchecksum     = env.Get("checksum");
  fmd.locations       = env.Get("location");
  return true;
}

/*----------------------------------------------------------------------------*/
/** 
 * Return FMD from a remote filesystem
 * 
 * @param manager host:port of the server to contact
 * @param shexfid hex string of the file id
 * @param sfsid string of filesystem id
 * @param fmd reference to the FMD struct to store FMD
 * 
 * @return 
 */
int
FmdSqliteHandler::GetRemoteFmdSqlite(const char* manager, const char* shexfid, const char* sfsid, struct FmdSqlite::FMD &fmd)
{
  char result[64*1024]; result[0]=0;
  int  result_size=64*1024;

  XrdOucString fmdquery="/?fst.pcmd=getfmd&fst.getfmd.fid=";fmdquery += shexfid;
  fmdquery += "&fst.getfmd.fsid="; fmdquery += sfsid;

  if ( (!manager) || (!shexfid) || (!sfsid) ) {
    return EINVAL;
  }

  XrdOucString url = "root://"; url += manager; url += "//dummy";

  XrdClientAdmin* admin = new XrdClientAdmin(url.c_str());

  int rc=0;
  admin->Connect();
  admin->GetClientConn()->ClearLastServerError();
  admin->GetClientConn()->SetOpTimeLimit(10);
  admin->Query(kXR_Qopaquf,
                           (kXR_char *) fmdquery.c_str(),
                           (kXR_char *) result, result_size);
  
  if (!admin->LastServerResp()) {
    eos_static_err("Unable to retrieve meta data from server %s for fid=%s fsid=%s",manager, shexfid, sfsid);
    
    rc = 1;
  }
  switch (admin->LastServerResp()->status) {
  case kXR_ok:
    eos_static_debug("got replica file meta data from server %s for fid=%s fsid=%s",manager, shexfid, sfsid);
    rc = 0;
    break;
    
  case kXR_error:
    eos_static_err("Unable to retrieve meta data from server %s for fid=%s fsid=%s",manager, shexfid, sfsid);
    rc = ECOMM;
    break;
    
  default:
    rc = ECOMM;
    break;
  }

  delete admin;

  if (rc) {
    return EIO;
  }

  if (!strncmp(result,"ERROR", 5)) {
    // remote side couldn't get the record
    eos_static_err("Unable to retrieve meta data on remote server %s for fid=%s fsid=%s",manager, shexfid, sfsid);
    return ENODATA;
  }
  // get the remote file meta data into an env hash
  XrdOucEnv fmdenv(result);

  if (!FmdSqlite::EnvFstToFmdSqlite(fmdenv, fmd)) {
    int envlen;
    eos_static_err("Failed to unparse file meta data %s", fmdenv.Env(envlen));
    return EIO;
  }
  // very simple check
  if (fmd.fid != eos::common::FileId::Hex2Fid(shexfid)) {
    eos_static_err("Uups! Received wrong meta data from remote server - fid is %lu instead of %lu !", fmd.fid, eos::common::FileId::Hex2Fid(shexfid));
    return EIO;
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
/** 
 * Return FMD from an mgm
 * 
 * @param manager host:port of the mgm to contact
 * @param fid file id
 * @param fmd reference to the FMD struct to store FMD
 * 
 * @return 
 */
int
FmdSqliteHandler::GetMgmFmdSqlite(const char* manager, eos::common::FileId::fileid_t fid,  struct FmdSqlite::FMD &fmd)
{
  char result[64*1024]; result[0]=0;
  int  result_size=64*1024;

  char sfmd[1024];
  snprintf(sfmd,sizeof(sfmd)-1,"%llu", fid);
  XrdOucString fmdquery="/?mgm.pcmd=getfmd&mgm.getfmd.fid=";fmdquery += sfmd;

  if ( (!manager) || (!fid) ) {
    return EINVAL;
  }

  XrdOucString url = "root://"; url += manager; url += "//dummy";

  XrdClientAdmin* admin = new XrdClientAdmin(url.c_str());

  int rc=0;
  admin->Connect();
  admin->GetClientConn()->ClearLastServerError();
  admin->GetClientConn()->SetOpTimeLimit(10);
  admin->Query(kXR_Qopaquf,
                           (kXR_char *) fmdquery.c_str(),
                           (kXR_char *) result, result_size);
  
  if (!admin->LastServerResp()) {
    eos_static_err("Unable to retrieve meta data from mgm %s for fid=%08llx",manager, fid);
    
    rc = 1;
  }
  switch (admin->LastServerResp()->status) {
  case kXR_ok:
    eos_static_debug("got replica file meta data from mgm %s for fid=%08llx",manager, fid);
    rc = 0;
    break;
    
  case kXR_error:
    eos_static_err("Unable to retrieve meta data from mgm %s for fid=%08llx",manager, fid);
    rc = ECOMM;
    break;
    
  default:
    rc = ECOMM;
    break;
  }

  delete admin;

  if (rc) {
    return EIO;
  }


  std::string sresult = result;
  if ( (sresult.find("getfmd: retc=0 ")) == std::string::npos ) {
    // remote side couldn't get the record
    eos_static_err("Unable to retrieve meta data on remote mgm %s for fid=%08llx - result=%s",manager, fid, result);
    return ENODATA;
  } else {
    // truncate 'getfmd: retc=0 '  away
    sresult.erase(0,15);
  }

  // get the remote file meta data into an env hash
  
  XrdOucEnv fmdenv(sresult.c_str());
  
  if (!FmdSqlite::EnvMgmToFmdSqlite(fmdenv, fmd)) {
    int envlen;
    eos_static_err("Failed to unparse file meta data %s", fmdenv.Env(envlen));
    return EIO;
  }
  // very simple check
  if (fmd.fid != fid) {
    eos_static_err("Uups! Received wrong meta data from remote server - fid is %lu instead of %lu !", fmd.fid, fid);
    return EIO;
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
/** 
 * Return a remote file attribute
 * 
 * @param manager host:port of the server to contact
 * @param key extended attribute key to get
 * @param path file path to read attributes from
 * @param attribute reference where to store the attribute value
 * 
 * @return 
 */
/*----------------------------------------------------------------------------*/
int
FmdSqliteHandler::GetRemoteAttribute(const char* manager, const char* key, const char* path, XrdOucString& attribute)
{
  char result[64*1024]; result[0]=0;
  int  result_size=64*1024;

  XrdOucString fmdquery="/?fst.pcmd=getxattr&fst.getxattr.key="; fmdquery += key; fmdquery+="&fst.getxattr.path=";fmdquery += path;

  if ( (!manager) || (!key) || (!path) ) {
    return EINVAL;
  }

  int rc=0;

  XrdOucString url = "root://"; url += manager; url += "//dummy";
  XrdClientAdmin* admin = new XrdClientAdmin(url.c_str());

  admin->Connect();
  admin->GetClientConn()->ClearLastServerError();
  admin->GetClientConn()->SetOpTimeLimit(10);
  admin->Query(kXR_Qopaquf,
                           (kXR_char *) fmdquery.c_str(),
                           (kXR_char *) result, result_size);
  
  if (!admin->LastServerResp()) {
    eos_static_err("Unable to retrieve meta data from server %s for key=%s path=%s",manager, key, path);
    
    rc = 1;
  }
  switch (admin->LastServerResp()->status) {
  case kXR_ok:
    eos_static_debug("got attribute meta data from server %s for key=%s path=%s attribute=%s",manager, key, path, result);
    rc = 0;
    break;
    
  case kXR_error:
    eos_static_err("Unable to retrieve meta data from server %s for key=%s path=%s",manager, key, path);
    rc = ECOMM;
    break;
    
  default:
    rc = ECOMM;
    break;
  }

  delete admin;

  if (rc) {
    return EIO;
  }

  if (!strncmp(result,"ERROR", 5)) {
    // remote side couldn't get the record
    eos_static_err("Unable to retrieve meta data on remote server %s for key=%s path=%s",manager, key, path);
    return ENODATA;
  }

  attribute = result;
  return 0;
}

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_END


