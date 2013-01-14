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
#include "fst/XrdFstOfs.hh"
#include "fst/checksum/ChecksumPlugins.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClFileSystem.hh"
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
    //    tx->Qr[i].swap(tx->Qr[i]);
    //    tx->Qr[i][ColName[k]].swap(tx->Qr[i][ColName[k]]);
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
/** 
 * Specialized Callback function to fill the memory objects from the FST SQLITE3 select * 
 * 
 * @param see sqlite manual
 * 
 * @return fills FmdSqliteMap for the referenced filesystem
 */
/*----------------------------------------------------------------------------*/
int
FmdSqliteHandler::ReadDBCallBack(void *object, int argc, char **argv, char **ColName)
{
  fs_callback_info_t* cbinfo = (fs_callback_info_t*) object;
  // the object contains the particular map for the used <fsid> 

  eos::fst::FmdSqliteHandler::qr_result_t Qr; // local variable!
  Qr.resize(1);
  // fill them into the Qr map
  for (int k=0; k< argc; k++) {
    Qr[0][ColName[k]] = argv[k]?argv[k]:"";
  }
  eos::common::FileId::fileid_t fid = strtoull(Qr[0]["fid"].c_str(),0,10);

  (*(cbinfo->fmdmap))[fid].fid      = fid;
  (*(cbinfo->fmdmap))[fid].fsid     = cbinfo->fsid;
  (*(cbinfo->fmdmap))[fid].cid      = strtoull(Qr[0]["cid"].c_str(),0,10);
  (*(cbinfo->fmdmap))[fid].ctime    = strtoul(Qr[0]["ctime"].c_str(),0,10);
  (*(cbinfo->fmdmap))[fid].ctime_ns = strtoul(Qr[0]["ctime_ns"].c_str(),0,10);
  (*(cbinfo->fmdmap))[fid].mtime    = strtoul(Qr[0]["mtime"].c_str(),0,10);
  (*(cbinfo->fmdmap))[fid].mtime_ns = strtoul(Qr[0]["mtime_ns"].c_str(),0,10);
  (*(cbinfo->fmdmap))[fid].atime    = strtoul(Qr[0]["atime"].c_str(),0,10);
  (*(cbinfo->fmdmap))[fid].atime_ns = strtoul(Qr[0]["atime_ns"].c_str(),0,10);
  (*(cbinfo->fmdmap))[fid].checktime= strtoul(Qr[0]["checktime"].c_str(),0,10);
  (*(cbinfo->fmdmap))[fid].size     = strtoull(Qr[0]["size"].c_str(),0,10);
  (*(cbinfo->fmdmap))[fid].disksize = strtoull(Qr[0]["disksize"].c_str(),0,10);
  (*(cbinfo->fmdmap))[fid].mgmsize  = strtoull(Qr[0]["mgmsize"].c_str(),0,10);
  (*(cbinfo->fmdmap))[fid].checksum     = Qr[0]["checksum"].c_str();
  (*(cbinfo->fmdmap))[fid].diskchecksum = Qr[0]["diskchecksum"].c_str();
  (*(cbinfo->fmdmap))[fid].mgmchecksum  = Qr[0]["mgmchecksum"].c_str();
  (*(cbinfo->fmdmap))[fid].lid      = strtoul(Qr[0]["lid"].c_str(),0,10);
  (*(cbinfo->fmdmap))[fid].uid      = strtoul(Qr[0]["uid"].c_str(),0,10);
  (*(cbinfo->fmdmap))[fid].gid      = strtoul(Qr[0]["gid"].c_str(),0,10);
  (*(cbinfo->fmdmap))[fid].filecxerror = atoi(Qr[0]["filecxerror"].c_str());
  (*(cbinfo->fmdmap))[fid].blockcxerror = atoi(Qr[0]["blockcxerror"].c_str());
  (*(cbinfo->fmdmap))[fid].layouterror = atoi(Qr[0]["layouterror"].c_str());
  (*(cbinfo->fmdmap))[fid].locations = Qr[0]["locations"].c_str();
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
  fprintf(stderr,"%08llx %06llu %04lu %010lu %010lu %010lu %010lu %010lu %010lu %010lu %08llu %08llu %08llu %s %s %s %03lu %05u %05u\n",
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
          fmd->gid);
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
    isDirty[fsid]   = true;
    stayDirty[fsid] = true;
    eos_warning("setting sqlite3 file dirty - unclean shutdown detected");
    if (!src) {
      if (chmod(DBfilename[fsid].c_str(),S_IRWXU)) {
	eos_crit("failed to switch the sqlite3 database file mode to S_IRWXU errno=%d",errno);
      }
    }
  } else {
    isDirty[fsid] = false;
    stayDirty[fsid] = false;
  }

  // run 'sqlite3' to commit a pending journal
  std::string sqlite3cmd="test -r ";
  sqlite3cmd += fsDBFileName;
  sqlite3cmd += " && sqlite3 ";
  sqlite3cmd += fsDBFileName;
  sqlite3cmd += " \"select count(*) from fst where 1;\"" ;
  int rc = system(sqlite3cmd.c_str());
  
  if (WEXITSTATUS(rc)) {
    eos_warning("sqlite3 command execution failed");
  } else {
    eos_info("msg=\"sqlite3 clean-procedure succeeded\"");
  }
  
  // create the SQLITE DB
  if ((sqlite3_open(fsDBFileName,&DB[fsid]) == SQLITE_OK)) {
    XrdOucString createtable = "CREATE TABLE if not exists fst ( fid integer PRIMARY KEY, cid integer, fsid integer, ctime integer, ctime_ns integer, mtime integer, mtime_ns integer, atime integer, atime_ns integer, checktime integer, size integer default 281474976710641, disksize integer default 281474976710641, mgmsize integer default 281474976710641, checksum varchar(32), diskchecksum varchar(32), mgmchecksum varchar(32), lid integer, uid integer, gid integer, filecxerror integer, blockcxerror integer, layouterror integer, locations varchar(128))";
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
      if (!stayDirty[fsid]) {
	// if there was a complete boot procedure done, we remove the dirty flag
	// set the mode back to S_IRWXU
	if (chmod(DBfilename[fsid].c_str(),S_IRWXU)) {
	  eos_crit("failed to switch the sqlite3 database file to S_IRWXU errno=%d", errno);
	}
      }
    }
    if ( (sqlite3_close(DB[fsid]) ==  SQLITE_OK) ) {
      return true;
    }
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
  
  fs_callback_info_t cbinfo;
  cbinfo.fsid = fsid;
  cbinfo.fmdmap = &(FmdSqliteMap[fsid]);
  // we use a specialized callback to avoid to have everything again in memory at once
  if ((sqlite3_exec(DB[fsid],query.c_str(), ReadDBCallBack, &cbinfo, &ErrMsg))) {
    eos_err("unable to query - msg=%s\n",ErrMsg);
    return false;
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
  // eos_info("fid=%08llx fsid=%lu", fid, (unsigned long) fsid);

  if (fid == 0) {
    eos_warning("fid=0 requested for fsid=", fsid);
    return 0;
  }

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
        eos_crit("unable to get fmd for fid %llu on fs %lu - file id mismatch in meta data block (%llu)", fid, (unsigned long )fsid, fmd->fMd.fid);
        delete fmd;
	Mutex.UnLockRead();
        return 0;
      }
      
      if ( fmd->fMd.fsid != fsid) {
        // fatal this is somehow a wrong record!
        eos_crit("unable to get fmd for fid %llu on fs %lu - filesystem id mismatch in meta data block (%llu)", fid, (unsigned long) fsid, fmd->fMd.fsid);
        delete fmd;
	Mutex.UnLockRead();
        return 0;
      }

      // the force flag allows to retrieve 'any' value even with inconsistencies as needed by ResyncAllMgm

      if ( !force ) {
        if ( strcmp( eos::common::LayoutId::GetLayoutTypeString( fmd->fMd.lid ), "reedS" ) &&
             strcmp( eos::common::LayoutId::GetLayoutTypeString( fmd->fMd.lid ), "raidDP" ) ) {
	  
	  // if we have a mismatch between the mgm/disk and 'ref' value in size,  we don't return the FMD record
	  if ( (!isRW) && ((fmd->fMd.disksize && (fmd->fMd.disksize != fmd->fMd.size)) ||
			   (fmd->fMd.mgmsize &&  (fmd->fMd.mgmsize != 0xfffffffffff1ULL) && (fmd->fMd.mgmsize  != fmd->fMd.size))) ) {
	    eos_crit("msg=\"size mismatch disk/mgm vs memory\" fid=%08llx fsid=%lu size=%llu disksize=%llu mgmsize=%llu", fid, (unsigned long) fsid, fmd->fMd.size, fmd->fMd.disksize, fmd->fMd.mgmsize);
	    delete fmd;
	    Mutex.UnLockRead();
	    return 0;
	  }
	}

	// if we have a mismatch between the mgm/disk and 'ref' value in checksum, we don't return the FMD record
	// this check we can do only if the file is !zero otherwise we don't have a checksum on disk (e.g. a touch <a> file)
	if ((!isRW) && fmd->fMd.mgmsize &&
            ((fmd->fMd.diskchecksum.length() && (fmd->fMd.diskchecksum != fmd->fMd.checksum)) ||
             (fmd->fMd.mgmchecksum.length() &&  (fmd->fMd.mgmchecksum  != fmd->fMd.checksum)) ))
          {
	  eos_crit("msg=\"checksum mismatch disk/mgm vs memory\" fid=%08llx "
                   "fsid=%lu checksum=%s diskchecksum=%s mgmchecksum=%s",
                   fid, (unsigned long) fsid, fmd->fMd.checksum.c_str(),
                   fmd->fMd.diskchecksum.c_str(), fmd->fMd.mgmchecksum.c_str());
	  
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

      eos::common::RWMutexWriteLock lock(Mutex); // --> (return)

      FmdSqliteMap[fsid][fid].uid = uid;
      FmdSqliteMap[fsid][fid].gid = gid;
      FmdSqliteMap[fsid][fid].lid = layoutid;
      FmdSqliteMap[fsid][fid].fsid= fsid; 
      FmdSqliteMap[fsid][fid].fid = fid; 

      FmdSqliteMap[fsid][fid].ctime = FmdSqliteMap[fsid][fid].mtime = FmdSqliteMap[fsid][fid].atime = tv.tv_sec;
      FmdSqliteMap[fsid][fid].ctime_ns = FmdSqliteMap[fsid][fid].mtime_ns = FmdSqliteMap[fsid][fid].atime_ns = tv.tv_usec*1000;
      
      FmdSqlite* fmd = new FmdSqlite(fid,fsid);
      if (!fmd) {
	return 0;
      }
      
      // make a copy of the current record
      fmd->Replicate(FmdSqliteMap[fsid][fid]);

      if (Commit(fmd,false)) {
	eos_debug("returning meta data block for fid %d on fs %d", fid, (unsigned long) fsid);
	// return the mmaped meta data block

        return fmd;
      } else {
        eos_crit("unable to write new block for fid %d on fs %d - no changelog db open for writing", fid, (unsigned long) fsid);
        delete fmd;
        return 0;
      }
    } else {
      eos_warning("unable to get fmd for fid %llu on fs %lu - record not found", fid, (unsigned long) fsid);
      Mutex.UnLockRead();
      return 0;
    }
  } else {
    eos_crit("unable to get fmd for fid %llu on fs %lu - there is no changelog file open for that file system id", fid, (unsigned long) fsid);
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
FmdSqliteHandler::Commit(FmdSqlite* fmd, bool lockit)
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


  if (lockit) {
    // ---->
    Mutex.LockWrite();
  }

  if (FmdSqliteMap.count(fsid)) {
    // update in-memory
    FmdSqliteMap[fsid][fid] = fmd->fMd;
    if (lockit) {
      Mutex.UnLockWrite(); // <----
    }
    return CommitFromMemory(fid,fsid);
  } else {
    eos_crit("no sqlite DB open for fsid=%llu", (unsigned long) fsid);
    if (lockit) {
      Mutex.UnLockWrite(); // <----
    }
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
  snprintf(insertentry,sizeof(insertentry),"insert or replace into fst(fid,fsid,cid,ctime,ctime_ns,mtime,mtime_ns,atime,atime_ns,checktime,size,disksize,mgmsize,checksum,diskchecksum,mgmchecksum, lid,uid,gid,filecxerror,blockcxerror,layouterror,locations) values ('%llu','%lu','%llu','%lu','%lu','%lu','%lu','%lu','%lu','%lu','%llu','%llu','%llu','%s','%s','%s','%lu','%u','%u','%d','%d','%d','%s')",
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
  
  eos_debug("fsid=%lu fid=%08llx disksize=%llu diskchecksum=%s checktime=%llu fcxerror=%d bcxerror=%d flaglayouterror=%d", (unsigned long) fsid, fid, disksize, diskchecksum.c_str(), checktime, filecxerror, blockcxerror, flaglayouterror);
	
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
      // if the mgm sync is run afterwards, every disk file is by construction an
      // orphan, until it is synced from the mgm
      FmdSqliteMap[fsid][fid].layouterror = eos::common::LayoutId::kOrphan;
    }
    return CommitFromMemory(fid,fsid);
  } else {
    eos_crit("no sqlite DB open for fsid=%llu", (unsigned long) fsid);
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
 * 
 * @return true if record has been commited
 */
/*----------------------------------------------------------------------------*/
bool
FmdSqliteHandler::UpdateFromMgm(eos::common::FileSystem::fsid_t fsid, eos::common::FileId::fileid_t fid, eos::common::FileId::fileid_t cid, eos::common::LayoutId::layoutid_t lid, unsigned long long mgmsize, std::string mgmchecksum, uid_t uid, gid_t gid, unsigned long long ctime, unsigned long long ctime_ns, unsigned long long mtime, unsigned long long mtime_ns, int layouterror, std::string locations)
{
  eos::common::RWMutexWriteLock lock(Mutex);
  
  eos_debug( "fsid=%lu fid=%08llx cid=%llu lid=%lx mgmsize=%llu mgmchecksum=%s",
             (unsigned long) fsid, fid, cid, lid, mgmsize, mgmchecksum.c_str() );

  if (!fid) {
    eos_info("skipping to insert a file with fid 0");
    return false;    
  }
		
  if (FmdSqliteMap.count(fsid)) {
    if (!FmdSqliteMap[fsid].count(fid)) {
      FmdSqliteMap[fsid][fid].disksize = 0xfffffffffff1ULL;  
    }	
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
    FmdSqliteMap[fsid][fid].layouterror = layouterror;
    FmdSqliteMap[fsid][fid].locations = locations;
    
    // truncate the checksum to the right string length
    FmdSqliteMap[fsid][fid].mgmchecksum.erase(eos::common::LayoutId::GetChecksumLen(lid)*2);
    FmdSqliteMap[fsid][fid].checksum.erase(eos::common::LayoutId::GetChecksumLen(lid)*2);
    return CommitFromMemory(fid,fsid);
  } else {
    eos_crit("no sqlite DB open for fsid=%llu", (unsigned long) fsid);
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
      it->second.disksize = 0xfffffffffff1ULL;
      it->second.diskchecksum = "";
      it->second.checktime = 0;
      it->second.filecxerror = -1;
      it->second.blockcxerror = -1;
    }

    // update SQLITE DB
    char updateentry[16384];
    snprintf( updateentry, sizeof( updateentry ),
              "update fst set disksize=281474976710641,diskchecksum='',"
              "checktime=0,filecxerror=-1,blockcxerror=-1 where 1");
    
    if ((sqlite3_exec(DB[fsid],updateentry, CallBack, this, &ErrMsg))) {
      eos_err("unable to update fsid=%lu - msg=%s\n",fsid,ErrMsg);
      return false;
    }
  } else {
    eos_crit("no sqlite DB open for fsid=%llu", (unsigned long) fsid);
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
      it->second.mgmsize = 0xfffffffffff1ULL;
      it->second.mgmchecksum = "";
      it->second.locations="";
    }

    // update SQLITE DB
    char updateentry[16384];
    snprintf( updateentry, sizeof( updateentry ),
              "update fst set mgmsize=281474976710641,mgmchecksum='',locations='' where 1");
    
    if ((sqlite3_exec(DB[fsid],updateentry, CallBack, this, &ErrMsg))) {
      eos_err("unable to update fsid=%lu - msg=%s\n",fsid,ErrMsg);
      return false;
    }
  } else {
    eos_crit("no sqlite DB open for fsid=%llu", (unsigned long) fsid);
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
FmdSqliteHandler::ResyncDisk(const char*                     path,
                             eos::common::FileSystem::fsid_t fsid,
                             bool                            flaglayouterror)
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
	
	checksumType = attr->Get("user.eos.checksumtype");
	filecxError  = attr->Get("user.eos.filecxerror");
	blockcxError = attr->Get("user.eos.blockcxerror");
	
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
	
	// now updaAte the SQLITE DB
	if (!UpdateFromDisk(fsid,fid, disksize, diskchecksum, checktime, (filecxError =="1")?1:0, (blockcxError == "1")?1:0,flaglayouterror)) {
	  eos_err("failed to update SQLITE DB for fsid=%lu fid=%08llx", (unsigned long) fsid, fid);
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
FmdSqliteHandler::ResyncAllDisk(const char*                     path,
                                eos::common::FileSystem::fsid_t fsid,
                                bool                            flaglayouterror)
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
  unsigned long long cnt=0;
  while ((node = fts_read(tree))) {
    if (node->fts_level > 0 && node->fts_name[0] == '.') {
      fts_set(tree, node, FTS_SKIP);
    } else {
      if (node->fts_info == FTS_F) {
        XrdOucString filePath = node->fts_accpath;
        if (!filePath.matches("*.xsmap")){
	  cnt++;
	  eos_debug("file=%s", filePath.c_str());
	  ResyncDisk(filePath.c_str(), fsid, flaglayouterror);
	  if (!(cnt %10000)) {
	    eos_info("msg=\"synced files so far\" nfiles=%llu fsid=%lu",cnt, (unsigned long)fsid);
	  }
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
FmdSqliteHandler::ResyncMgm( eos::common::FileSystem::fsid_t fsid,
                             eos::common::FileId::fileid_t   fid,
                             const char*                     manager )
{
  struct FmdSqlite::FMD fMd;
  FmdSqlite::Reset(fMd);
  int rc=0;
  if ( (!(rc=GetMgmFmdSqlite(manager, fid, fMd))) || 
       (rc == ENODATA)) {
    if (rc == ENODATA) {
      eos_warning("no such file on MGM for fid=%llu", fid);
      fMd.fid = fid;
      if (fid==0) {
	eos_warning("removing fid=0 entry");
	return DeleteFmd(fMd.fid, fsid);
      }
    }

    // define layouterrors
    fMd.layouterror = FmdSqlite::LayoutError(fsid,fMd.lid, fMd.locations);

    // get an existing record without creation of a record !!!
    FmdSqlite* fmd = GetFmd(fMd.fid, fsid, fMd.uid, fMd.gid, fMd.lid, false, true);
    if (fmd) {
      // check if there was a disk replica
      if ( fmd->fMd.disksize == 0xfffffffffff1ULL) {
	if (fMd.layouterror && eos::common::LayoutId::kUnregistered) {
	  // there is no replica supposed to be here and there is nothing on disk, so remove it from the SLIQTE database
	  eos_warning("removing <ghost> entry for fid=%llu on fsid=%lu", fid, (unsigned long) fsid);
	  delete fmd;
	  return DeleteFmd(fMd.fid, fsid);
	} else {
	  // we proceed 
	  delete fmd;
	}
      }
    } else {
      if (fMd.layouterror && eos::common::LayoutId::kUnregistered) {
	// this entry is deleted and we are not supposed to have it
	return true;
      }
    }

    if ( (!fmd) && (rc == ENODATA) ) {
      // no file on MGM and no file locally
      eos_info("fsid=%lu fid=%08lxx msg=\"file removed in the meanwhile\"", fsid, fid);
      return true;
    }

    if ( fmd ) {
      delete fmd;
    }
    
    // get/create a record
    fmd = GetFmd(fMd.fid, fsid, fMd.uid, fMd.gid, fMd.lid, true, true);
    if (fmd) {
      if (!UpdateFromMgm(fsid, fMd.fid, fMd.cid, fMd.lid, fMd.mgmsize, fMd.mgmchecksum, fMd.uid,fMd.gid, fMd.ctime, fMd.ctime_ns, fMd.mtime, fMd.mtime_ns, fMd.layouterror, fMd.locations)) {
	eos_err("failed to update fmd for fid=%08llx", fid);
	delete fmd;
	return false;
      }
      // check if it exists on disk
      if (fmd->fMd.disksize == 0xfffffffffff1ULL) {
	fMd.layouterror |= eos::common::LayoutId::kMissing;
	eos_warning("found missing replica for fid=%llu on fsid=%lu", fid, (unsigned long) fsid);
      }

      // check if it exists on disk and on the mgm
      if ( (fmd->fMd.disksize == 0xfffffffffff1ULL) && (fmd->fMd.mgmsize == 0xfffffffffff1ULL) ) {
	// there is no replica supposed to be here and there is nothing on disk, so remove it from the SLIQTE database
	eos_warning("removing <ghost> entry for fid=%llu on fsid=%lu", fid, (unsigned long) fsid);
	delete fmd;
	return DeleteFmd(fMd.fid, fsid);
      }
      delete fmd;
    } else {
      eos_err("failed to get/create fmd for fid=%08llx", fid);
      return false;
    }
  } else {
    eos_err("failed to retrieve MGM fmd for fid=%08llx", fid);
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

  unsigned long long cnt=0;
  while (std::getline(inFile, dumpentry)) {
    cnt++;
    eos_debug("line=%s", dumpentry.c_str());
    XrdOucEnv* env = new XrdOucEnv(dumpentry.c_str());
    if (env) {
      struct FmdSqlite::FMD fMd;
      FmdSqlite::Reset(fMd);
      if (FmdSqlite::EnvMgmToFmdSqlite(*env, fMd)) {
	// get/create one
	FmdSqlite* fmd = GetFmd(fMd.fid, fsid, fMd.uid, fMd.gid, fMd.lid, true, true);

	fMd.layouterror = FmdSqlite::LayoutError(fsid,fMd.lid, fMd.locations);
	
	if (fmd) {
	  // check if it exists on disk
	  if (fmd->fMd.disksize == 0xfffffffffff1ULL) {
	    fMd.layouterror |= eos::common::LayoutId::kMissing;
	    eos_warning("found missing replica for fid=%llu on fsid=%lu", fMd.fid, (unsigned long)fsid);
	  }

	  if (!UpdateFromMgm(fsid, fMd.fid, fMd.cid, fMd.lid, fMd.mgmsize, fMd.mgmchecksum, fMd.uid,fMd.gid, fMd.ctime, fMd.ctime_ns, fMd.mtime, fMd.mtime_ns, fMd.layouterror,fMd.locations)) {
	    eos_err("failed to update fmd %s", dumpentry.c_str());
	  }
	  delete fmd;
	} else {
	  eos_err("failed to get/create fmd %s", dumpentry.c_str());
	}
      } else {
	eos_err("failed to convert %s", dumpentry.c_str());
      }
      delete env;
    }
    if (!(cnt %10000)) {
      eos_info("msg=\"synced files so far\" nfiles=%llu fsid=%lu",cnt, (unsigned long) fsid);
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
FmdSqliteHandler::Query( eos::common::FileSystem::fsid_t             fsid, 
                         std::string                                 query,
                         std::vector<eos::common::FileId::fileid_t>& fidvector)
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
    Qr.clear();
    return fidvector.size();
  } else {
    eos_err("no SQL DB open for fsid=%lu", (unsigned long) fsid);
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
FmdSqliteHandler::GetInconsistencyStatistics( eos::common::FileSystem::fsid_t fsid,
                                              std::map<std::string, size_t>& statistics,
                                              std::map<std::string , std::set < eos::common::FileId::fileid_t> > &fidset )
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
  statistics["rep_missing_n"] = 0;  // number of files which are missing on disk

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
  fidset["rep_missing_n"].clear();

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
	if (it->second.layouterror & eos::common::LayoutId::kMissing) {
	  statistics["rep_missing_n"]++;
	  fidset["rep_missing_n"].insert(it->second.fid);
	}
      }
      
      if (it->second.mgmsize != 0xfffffffffff1ULL) {
	statistics["m_sync_n"]++;
	if (it->second.size != 0xfffffffffff1ULL) {
	  if (it->second.size != it->second.mgmsize) {
	    statistics["m_mem_sz_diff"]++;
	    fidset["m_mem_sz_diff"].insert(it->second.fid);
	  }
	}
      }
      
      if (!it->second.layouterror) {
	if (it->second.size  && it->second.diskchecksum.length() && (it->second.diskchecksum != it->second.checksum) ) {
	  statistics["d_cx_diff"]++;
	  fidset["d_cx_diff"].insert(it->second.fid);
	}
	
	if (it->second.size && it->second.mgmchecksum.length() && (it->second.mgmchecksum != it->second.checksum) ) {
	  statistics["m_cx_diff"]++;
	  fidset["m_cx_diff"].insert(it->second.fid);
	}
      }
      
      statistics["mem_n"]++;
      
      if (it->second.disksize != 0xfffffffffff1ULL) {
	statistics["d_sync_n"]++;
	if (it->second.size != 0xfffffffffff1ULL) {
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
 * Reset(clear) the contents of the DB
 * 
 * @param fsid filesystem id
 * 
 * @return true if deleted, false if it does not exist
 */
/*----------------------------------------------------------------------------*/
bool
FmdSqliteHandler::ResetDB(eos::common::FileSystem::fsid_t fsid) 
{
  bool rc = true;
  eos_static_info("");
  eos::common::RWMutexWriteLock lock(Mutex);
  // erase the hash entry
  if (FmdSqliteMap.count(fsid)) {
    // delete in the in-memory hash
    FmdSqliteMap[fsid].clear();

    char deleteentry[16384];
    snprintf(deleteentry,sizeof(deleteentry),"delete from fst where 1");
    Qr.clear();

    // delete in the local DB
    if ((sqlite3_exec(DB[fsid],deleteentry, CallBack, this, &ErrMsg))) {
      eos_err("unable to delete all from fst table - msg=%s\n",ErrMsg);
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
    eos_err("unable to trim DB for fsid=%lu - DB not open", (unsigned long) fsid);
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
  sprintf(serialized,"id=%llu&cid=%llu&ctime=%lu&ctime_ns=%lu&mtime=%lu&"
          "mtime_ns=%lu&size=%llu&checksum=%s&lid=%lu&uid=%u&gid=%u&",
          fMd.fid, fMd.cid, fMd.ctime, fMd.ctime_ns, fMd.mtime, fMd.mtime_ns, fMd.size,
          fMd.checksum.c_str(),fMd.lid,fMd.uid,fMd.gid);
  return new XrdOucEnv(serialized);
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
       !env.Get("gid"))

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
       !env.Get("ctime") ||
       !env.Get("ctime_ns") ||
       !env.Get("mtime") ||
       !env.Get("mtime_ns") ||
       !env.Get("size") ||
       !env.Get("checksum") ||
       !env.Get("lid") ||
       !env.Get("uid") ||
       !env.Get("gid"))
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
  fmd.mgmchecksum     = env.Get("checksum");
  fmd.locations       = env.Get("location")?env.Get("location"):"";
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
FmdSqliteHandler::GetRemoteFmdSqlite( const char*            manager,
                                      const char*            shexfid,
                                      const char*            sfsid,
                                      struct FmdSqlite::FMD& fmd )
{
  if ( (!manager) || (!shexfid) || (!sfsid) ) {
    return EINVAL;
  }

  int rc = 0;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  XrdCl::XRootDStatus status;
  XrdOucString fmdquery = "/?fst.pcmd=getfmd&fst.getfmd.fid=";
  fmdquery += shexfid;
  fmdquery += "&fst.getfmd.fsid=";
  fmdquery += sfsid;

  XrdOucString address = "root://";
  address += manager;
  address += "//dummy";
  XrdCl::URL url( address.c_str() );

  if ( !url.IsValid() ) {
    eos_static_err( "error=URL is not valid: %s", address.c_str() );
    return EINVAL;
  }

  //............................................................................
  // Get XrdCl::FileSystem object
  //............................................................................
  XrdCl::FileSystem* fs = new XrdCl::FileSystem( url );

  if ( !fs ) {
    eos_static_err( "error=failed to get new FS object" );
    return EINVAL;
  }
  
  arg.FromString( fmdquery.c_str() );
  status = fs->Query( XrdCl::QueryCode::OpaqueFile, arg, response );
  
  if ( status.IsOK() ) {
    rc = 0;
    eos_static_debug( "got replica file meta data from server %s for fid=%s fsid=%s",
                     manager, shexfid, sfsid );
  }
  else {
    rc = ECOMM;
    eos_static_err( "Unable to retrieve meta data from server %s for fid=%s fsid=%s",
                   manager, shexfid, sfsid );
  }

  // delete the FileSystem object
  delete fs;
  
  if (rc) {
    delete response;
    return EIO;
  }

  if (!strncmp( response->GetBuffer(), "ERROR", 5)) {
    // remote side couldn't get the record
    eos_static_info( "Unable to retrieve meta data on remote server %s for fid=%s fsid=%s",
                     manager, shexfid, sfsid);
    delete response;
    return ENODATA;
  }
  
  // get the remote file meta data into an env hash
  XrdOucEnv fmdenv( response->GetBuffer() );

  if (!FmdSqlite::EnvFstToFmdSqlite(fmdenv, fmd)) {
    int envlen;
    eos_static_err("Failed to unparse file meta data %s", fmdenv.Env(envlen));
    delete response;
    return EIO;
  }
  // very simple check
  if (fmd.fid != eos::common::FileId::Hex2Fid(shexfid)) {
    eos_static_err( "Uups! Received wrong meta data from remote server - fid is %lu instead of %lu !",
                    fmd.fid, eos::common::FileId::Hex2Fid( shexfid ) );
    delete response;
    return EIO;
  }

  delete response;
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
FmdSqliteHandler::GetMgmFmdSqlite( const char*                   manager,
                                   eos::common::FileId::fileid_t fid,
                                   struct FmdSqlite::FMD&        fmd )
{
  if ( (!manager) || (!fid) ) {
    return EINVAL;
  }
  
  int rc = 0;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  XrdCl::XRootDStatus status;
  char sfmd[1024];
  snprintf(sfmd,sizeof(sfmd)-1,"%llu", fid);
  XrdOucString fmdquery="/?mgm.pcmd=getfmd&mgm.getfmd.fid=";
  fmdquery += sfmd;

  XrdOucString address = "root://";
  address += manager;
  address += "//dummy";

  XrdCl::URL url( address.c_str() );

  if ( !url.IsValid() ) {
    eos_err( "error=URL is not valid: %s", address.c_str() );
    return EINVAL;
  }

  //............................................................................
  // Get XrdCl::FileSystem object
  //............................................................................
  XrdCl::FileSystem* fs = new XrdCl::FileSystem( url );

  if ( !fs ) {
    eos_err( "error=failed to get new FS object" );
    return EINVAL;
  }

  arg.FromString( fmdquery.c_str() );
  status = fs->Query( XrdCl::QueryCode::OpaqueFile, arg, response );

  if ( status.IsOK() ) {
    rc = 0;
    eos_static_debug( "got replica file meta data from mgm %s for fid=%08llx",
                      manager, fid );
  }
  else {
    rc = ECOMM;
    eos_static_err( "Unable to retrieve meta data from mgm %s for fid=%08llx",
                    manager, fid );
  }
        
  delete fs;

  if (rc) {
    delete response;
    return EIO;
  }

  std::string sresult = response->GetBuffer();
  
  if ( (sresult.find("getfmd: retc=0 ")) == std::string::npos ) {
    // remote side couldn't get the record
    eos_static_info( "Unable to retrieve meta data on remote mgm %s for fid=%08llx - result=%s",
                     manager, fid, response->GetBuffer() );
    delete response;
    return ENODATA;
  } else {
    // truncate 'getfmd: retc=0 ' away
    sresult.erase(0,15);
  }

  // get the remote file meta data into an env hash
  XrdOucEnv fmdenv(sresult.c_str());
  
  if (!FmdSqlite::EnvMgmToFmdSqlite(fmdenv, fmd)) {
    int envlen;
    eos_static_err("Failed to unparse file meta data %s", fmdenv.Env(envlen));
    delete response;
    return EIO;
  }
  // very simple check
  if (fmd.fid != fid) {
    eos_static_err( "Uups! Received wrong meta data from remote server - fid is %lu instead of %lu !",
                    fmd.fid, fid);
    delete response;
    return EIO;
  }

  delete response;
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
FmdSqliteHandler::GetRemoteAttribute(const char*   manager,
                                     const char*   key,
                                     const char*   path,
                                     XrdOucString& attribute)
{
  if ( (!manager) || (!key) || (!path) ) {
    return EINVAL;
  }

  int rc = 0;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  XrdCl::XRootDStatus status;
  XrdOucString fmdquery = "/?fst.pcmd=getxattr&fst.getxattr.key=";
  fmdquery += key;
  fmdquery += "&fst.getxattr.path=";
  fmdquery += path;
  
  XrdOucString address = "root://";
  address += manager;
  address += "//dummy";

  XrdCl::URL url( address.c_str() );

  if ( !url.IsValid() ) {
    eos_err( "error=URL is not valid: %s", address.c_str() );
    return EINVAL;
  }

  //............................................................................
  // Get XrdCl::FileSystem object
  //............................................................................
  XrdCl::FileSystem* fs = new XrdCl::FileSystem( url );

  if ( !fs ) {
    eos_err( "error=failed to get new FS object" );
    return EINVAL;
  }

  arg.FromString( fmdquery.c_str() );
  status = fs->Query( XrdCl::QueryCode::OpaqueFile, arg, response );

  if ( status.IsOK() ) {
    rc = 0;
    eos_debug("got attribute meta data from server %s for key=%s path=%s"
              " attribute=%s",manager, key, path, response->GetBuffer() );
  }
  else {
    rc = ECOMM;
    eos_err("Unable to retrieve meta data from server %s for key=%s path=%s",
                   manager, key, path);
  }

  delete fs;

  if (rc) {
    delete response;
    return EIO;
  }

  if ( !strncmp( response->GetBuffer(), "ERROR", 5) ) {
    // remote side couldn't get the record
    eos_info( "Unable to retrieve meta data on remote server %s for key=%s path=%s",
              manager, key, path );
    delete response;
    return ENODATA;
  }

  attribute = response->GetBuffer();
  delete response;

  return 0;
}

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_END



//  LocalWords:  ResyncAllMgm
