// ------------- ---------------------------------------------------------
// File: FmdDbMap.cc
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

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "common/FileId.hh"
#include "common/Path.hh"
#include "common/Attr.hh"
#include "common/DbMap.hh"
#include "fst/FmdDbMap.hh"
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
#include <algorithm>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
FmdDbMapHandler gFmdDbMapHandler; //< static
/*----------------------------------------------------------------------------*/

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
FmdDbMapHandler::SetDBFile (const char* dbfileprefix, int fsid, XrdOucString option)
{
  bool isattached = false;
  {
    // we first check if we have already this DB open - in this case we first do a shutdown
    eos::common::RWMutexReadLock lock(Mutex);
    if (dbmap.count(fsid))
    {
      isattached = true;
    }
  }

  if (isattached)
  {
    if(ShutdownDB(fsid))
      isattached = false;
  }

  eos::common::RWMutexWriteLock lock(Mutex);

  if(!isattached)
  { 
    dbmap[fsid]=new eos::common::DbMap();
  }


  //! -when we successfully attach to a DB we set the mode to S_IRWXU & ~S_IRGRP
  //! -when we shutdown the daemon clean we set the mode back to S_IRWXU | S_IRGRP
  //! -when we attach and the mode is S_IRWXU & ~S_IRGRP we know that the DB has not been shutdown properly and we set a 'dirty' flag to force a full resynchronization

  char fsDBFileName[1024];
  sprintf(fsDBFileName, "%s.%04d.%s", dbfileprefix, fsid, eos::common::DbMap::getDbType().c_str());
  eos_info("%s DB is now %s\n", eos::common::DbMap::getDbType().c_str(), fsDBFileName);

  // store the DB file name
  DBfilename[fsid] = fsDBFileName;

  // check the mode of the DB
  struct stat buf;
  int src = 0;
  if ((src = stat(fsDBFileName, &buf)) || ((buf.st_mode & S_IRGRP) != S_IRGRP))
  {
    isDirty[fsid] = true;
    stayDirty[fsid] = true;
    eos_warning("setting %s file dirty - unclean shutdown detected", eos::common::DbMap::getDbType().c_str());
    if (!src)
    {
      if (chmod(DBfilename[fsid].c_str(), S_IRWXU | S_IRGRP))
      {
        eos_crit("failed to switch the %s database file mode to S_IRWXU | S_IRGRP errno=%d", eos::common::DbMap::getDbType().c_str(), errno);
      }
    }
  }
  else
  {
    isDirty[fsid] = false;
    stayDirty[fsid] = false;
  }

  // create / or attach the db (try to repair if needed)
#ifndef EOS_SQLITE_DBMAP
    eos::common::LvDbDbMapInterface::Option *dbopt=&lvdboption;
#endif

  if(!dbmap[fsid]->attachDb(fsDBFileName,true,0,dbopt)) {
    eos_err("failed to attach %s database file %s", eos::common::DbMap::getDbType().c_str(), fsDBFileName);
    return false;
  }
  else {
    dbmap[fsid]->outOfCore(true);
  }

  // set the mode to S_IRWXU & ~S_IRGRP
  if (chmod(fsDBFileName, S_IRWXU & ~S_IRGRP))
  {
    eos_crit("failed to switch the %s database file mode to S_IRWXU & ~S_IRGRP errno=%d", eos::common::DbMap::getDbType().c_str(), errno);
    return false;
  }

  return true;
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
FmdDbMapHandler::ShutdownDB (eos::common::FileSystem::fsid_t fsid)
{
  eos::common::RWMutexWriteLock lock(Mutex);
  eos_info("%s DB shutdown for fsid=%lu\n", eos::common::DbMap::getDbType().c_str(), (unsigned long) fsid);
  if (dbmap.count(fsid))
  {
    if (!stayDirty[fsid])
    {
      // if there was a complete boot procedure done, we remove the dirty flag
      // set the mode back to S_IRWXU | S_IRGRP
      if (chmod(DBfilename[fsid].c_str(), S_IRWXU | S_IRGRP))
      {
        eos_crit("failed to switch the %s database file to S_IRWXU | S_IRGRP errno=%d", eos::common::DbMap::getDbType().c_str(), errno);
      }
    }
    if (dbmap[fsid]->detachDb())
    {
      delete dbmap[fsid];
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
FmdDbMapHandler::CompareMtime (const void* a, const void *b)
{

  struct filestat
  {
    struct stat buf;
    char filename[1024];
  };
  return ( (((struct filestat*) b)->buf.st_mtime) - ((struct filestat*) a)->buf.st_mtime);
}

/*----------------------------------------------------------------------------*/
/** 
 * Return or Create an Fmd struct for the given file/filesystem id for user uid/gid and layout layoutid
 * 
 * @param fid file id
 * @param fsid filesystem id
 * @param uid user id of the caller
 * @param gid group id of the caller
 * @param layoutid layout id used to store during creation
 * @param isRW indicates if we create a not existing Fmd 
 * 
 * @return pointer to Fmd struct if successfull otherwise 0
 */

/*----------------------------------------------------------------------------*/
FmdHelper*
FmdDbMapHandler::GetFmd (eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid, uid_t uid, gid_t gid, eos::common::LayoutId::layoutid_t layoutid, bool isRW, bool force)
{
  // eos_info("fid=%08llx fsid=%lu", fid, (unsigned long) fsid);

  if (fid == 0)
  {
    eos_warning("fid=0 requested for fsid=", fsid);
    return 0;
  }

  Mutex.LockRead();

  if (dbmap.count(fsid))
  {
    bool entryexist=ExistFmd(fid,fsid);
    Fmd valfmd=RetrieveFmd(fid,fsid);

    if (dbmap.count(fsid) && entryexist)
    {
      // this is to read an existing entry
      FmdHelper* fmd = new FmdHelper();
      if (!fmd)
      {
        Mutex.UnLockRead();
        return 0;
      }

      // make a copy of the current record
      fmd->Replicate(valfmd);

      if (fmd->fMd.fid() != fid)
      {
        // fatal this is somehow a wrong record!
        eos_crit("unable to get fmd for fid %llu on fs %lu - file id mismatch in meta data block (%llu)", fid, (unsigned long) fsid, fmd->fMd.fid());
        delete fmd;
        Mutex.UnLockRead();
        return 0;
      }

      if (fmd->fMd.fsid() != fsid)
      {
        // fatal this is somehow a wrong record!
        eos_crit("unable to get fmd for fid %llu on fs %lu - filesystem id mismatch in meta data block (%llu)", fid, (unsigned long) fsid, fmd->fMd.fsid());
        delete fmd;
        Mutex.UnLockRead();
        return 0;
      }

      // the force flag allows to retrieve 'any' value even with inconsistencies as needed by ResyncAllMgm

      if (!force)
      {
        if (strcmp(eos::common::LayoutId::GetLayoutTypeString(fmd->fMd.lid()), "raid6") &&
            strcmp(eos::common::LayoutId::GetLayoutTypeString(fmd->fMd.lid()), "raiddp") &&
            strcmp(eos::common::LayoutId::GetLayoutTypeString(fmd->fMd.lid()), "archive"))
        {

          // if we have a mismatch between the mgm/disk and 'ref' value in size,  we don't return the Fmd record
          if ((!isRW) && ((fmd->fMd.disksize() && (fmd->fMd.disksize() != fmd->fMd.size())) ||
              (fmd->fMd.mgmsize() && (fmd->fMd.mgmsize() != 0xfffffffffff1ULL) && (fmd->fMd.mgmsize() != fmd->fMd.size()))))
          {
            eos_crit("msg=\"size mismatch disk/mgm vs memory\" fid=%08llx fsid=%lu size=%llu disksize=%llu mgmsize=%llu", fid, (unsigned long) fsid, fmd->fMd.size(), fmd->fMd.disksize(), fmd->fMd.mgmsize());
            delete fmd;
            Mutex.UnLockRead();
            return 0;
          }
        }

        // if we have a mismatch between the mgm/disk and 'ref' value in checksum, we don't return the Fmd record
        // this check we can do only if the file is !zero otherwise we don't have a checksum on disk (e.g. a touch <a> file)
        if ((!isRW) && fmd->fMd.mgmsize() &&
            ((fmd->fMd.diskchecksum().length() && (fmd->fMd.diskchecksum() != fmd->fMd.checksum())) ||
                (fmd->fMd.mgmchecksum().length() && (fmd->fMd.mgmchecksum() != fmd->fMd.checksum()))))
        {
          eos_crit("msg=\"checksum mismatch disk/mgm vs memory\" fid=%08llx "
              "fsid=%lu checksum=%s diskchecksum=%s mgmchecksum=%s",
              fid, (unsigned long) fsid, fmd->fMd.checksum().c_str(),
              fmd->fMd.diskchecksum().c_str(), fmd->fMd.mgmchecksum().c_str());

          delete fmd;
          Mutex.UnLockRead();
          return 0;
        }
      }

      // return the new entry
      Mutex.UnLockRead();
      return fmd;
    }

    if (isRW)
    {
      // make a new record

      struct timeval tv;
      struct timezone tz;

      gettimeofday(&tv, &tz);

      Mutex.UnLockRead(); // <--

      eos::common::RWMutexWriteLock lock(Mutex); // --> (return)

      valfmd.set_uid(uid);
      valfmd.set_gid(gid);
      valfmd.set_lid(layoutid);
      valfmd.set_fsid(fsid);
      valfmd.set_fid(fid);

      valfmd.set_ctime(tv.tv_sec);
      valfmd.set_mtime(tv.tv_sec);
      valfmd.set_atime(tv.tv_sec);
      valfmd.set_ctime_ns(tv.tv_usec * 1000);
      valfmd.set_mtime_ns(tv.tv_usec * 1000);
      valfmd.set_atime_ns(tv.tv_usec * 1000);


      FmdHelper* fmd = new FmdHelper(fid, fsid);
      if (!fmd)
      {
        return 0;
      }

      // make a copy of the current record
      fmd->Replicate(valfmd);

      if (Commit(fmd, false))
      {
        eos_debug("returning meta data block for fid %d on fs %d", fid, (unsigned long) fsid);
        // return the mmaped meta data block

        return fmd;
      }
      else
      {
        eos_crit("unable to write new block for fid %d on fs %d - no changelog db open for writing", fid, (unsigned long) fsid);
        delete fmd;
        return 0;
      }
    }
    else
    {
      eos_warning("unable to get fmd for fid %llu on fs %lu - record not found", fid, (unsigned long) fsid);
      Mutex.UnLockRead();
      return 0;
    }
  }
  else
  {
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
FmdDbMapHandler::DeleteFmd (eos::common::FileId::fileid_t fid, eos::common::FileSystem::fsid_t fsid)
{
  bool rc = true;
  eos_static_info("");
  eos::common::RWMutexWriteLock lock(Mutex);

  bool entryexist=ExistFmd(fid,fsid);

  // erase the hash entry
  if (dbmap.count(fsid) && entryexist)
  {
    // delete in the in-memory hash
    if( dbmap[fsid]->remove(eos::common::Slice((const char*)&fid,sizeof(fid))) )
    {
      eos_err("unable to delete fid=%08llx from fst table\n", fid);
      rc = false;
    }
  }
  else
  {
    rc = false;
  }
  return rc;
}


/*----------------------------------------------------------------------------*/
/** 
 * Commit Fmd to the DB file
 * 
 * @param fmd pointer to Fmd
 * 
 * @return true if record has been commited
 */

/*----------------------------------------------------------------------------*/
bool
FmdDbMapHandler::Commit (FmdHelper* fmd, bool lockit)
{
  if (!fmd)
    return false;

  int fsid = fmd->fMd.fsid();
  int fid = fmd->fMd.fid();

  struct timeval tv;
  struct timezone tz;

  gettimeofday(&tv, &tz);
  fmd->fMd.set_mtime(tv.tv_sec);
  fmd->fMd.set_atime(tv.tv_sec);
  fmd->fMd.set_mtime_ns(tv.tv_usec * 1000);
  fmd->fMd.set_atime_ns(tv.tv_usec * 1000);


  if (lockit)
  {
    // ---->
    Mutex.LockWrite();
  }

  if (dbmap.count(fsid))
  {
    // update in-memory
    if (lockit)
    {
      Mutex.UnLockWrite(); // <----
    }
    return PutFmd(fid,fsid,fmd->fMd);
  }
  else
  {
    eos_crit("no %s DB open for fsid=%llu", eos::common::DbMap::getDbType().c_str(), (unsigned long) fsid);
    if (lockit)
    {
      Mutex.UnLockWrite(); // <----
    }
  }

  return false;
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
FmdDbMapHandler::UpdateFromDisk (eos::common::FileSystem::fsid_t fsid, eos::common::FileId::fileid_t fid, unsigned long long disksize, std::string diskchecksum, unsigned long checktime, bool filecxerror, bool blockcxerror, bool flaglayouterror)
{
  eos::common::RWMutexWriteLock lock(Mutex);

  eos_debug("fsid=%lu fid=%08llx disksize=%llu diskchecksum=%s checktime=%llu fcxerror=%d bcxerror=%d flaglayouterror=%d", (unsigned long) fsid, fid, disksize, diskchecksum.c_str(), checktime, filecxerror, blockcxerror, flaglayouterror);

  if (!fid)
  {
    eos_info("skipping to insert a file with fid 0");
    return false;
  }

  Fmd valfmd=RetrieveFmd(fid,fsid);

  if (dbmap.count(fsid))
  {
    // update in-memory
    valfmd.set_disksize(disksize);
    // fix the reference value from disk
    valfmd.set_size(disksize);
    valfmd.set_checksum(diskchecksum);
    valfmd.set_fid(fid);
    valfmd.set_fsid(fsid);
    valfmd.set_diskchecksum(diskchecksum);
    valfmd.set_checktime(checktime);
    valfmd.set_filecxerror(filecxerror);
    valfmd.set_blockcxerror(blockcxerror);
    if (flaglayouterror)
    {
      // if the mgm sync is run afterwards, every disk file is by construction an
      // orphan, until it is synced from the mgm
      valfmd.set_layouterror(eos::common::LayoutId::kOrphan);
    }
    return PutFmd(fid,fsid,valfmd);
  }
  else
  {
    eos_crit("no %s DB open for fsid=%llu", eos::common::DbMap::getDbType().c_str(), (unsigned long) fsid);
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
FmdDbMapHandler::UpdateFromMgm (eos::common::FileSystem::fsid_t fsid, eos::common::FileId::fileid_t fid, eos::common::FileId::fileid_t cid, eos::common::LayoutId::layoutid_t lid, unsigned long long mgmsize, std::string mgmchecksum, uid_t uid, gid_t gid, unsigned long long ctime, unsigned long long ctime_ns, unsigned long long mtime, unsigned long long mtime_ns, int layouterror, std::string locations)
{
  eos::common::RWMutexWriteLock lock(Mutex);

  eos_debug("fsid=%lu fid=%08llx cid=%llu lid=%lx mgmsize=%llu mgmchecksum=%s",
      (unsigned long) fsid, fid, cid, lid, mgmsize, mgmchecksum.c_str());

  if (!fid)
  {
    eos_info("skipping to insert a file with fid 0");
    return false;
  }

  bool entryexist=ExistFmd(fid,fsid);
  Fmd valfmd=RetrieveFmd(fid,fsid);

  if (dbmap.count(fsid))
  {
    if (!entryexist)
    {
      valfmd.set_disksize(0xfffffffffff1ULL);
    }
    // update in-memory
    valfmd.set_mgmsize(mgmsize);
    valfmd.set_size(mgmsize);
    valfmd.set_checksum(mgmchecksum);
    valfmd.set_mgmchecksum(mgmchecksum);
    valfmd.set_cid(cid);
    valfmd.set_lid(lid);
    valfmd.set_uid(uid);
    valfmd.set_gid(gid);
    valfmd.set_ctime(ctime);
    valfmd.set_ctime_ns(ctime_ns);
    valfmd.set_mtime(mtime);
    valfmd.set_mtime_ns(mtime_ns);
    valfmd.set_layouterror(layouterror);
    valfmd.set_locations(locations);

    // truncate the checksum to the right string length
    unsigned long cslen = eos::common::LayoutId::GetChecksumLen(lid)*2;
    valfmd.set_mgmchecksum(
        std::string(valfmd.mgmchecksum()).erase( std::min( valfmd.mgmchecksum().length(), cslen )) );
    valfmd.set_checksum(
        std::string(valfmd.checksum()).erase( std::min( valfmd.checksum().length(), cslen )) );
    return PutFmd(fid,fsid,valfmd);
  }
  else
  {
    eos_crit("no %s DB open for fsid=%llu", eos::common::DbMap::getDbType().c_str(), (unsigned long) fsid);
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
FmdDbMapHandler::ResetDiskInformation (eos::common::FileSystem::fsid_t fsid)
{
  eos::common::RWMutexWriteLock lock(Mutex);

  if (dbmap.count(fsid))
  {
    const eos::common::DbMapTypes::Tkey *k;
    const eos::common::DbMapTypes::Tval *v;
    eos::common::DbMapTypes::Tval val;
    dbmap[fsid]->beginSetSequence();
    unsigned long cpt=0;
    for ( dbmap[fsid]->beginIter(); dbmap[fsid]->iterate(&k, &v);) {
      Fmd f;
      f.ParseFromString(v->value);
      f.set_disksize(0xfffffffffff1ULL);
      f.set_diskchecksum("");
      f.set_checktime(0);
      f.set_filecxerror(-1);
      f.set_blockcxerror(-1);
      val=*v;
      f.SerializeToString(&val.value);
      dbmap[fsid]->set(*k,val);
      cpt++;
    }
    if( dbmap[fsid]->endSetSequence() != cpt )
      // the setsequence makes that it's impossible to know which key is faulty
    {
      eos_err("unable to update fsid=%lu\n", fsid);
      return false;
    }
  }
  else
  {
    eos_crit("no %s DB open for fsid=%llu", eos::common::DbMap::getDbType().c_str(), (unsigned long) fsid);
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
FmdDbMapHandler::ResetMgmInformation (eos::common::FileSystem::fsid_t fsid)
{
  eos::common::RWMutexWriteLock lock(Mutex);

  if (dbmap.count(fsid))
  {
    const eos::common::DbMapTypes::Tkey *k;
    const eos::common::DbMapTypes::Tval *v;
    eos::common::DbMapTypes::Tval val;
    dbmap[fsid]->beginSetSequence();
    unsigned long cpt=0;

    for ( dbmap[fsid]->beginIter(); dbmap[fsid]->iterate(&k, &v);) {
      Fmd f;
      f.ParseFromString(v->value);
      f.set_mgmsize(0xfffffffffff1ULL);
      f.set_mgmchecksum("");
      f.set_locations("");
      val=*v;
      f.SerializeToString(&val.value);
      dbmap[fsid]->set(*k,val);
      cpt++;
    }
    if( dbmap[fsid]->endSetSequence() != cpt )
      // the setsequence makes that it's impossible to know which key is faulty
    {
      eos_err("unable to update fsid=%lu\n", fsid);
      return false;
    }
  }
  else
  {
    eos_crit("no leveldb DB open for fsid=%llu", (unsigned long) fsid);
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
FmdDbMapHandler::ResyncDisk (const char* path,
    eos::common::FileSystem::fsid_t fsid,
    bool flaglayouterror, bool callautorepair)
{
  bool retc = true;
  eos::common::Path cPath(path);
  eos::common::FileId::fileid_t fid = eos::common::FileId::Hex2Fid(cPath.GetName());
  off_t disksize = 0;
  if (fid)
  {
    eos::common::Attr *attr = eos::common::Attr::OpenAttr(path);
    if (attr)
    {
      struct stat buf;
      if ((!stat(path, &buf)) && S_ISREG(buf.st_mode))
      {
        std::string checksumType, checksumStamp, filecxError, blockcxError;
        std::string diskchecksum = "";
        char checksumVal[SHA_DIGEST_LENGTH];
        size_t checksumLen = 0;

        unsigned long checktime = 0;
        // got the file size
        disksize = buf.st_size;
        memset(checksumVal, 0, sizeof (checksumVal));
        checksumLen = SHA_DIGEST_LENGTH;
        if (!attr->Get("user.eos.checksum", checksumVal, checksumLen))
        {
          checksumLen = 0;
        }

        checksumType = attr->Get("user.eos.checksumtype");
        filecxError = attr->Get("user.eos.filecxerror");
        blockcxError = attr->Get("user.eos.blockcxerror");

        checktime = (strtoull(checksumStamp.c_str(), 0, 10) / 1000000);
        if (checksumLen)
        {
          // retrieve a checksum object to get the hex representation
          XrdOucString envstring = "eos.layout.checksum=";
          envstring += checksumType.c_str();
          XrdOucEnv env(envstring.c_str());
          int checksumtype = eos::common::LayoutId::GetChecksumFromEnv(env);
          eos::common::LayoutId::layoutid_t layoutid = eos::common::LayoutId::GetId(eos::common::LayoutId::kPlain, checksumtype);
          eos::fst::CheckSum *checksum = eos::fst::ChecksumPlugins::GetChecksumObject(layoutid, false);

          if (checksum)
          {
            if (checksum->SetBinChecksum(checksumVal, checksumLen))
            {
              diskchecksum = checksum->GetHexChecksum();
            }
            delete checksum;
          }
        }

        // now updaAte the DB
        if (!UpdateFromDisk(fsid, fid, disksize, diskchecksum, checktime, (filecxError == "1") ? 1 : 0, (blockcxError == "1") ? 1 : 0, flaglayouterror))
        {
          eos_err("failed to update %s DB for fsid=%lu fid=%08llx", eos::common::DbMap::getDbType().c_str(), (unsigned long) fsid, fid);
          retc = false;
        }
      }
      delete attr;
    }
  }
  else
  {
    eos_debug("would convert %s (%s) to fid 0", cPath.GetName(), path);
    retc = false;
    ;
  }
  return retc;
}

/*----------------------------------------------------------------------------*/
/** 
 * Resync files under path into DB
 * 
 * @param path path to scan
 * @param fsid file system id
 * 
 * @return true if successfull
 */

/*----------------------------------------------------------------------------*/
bool
FmdDbMapHandler::ResyncAllDisk (const char* path,
    eos::common::FileSystem::fsid_t fsid,
    bool flaglayouterror)
{
  char **paths = (char**) calloc(2, sizeof (char*));
  paths[0] = (char*) path;
  paths[1] = 0;
  if (!paths)
  {
    return false;
  }

  if (flaglayouterror)
  {
    isSyncing[fsid] = true;
  }

  if (!ResetDiskInformation(fsid))
  {
    eos_err("failed to reset the disk information before resyncing");
    return false;
  }
  // scan all the files
  FTS *tree = fts_open(paths, FTS_NOCHDIR, 0);

  if (!tree)
  {
    eos_err("fts_open failed");
    free(paths);
    return false;
  }

  FTSENT *node;
  unsigned long long cnt = 0;
  while ((node = fts_read(tree)))
  {
    if (node->fts_level > 0 && node->fts_name[0] == '.')
    {
      fts_set(tree, node, FTS_SKIP);
    }
    else
    {
      if (node->fts_info == FTS_F)
      {
        XrdOucString filePath = node->fts_accpath;
        if (!filePath.matches("*.xsmap"))
        {
          cnt++;
          eos_debug("file=%s", filePath.c_str());
          ResyncDisk(filePath.c_str(), fsid, flaglayouterror);
          if (!(cnt % 10000))
          {
            eos_info("msg=\"synced files so far\" nfiles=%llu fsid=%lu", cnt, (unsigned long) fsid);
          }
        }
      }
    }
  }
  if (fts_close(tree))
  {
    eos_err("fts_close failed");
    free(paths);
    return false;
  }

  free(paths);
  return true;
}

/*----------------------------------------------------------------------------*/
/** 
 * Resync meta data from MGM into LEVELDB DB
 * 
 * @param fsid filesystem id
 * @param fid  file id
 * 
 * @return true if successfull
 */

/*----------------------------------------------------------------------------*/
bool
FmdDbMapHandler::ResyncMgm (eos::common::FileSystem::fsid_t fsid,
    eos::common::FileId::fileid_t fid,
    const char* manager)
{
  struct Fmd fMd;
  FmdHelper::Reset(fMd);
  int rc = 0;
  if ((!(rc = GetMgmFmd(manager, fid, fMd))) ||
      (rc == ENODATA))
  {
    if (rc == ENODATA)
    {
      eos_warning("no such file on MGM for fid=%llu", fid);
      fMd.set_fid(fid);
      if (fid == 0)
      {
        eos_warning("removing fid=0 entry");
        return DeleteFmd(fMd.fid(), fsid);
      }
    }

    // define layouterrors
    fMd.set_layouterror(FmdHelper::LayoutError(fsid, fMd.lid(), fMd.locations()));

    // get an existing record without creation of a record !!!
    FmdHelper* fmd = GetFmd(fMd.fid(), fsid, fMd.uid(), fMd.gid(), fMd.lid(), false, true);
    if (fmd)
    {
      // check if there was a disk replica
      if (fmd->fMd.disksize() == 0xfffffffffff1ULL)
      {
        if (fMd.layouterror() && eos::common::LayoutId::kUnregistered)
        {
          // there is no replica supposed to be here and there is nothing on disk, so remove it from the SLIQTE database
          eos_warning("removing <ghost> entry for fid=%llu on fsid=%lu", fid, (unsigned long) fsid);
          delete fmd;
          return DeleteFmd(fMd.fid(), fsid);
        }
        else
        {
          // we proceed 
          delete fmd;
        }
      }
    }
    else
    {
      if (fMd.layouterror() && eos::common::LayoutId::kUnregistered)
      {
        // this entry is deleted and we are not supposed to have it
        return true;
      }
    }

    if ((!fmd) && (rc == ENODATA))
    {
      // no file on MGM and no file locally
      eos_info("fsid=%lu fid=%08lxx msg=\"file removed in the meanwhile\"", fsid, fid);
      return true;
    }

    if (fmd)
    {
      delete fmd;
    }

    // get/create a record
    fmd = GetFmd(fMd.fid(), fsid, fMd.uid(), fMd.gid(), fMd.lid(), true, true);
    if (fmd)
    {
      if (!UpdateFromMgm(fsid, fMd.fid(), fMd.cid(), fMd.lid(), fMd.mgmsize(), fMd.mgmchecksum(), fMd.uid(), fMd.gid(), fMd.ctime(), fMd.ctime_ns(), fMd.mtime(), fMd.mtime_ns(), fMd.layouterror(), fMd.locations()))
      {
        eos_err("failed to update fmd for fid=%08llx", fid);
        delete fmd;
        return false;
      }
      // check if it exists on disk
      if (fmd->fMd.disksize() == 0xfffffffffff1ULL)
      {
        fMd.set_layouterror( fMd.layouterror() | eos::common::LayoutId::kMissing);
        eos_warning("found missing replica for fid=%llu on fsid=%lu", fid, (unsigned long) fsid);
      }

      // check if it exists on disk and on the mgm
      if ((fmd->fMd.disksize() == 0xfffffffffff1ULL) && (fmd->fMd.mgmsize() == 0xfffffffffff1ULL))
      {
        // there is no replica supposed to be here and there is nothing on disk, so remove it from the SLIQTE database
        eos_warning("removing <ghost> entry for fid=%llu on fsid=%lu", fid, (unsigned long) fsid);
        delete fmd;
        return DeleteFmd(fMd.fid(), fsid);
      }
      delete fmd;
    }
    else
    {
      eos_err("failed to get/create fmd for fid=%08llx", fid);
      return false;
    }
  }
  else
  {
    eos_err("failed to retrieve MGM fmd for fid=%08llx", fid);
    return false;
  }

  return true;
}

/*----------------------------------------------------------------------------*/
/** 
 * Resync all meta data from MGM into DB
 * 
 * @param fsid filesystem id
 * 
 * @return true if successfull
 */

/*----------------------------------------------------------------------------*/
bool
FmdDbMapHandler::ResyncAllMgm (eos::common::FileSystem::fsid_t fsid, const char* manager)
{

  if (!ResetMgmInformation(fsid))
  {
    eos_err("failed to reset the mgm information before resyncing");
    return false;
  }

  XrdOucString consolestring = "/proc/admin/?&mgm.format=fuse&mgm.cmd=fs&mgm.subcmd=dumpmd&mgm.dumpmd.storetime=1&mgm.dumpmd.option=m&mgm.fsid=";
  consolestring += (int) fsid;
  XrdOucString url = "root://";
  url += manager;
  url += "//";
  url += consolestring;

  // we run an external command and parse the output
  char* tmpfile = tempnam("/tmp/", "efstd");
  XrdOucString cmd = "env XrdSecPROTOCOL=sss xrdcp -s \"";
  cmd += url;
  cmd += "\" ";
  cmd += tmpfile;
  int rc = system(cmd.c_str());
  if (WEXITSTATUS(rc))
  {
    eos_err("%s returned %d", cmd.c_str(), WEXITSTATUS(rc));
    return false;
  }
  else
  {
    eos_debug("%s executed successfully", cmd.c_str());
  }

  // parse the result
  std::ifstream inFile(tmpfile);
  std::string dumpentry;

  unsigned long long cnt = 0;
  while (std::getline(inFile, dumpentry))
  {
    cnt++;
    eos_debug("line=%s", dumpentry.c_str());
    XrdOucEnv* env = new XrdOucEnv(dumpentry.c_str());
    if (env)
    {
      struct Fmd fMd;
      FmdHelper::Reset(fMd);
      if (EnvMgmToFmdSqlite(*env, fMd))
      {
        // get/create one
        FmdHelper* fmd = GetFmd(fMd.fid(), fsid, fMd.uid(), fMd.gid(), fMd.lid(), true, true);

        fMd.set_layouterror(FmdHelper::LayoutError(fsid, fMd.lid(), fMd.locations()));

        if (fmd)
        {
          // check if it exists on disk
          if (fmd->fMd.disksize() == 0xfffffffffff1ULL)
          {
            fMd.set_layouterror(fMd.layouterror() | eos::common::LayoutId::kMissing);
            eos_warning("found missing replica for fid=%llu on fsid=%lu", fMd.fid(), (unsigned long) fsid);
          }

          if (!UpdateFromMgm(fsid, fMd.fid(), fMd.cid(), fMd.lid(), fMd.mgmsize(), fMd.mgmchecksum(), fMd.uid(), fMd.gid(), fMd.ctime(), fMd.ctime_ns(), fMd.mtime(), fMd.mtime_ns(), fMd.layouterror(), fMd.locations()))
          {
            eos_err("failed to update fmd %s", dumpentry.c_str());
          }
          delete fmd;
        }
        else
        {
          eos_err("failed to get/create fmd %s", dumpentry.c_str());
        }
      }
      else
      {
        eos_err("failed to convert %s", dumpentry.c_str());
      }
      delete env;
    }
    if (!(cnt % 10000))
    {
      eos_info("msg=\"synced files so far\" nfiles=%llu fsid=%lu", cnt, (unsigned long) fsid);
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
FmdDbMapHandler::Query (eos::common::FileSystem::fsid_t fsid,
    std::string query,
    std::vector<eos::common::FileId::fileid_t>& fidvector)
{
  // NOT IMPLEMENTED
  return 0;
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
FmdDbMapHandler::GetInconsistencyStatistics (eos::common::FileSystem::fsid_t fsid,
    std::map<std::string, size_t>& statistics,
    std::map<std::string, std::set < eos::common::FileId::fileid_t> > &fidset)
{
  eos::common::RWMutexReadLock lock(Mutex);

  if (!dbmap.count(fsid))
    return false;

  // query in-memory
  statistics["mem_n"] = 0; // number of files in DB

  statistics["d_sync_n"] = 0; // number of synced files from disk
  statistics["m_sync_n"] = 0; // number of synced files from MGM server

  statistics["d_mem_sz_diff"] = 0; // number of files with disk and reference size mismatch
  statistics["m_mem_sz_diff"] = 0; // number of files with MGM and reference size mismatch

  statistics["d_cx_diff"] = 0; // number of files with disk and reference checksum mismatch
  statistics["m_cx_diff"] = 0; // number of files with MGM and reference checksum mismatch

  statistics["orphans_n"] = 0; // number of orphaned replicas
  statistics["unreg_n"] = 0; // number of unregistered replicas
  statistics["rep_diff_n"] = 0; // number of files with replica number mismatch
  statistics["rep_missing_n"] = 0; // number of files which are missing on disk

  fidset["m_sync_n"].clear(); // file set's for the same items as above
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

  if (!IsSyncing(fsid))
  {
    const eos::common::DbMapTypes::Tkey *k;
    const eos::common::DbMapTypes::Tval *v;
    eos::common::DbMapTypes::Tval val;

    // we report values only when we are not in the sync phase from disk/mgm
    for ( dbmap[fsid]->beginIter(); dbmap[fsid]->iterate(&k, &v);) {
      Fmd f;
      f.ParseFromString(v->value);

      if (f.layouterror())
      {
        if (f.layouterror() & eos::common::LayoutId::kOrphan)
        {
          statistics["orphans_n"]++;
          fidset["orphans_n"].insert(f.fid());
        }
        if (f.layouterror() & eos::common::LayoutId::kUnregistered)
        {
          statistics["unreg_n"]++;
          fidset["unreg_n"].insert(f.fid());
        }
        if (f.layouterror() & eos::common::LayoutId::kReplicaWrong)
        {
          statistics["rep_diff_n"]++;
          fidset["rep_diff_n"].insert(f.fid());
        }
        if (f.layouterror() & eos::common::LayoutId::kMissing)
        {
          statistics["rep_missing_n"]++;
          fidset["rep_missing_n"].insert(f.fid());
        }
      }

      if (f.mgmsize() != 0xfffffffffff1ULL)
      {
        statistics["m_sync_n"]++;
        if (f.size() != 0xfffffffffff1ULL)
        {
          if (f.size() != f.mgmsize())
          {
            statistics["m_mem_sz_diff"]++;
            fidset["m_mem_sz_diff"].insert(f.fid());
          }
        }
      }

      if (!f.layouterror())
      {
        if (f.size() && f.diskchecksum().length() && (f.diskchecksum() != f.checksum()))
        {
          statistics["d_cx_diff"]++;
          fidset["d_cx_diff"].insert(f.fid());
        }

        if (f.size() && f.mgmchecksum().length() && (f.mgmchecksum() != f.checksum()))
        {
          statistics["m_cx_diff"]++;
          fidset["m_cx_diff"].insert(f.fid());
        }
      }

      statistics["mem_n"]++;

      if (f.disksize() != 0xfffffffffff1ULL)
      {
        statistics["d_sync_n"]++;
        if (f.size() != 0xfffffffffff1ULL)
        {
          if (f.size() != f.disksize())
          {
            statistics["d_mem_sz_diff"]++;
            fidset["d_mem_sz_diff"].insert(f.fid());
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
FmdDbMapHandler::ResetDB (eos::common::FileSystem::fsid_t fsid)
{
  bool rc = true;
  eos_static_info("");
  eos::common::RWMutexWriteLock lock(Mutex);
  // erase the hash entry
  if (dbmap.count(fsid))
  {
    // delete in the in-memory hash
    if(!dbmap[fsid]->clear())
    {
      eos_err("unable to delete all from fst table\n");
      rc = false;
    }
    else
    {
      rc = true;
    }
  }
  else
  {
    rc = false;
  }
  return rc;
}


bool
FmdDbMapHandler::TrimDB()
{
  std::map<eos::common::FileSystem::fsid_t, eos::common::DbMap*>::iterator it;

  for (it = dbmap.begin(); it != dbmap.end(); ++it)
  {
    eos_static_info("Trimming fsid=%llu ", it->first);

    if (!it->second->trimDb())
    {
      eos_static_err("Cannot trim the DB file for fsid=%llu ", it->first);
      return false;
    }
    else
    {
      eos_static_info("Trimmed %s DB file for fsid=%llu ", it->second->getDbType().c_str(), it->first);
    }
  }
  return true;
}

/*----------------------------------------------------------------------------*/
/** 
 * Trim the DB for a given filesystem id
 * 
 * @param fsid file system id
 * @param option - not used
 * 
 * @return true if successful otherwise false
 */

/*----------------------------------------------------------------------------*/
bool
FmdDbMapHandler::TrimDBFile (eos::common::FileSystem::fsid_t fsid, XrdOucString option)
{
  return true;
}

EOSFSTNAMESPACE_END



//  LocalWords:  ResyncAllMgm
