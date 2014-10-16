// ----------------------------------------------------------------------
// File: DbMapLevelDb.cc
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
#include "common/DbMapLevelDb.hh"
/*----------------------------------------------------------------------------*/
#ifndef EOS_SQLITE_DBMAP
/*----------------------------------------------------------------------------*/
#include <sys/stat.h>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

LvDbInterfaceBase::Option LvDbInterfaceBase::gDefaultOption =
{ 10,100};
unsigned LvDbInterfaceBase::pNInstances = 0;
bool LvDbInterfaceBase::pDebugMode = false;
bool LvDbInterfaceBase::pAbortOnLvDbError = true;
RWMutex LvDbInterfaceBase::pDbMgmtMutex;
std::map<std::string , std::pair< std::pair<leveldb::DB*,leveldb::Options* > , int> > LvDbInterfaceBase::pName2CountedDb;
std::map<leveldb::DB* , std::pair<std::string,int> > LvDbInterfaceBase::pDb2CountedName;

pthread_t LvDbDbLogInterface::gArchThread;
bool LvDbDbLogInterface::gArchThreadStarted = false;
XrdSysMutex LvDbDbLogInterface::gUniqMutex;
XrdSysCondVar LvDbDbLogInterface::gArchmutex (0);
LvDbDbLogInterface::tTimeToPeriodedFile LvDbDbLogInterface::gArchQueue;
LvDbDbLogInterface::tMapFileToDb LvDbDbLogInterface::gFile2Db;

/*----------------------------------------------------------------------------*/
LvDbDbLogInterface::LvDbDbLogInterface ()
{
  init();
  pDbName = "";
  pDb = NULL;
}

LvDbDbLogInterface::LvDbDbLogInterface (const string &dbname, int volumeduration, int createperm, void *option)
{
  init();
  setDbFile(dbname, volumeduration, createperm, option);
}

LvDbDbLogInterface::~LvDbDbLogInterface ()
{
  setDbFile("", -1, 0,NULL);
  gUniqMutex.Lock();
  if (gFile2Db.empty() && gArchThreadStarted)
  {
    if (pDebugMode) printf("Shuting down archiving thread\n");
    XrdSysThread::Cancel(gArchThread);
    gArchmutex.Signal(); // wake up the thread to reach the cancel point
    gArchThreadStarted = false;
    XrdSysThread::Join(gArchThread, NULL);
  }
  gUniqMutex.UnLock();
  AtomicDec(pNInstances);
}

void
LvDbDbLogInterface::init ()
{
  pIsOpen = false;
  AtomicInc(pNInstances);
}

void
LvDbDbLogInterface::archiveThreadCleanup (void *dummy)
{
  gArchmutex.UnLock();
  if (pDebugMode) printf("Cleaning up archive thread\n");
  fflush(stdout);
}

void*
LvDbDbLogInterface::archiveThread (void *dummy)
{
  pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
  pthread_cleanup_push(LvDbDbLogInterface::archiveThreadCleanup, NULL); // this is to make sure the mutex ius unlocked before terminating the thread
  gArchmutex.Lock();
  while (true)
  {
    timespec now;
    eos::common::Timing::GetTimeSpec(now);

    // process and erase the entry of the queue of which are outdated
    long int nextinthefuture=-1;
    if (gArchQueue.size() != 0)
    {
      for (tTimeToPeriodedFile::iterator it = gArchQueue.begin(); it != gArchQueue.end(); )
      {
        if (now < it->first)
        {
          nextinthefuture = it->first.tv_sec;
          break;
        }
        if(!archive(it))
        updateArchiveSchedule(it++); // if the archiving was successful, plan the next one for the same log
        // if it was not successful leave this archiving task in the queue
        else
        {
          eos_static_warning("Error trying to archive %s, will retry soon",it->second.first.c_str());
          it++;
        }
      }
    }
    // sleep untill the next archving has to happen or untill a new archive is added to the queue
    const long int failedArchivingRetryDelay = 300;// retry the failed archiving operations in 5 minutes
    int waketime;
    if(gArchQueue.empty())
    waketime = now.tv_sec + 3600;
    else
    {
      if(now < gArchQueue.begin()->first)
      {
        waketime = gArchQueue.begin()->first.tv_sec; // the first task in the queue is in the future
      }
      else
      {
        if( nextinthefuture >0 ) // there is a task in the future and some failed tasks to run again
        waketime = std::min( now.tv_sec+failedArchivingRetryDelay , nextinthefuture );
        else// there are only failed tasks to run again
        waketime = now.tv_sec+failedArchivingRetryDelay;
      }
    }
    int timedout = gArchmutex.Wait(waketime - time(0));
    if (timedout) sleep(5); // a timeout to let the db requests started just before the deadline complete, possible cancellation point
  }
  pthread_cleanup_pop(0);
  return NULL;
}

int
LvDbDbLogInterface::archive (const tTimeToPeriodedFile::iterator &entry)
{
  char timeformat[32];
  char *cptr = timeformat;
  tm t1, t2;
  localtime_r(&entry->first.tv_sec, &t1);
  t2 = t1;
  cptr += sprintf(timeformat, "%%y-%%m-%%d-%%a");
  switch (entry->second.second)
  {
    case testly:
    t1.tm_sec -= 10;
    sprintf(cptr, "_%%Hh%%Mm%%Ss");
    break;
    case hourly:
    t1.tm_hour--;
    sprintf(cptr, "_%%Hh%%Mm%%Ss");
    break;
    case daily:
    t1.tm_mday--;
    break;
    case weekly:
    t1.tm_mday -= 7;
    break;
    default:
    t1.tm_sec -= entry->second.second;
    printf(cptr, "_%%Hh%%Mm%%Ss");
    break;
  }
  time_t tt;
  localtime_r(&(tt = mktime(&t1)), &t1);
  localtime_r(&(tt = mktime(&t2)), &t2);
  char ttbuf[64];
  TimeToStr(tt,ttbuf);
  std::string tts(ttbuf);
  char dbuf1[256];
  char dbuf2[256];
  strftime(dbuf1, 256, timeformat, &t1);
  strftime(dbuf2, 256, timeformat, &t2);

  const string &filename = entry->second.first;
  char *archivename = new char[filename.size() + 256 * 2 + 4];
  sprintf(archivename, "%s__%s--%s", filename.c_str(), dbuf1, dbuf2);

  leveldb::DB *db = gFile2Db[filename].first.first;
  leveldb::DB *archivedb = NULL;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = dbOpen(options, archivename, &archivedb);

  if (pDebugMode) printf("LEVELDB>> opening db %s --> %p\n", archivename, archivedb);
  if(!status.ok()) return 1;
  leveldb::WriteBatch batchcp;
  leveldb::WriteBatch batchrm;
  leveldb::Iterator *it = db->NewIterator(leveldb::ReadOptions());
  const int blocksize = 10000;
  int counter = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next())
  {
    string ts=it->key().ToString();
    if (ts < tts)
    {
      batchcp.Put(it->key(), it->value());
      batchrm.Delete(it->key());
      if (++counter == blocksize)
      {
        archivedb->Write(leveldb::WriteOptions(), &batchcp);
        batchcp.Clear();
        db->Write(leveldb::WriteOptions(), &batchrm);
        batchrm.Clear();
        counter = 0;
      }
    }
    //else break; // the keys should come in the ascending order so the optimization should be correct, not validated
    // this copy process could be optimized if necessary
  }
  if (counter > 0)
  {
    archivedb->Write(leveldb::WriteOptions(), &batchcp);
    db->Write(leveldb::WriteOptions(), &batchrm);
  }

  delete it;
  if (pDebugMode) printf("LEVELDB>> closing db --> %p\n", archivedb);
  dbClose(archivedb);
  delete[] archivename;

  return 0;
}

int
LvDbDbLogInterface::updateArchiveSchedule (const tTimeToPeriodedFile::iterator &entry)
{
  tm t;
  localtime_r(&entry->first.tv_sec, &t);
  switch (entry->second.second)
  {
    case testly:
    t.tm_sec += 10;
    break;
    case hourly:
    t.tm_hour++;
    break;
    case daily:
    t.tm_mday++;
    break;
    case weekly:
    t.tm_mday += 7;
    break;
    default:
    t.tm_sec += entry->second.second;
    break;
  }
  timespec ts;
  ts.tv_sec = mktime(&t);
  ts.tv_nsec = 0;
  tPeriodedFile pf = entry->second;
  gArchQueue.erase(entry); // remove the entry of the current archiving
  gArchQueue.insert(pair<timespec, tPeriodedFile > (ts, pf));// add a new entry for the next archiving

  return 0;
}

int
LvDbDbLogInterface::setArchivingPeriod (const string &dbname, int volumeduration)
{
  if (volumeduration > 0)
  {
    gArchmutex.Lock();
    if (gArchQueue.empty())
    { // init the archiving thread
      if (pDebugMode) printf("starting the archive thread\n");
      fflush(stdout);
      XrdSysThread::Run(&gArchThread, &archiveThread, NULL, XRDSYSTHREAD_HOLD, NULL);
      gArchThreadStarted = true;
    }
    gArchmutex.UnLock();

    gUniqMutex.Lock();
    if (gFile2Db.find(dbname) == gFile2Db.end())
    {
      gUniqMutex.UnLock();
      return 0; // no LvDbDbLogInterface instances related to this dbname
    }
    else
    {
      gUniqMutex.UnLock();
      timespec ts;
      eos::common::Timing::GetTimeSpec(ts);
      tm tm;
      localtime_r(&ts.tv_sec, &tm);

      switch (volumeduration)
      {
        case testly:
        tm.tm_sec = ((tm.tm_sec / 10) + 1)*10;
        break;
        case hourly:
        tm.tm_hour++;
        tm.tm_sec = tm.tm_min = 0;
        break;
        case daily:
        tm.tm_mday++;
        tm.tm_sec = tm.tm_min = tm.tm_hour;
        break;
        case weekly:
        tm.tm_mday += (7 - tm.tm_wday);
        tm.tm_sec = tm.tm_min = tm.tm_hour;
        break;
        default:
        tm.tm_sec += volumeduration;
        break;
      }
      ts.tv_sec = mktime(&tm);
      ts.tv_nsec = 0;
      gArchmutex.Lock();
      tTimeToPeriodedFile::iterator it;
      for (it = gArchQueue.begin(); it != gArchQueue.end(); it++)
      if (it->second.first.compare(dbname) == 0)
      break;
      if (it != gArchQueue.end())
      { // if an entry already exists delete it
        gArchQueue.erase(it);
      }
      bool awakearchthread = false;
      if (gArchQueue.empty())
      awakearchthread = true;
      else if (ts < gArchQueue.begin()->first)
      awakearchthread = true;
      gArchQueue.insert(pair<timespec, tPeriodedFile > (ts, tPeriodedFile(dbname, volumeduration)));
      gArchmutex.UnLock();
      if (awakearchthread) gArchmutex.Signal();

      return gFile2Db[dbname].second;
    }
  }
  return -1;
}

bool
LvDbDbLogInterface::setDbFile (const string &dbname, int volumeduration, int createperm, void *option)
{
  Option *opt=option?(Option*)option:&gDefaultOption;
  _unused(opt); // to get rid of the unused vairable warning

  // check if the file can be opened, it creates it with the required permissions if it does not exist.
  leveldb::DB *testdb = NULL;
  leveldb::Options options;
  options.max_open_files=2000;

  // try to create the directory if it doesn't exist (don't forget to add the x mode to all users to make the directory browsable)
  mkdir(dbname.c_str(), createperm ? createperm | 0111 : 0644 | 0111);

  gUniqMutex.Lock();
  gArchmutex.Lock();

  if (!dbname.empty())
  {
    if (gFile2Db.find(dbname) == gFile2Db.end())
    { // we do the check only if the file to attach is not yet attached
      options.create_if_missing = true;
      options.error_if_exists = false;
      leveldb::Status status = dbOpen(options, dbname.c_str(), &testdb);
      if (!status.ok())
      {
        gArchmutex.UnLock();
        gUniqMutex.UnLock();
        return false;
      }
    }
  }

  if (!this->pDbName.empty())
  { // detach the current sqname
    tCountedDbAndFilter &csqn = gFile2Db[this->pDbName];
    if (csqn.second > 1) csqn.second--;// if there is other instances pointing to the that db DON'T detach
    else
    { // if there is no other instances pointing to the that db DO detach
      for (tTimeToPeriodedFile::iterator it = gArchQueue.begin(); it != gArchQueue.end(); it++)
      {
        if (it->second.first.compare(this->pDbName) == 0)
        { // erase the reference to this db is the archiving queue
          gArchQueue.erase(it);
          break;
        }
      }
      if (pDebugMode) printf("LEVELDB>> closing db --> %p\n", csqn.first.first);
      dbClose(csqn.first.first);
      gFile2Db.erase(this->pDbName);
      this->pDb = NULL;
      this->pDbName = "";
    }
    pIsOpen = false;
  }

  this->pDbName = dbname;

  if (!dbname.empty())
  {
    if (gFile2Db.find(dbname) != gFile2Db.end())
    {
      gFile2Db[this->pDbName].second++; // if there is already others instances pointing to that db DON'T attach
      pDb = gFile2Db[this->pDbName].first.first;
    }
    else
    {
      pDb = testdb;
      gFile2Db[dbname] = tCountedDbAndFilter(DbAndFilter(pDb, (leveldb::FilterPolicy*)options.filter_policy), 1);
    }
    pIsOpen = true;
  }

  gArchmutex.UnLock();
  gUniqMutex.UnLock();

  if (volumeduration > 0) setArchivingPeriod(dbname, volumeduration);

  return true;
}

bool
LvDbDbLogInterface::isOpen () const
{
  return pIsOpen;
}

string
LvDbDbLogInterface::getDbFile () const
{
  return pDbName;
}

size_t
LvDbDbLogInterface::getTail (int nentries, TlogentryVec *retvec) const
{
  size_t count = retvec->size();
  leveldb::Iterator *it = pDb->NewIterator(leveldb::ReadOptions());
  size_t n = 0;
  for (it->SeekToLast(); it->Valid() && (nentries--) > 0; it->Prev())
  {
    Tlogentry entry;
    entry.timestampstr.assign(it->key().data(),it->key().size());

    size_t pos=0;
    Slice slice;
    if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
    else entry.seqid.assign(slice.data(),slice.size());
    if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
    else entry.writer.assign(slice.data(),slice.size());
    if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
    else entry.key.assign(slice.data(),slice.size());
    if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
    else entry.value.assign(slice.data(),slice.size());
    if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
    else entry.comment.assign(slice.data(),slice.size());

    retvec->push_back(entry);
    n++;
  }
  std::reverse(retvec->begin()+count, retvec->end());
  delete it;
  return retvec->size() - count;
}

size_t
LvDbDbLogInterface::getAll (TlogentryVec *retvec, size_t nmax, Tlogentry *startafter) const
{
  size_t count = retvec->size();
  leveldb::Iterator *it = pDb->NewIterator(leveldb::ReadOptions());
  it->SeekToFirst();
  if (startafter)
  {
    if (!startafter->timestampstr.empty())
    {
      // if a starting position is given seek to that position
      string skey;
      skey = startafter->timestampstr;
      it->Seek(skey);
      it->Next();
    }
  }
  if (!nmax) nmax = std::numeric_limits<size_t>::max();
  size_t n = 0;
  for (; it->Valid() && n < nmax; it->Next())
  {
    Tlogentry entry;
    entry.timestampstr.assign(it->key().data(),it->key().size());

    size_t pos=0;
    Slice slice;
    if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
    else entry.seqid.assign(slice.data(),slice.size());
    if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
    else entry.writer.assign(slice.data(),slice.size());
    if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
    else entry.key.assign(slice.data(),slice.size());
    if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
    else entry.value.assign(slice.data(),slice.size());
    if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
    else entry.comment.assign(slice.data(),slice.size());

    retvec->push_back(entry);
    n++;
  }
  if (startafter)
  {
    if (retvec->empty()) (*startafter) = Tlogentry();
    else (*startafter) = (*retvec)[retvec->size() - 1];
  }
  delete it;
  return retvec->size() - count;
}

bool LvDbDbLogInterface::clear()
{
  leveldb::Status s;
  leveldb::WriteBatch batch;
  leveldb::Iterator* it = pDb->NewIterator(leveldb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next())
  {
    batch.Delete(it->key());
  }
  s = pDb->Write(leveldb::WriteOptions(), &batch);
  delete it;

  return s.ok();
}

LvDbDbMapInterface::LvDbDbMapInterface () : pNDbEntries(0) , pBatched (false)
{
}

LvDbDbMapInterface::~LvDbDbMapInterface ()
{
  for (map<string, tOwnedLDLIptr>::iterator it = pAttachedDbs.begin(); it != pAttachedDbs.end(); it = pAttachedDbs.begin())
  { // strange loop because DetachDbLog erase entries from the map
    if (it->second.second)
    detachDbLog(it->first);
    else
    detachDbLog(static_cast<DbLogInterface*> (it->second.first));
  }
  detachDb();
}

void
LvDbDbMapInterface::setName (const string &name)
{
  this->pName = name;
}

const string &
LvDbDbMapInterface::getName () const
{
  return pName;
}

bool
LvDbDbMapInterface::beginTransaction ()
{
  pBatched = true;
  return true;
}

bool
LvDbDbMapInterface::endTransaction ()
{
  class SizeHandler : public leveldb::WriteBatch::Handler
  {
    LvDbDbMapInterface *dmi;
    int diff;
    std::set<std::string> addedKeys;
    std::set<std::string> removedKeys;
  public:
    SizeHandler(LvDbDbMapInterface *_dmi) : dmi(_dmi), diff(0)
    {}
    virtual ~SizeHandler()
    {}
    virtual void Put(const leveldb::Slice& key, const leveldb::Slice& value)
    {
      std::string skey=key.ToString();
      if( !dmi->count(key))
      {
        if(addedKeys.find(skey)==addedKeys.end())
        { //not in the initial keys m not in the added keys yet
          addedKeys.insert(skey);
          diff++;
        }
        else
        { // already added
          ;// do nothing
        }
      }
      else
      { // in the initial keys
        if(removedKeys.find(skey)==removedKeys.end())
        { //not in the removed keys
          ;// already there
        }
        else
        { // in the removed keys
          removedKeys.erase(skey);
          diff++;
        }
      }
    }
    virtual void Delete(const leveldb::Slice& key)
    {
      std::string skey=key.ToString();
      if( !dmi->count(key))
      {
        if(addedKeys.find(skey)!=addedKeys.end())
        { //not in the initial keys but in the added keys
          addedKeys.erase(skey);
          diff--;
        }
        else
        { //already not there
          ;// do nothing
        }
      }
      else
      { // in the initial keys
        if(removedKeys.find(skey)!=removedKeys.end())
        { //not in the removed keys
          ;// already removed
        }
        else
        { // in the removed keys
          removedKeys.insert(skey);
          diff--;
        }
      }
    }
    int GetDiff()
    { return diff;}
    void Reset()
    { diff=0; addedKeys.clear(); removedKeys.clear();}
  };

  if (pBatched)
  {
    leveldb::Status status;
    if(!pAttachedDbname.empty())
    {
      SizeHandler handler(this);
      pDbBatch.Iterate(&handler);
      pNDbEntries+=handler.GetDiff();
      status = AttachedDb->Write(leveldb::WriteOptions(), &pDbBatch);
      TestLvDbError(status, this);
      pDbBatch.Clear();
    }
    for (map<string, tOwnedLDLIptr>::iterator it = pAttachedDbs.begin(); it != pAttachedDbs.end(); it++)
    { // strange loop because DetachDbLog erase entries from the map
      status = it->second.first->pDb->Write(leveldb::WriteOptions(), &pExportBatch);
      TestLvDbError(status, this);
    }
    pExportBatch.Clear();
    pBatched = false;
  }
  return true;
}

bool
LvDbDbMapInterface::getEntry(const Slice &key, Tval *val)
{
  string sval;
  leveldb::Status s;
  if(!pAttachedDbname.empty())
  s=AttachedDb->Get(leveldb::ReadOptions(),key,&sval);
  else return false;
  if(s.IsNotFound()) return false;

  Tlogentry entry;
  entry.key.assign(key.data(),key.size());

  size_t pos=0;
  Slice slice;
  if(!ExtractSliceFromSlice(sval,&pos,&slice)) return false;
  else entry.value.assign(slice.data(),slice.size());
  if(!ExtractSliceFromSlice(sval,&pos,&slice)) return false;
  else entry.comment.assign(slice.data(),slice.size());
  if(!ExtractSliceFromSlice(sval,&pos,&slice)) return false;
  else entry.seqid.assign(slice.data(),slice.size());
  if(!ExtractSliceFromSlice(sval,&pos,&slice)) return false;
  else entry.timestampstr.assign(slice.data(),slice.size());
  if(!ExtractSliceFromSlice(sval,&pos,&slice)) return false;
  else entry.writer.assign(slice.data(),slice.size());

  Tlogentry2Tval(entry,val);

  return true;
}

bool
LvDbDbMapInterface::setEntry (const Slice &key, const TvalSlice &val)
{
  bool todb=val.seqid!=0 && !pAttachedDbname.empty();
  bool tolog=!pAttachedDbs.empty();

  if(todb || tolog)
  {
    const string tab("\t");
    char sseqid[24];
    modp_ulitoa10(val.seqid, sseqid);
    Slice seqid(sseqid,strlen(sseqid));

    if(tolog)
    {
      string sval_exp;
      sval_exp.reserve(1024);

      ((((sval_exp += seqid) += val.writer.empty() ? Slice(this->pName) : val.writer) += key) += val.value) += val.comment;

      if (pBatched)
      {
        pExportBatch.Put(leveldb::Slice(val.timestampstr.data(),val.timestampstr.size()), sval_exp);
      }
      else
      {
        for (map<string, tOwnedLDLIptr>::iterator it = pAttachedDbs.begin(); it != pAttachedDbs.end(); it++)
        { // strange loop because DetachDbLog erase entries from the map
          leveldb::Status status = it->second.first->pDb->Put(leveldb::WriteOptions(), leveldb::Slice(val.timestampstr.data(),val.timestampstr.size()), sval_exp);
          TestLvDbError(status, this);
        }
      }
    }

    if(todb)
    { // we don't want to commit to the Db if it's a deletion notification
      string sval;
      sval.reserve(1024);

      ((((sval += val.value) += val.comment) += seqid) += val.timestampstr) += val.writer.empty() ? Slice(this->pName) : val.writer;

      if(pBatched)
      {
        pDbBatch.Put(key, sval);
      }
      else
      {
        pNDbEntries+=(1-count(key)); // the key does NOT exist, a new entry will then be created
        leveldb::Status status = AttachedDb->Put(leveldb::WriteOptions(), key, sval);
        TestLvDbError(status, this);
      }
    }
  }
  return true;
}

bool
LvDbDbMapInterface::removeEntry (const Slice &key, const TvalSlice &val)
{
  if (pBatched)
  {
    setEntry(key,val); // update the dblog
    pDbBatch.Delete(key);// update the db
  }
  else
  {
    setEntry(key,val); // update the dblog
    if(!pAttachedDbname.empty())
    { // update the db
      pNDbEntries-=count(key);// the key DOES exist, a new entry will then be deleted
      leveldb::Status status = AttachedDb->Delete(leveldb::WriteOptions(), key);
      TestLvDbError(status, this);
    }
  }
  return true;
}

bool LvDbDbMapInterface::clear()
{
  if(!pAttachedDbname.empty())
  {
    leveldb::Status s;
    leveldb::WriteBatch batch;
    leveldb::Iterator* it = AttachedDb->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
      batch.Delete(it->key());
    }
    s = AttachedDb->Write(leveldb::WriteOptions(), &batch);
    TestLvDbError(s, this);
    delete it;
    pNDbEntries=0;
    return s.ok();
  }
  else
  return true;
}

size_t LvDbDbMapInterface::size() const
{
  size_t retval=0;
  if(!pAttachedDbname.empty())
  retval=pNDbEntries;
  return retval;
}

size_t LvDbDbMapInterface::count(const Slice& key) const
{
  size_t retval=0;
  if(!pAttachedDbname.empty())
  {
    leveldb::Iterator *it=AttachedDb->NewIterator(leveldb::ReadOptions());
    it->Seek(key);
    if(it->Valid()) retval=1; // the key does NOT exist, a new entry will then be created
    delete it;
  }
  return retval;
}

void LvDbDbMapInterface::rebuildSize()
{
  pNDbEntries=0;
  leveldb::Iterator* it = AttachedDb->NewIterator(leveldb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) pNDbEntries++;
  delete it;
}

bool LvDbDbMapInterface::attachDb(const std::string &dbname, bool repair, int createperm, void* option)
{
  Option *opt=option?(Option*)option:&gDefaultOption;
  if(pAttachedDbname.empty())
  {
    // try to create the directory if it doesn't exist (don't forget to add the x mode to all users to make the directory browsable)
    mkdir(dbname.c_str(), createperm ? createperm | 0111 : 0644 | 0111);
    pOptions.create_if_missing = true;
    pOptions.error_if_exists = false;
    leveldb::Status status = dbOpen(pOptions, dbname, &AttachedDb,opt->CacheSizeMb,opt->BloomFilterNbits);
    if(repair && !status.ok())
    {
      leveldb::RepairDB(dbname.c_str(),leveldb::Options());
      status = dbOpen(pOptions, dbname, &AttachedDb,opt->CacheSizeMb,opt->BloomFilterNbits);
    }
    TestLvDbError(status, this);
    if(status.ok())
    {
      pAttachedDbname=dbname;
      rebuildSize();
    }
    return status.ok();
  }
  else
  {
    return false;
  }
}

bool LvDbDbMapInterface::trimDb()
{
  if(!pAttachedDbname.empty())
  {
    AttachedDb->CompactRange(NULL,NULL);
    return true;
  }
  else
  return false;
}

std::string LvDbDbMapInterface::getAttachedDbName() const
{
  return pAttachedDbname;
}

#ifdef EOS_STDMAP_DBMAP
bool LvDbDbMapInterface::syncFromDb(std::map<Tkey,Tval> *map)
#else
bool LvDbDbMapInterface::syncFromDb(::google::dense_hash_map<Tkey,Tval> *map)
#endif
{

  if(!pAttachedDbname.empty())
  {
    leveldb::Iterator* it = AttachedDb->NewIterator(leveldb::ReadOptions());
    for(it->SeekToFirst(); it->Valid(); it->Next())
    {
      Tkey key;
      key.assign(it->key().data(),it->key().size());
      Tval val;
      Tlogentry entry;

      size_t pos=0;
      Slice slice;
      if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
      else entry.value.assign(slice.data(),slice.size());
      if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
      else entry.comment.assign(slice.data(),slice.size());
      if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
      else entry.seqid.assign(slice.data(),slice.size());
      if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
      else entry.timestampstr.assign(slice.data(),slice.size());
      if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
      else entry.writer.assign(slice.data(),slice.size());

      Tlogentry2Tval(entry,&val);
      (*map)[key]=val;
    }
    delete it;
    return true;
  }
  return false;
}

bool LvDbDbMapInterface::detachDb()
{
  if(!pAttachedDbname.empty())
  {
    endTransaction();
    pAttachedDbname.clear();
    dbClose(AttachedDb);
    return true;
  }
  return false;
}

size_t LvDbDbMapInterface::getAll( TlogentryVec *retvec, size_t nmax=0, Tlogentry *startafter=NULL ) const
{
  if(!pAttachedDbname.empty())
  {
    size_t count = retvec->size();
    leveldb::Iterator *it = AttachedDb->NewIterator(leveldb::ReadOptions());
    it->SeekToFirst();
    if (startafter)
    {
      // if a starting position is given seek to that position
      string skey;
      skey = startafter->key;
      it->Seek(skey);
      it->Next();
    }
    if (!nmax) nmax = std::numeric_limits<size_t>::max();
    size_t n = 0;
    for (; it->Valid() && n < nmax; it->Next())
    {
      Tlogentry entry;
      entry.key.assign(it->key().data(),it->key().size());

      size_t pos=0;
      Slice slice;
      if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
      else entry.value.assign(slice.data(),slice.size());
      if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
      else entry.comment.assign(slice.data(),slice.size());
      if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
      else entry.seqid.assign(slice.data(),slice.size());
      if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
      else entry.timestampstr.assign(slice.data(),slice.size());
      if(!ExtractSliceFromSlice(it->value(),&pos,&slice)) return false;
      else entry.writer.assign(slice.data(),slice.size());

      retvec->push_back(entry);
      n++;
    }
    if (startafter)
    {
      if (retvec->empty()) (*startafter) = Tlogentry();
      else (*startafter) = (*retvec)[retvec->size() - 1];
    }
    delete it;
    return retvec->size() - count;
  }
  return 0;
}

bool
LvDbDbMapInterface::attachDbLog (DbLogInterface *dblogint)
{
  string sname = dblogint->getDbFile();
  if (pAttachedDbs.find(sname) != pAttachedDbs.end())
  {
    pAttachedDbs[sname] = tOwnedLDLIptr(static_cast<LvDbDbLogInterface*> (dblogint), false);
    return true;
  }
  return false;
}

bool
LvDbDbMapInterface::detachDbLog (DbLogInterface *dblogint)
{
  string sname = dblogint->getDbFile();
  if (pAttachedDbs.find(sname) != pAttachedDbs.end())
  {
    // the ownership should be false
    pAttachedDbs.erase(sname);
    return true;
  }
  return false;
}

bool
LvDbDbMapInterface::attachDbLog (const string &dbname, int volumeduration, int createperm, void *option)
{
  if (pAttachedDbs.find(dbname) == pAttachedDbs.end())
  {
    pAttachedDbs[dbname] = tOwnedLDLIptr(new LvDbDbLogInterface(dbname, volumeduration, createperm, option), true);
    return true;
  }
  return false;
}

bool
LvDbDbMapInterface::detachDbLog (const string &dbname)
{
  if (pAttachedDbs.find(dbname) != pAttachedDbs.end())
  {
    dbClose((leveldb::DB*)pAttachedDbs[dbname].first);// the ownership should be true
    pAttachedDbs.erase(dbname);
    return true;
  }
  return false;
}

EOSCOMMONNAMESPACE_END

#endif
