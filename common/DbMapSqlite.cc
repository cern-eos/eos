// ----------------------------------------------------------------------
// File: DbMapSqlite.cc
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
#include "common/DbMapSqlite.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <fcntl.h>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

unsigned SqliteInterfaceBase::gNInstances = 0;
unsigned SqliteDbLogInterface::gNInstances = 0;

RWMutex SqliteInterfaceBase::gTransactionMutex;
RWMutex SqliteInterfaceBase::gBaseMutex;
sqlite3 *SqliteInterfaceBase::gDb = NULL;
__thread bool SqliteInterfaceBase::gCurrentThreadTransact=false;

SqliteInterfaceBase::Option SqliteInterfaceBase::gDefaultOption =
{ 0};
bool SqliteInterfaceBase::gDebugMode = false;
bool SqliteInterfaceBase::gAbortOnsSliteError = true;
pthread_t SqliteDbLogInterface::gArchThread;
bool SqliteDbLogInterface::gArchThreadStarted = false;
XrdSysMutex SqliteDbLogInterface::gUniqMutex;
XrdSysCondVar SqliteDbLogInterface::gArchMutex (0);
SqliteDbLogInterface::tTimeToPeriodedFile SqliteDbLogInterface::gArchQueue;
SqliteDbLogInterface::tMapFileToSqname SqliteDbLogInterface::gFile2SqName;
set<int> SqliteDbLogInterface::gIdPool;

int
SqliteInterfaceBase::execNoCallback (const char* str, int retrydelay, int nretry)
{
  if (gDebugMode) return exec(str);
  pRc = sqlite3_exec(gDb, str, NULL, NULL, &pErrStr);
  if(retrydelay>0)
  for(int retr=0; (pRc!=SQLITE_OK) && (pRc!=SQLITE_DONE) && (pRc!=SQLITE_ROW) && retr<nretry; retr++)
  {
    usleep(retrydelay);
    pRc = sqlite3_exec(gDb, str, NULL, NULL, &pErrStr);
  }
  TestSqliteError(str, &pRc, &pErrStr, this);
  if (pErrStr != NULL)
  {
    sqlite3_free(pErrStr);
    pErrStr = NULL;
  }
  return pRc;
}

int
SqliteInterfaceBase::execNoCallback2 (const char* str, int retrydelay, int nretry)
{
  char *errstr;
  int rc;
  if (gDebugMode) return exec2(str);
  rc = sqlite3_exec(gDb, str, NULL, NULL, &errstr);
  if(retrydelay>0)
  for(int retr=0; (rc!=SQLITE_OK) && (rc!=SQLITE_DONE) && (rc!=SQLITE_ROW) && retr<nretry; retr++)
  {
    usleep(retrydelay);
    rc = sqlite3_exec(gDb, str, NULL, NULL, &errstr);
  }
  TestSqliteError(str, &rc, &errstr, NULL);
  if (errstr != NULL)
  {
    sqlite3_free(errstr);
    errstr = NULL;
  }
  return rc;
}

int
SqliteInterfaceBase::exec (const char* str, int retrydelay, int nretry)
{
  printf("SQLITE3>> %p executing %s\n", this, str);
  fflush(stdout);
  pRc = sqlite3_exec(gDb, str, printCallback, NULL, &pErrStr);
  if(retrydelay>0)
  for(int retr=0; (pRc!=SQLITE_OK) && (pRc!=SQLITE_DONE) && (pRc!=SQLITE_ROW) && retr<nretry; retr++)
  {
    usleep(retrydelay);
    pRc = sqlite3_exec(gDb, str, NULL, NULL, &pErrStr);
  }
  printf("SQLITE3>> %p\terror code is %d\n", this, pRc);
  printf("SQLITE3>> %p\terror message is %s\n", this, pErrStr);
  TestSqliteError(str, &pRc, &pErrStr, this);
  if (pErrStr != NULL)
  {
    sqlite3_free(pErrStr);
    pErrStr = NULL;
  }
  return pRc;
}

int
SqliteInterfaceBase::exec2 (const char* str, int retrydelay, int nretry)
{
  char *errstr;
  int rc;
  printf("SQLITE3>> background thread executing %s\n", str);
  fflush(stdout);
  rc = sqlite3_exec(gDb, str, printCallback, NULL, &errstr);
  if(retrydelay>0)
  for(int retr=0; (rc!=SQLITE_OK) && (rc!=SQLITE_DONE) && (rc!=SQLITE_ROW) && retr<nretry; retr++)
  {
    usleep(retrydelay);
    rc = sqlite3_exec(gDb, str, NULL, NULL, &errstr);
  }
  printf("SQLITE3>> background thread\terror code is %d\n", rc);
  printf("SQLITE3>> background thread\terror message is %s\n", errstr);
  TestSqliteError(str, &rc, &errstr, NULL);
  if (errstr != NULL)
  {
    sqlite3_free(errstr);
    errstr = NULL;
  }
  return rc;
}

int
SqliteInterfaceBase::logWrapperCallback (void*isnull, int ncols, char**values, char**keys)
{
  if (ncols != 6) return -1; // given the query there should be 7 columns
  DbMapTypes::TlogentryVec *outputv = this->pRetVecPtr;
  DbMapTypes::Tlogentry newchunk;
  int index = 0;
  newchunk.timestampstr = values[index++];
  newchunk.seqid = values[index++];
  newchunk.writer = values[index++];
  newchunk.key = values[index++];
  newchunk.value = values[index++];
  newchunk.comment = values[index++];
  outputv->push_back(newchunk);
  return 0;
}

int
SqliteInterfaceBase::mapWrapperCallback (void*isnull, int ncols, char**values, char**keys)
{
  if (ncols != 6) return -1; // given the query there should be 7 columns
  DbMapTypes::TlogentryVec *outputv = this->pRetVecPtr;
  DbMapTypes::Tlogentry newchunk;
  int index = 0;
  newchunk.key = values[index++];
  newchunk.value = values[index++];
  newchunk.comment = values[index++];
  newchunk.timestampstr = values[index++];
  newchunk.seqid = values[index++];
  newchunk.writer = values[index++];
  outputv->push_back(newchunk);
  return 0;
}

SqliteDbMapInterface::SqliteDbMapInterface ():
pGetStmt(NULL), pSetStmt(NULL), pRemoveStmt(NULL), pSizeStmt(NULL), pCountStmt(NULL)
{
}

SqliteDbMapInterface::~SqliteDbMapInterface ()
{
  for (map<string, tOwnedSDLIptr>::iterator it = pAttachedDbLogs.begin(); it != pAttachedDbLogs.end(); it = pAttachedDbLogs.begin())
  { // strange loop because DetachDbLog erase entries from the map
    if (it->second.second)
    detachDbLog(it->first);
    else
    detachDbLog(static_cast<DbLogInterface*> (it->second.first));
  }
  detachDb();
  if (pGetStmt != NULL) sqlite3_finalize(pGetStmt);
  if (pSetStmt != NULL) sqlite3_finalize(pSetStmt);
  if (pRemoveStmt != NULL) sqlite3_finalize(pRemoveStmt);
  if (pSizeStmt != NULL) sqlite3_finalize(pSizeStmt);
  if (pCountStmt != NULL) sqlite3_finalize(pCountStmt);
}

void
SqliteDbMapInterface::setName (const string &name)
{
  this->pName = name;
  prepareExportStatement(); // to refresh the writer value in the export statement
}

const string &
SqliteDbMapInterface::getName () const
{
  return pName;
}

int
SqliteDbMapInterface::prepareStatements ()
{
  const char *dummy;
  if(!pAttachedDbName.empty())
  {
    if (pGetStmt != NULL) sqlite3_finalize(pGetStmt);
    sprintf(pStmt, "SELECT * FROM DbMap%p.dbmap WHERE key=?;", this);
    pRc = sqlite3_prepare_v2(gDb, pStmt, -1, &pGetStmt, &dummy);
    TestSqliteError(pStmt, &pRc, &pErrStr, this);

    if (pSetStmt != NULL) sqlite3_finalize(pSetStmt);
    sprintf(pStmt, "INSERT OR REPLACE INTO DbMap%p.dbmap VALUES(?,?,?,?,?,?);", this);
    pRc = sqlite3_prepare_v2(gDb, pStmt, -1, &pSetStmt, &dummy);
    TestSqliteError(pStmt, &pRc, &pErrStr, this);

    if (pRemoveStmt != NULL) sqlite3_finalize(pRemoveStmt);
    sprintf(pStmt, "DELETE FROM DbMap%p.dbmap WHERE key=?;", this);
    pRc = sqlite3_prepare_v2(gDb, pStmt, -1, &pRemoveStmt, &dummy);
    TestSqliteError(pStmt, &pRc, &pErrStr, this);

    if (pSizeStmt != NULL) sqlite3_finalize(pSizeStmt);
    sprintf(pStmt, "SELECT Count(*) FROM DbMap%p.dbmap;", this);
    pRc = sqlite3_prepare_v2(gDb, pStmt, -1, &pSizeStmt, &dummy);
    TestSqliteError(pStmt, &pRc, &pErrStr, this);

    if (pCountStmt != NULL) sqlite3_finalize(pCountStmt);
    sprintf(pStmt, "SELECT EXISTS(SELECT 1 FROM DbMap%p.dbmap WHERE key=?);", this);
    pRc = sqlite3_prepare_v2(gDb, pStmt, -1, &pCountStmt, &dummy);
    TestSqliteError(pStmt, &pRc, &pErrStr, this);
  }
  return SQLITE_OK;
}

int
SqliteDbMapInterface::prepareExportStatement ()
{
  const char *dummy;
  char buffer[1024];
  for (vector<sqlite3_stmt*>::iterator it = pExportStmts.begin(); it != pExportStmts.end(); it++) sqlite3_finalize((*it));
  pExportStmts.resize(pAttachedDbLogs.size());
  int count = 0;
  for (map<string, tOwnedSDLIptr>::iterator it = pAttachedDbLogs.begin(); it != pAttachedDbLogs.end(); it++)
  {
    sprintf(buffer, "INSERT INTO %s.ondisk VALUES(?,?,?,?,?,?);",
        it->second.first->GetSqName().c_str());
    if (gDebugMode) printf("SQLITE3>> %p Preparing export statement : %s\n", this, buffer);
    pRc = sqlite3_prepare_v2(gDb, buffer, -1, &pExportStmts[count++], &dummy);
    TestSqliteError(buffer, &pRc, &pErrStr, this);
  }
  return true;
}

bool
SqliteDbMapInterface::beginTransaction ()
{
  if(!gCurrentThreadTransact)
  {
    gTransactionMutex.LockWrite();
    gCurrentThreadTransact=true;
    return execNoCallback("BEGIN TRANSACTION;") == SQLITE_OK;
  }
  return true; // already into a transaction
}

bool
SqliteDbMapInterface::endTransaction ()
{
  bool rc=true; // default is already off transaction
  if(gCurrentThreadTransact)
  {
    rc = (execNoCallback("END TRANSACTION;") == SQLITE_OK);
    gCurrentThreadTransact=false;
    gTransactionMutex.UnLockWrite();
  }
  return rc;
}

bool
SqliteDbMapInterface::getEntry(const Slice &key, Tval *val)
{
#define sqlitecheck  TestSqliteError("Compiled Statement",&pRc,&pErrStr,this);
  if(!pAttachedDbName.empty())
  {
    pRc = sqlite3_bind_blob(pGetStmt, 1, key.data(), key.size(), NULL);
    sqlitecheck;

    pRc = sqlite3_step(pGetStmt);

    sqlitecheck;
    bool ret=false;
    if(pRc == SQLITE_ROW)
    {
      val->value = std::string((char*)sqlite3_column_blob(pGetStmt, 1),(size_t)sqlite3_column_bytes(pGetStmt,1));
      val->comment = (char*)sqlite3_column_text(pGetStmt, 2);
      val->timestampstr = (char*)sqlite3_column_text(pGetStmt, 3);
      val->seqid = sqlite3_column_int(pGetStmt, 4);
      val->writer = (char*)sqlite3_column_text(pGetStmt, 5);
      ret=true;
    }
    sqlite3_reset(pGetStmt);
    sqlitecheck;
    sqlite3_clear_bindings(pGetStmt);
    sqlitecheck;

    return ret;
  }
  return false;
}

bool
SqliteDbMapInterface::setEntry (const Slice &key, const TvalSlice &val)
{
#define sqlitecheck  TestSqliteError("Compiled Statement",&pRc,&pErrStr,this);
  const string &writer = val.writer.empty() ? this->pName : val.writer.ToString();
  if(val.seqid!=0 && !pAttachedDbName.empty())
  { // if it's a deletion notification we don't write anything in the db
    //rc = sqlite3_bind_text(set_stmt, 1, key.data(), key.size(), NULL);
    pRc = sqlite3_bind_blob(pSetStmt, 1, key.data(), key.size(), NULL);
    sqlitecheck;
    pRc = sqlite3_bind_blob(pSetStmt, 2, val.value.data(), val.value.size(), NULL);
    sqlitecheck;
    pRc = sqlite3_bind_text(pSetStmt, 3, val.comment.data(), val.comment.size(), NULL);
    sqlitecheck;
    pRc = sqlite3_bind_text(pSetStmt, 4, val.timestampstr.data(), val.timestampstr.size(), NULL);
    sqlitecheck;
    pRc = sqlite3_bind_int(pSetStmt, 5, val.seqid);
    sqlitecheck;
    pRc = sqlite3_bind_text(pSetStmt, 6, val.writer.data(), val.writer.size(), NULL);
    sqlitecheck;
    pRc = sqlite3_step(pSetStmt);
    sqlitecheck;
    pRc = sqlite3_reset(pSetStmt);
    sqlitecheck;
    pRc = sqlite3_clear_bindings(pSetStmt);
    sqlitecheck;
  }
  if (!pAttachedDbLogs.empty())
  {
    for (int i = 0; i < (int) pAttachedDbLogs.size(); i++)
    {
      pRc = sqlite3_bind_text(pExportStmts[i], 1, val.timestampstr.data(), val.timestampstr.size(), NULL);
      sqlitecheck;
      pRc = sqlite3_bind_int(pExportStmts[i], 2, val.seqid);
      sqlitecheck;
      pRc = sqlite3_bind_text(pExportStmts[i], 3, writer.c_str(), -1, NULL);
      sqlitecheck;
      pRc = sqlite3_bind_blob(pExportStmts[i], 4, key.data(), key.size(), NULL);
      sqlitecheck;
      pRc = sqlite3_bind_blob(pExportStmts[i], 5, val.value.data(), val.value.size(), NULL);
      sqlitecheck;
      pRc = sqlite3_bind_text(pExportStmts[i], 6, val.comment.data(), val.comment.size(), NULL);
      sqlitecheck;
      pRc = sqlite3_step(pExportStmts[i]);
      sqlitecheck;
      pRc = sqlite3_reset(pExportStmts[i]);
      sqlitecheck;
      pRc = sqlite3_clear_bindings(pExportStmts[i]);
      sqlitecheck;
    }
  }
#undef sqlitecheck
  return true;
}

bool
SqliteDbMapInterface::removeEntry (const Slice &key, const TvalSlice &val)
{
#define sqlitecheck  TestSqliteError("Compiled Statement",&pRc,&pErrStr,this);
  setEntry(key,val); // update the logdb
  if(!pAttachedDbName.empty())
  {
    pRc = sqlite3_bind_blob(pRemoveStmt, 1, key.data(), key.size(), NULL);
    sqlitecheck;
    pRc = sqlite3_step(pRemoveStmt);// update the db
    sqlitecheck;
    pRc = sqlite3_reset(pRemoveStmt);
    sqlitecheck;
    pRc = sqlite3_clear_bindings(pRemoveStmt);
    sqlitecheck;
  }
#undef sqlitecheck
  return true;
}

bool
SqliteDbMapInterface::clear()
{
  if(!pAttachedDbName.empty())
  {
    sprintf(pStmt, "DELETE FROM DbMap%p.dbmap;", this);
    pRc = execNoCallback(pStmt);
    return pRc==0;
  }

  return true;
}

size_t SqliteDbMapInterface::size() const
{
#define sqlitecheck  TestSqliteError("Compiled Statement",&pRc,&pErrStr,this);
  size_t retval=0;
  if(!pAttachedDbName.empty())
  {
    pRc = sqlite3_step(pSizeStmt);
    sqlitecheck;
    if(pRc == SQLITE_ROW)
    retval = sqlite3_column_int(pSizeStmt, 0);
    pRc = sqlite3_reset(pSizeStmt);
    sqlitecheck;
    pRc = sqlite3_clear_bindings(pSizeStmt);
    sqlitecheck;
  }
#undef sqlitecheck

  return retval;
}

size_t SqliteDbMapInterface::count(const Slice& key) const
{
#define sqlitecheck  TestSqliteError("Compiled Statement",&pRc,&pErrStr,this);
  size_t retval=0;
  if(!pAttachedDbName.empty())
  {
    pRc = sqlite3_bind_blob(pCountStmt, 1, key.data(), key.size(), NULL);
    sqlitecheck;
    pRc = sqlite3_step(pCountStmt);
    sqlitecheck;
    if(pRc == SQLITE_ROW)
    retval = sqlite3_column_int(pCountStmt, 0);
    pRc = sqlite3_reset(pCountStmt);
    sqlitecheck;
    pRc = sqlite3_clear_bindings(pCountStmt);
    sqlitecheck;
  }
#undef sqlitecheck

  return retval;
}

bool
SqliteDbMapInterface::attachDbLog (DbLogInterface *dblogint)
{
  string sname = dblogint->getDbFile();
  if (pAttachedDbLogs.find(sname) != pAttachedDbLogs.end())
  {
    pAttachedDbLogs[sname] = tOwnedSDLIptr(static_cast<SqliteDbLogInterface*> (dblogint), false);
    bool rc= prepareExportStatement();
    return rc;
  }
  return false;
}

bool
SqliteDbMapInterface::detachDbLog (DbLogInterface *dblogint)
{
  string sname = dblogint->getDbFile();
  if (pAttachedDbLogs.find(sname) != pAttachedDbLogs.end())
  {
    // the ownership should be false
    pAttachedDbLogs.erase(sname);
    bool rc=prepareExportStatement();
    return rc;
  }
  return false;
}

bool
SqliteDbMapInterface::attachDb(const std::string &dbname, bool repair, int createperm, void* option)
{
  bool halttransaction=gCurrentThreadTransact;
  if(halttransaction) endTransaction();
  gTransactionMutex.LockWrite();
  Option *opt=option?(Option*)option:&gDefaultOption;
  _unused(opt); // to get rid of the unused vairable warning

  if(pAttachedDbName.empty())
  {
    pAttachedDbName=dbname;
    sprintf(pStmt, "ATTACH \'%s\' AS DbMap%p;", pAttachedDbName.c_str(), this); // attached dbs for the logs are
    if(repair)
    { // run 'sqlite3' to commit a pending journal
      std::string sqlite3cmd = "test -r ";
      sqlite3cmd += pAttachedDbName.c_str();
      sqlite3cmd += " && sqlite3 ";
      sqlite3cmd += pAttachedDbName.c_str();
      sqlite3cmd += " \"select count(*) from fst where 1;\"";
      system(sqlite3cmd.c_str());
    }
    execNoCallback(pStmt);
    sprintf(pStmt, "CREATE TABLE IF NOT EXISTS DbMap%p.dbmap (key BLOB, value BLOB,comment TEXT, timestampstr TEXT, seqid INTEGER, writer TEXT, PRIMARY KEY(key) );", this);
    execNoCallback(pStmt,100000);
    bool rc= prepareStatements()==SQLITE_OK;
    gTransactionMutex.UnLockWrite();
    if(halttransaction) beginTransaction();
    return rc;
  }
  gTransactionMutex.UnLockWrite();
  if(halttransaction) beginTransaction();
  return false;
}

bool SqliteDbMapInterface::trimDb()
{
  if(!pAttachedDbName.empty())
  {
    sqlite3 *trimdb;
    int rc=sqlite3_open_v2(pAttachedDbName.c_str(),&trimdb,SQLITE_OPEN_READWRITE | SQLITE_OPEN_WAL,NULL);
    if(rc!=SQLITE_OK) return false;
    sprintf(pStmt, "VACUUM;"); // attached dbs for the logs are
    rc = sqlite3_exec(gDb, pStmt, NULL, NULL, &pErrStr);
    TestSqliteError(pStmt, &rc, &pErrStr, this);
    if (pErrStr != NULL)
    {
      sqlite3_free(pErrStr);
      pErrStr = NULL;
      return false;
    }
    sqlite3_close(trimdb);
    return true;
  }
  else
  return false;
}

std::string SqliteDbMapInterface::getAttachedDbName() const
{
  return pAttachedDbName;
}

bool
SqliteDbMapInterface::detachDb ()
{
  bool halttransaction=gCurrentThreadTransact;
  if(halttransaction) endTransaction();
  gTransactionMutex.LockWrite();

  if(!pAttachedDbName.empty())
  {
    sprintf(pStmt, "DETACH DbMap%p;", this); // attached dbs for the logs are
    execNoCallback(pStmt,100000);
    pAttachedDbName.clear();
    bool rc= prepareStatements()==SQLITE_OK;
    gTransactionMutex.UnLockWrite();
    if(halttransaction) beginTransaction();
    return rc;
  }
  gTransactionMutex.UnLockWrite();
  if(halttransaction) beginTransaction();
  return false;
}

size_t SqliteDbMapInterface::getAll( TlogentryVec *retvec, size_t nmax=0, Tlogentry *startafter=NULL ) const
{
  if(!pAttachedDbName.empty())
  {
    size_t count = retvec->size();
    char limit[1024], id[1024];
    limit[0] = id[0] = 0;
    if (nmax) sprintf(limit, " LIMIT %lu", nmax);
    if (startafter) sprintf(id, " WHERE KEY>(SELECT KEY FROM DbMap%p.dbmap WHERE KEY=\"%s\")", this, startafter->timestampstr.c_str());
    sprintf(pStmt, "SELECT * FROM DbMap%p.dbmap%s ORDER BY KEY%s;", this, id, limit);
    this->pRetVecPtr = retvec;
    if (gDebugMode) printf("SQLITE3>> %p executing %s\n", this, pStmt);
    pRc = sqlite3_exec(gDb, pStmt, staticMapWrapperCallback, (void*) this, &pErrStr);
    if (gDebugMode) printf("SQLITE3>> %p \terror code is %d\n", this, pRc);
    if (gDebugMode) printf("SQLITE3>> %p \terror message is %s\n", this, pErrStr);
    TestSqliteError(pStmt, &pRc, &pErrStr, (void*) this);
    if (pErrStr != NULL)
    {
      sqlite3_free(pErrStr);
      pErrStr = NULL;
    }
    // UPDATE startafter
    if (startafter)
    {
      if (retvec->empty()) (*startafter) = Tlogentry();
      else (*startafter) = (*retvec)[retvec->size() - 1];
    }
    return retvec->size() - count;
  }

  return 0;
}

bool
#ifdef EOS_STDMAP_DBMAP
SqliteDbMapInterface::syncFromDb(std::map<Tkey,Tval> *map)
#else
SqliteDbMapInterface::syncFromDb(::google::dense_hash_map<Tkey,Tval> *map)
#endif
{
  sqlite3_stmt *statement;
  sprintf(pStmt,"SELECT * FROM DbMap%p.dbmap;",this);
  if(sqlite3_prepare_v2(gDb,pStmt , -1, &statement, 0) == SQLITE_OK)
  {
    //int cols = sqlite3_column_count(statement);
    int result = 0;
    while(true)
    {
      result = sqlite3_step(statement);

      if(result == SQLITE_ROW)
      {
        Tkey key = (char*)sqlite3_column_text(statement, 0);
        Tval val;
        val.value = std::string((char*)sqlite3_column_blob(statement, 1),(size_t)sqlite3_column_bytes(statement,1));
        val.comment = (char*)sqlite3_column_text(statement, 2);
        val.timestampstr = (char*)sqlite3_column_text(statement, 3);
        val.seqid = sqlite3_column_int(statement, 4);
        val.writer = (char*)sqlite3_column_text(statement, 5);
        (*map)[key]=val;
      }
      else
      {
        break;
      }
    }

    sqlite3_finalize(statement);
  }
  else
  return false;

  return true;
}

bool
SqliteDbMapInterface::attachDbLog (const string &dbname, int volumeduration, int createperm, void* option)
{
  if (pAttachedDbLogs.find(dbname) == pAttachedDbLogs.end())
  {
    pAttachedDbLogs[dbname] = tOwnedSDLIptr(new SqliteDbLogInterface(dbname, volumeduration, createperm, option), true);
    bool rc= prepareExportStatement();
    return rc;
  }
  return false;
}

bool
SqliteDbMapInterface::detachDbLog (const string &dbname)
{
  if (pAttachedDbLogs.find(dbname) != pAttachedDbLogs.end())
  {
    delete pAttachedDbLogs[dbname].first; // the ownership should be true
    pAttachedDbLogs.erase(dbname);
    bool rc= prepareExportStatement();
    return rc;
  }
  return false;
}

SqliteDbLogInterface::SqliteDbLogInterface ()
{
  init();
  pDbName = "";
}

SqliteDbLogInterface::SqliteDbLogInterface (const string &dbname, int volumeduration, int createperm, void *option)
{
  init();
  setDbFile(dbname, volumeduration, createperm, option);
}

SqliteDbLogInterface::~SqliteDbLogInterface ()
{
  setDbFile("", -1, 0,NULL);
  gUniqMutex.Lock();
  if (gFile2SqName.empty() && gArchThreadStarted)
  {
    if (gDebugMode) printf("Shuting down archiving thread\n");
    XrdSysThread::Cancel(gArchThread);
    gArchMutex.Signal(); // wake up the thread to reach the cancel point
    gArchThreadStarted = false;
    XrdSysThread::Join(gArchThread, NULL);
  }
  gUniqMutex.UnLock();
  AtomicDec(gNInstances);
}

void
SqliteDbLogInterface::init ()
{
  AtomicInc(gNInstances);
  pIsOpen = false;
  if (gFile2SqName.empty()) for (int k = 0; k < 64; k++) gIdPool.insert(k); // 62 should be enough according to sqlite specs
}

void
SqliteDbLogInterface::archiveThreadCleanup (void *dummy)
{
  gArchMutex.UnLock();
  if (gDebugMode) printf("Cleaning up archive thread\n");
  fflush(stdout);
}

void*
SqliteDbLogInterface::archiveThread (void *dummy)
{
  pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
  pthread_cleanup_push(SqliteDbLogInterface::archiveThreadCleanup, NULL); // this is to make sure the mutex ius unlocked before terminating the thread
  gArchMutex.Lock();
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
    int timedout = gArchMutex.Wait(waketime - time(0));
    if (timedout) sleep(5); // a timeout to let the db requests started just before the deadline complete, possible cancellation point
  }
  pthread_cleanup_pop(0);
  return NULL;
}

int
SqliteDbLogInterface::archive (const tTimeToPeriodedFile::iterator &entry)
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
  tt = mktime(&t2);
  char dbuf1[256];
  char dbuf2[256];
  strftime(dbuf1, 256, timeformat, &t1);
  strftime(dbuf2, 256, timeformat, &t2);

  const string &filename = entry->second.first;
  char *archivename = new char[filename.size() + 256 * 2 + 4];
  sprintf(archivename, "%s__%s--%s", filename.c_str(), dbuf1, dbuf2);

  char *stmt = new char[1024 + filename.size() + 256 * 2 + 4];

  gUniqMutex.Lock();
  gTransactionMutex.LockWrite();

  sprintf(stmt, "ATTACH \'%s\' AS archive;", archivename);
  if(sqlite3_exec(gDb, stmt, NULL, NULL, NULL)!=SQLITE_OK) return 1;
  sprintf(stmt, "CREATE TABLE IF NOT EXISTS archive.ondisk (timestampstr TEXT, seqid INTEGER, writer TEXT, key BLOB, value BLOB,comment TEXT, PRIMARY KEY(timestampstr) );");
  execNoCallback2(stmt);
  TimeToStr(tt=mktime(&t2),dbuf1);
  sprintf(stmt, "INSERT INTO archive.ondisk SELECT * FROM %s.ondisk WHERE timestampstr<\"%s\";", gFile2SqName[filename].first.c_str(), dbuf1);
  execNoCallback2(stmt);
  sprintf(stmt, "DETACH archive;");
  execNoCallback2(stmt);
  sprintf(stmt, "DELETE FROM %s.ondisk WHERE timestampstr<\"%s\";", gFile2SqName[filename].first.c_str(), dbuf1);
  execNoCallback2(stmt);

  gTransactionMutex.UnLockWrite();
  gUniqMutex.UnLock();

  printf(" created archive %s\n", archivename);

  delete[] stmt;
  delete[] archivename;

  return 0;
}

int
SqliteDbLogInterface::updateArchiveSchedule (const tTimeToPeriodedFile::iterator &entry)
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
SqliteDbLogInterface::setArchivingPeriod (const string &dbname, int volumeduration)
{
  if (volumeduration > 0)
  {
    gArchMutex.Lock();
    if (gArchQueue.empty())
    { // init the archiving thread
      if (gDebugMode) printf("starting the archive thread\n");
      fflush(stdout);
      XrdSysThread::Run(&gArchThread, &archiveThread, NULL, XRDSYSTHREAD_HOLD, NULL);
      gArchThreadStarted = true;
    }
    gArchMutex.UnLock();

    gUniqMutex.Lock();
    if (gFile2SqName.find(dbname) == gFile2SqName.end())
    {
      gUniqMutex.UnLock();
      return 0; // no SqliteDbLogInterface instances related to this dbname
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
      gArchMutex.Lock();
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
      gArchMutex.UnLock();
      if (awakearchthread) gArchMutex.Signal();

      return gFile2SqName[dbname].second;
    }
  }
  return -1;
}

bool
SqliteDbLogInterface::setDbFile (const string &dbname, int volumeduration, int createperm, void *option)
{
  Option *opt=option?(Option*)option:&gDefaultOption;
  _unused(opt); // to get rid of the unused variable warning

  gArchMutex.Lock();
  gUniqMutex.Lock();
  // check if the file can be opened, it creates it with the required permissions if it does not exist.
  if (!dbname.empty())
  {
    if (gFile2SqName.find(dbname) == gFile2SqName.end())
    { // we do the check only if the file to attach is not yet attached
      int fd;
      if (createperm > 0)
      fd = open(dbname.c_str(), O_RDWR | O_CREAT, createperm);
      else
      fd = open(dbname.c_str(), O_RDWR | O_CREAT, 0644);// default mask is 644
      if (fd < 0)
      {
        gUniqMutex.UnLock();
        gArchMutex.UnLock();
        return false;
      }
      close(fd);
      // check if the file can be successfully opened by sqlite3
      bool halttransaction=gCurrentThreadTransact;
      if(halttransaction) execNoCallback("END TRANSACTION;");
      gTransactionMutex.LockWrite();
      sprintf(pStmt, "ATTACH \'%s\' AS %s_%u;", dbname.c_str(), "testattach", (unsigned int) pthread_self());
      execNoCallback(pStmt);
      if (pRc != SQLITE_OK)
      {
        if(halttransaction) execNoCallback("BEGIN TRANSACTION;");
        gUniqMutex.UnLock();
        gArchMutex.UnLock();
        return false;
      }
      sprintf(pStmt, "DETACH %s_%u;", "testattach", (unsigned int) pthread_self());
      pRc=execNoCallback(pStmt,100000);
      if(halttransaction) execNoCallback("BEGIN TRANSACTION;");
      else gTransactionMutex.UnLockWrite();
    }
  }

  char stmt[1024];
  if (!this->pDbName.empty())
  { // detach the current sqname
    tCountedSqname &csqn = gFile2SqName[this->pDbName];
    if (csqn.second > 1)
    {
      csqn.second--; // if there is other instances pointing to the that db DON'T detach
    }
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
      bool halttransaction=gCurrentThreadTransact;
      if(halttransaction) execNoCallback("END TRANSACTION;");
      gTransactionMutex.LockWrite();
      sprintf(stmt, "DETACH %s;", csqn.first.c_str()); // attached dbs for the logs are
      execNoCallback(stmt);
      if(halttransaction) execNoCallback("BEGIN TRANSACTION;");
      else gTransactionMutex.UnLockWrite();
      gFile2SqName.erase(this->pDbName);
      gIdPool.insert(atoi(this->pSqName.c_str() + 3));
    }
    pIsOpen = false;
  }

  this->pDbName = dbname;

  if (!dbname.empty())
  {
    if (gFile2SqName.find(dbname) != gFile2SqName.end())
    {
      gFile2SqName[this->pDbName].second++; // if there is already others instances pointing to that db DON'T attach
      pSqName = gFile2SqName[this->pDbName].first;
    }
    else
    {
      sprintf(stmt, "log%2.2d", *(gIdPool.begin())); // take the first id available in the pool
      pSqName = stmt;
      gIdPool.erase(*(gIdPool.begin()));// remove this id from the pool
      bool halttransaction=gCurrentThreadTransact;
      if(halttransaction) execNoCallback("END TRANSACTION;");
      gTransactionMutex.LockWrite();
      sprintf(stmt, "ATTACH \'%s\' AS %s;", dbname.c_str(), pSqName.c_str());// attached dbs for the logs are
      execNoCallback(stmt);
      sprintf(stmt, "CREATE TABLE IF NOT EXISTS %s.ondisk (timestampstr TEXT, seqid INTEGER, writer TEXT, key BLOB, value BLOB,comment TEXT, PRIMARY KEY(timestampstr) );", pSqName.c_str());
      execNoCallback(stmt);
      if(halttransaction) execNoCallback("BEGIN TRANSACTION;");
      else gTransactionMutex.UnLockWrite();
      gFile2SqName[dbname] = tCountedSqname(pSqName, 1);
    }
    pIsOpen = true;
  }

  gUniqMutex.UnLock();
  gArchMutex.UnLock();

  if (volumeduration > 0) setArchivingPeriod(dbname, volumeduration);

  return true;
}

bool
SqliteDbLogInterface::isOpen () const
{
  return pIsOpen;
}

string
SqliteDbLogInterface::getDbFile () const
{
  return pDbName;
}

size_t
SqliteDbLogInterface::getTail (int nentries, TlogentryVec *retvec) const
{
  char *stmt = new char[256];
  size_t count = retvec->size();
  sprintf(stmt, "SELECT * FROM (SELECT * FROM %s.ondisk ORDER BY timestampstr DESC LIMIT %d) ORDER BY timestampstr ASC;", this->GetSqName().c_str(), nentries);
  this->pRetVecPtr = retvec;
  if (gDebugMode) printf("SQLITE3>> %p executing %s\n", this, stmt);
  pRc = sqlite3_exec(gDb, stmt, staticLogWrapperCallback, (void*) this, &pErrStr);
  if (gDebugMode) printf("SQLITE3>> %p \terror code is %d\n", this, pRc);
  if (gDebugMode) printf("SQLITE3>> %p \terror message is %s\n", this, pErrStr);
  TestSqliteError(stmt, &pRc, &pErrStr, (void*) this);
  if (pErrStr != NULL)
  {
    sqlite3_free(pErrStr);
    pErrStr = NULL;
  }
  delete[] stmt;
  return retvec->size() - count;
}

size_t
SqliteDbLogInterface::getAll (TlogentryVec *retvec, size_t nmax, Tlogentry *startafter) const
{
  size_t count = retvec->size();
  char limit[1024], id[1024];
  limit[0] = id[0] = 0;
  if (nmax) sprintf(limit, " LIMIT %lu", nmax);
  if (startafter) if (startafter->timestampstr.length()) sprintf(id, " WHERE TIMESTAMPSTR>(SELECT TIMESTAMPSTR FROM %s.ondisk WHERE timestampstr=\"%s\")", this->GetSqName().c_str(), startafter->timestampstr.c_str());
  sprintf(pStmt, "SELECT * FROM %s.ondisk%s ORDER BY timestampstr%s;", this->GetSqName().c_str(), id, limit);
  this->pRetVecPtr = retvec;
  if (gDebugMode) printf("SQLITE3>> %p executing %s\n", this, pStmt);
  pRc = sqlite3_exec(gDb, pStmt, staticLogWrapperCallback, (void*) this, &pErrStr);
  if (gDebugMode) printf("SQLITE3>> %p \terror code is %d\n", this, pRc);
  if (gDebugMode) printf("SQLITE3>> %p \terror message is %s\n", this, pErrStr);
  TestSqliteError(pStmt, &pRc, &pErrStr, (void*) this);
  if (pErrStr != NULL)
  {
    sqlite3_free(pErrStr);
    pErrStr = NULL;
  }
  // UPDATE startafter
  if (startafter)
  {
    if (retvec->empty()) (*startafter) = Tlogentry();
    else (*startafter) = (*retvec)[retvec->size() - 1];
  }
  return retvec->size() - count;
}

bool SqliteDbLogInterface::clear()
{
  //!!! VACUUM
  sprintf(pStmt, "DELETE FROM %s;", pTableName);
  pRc = execNoCallback(pStmt);
  return pRc==0;
}

EOSCOMMONNAMESPACE_END
