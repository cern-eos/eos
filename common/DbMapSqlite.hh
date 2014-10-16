// ----------------------------------------------------------------------
// File: DbMapSqlite.hh
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
 * @file   DbMapSqlite.hh
 *
 * @brief  DbMap/DbLog interfaces for sqlite 3
 *
 */

#ifndef __EOSCOMMON_DBMAP_SQLITE_HH__
#define __EOSCOMMON_DBMAP_SQLITE_HH__

/*----------------------------------------------------------------------------*/
#include "common/DbMapCommon.hh"
#include "common/sqlite/sqlite3.h"
/*----------------------------------------------------------------------------*/EOSCOMMONNAMESPACE_BEGIN

/*-----------------    SQLITE INTERFACE IMPLEMENTATION     -------------------*/
//! Here follows the implementation of the DbMap/DbLog interfaces for sqlite3
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
//! this base class provides some sqlite helpers to implement the sqlite interfaces
//! sqlite is accessed using a unique connection to ":memory" (which is supposed to be thread-safely implemented in sqlite3).
//! The db files are attached.
//! the number of attached dbs (including redundant attachments) cannot exceed SQLITE_MAX_ATTACHED that in turns cannot exceed 62
/*----------------------------------------------------------------------------*/
class SqliteInterfaceBase : public eos::common::LogId
{
protected:
  static RWMutex gBaseMutex;
  static RWMutex gTransactionMutex;
  static __thread bool gCurrentThreadTransact;

  mutable char pStmt[1024];
  static bool gDebugMode;
  static bool gAbortOnsSliteError;
  static sqlite3 *gDb;
  static unsigned gNInstances;
  mutable DbMapTypes::TlogentryVec *pRetVecPtr;
  char pTableName[32];
  mutable char *pErrStr;
  mutable int pRc;
  friend void _testSqliteError_(const char* str, const int *rc, char **errstr, const void* _this, const char* __file, int __line);

  int execNoCallback(const char* str, int retrydelay=100000, int nretry=100);
  static int execNoCallback2(const char* str, int retrydelay=100000, int nretry=100); // for the second thread
  int exec(const char* str, int retrydelay=100000, int nretry=100);
  static int exec2(const char* str, int retrydelay=100000, int nretry=100);// for the second thread
  int logWrapperCallback(void*unused,int,char**,char**);
  int mapWrapperCallback(void*unused,int,char**,char**);
  inline static int staticLogWrapperCallback(void *_this,int n,char** k,char** v)
  { return ((SqliteInterfaceBase*)_this)->logWrapperCallback(NULL,n,k,v);}
  inline static int staticMapWrapperCallback(void *_this,int n,char** k,char** v)
  { return ((SqliteInterfaceBase*)_this)->mapWrapperCallback(NULL,n,k,v);}
  static int printCallback(void *unused,int n,char** key,char** val)
  { for(int k=0;k<n;k++) printf("%s = %s\t",key[k],val[k]); printf("\n"); return 0;}
public :
  SqliteInterfaceBase() : pRetVecPtr(NULL), pErrStr(NULL)
  {
    RWMutexWriteLock lock(gBaseMutex);
    AtomicInc(gNInstances);
    if(gDebugMode) printf("SQLITE3>> number of SqliteInterfaces instances %u\n",gNInstances);
    int rc; if(gNInstances==1)
    {
      if((rc=sqlite3_open_v2(":memory:",&gDb,SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_WAL,NULL))!=SQLITE_OK)
      {
        eos_err("Error Opening Sqlite3 Database, return code is %d\n",rc);
      }
      else
      {
        //sqlite3_create_function(db, "REGEXP", 2, SQLITE_UTF8, NULL, &user_regexp, NULL, NULL);
        sqlite3_config(SQLITE_CONFIG_SERIALIZED);
        sprintf(pStmt,"PRAGMA locking_mode = NORMAL;"); execNoCallback(pStmt);
        sprintf(pStmt,"PRAGMA encoding = \"UTF-8\";"); execNoCallback(pStmt);
        // ==> Might need to modify these parameters to tune the performances
        // ==> Member functions may be coded to alter these parameters
        //sprintf(stmt,"PRAGMA journal_mode = OFF;"); ExecNoCallback(stmt);
        //sprintf(stmt,"PRAGMA synchronous = 0;"); ExecNoCallback(stmt); // already in the embedded compilation of SQLITE
        //sprintf(stmt,"PRAGMA default_cache_size = 20000"); ExecNoCallback(stmt);
      }
    }
    while(gDb==NULL) usleep(10000); // to wait for the openning if another thread is doing it
  }
  static void setDebugMode(bool on)
  { RWMutexWriteLock lock(gBaseMutex); gDebugMode=on;}
  virtual ~SqliteInterfaceBase()
  {
    RWMutexWriteLock lock(gBaseMutex);
    AtomicDec(gNInstances);
    if(gDebugMode) printf("SQLITE3>> number of SqliteInterfaces instances %u\n",gNInstances);
    if(gNInstances==0)
    {
      if(gDebugMode) printf("SQLITE3>> closing db connection\n");
      sqlite3_close(gDb);
    }
  }
  static void setAbortOnSqliteError(bool b)
  { RWMutexWriteLock lock(gBaseMutex); gAbortOnsSliteError=b;}
  struct Option
  {
    size_t test;
  };
protected:
  static Option gDefaultOption;
};
/*----------------------------------------------------------------------------*/
//! This function looks for sqlite3 error and prints it if some is found
/*----------------------------------------------------------------------------*/
inline void _testSqliteError_(const char* str, const int *rc, char **errstr, const void* _this, const char* __file, int __line)
{
  if(SqliteInterfaceBase::gAbortOnsSliteError && (*rc!=SQLITE_OK) && (*rc!=SQLITE_DONE) && (*rc!=SQLITE_ROW) )
  {
//    fprintf(stderr," Sqlite 3 Error in %s at line %d , object %p\t Executing %s returned %d\t The error message was %s\n",__file,__line,_this,str,*rc,*errstr);
//    fflush(stderr);
    eos_static_emerg(" Sqlite 3 Error in %s at line %d , object %p\t Executing %s returned %d\t The error message was %s\n",__file,__line,_this,str,*rc,*errstr);
    assert(false);
//    exit(1);
    abort();
  }
}

#define TestSqliteError(stmt,returncode,errstr,_this) _testSqliteError_(stmt,returncode,errstr,_this,__FILE__,__LINE__);
/*----------------------------------------------------------------------------*/
//! This class implements the DbLogInterface using Sqlite3
/*----------------------------------------------------------------------------*/
class SqliteDbLogInterface : public SqliteInterfaceBase, public DbLogInterface
{
public:
  typedef std::pair<std::string,int> tCountedSqname;
  typedef std::map<std::string,tCountedSqname> tMapFileToSqname; // filename -> attach-name, number of DbLogInterfaces
  typedef std::pair<std::string,int> tPeriodedFile;
  typedef std::multimap<timespec,tPeriodedFile,DbMapTypes::TimeSpecComparator> tTimeToPeriodedFile;// next update -> filename,
private:
  friend class SqliteDbMapInterface;
  static unsigned gNInstances;

  // management of the uniqueness of the db attachments
  static XrdSysMutex gUniqMutex;
  static tMapFileToSqname gFile2SqName;
  static std::set<int> gIdPool;
  std::string pSqName;
  inline const std::string & GetSqName() const
  { return pSqName;}

  // archiving management
  static tTimeToPeriodedFile gArchQueue;
  static pthread_t gArchThread;
  static bool gArchThreadStarted;
  static void archiveThreadCleanup(void *dummy=NULL);
  static void* archiveThread(void*dummy=NULL);
  static XrdSysCondVar gArchMutex;
  static pthread_cond_t gArchCond;
  static int archive(const tTimeToPeriodedFile::iterator &entry );
  static int updateArchiveSchedule(const tTimeToPeriodedFile::iterator &entry);

  std::string pDbName;
  bool pIsOpen;

  void init();
  SqliteDbLogInterface(const SqliteDbLogInterface &);
public:
  typedef enum
  { testly=10, hourly=3600, daily=3600*24, weekly=3600*24*7}period;
  SqliteDbLogInterface();
  SqliteDbLogInterface(const std::string &dbname, int volumeduration, int createperm, void *options=NULL);
  virtual bool setDbFile( const std::string &dbname, int volumeduration, int createperm, void* option);
  virtual bool isOpen() const;
  virtual std::string getDbFile() const;
  static std::string getDbType()
  { return "Sqlite3";}
  virtual size_t getAll( TlogentryVec *retvec , size_t nmax=0, Tlogentry *startafter=NULL ) const;
  virtual size_t getTail( int nentries, TlogentryVec *retvec ) const;
  virtual bool clear();
  static int setArchivingPeriod(const std::string &dbname, int volumeduration);
  virtual ~SqliteDbLogInterface();
};

/*----------------------------------------------------------------------------*/
//! This class implements the DbMapInterface using Sqlite3
/*----------------------------------------------------------------------------*/
class SqliteDbMapInterface : public SqliteInterfaceBase, public DbMapInterface
{
  friend class SqliteDbLogInterface;

  std::string pName;
  sqlite3_stmt *pGetStmt,*pSetStmt,*pRemoveStmt,*pSizeStmt,*pCountStmt;
  std::vector<sqlite3_stmt *> pExportStmts;

  typedef std::pair<SqliteDbLogInterface*,bool> tOwnedSDLIptr; // pointer, ownit
  std::map<std::string,tOwnedSDLIptr> pAttachedDbLogs;

  std::string pAttachedDbName;

  int prepareStatements();
  int prepareExportStatement();
  SqliteDbMapInterface(const SqliteDbMapInterface &);
public:
  SqliteDbMapInterface();
  void setName( const std::string &);
  static std::string getDbType()
  { return "Sqlite3";}
  virtual const std::string & getName() const;
  virtual bool beginTransaction();
  virtual bool endTransaction();
  virtual bool getEntry(const Slice &key, Tval *val);
  virtual bool setEntry(const Slice &key, const TvalSlice &val);
  virtual size_t size() const;
  virtual size_t count(const Slice &key) const;
  virtual bool removeEntry(const Slice &key, const TvalSlice &val);
  virtual bool clear();
  // Persistent
  virtual bool attachDb(const std::string &dbname, bool repair=false, int createperm=0, void* option=NULL);
  virtual bool trimDb();
  virtual std::string getAttachedDbName() const;
#ifdef EOS_STDMAP_DBMAP
  virtual bool syncFromDb(std::map<Tkey,Tval> *map);
#else
  virtual bool syncFromDb(::google::dense_hash_map<Tkey,Tval> *map);
#endif
  virtual bool detachDb();
  // Requestors
  virtual size_t getAll( TlogentryVec *retvec, size_t nmax, Tlogentry *startafter ) const;

  virtual bool attachDbLog(const std::string &dbname,int volumeduration, int createperm, void* option);
  virtual bool detachDbLog(const std::string &dbname);
  virtual bool attachDbLog(DbLogInterface *dbname);
  virtual bool detachDbLog(DbLogInterface *dbname);
  virtual ~SqliteDbMapInterface();
};
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_END
#endif
