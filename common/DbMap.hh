// ----------------------------------------------------------------------
// File: DbMap.hh
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
 * @file   DbMap.hh
 * 
 * @brief  Classes for a simple logging facility into a DB.
 *		   The default underlying DB is leveldb from google.
 *         If the EOS_SQLITE_DBMAP is defined, then SQLITE3 is used instead
 *         Note that the SQLITE3 implementation is roughly 10 times slower for write operations
 *
 */

#ifndef __EOSCOMMON_DBMAP_HH__
#define __EOSCOMMON_DBMAP_HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/Logging.hh"
#include "common/sqlite/sqlite3.h"
#ifndef EOS_SQLITE_DBMAP
#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "leveldb/filter_policy.h"
#endif
#include "common/RWMutex.hh"
#include "common/stringencoders/modp_numtoa.h"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysAtomics.hh"
/*----------------------------------------------------------------------------*/
#include <pthread.h>
#include <time.h>
#include <vector>
#include <set>
#include <map>
#include <cmath>

// By default, the LEVELDB implementation is used
// define this macro to enable the SQLITE3 implementation
// #define EOS_SQLITE_DBMAP

EOSCOMMONNAMESPACE_BEGIN

inline bool operator<(const timespec &t1, const timespec &t2) { return (t1.tv_sec)<(t2.tv_sec); }
inline bool operator<=(const timespec &t1, const timespec &t2) { return (t1.tv_sec)<=(t2.tv_sec); }
struct mytimespecordering {
  inline bool operator() (const timespec &t1, const timespec &t2) const {return t1<t2;};
};

/*-------------------------  DATA TYPES  --------------------------------*/
/*----------------------------------------------------------------------------*/
//! Class provides the data types for the DbMap implementation
/*----------------------------------------------------------------------------*/
class DbMapTypes {
public:
  typedef std::string Tkey;
  struct Tlogentry {
    std::string timestamp;
    std::string timestampstr;
    std::string seqid;
    std::string writer;			// each map has a unique name. This name is reported for each entry in the log.
    std::string key;
    std::string value;
    std::string comment;
  };
  struct Tval {
    size_t timestamp;
    std::string timestampstr;
    unsigned long seqid;
    std::string writer;
    std::string value;
    std::string comment;
  };
  typedef std::vector<Tlogentry> TlogentryVec;
};
bool operator == ( const DbMapTypes::Tlogentry &l, const DbMapTypes::Tlogentry &r);
DbMapTypes::Tval Tlogentry2Tval (const DbMapTypes::Tlogentry &tle);
/*----------------------------------------------------------------------------*/


/*------------------------    INTERFACES     ---------------------------------*/
// The following abstract classes are interface to the db representation for the DbMap and DbLog class. These classes work as a pair and should be implemented as is.
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
//! This class provides an interface to implement the necessary functions for the representation in a DB of the DbLog class
//! This should implement an in-memory six columns table. Each column should contain string
//! The names of the column are timestamp, logid, key, value, comment. There must be an uniqueness constraint on the timestamp (which should also be the primary key).
/*----------------------------------------------------------------------------*/
class DbLogInterface {
public:
  typedef DbMapTypes::Tkey Tkey;
  typedef DbMapTypes::Tval Tval;
  typedef DbMapTypes::Tlogentry Tlogentry;
  typedef DbMapTypes::TlogentryVec TlogentryVec;

  virtual bool SetDbFile( const std::string &dbname, int sliceduration, int createperm)=0;
  virtual bool IsOpen() const=0;
  virtual std::string GetDbFile() const =0;
  virtual size_t GetAll( TlogentryVec &retvec, size_t nmax=0, Tlogentry *startafter=NULL ) const =0;
  virtual size_t GetByRegex( const std::string &regex, TlogentryVec &retvec, size_t nmax=0, Tlogentry *startafter=NULL ) const=0;
  virtual size_t GetTail( int nentries, TlogentryVec &retvec) const=0;
  virtual ~DbLogInterface() {}

};
/*----------------------------------------------------------------------------*/
//! this class provides an interface to implement the necessary functions for the representation in a DB of the DbMap class
//! this should implement an in-memory six columns table. Each column should contain string
//! the names of the column are timestamp, timestampstr, logid, key, value, comment. There must be an uniqueness constraint on the key.
/*----------------------------------------------------------------------------*/
class DbMapInterface {
public:
  typedef DbMapTypes::Tkey Tkey;
  typedef DbMapTypes::Tval Tval;
  typedef DbMapTypes::Tlogentry Tlogentry;
  virtual void SetName( const std::string &)=0;
  virtual const std::string & GetName() const =0;
  virtual bool BeginTransaction()=0;
  virtual bool EndTransaction()=0;
  virtual bool InsertEntry(const Tkey &key, const Tval &val)=0;
  virtual bool ChangeEntry(const Tkey &key, const Tval &val)=0;
  virtual bool RemoveEntry(const Tkey &key)=0;
  virtual bool AttachDbLog(const std::string &dbname, int sliceduration, int createperm)=0;
  virtual bool DetachDbLog(const std::string &dbname)=0;
  virtual bool AttachDbLog(DbLogInterface *dblogint)=0;
  virtual bool DetachDbLog(DbLogInterface *dblogint)=0;
  virtual ~DbMapInterface() {}
};
/*----------------------------------------------------------------------------*/


/*-----------------    SQLITE INTERFACE IMPLEMENTATION     -------------------*/
//! Here follows the implementation of the DbMap/DbLog interfaces for sqlite3
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
//! this base class provides some sqlite helpers to implement the sqlite interfaces
//! sqlite is accessed using a unique connection to ":memory" (which is supposed to be thread-safely implemented in sqlite3).
//! The db files are attached.
//! the number of attached dbs (including redundant attachments) cannot exceed SQLITE_MAX_ATTACHED that in turns cannot exceed 62
/*----------------------------------------------------------------------------*/
class SqliteInterfaceBase : public eos::common::LogId {
protected:
  static RWMutex transactionmutex;

  mutable char stmt[1024];
  static bool debugmode;
  static bool abortonsqliteerror;
  static sqlite3 *db;
  static unsigned ninstances;
  mutable DbMapTypes::TlogentryVec *retvecptr;
  char tablename[32];
  mutable char *errstr;
  mutable int rc;
  friend void _TestSqliteError_(const char* str, const int *rc, char **errstr, void* _this, const char* __file, int __line);

  int ExecNoCallback(const char *);
  static int ExecNoCallback2(const char *); // for the second thread
  int Exec(const char *);
  static int Exec2(const char *);		   // for the second thread
  int WrapperCallback(void*unused,int,char**,char**);
  inline static int StaticWrapperCallback(void *_this,int n,char** k,char** v) {return ((SqliteInterfaceBase*)_this)->WrapperCallback(NULL,n,k,v);}
  static int PrintCallback(void *unused,int n,char** key,char** val) {for(int k=0;k<n;k++) printf("%s = %s\t",key[k],val[k]); printf("\n"); return 0;}
  static void user_regexp(sqlite3_context *context, int argc, sqlite3_value **argv);
public :
  SqliteInterfaceBase() : retvecptr(NULL), errstr(NULL) {
    AtomicInc(ninstances);
    if(debugmode) printf("SQLITE3>> number of SqliteInterfaces instances %u\n",ninstances);
    int rc; if(ninstances==1) {
      if((rc=sqlite3_open_v2(":memory:",&db,SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_WAL,NULL))!=SQLITE_OK) {
        eos_err("Error Opening Sqlite3 Database, return code is %d\n",rc);
      }
      else {
        sqlite3_create_function(db, "REGEXP", 2, SQLITE_UTF8, NULL, &user_regexp, NULL, NULL);
        sqlite3_config(SQLITE_CONFIG_SERIALIZED);
        sprintf(stmt,"PRAGMA locking_mode = NORMAL;"); ExecNoCallback(stmt);
        sprintf(stmt,"PRAGMA encoding = \"UTF-8\";"); ExecNoCallback(stmt);
        // ==> Might need to modify these parameters to tune the performances
        // ==> Member functions may be coded to alter these parameters
        //sprintf(stmt,"PRAGMA journal_mode = OFF;"); ExecNoCallback(stmt);
        //sprintf(stmt,"PRAGMA synchronous = 0;"); ExecNoCallback(stmt); // already in the embedded compilation of SQLITE
        //sprintf(stmt,"PRAGMA default_cache_size = 20000"); ExecNoCallback(stmt);
      }
    }
    while(db==NULL) usleep(10000); // to wait for the openning if another thread is doing it
  }
  static void SetDebugMode(bool on) {debugmode=on;}
  virtual ~SqliteInterfaceBase() {
    AtomicDec(ninstances);
    if(debugmode) printf("SQLITE3>> number of SqliteInterfaces instances %u\n",ninstances);
    if(ninstances==0) {
      if(debugmode) printf("SQLITE3>> closing db connection\n");
      sqlite3_close(db);
    }
  }
  static void SetAbortOnSqliteError(bool b) {abortonsqliteerror=b;}
};
/*----------------------------------------------------------------------------*/
//! This function looks for sqlite3 error and prints it if some is found
/*----------------------------------------------------------------------------*/
inline void _TestSqliteError_(const char* str, const int *rc, char **errstr, void* _this, const char* __file, int __line) {
  if(SqliteInterfaceBase::abortonsqliteerror && (*rc!=SQLITE_OK) && (*rc!=SQLITE_DONE) ) {
    fprintf(stderr," Sqlite 3 Error in %s at line %d , object %p\t Executing %s returned %d\t The error message was %s\n",__file,__line,_this,str,*rc,*errstr);
    eos_static_emerg(" Sqlite 3 Error in %s at line %d , object %p\t Executing %s returned %d\t The error message was %s\n",__file,__line,_this,str,*rc,*errstr);
    exit(1);
  }
}
#define TestSqliteError(stmt,returncode,errstr,_this) _TestSqliteError_(stmt,returncode,errstr,_this,__FILE__,__LINE__);
/*----------------------------------------------------------------------------*/
//! This class implements the DbLogInterface using Sqlite3
/*----------------------------------------------------------------------------*/
class SqliteDbLogInterface : public SqliteInterfaceBase, public DbLogInterface {
public:
  typedef std::pair<std::string,int> tCountedSqname;
  typedef std::map<std::string,tCountedSqname> tMapFileToSqname; // filename -> attach-name, number of DbLogInterfaces
  typedef std::pair<std::string,int> tPeriodedFile;
  typedef std::multimap<timespec,tPeriodedFile,mytimespecordering> tTimeToPeriodedFile; // next update -> filename,
private:
  friend class SqliteDbMapInterface;
  static unsigned ninstances;

  // management of the uniqueness of the db attachments
  static XrdSysMutex uniqmutex;
  static tMapFileToSqname file2sqname;
  static std::set<int> idpool;
  std::string sqname;
  inline const std::string & GetSqName() const {return sqname;}

  // archiving management
  static tTimeToPeriodedFile archqueue;
  static pthread_t archthread;
  static bool archthreadstarted;
  static void ArchiveThreadCleanup(void *dummy=NULL);
  static void* ArchiveThread(void*dummy=NULL);
  static XrdSysCondVar archmutex;
  static pthread_cond_t archcond;
  static int Archive(const tTimeToPeriodedFile::iterator &entry );
  static int UpdateArchiveSchedule(const tTimeToPeriodedFile::iterator &entry);

  std::string dbname;
  bool isopen;

  void Init();
  SqliteDbLogInterface(const SqliteDbLogInterface &);
public:
  typedef enum {testly=10, hourly=3600, daily=3600*24, weekly=3600*24*7} period;
  SqliteDbLogInterface();
  SqliteDbLogInterface(const std::string &dbname, int sliceduration, int createperm);
  bool SetDbFile( const std::string &dbname, int sliceduration, int createperm);
  bool IsOpen() const;
  std::string GetDbFile() const;
  static std::string GetDbType() { return "Sqlite3";}
  size_t GetAll( TlogentryVec &retvec , size_t nmax=0, Tlogentry *startafter=NULL ) const;
  size_t GetByRegex( const std::string &regex, TlogentryVec &retvec , size_t nmax=0, Tlogentry *startafter=NULL ) const;
  size_t GetTail( int nentries, TlogentryVec &retvec ) const;
  static int SetArchivingPeriod(const std::string &dbname, int sliceduration);
  virtual ~SqliteDbLogInterface();
};

/*----------------------------------------------------------------------------*/
//! This class implements the DbMapInterface using Sqlite3
/*----------------------------------------------------------------------------*/
class SqliteDbMapInterface : public SqliteInterfaceBase, public DbMapInterface {
  friend class SqliteDbLogInterface;

  std::string name;
  sqlite3_stmt *insert_stmt,*change_stmt,*remove_stmt;
  std::vector<sqlite3_stmt *> export_stmts;

  typedef std::pair<SqliteDbLogInterface*,bool> tOwnedSDLIptr; // pointer, ownit
  std::map<std::string,tOwnedSDLIptr> attacheddbs;

  int PrepareStatements();
  int PrepareExportStatement();
  SqliteDbMapInterface(const SqliteDbMapInterface &);
public:
  SqliteDbMapInterface();
  void SetName( const std::string &);
  static std::string GetDbType() { return "Sqlite3";}
  const std::string & GetName() const;
  bool BeginTransaction();
  bool EndTransaction();
  bool InsertEntry(const Tkey &key, const Tval &val);
  bool ChangeEntry(const Tkey &key, const Tval &val);
  bool RemoveEntry(const Tkey &key);
  bool AttachDbLog(const std::string &dbname,int sliceduration, int createperm);
  bool DetachDbLog(const std::string &dbname);
  bool AttachDbLog(DbLogInterface *dbname);
  bool DetachDbLog(DbLogInterface *dbname);
  virtual ~SqliteDbMapInterface();
};
/*----------------------------------------------------------------------------*/

#ifndef EOS_SQLITE_DBMAP
/*-----------------    LEVELDB INTERFACE IMPLEMENTATION     -------------------*/
//! Here follows the implementation of the DbMap/DbLog interfaces for LevelDb
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
//! this base class provides some LevelDb helpers to implement the LevelDb interfaces
//! the db is accessed using via a unique leveldb::DB which is supposed to be thread-safely implemented in leveldb.
//! each transaction (batch in LevelDb words) is instanciated as a leveldb::batch object which is NOT thread-safe
//! Contrary to the sqlite3 implementation, the DbMap does NOT have a representation into the DB library.
//! Although the function to maintain such a representation are provided, they actually do NOTHING in leveldb
//! The actual interaction with leveldb is in the DbLog interface.
/*----------------------------------------------------------------------------*/
class LvDbInterfaceBase : public eos::common::LogId {
protected:
  friend void _TestLvDbError_(const leveldb::Status &s, void* _this, const char* __file, int __line);
  static bool abortonlvdberror;
  static bool debugmode;
  static unsigned ninstances;
public :
  static void SetDebugMode(bool on) {debugmode=on;}
  static void SetAbortOnLvDbError(bool b) {abortonlvdberror=b;}

};
/*----------------------------------------------------------------------------*/
//! This function looks for leveldb error and prints it if some is found
/*----------------------------------------------------------------------------*/
inline void _TestLvDbError_(const leveldb::Status &s, void* _this, const char* __file, int __line) {
  if(LvDbInterfaceBase::abortonlvdberror && !s.ok() ) {
    fprintf(stderr," LevelDb Error in %s at line %d involving object %p : %s\n",__file,__line,_this,s.ToString().c_str());
    eos_static_emerg(" LevelDb Error in %s at line %d involving object %p : %s\n",__file,__line,_this,s.ToString().c_str());
    exit(1);
  }
}
#define TestLvDbError(s,_this) _TestLvDbError_(s,_this,__FILE__,__LINE__);
/*----------------------------------------------------------------------------*/
//! This class implements the DbLogInterface using leveldb
/*----------------------------------------------------------------------------*/
class LvDbDbLogInterface : public LvDbInterfaceBase, public DbLogInterface {
public:
  typedef std::pair<leveldb::DB*,leveldb::FilterPolicy*> DbAndFilter;
  typedef std::pair<DbAndFilter,int> tCountedDbAndFilter;
  typedef std::map<std::string,tCountedDbAndFilter> tMapFileToDb; // filename -> attach-name, number of DbLogInterfaces
  typedef std::pair<std::string,int> tPeriodedFile;
  typedef std::multimap<timespec,tPeriodedFile,mytimespecordering> tTimeToPeriodedFile; // next update -> filename,
private:
  friend class LvDbDbMapInterface;

  // management of the uniqueness of the db attachments
  static XrdSysMutex uniqmutex;
  static tMapFileToDb file2db ;
  static std::set<int> idpool;
  //std::string db;
  leveldb::DB *db;

  // archiving management
  static tTimeToPeriodedFile archqueue;
  static pthread_t archthread;
  static bool archthreadstarted;
  static void ArchiveThreadCleanup(void *dummy=NULL);
  static void* ArchiveThread(void*dummy=NULL);
  static XrdSysCondVar archmutex;
  static pthread_cond_t archcond;
  static int Archive(const tTimeToPeriodedFile::iterator &entry );
  static int UpdateArchiveSchedule(const tTimeToPeriodedFile::iterator &entry);

  std::string dbname;
  bool isopen;

  void Init();
  LvDbDbLogInterface(const LvDbDbLogInterface &);
public:
  typedef enum {testly=10, hourly=3600, daily=3600*24, weekly=3600*24*7} period;
  LvDbDbLogInterface();
  LvDbDbLogInterface(const std::string &dbname, int sliceduration, int createperm);
  bool SetDbFile( const std::string &dbname, int sliceduration, int createperm);
  bool IsOpen() const;
  std::string GetDbFile() const;
  static std::string GetDbType() { return "LevelDB";}
  size_t GetAll( TlogentryVec &retvec , size_t nmax=0, Tlogentry *startafter=NULL ) const;
  size_t GetByRegex( const std::string &regex, TlogentryVec &retvec , size_t nmax=0, Tlogentry *startafter=NULL ) const;
  size_t GetTail( int nentries, TlogentryVec &retvec ) const;
  static int SetArchivingPeriod(const std::string &dbname, int sliceduration);
  virtual ~LvDbDbLogInterface();
};

/*----------------------------------------------------------------------------*/
//! This class implements the DbMapInterface using LvDb
//! Note that the map doesn't have a LevelDb representation in memory
/*----------------------------------------------------------------------------*/
class LvDbDbMapInterface : public LvDbInterfaceBase, public DbMapInterface {
  friend class LvDbDbLogInterface;

  std::string name;

  RWMutex batchmutex;
  leveldb::WriteBatch writebatch;
  bool batched;

  typedef std::pair<LvDbDbLogInterface*,bool> tOwnedLDLIptr; // pointer, ownit
  std::map<std::string,tOwnedLDLIptr> attacheddbs;

  LvDbDbMapInterface(const LvDbDbMapInterface &);
public:
  LvDbDbMapInterface();
  void SetName( const std::string &);
  static std::string GetDbType() { return "LevelDB";}
  const std::string & GetName() const;
  bool BeginTransaction();
  bool EndTransaction();
  bool InsertEntry(const Tkey &key, const Tval &val);
  bool ChangeEntry(const Tkey &key, const Tval &val);
  bool RemoveEntry(const Tkey &key);
  bool AttachDbLog(const std::string &dbname,int sliceduration, int createperm);
  bool DetachDbLog(const std::string &dbname);
  bool AttachDbLog(DbLogInterface *dbname);
  bool DetachDbLog(DbLogInterface *dbname);
  virtual ~LvDbDbMapInterface();
};
/*----------------------------------------------------------------------------*/
#endif

/*------------    DbMap and DbLog TEMPLATE IMPLEMENTATIONS     ---------------*/
template<class TDbMapInterface, class TDbLogInterface> class DbLogT; // the definition of this template comes later in the same file

/*----------------------------------------------------------------------------*/
//! this class is like a map which has both an in memory storage and a db storage.
//! it maps a key (a string) to a value (a string) and a comment (a string).
//! Additional informations are automatically added into the map, namely :
//! a timestamp (a size_t), a string representation of the timestamp, a sequence id ( an integer ).
//! The db storage aims at implementing logging facilities by using the DbLog class.
/*----------------------------------------------------------------------------*/
template<class TDbMapInterface,class TDbLogInterface> class DbMapT : public eos::common::LogId  {
public:
  // typedefs
  typedef DbMapTypes::Tkey Tkey;
  typedef DbMapTypes::Tval Tval;
  typedef std::pair<Tkey,Tval> Tkeyval;
  typedef std::map<Tkey,Tval> Tmap;
  typedef std::vector<Tkeyval> Tlist;

private:
  //------------------------------------------------------------------------
  //! this set makes sure that every name is unique
  //------------------------------------------------------------------------
  static std::set<std::string> names;

  static RWMutex namesmutex;

  //------------------------------------------------------------------------
  //! the name of the DbMap instance. Default is db%p where %p is the value of 'this' pointer. it can be changed
  //! this name is used as the 'writer' value into the DbLog
  //------------------------------------------------------------------------
  std::string name;

  //------------------------------------------------------------------------
  //! iterating shows that the instance is being iterated (const_iteration is the only available). all others threads trying to access to the instance are blocked while a thread is iterating iterating.
  //------------------------------------------------------------------------
  mutable bool iterating;

  //------------------------------------------------------------------------
  //! this is the map containing the data. Any read access to the instance is made to this map without accessing it. Any write access to the instance is made on both this map and the DB.
  //------------------------------------------------------------------------
  Tmap map;

  //------------------------------------------------------------------------
  //! this is the underlying iterator to the const_iteration feature
  //------------------------------------------------------------------------
  mutable Tmap::const_iterator it;

  //------------------------------------------------------------------------
  //! this list is used to accumulate pair of key values during a "set" sequence.
  //------------------------------------------------------------------------
  Tlist setseqlist;

  //------------------------------------------------------------------------
  //! setsequence is true if an ongoing set sequence is running. It doesn't lock the instance.
  //------------------------------------------------------------------------
  mutable bool setsequence;

  //------------------------------------------------------------------------
  //! db is a pointer to the db manager of the data
  //! the TDmapInterface is not supposed to be thread-safe. So db must be protected by a lock.
  //------------------------------------------------------------------------
  TDbMapInterface *db;

  //------------------------------------------------------------------------
  //! this is mutex at instance granularity
  //------------------------------------------------------------------------
  mutable RWMutex mutex;

  //------------------------------------------------------------------------
  //! this are the counters of 'set' and 'get' calls to this instance
  //------------------------------------------------------------------------
  mutable size_t setCounter, getCounter;

  //------------------------------------------------------------------------
  //! this tells if fractional seconds should be added to timestamp string in the db. it Doesn't impact the precision the timestamp.
  //------------------------------------------------------------------------
  bool secfract;

  size_t Now() const {
    struct timespec ts;
#ifdef __APPLE__
    struct timeval tv;
    gettimeofday(&tv, 0);
    ts.tv_sec = tv.tv_sec;
    ts.tv_nsec = tv.tv_usec * 1000;
#else
    clock_gettime(CLOCK_REALTIME, &ts);
#endif
    return 1e9*ts.tv_sec+ts.tv_nsec;
  }

  std::string IntTimeToStr(const size_t &d) const {
    char buffer[64];
    time_t sec=floor(d/(int)1e9);
    size_t nsec=size_t((d-sec)%(int)1e9);
    // Convert a time_t into a struct tm using your current time zone
    struct tm ptm;
    localtime_r(&sec,&ptm);
    size_t offset=strftime(buffer, 64, "%Y-%m-%d %H:%M:%S", &ptm);
    //if(secfract)  sprintf(buffer+offset,".%9.9ld",nsec);
    if(secfract)  sprintf(buffer+offset, "%9.9lu",nsec);
    return buffer;
  }
  //------------------------------------------------------------------------
  //! this function is used when closing a set sequence it flushes setseqlist to both 'map' and 'db'
  //! it returns the number of processed elements in the list
  //------------------------------------------------------------------------
  unsigned long ProcessSetSeqList() {
    unsigned long rc=(unsigned long)setseqlist.size();
    db->BeginTransaction();
    for(Tlist::iterator it=setseqlist.begin();it!=setseqlist.end();it++) {
      DoSet(it->first,it->second);
    }
    db->EndTransaction();
    return rc;
  }

  void DoSet(const Tkey& key, const std::string& value, const std::string& comment) {
    size_t d=Now();
    Tval val= {d,IntTimeToStr(d),1,"",value,comment};
    DoSet(key,val);
  }

  void DoSet(const size_t &time, const Tkey& key, const std::string& value, const std::string& comment) {
    size_t d=time;
    Tval val= {d,IntTimeToStr(d),1,"",value,comment};
    DoSet(key,val);
  }

  void DoSet(const Tkey& key, Tval& val) {
    Tmap::iterator it=map.find(key);
    if(it!=map.end()) {
      val.seqid=it->second.seqid+1;
      it->second=val;
      db->ChangeEntry(key,val);
    }
    else {
      db->InsertEntry(key,val);
      map[key]=val;
    }
    AtomicInc(setCounter);
  }

public:
  //------------------------------------------------------------------------
  /// capacity
  //------------------------------------------------------------------------
  bool Empty()	const { return map.empty();	}
  size_t Size()	const { return map.size(); 	}

  //------------------------------------------------------------------------
  /// counters
  //------------------------------------------------------------------------
  size_t GetReadCount() const		{ return getCounter; }
  size_t GetWriteCount() const	{ return setCounter; }

  //------------------------------------------------------------------------
  /// logging
  //------------------------------------------------------------------------

  //------------------------------------------------------------------------
  //! this function return true if the dbname was attached else it returns false (it maybe because the file is already attached)
  //------------------------------------------------------------------------
  bool AttachLog(const std::string &dbname,int sliceduration=-1, int createperm=0) {
    mutex.LockWrite();
    bool rc=db->AttachDbLog(dbname,sliceduration,createperm);
    mutex.UnLockWrite();
    return rc;
  }

  //------------------------------------------------------------------------
  //! this function return true if the dbname was attached else it returns false (it maybe because the file is already attached)
  //------------------------------------------------------------------------
  bool AttachLog(DbLogT<TDbMapInterface,TDbLogInterface>* dblog) {
    mutex.LockWrite();
    bool rc=db->AttachDbLog(dblog->db);
    mutex.UnLockWrite();
    return rc;
  }

  //------------------------------------------------------------------------
  //! this function return true if the dbname was detached else it returns false (it maybe because the file has already been attached)
  //------------------------------------------------------------------------
  bool DetachLog(const std::string &dbname) {
    mutex.LockWrite();
    bool rc=db->DetachDbLog(dbname);
    mutex.UnLockWrite();
    return rc;
  }

  //------------------------------------------------------------------------
  //! this function return true if the dbname was detached else it returns false (it maybe because the file has already been attached)
  //------------------------------------------------------------------------
  bool DetachLog(DbLogT<TDbMapInterface,TDbLogInterface>* dblog) {
    mutex.LockWrite();
    bool rc=db->DetachDbLog(dblog->db);
    mutex.UnLockWrite();
    return rc;
  }

  //------------------------------------------------------------------------
  /// constr , destr
  //------------------------------------------------------------------------
  DbMapT() : iterating(false), setsequence(false), setCounter(0), getCounter(0), secfract(false) {
    db = new TDbMapInterface();
    char buffer[32];
    sprintf(buffer,"dbmap%p",this);
    name = buffer;
    namesmutex.LockWrite();
    names.insert(name);
    namesmutex.UnLockWrite();
    db->SetName(name);
  }
  ~DbMapT() {
    namesmutex.LockWrite();
    names.erase(name);
    namesmutex.UnLockWrite();
    delete static_cast<TDbMapInterface*>(db);
  }

  //------------------------------------------------------------------------
  //! this function returns true if the name of the instance was successfully set. If the return value is false, the name is alreday given to another instance.
  //------------------------------------------------------------------------
  bool SetName(const std::string &name) {
    namesmutex.LockWrite();
    if(names.find(name)!=names.end()) {
      namesmutex.UnLockWrite();
      return false;
    }
    names.erase(this->name);
    names.insert(name);
    this->name=name;
    db->SetName(name);
    namesmutex.UnLockWrite();
    return true;
  }

  //------------------------------------------------------------------------
  //!
  //------------------------------------------------------------------------
  void SetFractionalSec(bool b) {
    secfract=b;
  }

  //------------------------------------------------------------------------
  //! this function begins a const_iteration. It blocks the access to all other thread as long the iteration is not over.
  //! the actual iteration are done by calling Iterate
  //! the iteration can be terminated using StopIter() or if it reaches the end.
  //------------------------------------------------------------------------
  void BeginIter() const {
    mutex.LockWrite(); // to prevent collisions in multiple threads iterating simultaneously
    it=map.begin();
    iterating=true;
  }

  //------------------------------------------------------------------------
  //! this function returns in its argumets the nex pair of (key,value) in the iteration
  //! it returns true if another iteration could be done then
  //! it returns false if the iteration reaches the end
  //------------------------------------------------------------------------
  bool Iterate(const Tkey** keyOut, const Tval **valOut) const {
    if(!iterating) return false;
    bool ret=(it!=map.end());
    if(ret) {
      *keyOut=&it->first;
      *valOut=&it->second;
      AtomicInc(getCounter);
      it++;
      return true;
    }
    else {
      StopIter();
      return false;
    }
  }

  //------------------------------------------------------------------------
  //! call this function to stop an ongoing iteration
  //! WARINING : if an ongoing iteration is not stopped, no readings or writings can be done oh the instance.
  //------------------------------------------------------------------------
  void StopIter() const {
    iterating=false;
    mutex.UnLockWrite();
  }

  //------------------------------------------------------------------------
  //! element access
  //! this function returns the number of entries buffered in the seqlist. If means 0 if the set sequence is not enabled.
  //------------------------------------------------------------------------
  unsigned long Set (const Tkey& key, const std::string& value, const std::string& comment) {
    mutex.LockWrite();
    if(setsequence) {
      Tval val;
      val.value=value;
      val.comment=comment;
      val.seqid=1;
      val.timestamp=Now();
      val.timestampstr=IntTimeToStr(val.timestamp);
      setseqlist.push_back( Tkeyval(key,val) );
      mutex.UnLockWrite();
      return setseqlist.size();
    }
    else {
      DoSet(key, value, comment);
      mutex.UnLockWrite();
      return 0;
    }
  }

  //------------------------------------------------------------------------
  //! element access
  //! this function returns the number of entries buffered in the seqlist. If means 0 if the set sequence is not enabled.
  //------------------------------------------------------------------------
  unsigned long Set( const size_t &time, const Tkey& key, const std::string& value, const std::string& comment) {
    mutex.LockWrite();
    if(setsequence) {
      Tval val;
      val.value=value;
      val.comment=comment;
      val.seqid=1;
      val.timestamp=time;
      val.timestampstr=IntTimeToStr(val.timestamp);
      setseqlist.push_back( Tkeyval(key,val) );
      mutex.UnLockWrite();
      return setseqlist.size();
    }
    else {
      DoSet(time, key, value, comment);
      mutex.UnLockWrite();
      return 0;
    }
  }

  //------------------------------------------------------------------------
  //! element access
  //! this function returns the number of entries buffered in the seqlist. If means 0 if the set sequence is not enabled.
  //------------------------------------------------------------------------
  unsigned long Set( const Tkey& key, const Tval& val) {
    mutex.LockWrite();
    if(setsequence) {
      setseqlist.push_back( Tkeyval(key,val) );
      mutex.UnLockWrite();
      return setseqlist.size();
    }
    else {
      Tval val2(val);
      DoSet(key,val2);
      mutex.UnLockWrite();
      return 0;
    }
  }

  //------------------------------------------------------------------------
  //! this function returns the number of elements actually removed from the map (0 or 1)
  //------------------------------------------------------------------------
  unsigned long Remove (const Tkey& key) {
    mutex.LockWrite();
    Tmap::iterator it=map.find(key);
    if(it!=map.end()) {
      db->RemoveEntry(key);
      map.erase(it);
      mutex.UnLockWrite();
      return 1;
    }
    mutex.UnLockWrite();
    return 0;
  }

  //------------------------------------------------------------------------
  //! this function return a pointer to the value associated with the key if there is some. It returns a null pointer otherwise.
  //------------------------------------------------------------------------
  const Tval* Get (const Tkey& key) const {
    mutex.LockRead();
    const Tval* retptr=NULL;
    Tmap::const_iterator it=map.find(key);
    if(it!=map.end()) retptr=&(it->second);
    AtomicInc(getCounter);
    mutex.UnLockRead();
    return retptr;
  }

  //------------------------------------------------------------------------
  /// caching
  //------------------------------------------------------------------------

  //------------------------------------------------------------------------
  //!  when using a set sequence all subsequent 'set' operations are cached into a list.
  //!  it can be filled by multiple thread which are actually serialized.
  //!  beware, the setsequence status of an instance can be changed from multiple threads.
  //------------------------------------------------------------------------
  void BeginSetSequence() const {
    mutex.LockWrite();
    setsequence=true;
    mutex.UnLockWrite();
  }

  //------------------------------------------------------------------------
  //! when a write sequence ends. All the cached 'set' operations are actually done both in the map and in the db.
  //! this function return the number of flushed set operations.
  //------------------------------------------------------------------------
  unsigned long EndSetSequence() {
    mutex.LockWrite();
    unsigned long ret=ProcessSetSeqList();
    setseqlist.clear();
    setsequence=false;
    mutex.UnLockWrite();
    return ret;
  }

  static std::string GetDbType() { return TDbMapInterface::GetDbType();}

};

/*----------------------------------------------------------------------------*/
//! this class is a logging container which store snapshots of entries of DbMap.
//! It provides some reading facilities.
//! Writing to this class must be done via the TDbMap class.
/*----------------------------------------------------------------------------*/
template<class TDbMapInterface, class TDbLogInterface> class DbLogT : public eos::common::LogId  {
  friend class DbMapT<TDbMapInterface,TDbLogInterface>;
  // db is a pointer to the db manager of the data
  // the TDmapInterface is not supposed to be thread-safe. So db must be protected by a lock.
  TDbLogInterface *db;
  mutable RWMutex mutex;
public:
  // typedefs
  typedef DbMapTypes::Tkey Tkey;
  typedef DbMapTypes::Tval Tval;
  typedef DbMapTypes::Tlogentry Tlogentry;
  typedef DbMapTypes::TlogentryVec TlogentryVec;
  typedef std::pair<Tkey,Tval> Tkeyval;
  typedef std::map<Tkey,Tval> Tmap;
  typedef std::vector<Tkeyval> Tlist;

  DbLogT() {
    db=new TDbLogInterface();
  }

  DbLogT(const std::string dbfile,int sliceduration=-1, int createperm=0) {
    db=new TDbLogInterface(dbfile,sliceduration, createperm);
  };
  ~DbLogT(){RWMutexWriteLock lock(mutex); delete db;}
  bool SetDbFile( const std::string &dbname, int sliceduration=-1, int createperm=0)	{
    RWMutexWriteLock lock(mutex);
    return db->SetDbFile(dbname,sliceduration, createperm);
  }
  bool IsOpen() {
    return db->IsOpen();
  }
  std::string GetDbFile() const {
    RWMutexWriteLock lock(mutex);
    return db->GetDbFile();
  }
  size_t GetAll( TlogentryVec &retvec, size_t nmax=0, Tlogentry *startafter=NULL ) const {
    RWMutexReadLock lock(mutex);
    return db->GetAll(retvec,nmax,startafter);
  }
  size_t GetByRegex( const std::string &regex, TlogentryVec &retvec , size_t nmax=0, Tlogentry *startafter=NULL ) const {
    RWMutexReadLock lock(mutex);
    return db->GetByRegex(regex,retvec,nmax,startafter);
  }

  size_t GetTail( int nentries, TlogentryVec &retvec ) const {
    RWMutexReadLock lock(mutex);
    return db->GetTail(nentries,retvec);
  }

  static std::string GetDbType() { return TDbLogInterface::GetDbType();}
};
/*----------------------------------------------------------------------------*/

/*-------------    DEFAULT DbMap and DbLog IMPLEMENTATIONS     ---------------*/
#ifdef EOS_SQLITE_DBMAP
typedef DbMapT<SqliteDbMapInterface,SqliteDbLogInterface> DbMap;
typedef DbLogT<SqliteDbMapInterface,SqliteDbLogInterface> DbLog;
#else
typedef DbMapT<LvDbDbMapInterface,LvDbDbLogInterface> DbMap;
typedef DbLogT<LvDbDbMapInterface,LvDbDbLogInterface> DbLog;
#endif
/*----------------------------------------------------------------------------*/

/*------------------------    DISPLAY HELPERS     ----------------------------*/
std::ostream& operator << (std::ostream &os, const DbMap &map );
std::ostream& operator << (std::ostream &os, const DbMapTypes::Tval &val );
std::istream& operator >> (std::istream &is, DbMapTypes::Tval &val );
std::ostream& operator << (std::ostream &os, const DbMapTypes::Tlogentry &entry );
std::ostream& operator << (std::ostream &os, const DbMapTypes::TlogentryVec &entryvec );
/*----------------------------------------------------------------------------*/

#ifndef EOS_SQLITE_DBMAP
/*-----------------------    CONVERSION HELPERS     --------------------------*/
DbMapT<SqliteDbMapInterface,SqliteDbLogInterface> DbMapLevelDb2Sqlite  (const DbMapT<LvDbDbMapInterface,LvDbDbLogInterface>&);
DbMapT<LvDbDbMapInterface,LvDbDbLogInterface>     DbMapSqlite2LevelDb  (const DbMapT<SqliteDbMapInterface,SqliteDbLogInterface>&);
bool                                              ConvertSqlite2LevelDb(const std::string &sqlpath, const std::string &lvdbpath, const std::string &sqlrename="");
bool                                              ConvertLevelDb2Sqlite(const std::string &lvdbpath, const std::string &sqlpath, const std::string &lvdbrename="");
/*----------------------------------------------------------------------------*/
#endif

EOSCOMMONNAMESPACE_END

#endif
