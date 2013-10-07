// ----------------------------------------------------------------------
// File: DbMapLevelDb.hh
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
 * @file   DbMapLevelDb.hh
 *
 * @brief  DbMap/DbLog interfaces for google's leveldb
 *
 */

#if !defined( __EOSCOMMON_DBMAP_LEVELDB_HH__ ) && !defined( EOS_SQLITE_DBMAP )
#define __EOSCOMMON_DBMAP_LEVELDB_HH__

/*----------------------------------------------------------------------------*/
#include "common/DbMapCommon.hh"
#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "leveldb/filter_policy.h"
#include "leveldb/cache.h"
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

/*-----------------    LEVELDB INTERFACE IMPLEMENTATION     -------------------*/
//! Here follows the implementation of the DbMap/DbLog interfaces for LevelDb
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
//! this base class provides some LevelDb helpers to implement the LevelDb interfaces
//! the db is accessed using via a unique leveldb::DB which is supposed to be thread-safely implemented in leveldb.
//! each transaction (batch in LevelDb words) is instantiated as a leveldb::batch object which is NOT thread-safe
//! Contrary to the sqlite3 implementation, the DbMap does NOT have a representation into the DB library.
//! Although the function to maintain such a representation are provided, they actually do NOTHING in leveldb
//! The actual interaction with leveldb is in the DbLog interface.
/*----------------------------------------------------------------------------*/
class LvDbInterfaceBase : public eos::common::LogId
{
protected:
  friend void _testLvDbError_(const leveldb::Status &s, void* _this, const char* __file, int __line);
  static bool pAbortOnLvDbError;
  static bool pDebugMode;
  static unsigned pNInstances;
  leveldb::Options pOptions;
public :
  static void setDebugMode(bool on)
  { pDebugMode=on;}
  static void setAbortOnLvDbError(bool b)
  { pAbortOnLvDbError=b;}
  struct Option
  {
    size_t BloomFilterNbits;
    size_t CacheSizeMb;
  };

  LvDbInterfaceBase()
  {
  }
protected:
  static Option gDefaultOption;
};
/*----------------------------------------------------------------------------*/
//! This function looks for leveldb error and prints it if some is found
/*----------------------------------------------------------------------------*/
inline void _testLvDbError_(const leveldb::Status &s, void* _this, const char* __file, int __line)
{
  if(LvDbInterfaceBase::pAbortOnLvDbError && !s.ok() )
  {
    //fprintf(stderr," LevelDb Error in %s at line %d involving object %p : %s\n",__file,__line,_this,s.ToString().c_str());
    eos_static_emerg(" LevelDb Error in %s at line %d involving object %p : %s\n",__file,__line,_this,s.ToString().c_str());
    //exit(1);
    abort();
  }
}
#define TestLvDbError(s,_this) _testLvDbError_(s,_this,__FILE__,__LINE__);
/*----------------------------------------------------------------------------*/
//! This class implements the DbLogInterface using leveldb
/*----------------------------------------------------------------------------*/
class LvDbDbLogInterface : public LvDbInterfaceBase, public DbLogInterface
{
public:
  // TODO remove the BloomFilter useless for DbLog use case
  typedef std::pair<leveldb::DB*,leveldb::FilterPolicy*> DbAndFilter;
  typedef std::pair<DbAndFilter,int> tCountedDbAndFilter;
  typedef std::map<std::string,tCountedDbAndFilter> tMapFileToDb; // filename -> attach-name, number of DbLogInterfaces
  typedef std::pair<std::string,int> tPeriodedFile;
  typedef std::multimap<timespec,tPeriodedFile,DbMapTypes::TimeSpecComparator> tTimeToPeriodedFile;// next update -> filename,
private:
  friend class LvDbDbMapInterface;

  // management of the uniqueness of the db attachments
  static XrdSysMutex gUniqMutex;
  static tMapFileToDb gFile2Db;
  static std::set<int> gIdPool;
  //std::string db;
  leveldb::DB *pDb;

  // archiving management
  static tTimeToPeriodedFile gArchQueue;
  static pthread_t gArchThread;
  static bool gArchThreadStarted;
  static void archiveThreadCleanup(void *dummy=NULL);
  static void* archiveThread(void*dummy=NULL);
  static XrdSysCondVar gArchmutex;
  static pthread_cond_t gArchCond;
  static int archive(const tTimeToPeriodedFile::iterator &entry );
  static int updateArchiveSchedule(const tTimeToPeriodedFile::iterator &entry);

  std::string pDbName;
  bool pIsOpen;

  void init();
  LvDbDbLogInterface(const LvDbDbLogInterface &);
public:
  typedef enum
  { testly=10, hourly=3600, daily=3600*24, weekly=3600*24*7}period;
  LvDbDbLogInterface();
  LvDbDbLogInterface(const std::string &dbname, int volumeduration, int createperm, void* options=NULL);
  virtual bool setDbFile( const std::string &dbname, int volumeduration, int createperm, void* option);
  virtual bool isOpen() const;
  virtual std::string getDbFile() const;
  static std::string getDbType()
  { return "LevelDB";}
  virtual size_t getAll( TlogentryVec *retvec , size_t nmax=0, Tlogentry *startafter=NULL ) const;
  virtual size_t getTail( int nentries, TlogentryVec *retvec ) const;
  virtual bool clear();
  static int setArchivingPeriod(const std::string &dbname, int volume);
  virtual ~LvDbDbLogInterface();
};

/*----------------------------------------------------------------------------*/
//! This class implements the DbMapInterface using LvDb
//! Note that the map doesn't have a LevelDb representation in memory
/*----------------------------------------------------------------------------*/
class LvDbDbMapInterface : public LvDbInterfaceBase, public DbMapInterface
{
  friend class LvDbDbLogInterface;
  size_t pNDbEntries;

  std::string pName;

  RWMutex pBatchMutex;
  leveldb::WriteBatch pExportBatch;
  leveldb::WriteBatch pDbBatch;
  bool pBatched;

  typedef std::pair<LvDbDbLogInterface*,bool> tOwnedLDLIptr; // pointer, ownit
  std::map<std::string,tOwnedLDLIptr> pAttachedDbs;

  std::string pAttachedDbname;
  leveldb::DB *AttachedDb;

  LvDbDbMapInterface(const LvDbDbMapInterface &);
public:
  LvDbDbMapInterface();
  virtual void setName( const std::string &name);
  static std::string getDbType()
  { return "LevelDB";}
  virtual const std::string & getName() const;
  virtual bool beginTransaction();
  virtual bool endTransaction();
  virtual bool getEntry(const Slice &key, Tval *val);
  virtual bool setEntry(const Slice &key, const TvalSlice &val);
  virtual bool removeEntry(const Slice &key, const TvalSlice &val);
  virtual bool clear();
  virtual size_t size() const;
  void rebuildSize();
  virtual size_t count(const Slice &key) const;
  // Persistent
  virtual bool attachDb(const std::string &dbname, bool repair=false, int createperm=0, void *option=NULL);
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

  virtual bool attachDbLog(const std::string &dbname,int volumeduration, int createperm, void*option);
  virtual bool detachDbLog(const std::string &dbname);
  virtual bool attachDbLog(DbLogInterface *dbname);
  virtual bool detachDbLog(DbLogInterface *dbname);
  virtual ~LvDbDbMapInterface();
};
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_END
#endif
