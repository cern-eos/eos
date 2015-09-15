// ----------------------------------------------------------------------
// File: DbMap.cc
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
#include <google/dense_hash_map>
#include "common/DbMap.hh"
#include "common/Namespace.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <sys/stat.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <fts.h>
#include <iostream>
#include <sstream>
#include <ostream>
#include <istream>

using namespace std;
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN
/*-------------------- EXPLICIT INSTANTIATIONS -------------------------------*/
template class DbMapT<SqliteDbMapInterface, SqliteDbLogInterface>;
template class DbLogT<SqliteDbMapInterface, SqliteDbLogInterface>;
#ifndef EOS_SQLITE_DBMAP
template class DbMapT<LvDbDbMapInterface, LvDbDbLogInterface>;
template class DbLogT<LvDbDbMapInterface, LvDbDbLogInterface>;
#endif
/*----------------------------------------------------------------------------*/

/*-------------- IMPLEMENTATIONS OF STATIC MEMBER VARIABLES ------------------*/
template<class A, class B> set<string> DbMapT<A, B>::gNames;
template<class A, class B> eos::common::RWMutex DbMapT<A, B>::gNamesMutex;
template<class A, class B> eos::common::RWMutex DbMapT<A, B>::gTimeMutex;
template<class A, class B> bool DbMapT<A, B>::gInitialized = false;
template<class A, class B> size_t DbMapT<A, B>::pDbIterationChunkSize=10000;

/*----------------------------------------------------------------------------*/

#ifndef EOS_SQLITE_DBMAP
typedef DbMapT<SqliteDbMapInterface, SqliteDbLogInterface> DbMapSqlite;
typedef DbMapT<LvDbDbMapInterface, LvDbDbLogInterface> DbMapLeveldb;
typedef DbLogT<SqliteDbLogInterface, SqliteDbLogInterface> DbLogSqlite;
typedef DbLogT<LvDbDbLogInterface, LvDbDbLogInterface> DbLogLeveldb;

DbMapSqlite
DbMapLevelDb2Sqlite (const DbMapLeveldb& toconvert)
{
  DbMapSqlite converted;
  const DbMapLeveldb::Tkey *key = 0;
  const DbMapLeveldb::Tval *val = 0;
  converted.beginSetSequence();
  toconvert.beginIter();
  while (toconvert.iterate(&key, &val))
  converted.set(*(DbMapSqlite::Tkey*)key, *(DbMapSqlite::Tval*)val);
  converted.endSetSequence();
  return converted;
}

DbMapLeveldb
DbMapSqlite2LevelDb (const DbMapSqlite& toconvert)
{
  DbMapLeveldb converted;
  const DbMapSqlite::Tkey *key = 0;
  const DbMapSqlite::Tval *val = 0;
  converted.beginSetSequence();
  toconvert.beginIter();
  while (toconvert.iterate(&key, &val))
  converted.set(*(DbMapLeveldb::Tkey*)key, *(DbMapLeveldb::Tval*)val);
  converted.endSetSequence();
  return converted;
}

bool
ConvertSqlite2LevelDb (const std::string &sqlpath, const std::string &lvdbpath, const std::string &sqlrename)
{
  const int blocksize = 1e5;
  // if the source and target file have the same name a proper renaming is required for the source
  if ((!sqlpath.compare(lvdbpath)) && (sqlrename.empty() || (!sqlrename.compare(sqlpath))))
    return false;

  // stat the file to copy the permission
  struct stat st;
  if (stat(sqlpath.c_str(), &st)) return false; // cannot stat source file

  if (!sqlrename.empty())
    if (rename(sqlpath.c_str(), sqlrename.c_str())) return false; // cannot rename the source file

  DbLogSqlite sqdbl(sqlrename.empty() ? sqlpath : sqlrename);
  if (!sqdbl.isOpen())
  { // cannot read the source file
    if (!sqlrename.empty()) rename(sqlrename.c_str(), sqlpath.c_str()); // revert the source renaming
    return false;
  }

  DbMapLeveldb lvdbm;
  if (!lvdbm.attachLog(lvdbpath, 0, st.st_mode))
  {
    if (!sqlrename.empty()) rename(sqlrename.c_str(), sqlpath.c_str()); // revert the source renaming
    return false; // cannot open the target file
  }

  DbLogSqlite::TlogentryVec sqentryvec;
  DbLogSqlite::Tlogentry sqentry;
  lvdbm.beginSetSequence();
  while (sqdbl.getAll(&sqentryvec, blocksize, &sqentry))
  {
    DbMapTypes::Tval tval;
    for (DbLogSqlite::TlogentryVec::iterator it = sqentryvec.begin(); it != sqentryvec.end(); it++)
    {
      Tlogentry2Tval(*it,&tval);
      lvdbm.set(it->key, tval); // keep the original timestamp
    }
    sqentryvec.clear();
  }
  lvdbm.endSetSequence();
  return true;
}

bool
ConvertLevelDb2Sqlite (const std::string &lvdbpath, const std::string &sqlpath, const std::string &lvdbrename)
{
  const int blocksize = 1e5;
  // if the source and target file have the same name a proper renaming is required for the source
  if ((!lvdbpath.compare(sqlpath)) && (lvdbrename.empty() || (!lvdbrename.compare(lvdbpath))))
    return false;
  // stat the file to copy the permission
  struct stat st;
  if (stat(lvdbpath.c_str(), &st)) return false; // cannot stat source file

  if (!lvdbrename.empty())
    if (rename(lvdbpath.c_str(), lvdbrename.c_str())) return false; // cannot rename the source file

  DbLogLeveldb lvdbl(lvdbrename.empty() ? lvdbpath : lvdbrename);
  if (!lvdbl.isOpen())
  { // cannot read the source file
    if (!lvdbrename.empty()) rename(lvdbrename.c_str(), sqlpath.c_str()); // revert the source renaming
    return false;
  }

  DbMapSqlite sqdbm;
  if (!sqdbm.attachLog(sqlpath, 0, st.st_mode & ~0111))
  { // forget the executable mode related to directories
    if (!lvdbrename.empty()) rename(lvdbrename.c_str(), lvdbpath.c_str()); // revert the source renaming
    return false; // cannot open the target file
  }

  DbLogLeveldb::TlogentryVec lventryvec;
  DbLogLeveldb::Tlogentry lventry;
  sqdbm.beginSetSequence();
  while (lvdbl.getAll(&lventryvec, blocksize, &lventry))
  {
    DbMapTypes::Tval tval;
    for (DbLogLeveldb::TlogentryVec::iterator it = lventryvec.begin(); it != lventryvec.end(); it++)
{
      Tlogentry2Tval(*it,&tval);
      sqdbm.set(it->key, tval); // keep the original metadata
  }
    lventryvec.clear();
}
  sqdbm.endSetSequence();
  return true;
}
#endif

EOSCOMMONNAMESPACE_END

