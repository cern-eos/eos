// ----------------------------------------------------------------------
// File: DbMapCommon.hh
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
 * @file   DbMapCommon.hh
 *
 * @brief  Auxiliary classes and Interfaces for DbMap and DbLoh classes defined in DbMap.hh
 *
 */

#ifndef __EOSCOMMON_DBMAP_COMMON_HH__
#define __EOSCOMMON_DBMAP_COMMON_HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/Logging.hh"
#include "common/RWMutex.hh"
#include "common/stringencoders/modp_numtoa.h"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysAtomics.hh"
#ifndef EOS_SQLITE_DBMAP
#include "leveldb/db.h"
#endif
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <pthread.h>
#include <time.h>
#include <vector>
#include <set>
#include <map>
#include <cmath>
#include <algorithm>
/*----------------------------------------------------------------------------*/

// to get rid of the unused variable warning
#define _unused(x) ((void)x)

// The in memory map of DbMap can be implemented using
// std::map or google dense_hash_map
// By default Google's dense_hash_map is used. If the following macro is defined, then std::map is used instead.
// #define EOS_STDMAP_DBMAP
// By default, the LEVELDB implementation is used if LevelDb is available
// define this macro to enable the SQLITE3 implementation
// #define EOS_SQLITE_DBMAP
EOSCOMMONNAMESPACE_BEGIN

inline bool operator<(const timespec &t1, const timespec &t2)
{ return (t1.tv_sec)<(t2.tv_sec);}
inline bool operator<=(const timespec &t1, const timespec &t2)
{ return (t1.tv_sec)<=(t2.tv_sec);}

/*----------------------------------------------------------------------------*/
//! This class provides wrapper for copy-less C++ const strings
/*----------------------------------------------------------------------------*/
class Slice
{
public:
  /*----------------------------------------------------------------------------*/
  //! Create an empty slice.
  /*----------------------------------------------------------------------------*/
  Slice() : data_(""), size_(0)
  {}

  /*----------------------------------------------------------------------------*/
  //! Create a slice that refers to d[0,n-1].
  /*----------------------------------------------------------------------------*/
  Slice(const char* d, size_t n) : data_(d), size_(n)
  {}

  /*----------------------------------------------------------------------------*/
  //! Create a slice that refers to the contents of "s"
  /*----------------------------------------------------------------------------*/
  Slice(const std::string& s) : data_(s.data()), size_(s.size())
  {}

  /*----------------------------------------------------------------------------*/
  //! Create a slice that refers to s[0,strlen(s)-1]
  /*----------------------------------------------------------------------------*/
  Slice(const char* s) : data_(s), size_(strlen(s))
  {}
#ifndef EOS_SQLITE_DBMAP
  operator leveldb::Slice() const
  { return leveldb::Slice(data_,size_);}
#endif

  Slice(const leveldb::Slice &slice) : data_(slice.data()) , size_(slice.size())
  {}

  /*----------------------------------------------------------------------------*/
  //! Return a pointer to the beginning of the referenced data
  /*----------------------------------------------------------------------------*/
  const char* data() const
  { return data_;}

  /*----------------------------------------------------------------------------*/
  //! Return the length (in bytes) of the referenced data
  /*----------------------------------------------------------------------------*/
  size_t size() const
  { return size_;}

  /*----------------------------------------------------------------------------*/
  //! Return true iff the length of the referenced data is zero
  /*----------------------------------------------------------------------------*/
  bool empty() const
  { return size_ == 0;}

  /*----------------------------------------------------------------------------*/
  //! Return the ith byte in the referenced data.
  //! REQUIRES: n < size()
  /*----------------------------------------------------------------------------*/
  char operator[](size_t n) const
  {
    assert(n < size());
    return data_[n];
  }

  /*----------------------------------------------------------------------------*/
  //! Change this slice to refer to an empty array
  /*----------------------------------------------------------------------------*/
  void clear()
  { data_ = ""; size_ = 0;}

  /*----------------------------------------------------------------------------*/
  //! Drop the first "n" bytes from this slice.
  /*----------------------------------------------------------------------------*/
  void remove_prefix(size_t n)
  {
    assert(n <= size());
    data_ += n;
    size_ -= n;
  }

  /*----------------------------------------------------------------------------*/
  //! Return a string that contains the copy of the referenced data.
  /*----------------------------------------------------------------------------*/
  std::string ToString() const
  { return std::string(data_, size_);}

  /*----------------------------------------------------------------------------*/
  //! Three-way comparison.
  //! @return
  //!   <  0 iff "*this" <  "b",
  //!   == 0 iff "*this" == "b",
  //!   >  0 iff "*this" >  "b"
  /*----------------------------------------------------------------------------*/
  int compare(const Slice& b) const;

  /*----------------------------------------------------------------------------*/
  //! Return true iff "x" is a prefix of "*this"
  /*----------------------------------------------------------------------------*/
  bool starts_with(const Slice& x) const
  {
    return ((size_ >= x.size_) &&
        (memcmp(data_, x.data_, x.size_) == 0));
  }

private:
  const char* data_;
  size_t size_;

  // Intentionally copyable
};

inline bool operator==(const Slice& x, const Slice& y)
{
  return ((x.size() == y.size()) &&
      (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice& x, const Slice& y)
{
  return !(x == y);
}

inline int Slice::compare(const Slice& b) const
{
  const int min_len = (size_ < b.size_) ? size_ : b.size_;
  int r = memcmp(data_, b.data_, min_len);
  if (r == 0)
  {
    if (size_ < b.size_) r = -1;
    else if (size_ > b.size_) r = +1;
  }
  return r;
}

inline std::string& operator += (std::string &lhs, const Slice& rhs)
{
  size_t srhs=rhs.size();
  return lhs.append((char*)&srhs,sizeof(size_t)).append(rhs.data(),rhs.size());
}

/*----------------------------------------------------------------------------*/
//! Extract a Slice encoded under the form size,content out of another Slice
//! @param[in] slice
//!   The slice to extract the subslice from
//! @param[in,out] pos
//!   The position at which the subslice to extract is.
//!   At the end of the call, the content of this ptr is modified to point to
//!   first character after the subslice.
//! @param[out] extracted
//!   A ptr to Slice to store the subslice.
//! @return true if a subslice has been extracted.
/*----------------------------------------------------------------------------*/
inline bool ExtractSliceFromSlice( const Slice& slice, size_t *pos, Slice* extracted)
{
  if(*pos+sizeof(size_t)>slice.size()) return false; // not enough data to read the size
  const size_t &size=*(const size_t*)(slice.data()+*pos);
  *pos+=sizeof(size_t);
  if(*pos+size>slice.size()) return false;// not enough data to fit in the content
  *extracted=Slice(slice.data()+*pos,size);
  *pos+=size;
  return true;// Slice successfully read
}

/*-------------------------  DATA TYPES  --------------------------------*/
/*----------------------------------------------------------------------------*/
//! Class provides the data types for the DbMap implementation
/*----------------------------------------------------------------------------*/
class DbMapTypes
{
public:
  typedef std::string Tkey;
  struct Tlogentry
  {
    std::string timestampstr;
    std::string seqid;
    std::string writer; // each map has a unique name. This name is reported for each entry in the log.
    std::string key;
    std::string value;
    std::string comment;
  };
  struct Tval
  {
    std::string timestampstr;
    unsigned long seqid;
    std::string writer;
    std::string value;
    std::string comment;
  };
  struct TvalSlice
  {
    Slice timestampstr;
    unsigned long seqid;
    Slice writer;
    Slice value;
    Slice comment;

    TvalSlice(const Tval &slice) :
    timestampstr(slice.timestampstr),
    seqid(slice.seqid),
    writer(slice.writer),
    value(slice.value),
    comment(slice.comment)
    {
    }

    TvalSlice(
        const Slice &_timestampstr,
        const unsigned long &_seqid,
        const Slice & _writer,
        const Slice & _value,
        const Slice & _comment
    ) :
    timestampstr(_timestampstr),
    seqid(_seqid),
    writer(_writer),
    value(_value),
    comment(_comment)
    {}

    operator Tval() const
    {
      return
      { timestampstr.ToString(),seqid,writer.ToString(),value.ToString(),comment.ToString()};
    }
  };
  typedef std::vector<Tlogentry> TlogentryVec;

  struct TimeSpecComparator
  {
    inline bool operator() (const timespec &t1, const timespec &t2) const
    { return t1<t2;};
  };
};
bool operator == ( const DbMapTypes::Tlogentry &l, const DbMapTypes::Tlogentry &r);
bool operator == ( const DbMapTypes::Tval &l, const DbMapTypes::Tval &r);
void Tlogentry2Tval(const DbMapTypes::Tlogentry &tle, DbMapTypes::Tval *tval);
/*----------------------------------------------------------------------------*/

/*------------------------    DISPLAY HELPERS     ----------------------------*/
std::ostream& operator << (std::ostream &os, const DbMapTypes::Tval &val );
std::istream& operator >> (std::istream &is, DbMapTypes::Tval &val );
std::ostream& operator << (std::ostream &os, const DbMapTypes::Tlogentry &entry );
std::ostream& operator << (std::ostream &os, const DbMapTypes::TlogentryVec &entryvec );
/*----------------------------------------------------------------------------*/

void TimeToStr(const time_t t, char *tstr );

/*------------------------    INTERFACES     ---------------------------------*/
// The following abstract classes are interface to the db representation for the DbMap and DbLog class. These classes work as a pair and should be implemented as is.
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
//! This class provides an interface to implement the necessary functions for the representation in a DB of the DbLog class
//! This should implement an in-memory five columns table. Each column should contain a string (not a null terminated)
//! The names of the column are timestampstr, logid, key, value, comment. There must be an uniqueness constraint on the timestamp (which should also be the primary key).
/*----------------------------------------------------------------------------*/
class DbLogInterface
{
public:
  typedef DbMapTypes::Tkey Tkey;
  typedef DbMapTypes::Tval Tval;
  typedef DbMapTypes::TvalSlice TvalSlice;
  typedef DbMapTypes::Tlogentry Tlogentry;
  typedef DbMapTypes::TlogentryVec TlogentryVec;
  // ------------------------------------------------------------------------
  //! Change the file of the underlying log db
  //! volume splitting feature should also be provided splitting the db representation into chunks of the given duration
  //! @param[in] dbname the name of the new db file name
  //! @param[in] volumeduration the duration in seconds of each volume of the dblog (if set to -1, no splitting is processed)
  //! @param[in] createperm the unix permission in case the file is to be created.
  //! @param[in] option a pointer to a structure holding some parameters for the underlying db
  //! @return true if success, false otherwise
  // ------------------------------------------------------------------------
  virtual bool setDbFile( const std::string &dbname, int volumeduration, int createperm, void* option)=0;

  // ------------------------------------------------------------------------
  //! Check if the log db is properly opened
  //! @return true if the db is open, false otherwise
  // ------------------------------------------------------------------------
  virtual bool isOpen() const=0;

  // ------------------------------------------------------------------------
  //! this function should get the name of the db file of the db representation
  //! @return the name of the db file
  // ------------------------------------------------------------------------
  virtual std::string getDbFile() const =0;

  // ------------------------------------------------------------------------
  //! Get all the entries of the db representation optionally by block
  //! @param[out] retvec a pointer to a vector of entries to which the result of the operation will be appended
  //! @param[in] nmax maximum number of elements to get at once
  //! @param[in,out] startafter before execution : position at which the search is to be started
  //!                           after  execution : position at which the search has to start at the next iteration
  //! @return the number of entries appended to the result vector retvec
  // ------------------------------------------------------------------------
  virtual size_t getAll( TlogentryVec *retvec, size_t nmax=0, Tlogentry *startafter=NULL ) const =0;

  // ------------------------------------------------------------------------
  //! Get the latest entries of the db representation
  //! @param[out] retvec a pointer to a vector of entries to which the result of the operation will be appended
  //! @param[in] nentries maximum number of elements to get starting from the end
  //! @return the number of entries appended to the result vector retvec
  // ------------------------------------------------------------------------
  virtual size_t getTail( int nentries, TlogentryVec *retvec) const=0;

  // ------------------------------------------------------------------------
  //! Clear the db representation
  //! @return false if an error occurs, true otherwise
  // ------------------------------------------------------------------------
  virtual bool clear() =0;

  virtual ~DbLogInterface()
  {}
};

/*----------------------------------------------------------------------------*/
//! this class provides an interface to implement the necessary functions for the representation in a DB of the DbMap class
//! this should implement an in-memory five columns table. Each column should contain string
//! the names of the column are timestampstr, logid, key, value, comment. There must be an uniqueness constraint on the key (which should also be the primary key)
/*----------------------------------------------------------------------------*/
class DbMapInterface
{
protected:
public:
  typedef DbMapTypes::Tkey Tkey;
  typedef DbMapTypes::Tval Tval;
  typedef DbMapTypes::TvalSlice TvalSlice;
  typedef DbMapTypes::Tlogentry Tlogentry;
  typedef DbMapTypes::TlogentryVec TlogentryVec;

  // ------------------------------------------------------------------------
  //! this function should set the default writer name of the db
  //! @param[in] name the default writer name
  // ------------------------------------------------------------------------
  virtual void setName( const std::string &name)=0;

  // ------------------------------------------------------------------------
  //! this function should get the default writer name of the db
  //! @return the default writer name
  virtual const std::string & getName() const =0;

  // Accessors
  // ------------------------------------------------------------------------
  //! Get the value associated with a key.
  //! Note that no value is stored inside the db if AttachDb has not been used beforehand.
  //! @param[in] key the key of which the value is to be retrieved
  //! @param[out] val a pointer to a struct where the full value is to be written
  //! @return true if a value was retrieved, false otherwise
  // ------------------------------------------------------------------------
  virtual bool getEntry(const Slice &key, Tval *val)=0;

  // ------------------------------------------------------------------------
  //! Set a Key / full Value
  //! @param[in] key the key
  //! @param[in] val the full value struct
  //! @return true if the write is su
  //!         -1 if an error occurs
  // ------------------------------------------------------------------------
  virtual bool setEntry(const Slice &key, const TvalSlice &val)=0;

  // ------------------------------------------------------------------------
  //! Remove the entry associated to the key
  //! Note that a value is provide to get the writer value to log the action
  //! @param[in] key the key
  //! @param[in] val the full value struct
  //! @return true if the write is successful
  //!         -1 if an error occurs
  // ------------------------------------------------------------------------
  virtual bool removeEntry(const Slice &key, const TvalSlice &val)=0;

  // ------------------------------------------------------------------------
  //! Clear the content db
  //! @return false if an error occurs, true otherwise
  // ------------------------------------------------------------------------
  virtual bool clear()=0;

  // ------------------------------------------------------------------------
  //! Get the number of entries in the content db
  //! @return false if an error occurs, true otherwise
  // ------------------------------------------------------------------------
  virtual size_t size() const=0;

  // ------------------------------------------------------------------------
  //! Get the number of entries matching a key into the
  //! @return the number of corresponding entries (should be 0 or 1)
  // ------------------------------------------------------------------------
  virtual size_t count(const Slice &key) const=0;

  // Requestors

  // ------------------------------------------------------------------------
  //! Get all the entries of the content db optionally by block
  //! @param[out] retvec a pointer to a vector of entries to which the result of the operation will be appended
  //! @param[in] nmax maximum number of elements to get at once
  //! @param[in,out] startafter before execution : position at which the search is to be started
  //!                           after  execution : position at which the search has to start at the next iteration
  //! @return the number of entries appended to the result vector retvec
  // ------------------------------------------------------------------------
  virtual size_t getAll( TlogentryVec *retvec, size_t nmax=0, Tlogentry *startafter=NULL ) const =0;

  // Atomicity

  // ------------------------------------------------------------------------
  //! Start a transaction.
  //! @return true is successful, false otherwise
  // ------------------------------------------------------------------------
  virtual bool beginTransaction()=0;

  // ------------------------------------------------------------------------
  //! End and commit a transaction.
  //! @return true is successful, false otherwise
  // ------------------------------------------------------------------------
  virtual bool endTransaction()=0;

  // Data Content Persistency

  // ------------------------------------------------------------------------
  //! Attach a content db
  //! When using out-of-core, this function should be used first.
  //! At most one db can be attached to a given instance of a DbMap.
  //! If a db is already attached or if an error occurs, the return value is false.
  //! @param[in] dbname the file name of the db to be attached
  //! @param[in] repair if true, a repair will attempted if opening the db fails
  //! @param[in] create createperm UNIX permission flag in case the file would have to be created
  //! @param[in] option a pointer to a struct containing parameters for the underlying db
  //! @return true if the attach is successful. false otherwise.
  // ------------------------------------------------------------------------
  virtual bool attachDb(const std::string &dbname, bool repair=false, int createperm=0, void* option=NULL)=0;

  // ------------------------------------------------------------------------
  //! Consolidate the content db.
  //! @return true if the operation succeeded. False otherwise.
  // ------------------------------------------------------------------------
  virtual bool trimDb()=0;

  // ------------------------------------------------------------------------
  //! Get the name of the attached content db
  //! @return the name of the attached content db file name
  // ------------------------------------------------------------------------
  virtual std::string getAttachedDbName() const =0;

  // ------------------------------------------------------------------------
  //! Copy the content of the content db to an in memory map.
  //! @return true if the operation succeeded, false otherwise
  // ------------------------------------------------------------------------
#ifdef EOS_STDMAP_DBMAP
  virtual bool syncFromDb(std::map<Tkey,Tval> *map)=0;
#else
  virtual bool syncFromDb(::google::dense_hash_map<Tkey,Tval> *map)=0;
#endif

  // ------------------------------------------------------------------------
  //! Detach the content db
  //! @return true if the operation succeeded, false otherwise
  // ------------------------------------------------------------------------
  virtual bool detachDb()=0;

  // Change Log

  // ------------------------------------------------------------------------
  //! Attach a log db.
  //! all operations are logged to the db. The name of the DbMap is refered as writer in the log.
  //! Multiple log db can be attached
  //! A db log can attached in several instances
  //! @param[in] dbname the file name of the db to be attached
  //! @param[in] volumeduration the duration in seconds of each volume of the dblog (if set to -1, no splitting is processed)
  //! @param[in] createperm the unix permission in case the file is to be created.
  //! @param[in] option a pointer to a structure holding some parameters for the underlying db.
  //! @return true if the dbname was attached, false otherwise (it maybe because the file is already attached to this DbMap)
  // ------------------------------------------------------------------------
  virtual bool attachDbLog(const std::string &dbname, int volumeduration, int createperm, void *option)=0;

  // ------------------------------------------------------------------------
  //! Detach a log db
  //! @param[in] dbname the file name of the db to be detached
  //! @return true if the operation succeeded, false otherwise
  // ------------------------------------------------------------------------
  virtual bool detachDbLog(const std::string &dbname)=0;

  // ------------------------------------------------------------------------
  //! Attach a log to the DbMap
  //! @param[in] dblog an existing DbLogInterface. This DbLogInterface instance is not destroyed when the DbMapInterface is destroyed.
  //! @return true if the dbname was attached, false otherwise (it maybe because the file is already attached to this DbMapInterface)
  // ------------------------------------------------------------------------
  virtual bool attachDbLog(DbLogInterface *dblogint)=0;

  // ------------------------------------------------------------------------
  //! Detach a log db
  //! @param[in] dblog an existing DbLogInterface. This DbLog instance is not destroyed.
  //! @return true if the operation succeeded, false otherwise
  // ------------------------------------------------------------------------
  virtual bool detachDbLog(DbLogInterface *dblogint)=0;

  virtual ~DbMapInterface()
  {}
};
/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END
#endif
