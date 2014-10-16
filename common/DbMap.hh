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
 * @brief  Classes for a key-value container stored into an underlying DB/key-value store.
 *         Especially, changes issued to this container can be logged to a db file.
 *         The default underlying DB is leveldb from google.
 *         If the EOS_SQLITE_DBMAP is defined, then SQLITE3 is used instead
 *         Note that the SQLITE3 implementation is roughly 10 times slower for write operations
 *
 */
/*----------------------------------------------------------------------------*/
#include "common/DbMapCommon.hh"
#include "common/DbMapLevelDb.hh"
#include "common/DbMapSqlite.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#include <regex.h>
/*----------------------------------------------------------------------------*/

#ifndef __EOSCOMMON_DBMAP_HH__
#define __EOSCOMMON_DBMAP_HH__
EOSCOMMONNAMESPACE_BEGIN

class RegexBranch
{
public:
  RegexBranch()
  {
    pContent=new Content;
    pContent->ptrcount=1;
  }

  /*----------------------------------------------------------------------------*/
  //! Construct a branch out of a variable name and a pattern
  //!
  //! If any value of these two arguments is wrong, HasError return true
  /*----------------------------------------------------------------------------*/
  RegexBranch(const std::string &variable, const std::string &pattern)
  {
    pContent=new Content;
    pContent->ptrcount=1;
    pContent->op=Content::op_expr;
    pContent->pattern=pattern;
    re_set_syntax(RE_SYNTAX_POSIX_EGREP);
    pContent->buffer=new re_pattern_buffer;
    memset(pContent->buffer, 0, sizeof (*pContent->buffer));
    if(re_compile_pattern(pattern.data(), pattern.size(), pContent->buffer))
    {
      //eos_err("DbMap: Error Compiling Regex");
      pContent->op=Content::op_error;
    }

    if(variable=="key") pContent->var=Content::var_key;
    else if(variable=="value") pContent->var=Content::var_val;
    else if(variable=="writer") pContent->var=Content::var_writer;
    else if(variable=="seqid") pContent->var=Content::var_seqid;
    else if(variable=="comment") pContent->var=Content::var_comment;
    else if(variable=="timestampstr") pContent->var=Content::var_tstampstr;
    else pContent->op=Content::op_error;
  }

  ~RegexBranch()
  {
    clear();
  }

  RegexBranch( const RegexBranch &rb) : pContent(rb.pContent)
  {
    pContent->ptrcount++;
  }

  /*----------------------------------------------------------------------------*/
  //! Assignment operator
  //!
  //! Note that no content is copied, it uses a smart pointer mechanism
  /*----------------------------------------------------------------------------*/
  RegexBranch& operator = (const RegexBranch &model)
  {
    clear();
    pContent=model.pContent;
    pContent->ptrcount++;
    if(pContent->buffer)
    {
      regfree(pContent->buffer);
      delete pContent->buffer;
    }
    return *this;
  }

  /*----------------------------------------------------------------------------*/
  //! Evaluate the logical value of the regex branch for the given entry
  //!
  //! Note that any part of the branch having an error is evaluated to false.
  //! So it's an advisable practice to check the branch for error before
  //! evaluating its logical value for a given entry.
  /*----------------------------------------------------------------------------*/
  bool eval(const DbMapTypes::Tlogentry &entry) const
  {
    bool result;
    switch(pContent->op)
    {
      case Content::op_expr:
      switch(pContent->var)
      {
        case Content::var_key:
        result = re_match(pContent->buffer, entry.key.data(), entry.key.size(), 0, NULL)>=0;
        break;
        case Content::var_val:
        result = re_match(pContent->buffer, entry.value.data(), entry.value.size(), 0, NULL)>=0;
        break;
        case Content::var_comment:
        result = re_match(pContent->buffer, entry.comment.data(), entry.comment.size(), 0, NULL)>=0;
        break;
        case Content::var_seqid:
        result = re_match(pContent->buffer, entry.seqid.data(), entry.seqid.size(), 0, NULL)>=0;
        break;
        case Content::var_writer:
        result = re_match(pContent->buffer, entry.writer.data(), entry.writer.size(), 0, NULL)>=0;
        break;
        case Content::var_tstampstr:
        result = re_match(pContent->buffer, entry.timestampstr.data(), entry.timestampstr.size(), 0, NULL)>=0;
        break;
        default:
        result=false;
        break;
      }
      return result;
      break;
      case Content::op_not:
      return ! pContent->right->eval(entry);
      case Content::op_and:
      return pContent->left->eval(entry) && pContent->right->eval(entry);
      case Content::op_or:
      return pContent->left->eval(entry) || pContent->right->eval(entry);
      default:
      return false;
    }
  }

  RegexBranch operator ! () const
  {
    RegexBranch ret;
    ret.pContent->op=Content::op_not;
    ret.pContent->right=new RegexBranch(*this);
    return ret;
  }

  RegexBranch operator || (const RegexBranch &right) const
  {
    RegexBranch ret;
    ret.pContent->op=Content::op_or;
    ret.pContent->left = new RegexBranch(*this);
    ret.pContent->right = new RegexBranch(right);
    return ret;
  }

  RegexBranch operator && (const RegexBranch & right) const
  {
    RegexBranch ret;
    ret.pContent->op=Content::op_and;
    ret.pContent->left = new RegexBranch(*this);
    ret.pContent->right = new RegexBranch(right);
    return ret;
  }

  /*----------------------------------------------------------------------------*/
  //! Non-recursively check if the branch is blank.
  /*----------------------------------------------------------------------------*/
  bool isBlank() const
  {
    return pContent->op==Content::op_error
    && pContent->pattern.empty()
    && pContent->var==Content::var_none;
  }

  /*----------------------------------------------------------------------------*/
  //! Recursively check if the branch contains any error
  /*----------------------------------------------------------------------------*/
  bool hasError() const
  {
    if(isBlank()) return false;
    switch(pContent->op)
    {
      case Content::op_expr:
      return false; // it's fine
      case Content::op_error:
      return true;// something is wrong
      default:
      bool result=false;
      if(pContent->left) result|=pContent->left->hasError();
      if(pContent->right) result|=pContent->right->hasError();
      return result;
    }
  }

  /*----------------------------------------------------------------------------*/
  //! Non-recursively return the pattern of the branch.
  /*----------------------------------------------------------------------------*/
  const std::string &getPattern() const
  { return pContent->pattern;}

protected:
  void clear()
  {
    if(!--pContent->ptrcount)
    {
      if(pContent->left) delete pContent->left;
      if(pContent->left) delete pContent->right;
      delete pContent;
    }
  }

  struct Content
  {
    //! this enum describes what type of logical operation in this branch if any
    enum t_op
    { op_error,op_expr,op_and,op_or,op_not};
    //! this enum describes what on what field the regex should be run
    enum t_var
    { var_none,var_key,var_val,var_comment,var_seqid,var_writer,var_tstampstr};

    //! counts the number of pointers on this object
    size_t ptrcount;
    //! logical operation in this branch
    t_op op;
    //! variable to run the regex
    t_var var;
    //! regex pattern
    std::string pattern;
    //! compiled regex
    struct re_pattern_buffer *buffer;
    //! left subranch
    RegexBranch *left;
    //! right subranch
    RegexBranch *right;

    Content() : ptrcount(0), op(op_error), var(var_none), pattern(), buffer(NULL), left(NULL), right(NULL)
    {}
  };
  Content *pContent;
};

/*----------------------------------------------------------------------------*/
//! Helper function to create RegexBranches
//! @param[in] variable
//!  the name of the variable to be matched with the regex
//! @param[in] regex
//!  the regular expression to match (POSIX regexp)
/*----------------------------------------------------------------------------*/
inline RegexBranch RegexAtom(const std::string &variable, const std::string &regex)
{
  return RegexBranch(variable,regex);
}

/*----------------------------------------------------------------------------*/
//! Helper Predicate to use with std::algorithm
/*----------------------------------------------------------------------------*/
class RegexPredicate : std::unary_function<DbMapTypes::Tlogentry,bool>
{
  RegexBranch mRegex;
public:
  RegexPredicate (const RegexBranch &regex) : mRegex(regex)
  {}
  bool operator () (const DbMapTypes::Tlogentry &entry) const
  {
    bool regeval=mRegex.eval(entry);
    return !regeval;
  }
};

/*------------    DbMap and DbLog TEMPLATE IMPLEMENTATIONS     ---------------*/
template<class TDbMapInterface, class TDbLogInterface> class DbLogT; // the definition of this template comes later in the same file

/*----------------------------------------------------------------------------*/
//! this class is like a map its content lays in a db or in memory or both.
//! If the data lays only in the db, it's called out of core.
//! it maps a key (a string) to a value (a string) and a comment (a string).
//! Additional informations are automatically added into the map, namely :
//! a time stamp (a size_t), a string representation of the timestamp, a sequence id ( an integer ).
//! Any modification to the content data can be logged thanks to the DbLog Class.
/*----------------------------------------------------------------------------*/
template<class TDbMapInterface,class TDbLogInterface> class DbMapT : public eos::common::LogId
{
public:
  // typedefs
  typedef DbMapTypes::Tkey Tkey;
  typedef DbMapTypes::Tval Tval;
  typedef DbMapTypes::TvalSlice TvalSlice;
  typedef DbMapTypes::Tlogentry Tlogentry;
  typedef DbMapTypes::TlogentryVec TlogentryVec;
  typedef std::pair<Tkey,Tval> Tkeyval;
  typedef typename TDbMapInterface::Option Toption;
#ifdef EOS_STDMAP_DBMAP
  typedef std::map<Tkey,Tval> Tmap;
#else
  typedef ::google::dense_hash_map<Tkey,Tval> Tmap;
#endif
  typedef std::vector<Tkeyval> Tlist;

private:
  // ------------------------------------------------------------------------
  //! some db interface parameters
  // ------------------------------------------------------------------------
  static size_t pDbIterationChunkSize;

  // ------------------------------------------------------------------------
  //! this set makes sure that every name is unique
  // ------------------------------------------------------------------------
  static std::set<std::string> gNames;

  static RWMutex gNamesMutex;
  static RWMutex gTimeMutex;

  // ------------------------------------------------------------------------
  //! the name of the DbMap instance. Default is db%p where %p is the value of 'this' pointer. it can be changed
  //! this name is used as the 'writer' value into the DbLog
  // ------------------------------------------------------------------------
  std::string pName;

  // ------------------------------------------------------------------------
  //! this variable shows if the content of the db should be kept in memory inside a map.
  //! Then all read operations are issued from the memory and write operations are issued to both memory and db.
  // ------------------------------------------------------------------------
  mutable bool pUseMap;

  // ------------------------------------------------------------------------
  //! this variable shows if the content sequence id should be used. It requires a look up each time a value is written.
  // ------------------------------------------------------------------------
  mutable bool pUseSeqId;

  // ------------------------------------------------------------------------
  //! this variable shows that the instance is being iterated (const_iteration is the only available). all others threads trying to access to the instance are blocked while a thread is iterating iterating.
  // ------------------------------------------------------------------------
  mutable bool pIterating;

  // ------------------------------------------------------------------------
  //! This is the thread id of the thread currently iterating over the DbMap (if it's being iterated). It allows to have a set sequence while iterating.
  // ------------------------------------------------------------------------
  mutable pthread_t pItThreadId;

  // ------------------------------------------------------------------------
  //! this is the map containing the data. Any read access to the instance is made to this map without accessing it. Any write access to the instance is made on both this map and the DB.
  // ------------------------------------------------------------------------
  Tmap pMap;

  // ------------------------------------------------------------------------
  //! this map is meant to allow to get values without interrupting a set sequence by updating the db. All pending changes are there.
  // ------------------------------------------------------------------------
  Tmap pSetSeqMap;

  // ------------------------------------------------------------------------
  //! this is the underlying iterator to the const_iteration feature from the memory
  // ------------------------------------------------------------------------
  mutable Tmap::const_iterator pIt;

  // ------------------------------------------------------------------------
  //! this is the underlying iterator to the const_iteration feature from the db
  // ------------------------------------------------------------------------
  mutable TlogentryVec::const_iterator pDbIt;

  // ------------------------------------------------------------------------
  //! this list is used to accumulate pairs of key values during a "set" sequence.
  // ------------------------------------------------------------------------
  Tlist pSetSeqList;

  // ------------------------------------------------------------------------
  //! this list is used to iterate through the db by block
  // ------------------------------------------------------------------------
  mutable TlogentryVec pDbItList;

  // ------------------------------------------------------------------------
  //! these members are used for the iteration through the map
  // ------------------------------------------------------------------------
  mutable Tkey pDbItKey;
  mutable Tval pDbItVal;

  // ------------------------------------------------------------------------
  //! setsequence is true if an ongoing set sequence is running. It doesn't lock the instance.
  // ------------------------------------------------------------------------
  mutable bool pSetSequence;

  // ------------------------------------------------------------------------
  //! db is a pointer to the db manager of the data
  //! the TDmapInterface is not supposed to be thread-safe. So db must be protected by a lock.
  // ------------------------------------------------------------------------
  TDbMapInterface *pDb;

  // ------------------------------------------------------------------------
  //! this is mutex at instance granularity
  // ------------------------------------------------------------------------
  mutable RWMutex pMutex;

  // ------------------------------------------------------------------------
  //! this are the counters of 'set' and 'get' calls to this instance
  // ------------------------------------------------------------------------
  mutable size_t pSetCounter, pGetCounter;
  mutable size_t pNestedSetSeq;

protected:
  // ------------------------------------------------------------------------
  //! this function generates a new time stamp suffixed by a sequence tag
  // ------------------------------------------------------------------------
  static bool now( time_t *timearg, size_t *orderarg)
  {
    RWMutexWriteLock lock(gTimeMutex);
    static size_t orderinsec=0;
    static time_t prevtime=0;
    time_t now = time(0);
    if(now==prevtime)
    orderinsec++;
    else
    {
      prevtime=now;
      orderinsec=0;
    }
    *timearg=now;
    *orderarg=orderinsec;
    return orderinsec==0;
  }

  // ------------------------------------------------------------------------
  //! generates a new time stamp suffixed by a sequence tag and build a string representation.
  // ------------------------------------------------------------------------
  static void nowStr(const char** timestrarg,time_t *timet=NULL)
  {
    // this function is thread-safe
    // each thread has its own static variables and update them when needed
    static __thread char timestr[64];
    static __thread time_t prevtime=0;
    static __thread size_t offset;
    time_t time;
    size_t order;
    now(&time,&order);
    if(time!=prevtime)
    {
      // convert time to str
      struct tm ptm;
      localtime_r(&time,&ptm);
      offset=strftime(timestr, 64, "%Y-%m-%d %H:%M:%S", &ptm);
      timestr[offset++]='#';
      prevtime=time;
    }
    // convert the ordernumber
    sprintf(timestr+offset, "%9.9lu",order);

    *timestrarg=timestr;
    if(timet!=NULL) *timet=time;
  }

  // ------------------------------------------------------------------------
  //! this function is used when closing a set sequence it flushes setseqlist to 'db' and to 'db' if necessary
  //! it returns the number of processed elements in the list
  // ------------------------------------------------------------------------
  int processSetSeqList()
  {
    unsigned long rc=(unsigned long)pSetSeqList.size();
    int i=0;
    pDb->beginTransaction();
    for(Tlist::iterator it=pSetSeqList.begin();it!=pSetSeqList.end();it++)
    {
      i++;
      if(it->second.seqid==0)
      {
        if(!doRemove(it->first,it->second)) return -1;
      }
      else
      if(!doSet(it->first,it->second)) return -1;
    }
    pDb->endTransaction();
    return rc;
  }

  // ------------------------------------------------------------------------
  //! This function sets an entry in the DbMap of which is the time stamp is set to now
  // ------------------------------------------------------------------------
  bool doSet(const Slice& key, const Slice& value, const Slice& comment)
  {
    const char *tstr;
    nowStr(&tstr);
    Slice tstrslice(tstr,strlen(tstr));
    TvalSlice val=
    { tstrslice,1,pName,value,comment};
    return doSet(key,val);
  }

  // ------------------------------------------------------------------------
  //! This function sets an entry in the DbMap
  // ------------------------------------------------------------------------
  bool doSet(const Slice &timestr, const Slice& key, const Slice& value, const Slice& comment)
  {
    TvalSlice val=
    { timestr,1,pName,value,comment};

    return doSet(key,val);
  }

  // ------------------------------------------------------------------------
  //! This function sets an entry in the DbMap of which is the time stamp is set to now
  // ------------------------------------------------------------------------
  bool doSet(const Slice& key, const TvalSlice& val)
  {
    Tmap::iterator it;
    if(pUseMap)
    { // using memory map and updating the db
      pMap[key.ToString()]=(Tval)val;
    }
    if(pDb->setEntry(key,val))
    {
      AtomicInc(pSetCounter);
      return true;}
    else
    return false;
  }

  // ------------------------------------------------------------------------
  //! This function removes an entry from the DbMap
  // ------------------------------------------------------------------------
  bool doRemove(const Slice& key, const TvalSlice& val)
  {
    if(pUseMap)
    {
      std::string keystr(key.ToString());
      Tmap::iterator it=pMap.find(keystr);
      if(it!=pMap.end())
      {
        pMap.erase(it);
      }
    }
    return pDb->removeEntry(key,val);
  }

  // ------------------------------------------------------------------------
  //! This function retrieves the data associated to a given key.
  // ------------------------------------------------------------------------
  bool doGet (const Slice& key,Tval *val) const
  {
    std::string keystr;
    if(pSetSequence || pUseMap) keystr=key.ToString();
    if(pSetSequence)
    {
      Tmap::const_iterator it=pSetSeqMap.find(keystr);
      if(it!=pSetSeqMap.end())
      {
        *val=(it->second);
        return true;
      }
    }
    if(pUseMap)
    { // NOT out-of-core
      Tmap::const_iterator it=pMap.find(keystr);
      if(it!=pMap.end())
      {
        *val=(it->second);
        return true;
      }
      else
      return false;
    }
    else // out-of-core
    return pDb->getEntry(key,val);
  }

  // ------------------------------------------------------------------------
  //! Removes an entry from the DbMap
  //! @param[in] key the key of which the entry is to be removed
  //! @param[in] value is a place holder for some information
  //! @return the number of entries buffered in the seqlist. It means 0 if the set sequence is not enabled.
  //!         -1 if an error occurs
  // ------------------------------------------------------------------------
  int remove (const Slice& key, const TvalSlice &val)
  {
    RWMutexWriteLock lock(pMutex);
    if(pSetSequence)
    {
      std::string keystr(key.ToString());
      pSetSeqList.push_back( Tkeyval(keystr,val) );
      pSetSeqMap.erase(keystr);
      return pSetSeqList.size();
    }
    else
    {
      if(doRemove(key,val))
      return 0;
      else
      return -1;
    }
  }

public:
  // ------------------------------------------------------------------------
  /// capacity
  // ------------------------------------------------------------------------

  // ------------------------------------------------------------------------
  //! Get the number of entries in the DbMap
  //! @return the number of entries in the DbMap
  // ------------------------------------------------------------------------
  size_t size() const
  {
    if(!pDb->getAttachedDbName().empty())
    {
      RWMutexReadLock lock(pMutex);
      return pDb->size();
    }
    else
    return pMap.size();
  }

  // ------------------------------------------------------------------------
  //! Check if the DbMap is empty
  //! @return true is the DbMap is empty, false else.
  // ------------------------------------------------------------------------
  bool empty() const
  { return size()==0;}
  size_t Count(const Slice &key) const
  {
    if(!pDb->getAttachedDbName().empty())
    {
      RWMutexReadLock lock(pMutex);
      return pDb->count(key);
    }
    else
    return pMap.count(key.ToString());
  }

  // ------------------------------------------------------------------------
  /// counters
  // ------------------------------------------------------------------------

  // ------------------------------------------------------------------------
  //! Get the number of reads in the DbMap
  //! @return the number of reads in the DbMap
  // ------------------------------------------------------------------------
  size_t getReadCount() const
  { return pGetCounter;}

  // ------------------------------------------------------------------------
  //! Get the number of writes in the DbMap
  //! @return the number of writes in the DbMap
  // ------------------------------------------------------------------------
  size_t getWriteCount() const
  { return pSetCounter;}

  // ------------------------------------------------------------------------
  /// Persistency
  // ------------------------------------------------------------------------

  // ------------------------------------------------------------------------
  //! Attach a db to the DbMap.
  //! When using out-of-core, this function should be used first.
  //! At most one db can be attached to a given instance of a DbMap.
  //! If a db is already attached or if an error occurs, the return value is false.
  //! @param[in] dbname the file name of the db to be attached
  //! @param[in] repair if true, a repair will attempted if opening the db fails
  //! @param[in] create createperm UNIX permission flag in case the file would have to be created
  //! @param[in] option a pointer to a struct containing parameters for the underlying db
  //! @return true if the attach is successful. false otherwise.
  // ------------------------------------------------------------------------
  bool attachDb(const std::string &dbname, bool repair=false, int createperm=0, Toption *option=NULL)
  {
    RWMutexWriteLock lock(pMutex);
    return pDb->attachDb(dbname,repair, createperm, option) && pDb->syncFromDb(&pMap);
  }

  // ------------------------------------------------------------------------
  //! Consolidate the underlying db.
  //! @return true if the operation succeeded. False otherwise.
  // ------------------------------------------------------------------------
  bool trimDb()
  {
    RWMutexWriteLock lock(pMutex);
    return pDb->trimDb();
  }

  // ------------------------------------------------------------------------
  //! Detach the currently attached db from the DbMap.
  //! @return true if successfully detached or if no db previously attached db, false otherwise
  // ------------------------------------------------------------------------
  bool detachDb()
  {
    if(!pDb->getAttachedDbName().empty())
    {
      RWMutexWriteLock lock(pMutex);
      return pDb->detachDb();
    }
    return true;
  }

  // ------------------------------------------------------------------------
  //! Turn out-of-core mode on or off.
  //! It means that the data are not stored in memory (apart from any caching of the underlying db)
  //! A necessary condition to turn ofc on is to have a db attached to the DbMap.
  //! @param[in] ofc specify if out-of-core should be on
  //! @return true if the out-of-core was successfully set as requested, false otherwise
  // ------------------------------------------------------------------------
  bool outOfCore(bool ofc)
  {
    if(pUseMap==ofc)
    {
      RWMutexWriteLock lock(pMutex);
      if(this->pDb->getAttachedDbName().empty())
      return false;
      if(pSetSequence)
      endSetSequence();
      if(pIterating)
      endIter();
      if(ofc)
      { // moving to out of core
        pMap.clear();
        pUseMap=false;
        return true;
      }
      else
      { // leaving out of core
        const DbMapTypes::Tkey *key;
        const DbMapTypes::Tval *val;
        for (beginIter(false); iterate(&key, &val,false);)
        {
          pMap[*key]=*val;
          pUseMap=true;
          return true;
        }
      }
    }
    return true; // nothing to do, so the change is ok
  }

  // ------------------------------------------------------------------------
  //! Turn the sequence id on or off.
  //! If on, a version number is associated to each key. it's incremented every time the value is modified.
  //! It needs an additional lookup every time the value is modified
  //! @param[in] on specify if the sequence id should be enabled
  // ------------------------------------------------------------------------
  void useSeqId(bool on)
  {
    RWMutexWriteLock lock(pMutex);
    pUseSeqId=on;
  }

  // ------------------------------------------------------------------------
  /// logging
  // ------------------------------------------------------------------------

  // ------------------------------------------------------------------------
  //! Attach a log to the DbMap
  //! all operations are logged to the db. The name of the DbMap is refered as writer in the log.
  //! Multiple log can be attached to a DbMap.
  //! A log can attached to several DbMap.
  //! @param[in] dbname the file name of the db to be attached
  //! @param[in] volumeduration the duration in seconds of each volume of the dblog (if set to -1, no splitting is processed)
  //! @param[in] createperm the unix permission in case the file is to be created.
  //! @param[in] option a pointer to a structure holding some parameters for the underlying db.
  //! @return true if the dbname was attached, false otherwise (it maybe because the file is already attached to this DbMap)
  // ------------------------------------------------------------------------
  bool attachLog(const std::string &dbname,int volumeduration=-1, int createperm=0, Toption *option=NULL)
  {
    RWMutexWriteLock lock(pMutex);
    return pDb->attachDbLog(dbname,volumeduration,createperm,(void*)option);
  }

  // ------------------------------------------------------------------------
  //! Attach a log to the DbMap
  //! @param[in] dblog an existing DbLog. This DbLog instance is not destroyed when the DbMap is destroyed.
  //! @return true if the dbname was attached, false otherwise (it maybe because the file is already attached to this DbMap)
  // ------------------------------------------------------------------------
  bool attachLog(DbLogT<TDbMapInterface,TDbLogInterface>* dblog)
  {
    RWMutexWriteLock lock(pMutex);
    return pDb->attachDbLog(dblog->pDb);
  }

  // ------------------------------------------------------------------------
  //! Detach a previously attached DbLog.
  //! Then all subsequent operations on the DbMap are not logged anymore into that DbLog.
  //! @param[in] dbname the file name of the db to be detached
  //! @return true if the dbname was successfully detached, false otherwise (it may be because the file has already been detached)
  // ------------------------------------------------------------------------
  bool detachLog(const std::string &dbname)
  {
    RWMutexWriteLock lock(pMutex);
    return pDb->detachDbLog(dbname);
  }

  // ------------------------------------------------------------------------
  //! Detach a previously attached DbLog. Note that the DbLog instance is not destroyed.
  //! Then all subsequent operations on the DbMap are not logged anymore into that DbLog.
  //! @param[in] dblog an existing DbLog. This DbLog instance is not destroyed.
  //! @return true if the dbname was successfully detached, false otherwise (it may be because the file has already been detached)
  // ------------------------------------------------------------------------
  bool detachLog(DbLogT<TDbMapInterface,TDbLogInterface>* dblog)
  {
    RWMutexWriteLock lock(pMutex);
    return pDb->detachDbLog(dblog->pDb);
  }

  // ------------------------------------------------------------------------
  /// constr , destr
  // ------------------------------------------------------------------------
  DbMapT() : pUseMap(true), pUseSeqId(true), pIterating(false), pSetSequence(false), pSetCounter(0), pGetCounter(0), pNestedSetSeq(0)
  {
    pDb = new TDbMapInterface();
    char buffer[32];
    sprintf(buffer,"dbmap%p",this);
    pName = buffer;
    gNamesMutex.LockWrite();
    gNames.insert(pName);
    gNamesMutex.UnLockWrite();
    pDb->setName(pName);
#ifndef EOS_STDMAP_DBMAP
    pMap.set_empty_key("\x01");
    pMap.set_deleted_key("\x02");
    pSetSeqMap.set_empty_key("\x01");
    pSetSeqMap.set_deleted_key("\x02");
#endif
  }
  ~DbMapT()
  {
    gNamesMutex.LockWrite();
    gNames.erase(pName);
    gNamesMutex.UnLockWrite();
    delete static_cast<TDbMapInterface*>(pDb);
  }

  // ------------------------------------------------------------------------
  //! Set the name of the DbMap. This name is used in the logs attached to this DbMap.
  //! if not changed with this function the default name of a dbmap is dbmap%p where %p is the pointer to the DbMap Object.
  //! this avoid any conflict at any time but doesn't guarantee consistency over time.
  //! @param[in] name the name used for this DbMap as the writer in the logs
  //! @return true if the name of the instance was successfully set. If the return value is false, the name is already given to another instance.
  // ------------------------------------------------------------------------
  bool setName(const std::string &name)
  {
    gNamesMutex.LockWrite();
    if(gNames.find(name)!=gNames.end())
    {
      gNamesMutex.UnLockWrite();
      return false;
    }
    gNames.erase(this->pName);
    gNames.insert(name);
    this->pName=name;
    pDb->setName(name);
    gNamesMutex.UnLockWrite();
    return true;
  }

  // ------------------------------------------------------------------------
  //! Begin a const_iteration. It blocks the access to all the other threads as long as the iteration is not over.
  //! the actual iteration steps are done by calling iterate
  //! the iteration can be terminated using EndIter().
  //! the iteration is automatically stopped if it reaches the end.
  //! @param[in] lockit should be given true (default value)
  // ------------------------------------------------------------------------
  void beginIter(bool lockit=true) const
  {
    if (lockit) pMutex.LockWrite(); // to prevent collisions in multiple threads iterating simultaneously
    if(pUseMap)
    {
      pIt=pMap.begin();
    }
    else
    {
      pDbItList.clear();
      pDb->getAll( &pDbItList, pDbIterationChunkSize,NULL );
      pDbIt=pDbItList.begin();
    }
    pIterating=true;
    pItThreadId=pthread_self();
  }

  // ------------------------------------------------------------------------
  //! Get the next pair of (key,value) in the iteration
  //! @param[out] keyOut a pointer to the const C-string holding the key
  //! @param[out] valOut a pointer to the const C-string holding the value
  //! @param[in] unlockit should be given true (default value)
  //! @return true if there is at least one more step in the iteration after the current one. false otherwise.
  // ------------------------------------------------------------------------
  bool iterate(const Tkey** keyOut, const Tval **valOut, bool unlockit=true) const
  {
    if(!pIterating) return false;
    if(pUseMap)
    {
      bool ret=(pIt!=pMap.end());
      if(ret)
      {
        *keyOut=&pIt->first;
        *valOut=&pIt->second;
        Tval valdb;

        AtomicInc(pGetCounter);
        pIt++;
        return true;
      }
      else
      {
        endIter(unlockit);
        return false;
      }
    }
    else
    { // iter directly from the db
      if(pDbIt==pDbItList.end())
      {
        Tlogentry entry;
        Tlogentry* lastentry;
        if(pDbItList.empty())
        {
          lastentry=NULL;
        }
        else
        {
          entry=*--pDbIt;
          lastentry=&entry;
        }
        pDbItList.clear();
        if(pDb->getAll( &pDbItList, pDbIterationChunkSize, lastentry )==0)
        {
          endIter();
          return false;
        }
        pDbIt=pDbItList.begin();
      }
      // dbit actually points to something
      Tkeyval entry;
      pDbItKey=pDbIt->key;
      Tlogentry2Tval(*pDbIt,&pDbItVal);
      *keyOut=&pDbItKey;
      *valOut=&pDbItVal;

      pDbIt++;
      return true;
    }
  }

  // ------------------------------------------------------------------------
  //! Stop an ongoing iteration
  //! WARNING : if an ongoing iteration is not stopped, no readings or writings can be done oh the instance.
  //! @param[in] unlockit should be given true (default value)
  // ------------------------------------------------------------------------
  void endIter(bool unlockit=true) const
  {
    if(pIterating)
    {
      pIterating=false;
      if(unlockit) pMutex.UnLockWrite();
    }
  }

  // ------------------------------------------------------------------------
  //! Set a Key / Value / Comment Entry
  //! this function returns the number of entries buffered in the seqlist. It means 0 if the set sequence is not enabled.
  // ------------------------------------------------------------------------
  unsigned long set (const Slice& key, const Slice& value, const Slice& comment)
  {
    const char *tstr;
    nowStr(&tstr);

    return set(tstr,key,value,comment);
  }

  // ------------------------------------------------------------------------
  //! this function sets an entry in the DbMap.
  //! it returns the number of entries buffered in the seqlist. It means 0 if the set sequence is not enabled.
  // ------------------------------------------------------------------------
  int set( const Slice& timestr, const Slice& key, const Slice& value, const Slice& comment)
  {
    RWMutexWriteLock lock(pMutex);
    Tval gval;
    TvalSlice val=
    { timestr,1,pName,value,comment};
    if(pUseSeqId)
    {
      bool keyfound=doGet(key,&gval);
      if(keyfound) val.seqid=gval.seqid+1;
    }

    if(pSetSequence)
    {
      pSetSeqList.push_back( Tkeyval(key.ToString(),val) );
      pSetSeqMap[key.ToString()]=val;
      return pSetSeqList.size();
    }
    else
    {
      if(doSet(key,val))
      return 0;
      else
      return -1;

    }
  }

  // ------------------------------------------------------------------------
  //! Set a Key / full Value
  //! @param[in] key the key
  //! @param[in] val the full value struct
  //! @return the number of entries buffered in the seqlist. It means 0 if the set sequence is not enabled.
  //!         -1 if an error occurs
  // ------------------------------------------------------------------------
  int set( const Slice& key, const TvalSlice& val)
  {
    // RWMutexWriteLock lock(mutex);
    if(pSetSequence)
    {
      if(!pIterating || pItThreadId!=pthread_self()) pMutex.LockWrite(); // if the current thread is iterating through the dbmap, don't lock
      // On the other hand, it allows to do some set inside an iteration by using a setsequence
      // then endSetSequence should be called after Enditerate
      std::string keystr(key.ToString());
      pSetSeqList.push_back( Tkeyval(keystr,val) );
      pSetSeqMap[keystr]=val;
      unsigned long ret=pSetSeqList.size();
      if(!pIterating || pItThreadId!=pthread_self()) pMutex.UnLockWrite();

      return ret;
    }
    else
    {
      RWMutexWriteLock lock(pMutex);
      if(doSet(key,val))
      return 0;
      else
      return -1;
    }
  }

  // ------------------------------------------------------------------------
  //! Remove an entry from the DbMap
  //! @param[in] key the key of which the entry is to be removed
  //! @return the number of entries buffered in the seqlist. It means 0 if the set sequence is not enabled.
  //!         -1 if an error occurs
  // ------------------------------------------------------------------------
  int remove (const Slice& key)
  {
    const char *tstr;
    nowStr(&tstr);

    Tval val=
    { tstr,0,pName,"","!DELETE"};
    return remove(key,val);
  }

  // ------------------------------------------------------------------------
  //! Erase all the entries in the DbMap
  //! WARNING : if a db is attached, its content is erased
  //! @return false if an error occurs, true otherwise
  // ------------------------------------------------------------------------
  bool clear ()
  {
    pMutex.LockWrite();
    if(pDb->clear())
    pMap.clear();
    else
    {
      pMutex.UnLockWrite();
      return false;
    }

    pMutex.UnLockWrite();
    return true;
  }

  // ------------------------------------------------------------------------
  //! Get the value associated with a key.
  //! @param[in] key the key of which the value is to be retreived
  //! @param[out] val a pointer to a struct where the full value is to be written
  //! @return false if an error occurs, true otherwise
  // ------------------------------------------------------------------------
  bool get (const Slice& key, Tval* val) const
  {
    RWMutexReadLock lock(pMutex);
    if(doGet(key,val))
    {
      AtomicInc(pGetCounter);
      return true;
    }
    return false;
  }

  // ------------------------------------------------------------------------
  /// transactions
  // ------------------------------------------------------------------------

  // ------------------------------------------------------------------------
  //! Begin a set sequence
  //! when using a set sequence all subsequent 'set' operations are cached into a list.
  //! it can be filled by multiple thread which are actually serialized.
  //! beware, the setsequence status of an instance can be changed from multiple threads.
  // ------------------------------------------------------------------------
  void beginSetSequence() const
  {
    RWMutexWriteLock lock(pMutex);
    AtomicInc(pNestedSetSeq);
    //assert(!setsequence);
    if(!pSetSequence)
    {
      pSetSequence=true;
    }
  }

  // ------------------------------------------------------------------------
  //! Terminate a set sequence
  //! when a write sequence ends. All the pending 'set' and 'remove' are committed atomically.
  //! @return the number of committed changes.
  // ------------------------------------------------------------------------
  unsigned long endSetSequence()
  {
    RWMutexWriteLock lock(pMutex);
    //assert(setsequence);
    AtomicDec(pNestedSetSeq);
    if(pSetSequence && pNestedSetSeq==0)
    {
      unsigned long ret=processSetSeqList();
      pSetSeqList.clear();
      pSetSeqMap.clear();
      pSetSequence=false;
      return ret;
    }
    return 0;
  }

  // ------------------------------------------------------------------------
  //! Replay the operations recorded into a logdb
  //! @param[in] dblogint a pointer to an existing dblog
  //! @return -1 if any error occurs, otherwise the number of replayed changes.
  // ------------------------------------------------------------------------
  int loadDbLog(const DbLogT<TDbMapInterface,TDbLogInterface> *dblogint)
  {
    const int blocksize=1000;
    DbMapTypes::Tlogentry entry;
    DbMapTypes::TlogentryVec entryvec;
    DbMapTypes::TlogentryVec::const_iterator entryit;
    entryvec.reserve(blocksize);
    int count=0;
    for(; dblogint->getAll(&entryvec,blocksize,&entry); entryvec.clear() )
    {
      beginSetSequence();
      for( entryit=entryvec.begin(); entryit!=entryvec.end(); entryit++ )
      {
        count++;
        Tval val;
        Tlogentry2Tval(*entryit,&val);
        if(val.seqid==0)
        {
          if(remove(entryit->key,val)<0) return -1;
        }
        else
        {
          if(set(entryit->key,val)<0) return -1;
        }
      }
      endSetSequence();
    }
    return count;
  }

  // ------------------------------------------------------------------------
  //! Replay the operations recorded into a logdb and attaches this logdb
  //! WARNING: This is not an atomic operation.
  //! Another thread might issue some change to the content between the Load and the Attach.
  //! @param[in] dblogint a pointer to an existing dblog. This object is never destroyed by the DbMap.
  //! @return -1 if any error occurs, otherwise the number of replayed changes.
  // ------------------------------------------------------------------------
  int loadAndattachDbLog(DbLogT<TDbMapInterface,TDbLogInterface> *dblogint)
  {
    int count=loadDbLog(dblogint);
    if(count>=0)
    attachLog(dblogint);
    return count;
  }

  // ------------------------------------------------------------------------
  //! Replay the operations recorded into a logdb
  //! @param[in] dbname the file name of the dblog to be replayed
  //! @return -1 if any error occurs, otherwise the number of replayed changes.
  // ------------------------------------------------------------------------
  int loadDbLog(const std::string &dbname)
  {
    DbLogT<TDbMapInterface,TDbLogInterface> dblg(dbname);
    return loadDbLog(&dblg);
  }

  // ------------------------------------------------------------------------
  //! Replay the operations recorded into a logdb and attaches this logdb
  //! WARNING: This is not an atomic operation.
  //! Another thread might issue some change to the content between the Load and the Attach.
  //! @param[in] dbname the file name of the dblog to be replayed and attached
  //! @param[in] volumeduration the duration in seconds of each volume of the dblog (if set to -1, no splitting is processed)
  //! @param[in] createperm the unix permission in case the file is to be created.
  //! @param[in] option a pointer to a structure holding some parameters for the underlying db.
  //! @return true if the dbname was successfully replayed and attached,
  //!         false otherwise (it maybe because the file is already attached to this DbMap)
  // ------------------------------------------------------------------------
  int loadAndattachDbLog(const std::string &dbname, int volumeduration, int createperm, Toption *option)
  {
    int count=loadDbLog(dbname);
    if(count>=0)
    attachLog(dbname,volumeduration,createperm,option);
    return count;
  }

  // ------------------------------------------------------------------------
  //! Get the underlying db system
  //! @return a string containing the name of the underlying db system
  // ------------------------------------------------------------------------
  static std::string getDbType()
  { return TDbMapInterface::getDbType();}
};

/*----------------------------------------------------------------------------*/
//! this class is a logging container which store snapshots of entries of DbMap.
//! It provides some reading facilities.
//! Writing to this class must be done via the TDbMap class.
/*----------------------------------------------------------------------------*/
template<class TDbMapInterface, class TDbLogInterface> class DbLogT : public eos::common::LogId
{
  friend class DbMapT<TDbMapInterface,TDbLogInterface>;
  // db is a pointer to the db manager of the data
  // the TDbMapInterface is not supposed to be thread-safe. So db must be protected by a lock.
  TDbLogInterface *pDb;
  mutable RWMutex pMutex;
public:
  // typedefs
  typedef DbMapTypes::Tkey Tkey;
  typedef DbMapTypes::Tval Tval;
  typedef DbMapTypes::TvalSlice TvalSlice;
  typedef DbMapTypes::Tlogentry Tlogentry;
  typedef DbMapTypes::TlogentryVec TlogentryVec;
  typedef std::pair<Tkey,Tval> Tkeyval;
  typedef std::map<Tkey,Tval> Tmap;
  typedef std::vector<Tkeyval> Tlist;
  typedef typename TDbLogInterface::Option Toption;

  // ------------------------------------------------------------------------
  /// constr , destr
  // ------------------------------------------------------------------------
  DbLogT()
  {
    pDb=new TDbLogInterface();
  }

  DbLogT(const std::string dbfile,int volumeduration=-1, int createperm=0, Toption *option=NULL)
  {
    pDb=new TDbLogInterface(dbfile,volumeduration, createperm, option);
  };
  ~DbLogT()
  { RWMutexWriteLock lock(pMutex); delete pDb;}
  bool setDbFile( const std::string &dbname, int volumeduration=-1, int createperm=0, Toption* option=NULL)
  {
    RWMutexWriteLock lock(pMutex);
    return pDb->setDbFile(dbname,volumeduration, createperm, (void*)option);
  }

  // ------------------------------------------------------------------------
  //! Check if the underlying db is properly opened
  //! @return true if the underlying db is open, false otherzise
  // ------------------------------------------------------------------------
  bool isOpen()
  {
    return pDb->isOpen();
  }

  // ------------------------------------------------------------------------
  //! Get the name of the underlying db file
  //! @return the name of the underlying dbfile
  // ------------------------------------------------------------------------
  std::string getDbFile() const
  {
    RWMutexWriteLock lock(pMutex);
    return pDb->getDbFile();
  }

  // ------------------------------------------------------------------------
  //! Get all the entries of the DbLog optionally by block and optionally matching a regexp
  //! @param[out] retvec a pointer to a vector of entries to which the result of the operation will be appended
  //! @param[in] nmax maximum number of elements to get at once
  //! @param[in,out] startafter before execution : position at which the search is to be started
  //!                           after  execution : position at which the search has to start at the next iteration
  //! @param[in] regex the regular expression the entries need to match
  //! @return the number of entries appended to the result vector retvec
  // ------------------------------------------------------------------------
  int getAll( TlogentryVec *retvec, size_t nmax=0, Tlogentry *startafter=NULL, RegexBranch regex=RegexBranch() ) const
  {
    if(regex.hasError()) return -1;
    int startsize=retvec->size();
    RWMutexReadLock lock(pMutex);
    pDb->getAll(retvec,nmax,startafter);
    if(regex.isBlank()) return retvec->size()-startsize;
    RegexPredicate rmpred(regex);
    TlogentryVec::iterator newend=std::remove_if(retvec->begin(),retvec->end(),rmpred);
    retvec->erase(newend,retvec->end());
    return retvec->size()-startsize;
  }

  // ------------------------------------------------------------------------
  //! Get the latest entries of the DbLog optionally matching a regexp
  //! @param[in] nentries maximum number of elements to run the search on starting from the end
  //! @param[out] retvec a pointer to a vector of entries to which the result of the operation will be appended
  //! @param[in] regex the regular expression the entries need to match
  //! @return the number of entries appended to the result vector retvec
  // ------------------------------------------------------------------------
  int getTail( int nentries, TlogentryVec *retvec, RegexBranch regex=RegexBranch()) const
  {
    if(regex.hasError()) return -1;
    int startsize=retvec->size();
    RWMutexReadLock lock(pMutex);
    pDb->getTail(nentries,retvec);
    if(regex.isBlank()) return retvec->size()-startsize;
    RegexPredicate rmpred(regex);
    TlogentryVec::iterator newend=std::remove_if(retvec->begin(),retvec->end(),rmpred);
    retvec->erase(newend,retvec->end());
    return retvec->size()-startsize;
  }

  // ------------------------------------------------------------------------
  //! Clear the content of the DbLog
  //! @return false if an error occurs, true otherwise
  // ------------------------------------------------------------------------
  bool clear()
  {
    RWMutexReadLock lock(pMutex);
    return pDb->clear();
  }

  // ------------------------------------------------------------------------
  //! Compactify a DbLog in place i.e. reduce the set of changes to the minimum equivalent set of changes.
  //! for example set a = 2, set a = 3, remove a, set a = 4 => set a = 4
  //! @return a pair containing first the number of entries before the compactation
  //!                           and second the number of entries after the compactation
  // ------------------------------------------------------------------------
  std::pair<int,int> compactify()
  {
    std::pair<int,int> retval;
    DbMapT<TDbMapInterface,TDbLogInterface> dbmap,dbmap2;
    retval.second=0;
    retval.first=dbmap.loadDbLog(this);
    this->clear();
    dbmap2.attachLog(this);
    const DbMapTypes::Tkey *key;
    const DbMapTypes::Tval *val;
    dbmap2.beginSetSequence();
    for (dbmap.beginIter(); dbmap.iterate(&key, &val);)
    {
      if(dbmap2.set(*key,*val))
      retval.second++;
    }
    dbmap2.endSetSequence();
    return retval;
  }

  // ------------------------------------------------------------------------
  //! Compactify a DbLog to another db file i.e. reduce the set of changes to the minimum equivalent set of changes.
  //! for example set a = 2, set a = 3, remove a, set a = 4 => set a = 4
  //! @param[in] dbname the name of the db file to write the compacted version of the DbLog
  //! @return a pair containing first the number of entries before the compactation
  //!                           and second the number of entries after the compactation
  // ------------------------------------------------------------------------
  std::pair<int,int> compactifyTo( const string &dbname) const
  {
    std::pair<int,int> retval;
    DbMapT<TDbMapInterface,TDbLogInterface> dbmap,dbmap2;
    retval.first=dbmap.loadDbLog(this);
    dbmap2.attachLog(dbname);
    const DbMapTypes::Tkey *key;
    const DbMapTypes::Tval *val;
    dbmap2.beginSetSequence();
    for (dbmap.beginIter(); dbmap.iterate(&key, &val);)
    {
      if(dbmap2.set(*key,*val))
      retval.second++;
    }
    dbmap2.endSetSequence();
    return retval;
  }

  // ------------------------------------------------------------------------
  //! Get the underlying db system
  //! @return a string containing the name of the underlying db system
  // ------------------------------------------------------------------------
  static std::string getDbType()
  { return TDbLogInterface::getDbType();}
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
//std::ostream& operator << (std::ostream &os, const DbMap &map );
template<class DbMapInterface, class DbLogInterface> std::ostream& operator << (std::ostream &os, const DbMapT<DbMapInterface,DbLogInterface> &map)
{
  const DbMapTypes::Tkey *key;
  const DbMapTypes::Tval *val;
  for (map.beginIter(); map.iterate(&key, &val);)
  {
    os << *key << " --> " << *val << std::endl;
  }
  return os;
}

/*----------------------------------------------------------------------------*/

#ifndef EOS_SQLITE_DBMAP
/*-----------------------    CONVERSION HELPERS     --------------------------*/
DbMapT<SqliteDbMapInterface,SqliteDbLogInterface> DbMapLevelDb2Sqlite (const DbMapT<LvDbDbMapInterface,LvDbDbLogInterface>&);
DbMapT<LvDbDbMapInterface,LvDbDbLogInterface> DbMapSqlite2LevelDb (const DbMapT<SqliteDbMapInterface,SqliteDbLogInterface>&);
bool ConvertSqlite2LevelDb(const std::string &sqlpath, const std::string &lvdbpath, const std::string &sqlrename="");
bool ConvertLevelDb2Sqlite(const std::string &lvdbpath, const std::string &sqlpath, const std::string &lvdbrename="");
/*----------------------------------------------------------------------------*/
#endif

EOSCOMMONNAMESPACE_END

#endif

