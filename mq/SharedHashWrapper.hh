// ----------------------------------------------------------------------
// File: SharedHashWrapper.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#ifndef EOS_MQ_SHARED_HASH_WRAPPER_HH
#define EOS_MQ_SHARED_HASH_WRAPPER_HH

#include "mq/Namespace.hh"
#include "common/Locators.hh"
#include "common/RWMutex.hh"
#include "mgm/TableFormatter/TableCell.hh"
#include <string>
#include <vector>

class XrdMqSharedHash;
class XrdMqSharedObjectManager;

EOSMQNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Compatibility class for shared hashes - work in progress.
//------------------------------------------------------------------------------
class SharedHashWrapper {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  SharedHashWrapper(const common::SharedHashLocator &locator, bool takeLock = true, bool create = true);

  //----------------------------------------------------------------------------
  //! "Constructor" for global MGM hash
  //----------------------------------------------------------------------------
  static SharedHashWrapper makeGlobalMgmHash();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~SharedHashWrapper();

  //----------------------------------------------------------------------------
  //! Release any interal locks - DO NOT use this object any further
  //----------------------------------------------------------------------------
  void releaseLocks();

  //----------------------------------------------------------------------------
  //! Set key-value pair
  //----------------------------------------------------------------------------
  bool set(const std::string &key, const std::string &value, bool broadcast = true);

  //----------------------------------------------------------------------------
  //! Query the given key
  //----------------------------------------------------------------------------
  std::string get(const std::string &key);

  //----------------------------------------------------------------------------
  //! Query the given key - convert to long long automatically
  //----------------------------------------------------------------------------
  long long getLongLong(const std::string &key);

  //----------------------------------------------------------------------------
  //! Query the given key - convert to double automatically
  //----------------------------------------------------------------------------
  double getDouble(const std::string &key);

  //----------------------------------------------------------------------------
  //! Query the given key, return if retrieval successful
  //----------------------------------------------------------------------------
  bool get(const std::string &key, std::string &value);

  //----------------------------------------------------------------------------
  //! Delete the given key
  //----------------------------------------------------------------------------
  bool del(const std::string &key, bool broadcast = true);

  //----------------------------------------------------------------------------
  //! Get all keys in hash
  //----------------------------------------------------------------------------
  bool getKeys(std::vector<std::string> &out);

  //----------------------------------------------------------------------------
  //! Initialize, set shared manager.
  //! Call this function before using any SharedHashWrapper!
  //----------------------------------------------------------------------------
  static void initialize(XrdMqSharedObjectManager *som);

  //----------------------------------------------------------------------------
  //! Print contents onto a table: Compatibility function, originally
  //! implemented in XrdMqSharedHash.
  //!
  //! Format contents of the hash map to be displayed using the table object.
  //!
  //! @param table_mq_header table header
  //! @param talbe_md_data table data
  //! @param format format has to be provided as a chain separated by "|" of
  //!        the following tags
  //! "key=<key>:width=<width>:format=[+][-][slfo]:unit=<unit>:tag=<tag>:condition=<key>=<val>"
  //! -> to print a key of the attached children
  //! "sep=<seperator>" -> to put a seperator
  //! "header=1" -> to put a header with description on top - this must be the
  //!               first format tag.
  //! "indent=<n>" -> indent the output
  //! The formats are:
  //! 's' : print as string
  //! 'S' : print as short string (truncated after .)
  //! 'l' : print as long long
  //! 'f' : print as double
  //! 'o' : print as <key>=<val>
  //! '-' : left align the printout
  //! '+' : convert numbers into k,M,G,T,P ranges
  //! The unit is appended to every number:
  //! e.g. 1500 with unit=B would end up as '1.5 kB'
  //! "tag=<tag>" -> use <tag> instead of the variable name to print the header
  //! @param filter to filter out hash content
  //----------------------------------------------------------------------------
  void print(TableHeader& table_mq_header, TableData& table_mq_data,
    std::string format, const std::string &filter);

private:
  common::SharedHashLocator mLocator;
  common::RWMutexReadLock mReadLock;
  XrdMqSharedHash *mHash;

  static XrdMqSharedObjectManager *mSom;
};

EOSMQNAMESPACE_END

#endif

