//------------------------------------------------------------------------------
//! @file FuseCacheEntry.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Information stored in the FUSE dir. cache about a directory
//------------------------------------------------------------------------------

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

#ifndef __EOSFUSE_FUSECACHEENTRY_HH__
#define __EOSFUSE_FUSECACHEENTRY_HH__

/*----------------------------------------------------------------------------*/
#include <map>
#include <set>
/*----------------------------------------------------------------------------*/
#include "common/RWMutex.hh"
/*----------------------------------------------------------------------------*/

struct dirbuf {
  char* p;
  size_t size;
  size_t alloc_size;
};

using eos::common::RWMutex;

//------------------------------------------------------------------------------
//! Information about a directory saved in cache
//------------------------------------------------------------------------------
class FuseCacheEntry
{
public:

  //--------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param noEntries number of subentries in the directory
  //! @param modifTime modification time
  //! @param pBuf dirbuf structure
  //!
  //--------------------------------------------------------------------------
  FuseCacheEntry(int             noEntries,
                 struct timespec modifTime,
                 struct dirbuf*  pBuf,
                 long lifetime);


  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  ~FuseCacheEntry();


  //--------------------------------------------------------------------------
  //! Test if directory is filled
  //!
  //! @return true if filled, otherwise false
  //!
  //--------------------------------------------------------------------------
  bool IsFilled();


  //--------------------------------------------------------------------------
  //! Update directory information
  //!
  //! @param noEntries number of entries in the directory
  //! @param modifTime modification time
  //! @param pBuf dirbuf structure
  //!
  //--------------------------------------------------------------------------
  void Update(int             noEntries,
              struct timespec modifTime,
              struct dirbuf*  pBuf);


  //--------------------------------------------------------------------------
  //! Get the dirbuf structure
  //!
  //! @param rpBuf dirbuf structure
  //!
  //--------------------------------------------------------------------------
  void GetDirbuf(struct dirbuf*& rpBuf);


  //--------------------------------------------------------------------------
  //! Get the modification time
  //!
  //! @return timespec structure
  //!
  //--------------------------------------------------------------------------
  struct timespec GetModifTime();


  //--------------------------------------------------------------------------
  //! Add subentry
  //!
  //! @param inode new entry inode
  //! @param e fuse_entry_param structure
  //!
  //--------------------------------------------------------------------------
  void AddEntry(unsigned long long inode, struct fuse_entry_param* e);


  //--------------------------------------------------------------------------
  //! Get subentry
  //!
  //! @param inode subentry inode
  //! @param e fuse_entryu_param structure
  //!
  //! @return true if entry found, otherwise false
  //!
  //--------------------------------------------------------------------------
  bool GetEntry(unsigned long long inode, struct fuse_entry_param& e);


  //--------------------------------------------------------------------------
  //! Update subentry stat
  //!
  //! @param inode subentry inode
  //! @param buf statu strcuture
  //!
  //! @return true if entry found, otherwise false
  //!
  //--------------------------------------------------------------------------
  bool UpdateEntry(unsigned long long inode, struct stat* buf);

  //--------------------------------------------------------------------------
  //! Get set with all entry inodes
  //!
  //! @param inode subentry inode
  //!
  //! @return set with entry inodes
  //!
  //--------------------------------------------------------------------------

  std::set<unsigned long long> GetEntryInodes();


private:

  int mNumEntries;                   ///< number of subentries in directory
  struct dirbuf mBuf;                ///< dirbuf structure
  struct timespec mModifTime;        ///< modification time of the directory
  struct timespec mQueryTime;        ///< time when queried
  long mLifeTime;                    ///< lifetime in nanoseconds
  eos::common::RWMutex mMutex;       ///< mutex protecting the subentries map
  std::map<unsigned long long, struct fuse_entry_param>
    mSubEntries;  ///< map of subentries
};

#endif



