//------------------------------------------------------------------------------
// File: FuseCachEntry.hh
// Author: Elvin-Alin Sindrilaru - CERN
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
/*----------------------------------------------------------------------------*/
#include "xrdposix.hh"
#include "common/RWMutex.hh"
/*----------------------------------------------------------------------------*/

using eos::common::RWMutex;

//------------------------------------------------------------------------------
// Information about a directory saved in cache
//------------------------------------------------------------------------------
class FuseCacheEntry
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param no_entries number of subentries in the directory
    //! @param mt modification time
    //! @param buf dirbuf structure
    //!
    //--------------------------------------------------------------------------
    FuseCacheEntry( int no_entries, struct timespec mt, struct dirbuf* buf );


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
    //! @param no_entries number of entries in the directory
    //! @param mt modification time
    //! @param buf dirbuf structure
    //!
    //--------------------------------------------------------------------------
    void Update( int no_entries, struct timespec mt, struct dirbuf* buf );


    //--------------------------------------------------------------------------
    //! Get the dirbuf structure
    //!
    //! @param buf dirbuf structure
    //!
    //--------------------------------------------------------------------------
    void GetDirbuf( struct dirbuf*& buf );


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
    void AddEntry( unsigned long long inode, struct fuse_entry_param* e );


    //--------------------------------------------------------------------------
    //! Get subentry
    //!
    //! @param inode subentry inode
    //! @param e fuse_entryu_param structure
    //!
    //! @return true if entry found, otherwise false
    //!
    //--------------------------------------------------------------------------
    bool GetEntry( unsigned long long inode, struct fuse_entry_param& e );


  private:
    eos::common::RWMutex mutex;        //< mutex protecting the subentries map
    int num_entries;                   //< number of subentries in directory
    struct dirbuf b;                   //< dirbuf structure
    struct timespec mtime;             //< modification time of the directory
    std::map<unsigned long long, struct fuse_entry_param> children;  //< map of subentries
};

#endif
