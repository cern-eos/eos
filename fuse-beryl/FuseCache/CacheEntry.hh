//------------------------------------------------------------------------------
// File: CacheEntry.hh
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch> CERN
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

#ifndef __EOS_FUSE_CACHEENTRY_HH__
#define __EOS_FUSE_CACHEENTRY_HH__

/*----------------------------------------------------------------------------*/
#include <map>
/*----------------------------------------------------------------------------*/
#include <sys/time.h>
#include <sys/types.h>
/*----------------------------------------------------------------------------*/
#include "FileAbstraction.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/


//------------------------------------------------------------------------------
//! Class representing a block saved in write cache
//------------------------------------------------------------------------------
class CacheEntry: public eos::common::LogId
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param fabst file abstraction object
    //! @param buf data buffer
    //! @param off offset
    //! @param len length
    //!
    //--------------------------------------------------------------------------
    CacheEntry(FileAbstraction*& fabst,
               char* buf,
               off_t off,
               size_t len);


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    ~CacheEntry();


    //--------------------------------------------------------------------------
    //! Get maximum size of the cache
    //--------------------------------------------------------------------------
    inline static const size_t GetMaxSize()
    {
      return msMaxSize;
    }


    //--------------------------------------------------------------------------
    //! Get handler to the data buffer
    //--------------------------------------------------------------------------
    char* GetDataBuffer();


    //--------------------------------------------------------------------------
    //! Get the size of meaningful data
    //--------------------------------------------------------------------------
    inline size_t GetSizeData() const
    {
      return mSizeData;
    };


    //--------------------------------------------------------------------------
    //! Get start offset value
    //--------------------------------------------------------------------------
    inline off_t GetOffsetStart() const
    {
      return mOffsetStart;
    };


    //--------------------------------------------------------------------------
    //! Get end offset value
    //--------------------------------------------------------------------------
    inline off_t GetOffsetEnd() const
    {
      return (mOffsetStart + mCapacity);
    };


    //--------------------------------------------------------------------------
    //! Test if block full with meaningfull data
    //--------------------------------------------------------------------------
    inline bool IsFull()
    {
      return (mCapacity == mSizeData);
    };


    //--------------------------------------------------------------------------
    //! Get handler to the parent file object
    //--------------------------------------------------------------------------
    FileAbstraction* GetParentFile() const;


    //--------------------------------------------------------------------------
    //! Method that does the actual writing
    //!
    //! @return size of data written
    //--------------------------------------------------------------------------
    int64_t DoWrite();


    //--------------------------------------------------------------------------
    //! Add a new piece to the block
    //!
    //! @param buf buffer from where to take the data
    //! @param off offset
    //! @param len length
    //!
    //! @return the actual size increase of the meaningful data after adding the
    //!         current piece (does not include the overwritten sections)
    //!
    //--------------------------------------------------------------------------
    size_t AddPiece(const char* buf, off_t off, size_t len);

  
    //--------------------------------------------------------------------------
    //! Method to recycle a previously used block
    //!
    //! @param fabst file abstraction object
    //! @param buf data buffer
    //! @param off offset
    //! @param len length
    //!
    //--------------------------------------------------------------------------
    void DoRecycle(FileAbstraction*& fabst,
                   char* buf,
                   off_t off,
                   size_t len);

  private:

    static size_t msMaxSize; ///< max size of entry
    FileAbstraction* mParentFile; ///< file layout type handler
    char*  mBuffer; ///< buffer of the object
    size_t mCapacity; ///< total capcity 512 KB ~ 4MB
    size_t mSizeData; ///< size of useful data
    off_t  mOffsetStart; ///< offset relative to the file
    std::map<off_t, size_t> mMapPieces; ///< pieces to be written
};

#endif // __EOS_FUSE_CACHEENTRY_HH__

