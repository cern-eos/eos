//------------------------------------------------------------------------------
//! @file: CacheEntry.hh
//! @author: Elvin-Alin Sindrilaru - CERN
//! @brief Class representing a block saved in cache
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

#ifndef __EOS_CACHEENTRY_HH__
#define __EOS_CACHEENTRY_HH__

/*----------------------------------------------------------------------------*/
#include <map>
/*----------------------------------------------------------------------------*/
#include <sys/time.h>
#include <sys/types.h>
/*----------------------------------------------------------------------------*/
#include "FileAbstraction.hh"
/*----------------------------------------------------------------------------*/
#include "../../fst/layout/Layout.hh"
/*----------------------------------------------------------------------------*/


//------------------------------------------------------------------------------
//! Class representing a block saved in cache
//------------------------------------------------------------------------------
class CacheEntry
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param file file layout type handler
    //! @param buf data buffer
    //! @param off offset
    //! @param len length
    //! @param rFileAbst FileAbstraction handler
    //! @param isWr set if entry is for writing
    //!
    //--------------------------------------------------------------------------
    CacheEntry( eos::fst::Layout*& file,
                char*              buf,
                off_t              off,
                size_t             len,
                FileAbstraction&   rFileAbst,
                bool               isWr );

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    ~CacheEntry();

    //--------------------------------------------------------------------------
    //! Get maximum size of the cache
    //--------------------------------------------------------------------------
    static const size_t GetMaxSize() {
      return msMaxSize;
    }

    //--------------------------------------------------------------------------
    //! Get handler to the data buffer
    //--------------------------------------------------------------------------
    char* GetDataBuffer();

    //--------------------------------------------------------------------------
    //! Get the size of meaningful data
    //--------------------------------------------------------------------------
    size_t GetSizeData() const;

    //--------------------------------------------------------------------------
    //! Get total capacity of the object
    //--------------------------------------------------------------------------
    size_t GetCapacity() const;

    //--------------------------------------------------------------------------
    //! Get start offset value
    //--------------------------------------------------------------------------
    off_t  GetOffsetStart() const;

    //--------------------------------------------------------------------------
    //! Get end offset value
    //--------------------------------------------------------------------------
    off_t  GetOffsetEnd() const;

    //--------------------------------------------------------------------------
    //! Try to get a piece from the current block
    //!
    //! @param buf place where to save the data
    //! @param off offset
    //! @param len length
    //!
    //! @return true if piece found, otherwise false
    //!
    //--------------------------------------------------------------------------
    bool GetPiece( char* buf, off_t off, size_t len );

    //--------------------------------------------------------------------------
    //! Get handler to the parent file object
    //--------------------------------------------------------------------------
    FileAbstraction* GetParentFile() const;

    //--------------------------------------------------------------------------
    //! Test if block is for writing
    //--------------------------------------------------------------------------
    bool IsWr();

    //--------------------------------------------------------------------------
    //! Test if block full with meaningfull data
    //--------------------------------------------------------------------------
    bool IsFull();

    //--------------------------------------------------------------------------
    //! Method that does the actual writing
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
    size_t AddPiece( const char* buf, off_t off, size_t len );

    //--------------------------------------------------------------------------
    //! Method to recycle a previously used block
    //!
    //! @param file file layout type handler
    //! @param buf data buffer
    //! @param off offset
    //! @param len length
    //! @param rFileAbst FileAbstraction handler
    //! @param isWr set of entry is for writing
    //!
    //--------------------------------------------------------------------------
    void DoRecycle( eos::fst::Layout*& file,
                    char*              buf,
                    off_t              off,
                    size_t             len,
                    FileAbstraction&   rFileAbst,
                    bool               isWr );

  private:

    static size_t msMaxSize;     ///< max size of entry

    eos::fst::Layout* mpFile;    ///< file layout type handler
    bool mIsWrType;              ///< is write block type
    char*  mpBuffer;             ///< buffer of the object
    size_t mCapacity;            ///< total capcity 512 KB ~ 4MB
    size_t mSizeData;            ///< size of useful data
    off_t  mOffsetStart;         ///< offset relative to the file

    std::map<off_t, size_t> mMapPieces; ///< pieces read/to be written
    FileAbstraction* pParentFile;       ///< pointer to parent file
};

#endif

