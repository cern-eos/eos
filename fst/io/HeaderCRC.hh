//------------------------------------------------------------------------------
//! @file HeaderCRC.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Header information present at the start of each stripe file
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

#ifndef __EOSFST_HEADERCRC_HH__
#define __EOSFST_HEADERCRC_HH__

/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "fst/Namespace.hh"
#include "fst/layout/FileIo.hh"
/*----------------------------------------------------------------------------*/
#include <XrdCl/XrdClFile.hh>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Header information present at the start of each stripe file
//------------------------------------------------------------------------------
class HeaderCRC: public eos::common::LogId
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    HeaderCRC();

  
    //--------------------------------------------------------------------------
    //! Constructor with parameter
    //--------------------------------------------------------------------------
    HeaderCRC( long numBlocks );

  
    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    ~HeaderCRC();

  
    //--------------------------------------------------------------------------
    //! Write header to file
    //!
    //! @param pFile file to which to header will be written
    //!
    //! @return status of the operation
    //!
    //--------------------------------------------------------------------------
    bool WriteToFile( XrdCl::File*& pFile );


    //--------------------------------------------------------------------------
    //! Write header to file
    //!
    //! @param pFile file to which to header will be written
    //!
    //! @return status of the operation
    //!
    //--------------------------------------------------------------------------
    bool WriteToFile( FileIo*& pFile );

  
    //--------------------------------------------------------------------------
    //! Read header from file
    //!
    //! @param pFile file from which the header will be read
    //!
    //! @return status of the operation
    //!
    //--------------------------------------------------------------------------
    bool ReadFromFile( XrdCl::File*& pFile );

  
    //--------------------------------------------------------------------------
    //! Read header from file
    //!
    //! @param pFile file from which the header will be read
    //!
    //! @return status of the operation
    //!
    //--------------------------------------------------------------------------
    bool ReadFromFile( FileIo*& pFile );
  

    //--------------------------------------------------------------------------
    //! Get tag of the header
    //--------------------------------------------------------------------------
    const char* GetTag() const {
      return mTag;
    }


    //--------------------------------------------------------------------------
    //! Get size of header
    //--------------------------------------------------------------------------
    static const int GetSize() {
      return msSizeHeader;
    }


    //--------------------------------------------------------------------------
    //! Get size of last block in file
    //--------------------------------------------------------------------------
    const size_t GetSizeLastBlock() const {
      return mSizeLastBlock;
    }

  
    //--------------------------------------------------------------------------
    //! Get number of blocks in file
    //--------------------------------------------------------------------------
    const long int GetNoBlocks() const {
      return mNumBlocks;
    }

  
    //--------------------------------------------------------------------------
    //! Get id of the stripe the header belongs to
    //--------------------------------------------------------------------------
    const unsigned int GetIdStripe() const {
      return mIdStripe;
    }

  
    //--------------------------------------------------------------------------
    //! Set number of blocks in the file
    //--------------------------------------------------------------------------
    void SetNoBlocks( long int numBlocks ) {
      mNumBlocks = numBlocks;
    }

  
    //--------------------------------------------------------------------------
    //! Set size of last block in the file
    //--------------------------------------------------------------------------
    void SetSizeLastBlock( size_t sizeLastBlock ) {
      mSizeLastBlock = sizeLastBlock;
    }

    //--------------------------------------------------------------------------
    //! Set id of the stripe the header belongs to
    //--------------------------------------------------------------------------
    void SetIdStripe( unsigned int stripe ) {
      mIdStripe = stripe;
    }
  

    //--------------------------------------------------------------------------
    //! Test if header is valid
    //--------------------------------------------------------------------------
    const bool IsValid() const {
      return mValid;
    }
  

    //--------------------------------------------------------------------------
    //! Set the header state (valid/corrupted)
    //--------------------------------------------------------------------------
    void SetState( bool state ) {
      mValid = state;
    }

  private:

    char mTag[16];           ///< layout tag
    bool mValid;             ///< status of the file
    long int mNumBlocks;     ///< total number of blocks
    unsigned int mIdStripe;  ///< index of the stripe the header belongs to
    size_t mSizeLastBlock;   ///< size of the last block of data

    static int msSizeHeader; ///< size of the header
    static char msTagName[]; ///< default tag name
};

EOSFSTNAMESPACE_END

#endif     // __EOSFST_HEADERCRC_HH__
