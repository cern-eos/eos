//------------------------------------------------------------------------------
// File: HeaderCRC.hh
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

#ifndef __EOSFST_HEADERCRC_HH__
#define __EOSFST_HEADERCRC_HH__

/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "fst/Namespace.hh"
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
    HeaderCRC( long noblocks );

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    ~HeaderCRC();

    //--------------------------------------------------------------------------
    //! Write header to file
    //!
    //! @param f file to which to header will be written
    //!
    //! @return status of the operation
    //--------------------------------------------------------------------------
    bool writeToFile( XrdCl::File* f );

    //--------------------------------------------------------------------------
    //! Read header from file
    //!
    //! @param f file from which the header will be read
    //!
    //! @return status of the operation
    //--------------------------------------------------------------------------
    bool readFromFile( XrdCl::File* f );

    //--------------------------------------------------------------------------
    //! Get tag of the header
    //--------------------------------------------------------------------------
    const char* getTag() const
    {
      return tag;
    };

    //--------------------------------------------------------------------------
    //! Get size of header
    //--------------------------------------------------------------------------
    static const int getSize()
    {
      return sizeHeader;
    };

    //--------------------------------------------------------------------------
    //! Get size of last block in file
    //--------------------------------------------------------------------------
    const size_t getSizeLastBlock() const
    {
      return sizeLastBlock;
    };

    //--------------------------------------------------------------------------
    //! Get number of blocks in file
    //--------------------------------------------------------------------------
    const long int getNoBlocks() const
    {
      return noBlocks;
    };

    //--------------------------------------------------------------------------
    //! Get id of the stripe the header belongs to
    //--------------------------------------------------------------------------
    const unsigned int getIdStripe() const
    {
      return idStripe;
    };

    //--------------------------------------------------------------------------
    //! Set number of blocks in the file
    //--------------------------------------------------------------------------
    void setNoBlocks( long int nblocks )
    {
      noBlocks = nblocks;
    };

    //--------------------------------------------------------------------------
    //! Set size of last block in the file
    //--------------------------------------------------------------------------
    void setSizeLastBlock( size_t sizelastblock )
    {
      sizeLastBlock = sizelastblock;
    };

    //--------------------------------------------------------------------------
    //! Set id of the stripe the header belongs to
    //--------------------------------------------------------------------------
    void setIdStripe( unsigned int idstripe )
    {
      idStripe = idstripe;
    };

    //--------------------------------------------------------------------------
    //! Test if header is valid
    //--------------------------------------------------------------------------
    const bool isValid() const
    {
      return valid;
    };

    //--------------------------------------------------------------------------
    //! Set the header state (valid/corrupted)
    //--------------------------------------------------------------------------
    void setState( bool state )
    {
      valid = state;
    };

  private:

    char tag[16];            //< layout tag
    bool valid;              //< status of the file
    long int noBlocks;       //< total number of blocks
    unsigned int idStripe;   //< index of the stripe the header belongs to
    size_t sizeLastBlock;    //< size of the last block of data

    static int sizeHeader;   //< size of the header
    static char tagName[];   //< default tag name

};

EOSFSTNAMESPACE_END

#endif     // __EOSFST_HEADERCRC_HH__
