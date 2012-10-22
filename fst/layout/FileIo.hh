//------------------------------------------------------------------------------
//! @file FileIo.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Abstract class modelling an IO plugin
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

#ifndef __EOSFST_FILEIO__HH__
#define __EOSFST_FILEIO__HH__


/*----------------------------------------------------------------------------*/
#include <string>
/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "common/Logging.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Abstract class modelling an IO plugin
//------------------------------------------------------------------------------
class FileIo: public eos::common::LogId
{
  public:
    //----------------------------------------------------------------------------
    //! Constructor
    //----------------------------------------------------------------------------
    FileIo( const XrdSecEntity* client,
            XrdOucErrInfo*      error ):
      mError( error ),
      mSecEntity( client ) {
      //empty
    }


    //----------------------------------------------------------------------------
    //! Destructor
    //----------------------------------------------------------------------------
    virtual ~FileIo() {
      //empty
    }


    //----------------------------------------------------------------------------
    //! Open file
    //!
    //! @param path file path
    //! @param flags open flags 
    //! @param mode open mode
    //! @param opaque opaque information
    //!
    //! @return 0 if successful, error code otherwise
    //!
    //----------------------------------------------------------------------------
    virtual int Open( const std::string& path,
                      uint16_t           flags,
                      uint16_t           mode,
                      const std::string& opaque ) = 0;


    //----------------------------------------------------------------------------
    //! Read from file
    //!
    //! @param offset offset in file
    //! @param buffer where the data is read
    //! @param lenght read length
    //!
    //! @return number og bytes read
    //!
    //----------------------------------------------------------------------------
    virtual uint32_t Read( uint64_t offset, char* buffer, uint32_t length ) = 0;


    //--------------------------------------------------------------------------
    //! Write to file
    //!
    //! @param offset offset
    //! @paramm buffer data to be written
    //! @param length length
    //!
    //! @return number of bytes written
    //!
    //--------------------------------------------------------------------------
    virtual uint32_t Write( uint64_t offset, char* buffer, uint32_t length ) = 0;


    //--------------------------------------------------------------------------
    //! Truncate
    //!
    //! @param offset truncate file to this value
    //!
    //! @return 0 if successful, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Truncate( uint64_t offset ) = 0;


    //--------------------------------------------------------------------------
    //! Allocate file space
    //!
    //! @param length space to be allocated
    //!
    //! @return 0 on success, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Fallocate( uint64_t lenght ) {
      return 0;
    }


    //--------------------------------------------------------------------------
    //! Deallocate file space
    //!
    //! @param fromOffset offset start
    //! @param toOffset offset end
    //!
    //! @return 0 on success, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Fdeallocate( uint64_t fromOffset, uint64_t toOffset ) {
      return 0;
    }


    //--------------------------------------------------------------------------
    //! Remove file
    //!
    //! @return 0 on success, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Remove() {
      return 0;
    }


    //--------------------------------------------------------------------------
    //! Sync file to disk
    //!
    //! @return 0 on success, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Sync() = 0;


    //--------------------------------------------------------------------------
    //! Close file
    //!
    //! @return 0 on success, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Close() = 0;


    //--------------------------------------------------------------------------
    //! Get stats about the file
    //!
    //! @param buf stat buffer
    //!
    //! @return 0 on success, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Stat( struct stat* buf ) = 0;

  protected:

    std::string         mPath;
    XrdOucErrInfo*      mError;
    const XrdSecEntity* mSecEntity;
};

EOSFSTNAMESPACE_END

#endif



