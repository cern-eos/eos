//------------------------------------------------------------------------------
//! @file LocalFileIo.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Class used for doing local IO operations
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

#ifndef __EOSFST_LOCALFILEIO__HH__
#define __EOSFST_LOCALFILEIO__HH__

/*----------------------------------------------------------------------------*/
#include "fst/layout/FileIo.hh"
#include "fst/XrdFstOfsFile.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class used for doing local IO operations
//------------------------------------------------------------------------------
class LocalFileIo: public FileIo
{

  public:
    //----------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param handle to logical file
    //! @param client security entity
    //! @param error error information
    //!
    //----------------------------------------------------------------------------
    LocalFileIo( XrdFstOfsFile*      file,
                 const XrdSecEntity* client,
                 XrdOucErrInfo*      error );


    //----------------------------------------------------------------------------
    //! Destructor
    //----------------------------------------------------------------------------
    virtual ~LocalFileIo();


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
                      const std::string& opaque );


    //----------------------------------------------------------------------------
    //! Read from file
    //!
    //! @param offset offset in file
    //! @param buffer where the data is read
    //! @param lenght read length
    //!
    //! @return number of bytes read
    //!
    //----------------------------------------------------------------------------
    virtual uint32_t Read( uint64_t offset, char* buffer, uint32_t length );


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
    virtual uint32_t Write( uint64_t offset, char* buffer, uint32_t length );


    //--------------------------------------------------------------------------
    //! Truncate
    //!
    //! @param offset truncate file to this value
    //!
    //! @return 0 if successful, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Truncate( uint64_t offset );


    //--------------------------------------------------------------------------
    //! Allocate file space
    //!
    //! @param length space to be allocated
    //!
    //! @return 0 on success, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Fallocate( uint64_t lenght );


    //--------------------------------------------------------------------------
    //! Deallocate file space
    //!
    //! @param fromOffset offset start
    //! @param toOffset offset end
    //!
    //! @return 0 on success, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Fdeallocate( uint64_t fromOffset, uint64_t toOffset );


    //--------------------------------------------------------------------------
    //! Remove file
    //!
    //! @return 0 on success, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Remove();


    //--------------------------------------------------------------------------
    //! Sync file to disk
    //!
    //! @return 0 on success, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Sync();


    //--------------------------------------------------------------------------
    //! Close file
    //!
    //! @return 0 on success, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Close();


    //--------------------------------------------------------------------------
    //! Get stats about the file
    //!
    //! @param buf stat buffer
    //!
    //! @return 0 on success, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Stat( struct stat* buf );

  private:

    bool mIsOpen;             ///< mark if file is opened
    XrdFstOfsFile* mOfsFile;  ///< local file handler

};

EOSFSTNAMESPACE_END

#endif



