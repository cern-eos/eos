//------------------------------------------------------------------------------
// File: LocalFileIo.hh
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

//------------------------------------------------------------------------------
//! @file LocalFileIo.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Class used for doing local IO operations
//------------------------------------------------------------------------------

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
    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param handle to logical file
    //! @param client security entity
    //! @param error error information
    //!
    //--------------------------------------------------------------------------
    LocalFileIo( XrdFstOfsFile*      file,
                 const XrdSecEntity* client,
                 XrdOucErrInfo*      error );


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~LocalFileIo();


    //--------------------------------------------------------------------------
    //! Open file
    //!
    //! @param path file path
    //! @param flags open flags
    //! @param mode open mode
    //! @param opaque opaque information
    //!
    //! @return 0 on success, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Open( const std::string& path,
                      XrdSfsFileOpenMode flags,
                      mode_t             mode,
                      const std::string& opaque );


    //--------------------------------------------------------------------------
    //! Read from file - sync
    //!
    //! @param offset offset in file
    //! @param buffer where the data is read
    //! @param length read length
    //!
    //! @return number of bytes read or -1 if error
    //!
    //--------------------------------------------------------------------------
    virtual int64_t Read( XrdSfsFileOffset offset,
                          char*            buffer,
                          XrdSfsXferSize   length );


    //--------------------------------------------------------------------------
    //! Write to file - sync
    //!
    //! @param offset offset in file
    //! @param buffer data to be written
    //! @param length length
    //!
    //! @return number of bytes written or -1 if error
    //!
    //--------------------------------------------------------------------------
    virtual int64_t Write( XrdSfsFileOffset offset,
                           char*            buffer,
                           XrdSfsXferSize   length );


    //--------------------------------------------------------------------------
    //! Truncate
    //!
    //! @param offset truncate file to this value
    //!
    //! @return 0 on success, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Truncate( XrdSfsFileOffset offset );


    //--------------------------------------------------------------------------
    //! Allocate file space
    //!
    //! @param length space to be allocated
    //!
    //! @return 0 on success, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Fallocate( XrdSfsFileOffset lenght );


    //--------------------------------------------------------------------------
    //! Deallocate file space
    //!
    //! @param fromOffset offset start
    //! @param toOffset offset end
    //!
    //! @return 0 on success, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Fdeallocate( XrdSfsFileOffset fromOffset,
                             XrdSfsFileOffset toOffset );


    //--------------------------------------------------------------------------
    //! Remove file
    //!
    //! @return 0 on success, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Remove();


    //--------------------------------------------------------------------------
    //! Sync file to disk
    //!
    //! @return 0 on success, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Sync();


    //--------------------------------------------------------------------------
    //! Close file
    //!
    //! @return 0 on success, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Close();


    //--------------------------------------------------------------------------
    //! Get stats about the file
    //!
    //! @param buf stat buffer
    //!
    //! @return 0 on success, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Stat( struct stat* buf );

  private:

    bool mIsOpen;  ///< mark if file is opened


    //--------------------------------------------------------------------------
    //! Read from file - async - not used yet
    //!
    //! @param offset offset in file
    //! @param buffer where the data is read
    //! @param length read length
    //! @param handler async read handler
    //!
    //! @return number of bytes read or -1 if error
    //!
    //--------------------------------------------------------------------------
    virtual int64_t Read( XrdSfsFileOffset offset,
                          char*            buffer,
                          XrdSfsXferSize   length,
                          void*            handler );


    //--------------------------------------------------------------------------
    //! Write to file - async - not used yet
    //!
    //! @param offset offset
    //! @param buffer data to be written
    //! @param length length
    //! @param handler async write handler
    //!
    //! @return number of bytes written or -1 if error
    //!
    //--------------------------------------------------------------------------
    virtual int64_t Write( XrdSfsFileOffset offset,
                           char*            buffer,
                           XrdSfsXferSize   length,
                           void*            handler );

};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_LOCALFILEIO_HH__



