//------------------------------------------------------------------------------
// File: XrdClFileIo.hh
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
//! @file XrdClFileIo.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Class used for doing remote IO operations unsing the xrd client
//------------------------------------------------------------------------------

#ifndef __EOSFST_XRDFILEIO_HH__
#define __EOSFST_XRDFILEIO_HH__

/*----------------------------------------------------------------------------*/
#include "fst/layout/FileIo.hh"
#include "fst/XrdFstOfsFile.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class used for doing remote IO operations using the Xrd client
//------------------------------------------------------------------------------
class XrdFileIo: public FileIo
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
    XrdFileIo( XrdFstOfsFile*      file,
               const XrdSecEntity* client,
               XrdOucErrInfo*      error );


    //----------------------------------------------------------------------------
    //! Destructor
    //----------------------------------------------------------------------------
    virtual ~XrdFileIo();


    //----------------------------------------------------------------------------
    //! Open file
    //!
    //! @param path file path
    //! @param flags open flags
    //! @param mode open mode
    //! @param opaque opaque information
    //!
    //! @return 0 on success, -1 otherwise and error code is set
    //!
    //----------------------------------------------------------------------------
    virtual int Open( const std::string& path,
                      XrdSfsFileOpenMode flags,
                      mode_t             mode = 0,
                      const std::string& opaque = "" );


    //----------------------------------------------------------------------------
    //! Read from file - sync
    //!
    //! @param offset offset in file
    //! @param buffer where the data is read
    //! @param length read length
    //!
    //! @return number of bytes read or -1 if error
    //!
    //----------------------------------------------------------------------------
    virtual int64_t Read( XrdSfsFileOffset offset,
                          char*            buffer,
                          XrdSfsXferSize   length );


    //--------------------------------------------------------------------------
    //! Write to file - sync
    //!
    //! @param offset offset
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
    //! Read from file - async
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
    //! Write to file - async
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

    std::string mPath;      ///< path to file
    XrdCl::File* mXrdFile;  ///< handler to xrd file

};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_XRDFILEIO_HH__



