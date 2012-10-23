//------------------------------------------------------------------------------
// File: FileIo.hh
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
//! @file FileIo.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Abstract class modelling an IO plugin
//------------------------------------------------------------------------------

#ifndef __EOSFST_FILEIO_HH__
#define __EOSFST_FILEIO_HH__

/*----------------------------------------------------------------------------*/
#include <string>
/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "fst/Namespace.hh"
#include "fst/XrdFstOfsFile.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class XrdFstOfsFile;

//------------------------------------------------------------------------------
//! The truncate offset (1TB) is used to indicate that a file should be deleted
//! during the close as there is no better interface usable via XrdClient to
//! communicate a deletion on a open file
//------------------------------------------------------------------------------
#define EOS_FST_DELETE_FLAG_VIA_TRUNCATE_LEN 1024 * 1024 * 1024 * 1024ll


//------------------------------------------------------------------------------
//! Abstract class modelling an IO plugin
//------------------------------------------------------------------------------
class FileIo: public eos::common::LogId
{
  public:
    //----------------------------------------------------------------------------
    //! Constructor
    //----------------------------------------------------------------------------
    FileIo( XrdFstOfsFile*      file,
            const XrdSecEntity* client,
            XrdOucErrInfo*      error ):
      mLocalPath( "" ),
      mLogicalFile( file ),
      mError( error ),
      mSecEntity( client )
    {
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
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Open( const std::string& path,
                      XrdSfsFileOpenMode flags,
                      mode_t             mode,
                      const std::string& opaque ) = 0;


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
                          XrdSfsXferSize   length ) = 0;


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
                           XrdSfsXferSize   length ) = 0;


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
                          void*            handler ) = 0;


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
                           void*            handler ) = 0;


    //--------------------------------------------------------------------------
    //! Truncate
    //!
    //! @param offset truncate file to this value
    //!
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Truncate( XrdSfsFileOffset offset ) = 0;


    //--------------------------------------------------------------------------
    //! Allocate file space
    //!
    //! @param length space to be allocated
    //!
    //! @return 0 on success, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Fallocate( XrdSfsFileOffset length ) {
      return 0;
    }


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
                             XrdSfsFileOffset toOffset ) {
      return 0;
    }


    //--------------------------------------------------------------------------
    //! Remove file
    //!
    //! @return 0 on success, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Remove() {
      return 0;
    }


    //--------------------------------------------------------------------------
    //! Sync file to disk
    //!
    //! @return 0 on success, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Sync() = 0;


    //--------------------------------------------------------------------------
    //! Close file
    //!
    //! @return 0 on success, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Close() = 0;


    //--------------------------------------------------------------------------
    //! Get stats about the file
    //!
    //! @param buf stat buffer
    //!
    //! @return 0 on success, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Stat( struct stat* buf ) = 0;

    //--------------------------------------------------------------------------
    //! Get path to current file
    //--------------------------------------------------------------------------
    const std::string& GetPath() {
      return mLocalPath;
    }

  protected:

    std::string         mLocalPath;   ///< path to current file
    XrdFstOfsFile*      mLogicalFile; ///< handler to logical file
    XrdOucErrInfo*      mError;       ///< error information
    const XrdSecEntity* mSecEntity;   ///< security entity
};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_FILEIO_HH__



