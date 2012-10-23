//------------------------------------------------------------------------------
// File: ReplicaParLayout.hh
// Author: Andreas-Joachim Peters - CERN
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
//! @file ReplicaParLayout.hh
//! @author Andreas-Joachim Peters - CERN
//! @brief Physical layout of a file with replicas
//------------------------------------------------------------------------------

#ifndef __EOSFST_REPLICAPARLAYOUT_HH__
#define __EOSFST_REPLICAPARLAYOUT_HH__

/*----------------------------------------------------------------------------*/
#include "fst/layout/Layout.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class abstracting the physical layout of a file with replicas
//------------------------------------------------------------------------------
class ReplicaParLayout : public Layout
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param file file handler
    //! @param lid layout id
    //! @param client security information
    //! @param error error information
    //!
    //--------------------------------------------------------------------------
    ReplicaParLayout( XrdFstOfsFile*      file,
                      int                 lid,
                      const XrdSecEntity* client,
                      XrdOucErrInfo*      outError );


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~ReplicaParLayout();


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
                      const char*        opaque );


    //--------------------------------------------------------------------------
    //! Read from file
    //!
    //! @param offset offset
    //! @param buffer place to hold the read data
    //! @param length length
    //!
    //! @return number of bytes read or -1 if error
    //!
    //--------------------------------------------------------------------------
    virtual int64_t Read( XrdSfsFileOffset offset,
                          char*            buffer,
                          XrdSfsXferSize   length );


    //--------------------------------------------------------------------------
    //! Write to file
    //!
    //! @param offset offset
    //! @paramm buffer data to be written
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
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Fallocate( XrdSfsFileOffset length );


    //--------------------------------------------------------------------------
    //! Deallocate file space
    //!
    //! @param fromOffset offset start
    //! @param toOffset offset end
    //!
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Fdeallocate( XrdSfsFileOffset fromOffset,
                             XrdSfsFileOffset toOffset );


    //--------------------------------------------------------------------------
    //! Remove file
    //!
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Remove();


    //--------------------------------------------------------------------------
    //! Sync file to disk
    //!
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Sync();


    //--------------------------------------------------------------------------
    //! Get stats about the file
    //!
    //! @param buf stat buffer
    //!
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Stat( struct stat* buf );


    //--------------------------------------------------------------------------
    //! Close file
    //!
    //! @return 0 if successful, -1 otherwise and error code is set
    //!
    //--------------------------------------------------------------------------
    virtual int Close();

  private:

    int  mNumReplicas;        ///< number of replicas for current file
    bool ioLocal;             ///< mark if we are to do local IO

    //! replica file object, index 0 is the local file
    std::vector<FileIo*>     mReplicaFile;
    std::vector<std::string> mReplicaUrl;  ///< URLs of the replica files

};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_REPLICAPARLAYOUT_HH__
