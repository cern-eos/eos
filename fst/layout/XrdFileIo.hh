//------------------------------------------------------------------------------
//! @file XrdClFileIo.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Class used for doing remote IO operations unsing the xrd client
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

#ifndef __EOSFST_XRDFILEIO_HH__
#define __EOSFST_XRDFILEIO_HH__

/*----------------------------------------------------------------------------*/
#include "fst/layout/FileIo.hh"
#include "fst/XrdFstOfsFile.hh"
#include "io/SimpleHandler.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClFile.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class AsyncMetaHandler;


//------------------------------------------------------------------------------
//! Struct that holds a readahead buffer and corresponding handler
//------------------------------------------------------------------------------
struct ReadaheadBlock {

  static const uint64_t sDefaultBlocksize; ///< default value for readahead

  //----------------------------------------------------------------------------
  //! Constuctor
  //!
  //! @param blocksize the size of the readahead
  //!
  //----------------------------------------------------------------------------
  ReadaheadBlock( uint64_t blocksize = sDefaultBlocksize ) {
    buffer = new char[blocksize];
    handler = new SimpleHandler();
  }


  //----------------------------------------------------------------------------
  //! Update current request
  //!
  //! @param offset offset
  //! @param length length
  //! @param isWrite true if write request, otherwise false
  //!
  //----------------------------------------------------------------------------
  void Update( uint64_t offset, uint32_t length, bool isWrite ) {
    handler->Update( offset, length, isWrite );
  }


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ReadaheadBlock() {
    delete[] buffer;
    delete handler;
  }

  char*          buffer;  ///< pointer to where the data is read
  SimpleHandler* handler; ///< async handler for the requests
};


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
    //! @param pFileHandler async handler for file
    //! @param readahead true if readahead is to be enabled, otherwise false
    //!
    //! @return number of bytes read or -1 if error
    //!
    //--------------------------------------------------------------------------
    virtual int64_t Read( XrdSfsFileOffset offset,
                          char*            buffer,
                          XrdSfsXferSize   length,
                          void*            pFileHandler,
                          bool             readahead = false );


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
                           void*            pFileHandler );


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

    int              mIndex;        ///< inded of readahead block in use ( 0 or 1 )
    bool             mDoReadahead;  ///< mark if readahead is enabled
    uint32_t         mBlocksize;    ///< block size for rd/wr opertations
    std::string      mPath;         ///< path to file
    XrdCl::File*     mXrdFile;      ///< handler to xrd file
    ReadaheadBlock** mReadahead;    ///< two blocks used for readahead

    //--------------------------------------------------------------------------
    //! Method used to prefetch the next block using the readahead mechanism
    //!
    //! @param offsetEnd end of the current block we are reading
    //! @param isWrite true if block is for write, false otherwise
    //!
    //--------------------------------------------------------------------------
    void PrefetchBlock( uint64_t offsetEnd, bool isWrite );


    //--------------------------------------------------------------------------
    //! Disable copy constructor
    //--------------------------------------------------------------------------
    XrdFileIo( const XrdFileIo& ) = delete;


    //--------------------------------------------------------------------------
    //! Disable assign operator
    //--------------------------------------------------------------------------
    XrdFileIo& operator =( const XrdFileIo& ) = delete;


};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_XRDFILEIO_HH__

