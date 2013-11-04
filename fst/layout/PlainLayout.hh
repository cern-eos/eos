//------------------------------------------------------------------------------
//! @file PlainLayout.hh
//! @author Elvin-Alin Sindrilaru / Andreas-Joachim Peters - CERN
//! @brief Layout of a plain file without any replication or striping
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

#ifndef __EOSFST_PLAINLAYOUT_HH__
#define __EOSFST_PLAINLAYOUT_HH__

/*----------------------------------------------------------------------------*/
#include "fst/layout/Layout.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class abstracting the physical layout of a plain file
//------------------------------------------------------------------------------
class PlainLayout : public Layout
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param file file handler
  //! @param lid layout id
  //! @param client security information
  //! @param error error information
  //! @param io io access type ( ofs/xrd )
  //! @param timeout timeout value
  //!
  //----------------------------------------------------------------------------
  PlainLayout (XrdFstOfsFile* file,
               unsigned long lid,
               const XrdSecEntity* client,
               XrdOucErrInfo* outError,
               eos::common::LayoutId::eIoType io,
               uint16_t timeout = 0);


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~PlainLayout ();


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
  //----------------------------------------------------------------------------
  virtual int Open (const std::string& path,
                    XrdSfsFileOpenMode flags,
                    mode_t mode,
                    const char* opaque = "");


  //----------------------------------------------------------------------------
  //! Read from file
  //!
  //! @param offset offset
  //! @param buffer place to hold the read data
  //! @param length length
  //!
  //! @return number of bytes read or -1 if error
  //!
  //----------------------------------------------------------------------------
  virtual int64_t Read (XrdSfsFileOffset offset,
                        char* buffer,
                        XrdSfsXferSize length);

  
  //----------------------------------------------------------------------------
  //! Vector read 
  //!
  //! @param chunkList list of chunks for the vector read
  //! @param len total length of the vector read
  //!
  //! @return number of bytes read of -1 if error
  //!
  //----------------------------------------------------------------------------
  virtual int64_t ReadV (XrdCl::ChunkList& chunkList,
                         uint32_t len);

  
  //----------------------------------------------------------------------------
  //! Write to file
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //!
  //! @return number of bytes written or -1 if error
  //!
  //----------------------------------------------------------------------------
  virtual int64_t Write (XrdSfsFileOffset offset,
                         const char* buffer,
                         XrdSfsXferSize length);


  //----------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //----------------------------------------------------------------------------
  virtual int Truncate (XrdSfsFileOffset offset);


  //----------------------------------------------------------------------------
  //! Allocate file space
  //!
  //! @param length space to be allocated
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //----------------------------------------------------------------------------
  virtual int Fallocate (XrdSfsFileOffset length);


  //----------------------------------------------------------------------------
  //! Deallocate file space
  //!
  //! @param fromOffset offset start
  //! @param toOffset offset end
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //----------------------------------------------------------------------------
  virtual int Fdeallocate (XrdSfsFileOffset fromOffset,
                           XrdSfsFileOffset toOffset);


  //----------------------------------------------------------------------------
  //! Remove file
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //----------------------------------------------------------------------------
  virtual int Remove ();


  //----------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //----------------------------------------------------------------------------
  virtual int Sync ();


  //----------------------------------------------------------------------------
  //! Get stats about the file
  //!
  //! @param buf stat buffer
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //----------------------------------------------------------------------------
  virtual int Stat (struct stat* buf);


  //----------------------------------------------------------------------------
  //! Close file
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //!
  //----------------------------------------------------------------------------
  virtual int Close ();

  
private:

  // TODO: add an async handler and if we are doing a remote access using
  //       XrdCl then we should try to do async requests

  FileIo* mPlainFile; ///< file handler, in this case the same as the initial one

  //----------------------------------------------------------------------------
  //! Disable copy constructor
  //----------------------------------------------------------------------------
  PlainLayout (const PlainLayout&) = delete;


  //----------------------------------------------------------------------------
  //! Disable assign operator
  //----------------------------------------------------------------------------
  PlainLayout& operator = (const PlainLayout&) = delete;

};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_PLAINLAYOUT_HH__
