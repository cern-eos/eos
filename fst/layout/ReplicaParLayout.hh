//------------------------------------------------------------------------------
//! @file ReplicaParLayout.hh
//! @author Andreas-Joachim Peters - CERN
//! @brief Physical layout of a file with replicas
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

#ifndef __EOSFST_REPLICAPARLAYOUT_HH__
#define __EOSFST_REPLICAPARLAYOUT_HH__

#include "fst/layout/Layout.hh"
#include "fst/io/xrd/ResponseCollector.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class abstracting the physical layout of a file with replicas
//------------------------------------------------------------------------------
class ReplicaParLayout: public Layout
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
  //----------------------------------------------------------------------------
  ReplicaParLayout(XrdFstOfsFile* file,
                   unsigned long lid,
                   const XrdSecEntity* client,
                   XrdOucErrInfo* outError,
                   const char* path,
                   uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ReplicaParLayout();

  //--------------------------------------------------------------------------
  // Redirect to new target
  //--------------------------------------------------------------------------
  virtual void Redirect(const char* path);

  //----------------------------------------------------------------------------
  //! Open file
  //!
  //! @param path file path
  //! @param flags open flags
  //! @param mode open mode
  //! @param opaque opaque information
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int Open(
    XrdSfsFileOpenMode flags,
    mode_t mode,
    const char* opaque);

  //----------------------------------------------------------------------------
  //! Read from file
  //!
  //! @param offset offset
  //! @param buffer place to hold the read data
  //! @param length length
  //! @param readahead readahead switch
  //!
  //! @return number of bytes read or -1 if error
  //----------------------------------------------------------------------------
  virtual int64_t Read(XrdSfsFileOffset offset,
                       char* buffer,
                       XrdSfsXferSize length,
                       bool readahead = false);

  //----------------------------------------------------------------------------
  //! Vector read
  //!
  //! @param chunkList list of chunks for the vector read
  //! @param len total length of the vector read
  //!
  //! @return number of bytes read of -1 if error
  //----------------------------------------------------------------------------
  virtual int64_t ReadV(XrdCl::ChunkList& chunkList,
                        uint32_t len);

  //----------------------------------------------------------------------------
  //! Write to file
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //!
  //! @return number of bytes written or -1 if error
  //----------------------------------------------------------------------------
  virtual int64_t Write(XrdSfsFileOffset offset,
                        const char* buffer,
                        XrdSfsXferSize length);

  //----------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int Truncate(XrdSfsFileOffset offset);

  //----------------------------------------------------------------------------
  //! Allocate file space
  //!
  //! @param length space to be allocated
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int Fallocate(XrdSfsFileOffset length);

  //----------------------------------------------------------------------------
  //! Deallocate file space
  //!
  //! @param fromOffset offset start
  //! @param toOffset offset end
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int Fdeallocate(XrdSfsFileOffset fromOffset,
                          XrdSfsFileOffset toOffset);

  //----------------------------------------------------------------------------
  //! Remove file
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int Remove();

  //----------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int Sync();

  //----------------------------------------------------------------------------
  //! Get stats about the file
  //!
  //! @param buf stat buffer
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int Stat(struct stat* buf);

  //----------------------------------------------------------------------------
  //! Execute implementation dependant command
  //!
  //! @param cmd command
  //! @param client client identity
  //!
  //! @return 0 if successful, -1 otherwise
  //----------------------------------------------------------------------------
  virtual int Fctl(const std::string& cmd, const XrdSecEntity* client);

  //----------------------------------------------------------------------------
  //! Close file
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int Close();

private:
  ///! Max offset for async writes before trying to collect some responses
  static const uint64_t sMaxOffsetWrAsync {5 * 1024 * 1024 * 1024ull};
  int mNumReplicas; ///< number of replicas for current file
  bool mIoLocal; ///< mark if we are to do local IO
  std::atomic<bool> mHasWriteErr;
  std::atomic<bool> mDoAsyncWrite;
  ///! Replica file object, index 0 is the local file
  std::vector<FileIo*> mReplicaFile;
  ///! URLs for all the replica files
  std::vector<std::string> mReplicaUrl;
  ///! Vector of reponse collector for all replicas
  std::vector<ResponseCollector> mResponses;

  //----------------------------------------------------------------------------
  //! Write using async requests
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //!
  //! @return number of bytes written or -1 if error
  //----------------------------------------------------------------------------
  int64_t WriteAsync(XrdSfsFileOffset offset, const char* buffer,
                     XrdSfsXferSize length);
};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_REPLICAPARLAYOUT_HH__
