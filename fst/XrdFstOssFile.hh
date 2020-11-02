//------------------------------------------------------------------------------
//! @file XrdFstOssFile.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Oss plugin for EOS dealing with files and adding block checksuming
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

#ifndef __EOSFST_FSTOSSFILE_HH__
#define __EOSFST_FSTOSSFILE_HH__

#include "fst/Namespace.hh"
#include "common/Logging.hh"
#include "XrdOss/XrdOss.hh"
#include <map>
#include <string>

//! Forward declarations
namespace eos
{
namespace common
{
class Buffer;
}
}

EOSFSTNAMESPACE_BEGIN

class CheckSum;

//------------------------------------------------------------------------------
//! Class XrdFstOssFile using blockxs information
//------------------------------------------------------------------------------
class XrdFstOssFile : public XrdOssDF, public eos::common::LogId
{
public:
  //--------------------------------------------------------------------------
  //! Constuctor
  //!
  //! @param tid
  //--------------------------------------------------------------------------
  XrdFstOssFile(const char* tid);

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  virtual ~XrdFstOssFile();

  //--------------------------------------------------------------------------
  //! Open function
  //!
  //! @param path file path
  //! @param flags open flags
  //! @param mode open mode
  //! @param env env variables passed to the function
  //!
  //! @return XrdOssOK upon success, -errno otherwise
  //--------------------------------------------------------------------------
  virtual int Open(const char* path, int flags, mode_t mode, XrdOucEnv& env);

  //--------------------------------------------------------------------------
  //! Read
  //!
  //! @param buffer data container for read operation
  //! @param offset file offet
  //! @param length read length
  //!
  //! @return number of bytes read
  //--------------------------------------------------------------------------
  ssize_t Read(void* buffer, off_t offset, size_t length);

  //--------------------------------------------------------------------------
  //! Read raw
  //!
  //! @param buffer data container for read operation
  //! @param offset file offet
  //! @param length read length
  //!
  //! @return number of bytes read
  //--------------------------------------------------------------------------
  ssize_t ReadRaw(void* buffer, off_t offset, size_t length);

  //--------------------------------------------------------------------------
  //! Vector read
  //!
  //! @param readV generic data structure for vector reads
  //! @param n number of individual reads in the vector request
  //!
  //! @return is successful total number of bytes read, otherwise -ESPIPE
  //--------------------------------------------------------------------------
  ssize_t ReadV(XrdOucIOVec* readV, int n);

  //--------------------------------------------------------------------------
  //! Vector write
  //!
  //! @param writeV generic data structure for vector writes
  //! @param n number of individual reads in the vector request
  //!
  //! @return is successful total number of bytes written, otherwise -ESPIPE
  //--------------------------------------------------------------------------
  ssize_t WriteV(XrdOucIOVec* writeV, int n);

  //--------------------------------------------------------------------------
  //! Write
  //!
  //! @param buffer data container for write operation
  //! @param offset file offet
  //! @param length write length
  //!
  //! @return number of byes written
  //--------------------------------------------------------------------------
  ssize_t Write(const void* buffer, off_t offset, size_t length);

  //--------------------------------------------------------------------------
  //! Chmod function
  //!
  //! @param mode the mode to set
  //!
  //! @return XrdOssOK upon success, (-errno) upon failure
  //--------------------------------------------------------------------------
  int Fchmod(mode_t mode);

  //--------------------------------------------------------------------------
  //! Get file status
  //!
  //! @param statinfo stat info structure
  //!
  //! @return XrdOssOK upon success, (-errno) upon failure
  //--------------------------------------------------------------------------
  int Fstat(struct stat* statinfo);

  //--------------------------------------------------------------------------
  //! Sync file to local disk
  //--------------------------------------------------------------------------
  int Fsync();

  //--------------------------------------------------------------------------
  //! Truncate the file
  //!
  //! @param offset truncate offset
  //!
  //! @return XrdOssOK upon success, -1 upon failure
  //--------------------------------------------------------------------------
  int Ftruncate(unsigned long long offset);

  //--------------------------------------------------------------------------
  //! Get file descriptor
  //--------------------------------------------------------------------------
  int getFD();

  //--------------------------------------------------------------------------
  //! Close function
  //!
  //! @param retsz
  //!
  //! @return XrdOssOK upon success, -1 otherwise
  //--------------------------------------------------------------------------
  virtual int Close(long long* retsz = 0);

private:
  XrdOucString mPath; ///< path of the file
  bool mIsRW; ///< mark if opened for rw operations
  XrdSysRWLock* mRWLockXs; ///< rw lock for the block xs
  CheckSum* mBlockXs; ///< block xs object
#ifdef IN_TEST_HARNESS
public:
#endif
  //--------------------------------------------------------------------------
  //! Align request to the blockchecksum offset so that the whole request is
  //! checksummed
  //!
  //! @param buffer buffer holding the data
  //! @param offset request offset
  //! @param lenght request length
  //! @param start_piece extra piece of buffer used to align at the
  //!        beginning to the blockxs boundary for the given read request
  //! @param end_piece extrace piece of buffer used to align at the end to
  //!        the blockxs boundary for the given read request
  //!
  //! @return vector of aligned requests. There should never be more than 3
  //!         requests since in worst case both ends are unaligned
  //--------------------------------------------------------------------------
  std::vector<XrdOucIOVec>
  AlignBuffer(void* buffer, off_t offset, size_t length,
              std::shared_ptr<eos::common::Buffer>& start_piece,
              std::shared_ptr<eos::common::Buffer>& end_piece);
};

EOSFSTNAMESPACE_END

#endif // __EOSFST_FSTOSSFILE_HH__
