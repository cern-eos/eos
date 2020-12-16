//------------------------------------------------------------------------------
// File FileAbstraction.hh
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch> CERN
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

#ifndef __EOS_FUSE_FILEABSTRACTION_HH__
#define __EOS_FUSE_FILEABSTRACTION_HH__

//------------------------------------------------------------------------------
#include <utility>
#include <sys/types.h>
//------------------------------------------------------------------------------
#include <XrdSys/XrdSysPthread.hh>
#include "fst/layout/Layout.hh"
//------------------------------------------------------------------------------
#include "common/ConcurrentQueue.hh"
#include "common/RWMutex.hh"
//------------------------------------------------------------------------------

//! Forward declaration
namespace eos
{
namespace fst
{
class Layout;
}
};

//! Definition of an error occurring in a write operation
typedef std::pair<int, off_t> error_type;

class LayoutWrapper;

//------------------------------------------------------------------------------
//! Class that keeps track of the operations done at file level
//------------------------------------------------------------------------------
class FileAbstraction
{
public:
  //! Errors collected during writes
  eos::common::ConcurrentQueue<error_type>* errorsQueue;
  XrdSysRWLock mMutexRW; ///< RW mutex for file access

  //--------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param fd file descriptor
  //! @param file raw file object
  //--------------------------------------------------------------------------
  FileAbstraction(const char* path = "");

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  ~FileAbstraction();

  //--------------------------------------------------------------------------
  //! Get size of writes in cache for current file
  //--------------------------------------------------------------------------
  size_t GetSizeWrites();

  //--------------------------------------------------------------------------
  //! Get number of write blocks in cache for the file
  //--------------------------------------------------------------------------
  long long int GetNoWriteBlocks();

  //--------------------------------------------------------------------------
  //! Get fd value
  //--------------------------------------------------------------------------
  inline int GetFd() const
  {
    return mFd;
  };

  //--------------------------------------------------------------------------
  //! Set fd value
  //--------------------------------------------------------------------------
  inline void SetFd(const int& fd)
  {
    mFd = fd;
    mFirstPossibleKey = static_cast<long long>(1e14 * mFd);
    mLastPossibleKey = static_cast<long long>((1e14 * (mFd + 1)));
    eos_static_debug("ptr_obj=%p, first_key=%llu, last_key=%llu",
                     this, mFirstPossibleKey, mLastPossibleKey);
  };

  //--------------------------------------------------------------------------
  //! Get undelying raw file object
  //--------------------------------------------------------------------------
  inline LayoutWrapper* GetRawFileRW() const
  {
    return mFileRW;
  };

  //--------------------------------------------------------------------------
  //! Set undelying raw file object
  //--------------------------------------------------------------------------
  void SetRawFileRW(LayoutWrapper* file);

  //--------------------------------------------------------------------------
  //! Get undelying raw file object for RO
  //--------------------------------------------------------------------------
  inline LayoutWrapper* GetRawFileRO() const
  {
    return mFileRO;
  };

  //--------------------------------------------------------------------------
  //! Set undelying raw file object
  //--------------------------------------------------------------------------
  void SetRawFileRO(LayoutWrapper* file);


  //--------------------------------------------------------------------------
  //! Get first possible key value
  //--------------------------------------------------------------------------
  inline long long GetFirstPossibleKey() const
  {
    return mFirstPossibleKey;
  };

  //--------------------------------------------------------------------------
  //! Get last possible key value
  //--------------------------------------------------------------------------
  inline long long GetLastPossibleKey() const
  {
    return mLastPossibleKey;
  };

  //--------------------------------------------------------------------------
  //! Increment the size of writes
  //!
  //! @param sizeWrite size writes
  //!
  //--------------------------------------------------------------------------
  void IncrementWrites(size_t sizeWrite);

  //--------------------------------------------------------------------------
  //! Decrement the size of writes
  //!
  //! @param sizeWrite size writes
  //!
  //--------------------------------------------------------------------------
  void DecrementWrites(size_t sizeWrite);

  //--------------------------------------------------------------------------
  //! Increment the number of open requests
  //--------------------------------------------------------------------------
  void IncNumOpenRW();

  //--------------------------------------------------------------------------
  //! Decrement the number of open requests
  //--------------------------------------------------------------------------
  void DecNumOpenRW();

  //--------------------------------------------------------------------------
  //! Increment the number of open requests ni RO
  //--------------------------------------------------------------------------
  void IncNumOpenRO();

  //--------------------------------------------------------------------------
  //! Decrement the number of open requests in RO
  //--------------------------------------------------------------------------
  void DecNumOpenRO();

  //--------------------------------------------------------------------------
  //! Increment the number of references
  //--------------------------------------------------------------------------
  void IncNumRefRW();


  //--------------------------------------------------------------------------
  //! Decrement the number of references
  //--------------------------------------------------------------------------
  void DecNumRefRW();

  //--------------------------------------------------------------------------
  //! Increment the number of references in RO
  //--------------------------------------------------------------------------
  void IncNumRefRO();

  //--------------------------------------------------------------------------
  //! Decrement the number of references in RO
  //--------------------------------------------------------------------------
  void DecNumRefRO();

  //--------------------------------------------------------------------------
  //! Decide if the file is still in use for RW access
  //!
  //! @return true if file is in use, otherwise false
  //!
  //--------------------------------------------------------------------------
  bool IsInUseRW();

  //--------------------------------------------------------------------------
  //! Decide if the file is still in use for RO access
  //!
  //! @return true if file is in use, otherwise false
  //!
  //--------------------------------------------------------------------------
  bool IsInUseRO();

  //--------------------------------------------------------------------------
  //! Decide if the file is still in use at all (RO or RW)
  //!
  //! @return true if file is in use, otherwise false
  //!
  //--------------------------------------------------------------------------
  bool IsInUse();

  //--------------------------------------------------------------------------
  //! Method used to wait for writes to be done
  //--------------------------------------------------------------------------
  void WaitFinishWrites();

  //--------------------------------------------------------------------------
  //! Clean all internal caches (read-ahead cache)
  //--------------------------------------------------------------------------
  void CleanReadCache();

  //--------------------------------------------------------------------------
  //! Genereate block key
  //!
  //! @param offset offset piece
  //!
  //! @return block key
  //!
  //--------------------------------------------------------------------------
  long long int GenerateBlockKey(off_t offset);

  //--------------------------------------------------------------------------
  //! Get the queue of errros
  //--------------------------------------------------------------------------
  std::queue<error_type> GetErrorQueue();

  //--------------------------------------------------------------------------
  //! Set a new utime on a file
  //--------------------------------------------------------------------------
  void SetUtimes(struct timespec* utime);

  //--------------------------------------------------------------------------
  //! Get last utime setting of a file and the path to it
  //--------------------------------------------------------------------------
  const char* GetUtimes(struct timespec* utime);

  //--------------------------------------------------------------------------
  //! Conditionally increase the max write offset if offset is bigger
  //--------------------------------------------------------------------------
  void TestMaxWriteOffset(off_t offset);

  void GrabMaxWriteOffset();

  void GrabUtimes();

  //--------------------------------------------------------------------------
  //! Set the max write offset to offset
  //--------------------------------------------------------------------------
  void SetMaxWriteOffset(off_t offset);

  //--------------------------------------------------------------------------
  //! Get the max write offset
  //--------------------------------------------------------------------------
  off_t GetMaxWriteOffset();

  eos::common::RWMutexR
  mInUse; ///< we use this look to indicate that someone has a reference to it

private:
  int mFd; ///< file descriptor used for the block key range
  LayoutWrapper* mFileRW; ///< raw file object for RW access
  LayoutWrapper* mFileRO; ///< raw file object for RO access
  int mNoReferencesRW; ///< number of held referencess to this file in RW
  int mNoReferencesRO; ///< number of held referencess to this file in RO
  int mNumOpenRW; ///< number of open request without a matching close in RW
  int mNumOpenRO; ///< number of open request without a matching close in RO
  size_t mSizeWrites; ///< the size of write blocks in cache
  long long mLastPossibleKey; ///< last possible offset in file
  long long mFirstPossibleKey; ///< first possible offset in file
  XrdSysCondVar mCondUpdate; ///< cond variable for updating file attributes
  XrdSysMutex mUtimeMutex;///<protect utime changes
  struct timespec
    mUtime[2]; ///< cond variable tracking last set utime while file is still open
  std::string mPath; ///< valid path to this file
  XrdSysMutex mMaxWriteOffsetMutex; ///< mutex protecting the maximum write offset
  off_t mMaxWriteOffset; ///< maximum written offset
};

#endif // __EOS_FUSE_FILEABSTRACTION_HH__
