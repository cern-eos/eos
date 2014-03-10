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
//------------------------------------------------------------------------------
#include "common/ConcurrentQueue.hh"
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


//------------------------------------------------------------------------------
//! Class that keeps track of the operations done at file level
//------------------------------------------------------------------------------
class FileAbstraction
{
  public:

    //! Errors collected during writes
    eos::common::ConcurrentQueue<error_type>* errorsQueue;

    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param fd file descriptor
    //! @param file raw file object
    //!
    //--------------------------------------------------------------------------
    FileAbstraction(int fd, eos::fst::Layout* file);


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
    //! Get undelying raw file object
    //--------------------------------------------------------------------------
    inline eos::fst::Layout* GetRawFile() const
    {
      return mFile;
    };


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
    //! @param newBlock mark if a new write block is added
    //!
    //--------------------------------------------------------------------------
    void IncrementWrites(size_t sizeWrite, bool newBlock);


    //--------------------------------------------------------------------------
    //! Decrement the size of writes
    //!
    //! @param sizeWrite size writes
    //! @param fullBlock mark if it is a full block
    //!
    //--------------------------------------------------------------------------
    void DecrementWrites(size_t sizeWrite, bool fullBlock);


    //--------------------------------------------------------------------------
    //! Increment the number of references
    //--------------------------------------------------------------------------
    void IncNumRef();


    //--------------------------------------------------------------------------
    //! Decrement the number of references
    //--------------------------------------------------------------------------
    void DecNumRef();


    //--------------------------------------------------------------------------
    //! Decide if the file is still in use
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
    eos::common::ConcurrentQueue<error_type>& GetErrorQueue() const;

  
  private:

    int mFd; ///< file descriptor used for the block key range
    eos::fst::Layout* mFile; ///< raw file object
    int mNoReferences; ///< number of held referencess to this file
    size_t mSizeWrites; ///< the size of write blocks in cache
    long long int mNoWrBlocks; ///< no. of blocks in cache for this file
    long long mLastPossibleKey; ///< last possible offset in file
    long long mFirstPossibleKey; ///< first possible offset in file
    XrdSysCondVar mCondUpdate; ///< cond variable for updating file attributes
};

#endif // __EOS_FUSE_FILEABSTRACTION_HH__
