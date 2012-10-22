//------------------------------------------------------------------------------
//! @file FileAbstraction.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Class that keeps track of the operations done at file level
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


#ifndef __EOS_FILEABSTRACTION_HH__
#define __EOS_FILEABSTRACTION_HH__

//------------------------------------------------------------------------------
#include <sys/types.h>
#include <XrdSys/XrdSysPthread.hh>
//------------------------------------------------------------------------------
#include "ConcurrentQueue.hh"
//------------------------------------------------------------------------------

//! Definition of an error occurring in a write operation
typedef std::pair<int, off_t> error_type;

//------------------------------------------------------------------------------
//! Class that keeps track of the operations done at file level
//------------------------------------------------------------------------------
class FileAbstraction
{
  public:

    //! Errors collected during writes
    ConcurrentQueue<error_type>* errorsQueue;

    // -------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param id file id
    //! @param ino file inode
    //!
    // -------------------------------------------------------------------------
    FileAbstraction( int id, unsigned long ino );

    // -------------------------------------------------------------------------
    //! Destructor
    // -------------------------------------------------------------------------
    ~FileAbstraction();

    // -------------------------------------------------------------------------
    //! Get file id
    // -------------------------------------------------------------------------
    int GetId() const;

    // -------------------------------------------------------------------------
    //! Get number of references held to the file
    // -------------------------------------------------------------------------
    int GetNoReferences();

    // -------------------------------------------------------------------------
    //! Get size of reads and writes in cache for the file
    // -------------------------------------------------------------------------
    size_t GetSizeRdWr();

    // -------------------------------------------------------------------------
    //! Get size reads in cache for the file
    // -------------------------------------------------------------------------
    size_t GetSizeReads();

    // -------------------------------------------------------------------------
    //! Get size of writes in cache for the file
    // -------------------------------------------------------------------------
    size_t GetSizeWrites();

    // -------------------------------------------------------------------------
    //! Get number of write blocks in cache for the file
    // -------------------------------------------------------------------------
    long long int GetNoWriteBlocks();

    // -------------------------------------------------------------------------
    //! Get inode value
    // -------------------------------------------------------------------------
    unsigned long GetInode() const;

    // -------------------------------------------------------------------------
    //! Get last possible key value
    // -------------------------------------------------------------------------
    long long GetLastPossibleKey() const;

    // -------------------------------------------------------------------------
    //! Get first possible key value
    // -------------------------------------------------------------------------
    long long GetFirstPossibleKey() const;

    // -------------------------------------------------------------------------
    //! Increment the size of writes
    //!
    //! @param sizeWrite size writes
    //! @param newBlock mark if a new write block is added
    //!
    // -------------------------------------------------------------------------
    void IncrementWrites( size_t sizeWrite, bool newBlock );

    // -------------------------------------------------------------------------
    //! Increment the size of reads
    //!
    //! @param sizeRead size reads
    //!
    // -------------------------------------------------------------------------
    void IncrementReads( size_t sizeRead );

    // -------------------------------------------------------------------------
    //! Decrement the size of writes
    //!
    //! @param sizeWrite size writes
    //! @param fullBlock mark if it is a full block
    //!
    // -------------------------------------------------------------------------
    void DecrementWrites( size_t sizeWrite, bool fullBlock );

    // -------------------------------------------------------------------------
    //! Decrement the size of reads
    //!
    //! @param sizeRead size reads
    //!
    // -------------------------------------------------------------------------
    void DecrementReads( size_t sizeRead );

    // -------------------------------------------------------------------------
    //! Increment the number of references
    // -------------------------------------------------------------------------
    void IncrementNoReferences();

    // -------------------------------------------------------------------------
    //! Decrement the number of references
    // -------------------------------------------------------------------------
    void DecrementNoReferences();

    // -------------------------------------------------------------------------
    //! Decide if the file is still in use
    //!
    //! @param strongConstraint if set use strong constraints
    //!
    //! @return true if file is in use, otherwise false
    //!
    // -------------------------------------------------------------------------
    bool IsInUse( bool strongConstraint );

    // -------------------------------------------------------------------------
    //! Method used to wait for writes to be done
    // -------------------------------------------------------------------------
    void WaitFinishWrites();

    // -------------------------------------------------------------------------
    //! Genereate block key
    //!
    //! @param offset offset piece
    //!
    //! @return block key
    //!
    // -------------------------------------------------------------------------
    long long int GenerateBlockKey( off_t offset );

    // -------------------------------------------------------------------------
    //! Get the queue of errros
    // -------------------------------------------------------------------------
    ConcurrentQueue<error_type>& GetErrorQueue() const;

  private:

    int mIdFile;                  ///< internally assigned key
    int mNoReferences;            ///< number of held referencess to this file
    unsigned long mInode;         ///< inode of current file
    size_t mSizeWrites;           ///< the size of write blocks in cache
    size_t mSizeReads;            ///< the size of read blocks in cache
    long long int mNoWrBlocks;    ///< no. of blocks in cache for this file

    long long mLastPossibleKey;   ///< last possible offset in file
    long long mFirstPossibleKey;  ///< first possible offset in file
    XrdSysCondVar mCondUpdate;    ///< cond variable for updating file attributes
};

#endif
