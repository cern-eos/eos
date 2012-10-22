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
    //! @param key key value
    //! @param ino file inode
    //!
    // -------------------------------------------------------------------------
    FileAbstraction( int key, unsigned long ino );

    // -------------------------------------------------------------------------
    //! Destructor
    // -------------------------------------------------------------------------
    ~FileAbstraction();

    // -------------------------------------------------------------------------
    //! Get file id
    // -------------------------------------------------------------------------
    int GetId() const;

    // -------------------------------------------------------------------------
    //! Get number of refernces held to the file
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
    //! @param s_write size writes
    //! @param new_block mark if a new write block is added
    //!
    // -------------------------------------------------------------------------
    void IncrementWrites( size_t s_write, bool new_block );

    // -------------------------------------------------------------------------
    //! Increment the size of reads
    //!
    //! @param s_read size reads
    //!
    // -------------------------------------------------------------------------
    void IncrementReads( size_t s_read );

    // -------------------------------------------------------------------------
    //! Decrement the size of writes
    //!
    //! @param s_write size writes
    //! @param full_block mark if it is a full block
    //!
    // -------------------------------------------------------------------------
    void DecrementWrites( size_t s_write, bool full_block );

    // -------------------------------------------------------------------------
    //! Decrement the size of reads
    //!
    //! @param s_read size reads
    //!
    // -------------------------------------------------------------------------
    void DecrementReads( size_t s_read );

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
    //! @param strong_constraint if set use strong constraints
    //!
    //! @return true if file is in use, otherwise false
    //!
    // -------------------------------------------------------------------------
    bool IsInUse( bool strong_constraint );

    // -------------------------------------------------------------------------
    //! Method used to wait for writes to be done
    // -------------------------------------------------------------------------
    void WaitFinishWrites();

    // -------------------------------------------------------------------------
    //! Genereate block key
    //!
    //! @param off_end offset end
    //!
    //! @return block key
    //!
    // -------------------------------------------------------------------------
    long long int GenerateBlockKey( off_t off_end );

    // -------------------------------------------------------------------------
    //! Get the queue of errros
    // -------------------------------------------------------------------------
    ConcurrentQueue<error_type>& GetErrorQueue() const;

  private:

    int id_file;                  ///< internally assigned key
    int no_references;            ///< number of held referencess to this file
    unsigned long inode;          ///< inode of current file
    size_t size_writes;           ///< the size of write blocks in cache
    size_t size_reads;            ///< the size of read blocks in cache
    long long int no_wr_blocks;   ///< no. of blocks in cache for this file

    long long last_possible_key;  ///< last possible offset in file
    long long first_possible_key; ///< first possible offset in file

    XrdSysCondVar cond_update;    ///< cond variable for updating file attributes
};

#endif
