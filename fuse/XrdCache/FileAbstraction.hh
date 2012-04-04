// ----------------------------------------------------------------------
// File: FileAbstraction.hh
// Author: Elvin-Alin Sindrilaru - CERN
// ----------------------------------------------------------------------

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

/**
 * @file   FileAbstraction.hh
 *
 * @brief  Class representing a file object.
 *
 *
 */

#ifndef __EOS_FILEABSTRACTION_HH__
#define __EOS_FILEABSTRACTION_HH__

//------------------------------------------------------------------------------
#include <sys/types.h>
#include <XrdSys/XrdSysPthread.hh>
//------------------------------------------------------------------------------
#include "ConcurrentQueue.hh"
//------------------------------------------------------------------------------

typedef std::pair<int, off_t> error_type;

//------------------------------------------------------------------------------
//! Class that keeps track of the operations done at file level
//------------------------------------------------------------------------------
class FileAbstraction
{
public:

  ConcurrentQueue<error_type>* errorsQueue; //< errors collected during writes

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  FileAbstraction(int key, unsigned long ino);

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~FileAbstraction();

  // ---------------------------------------------------------------------------
  //! Get file id
  // ---------------------------------------------------------------------------
  int getId() const;

  // ---------------------------------------------------------------------------
  //! Get number of refernces held to the file
  // ---------------------------------------------------------------------------
  int getNoReferences();

  // ---------------------------------------------------------------------------
  //! Get size of reads and writes in cache for the file
  // ---------------------------------------------------------------------------
  size_t getSizeRdWr();

  // ---------------------------------------------------------------------------
  //! Get size reads in cache for the file
  // ---------------------------------------------------------------------------
  size_t getSizeReads();

  // ---------------------------------------------------------------------------
  //! Get size of writes in cache for the file
  // ---------------------------------------------------------------------------
  size_t getSizeWrites();

  // ---------------------------------------------------------------------------
  //! Get number of write blocks in cache for the file
  // ---------------------------------------------------------------------------
  long long int getNoWriteBlocks();

  // ---------------------------------------------------------------------------
  //! Get inode value
  // ---------------------------------------------------------------------------
  unsigned long getInode() const;

  // ---------------------------------------------------------------------------
  //! Get last possible key value
  // ---------------------------------------------------------------------------
  long long getLastPossibleKey() const;

  // ---------------------------------------------------------------------------
  //! Get first possible key value
  // ---------------------------------------------------------------------------
  long long getFirstPossibleKey() const;

  // ---------------------------------------------------------------------------
  //! Increment the size of writes 
  // ---------------------------------------------------------------------------
  void incrementWrites(size_t sWrite, bool newBlock);

  // ---------------------------------------------------------------------------
  //! Increment the size of reads
  // ---------------------------------------------------------------------------
  void incrementReads(size_t sRead);

  // ---------------------------------------------------------------------------
  //! Decrement the size of writes
  // ---------------------------------------------------------------------------
  void decrementWrites(size_t sWrite, bool fullBlock);

  // ---------------------------------------------------------------------------
  //! Decrement the size of reads 
  // ---------------------------------------------------------------------------
  void decrementReads(size_t sRead);

  // ---------------------------------------------------------------------------
  //! Increment the number of references
  // ---------------------------------------------------------------------------
  void incrementNoReferences();

  // ---------------------------------------------------------------------------
  //! Decrement the number of references
  // ---------------------------------------------------------------------------
  void decrementNoReferences();

  // ---------------------------------------------------------------------------
  //! Decide if the file is still in use
  // ---------------------------------------------------------------------------
  bool isInUse(bool strongConstraint);

  // ---------------------------------------------------------------------------
  //! Method used to wait for writes to be done
  // ---------------------------------------------------------------------------
  void waitFinishWrites();

  // ---------------------------------------------------------------------------
  //! Genereate block key
  // ---------------------------------------------------------------------------
  long long int generateBlockKey(off_t offsetEnd);

  // ---------------------------------------------------------------------------
  //! Get the queue of errros
  // ---------------------------------------------------------------------------
  ConcurrentQueue<error_type>& getErrorQueue() const;

private:

  int idFile;                   //< internally assigned key
  int nReferences;              //< number of held referencess to this file
  unsigned long inode;          //< inode of current file
  size_t sizeWrites;            //< the size of write blocks in cache
  size_t sizeReads;             //< the size of read blocks in cache
  long long int nWriteBlocks;   //< no of blocks in cache for this file

  long long lastPossibleKey;    //< last possible offset in file
  long long firstPossibleKey;   //< first possible offset in file

  XrdSysCondVar cUpdate;        //< cond variable for updating file attributes
};

#endif
