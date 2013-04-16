//------------------------------------------------------------------------------
// File: FileEos.hh
// Author: Elvin-Alin Sindrilaru - CERN
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

#ifndef __EOSBMK_FILEEOS_HH__
#define __EOSBMK_FILEEOS_HH__

/*-----------------------------------------------------------------------------*/
#include <string>
/*-----------------------------------------------------------------------------*/
#include "Namespace.hh"
#include "common/Logging.hh"
/*-----------------------------------------------------------------------------*/

EOSBMKNAMESPACE_BEGIN

//! Forward declaration
class Result;

//------------------------------------------------------------------------------
// Class FileEos
//------------------------------------------------------------------------------
class FileEos: public eos::common::LogId
{
 public:
  
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param filePath path of the file on which to do operations
  //! @param bmkInstance benchmark instance name ex. eosdev.cern.ch
  //! @param fileSize size of the file to be manipulated
  //! @param blockSize block size used for operations
  //!
  //----------------------------------------------------------------------------
  FileEos(const std::string& filePath,
          const std::string& bmkInstance,
          uint64_t           fileSize,
          uint32_t           blockSize);


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FileEos();


  //----------------------------------------------------------------------------
  //! Execute write operation
  //!
  //! @return 0 if successful, otherwise -1
  //!
  //----------------------------------------------------------------------------
  int Write(Result*& result);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int ReadGw(Result*& result);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int ReadPio(Result*& result);

  
  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int ReadWriteGw(Result*& result);


  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int ReadWritePio(Result*& result);
  
 private:

  std::string mFilePath;     ///< file path
  std::string mBmkInstance;  ///< benchmark instance
  uint64_t mFileSize;        ///< file size
  uint32_t mBlockSize;       ///< block size for operations

};

EOSBMKNAMESPACE_END

#endif // __EOSBMK_FILEEOS_HH__
