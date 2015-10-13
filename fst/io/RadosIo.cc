//------------------------------------------------------------------------------
// File: RadosIo.cc
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

/*----------------------------------------------------------------------------*/
#include "fst/XrdFstOfsFile.hh"
#include "fst/io/RadosIo.hh"
/*----------------------------------------------------------------------------*/
#ifndef __APPLE__
#include <xfs/xfs.h>
#endif

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RadosIo::RadosIo (XrdFstOfsFile* file,
                  const XrdSecEntity* client) :
FileIo (),
mLogicalFile (file),
mSecEntity (client)
{
  //............................................................................
  // In this case the logical file is the same as the local physical file
  //............................................................................
  // empty
  mType = "RadosIO";
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

RadosIo::~RadosIo ()
{
  //empty
}


//------------------------------------------------------------------------------
// Open file
//------------------------------------------------------------------------------

int
RadosIo::Open (const std::string& path,
               XrdSfsFileOpenMode flags,
               mode_t mode,
               const std::string& opaque,
               uint16_t timeout)
{
  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Read from file - sync
//------------------------------------------------------------------------------

int64_t
RadosIo::Read (XrdSfsFileOffset offset,
               char* buffer,
               XrdSfsXferSize length,
               uint16_t timeout)
{
  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Write to file - sync
//------------------------------------------------------------------------------

int64_t
RadosIo::Write (XrdSfsFileOffset offset,
                const char* buffer,
                XrdSfsXferSize length,
                uint16_t timeout)
{
  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Read from file async - falls back on synchronous mode
//------------------------------------------------------------------------------

int64_t
RadosIo::ReadAsync (XrdSfsFileOffset offset,
                    char* buffer,
                    XrdSfsXferSize length,
                    bool readahead,
                    uint16_t timeout)
{
  return Read(offset, buffer, length, timeout);
}


//------------------------------------------------------------------------------
// Write to file async - falls back on synchronous mode
//------------------------------------------------------------------------------

int64_t
RadosIo::WriteAsync (XrdSfsFileOffset offset,
                     const char* buffer,
                     XrdSfsXferSize length,
                     uint16_t timeout)
{
  return Write(offset, buffer, length, timeout);
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------

int
RadosIo::Truncate (XrdSfsFileOffset offset, uint16_t timeout)
{
  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Allocate space for file
//------------------------------------------------------------------------------

int
RadosIo::Fallocate (XrdSfsFileOffset length)
{
  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Deallocate space reserved for file
//------------------------------------------------------------------------------

int
RadosIo::Fdeallocate (XrdSfsFileOffset fromOffset,
                      XrdSfsFileOffset toOffset)
{
  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Sync file to disk
//------------------------------------------------------------------------------

int
RadosIo::Sync (uint16_t timeout)
{
  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Get stats about the file
//------------------------------------------------------------------------------

int
RadosIo::Stat (struct stat* buf, uint16_t timeout)
{
  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------

int
RadosIo::Close (uint16_t timeout)
{
  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------

int
RadosIo::Remove (uint16_t timeout)
{
  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Get pointer to async meta handler object
//------------------------------------------------------------------------------

void*
RadosIo::GetAsyncHandler ()
{
  return NULL;
}

EOSFSTNAMESPACE_END


