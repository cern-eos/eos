//------------------------------------------------------------------------------
// File: PlainLayout.cc
// Author: Elvin-Alin Sindrilaru / Andreas-Joachim Peters - CERN
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
#include "fst/layout/PlainLayout.hh"
#include "fst/layout/FileIoPlugin.hh"
#include "fst/XrdFstOfsFile.hh"

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
PlainLayout::PlainLayout (XrdFstOfsFile* file,
                          int lid,
                          const XrdSecEntity* client,
                          XrdOucErrInfo* outError,
                          eos::common::LayoutId::eIoType io) :
Layout (file, lid, client, outError, io)
{
  //............................................................................
  // For the plain layout we use only the LocalFileIo type
  //............................................................................
  mPlainFile = FileIoPlugin::GetIoObject(mIoType,
                                         mOfsFile,
                                         mSecEntity,
                                         mError);
  mIsEntryServer = true;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

PlainLayout::~PlainLayout ()
{
  delete mPlainFile;
}


//------------------------------------------------------------------------------
// Open File
//------------------------------------------------------------------------------

int
PlainLayout::Open (const std::string& path,
                   XrdSfsFileOpenMode flags,
                   mode_t mode,
                   const char* opaque)
{
  mLocalPath = path;
  return mPlainFile->Open(path, flags, mode, opaque);
}


//------------------------------------------------------------------------------
// Read from file
//------------------------------------------------------------------------------

int64_t
PlainLayout::Read (XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  return mPlainFile->Read(offset, buffer, length);
}


//------------------------------------------------------------------------------
// Write to file
//------------------------------------------------------------------------------

int64_t
PlainLayout::Write (XrdSfsFileOffset offset, const char* buffer, XrdSfsXferSize length)
{
  return mPlainFile->Write(offset, buffer, length);
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------

int
PlainLayout::Truncate (XrdSfsFileOffset offset)
{
  return mPlainFile->Truncate(offset);
}


//------------------------------------------------------------------------------
// Reserve space for file
//------------------------------------------------------------------------------

int
PlainLayout::Fallocate (XrdSfsFileOffset length)
{
  return mPlainFile->Fallocate(length);
}


//------------------------------------------------------------------------------
// Deallocate reserved space
//------------------------------------------------------------------------------

int
PlainLayout::Fdeallocate (XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset)
{
  return mPlainFile->Fdeallocate(fromOffset, toOffset);
}


//------------------------------------------------------------------------------
// Syn file to disk
//------------------------------------------------------------------------------

int
PlainLayout::Sync ()
{
  return mPlainFile->Sync();
}


//------------------------------------------------------------------------------
// Get stats for file
//------------------------------------------------------------------------------

int
PlainLayout::Stat (struct stat* buf)
{
  return mPlainFile->Stat(buf);
}


//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------

int
PlainLayout::Close ()
{
  return mPlainFile->Close();
}


//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------

int
PlainLayout::Remove ()
{
  return mPlainFile->Remove();
}

EOSFSTNAMESPACE_END
