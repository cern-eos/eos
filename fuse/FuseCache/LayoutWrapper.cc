//------------------------------------------------------------------------------
// File LayoutWrapper.cc
// Author: Geoffray Adde <geoffray.adde@cern.ch> CERN
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

#include "LayoutWrapper.hh"
#include "FileAbstraction.hh"

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
LayoutWrapper::LayoutWrapper (eos::fst::Layout* file) :
    mFile (file), mOpen (false), mHasWrite (false)
{
}

//----------------------------------------------------------------------------
// Destructor
//----------------------------------------------------------------------------
LayoutWrapper::~LayoutWrapper ()
{
  delete mFile;
}

//--------------------------------------------------------------------------
// Make sure that the file layout is open
// Reopen it if needed using (almost) the same argument as the previous open
//--------------------------------------------------------------------------
int LayoutWrapper::MakeOpen ()
{
  eos_static_debug("makeopening file %s", mPath.c_str ());
  if (!mOpen)
  {
    if (mPath.size ())
    {
      if (mFile->Open (mPath, mFlags, mMode, mOpaque.c_str ()))
      {
        eos_static_debug("error while openning");
        return -1;
      }
      else
      {
        eos_static_debug("successfully opened");
        mOpen = true;
        return 0;
      }

    }
    else
      return -1;
  }
  else
    eos_static_debug("already opened");
  return 0;
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
const char*
LayoutWrapper::GetName ()
{
  MakeOpen ();
  return mFile->GetName ();
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
const char*
LayoutWrapper::GetLocalReplicaPath ()
{
  MakeOpen ();
  return mFile->GetLocalReplicaPath ();
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
unsigned int LayoutWrapper::GetLayoutId ()
{
  MakeOpen ();
  return mFile->GetLayoutId ();
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
const std::string&
LayoutWrapper::GetLastUrl ()
{
  MakeOpen ();
  return mFile->GetLastUrl ();
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
bool LayoutWrapper::IsEntryServer ()
{
  MakeOpen ();
  return mFile->IsEntryServer ();
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
int LayoutWrapper::Open (const std::string& path, XrdSfsFileOpenMode flags, mode_t mode, const char* opaque)
{
  eos_static_debug("opening file %s", path.c_str ());
  if (mOpen)
  {
    eos_static_debug("already open");
    return -1;
  }
  mPath = path;
  mFlags = flags | ~O_TRUNC; // we don't want to truncate the file in case it does not exist
  mMode = mode;
  mOpaque = opaque;
  if (mFile->Open (path, flags, mode, opaque))
  {
    eos_static_debug("error while openning");
    return -1;
  }
  else
  {
    eos_static_debug("successfully opened");
    mOpen = true;
    return 0;
  }
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
int64_t LayoutWrapper::Read (XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length, bool readahead)
{
  MakeOpen ();
  return mFile->Read (offset, buffer, length, readahead);
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
int64_t LayoutWrapper::ReadV (XrdCl::ChunkList& chunkList, uint32_t len)
{
  MakeOpen ();
  return mFile->ReadV (chunkList, len);
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
int64_t LayoutWrapper::Write (XrdSfsFileOffset offset, const char* buffer, XrdSfsXferSize length)
{
  LayoutWrapper::MakeOpen ();
  mHasWrite = true;
  // TODO: enable this to update the timestamp to local modification date
  //  UtimesNow();
  return mFile->Write (offset, buffer, length);
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
int LayoutWrapper::Truncate (XrdSfsFileOffset offset)
{
  MakeOpen ();
  mHasWrite = true;
  // TODO: enable this to update the timestamp to local modification date
  //  UtimesNow();
  return mFile->Truncate (offset);
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
int LayoutWrapper::Sync ()
{
  MakeOpen ();
  return mFile->Sync ();
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
int LayoutWrapper::Close ()
{
  eos_static_debug("closing file %s", mPath.c_str ());
  if (!mOpen)
  {
    eos_static_debug("already closed");
    return 0;
  }
  if (mFile->Close ())
  {
    eos_static_debug("error while closing");
    return -1;
  }
  else
  {
    mOpen = false;
    mHasWrite = false;
    eos_static_debug("successfully closed");
    return 0;
  }
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
int LayoutWrapper::Stat (struct stat* buf)
{
  MakeOpen ();
  return mFile->Stat (buf);
}

//--------------------------------------------------------------------------
// Set atime and mtime at current time
//--------------------------------------------------------------------------
void LayoutWrapper::UtimesNow ()
{
  struct timespec ts[2];
  clock_gettime (CLOCK_REALTIME, ts);
  ts[1] = ts[0];
  fabs->SetUtimes (ts);
}
