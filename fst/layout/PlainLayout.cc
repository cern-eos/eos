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
#include "fst/io/FileIoPlugin.hh"
#include "fst/io/AsyncMetaHandler.hh"
#include "fst/XrdFstOfsFile.hh"

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
PlainLayout::PlainLayout (XrdFstOfsFile* file,
                          unsigned long lid,
                          const XrdSecEntity* client,
                          XrdOucErrInfo* outError,
                          const char *path,
                          uint16_t timeout) :
Layout (file, lid, client, outError, path, timeout),
mFileSize (0),
mDisableRdAhead (false)
{
  // evt. mark an IO module as talking to external storage
  if ((mFileIO->GetIoType() != "LocalIo"))
    mFileIO->SetExternalStorage();

  mIsEntryServer = true;
  mLocalPath = path;
}

//------------------------------------------------------------------------------
// Redirect toa new target
//------------------------------------------------------------------------------
void PlainLayout::Redirect(const char* path)
{
  if (mFileIO)
    delete mFileIO;
  mFileIO = FileIoPlugin::GetIoObject(path, mOfsFile, mSecEntity);
  mLocalPath = path;
}
//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

PlainLayout::~PlainLayout ()
{
  // mFileIO is deleted via mFileIO in the base class
}

//------------------------------------------------------------------------------
// Open File
//------------------------------------------------------------------------------

int
PlainLayout::Open(XrdSfsFileOpenMode flags, mode_t mode, const char* opaque)
{
  int retc = mFileIO->fileOpen(flags, mode, opaque, mTimeout);
  mLastUrl = mFileIO->GetLastUrl();

  // Get initial file size
  struct stat st_info;
  int retc_stat = mFileIO->fileStat(&st_info);

  if (retc_stat)
  {
    eos_err("failed stat for file=%s", mFileIO->GetPath().c_str());
    return SFS_ERROR;
  }

  mFileSize = st_info.st_size;
  return retc;
}

//------------------------------------------------------------------------------
// Read from file
//------------------------------------------------------------------------------

int64_t
PlainLayout::Read (XrdSfsFileOffset offset, char* buffer,
                   XrdSfsXferSize length, bool readahead)
{
  if (readahead && !mDisableRdAhead)
  {
    if (mIoType == eos::common::LayoutId::eIoType::kXrdCl)
    {
      if ((uint64_t)(offset + length) > mFileSize)
	length = mFileSize - offset;

      if (length<0)
	length = 0;

      eos_static_info("read offset=%llu length=%lu", offset, length);
      int64_t nread = mFileIO->fileReadAsync(offset, buffer, length, readahead);

      // Wait for any async requests
      AsyncMetaHandler* ptr_handler = static_cast<AsyncMetaHandler*>
              (mFileIO->fileGetAsyncHandler());

      if (ptr_handler)
      {
        uint16_t error_type = ptr_handler->WaitOK();

        if (error_type != XrdCl::errNone)
          return SFS_ERROR;
      }

      if ( (nread+offset) > (off_t)mFileSize)
	mFileSize = nread+offset;

      if ( (nread != length) && ( (nread+offset) < (int64_t)mFileSize) )
	mFileSize = nread+offset;

      return nread;
    }
  }

  return mFileIO->fileRead(offset, buffer, length, mTimeout);
}

//------------------------------------------------------------------------------
// Vector read 
//------------------------------------------------------------------------------
int64_t
PlainLayout::ReadV (XrdCl::ChunkList& chunkList, uint32_t len)
{
  return mPlainFile->ReadV(chunkList);
}


//------------------------------------------------------------------------------
// Write to file
//------------------------------------------------------------------------------

int64_t
PlainLayout::Write (XrdSfsFileOffset offset, const char* buffer,
                    XrdSfsXferSize length)
{
  mDisableRdAhead = true;

  if ((uint64_t) (offset + length) > mFileSize)
    mFileSize = offset + length;

  return mFileIO->fileWrite(offset, buffer, length, mTimeout);
}

//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------

int
PlainLayout::Truncate (XrdSfsFileOffset offset)
{
  mFileSize = offset;
  return mFileIO->fileTruncate(offset, mTimeout);
}

//------------------------------------------------------------------------------
// Reserve space for file
//------------------------------------------------------------------------------

int
PlainLayout::Fallocate (XrdSfsFileOffset length)
{
  return mFileIO->fileFallocate(length);
}

//------------------------------------------------------------------------------
// Deallocate reserved space
//------------------------------------------------------------------------------

int
PlainLayout::Fdeallocate (XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset)
{
  return mFileIO->fileFdeallocate(fromOffset, toOffset);
}

//------------------------------------------------------------------------------
// Sync file to disk
//------------------------------------------------------------------------------

int
PlainLayout::Sync ()
{
  return mFileIO->fileSync(mTimeout);
}

//------------------------------------------------------------------------------
// Get stats for file
//------------------------------------------------------------------------------

int
PlainLayout::Stat (struct stat* buf)
{
  return mFileIO->fileStat(buf, mTimeout);
}

//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------

int
PlainLayout::Close ()
{
  return mFileIO->fileClose(mTimeout);
}

//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------

int
PlainLayout::Remove ()
{
  return mFileIO->fileRemove();
}

EOSFSTNAMESPACE_END
