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
// Handle asynchronous open responses
//------------------------------------------------------------------------------
void AsyncLayoutOpenHandler::HandleResponseWithHosts(XrdCl::XRootDStatus* status,
                                                     XrdCl::AnyObject* response,
                                                     XrdCl::HostList* hostList)
{
  eos_info("handling response in AsyncLayoutOpenHandler");
  // response and hostList are nullptr
  bool is_ok = false;

  if (status->IsOK())
  {
    // Store the last URL we are connected after open
    mPlainLayout->mLastUrl = mPlainLayout->mPlainFile->GetLastUrl();
    is_ok = true;
  }

  // Notify any blocked threads
  pthread_mutex_lock(&mPlainLayout->mMutex);
  mPlainLayout->mAsyncResponse = is_ok;
  mPlainLayout->mHasAsyncResponse = true;
  pthread_cond_signal(&mPlainLayout->mCondVar);
  pthread_mutex_unlock(&mPlainLayout->mMutex);
  delete status;
  mPlainLayout->mIoOpenHandler = NULL;
  delete this;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
PlainLayout::PlainLayout (XrdFstOfsFile* file,
                          unsigned long lid,
                          const XrdSecEntity* client,
                          XrdOucErrInfo* outError,
                          eos::common::LayoutId::eIoType io,
                          uint16_t timeout) :
    Layout (file, lid, client, outError, io, timeout),
    mFileSize(0), mDisableRdAhead(false), mHasAsyncResponse(false),
    mAsyncResponse(false), mIoOpenHandler(NULL), mFlags(0)
{
  pthread_mutex_init(&mMutex, NULL);
  pthread_cond_init(&mCondVar, NULL);
  mPlainFile = FileIoPlugin::GetIoObject(mIoType, mOfsFile, mSecEntity);
  mIsEntryServer = true;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
PlainLayout::~PlainLayout ()
{
  pthread_mutex_destroy(&mMutex);
  pthread_cond_destroy(&mCondVar);

  if (mIoOpenHandler)
    delete mIoOpenHandler;

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
  mFlags = flags;
  mLocalPath = path;
  int retc = mPlainFile->Open(path, flags, mode, opaque, mTimeout);
  mLastUrl = mPlainFile->GetLastUrl();
  mLastErrCode = mPlainFile->GetLastErrCode();
  mLastErrNo = mPlainFile->GetLastErrNo();

  // Get initial file size if not new file or truncated
  if (!(mFlags & (SFS_O_CREAT | SFS_O_TRUNC)))
  {
    struct stat st_info;
    int retc_stat = mPlainFile->Stat(&st_info);

    if (retc_stat)
    {
      eos_err("failed stat for file=%s", path.c_str());
      return SFS_ERROR;
    }

    mFileSize = st_info.st_size;
  }

  return retc;
}

//------------------------------------------------------------------------------
// Open asynchronously
//------------------------------------------------------------------------------
int
PlainLayout::OpenAsync(const std::string& path, XrdSfsFileOpenMode flags,
                       mode_t mode, XrdCl::ResponseHandler* layout_handler,
                       const char* opaque)
{
  mFlags = flags;
  mLocalPath = path;
  mIoOpenHandler = new eos::fst::AsyncIoOpenHandler(
      static_cast<eos::fst::XrdIo*>(mPlainFile), layout_handler);
  return static_cast<eos::fst::XrdIo*>(mPlainFile)->OpenAsync(
      path, mIoOpenHandler, flags, mode, opaque, mTimeout);
}

//------------------------------------------------------------------------------
// Wait for asynchronous open reponse
//------------------------------------------------------------------------------
bool
PlainLayout::WaitOpenAsync()
{
  pthread_mutex_lock(&mMutex);
  while (!mHasAsyncResponse)
    pthread_cond_wait(&mCondVar, &mMutex);

  pthread_mutex_unlock(&mMutex);

  if (mAsyncResponse)
  {
    // Get initial file size if not new file or truncated
    if (!(mFlags & (SFS_O_CREAT | SFS_O_TRUNC)))
    {
      struct stat st_info;
      int retc_stat = mPlainFile->Stat(&st_info);

      if (retc_stat)
      {
        eos_err("failed stat");
        mAsyncResponse = false;
      }
      else
      {
        mFileSize = st_info.st_size;
      }
    }
  }

  return mAsyncResponse;
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

      if (length < 0)
        length = 0;

      eos_static_info("read offset=%llu length=%lu", offset, length);
      int64_t nread = mPlainFile->ReadAsync(offset, buffer, length, readahead);

      // Wait for any async requests
      AsyncMetaHandler* ptr_handler = static_cast<AsyncMetaHandler*>
          (mPlainFile->GetAsyncHandler());

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

  return mPlainFile->Read(offset, buffer, length, mTimeout);
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

  if ((uint64_t)(offset + length) > mFileSize)
    mFileSize = offset + length;

  return mPlainFile->WriteAsync(offset, buffer, length, mTimeout);
}

//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
PlainLayout::Truncate (XrdSfsFileOffset offset)
{
  mFileSize = offset;
  return mPlainFile->Truncate(offset, mTimeout);
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
// Sync file to disk
//------------------------------------------------------------------------------
int
PlainLayout::Sync ()
{
  return mPlainFile->Sync(mTimeout);
}

//------------------------------------------------------------------------------
// Get stats for file
//------------------------------------------------------------------------------
int
PlainLayout::Stat (struct stat* buf)
{
  return mPlainFile->Stat(buf, mTimeout);
}

//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------
int
PlainLayout::Close ()
{
  int rc = SFS_OK;
  AsyncMetaHandler* ptr_handler =
      static_cast<AsyncMetaHandler*> (mPlainFile->GetAsyncHandler());

  if (ptr_handler)
  {
    if (ptr_handler->WaitOK() != XrdCl::errNone)
    {
      eos_err("error=async requests failed for file %s", mLastUrl.c_str());
      rc = SFS_ERROR;
    }
  }

  int rc_close = mPlainFile->Close(mTimeout);

  if (rc != SFS_OK)
    rc_close = rc;

  return rc_close;
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
