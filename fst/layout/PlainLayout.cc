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
#include "fst/io/xrd/XrdIo.hh"

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Handle asynchronous open responses
//------------------------------------------------------------------------------
void
AsyncLayoutOpenHandler::HandleResponseWithHosts(XrdCl::XRootDStatus* status,
    XrdCl::AnyObject* response,
    XrdCl::HostList* hostList)
{
  eos_info("handling response in AsyncLayoutOpenHandler");
  // response and hostList are nullptr
  bool is_ok = false;
  mPlainLayout->mLastTriedUrl = mPlainLayout->mFileIO->GetLastTriedUrl();

  if (status->IsOK()) {
    // Store the last URL we are connected after open
    mPlainLayout->mLastUrl = mPlainLayout->mFileIO->GetLastUrl();
    is_ok = true;
  }

  // Notify any blocked threads
  pthread_mutex_lock(&mPlainLayout->mMutex);
  mPlainLayout->mAsyncResponse = is_ok;
  mPlainLayout->mHasAsyncResponse = true;
  pthread_cond_signal(&mPlainLayout->mCondVar);
  mPlainLayout->mIoOpenHandler = NULL;
  pthread_mutex_unlock(&mPlainLayout->mMutex);
  delete status;

  if (response) {
    delete response;
  }

  if (hostList) {
    delete hostList;
  }

  delete this;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
PlainLayout::PlainLayout(XrdFstOfsFile* file,
                         unsigned long lid,
                         const XrdSecEntity* client,
                         XrdOucErrInfo* outError,
                         const char* path,
                         uint16_t timeout) :
  Layout(file, lid, client, outError, path, timeout),
  mFileSize(0), mDisableRdAhead(false), mHasAsyncResponse(false),
  mAsyncResponse(false), mIoOpenHandler(NULL), mFlags(0)
{
  pthread_mutex_init(&mMutex, NULL);
  pthread_cond_init(&mCondVar, NULL);
  mIsEntryServer = true;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
PlainLayout::~PlainLayout()
{
  // mFileIO is deleted via mFileIO in the base class
  pthread_mutex_destroy(&mMutex);
  pthread_cond_destroy(&mCondVar);

  if (mIoOpenHandler) {
    delete mIoOpenHandler;
  }
}

//------------------------------------------------------------------------------
// Redirect to a new target
//------------------------------------------------------------------------------
void PlainLayout::Redirect(const char* path)
{
  mFileIO.reset(FileIoPlugin::GetIoObject(path, mOfsFile, mSecEntity));
  mLocalPath = path;
}

//------------------------------------------------------------------------------
// Open File
//------------------------------------------------------------------------------
int
PlainLayout::Open(XrdSfsFileOpenMode flags, mode_t mode, const char* opaque)
{
  int retc = mFileIO->fileOpen(flags, mode, opaque, mTimeout);
  mLastUrl = mFileIO->GetLastUrl();
  mLastTriedUrl = mFileIO->GetLastTriedUrl();
  mFlags = flags;
  mLastErrCode = mFileIO->GetLastErrCode();
  mLastErrNo = mFileIO->GetLastErrNo();

  // If open for read succeeded then get initial file size
  if (!retc && !(mFlags & (SFS_O_CREAT | SFS_O_TRUNC))) {
    struct stat st_info;
    int retc_stat = mFileIO->fileStat(&st_info);

    if (retc_stat) {
      eos_err("failed stat for file=%s", mLocalPath.c_str());
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
PlainLayout::OpenAsync(XrdSfsFileOpenMode flags,
                       mode_t mode, XrdCl::ResponseHandler* layout_handler,
                       const char* opaque)
{
  mFlags = flags;
  eos::fst::XrdIo* io_file = dynamic_cast<eos::fst::XrdIo*>(mFileIO.get());

  if (!io_file) {
    eos_err("failed dynamic cast to XrdIo object");
    return SFS_ERROR;
  }

  mIoOpenHandler = new eos::fst::AsyncIoOpenHandler(io_file, layout_handler);

  if (io_file->fileOpenAsync(mIoOpenHandler, flags, mode, opaque, mTimeout)) {
    // Error
    delete mIoOpenHandler;
    mIoOpenHandler = NULL;
    return SFS_ERROR;
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// Wait for asynchronous open reponse
//------------------------------------------------------------------------------
bool
PlainLayout::WaitOpenAsync()
{
  pthread_mutex_lock(&mMutex);

  while (!mHasAsyncResponse) {
    pthread_cond_wait(&mCondVar, &mMutex);
  }

  bool open_resp = mAsyncResponse;
  pthread_mutex_unlock(&mMutex);

  if (open_resp) {
    // Get initial file size if not new file or truncated
    if (!(mFlags & (SFS_O_CREAT | SFS_O_TRUNC))) {
      struct stat st_info;
      int retc_stat = mFileIO->fileStat(&st_info);

      if (retc_stat) {
        eos_err("failed stat");
        open_resp = false;
      } else {
        mFileSize = st_info.st_size;
      }
    }
  }

  return open_resp;
}

//------------------------------------------------------------------------------
// Clean read-ahead caches and update filesize
//------------------------------------------------------------------------------
void
PlainLayout::CleanReadCache()
{
  if (!mDisableRdAhead) {
    mFileIO->CleanReadCache();
    struct stat st_info;
    int retc_stat = mFileIO->fileStat(&st_info);

    if (!retc_stat) {
      mFileSize = st_info.st_size;
    }
  }
}

//------------------------------------------------------------------------------
// Read from file
//------------------------------------------------------------------------------
int64_t
PlainLayout::Read(XrdSfsFileOffset offset, char* buffer,
                  XrdSfsXferSize length, bool readahead)
{
  if (readahead && !mDisableRdAhead) {
    if (mIoType == eos::common::LayoutId::eIoType::kXrdCl) {
      if ((uint64_t)(offset + length) > mFileSize) {
        length = mFileSize - offset;
      }

      if (length < 0) {
        length = 0;
      }

      eos_static_info("read offset=%llu length=%lu", offset, length);
      int64_t nread = mFileIO->fileReadPrefetch(offset, buffer, length);
      // Wait for any async requests
      AsyncMetaHandler* ptr_handler = static_cast<AsyncMetaHandler*>
                                      (mFileIO->fileGetAsyncHandler());

      if (ptr_handler) {
        uint16_t error_type = ptr_handler->WaitOK();

        if (error_type != XrdCl::errNone) {
          return SFS_ERROR;
        }
      }

      if ((nread + offset) > (off_t)mFileSize) {
        mFileSize = nread + offset;
      }

      if ((nread != length) && ((nread + offset) < (int64_t)mFileSize)) {
        mFileSize = nread + offset;
      }

      return nread;
    }
  }

  return mFileIO->fileRead(offset, buffer, length, mTimeout);
}

//------------------------------------------------------------------------------
// Vector read
//------------------------------------------------------------------------------
int64_t
PlainLayout::ReadV(XrdCl::ChunkList& chunkList, uint32_t len)
{
  return mFileIO->fileReadV(chunkList);
}


//------------------------------------------------------------------------------
// Write to file
//------------------------------------------------------------------------------

int64_t
PlainLayout::Write(XrdSfsFileOffset offset, const char* buffer,
                   XrdSfsXferSize length)
{
  mDisableRdAhead = true;

  if ((uint64_t)(offset + length) > mFileSize) {
    mFileSize = offset + length;
  }

  return mFileIO->fileWriteAsync(offset, buffer, length, mTimeout);
}

//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------

int
PlainLayout::Truncate(XrdSfsFileOffset offset)
{
  mFileSize = offset;
  return mFileIO->fileTruncate(offset, mTimeout);
}

//------------------------------------------------------------------------------
// Reserve space for file
//------------------------------------------------------------------------------

int
PlainLayout::Fallocate(XrdSfsFileOffset length)
{
  return mFileIO->fileFallocate(length);
}

//------------------------------------------------------------------------------
// Deallocate reserved space
//------------------------------------------------------------------------------

int
PlainLayout::Fdeallocate(XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset)
{
  return mFileIO->fileFdeallocate(fromOffset, toOffset);
}

//------------------------------------------------------------------------------
// Sync file to disk
//------------------------------------------------------------------------------

int
PlainLayout::Sync()
{
  return mFileIO->fileSync(mTimeout);
}

//------------------------------------------------------------------------------
// Get stats for file
//------------------------------------------------------------------------------

int
PlainLayout::Stat(struct stat* buf)
{
  return mFileIO->fileStat(buf, mTimeout);
}

//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------

int
PlainLayout::Close()
{
  return mFileIO->fileClose(mTimeout);
}

//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------

int
PlainLayout::Remove()
{
  return mFileIO->fileRemove();
}

EOSFSTNAMESPACE_END
