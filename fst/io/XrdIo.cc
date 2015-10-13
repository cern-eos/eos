//------------------------------------------------------------------------------
// File: XrdIo.cc
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
#include <stdint.h>
#include <cstdlib>
/*----------------------------------------------------------------------------*/
#include "fst/io/XrdIo.hh"
#include "fst/io/ChunkHandler.hh"
#include "common/FileMap.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClBuffer.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

        const uint64_t ReadaheadBlock::sDefaultBlocksize = 1 * 1024 * 1024; ///< 1MB default
const uint32_t XrdIo::sNumRdAheadBlocks = 2;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------

XrdIo::XrdIo () :
FileIo (),
mDoReadahead (false),
mBlocksize (ReadaheadBlock::sDefaultBlocksize),
mXrdFile (NULL),
mMetaHandler (new AsyncMetaHandler ())
{
  // Set the TimeoutResolution to 1
  XrdCl::Env* env = XrdCl::DefaultEnv::GetEnv();
  env->PutInt("TimeoutResolution", 1);
  mType = "XrdIo";
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

XrdIo::~XrdIo ()
{
  if (mDoReadahead)
  {
    while (!mQueueBlocks.empty())
    {
      ReadaheadBlock* ptr_readblock = mQueueBlocks.front();
      mQueueBlocks.pop();
      delete ptr_readblock;
    }

    while (!mMapBlocks.empty())
    {
      delete mMapBlocks.begin()->second;
      mMapBlocks.erase(mMapBlocks.begin());
    }
  }

  delete mMetaHandler;

  if (mXrdFile)
    delete mXrdFile;
}


//------------------------------------------------------------------------------
// Open file
//------------------------------------------------------------------------------

int
XrdIo::Open (const std::string& path,
             XrdSfsFileOpenMode flags,
             mode_t mode,
             const std::string& opaque,
             uint16_t timeout)
{
  const char* val = 0;
  std::string request;

  std::string lOpaque;
  size_t qpos = 0;

  mFilePath = path;

  //............................................................................
  // Opaque info can be part of the 'path'
  //............................................................................
  if (((qpos = path.find("?")) != std::string::npos))
  {
    lOpaque = path.substr(qpos + 1);
    mFilePath.erase(qpos);
  }
  else
  {
    lOpaque = opaque;
  }

  XrdOucEnv open_opaque(lOpaque.c_str());

  //............................................................................
  // Decide if readahead is used and the block size
  //............................................................................
  if ((val = open_opaque.Get("fst.readahead")) &&
      (strncmp(val, "true", 4) == 0))
  {
    eos_debug("Enabling the readahead.");
    mDoReadahead = true;
    val = 0;

    if ((val = open_opaque.Get("fst.blocksize")))
    {
      mBlocksize = static_cast<uint64_t> (atoll(val));
    }

    for (unsigned int i = 0; i < sNumRdAheadBlocks; i++)
    {
      mQueueBlocks.push(new ReadaheadBlock(mBlocksize));
    }
  }

  request = path;
  request += "?";
  request += lOpaque;
  mXrdFile = new XrdCl::File();

  // Disable recovery on read and write
  mXrdFile->EnableReadRecovery(false);
  mXrdFile->EnableWriteRecovery(false);

  XrdCl::OpenFlags::Flags flags_xrdcl = eos::common::LayoutId::MapFlagsSfs2XrdCl(flags);
  XrdCl::Access::Mode mode_xrdcl = eos::common::LayoutId::MapModeSfs2XrdCl(mode);
  XrdCl::XRootDStatus status = mXrdFile->Open(request, flags_xrdcl, mode_xrdcl, timeout);

  if (!status.IsOK())
  {
    eos_err("error=opening remote XrdClFile");
    errno = status.errNo;
    mLastErrMsg = status.ToString().c_str();
    return SFS_ERROR;
  }
  else
  {
    errno = 0;
  }

  //............................................................................
  // store the last URL we are connected after open
  //............................................................................

  XrdCl::URL cUrl = mXrdFile->GetLastURL();
  mLastUrl = cUrl.GetURL();
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Read from file - sync
//------------------------------------------------------------------------------

int64_t
XrdIo::Read (XrdSfsFileOffset offset,
             char* buffer,
             XrdSfsXferSize length,
             uint16_t timeout)
{
  eos_debug("offset=%llu length=%llu",
            static_cast<uint64_t> (offset),
            static_cast<uint64_t> (length));

  uint32_t bytes_read = 0;
  XrdCl::XRootDStatus status = mXrdFile->Read(static_cast<uint64_t> (offset),
                                              static_cast<uint32_t> (length),
                                              buffer,
                                              bytes_read,
                                              timeout);

  if (!status.IsOK())
  {
    errno = status.errNo;
    mLastErrMsg = status.ToString().c_str();
    return SFS_ERROR;
  }

  return bytes_read;
}


//------------------------------------------------------------------------------
// Write to file - sync
//------------------------------------------------------------------------------

int64_t
XrdIo::Write (XrdSfsFileOffset offset,
              const char* buffer,
              XrdSfsXferSize length,
              uint16_t timeout)
{
  eos_debug("offset=%llu length=%llu",
            static_cast<uint64_t> (offset),
            static_cast<uint64_t> (length));

  XrdCl::XRootDStatus status = mXrdFile->Write(static_cast<uint64_t> (offset),
                                               static_cast<uint32_t> (length),
                                               buffer,
                                               timeout);

  if (!status.IsOK())
  {
    errno = status.errNo;
    mLastErrMsg = status.ToString().c_str();
    return SFS_ERROR;
  }

  return length;
}


//------------------------------------------------------------------------------
// Read from file - async
//------------------------------------------------------------------------------

int64_t
XrdIo::ReadAsync (XrdSfsFileOffset offset,
                  char* buffer,
                  XrdSfsXferSize length,
                  bool readahead,
                  uint16_t timeout)
{
  eos_debug("offset=%llu length=%llu",
            static_cast<uint64_t> (offset),
            static_cast<uint64_t> (length));

  bool done_read = false;
  int64_t nread = 0;
  char* pBuff = buffer;
  XrdCl::XRootDStatus status;
  ChunkHandler* handler = NULL;

  if (!mDoReadahead)
  {
    readahead = false;
    eos_debug("Readahead is disabled");
  }

  if (!readahead)
  {
    handler = mMetaHandler->Register(offset, length, NULL, false);

    // If previous read requests failed with a timeout error then we won't
    // get a new handler and we return directly an error
    if (!handler)
    {
      return SFS_ERROR;
    }

    status = mXrdFile->Read(static_cast<uint64_t> (offset),
                            static_cast<uint32_t> (length),
                            buffer,
                            static_cast<XrdCl::ResponseHandler*> (handler),
                            timeout);

    if (!status.IsOK())
    {
      //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      // TODO: for the time being we call this ourselves but this should be
      // dropped once XrdCl will call the handler for a request as it knows it
      // has already failed
      //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      mMetaHandler->HandleResponse(&status, handler);
    }
    nread = length;
  }
  else
  {
    eos_debug("readahead enabled, request offset=%lli, length=%i", offset, length);
    uint64_t read_length = 0;
    uint32_t aligned_length;
    uint32_t shift;
    std::map<uint64_t, ReadaheadBlock*>::iterator iter;

    mPrefetchMutex.Lock(); // -->

    while (length)
    {
      iter = FindBlock(offset);

      if (iter != mMapBlocks.end())
      {
        // Block found in prefetched blocks
        SimpleHandler* sh = iter->second->handler;
        shift = offset - iter->first;

        // We can prefetch another block if we still have available blocks in
        // the queue or if first read was from second prefetched block
        if (!mQueueBlocks.empty() || (iter != mMapBlocks.begin()))
        {
          if (iter != mMapBlocks.begin())
          {
            eos_debug("recycle the oldest block");
            mQueueBlocks.push(mMapBlocks.begin()->second);
            mMapBlocks.erase(mMapBlocks.begin());
          }

          eos_debug("prefetch new block(2)");
          if (!PrefetchBlock(offset + mBlocksize, false, timeout))
          {
            eos_warning("failed to send prefetch request(2)");
            break;
          }
        }

        if (sh->WaitOK())
        {
          eos_debug("block in cache, blk_off=%lld, req_off= %lld", iter->first, offset);

          if (sh->GetRespLength() == 0)
          {
            // The request got a response but it read 0 bytes
            eos_warning("response contains 0 bytes");
            break;
          }

          aligned_length = sh->GetRespLength() - shift;
          read_length = ((uint32_t) length < aligned_length) ? length : aligned_length;

          // If prefetch block smaller than mBlocksize and current offset at end
          // of the prefetch block then we reached the end of file
          if ((sh->GetRespLength() != mBlocksize) &&
              ((uint64_t) offset >= iter->first + sh->GetRespLength()))
          {
            done_read = true;
            break;
          }

          pBuff = static_cast<char*> (memcpy(pBuff, iter->second->buffer + shift,
                                             read_length));

          pBuff += read_length;
          offset += read_length;
          length -= read_length;
          nread += read_length;
        }
        else
        {
          // Error while prefetching, remove block from map
          mQueueBlocks.push(iter->second);
          mMapBlocks.erase(iter);
          eos_err("error=prefetching failed, disable it and remove block from map");
          mDoReadahead = false;
          break;
        }
      }
      else
      {
        //......................................................................
        // Remove all elements from map so that we can align with the new
        // requests and prefetch a new block. But first we need to collect any
        // responses which are in-flight as otherwise these response might
        // arrive later on, when we are expecting replies for other blocks since
        // we are recycling the SimpleHandler objects.
        while (!mMapBlocks.empty())
        {
          SimpleHandler* sh = mMapBlocks.begin()->second->handler;

          if (sh->HasRequest())
          {
            // Not interested in the result - discard it
            sh->WaitOK();
          }

          mQueueBlocks.push(mMapBlocks.begin()->second);
          mMapBlocks.erase(mMapBlocks.begin());
        }

        if (!mQueueBlocks.empty())
        {
          eos_debug("prefetch new block(1)");

          if (!PrefetchBlock(offset, false, timeout))
          {
            eos_err("error=failed to send prefetch request(1)");
            mDoReadahead = false;
            break;
          }
        }
      }
    }

    mPrefetchMutex.UnLock(); // <--

    // If readahead not useful, use the classic way to read
    if (length && !done_read)
    {
      eos_debug("readahead useless, use the classic way for reading");
      handler = mMetaHandler->Register(offset, length, NULL, false);

      // If previous read requests failed then we won't get a new handler
      // and we return directly an error
      if (!handler)
      {
        return SFS_ERROR;
      }

      status = mXrdFile->Read(static_cast<uint64_t> (offset),
                              static_cast<uint32_t> (length),
                              pBuff,
                              handler,
                              timeout);
      if (!status.IsOK())
      {
        //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        // TODO: for the time being we call this ourselves but this should be
        // dropped once XrdCl will call the handler for a request as it knows it
        // has already failed
        //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        mMetaHandler->HandleResponse(&status, handler);
      }

      nread = length;
    }
  }

  return nread;
}


//------------------------------------------------------------------------------
// Try to find a block in cache which contains the required offset
//------------------------------------------------------------------------------

PrefetchMap::iterator
XrdIo::FindBlock (uint64_t offset)
{
  if (mMapBlocks.empty())
  {
    return mMapBlocks.end();
  }

  PrefetchMap::iterator iter = mMapBlocks.lower_bound(offset);
  if ((iter != mMapBlocks.end()) && (iter->first == offset))
  {
    // Found exactly the block needed
    return iter;
  }
  else
  {
    if (iter == mMapBlocks.begin())
    {
      // Only blocks with bigger offsets, return pointer to end of the map
      return mMapBlocks.end();
    }
    else
    {
      // Check if the previous block, we know the map is not empty
      iter--;

      if ((iter->first <= offset) && (offset < (iter->first + mBlocksize)))
      {
        return iter;
      }
      else
      {
        return mMapBlocks.end();
      }
    }
  }
}


//------------------------------------------------------------------------------
// Write to file - async
//------------------------------------------------------------------------------

int64_t
XrdIo::WriteAsync (XrdSfsFileOffset offset,
                   const char* buffer,
                   XrdSfsXferSize length,
                   uint16_t timeout)
{
  eos_debug("offset=%llu length=%i", static_cast<uint64_t> (offset), length);

  ChunkHandler* handler;
  XrdCl::XRootDStatus status;

  handler = mMetaHandler->Register(offset, length, buffer, true);

  // If previous write requests failed then we won't get a new handler
  // and we return directly an error
  if (!handler)
  {
    return SFS_ERROR;
  }

  // Obs: Use the handler buffer for write requests
  status = mXrdFile->Write(static_cast<uint64_t> (offset),
                           static_cast<uint32_t> (length),
                           handler->GetBuffer(),
                           handler,
                           timeout);
  return length;
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------

int
XrdIo::Truncate (XrdSfsFileOffset offset, uint16_t timeout)
{

  if (mExternalStorage)
  {
    if (offset == EOS_FST_DELETE_FLAG_VIA_TRUNCATE_LEN)
    {
      // if we have an external XRootD we cannot send this truncate
      // we issue an 'rm' instead
      return Delete(mFilePath.c_str());
    }

    if (offset == EOS_FST_NOCHECKSUM_FLAG_VIA_TRUNCATE_LEN)
    {
      // if we have an external XRootD we cannot send this truncate
      // we can just ignore this message
      return 0;
    }
  }
  XrdCl::XRootDStatus status = mXrdFile->Truncate(static_cast<uint64_t> (offset),
                                                  timeout);

  if (!status.IsOK())
  {

    errno = status.errNo;
    mLastErrMsg = status.ToString().c_str();
    return SFS_ERROR;
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Sync file to disk
//------------------------------------------------------------------------------

int
XrdIo::Sync (uint16_t timeout)
{
  XrdCl::XRootDStatus status = mXrdFile->Sync(timeout);

  if (!status.IsOK())
  {

    errno = status.errNo;
    mLastErrMsg = status.ToString().c_str();
    return SFS_ERROR;
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Get stats about the file
//------------------------------------------------------------------------------

int
XrdIo::Stat (struct stat* buf, uint16_t timeout)
{
  int rc = SFS_ERROR;
  XrdCl::StatInfo* stat = 0;
  //............................................................................
  XrdCl::XRootDStatus status = mXrdFile->Stat(true, stat, timeout);

  if (!status.IsOK())
  {
    errno = status.errNo;
    mLastErrMsg = status.ToString().c_str();
  }
  else
  {
    buf->st_dev = static_cast<dev_t> (atoi(stat->GetId().c_str()));
    buf->st_mode = static_cast<mode_t> (stat->GetFlags());
    buf->st_size = static_cast<off_t> (stat->GetSize());
    buf->st_mtime = static_cast<time_t> (stat->GetModTime());
    rc = SFS_OK;
  }

  if (stat)
  {

    delete stat;
  }

  return rc;
}


//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------

int
XrdIo::Close (uint16_t timeout)
{
  bool async_ok = true;

  if (mDoReadahead)
  {
    //..........................................................................
    // Wait for any requests on the fly and then close
    //..........................................................................
    while (!mMapBlocks.empty())
    {
      SimpleHandler* shandler = mMapBlocks.begin()->second->handler;
      if (shandler->HasRequest())
      {
        async_ok = shandler->WaitOK();
      }
      delete mMapBlocks.begin()->second;
      mMapBlocks.erase(mMapBlocks.begin());
    }
  }

  XrdCl::XRootDStatus status = mXrdFile->Close(timeout);

  if (!status.IsOK())
  {
    errno = status.errNo;
    mLastErrMsg = status.ToString().c_str();
    return SFS_ERROR;
  }

  // If any of the async requests failed then we have an error
  if (!async_ok)
  {

    return SFS_ERROR;
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------

int
XrdIo::Remove (uint16_t timeout)
{
  //............................................................................
  // Remove the file by truncating using the special value offset
  //............................................................................
  XrdCl::XRootDStatus status = mXrdFile->Truncate(EOS_FST_DELETE_FLAG_VIA_TRUNCATE_LEN, timeout);

  if (!status.IsOK())
  {

    eos_err("error=failed to truncate file with deletion offset - %s", mPath.c_str());
    mLastErrMsg = "failed to truncate file with deletion offset";
    return SFS_ERROR;
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// Check for existance
//------------------------------------------------------------------------------

int
XrdIo::Exists (const char* url)
{
  XrdCl::URL xUrl(url);
  XrdCl::FileSystem fs(xUrl);
  XrdCl::StatInfo* stat;
  ;
  XrdCl::XRootDStatus status = fs.Stat(xUrl.GetPath(), stat);
  errno = 0;
  if (!status.IsOK())
  {
    if (status.errNo == kXR_NotFound)
    {
      errno = ENOENT;
      mLastErrMsg = "no such file or directory";
    }
    else
    {
      errno = EIO;
      mLastErrMsg = "failed to check for existance";
    }

    return SFS_ERROR;
  }
  if (stat)
  {
    delete stat;
    return SFS_OK;
  }
  else
  {

    errno = ENODATA;
    return SFS_ERROR;
  }
}

//------------------------------------------------------------------------------
// Delete file by path
//------------------------------------------------------------------------------

int
XrdIo::Delete (const char* url)
{
  XrdCl::URL xUrl(url);
  XrdCl::FileSystem fs(xUrl);

  Attr xAttr(url);

  XrdCl::XRootDStatus status = fs.Rm(xUrl.GetPath());
  XrdCl::XRootDStatus status_attr = fs.Rm(xAttr.GetUrl());
  errno = 0;
  if (!status.IsOK())
  {

    eos_err("error=failed to delete file - %s", url);
    mLastErrMsg = "failed to delete file";
    errno = EIO;
    return SFS_ERROR;
  }
  return true;
}


//------------------------------------------------------------------------------
// Prefetch block using the readahead mechanism
//------------------------------------------------------------------------------

bool
XrdIo::PrefetchBlock (int64_t offset, bool isWrite, uint16_t timeout)
{
  bool done = true;
  XrdCl::XRootDStatus status;
  ReadaheadBlock* block = NULL;
  eos_debug("try to prefetch with offset: %lli, length: %4u",
            offset, mBlocksize);

  if (!mQueueBlocks.empty())
  {
    block = mQueueBlocks.front();
    mQueueBlocks.pop();
  }
  else
  {
    done = false;
    return done;
  }

  block->handler->Update(offset, mBlocksize, isWrite);
  status = mXrdFile->Read(offset,
                          mBlocksize,
                          block->buffer,
                          block->handler,
                          timeout);

  if (!status.IsOK())
  {
    // Create tmp status which is deleted in the HandleResponse method
    XrdCl::XRootDStatus* tmp_status = new XrdCl::XRootDStatus(status);
    block->handler->HandleResponse(tmp_status, NULL);
    mQueueBlocks.push(block);
    done = false;
  }
  else
  {

    mMapBlocks.insert(std::make_pair(offset, block));
  }

  return done;
}


//------------------------------------------------------------------------------
// Get pointer to async meta handler object
//------------------------------------------------------------------------------

void*
XrdIo::GetAsyncHandler ()
{

  return static_cast<void*> (mMetaHandler);
}


//------------------------------------------------------------------------------
// Run a space query command as statfs
//------------------------------------------------------------------------------

int
XrdIo::Statfs (const char* path, struct statfs* sfs)
{
  XrdCl::URL xUrl(path);
  XrdCl::FileSystem fs(xUrl);

  XrdCl::Buffer *response = 0;
  XrdCl::Buffer arg(xUrl.GetPath().size());
  arg.FromString(xUrl.GetPath());

  XrdCl::XRootDStatus status = fs.Query(
                                        XrdCl::QueryCode::Space,
                                        arg,
                                        response,
                                        (uint16_t) 15);

  errno = 0;

  if (!status.IsOK())
  {
    eos_err("msg=\"failed to statfs remote XRootD\" url=\"%s\"", path);
    mLastErrMsg = "failed to statfs remote XRootD";
    errno = EREMOTEIO;
    return errno;
  }

  if (response)
  {
    //  oss.cgroup=default&oss.space=469799256416256&oss.free=468894771826688&oss.maxf=68719476736&oss.used=904484589568&oss.quota=469799256416256
    XrdOucEnv spaceEnv(response->ToString().c_str());

    unsigned long long free_bytes = 0;
    unsigned long long used_bytes = 0;
    unsigned long long total_bytes = 0;
    unsigned long long max_file = 0;

    if (spaceEnv.Get("oss.free"))
    {
      free_bytes = strtoull(spaceEnv.Get("oss.free"), 0, 10);
    }
    else
    {
      errno = EINVAL;
      return errno;
    }

    if (spaceEnv.Get("oss.used"))
    {
      used_bytes = strtoull(spaceEnv.Get("oss.used"), 0, 10);
    }
    else
    {
      errno = EINVAL;
      return errno;
    }

    if (spaceEnv.Get("oss.maxf"))
    {
      max_file = strtoull(spaceEnv.Get("oss.maxf"), 0, 10);
    }
    else
    {
      errno = EINVAL;
      return errno;
    }

    if (spaceEnv.Get("oss.space"))
    {
      total_bytes = strtoull(spaceEnv.Get("oss.space"), 0, 10);
    }
    else
    {
      errno = EINVAL;
      return errno;
    }

    sfs->f_frsize = 4096;
    sfs->f_bsize = sfs->f_frsize;
    sfs->f_blocks = (fsblkcnt_t) (total_bytes / sfs->f_frsize);
    sfs->f_bavail = (fsblkcnt_t) (free_bytes / sfs->f_frsize);
    sfs->f_bfree = sfs->f_bavail;
    sfs->f_files = 1000000;
    sfs->f_ffree = 1000000;
    delete response;

    return 0;
  }
  else
  {

    errno = EREMOTEIO;
    return errno;
  }
}

//------------------------------------------------------------------------------
// Attribute Interface
//------------------------------------------------------------------------------


//----------------------------------------------------------------
//! Set a binary attribute (name has to start with 'user.' !!!)
// ------------------------------------------------------------------------

bool
XrdIo::Attr::Set (const char* name, const char* value, size_t len)
{
  std::string lBlob;
  // download
  if (!XrdIo::Download(mUrl, lBlob))
  {
    if (mFileMap.Load(lBlob))
    {
      std::string key = name;
      std::string val;
      val.assign(value, len);
      mFileMap.Set(key, val);
      std::string lMap = mFileMap.Trim();
      fprintf(stderr, "### %s", lMap.c_str());
      if (!XrdIo::Upload(mUrl, lMap))
      {
        return true;
      }
      else
      {
        eos_static_err("msg=\"unable to upload to remote file map\" url=\"%s\"",
                       mUrl.c_str());
      }
    }
    else
    {
      eos_static_err("msg=\"unable to parse remote file map\" url=\"%s\"",
                     mUrl.c_str());
      errno = EINVAL;
    }
  }
  else
  {

    eos_static_err("msg=\"unable to download remote file map\" url=\"%s\"",
                   mUrl.c_str());
  }

  return false;
}

// ------------------------------------------------------------------------
//! Set a string attribute (name has to start with 'user.' !!!)
// ------------------------------------------------------------------------

bool
XrdIo::Attr::Set (std::string key, std::string value)
{

  return Set(key.c_str(), value.c_str(), value.length());
}


// ------------------------------------------------------------------------
//! Get a binary attribute by name (name has to start with 'user.' !!!)
// ------------------------------------------------------------------------

bool
XrdIo::Attr::Get (const char* name, char* value, size_t &size)
{
  std::string lBlob;
  if (!XrdIo::Download(mUrl, lBlob))
  {
    if (mFileMap.Load(lBlob))
    {
      std::string val = mFileMap.Get(name);
      size_t len = val.length() + 1;
      if (len > size)
        len = size;
      memcpy(value, val.c_str(), len);
      eos_static_info("key=%s value=%s", name, value);
      return true;
    }
  }
  else
  {

    eos_static_err("msg=\"unable to download remote file map\" url=\"%s\"",
                   mUrl.c_str());
  }
  return false;
}

// ------------------------------------------------------------------------
//! Get a string attribute by name (name has to start with 'user.' !!!)
// ------------------------------------------------------------------------

std::string
XrdIo::Attr::Get (std::string name)
{
  std::string lBlob;
  if (!XrdIo::Download(mUrl, lBlob))
  {
    if (mFileMap.Load(lBlob))
    {
      return mFileMap.Get(name);
    }
  }
  else
  {

    eos_static_err("msg=\"unable to download remote file map\" url=\"%s\"",
                   mUrl.c_str());
  }
  return "";
}

// ------------------------------------------------------------------------
//! Factory function to create an attribute object
// ------------------------------------------------------------------------

XrdIo::Attr*
XrdIo::Attr::OpenAttr (const char* url)
{

  return new XrdIo::Attr(url);
}

// ------------------------------------------------------------------------
//! Non static Factory function to create an attribute object
// ------------------------------------------------------------------------

XrdIo::Attr*
XrdIo::Attr::OpenAttribute (const char* url)
{

  return OpenAttr(url);
}

// ------------------------------------------------------------------------
// Constructor
// ------------------------------------------------------------------------

XrdIo::Attr::Attr (const char* url)
{
  mUrl = url;
  size_t rfind = mUrl.rfind("/");
  if (rfind != std::string::npos)
  {

    mUrl.insert(rfind + 1, ".");
  }
  mUrl += ".xattr";
}


//--------------------------------------------------------------------------
//! traversing filesystem/storage routines
//--------------------------------------------------------------------------

//--------------------------------------------------------------------------
//! Open a curser to traverse a storage system
//--------------------------------------------------------------------------

FileIo::FtsHandle*
XrdIo::ftsOpen (std::string subtree)
{

  XrdCl::URL url(subtree);
  XrdCl::FileSystem fs(url);
  std::vector<std::string> files;
  std::vector<std::string> directories;

  XrdCl::XRootDStatus status =
          XrdIo::GetDirList(&fs,
                            url,
                            &files,
                            &directories);

  if (!status.IsOK())
  {
    eos_err("error=listing remote XrdClFile - %s", status.ToString().c_str());
    errno = status.errNo;
    mLastErrMsg = status.ToString().c_str();
    return 0;
  }


  FtsHandle* handle = new FtsHandle(subtree.c_str());
  if (!handle)
    return 0;

  for (auto it = files.begin(); it != files.end(); ++it)
  {
    XrdOucString fname = it->c_str();
    // skip attribute files
    if (fname.beginswith(".") && fname.endswith(".xattr"))
      continue;
    handle->found_files.push_back(subtree + *it);
  }

  for (auto it = directories.begin(); it != directories.end(); ++it)
  {
    eos_info("adding dir=%s deepness=%d", (subtree + *it + "/").c_str(), handle->deepness);
    handle->found_dirs[0].push_back(subtree + *it + "/");
  }

  return (FileIo::FtsHandle*) (handle);
}

//--------------------------------------------------------------------------
//! Return the next path related to a traversal cursor obtained with ftsOpen
//--------------------------------------------------------------------------

std::string
XrdIo::ftsRead (FileIo::FtsHandle* fts_handle)
{
  FtsHandle* handle = (FtsHandle*) fts_handle;
  if (!handle->found_files.size())
  {
    do
    {
      XrdCl::XRootDStatus status;
      std::vector<std::string> files;
      std::vector<std::string> directories;

      auto dit = handle->found_dirs[handle->deepness].begin();
      if (dit == handle->found_dirs[handle->deepness].end())
      {
        // move to next level
        handle->deepness++;
        handle->found_dirs.resize(handle->deepness + 1);
        if (!handle->found_dirs[handle->deepness].size())
          break;
      }

      eos_info("searching at deepness=%d directory=%s", handle->deepness, dit->c_str());
      XrdCl::URL url(*dit);
      XrdCl::FileSystem fs(url);

      status = XrdIo::GetDirList(&fs,
                                 url,
                                 &files,
                                 &directories);


      if (!status.IsOK())
      {
        eos_err("error=listing remote XrdClFile - %s", status.ToString().c_str());
        errno = status.errNo;
        mLastErrMsg = status.ToString().c_str();
        return "";
      }
      else
      {
        handle->found_dirs[handle->deepness].erase(dit);
      }

      for (auto it = files.begin(); it != files.end(); ++it)
      {
        XrdOucString fname = it->c_str();
        if (fname.beginswith(".") && fname.endswith(".xattr"))
          continue;
        eos_info("adding file=%s", (*dit + *it).c_str());
        handle->found_files.push_back(*dit + *it);
      }

      for (auto it = directories.begin(); it != directories.end(); ++it)
      {
        eos_info("adding dir=%s deepness=%d", (*dit + *it + "/").c_str(), handle->deepness + 1);
        handle->found_dirs[handle->deepness + 1].push_back(*dit + *it + "/");
      }
    }
    while (!handle->found_files.size());
  }
  if (handle->found_files.size())
  {
    std::string new_path = handle->found_files.front();
    handle->found_files.pop_front();
    return new_path;
  }
  return "";
}

//--------------------------------------------------------------------------
//! Close a traversal cursor
//--------------------------------------------------------------------------

int
XrdIo::ftsClose (FileIo::FtsHandle* fts_handle)
{
  FtsHandle* handle = (FtsHandle*) fts_handle;
  handle->found_files.clear();
  handle->found_dirs.resize(1);
  handle->found_dirs[0].resize(1);
  handle->deepness = 0;
  return 0;
}

//--------------------------------------------------------------------------
//! Download a remote file into a string object
//--------------------------------------------------------------------------

int
XrdIo::Download (std::string url, std::string& download)
{
  errno = 0;
  static int s_blocksize = 65536;
  XrdIo io;

  off_t offset = 0;
  std::string opaque;
  if (!io.Open(url.c_str(), 0, 0, opaque, 10))
  {
    ssize_t rbytes = 0;
    download.resize(s_blocksize);
    do
    {
      rbytes = io.Read(offset, (char*) download.c_str(), s_blocksize, 30);
      if (rbytes == s_blocksize)
      {
        download.resize(download.size() + 65536);
      }
      if (rbytes > 0)
      {
        offset += rbytes;
      }
    }
    while (rbytes == s_blocksize);
    io.Close();
    download.resize(offset);
    return 0;
  }

  if (errno == 3011)
    return 0;
  return -1;
}

//--------------------------------------------------------------------------
//! Upload a string object into a remote file
//--------------------------------------------------------------------------

int
XrdIo::Upload (std::string url, std::string& upload)
{
  errno = 0;
  XrdIo io;

  std::string opaque;
  int rc = 0;

  if (!io.Open(url.c_str(),
               SFS_O_WRONLY | SFS_O_CREAT, S_IRWXU | S_IRGRP | SFS_O_MKPTH,
               opaque,
               10))
  {
    eos_static_info("opened %s", url.c_str());
    if ((io.Write(0, upload.c_str(), upload.length(), 30)) != (ssize_t) upload.length())
    {
      eos_static_err("failed to write %d", upload.length());
      rc = -1;
    }
    else
    {
      eos_static_info("uploaded %d\n", upload.length());
    }
    io.Close();
  }
  else
  {

    eos_static_err("failed to open %s", url.c_str());
    rc = -1;
  }
  return rc;
}


//------------------------------------------------------------------------------
// Get a list of files and a list of directories inside a remote directory
//------------------------------------------------------------------------------

XrdCl::XRootDStatus
XrdIo::GetDirList (XrdCl::FileSystem *fs,
                   const XrdCl::URL &url,
                   std::vector<std::string> *files,
                   std::vector<std::string> *directories)
{
  eos_info("url=%s", url.GetURL().c_str());
  using namespace XrdCl;
  DirectoryList *list;
  XrdCl::XRootDStatus status;

  status = fs->DirList(url.GetPath(), DirListFlags::Stat, list);
  if (!status.IsOK())
  {
    return status;
  }
  for (DirectoryList::Iterator it = list->Begin(); it != list->End(); ++it)
  {
    if ((*it)->GetStatInfo()->TestFlags(StatInfo::IsDir))
    {
      std::string directory = (*it)->GetName();
      directories->push_back(directory);
    }
    else
    {
      std::string file = (*it)->GetName();
      files->push_back(file);
    }
  }
  return XRootDStatus();
}

EOSFSTNAMESPACE_END

