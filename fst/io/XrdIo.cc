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
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClDefaultEnv.hh"
/*----------------------------------------------------------------------------*/


EOSFSTNAMESPACE_BEGIN

const uint64_t ReadaheadBlock::sDefaultBlocksize = 1024 * 1024; ///< 1MB default
const uint32_t XrdIo::sNumRdAheadBlocks = 2;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------

XrdIo::XrdIo () :
FileIo(),
mIndex (0),
mDoReadahead (false),
mBlocksize (ReadaheadBlock::sDefaultBlocksize),
mXrdFile (NULL)
{
  // Set the TimeoutResolution to 1 
  XrdCl::Env* env = XrdCl::DefaultEnv::GetEnv();
  env->PutInt( "TimeoutResolution", 1 );
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

  if (mXrdFile)
  {
    delete mXrdFile;
  }
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
  XrdOucEnv open_opaque(opaque.c_str());

  mFilePath = path;

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
  request += opaque;
  mXrdFile = new XrdCl::File();

  // Disable recover on read and write
  mXrdFile->EnableReadRecovery(false);
  mXrdFile->EnableWriteRecovery(false);
  
  XrdCl::OpenFlags::Flags flags_xrdcl = eos::common::LayoutId::MapFlagsSfs2XrdCl(flags);
  XrdCl::Access::Mode mode_xrdcl = eos::common::LayoutId::MapModeSfs2XrdCl(mode);
  XrdCl::XRootDStatus status = mXrdFile->Open(request, flags_xrdcl,
                                              mode_xrdcl, timeout);

  if (!status.IsOK())
  {
    eos_err("error=opening remote XrdClFile");
    errno = status.errNo;
    return SFS_ERROR;
  }
  else
  {
    errno = 0;
  }
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
  eos_debug("offset = %llu, length = %lu",
            static_cast<uint64_t> (offset),
            static_cast<uint32_t> (length));

  uint32_t bytes_read = 0;
  XrdCl::XRootDStatus status = mXrdFile->Read(static_cast<uint64_t> (offset),
                                              static_cast<uint32_t> (length),
                                              buffer,
                                              bytes_read,
                                              timeout);

  if (!status.IsOK())
  {
    errno = status.errNo;
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
  eos_debug("offset = %llu, length = %lu",
            static_cast<uint64_t> (offset),
            static_cast<uint32_t> (length));

  XrdCl::XRootDStatus status = mXrdFile->Write(static_cast<uint64_t> (offset),
                                               static_cast<uint32_t> (length),
                                               buffer,
                                               timeout);

  if (!status.IsOK())
  {
    errno = status.errNo;
    return SFS_ERROR;
  }

  return length;
}


//------------------------------------------------------------------------------
// Read from file - async
//------------------------------------------------------------------------------

int64_t
XrdIo::Read (XrdSfsFileOffset offset,
                 char* buffer,
                 XrdSfsXferSize length,
                 void* pFileHandler,
                 bool readahead,
                 uint16_t timeout)
{
  eos_debug("offset = %llu, length = %li",
            static_cast<uint64_t> (offset),
            (long int) length);

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
    handler = static_cast<AsyncMetaHandler*> (pFileHandler)->Register(offset, length, false);
    status = mXrdFile->Read(static_cast<uint64_t> (offset),
                            static_cast<uint32_t> (length),
                            buffer,
                            static_cast<XrdCl::ResponseHandler*> (handler),
                            timeout);
    nread += length;    
  }
  else
  {
    eos_debug("Readahead enabled and request offset=%lli, length=%lli", offset, length);
    int64_t read_length;
    int64_t aligned_offset;
    int64_t aligned_length;
    std::map<uint64_t, ReadaheadBlock*>::iterator iter;

    while (length)
    {
      aligned_offset = offset - (offset % mBlocksize);
      iter = mMapBlocks.find(aligned_offset);

      if (iter != mMapBlocks.end())
      {
        //......................................................................
        // Block found in prefetched blocks
        //......................................................................
        SimpleHandler* sh = iter->second->handler;
        
        if (sh->WaitOK())
        {
          aligned_length = sh->GetRespLength() - (offset % mBlocksize);
          eos_debug("Found block in cache, aligned_offset=%lld, aligned_length=%lld.",
                    aligned_offset, aligned_length);
          
          read_length = (length < aligned_length) ? length : aligned_length;
          pBuff = static_cast<char*> (memcpy(pBuff,
                                             iter->second->buffer,
                                             read_length));

          //....................................................................
          // We can prefetch another block if we still have available blocks in
          // the queue or if first read was from second prefetched block
          //....................................................................
          if (!mQueueBlocks.empty() ||
              ((pBuff == buffer) && (iter != mMapBlocks.begin())))
          {
            eos_debug("Prefetch new block(2).");

            if (iter != mMapBlocks.begin())
            {
              eos_debug("Recycle the oldest block. ");
              mQueueBlocks.push(mMapBlocks.begin()->second);
              mMapBlocks.erase(mMapBlocks.begin());
            }

            PrefetchBlock(aligned_offset + mBlocksize, false);
          }

          pBuff += read_length;
          offset += read_length;
          length -= read_length;
          nread += read_length;
        }
        else
        {
          //....................................................................
          // Error while prefetching, remove block from map
          //....................................................................
          mQueueBlocks.push(iter->second);
          mMapBlocks.erase(iter);
          eos_debug("Error while prefetching, remove block from map.");
          break;
        }
      }
      else
      {
        // TODO: deal here with the case we received a read timeout from prefetching
        eos_debug("Block not found in prefetched ones offset: %li", aligned_offset);
        
        //......................................................................
        // Remove first element from map and prefetch a new block 
        //......................................................................
        if (!mMapBlocks.empty())
        {
          mQueueBlocks.push(mMapBlocks.begin()->second);
          mMapBlocks.erase(mMapBlocks.begin());
        }

        if (!mQueueBlocks.empty())
        {
          eos_debug("Prefetch new block(1).");
          PrefetchBlock(aligned_offset, false);
        }
      }
    }

    //..........................................................................
    // If readahead not useful, use the classic way to read
    //..........................................................................
    if (length)
    {
      eos_debug("Readahead not useful, use the classic way for the rest fo the block.");
      handler = static_cast<AsyncMetaHandler*> (pFileHandler)->Register(offset, length, false);
      status = mXrdFile->Read(static_cast<uint64_t> (offset),
                              static_cast<uint32_t> (length),
                              pBuff,
                              handler,
                              timeout);
      nread += length;
    }
  }

  return nread;
}


//------------------------------------------------------------------------------
// Write to file - async
//------------------------------------------------------------------------------

int64_t
XrdIo::Write (XrdSfsFileOffset offset,
                  const char* buffer,
                  XrdSfsXferSize length,
                  void* pFileHandler,
                  uint16_t timeout)
{
  eos_debug("offset = %llu, length = %lu",
            static_cast<uint64_t> (offset),
            static_cast<uint32_t> (length));

  ChunkHandler* handler;
  XrdCl::XRootDStatus status;

  handler = static_cast<AsyncMetaHandler*> (pFileHandler)->Register(offset, length, true);
  status = mXrdFile->Write(static_cast<uint64_t> (offset),
                           static_cast<uint32_t> (length),
                           buffer,
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
  eos_debug("Calling XrdIo::Truncate with timeout = %lu", timeout);
  XrdCl::XRootDStatus status = mXrdFile->Truncate(static_cast<uint64_t> (offset),
                                                  timeout);
  
  if (!status.IsOK())
  {
    errno = status.errNo;
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
  // TODO: once Stat using the file handler works properly in XRootD, one can
  // revert the flag on the first position to true, so stat stat is forced and
  // not taken from the cache as it is the case now
  XrdCl::XRootDStatus status = mXrdFile->Stat(false, stat, timeout);

  if (!status.IsOK())
  {
    errno = status.errNo;
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
    return SFS_ERROR;
  }

  // If any of the async requests failes then we have an error
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
XrdIo::Remove ()
{
  //............................................................................
  // Remove the file by truncating using the special value offset
  //............................................................................
  XrdCl::XRootDStatus status = mXrdFile->Truncate(EOS_FST_DELETE_FLAG_VIA_TRUNCATE_LEN);

  if (!status.IsOK())
  {
    eos_err("error=failed to truncate file with deletion offset - %s", mPath.c_str());
    return SFS_ERROR;
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Prefetch block using the readahead mechanism
//------------------------------------------------------------------------------

void
XrdIo::PrefetchBlock (int64_t offset, bool isWrite, uint16_t timeout)
{
  XrdCl::XRootDStatus status;
  eos_debug("Try to prefetch with offset: %lli.", offset);
  ReadaheadBlock* block = NULL;

  if (!mQueueBlocks.empty())
  {
    block = mQueueBlocks.front();
    mQueueBlocks.pop();
  }
  else
  {
    return;
  }

  block->handler->Update(offset, mBlocksize, isWrite);
  status = mXrdFile->Read(offset,
                          mBlocksize,
                          block->buffer,
                          block->handler,
                          timeout);

  mMapBlocks.insert(std::make_pair(offset, block));
}


EOSFSTNAMESPACE_END

