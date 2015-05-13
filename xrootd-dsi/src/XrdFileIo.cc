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

//------------------------------------------------------------------------------
//! @file XrdFileIo.cc
//! @author Elvin-Alin Sindrilaru - CERN , Geoffray Adde - CERN
//! @brief Class used for doing remote IO operations using the Xrd client
//!
//! @details The following code has been extracted from EOS.
//! The API signatures have been slightly modified to better fit the needs.
//! The logic itself remains unchanged.
//------------------------------------------------------------------------------

/*----------------------------------------------------------------------------*/
#include "XrdFileIo.hh"
#include "ChunkHandler.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
/*----------------------------------------------------------------------------*/
#include <stdint.h>
#include <cstdlib>
/*----------------------------------------------------------------------------*/
extern "C" {
#include "globus_gridftp_server.h"
}

uint64_t ReadaheadBlock::sDefaultBlocksize = 1024 * 1024; ///< 1MB default
uint32_t XrdFileIo::sNumRdAheadBlocks = 2;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------

XrdFileIo::XrdFileIo () :
    mIndex (0),
    mDoReadahead (false),
    mBlocksize (ReadaheadBlock::sDefaultBlocksize),
    mXrdFile (NULL) {
  // empty
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------

XrdFileIo::~XrdFileIo ()
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

XrdCl::XRootDStatus
XrdFileIo::Open (const std::string& path,
    XrdCl::OpenFlags::Flags flags,
    mode_t mode,
    bool readahead)
{
  mDoReadahead=readahead;

  for (unsigned int i = 0; i < sNumRdAheadBlocks; i++)
  {
    mQueueBlocks.push(new ReadaheadBlock(mBlocksize));
  }

  mXrdFile = new XrdCl::File();

  return(
      mXrdFile->Open(path, flags,
          (XrdCl::Access::Mode) mode) );
}


//------------------------------------------------------------------------------
// Read from file - sync
//------------------------------------------------------------------------------

int64_t
XrdFileIo::Read (uint64_t offset,
    char* buffer,
    uint32_t length)
{
  uint32_t bytes_read = 0;
  XrdCl::XRootDStatus status = mXrdFile->Read(offset,
      length,
      buffer,
      bytes_read);

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
XrdFileIo::Write (uint64_t offset,
    const char* buffer,
    uint32_t length)
{
  XrdCl::XRootDStatus status = mXrdFile->Write(offset,
      length,
      buffer);

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
XrdFileIo::Read (uint64_t offset,
    char* buffer,
    uint32_t length,
    void* pFileHandler,
    bool readahead,
    bool *usedCallBack)
{
  int64_t nread = 0;
  char* pBuff = buffer;
  XrdCl::XRootDStatus status;
  ChunkHandler* handler = NULL;
  if(usedCallBack) *usedCallBack=false;

  if (!mDoReadahead)
  {
    readahead = false;
  }

  if (!readahead)
  {
    handler = static_cast<AsyncMetaHandler*> (pFileHandler)->Register(offset, length, false);
    status = mXrdFile->Read(offset,
        length,
        buffer,
        static_cast<XrdCl::ResponseHandler*> (handler));
    if(usedCallBack) *usedCallBack=true;
    nread += length;
  }
  else
  {
    int64_t read_length,resp_length,actual_length,aoffset,shift;
    std::map<uint64_t, ReadaheadBlock*>::iterator iter;

    while (length)
    {
      shift   = offset%mBlocksize;
      aoffset = offset-shift;
      iter = mMapBlocks.find(aoffset);

      if (iter != mMapBlocks.end())
      {
        //......................................................................
        // Block found in prefetched blocks
        //......................................................................
        SimpleHandler* sh = iter->second->handler;

        if (sh->WaitOK())
        {
          resp_length=sh->GetRespLength();
          if(resp_length<=shift) { length=0; break;} /// if the block doesn't contain any relevant data

          read_length = (length < (int64_t) mBlocksize-shift) ? length : (mBlocksize-shift);
          actual_length = std::min((size_t)resp_length-shift,(size_t)read_length);

          pBuff = static_cast<char*> (memcpy(pBuff,
              iter->second->buffer+shift,
              actual_length)); /// read_length is the old value

          //....................................................................
          // We can prefetch another block if we still have available blocks in
          // the queue or if first read was from second prefetched block
          //....................................................................
          if ( (!mQueueBlocks.empty() ||
              ((pBuff == buffer) && (iter != mMapBlocks.begin())))
              //)
              && (resp_length==mBlocksize))
          {
            if (iter != mMapBlocks.begin())
            {
              mQueueBlocks.push(mMapBlocks.begin()->second);
              mMapBlocks.erase(mMapBlocks.begin());
            }

            PrefetchBlock(aoffset + mBlocksize, false);
          }

          pBuff += actual_length;
          offset += actual_length;
          length -= actual_length;
          nread += actual_length;

          if(shift+actual_length<mBlocksize) { length=0; break;}
        }
        else
        {
          //....................................................................
          // Error while prefetching, remove block from map
          //....................................................................
          mQueueBlocks.push(iter->second);
          mMapBlocks.erase(iter);
          break;
        }
      }
      else
      {
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
          PrefetchBlock(offset, false);
        }
      }
    }

    //..........................................................................
    // If readahead not useful, use the classic way to read
    //..........................................................................
    if (length)
    {
      handler = static_cast<AsyncMetaHandler*> (pFileHandler)->Register(offset, length, false);
      status = mXrdFile->Read(offset,
          length,
          pBuff,
          handler);
      if(usedCallBack) *usedCallBack=true;
      nread += length;
    }
  }

  return nread;
}

//------------------------------------------------------------------------------
// Write to file - async
//------------------------------------------------------------------------------

int64_t
XrdFileIo::Write (uint64_t offset,
    const char* buffer,
    uint32_t length,
    void* pFileHandler)
{
  ChunkHandler* handler;
  XrdCl::XRootDStatus status;

  handler = static_cast<AsyncMetaHandler*> (pFileHandler)->Register(offset, length, true);
  status = mXrdFile->Write(offset,
      length,
      buffer,
      handler);

  if (!status.IsOK())
  {
    errno = status.errNo;
    return SFS_ERROR;
  }

  return length;
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------

int
XrdFileIo::Truncate (uint64_t offset)
{
  XrdCl::XRootDStatus status = mXrdFile->Truncate(offset);

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
XrdFileIo::Sync ()
{
  XrdCl::XRootDStatus status = mXrdFile->Sync();

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
XrdFileIo::Stat (struct stat* buf)
{
  int rc = SFS_ERROR;
  XrdCl::StatInfo* stat = 0;
  XrdCl::XRootDStatus status = mXrdFile->Stat(true, stat);

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
XrdFileIo::Close ()
{
  bool tmp_resp;

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
        tmp_resp = shandler->WaitOK();
      }
      delete mMapBlocks.begin()->second;
      mMapBlocks.erase(mMapBlocks.begin());
    }
  }

  XrdCl::XRootDStatus status = mXrdFile->Close();

  if (!status.IsOK())
  {
    errno = status.errNo;
    globus_gfs_log_message (GLOBUS_GFS_LOG_ERR, "!!OUPS %s\n", status.ToStr().c_str ());
    return SFS_ERROR;
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------

int
XrdFileIo::Remove ()
{
  //............................................................................
  // Remove the file by truncating using the special value offset
  //............................................................................
  XrdCl::XRootDStatus status = mXrdFile->Truncate(1024 * 1024 * 1024 * 1024ll);

  if (!status.IsOK())
  {
    //eos_err("error=failed to truncate file with deletion offset - %s", mPath.c_str());
    return SFS_ERROR;
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Prefetch block using the readahead mechanism
//------------------------------------------------------------------------------

void
XrdFileIo::PrefetchBlock (int64_t offset, bool isWrite)
{
  XrdCl::XRootDStatus status;
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
      block->handler);
  mMapBlocks.insert(std::make_pair(offset, block));
}

