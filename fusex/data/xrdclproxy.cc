//------------------------------------------------------------------------------
//! @file xrdclproxy.cc
//! @author Andreas-Joachim Peters CERN
//! @brief XrdCl proxy class
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "xrdclproxy.hh"
#include "common/Logging.hh"
#include "common/Path.hh"
#include "XrdCl/XrdClXRootDResponses.hh"

using namespace XrdCl;

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::Write( uint64_t         offset,
                    uint32_t         size,
                    const void      *buffer,
                    ResponseHandler *handler,
                    uint16_t         timeout)
/* -------------------------------------------------------------------------- */
{
  eos_debug("offset=%lu size=%u", offset, size);
  XRootDStatus status = WaitOpen();
  if (!status.IsOK())
    return status;
  return File::Write(offset, size, buffer, handler, timeout);
}

/* -------------------------------------------------------------------------- */
XRootDStatus
XrdCl::Proxy::Read( uint64_t  offset,
                   uint32_t  size,
                   void     *buffer,
                   uint32_t &bytesRead,
                   uint16_t  timeout)
/* -------------------------------------------------------------------------- */
{
  eos_debug("offset=%lu size=%u", offset, size);
  XRootDStatus status = WaitOpen();

  if (!status.IsOK())
    return status;

  eos_debug("----: read: offset=%lu size=%u", offset, size);
  int readahead_window_hit = 0;

  uint64_t current_offset = offset;
  uint32_t current_size = size;

  bool isEOF=false;
  bool request_next = true;
  std::set<uint64_t> delete_chunk;

  void *pbuffer = buffer;


  if (XReadAheadStrategy != NONE)
  {
    ReadCondVar().Lock();

    if ( ChunkRMap().size())
    {
      bool has_successor = false;
      // see if there is anything in our read-ahead map
      for ( auto it = ChunkRMap().begin(); it != ChunkRMap().end(); ++it)
      {
        off_t match_offset;
        uint32_t match_size;

        eos_debug("----: eval offset=%lu chunk-offset=%lu", offset, it->second->offset());
        if (it->second->matches(current_offset, current_size, match_offset, match_size))
        {
          readahead_window_hit++;

          XrdSysCondVarHelper lLock(it->second->ReadCondVar());
          while ( !it->second->done() )
            it->second->ReadCondVar().WaitMS(25);

          status = it->second->Status();

          if (it->second->Status().IsOK())
          {
            // the match result can change after the read actually returned
            if (!it->second->matches(current_offset, current_size, match_offset, match_size))
            {
              continue;
            }

            eos_debug("----: prefetched offset=%lu m-offset=%lu current-size=%u m-size=%u dim=%ld", current_offset, match_offset, current_size, match_size, (char*) buffer - (char*) pbuffer);
            // just copy what we have
            eos_debug("----: out-buffer=%lx in-buffer=%lx in-buffer-size=%lu", (long unsigned int) buffer, (long unsigned int) it->second->buffer(), it->second->vbuffer().size());


            memcpy(buffer, it->second->buffer() + match_offset - it->second->offset(), match_size);
            bytesRead += match_size;
            mTotalReadAheadHitBytes += match_size;
            buffer = (char*) buffer + match_size;
            current_offset = match_offset + match_size;
            current_size -= match_size;

            isEOF = it->second->eof();
            if (isEOF)
            {
              request_next = false;
              break;
            }
          }
        }
        else
        {
          eos_debug("----: considering chunk address=%lx offset=%ld", it->first, it->second->offset());
          if (!it->second->successor(offset, size))
          {
            eos_debug("----: delete chunk address=%lx offset=%ld", it->first, it->second->offset());
            while ( !it->second->done() )
              it->second->ReadCondVar().WaitMS(25);
            // remove this chunk
            delete_chunk.insert(it->first);
            request_next = false;
          }
          else
          {
            has_successor = true;
          }
        }
      }

      if (!has_successor)
        request_next = true;
      else
        request_next = false;

      for ( auto it = delete_chunk.begin(); it != delete_chunk.end(); ++it)
      {
        ChunkRMap().erase(*it);
      }
    }
    else
    {
      if ((off_t) offset == mPosition)
      {
        // re-enable read-ahead if sequential reading occurs 
        request_next = true;
      }
      else
      {
        request_next = false;
      }
    }

    if (request_next)
    {
      // dynamic window scaling
      if (readahead_window_hit)
      {
        if (XReadAheadStrategy == DYNAMIC)
        {
          // increase the read-ahead window
          XReadAheadNom *=2;
          if (XReadAheadNom > XReadAheadMax)
            XReadAheadNom = XReadAheadMax;
        }
      }

      off_t align_offset = aligned_offset( ChunkRMap().size() ? offset + XReadAheadNom : offset);
      eos_debug("----: pre-fetch window=%lu pf-offset=%lu,",
                XReadAheadNom,
                (unsigned long) align_offset
                );

      if (ChunkRMap().count(align_offset))
      {
        ReadCondVar().UnLock();
      }
      else
      {
        ReadCondVar().UnLock();
        XrdCl::Proxy::read_handler rahread = ReadAsyncPrepare(align_offset, XReadAheadNom);
        XRootDStatus rstatus = PreReadAsync(align_offset, XReadAheadNom,
                                            rahread, timeout);
      }
    }
    else
    {
      ReadCondVar().UnLock();
    }
  }

  if (current_size)
  {
    uint32_t rbytes_read=0;
    status = File::Read(current_offset,
                        current_size,
                        buffer, rbytes_read, timeout);
    if (status.IsOK())
    {
      if (rbytes_read)
      {
        eos_debug("----: postfetched offset=%lu size=%u rbytes=%d", current_offset, current_size, rbytes_read);
      }
      bytesRead+=rbytes_read;
    }
  }
  if (status.IsOK())
  {

    mPosition = offset + size;
    mTotalBytes += bytesRead;
  }
  return status;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::OpenAsync( const std::string &url,
                        OpenFlags::Flags   flags,
                        Access::Mode       mode,
                        uint16_t         timeout)
/* -------------------------------------------------------------------------- */
{
  eos_debug("url=%s flags=%x mode=%x", url.c_str(), (int) flags, (int) mode);
  XrdSysCondVarHelper lLock(OpenCondVar());

  mUrl = url;
  
  if ( state() == OPENING )
  {
    XRootDStatus status(XrdCl::stError,
                        suAlreadyDone,
                        XrdCl::errInProgress,
                        "in progress"
                        );
    return status;
  }

  if ( state() == OPENED )
  {
    XRootDStatus status(XrdCl::stOK,
                        0,
                        0,
                        "opened"
                        );
    return status;
  }

  if ( state() == FAILED )
  {

    return XOpenState;
  }

  // Disable recovery on read and write
#if kXR_PROTOCOLVERSION == 0x00000297
  ((XrdCl::File*)(this))->EnableReadRecovery(false);
  ((XrdCl::File*)(this))->EnableWriteRecovery(false);
#else
  SetProperty("ReadRecovery", "false");
  SetProperty("WriteRecovery", "false");
#endif

  set_state(OPENING);

  XrdCl::XRootDStatus status = Open(url.c_str(),
                                    flags,
                                    mode,
                                    &XOpenAsyncHandler,
                                    timeout);


  return XOpenState;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::OpenAsyncHandler::HandleResponseWithHosts(XrdCl::XRootDStatus* status,
                                                        XrdCl::AnyObject* response,
                                                        XrdCl::HostList * hostList)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("");

  XrdSysCondVarHelper lLock(proxy()->OpenCondVar());
  if (status->IsOK())
  {

    proxy()->set_state(OPENED);
  }
  else
  {

    proxy()->set_state(FAILED, status);
  }

  proxy()->OpenCondVar().Signal();

  delete hostList;
  delete status;
  if (response) delete response;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::CloseAsync(uint16_t         timeout)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  // don't close files attached by several clients
  if (mAttached > 1)
  {
    eos_debug("still attached");
    return XRootDStatus();
  }

  WaitOpen();
  XrdSysCondVarHelper lLock(OpenCondVar());

  XrdCl::XRootDStatus status = XrdCl::File::Close(&XCloseAsyncHandler,
                                                  timeout);
  set_state(CLOSING, &status);
  return XOpenState;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::Close(uint16_t         timeout)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  // don't close files attached by several clients
  if (mAttached > 1)
    return XRootDStatus();

  WaitOpen();
  Collect();
  XrdSysCondVarHelper lLock(OpenCondVar());

  XrdCl::XRootDStatus status = XrdCl::File::Close(timeout);
  set_state(CLOSED, &status);
  return status;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::CloseAsyncHandler::HandleResponse (XrdCl::XRootDStatus* status,
                                                 XrdCl::AnyObject * response)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("");
  XrdSysCondVarHelper lLock(mProxy->OpenCondVar());
  if (!status->IsOK())
  {

    mProxy->set_state(XrdCl::Proxy::CLOSEFAILED, status);
  }
  else
  {
    mProxy->set_state(XrdCl::Proxy::CLOSED, status);
  }

  mProxy->OpenCondVar().Signal();
  delete response;
  delete status;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::WaitClose()
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  Collect();
  XrdSysCondVarHelper lLock(OpenCondVar());

  while (state () == CLOSING)
    OpenCondVar().WaitMS(25);

  return XOpenState;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::WaitOpen()
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  XrdSysCondVarHelper lLock(OpenCondVar());

  while (state () == OPENING)
    OpenCondVar().WaitMS(25);

  return XOpenState;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::IsOpening()
/* -------------------------------------------------------------------------- */
{
  XrdSysCondVarHelper lLock(OpenCondVar());
  eos_debug("state=%d", state());
  return (state () == OPENING) ? true : false;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::IsClosing()
/* -------------------------------------------------------------------------- */
{
  XrdSysCondVarHelper lLock(OpenCondVar());
  eos_debug("state=%d", state());
  return (state () == CLOSING) ? true : false;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::IsOpen()
/* -------------------------------------------------------------------------- */
{
  XrdSysCondVarHelper lLock(OpenCondVar());
  eos_debug("state=%d", state());
  return (state () == OPENED) ? true : false;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::IsClosed()
/* -------------------------------------------------------------------------- */
{
  XrdSysCondVarHelper lLock(OpenCondVar());
  eos_debug("state=%d", state());
  return ( (state () == CLOSED) || (state () == CLOSEFAILED) ) ? true : false;
}

bool

XrdCl::Proxy::IsWaitWrite()
{
  XrdSysCondVarHelper lLock(OpenCondVar());
  eos_debug("state=%d", state());
  return (state () == WAITWRITE) ? true : false;
}

bool
XrdCl::Proxy::HadFailures(std::string &message)
{
  bool ok=true;
  XrdSysCondVarHelper lLock(OpenCondVar());
  if ( state () == CLOSEFAILED)
  {
    message = "file close failed";
    ok = false;
  }
  if ( state () == FAILED)
  {
    message = "file open failed";
    ok = false;
  }
  if ( !write_state().IsOK())
  {
    message = "file writing failed";
    ok = false;
  }
  eos_debug("state=%d had-failures=%d", state(), !ok);
  return !ok;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::WriteAsyncHandler::HandleResponse (XrdCl::XRootDStatus* status,
                                                 XrdCl::AnyObject * response)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("");
  XrdSysCondVarHelper lLock(mProxy->WriteCondVar());
  if (!status->IsOK())
  {

    mProxy->set_writestate(status);
  }
  mProxy->WriteCondVar().Signal();
  delete response;
  delete status;
  mProxy->ChunkMap().erase((uint64_t)this);
}

/* -------------------------------------------------------------------------- */
XrdCl::Proxy::write_handler
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::WriteAsyncPrepare(uint32_t size)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  write_handler dst = std::make_shared<WriteAsyncHandler>(this, size);
  XrdSysCondVarHelper lLock(WriteCondVar());
  ChunkMap()[(uint64_t) dst.get()] = dst;
  return dst;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::WriteAsync( uint64_t         offset,
                         uint32_t         size,
                         const void      *buffer,
                         XrdCl::Proxy::write_handler handler,
                         uint16_t         timeout)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  handler->copy(buffer, size);

  XRootDStatus status = Write(static_cast<uint64_t> (offset),
                              static_cast<uint32_t> (size),
                              handler->buffer(), handler.get(), timeout);

  if (!status.IsOK())
  {
    // remove failing requests
    XrdSysCondVarHelper lLock(WriteCondVar());
    ChunkMap().erase((uint64_t) handler.get());
  }
  return status;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::WaitWrite()
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  XrdSysCondVarHelper lLock(WriteCondVar());

  while ( ChunkMap().size() )
  {

    eos_debug("     [..] map-size=%lu", ChunkMap().size());
    WriteCondVar().WaitMS(1000);
  }
  eos_debug(" [..] map-size=%lu", ChunkMap().size());
  return XOpenState;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::OutstandingWrites()
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  XrdSysCondVarHelper lLock(WriteCondVar());
  return ChunkMap().size() ? true : false;
}

void
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::ReadAsyncHandler::HandleResponse (XrdCl::XRootDStatus* status,
                                                XrdCl::AnyObject * response)
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("");
  XrdSysCondVarHelper lLock(ReadCondVar());
  mStatus = *status;
  if (status->IsOK())
  {
    XrdCl::ChunkInfo* chunk=0;
    if (response)
    {
      response->Get(chunk);
      if (chunk->length < mBuffer.size())
      {
        mBuffer.resize(chunk->length);
        mEOF = true;
      }
      delete response;
    }

    else
      mBuffer.resize(0);
  }
  mDone = true;
  delete status;
  ReadCondVar().Signal();
}

/* -------------------------------------------------------------------------- */
XrdCl::Proxy::read_handler
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::ReadAsyncPrepare(off_t offset, uint32_t size)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  read_handler src = std::make_shared<ReadAsyncHandler>(this, offset, size);
  XrdSysCondVarHelper lLock(ReadCondVar());
  ChunkRMap()[(uint64_t) src->offset()] = src;
  ReadCondVar().Signal();
  return src;
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::PreReadAsync( uint64_t         offset,
                           uint32_t          size,
                           read_handler handler,
                           uint16_t          timeout)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  XRootDStatus status = WaitOpen();

  if (!status.IsOK())
    return status;

  return File::Read(static_cast<uint64_t> (offset),
                    static_cast<uint32_t> (size),
                    (void*) handler->buffer(), handler.get(), timeout);
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::WaitRead(read_handler handler)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  XrdSysCondVarHelper lLock(handler->ReadCondVar());

  while ( !handler->done() )
  {

    handler->ReadCondVar().WaitMS(1000);
  }
  eos_debug(" [..] read-size=%lu", handler->vbuffer().size());
  return handler->Status();
}

/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::ReadAsync(read_handler handler,
                        uint32_t  size,
                        void     *buffer,
                        uint32_t & bytesRead)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  XRootDStatus status = WaitRead(handler);
  if (!status.IsOK())
    return status;
  bytesRead = (size < handler->vbuffer().size()) ? size : handler->vbuffer().size();
  memcpy(buffer, handler->buffer(), bytesRead);
  return status;
}


/* -------------------------------------------------------------------------- */
XRootDStatus
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::Sync( uint16_t timeout )
{
  eos_debug("");
  return File::Sync ( timeout );
}


/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::attach()
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lLock(mAttachedMutex);
  mAttached++;
  eos_debug("attached=%u", mAttached);
  return;
}

/* -------------------------------------------------------------------------- */
size_t
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::detach()
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lLock(mAttachedMutex);
  mAttached--;
  eos_debug("attached=%u", mAttached);
  return mAttached;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
XrdCl::Proxy::attached()
/* -------------------------------------------------------------------------- */
{

  XrdSysMutexHelper lLock(mAttachedMutex);

  return mAttached ? true : false;
}

