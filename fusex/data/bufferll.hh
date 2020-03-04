//------------------------------------------------------------------------------
// File bufferll.hh
// Author: Andreas Peters <Andreas.Joachim.Peters@cern.ch> CERN
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

#ifndef __EOS_FUSE_BUFFERLL_HH__
#define __EOS_FUSE_BUFFERLL_HH__

#include "common/RWMutex.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <memory>
#include <vector>
#include <string.h>
#include <queue>

using namespace eos::common;

class bufferll : public std::vector<char>
{
public:

  bufferll(unsigned size = 0, unsigned capacity = 0)
  {
    if (size) {
      resize(size);
    }

    if (capacity) {
      reserve(capacity);
    }
  }

  virtual
  ~bufferll() { }

  //------------------------------------------------------------------------
  //! Add data
  //------------------------------------------------------------------------

  size_t
  putData(const void* ptr, size_t dataSize)
  {
    RWMutexWriteLock dLock(mMutex);
    size_t currSize = size();
    resize(currSize + dataSize);
    memcpy(&operator[](currSize), ptr, dataSize);
    return dataSize;
  }

  off_t
  writeData(const void* ptr, off_t offset, size_t dataSize)
  {
    RWMutexWriteLock dLock(mMutex);
    size_t currSize = size();

    if ((offset + dataSize) > currSize) {
      currSize = offset + dataSize;
      resize(currSize);

      if (currSize > capacity()) {
        reserve(capacity() * 2);
      }
    }

    memcpy(&operator[](offset), ptr, dataSize);
    return currSize;
  }

  //------------------------------------------------------------------------
  //! Retrieve data
  //------------------------------------------------------------------------

  size_t
  readData(void* ptr, off_t offset, size_t dataSize)
  {
    RWMutexReadLock dLock(mMutex);

    if (offset + dataSize > size()) {
      if ((size_t) offset > size()) {
        return 0;
      }

      dataSize = size() - offset;
    }

    memcpy(ptr, &operator[](offset), dataSize);
    return dataSize;
  }

  //------------------------------------------------------------------------
  //! peek data ( one has to call release claim aftewards )
  //------------------------------------------------------------------------

  size_t
  peekData(char*& ptr, off_t offset, size_t dataSize)
  {
    mMutex.LockRead();
    ptr = &(operator[](0)) + offset;
    int avail = size() - offset;

    if (((int) dataSize > avail)) {
      if (avail > 0) {
        return avail;
      } else {
        return 0;
      }
    }

    return dataSize;
  }

  //------------------------------------------------------------------------
  //! release a lock related to peekData
  //------------------------------------------------------------------------

  void
  releasePeek()
  {
    mMutex.UnLockRead();
  }

  //------------------------------------------------------------------------
  //! truncate a buffer
  //------------------------------------------------------------------------

  void
  truncateData(off_t offset)
  {
    RWMutexWriteLock dLock(mMutex);
    resize(offset);
    reserve(offset);
  }

  off_t getSize()
  {
    RWMutexReadLock dLock(mMutex);
    return size();
  }

  void zero()
  {
    RWMutexReadLock dLock(mMutex);
    memset(ptr(), 0, size());
  }

  //------------------------------------------------------------------------
  //! low-level pointer to the memory - better know what you do with that
  //------------------------------------------------------------------------

  char* ptr()
  {
    return &(operator[](0));
  }

private:
  eos::common::RWMutex mMutex;
};

class bufferllmanager : public XrdSysMutex
{
public:

  bufferllmanager(size_t _max = 128, size_t _default_size = 128 * 1024)
  {
    max = _max;
    buffersize = _default_size;
    queued_size = 0;
    inflight_size = 0;
  }

  virtual ~bufferllmanager() { }

  typedef std::shared_ptr<bufferll> shared_buffer;

  void configure(size_t _max, size_t _size)
  {
    max = _max;
    buffersize = _size;
  }

  shared_buffer get_buffer(size_t size)
  {
    XrdSysMutexHelper lLock(this);
    size_t cap_size = (size > buffersize) ? size : buffersize;

    if (!queue.size()) {
      inflight_size += cap_size;
      return std::make_shared<bufferll>(cap_size, 0);
    } else {
      shared_buffer buffer = queue.front();
      queued_size -= buffer->capacity();
      buffer->resize(cap_size);
      buffer->reserve(cap_size);
      inflight_size += buffer->capacity();
      queue.pop();
      return buffer;
    }
  }

  void put_buffer(shared_buffer buffer)
  {
    XrdSysMutexHelper lLock(this);

    if (inflight_size >= buffer->capacity()) {
      inflight_size -= buffer->capacity();
    } else {
      inflight_size = 0;
    }

    if (queue.size() == max) {
      return;
    } else {
      queue.push(buffer);
      buffer->resize(buffersize);
      buffer->reserve(buffersize);
      buffer->shrink_to_fit();
      buffer->zero();
      queued_size += buffersize;
      return;
    }
  }

  const size_t queued()
  {
    XrdSysMutexHelper lLock(this);
    return queued_size;
  }

  const size_t inflight()
  {
    XrdSysMutexHelper lLock(this);
    return inflight_size;
  }

private:
  std::queue<shared_buffer> queue;
  size_t max;
  size_t buffersize;
  size_t queued_size;
  size_t inflight_size;
};
#endif

