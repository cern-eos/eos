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
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Data Buffer
//------------------------------------------------------------------------------

#pragma once
#include <cstring>
#include <vector>
#include <stdint.h>
#include <zlib.h>
#include "namespace/MDException.hh"

namespace eos
{
//----------------------------------------------------------------------------
//! Data Buffer
//----------------------------------------------------------------------------
class Buffer: public std::vector<char>
{
public:
  //------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------
  Buffer(unsigned size = 512):
    data(0), len(0)
  {
    reserve(size);
  }

  //------------------------------------------------------------------------
  //! Destructor
  //------------------------------------------------------------------------
  virtual ~Buffer() {}

  //------------------------------------------------------------------------
  //! Copy constructor
  //------------------------------------------------------------------------
  Buffer(const Buffer& other)
  {
    data = 0;
    len = 0;
    *this = other;
  };

  //------------------------------------------------------------------------
  //! Assignment operator
  //------------------------------------------------------------------------
  Buffer& operator = (const Buffer& other)
  {
    if (this != &other) {
      data = 0;
      len = 0;
      resize(other.getSize());
      memcpy(getDataPtr(), other.getDataPtr(), other.getSize());
    }

    return *this;
  };

  //------------------------------------------------------------------------
  //! Get data pointer
  //------------------------------------------------------------------------
  char* getDataPtr()
  {
    if (!data) {
      return &operator[](0);
    } else {
      return data;
    }
  }

  //------------------------------------------------------------------------
  //! Get data pointer
  //------------------------------------------------------------------------
  const char* getDataPtr() const
  {
    if (!data) {
      return &operator[](0);
    } else {
      return data;
    }
  }

  //------------------------------------------------------------------------
  //! Set data pointer
  //------------------------------------------------------------------------
  void setDataPtr(char* ptr, size_t size)
  {
    data = ptr;
    len = size;
  }

  //------------------------------------------------------------------------
  //! Get data padded (if we read over the size we get 0 as response)
  //------------------------------------------------------------------------
  const char getDataPadded(size_t i) const
  {
    if (!data) {
      if (i < size()) {
        return (operator[](i));
      }
    } else {
      if (i < len) {
        return *(data + i);
      }
    }

    return 0;
  }

  //------------------------------------------------------------------------
  //! Get size
  //------------------------------------------------------------------------
  size_t getSize() const
  {
    if (!data) {
      return size();
    } else {
      return len;
    }
  }

  //------------------------------------------------------------------------
  //! Set size
  //------------------------------------------------------------------------
  void setSize(size_t size)
  {
    resize(size);
  }

  //------------------------------------------------------------------------
  //! Add data
  //------------------------------------------------------------------------
  void putData(const void* ptr, size_t dataSize)
  {
    if (!data) {
      size_t currSize = size();
      resize(currSize + dataSize);
      memcpy(&operator[](currSize), ptr, dataSize);
    } else {
      MDException e(EINVAL);
      e.getMessage() << "Read only structure";
      throw e;
    }
  }

  //------------------------------------------------------------------------
  //! Copy data to pointed location
  //------------------------------------------------------------------------
  uint16_t grabData(uint16_t offset, void* ptr, size_t dataSize) const
  {
    if (offset + dataSize > getSize()) {
      MDException e(EINVAL);
      e.getMessage() << "Not enough data to fulfil the request";
      throw e;
    }

    if (!data) {
      memcpy(ptr, &operator[](offset), dataSize);
    } else  {
      memcpy(ptr, data + offset, dataSize);
    }

    return offset + dataSize;
  }

  //------------------------------------------------------------------------
  //! Calculate the CRC32 checksum
  //------------------------------------------------------------------------
  uint32_t getCRC32() const
  {
    return crc32(crc32(0L, Z_NULL, 0), (const Bytef*)getDataPtr(), size());
  }

protected:
  char* data;
  size_t len;
};
}
