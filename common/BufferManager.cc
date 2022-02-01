//------------------------------------------------------------------------------
//! @file BufferManager.cc
//! @author Elvin-Alin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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

#include "common/BufferManager.hh"

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Get OS page size aligned buffer
//------------------------------------------------------------------------------
std::unique_ptr<char, void(*)(void*)>
GetAlignedBuffer(const size_t size)
{
  static long os_pg_size = sysconf(_SC_PAGESIZE);
  char* raw_buffer = nullptr;
  std::unique_ptr<char, void(*)(void*)> buffer
  ((char*) raw_buffer, [](void* ptr) {
    if (ptr) {
      free(ptr);
    }
  });

  if (posix_memalign((void**) &raw_buffer, os_pg_size, size)) {
    return buffer;
  }

  buffer.reset(raw_buffer);
  return buffer;
}

EOSCOMMONNAMESPACE_END
