// ----------------------------------------------------------------------
// File: CachEntry.cc
// Author: Elvin-Alin Sindrilaru - CERN
// ----------------------------------------------------------------------

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
#include <cstdlib>
#include <cstdio>
#include <cstring>
//------------------------------------------------------------------------------
#include "CacheEntry.hh"
//------------------------------------------------------------------------------

CacheEntry::CacheEntry(int filedes, char* buf, off_t off, size_t len, FileAbstraction* ptr):
  fd(filedes),
  capacity(len),
  length(len), 
  offset(off), 
  pParentFile(ptr)
{
  buffer = (char*) calloc(length + 1, sizeof(char));
  buffer = (char*) memcpy(buffer, buf, length);
}


//------------------------------------------------------------------------------
CacheEntry::~CacheEntry()
{
  free(buffer);
}


//------------------------------------------------------------------------------
void
CacheEntry::Recycle(int filedes, char* buf, off_t off, size_t len, FileAbstraction* ptr)
{
  fd = filedes;
  offset = off;
  pParentFile = ptr;
  
  if (len > capacity) {
    length = len;
    capacity = len;
    buffer = (char*) realloc(buffer, (length + 1) * sizeof(char));
  }
  else {
    length = len;
  }

  buffer = (char*) memcpy(buffer, buf, length);
}

