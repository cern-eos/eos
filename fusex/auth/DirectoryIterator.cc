// ----------------------------------------------------------------------
// File: DirectoryIterator.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "DirectoryIterator.hh"
#include "Utils.hh"

#include "common/Logging.hh"

#include <string.h>

//------------------------------------------------------------------------------
// Construct iterator object on the given path - must be a directory.
//------------------------------------------------------------------------------
DirectoryIterator::DirectoryIterator(const std::string& mypath)
  : path(mypath), reachedEnd(false), dir(nullptr), nextEntry(nullptr)
{
  dir = opendir(path.c_str());

  if (!dir) {
    error = SSTR("Unable to opendir: " << path);
    return;
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
DirectoryIterator::~DirectoryIterator()
{
  if (dir) {
    if (closedir(dir) != 0) {
      eos_static_crit("Unable to close DIR* for %s", path.c_str());
    }

    dir = nullptr;
  }
}

//------------------------------------------------------------------------------
// Retrieve next directory entry.
// This object retains ownership on the given pointer, never call free on it.
//
// If the iterator is in an error state, next() will only ever return nullptr.
//------------------------------------------------------------------------------
struct dirent* DirectoryIterator::next()
{
  if (!ok()) {
    return nullptr;
  }

  if (reachedEnd) {
    return nullptr;
  }

  errno = 0;
  nextEntry = readdir(dir);

  if (!nextEntry && errno == 0) {
    reachedEnd = true;
  } else if (!nextEntry) {
    error = SSTR("Error when calling readdir: " << strerror(errno));
  }

  return nextEntry;
}

//------------------------------------------------------------------------------
// Checks if the iterator is in an error state. EOF is not an error state!
//------------------------------------------------------------------------------
bool DirectoryIterator::ok() const
{
  return error.empty();
}

//------------------------------------------------------------------------------
// Checks whether we have reached the end.
//------------------------------------------------------------------------------
bool DirectoryIterator::eof() const
{
  return reachedEnd;
}

//------------------------------------------------------------------------------
// Retrieve the error message if the iterator object is in an error state.
// If no error state, returns an empty string.
//------------------------------------------------------------------------------
std::string DirectoryIterator::err() const
{
  return error;
}
