// ----------------------------------------------------------------------
// File: DirectoryIterator.hh
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

#ifndef FUSEX_DIRECTORY_ITERATOR_HH
#define FUSEX_DIRECTORY_ITERATOR_HH

#include <dirent.h>
#include <string>

class DirectoryIterator
{
public:

  //----------------------------------------------------------------------------
  // Construct iterator object on the given path - must be a directory.
  //----------------------------------------------------------------------------
  DirectoryIterator(const std::string& path);

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  ~DirectoryIterator();

  //----------------------------------------------------------------------------
  // Checks if the iterator is in an error state. EOF is not an error state!
  //----------------------------------------------------------------------------
  bool ok() const;

  //----------------------------------------------------------------------------
  // Retrieve the error message if the iterator object is in an error state.
  // If no error state, returns an empty string.
  //----------------------------------------------------------------------------
  std::string err() const;

  //----------------------------------------------------------------------------
  // Checks whether we have reached the end.
  //----------------------------------------------------------------------------
  bool eof() const;

  //----------------------------------------------------------------------------
  // Retrieve next directory entry.
  // This object retains ownership on the given pointer, never call free on it.
  //
  // If the iterator is in an error state, next() will only ever return nullptr.
  //----------------------------------------------------------------------------
  struct dirent* next();

private:
  std::string error;
  std::string path;
  bool reachedEnd;

  DIR* dir;
  struct dirent* nextEntry;
};

#endif
