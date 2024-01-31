// ----------------------------------------------------------------------
// File: RunningPidScanner.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#ifndef FUSEX_MISC_RUNNING_PID_SCANNER_HH
#define FUSEX_MISC_RUNNING_PID_SCANNER_HH

#include "../auth/DirectoryIterator.hh"

//------------------------------------------------------------------------------
//! Class to scan through all pids in the system, as found in /proc/<pid>.
//! Only provides readlink(cwd) for now.
//------------------------------------------------------------------------------
class RunningPidScanner
{
public:
  //----------------------------------------------------------------------------
  //! Entry
  //----------------------------------------------------------------------------
  struct Entry {
    std::string cwd;
  };

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  RunningPidScanner();

  //----------------------------------------------------------------------------
  //! Fetch next element
  //----------------------------------------------------------------------------
  bool next(Entry& out);

  //----------------------------------------------------------------------------
  //! Has there been an error? Reaching EOF is not an error.
  //----------------------------------------------------------------------------
  bool ok() const;

  //----------------------------------------------------------------------------
  //! Return error string. If no error has occurred, return the empty string.
  //----------------------------------------------------------------------------
  std::string err() const;


private:
  DirectoryIterator iter;

};

#endif
