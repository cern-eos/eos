// ----------------------------------------------------------------------
// File: Glob.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/ASwitzerland                                  *
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

//-----------------------------------------------------------------------------
//! @brief  Class applying bash pattern amtching
//-----------------------------------------------------------------------------
#pragma once
#include "common/Namespace.hh"
#include "common/Path.hh"
#include <fnmatch.h>
#include <string>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class running bash pattern matching
//! Example (call this for each entry in a listing)
//!   using eos::common::Glob;
//!   Glob glob;
//!   glob.Match("asdf*.txt", "asdf1.txt"; (true in this case, otherwise false)
//------------------------------------------------------------------------------
class Glob
{
public:
  //----------------------------------------------------------------------------
  //! Match the basename of path against a shell wildcard pattern. Wildcards do
  //! not match a leading '.', as with glob(3). Reentrant and lock-free.
  //----------------------------------------------------------------------------
  bool
  Match(const std::string& pattern, const std::string& path) const
  {
    eos::common::Path cPath(path.c_str());
    return ::fnmatch(pattern.c_str(), cPath.GetName(), FNM_PERIOD) == 0;
  }
};

EOSCOMMONNAMESPACE_END
