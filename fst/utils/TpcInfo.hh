// ----------------------------------------------------------------------
// File: TpcInfo.hh
// Author: Mihai Patrascoiu - CERN
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

#pragma once

#include "fst/Namespace.hh"
#include <string>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! TPC data structure to hold useful TPC information.
//!
//! Note: The structure has been extracted to a header file
//!       as it is being used by the XrdFstOfs and XrdFstOfsFile objects
//------------------------------------------------------------------------------
struct TpcInfo {
  std::string path; ///< file path to read/write
  std::string opaque; ///< opaque info
  std::string capability; ///< EOS capability
  std::string key; ///< Transfer key
  std::string src; ///< Source hostname
  std::string dst; ///< Destination hostname
  std::string org; ///< Origin client
  std::string lfn; ///< File name at source
  time_t expires; ///< Expiry timestamp
};

EOSFSTNAMESPACE_END
