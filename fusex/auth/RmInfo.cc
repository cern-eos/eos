//------------------------------------------------------------------------------
//! @file RmInfo.hh
//! @author Georgios Bitzes CERN
//! @brief Utility class to prevent "rm -rf" or equivalent to top-level
//!        directories of the FUSE mount. A bit hacky, we do string
//!        comparisons with cmdline.
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

#include "auth/ProcessInfo.hh"
#include "RmInfo.hh"
#include "common/Logging.hh"

RmInfo::RmInfo(const std::string &executablePath, const std::vector<std::string> &cmdline) {
  eos_static_crit("path: %s", executablePath.c_str());

  if(executablePath != "/bin/rm" &&
     executablePath != "/usr/bin/rm" &&
     executablePath != "/usr/local/bin/rm" ) {
    return;
  }

  rm = true;

  for(auto it = cmdline.begin(); it != cmdline.end(); it++) {
    const std::string& arg = *it;

    if(arg == "--recursive") {
      recursive = true;
    }

    if(arg.size() >= 2 && arg[0] == '-' && arg[1] != '-') {
      for(size_t i = 1; i < arg.size(); i++) {
        if(arg[i] == 'r' || arg[i] == 'R') {
          recursive = true;
          break;
        }
      }
    }
  }
}
