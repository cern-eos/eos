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

#ifndef FUSE_RM_INFO_HH_
#define FUSE_RM_INFO_HH_

#include <vector>
#include <string>

//------------------------------------------------------------------------------
// This thing is really hacky.. Tries to determine if the process contacting
// us is an rm, and extract a few details about what it's trying to do, based
// on its command line arguments.
//
// We should try not to have false positives! A process which is not rm should
// never be misidentified as rm..
//------------------------------------------------------------------------------

class RmInfo {
public:
  RmInfo() {}
  RmInfo(const std::string &executablePath, const std::vector<std::string> &cmdline);

  bool isRm() const {
    return rm;
  }

  bool isRecursive() const {
    return recursive;
  }

private:
  bool rm = false;
  bool recursive = false;
};

#endif
