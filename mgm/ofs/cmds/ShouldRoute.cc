//------------------------------------------------------------------------------
// File: ShouldRoute.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

//------------------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Test if a client based on the called function and his identity
// should be re-routed
//------------------------------------------------------------------------------
bool
XrdMgmOfs::ShouldRoute(const char* function, int accessmode,
                       eos::common::VirtualIdentity& vid,
                       const char* path, const char* info,
                       std::string& host, int& port, int& stall_timeout)
{
  if ((vid.uid == 0) ||
      (vid.host == "localhost") ||
      (vid.host == "localhost.localdomain")) {
    return false;
  }

  // Might happen during shutdown
  if (mRouting == nullptr) {
    return false;
  }

  std::string stat_info;
  eos::mgm::PathRouting::Status st =
    mRouting->Reroute(path, info, vid, host, port, stat_info);

  if (st == PathRouting::Status::REROUTE) {
    gOFS->MgmStats.Add(stat_info.c_str(), vid.uid, vid.gid, 1);
    return true;
  } else if (st == PathRouting::Status::STALL) {
    stall_timeout = 5; // seconds
    return true;
  } else {
    return false;
  }
}
