//------------------------------------------------------------------------------
// File: RmCmd.cc
// Author: Jozsef Makai - CERN
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


#include "RmCmd.hh"

EOSMGMNAMESPACE_BEGIN

eos::console::ReplyProto
eos::mgm::RmCmd::ProcessRequest() {
  eos::console::ReplyProto reply;

  eos::console::RmProto rm = mReqProto.rm();
  auto recursive = rm.recursive();
  auto deep = rm.deep();
  auto force = rm.bypassrecycle();

  std::string path;
  if (rm.path().empty()) {
    XrdOucString pathOut;
    GetPathFromFid(pathOut, rm.fileid(), "Cannot get fid");
    path = pathOut.c_str();
  } else {
    path = rm.path();
  }

  return reply;
}

EOSMGMNAMESPACE_END