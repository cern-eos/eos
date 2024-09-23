//------------------------------------------------------------------------------
// File: DropReplica.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                                  *
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
// Drop replica form FST and also update the namespace view for the given
// file system id
//------------------------------------------------------------------------------
bool
XrdMgmOfs::DropReplica(eos::IFileMD::id_t fid,
                       eos::common::FileSystem::fsid_t fsid) const
{
  bool retc = true;

  if (fsid == 0ull) {
    return retc;
  }

  eos_info("msg=\"drop replica/stripe\" fxid=%08llx fsid=%lu",
           fid, fsid);

  // Send external deletion to the FST
  if (gOFS && !gOFS->DeleteExternal(fsid, fid, true)) {
    eos_err("msg=\"failed to send unlink to FST\" fxid=%08llx fsid=%lu",
            fid, fsid);
    retc = false;
  }

  // Drop from the namespace, we don't need the path as root can drop by fid
  XrdOucErrInfo err;
  eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();

  if (gOFS && gOFS->_dropstripe("", fid, err, vid, fsid, true)) {
    eos_err("msg=\"failed to drop replicas from ns\" fxid=%08llx fsid=%lu",
            fid, fsid);
  }

  return retc;
}
