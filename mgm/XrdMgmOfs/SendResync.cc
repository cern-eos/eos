// ----------------------------------------------------------------------
// File: SendResync.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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
// Send a resync command for a file identified by id and filesystem.
//------------------------------------------------------------------------------
int
XrdMgmOfs::SendResync(eos::common::FileId::fileid_t fid,
                      eos::common::FileSystem::fsid_t fsid, bool force)
{
  EXEC_TIMING_BEGIN("SendResync");
  gOFS->MgmStats.Add("SendResync", vid.uid, vid.gid, 1);
  XrdOucString msgbody = "mgm.cmd=resync";
  char payload[4096];
  // @todo(esindril) Transition, eventually send mgm.fid=HEX
  snprintf(payload, sizeof(payload) - 1,
           "&mgm.fsid=%lu&mgm.fid=%llu&mgm.fxid=%08llx&mgm.resync_force=%i",
           (unsigned long) fsid, fid, fid, (int)force);
  msgbody += payload;
  // Figure out the receiver
  std::string receiver;
  {
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    eos::mgm::FileSystem* fs = FsView::gFsView.mIdView.lookupByID(fsid);

    if (!fs) {
      eos_err("msg=\"no resync msg sent, no such file system\" fsid=%lu", fsid);
      return -1;
    }

    receiver = fs->GetQueue();
  }
  eos::mq::MessagingRealm::Response response =
    mMessagingRealm->sendMessage("resync", msgbody.c_str(), receiver);

  if (!response.ok()) {
    eos_err("msg=\"failed to send resync message\" dst=%s", receiver.c_str());
    return -1;
  }

  EXEC_TIMING_END("SendResync");
  return 0;
}
