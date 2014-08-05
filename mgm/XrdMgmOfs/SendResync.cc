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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::SendResync (eos::common::FileId::fileid_t fid,
                       eos::common::FileSystem::fsid_t fsid)
/*----------------------------------------------------------------------------*/
/*
 * @brief send a resync command for a file identified by id and filesystem
 *
 * @param fid file id to be resynced
 * @param fsid filesystem id where the file should be resynced
 *
 * @return true if successfully send otherwise false
 *
 * A resync synchronizes the cache DB on the FST with the meta data on disk
 * and on the MGM and flags files accordingly with size/checksum errors.
 */
/*----------------------------------------------------------------------------*/
{
  EXEC_TIMING_BEGIN("SendResync");

  gOFS->MgmStats.Add("SendResync", vid.uid, vid.gid, 1);

  XrdMqMessage message("resync");
  XrdOucString msgbody = "mgm.cmd=resync";

  char payload[4096];
  snprintf(payload, sizeof (payload) - 1, "&mgm.fsid=%lu&mgm.fid=%llu",
           (unsigned long) fsid, (unsigned long long) fid);
  msgbody += payload;

  message.SetBody(msgbody.c_str());

  // figure out the receiver
  XrdOucString receiver;

  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    eos::mgm::FileSystem* verifyfilesystem = 0;
    if (FsView::gFsView.mIdView.count(fsid))
    {
      verifyfilesystem = FsView::gFsView.mIdView[fsid];
    }
    if (!verifyfilesystem)
    {
      eos_err("fsid=%lu is not in the configuration - cannot send resync message",
              fsid);
      return false;
    }
    receiver = verifyfilesystem->GetQueue().c_str();
  }


  if (!Messaging::gMessageClient.SendMessage(message, receiver.c_str()))
  {

    eos_err("unable to send resync message to %s", receiver.c_str());
    return false;
  }

  EXEC_TIMING_END("SendResync");

  return true;
}

