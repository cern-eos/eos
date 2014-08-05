// ----------------------------------------------------------------------
// File: DeleteExternal.cc
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
bool
XrdMgmOfs::DeleteExternal (eos::common::FileSystem::fsid_t fsid,
                           unsigned long long fid)
/*----------------------------------------------------------------------------*/
/*
 * @brief send an explicit deletion message to a fsid/fid pair
 *
 * @param fsid file system id where to run a deletion
 * @param fid file id to be deleted
 *
 * @result true if successfully sent otherwise false
 *
 * This routine signs a deletion message for the given file id and sends it
 * to the referenced file system.
 */
/*----------------------------------------------------------------------------*/
{
  // ---------------------------------------------------------------------------
  // send an explicit deletion message to any fsid/fid pair
  // ---------------------------------------------------------------------------


  XrdMqMessage message("deletion");

  eos::mgm::FileSystem* fs = 0;
  XrdOucString receiver = "";
  XrdOucString msgbody = "mgm.cmd=drop";
  XrdOucString capability = "";
  XrdOucString idlist = "";

  // get the filesystem from the FS view
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    if (FsView::gFsView.mIdView.count(fsid))
    {
      fs = FsView::gFsView.mIdView[fsid];
      if (fs)
      {
        capability += "&mgm.access=delete";
        capability += "&mgm.manager=";
        capability += gOFS->ManagerId.c_str();
        capability += "&mgm.fsid=";
        capability += (int) fs->GetId();
        capability += "&mgm.localprefix=";
        capability += fs->GetPath().c_str();
        capability += "&mgm.fids=";
        XrdOucString hexfid = "";
        eos::common::FileId::Fid2Hex(fid, hexfid);
        capability += hexfid;
        receiver = fs->GetQueue().c_str();
      }
    }
  }

  bool ok = false;

  if (fs)
  {
    XrdOucEnv incapability(capability.c_str());
    XrdOucEnv* capabilityenv = 0;
    eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

    int caprc = 0;
    if ((caprc = gCapabilityEngine.Create(&incapability, capabilityenv, symkey)))
    {
      eos_static_err("unable to create capability - errno=%u", caprc);
    }
    else
    {
      int caplen = 0;
      msgbody += capabilityenv->Env(caplen);
      message.SetBody(msgbody.c_str());
      if (!Messaging::gMessageClient.SendMessage(message, receiver.c_str()))
      {
        eos_static_err("unable to send deletion message to %s", receiver.c_str());
      }
      else
      {

        ok = true;
      }
    }
  }
  return ok;
}


