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
bool
XrdMgmOfs::DeleteExternal(eos::common::FileSystem::fsid_t fsid,
                          unsigned long long fid, bool is_fsck)
{
  using namespace eos::common;
  std::string fst_queue;
  std::string fst_host;
  int fst_port = 1095;
  XrdOucString capability = "";
  {
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    auto* fs = FsView::gFsView.mIdView.lookupByID(fsid);

    if (fs) {
      capability += "&mgm.access=delete";
      capability += "&mgm.manager=";
      capability += gOFS->ManagerId.c_str();
      capability += "&mgm.fsid=";
      capability += (int) fs->GetId();
      capability += "&mgm.fids=";
      capability += eos::common::FileId::Fid2Hex(fid).c_str();
      fst_queue = fs->GetQueue().c_str();
      fst_host = fs->GetHost();
      fst_port = fs->getCoreParams().getLocator().getPort();
    } else {
      eos_static_err("msg=\"no such file system object\" fsid=%lu", fsid);
      return false;
    }
  }
  // Encrypt the capability information
  XrdOucEnv incapenv(capability.c_str());
  XrdOucEnv* outcapenv = 0;
  SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
  int caprc = 0;

  if ((caprc = SymKey::CreateCapability(&incapenv, outcapenv, symkey,
                                        mCapabilityValidity))) {
    eos_static_err("msg=\"unable to create capability\" errno=%u", caprc);
    return false;
  }

  int caplen = 0;
  bool ok = false;
  std::string qreq = "/?fst.pcmd=drop";

  if (is_fsck) {
    qreq += "&fst.drop.type=fsck";
  }

  qreq += outcapenv->Env(caplen);
  std::string qresp;

  if (SendQuery(fst_host, fst_port, qreq, qresp)) {
    eos_static_err("msg=\"unable to send deletion message\" target=%s",
                   fst_queue.c_str());
  } else {
    ok = true;
  }

  delete outcapenv;
  return ok;
}
