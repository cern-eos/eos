// ----------------------------------------------------------------------
// File: Checksum.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

#include "common/Logging.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IView.hh"
#include "namespace/utils/Checksum.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Macros.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Check access rights
//----------------------------------------------------------------------------
int
XrdMgmOfs::Checksum(const char* path,
                    const char* ininfo,
                    XrdOucEnv& env,
                    XrdOucErrInfo& error,
                    eos::common::LogId& ThreadLogId,
                    eos::common::Mapping::VirtualIdentity& vid,
                    const XrdSecEntity* client)
{
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  gOFS->MgmStats.Add("Fuse-Checksum", vid.uid, vid.gid, 1);

  XrdOucString checksum = "";
  std::shared_ptr<eos::IFileMD> fmd;
  int retc = 0;

  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  try {
    fmd = gOFS->eosView->getFile(path);
    eos::appendChecksumOnStringAsHex(fmd.get(), checksum, 0x00, SHA_DIGEST_LENGTH);
  } catch (eos::MDException& e) {
    eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                     e.getErrno(), e.getMessage().str().c_str());
    errno = e.getErrno();
    retc = errno;
  }

  XrdOucString response = "checksum: ";
  response += checksum;
  response += " retc=";
  response += retc;
  error.setErrInfo(response.length() + 1, response.c_str());
  return SFS_DATA;
}
