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
#include "namespace/Resolver.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IView.hh"
#include "namespace/utils/Checksum.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Macros.hh"

#include <XrdOuc/XrdOucEnv.hh>
#include <openssl/sha.h>

//----------------------------------------------------------------------------
// Check access rights
//----------------------------------------------------------------------------
int
XrdMgmOfs::Checksum(const char* path,
                    const char* ininfo,
                    XrdOucEnv& env,
                    XrdOucErrInfo& error,
                    eos::common::VirtualIdentity& vid,
                    const XrdSecEntity* client)
{
  ACCESSMODE_R_MASTER;
  MAYSTALL;
  MAYREDIRECT;
  gOFS->MgmStats.Add("Fuse-Checksum", vid.uid, vid.gid, 1);
  XrdOucString checksum = "";
  std::shared_ptr<eos::IFileMD> fmd;
  int retc = 0;
  bool fuse_readable = env.Get("mgm.option") ? (std::string(
                         env.Get("mgm.option")) == "fuse") ? true : false : false;
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                    __FILE__);
  XrdOucString spath = path;
  unsigned long byfid = eos::Resolver::retrieveFileIdentifier(
                          spath).getUnderlyingUInt64();

  try {
    if (byfid) {
      fmd = gOFS->eosFileService->getFileMD(byfid);
    } else {
      fmd = gOFS->eosView->getFile(path);
    }

    size_t xs_length = SHA_DIGEST_LENGTH;

    if (fuse_readable) {
      xs_length = eos::common::LayoutId:: GetChecksumLen(fmd->getLayoutId());
    }

    eos::appendChecksumOnStringAsHex(fmd.get(), checksum, 0x00, xs_length);
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
