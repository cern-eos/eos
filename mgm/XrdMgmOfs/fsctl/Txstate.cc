// ----------------------------------------------------------------------
// File: Txstate.cc
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
#include "common/SymKeys.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Macros.hh"
#include "mgm/txengine/TransferEngine.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Set transfer state and log
//----------------------------------------------------------------------------
int
XrdMgmOfs::Txstate(const char* path,
                   const char* ininfo,
                   XrdOucEnv& env,
                   XrdOucErrInfo& error,
                   eos::common::VirtualIdentity& vid,
                   const XrdSecEntity* client)
{
  static const char* epname = "TxState";
  REQUIRE_SSS_OR_LOCAL_AUTH;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  EXEC_TIMING_BEGIN("TxState");
  int envlen;
  eos_thread_debug("Transfer state + log received for %s", env.Env(envlen));
  char* txid = env.Get("tx.id");

  if (txid) {
    char* sstate = env.Get("tx.state");
    char* logb64 = env.Get("tx.log.b64");
    char* sprogress = env.Get("tx.progress");
    long long id = strtoll(txid, 0, 10);

    if (sprogress) {
      float progress = atof(sprogress);

      if (!gTransferEngine.SetProgress(id, progress)) {
        eos_thread_err("unable to set progress for transfer "
                       "id=%lld progress=%.02f", id, progress);
        return Emsg(epname, error, ENOENT,
                    "set transfer state - transfer has been canceled [EIDRM]", "");
      }

      eos_thread_info("id=%lld progress=%.02f", id, progress);
    }

    if (sstate) {
      char* logout = 0;
      ssize_t loglen = 0;

      if (logb64) {
        XrdOucString slogb64 = logb64;

        if (eos::common::SymKey::Base64Decode(slogb64, logout, loglen)) {
          logout[loglen] = 0;

          if (!gTransferEngine.SetLog(id, logout)) {
            eos_thread_err("unable to set log for transfer id=%lld", id);
          }
        }
      }

      int state = atoi(sstate);

      if (!gTransferEngine.SetState(id, state)) {
        eos_thread_err("unable to set state for transfer id=%lld state=%s",
                       id, TransferEngine::GetTransferState(state));
      } else {
        eos_thread_info("id=%lld state=%s",
                        id, TransferEngine::GetTransferState(state));
      }
    }
  } else {
    eos_thread_err("Txstate message does not contain transfer id: %s",
                   env.Env(envlen));
    return Emsg(epname, error, EINVAL, "set transfer state [EINVAL]",
                "missing transfer id");
  }

  gOFS->MgmStats.Add("TxState", vid.uid, vid.gid, 1);
  const char* ok = "OK";
  error.setErrInfo(strlen(ok) + 1, ok);
  EXEC_TIMING_END("TxState");
  return SFS_DATA;
}
