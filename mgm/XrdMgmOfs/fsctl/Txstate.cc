// ----------------------------------------------------------------------
// File: Txstate.cc
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

{
  REQUIRE_SSS_OR_LOCAL_AUTH;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  int envlen;
  EXEC_TIMING_BEGIN("TxStateLog");
  eos_thread_debug("Transfer state + log received for %s", env.Env(envlen));

  char* txid = env.Get("tx.id");
  char* sstate = env.Get("tx.state");
  char* logb64 = env.Get("tx.log.b64");
  char* sprogress = env.Get("tx.progress");

  if (txid)
  {
    long long id = strtoll(txid, 0, 10);
    if (sprogress)
    {
      if (sprogress)
      {
        float progress = atof(sprogress);
        if (!gTransferEngine.SetProgress(id, progress))
        {
          eos_thread_err("unable to set progress for transfer id=%lld progress=%.02f", id, progress);
          return Emsg(epname, error, ENOENT, "set transfer state - transfer has been canceled [EIDRM]", "");
        }
        else
        {
          eos_thread_info("id=%lld progress=%.02f", id, progress);
        }
      }
    }

    if (sstate)
    {
      char* logout = 0;
      unsigned loglen = 0;
      if (logb64)
      {
        XrdOucString slogb64 = logb64;

        if (eos::common::SymKey::Base64Decode(slogb64, logout, loglen))
        {
          logout[loglen] = 0;
          if (!gTransferEngine.SetLog(id, logout))
          {
            eos_thread_err("unable to set log for transfer id=%lld", id);
          }
        }
      }

      int state = atoi(sstate);
      if (!gTransferEngine.SetState(id, state))
      {
        eos_thread_err("unable to set state for transfer id=%lld state=%s",
                       id, TransferEngine::GetTransferState(state));
      }
      else
      {
        eos_thread_info("id=%lld state=%s", id, TransferEngine::GetTransferState(state));
      }
    }
  }

  gOFS->MgmStats.Add("TxState", vid.uid, vid.gid, 1);

  const char* ok = "OK";
  error.setErrInfo(strlen(ok) + 1, ok);
  EXEC_TIMING_END("TxState");
  return SFS_DATA;
}
