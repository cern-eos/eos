//------------------------------------------------------------------------------
//! @file DrainCmd.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "XrdOuc/XrdOucEnv.hh"
#include "DrainCmd.hh"
#include "mgm/drain/Drainer.hh"
#include "mgm/XrdMgmOfs.hh"

extern XrdMgmOfs* gOFS;

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Process request
//------------------------------------------------------------------------------
eos::console::ReplyProto
DrainCmd::ProcessRequest()
{
  using eos::console::DrainProto;
  eos::console::ReplyProto reply;
  eos::console::DrainProto drain = mReqProto.drain();
  XrdOucEnv env;
  if (mVid.uid != 0) {
    reply.set_std_err("error: you have to take role 'root' to execute this command");
    reply.set_retc(EPERM);
  } else if (drain.op() == DrainProto::START) {
    eos_notice("ID to drain %s", drain.fsid().c_str());
    if (!gOFS->DrainerEngine->StartFSDrain(atoi(drain.fsid().c_str()), atoi(drain.targetfsid().c_str()), stdErr)) {
      reply.set_std_err(stdErr.c_str());
      reply.set_retc(EINVAL);
    } else {
      reply.set_std_out("success: drain successfully started!");
      reply.set_retc(0);
    }
  } else if (drain.op() == DrainProto::STOP) {
    if (!gOFS->DrainerEngine->StopFSDrain(atoi(drain.fsid().c_str()), stdErr)) {
      reply.set_std_err(stdErr.c_str());
      reply.set_retc(EINVAL);
    } else {
      reply.set_std_out("success: drain successfully stopped!");
      reply.set_retc(0);
    }
  } else if (drain.op() == DrainProto::CLEAR) {
    if (!gOFS->DrainerEngine->ClearFSDrain(atoi(drain.fsid().c_str()), stdErr)) {
      reply.set_std_err(stdErr.c_str());
      reply.set_retc(EINVAL);
    } else {
      reply.set_std_out("success: drain successfully cleared!");
      reply.set_retc(0);
    }
  } else if (drain.op() == DrainProto::STATUS) {
    XrdOucString status;
    if (!gOFS->DrainerEngine->GetDrainStatus(atoi(drain.fsid().c_str()), status, stdErr)) {
      reply.set_std_err(stdErr.c_str());
      reply.set_retc(EINVAL);
    } else {
      reply.set_std_out(status.c_str());
      reply.set_retc(0);
    }
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not supported");
  }

  return reply;
}

EOSMGMNAMESPACE_END
