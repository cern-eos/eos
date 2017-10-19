//------------------------------------------------------------------------------
//! @file IProcCommand.cc
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

#include "mgm/proc/IProcCommand.hh"
#include "mgm/proc/ProcInterface.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Open a proc command e.g. call the appropriate user or admin commmand and
// store the output in a resultstream or in case of find in a temporary output
// file.
//------------------------------------------------------------------------------
int
IProcCommand::open(const char* path, const char* info,
                   eos::common::Mapping::VirtualIdentity& vid,
                   XrdOucErrInfo* error)
{
  int delay = 5;

  if (!mExecRequest) {
    LaunchJob();
    mExecRequest = true;
  }

  std::future_status status = mFuture.wait_for(std::chrono::seconds(delay));

  if (status != std::future_status::ready) {
    // Stall the client
    std::string msg = "acl command not ready, stall the client 5 seconds";
    eos_notice("%s", msg.c_str());
    error->setErrInfo(0, msg.c_str());
    return delay;
  } else {
    eos::console::ReplyProto reply = mFuture.get();
    std::ostringstream oss;
    oss << "mgm.proc.stdout=" << reply.std_out()
        << "&mgm.proc.stderr=" << reply.std_err()
        << "&mgm.proc.retc=" << reply.retc();
    mTmpResp = oss.str();
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// Read a part of the result stream created during open
//------------------------------------------------------------------------------
int
IProcCommand::read(XrdSfsFileOffset offset, char* buff, XrdSfsXferSize blen)
{
  if ((size_t)offset < mTmpResp.length()) {
    size_t cpy_len = std::min((size_t)(mTmpResp.size() - offset), (size_t)blen);
    memcpy(buff, mTmpResp.data() + offset, cpy_len);
    return cpy_len;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Lauch command asynchronously, creating the corresponding promise and future
//------------------------------------------------------------------------------
void
IProcCommand::LaunchJob()
{
  if (mDoAsync) {
    mFuture = ProcInterface::sProcThreads.PushTask<eos::console::ReplyProto>
    ([&]() -> eos::console::ReplyProto {
      std::lock_guard<std::mutex> lock(mMutexAsync);
      return ProcessRequest();
    });
  } else {
    std::promise<eos::console::ReplyProto> promise;
    mFuture = promise.get_future();
    promise.set_value(ProcessRequest());
  }
}

//------------------------------------------------------------------------------
// Check if we can safely delete the current object as there is no async
// thread executing the ProcessResponse method
//------------------------------------------------------------------------------
bool
IProcCommand::KillJob()
{
  if (!mDoAsync) {
    return true;
  }

  mForceKill.store(true);

  if (mMutexAsync.try_lock()) {
    mMutexAsync.unlock();
    return true;
  } else {
    return false;
  }
}

EOSMGMNAMESPACE_END
