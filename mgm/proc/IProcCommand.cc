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

#include "common/Path.hh"
#include "mgm/proc/IProcCommand.hh"
#include "mgm/proc/ProcInterface.hh"

EOSMGMNAMESPACE_BEGIN

std::atomic_uint_least64_t IProcCommand::uuid{0};

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
    std::string msg = "command not ready, stall the client 5 seconds";
    eos_notice("%s", msg.c_str());
    error->setErrInfo(0, msg.c_str());
    return delay;
  } else {
    eos::console::ReplyProto reply = mFuture.get();

    // output is written in file
    if (!ofstdoutStreamFilename.empty() && !ofstderrStreamFilename.empty()) {
      ifstdoutStream.open(ofstdoutStreamFilename, std::ifstream::in);
      ifstderrStream.open(ofstderrStreamFilename, std::ifstream::in);
      iretcStream.str(std::string("&mgm.proc.retc=") + std::to_string(reply.retc()));
      readStdOutStream = true;
    }
    else {
      std::ostringstream oss;
      oss << "mgm.proc.stdout=" << reply.std_out()
          << "&mgm.proc.stderr=" << reply.std_err()
          << "&mgm.proc.retc=" << reply.retc();
      mTmpResp = oss.str();
    }
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// Read a part of the result stream created during open
//------------------------------------------------------------------------------
size_t
IProcCommand::read(XrdSfsFileOffset offset, char* buff, XrdSfsXferSize blen)
{
  size_t cpy_len = 0;
  if (readStdOutStream && ifstdoutStream.is_open() && ifstderrStream.is_open()) {
    ifstdoutStream.read(buff, blen);
    cpy_len = (size_t)ifstdoutStream.gcount();

    if (cpy_len < (size_t)blen) {
      readStdOutStream = false;
      readStdErrStream = true;

      ifstderrStream.read(buff + cpy_len, blen - cpy_len);
      cpy_len += (size_t)ifstderrStream.gcount();
    }
  }
  else if (readStdErrStream && ifstderrStream.is_open()) {
    ifstderrStream.read(buff, blen);
    cpy_len = (size_t)ifstderrStream.gcount();

    if (cpy_len < (size_t)blen) {
      readStdErrStream = false;
      readRetcStream = true;

      iretcStream.read(buff + cpy_len, blen - cpy_len);
      cpy_len += (size_t)iretcStream.gcount();
    }
  }
  else if (readRetcStream) {
    iretcStream.read(buff, blen);
    cpy_len = (size_t)iretcStream.gcount();

    if (cpy_len < (size_t)blen) {
      readRetcStream = false;
    }
  }
  else if ((size_t)offset < mTmpResp.length()) {
    cpy_len = std::min((size_t)(mTmpResp.size() - offset), (size_t)blen);
    memcpy(buff, mTmpResp.data() + offset, cpy_len);
  }

  return cpy_len;
}

//------------------------------------------------------------------------------
// Launch command asynchronously, creating the corresponding promise and future
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

bool
IProcCommand::OpenTemporaryOutputFiles() {
  ostringstream tmpdir;
  tmpdir << "/tmp/eos.mgm/";
  tmpdir << uuid++;

  ofstdoutStreamFilename = tmpdir.str();
  ofstdoutStreamFilename += ".stdout";
  ofstderrStreamFilename = tmpdir.str();
  ofstderrStreamFilename += ".stderr";
  eos::common::Path cPath(ofstdoutStreamFilename.c_str());

  if (!cPath.MakeParentPath(S_IRWXU)) {
    eos_err("Unable to create temporary outputfile directory %s", tmpdir.str().c_str());
    return false;
  }

  // own the directory by daemon
  if (::chown(cPath.GetParentPath(), 2, 2)) {
    eos_err("Unable to own temporary outputfile directory %s",
            cPath.GetParentPath());
  }

  ofstdoutStream.open(ofstdoutStreamFilename, std::ofstream::out);
  ofstderrStream.open(ofstderrStreamFilename, std::ofstream::out);

  if ((!ofstdoutStream) || (!ofstderrStream)) {
    if (ofstdoutStream.is_open()) {
      ofstdoutStream.close();
    }

    if (ofstderrStream.is_open()) {
      ofstderrStream.close();
    }

    return false;
  }

  ofstdoutStream << "mgm.proc.stdout=";
  ofstderrStream << "&mgm.proc.stderr=";

  return true;
}

bool
IProcCommand::CloseTemporaryOutputFiles() {
  ofstdoutStream.close();
  ofstderrStream.close();

  return !(ofstdoutStream.is_open() || ofstderrStream.is_open());
}

EOSMGMNAMESPACE_END
