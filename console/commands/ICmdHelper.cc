//------------------------------------------------------------------------------
//! @file ICmdHelper.cc
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
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "console/commands/ICmdHelper.hh"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "common/Logging.hh"
#include "common/SymKeys.hh"

//------------------------------------------------------------------------------
// Execute command and display any output information
//------------------------------------------------------------------------------
int
ICmdHelper::Execute()
{
  if (!mReq.command_case()) {
    std::cerr << "error: generic request object not populated with command"
              << std::endl;
    return EINVAL;
  }

  size_t sz = mReq.ByteSize();
  std::string buffer(sz , '\0');
  google::protobuf::io::ArrayOutputStream aos((void*)buffer.data(), sz);

  if (!mReq.SerializeToZeroCopyStream(&aos)) {
    std::cerr << "error: failed to serialize ProtocolBuffer request"
              << std::endl;
    return EINVAL;
  }

  std::string b64buff;

  if (!eos::common::SymKey::Base64Encode(buffer.data(), buffer.size(), b64buff)) {
    std::cerr << "error: failed to base64 encode the request" << std::endl;
    return EINVAL;
  }

  std::string cmd = "mgm.cmd.proto=";
  cmd += b64buff;
  int retc = mMgmExec.ExecuteCommand(cmd.c_str(), mIsAdmin);

  if (retc) {
    if (mMgmExec.GetError().length()) {
      std::cerr << mMgmExec.GetError() << std::endl;
    }
  } else {
    if (mMgmExec.GetResult().size()) {
      if (mHighlight) {
        TextHighlight(mMgmExec.GetResult());
      }

      // Add new line if necessary
      std::string out = mMgmExec.GetResult();

      if (*out.rbegin() != '\n') {
        out += '\n';
      }

      std::cout << out;
    }
  }

  return retc;
}

//------------------------------------------------------------------------------
// Apply highlighting to text
//------------------------------------------------------------------------------
void
ICmdHelper::TextHighlight(std::string& text)
{
  if (global_highlighting) {
    XrdOucString tmp = text.c_str();
    // Color replacements
    tmp.replace("[booted]", "\033[1m[booted]\033[0m");
    tmp.replace("[down]", "\033[49;31m[down]\033[0m");
    tmp.replace("[failed]", "\033[49;31m[failed]\033[0m");
    tmp.replace("[booting]", "\033[49;32m[booting]\033[0m");
    tmp.replace("[compacting]", "\033[49;34m[compacting]\033[0m");
    // Replication highlighting
    tmp.replace("master-rw", "\033[49;31mmaster-rw\033[0m");
    tmp.replace("master-ro", "\033[49;34mmaster-ro\033[0m");
    tmp.replace("slave-ro", "\033[1mslave-ro\033[0m");
    tmp.replace("=ok", "=\033[49;32mok\033[0m");
    tmp.replace("=compacting", "=\033[49;32mcompacting\033[0m");
    tmp.replace("=off", "=\033[49;34moff\033[0m");
    tmp.replace("=blocked", "=\033[49;34mblocked\033[0m");
    tmp.replace("=wait", "=\033[49;34mwait\033[0m");
    tmp.replace("=starting", "=\033[49;34mstarting\033[0m");
    tmp.replace("=true", "=\033[49;32mtrue\033[0m");
    tmp.replace("=false", "=\033[49;31mfalse\033[0m");
    text = tmp.c_str();
  }
}
