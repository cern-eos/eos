//------------------------------------------------------------------------------
//! @file ICmdHelper.cc
//! @author Elvin Sindrilaru - CERN
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
#include "common/Logging.hh"
#include "common/SymKeys.hh"
#include "XrdCl/XrdClFile.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include <sstream>
#include <zmq.hpp>

//------------------------------------------------------------------------------
// Execute command and display any output information
//------------------------------------------------------------------------------
int
ICmdHelper::Execute(bool print_err, bool add_route)
{
  if (mIsLocal) {
    return 0;
  }

  int retc = ExecuteWithoutPrint(add_route);

  if (!mIsSilent && !mOutcome.result.empty()) {
    std::cout << GetResult();
  }

  if (print_err && !mOutcome.error.empty()) {
    std::cerr << GetError();
  }

  return retc;
}

//------------------------------------------------------------------------------
// Execute command without displaying the result
//------------------------------------------------------------------------------
int
ICmdHelper::ExecuteWithoutPrint(bool add_route)
{
  if (!mReq.command_case()) {
    std::cerr << "error: generic request object not populated with command"
              << std::endl;
    return EINVAL;
  }

  std::string b64buff;

  if (!eos::common::SymKey::ProtobufBase64Encode(&mReq, b64buff)) {
    std::cerr << "error: failed to base64 encode the request" << std::endl;
    return EINVAL;
  }

  std::string cmd = "mgm.cmd.proto=";
  cmd += b64buff;

  if (add_route) {
    AddRouteInfo(cmd);
  }

  std::ostringstream oss;
  oss << mGlobalOpts.mMgmUri
      << (mIsAdmin ? "//proc/admin/" : "//proc/user/") << "?"
      << cmd;

  if (!mGlobalOpts.mUserRole.empty()) {
    oss << "&eos.ruid=" << mGlobalOpts.mUserRole;
  }

  if (!mGlobalOpts.mGroupRole.empty()) {
    oss << "&eos.rgid=" << mGlobalOpts.mGroupRole;
  }

  if (mGlobalOpts.mForceSss) {
    oss << "&xrd.wantprot=sss";
  }

  if (getenv("EOSAUTHZ")) {
    oss << "&authz=" << getenv("EOSAUTHZ");
  }

  if (mGlobalOpts.mDebug) {
    PrintDebugMsg(oss.str());
  }

  return RawExecute(oss.str());
}

//------------------------------------------------------------------------------
// Execute command using the xrootd client
//------------------------------------------------------------------------------
int
ICmdHelper::RawExecute(const std::string& full_url)
{
  if (mSimulationMode) {
    if (mSimulatedData.front().expectedCommand != full_url) {
      mSimulationErrors +=
        SSTR("Expected command '" << mSimulatedData.front().expectedCommand
             << "', received '" << full_url << "'");
      return EIO;
    }

    // Command is OK
    mOutcome = mSimulatedData.front().outcome;
    mSimulatedData.pop();
    return mOutcome.errc;
  }

  std::ostringstream oss;

  if (mGlobalOpts.mMgmUri.substr(0, 6) == "ipc://") {
    // ZMQ connection
    zmq::context_t context(1);
    zmq::socket_t socket(context, ZMQ_REQ);
    std::string path = full_url;
    path.erase(0, mGlobalOpts.mMgmUri.length() + 1);
    socket.connect(mGlobalOpts.mMgmUri);
    zmq::message_t request(path.length());
    memcpy(request.data(), path.c_str(), path.length());
    socket.send(request);
    zmq::message_t response;
    socket.recv(&response);
    std::string sout;
    sout.assign((char*)response.data(), response.size());
    oss << sout;
  } else {
    // XRootD connection
    std::unique_ptr<XrdCl::File> client {new XrdCl::File()};
    XrdCl::XRootDStatus status = client->Open(full_url.c_str(),
                                 XrdCl::OpenFlags::Read);

    if (status.IsOK()) {
      off_t offset = 0;
      uint32_t nbytes = 0;
      char buffer[4096 + 1];
      status = client->Read(offset, 4096, buffer, nbytes);

      while (status.IsOK() && (nbytes > 0)) {
        buffer[nbytes] = 0;
        oss << buffer;
        offset += nbytes;
        status = client->Read(offset, 4096, buffer, nbytes);
      }

      status = client->Close();
    } else {
      int retc = status.GetShellCode();

      if (status.errNo) {
        retc = status.errNo;
      }

      oss << "mgm.proc.stdout="
          << "&mgm.proc.stderr=" << "error: errc=" << retc
          << " msg=\"" << status.ToString() << "\""
          << "&mgm.proc.retc=" << retc;
    }
  }

  return ProcessResponse(oss.str());
}

//------------------------------------------------------------------------------
// Process MGM response
//------------------------------------------------------------------------------
int ICmdHelper::ProcessResponse(const std::string& response)
{
  if (response.empty()) {
    mOutcome.error = "error: failed to read proc response";
    mOutcome.errc = EIO;
    return mOutcome.errc;
  }

  if (mGlobalOpts.mDebug) {
    PrintDebugMsg(response);
  }

  mOutcome.errc = 0;
  std::vector<std::pair<std::string, size_t>> tags {
    std::make_pair("mgm.proc.stdout=", -1),
    std::make_pair("&mgm.proc.stderr=", -1),
    std::make_pair("&mgm.proc.retc=", -1)
  };

  for (auto& elem : tags) {
    elem.second = response.find(elem.first);
  }

  if ((tags[0].second == std::string::npos) &&
      (tags[1].second == std::string::npos)) {
    // This is a "FUSE" format response that only contains the stdout without
    // error message or return code
    mOutcome.result = response;
    return mOutcome.errc;
  }

  // Parse stdout.
  if (tags[0].second != std::string::npos) {
    if (tags[1].second != std::string::npos) {
      mOutcome.result = response.substr(tags[0].first.length(),
                                        tags[1].second - tags[1].first.length() + 1);
    } else {
      mOutcome.result = response.substr(tags[0].first.length(),
                                        tags[2].second - tags[2].first.length() - 1);
    }
  }

  // Parse stderr
  if (tags[1].second != std::string::npos) {
    mOutcome.error = response.substr(tags[1].second + tags[1].first.length(),
                                     tags[2].second - (tags[1].second + tags[1].first.length()));
  }

  // Parse return code
  try {
    mOutcome.errc = std::stoi(response.substr(tags[2].second +
                              tags[2].first.length()));
  } catch (...) {
    mOutcome.error = "error: failed to parse response from server";
    return EINVAL;
  }

  return mOutcome.errc;
}


//------------------------------------------------------------------------------
// Method used for user confirmation of the specified command
//------------------------------------------------------------------------------
bool
ICmdHelper::ConfirmOperation()
{
  std::ostringstream out;
  std::string confirmation;
  srand(time(NULL));

  for (int i = 0; i < 10; i++) {
    confirmation += std::to_string((int)(9.0 * rand() / RAND_MAX));
  }

  out << "Confirm operation by typing => " << confirmation << std::endl;
  out << "                            => ";
  std::string userInput;
  std::cout << out.str();
  getline(std::cin, userInput);

  if (userInput == confirmation) {
    std::cout << std::endl << "Operation confirmed" << std::endl;
    return true;
  } else {
    std::cout << std::endl << "Operation not confirmed" << std::endl;
    return false;
  }
}

//------------------------------------------------------------------------------
// Get command output string
//------------------------------------------------------------------------------
std::string
ICmdHelper::GetResult()
{
  // Add new line if necessary
  std::string out = mOutcome.result;

  if (*out.rbegin() != '\n') {
    out += '\n';
  }

  return out;
}

//------------------------------------------------------------------------------
// Get command error string
//------------------------------------------------------------------------------
std::string
ICmdHelper::GetError()
{
  // Add new line if necessary
  std::string err = mOutcome.error;

  if (*err.rbegin() != '\n') {
    err += '\n';
  }

  return err;
}

//------------------------------------------------------------------------------
// Guess a default 'route' e.g. home directory
//------------------------------------------------------------------------------
std::string
ICmdHelper::DefaultRoute(bool verbose)
{
  std::string default_route = "";

  // add a default 'route' for the command
  if (getenv("EOSHOME")) {
    default_route = getenv("EOSHOME");
  } else {
    char default_home[4096];
    std::string username;

    if (getenv("EOSUSER")) {
      username = getenv("EOSUSER");
    }

    if (getenv("USER")) {
      username = getenv("USER");
    }

    if (username.length()) {
      snprintf(default_home, sizeof(default_home), "/eos/user/%s/%s/",
               username.substr(0, 1).c_str(), username.c_str());

      if (verbose) {
        // @note route warning is no longer displayed
        // fprintf(stderr,
        //         "# pre-configuring default route to %s\n"
        //         "# -use $EOSHOME variable to override\n",
        //         default_home);
      }

      default_route = default_home;
    }
  }

  return default_route;
}

//------------------------------------------------------------------------------
// Add eos.route opaque info depending on the type of request and on the
// default route configuration
//------------------------------------------------------------------------------
void
ICmdHelper::AddRouteInfo(std::string& cmd)
{
  using eos::console::RequestProto;
  bool verbose = true;

  // suppress routing output for formatted quota command
  switch (mReq.command_case()) {
  case RequestProto::kQuota:
    if (mReq.quota().lsuser().format()) {
      verbose = false;
    }

    if (mReq.quota().ls().format()) {
      verbose = false;
    }

    break;

  case RequestProto::kRm:
    verbose = false;
    break;

  default:
    break;
  }

  const std::string default_route = DefaultRoute(verbose);
  std::ostringstream oss;

  switch (mReq.command_case()) {
  case RequestProto::kRecycle:
    if (!default_route.empty()) {
      oss << "&eos.route=" << default_route;
    }

    break;

  case RequestProto::kAcl:
    oss << "&eos.route=" << mReq.acl().path();
    break;

  case RequestProto::kToken:
    oss << "&eos.route=" << mReq.token().path();
    break;

  case RequestProto::kRm:
    if (mReq.rm().path().empty()) {
      if (!default_route.empty()) {
        oss << "&eos.route=" << default_route;
      }
    } else {
      oss << "&eos.route=" << mReq.rm().path();
    }

    break;

  case RequestProto::kQuota:
    if (mReq.quota().subcmd_case() ==
        eos::console::QuotaProto::kLsuser) {
      oss << "&eos.route=" << mReq.quota().lsuser().space();
    }

    break;

  case RequestProto::kFind:
    oss << "&eos.route=" << mReq.find().path();
    break;

  default:
    break;
  }

  cmd += oss.str();
}
