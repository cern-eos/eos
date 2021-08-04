//------------------------------------------------------------------------------
//! @file IProcCommand.cc
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

#include "common/Path.hh"
#include "common/CommentLog.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/proc/IProcCommand.hh"
#include "mgm/proc/ProcInterface.hh"
#include "mgm/Macros.hh"
#include "namespace/interface/IView.hh"
#include "json/json.h"
#include <google/protobuf/util/json_util.h>

EOSMGMNAMESPACE_BEGIN

std::atomic_uint_least64_t IProcCommand::uuid{0};
std::map<eos::console::RequestProto::CommandCase, std::atomic<uint64_t>>
    IProcCommand::mCmdsExecuting;

//------------------------------------------------------------------------------
// Open a proc command e.g. call the appropriate user or admin command and
// store the output in a resultstream or in case of find in a temporary output
// file.
//------------------------------------------------------------------------------
int
IProcCommand::open(const char* path, const char* info,
                   eos::common::VirtualIdentity& vid,
                   XrdOucErrInfo* error)
{
  // @todo (esindril): configure delay based on the type of command
  int delay = 5;

  if (!mExecRequest) {
    if (HasSlot()) {
      LaunchJob();
      mExecRequest = true;
    } else {
      eos_notice("%s", SSTR("cmd_type=" << mReqProto.command_case() <<
                            " no more slots, stall client 3 seconds").c_str());
      return delay - 2;
    }
  }

  if (mFuture.wait_for(std::chrono::seconds(delay)) !=
      std::future_status::ready) {
    // Stall the client
    std::string msg = "command not ready, stall the client 5 seconds";
    eos_notice("%s", msg.c_str());
    error->setErrInfo(0, msg.c_str());
    return delay;
  } else {
    eos::console::ReplyProto reply = mFuture.get();

    // Routing redirect encountered
    if (reply.retc() == SFS_REDIRECT) {
      eos_notice("msg=\"routing redirect\" path=%s hostport=%s:%d "
                 "stall_timeout=%d", mRoutingInfo.path.c_str(),
                 mRoutingInfo.host.c_str(), mRoutingInfo.port,
                 mRoutingInfo.stall_timeout);

      if (mRoutingInfo.stall_timeout) {
        // Force re-execution of the command upon return from stall
        mExecRequest = false;
        std::string stall_msg = "No master MGM available";
        return gOFS->Stall(*error, mRoutingInfo.stall_timeout,
                           stall_msg.c_str());
      }

      return gOFS->Redirect(*error, mRoutingInfo.host.c_str(),
                            mRoutingInfo.port);
    }

    // Output is written in file
    if (!ofstdoutStreamFilename.empty() && !ofstderrStreamFilename.empty()) {
      ifstdoutStream.open(ofstdoutStreamFilename, std::ifstream::in);
      ifstderrStream.open(ofstderrStreamFilename, std::ifstream::in);
      iretcStream.str(std::string("&mgm.proc.retc=") + std::to_string(reply.retc()));
      readStdOutStream = true;
    } else {
      std::ostringstream oss;

      if (mReqProto.format() == eos::console::RequestProto::FUSE) {
        // The proto dumpmd issued by the FST uses the FUSE format
        // (resync metadata, background Fsck and standalone Fsck)
        // @todo This format should be dropped once Quarkdb migration is complete
        //       and the NS will be queried directly
        oss << reply.std_out();
      } else {
        oss << "mgm.proc.stdout=" << reply.std_out().c_str()
            << "&mgm.proc.stderr=" << reply.std_err().c_str()
            << "&mgm.proc.retc=" << reply.retc();
      }

      mTmpResp = oss.str();
    }

    // Store the client's command comment in the comments logbook
    if ((vid.uid <= 2) || (vid.sudoer)) {
      // Only instance users or sudoers can add to the logbook
      if (mComment.length() && gOFS->mCommentLog) {
        std::string argsJson;
        (void) google::protobuf::util::MessageToJsonString(mReqProto, &argsJson);

        if (!gOFS->mCommentLog->Add(mTimestamp, "", "", argsJson.c_str(),
                                    mComment.c_str(), stdErr.c_str(), // @note stErr or reply.std_err()?
                                    reply.retc())) {
          eos_err("failed to log to comments logbook");
        }
      }
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
  } else if (readStdErrStream && ifstderrStream.is_open()) {
    ifstderrStream.read(buff, blen);
    cpy_len = (size_t)ifstderrStream.gcount();

    if (cpy_len < (size_t)blen) {
      readStdErrStream = false;
      readRetcStream = true;
      iretcStream.read(buff + cpy_len, blen - cpy_len);
      cpy_len += (size_t)iretcStream.gcount();
    }
  } else if (readRetcStream) {
    iretcStream.read(buff, blen);
    cpy_len = (size_t)iretcStream.gcount();

    if (cpy_len < (size_t)blen) {
      readRetcStream = false;
    }
  } else if ((size_t)offset < mTmpResp.length()) {
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
    ([this]() -> eos::console::ReplyProto {
      return ProcessRequest();
    });

    if (EOS_LOGS_DEBUG) {
      eos_debug("%s", ProcInterface::sProcThreads.GetInfo().c_str());
    }
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
  bool is_killed = true;

  if (mDoAsync) {
    mForceKill.store(true);

    if (mFuture.valid()) {
      is_killed = (mFuture.wait_for(std::chrono::seconds(0)) ==
                   std::future_status::ready);
    }
  }

  return is_killed;
}

//------------------------------------------------------------------------------
// Open temporary output files for file based results
//------------------------------------------------------------------------------
bool
IProcCommand::OpenTemporaryOutputFiles()
{
  ostringstream tmpdir;
  tmpdir << "/var/tmp/eos/mgm/";
  tmpdir << uuid++;
  ofstdoutStreamFilename = tmpdir.str();
  ofstdoutStreamFilename += ".stdout";
  ofstderrStreamFilename = tmpdir.str();
  ofstderrStreamFilename += ".stderr";
  eos::common::Path cPath(ofstdoutStreamFilename.c_str());

  if (!cPath.MakeParentPath(S_IRWXU)) {
    eos_err("Unable to create temporary outputfile directory %s",
            tmpdir.str().c_str());
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

//------------------------------------------------------------------------------
// Open temporary output files for file based results
//------------------------------------------------------------------------------
bool
IProcCommand::CloseTemporaryOutputFiles()
{
  ofstdoutStream.close();
  ofstderrStream.close();
  return !(ofstdoutStream.is_open() || ofstderrStream.is_open());
}

//------------------------------------------------------------------------------
// Format console output string as json
//------------------------------------------------------------------------------
Json::Value
IProcCommand::ConvertOutputToJsonFormat(const std::string& stdOut)
{
  using eos::common::StringConversion;
  std::stringstream ss(stdOut);
  Json::Value jsonOut;
  std::string line;

  do {
    Json::Value jsonEntry;
    line.clear();

    if (!std::getline(ss, line)) {
      break;
    }

    if (!line.length()) {
      continue;
    }

    XrdOucString sline = line.c_str();

    while (sline.replace("<n>", "n")) {}

    while (sline.replace("?configstatus@rw", "_rw")) {}

    line = sline.c_str();
    std::map <std::string, std::string> map;
    StringConversion::GetKeyValueMap(line.c_str(), map, "=", " ");
    // These values violate the JSON hierarchy and have to be rewritten
    StringConversion::ReplaceMapKey(map, "cfg.balancer",
                                    "cfg.balancer.status");
    StringConversion::ReplaceMapKey(map, "cfg.geotagbalancer",
                                    "cfg.geotagbalancer.status");
    StringConversion::ReplaceMapKey(map, "cfg.geobalancer",
                                    "cfg.geobalancer.status");
    StringConversion::ReplaceMapKey(map, "cfg.groupbalancer",
                                    "cfg.groupbalancer.status");
    StringConversion::ReplaceMapKey(map, "geotagbalancer",
                                    "geotagbalancer.status");
    StringConversion::ReplaceMapKey(map, "geobalancer",
                                    "geobalancer.status");
    StringConversion::ReplaceMapKey(map, "groupbalancer",
                                    "groupbalancer.status");
    StringConversion::ReplaceMapKey(map, "cfg.wfe", "cfg.wfe.status");
    StringConversion::ReplaceMapKey(map, "cfg.lru", "cfg.lru.status");
    StringConversion::ReplaceMapKey(map, "local.drain", "local.drain.status");
    StringConversion::ReplaceMapKey(map, "stat.health", "stat.health.status");
    StringConversion::ReplaceMapKey(map, "wfe", "wfe.status");
    StringConversion::ReplaceMapKey(map, "lru", "lru.status");
    StringConversion::ReplaceMapKey(map, "balancer", "balancer.status");
    StringConversion::ReplaceMapKey(map, "converter", "converter.status");

    for (auto& it : map) {
      std::vector<std::string> token;
      char* conv;
      errno = 0;
      StringConversion::Tokenize(it.first, token, ".");
      double val = strtod(it.second.c_str(), &conv);
      std::string value;

      if (token.empty()) {
        continue;
      }

      if (it.second.length()) {
        value = it.second;
      } else {
        value = "NULL";
      }

      auto* jep = &(jsonEntry[token[0]]);

      for (int i = 1; i < (int)token.size(); i++) {
        jep = &((*jep)[token[i]]);
      }

      // Unquote value
      std::stringstream quoted_ss(value);
      quoted_ss >> std::quoted(value);
      // Seal value
      XrdOucString svalue = value.c_str();
      XrdMqMessage::Seal(svalue);
      value = svalue.c_str();

      if (errno || (!val && (conv  == it.second.c_str())) ||
          ((conv - it.second.c_str()) != (long long)it.second.length())) {
        // non numeric
        (*jep) = value;
      } else {
        // numeric
        (*jep) = val;
      }
    }

    jsonOut.append(jsonEntry);
  } while (true);

  return jsonOut;
}

//------------------------------------------------------------------------------
// Create a JSON string from the command output, error and return code
//------------------------------------------------------------------------------
std::string
IProcCommand::ResponseToJsonString(const std::string& out,
                                   const std::string& err, int rc)
{
  Json::Value json;

  try {
    json["result"] = ConvertOutputToJsonFormat(out);
    json["errormsg"] = err;
    json["retc"] = std::to_string(rc);
  } catch (Json::Exception& e) {
    eos_err("Json conversion exception cmd_type=%s emsg=\"%s\"",
            SSTR(mReqProto.command_case()).c_str(), e.what());
    json["errormsg"] = "illegal string in json conversion";
    json["retc"] = std::to_string(EFAULT);
  }

  return SSTR(json);
}

//------------------------------------------------------------------------------
// Retrieve the file's full path given its numeric id
//------------------------------------------------------------------------------
// drop when we drop non-proto commands using it
void
IProcCommand::GetPathFromFid(XrdOucString& path, unsigned long long fid,
                             const std::string& err_msg_prefix)
{
  std::string serr;
  std::string spath(path.c_str());
  retc = GetPathFromFid(spath, fid, serr);
  path = spath.c_str();
  stdErr = serr.c_str();
}

int
IProcCommand::GetPathFromFid(std::string& path, unsigned long long fid,
                             std::string& err_msg)
{
  if (path.empty()) {
    if (fid == 0ULL) {
      err_msg += "error: fid is 0";
      return EINVAL;
    }

    try {
      eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                              __LINE__, __FILE__);
      std::string temp = gOFS->eosView->getUri(gOFS->eosFileService->getFileMD(
                           fid).get());
      path = temp;
      return 0;
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_debug("caught exception %d %s\n", e.getErrno(),
                e.getMessage().str().c_str());
      err_msg = "error: " + e.getMessage().str() + '\n';
      return errno;
    }
  }

  return EINVAL;
}

//------------------------------------------------------------------------------
// Retrieve the container's full path given its numeric id
//------------------------------------------------------------------------------
// drop when we drop non-proto commands using it
void
IProcCommand::GetPathFromCid(XrdOucString& path, unsigned long long cid,
                             const std::string& err_msg_prefix)
{
  std::string serr;
  std::string spath(path.c_str());
  retc = GetPathFromCid(spath, cid, serr);
  path = spath.c_str();
  stdErr = serr.c_str();
}

int
IProcCommand::GetPathFromCid(std::string& path, unsigned long long cid,
                             std::string& err_msg)
{
  if (path.empty()) {
    if (cid == 0ULL) {
      err_msg += "error: cid is 0";
      return EINVAL;
    }

    try {
      eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
      std::string temp = gOFS->eosView->getUri
                         (gOFS->eosDirectoryService->getContainerMD(cid).get());
      path = temp;
      return 0;
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_debug("caught exception %d %s\n", e.getErrno(),
                e.getMessage().str().c_str());
      err_msg = "error: " + e.getMessage().str() + '\n';
      return errno;
    }
  }

  return EINVAL;
}

//------------------------------------------------------------------------------
// Check if operation forbidden
//------------------------------------------------------------------------------
bool
IProcCommand::IsOperationForbidden(const std::string& path,
                                   const eos::common::VirtualIdentity& vid,
                                   std::string& err_check, int& errno_check) const
{
  if (eos::mgm::ProcBounceIllegalNames(path, err_check, errno_check) ||
      eos::mgm::ProcBounceNotAllowed(path, mVid, err_check, errno_check)) {
    return true;
  }

  return false;
}

//----------------------------------------------------------------------------
// Fill routing information if a routing redirect should happen
//----------------------------------------------------------------------------
bool
IProcCommand::ShouldRoute(const std::string& path,
                          eos::console::ReplyProto& reply)
{
  eos_debug("msg=\"applying routing\" path=%s is_redirect=%d",
            path.c_str(), gOFS->IsRedirect);

  if (gOFS->IsRedirect) {
    if (gOFS->ShouldRoute(__FUNCTION__, 0, mVid, path.c_str(), 0,
                          mRoutingInfo.host, mRoutingInfo.port,
                          mRoutingInfo.stall_timeout)) {
      mRoutingInfo.path = path;
      reply.set_retc(SFS_REDIRECT);
      return true;
    }
  }

  return false;
}

//------------------------------------------------------------------------------
// Check if there is still an available slot for the current type of command
//------------------------------------------------------------------------------
bool
IProcCommand::HasSlot()
{
  static std::atomic<bool> init {false};

  // Initialize only once in the beginning
  if (!init) {
    init = true;

    for (const auto& type : {
    eos::console::RequestProto::kAcl,
        eos::console::RequestProto::kNs,
        eos::console::RequestProto::kFind,
        eos::console::RequestProto::kFs,
        eos::console::RequestProto::kRm,
        eos::console::RequestProto::kStagerRm,
        eos::console::RequestProto::kRoute,
        eos::console::RequestProto::kIo,
        eos::console::RequestProto::kGroup,
        eos::console::RequestProto::kDebug,
        eos::console::RequestProto::kNode,
        eos::console::RequestProto::kQuota,
        eos::console::RequestProto::kSpace,
        eos::console::RequestProto::kConfig,
        eos::console::RequestProto::kAccess,
        eos::console::RequestProto::kToken,
        eos::console::RequestProto::kQos,
        eos::console::RequestProto::kConvert
  }) {
      mCmdsExecuting.emplace(type, 0ull);
    }
  }

  uint64_t slot_limit {50};
  auto it = mCmdsExecuting.find(mReqProto.command_case());

  if (it == mCmdsExecuting.end()) {
    // This should not happen unless you forgot to populate the map in the
    // section above
    mCmdsExecuting[mReqProto.command_case()] = 1;
    mHasSlot = true;
  } else {
    if (it->second >= slot_limit) {
      return false;
    } else {
      ++it->second;
      mHasSlot = true;
    }
  }

  return true;
}

EOSMGMNAMESPACE_END
