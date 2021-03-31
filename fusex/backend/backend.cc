//------------------------------------------------------------------------------
//! @file backend.cc
//! @author Andreas-Joachim Peters CERN
//! @brief backend IO handling class
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "backend/backend.hh"
#include "cap/cap.hh"
#include "misc/fusexrdlogin.hh"
#include "eosfuse.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClURL.hh"

/* -------------------------------------------------------------------------- */
backend::backend()
/* -------------------------------------------------------------------------- */
{
  timeout = 0;
  put_timeout = 0;
}

/* -------------------------------------------------------------------------- */
backend::~backend()
/* -------------------------------------------------------------------------- */
{
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::init(std::string& _hostport, std::string& _remotemountdir,
              double& _timeout, double& _put_timeout)
/* -------------------------------------------------------------------------- */
{
  hostport = _hostport;
  mount = _remotemountdir;
  timeout = _timeout;
  put_timeout = _put_timeout;

  if ((mount.length() && (mount.at(mount.length() - 1) == '/'))) {
    mount.erase(mount.length() - 1);
  }

  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::mapErrCode(int retc)
{
  if( !retc ) return retc;
  return XProtocol::toErrno( retc );
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::getMD(fuse_req_t req,
               const std::string& path,
               std::vector<eos::fusex::container>& contv,
               bool listing,
               std::string authid
              )
/* -------------------------------------------------------------------------- */
{
  // return's the inode of path in inode and rc=0 for success, otherwise errno
  std::string requestURL = getURL(req, path, "fuseX" , "getfusex",
                                  listing ? "LS" : "GET", authid, listing ? true : false);

  if (listing || !use_mdquery()) {
    return fetchResponse(requestURL, contv);
  } else {
    return fetchQueryResponse(requestURL, contv);
  }
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::getMD(fuse_req_t req,
               uint64_t inode,
               const std::string& name,
               std::vector<eos::fusex::container>& contv,
               bool listing,
               std::string authid
              )
{
  std::string requestURL = getURL(req, inode, name, "fuseX" , "getfusex",
                                  listing ? "LS" : "GET",
                                  authid, listing ? true : false);

  if (listing || !use_mdquery()) {
    return fetchResponse(requestURL, contv);
  } else {
    return fetchQueryResponse(requestURL, contv);
  }
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::getMD(fuse_req_t req,
               uint64_t inode,
               uint64_t myclock,
               std::vector<eos::fusex::container>& contv,
               bool listing,
               std::string authid
              )
/* -------------------------------------------------------------------------- */
{
  std::string requestURL = getURL(req, inode, myclock, "fuseX" , "getfusex",
                                  listing ? "LS" : "GET",
                                  authid, listing ? true : false);

  if (listing || !use_mdquery()) {
    return fetchResponse(requestURL, contv);
  } else {
    return fetchQueryResponse(requestURL, contv);
  }
}

/* -------------------------------------------------------------------------- */
int
backend::getCAP(fuse_req_t req,
                uint64_t inode,
                std::vector<eos::fusex::container>& contv
               )
/* -------------------------------------------------------------------------- */
{
  uint64_t myclock = (uint64_t) time(NULL)+13; // allow for 'slow' requests up-to 15s
  std::string requestURL = getURL(req, inode, myclock, "fuseX", "getfusex",
                                  "GETCAP", "", true);
  return fetchResponse(requestURL, contv);
}


/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::fetchQueryResponse(std::string& requestURL,
                            std::vector<eos::fusex::container>& contv
                           )
/* -------------------------------------------------------------------------- */
{
  XrdCl::URL url(requestURL);
  eos_static_debug("request='%s'", requestURL.c_str());
  std::string sarg = url.GetPathWithParams();
  XrdCl::Buffer arg;
  arg.FromString(sarg);
  XrdCl::Buffer* bresponse = 0;
  XrdCl::XRootDStatus status = Query(url, XrdCl::QueryCode::OpaqueFile, arg,
                                     bresponse, 30, false);

  if (status.IsOK()) {
    eos_static_debug("%x", bresponse);
    eos_static_debug("response-size=%d",
                     bresponse ? bresponse->GetSize() : 0);

    if (bresponse && bresponse->GetBuffer()) {
      off_t offset = 0;
      eos::fusex::container cont;
      std::string response(bresponse->GetBuffer(), bresponse->GetSize());

      if (EOS_LOGS_DEBUG)
        eos_static_debug("result-dump=%s",
                         eos::common::StringConversion::string_to_hex(response).c_str());

      do {
        cont.Clear();

        if ((response.size() - offset) > 10) {
          std::string slen = response.substr(1 + offset, 8);
          size_t len = strtoll(slen.c_str(), 0, 16);
          eos_static_debug("len=%llu offset=%llu", len, offset);

          if (!len) {
            eos_static_debug("response had illegal length");
            return EINVAL;
          }

          std::string item;
          item.assign(response.c_str() + offset + 10, len);
          offset += (10 + len);

          if (cont.ParseFromString(item)) {
            eos_static_debug("response parsing OK");

            if ((cont.type() != cont.MD) &&
                (cont.type() != cont.MDMAP) &&
                (cont.type() != cont.CAP)) {
              eos_static_debug("wrong response type");
              return EINVAL;
            }

            contv.push_back(cont);
            eos_static_debug("parsed %ld/%ld", offset, response.size());

            if (offset == (off_t) response.size()) {
              break;
            }
          } else {
            eos_static_debug("response parsing FAILED");
            return EIO;
          }
        } else {
          eos_static_err("fatal protocol parsing error");
          return EINVAL;
        };
      } while (1);

      return 0;
    }

    eos_static_debug("");
  } else {
    if (status.errNo == XErrorCode::kXR_NotFound) {
      // this is just no such file or directory
      eos_static_debug("error=status is NOT ok : %s", status.ToString().c_str());
      errno = ENOENT;
      return ENOENT;
    }

    if (status.code == XrdCl::errAuthFailed) {
      eos_static_debug("");
      // this is an authentication error which results in permission denied
      errno = EPERM;
      return EPERM;
    }

    // all the other errors are reported back
    if (status.errNo) {
      errno = XrdCl::Proxy::status2errno(status);
      eos_static_err("error=status is not ok : errno=%d", errno);

      // xrootd does not transport E2BIG ... sigh
      if (errno == ENAMETOOLONG) {
        errno = E2BIG;
      }

      return errno;
    }

    eos_static_debug("");
  }

  return EIO;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::fetchResponse(std::string& requestURL,
                       std::vector<eos::fusex::container>& contv
                      )
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("request='%s'", requestURL.c_str());
  double total_exec_time_sec = 0;
  XrdCl::XRootDStatus status;
  std::unique_ptr <XrdCl::File> file(new XrdCl::File());
  std::string response;
  off_t offset = 0;
  const int kPAGE = 512 * 1024;
  std::vector<char> rbuff;
  rbuff.reserve(kPAGE);
  uint32_t bytesread = 0;

  do {
    struct timespec ts;
    eos::common::Timing::GetTimeSpec(ts, true);

    // the MD get operation is implemented via a stream: open/read/close
    if (EOS_LOGS_DEBUG) {
      eos_static_debug("opening %s", requestURL.c_str());
    }

    status = file->Open(requestURL.c_str(),
                        XrdCl::OpenFlags::Flags::Read);
    double exec_time_sec = 1.0 * eos::common::Timing::GetCoarseAgeInNs(&ts,
                           0) / 1000000000.0;
    total_exec_time_sec += exec_time_sec;
    std::string lasturl;
    file->GetProperty("LastURL", lasturl);

    if (lasturl.length()) {
      EosFuse::Instance().TrackMgm(lasturl);
    }

    if (!status.IsOK()) {
      // check if we got an inlined response in an error object
      std::string b64response = status.GetErrorMessage();

      if (b64response.substr(0, 6) == "base64") {
        eos::common::SymKey::DeBase64(b64response, response);
        goto has_response;
      }

      // in case of any failure
      if (status.errNo == XErrorCode::kXR_NotFound) {
        // this is just no such file or directory
        eos_static_debug("error=status is NOT ok : %s", status.ToString().c_str());
        errno = ENOENT;
        return ENOENT;
      }

      if (status.IsFatal() || EOS_LOGS_DEBUG || (status.errNo != kXR_NotAuthorized)) {
        eos_static_err("fetch-exec-ms=%.02f sum-query-exec-ms=%.02f ok=%d err=%d fatal=%d status-code=%d err-no=%d",
                       exec_time_sec * 1000.0, total_exec_time_sec * 1000.0, status.IsOK(),
                       status.IsError(), status.IsFatal(), status.code, status.errNo);
        eos_static_err("error=status is NOT ok : %s %d %d", status.ToString().c_str(),
                       status.code, status.errNo);
      }

      if (status.code == XrdCl::errAuthFailed) {
        // this is an authentication error which results in permission denied
        errno = EPERM;
        return EPERM;
      }

      std::string xrootderr = status.GetErrorMessage();

      // the xrootd mapping of errno to everything unknown to EIO is really unfortunate
      if (xrootderr.find("get-cap-clock-out-of-sync") != std::string::npos) {
        // this is a time synchronization error
        errno = EL2NSYNC;
        return EL2NSYNC;
      }

      if (
        (status.code == XrdCl::errConnectionError) ||
        (status.code == XrdCl::errSocketTimeout) ||
        (status.code == XrdCl::errOperationExpired) ||
        (status.code == XrdCl::errSocketDisconnected)
      ) {
        // if there is a timeout, we might retry according to the backend timeout setting
        if (timeout &&
            (total_exec_time_sec >
             timeout)) {
          // it took longer than our backend timeout allows
          eos_static_err("giving up fetch after sum-fetch-exec-s=%.02f backend-timeout-s=%.02f",
                         total_exec_time_sec, timeout);
        } else {
          // retry
          std::this_thread::sleep_for(std::chrono::seconds(5));
          file.reset(new XrdCl::File());
          continue;
        }
      }

      // all the other errors are reported back
      if (status.errNo) {
        errno = XrdCl::Proxy::status2errno(status);

        if ((status.errNo != EPERM)) {
          eos_static_err("error=status is not ok : errno=%d", errno);
        }

        // xrootd does not transport E2BIG ... sigh
        if (errno == ENAMETOOLONG) {
          errno = E2BIG;
        }

        return errno;
      }

      if (status.code) {
        errno = EIO;
        eos_static_err("error=status is not ok : code=%d", errno);
        return errno;
      }
    } else {
      eos_static_debug("fetch-exec-ms=%.02f sum-fetch-exec-ms=%.02f ok=%d err=%d fatal=%d status-code=%d err-no=%d",
                       exec_time_sec * 1000.0, total_exec_time_sec * 1000.0, status.IsOK(),
                       status.IsError(), status.IsFatal(), status.code, status.errNo);
      break;
    }
  } while (1);

  // Start to read

  do {
    status = file->Read(offset, kPAGE, (char*) & rbuff[0], bytesread);

    if (status.IsOK()) {
      offset += bytesread;
      response.append(&rbuff[0], bytesread);
      eos_static_debug("+response=%s size=%u rsize=%u",
                       response.c_str(),
                       response.size(),
                       rbuff.size());
    } else {
      // failure
      bytesread = 0;
    }

    eos_static_debug("rbytes=%lu offset=%llu", bytesread, offset);
  } while (bytesread);

has_response:
  eos_static_debug("response-size=%u response=%s",
                   response.size(), response.c_str());
  //eos_static_debug("response-dump=%s", eos::common::StringConversion::string_to_hex(response).c_str());
  offset = 0;
  eos::fusex::container cont;

  do {
    cont.Clear();

    if ((response.size() - offset) > 10) {
      std::string slen = response.substr(1 + offset, 8);
      size_t len = strtoll(slen.c_str(), 0, 16);
      eos_static_debug("len=%llu offset=%llu", len, offset);

      if (!len) {
        eos_static_debug("response had illegal length");
        return EINVAL;
      }

      std::string item;
      item.assign(response.c_str() + offset + 10, len);
      offset += (10 + len);

      if (cont.ParseFromString(item)) {
        eos_static_debug("response parsing OK");

        if ((cont.type() != cont.MD) &&
            (cont.type() != cont.MDMAP) &&
            (cont.type() != cont.CAP)) {
          eos_static_debug("wrong response type");
          return EINVAL;
        }

        contv.push_back(cont);
        eos_static_debug("parsed %ld/%ld", offset, response.size());

        if (offset == (off_t) response.size()) {
          break;
        }
      } else {
        eos_static_debug("response parsing FAILED");
        return EIO;
      }
    } else {
      eos_static_err("fatal protocol parsing error");
      return EINVAL;
    };
  } while (1);

  return 0;
}

int
/* -------------------------------------------------------------------------- */
backend::rmRf(fuse_req_t req, eos::fusex::md* md)
/* -------------------------------------------------------------------------- */
{
  fuse_id id(req);
  XrdCl::URL url("root://" + hostport);
  url.SetPath("/proc/user/");
  XrdCl::URL::ParamsMap query;
  query["mgm.cmd"] = "rm";
  query["mgm.option"] = "r";
  query["mgm.container.id"] = std::to_string(md->md_ino());
  query["mgm.uuid"] = clientuuid;
  query["mgm.retc"] = "1";

  if (req) {
    query["mgm.cid"] = cap::capx::getclientid(req);
  }

  query["eos.app"] = get_appname();
  query["fuse.v"] = std::to_string(FUSEPROTOCOLVERSION);

  if (req) {
    fusexrdlogin::loginurl(url, query, req, 0);
  }

  url.SetParams(query);
  std::unique_ptr <XrdCl::File> file(new XrdCl::File());
  XrdCl::XRootDStatus status = file->Open(url.GetURL().c_str(),
                                          XrdCl::OpenFlags::Flags::Read);

  if (status.IsOK()) {
    return 0;
  } else {
    int retc = EREMOTEIO;

    if (status.code == XrdCl::errErrorResponse) {
      return mapErrCode(status.errNo);
    } else {
      return retc;
    }
  }
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::putMD(fuse_req_t req, eos::fusex::md* md, std::string authid,
               XrdSysMutex* locker)
{
  fuse_id id(req);
  return putMD(id, md, authid, locker);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::putMD(fuse_id& id, eos::fusex::md* md, std::string authid,
               XrdSysMutex* locker)
{
  XrdCl::URL url;
  XrdCl::URL::ParamsMap query;
  bool was_bound = false;

  if (!(id.getid())) {
    id.bind();
  } else {
    was_bound = true;
  }


  {
    // update host + port NOW
    XrdCl::URL lurl("root://" + hostport);
    id.getid()->url.SetHostPort(lurl.GetHostName(), lurl.GetPort());
  }

  id.getid()->query["eos.app"] = get_appname();
  id.getid()->query["fuse.v"] = std::to_string(FUSEPROTOCOLVERSION);
  id.getid()->url.SetParams(id.getid()->query);
  eos_static_debug("identity bound url=%s was-bound=%d",
                   id.getid()->url.GetURL().c_str(), was_bound);
  // temporary add the authid to be used for that request
  md->set_authid(authid);
  md->set_clientuuid(clientuuid);
  std::string mdstream;
  eos_static_info("proto-serialize");

  if (!md->SerializeToString(&mdstream)) {
    md->clear_authid();
    md->clear_clientuuid();
    md->clear_implied_authid();
    eos_static_err("fatal serialization error");
    return EFAULT;
  }

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("MD:\n%s", EosFuse::Instance().mds.dump_md(*md).c_str());
  }

  md->clear_authid();
  md->clear_clientuuid();
  md->clear_implied_authid();

  if (locker) {
    locker->UnLock();
  }

  eos_static_info("proto-serialize unlock");
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  std::string prefix = "/?fusex:";
  arg.Append(prefix.c_str(), prefix.length());
  arg.Append(mdstream.c_str(), mdstream.length());
  eos_static_debug("query: url=%s path=%s length=%d",
                   id.getid()->url.GetURL().c_str(),
                   prefix.c_str(), mdstream.length());
  XrdCl::XRootDStatus status = Query(id.getid()->url,
                                     XrdCl::QueryCode::OpaqueFile, arg,
                                     response, put_timeout);
  eos_static_info("sync-response");
  eos_static_debug("response-size=%d",
                   response ? response->GetSize() : 0);

  if (status.IsOK()) {
    if (response && response->GetBuffer()) {
      // eos_static_debug("response=%s response-size=%d",
      // response->GetBuffer(),
      //        response->GetSize());
      std::string responseprefix;

      if (response->GetSize() > 6) {
        responseprefix.assign(response->GetBuffer(), 6);
        // retrieve response
      } else {
        eos_static_err("protocol error - to short response received");
        delete response;

        if (locker) {
          locker->Lock();
        }

        return EIO;
      }

      if (responseprefix != "Fusex:") {
        eos_static_err("protocol error - fusex: prefix missing in response");
        delete response;

        if (locker) {
          locker->Lock();
        }

        return EIO;
      }

      std::string sresponse;
      std::string b64response;
      b64response.assign(response->GetBuffer() + 6, response->GetSize() - 6);
      eos::common::SymKey::DeBase64(b64response, sresponse);
      eos::fusex::response resp;

      if (!resp.ParseFromString(sresponse) ||
          ((resp.type() != resp.ACK) && (resp.type() != resp.NONE))) {
        eos_static_err("parsing error/wrong response type received");
        delete response;

        if (locker) {
          locker->Lock();
        }

        return EIO;
      }

      if (resp.type() == resp.ACK) {
        if (resp.ack_().code() == resp.ack_().OK) {
          eos_static_info("relock do");

          if (locker) {
            locker->Lock();
          }

	  if (resp.ack_().md_ino()) {
	    md->set_md_ino(resp.ack_().md_ino());
	  }
          eos_static_debug("directory inode %lx => %lx/%lx tid=%lx error='%s'", md->id(),
                           md->md_ino(),
                           resp.ack_().md_ino(), resp.ack_().transactionid(),
                           resp.ack_().err_msg().c_str());
          eos_static_info("relock done");
          delete response;
          return 0;
        }

        eos_static_err("failed query command for ino=%lx error='%s'", md->id(),
                       resp.ack_().err_msg().c_str());

        if (EOS_LOGS_DEBUG) {
          eos_static_err("MD:\n%s", EosFuse::Instance().mds.dump_md(*md).c_str());
        }

        delete response;

        if (locker) {
          locker->Lock();
        }

        return EIO;
      }

      if (resp.type() == resp.NONE) {
        delete response;

        if (locker) {
          locker->Lock();
        }

        return 0;
      }
    } else {
      eos_static_err("no response retrieved response=%lu response-buffer=%lu",
                     response, response ? response->GetBuffer() : 0);

      if (response) {
        delete response;
      }

      if (locker) {
        locker->Lock();
      }

      return EIO;
    }

    if (locker) {
      locker->Lock();
    }

    return 0;
  } else {
    eos_static_err("query resulted in error for ino=%lx url=%s", md->id(),
                   id.getid()->url.GetURL().c_str());

    if (locker) {
      locker->Lock();
    }

    if (status.code == XrdCl::errErrorResponse) {
      eos_static_err("errno=%i", status.errNo);
      return mapErrCode(status.errNo);
    } else {
      return EIO;
    }
  }
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::doLock(fuse_req_t req,
                eos::fusex::md& md,
                XrdSysMutex* locker)
/* -------------------------------------------------------------------------- */
{
  XrdCl::URL url("root://" + hostport);
  url.SetPath("/dummy");
  XrdCl::URL::ParamsMap query;
  fusexrdlogin::loginurl(url, query, req, 0);
  query["fuse.v"] = std::to_string(FUSEPROTOCOLVERSION);
  url.SetParams(query);
  md.set_clientuuid(clientuuid);
  std::string mdstream;
  eos_static_info("proto-serialize");

  if (!md.SerializeToString(&mdstream)) {
    md.clear_clientuuid();
    md.clear_flock();
    eos_static_err("fatal serialization error");
    return EFAULT;
  }

  md.clear_clientuuid();
  md.clear_flock();
  locker->UnLock();
  eos_static_info("proto-serialize unlock");
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  std::string prefix = "/?fusex:";
  arg.Append(prefix.c_str(), prefix.length());
  arg.Append(mdstream.c_str(), mdstream.length());
  eos_static_debug("query: url=%s path=%s length=%d", url.GetURL().c_str(),
                   prefix.c_str(), mdstream.length());
  XrdCl::XRootDStatus status = Query(url, XrdCl::QueryCode::OpaqueFile, arg,
                                     response);
  eos_static_info("sync-response");

  if (status.IsOK()) {
    eos_static_debug("response=%s response-size=%d",
                     response ? response->GetBuffer() : "null", response ? response->GetSize() : 0);

    if (response && response->GetBuffer()) {
      std::string responseprefix;

      if (response->GetSize() > 6) {
        responseprefix.assign(response->GetBuffer(), 6);
        // retrieve response
      } else {
        eos_static_err("protocol error - to short response received");
        locker->Lock();
        return EIO;
      }

      if (responseprefix != "Fusex:") {
        eos_static_err("protocol error - fusex: prefix missing in response");
        locker->Lock();
        return EIO;
      }

      std::string sresponse;
      std::string b64response;
      b64response.assign(response->GetBuffer() + 6, response->GetSize() - 6);
      eos::common::SymKey::DeBase64(b64response, sresponse);
      eos::fusex::response resp;

      if (!resp.ParseFromString(sresponse) || (resp.type() != resp.LOCK)) {
        eos_static_err("parsing error/wrong response type received");
        locker->Lock();
        return EIO;
      }

      if (resp.ack_().code() == resp.ack_().OK) {
        eos_static_info("relock do");
        locker->Lock();
        (*(md.mutable_flock())) = (resp.lock_());
        eos_static_debug("directory inode %lx => %lx/%lx tid=%lx error='%s'", md.id(),
                         md.md_ino(),
                         resp.ack_().md_ino(), resp.ack_().transactionid(),
                         resp.ack_().err_msg().c_str());
        eos_static_info("relock done");
        return 0;
      }

      eos_static_err("failed query command for ino=%lx error='%s'", md.id(),
                     resp.ack_().err_msg().c_str());

      if (EOS_LOGS_DEBUG) {
        eos_static_err("MD:\n%s", EosFuse::Instance().mds.dump_md(md).c_str());
      }

      locker->Lock();
      return EIO;
    } else {
      eos_static_err("no response retrieved response=%lu response-buffer=%lu",
                     response, response ? response->GetBuffer() : 0);
      locker->Lock();
      return EIO;
    }
  } else {
    eos_static_err("query resulted in error url=%s", url.GetURL().c_str());
  }

  locker->Lock();
  return EIO;
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
backend::getURL(fuse_req_t req, const std::string& path, std::string cmd,
                std::string pcmd, std::string op,
                std::string authid,
                bool setinline)
/* -------------------------------------------------------------------------- */
{
  XrdCl::URL url("root://" + hostport);
  url.SetPath("/proc/user/");
  XrdCl::URL::ParamsMap query;
  query["mgm.cmd"] = cmd;
  query["mgm.pcmd"] = pcmd;
  query["mgm.clock"] = "0";
  query["mgm.path"] = eos::common::StringConversion::curl_escaped(mount + path);
  query["mgm.op"] = op;
  query["mgm.uuid"] = clientuuid;

  if (setinline) {
    query["mgm.inline"] = "1";
  }

  if (req) {
    query["mgm.cid"] = cap::capx::getclientid(req);
  }

  query["eos.app"] = get_appname();

  if (authid.length()) {
    query["mgm.authid"] = authid;
  }

  query["fuse.v"] = std::to_string(FUSEPROTOCOLVERSION);

  if (req) {
    fusexrdlogin::loginurl(url, query, req, 0);
  }

  url.SetParams(query);
  return url.GetURL();
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
backend::getURL(fuse_req_t req, uint64_t inode, const std::string& name,
                std::string cmd,
                std::string pcmd, std::string op, std::string authid, bool setinline)
{
  XrdCl::URL url("root://" + hostport);
  url.SetPath("/proc/user/");
  XrdCl::URL::ParamsMap query;
  query["mgm.cmd"] = cmd;
  query["mgm.pcmd"] = pcmd;
  query["mgm.clock"] = "0";
  query["mgm.child"] = eos::common::StringConversion::curl_escaped(name);
  char hexinode[32];
  snprintf(hexinode, sizeof(hexinode), "%08lx", (unsigned long) inode);
  query["mgm.inode"] =
    hexinode;
  query["mgm.op"] = op;
  query["mgm.uuid"] = clientuuid;
  query["eos.app"] = get_appname();

  if (authid.length()) {
    query["mgm.authid"] = authid;
  }

  query["mgm.cid"] = cap::capx::getclientid(req);

  if (setinline) {
    query["mgm.inline"] = "1";
  }

  query["fuse.v"] = std::to_string(FUSEPROTOCOLVERSION);
  fusexrdlogin::loginurl(url, query, req, inode);
  url.SetParams(query);
  return url.GetURL();
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
backend::getURL(fuse_req_t req, uint64_t inode, uint64_t clock, std::string cmd,
                std::string pcmd, std::string op,
                std::string authid, bool setinline)
/* -------------------------------------------------------------------------- */
{
  XrdCl::URL url("root://" + hostport);
  url.SetPath("/proc/user/");
  XrdCl::URL::ParamsMap query;
  std::string sclock;
  query["mgm.cmd"] = cmd;
  query["mgm.pcmd"] = pcmd;
  query["mgm.clock"] =
    eos::common::StringConversion::GetSizeString(sclock,
        (unsigned long long) clock);
  char hexinode[32];
  snprintf(hexinode, sizeof(hexinode), "%08lx", (unsigned long) inode);
  query["mgm.inode"] =
    hexinode;
  query["mgm.op"] = op;
  query["mgm.uuid"] = clientuuid;
  query["eos.app"] = get_appname();

  if (authid.length()) {
    query["mgm.authid"] = authid;
  }

  query["mgm.cid"] = cap::capx::getclientid(req);

  if (setinline) {
    query["mgm.inline"] = "1";
  }

  query["fuse.v"] = std::to_string(FUSEPROTOCOLVERSION);
  fusexrdlogin::loginurl(url, query, req, inode);
  url.SetParams(query);
  return url.GetURL();
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::statvfs(fuse_req_t req,
                 struct statvfs* stbuf
                )
/* -------------------------------------------------------------------------- */
{
  XrdCl::URL url("root://" + hostport);
  url.SetPath("/");
  XrdCl::URL::ParamsMap query;
  std::string sclock;
  query["mgm.pcmd"] = "statvfs";
  query["eos.app"] = get_appname();
  query["path"] = "/";
  query["fuse.v"] = std::to_string(FUSEPROTOCOLVERSION);
  fusexrdlogin::loginurl(url, query, req, 0);
  url.SetParams(query);
  std::string sarg = url.GetPathWithParams();
  static unsigned long long a1 = 0;
  static unsigned long long a2 = 0;
  static unsigned long long a3 = 0;
  static unsigned long long a4 = 0;
  // ---------------------------------------------------------------------------
  // statfs caching around 10s
  // ---------------------------------------------------------------------------
  static XrdSysMutex statmutex;
  static time_t laststat = 0;
  errno = 0;
  {
    XrdSysMutexHelper sLock(statmutex);

    if ((time(NULL) - laststat) < ((15 + (int) 5.0 * rand() / RAND_MAX))) {
      stbuf->f_bsize = 4096;
      stbuf->f_frsize = 4096;
      stbuf->f_blocks = a3 / 4096;
      stbuf->f_bfree = a1 / 4096;
      stbuf->f_bavail = a1 / 4096;
      stbuf->f_files = a4;
      stbuf->f_ffree = a2;
      stbuf->f_fsid = 0xcafe;
      stbuf->f_namemax = 1024;
      eos_static_info("not calling %s\n", url.GetURL().c_str());
      return errno;
    }
  }
  XrdCl::Buffer arg;
  arg.FromString(sarg);
  XrdCl::Buffer* response = 0;
  XrdCl::XRootDStatus status = Query(url, XrdCl::QueryCode::OpaqueFile, arg,
                                     response, 2, true);
  eos_static_info("calling %s\n", url.GetURL().c_str());

  if (status.IsOK() && response && response->GetBuffer()) {
    int retc;
    char tag[1024];

    if (!response->GetBuffer()) {
      errno = EFAULT;
      delete response;
      return errno;
    }

    XrdSysMutexHelper sLock(statmutex);
    int items = sscanf(response->GetBuffer(),
                       "%s retc=%d f_avail_bytes=%llu f_avail_files=%llu "
                       "f_max_bytes=%llu f_max_files=%llu",
                       tag, &retc, &a1, &a2, &a3, &a4);

    if ((items != 6) || (strcmp(tag, "statvfs:"))) {
      errno = EFAULT;
      delete response;
      return errno;
    }

    errno = retc;
    laststat = time(NULL);
    stbuf->f_bsize = 4096;
    stbuf->f_frsize = 4096;
    stbuf->f_blocks = a3 / 4096;
    stbuf->f_bfree = a1 / 4096;
    stbuf->f_bavail = a1 / 4096;
    stbuf->f_files = a4;
    stbuf->f_ffree = a2;
    stbuf->f_namemax = 1024;
    eos_static_debug("vol=%lu ino=%lu", a1, a4);
  } else {
    errno = ETIMEDOUT;
    ;
  }

  delete response;
  return errno;
}


/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::getChecksum(fuse_req_t req,
                     uint64_t inode,
                     std::string& checksum_return)
/* -------------------------------------------------------------------------- */
{
  fuse_id id(req);
  XrdCl::URL url("root://" + hostport);
  std::string path = "ino:";
  char sino[64];
  snprintf(sino, sizeof(sino), "%lx", inode);
  path += sino;
  url.SetPath("/");
  XrdCl::URL::ParamsMap query;
  fusexrdlogin::loginurl(url, query, id.uid, id.gid, id.pid, 0);
  query["eos.app"] = get_appname();
  query["mgm.pcmd"] = "checksum";
  query["eos.lfn"] = path;
  query["mgm.option"] = "fuse";
  url.SetParams(query);
  std::string sarg = url.GetPathWithParams();
  XrdCl::Buffer arg;
  arg.FromString(sarg);
  XrdCl::Buffer* response = 0;
  eos_static_debug("query: url=%s", url.GetURL().c_str());
  XrdCl::XRootDStatus status = Query(url, XrdCl::QueryCode::OpaqueFile, arg,
                                     response, put_timeout);
  eos_static_info("sync-response");
  eos_static_debug("response-size=%d",
                   response ? response->GetSize() : 0);

  if (status.IsOK()) {
    if (response && response->GetBuffer()) {
      std::string checksum_response;
      checksum_response.assign(response->GetBuffer(), response->GetSize());
      eos_static_debug("response=%s", checksum_response.c_str());
      char checksum[1024]; // there should be no checksum with length 1024 bytes... unless you have corruption
      int retc = 0;
      size_t items = sscanf(checksum_response.c_str(), "checksum: %1023s retc=%i",
                            checksum, &retc);

      if (items != 2) {
        size_t items = sscanf(checksum_response.c_str(), "checksum:  retc=%i", &retc);

        if (items == 1) {
          if (retc == ENOENT) {
            // an old server might not be able to call getChecksum by file id, we return an empty one in that case
            checksum_return = "unknown";
          } else {
            delete response;
            return retc;
          }
        } else {
          delete response;
          return ENODATA;
        }
      } else {
        if (retc) {
          if (retc == ENOENT) {
            checksum_return = "unknown";
          } else {
            delete response;
            return ENODATA;
          }
        } else {
          checksum_return = checksum;
        }
      }
    }

    if (response) {
      delete response;
    }

    return 0;
  } else {
    eos_static_err("query resulted in error for ino=%lx url=%s rc=%d", inode,
                   url.GetURL().c_str(),
                   (status.code == XrdCl::errErrorResponse) ? mapErrCode(status.errNo) : EIO);

    if (status.code == XrdCl::errErrorResponse) {
      return mapErrCode(status.errNo);
    } else {
      return EIO;
    }
  }
}



/* -------------------------------------------------------------------------- */
XrdCl::XRootDStatus
/* -------------------------------------------------------------------------- */
backend::Query(XrdCl::URL& url, XrdCl::QueryCode::Code query_code,
               XrdCl::Buffer& arg, XrdCl::Buffer*& response, uint16_t rtimeout,
               bool noretry)
/* -------------------------------------------------------------------------- */
{
  // this function retries queries until the given timeout period has been reached
  // it does not proceed if there is an authentication failure
  double total_exec_time_sec = 0;
  std::unique_ptr <XrdCl::FileSystem> fs(new XrdCl::FileSystem(url));

  do {
    struct timespec ts;
    eos::common::Timing::GetTimeSpec(ts, true);
    XrdCl::XRootDStatus status;
    status = fs->Query(XrdCl::QueryCode::OpaqueFile, arg, response, rtimeout);

    // we can't do anything if we cannot authenticate
    if (status.code == XrdCl::errAuthFailed) {
      return status;
    }

    // we want to report all errors which are not timeout related
    if (
      (status.code != XrdCl::errConnectionError) &&
      (status.code != XrdCl::errSocketTimeout) &&
      (status.code != XrdCl::errOperationExpired) &&
      (status.code != XrdCl::errSocketDisconnected)
    ) {
      return status;
    }

    double exec_time_sec = 1.0 * eos::common::Timing::GetCoarseAgeInNs(&ts,
                           0) / 1000000000.0;
    total_exec_time_sec += exec_time_sec;
    eos_static_err("query-exec-ms=%.02f sum-query-exec-ms=%.02f ok=%d err=%d fatal=%d status-code=%d err-no=%d",
                   exec_time_sec * 1000.0, total_exec_time_sec * 1000.0, status.IsOK(),
                   status.IsError(), status.IsFatal(), status.code, status.errNo);

    if ((noretry) || (timeout &&
                      (total_exec_time_sec >
                       timeout))) {
      std::string sarg = url.GetPathWithParams();
      eos_static_err("giving up query after sum-query-exec-s=%.02f backend-timeout-s=%.02f no-retry=%d url=%s",
                     total_exec_time_sec, timeout, noretry, sarg.c_str());
      return status;
    }

    std::this_thread::sleep_for(std::chrono::seconds(5));
    fs.reset(new XrdCl::FileSystem(url));
  } while (1);
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
backend::get_appname()
{
  if (EosFuse::Instance().mds.supports_appname()) {
    return EosFuse::Instance().Config().appname;
  } else {
    return "fuse";
  }
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
backend::use_mdquery()
{
  return EosFuse::Instance().mds.supports_mdquery();
}
