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
#include "eosfuse.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
#include "fuse/SyncResponseHandler.hh"
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClURL.hh"

/* -------------------------------------------------------------------------- */
backend::backend()
/* -------------------------------------------------------------------------- */
{

}

/* -------------------------------------------------------------------------- */
backend::~backend()
/* -------------------------------------------------------------------------- */
{

}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::init(std::string& _hostport, std::string& _remotemountdir)
/* -------------------------------------------------------------------------- */
{
  hostport = _hostport;
  mount = _remotemountdir;

  if ( (mount.length() && (mount.at(mount.length() - 1) == '/')) )
    mount.erase(mount.length() - 1);

  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::mapErrCode(int retc)
{

  switch (retc) {
  case 0:
    break;
  case kXR_ArgInvalid:
    retc = EINVAL;
    break;
  case kXR_ArgMissing:
    retc = EINVAL;
    break;
  case kXR_ArgTooLong:
    retc = E2BIG;
    break;
  case kXR_FileNotOpen:
    retc = EBADF;
    break;
  case kXR_FSError:
    retc = EIO;
    break;
  case kXR_InvalidRequest:
    retc = EEXIST;
    break;
  case kXR_IOError:
    retc = EIO;
    break;
  case kXR_NoMemory:
    retc = ENOMEM;
    break;
  case kXR_NoSpace:
    retc = ENOSPC;
    break;
  case kXR_ServerError:
    retc = EIO;
    break;
  case kXR_NotAuthorized:
    retc = EACCES;
    break;
  case kXR_NotFound:
    retc = ENOENT;
    break;
  case kXR_Unsupported:
    retc = ENOTSUP;
    break;
  case kXR_NotFile:
    retc = EISDIR;
    break;
  case kXR_isDirectory:
    retc = EISDIR;
    break;
  case kXR_Cancelled:
    retc = ECANCELED;
    break;
  case kXR_ChkLenErr:
    retc = ERANGE;
    break;
  case kXR_ChkSumErr:
    retc = ERANGE;
    break;
  case kXR_inProgress:
    retc = EAGAIN;
    break;
  default:
    retc = EIO;
  }
  return retc;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::getMD(fuse_req_t req,
               const std::string& path,
               std::vector<eos::fusex::container>& contv,
               std::string authid
               )
/* -------------------------------------------------------------------------- */
{
  // return's the inode of path in inode and rc=0 for success, otherwise errno
  std::string requestURL = getURL(req, path, "LS", authid);
  return fetchResponse(requestURL, contv);
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
  std::string requestURL = getURL(req, inode, name, listing ? "LS" : "GET", authid);
  return fetchResponse(requestURL, contv);
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
  std::string requestURL = getURL(req, inode, myclock, listing ? "LS" : "GET", authid);
  return fetchResponse(requestURL, contv);
}

/* -------------------------------------------------------------------------- */
int
backend::getCAP(fuse_req_t req,
                uint64_t inode,
                std::vector<eos::fusex::container>& contv
                )
/* -------------------------------------------------------------------------- */
{
  uint64_t myclock = (uint64_t) time(NULL);
  std::string requestURL = getURL(req, inode, myclock, "GETCAP");
  return fetchResponse(requestURL, contv);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::fetchResponse(std::string& requestURL,
                       std::vector<eos::fusex::container> & contv
                       )
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("request='%s'", requestURL.c_str());

  // the MD get operation is implemented via a stream: open/read/close 
  XrdCl::File file;
  XrdCl::XRootDStatus status = file.Open (requestURL.c_str (),
                                          XrdCl::OpenFlags::Flags::Read);

  if (!status.IsOK ())
  {
    eos_static_err ("error=status is NOT ok : %s", status.ToString ().c_str ());

    if (status.code == XrdCl::errNotFound )
      return ENOENT;

    errno = status.code == XrdCl::errAuthFailed ? EPERM : EFAULT;
    return errno;
  }

  if (status.errNo)
  {
    eos_static_err ("error=status is ok : errno=%d", errno);
    errno = status.errNo;
    return errno;
  }

  // Start to read
  off_t offset = 0;

  const int kPAGE=512 * 1024;

  std::string response;
  std::vector<char> rbuff;
  rbuff.reserve(kPAGE);
  uint32_t bytesread = 0;
  do
  {

    status = file.Read(offset, kPAGE, (char*) & rbuff[0], bytesread);

    if (status.IsOK())
    {
      offset += bytesread;
      response.append(&rbuff[0], bytesread);
      eos_static_debug("+response=%s size=%u rsize=%u",
                       response.c_str(),
                       response.size(),
                       rbuff.size());
    }
    else
    {
      // failure
      bytesread = 0;
    }
    eos_static_debug("rbytes=%lu offset=%llu", bytesread, offset);
  }
  while (bytesread);

  eos_static_debug("response-size=%u response=%s",
                   response.size(), response.c_str());

  //eos_static_debug("response-dump=%s", eos::common::StringConversion::string_to_hex(response).c_str());

  offset=0;

  eos::fusex::container cont;

  do
  {
    cont.Clear();

    if ( (response.size() - offset) > 10)
    {
      std::string slen=response.substr(1 + offset, 8);
      size_t len = strtoll(slen.c_str(), 0, 16);

      eos_static_debug("len=%llu offset=%llu", len, offset);
      if (!len)
      {
        eos_static_debug("response had illegal length");
        return EINVAL;
      }

      std::string item;
      item.assign(response.c_str() + offset + 10, len);
      offset+=(10 + len);
      if (cont.ParseFromString(item))
      {
        eos_static_debug("response parsing OK");

        if ( (cont.type() != cont.MD) &&
            (cont.type() != cont.MDMAP)&&
            (cont.type() != cont.CAP))
        {
          eos_static_debug("wrong response type");
          return EINVAL;
        }
        contv.push_back(cont);
        eos_static_debug("parsed %ld/%ld", offset, response.size());
        if ( offset == (off_t) response.size())
          break;
      }
      else
      {
        eos_static_debug("response parsing FAILED");
        return EIO;
      }
    }
    else
    {
      eos_static_err("fatal protocol parsing error");
      return EINVAL;
    };
  }
  while (1);

  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::putMD(eos::fusex::md* md, std::string authid, XrdSysMutex * locker)
/* -------------------------------------------------------------------------- */
{
  XrdCl::URL url ("root://" + hostport);
  url.SetPath("/dummy");

  // temporary add the authid to be used for that request
  md->set_authid(authid);
  md->set_clientuuid(clientuuid);

  std::string mdstream;
  eos_static_info("proto-serialize");
  if (!md->SerializeToString(&mdstream))
  {
    md->clear_authid();
    md->clear_clientuuid();
    md->clear_implied_authid();
    eos_static_err("fatal serialization error");
    return EFAULT;
  }

  eos_static_debug("MD:\n%s", EosFuse::Instance().mds.dump_md(*md).c_str());

  md->clear_authid();
  md->clear_clientuuid();
  md->clear_implied_authid();

  locker->UnLock();

  eos_static_info("proto-serialize unlock");
  XrdCl::FileSystem fs(url);
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;

  std::string prefix="/?fusex:";
  arg.Append(prefix.c_str(), prefix.length());

  arg.Append(mdstream.c_str(), mdstream.length());
  eos_static_debug("query: url=%s path=%s length=%d", url.GetURL().c_str(), prefix.c_str(), mdstream.length());

  SyncResponseHandler handler;
  fs.Query (XrdCl::QueryCode::OpaqueFile, arg, &handler);
  XrdCl::XRootDStatus status = handler.Sync(response);

  eos_static_info("sync-response");

  if (status.IsOK ())
  {
    eos_static_debug("response=%s response-size=%d", response->GetBuffer(), response->GetSize());
    if (response && response->GetBuffer())
    {
      std::string responseprefix;

      if (response->GetSize() > 6)
      {
        responseprefix.assign(response->GetBuffer(), 6);
        // retrieve response
      }
      else
      {
        eos_static_err("protocol error - to short response received");
        locker->Lock();
        return EIO;
      }

      if (responseprefix != "Fusex:")
      {
        eos_static_err("protocol error - fusex: prefix missing in response");
        locker->Lock();
        return EIO;
      }
      std::string sresponse;
      std::string b64response;
      b64response.assign(response->GetBuffer() + 6, response->GetSize() - 6);
      eos::common::SymKey::DeBase64(b64response, sresponse);


      eos::fusex::response resp;
      if (!resp.ParseFromString(sresponse) || (resp.type() != resp.ACK))
      {
        eos_static_err("parsing error/wrong response type received");
        locker->Lock();
        return EIO;
      }
      if (resp.ack_().code() == resp.ack_().OK)
      {
        eos_static_info("relock do");
        locker->Lock();
        md->set_md_ino(resp.ack_().md_ino());
        eos_static_debug("directory inode %lx => %lx/%lx tid=%lx error=%s", md->id(), md->md_ino(),
                         resp.ack_().md_ino(), resp.ack_().transactionid(),
                         resp.ack_().err_msg().c_str());
        eos_static_info("relock done");
        return 0;
      }
      eos_static_err("failed query command for ino=%lx", md->id());
      locker->Lock();
      return EIO;
    }
    else
    {
      eos_static_err("no response retrieved response=%lu response-buffer=%lu",
                     response, response ? response->GetBuffer() : 0);
      locker->Lock();
      return EIO;
    }
    locker->Lock();
    return 0;
  }
  else
  {
    eos_static_err("query resulted in error url=%s", url.GetURL().c_str());
    locker->Lock();
    if ( status.code == XrdCl::errErrorResponse )
      return mapErrCode(status.errNo);
    else
      return EIO;
  }
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::doLock(fuse_req_t req,
                eos::fusex::md& md,
                XrdSysMutex * locker)
/* -------------------------------------------------------------------------- */
{
  XrdCl::URL url ("root://" + hostport);
  url.SetPath("/dummy");


  md.set_clientuuid(clientuuid);

  std::string mdstream;
  eos_static_info("proto-serialize");
  if (!md.SerializeToString(&mdstream))
  {
    md.clear_clientuuid();
    md.clear_flock();
    eos_static_err("fatal serialization error");
    return EFAULT;
  }

  md.clear_clientuuid();
  md.clear_flock();

  locker->UnLock();

  eos_static_info("proto-serialize unlock");
  XrdCl::FileSystem fs(url);
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;

  std::string prefix="/?fusex:";
  arg.Append(prefix.c_str(), prefix.length());

  arg.Append(mdstream.c_str(), mdstream.length());
  eos_static_debug("query: url=%s path=%s length=%d", url.GetURL().c_str(), prefix.c_str(), mdstream.length());

  SyncResponseHandler handler;
  fs.Query (XrdCl::QueryCode::OpaqueFile, arg, &handler);
  XrdCl::XRootDStatus status = handler.Sync(response);

  eos_static_info("sync-response");

  if (status.IsOK ())
  {
    eos_static_debug("response=%s response-size=%d", response->GetBuffer(), response->GetSize());
    if (response && response->GetBuffer())
    {
      std::string responseprefix;

      if (response->GetSize() > 6)
      {
        responseprefix.assign(response->GetBuffer(), 6);
        // retrieve response
      }
      else
      {
        eos_static_err("protocol error - to short response received");
        locker->Lock();
        return EIO;
      }

      if (responseprefix != "Fusex:")
      {
        eos_static_err("protocol error - fusex: prefix missing in response");
        locker->Lock();
        return EIO;
      }
      std::string sresponse;
      std::string b64response;
      b64response.assign(response->GetBuffer() + 6, response->GetSize() - 6);
      eos::common::SymKey::DeBase64(b64response, sresponse);


      eos::fusex::response resp;
      if (!resp.ParseFromString(sresponse) || (resp.type() != resp.LOCK))
      {
        eos_static_err("parsing error/wrong response type received");
        locker->Lock();
        return EIO;
      }
      if (resp.ack_().code() == resp.ack_().OK)
      {
        eos_static_info("relock do");
        locker->Lock();
        (*(md.mutable_flock())) = (resp.lock_());
        eos_static_debug("directory inode %lx => %lx/%lx tid=%lx error=%s", md.id(), md.md_ino(),
                         resp.ack_().md_ino(), resp.ack_().transactionid(),
                         resp.ack_().err_msg().c_str());
        eos_static_info("relock done");
        return 0;
      }
      eos_static_err("failed query command for ino=%lx", md.id());
      locker->Lock();
      return EIO;
    }
    else
    {
      eos_static_err("no response retrieved response=%lu response-buffer=%lu",
                     response, response ? response->GetBuffer() : 0);
      locker->Lock();
      return EIO;
    }
    locker->Lock();
    return 0;
  }
  else
  {
    eos_static_err("query resulted in error url=%s", url.GetURL().c_str());
  }
  locker->Lock();
  return EIO;
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
backend::getURL(fuse_req_t req, const std::string & path, std::string op, std::string authid)
/* -------------------------------------------------------------------------- */
{
  XrdCl::URL url ("root://" + hostport);
  url.SetPath("/proc/user/");

  XrdCl::URL::ParamsMap query;
  query["mgm.cmd"] = "fuseX";
  query["mgm.clock"] = "0";
  query["mgm.path"] = eos::common::StringConversion::curl_escaped(mount + path);
  query["mgm.op"] = op;
  query["mgm.uuid"] = clientuuid;
  query["mgm.cid"] = cap::capx::getclientid(req);

  if (authid.length())
    query["mgm.authid"]= authid;

  url.SetParams(query);
  return url.GetURL();
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
backend::getURL(fuse_req_t req, uint64_t inode, const std::string& name, std::string op, std::string authid)
{
  XrdCl::URL url ("root://" + hostport);
  url.SetPath("/proc/user/");

  XrdCl::URL::ParamsMap query;
  query["mgm.cmd"] = "fuseX";
  query["mgm.clock"] = "0";
  query["mgm.child"] = name;
  char hexinode[32];
  snprintf(hexinode, sizeof (hexinode), "%08lx", (unsigned long) inode);
  query["mgm.inode"]=
          hexinode;
  query["mgm.op"] = op;
  query["mgm.uuid"] = clientuuid;
  if (authid.length())
    query["mgm.authid"]= authid;
  query["mgm.cid"] = cap::capx::getclientid(req);
  url.SetParams(query);
  return url.GetURL();
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
backend::getURL(fuse_req_t req, uint64_t inode, uint64_t clock, std::string op, std::string authid)
/* -------------------------------------------------------------------------- */
{
  XrdCl::URL url ("root://" + hostport);
  url.SetPath("/proc/user/");
  XrdCl::URL::ParamsMap query;
  std::string sclock;
  query["mgm.cmd"] = "fuseX";
  query["mgm.clock"]=
          eos::common::StringConversion::GetSizeString(sclock,
                                                       (unsigned long long) clock);
  char hexinode[32];
  snprintf(hexinode, sizeof (hexinode), "%08lx", (unsigned long) inode);
  query["mgm.inode"]=
          hexinode;
  query["mgm.op"]=op;
  query["mgm.uuid"] = clientuuid;
  if (authid.length())
    query["mgm.authid"]= authid;
  query["mgm.cid"] = cap::capx::getclientid(req);
  url.SetParams(query);
  return url.GetURL();
}
