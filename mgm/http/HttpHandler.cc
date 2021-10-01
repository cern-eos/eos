// ----------------------------------------------------------------------
// File: HttpHandler.cc
// Author: Justin Lewis Salmon - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

#include "mgm/http/HttpServer.hh"
#include "mgm/http/HttpHandler.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Stat.hh"
#include "common/Timing.hh"
#include "common/ErrnoToString.hh"
#include "common/http/PlainHttpResponse.hh"
#include "common/http/OwnCloud.hh"
#include "namespace/utils/Mode.hh"
#include "mgm/http/rest-api/handler/tape/TapeRestHandler.hh"

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
bool
HttpHandler::Matches(const std::string& meth, HeaderMap& headers)
{
  int method = ParseMethodString(meth);

  if (method == GET || method == HEAD || method == POST ||
      method == PUT || method == DELETE || method == TRACE ||
      method == OPTIONS || method == CONNECT || method == PATCH) {
    eos_static_debug("Matched HTTP protocol for request");
    return true;
  } else {
    return false;
  }
}

/*----------------------------------------------------------------------------*/
void
HttpHandler::HandleRequest(eos::common::HttpRequest* request)
{
  eos_static_debug("handling http request");
  eos::common::HttpResponse* response = 0;
  eos::mgm::rest::TapeRestHandler & tapeRestHandler = gOFS->mHttpd->mTapeRestHandler;
  if(tapeRestHandler.isRestRequest(request->GetUrl())) {
    response = tapeRestHandler.handleRequest(request,mVirtualIdentity);
  } else {
    request->AddEosApp();

    for (auto it = request->GetHeaders().begin();
         it != request->GetHeaders().end(); ++it) {
      eos_static_info("header:%s => %s", it->first.c_str(), it->second.c_str());
    }

    int meth = ParseMethodString(request->GetMethod());
    {
      // call the routing module before doing anything with http
      int port;
      std::string host;
      int stall_timeout = 0;

      if (gOFS->ShouldRoute(
              __FUNCTION__, 0, *mVirtualIdentity, request->GetUrl().c_str(),
              request->GetQuery().c_str(), host, port, stall_timeout)) {
        response = HttpServer::HttpRedirect(request->GetUrl().c_str(),
                                            host.c_str(), port, false);
        mHttpResponse = response;
        return;
      }
    }

    switch (meth) {
    case GET:
      gOFS->MgmStats.Add("Http-GET", mVirtualIdentity->uid,
                         mVirtualIdentity->gid, 1);
      response = Get(request);
      break;

    case HEAD:
      gOFS->MgmStats.Add("Http-HEAD", mVirtualIdentity->uid,
                         mVirtualIdentity->gid, 1);
      response = Head(request);
      response->SetBody("");
      break;

    case POST:
      gOFS->MgmStats.Add("Http-POST", mVirtualIdentity->uid,
                         mVirtualIdentity->gid, 1);
      response = Post(request);
      break;

    case PUT:
      gOFS->MgmStats.Add("Http-PUT", mVirtualIdentity->uid,
                         mVirtualIdentity->gid, 1);
      response = Put(request);
      break;

    case DELETE:
      gOFS->MgmStats.Add("Http-DELETE", mVirtualIdentity->uid,
                         mVirtualIdentity->gid, 1);
      response = Delete(request);
      break;

    case TRACE:
      gOFS->MgmStats.Add("Http-TRACE", mVirtualIdentity->uid,
                         mVirtualIdentity->gid, 1);
      response = Trace(request);
      break;

    case OPTIONS:
      gOFS->MgmStats.Add("Http-OPTIONS", mVirtualIdentity->uid,
                         mVirtualIdentity->gid, 1);
      response = Options(request);
      break;

    case CONNECT:
      gOFS->MgmStats.Add("Http-CONNECT", mVirtualIdentity->uid,
                         mVirtualIdentity->gid, 1);
      response = Connect(request);
      break;

    case PATCH:
      gOFS->MgmStats.Add("Http-PATCH", mVirtualIdentity->uid,
                         mVirtualIdentity->gid, 1);
      response = Patch(request);
      break;

    default:
      response = new eos::common::PlainHttpResponse();
      response->SetResponseCode(eos::common::HttpResponse::BAD_REQUEST);
      response->SetBody("No such method");
    }
  }
  mHttpResponse = response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
HttpHandler::Get(eos::common::HttpRequest* request, bool isHEAD)
{
  using eos::common::ErrnoToString;
  XrdSecEntity client(mVirtualIdentity->prot.c_str());
  client.name = const_cast<char*>(mVirtualIdentity->uid_string.c_str());
  client.host = const_cast<char*>(mVirtualIdentity->host.c_str());
  client.tident = const_cast<char*>(mVirtualIdentity->tident.c_str());
  // Classify path to split between directory or file objects
  bool isfile = true;
  std::string url = request->GetUrl();
  std::string query = request->GetQuery();
  eos::common::HttpResponse* response = 0;
  struct stat buf;
  XrdOucString spath = request->GetUrl().c_str();

  // redirect '/' to '/eos/<instance>/'
  if (spath == "/") {
    XrdOucString instance = gOFS->MgmOfsInstanceName;

    if (instance.beginswith("eos")) {
      instance.replace("eos", "");
    }

    response = HttpServer::HttpRedirect(url + "eos/" + instance.c_str(),
                                        gOFS->HostName,
                                        gOFS->mHttpdPort, false);
    return response;
  }

  std::string etag = "undef";
  eos::common::OwnCloud::OwnCloudRemapping(spath, request);
  eos::common::OwnCloud::ReplaceRemotePhp(spath);

  if (!spath.beginswith("/proc/")) {
    XrdOucErrInfo error(mVirtualIdentity->tident.c_str());
    {
      // check if this is a symlink
      XrdOucString link;

      if ((!gOFS->_readlink(url.c_str(), error, *mVirtualIdentity, link)) &&
          (link != "") && (link.beginswith("http://") ||
                           link.beginswith("https://"))) {
        if (gOFS->access(url.c_str(), R_OK, error, &client, query.c_str())) {
          // no permission or entry doesn't exist
          eos_static_info("method=GET error=%i path=%s", error.getErrInfo(),
                          url.c_str());
          response = HttpServer::HttpError(ErrnoToString(error.getErrInfo()).c_str(),
                                           (error.getErrInfo() == ENOENT) ?
                                           response->NOT_FOUND :
                                           response->FORBIDDEN);
          return response;
        }

        // create an external redirect
        response = new eos::common::PlainHttpResponse();
        response->SetResponseCode(
          eos::common::HttpResponse::ResponseCodes::TEMPORARY_REDIRECT);
        response->AddHeader("Location", link.c_str());
        response->AddHeader("X-Accel-Redirect", link.c_str());
        response->AddHeader("X-Sendfile", link.c_str());
        return response;
      } else {
        if (gOFS->access(url.c_str(), R_OK, error, &client, query.c_str())) {
          // no permission or entry doesn't exist
          eos_static_info("method=GET error=%i path=%s", error.getErrInfo(),
                          url.c_str());
          response = HttpServer::HttpError(ErrnoToString(error.getErrInfo()).c_str(),
                                           (error.getErrInfo() == ENOENT) ?
                                           response->NOT_FOUND :
                                           response->FORBIDDEN);
          return response;
        }
      }
    }

    if (gOFS->stat(url.c_str(), &buf, error, &etag, &client, query.c_str())) {
      eos_static_info("method=GET error=ENOENT path=%s",
                      url.c_str());
      response = HttpServer::HttpError("No such file or directory",
                                       response->NOT_FOUND);
      return response;
    }

    if (gOFS->stat(url.c_str(), &buf, error, &etag, &client, query.c_str())) {
      eos_static_info("method=GET error=ENOENT path=%s",
                      url.c_str());
      response = HttpServer::HttpError("No such file or directory",
                                       response->NOT_FOUND);
      return response;
    }

    if (request->GetHeaders().count("if-match") &&
        (etag != request->GetHeaders()["if-match"])) {
      // ETag mismatch
      eos_static_info("method=GET error=precondition-failed path=%s etag=%s cond=match r-etag=%s",
                      url.c_str(), etag.c_str(), request->GetHeaders()["If-Match"].c_str());
      response = HttpServer::HttpError("ETag precondition failed",
                                       response->PRECONDITION_FAILED);
      return response;
    }

    if (request->GetHeaders().count("if-non-match") &&
        (etag == request->GetHeaders()["if-non-match"])) {
      // ETag match
      eos_static_info("method=GET error=precondition-failed path=%s etag=%s cond=not-match r-etag=%s",
                      url.c_str(), etag.c_str(), request->GetHeaders()["if-not-match"].c_str());
      response = HttpServer::HttpError("ETag is not modified",
                                       response->NOT_MODIFIED);
      return response;
    }

    // find out if it is a file or directory
    if (S_ISDIR(buf.st_mode)) {
      isfile = false;

      if (isHEAD) {
        // HEAD requests for dirs just act like 'exists'
        eos_static_info("cmd=GET(HEAD) size=%llu path=%s type=dir",
                        buf.st_size,
                        url.c_str());
        response = new eos::common::PlainHttpResponse();
        response->SetBody("");
        response->AddHeader("ETag", etag);
        response->AddHeader("Last-Modified",
                            eos::common::Timing::utctime(buf.st_mtime));
        return response;
      }
    } else {
      isfile = true;

      if (isHEAD) {
        std::string basename = url.substr(url.rfind('/') + 1);
        eos_static_info("cmd=GET(HEAD) size=%llu path=%s type=file",
                        buf.st_size,
                        url.c_str());
        // HEAD requests on files can return from the MGM without redirection
        response = HttpServer::HttpHead(buf.st_size, basename);
        response->AddHeader("ETag", etag);
        response->AddHeader("Last-Modified",
                            eos::common::Timing::utctime(buf.st_mtime));

        if (request->GetHeaders().count("want-digest")) {
          std::string type = request->GetHeaders()["want-digest"];
          type = LC_STRING(type);
          XrdOucString digest = "";
          eos_static_debug("method=HEAD, path=%s, checksum requested=%s",
                           url.c_str(), type.c_str());
          //check if there is a checksum type and checksum
          std::string xstype;
          std::string xs;

          if (!gOFS->_getchecksum(url.c_str(),
                                  error,
                                  &xstype,
                                  &xs,
                                  &client,
                                  query.c_str())) {
            //check if the type match what requested
            if (xstype == type) {
              eos_static_debug("method=HEAD, path=%s, checksum requested=%s, checksum available=%s",
                               url.c_str(), type.c_str(), xstype.c_str());
              digest += xstype.c_str();
              digest += "=";
              digest += xs.c_str();
              response->AddHeader("Digest", digest.c_str());
            }
          }
        }

        return response;
      }
    }
  }

  if (!isfile) {
    eos_static_info("method=GET dir=%s",
                    url.c_str());
    errno = 0;
    {
      // Check if there is an index attribute
      XrdOucString index;
      XrdOucErrInfo error(mVirtualIdentity->tident.c_str());

      if (!gOFS->_attr_get(url.c_str(), error, *mVirtualIdentity, query.c_str(),
                           "sys.http.index", index)) {
        if (gOFS->access(url.c_str(), R_OK, error, &client, query.c_str())) {
          // no permission or entry doesn't exist
          eos_static_info("method=GET error=%i path=%s", error.getErrInfo(),
                          url.c_str());
          response = HttpServer::HttpError(ErrnoToString(error.getErrInfo()).c_str(),
                                           (error.getErrInfo() == ENOENT) ?
                                           response->NOT_FOUND :
                                           response->FORBIDDEN);
          return response;
        }

        // create an external redirect
        response = new eos::common::PlainHttpResponse();
        response->SetResponseCode(
          eos::common::HttpResponse::ResponseCodes::TEMPORARY_REDIRECT);
        response->AddHeader("Location", index.c_str());
        response->AddHeader("X-Accel-Redirect", index.c_str());
        response->AddHeader("X-Sendfile", index.c_str());
        return response;
      }
    }
    XrdMgmOfsDirectory directory;
    int listrc = directory.open(request->GetUrl().c_str(), *mVirtualIdentity,
                                query.c_str());

    if (!listrc) {
      std::string result;
      const char* val;
      // -----------------------------------------------------------------------
      // HTML Header, Start of Body and Scripts
      // -----------------------------------------------------------------------
#include "HttpHandler.js.html"
      // -----------------------------------------------------------------------
      // show [ name@instance ]
      // -----------------------------------------------------------------------
      result += R"literal(
        <h2 ><font color="#2C3539">
        )literal";
      result += "<span id=\"clientid\">";
      result += client.name;
      result += "</span>";
      result += "@";
      result += gOFS-> MgmOfsInstanceName.c_str();
      result += " ]:</font> ";
      result += url.c_str();
      result += "</h2>";
      result += R"literal(
        <div id="newlisting" style="display:none"></div>
        <div id="listing">

        <table id="dirlist" border:1px solid #aaa !important;>
        <tr>
          <th style="min-width:150px">Path</th> <th style="min-width:20px"></th>  <th style="min-width:150px">Size</th>
          <th style="min-width:150px">Created</th> <th style="min_width:100">Mode</th>
          <th style="min-width:60px">owner</th> <th style="min-width:60px">group</th>
          <th style="min-width:150px">Acl</th>
        </tr>
        )literal";

      // -----------------------------------------------------------------------
      // fill the directory table
      // -----------------------------------------------------------------------
      while ((val = directory.nextEntry())) {
        XrdOucString entryname = val;
        XrdOucString linkname = "";
        bool isFile = true;

        if ((spath == "/") &&
            ((entryname == ".") ||
             (entryname == ".."))) {
          continue;
        }

        result += "       <tr>\n";
        result += "       <td style=\"padding-right: 5px\">";
        result += "       <a title=\"\" class=\"hasmenu\" href=\"";

        if (entryname == ".") {
          linkname = spath.c_str();
        } else {
          if (entryname == "..") {
            if (spath != "/") {
              eos::common::Path cPath(spath.c_str());
              linkname = cPath.GetParentPath();
            } else {
              linkname = "/";
            }
          } else {
            linkname = spath.c_str();

            if (!spath.endswith("/") && (spath != "/")) {
              linkname += "/";
            }

            linkname += entryname.c_str();
          }
        }

        struct stat buf;

        buf.st_mode = 0;

        XrdOucErrInfo error(mVirtualIdentity->tident.c_str());

        XrdOucString sizestring;

        XrdOucString entrypath = spath.c_str();

        entrypath += "/";

        entrypath += entryname.c_str();

        isFile = true;

        // find out if it is a file or directory
        if (!gOFS->stat(entrypath.c_str(), &buf, error, &client, "")) {
          if (S_ISDIR(buf.st_mode)) {
            isFile = false;
          }
        }

        if (!isFile) {
          entryname += "/";
        }

        result += linkname.c_str();
        result += "\">";
        result += "<font size=\"2\">";
        result += entryname.c_str();
        result += "</font>";
        result += "       </a>\n";
        result += "<div fullpath=\"";
        result += entryname.c_str();
        result += "\"></div></td>\n";
        // ---------------------------------------------------------------------
        // share link icon
        // ---------------------------------------------------------------------
        result += "       <td> \n";

        if (isFile) {
          result += R"literal(
       <a href="JavaScript:newPopup('/proc/user/?mgm.cmd=file&mgm.subcmd=share&mgm.option=s&mgm.file.expires=0&mgm.format=http&mgm.path=)literal";
          result += linkname.c_str();
          result += R"literal(');">)literal";
          result += R"literal(<img alt="" src="data:image/gif;base64,R0lGODlhEAANAJEAAAJ6xv///wAAAAAAACH5BAkAAAEALAAAAAAQAA0AAAg0AAMIHEiwoMGDCBMqFAigIYCFDBsadPgwAMWJBB1axBix4kGPEhN6HDgyI8eTJBFSvEgwIAA7" />
            </a>
            )literal";
        }

        result += "       </td>\n";
        // ---------------------------------------------------------------------
        // file size
        // ---------------------------------------------------------------------
        result += "       <td style=\"padding-right: 5px\">";
        result += "<font size=\"2\">";

        if (S_ISDIR(buf.st_mode)) {
          result += "";
        } else {
          result += eos::common::StringConversion::GetReadableSizeString(sizestring,
                    buf.st_size, "Bytes");
        }

        result += "</font>";
        result += "</td>\n";
        char uidlimit[16];
        char gidlimit[16];
        // try to translate with password database
        int terrc = 0;
        std::string username;
        username = eos::common::Mapping::UidToUserName(buf.st_uid, terrc);

        if (!terrc) {
          snprintf(uidlimit, 16, "%-12s", username.c_str());
        } else {
          snprintf(uidlimit, 16, "%d", buf.st_uid);
        }

        // try to translate with password database
        std::string groupname;
        groupname = eos::common::Mapping::GidToGroupName(buf.st_gid, terrc);

        if (!terrc) {
          snprintf(gidlimit, 16, "%-12s", groupname.c_str());
        } else {
          snprintf(gidlimit, 16, "%d", buf.st_gid);
        }

        char t_creat[36];
        char modestr[11];
        eos::modeToBuffer(buf.st_mode, modestr);
        {
          struct tm* t_tm;
          struct tm t_tm_local;
          t_tm = localtime_r(&buf.st_ctime, &t_tm_local);
          strftime(t_creat, 36, "%b %d %Y %H:%M", t_tm);
        }
        // ---------------------------------------------------------------------
        // show creation date
        // ---------------------------------------------------------------------
        result += "       <td style=\"padding-right: 5px\"><font size=\"2\" face=\"Courier New\" color=\"darkgrey\">";
        result += t_creat;
        result += "</font></td>\n";
        // ---------------------------------------------------------------------
        // show permissions
        // ---------------------------------------------------------------------
        result += "       <td style=\"padding-right: 5px\"><font size=\"2\" face=\"Courier New\" color=\"darkgrey\">";
        result += modestr;
        result += "</font></td>\n";
        // ---------------------------------------------------------------------
        // show user name
        // ---------------------------------------------------------------------
        result += "       <td style=\"padding-right: 5px\"><font color=\"darkgrey\">";
        result += uidlimit;
        result += "</font></td>\n";
        // ---------------------------------------------------------------------
        // show group name
        // ---------------------------------------------------------------------
        result += "       <td style=\"padding-right: 5px\"><font color=\"grey\">\n";
        result += gidlimit;
        result += "</font></td>\n";
        // ---------------------------------------------------------------------
        // show acl's if there
        // ---------------------------------------------------------------------
        XrdOucString acl;
        result += "       <td style=\"padding-right: 5px\"><font color=\"#81DAF5\">";

        if (S_ISDIR(buf.st_mode)) {
          if (!gOFS->attr_get(linkname.c_str(),
                              error,
                              &client,
                              "",
                              "sys.acl",
                              acl)) {
            result += acl.c_str();
          }
        }

        result += "</font></td>\n";
        result += "       </tr>\n";
      }

      // -----------------------------------------------------------------------
      // terminate table, body and html
      // ---------------------------------------------------------------------
      result += "       </table></div>\n";
      result += "       </body>\n";
      result += "       </html>\n";
      response = new eos::common::PlainHttpResponse();
      response->SetBody(result);
      response->AddHeader("ETag", etag);
      response->AddHeader("Last-Modified",
                          eos::common::Timing::utctime(buf.st_mtime));
    } else {
      response = HttpServer::HttpError("Unable to open directory",
                                       errno);
    }

    return response;
  } else {
    eos_static_info("method=GET file=%s tident=%s query=%s",
                    url.c_str(), client.tident, query.c_str());
    XrdSfsFile* file = gOFS->newFile((char*) mVirtualIdentity->tident.c_str());

    if (file) {
      XrdSfsFileOpenMode open_mode = 0;
      mode_t create_mode = 0;
      int rc = file->open(url.c_str(), open_mode, create_mode, &client,
                          query.c_str());

      // TODO (apeters): review this part - dead code open_mode = 0
      if ((rc != SFS_REDIRECT) && open_mode) {
        // retry as a file creation
        open_mode |= SFS_O_CREAT;
        rc = file->open(url.c_str(), open_mode, create_mode, &client,
                        query.c_str());
      }

      if (rc != SFS_OK) {
        if (rc == SFS_REDIRECT) {
          response = HttpServer::HttpRedirect(request->GetUrl(),
                                              file->error.getErrText(),
                                              file->error.getErrInfo(), false);
        } else if (rc == SFS_ERROR) {
          if (file->error.getErrInfo() == ENODEV) {
            response = new eos::common::PlainHttpResponse();
          } else {
            response = HttpServer::HttpError(file->error.getErrText(),
                                             file->error.getErrInfo());
          }
        } else if (rc == SFS_DATA) {
          response = HttpServer::HttpData(file->error.getErrText(),
                                          file->error.getErrInfo());
        } else if (rc == SFS_STALL) {
          response = HttpServer::HttpStall(file->error.getErrText(),
                                           file->error.getErrInfo());
        } else {
          response = HttpServer::HttpError("Unexpected result from file open",
                                           EOPNOTSUPP);
        }

        response->AddHeader("ETag", etag);
      } else {
        char buffer[65536];
        offset_t offset = 0;
        std::string result;

        do {
          size_t nread = file->read(offset, buffer, sizeof(buffer));

          if (nread > 0) {
            result.append(buffer, nread);
          }

          if (nread != sizeof(buffer)) {
            break;
          }

          offset += nread;
        } while (true);

        file->close();
        response = new eos::common::PlainHttpResponse();
        XrdOucErrInfo error(mVirtualIdentity->tident.c_str());

        if (!gOFS->stat(url.c_str(), &buf, error, &etag, &client, "")) {
          response->AddHeader("ETag", etag);
          response->AddHeader("Last-Modified",
                              eos::common::Timing::utctime(buf.st_mtime));
        }

        response->SetBody(result);
      }

      // clean up the object
      delete file;
    }
  }

  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
HttpHandler::Head(eos::common::HttpRequest* request)
{
  eos::common::HttpResponse* response = Get(request, true);
  response->mUseFileReaderCallback = false;
  response->SetBody("");
  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
HttpHandler::Post(eos::common::HttpRequest* request)
{
  using namespace eos::common;
  std::string url = request->GetUrl();
  eos_static_info("method=POST error=NOTIMPLEMENTED path=%s",
                  url.c_str());
  HttpResponse* response = new PlainHttpResponse();
  response->SetResponseCode(HttpResponse::ResponseCodes::NOT_IMPLEMENTED);
  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
HttpHandler::Put(eos::common::HttpRequest* request)
{
  XrdSecEntity client(mVirtualIdentity->prot.c_str());
  client.name = const_cast<char*>(mVirtualIdentity->name.c_str());
  client.host = const_cast<char*>(mVirtualIdentity->host.c_str());
  client.tident = const_cast<char*>(mVirtualIdentity->tident.c_str());
  std::string url = request->GetUrl();
  eos_static_info("method=PUT path=%s",
                  url.c_str());
  // Classify path to split between directory or file objects
  bool isfile = true;
  bool isOcChunked = false;
  bool isPartialPut = false;
  std::map<std::string, std::string> ocHeader;
  eos::common::HttpResponse* response = 0;
  XrdOucString spath = request->GetUrl().c_str();

  if (!spath.beginswith("/proc/")) {
    if (spath.endswith("/")) {
      isfile = false;
    }
  }

  if (eos::common::OwnCloud::isChunkUpload(request)) {
    isOcChunked = true;
    // we have to rewrite the path and add some additional header describing
    // the chunking which was stored in the name
    url = eos::common::OwnCloud::prepareChunkUpload(request, &response, ocHeader);

    if (response) {
      return response;
    }
  }

  if (request->GetHeaders().count("x-upload-range")) {
    // this is a partial put, we have to remove the truncate flag
    isPartialPut = true;
  }

  std::string etag;
  {
    // retrieve the ETag if existing ..
    struct stat buf;
    XrdOucErrInfo error(mVirtualIdentity->tident.c_str());

    if (gOFS->stat(url.c_str(), &buf, error, &etag, &client, "")) {
      etag = "undef";
    }
  }

  if ((etag != "undef") && (request->GetHeaders().count("if-match") &&
                            (etag != request->GetHeaders()["if-match"]))) {
    // ETag mismatch
    eos_static_info("method=PUT error=precondition-failed path=%s etag=%s cond=match r-etag=%s",
                    url.c_str(), etag.c_str(), request->GetHeaders()["if-match"].c_str());
    response = HttpServer::HttpError("ETag precondition failed",
                                     response->PRECONDITION_FAILED);
    return response;
  }

  if ((etag != "undef" && (request->GetHeaders().count("if-non-match") &&
                           (etag == request->GetHeaders()["if-non-match"])))) {
    // ETag match
    eos_static_info("method=PUT error=precondition-failed path=%s etag=%s cond=not-match r-etag=%s",
                    url.c_str(), etag.c_str(), request->GetHeaders()["if-not-match"].c_str());
    response = HttpServer::HttpError("ETag is not modified",
                                     response->NOT_MODIFIED);
    return response;
  }

  if (isfile) {
    XrdSfsFile* file = gOFS->newFile((char*) mVirtualIdentity->tident.c_str());

    if (file) {
      XrdSfsFileOpenMode open_mode = 0;
      mode_t create_mode = 0;

      // use the proper creation/open flags for PUT's
      if (!isPartialPut) {
        open_mode |= SFS_O_TRUNC;
      }

      open_mode |= SFS_O_RDWR;
      create_mode |= (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
      std::string query = request->GetQuery();

      if (request->GetHeaders().count("content-length")) {
        query += "&eos.bookingsize=";
        //or OC chunked uploads we book the full size
        const char* oclength = eos::common::OwnCloud::getContentSize(request);

        if (oclength) {
          query += oclength;
        } else {
          query += request->GetHeaders()["content-length"];
        }

        if (!isOcChunked && !isPartialPut) {
          query += "&eos.targetsize=";
          query += request->GetHeaders()["content-length"];
        }
      } else {
        query = "eos.bookingsize=0";
      }

      if (request->GetHeaders().count("x-oc-mtime")) {
        // there is an X-OC-Mtime header to force the mtime for that file
        query += "&eos.mtime=";
        query += request->GetHeaders()["x-oc-mtime"];
      }

      if (request->GetHeaders().count("x-upload-mtime")) {
        // there is an x-upload-mtime header to force the mtime for that file
        query += "&eos.mtime=";
        query += request->GetHeaders()["x-upload-mtime"];
      }

      if (isOcChunked) {
        // add the OC opaque information
        query += eos::common::OwnCloud::HeaderToQuery(ocHeader).c_str();
      }

      // -----------------------------------------------------------
      // OC clients are switched automatically to atomic upload mode
      // -----------------------------------------------------------
      if (request->GetHeaders().count("oc-total-length") || isOcChunked) {
        if (query.length()) {
          query += "&";
        }

        query += "eos.atomic=1";
      }

      if (isOcChunked) {
        if (etag != "undef") { // file exists already
          eos_static_info("removing truncation flag ");
          //open_mode ^= SFS_O_TRUNC;
        }
      }

      int rc = file->open(url.c_str(), open_mode, create_mode, &client,
                          query.c_str());

      if (rc != SFS_OK) {
        if ((rc != SFS_REDIRECT) && open_mode && (file->error.getErrInfo() == ENOENT)) {
          // retry as a file creation
          open_mode |= SFS_O_CREAT;
          open_mode |= SFS_O_TRUNC;
          rc = file->open(url.c_str(), open_mode, create_mode, &client,
                          query.c_str());
        }
      }

      if (rc != SFS_OK) {
        if (rc == SFS_REDIRECT) {
          std::string redirection_cgi = file->error.getErrText();

          if (file->error.getErrInfo() == 1094) {
            // MGM redirect
            response = HttpServer::HttpRedirect(request->GetUrl(),
                                                redirection_cgi,
                                                gOFS->mHttpdPort, false);
          } else {
            if (isOcChunked) {
              redirection_cgi += eos::common::OwnCloud::HeaderToQuery(ocHeader).c_str();
            }

            // FST redirect
            response = HttpServer::HttpRedirect(request->GetUrl(),
                                                redirection_cgi,
                                                file->error.getErrInfo(), false);
          }
        } else if (rc == SFS_ERROR) {
          if (file->error.getErrInfo() == ENOENT) {
            response = HttpServer::HttpError(file->error.getErrText(), 409);
          } else
            response = HttpServer::HttpError(file->error.getErrText(),
                                             file->error.getErrInfo());
        } else if (rc == SFS_DATA) {
          response = HttpServer::HttpData(file->error.getErrText(),
                                          file->error.getErrInfo());
        } else if (rc == SFS_STALL) {
          response = HttpServer::HttpStall(file->error.getErrText(),
                                           file->error.getErrInfo());
        } else {
          response = HttpServer::HttpError("Unexpected result from file open",
                                           EOPNOTSUPP);
        }
      } else {
        response = new eos::common::PlainHttpResponse();
        response->SetResponseCode(response->CREATED);
      }

      std::string rurl = file->error.getErrText();
      rurl.erase(0, rurl.find('?') + 1);
      XrdOucEnv env(rurl.c_str());
      char* etag = env.Get("mgm.etag");

      if (etag) {
        // add the ETag into the header
        response->AddHeader("ETag", etag);
      }

      // clean up the object
      delete file;
    }
  } else {
    // DIR requests
    response = HttpServer::HttpError("Not Implemented", EOPNOTSUPP);
  }

  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
HttpHandler::Delete(eos::common::HttpRequest* request)
{
  eos::common::HttpResponse* response = 0;
  XrdOucErrInfo error(mVirtualIdentity->tident.c_str());
  struct stat buf;
  ProcCommand cmd;
  std::string url = request->GetUrl();
  eos_static_info("method=DELETE path=%s", url.c_str());

  if (gOFS->_stat(request->GetUrl().c_str(), &buf, error,
                  *mVirtualIdentity, "")) {
    response = HttpServer::HttpError(error.getErrText(), response->NOT_FOUND);
    return response;
  }

  XrdOucString info = "mgm.cmd=rm&mgm.path=";
  info += request->GetUrl().c_str();

  if (S_ISDIR(buf.st_mode)) {
    info += "&mgm.option=r";
  }

  cmd.open("/proc/user", info.c_str(), *mVirtualIdentity, &error);
  cmd.close();
  int rc = cmd.GetRetc();

  if (rc != SFS_OK) {
    if (error.getErrInfo() == EPERM) {
      response = HttpServer::HttpError(error.getErrText(), response->FORBIDDEN);
    } else if (error.getErrInfo() == ENOENT) {
      response = HttpServer::HttpError(error.getErrText(), response->NOT_FOUND);
    } else {
      response = HttpServer::HttpError(error.getErrText(), error.getErrInfo());
    }
  } else {
    response = new eos::common::PlainHttpResponse();
    response->SetResponseCode(response->NO_CONTENT);
  }

  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
HttpHandler::Trace(eos::common::HttpRequest* request)
{
  using namespace eos::common;
  std::string url = request->GetUrl();
  eos_static_info("method=TRACE error=NOTIMPLEMENTED path=%s",
                  url.c_str());
  HttpResponse* response = new PlainHttpResponse();
  response->SetResponseCode(HttpResponse::ResponseCodes::NOT_IMPLEMENTED);
  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
HttpHandler::Options(eos::common::HttpRequest* request)
{
  eos::common::HttpResponse* response = new eos::common::PlainHttpResponse();
  response->AddHeader("DAV", "1,2");
  response->AddHeader("Allow", "OPTIONS,GET,HEAD,PUT,DELETE,TRACE,"\
                      "PROPFIND,PROPPATCH,MKCOL,COPY,MOVE,LOCK,UNLOCK");
  response->AddHeader("Content-Length", "0");
  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
HttpHandler::Connect(eos::common::HttpRequest* request)
{
  using namespace eos::common;
  std::string url = request->GetUrl();
  eos_static_info("method=CONNECT error=NOTIMPLEMENTED path=%s",
                  url.c_str());
  HttpResponse* response = new PlainHttpResponse();
  response->SetResponseCode(HttpResponse::ResponseCodes::NOT_IMPLEMENTED);
  return response;
}

/*----------------------------------------------------------------------------*/
eos::common::HttpResponse*
HttpHandler::Patch(eos::common::HttpRequest* request)
{
  using namespace eos::common;
  std::string url = request->GetUrl();
  eos_static_info("method=PATCH error=NOTIMPLEMENTED path=%s",
                  url.c_str());
  HttpResponse* response = new PlainHttpResponse();
  response->SetResponseCode(HttpResponse::ResponseCodes::NOT_IMPLEMENTED);
  return response;
}

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END
