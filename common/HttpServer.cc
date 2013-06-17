// ----------------------------------------------------------------------
// File: HttpServer.cc
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

/*----------------------------------------------------------------------------*/
#include "common/HttpServer.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <map>
#include <sstream>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

HttpServer* HttpServer::gHttp;

/*----------------------------------------------------------------------------*/
#define EOSCOMMON_HTTP_PAGE "<html><head><title>No such file or directory</title></head><body>No such file or directory</body></html>"

/*----------------------------------------------------------------------------*/
HttpServer::HttpServer (int port)
{
  //.............................................................................
  // Constructor
  //.............................................................................
  gHttp = this;
  mPort = port;
  mThreadId = 0;
  mRunning = false;
}

/*----------------------------------------------------------------------------*/
HttpServer::~HttpServer () {
  //.............................................................................
  // Destructor
  //.............................................................................
}

/*----------------------------------------------------------------------------*/
bool
HttpServer::Start ()
{
  //.............................................................................
  // Startup the HTTP server
  //.............................................................................
  if (!mRunning)
  {
    XrdSysThread::Run(&mThreadId, HttpServer::StaticHttp, static_cast<void *> (this), XRDSYSTHREAD_HOLD, "Httpd Thread");
    mRunning = true;
    return true;
  }
  else
  {
    return false;
  }

}

/*----------------------------------------------------------------------------*/
void*
HttpServer::StaticHttp (void* arg)
{
  //.............................................................................
  // Asynchronoous thread start function
  //.............................................................................
  return reinterpret_cast<HttpServer*> (arg)->Run();
}

/*----------------------------------------------------------------------------*/
void*
HttpServer::Run ()
{
#ifdef EOS_MICRO_HTTPD

  {
    mDaemon = MHD_start_daemon(MHD_USE_DEBUG | MHD_USE_SELECT_INTERNALLY,
                               mPort,
                               NULL,
                               NULL,
                               &HttpServer::StaticHandler,
                               (void*) EOSCOMMON_HTTP_PAGE,
                               MHD_OPTION_CONNECTION_MEMORY_LIMIT,
                               128*1024*1024 /* 128MB */,
                               MHD_OPTION_END
                               );
  }
  if (!mDaemon)
  {
    mRunning = false;
    eos_static_warning("msg=\"start of micro httpd failed [port=%d]\"", mPort);
    return (0);
  }
  else
  {
    mRunning = true;
  }

  eos_static_info("msg=\"start of micro httpd succeeded [port=%d]\"", mPort);

  fd_set rs;
  fd_set ws;
  fd_set es;
  int max;
  unsigned MHD_LONG_LONG mhd_timeout;
  struct timeval tv;

  while (1)
  {
    tv.tv_sec = 3600;
    tv.tv_usec = 0;

    max = 0;
    FD_ZERO(&rs);
    FD_ZERO(&ws);
    FD_ZERO(&es);

    if (MHD_YES != MHD_get_fdset(mDaemon, &rs, &ws, &es, &max))
      break; /* fatal internal error */

    if (MHD_get_timeout(mDaemon, &mhd_timeout) == MHD_YES)
    {
      if ((tv.tv_sec * 1000) < (long long) mhd_timeout)
      {
        tv.tv_sec = mhd_timeout / 1000;
        tv.tv_usec = (mhd_timeout - (tv.tv_sec * 1000)) * 1000;
      }
    }
    select(max + 1, &rs, &ws, &es, &tv);
    MHD_run(mDaemon);
  }
  MHD_stop_daemon(mDaemon);
#endif

  return (0);
}

#ifdef EOS_MICRO_HTTPD

/*----------------------------------------------------------------------------*/
int
HttpServer::StaticHandler (void *cls,
                           struct MHD_Connection *connection,
                           const char *url,
                           const char *method,
                           const char *version,
                           const char *upload_data,
                           size_t *upload_data_size, void **ptr)
{
  // The static handler function calls back the original http object
  if (gHttp)
  {
    return gHttp->Handler(cls,
                          connection,
                          url,
                          method,
                          version,
                          upload_data,
                          upload_data_size,
                          ptr);
  }
  else
  {
    return 0;
  }
}

/*----------------------------------------------------------------------------*/
//int
//HttpServer::Handler (void *cls,
//               struct MHD_Connection *connection,
//               const char *url,
//               const char *method,
//               const char *version,
//               const char *upload_data,
//               size_t *upload_data_size, void **ptr)
//{
//  static int aptr;
//  struct MHD_Response *response;
//
//  std::string query;
//  std::map<std::string, std::string> header;
//
//  // currently support only GET methods
//  if (0 != strcmp(method, MHD_HTTP_METHOD_GET))
//    return MHD_NO; /* unexpected method */
//
//  if (&aptr != *ptr)
//  {
//    /* do never respond on first call */
//    *ptr = &aptr;
//    return MHD_YES;
//  }
//
//  // get the query CGI
//  MHD_get_connection_values(connection, MHD_GET_ARGUMENT_KIND, &HttpServer::BuildQueryString,
//                            (void*) &query);
//
//  // get the header INFO
//  MHD_get_connection_values(connection, MHD_HEADER_KIND, &HttpServer::BuildHeaderMap,
//                            (void*) &header);
//
//  *ptr = NULL; /* reset when done */
//
//  eos_static_info("url=%s query=%s", url ? url : "", query.c_str() ? query.c_str() : "");
//
//  for (auto it = header.begin(); it != header.end(); it++)
//  {
//    eos_static_info("header:%s=%s", it->first.c_str(), it->second.c_str());
//  }
//
//  std::string result = "Welcome to EOS!";
//
//  response = MHD_create_response_from_buffer(result.length(),
//                                             (void *) result.c_str(),
//                                             MHD_RESPMEM_MUST_FREE);
//
//  int ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
//
//  return ret;
//}

/*----------------------------------------------------------------------------*/
int
HttpServer::BuildHeaderMap (void *cls,
                            enum MHD_ValueKind kind,
                            const char *key,
                            const char *value)
{
  // Call back function to return the header key-val map of an HTTP request
  std::map<std::string, std::string>* hMap = static_cast<std::map<std::string, std::string>*> (cls);
  if (key && value && hMap)
  {
    (*hMap)[key] = value;
  }
  return MHD_YES;
}

/*----------------------------------------------------------------------------*/
int
HttpServer::BuildQueryString (void *cls,
                              enum MHD_ValueKind kind,
                              const char *key,
                              const char *value)
{
  // Call back function to return the query string of an HTTP request
  std::string* qString = static_cast<std::string*> (cls);
  if (key && value && qString)
  {
    if (qString->length())
    {
      *qString += "&";
    }
    *qString += key;
    *qString += "=";
    *qString += value;
  }
  return MHD_YES;
}
#endif

/*----------------------------------------------------------------------------*/
std::string
HttpServer::HttpRedirect (int& response_code, std::map<std::string, std::string>& response_header, const char* host_cgi, int port, std::string& path, std::string& query, bool cookie)
{
  response_code = 307;
  // return an HTTP redirect
  std::string host = host_cgi;
  std::string cgi = "";
  size_t qpos;

  if ((qpos = host.find("?")) != std::string::npos)
  {
    cgi = host;
    cgi.erase(0, qpos + 1);
    host.erase(qpos);
  }

  std::string redirect;

  redirect = "http://";
  redirect += host;
  char sport[16];
  snprintf(sport, sizeof (sport) - 1, ":%d", port);
  redirect += sport;
  redirect += path.c_str();

  EncodeURI(cgi); // encode '+' '/' '='

  if (cookie)
  {
    response_header["Set-Cookie"] = "EOSCAPABILITY=";
    response_header["Set-Cookie"] += cgi;
    response_header["Set-Cookie"] += ";Max-Age=60;";
    response_header["Set-Cookie"] += "Path=";
    response_header["Set-Cookie"] += path.c_str();
    response_header["Set-Cookie"] += ";Version=1";
    response_header["Set-Cookie"] += ";Domain=";
    response_header["Set-Cookie"] += "cern.ch";
  }
  else
  {
    redirect += "?";
    redirect += cgi;
  }
  response_header["Location"] = redirect;
  return "";
}

/*----------------------------------------------------------------------------*/
std::string
HttpServer::HttpError (int                                &response_code,
                       std::map<std::string, std::string> &response_header,
                       const char                         *errtxt,
                       int                                 errc)
{
  if (errc == ENOENT)
    response_code = 404;
  else
    if (errc == EOPNOTSUPP)
    response_code = 501;
  else
    response_code = 500;

  if (errc > 400)
    response_code = errc;

  XrdOucString html_dir, error;
  char errct[256];
  snprintf(errct, sizeof (errct) - 1, "%d", errc);

  if (getenv("EOS_HTMLDIR"))
    html_dir = getenv("EOS_HTMLDIR");
  else
    html_dir = "/var/share/eos/";

  std::ifstream in(std::string(html_dir.c_str()) + std::string("error.html"));
  std::stringstream buffer;
  buffer << in.rdbuf();
  error = buffer.str().c_str();

  eos_static_info("errc=%d, retcode=%d", errc, response_code);
  while (error.replace("__RESPONSE_CODE__",
                       std::to_string(static_cast<long long>(response_code))
                       .c_str())) {}
  while (error.replace("__ERROR_TEXT__",    errtxt)) {}

  eos_static_debug("html=%s", error.c_str());

  return std::string(error.c_str());
}

/*----------------------------------------------------------------------------*/
std::string
HttpServer::HttpData (int& response_code, std::map<std::string, std::string>& response_header, const char* data, int length)
{
  response_code = 200;
  // return data as HTTP message
  std::string httpdata;
  httpdata.append(data, length);
  return httpdata;
}

/*----------------------------------------------------------------------------*/
std::string
HttpServer::HttpStall (int& response_code, std::map<std::string, std::string>& response_header, const char* stallxt, int stallsec)
{
  // return an HTTP stall
  response_code = 501;
  return HttpError(response_code, response_header, "unable to stall", 503);
}

/*----------------------------------------------------------------------------*/
void
HttpServer::EncodeURI (std::string& cgi)
{
  // replace '+' '/' '='
  XrdOucString scgi = cgi.c_str();
  while (scgi.replace("+", "%2B"))
  {
  }
  while (scgi.replace("/", "%2F"))
  {
  }
  while (scgi.replace("=", "%3D"))
  {
  }
  while (scgi.replace("&", "%26"))
  {
  }
  while (scgi.replace("#", "%23"))
  {
  }
  cgi = "encURI=";
  cgi += scgi.c_str();
}

/*----------------------------------------------------------------------------*/
void
HttpServer::DecodeURI (std::string& cgi)
{
  // replace "%2B" "%2F" "%3D"
  XrdOucString scgi = cgi.c_str();
  while (scgi.replace("%2B", "+"))
  {
  }
  while (scgi.replace("%2F", "/"))
  {
  }
  while (scgi.replace("%3D", "="))
  {
  }
  while (scgi.replace("%26", "&"))
  {
  }
  while (scgi.replace("%23", "#"))
  {
  }
  if (scgi.beginswith("encURI="))
  {
    scgi.erase(0, 7);
  }
  cgi = scgi.c_str();
}

/*----------------------------------------------------------------------------*/
bool
HttpServer::DecodeByteRange (std::string rangeheader, std::map<off_t, ssize_t>& offsetmap, ssize_t& requestsize, off_t filesize)
{
  std::vector<std::string> tokens;
  if (rangeheader.substr(0, 6) != "bytes=")
  {
    // this is an illegal header
    return false;
  }
  else
  {
    rangeheader.erase(0, 6);
  }

  eos::common::StringConversion::Tokenize(rangeheader, tokens, ",");
  // decode the string parts
  for (size_t i = 0; i < tokens.size(); i++)
  {
    eos_static_info("decoding %s", tokens[i].c_str());
    off_t start = 0;
    off_t stop = 0;
    off_t length = 0;

    size_t mpos = tokens[i].find("-");
    if (mpos == std::string::npos)
    {
      // there must always be a '-'
      return false;
    }
    std::string sstop = tokens[i];
    std::string sstart = tokens[i];
    sstart.erase(mpos);
    sstop.erase(0, mpos + 1);
    if (sstart.length())
    {
      start = strtoull(sstart.c_str(), 0, 10);
    }
    if (sstop.length())
    {
      stop = strtoull(sstop.c_str(), 0, 10);
    }

    if ((start > filesize) || (stop > filesize))
    {
      return false;
    }

    if (stop >= start)
    {
      length = (stop - start) + 1;
    }
    else
    {
      continue;
    }

    if (offsetmap.count(start))
    {
      if (offsetmap[start] < length)
      {
        // a previous block has been replaced with a longer one
        offsetmap[start] = length;
      }
    }
    else
    {
      offsetmap[start] = length;
    }
  }

  // now merge overlapping requests
  bool merged = true;
  while (merged)
  {
    requestsize = 0;
    if (offsetmap.begin() == offsetmap.end())
    {
      // if there is nothing in the map just return with error
      eos_static_err("msg=\"range map is empty\"");
      return false;
    }
    for (auto it = offsetmap.begin(); it != offsetmap.end(); it++)
    {
      eos_static_info("offsetmap %llu:%llu", it->first, it->second);
      auto next = it;
      next++;
      if (next != offsetmap.end())
      {
        // check if we have two overlapping requests
        if ((it->first + it->second) >= (next->first))
        {
          merged = true;
          // merge this two
          it->second = next->first + next->second - it->first;
          offsetmap.erase(next);
          break;
        }
        else
        {
          merged = false;
        }
      }
      else
      {
        merged = false;
      }
      // compute the total size
      requestsize += it->second;
    }
  }
  return true;
}

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END
