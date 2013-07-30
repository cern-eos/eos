// ----------------------------------------------------------------------
// File: HttpServer.cc
// Author: Andreas-Joachim Peters & Justin Lewis Salmon - CERN
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
#include "common/http/HttpServer.hh"
#include "common/http/PlainHttpResponse.hh"
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

HttpServer* HttpServer::gHttp; //!< Global HTTP server

/*----------------------------------------------------------------------------*/
HttpServer::HttpServer (int port)
{
  gHttp = this;
#ifdef EOS_MICRO_HTTPD
  mDaemon = 0;
#endif
  mPort = port;
  mThreadId = 0;
  mRunning = false;
}

/*----------------------------------------------------------------------------*/
bool
HttpServer::Start ()
{
  if (!mRunning)
  {
    XrdSysThread::Run(&mThreadId,
                      HttpServer::StaticHttp,
                      static_cast<void *> (this),
                      XRDSYSTHREAD_HOLD,
                      "Httpd Thread");
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
  return reinterpret_cast<HttpServer*> (arg)->Run();
}

/*----------------------------------------------------------------------------*/
void*
HttpServer::Run ()
{
#ifdef EOS_MICRO_HTTPD

  {
    // Delay to make sure xrootd is configured before serving
    XrdSysTimer::Snooze(1);

    mDaemon = MHD_start_daemon(MHD_USE_DEBUG,
                               mPort,
                               NULL,
                               NULL,
                               &HttpServer::StaticHandler,
                               (void*) 0,
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
HttpServer::StaticHandler (void                  *cls,
                           struct MHD_Connection *connection,
                           const char            *url,
                           const char            *method,
                           const char            *version,
                           const char            *upload_data,
                           size_t                *upload_data_size,
                           void                 **ptr)
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
int
HttpServer::BuildHeaderMap (void              *cls,
                            enum MHD_ValueKind kind,
                            const char        *key,
                            const char        *value)
{
  // Call back function to return the header key-val map of an HTTP request
  std::map<std::string, std::string>* hMap
    = static_cast<std::map<std::string, std::string>*> (cls);
  if (key && value && hMap)
  {
    (*hMap)[key] = value;
  }
  return MHD_YES;
}

/*----------------------------------------------------------------------------*/
int
HttpServer::BuildQueryString (void              *cls,
                              enum MHD_ValueKind kind,
                              const char        *key,
                              const char        *value)
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

HttpResponse*
HttpServer::HttpRedirect (const std::string &url,
                          const std::string &hostCGI,
                          int                port,
                          bool               cookie)
{
  eos_static_info("info=redirecting");
  HttpResponse *response = new PlainHttpResponse();
  response->SetResponseCode(HttpResponse::ResponseCodes::TEMPORARY_REDIRECT);
  std::string host = hostCGI;

  std::string cgi = "";
  size_t qpos;

  if ((qpos = host.find("?")) != std::string::npos)
  {
    cgi = host;
    cgi.erase(0, qpos + 1);
    host.erase(qpos);
  }

  eos_static_debug("host=%s", host.c_str());
  eos_static_debug("cgi=%s", cgi.c_str());

  std::string redirect;

  redirect = "http://";
  redirect += host;
  char sport[16];
  snprintf(sport, sizeof (sport) - 1, ":%d", port);
  redirect += sport;
  redirect += url;

  EncodeURI(cgi); // encode '+' '/' '='

  if (cookie)
  {
    response->AddHeader("Set-Cookie", "EOSCAPABILITY="
                                      + cgi
                                      + ";Max-Age=60;"
                                      + "Path="
                                      + url
                                      + ";Version=1"
                                      + ";Domain="
                                      + "cern.ch");
  }
  else
  {
    redirect += "?";
    redirect += cgi;
  }

  response->AddHeader("Location", redirect);
  redirect = "/internal_redirect/" + redirect.substr(7);
  response->AddHeader("X-Accel-Redirect", redirect);
  response->AddHeader("X-Sendfile", redirect);
  return response;
}

/*----------------------------------------------------------------------------*/
HttpResponse*
HttpServer::HttpError (const char *errorText, int errorCode)
{
  HttpResponse *response = new PlainHttpResponse();

  if (errorCode == ENOENT)
    response->SetResponseCode(response->NOT_FOUND);
  else if (errorCode == EOPNOTSUPP)
    response->SetResponseCode(response->NOT_IMPLEMENTED);
  else
    response->SetResponseCode(response->INTERNAL_SERVER_ERROR);

  if (errorCode > 400)
    response->SetResponseCode(errorCode);

  XrdOucString html_dir, error;
  char errct[256];
  snprintf(errct, sizeof (errct) - 1, "%d", errorCode);

  if (getenv("EOS_HTMLDIR"))
    html_dir = getenv("EOS_HTMLDIR");
  else
    html_dir = "/var/share/eos/";

  std::string path = html_dir.c_str();
  path += std::string("error.html");
  std::ifstream in(path.c_str());
  std::stringstream buffer;
  buffer << in.rdbuf();
  error = buffer.str().c_str();

  eos_static_info("errc=%d, retcode=%d", errorCode, response->GetResponseCode());
  while (error.replace("__RESPONSE_CODE__", std::to_string((long long)
                                            response->GetResponseCode()).c_str())) {}
  while (error.replace("__ERROR_TEXT__",    errorText)) {}

  response->SetBody(error.c_str());
  response->AddHeader("Content-Length", std::to_string((long long)
                                        response->GetBodySize()));
  response->AddHeader("Content-Type", "text/html");
  return response;
}

/*----------------------------------------------------------------------------*/
HttpResponse*
HttpServer::HttpData (const char *data, int length)
{
  HttpResponse *response = new PlainHttpResponse();
  response->SetResponseCode(HttpResponse::ResponseCodes::OK);
  response->SetBody(std::string(data, length));
  return response;
}

/*----------------------------------------------------------------------------*/
HttpResponse*
HttpServer::HttpStall(const char *stallText, int seconds)
{
  return HttpError("Unable to stall",
                   HttpResponse::ResponseCodes::SERVICE_UNAVAILABLE);
}

/*----------------------------------------------------------------------------*/
void
HttpServer::EncodeURI (std::string &cgi)
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
HttpServer::DecodeURI (std::string &cgi)
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
EOSCOMMONNAMESPACE_END
