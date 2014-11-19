// ----------------------------------------------------------------------
// File: HttpServer.cc
// Author: ABndreas-Joachim Peters & Justin Lewis Salmon - CERN
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
#include "XrdSys/XrdSysLogger.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdNet/XrdNet.hh"
#include "XrdNet/XrdNetBuffer.hh"
#include "XrdNet/XrdNetPeer.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <map>
#include <sstream>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

#if MHD_VERSION < 0x00093300
#define MHD_USE_EPOLL_LINUX_ONLY 512
#endif

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
void
HttpServer::CleanupConnections ()
{
  // currently we cannot call the clean-up in libmicrohttpd directly, 
  // we just connect to our self and trigger the cleanup
  XrdSysLogger logger;
  XrdSysError error(&logger);
  XrdNet cNet(&error);
  XrdNetPeer cPeer;
  cNet.Connect(cPeer, "localhost",mPort);
}


/*----------------------------------------------------------------------------*/
void*
HttpServer::Run ()
{
#ifdef EOS_MICRO_HTTPD
  std::string thread_model="threads";

  {
    // Delay to make sure xrootd is configured before serving
    XrdSysTimer::Snooze(1);
    
    int nthreads = 16;
    if (getenv("EOS_HTTP_THREADPOOL"))
      thread_model = getenv("EOS_HTTP_THREADPOOL");

    if (getenv("EOS_HTTP_THREADPOOL_SIZE")) 
    {
      nthreads = atoi(getenv("EOS_HTTP_THREADPOOL_SIZE"));
      if (nthreads < 1) 
	nthreads = 16;
      if (nthreads > 4096)
	nthreads = 4096;
    }
    
    if (thread_model == "threads")
    {
      eos_static_notice("msg=\"starting http server\" mode=\"thread-per-connection\"");
      mDaemon = MHD_start_daemon(MHD_USE_DEBUG |  MHD_USE_THREAD_PER_CONNECTION | MHD_USE_POLL,
                                 mPort,
                                 NULL,
                                 NULL,
                                 &HttpServer::StaticHandler,
                                 (void*) 0,
				 MHD_OPTION_NOTIFY_COMPLETED, &HttpServer::StaticCompleteHandler, NULL, 
                                 MHD_OPTION_CONNECTION_MEMORY_LIMIT,
				 getenv("EOS_HTTP_CONNECTION_MEMORY_LIMIT")?atoi(getenv("EOS_HTTP_CONNECTION_MEMORY_LIMIT")): (128*1024*1024),
				 MHD_OPTION_CONNECTION_TIMEOUT, 
				 getenv("EOS_HTTP_CONNECTION_TIMEOUT")?atoi(getenv("EOS_HTTP_CONNECTION_TIMEOUT")):128,
                                 MHD_OPTION_END
                                 );
    } else 
    if (thread_model == "epoll") 
    {
      eos_static_notice("msg=\"starting http server\" mode=\"epool\" threads=%d", nthreads);
      mDaemon = MHD_start_daemon(MHD_USE_DEBUG |  MHD_USE_SELECT_INTERNALLY | MHD_USE_EPOLL_LINUX_ONLY,
                                 mPort,
                                 NULL,
                                 NULL,
                                 &HttpServer::StaticHandler,
                                 (void*) 0,
				 MHD_OPTION_THREAD_POOL_SIZE,
				 nthreads,
				 MHD_OPTION_NOTIFY_COMPLETED, &HttpServer::StaticCompleteHandler, NULL, 
                                 MHD_OPTION_CONNECTION_MEMORY_LIMIT,
				 getenv("EOS_HTTP_CONNECTION_MEMORY_LIMIT")?atoi(getenv("EOS_HTTP_CONNECTION_MEMORY_LIMIT")): (128*1024*1024),
				 MHD_OPTION_CONNECTION_TIMEOUT, 
				 getenv("EOS_HTTP_CONNECTION_TIMEOUT")?atoi(getenv("EOS_HTTP_CONNECTION_TIMEOUT")):128,
                                 MHD_OPTION_END
                                 );
    } else {
      eos_static_notice("msg=\"starting http server\" mode=\"single-threaded\"");
      mDaemon = MHD_start_daemon(MHD_USE_DEBUG,
                                 mPort,
                                 NULL,
                                 NULL,
                                 &HttpServer::StaticHandler,
                                 (void*) 0,
				 MHD_OPTION_NOTIFY_COMPLETED, &HttpServer::StaticCompleteHandler, NULL,
                                 MHD_OPTION_CONNECTION_MEMORY_LIMIT,
                                 128 * 1024 * 1024 /* 128MB */,
                                 MHD_OPTION_END
                                 );
    }
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

  if ( (thread_model == "epoll") || (thread_model == "threads") )
  {
    while (1)
    {
      pause();
    }
  }
  else
  {
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
                           size_t *upload_data_size,
                           void **ptr)
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
void
HttpServer::StaticCompleteHandler ( void *cls, 
				    struct MHD_Connection *connection, 
				    void **con_cls,
				    enum MHD_RequestTerminationCode toe)
{
  // The static handler function calls back the original http object
  if (gHttp)
  {
      gHttp->CompleteHandler(cls, connection, con_cls, toe);
  }
  return;
}

/*----------------------------------------------------------------------------*/
int
HttpServer::BuildHeaderMap (void *cls,
                            enum MHD_ValueKind kind,
                            const char *key,
                            const char *value)
{
  // Call back function to return the header key-val map of an HTTP request
  std::map<std::string, std::string>* hMap
    = static_cast<std::map<std::string, std::string>*> (cls);

  std::string low_key = LC_STRING(key);

  if (key && value && hMap)
  {
    (*hMap)[low_key] = value?value:"";
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

  if (key && qString)
  {
    if (value) 
    {
      if (qString->length())
      {
	*qString += "&";
      }
      *qString += key;
      *qString += "=";
      *qString += value;
    }
    else
    {
      if (qString->length())
      {
	*qString += "&";
      }
      *qString += key;
    }
  }
  return MHD_YES;
}
#endif

HttpResponse*
HttpServer::HttpRedirect (const std::string &url,
                          const std::string &hostCGI,
                          int port,
                          bool cookie)
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
  else if ( (errorCode == EDQUOT) || (errorCode == ENOSPC) )
    response->SetResponseCode(response->INSUFFICIENT_STORAGE);
  else
    response->SetResponseCode(response->INTERNAL_SERVER_ERROR);

  if (errorCode >= 400)
    response->SetResponseCode(errorCode);

  XrdOucString html_dir, error;
  char errct[256];
  snprintf(errct, sizeof (errct) - 1, "%d", errorCode);

  if (getenv("EOS_HTMLDIR"))
    html_dir = getenv("EOS_HTMLDIR");
  else
    html_dir = "/var/eos/html/";

  std::string path = html_dir.c_str();
  path += std::string("error.html");
  std::ifstream in(path.c_str());
  std::stringstream buffer;
  buffer << in.rdbuf();
  error = buffer.str().c_str();

  eos_static_info("errc=%d, retcode=%d errmsg=\"%s\"", errorCode, response->GetResponseCode(), errorText?errorText:"<none>");
  while (error.replace("__RESPONSE_CODE__", to_string((long long)
                                                           response->GetResponseCode()).c_str()))
  {
  }
  while (error.replace("__ERROR_TEXT__", errorText))
  {
  }

  response->SetBody(error.c_str());
  response->AddHeader("Content-Length", to_string((long long)
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
HttpServer::HttpHead (off_t length, std::string name)
{
  HttpResponse *response = new PlainHttpResponse();
  response->SetResponseCode(HttpResponse::ResponseCodes::OK);
  response->SetBody(std::string(""));
  response->AddHeader("Content-Length", to_string((long long) length));
  response->AddHeader("Content-Type", "application/octet-stream");
  response->AddHeader("Accept-Ranges", "bytes");
  response->AddHeader("Content-Disposition", std::string("filename=\"") + name
                      + std::string("\""));
  return response;
}

/*----------------------------------------------------------------------------*/
HttpResponse*
HttpServer::HttpStall (const char *stallText, int seconds)
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
