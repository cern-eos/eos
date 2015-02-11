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
#include "mgm/http/HttpServer.hh"
#include "mgm/http/ProtocolHandlerFactory.hh"
#include "mgm/XrdMgmOfs.hh"
#include "common/http/ProtocolHandler.hh"
#include "common/http/HttpRequest.hh"
#include "common/StringConversion.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysDNS.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/
#include <sstream>

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

#define EOSMGM_HTTP_PAGE "<html><head><title>No such file or directory</title>\
                          </head><body>No such file or directory</body></html>"

#ifdef EOS_MICRO_HTTPD
/*----------------------------------------------------------------------------*/
int
HttpServer::Handler (void *cls,
                     struct MHD_Connection *connection,
                     const char *url,
                     const char *method,
                     const char *version,
                     const char *uploadData,
                     size_t *uploadDataSize,
                     void **ptr)
{
  std::map<std::string, std::string> headers;
  bool go=false;
  do
  {
    // --------------------------------------------------------
    // wait that the namespace is booted
    // --------------------------------------------------------
    {
      XrdSysMutexHelper(gOFS->InitializationMutex);
      if (gOFS->Initialized == gOFS->kBooted)
      {
	go = true;
      }
      else
      {
	XrdSysTimer sleeper;
	sleeper.Wait(100);
      }
    }
  }
  while (!go);

  // If this is the first call, create an appropriate protocol handler based
  // on the headers and store it in *ptr. We should only return MHD_YES here
  // (unless error)
  if (*ptr == 0)
  {
    // Get the headers
    MHD_get_connection_values(connection, MHD_HEADER_KIND,
                              &HttpServer::BuildHeaderMap, (void*) &headers);
 
    char buf[INET6_ADDRSTRLEN];

    // Retrieve Client IP
    const MHD_ConnectionInfo* info = MHD_get_connection_info(connection, MHD_CONNECTION_INFO_CLIENT_ADDRESS);
    if (info && info->client_addr) {
      headers["client-real-ip"] = inet_ntop(info->client_addr->sa_family, info->client_addr->sa_data + 2, buf, INET6_ADDRSTRLEN);
      
      char* haddr[1];
      char* hname[1];
      if ( (XrdSysDNS::getAddrName(const_cast<char*> (headers["client-real-ip"].c_str()),
				   1,
				   haddr,
				   hname)) > 0 ) 
      {
	headers["client-real-host"] =const_cast<char*> (hname[0]);
	free(hname[0]);
	free(haddr[0]);
      }
    }
    
    // Authenticate the client
    eos::common::Mapping::VirtualIdentity *vid = Authenticate(headers);

    eos_static_info("request=%s client-real-ip=%s client-real-host=%s vid.uid=%s vid.gid=%s vid.host=%s vid.tident=%s\n", method, headers["client-real-ip"].c_str(), headers["client-real-host"].c_str(), vid->uid_string.c_str(), vid->gid_string.c_str(), vid->host.c_str(), vid->tident.c_str());
	  
    eos::common::ProtocolHandler *handler;
    ProtocolHandlerFactory factory = ProtocolHandlerFactory();
    handler = factory.CreateProtocolHandler(method, headers, vid);
    if (!handler)
    {
      eos_static_err("msg=\"no matching protocol for request method %s\"",
                     method);
      return MHD_NO;
    }

    *ptr = handler;
    // PUT has to run through to avoid the generation of 100-CONTINUE before a redirect
    if ( strcmp(method,"PUT") )
      return MHD_YES;
  }
  // Retrieve the protocol handler stored in *ptr
  eos::common::ProtocolHandler *protocolHandler = (eos::common::ProtocolHandler*) * ptr;

  // For requests which have a body (i.e. uploadDataSize != 0) we must handle
  // the body data on the second reentrant call to this function. We must
  // create the response and store it inside the protocol handler, but we must
  // NOT queue the response until the third call.
  if (!protocolHandler->GetResponse())
  {
    // Get the request headers again
    MHD_get_connection_values(connection, MHD_HEADER_KIND,
                              &HttpServer::BuildHeaderMap, (void*) &headers);

    // Get the request query string
    std::string query;
    MHD_get_connection_values(connection, MHD_GET_ARGUMENT_KIND,
                              &HttpServer::BuildQueryString, (void*) &query);

    // Get the cookies
    std::map<std::string, std::string> cookies;
    MHD_get_connection_values(connection, MHD_COOKIE_KIND,
                              &HttpServer::BuildHeaderMap, (void*) &cookies);

    // Make a request object
    std::string body(uploadData, *uploadDataSize);
    eos::common::HttpRequest *request = new eos::common::HttpRequest(
                                                                     headers, method, url,
                                                                     query.c_str() ? query : "",
                                                                     body, uploadDataSize, cookies);
    eos_static_debug("\n\n%s\n%s\n", request->ToString().c_str(), request->GetBody().c_str());
    
    // Handle the request and build a response based on the specific protocol
    protocolHandler->HandleRequest(request);
    delete request;
  }

  // If we have a non-empty body, we must "process" it, set the body size to
  // zero, and return MHD_YES. We should not queue the response yet - we must
  // do that on the next (third) call.
  if (*uploadDataSize != 0)
  {
    *uploadDataSize = 0;
    return MHD_YES;
  }

  eos::common::HttpResponse *response = protocolHandler->GetResponse();
  if (!response)
  {
    eos_static_crit("msg=\"response creation failed\"");
    delete protocolHandler;
    *ptr = 0;
    return MHD_NO;
  }
  eos_static_debug("\n\n%s", response->ToString().c_str());

  // Create the response
  struct MHD_Response *mhdResponse;
  mhdResponse = MHD_create_response_from_buffer(response->GetBodySize(), (void*)
                                                response->GetBody().c_str(),
                                                MHD_RESPMEM_MUST_COPY);

  if (mhdResponse)
  {
    // Add all the response header tags
    headers = response->GetHeaders();
    for (auto it = headers.begin(); it != headers.end(); it++)
    {
      MHD_add_response_header(mhdResponse, it->first.c_str(), it->second.c_str());
    }

    // Queue the response
    int ret = MHD_queue_response(connection, response->GetResponseCode(),
                                 mhdResponse);
    eos_static_debug("msg=\"MHD_queue_response\" retc=%d", ret);
    MHD_destroy_response(mhdResponse);
    delete protocolHandler;
    *ptr = 0;
    return ret;
  }
  else
  {
    eos_static_crit("msg=\"response creation failed\"");
    delete protocolHandler;
    *ptr = 0;
    return MHD_NO;
  }
}

#endif

/*----------------------------------------------------------------------------*/
void
HttpServer::CompleteHandler (void                              *cls,
			     struct MHD_Connection             *connection,
			     void                             **con_cls,
			     enum MHD_RequestTerminationCode    toe)
{
  std::string scode="";
  if ( toe == MHD_REQUEST_TERMINATED_COMPLETED_OK)
    scode="OK";
  if ( toe == MHD_REQUEST_TERMINATED_WITH_ERROR)
    scode="Error";
  if ( toe == MHD_REQUEST_TERMINATED_TIMEOUT_REACHED)
    scode="Timeout";
  if ( toe == MHD_REQUEST_TERMINATED_DAEMON_SHUTDOWN)
    scode="Shutdown";
  if ( toe == MHD_REQUEST_TERMINATED_READ_ERROR)
    scode="ReadError";

  eos_static_info("msg=\"http connection disconnect\" reason=\"Request %s\" ", scode.c_str());
}

/*----------------------------------------------------------------------------*/
eos::common::Mapping::VirtualIdentity*
HttpServer::Authenticate (std::map<std::string, std::string> &headers)
{
  eos::common::Mapping::VirtualIdentity *vid = 0;
  std::string clientDN = headers["ssl_client_s_dn"];
  std::string remoteUser = headers["remote-user"];
  std::string dn;
  std::string username;
  unsigned pos;

  if (clientDN.empty() && remoteUser.empty())
  {
    eos_static_debug("msg=\"client supplied neither SSL_CLIENT_S_DN nor "
                    "Remote-User headers\"");
  }
  else
  {
    if (clientDN.length())
    {
      // Stat the gridmap file
      struct stat info;
      if (stat("/etc/grid-security/grid-mapfile", &info) == -1)
      {
	eos_static_warning("msg=\"error stating gridmap file: %s\"", strerror(errno));
	username = "";
      }  
      else
      {  
	
	// Initially load the file, or reload it if it was modified
	if (!mGridMapFileLastModTime.tv_sec ||
	    mGridMapFileLastModTime.tv_sec != info.st_mtim.tv_sec)
	{
	  eos_static_info("msg=\"reloading gridmap file\"");
	  
	  std::ifstream in("/etc/grid-security/grid-mapfile");
	  std::stringstream buffer;
	  buffer << in.rdbuf();
	  mGridMapFile = buffer.str();
	  mGridMapFileLastModTime = info.st_mtim;
	  in.close();
	}
	
	// Process each mapping
	std::vector<std::string> mappings;
	eos::common::StringConversion::Tokenize(mGridMapFile, mappings, "\n");
	
	for (auto it = mappings.begin(); it != mappings.end(); ++it)
	{
	  eos_static_debug("grid mapping: %s", (*it).c_str());
	  
	  // Split off the last whitespace-separated token (i.e. username)
	  pos = (*it).find_last_of(" \t");
	  if (pos == string::npos)
	  {
	    eos_static_err("msg=malformed gridmap file");
	    return NULL;
	  }
	  
	  dn = (*it).substr(1, pos - 2); // Remove quotes around DN
	  username = (*it).substr(pos + 1);
	  
	  // for proxies clientDN as appended a ../CN=... which has to be removed
	  std::string clientDNproxy = clientDN;
	  if (!clientDN.empty())
	    clientDNproxy.erase(clientDN.rfind("/CN="));
	  // Try to match with SSL header
	  if (dn == clientDN)
	  {
	    eos_static_info("msg=\"mapped client certificate successfully\" "
			    "dn=\"%s\"username=\"%s\"", dn.c_str(), username.c_str());
	    break;
	  }
	  
	  if (dn == clientDNproxy)
	  {
	    eos_static_info("msg=\"mapped client proxy certificate successfully\" "
			    "dn=\"%s\"username=\"%s\"", dn.c_str(), username.c_str());
	    break;
	  }
	  
	  username = "";
	}
      }
    }

    if (remoteUser.length()) 
    {
      // extract kerberos username
      pos = remoteUser.find_last_of("@");
      std::string remoteUserName = remoteUser.substr(0, pos);
      username = remoteUserName;
      eos_static_info("msg=\"mapped client remote username successfully\" "
		      "username=\"%s\"", username.c_str());
    }
  }
  
  if (username.empty())
  {
    eos_static_info("msg=\"unauthenticated client mapped to nobody"
                    "\" SSL_CLIENT_S_DN=\"%s\", Remote-User=\"%s\"",
                    clientDN.c_str(), remoteUser.c_str());
    username = "nobody";
  }

  XrdSecEntity client(headers.count("x-real-ip")?"https":"http");
  std::string remotehost="";

  if (headers.count("x-real-ip"))
  {
    // translate a proxied host name
    remotehost = const_cast<char*> (headers["x-real-ip"].c_str());

    char* haddr[1];
    char* hname[1];
    if ( (XrdSysDNS::getAddrName(remotehost.c_str(),
				     1,
				     haddr,
				     hname)) > 0 )
    {
      remotehost =const_cast<char*> (hname[0]);
      free(hname[0]);
      free(haddr[0]);
    }
  
    if (headers.count("auth-type"))
    {
      remotehost += "=>";
      remotehost += headers["auth-type"];
    }
    client.host = const_cast<char*> (remotehost.c_str());
  }

  XrdOucString tident = username.c_str();
  tident += ".1:1@"; tident += client.host;
  client.name = const_cast<char*> (username.c_str());
  client.host = const_cast<char*> (headers["client-real-host"].c_str());
  client.tident = const_cast<char*> (tident.c_str());
  {
    // Make a virtual identity object
    vid = new eos::common::Mapping::VirtualIdentity();

    EXEC_TIMING_BEGIN("IdMap");
    eos::common::Mapping::IdMap (&client, "eos.app=http", client.tident, *vid, true);
    EXEC_TIMING_END("IdMap");

    // Verify that a connection originates from the host stated in the header

    std::string header_host = headers["host"];
    size_t pos= header_host.find(":");
    // remove the port if present
    if ( pos != std::string::npos)
      header_host.erase(pos);
    
    if ( (headers.count("client-real-host")) &&
	 (headers["client-real-host"] != "localhost") && 
	 (headers["client-real-host"] != "localhost.localdomain") &&
	 (headers["client-real-host"] != "localhost6") &&
	 (headers["client-real-host"] != "localhost6.localdomain6") &&
	 ( headers["client-real-host"] != header_host ) ) {
      // map the betrayer to nobody
      eos::common::Mapping::Nobody(*vid);
      eos_static_notice("msg=\"connection/header mismatch\" header-host=\"%s\" connection-host=\"%s\" real-ip=%s", header_host.c_str(), headers["client-real-host"].c_str(), headers["client-real-ip"].c_str());
    } else {
      eos_static_debug("msg=\"connection/header match\" header-host=\"%s\" connection-host=\"%s\" real-ip=%s", header_host.c_str(), headers["client-real-host"].c_str(), headers["client-real-ip"].c_str());
    }


    
    // if we have been mapped to nobody, change also the name accordingly
    if (vid->uid == 99)
      vid->name = const_cast<char*> ("nobody");
    vid->dn = dn;
    vid->tident = tident.c_str();
  }

  return vid;
}

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_END
