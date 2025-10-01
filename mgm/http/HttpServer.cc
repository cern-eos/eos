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

#include "mgm/http/HttpServer.hh"
#include "mgm/http/ProtocolHandlerFactory.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Stat.hh"
#include "mgm/Macros.hh"
#include "common/Path.hh"
#include "common/SecEntity.hh"
#include "common/StringTokenizer.hh"
#include "common/ErrnoToString.hh"
#include <XrdNet/XrdNetAddr.hh>
#include <XrdAcc/XrdAccAuthorize.hh>
#include <netdb.h>

EOSMGMNAMESPACE_BEGIN

#define EOSMGM_HTTP_PAGE "<html><head><title>No such file or directory</title>\
                          </head><body>No such file or directory</body></html>"

#ifdef EOS_MICRO_HTTPD
/*----------------------------------------------------------------------------*/
int
HttpServer::Handler(void* cls,
                    struct MHD_Connection* connection,
                    const char* url,
                    const char* method,
                    const char* version,
                    const char* uploadData,
                    size_t* uploadDataSize,
                    void** ptr)
{
  using namespace eos::common;
  std::map<std::string, std::string> headers;
  // Wait for the namespace to boot
  WAIT_BOOT;

  // If this is the first call, create an appropriate protocol handler based
  // on the headers and store it in *ptr. We should only return MHD_YES here
  // (unless error)
  if (*ptr == 0) {
    // Get the headers
    MHD_get_connection_values(connection, MHD_HEADER_KIND,
                              &HttpServer::BuildHeaderMap, (void*) &headers);
    // Retrieve Client IP
    const MHD_ConnectionInfo* info = MHD_get_connection_info(connection,
                                     MHD_CONNECTION_INFO_CLIENT_ADDRESS);

    if (info && info->client_addr) {
      char host[NI_MAXHOST];

      if (! getnameinfo(info->client_addr,
                        (info->client_addr->sa_family == AF_INET) ? sizeof(struct sockaddr_in) : sizeof(
                          struct sockaddr_in6), host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST)) {
        headers["client-real-ip"] = host;
      } else {
        headers["client-real-ip"] = "NOIPLOOKUP";
      }

      XrdNetAddr netaddr(info->client_addr);
      const char* name = netaddr.Name();

      if (name) {
        headers["client-real-host"] = name;
      }
    }

    // Clients which are gateways/sudoer can pass x-forwarded-for and remote-user
    if (headers.count("x-forwarded-for")) {
      // Check if this is a http gateway and sudoer by calling the mapping function
      std::unique_ptr<VirtualIdentity> vid_tmp  {new VirtualIdentity()};

      if (vid_tmp) {
        XrdSecEntity eclient(headers.count("x-real-ip") ? "https" : "http");
        eclient.tident = "";
        eclient.name = (char*)"nobody";
        eclient.host = (char*)(headers["client-real-host"].length() ?
                               headers["client-real-host"].c_str() : "");

        if (headers.count("x-gateway-authorization")) {
          eclient.endorsements = (char*)headers["x-gateway-authorization"].c_str();
        }

        std::string stident = "https.0:0@";
        stident += headers["client-real-host"];
        eos::common::Mapping::IdMap(&eclient, "", stident.c_str(), *vid_tmp);

        if (!vid_tmp->isGateway() ||
            ((vid_tmp->prot != "https") && (vid_tmp->prot != "http"))) {
          headers.erase("x-forwarded-for");
          headers.erase("x-real-ip");
        }

        eos_static_debug("vid trace: %s gw:%d", vid_tmp->getTrace().c_str(),
                         vid_tmp->isGateway());

        if (headers.count("x-gateway-authorization") && !vid_tmp->sudoer) {
          headers.erase("remote-user");
        }
      } else {
        eos_static_err("msg=\"failed to allocate VirtualIdentity object\" "
                       "method=%s", method);
        return MHD_NO;
      }
    } else {
      headers.erase("x-real-ip");
      headers.erase("remote-user");
    }

    // Authenticate the client
    std::unique_ptr<eos::common::VirtualIdentity> vid = Authenticate(headers);
    if (!vid) {
      eos_static_info("msg=\"could not build VirtualIdentity based on headers\" "
                      "method=%s", method);
      return MHD_NO;
    }
    eos_static_info("request=%s client-real-ip=%s client-real-host=%s vid.uid=%s vid.gid=%s vid.host=%s vid.tident=%s\n",
                    method, headers["client-real-ip"].c_str(), headers["client-real-host"].c_str(),
                    vid->uid_string.c_str(), vid->gid_string.c_str(), vid->host.c_str(),
                    vid->tident.c_str());
    eos::common::ProtocolHandler* handler;
    ProtocolHandlerFactory factory = ProtocolHandlerFactory();
    handler = factory.CreateProtocolHandler(method, headers, vid.release());

    if (!handler) {
      eos_static_err("msg=\"no matching protocol for request method %s\"",
                     method);
      return MHD_NO;
    }

    *ptr = handler;

    // PUT has to run through to avoid the generation of 100-CONTINUE before a redirect
    if (strcmp(method, "PUT")) {
      return MHD_YES;
    }
  }

  // Retrieve the protocol handler stored in *ptr
  eos::common::ProtocolHandler* protocolHandler = (eos::common::ProtocolHandler*)
      * ptr;

  // For requests which have a body (i.e. uploadDataSize != 0) we must handle
  // the body data on the last call to this function. We must
  // create the response and store it inside the protocol handler, but we must
  // NOT queue the response until the third call.
  if (!protocolHandler->GetResponse() && (!*uploadDataSize)) {
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
    size_t bodySize = protocolHandler->GetBody().size();
    // Make a request object
    eos::common::HttpRequest* request = new eos::common::HttpRequest(
      headers, method, url,
      query.c_str() ? query : "",
      protocolHandler->GetBody(), &bodySize, cookies);
    eos_static_debug("\n\n%s\n%s\n", request->ToString().c_str(),
                     request->GetBody().c_str());
    // Handle the request and build a response based on the specific protocol unless the body is not complete ...
    protocolHandler->HandleRequest(request);
    delete request;
  }

  // If we have a non-empty body, we must "process" it, set the body size to
  // zero, and return MHD_YES. We should not queue the response yet - we must
  // do that on the next (third) call.
  if (*uploadDataSize != 0) {
    // we store the partial body into the handler
    protocolHandler->AddToBody(uploadData, *uploadDataSize);
    *uploadDataSize = 0;
    return MHD_YES;
  }

  eos::common::HttpResponse* response = protocolHandler->GetResponse();

  if (!response) {
    eos_static_crit("msg=\"response creation failed\"");
    delete protocolHandler;
    *ptr = 0;
    return MHD_NO;
  }

  eos_static_debug("\n\n%s", response->ToString().c_str());
  // Create the response
  struct MHD_Response* mhdResponse;
  mhdResponse = MHD_create_response_from_buffer(response->GetBodySize(), (void*)
                response->GetBody().c_str(),
                MHD_RESPMEM_MUST_COPY);

  if (mhdResponse) {
    // Add all the response header tags
    headers = response->GetHeaders();

    for (auto it = headers.begin(); it != headers.end(); it++) {
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
  } else {
    eos_static_crit("msg=\"response creation failed\"");
    delete protocolHandler;
    *ptr = 0;
    return MHD_NO;
  }
}

/*----------------------------------------------------------------------------*/
void
HttpServer::CompleteHandler(void*                              cls,
                            struct MHD_Connection*             connection,
                            void**                             con_cls,
                            enum MHD_RequestTerminationCode    toe)
{
  std::string scode = "";

  if (toe == MHD_REQUEST_TERMINATED_COMPLETED_OK) {
    scode = "OK";
  }

  if (toe == MHD_REQUEST_TERMINATED_WITH_ERROR) {
    scode = "Error";
  }

  if (toe == MHD_REQUEST_TERMINATED_TIMEOUT_REACHED) {
    scode = "Timeout";
  }

  if (toe == MHD_REQUEST_TERMINATED_DAEMON_SHUTDOWN) {
    scode = "Shutdown";
  }

  if (toe == MHD_REQUEST_TERMINATED_READ_ERROR) {
    scode = "ReadError";
  }

  eos_static_info("msg=\"http connection disconnect\" reason=\"Request %s\" ",
                  scode.c_str());
}

#endif

//------------------------------------------------------------------------------
// Do a "rough" mapping between HTTP verbs and access operation types
//
// @param http_verb type of HTTP request
//
// @return XRootD access operation type
//------------------------------------------------------------------------------
Access_Operation MapHttpVerbToAOP(const std::string& http_verb)
{
  Access_Operation op = AOP_Stat;

  if (http_verb == "GET") {
    op = AOP_Read;
  } else if (http_verb == "PUT") {
    op = AOP_Create;
  } else if (http_verb == "DELETE") {
    op = AOP_Delete;
  }

  return op;
}

//------------------------------------------------------------------------------
// HTTP object handler function called by XrdHttp
//------------------------------------------------------------------------------
std::unique_ptr<eos::common::ProtocolHandler>
HttpServer::XrdHttpHandler(std::string& method,
                           std::string& uri,
                           std::map<std::string, std::string>& headers,
                           std::map<std::string, std::string>& cookies,
                           std::string& body,
                           const XrdSecEntity& client,
                           XrdAccAuthorize* authz_obj,
                           std::string& err_msg)
{
  using namespace eos::common;
  WAIT_BOOT;

  // Clients which are gateways/sudoer can pass x-forwarded-for and remote-user
  if (headers.count("x-forwarded-for")) {
    // Check if this is a http gateway and sudoer by calling the mapping function
    std::unique_ptr<VirtualIdentity> vid_tmp  {new VirtualIdentity()};

    if (vid_tmp) {
      XrdSecEntity eclient(client.prot);
      // Save initial eaAPI pointer and reset after the copy to avoid
      // double free of the same pointer.
      auto ea = eclient.eaAPI;
      eclient = client;
      eclient.eaAPI = ea;

      if (headers.count("x-gateway-authorization")) {
        eclient.endorsements = (char*)headers["x-gateway-authorization"].c_str();
      }

      std::string stident = "https.0:0@";
      stident += std::string(client.host);
      eos::common::Mapping::IdMap(&eclient, "", stident.c_str(), *vid_tmp);

      if (!vid_tmp->isGateway() ||
          ((vid_tmp->prot != "https") && (vid_tmp->prot != "http"))) {
        headers.erase("x-forwarded-for");
        headers.erase("x-real-ip");
      }

      eos_static_debug("vid trace: %s gw:%d", vid_tmp->getTrace().c_str(),
                       vid_tmp->isGateway());

      if (headers.count("x-gateway-authorization") && !vid_tmp->sudoer) {
        headers.erase("remote-user");
      }
    } else {
      err_msg = "failed to allocate memory";
      eos_static_err("msg=\"failed to allocate VirtualIdentity object\" "
                     "method=%s uri=\"%s\"", method.c_str(), uri.c_str());
      return nullptr;
    }
  }

  bool s3_access = false;
  auto it_authz = headers.find("authorization");

  if (it_authz != headers.end()) {
    if (it_authz->second.substr(0, 3) == "AWS") {
      s3_access = true;
    }
  }

  std::string query; //@todo(esindril) decide if this is needed
  std::unique_ptr<VirtualIdentity> vid;

  // Native XrdHttp access
  if (headers.find("x-forwarded-for") == headers.end() && !s3_access) {
    std::string path;
    std::unique_ptr<XrdOucEnv> env_opaque;

    if (!BuildPathAndEnvOpaque(headers, path, env_opaque)) {
      err_msg = "conflicting authorization info present";
      eos_static_err("msg=\"%s\" path=\"%s\"", err_msg.c_str(), path.c_str());
      return nullptr;
    }

    int envlen;
    char* ptr = env_opaque->Env(envlen);

    if (ptr == nullptr) {
      err_msg = "empty opaque info for request";
      eos_static_err("msg=\"%s\" path=\"%s\"", err_msg.c_str(), path.c_str());
      return nullptr;
    }

    // Get access operation type for the authz library
    Access_Operation acc_op = MapHttpVerbToAOP(method);
    const std::string env = ptr;
    query = env;
    vid = std::make_unique<VirtualIdentity>();
    EXEC_TIMING_BEGIN("IdMap");
    Mapping::IdMap(&client, env.c_str(), client.tident, *vid,
                   authz_obj, acc_op, path);
    EXEC_TIMING_END("IdMap");
  } else { // HTTP access through Nginx
    headers["client-real-ip"] = "NOIPLOOKUP";
    headers["client-real-host"] = client.host;
    headers["x-real-ip"] = client.host;

    auto it = headers.find("xrd-http-fullresource");

    if (it != headers.end()) {
      extractOpaqueWithoutAuthz(it->second,query);
    }

    if (client.moninfo && strlen(client.moninfo)) {
      headers["ssl_client_s_dn"] = client.moninfo;
    }

    vid = Authenticate(headers);
    if (!vid) {
      eos_static_info("msg=\"could not build VirtualIdentity based on headers\" "
                      "method=%s", method.c_str());
      return nullptr;
    }
  }

  // Update the vid.name as the mapping might have changed the vid.uid and it
  // is the name that is used later on for all the authorization bits
  int errc = 0;
  std::string usr_name = eos::common::Mapping::UidToUserName(vid->uid, errc);
  vid->name = (errc ? std::to_string(vid->uid).c_str() : usr_name.c_str());
  // Add the path to the vid's scope member for token ACL path validation
  vid->scope = uri;
  eos_static_info("request=%s client-real-ip=%s client-real-host=%s "
                  "vid.name=%s vid.uid=%s vid.gid=%s vid.host=%s "
                  "vid.dn=%s vid.tident=%s",
                  method.c_str(), headers["client-real-ip"].c_str(),
                  headers["client-real-host"].c_str(), vid->name.c_str(),
                  vid->uid_string.c_str(), vid->gid_string.c_str(),
                  vid->host.c_str(), vid->dn.c_str(), vid->tident.c_str());
  ProtocolHandlerFactory factory = ProtocolHandlerFactory();
  std::unique_ptr<eos::common::ProtocolHandler> handler
  {factory.CreateProtocolHandler(method, headers, vid.release())};

  if (!handler) {
    eos_static_err("msg=\"no matching protocol for request method %s\"",
                   method.c_str());
    return nullptr;
  }

  size_t bodySize = body.length();
  // Retrieve the protocol handler stored in *ptr
  std::unique_ptr<eos::common::HttpRequest> request {
    new eos::common::HttpRequest(headers, method, uri,
    (query.c_str() ? query : ""),
    body, &bodySize, cookies)};
  eos_static_debug("\n\n%s\n%s\n", request->ToString().c_str(),
                   request->GetBody().c_str());
  handler->HandleRequest(request.get());
  eos_static_debug("method=%s uri=\"%s\"client=\"%s\" msg=\"warning this is "
                   "not the mapped identity\"", method.c_str(), uri.c_str(),
                   eos::common::SecEntity::ToString(&client, "xrdhttp").c_str());
  return handler;
}


//------------------------------------------------------------------------------
// Build path and opaque information based on the HTTP headers
//------------------------------------------------------------------------------
bool
HttpServer::BuildPathAndEnvOpaque
(const std::map<std::string, std::string>& normalized_headers,
 std::string& path, std::unique_ptr<XrdOucEnv>& env_opaque)
{
  using eos::common::StringConversion;
  // Extract path and any opaque info that might be present in the headers
  // /path/to/file?and=some&opaque=info
  path.clear();
  auto it = normalized_headers.find("xrd-http-fullresource");

  if (it == normalized_headers.end()) {
    eos_static_err("%s", "msg=\"no xrd-http-fullresource header\"");
    return false;
  }

  std::string opaque;
  extractPathAndOpaque(it->second,path,opaque);

  // Check if there is an explicit authorization header
  std::string http_authz;
  it = normalized_headers.find("authorization");

  if (it != normalized_headers.end()) {
    http_authz = it->second;
  }

  // If opaque data aleady contains authorization info i.e. "&authz=..." and we also
  // have a HTTP authorization header then we fail
  bool has_opaque_authz = (opaque.find("authz=") != std::string::npos);

  if (has_opaque_authz && !http_authz.empty()) {
    eos_static_err("msg=\"request has both opaque and http authorization\" "
                   "opaque=\"%s\" http_authz=\"%s\"", opaque.c_str(),
                   http_authz.c_str());
    return false;
  }

  if (!http_authz.empty()) {
    std::string enc_authz = StringConversion::curl_default_escaped(http_authz);
    opaque += "&authz=";
    opaque += enc_authz;
  }

  it = normalized_headers.find("xrd-http-query");

  if (it != normalized_headers.end()) {
    std::string query = it->second;

    if (!query.empty()) {
      if (*query.begin() != '&') {
        opaque += "&";
      }

      opaque += query;
    }
  }

  // Append eos.app tag
  eos::common::AddEosApp(opaque,"http");

  env_opaque = std::make_unique<XrdOucEnv>(opaque.c_str(), opaque.length());
  return true;
}

void HttpServer::extractPathAndOpaque(const std::string & fullpath, std::string & path, std::string & opaque)
{
  path = fullpath;

  size_t pos = fullpath.find('?');
  if ((pos != std::string::npos) && (pos != fullpath.length())) {
    opaque = path.substr(pos + 1);
    path = path.substr(0, pos);
    eos::common::Path canonical_path(path);
    path = canonical_path.GetFullPath().c_str();
  }

}

void HttpServer::extractOpaqueWithoutAuthz(const std::string & fullpath, std::string & opaque) {
  std::string path;
  opaque.clear();
  extractPathAndOpaque(fullpath,path,opaque);
  if(opaque.size()) {
    auto env_opaque = std::make_unique<XrdOucEnv>(opaque.c_str(), opaque.length());
    int envTidyLen;
    char * envTidy = env_opaque->EnvTidy(envTidyLen);
    if(envTidyLen > 1) {
      // Seen during unit tests, EnvTidy puts an ampersand at the beginning of the resulting string
      opaque.assign(envTidy + 1, envTidyLen - 1);
    }
  }
}

//------------------------------------------------------------------------------
// Handle clientDN specified using RFC2253 (and RFC4514) where the
// separator is "," instead of the usual "/" and also the order of the DNs
// is reversed
//------------------------------------------------------------------------------
std::string
HttpServer::ProcessClientDN(const std::string& cdn) const
{
  std::string new_cdn = cdn;

  if (new_cdn.empty()) {
    return new_cdn;
  }

  if (new_cdn.find(',') != std::string::npos) {
    // clientDN specified using RFC2253 (and RFC4514) where the separator is
    // "," instead of the usual "/" and DNs reversed
    std::replace(new_cdn.begin(), new_cdn.end(), ',', '/');
    // Reverse the DN tokens
    auto tokens =  eos::common::StringTokenizer::split
                   <std::list<std::string>>(new_cdn, '/');
    new_cdn.clear();

    for (auto token = tokens.rbegin(); token != tokens.rend(); ++token) {
      new_cdn += '/';
      new_cdn += *token;
    }
  }

  return new_cdn;
}

/*----------------------------------------------------------------------------*/
std::unique_ptr<eos::common::VirtualIdentity>
HttpServer::Authenticate(std::map<std::string, std::string>& headers)
{
  std::unique_ptr<eos::common::VirtualIdentity> vid;
  std::string clientDN = headers["ssl_client_s_dn"];
  std::string remoteUser = headers["remote-user"];
  std::string dn;
  std::string username;
  unsigned pos;

  if (clientDN.empty() && remoteUser.empty()) {
    eos_static_debug("msg=\"client supplied neither SSL_CLIENT_S_DN nor "
                     "Remote-User headers\"");
  } else {
    if (clientDN.length()) {
      clientDN = ProcessClientDN(clientDN);
      // Stat the gridmap file
      struct stat info;

      if (stat("/etc/grid-security/grid-mapfile", &info) == -1) {
        eos_static_warning("msg=\"error stating gridmap file: %s\"",
                           eos::common::ErrnoToString(errno).c_str());
        username = "";
      } else {
        {
          static XrdSysMutex mGridMapMutex;
          XrdSysMutexHelper gLock(mGridMapMutex);

          // Initially load the file, or reload it if it was modified
          if (!mGridMapFileLastModTime.tv_sec ||
              mGridMapFileLastModTime.tv_sec != info.st_mtim.tv_sec) {
            eos_static_info("msg=\"reloading gridmap file\"");
            std::ifstream in("/etc/grid-security/grid-mapfile");
            std::stringstream buffer;
            buffer << in.rdbuf();
            mGridMapFile = buffer.str();
            mGridMapFileLastModTime = info.st_mtim;
            in.close();
          }
        }
        // For proxy certificates clientDN can have multiple ../CN=... appended
        size_t pos = 0;
        int num_cns = 0;

        while ((pos = clientDN.find("/CN=", pos)) != std::string::npos) {
          ++num_cns;
          ++pos;
        }

        // Remove the CNs from the end one by one to check if the remaining
        // DN is in the map
        std::set<std::string> proxy_dns;
        std::string clientDNproxy = clientDN;

        while (num_cns >= 2) {
          clientDNproxy.erase(clientDNproxy.rfind("/CN="));
          proxy_dns.insert(clientDNproxy);
          --num_cns;
        }

        // Process each mapping
        std::vector<std::string> mappings;
        eos::common::StringConversion::Tokenize(mGridMapFile, mappings, "\n");

        for (auto it = mappings.begin(); it != mappings.end(); ++it) {
          eos_static_debug("grid mapping: %s", (*it).c_str());
          // Split off the last whitespace-separated token (i.e. username)
          pos = (*it).find_last_of(" \t");

          if (pos == std::string::npos) {
            eos_static_err("msg=malformed gridmap file");
            return nullptr;
          }

          dn = (*it).substr(1, pos - 2); // Remove quotes around DN
          username = (*it).substr(pos + 1);

          // Try to match with SSL header
          if (dn == clientDN) {
            eos_static_info("msg=\"mapped client certificate successfully\" "
                            "dn=\"%s\" username=\"%s\"", dn.c_str(), username.c_str());
            break;
          }

          // Check if any of the proxy dns matches
          if (proxy_dns.find(dn) != proxy_dns.end()) {
            eos_static_info("msg=\"mapped client proxy certificate successfully\" "
                            "dn=\"%s\"username=\"%s\"", dn.c_str(), username.c_str());
            break;
          }

          username = "";
        }
      }
    } else {
      if (remoteUser.length()) {
        // extract kerberos username
        pos = remoteUser.find_last_of("@");
        std::string remoteUserName = remoteUser.substr(0, pos);
        username = remoteUserName;
        eos_static_info("msg=\"mapped client remote username successfully\" "
                        "username=\"%s\"", username.c_str());
      }
    }
  }

  if (username.empty()) {
    eos_static_info("msg=\"unauthenticated client mapped to nobody"
                    "\" SSL_CLIENT_S_DN=\"%s\", Remote-User=\"%s\"",
                    clientDN.c_str(), remoteUser.c_str());
    username = "nobody";
  }

  XrdSecEntity client(headers.count("x-real-ip") ? "https" : "http");
  std::string remotehost;

  if (headers.count("x-real-ip")) {
    // Translate a proxied host name
    std::string real_ip = headers["x-real-ip"];

    if (real_ip.empty()) {
      eos_static_err("msg=\"x-real-ip header is empty\"");
      return nullptr;
    }

    // XrdNetAddr deals properly with IPv6 addresses only if they use the
    // bracket format [ipv6_addr][:<port>]
    if (real_ip.find('.') == std::string::npos) {
      // We can safely assume this is an IPv6 address now
      if (real_ip[0] != '[') {
        std::ostringstream oss;
        oss << '[' << real_ip << ']';
        real_ip = oss.str();
      }
    }

    remotehost = real_ip;
    XrdNetAddr netaddr;
    netaddr.Set(real_ip.c_str());
    // Try to convert IP to corresponding [host] name
    const char* name = netaddr.Name();

    if (name) {
      remotehost = name;
    }

    if (headers.count("auth-type")) {
      remotehost += "=>";
      remotehost += headers["auth-type"];
    }
  }

  client.host = const_cast<char*>(remotehost.c_str());
  XrdOucString tident = username.c_str();
  tident += ".1:1@";
  tident += const_cast<char*>(headers["client-real-host"].c_str());
  client.name = const_cast<char*>(username.c_str());
  client.tident = const_cast<char*>(tident.c_str());
  {
    // Make a virtual identity object
    vid = std::make_unique<eos::common::VirtualIdentity>();
    EXEC_TIMING_BEGIN("IdMap");
    eos::common::Mapping::IdMap(&client, "eos.app=http", client.tident, *vid);
    EXEC_TIMING_END("IdMap");
    std::string header_host = headers["host"];
    size_t pos = header_host.find(':');

    // remove the port if present
    if (pos != std::string::npos) {
      header_host.erase(pos);
    }

    eos_static_debug("msg=\"connection/header\" header-host=\"%s\" "
                     "connection-host=\"%s\" real-ip=%s",
                     header_host.c_str(), headers["client-real-host"].c_str(),
                     headers["client-real-ip"].c_str());

    // if we have been mapped to nobody, change also the name accordingly
    if (vid->uid == 99) {
      vid->name = const_cast<char*>("nobody");
    }

    vid->dn = dn;
    vid->tident = tident.c_str();
  }
  return vid;
}

EOSMGMNAMESPACE_END
