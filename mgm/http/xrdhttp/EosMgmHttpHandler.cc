//------------------------------------------------------------------------------
//! file EosMgmHttpHandler.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "EosMgmHttpHandler.hh"
#include "common/Logging.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm//http/HttpServer.hh"
#include "common/http/ProtocolHandler.hh"
#include "common/StringConversion.hh"
#include "common/StringTokenizer.hh"
#include "common/StringUtils.hh"
#include "common/Timing.hh"
#include "common/Path.hh"
#include <XrdSec/XrdSecEntityAttr.hh>
#include <XrdSfs/XrdSfsInterface.hh>
#include <XrdSys/XrdSysPlugin.hh>
#include <XrdAcc/XrdAccAuthorize.hh>
#include <XrdOuc/XrdOucPinPath.hh>
#include <stdio.h>
#include "mgm/http/rest-api/manager/RestApiManager.hh"

XrdVERSIONINFO(XrdHttpGetExtHandler, EosMgmHttp);
static XrdVERSIONINFODEF(compiledVer, EosMgmHttp, XrdVNUMBER, XrdVERSION);

//------------------------------------------------------------------------------
//! Obtain an instance of the XrdHttpExtHandler object.
//!
//! This extern "C" function is called when a shared library plug-in containing
//! implementation of this class is loaded. It must exist in the shared library
//! and must be thread-safe.
//!
//! @param  eDest -> The error object that must be used to print any errors or
//!                  other messages (see XrdSysError.hh).
//! @param  confg -> Name of the configuration file that was used. This pointer
//!                  may be null though that would be impossible.
//! @param  parms -> Argument string specified on the namelib directive. It may
//!                  be null or point to a null string if no parms exist.
//! @param  myEnv -> Environment variables for configuring the external handler;
//!                  it my be null.
//!
//! @return Success: A pointer to an instance of the XrdHttpSecXtractor object.
//!         Failure: A null pointer which causes initialization to fail.
//!
//------------------------------------------------------------------------------
#define XrdHttpExtHandlerArgs XrdSysError       *eDest, \
                              const char        *confg, \
                              const char        *parms, \
                              XrdOucEnv         *myEnv

extern "C" XrdHttpExtHandler* XrdHttpGetExtHandler(XrdHttpExtHandlerArgs)
{
  auto handler = new EosMgmHttpHandler();

  if (handler->Init(confg)) {
    delete handler;
    return nullptr;
  }

  if (handler->Config(eDest, confg, parms, myEnv)) {
    eDest->Emsg("EosMgmHttpHandler", EINVAL, "Failed config of EosMgmHttpHandler");
    delete handler;
    return nullptr;
  }

  return (XrdHttpExtHandler*)handler;
}


//----------------------------------------------------------------------------
//! Destructor
//----------------------------------------------------------------------------
EosMgmHttpHandler::~EosMgmHttpHandler()
{
  eos_info("msg=\"call %s destructor\"", __FUNCTION__);
}

//------------------------------------------------------------------------------
// Configure the external request handler
//------------------------------------------------------------------------------
int
EosMgmHttpHandler::Config(XrdSysError* eDest, const char* confg,
                          const char* parms, XrdOucEnv* myEnv)
{
  using namespace eos::common;
  const std::string ofs_lib_tag = "xrootd.fslib";
  const std::string authz_lib_tag = "mgmofs.macaroonslib";
  std::list<std::string> authz_libs;
  std::string ofs_lib_path, http_ext_lib_path;
  std::string cfg;
  StringConversion::LoadFileIntoString(confg, cfg);
  auto lines = StringTokenizer::split<std::vector<std::string>>(cfg, '\n');

  for (auto& line : lines) {
    eos::common::trim(line);

    if (line.find("eos::mgm::http::redirect-to-https=1") != std::string::npos) {
      mRedirectToHttps = true;
    } else if (line.find(ofs_lib_tag) == 0) {
      ofs_lib_path = GetOfsLibPath(line);
      // XRootD guarantees that the XRootD protocol and its associated
      // plugins are loaded before HTTP therefore we can get a pointer
      // to the MGM OFS plugin
      mMgmOfsHandler = GetOfsPlugin(eDest, ofs_lib_path, confg);

      if (!mMgmOfsHandler) {
        eDest->Emsg("Config", "failed to get MGM OFS plugin pointer");
        return 1;
      }
    } else if (line.find(authz_lib_tag) == 0) {
      authz_libs = GetAuthzLibPaths(line);
      http_ext_lib_path = GetHttpExtLibPath(line);

      if (authz_libs.empty() || http_ext_lib_path.empty()) {
        eos_err("msg=\"wrong mgmofs.macaroonslib configuration\" data=\"%s\"",
                line.c_str());
        return 1;
      }
    }
  }

  if (authz_libs.empty() || http_ext_lib_path.empty())  {
    eos_notice("%s", "msg=\"mgmofs.macaroonslib configuration missing so "
               "there is no token authorization support\"");
    return 0;
  }

  if (!mMgmOfsHandler || !mMgmOfsHandler->mMgmAuthz) {
    eos_err("%s", "msg=\"missing MGM OFS handler or MGM AUTHZ handler\"");
    return 1;
  }

  eos_notice("configuration: redirect-to-https:%d", mRedirectToHttps);

  // Load the XrdHttpExHandler plugin from the XrdMacaroons library which
  // is always on the first position
  if (!(mTokenHttpHandler = GetHttpExtPlugin(eDest, *authz_libs.begin(),
                            confg, myEnv))) {
    return 1;
  }

  // The chaining of the authz libs always has the XrdAccAuthorize plugin
  // from the MGM in the last postion as a fallback. Therefore, we can
  // have the following combinations:
  // libXrdMacaroons.so -> libEosMgmOfs.so
  // libXrdMacaroons.so -> libXrdAccSciTokens.so -> libEosMgmOfs.so
  XrdAccAuthorize* authz {nullptr};
  XrdAccAuthorize* chain_authz = (XrdAccAuthorize*)mMgmOfsHandler->mMgmAuthz;

  for (auto it = authz_libs.rbegin(); it != authz_libs.rend(); ++it) {
    eos_info("msg=\"chaining XrdAccAuthorize object\" lib=\"%s\"", it->c_str());

    try {
      if (!(authz = GetAuthzPlugin(eDest, *it, confg, myEnv, chain_authz))) {
        eos_err("msg=\"failed to chain XrdAccAuthorize plugin\" lib=\"%s\"",
                it->c_str());
        return 1;
      }
    } catch (const std::exception& e) {
      eos_err("msg=\"caught exception\" msg=\"%s\"", e.what());
      return 1;
    }

    chain_authz = authz;
  }

  eos_info("%s", "msg=\"successfully chained the XrdAccAuthorizeObject "
           "plugins and updated the MGM token authorization handler\"");
  mTokenAuthzHandler = authz;
  mMgmOfsHandler->SetTokenAuthzHandler(mTokenAuthzHandler);
  return 0;
}

//------------------------------------------------------------------------------
// Decide if current handler should be invoked
//------------------------------------------------------------------------------
bool
EosMgmHttpHandler::MatchesPath(const char* verb, const char* path)
{
  eos_static_info("verb=%s path=%s", verb, path);

  // Leave the XrdHttpTPC plugin deal with COPY/OPTIONS verbs
  if ((strcmp(verb, "COPY") == 0) || (strcmp(verb, "OPTIONS") == 0)) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Process the HTTP request and send the response using by calling the
// XrdHttpProtocol directly
//------------------------------------------------------------------------------
int
EosMgmHttpHandler::ProcessReq(XrdHttpExtReq& req)
{
  std::string body;
  // @todo(esindril): handle redirection to new MGM master if the
  // current one is a slave

  // Stop accepting requests if the MGM started the shutdown procedure
  if (mMgmOfsHandler->Shutdown) {
    std::string errmsg = "MGM daemon is shutting down";
    return req.SendSimpleResp(500, errmsg.c_str(), nullptr, errmsg.c_str(),
                              errmsg.length());
  }

  // Normalize the input headers to lower-case
  std::map<std::string, std::string> normalized_headers;

  for (const auto& hdr : req.headers) {
    eos_static_debug("msg=\"normalize hdr\" key=\"%s\" value=\"%s\"",
                    hdr.first.c_str(), hdr.second.c_str());
    std::string lc_string = LC_STRING(hdr.first);
    normalized_headers[lc_string] = hdr.second;

    if (lc_string == "authorization") {
      eos_static_debug("msg=\"normalize hdr\" key=\"%s\" value=\"%s\"",
                       hdr.first.c_str(), hdr.second.c_str());
    }
  }

  if (IsMacaroonRequest(req)) {
    if (mTokenHttpHandler) {
      // Delegate request to the XrdMacaroons library
      eos_info("%s", "msg=\"delegate request to XrdMacaroons library\"");
      return ProcessMacaroonPOST(req);
    } else {
      std::string errmsg = "POST request not supported";
      return req.SendSimpleResp(404, errmsg.c_str(), nullptr, errmsg.c_str(),
                                errmsg.length());
    }
  }

  if (mMgmOfsHandler->mRestGrpcSrv && IsRestApiRequest(req)) {
    return ProcessRestApiPost(req, normalized_headers);
  }

  bool is_rest_req = mMgmOfsHandler->mRestApiManager->isRestRequest(
                       req.resource);

  if (is_rest_req) {
    std::optional<int> retCode = readBody(req, body);

    if (retCode) {
      return retCode.value();
    }
  }

  if (req.verb == "PROPFIND" && !is_rest_req) {
    // read the body
    body.resize(req.length);
    char* data = 0;
    int rbytes = req.BuffgetData(req.length, &data, true);
    body.assign(data, (size_t) rbytes);
  }

  std::string err_msg;
  std::map<std::string, std::string> cookies;
  std::unique_ptr<eos::common::ProtocolHandler> handler =
    mMgmOfsHandler->mHttpd->XrdHttpHandler
    (req.verb, req.resource, normalized_headers, cookies, body, req.GetSecEntity(),
     mTokenAuthzHandler, err_msg);

  if (handler == nullptr) {
    return req.SendSimpleResp(500, err_msg.c_str(), "", err_msg.c_str(),
                              err_msg.length());
  }

  eos::common::HttpResponse* response = handler->GetResponse();

  if (response == nullptr) {
    std::string errmsg = "failed to create response object";
    return req.SendSimpleResp(500, errmsg.c_str(), nullptr, errmsg.c_str(),
                              errmsg.length());
  }

  std::ostringstream oss_header;
  response->AddHeader("Date",  eos::common::Timing::utctime(time(NULL)));
  const auto& headers = response->GetHeaders();

  for (const auto& hdr : headers) {
    std::string key = hdr.first;
    std::string val = hdr.second;

    // This is added by SendSimpleResp, don't add it here
    if (key == "Content-Length") {
      continue;
    }

    if (mRedirectToHttps) {
      if (key == "Location") {
        if (normalized_headers["xrd-http-prot"] == "https") {
          if (!normalized_headers.count("xrd-http-redirect-http") ||
              (normalized_headers["xrd-http-redirect-http"] == "0")) {
            // Re-write http: as https:
            val.insert(4, "s");
          }
        }
      }
    }

    if (!oss_header.str().empty()) {
      oss_header << "\r\n";
    }

    oss_header << key << ": " << val;
  }

  eos_debug("response-header=\"%s\"", oss_header.str().c_str());

  if (req.verb == "HEAD") {
    long long content_length = 0;
    auto it = headers.find("Content-Length");

    if (it != headers.end()) {
      try {
        content_length = std::stoll(it->second);
      } catch (...) {}
    }

    return req.SendSimpleResp(response->GetResponseCode(),
                              response->GetResponseCodeDescription().c_str(),
                              oss_header.str().c_str(),
                              nullptr, content_length);
  } else {
    return req.SendSimpleResp(response->GetResponseCode(),
                              response->GetResponseCodeDescription().c_str(),
                              oss_header.str().c_str(),
                              response->GetBody().c_str(),
                              response->GetBody().length());
  }
}

//------------------------------------------------------------------------------
// Process macaroon POST request
//------------------------------------------------------------------------------
int
EosMgmHttpHandler::ProcessMacaroonPOST(XrdHttpExtReq& req)
{
  auto& sec_entity = const_cast<XrdSecEntity&>(req.GetSecEntity());

  // If the XrdSecEntity comes with VOMS extensions then we need to call the
  // eos vid mapping funcationality to actually determine the local user
  // mapping which could be different then the one embedded in the GSI auth.
  // This happens for example when we have VOMS mapping enabled in eos vid
  if (sec_entity.vorg && (sec_entity.vorg[0] != '\0')) {
    std::unique_ptr<eos::common::VirtualIdentity>
    vid_tmp {new eos::common::VirtualIdentity()};
    std::string stident = "https.0:0@" + std::string(sec_entity.host);
    eos::common::Mapping::IdMap(&sec_entity, "", stident.c_str(), *vid_tmp);

    if (!vid_tmp->uid_string.empty()) {
      free(sec_entity.name);
      sec_entity.name = strndup(vid_tmp->uid_string.c_str(),
                                vid_tmp->uid_string.length());
    }
  }

  return mTokenHttpHandler->ProcessReq(req);
}

//------------------------------------------------------------------------------
// Process rest api gw POST request
//------------------------------------------------------------------------------
int
EosMgmHttpHandler::ProcessRestApiPost(XrdHttpExtReq& req,
                                      const HdrsMapT& norm_hdrs)
{
  std::string errmsg;
  // Extract request body
  std::string body;
  std::optional<int> retCode = readBody(req, body);

  if (retCode) {
    return retCode.value();
  }

  // Extract command name from the resource path
  // To do so search for the last occurrence of '/'
  std::string eosCommand;
  size_t lastSlashPos = req.resource.rfind('/');

  if (lastSlashPos != std::string::npos &&
      lastSlashPos + 1 < req.resource.length()) {
    // Extract the command string
    eosCommand = req.resource.substr(lastSlashPos + 1);
  } else {
    errmsg = "invalid input string";
    eos_static_err("msg=\"%s\"", errmsg.c_str());
    return req.SendSimpleResp(500, errmsg.c_str(), "", errmsg.c_str(),
                              errmsg.length());
  }

  // Initialize curl object
  CURL* curl = curl_easy_init();

  if (!curl) {
    errmsg = "failed to initialize curl object";
    eos_static_err("msg=\"%s\"", errmsg.c_str());
    return req.SendSimpleResp(500, errmsg.c_str(), "", errmsg.c_str(),
                              errmsg.length());
  }

  // Set the URL for the POST request
  const std::string url = std::string(mRestApiGwUrl) + std::string(
                            mRestApiGwPath) + eosCommand;
  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  // Set the HTTP method to POST
  curl_easy_setopt(curl, CURLOPT_POST, 1L);

  // Forward relevant headers for auth
  if (!RestApiGwFrwAuthHeaders(curl, req.GetSecEntity(), norm_hdrs)) {
    errmsg = "failure while forwarding auth headers";
    eos_static_err("msg=\"%s\"", errmsg.c_str());
    return req.SendSimpleResp(500, errmsg.c_str(), "", errmsg.c_str(),
                              errmsg.length());
  }

  // Set the request data
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
  // Response data will be stored in this string
  std::string responseData;
  // Set the callback function to handle response data
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, EosMgmHttpHandler::WriteCallback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseData);
  // Perform the HTTP request
  CURLcode curlRes = curl_easy_perform(curl);
  int res = 0;

  if (curlRes != CURLE_OK) {
    std::string errmsg = std::string("curl_easy_perform() failed: ") +
                         curl_easy_strerror(curlRes);
    res = req.SendSimpleResp(500, errmsg.c_str(), "", errmsg.c_str(),
                             errmsg.length());
  } else {
    // HTTP request successful, send the response
    res = req.SendSimpleResp(200, responseData.c_str(), "", responseData.c_str(),
                             responseData.length());
  }

  // Clean up and free resources
  curl_easy_cleanup(curl);
  return res;
}


//----------------------------------------------------------------------------
// Forward the authentication relevant info as custom headers to the
// GRPC-gateway that will then send them further down to the GRPC server
//----------------------------------------------------------------------------
bool
EosMgmHttpHandler::RestApiGwFrwAuthHeaders(CURL* curl,
    const XrdSecEntity& client,
    const HdrsMapT& norm_hdrs)
{
  static const std::string hdr_prefix = "Grpc-Metadata-";
  static const std::string authz_hdr = "authorization";
  struct curl_slist* list = NULL;
  list = curl_slist_append(list, SSTR(hdr_prefix << "client-name: "
                                      << client.name).c_str());
  list = curl_slist_append(list, SSTR(hdr_prefix << "client-tident: "
                                      << "https.0:0@"
                                      << client.host).c_str());
  auto it_authz = norm_hdrs.find(authz_hdr);

  if (it_authz != norm_hdrs.end()) {
    list = curl_slist_append(list, SSTR(hdr_prefix << "client-authorization: "
                                        << it_authz->second).c_str());
  }

  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, list);
  return true;
}

//----------------------------------------------------------------------------
// Function used to handle responses from grpc server
//----------------------------------------------------------------------------
size_t
EosMgmHttpHandler::WriteCallback(void* contents, size_t size, size_t nmemb,
                                 std::string* output)
{
  size_t total_size = size * nmemb;
  output->append(static_cast<char*>(contents), total_size);
  return total_size;
}

//------------------------------------------------------------------------------
// Get OFS library path from the given configuration
//------------------------------------------------------------------------------
std::string
EosMgmHttpHandler::GetOfsLibPath(const std::string& cfg_line)
{
  using namespace eos::common;
  std::string lib_path;
  auto tokens = StringTokenizer::split<std::vector<std::string>>(cfg_line, ' ');

  if (tokens.size() < 2) {
    eos_err("msg=\"failed parsing xrootd.fslib directive\" line=\"%s\"",
            cfg_line.c_str());
    return lib_path;
  }

  eos::common::trim(tokens[1]);
  lib_path = tokens[1];

  // Account for different specifications of the OFS plugin
  if (lib_path == "-2")  {
    if (tokens.size() < 3) {
      eos_err("msg=\"failed parsing xrootd.fslib directive\" line=\"%s\"",
              cfg_line.c_str());
      lib_path.clear();
      return lib_path;
    }

    eos::common::trim(tokens[2]);
    lib_path = tokens[2];
  }

  return lib_path;
}

//------------------------------------------------------------------------------
// Get list of external authorization libraries present in the configuration.
// If multiple are present then the order is kept to properly apply chaining
// to these libraries.
//------------------------------------------------------------------------------
std::list<std::string>
EosMgmHttpHandler::GetAuthzLibPaths(const std::string& cfg_line)
{
  using namespace eos::common;
  std::list<std::string> authz_libs;
  auto tokens = StringTokenizer::split<std::vector<std::string>>(cfg_line, ' ');

  if (tokens.size() < 2) {
    eos_err("msg=\"missing mgmofs.macaroonslib configuration\" "
            "tokens_sz=%i", tokens.size());
    return authz_libs;
  }

  // The first one MUST BE the XrdMacroons lib
  eos::common::trim(tokens[1]);
  authz_libs.push_back(tokens[1]);

  // Enable also the SciTokens library if present in the configuration
  if (tokens.size() > 2) {
    eos::common::trim(tokens[2]);
    authz_libs.push_back(tokens[2]);
  }

  return authz_libs;
}

//----------------------------------------------------------------------------
// Get XrdHttpExHandler library path from the given configuration
//----------------------------------------------------------------------------
std::string
EosMgmHttpHandler::GetHttpExtLibPath(const std::string& cfg_line)
{
  using namespace eos::common;
  auto tokens = StringTokenizer::split<std::vector<std::string>>(cfg_line, ' ');

  if (tokens.size() < 2) {
    eos_err("msg=\"missing mgmofs.macaroonslib configuration\" "
            "tokens_sz=%i", tokens.size());
    return std::string();
  }

  // The first one MUST BE the XrdMacroons lib
  eos::common::trim(tokens[1]);
  return tokens[1];
}

//------------------------------------------------------------------------------
// Get a pointer to the MGM OFS plug-in
//------------------------------------------------------------------------------
XrdMgmOfs*
EosMgmHttpHandler::GetOfsPlugin(XrdSysError* eDest, const std::string& lib_path,
                                const char* confg)
{
  char resolve_path[2048];
  bool no_alt_path {false};
  XrdMgmOfs* mgm_ofs_handler {nullptr};

  if (!XrdOucPinPath(lib_path.c_str(), no_alt_path, resolve_path,
                     sizeof(resolve_path))) {
    eDest->Emsg("Config", "Failed to locate the MGM OFS library path for ",
                lib_path.c_str());
    return mgm_ofs_handler;
  }

  // Try to load the XrdSfsGetFileSystem from the library (libXrdEosMgm.so)
  XrdSfsFileSystem *(*ep)(XrdSfsFileSystem*, XrdSysLogger*, const char*);
  std::string ofs_symbol {"XrdSfsGetFileSystem"};
  XrdSysPlugin ofs_plugin(eDest, resolve_path, "mgmofs", &compiledVer, 1);
  void* ofs_addr = ofs_plugin.getPlugin(ofs_symbol.c_str(), 0, 0);
  ofs_plugin.Persist();

  if (ofs_addr == nullptr) {
    eDest->Emsg("Config", "Failed to get the OFS plugin address from ",
                lib_path.c_str());
    return mgm_ofs_handler;
  }

  ep = (XrdSfsFileSystem * (*)(XrdSfsFileSystem*, XrdSysLogger*, const char*))
       (ofs_addr);
  XrdSfsFileSystem* sfs_fs {nullptr};

  if (!(ep && (sfs_fs = ep(nullptr, eDest->logger(), confg)))) {
    eDest->Emsg("Config", "Failed loading XrdSfsFileSystem from ",
                lib_path.c_str());
    return mgm_ofs_handler;
  }

  mgm_ofs_handler = static_cast<XrdMgmOfs*>(sfs_fs);
  eos_info("msg=\"successfully loaed XrdSfsFileSystem\" mgm_plugin_addr=%p",
           mgm_ofs_handler);
  return mgm_ofs_handler;
}

//------------------------------------------------------------------------------
// Get a pointer to the XrdHttpExtHandler plugin
//------------------------------------------------------------------------------
XrdHttpExtHandler*
EosMgmHttpHandler::GetHttpExtPlugin(XrdSysError* eDest,
                                    const std::string& lib_path,
                                    const char* confg, XrdOucEnv* myEnv)
{
  bool no_alt_path = false;
  char resolve_path[2048];
  XrdHttpExtHandler* http_ptr {nullptr};

  if (!XrdOucPinPath(lib_path.c_str(), no_alt_path, resolve_path,
                     sizeof(resolve_path))) {
    eos_err("msg=\"failed to locate library path\" lib=\"%s\"",
            lib_path.c_str());
    return http_ptr;
  }

  eos_info("msg=\"loading HttpExtHandler(XrdMacaroons) plugin\" path=\"%s\"",
           resolve_path);
  XrdHttpExtHandler *(*ep)(XrdHttpExtHandlerArgs);
  std::string http_symbol {"XrdHttpGetExtHandler"};
  XrdSysPlugin http_plugin(eDest, resolve_path, "httpexthandler",
                           &compiledVer, 1);
  void* http_addr = http_plugin.getPlugin(http_symbol.c_str(), 0, 0);
  http_plugin.Persist();
  ep = (XrdHttpExtHandler * (*)(XrdHttpExtHandlerArgs))(http_addr);

  if (!http_addr) {
    eos_err("msg=\"no XrdHttpGetExtHandler entry point in library\" "
            "lib=\"%s\"", resolve_path);
    return http_ptr;
  }

  // Add a pointer to the MGM authz handler so that it can be used by the
  // macaroons library to get access permissions for token requests
  myEnv->PutPtr("XrdAccAuthorize*", (void*)mMgmOfsHandler->mMgmAuthz);

  if (ep && (http_ptr = ep(eDest, confg, (const char*) nullptr, myEnv))) {
    eos_info("msg=\"successfully loaded XrdHttpGetExtHandler\" lib=\"%s\"",
             resolve_path);
  } else {
    eos_err("msg=\"failed loading XrdHttpGetExtHandler\" lib=\"%s\"",
            resolve_path);
  }

  return http_ptr;
}

//------------------------------------------------------------------------------
// Get a pointer to the XrdAccAuthorize plugin present in the given library
//------------------------------------------------------------------------------
XrdAccAuthorize*
EosMgmHttpHandler::GetAuthzPlugin(XrdSysError* eDest,
                                  const std::string& lib_path,
                                  const char* confg, XrdOucEnv* myEnv,
                                  XrdAccAuthorize* to_chain)
{
  bool no_alt_path = false;
  char resolve_path[2048];
  XrdAccAuthorize* authz_ptr {nullptr};

  if (!XrdOucPinPath(lib_path.c_str(), no_alt_path, resolve_path,
                     sizeof(resolve_path))) {
    eos_err("msg=\"failed to locate library path\" lib=\"%s\"",
            lib_path.c_str());
    return authz_ptr;
  }

  eos_info("msg=\"loading XrdAccAuthorize plugin\" lib=\"%s\"", resolve_path);
  XrdAccAuthorize *(*authz_add_ep)(XrdSysLogger*, const char*, const char*,
                                   XrdOucEnv*, XrdAccAuthorize*);
  std::string authz_add_symbol {"XrdAccAuthorizeObjAdd"};
  XrdSysPlugin authz_add_plugin(eDest, resolve_path, "authz", &compiledVer, 1);
  void* authz_addr = authz_add_plugin.getPlugin(authz_add_symbol.c_str(), 0, 0);
  authz_add_plugin.Persist();
  authz_add_ep = (XrdAccAuthorize * (*)(XrdSysLogger*, const char*, const char*,
                                        XrdOucEnv*, XrdAccAuthorize*))(authz_addr);

  if (authz_add_ep &&
      (authz_ptr = authz_add_ep(eDest->logger(), confg, nullptr, myEnv, to_chain))) {
    eos_info("msg=\"successfully loaded XrdAccAuthorizeObject\" lib=\"%s\" ptr=%p",
             resolve_path, authz_ptr);
  } else {
    eos_err("msg=\"failed loading XrdAccAuthorizeObject\" lib=\"%s\"",
            resolve_path);
  }

  return authz_ptr;
}

//------------------------------------------------------------------------------
// Reads the body of the XrdHttpExtReq object and put it in the body string
//------------------------------------------------------------------------------
std::optional<int> EosMgmHttpHandler::readBody(XrdHttpExtReq& req,
    std::string& body)
{
  std::optional<int> returnCode;
  body.reserve(req.length);
  const unsigned long long eoshttp_sz = 1024 * 1024;
  const unsigned long long xrdhttp_sz = 256 * 1024;
  unsigned long long contentLeft = req.length;
  std::string bodyTemp;

  do {
    unsigned long long contentToRead = std::min(eoshttp_sz, contentLeft);
    bodyTemp.clear();
    bodyTemp.reserve(contentToRead);
    char* data = nullptr;
    unsigned long long dataRead = 0;

    do {
      size_t chunk_len = std::min(xrdhttp_sz, contentToRead - dataRead);
      int bytesRead = req.BuffgetData(chunk_len, &data, true);
      eos_static_debug("contentToRead=%lli rb=%i body=%u contentLeft=%lli",
                       contentToRead, bytesRead, body.size(), contentLeft);

      if (bytesRead > 0) {
        bodyTemp.append(data, bytesRead);
        dataRead += bytesRead;
      } else if (bytesRead == -1) {
        std::ostringstream oss;
        oss << "msg=\"In EosMgmHttpHandler::ProcessReq(), unable to read the "
            << "body of the request coming from the user. Internal XRootD Http"
            << " request buffer error\"";
        eos_static_err(oss.str().c_str());
        std::string errorMsg = "Http server error: unable to read the request received";
        return req.SendSimpleResp(500, errorMsg.c_str(), nullptr, errorMsg.c_str(),
                                  errorMsg.length());
      } else {
        break;
      }
    } while (dataRead < contentToRead);

    contentLeft -= dataRead;
    body += bodyTemp;
  } while (contentLeft);

  return returnCode;
}

//------------------------------------------------------------------------------
// Returns true if the request is a macaroon token request false otherwise
//------------------------------------------------------------------------------
bool EosMgmHttpHandler::IsMacaroonRequest(const XrdHttpExtReq& req) const
{
  if (req.verb == "POST") {
    const auto& contentTypeItor = req.headers.find("Content-Type");

    if (contentTypeItor != req.headers.end()) {
      if (contentTypeItor->second == "application/macaroon-request") {
        return true;
      }
    }
  }

  return false;
}

//------------------------------------------------------------------------------
// Returns true if the request is a rest api gateway token request
// false otherwise
// @todo(esindril) this should be moved in the GrpcRestGwServer
//------------------------------------------------------------------------------
bool EosMgmHttpHandler::IsRestApiRequest(const XrdHttpExtReq& req) const
{
  if (req.verb == "POST") {
    const auto& resourcePath = req.resource.find(mRestApiGwPath);

    if (resourcePath != std::string::npos) {
      return true;
    }
  }

  return false;
}
