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
#include "XrdSec/XrdSecEntityAttr.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSys/XrdSysPlugin.hh"
#include "XrdAcc/XrdAccAuthorize.hh"
#include "XrdOuc/XrdOucPinPath.hh"
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

//------------------------------------------------------------------------------
// Do a "rough" mapping between HTTP verbs and access operation types
// @todo(esindril): this should be improved and used when deciding what type
// of operation the current access requires
//------------------------------------------------------------------------------
Access_Operation MapHttpVerbToAOP(const std::string& http_verb)
{
  Access_Operation op = AOP_Any;

  if (http_verb == "GET") {
    op = AOP_Read;
  } else if (http_verb == "PUT") {
    op = AOP_Create;
  } else if (http_verb == "DELETE") {
    op = AOP_Delete;
  } else {
    op  = AOP_Stat;
  }

  return op;
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
      eos_err("msg=\"caught execption\" msg=\"%s\"", e.what());
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

  if (IsMacaroonRequest(req)) {
    if (mTokenHttpHandler) {
      // Delegate request to the XrdMacaroons library
      eos_info("%s", "msg=\"delegate request to XrdMacaroons library\"");
      return mTokenHttpHandler->ProcessReq(req);
    } else {
      std::string errmsg = "POST request not supported";
      return req.SendSimpleResp(404, errmsg.c_str(), nullptr, errmsg.c_str(),
                                errmsg.length());
    }
  }

  bool isRestRequest = mMgmOfsHandler->mRestApiManager->isRestRequest(
                         req.resource);

  if (isRestRequest) {
    std::optional<int> retCode = readBody(req, body);

    if (retCode) {
      return retCode.value();
    }
  }

  if (req.verb == "PROPFIND" && !isRestRequest) {
    // read the body
    body.resize(req.length);
    char* data = 0;
    int rbytes = req.BuffgetData(req.length, &data, true);
    body.assign(data, (size_t) rbytes);
  }

  // Normalize the input headers to lower-case
  std::map<std::string, std::string> normalized_headers;

  for (const auto& hdr : req.headers) {
    eos_static_info("msg=\"normalize hdr\" key=\"%s\" value=\"%s\"",
                    hdr.first.c_str(), hdr.second.c_str());
    normalized_headers[LC_STRING(hdr.first)] = hdr.second;
  }

  std::string query;
  const XrdSecEntity& client = req.GetSecEntity();
  bool s3_access = false;

  if (normalized_headers.count("authorization")) {
    if (normalized_headers["authorization"].substr(0, 3) == "AWS") {
      s3_access = true;
    }
  }

  // Native XrdHttp access - not nginx and not S3
  if ((normalized_headers.find("x-forwarded-for") == normalized_headers.end()) &&
      !s3_access) {
    std::string path;
    std::unique_ptr<XrdOucEnv> env_opaque;
    Access_Operation oper = MapHttpVerbToAOP(req.verb);

    if (!BuildPathAndEnvOpaque(normalized_headers, path, env_opaque)) {
      const std::string errmsg = "conflicting authorization info present";
      eos_static_err("msg=\"%s\" path=\"%s\"", errmsg.c_str(), path.c_str());
      return req.SendSimpleResp(400, errmsg.c_str(), nullptr, errmsg.c_str(),
                                errmsg.length());
    }

    if (mTokenAuthzHandler &&
        mTokenAuthzHandler->Access(&client, path.c_str(), oper,
                                   env_opaque.get()) == XrdAccPriv_None) {
      eos_static_err("msg=\"(token) authorization failed\" path=\"%s\"",
                     path.c_str());
      std::string errmsg = "token authorization failed";
      return req.SendSimpleResp(403, errmsg.c_str(), nullptr, errmsg.c_str(),
                                errmsg.length());
    }

    if (client.name == nullptr) {
      // Check if we have the request.name in the attributes of the XrdSecEntity
      // object which should contain the client username that the request
      // belogs to
      const std::string user_key = "request.name";
      std::string user_value;

      if (client.eaAPI->Get(user_key, user_value)) {
        eos_static_info("msg=\"(token) authorization done\" client_name=\"%s\" "
                        "client_request.name=\"%s\" client_prot=\"%s\"",
                        client.name, user_value.c_str(), client.prot);
      } else {
        eos_static_info("msg=\"(token) authorization done but no username "
                        "found\" client_prot=%s", client.prot);
      }
    } else {
      eos_static_info("msg=\"(token) authorization done\" client_name=\"%s\" "
                      "client_prot=\"%s\"", client.name, client.prot);
    }

    query = (normalized_headers.count("xrd-http-query") ?
             normalized_headers["xrd-http-query"] : "");
  }

  std::map<std::string, std::string> cookies;
  std::unique_ptr<eos::common::ProtocolHandler> handler =
    mMgmOfsHandler->mHttpd->XrdHttpHandler(req.verb, req.resource,
        normalized_headers, query, cookies,
        body, client);

  if (handler == nullptr) {
    std::string errmsg = "failed to create handler";
    return req.SendSimpleResp(500, errmsg.c_str(), nullptr, errmsg.c_str(),
                              errmsg.length());
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

  eos_debug("response-header: %s", oss_header.str().c_str());

  if (req.verb == "HEAD") {
    long long content_length = 0ll;
    auto it = headers.find("Content-Length");

    if (it != headers.end()) {
      try {
        content_length = std::stoll(it->second);
      } catch (...) {}
    }

    return req.SendSimpleResp(response->GetResponseCode(),
                              response->GetResponseCodeDescription().c_str(),
                              oss_header.str().c_str(), nullptr, content_length);
  } else {
    return req.SendSimpleResp(response->GetResponseCode(),
                              response->GetResponseCodeDescription().c_str(),
                              oss_header.str().c_str(), response->GetBody().c_str(),
                              response->GetBody().length());
  }
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
    eos_err("msg=\"failed parsing xrootd.ofslib directive\" line=\"%s\"",
            cfg_line.c_str());
    return lib_path;
  }

  eos::common::trim(tokens[1]);
  lib_path = tokens[1];

  // Account for different specifications of the OFS plugin
  if (lib_path == "-2")  {
    if (tokens.size() < 3) {
      eos_err("msg=\"failed parsing xrootd.ofslib directive\" line=\"%s\"",
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
bool EosMgmHttpHandler::IsMacaroonRequest(const XrdHttpExtReq& req)
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
// Build path and opaque information based on the HTTP headers
//------------------------------------------------------------------------------
bool
EosMgmHttpHandler::BuildPathAndEnvOpaque(const
    std::map<std::string, std::string>&
    normalized_headers, std::string& path,
    std::unique_ptr<XrdOucEnv>& env_opaque)
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

  path = it->second;
  std::string opaque;
  size_t pos = path.find('?');

  if ((pos != std::string::npos) && (pos != path.length())) {
    opaque = path.substr(pos + 1);
    path = path.substr(0, pos);
    eos::common::Path canonical_path(path);
    path = canonical_path.GetFullPath().c_str();
  }

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

  env_opaque = std::make_unique<XrdOucEnv>(opaque.c_str(), opaque.length());
  return true;
}
