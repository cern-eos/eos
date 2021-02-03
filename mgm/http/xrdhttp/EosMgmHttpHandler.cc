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
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSys/XrdSysPlugin.hh"
#include "XrdAcc/XrdAccAuthorize.hh"
#include "XrdOuc/XrdOucPinPath.hh"
#include <stdio.h>

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

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
OwningXrdSecEntity::~OwningXrdSecEntity()
{
  if (mSecEntity) {
    free(mSecEntity->name);
    free(mSecEntity->host);
    free(mSecEntity->vorg);
    free(mSecEntity->role);
    free(mSecEntity->grps);
    free(mSecEntity->endorsements);
    free(mSecEntity->moninfo);
    free(mSecEntity->creds);
    free(mSecEntity->addrInfo);
    free((char*)mSecEntity->tident);
  }
}

//------------------------------------------------------------------------------
// Copy XrdSecEntity info
//------------------------------------------------------------------------------
void
OwningXrdSecEntity::CreateFrom(const XrdSecEntity& other)
{
  if (mSecEntity == nullptr) {
    mSecEntity = std::make_unique<XrdSecEntity>();
  }

  mSecEntity->Reset();
  strncpy(mSecEntity->prot, other.prot, XrdSecPROTOIDSIZE);

  if (other.name) {
    mSecEntity->name = strdup(other.name);
  }

  if (other.host) {
    mSecEntity->host = strdup(other.host);
  }

  if (other.vorg) {
    mSecEntity->vorg = strdup(other.vorg);
  }

  if (other.role) {
    mSecEntity->role = strdup(other.role);
  }

  if (other.grps) {
    mSecEntity->grps = strdup(other.grps);
  }

  if (other.endorsements) {
    mSecEntity->endorsements = strdup(other.endorsements);
  }

  if (other.moninfo) {
    mSecEntity->moninfo = strdup(other.moninfo);
  }

  if (other.creds) {
    mSecEntity->creds = strdup(other.creds);
  }

  mSecEntity->credslen = other.credslen;
  //mSecEntity->rsvd = other.rsvd;
  // @note addrInfo is not copied for the moment
  mSecEntity->addrInfo = nullptr;

  if (other.tident) {
    mSecEntity->tident = strdup(other.tident);
  }

  // @note sessvar is null
  mSecEntity->sessvar = nullptr;
  *mSecEntity;
}

//------------------------------------------------------------------------------
// Standardise VOMS info so that HTTP and XRootD populate the XrdSecEntity in
// a similar way
//------------------------------------------------------------------------------
void
OwningXrdSecEntity::StandardiseVOMS()
{
  // No voms info
  if ((mSecEntity->grps == nullptr) || (strlen(mSecEntity->grps) == 0)) {
    return;
  }

  // The vorg is properly populated we just need to tweak the grps and
  // endorsements fields. The grps info provided by the secxtractor is in the
  // form: '/dteam /dteam/Role=NULL /dteam/Role=NULL/Capability=NULL'
  std::string voms_info {mSecEntity->grps};
  auto tokens = eos::common::StringTokenizer::split<std::vector<std::string>>
                (voms_info, ' ');

  if (tokens.size() <= 1) {
    return;
  }

  voms_info = *tokens.rbegin();
  free(mSecEntity->endorsements);
  mSecEntity->endorsements = strdup(voms_info.c_str());
  // Extract the group info
  size_t pos = voms_info.find("/Role=");
  free(mSecEntity->grps);
  mSecEntity->grps = strdup(voms_info.substr(0, pos).c_str());

  // Extract the role info
  if (pos != std::string::npos) {
    voms_info.erase(0, pos + strlen("/Role="));
    pos = voms_info.find("/Capability=");
    free(mSecEntity->role);
    mSecEntity->role = nullptr;

    if (voms_info.substr(0, pos) != "NULL") {
      mSecEntity->role = strdup(voms_info.substr(0, pos).c_str());
    }
  }
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
  std::string macaroons_lib_path {""};
  std::string scitokens_lib_path {""};

  // XRootD guarantees that the XRootD protocol and its associated plugins are
  // loaded before HTTP therefore we can get a pointer to the MGM OFS plugin
  if (!GetOfsPlugin(eDest, confg, myEnv)) {
    eDest->Emsg("Config", "failed to get MGM OFS plugin pointer");
    return 1;
  }

  std::string cfg;
  StringConversion::LoadFileIntoString(confg, cfg);
  auto lines = StringTokenizer::split<std::vector<std::string>>(cfg, '\n');

  for (auto& line : lines) {
    eos::common::trim(line);

    if (line.find("eos::mgm::http::redirect-to-https=1") != std::string::npos) {
      mRedirectToHttps = true;
    } else if (line.find("mgmofs.macaroonslib") == 0) {
      auto tokens = StringTokenizer::split<std::vector<std::string>>(line, ' ');

      if (tokens.size() < 2) {
        eos_err("%s", "msg=\"missing mgmofs.macaroonslib configuration\"");
        eos_err("tokens_size=%i", tokens.size());
        return 1;
      }

      macaroons_lib_path = tokens[1];
      eos::common::trim(macaroons_lib_path);

      // Enable also the SciTokens library if present in the configuration
      if (tokens.size() > 2) {
        scitokens_lib_path = tokens[2];
        eos::common::trim(scitokens_lib_path);
      }
    }
  }

  if (macaroons_lib_path.empty()) {
    eos_err("%s", "msg=\"missing mandatory mgmofs.macaroonslib config\"");
    return 1;
  }

  eos_notice("configuration: redirect-to-https:%d", mRedirectToHttps);
  // Try to load the XrdHttpGetExtHandler from the libXrdMacaroons library
  bool no_alt_path = false;
  char resolve_path[2048];

  if (!XrdOucPinPath(macaroons_lib_path.c_str(), no_alt_path, resolve_path,
                     sizeof(resolve_path))) {
    eos_err("msg=\"failed to locate library path\" lib=\"%s\"",
            macaroons_lib_path.c_str());
    return 1;
  }

  eos_info("msg=\"loading XrdMacaroons(http) plugin\" path=\"%s\"",
           resolve_path);
  XrdHttpExtHandler *(*ep)(XrdHttpExtHandlerArgs);
  std::string http_symbol {"XrdHttpGetExtHandler"};
  XrdSysPlugin tokens_plugin(eDest, resolve_path, "macaroonslib", &compiledVer,
                             1);
  void* http_addr = tokens_plugin.getPlugin(http_symbol.c_str(), 0, 0);
  tokens_plugin.Persist();
  ep = (XrdHttpExtHandler * (*)(XrdHttpExtHandlerArgs))(http_addr);

  if (!http_addr) {
    eos_err("msg=\"no XrdHttpGetExtHandler entry point in library\" "
            "lib=\"%s\"", macaroons_lib_path.c_str());
    return 1;
  }

  // Add a pointer to the MGM authz handler so that it can be used by the
  // macaroons library to get access persissions for token requests
  myEnv->PutPtr("XrdAccAuthorize*", (void*)mMgmOfsHandler->mMgmAuthz);

  if (ep && (mTokenHttpHandler = ep(eDest, confg, (const char*) nullptr,
                                    myEnv))) {
    eos_info("%s", "msg=\"XrdHttpGetExthandler from libXrdMacaroons loaded "
             "successfully\"");
  } else {
    eos_err("%s", "msg=\"failed loading XrdHttpGetExtHandler from "
            "libXrdMacaroons\"");
    return 1;
  }

  // Load the XrdAccAuthorizeObject provided by the libXrdMacaroons library
  XrdAccAuthorize *(*authz_ep)(XrdSysLogger*, const char*, const char*);
  std::string authz_symbol {"XrdAccAuthorizeObject"};
  void* authz_addr = tokens_plugin.getPlugin(authz_symbol.c_str(), 0, 0);
  authz_ep = (XrdAccAuthorize * (*)(XrdSysLogger*, const char*,
                                    const char*))(authz_addr);
  // The "authz_parms" argument needs to be set to
  // [<sci_tokens_lib_path>] <libEosMgmOfs.so>
  // so that the XrdMacaroons library properly chains the various authz plugins
  std::string authz_parms = "libXrdEosMgm.so";

  if (!scitokens_lib_path.empty()) {
    std::ostringstream oss;
    oss <<  scitokens_lib_path << " " << authz_parms;;
    authz_parms = oss.str();
  }

  if (authz_ep &&
      (mTokenAuthzHandler = authz_ep(eDest->logger(), confg,
                                     (authz_parms.empty() ?
                                      nullptr : authz_parms.c_str())))) {
    eos_info("%s", "msg=\"XrdAccAuthorizeObject from libXrdMacaroons loaded "
             "successfully\"");
    mMgmOfsHandler->SetTokenAuthzHandler(mTokenAuthzHandler);
  } else {
    eos_err("%s", "msg=\"failed loading XrdAccAuthorizeObject from "
            "libXrdMacaroons\"");
    return 1;
  }

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
  using eos::common::StringConversion;
  std::string body;
  // @todo(esindril): handle redirection to new MGM master if the
  // current one is a slave

  if (req.verb == "POST") {
    // Delegate request to the XrdMacaroons library
    eos_info("%s", "msg=\"delegate request to XrdMacaroons library\"");
    return mTokenHttpHandler->ProcessReq(req);
  }

  if (req.verb == "PROPFIND") {
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
  OwningXrdSecEntity client(req.GetSecEntity());
  client.StandardiseVOMS();

  // Native XrdHttp access
  if (normalized_headers.find("x-forwarded-for") == normalized_headers.end()) {
    std::string authz_data = (normalized_headers.count("authorization") ?
                              normalized_headers["authorization"] : "");
    std::string path = normalized_headers["xrd-http-fullresource"];
    eos::common::Path canonical_path(path);
    path = canonical_path.GetFullPath().c_str();
    std::string enc_authz = StringConversion::curl_default_escaped(authz_data);
    Access_Operation oper = MapHttpVerbToAOP(req.verb);
    std::string data = "authz=";
    data += enc_authz;
    std::unique_ptr<XrdOucEnv> env = std::make_unique<XrdOucEnv>(data.c_str(),
                                     data.length());

    // Make a copy of the original XrdSecEntity so that the authorization plugin
    // can update the name of the client from the macaroon info
    if (mTokenAuthzHandler->Access(client.GetObj(), path.c_str(), oper,
                                   env.get()) == XrdAccPriv_None) {
      eos_static_err("msg=\"(token) authorization failed\" path=\"%s\"",
                     path.c_str());
      std::string errmsg = "token authorization failed";
      return req.SendSimpleResp(403, errmsg.c_str(), "", errmsg.c_str(),
                                errmsg.length());
    }

    eos_static_info("msg=\"(token) authorization done\" client_name=%s "
                    " client_prot=%s", client.GetObj()->name,
                    client.GetObj()->prot);
    query = (normalized_headers.count("xrd-http-query") ?
             normalized_headers["xrd-http-query"] : "");
  }

  std::map<std::string, std::string> cookies;
  std::unique_ptr<eos::common::ProtocolHandler> handler =
    mMgmOfsHandler->mHttpd->XrdHttpHandler(req.verb, req.resource,
        normalized_headers, query, cookies,
        body, *client.GetObj());

  if (handler == nullptr) {
    std::string errmsg = "failed to create handler";
    return req.SendSimpleResp(500, errmsg.c_str(), "", errmsg.c_str(),
                              errmsg.length());
  }

  eos::common::HttpResponse* response = handler->GetResponse();

  if (response == nullptr) {
    std::string errmsg = "failed to create response object";
    return req.SendSimpleResp(500, errmsg.c_str(), "", errmsg.c_str(),
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
// Get a pointer to the MGM OFS plug-in
//------------------------------------------------------------------------------
bool
EosMgmHttpHandler::GetOfsPlugin(XrdSysError* eDest, const std::string& confg,
                                XrdOucEnv* myEnv)
{
  using namespace eos::common;
  std::string cfg;
  StringConversion::LoadFileIntoString(confg.c_str(), cfg);
  auto lines = StringTokenizer::split<std::vector<std::string>>(cfg, '\n');

  for (const auto& line : lines) {
    if (line.find("xrootd.fslib") == 0) {
      auto tokens = StringTokenizer::split<std::vector<std::string>>(line, ' ');

      if (tokens.size() < 2) {
        eDest->Emsg("Config", "Failed parsing xrootd.fslib directive", "");
        break;
      }

      std::string lib = tokens[1];

      // Account for different specifications of the OFS plugin
      if (lib == "-2")  {
        if (tokens.size() < 3) {
          eDest->Emsg("Config", "Failed parsing xrootd.fslib directive", "");
          break;
        }

        lib = tokens[2];
      }

      char resolve_path[2048];
      bool no_alt_path {false};

      if (!XrdOucPinPath(lib.c_str(), no_alt_path, resolve_path,
                         sizeof(resolve_path))) {
        eDest->Emsg("Config", "Failed to locate the MGM OFS library path for ",
                    lib.c_str());
        break;
      }

      // Try to load the XrdSfsGetFileSystem from the libXrdEosMgm library
      XrdSfsFileSystem *(*ep)(XrdSfsFileSystem*, XrdSysLogger*, const char*);
      std::string ofs_symbol {"XrdSfsGetFileSystem"};
      XrdSysPlugin ofs_plugin(eDest, resolve_path, "mgmofs", &compiledVer, 1);
      void* ofs_addr = ofs_plugin.getPlugin(ofs_symbol.c_str(), 0, 0);
      ofs_plugin.Persist();
      ep = (XrdSfsFileSystem * (*)(XrdSfsFileSystem*, XrdSysLogger*, const char*))
           (ofs_addr);
      XrdSfsFileSystem* sfs_fs {nullptr};

      if (!(ep && (sfs_fs = ep(nullptr, eDest->logger(), confg.c_str())))) {
        eDest->Emsg("Config", "Failed loading XrdSfsFileSystem from "
                    "libXrdEosMgm");
        break;
      }

      mMgmOfsHandler = static_cast<XrdMgmOfs*>(sfs_fs);
      eos_info("msg=\"XrdSfsFileSystem from libXrdEosMgm loaded successfully\""
               " mgm_plugin_addr=%p", mMgmOfsHandler);
      break;
    }
  }

  return (mMgmOfsHandler != nullptr);
}
