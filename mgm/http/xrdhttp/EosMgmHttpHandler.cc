#include <stdio.h>
#include "EosMgmHttpHandler.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "common/Logging.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm//http/HttpServer.hh"
#include "common/http/ProtocolHandler.hh"
#include "common/StringConversion.hh"
#include "common/Timing.hh"

XrdVERSIONINFO(XrdSfsGetFileSystem, EosMgmHttp);

bool
EosMgmHttpHandler::MatchesPath(const char* verb, const char* path)
{
  if (std::string(verb) == "POST") {
    return false;
  }

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("verb=%s path=%s", verb, path);
  }

  return true;
}

int
EosMgmHttpHandler::ProcessReq(XrdHttpExtReq& req)
{
  std::string body;

  if (req.verb == "PROPFIND") {
    // read the body
    body.resize(req.length);
    char* data = 0;
    int rbytes = req.BuffgetData(req.length, &data, true);
    body.assign(data, (size_t) rbytes);
  }

  std::map<std::string, std::string> cookies;
  // normalize the input headers to lower-case
  std::map<std::string, std::string> normalized_headers;

  for (auto it = req.headers.begin(); it != req.headers.end(); ++it) {
    normalized_headers[LC_STRING(it->first)] = it->second;
  }

  std::string query = normalized_headers.count("xrd-http-query") ?
                      normalized_headers["xrd-http-query"] : "";
  std::unique_ptr<eos::common::ProtocolHandler> handler =
    OFS->Httpd->XrdHttpHandler(req.verb,
                               req.resource,
                               normalized_headers,
                               query,
                               cookies,
                               body,
                               req.GetSecEntity());
  eos::common::HttpResponse* response = handler->GetResponse();

  if (response) {
    std::string header;
    response->AddHeader("Date",  eos::common::Timing::utctime(time(NULL)));
    auto headers = response->GetHeaders();

    for (auto it = headers.begin(); it != headers.end(); ++it) {
      std::string key = it->first;
      std::string val = it->second;

      if (key == "Content-Length") {
        // this is added by SendSimpleResp, don't add it here
        continue;
      }

      if (mRedirectToHttps) {
        if (key == "Location") {
          if (normalized_headers["xrd-http-prot"] == "https") {
            if (!normalized_headers.count("xrd-http-redirect-http") ||
                (normalized_headers["xrd-http-redirect-http"] == "0")) {
              // write http: as https:
              val.insert(4, "s");
            }
          }
        }
      }

      header += key;
      header += ": ";
      header += val;
      header += "\r\n";
    }

    if (headers.size()) {
      header.erase(header.length() - 2);
    }

    if (EOS_LOGS_DEBUG) {
      eos_static_debug("response-header: %s", header.c_str());
    }

    return req.SendSimpleResp(response->GetResponseCode(),
                              response->GetResponseCodeDescription().c_str(),
                              header.c_str(), response->GetBody().c_str(), response->GetBody().length());
  } else {
    std::string errmsg = "failed to create response object";
    return req.SendSimpleResp(500, errmsg.c_str(), "", errmsg.c_str(),
                              errmsg.length());
  }
}

int
EosMgmHttpHandler::Init(const char* cfgfile)
{
  if (getenv("EOSMGMOFS")) {
    OFS = (XrdMgmOfs*)(strtoull(getenv("EOSMGMOFS"), 0, 10));
    std::string cfg;
    eos::common::StringConversion::LoadFileIntoString(cfgfile, cfg);

    if (cfg.find("eos::mgm::http::redirect-to-https=1") != std::string::npos) {
      mRedirectToHttps = true;
    }

    eos_static_notice("configuration: redirect-to-https:%d", mRedirectToHttps);
  }

  return 0;
}

