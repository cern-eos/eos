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
EosMgmHttpHandler::MatchesPath(const char *verb, const char *path)
{
  if (EOS_LOGS_DEBUG) {
    eos_static_debug("verb=%s path=%s", verb, path);
  }
  return true;
}



int
EosMgmHttpHandler::ProcessReq(XrdHttpExtReq &req)
{
  std::string body;
  
  if ( req.verb == "PROPFIND" ) {
    // read the body
    body.resize(req.length);
    char* data = 0;
    int rbytes = req.BuffgetData(req.length, &data, true);
    body.assign(data, (size_t) rbytes);
  }

  std::map<std::string,std::string> cookies;

  // normalize the input headers to lower-case
  std::map<std::string,std::string> normalized_headers;
  for (auto it = req.headers.begin(); it != req.headers.end(); ++it) {
    normalized_headers[LC_STRING(it->first)] = it->second;
  }

  std::string query = normalized_headers.count("xrd-http-query")? normalized_headers["xrd-http-query"]: "";

  std::unique_ptr<eos::common::ProtocolHandler> handler = OFS->Httpd->XrdHttpHandler(req.verb,
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
    for ( auto it = headers.begin(); it != headers.end(); ++it) {
      if (it->first == "Content-Length") {
	// this is added by SendSimpleResp, don't add it here
	continue;
      }
      header += it->first;
      header += ": ";
      header += it->second;
      header += "\r\n";
    }

    if (headers.size()) {
      header.erase(header.length()-2);
    }
    if (EOS_LOGS_DEBUG) {
      eos_static_debug("response-header: %s", header.c_str());
    }

    return req.SendSimpleResp(response->GetResponseCode(), response->GetResponseCodeDescription().c_str(),
			      header.c_str(), response->GetBody().c_str(), response->GetBody().length());
  } else {
    std::string errmsg = "failed to create response object";
    return req.SendSimpleResp(500, errmsg.c_str(), "", errmsg.c_str(), errmsg.length());
  }
}

int 
EosMgmHttpHandler::Init(const char *cfgfile)
{
  if (getenv("EOSMGMOFS")) {
    OFS = (XrdMgmOfs*) (strtoull(getenv("EOSMGMOFS"),0,10));
  }
  return 0;
}

