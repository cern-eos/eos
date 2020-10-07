#include <stdio.h>
#include "XrdSfs/XrdSfsInterface.hh"
#include "common/Logging.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/http/HttpServer.hh"
#include "common/http/ProtocolHandler.hh"
#include "common/StringConversion.hh"
#include "common/Timing.hh"
#include "EosFstHttpHandler.hh"

XrdVERSIONINFO(XrdSfsGetFileSystem, EosFstHttp);

bool
EosFstHttpHandler::MatchesPath(const char* verb, const char* path)
{
  if (EOS_LOGS_DEBUG) {
    eos_static_debug("verb=%s path=%s", verb, path);
  }

  // Leave the XrdHttpTPC plugin deal with COPY/OPTIONS verbs
  if ((strcmp(verb, "COPY") == 0) || (strcmp(verb, "OPTIONS") == 0)) {
    return false;
  }

  return true;
}


int
EosFstHttpHandler::ProcessReq(XrdHttpExtReq& req)
{
  std::string body;

  if (!OFS) {
    eos_static_crit("OFS not accessible");
    return -1;
  }

  std::map<std::string, std::string> cookies;
  std::map<std::string, std::string> normalized_headers;

  // Normalize the input headers to lower-case
  for (const auto& hdr : req.headers) {
    eos_static_info("msg\"normalize hdr\" key=\"%s\" value=\"%s\"",
                    hdr.first.c_str(),  hdr.second.c_str());
    normalized_headers[LC_STRING(hdr.first)] = hdr.second;
  }

  std::string query = normalized_headers.count("xrd-http-query") ?
                      normalized_headers["xrd-http-query"] : "";
  std::string verb = req.verb;

  if (req.verb == "PUT") {
    // CREATE makes sure the handler just opens the file and all writes
    // are done later
    verb = "CREATE";
  }

  std::unique_ptr<eos::common::ProtocolHandler>
  handler = OFS->mHttpd->XrdHttpHandler(verb, req.resource, normalized_headers,
                                        query, cookies, body, req.GetSecEntity());

  if (handler == nullptr) {
    std::string errmsg = "failed to create handler";
    return req.SendSimpleResp(500, errmsg.c_str(), "", errmsg.c_str(),
                              errmsg.length());
  }

  eos::common::HttpResponse* response = handler->GetResponse();

  if (response) {
    std::string header;
    response->AddHeader("Date",  eos::common::Timing::utctime(time(NULL)));
    long long content_length = 0ll;
    auto headers = response->GetHeaders();

    for (auto it = headers.begin(); it != headers.end(); ++it) {
      if (it->first == "Content-Length") {
        continue;
      }

      header += it->first;
      header += ": ";
      header += it->second;
      header += "\r\n";
    }

    if (headers.size()) {
      header.erase(header.length() - 2);
    }

    if (EOS_LOGS_DEBUG) {
      eos_static_debug("response-header: %s", header.c_str());
    }

    if (req.verb == "HEAD") {
      return req.SendSimpleResp(response->GetResponseCode(),
                                response->GetResponseCodeDescription().c_str(),
                                header.c_str(), response->GetBody().c_str(),
                                response->GetBody().length());
    }

    if (req.verb == "GET") {
      // Need to update the content length determined while opening the file
      auto it_hd = headers.find("Content-Length");
      
      if (it_hd != headers.end()) {
        try {
          content_length = std::stoll(it_hd->second);
        } catch (...) {}
      }

      if (response->GetResponseCode() != 200) {
        return req.SendSimpleResp(response->GetResponseCode(),
                                  response->GetResponseCodeDescription().c_str(),
                                  header.c_str(), response->GetBody().c_str(),
                                  response->GetBody().length());
      } else {
        int retc = 0;
        retc = req.SendSimpleResp(0, response->GetResponseCodeDescription().c_str(),
                                  header.c_str(), 0 , content_length);

        if (retc) {
          return retc;
        }

        ssize_t nread = 0;
        off_t pos = 0;
        // allocate an IO buffer of 1M or if smaller the required content length
        std::vector<char> buffer(content_length > (1024 * 1024) ?
                                 (1024 * 1024) : content_length);

        do {
          if (EOS_LOGS_DEBUG) {
            eos_static_debug("pos=%llu size=%u", pos, buffer.capacity());
          }

          nread = OFS->mHttpd->FileReader(handler.get(), pos, &buffer[0],
                                          buffer.capacity());

          if (nread >= 0) {
            pos += nread;
            retc |= req.SendSimpleResp(1, 0, 0, &buffer[0], nread);
            eos_static_debug("retc=%d", retc);
          } else {
            retc = -1;
          }
        } while ((pos != content_length) && (nread > 0) && !retc);

        OFS->mHttpd->FileClose(handler.get(), retc);
        return retc;
      }
    }

    if (req.verb == "PUT") {
      // If no content-length provided then return an error
      if (normalized_headers.count("content-length") == 0) {
        response->SetResponseCode(eos::common::HttpResponse::LENGTH_REQUIRED);
      }

      if (EOS_LOGS_DEBUG) {
        eos_static_debug("response-code=%d", response->GetResponseCode());
      }

      if ((response->GetResponseCode() != 0) &&
          (response->GetResponseCode() != 200)) {
        return req.SendSimpleResp(response->GetResponseCode(),
                                  response->GetResponseCodeDescription().c_str(),
                                  header.c_str(), response->GetBody().c_str(),
                                  response->GetBody().length());
      } else {
        try {
          content_length = std::stoll(normalized_headers["content-length"]);
        } catch (...) {}

        if ((response->GetResponseCode() == 0) &&
            (normalized_headers.count("expect") &&
             (normalized_headers["expect"] == "100-continue"))) {
          // reply to 100-CONTINUE request
          if (EOS_LOGS_DEBUG) {
            eos_static_debug("sending 100-continue");
          }

          req.SendSimpleResp(100, nullptr, header.c_str(), "", 0);
        }

        int retc = 0;
        long long content_left = content_length;
        const long long eoshttp_sz = 1024 * 1024;
        const long long xrdhttp_sz = 256 * 1024;
        std::string body(eoshttp_sz + 1, '\0');

        do {
          long long content_read = std::min(eoshttp_sz, content_left);
          body.resize(content_read, '\0');
          char* ptr = body.data();
          long long read_len = 0;

          do {
            size_t chunk_len = std::min(xrdhttp_sz, content_read - read_len);
            int rb = req.BuffgetData(chunk_len, &ptr, true);
            read_len += rb;
            ptr += rb;
            eos_static_debug("content-read=%lli rb=%lu body=%u content_left=%lli",
                             content_read, rb, body.size(), content_left);

            if (!rb) {
              break;
            }
          } while (read_len < content_read);

          if (read_len != content_read) {
            eos_static_crit("msg=\"short read during PUT, expected %lu bytes"
                            " but got %lu bytes", content_read, read_len);
            retc = -1;
          } else {
            retc |= OFS->mHttpd->FileWriter(handler.get(),
                                            req.verb,
                                            req.resource,
                                            normalized_headers,
                                            query,
                                            cookies,
                                            body);

            if (!retc) {
              content_left -= content_read;
            }
          }
        } while (!retc && content_left);

        if (EOS_LOGS_DEBUG) {
          eos_static_debug("retc=%d", retc);
        }

        if (!retc) {
          // trigger the close handler by calling with empty body
          body.clear();
          retc |= OFS->mHttpd->FileWriter(handler.get(),
                                          req.verb,
                                          req.resource,
                                          normalized_headers,
                                          query,
                                          cookies,
                                          body);
        }

        eos::common::HttpResponse* response = handler->GetResponse();

        if (response && response->GetResponseCode()) {
          return req.SendSimpleResp(response->GetResponseCode(),
                                    response->GetResponseCodeDescription().c_str(),
                                    header.c_str(), response->GetBody().c_str(), response->GetBody().length());
        } else {
          return req.SendSimpleResp(500, "fatal internal error", "", "", 0);
        }
      }
    }

    return 0;
  } else {
    std::string errmsg = "failed to create response object";
    return req.SendSimpleResp(500, errmsg.c_str(), "", errmsg.c_str(),
                              errmsg.length());
  }
}

int
EosFstHttpHandler::Init(const char* cfgfile)
{
  if (getenv("EOSFSTOFS")) {
    OFS = (eos::fst::XrdFstOfs*)(strtoull(getenv("EOSFSTOFS"), 0, 10));
  }

  std::string cfg;
  eos::common::StringConversion::LoadFileIntoString(cfgfile, cfg);
  size_t fpos = cfg.find("xrd.protocol XrdHttp:");

  if (fpos != std::string::npos) {
    size_t epos = cfg.find(" ", fpos + 21);

    if (epos != std::string::npos) {
      std::string port = cfg.substr(fpos + 21, epos - fpos - 21);
      setenv("EOSFSTXRDHTTP", port.c_str(), 1);
      eos_static_notice("publishing HTTP port: %s", port.c_str());
    }
  }

  return 0;
}
