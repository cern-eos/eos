//------------------------------------------------------------------------------
//! @file EosFstHttpHandler.hh
//! @author Andreas-Joachim Peters & Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
// Helper function to convert hex to decimal
//------------------------------------------------------------------------------
static int decode_hex(int ch)
{
  if ('0' <= ch && ch <= '9') {
    return ch - '0';
  } else if ('A' <= ch && ch <= 'F') {
    return ch - 'A' + 0xa;
  } else if ('a' <= ch && ch <= 'f') {
    return ch - 'a' + 0xa;
  } else {
    return -1;
  }
}

//------------------------------------------------------------------------------
// Initialize handler
//------------------------------------------------------------------------------
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
      eos_static_notice("publishing XrdHttp port: %s", port.c_str());
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// Check if request should be handled by the current handler
//------------------------------------------------------------------------------
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

//------------------------------------------------------------------------------
// Process current request
//------------------------------------------------------------------------------
int
EosFstHttpHandler::ProcessReq(XrdHttpExtReq& req)
{
  std::string body;

  if (!OFS) {
    eos_static_crit("%s", "msg=\"OFS not accessible\"");
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

  std::string verb = req.verb;
  std::string query = normalized_headers.count("xrd-http-query") ?
                      normalized_headers["xrd-http-query"] : "";

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

  if (!response) {
    std::string errmsg = "failed to create response object";
    return req.SendSimpleResp(500, errmsg.c_str(), "", errmsg.c_str(),
                              errmsg.length());
  }

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

  eos_static_debug("response-header: %s", header.c_str());

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

    if ((response->GetResponseCode() != response->OK) &&
        (response->GetResponseCode() != response->PARTIAL_CONTENT)) {
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
        eos_static_debug("pos=%llu size=%u", pos, buffer.capacity());
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
    bool is_chunked = (normalized_headers.count("transfer-encoding") &&
                       (normalized_headers["transfer-encoding"] == "chunked"));

    // If no content-length provided then return an error
    if ((normalized_headers.count("content-length") == 0) && !is_chunked) {
      response->SetResponseCode(eos::common::HttpResponse::LENGTH_REQUIRED);
    }

    eos_static_debug("response-code=%d", response->GetResponseCode());

    if ((response->GetResponseCode() != 0) &&
        (response->GetResponseCode() != 200)) {
      return req.SendSimpleResp(response->GetResponseCode(),
                                response->GetResponseCodeDescription().c_str(),
                                header.c_str(), response->GetBody().c_str(),
                                response->GetBody().length());
    }

    if (is_chunked) {
      if (!HandleChunkUpload(req, handler.get(), normalized_headers, cookies,
                             query)) {
        return req.SendSimpleResp(500, "fatal internal error", "during chunk upload",
                                  "", 0);
      }
    } else {
      try {
        content_length = std::stoll(normalized_headers["content-length"]);
      } catch (...) {}

      if ((response->GetResponseCode() == 0) &&
          (normalized_headers.count("expect") &&
           (normalized_headers["expect"] == "100-continue"))) {
        // reply to 100-CONTINUE request
        eos_static_debug("%s", "msg=\"sending 100-continue\"");
        req.SendSimpleResp(100, nullptr, header.c_str(), "", 0);
      }

      int retc = 0;
      long long content_left = content_length;
      const long long eoshttp_sz = 1024 * 1024;
      const long long xrdhttp_sz = 256 * 1024;
      std::string body;

      do {
        long long content_read = std::min(eoshttp_sz, content_left);
        body.clear();
        body.reserve(content_read);
        char* ptr = nullptr;
        long long read_len = 0;

        do {
          size_t chunk_len = std::min(xrdhttp_sz, content_read - read_len);
          int rb = req.BuffgetData(chunk_len, &ptr, true);
          eos_static_debug("content-read=%lli rb=%i body=%u content_left=%lli",
                           content_read, rb, body.size(), content_left);

          if (rb > 0) {
            body.append(ptr, rb);
            read_len += rb;
          } else {
            break;
          }
        } while (read_len < content_read);

        if (read_len != content_read) {
          eos_static_crit("msg=\"short read during PUT, expected %lu bytes"
                          " but got %lu bytes", content_read, read_len);
          retc = -1;
        } else {
          retc |= OFS->mHttpd->FileWriter(handler.get(), req.verb, req.resource,
                                          normalized_headers, query, cookies, body);

          if (!retc) {
            content_left -= content_read;
          }
        }
      } while (!retc && content_left);

      eos_static_debug("retc=%d", retc);

      if (!retc) {
        // trigger the close handler by calling with empty body
        body.clear();
        retc |= OFS->mHttpd->FileWriter(handler.get(), req.verb, req.resource,
                                        normalized_headers, query, cookies, body);
      }
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

  return 0;
}

//------------------------------------------------------------------------------
// Handle chunk upload operation
//------------------------------------------------------------------------------
bool
EosFstHttpHandler::HandleChunkUpload(XrdHttpExtReq& req,
                                     eos::common::ProtocolHandler* handler,
                                     std::map<std::string, std::string>& norm_hdrs,
                                     std::map<std::string, std::string>& cookies,
                                     std::string& query)
{
  bool success = false;
  const unsigned long long xrdhttp_sz = 256 * 1024;
  const int max_size = 4096;
  std::string ssize;
  std::string chunk;
  char* ptr = nullptr;
  eos::common::Timing tm("ChunkUpload");
  COMMONTIMING("START", &tm);

  while (true) {
    bool has_size = false;
    ssize.clear();

    // Read in line containing the chunk size
    while (ssize.length() < max_size) {
      if (req.BuffgetData(1, &ptr, true) == 1) {
        ssize.append(ptr, 1);
      }

      size_t len = ssize.length();

      if ((len >= 2) && (ssize[len - 2] == '\r') && (ssize[len - 1] == '\n')) {
        ssize.erase(len - 2);
        has_size = true;
        break;
      }
    }

    if (!has_size) {
      break;
    }

    // Get numeric value for the chunk size
    unsigned long long chunk_sz = 0ull;

    try {
      size_t pos = 0;
      chunk_sz =  std::stoull(ssize, &pos, 16);

      if (pos != ssize.length()) {
        throw std::runtime_error("failed to convert chunk size");
      }
    } catch (...) {
      eos_static_err("msg=\"chunk size is not a number\" data=\"%s\"",
                     ssize.c_str());
      break;
    }

    chunk.clear();
    chunk.reserve(chunk_sz);

    // This is the final byte, read in the CRLF ("\r\n")
    if (chunk_sz == 0) {
      if (req.BuffgetData(2, &ptr, true) != 2) {
        eos_static_err("%s", "msg=\"failed reading end message for chunk upload\"");
        break;
      }

      if ((*ptr != '\r') || (*(++ptr) != '\n')) {
        eos_static_err("%s", "msg=\"chunk upload end message not what we expected\"");
        break;
      }
    } else { // This is normal chunk with data, read it in and write to the file
      unsigned long long read_len = 0ull;

      do {
        size_t block_len = std::min(xrdhttp_sz, chunk_sz);
        int rb = req.BuffgetData(block_len, &ptr, true);

        if (rb > 0) {
          chunk.append(ptr, rb);
          read_len += rb;
        } else {
          eos_static_err("msg=\"failed to read chunk block\" block_len=%llu",
                         block_len);
          break;
        }
      } while (read_len < chunk_sz);

      // We read less than we expected, malformed chunk request
      if (read_len != chunk_sz) {
        eos_static_err("msg=\"chunk size less than what we expected\" len=%llu "
                       "expected=%llu", read_len, chunk_sz);
        break;
      }

      // Read also the line separator CRLF ("\r\n")
      if (req.BuffgetData(2, &ptr, true) != 2) {
        eos_static_err("%s", "msg=\"failed reading end message for chunk upload\"");
        break;
      }

      if ((*ptr != '\r') || (*(++ptr) != '\n')) {
        eos_static_err("%s", "msg=\"chunk upload end message not what we expected\"");
        break;
      }
    }

    // Write the chunk to the file. Last chunk with size 0 will trigger the
    // close handler
    //eos_static_info("msg=\"writing chunk\" len=%llu data=\"%s\"",
    //                chunk.length(), chunk.c_str());
    size_t wb = (size_t) OFS->mHttpd->FileWriter(handler, req.verb, req.resource,
                norm_hdrs, query, cookies, chunk);

    if (wb) {
      eos_static_err("msg=\"failed writing chunk to file\" chunk_sz=%llu",
                     chunk.length());
      break;
    }

    if (chunk.length() == 0) {
      success = true;
      break;
    }
  }

  COMMONTIMING("done", &tm);

  if (EOS_LOGS_DEBUG) {
    tm.Print();
  }

  return success;
}

//------------------------------------------------------------------------------
// Handle chunk upload operation - optimised version
//------------------------------------------------------------------------------
bool
EosFstHttpHandler::HandleChunkUpload2(XrdHttpExtReq& req,
                                      eos::common::ProtocolHandler* handler,
                                      std::map<std::string, std::string>& norm_hdrs,
                                      std::map<std::string, std::string>& cookies,
                                      std::string& query)
{
  enum {CHUNK_SIZE, CHUNK_CLRF1, CHUNK_CLRF2, CHUNK_DATA, ERROR};
  int retries = 0;
  const int max_retries = 5;
  const unsigned long long xrdhttp_sz = 256 * 1024;
  const unsigned long long eoshttp_sz = 1024 * 1024;
  char* ptr = nullptr, *end_ptr = nullptr;
  std::string chunk;
  chunk.reserve(eoshttp_sz);
  eos::common::Timing tm("ChunkUpload");
  COMMONTIMING("START", &tm);
  int state = CHUNK_SIZE;
  int nread = 0;
  int hex_count = 0;
  long int chunk_sz = 0;
  bool final_chunk = false;

  while (true) {
    eos_static_info("%s", "msg=\"calling BuffgetData\"");
    nread = req.BuffgetData(xrdhttp_sz, &ptr, false);
    end_ptr = ptr + nread;
    eos_static_info("msg=\"http read\" nread=%li", nread);

    if (nread < 0) {
      eos_static_err("%s", "msg=\"got a socket error from XrdHttp\"");
      state = ERROR;
      break;
    } else if (nread == 0) {
      ++retries;

      if (retries > max_retries) {
        eos_static_err("%s", "msg=\"reached the maximum number of retries\"");
        state = ERROR;
        break;
      } else {
        eos_static_warning("msg=\"wait for more data\" retry=%i", retries);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        continue;
      }
    }

    while (end_ptr - ptr != 0) {
      switch (state) {
      case CHUNK_SIZE:
        int v;

        if ((v = decode_hex(*ptr)) == -1) {
          if (hex_count == 0) {
            state = ERROR;
          } else {
            eos_static_info("msg=\"got chunk size\" chunk_sz=%li", chunk_sz);
            state = CHUNK_CLRF1;
          }
        } else {
          chunk_sz = chunk_sz * 16 + v;
          ++hex_count;
          ++ptr;
        }

        break;

      case CHUNK_CLRF1:
        if (*ptr != '\r') {
          state = ERROR;
        } else {
          state = CHUNK_CLRF2;
          ++ptr;
        }

        break;

      case CHUNK_CLRF2:
        if (*ptr != '\n') {
          state = ERROR;
        } else {
          eos_static_info("%s", "msg=\"done reading CLRF\"");
          ++ptr;

          if (hex_count) {
            // Entering after CHUNK_SIZE
            hex_count = 0;
            state = CHUNK_DATA;
          } else {
            // Entering after CHUNK_DATA
            state = CHUNK_SIZE;
          }
        }

        break;

      case CHUNK_DATA:
        if (chunk_sz == 0) {
          if (final_chunk) {
            eos_static_info("%s", "msg=\"done reading final chunk\"");
            break;
          } else {
            // This is the final chunk
            final_chunk = true;
            state = CHUNK_CLRF1;
            eos_static_info("%s", "msg=\"do read final chunk\"");
          }
        } else if (chunk_sz <= end_ptr - ptr) {
          eos_static_info("msg=\"add data to chunk [1]\" sz=%li", chunk_sz);
          chunk.append(ptr, chunk_sz);
          ptr += chunk_sz;
          chunk_sz = 0;
          state = CHUNK_CLRF1;
        } else {
          eos_static_info("msg=\"add data to chunk [2]\" sz=%li", (end_ptr - ptr));
          chunk.append(ptr, end_ptr - ptr);
          chunk_sz -= (end_ptr - ptr);
          ptr = end_ptr;
        }

        break;

      case ERROR:
        break;
      }

      if ((state == ERROR) ||
          (final_chunk && (state = CHUNK_DATA))) {
        break;
      }
    }

    if (state == ERROR) {
      eos_static_err("%s", "msg=\"error state\"");
      break;
    }

    // Write the chunk to the file. Last chunk with size 0 will trigger the
    // close handler
    if ((final_chunk && (state == CHUNK_DATA)) ||
        (chunk.size() >= eoshttp_sz)) {
      eos_static_info("msg=\"writing chunk\" len=%llu", chunk.length());
      size_t wb = (size_t) OFS->mHttpd->FileWriter(handler, req.verb, req.resource,
                  norm_hdrs, query, cookies, chunk);

      if (wb) {
        eos_static_err("msg=\"failed writing chunk to file\" chunk_sz=%llu",
                       chunk.length());
        state = ERROR;
        break;
      }

      chunk.clear();

      // For final chunk also trigger write of 0 length which closes the file
      if (final_chunk) {
        size_t wb = (size_t) OFS->mHttpd->FileWriter(handler, req.verb, req.resource,
                    norm_hdrs, query, cookies, chunk);

        if (wb) {
          eos_static_err("msg=\"failed writing chunk to file\" chunk_sz=%llu",
                         chunk.length());
          state = ERROR;
        }

        break;
      }
    }
  }

  COMMONTIMING("done", &tm);

  if (EOS_LOGS_DEBUG) {
    tm.Print();
  }

  return (state == CHUNK_DATA);
}
