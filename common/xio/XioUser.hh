//------------------------------------------------------------------------------
// User bearer for the HTTP/2 listener.
//
// The daemon token is never returned here. A uid other than the caller is
// accepted only from a token file owned by that uid.
//------------------------------------------------------------------------------
#ifndef EOS_COMMON_XIO_USER_HH
#define EOS_COMMON_XIO_USER_HH

#include "XioBearer.hh"
#include "XioHttp2.hh"
#include "XioUrl.hh"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <string>
#include <strings.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace eos {
namespace common {

inline bool XioIsInternalToken(const std::string& token)
{
  const char* internal = std::getenv("EOS_XIO_INTERNAL_TOKEN");
  return internal && internal[0] && token == internal;
}

inline std::string XioUserBearer(uid_t uid)
{
  if (uid == static_cast<uid_t>(-1)) {
    return {};
  }
  if (uid == ::geteuid()) {
    if (const char* env = std::getenv("BEARER_TOKEN")) {
      if (env[0] && !XioIsInternalToken(env)) {
        return env;
      }
    }
    if (const char* az = std::getenv("EOSAUTHZ")) {
      std::string token = az;
      if (token.compare(0, 9, "Bearer%20") == 0) {
        token.erase(0, 9);
      } else if (token.size() >= 7 &&
                 ::strncasecmp(token.c_str(), "Bearer ", 7) == 0) {
        token.erase(0, 7);
      }
      if (!token.empty() && !XioIsInternalToken(token)) {
        return token;
      }
    }
  }
  std::string token;
  std::string err;
  if (XioFS::loadBearerToken(uid, token, err) && !XioIsInternalToken(token)) {
    return token;
  }
  return {};
}

struct XioCall {
  std::string bearer;
  bool krb5{false};
  std::string krb5cc;
  std::string cert;
  std::string key;
  std::string cacert;
  bool verify_peer{false};
  // "name" or "name endorsement". Sent as Authorization: SSS on https only.
  std::string sss;
  // Numeric unix identity. Sent as x-eos-uid and x-eos-gid on https only.
  bool unix_id{false};
  uint32_t uid{0};
  uint32_t gid{0};
  // POST body. Used for fusex opaque queries, which may contain nuls.
  std::string upload;
  // Extra request headers, such as Depth for a directory listing.
  std::vector<std::pair<std::string, std::string>> extra;
};

// Process login: bearer file, Kerberos ccache, client certificate, or the
// process uid and gid over https. scheme is "http" or "https".
inline bool XioProcessCall(XioCall& call, std::string& scheme)
{
  scheme = "http";
  call.bearer = XioUserBearer(geteuid());
  if (call.bearer.empty()) {
    if (const char* cc = std::getenv("KRB5CCNAME")) {
      if (cc[0]) {
        call.krb5 = true;
        call.krb5cc = cc;
      }
    }
  }
  if (call.bearer.empty() && !call.krb5) {
    const char* cert = std::getenv("X509_USER_PROXY");
    if (!cert || !cert[0]) {
      cert = std::getenv("X509_USER_CERT");
    }
    if (cert && cert[0]) {
      const char* key = std::getenv("X509_USER_KEY");
      call.cert = cert;
      call.key = (key && key[0]) ? key : cert;
      scheme = "https";
    }
  }
  if (call.bearer.empty() && !call.krb5 && call.cert.empty()) {
    scheme = "https";
    call.unix_id = true;
    call.uid = static_cast<uint32_t>(geteuid());
    call.gid = static_cast<uint32_t>(getegid());
  }
  if (const char* ca = std::getenv("EOS_MGM_XIO_CA")) {
    call.cacert = ca;
    call.verify_peer = true;
    if (call.cert.empty() && std::getenv("EOS_MGM_XIO_TLS")) {
      scheme = "https";
    }
  }
  return true;
}

struct XioHeadInfo {
  std::string content_length;
  std::string mode;
  std::string mtime;
  std::string atime;
};

// method is GET, HEAD, or POST. url is http:// or https://host:port/path?query.
// http_status is 0 when the listener is not reached.
inline int XioUserExchange(const std::string& method, const std::string& url,
                           const XioCall& call, std::string& body,
                           std::string& err, int* http_status,
                           XioHeadInfo* head = nullptr)
{
  body.clear();
  err.clear();
  if (http_status) {
    *http_status = 0;
  }
  if (!call.bearer.empty() && XioIsInternalToken(call.bearer)) {
    err = "refusing the daemon token";
    return EPERM;
  }
  if (call.bearer.empty() && !call.krb5 && call.cert.empty() && call.sss.empty() &&
      !call.unix_id) {
    err = "no user credential";
    return EPERM;
  }
  XioFS::Url parsed;
  if (!XioFS::parseUrl(url, parsed, err)) {
    return EINVAL;
  }
  XioFS::Http2Session::Options opt;
  opt.verify_peer = call.verify_peer;
  opt.cacert = call.cacert;
  opt.timeout_ms = 30000;
  opt.bearer = call.bearer;
  opt.krb5 = call.krb5;
  opt.krb5cc = call.krb5cc;
  opt.cert = call.cert;
  opt.key = call.key;
  XioFS::Http2Session session;
  int rc = session.connect(parsed, opt, err);
  if (rc) {
    return rc < 0 ? -rc : rc;
  }
  XioFS::HttpRequest req;
  req.method = method;
  req.path = parsed.path.empty() ? "/" : parsed.path;
  if (!call.sss.empty()) {
    req.headers.emplace_back("authorization", "SSS " + call.sss);
  }
  if (call.unix_id) {
    req.headers.emplace_back("x-eos-uid", std::to_string(call.uid));
    req.headers.emplace_back("x-eos-gid", std::to_string(call.gid));
  }
  for (const auto& hdr : call.extra) {
    req.headers.push_back(hdr);
  }
  if (!call.upload.empty()) {
    req.body = call.upload;
  }
  XioFS::HttpResponse resp;
  rc = session.request(req, resp, err);
  if (rc) {
    return rc < 0 ? -rc : rc;
  }
  if (http_status) {
    *http_status = resp.status;
  }
  if (resp.status != 200 && resp.status != 201 && resp.status != 204 &&
      resp.status != 207) {
    err = resp.body.empty() ? ("HTTP " + std::to_string(resp.status)) : resp.body;
    return resp.status == 403 ? EACCES : EIO;
  }
  body = std::move(resp.body);
  if (head) {
    head->content_length = resp.header("content-length");
    head->mode = resp.header("x-xio-mode");
    head->mtime = resp.header("x-xio-mtime");
    head->atime = resp.header("x-xio-atime");
  }
  return 0;
}

inline int XioUserExchange(const std::string& method, const std::string& url,
                           const std::string& bearer, std::string& body,
                           std::string& err, int* http_status)
{
  XioCall call;
  call.bearer = bearer;
  return XioUserExchange(method, url, call, body, err, http_status);
}

// Stat a root:// or xiohttp(s):// URL with the process login.
// Returns 0 on success. http_status is 0 when the listener is not reached.
inline int XioUserStatUrl(const std::string& url, struct stat* st,
                          std::string& err, int* http_status)
{
  if (st) {
    memset(st, 0, sizeof(*st));
  }
  err.clear();
  if (http_status) {
    *http_status = 0;
  }
  XioCall call;
  std::string scheme = "http";
  XioProcessCall(call, scheme);
  std::string http = url;
  if (http.compare(0, 7, "root://") == 0) {
    const char* port_env = std::getenv("EOS_MGM_XIO_PORT");
    const int port = port_env ? std::atoi(port_env) : 0;
    if (port <= 0) {
      err = "HTTP/2 listener is not configured";
      return EIO;
    }
    std::string rest = http.substr(7);
    const auto slash = rest.find('/');
    std::string host = slash == std::string::npos ? rest : rest.substr(0, slash);
    std::string path = slash == std::string::npos ? "/" : rest.substr(slash);
    const auto at = host.find('@');
    if (at != std::string::npos) {
      host.erase(0, at + 1);
    }
    const auto colon = host.rfind(':');
    if (colon != std::string::npos) {
      host.erase(colon);
    }
    while (path.size() > 1 && path[0] == '/' && path[1] == '/') {
      path.erase(0, 1);
    }
    http = scheme + "://" + host + ":" + std::to_string(port) + path;
  } else if (http.compare(0, 3, "xio") == 0) {
    http.erase(0, 3);
  }
  std::string body;
  XioHeadInfo head;
  int http_st = 0;
  const int rc = XioUserExchange("HEAD", http, call, body, err, &http_st, &head);
  if (http_status) {
    *http_status = http_st;
  }
  if (rc || !st) {
    return rc ? rc : EINVAL;
  }
  st->st_size = static_cast<off_t>(std::strtoll(head.content_length.c_str(), nullptr, 10));
  if (!head.mode.empty()) {
    st->st_mode = static_cast<mode_t>(std::strtoul(head.mode.c_str(), nullptr, 10));
  } else {
    st->st_mode = S_IFREG | 0644;
  }
  if (!head.mtime.empty()) {
    st->st_mtime = static_cast<time_t>(std::strtoll(head.mtime.c_str(), nullptr, 10));
  }
  if (!head.atime.empty()) {
    st->st_atime = static_cast<time_t>(std::strtoll(head.atime.c_str(), nullptr, 10));
  }
  st->st_nlink = 1;
  return 0;
}

struct XioListEnt {
  std::string name;
  bool dir{false};
  uint64_t size{0};
  mode_t mode{0};
};

inline std::string
XioXmlText(const std::string& chunk, const char* open_tag, const char* close_tag)
{
  const auto start = chunk.find(open_tag);
  if (start == std::string::npos) {
    return {};
  }
  const auto from = start + std::strlen(open_tag);
  const auto end = chunk.find(close_tag, from);
  if (end == std::string::npos) {
    return {};
  }
  std::string text = chunk.substr(from, end - from);
  std::string out;
  for (size_t i = 0; i < text.size(); ++i) {
    if (text.compare(i, 5, "&amp;") == 0) {
      out.push_back('&');
      i += 4;
    } else if (text.compare(i, 4, "&lt;") == 0) {
      out.push_back('<');
      i += 3;
    } else {
      out.push_back(text[i]);
    }
  }
  return out;
}

// Directory children of a root:// or xiohttp(s):// URL. The collection itself
// is omitted. http_status is 0 when the listener is not reached.
inline int XioUserList(const std::string& url, std::vector<XioListEnt>& ents,
                       std::string& err, int* http_status)
{
  ents.clear();
  err.clear();
  if (http_status) {
    *http_status = 0;
  }
  XioCall call;
  std::string scheme = "http";
  XioProcessCall(call, scheme);
  std::string http = url;
  if (http.compare(0, 7, "root://") == 0) {
    const char* port_env = std::getenv("EOS_MGM_XIO_PORT");
    const int port = port_env ? std::atoi(port_env) : 0;
    if (port <= 0) {
      err = "HTTP/2 listener is not configured";
      return EIO;
    }
    std::string rest = http.substr(7);
    const auto slash = rest.find('/');
    std::string host = slash == std::string::npos ? rest : rest.substr(0, slash);
    std::string path = slash == std::string::npos ? "/" : rest.substr(slash);
    const auto at = host.find('@');
    if (at != std::string::npos) {
      host.erase(0, at + 1);
    }
    const auto colon = host.rfind(':');
    if (colon != std::string::npos) {
      host.erase(colon);
    }
    while (path.size() > 1 && path[0] == '/' && path[1] == '/') {
      path.erase(0, 1);
    }
    http = scheme + "://" + host + ":" + std::to_string(port) + path;
  } else if (http.compare(0, 3, "xio") == 0) {
    http.erase(0, 3);
  }
  call.extra.emplace_back("depth", "1");
  std::string body;
  int http_st = 0;
  const int rc = XioUserExchange("propfind", http, call, body, err, &http_st);
  if (http_status) {
    *http_status = http_st;
  }
  if (rc) {
    return rc;
  }
  size_t pos = 0;
  bool first = true;
  while (pos < body.size()) {
    const auto start = body.find("<D:response>", pos);
    if (start == std::string::npos) {
      break;
    }
    const auto end = body.find("</D:response>", start);
    if (end == std::string::npos) {
      break;
    }
    const std::string chunk = body.substr(start, end - start);
    pos = end + 13;
    if (first) {
      first = false;
      continue;
    }
    XioListEnt ent;
    const std::string href_raw = XioXmlText(chunk, "<D:href>", "</D:href>");
    std::string href = href_raw;
    while (!href.empty() && href.back() == '/') {
      href.pop_back();
    }
    const auto slash = href.find_last_of('/');
    ent.name = slash == std::string::npos ? href : href.substr(slash + 1);
    if (ent.name.empty() || ent.name == "." || ent.name == "..") {
      continue;
    }
    ent.dir = XioXmlText(chunk, "<D:iscollection>", "</D:iscollection>") == "1";
    ent.size = static_cast<uint64_t>(
                 std::strtoull(XioXmlText(chunk, "<D:getcontentlength>",
                                          "</D:getcontentlength>").c_str(),
                               nullptr, 10));
    const std::string mode = XioXmlText(chunk, "<X:mode>", "</X:mode>");
    ent.mode = static_cast<mode_t>(std::strtoul(mode.c_str(), nullptr, 8));
    if (ent.dir) {
      ent.mode |= S_IFDIR;
    } else {
      ent.mode |= S_IFREG;
    }
    ents.push_back(std::move(ent));
  }
  return 0;
}

} // namespace common
} // namespace eos

#endif
