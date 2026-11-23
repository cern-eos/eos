//------------------------------------------------------------------------------
// HTTP/2 opaque query and stat against an Xio listener.
//------------------------------------------------------------------------------
#include "common/xio/XioQuery.hh"
#include "XioClient.hh"
#include "XioHttp2.hh"
#include "XioUrl.hh"

#include <XrdSfs/XrdSfsInterface.hh>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

namespace eos {
namespace common {

namespace {

std::string baseUrl(const std::string& host, int port)
{
  return "http://" + host + ":" + std::to_string(port) + "/";
}

void addToken(XioFS::HttpRequest& req)
{
  const char* token = getenv("EOS_XIO_INTERNAL_TOKEN");
  if (token && token[0]) {
    req.headers.emplace_back("authorization", std::string("Bearer ") + token);
  }
}

std::string cgi(std::string opaque)
{
  if (opaque.compare(0, 2, "/?") == 0) {
    opaque.erase(0, 2);
  } else if (!opaque.empty() && opaque[0] == '?') {
    opaque.erase(0, 1);
  }
  return opaque;
}

} // namespace

std::string XioHostOf(const std::string& hostport)
{
  if (!hostport.empty() && hostport.front() == '[') {
    auto end = hostport.find(']');
    if (end != std::string::npos) {
      return hostport.substr(1, end - 1);
    }
  }
  auto colon = hostport.rfind(':');
  if (colon == std::string::npos) {
    return hostport;
  }
  return hostport.substr(0, colon);
}

int XioOpaqueQuery(const std::string& host, int port, const std::string& opaque,
                   std::string& body, std::string& err, int timeout_ms,
                   int* http_status, const std::string& path)
{
  body.clear();
  err.clear();
  if (http_status) {
    *http_status = 0;
  }
  if (host.empty() || port <= 0) {
    err = "missing xio endpoint";
    return EINVAL;
  }
  XioFS::Url url;
  if (!XioFS::parseUrl(baseUrl(host, port), url, err)) {
    return EINVAL;
  }
  XioFS::Http2Session::Options opt;
  opt.verify_peer = false;
  opt.timeout_ms = timeout_ms > 0 ? timeout_ms : 10000;
  XioFS::Http2Session session;
  int rc = session.connect(url, opt, err);
  if (rc) {
    return rc < 0 ? -rc : rc;
  }
  XioFS::HttpRequest req;
  req.method = "POST";
  std::string url_path = path.empty() ? "/" : path;
  if (url_path.front() != '/') {
    url_path.insert(url_path.begin(), '/');
  }
  const std::string query = cgi(opaque);
  req.path = query.empty() ? url_path : url_path + "?" + query;
  addToken(req);
  XioFS::HttpResponse resp;
  rc = session.request(req, resp, err);
  if (rc) {
    return rc < 0 ? -rc : rc;
  }
  if (http_status) {
    *http_status = resp.status;
  }
  if (resp.status != 200 && resp.status != 204) {
    err = resp.body.empty() ? ("HTTP " + std::to_string(resp.status)) : resp.body;
    return resp.status == 403 ? EACCES : EIO;
  }
  body = std::move(resp.body);
  return 0;
}

int XioStat(const std::string& host, int port, const std::string& path,
            struct stat* st, std::string& err, int timeout_ms, int* http_status)
{
  err.clear();
  if (http_status) {
    *http_status = 0;
  }
  if (!st || host.empty() || port <= 0) {
    err = "missing xio endpoint";
    return EINVAL;
  }
  memset(st, 0, sizeof(*st));
  XioFS::Client client;
  XioFS::Http2Session::Options opt;
  opt.verify_peer = false;
  opt.timeout_ms = timeout_ms > 0 ? timeout_ms : 10000;
  if (const char* token = getenv("EOS_XIO_INTERNAL_TOKEN")) {
    if (token[0]) {
      opt.bearer = token;
    }
  }
  int rc = client.open(baseUrl(host, port), opt, err);
  if (rc) {
    return rc < 0 ? -rc : rc;
  }
  const std::string rel = path.empty() ? "/" : path;
  XioFS::HttpResponse head;
  rc = client.exchange("HEAD", rel, {}, "", head, err);
  if (rc) {
    return rc < 0 ? -rc : rc;
  }
  if (http_status) {
    *http_status = head.status;
  }
  if (head.status != 200 && head.status != 204) {
    err = head.body.empty() ? ("HTTP " + std::to_string(head.status)) : head.body;
    return head.status == 404 ? ENOENT : EIO;
  }
  const std::string cl = head.header("content-length");
  st->st_size = cl.empty() ? 0 : static_cast<off_t>(std::atoll(cl.c_str()));
  const std::string mode = head.header("x-xio-mode");
  if (!mode.empty()) {
    st->st_mode = static_cast<mode_t>(std::strtoul(mode.c_str(), nullptr, 10));
  } else {
    st->st_mode = S_IFREG | 0644;
  }
  const std::string mtime = head.header("x-xio-mtime");
  if (!mtime.empty()) {
    st->st_mtime = static_cast<time_t>(std::atoll(mtime.c_str()));
  }
  const std::string atime = head.header("x-xio-atime");
  if (!atime.empty()) {
    st->st_atime = static_cast<time_t>(std::atoll(atime.c_str()));
  }
  return 0;
}

int XioChecksum(const std::string& host, int port, const std::string& path,
                std::string& body, std::string& err, int timeout_ms,
                int* http_status)
{
  body.clear();
  err.clear();
  if (http_status) {
    *http_status = 0;
  }
  if (host.empty() || port <= 0) {
    err = "missing xio endpoint";
    return EINVAL;
  }
  XioFS::Client client;
  XioFS::Http2Session::Options opt;
  opt.verify_peer = false;
  opt.timeout_ms = timeout_ms > 0 ? timeout_ms : 10000;
  if (const char* token = getenv("EOS_XIO_INTERNAL_TOKEN")) {
    if (token[0]) {
      opt.bearer = token;
    }
  }
  int rc = client.open(baseUrl(host, port), opt, err);
  if (rc) {
    return rc < 0 ? -rc : rc;
  }
  const std::string rel = path.empty() ? "/" : path;
  XioFS::HttpResponse resp;
  rc = client.exchange("GET", rel, {{"x-xio-chksum", "1"}}, "", resp, err);
  if (rc) {
    return rc < 0 ? -rc : rc;
  }
  if (http_status) {
    *http_status = resp.status;
  }
  if (resp.status != 200 && resp.status != 204) {
    err = resp.body.empty() ? ("HTTP " + std::to_string(resp.status)) : resp.body;
    return resp.status == 404 ? ENOENT : EIO;
  }
  body = std::move(resp.body);
  return 0;
}

namespace {

std::string pathOnly(const std::string& path)
{
  const auto q = path.find('?');
  return q == std::string::npos ? path : path.substr(0, q);
}

std::string addCgi(std::string path, const std::string& extra)
{
  if (extra.empty()) {
    return path;
  }
  path.push_back(path.find('?') == std::string::npos ? '?' : '&');
  path += extra;
  return path;
}

std::string qesc(const std::string& in)
{
  static const char* hex = "0123456789ABCDEF";
  std::string out;
  out.reserve(in.size());
  for (unsigned char c : in) {
    if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~' ||
        c == '/' || c == ':') {
      out.push_back(static_cast<char>(c));
    } else {
      out.push_back('%');
      out.push_back(hex[c >> 4]);
      out.push_back(hex[c & 0xf]);
    }
  }
  return out;
}

struct TpcSession {
  XioFS::Client client;
  std::string rel;
  std::string handle;

  void close()
  {
    if (handle.empty()) {
      return;
    }
    XioFS::HttpResponse resp;
    std::string ignored;
    client.exchange("POST", rel,
                    {{"x-xio-handle", handle}, {"x-xio-close", "1"}}, "", resp,
                    ignored);
    handle.clear();
    client.close();
  }

  ~TpcSession()
  {
    close();
  }
};

int openTpc(TpcSession& session, const XioFS::Url& url, const std::string& rel,
            int flags, std::string& err, int* http_status = nullptr)
{
  if (http_status) {
    *http_status = 0;
  }
  XioFS::Http2Session::Options opt;
  opt.verify_peer = false;
  opt.timeout_ms = 60000;
  if (const char* token = std::getenv("EOS_XIO_INTERNAL_TOKEN")) {
    if (token[0]) {
      opt.bearer = token;
    }
  }
  const std::string base = std::string(url.tls ? "https://" : "http://") +
                           url.authority + "/";
  int rc = session.client.open(base, opt, err);
  if (rc) {
    return rc < 0 ? -rc : rc;
  }
  session.rel = rel;
  XioFS::HttpResponse resp;
  rc = session.client.exchange(
      "POST", rel,
      {{"x-xio-open", "1"}, {"x-xio-flags", std::to_string(flags)}}, "", resp,
      err);
  if (rc) {
    return rc < 0 ? -rc : rc;
  }
  if (http_status) {
    *http_status = resp.status;
  }
  if (resp.status != 200 && resp.status != 204) {
    err = resp.body.empty() ? "xio open failed" : resp.body;
    return EIO;
  }
  session.handle = resp.header("x-xio-handle");
  if (session.handle.empty()) {
    err = "xio open missing handle";
    return EIO;
  }
  return 0;
}

int pollTpc(TpcSession& session, std::string& state, std::string& err)
{
  XioFS::HttpResponse resp;
  const int rc = session.client.exchange(
      "POST", session.rel,
      {{"x-xio-handle", session.handle}, {"x-xio-sync", "1"}}, "", resp, err);
  if (rc) {
    return rc < 0 ? -rc : rc;
  }
  state = resp.header("x-xio-tpc");
  if (resp.status == 200 || resp.status == 204) {
    return 0;
  }
  int en = std::atoi(resp.body.c_str());
  if (en <= 0) {
    en = EIO;
  }
  err = resp.body.empty() ? "tpc failed" : resp.body;
  return en;
}

} // namespace

int XioTpcCopy(const std::string& src_url, const std::string& dst_url,
               int timeout_sec, std::string& err,
               const std::function<bool()>& cancel)
{
  err.clear();
  XioFS::Url src;
  XioFS::Url dst;
  if (!XioFS::parseUrl(src_url, src, err) || !XioFS::parseUrl(dst_url, dst, err)) {
    return EINVAL;
  }
  unsigned char rnd[16];
  FILE* urandom = std::fopen("/dev/urandom", "rb");
  if (!urandom || std::fread(rnd, 1, sizeof(rnd), urandom) != sizeof(rnd)) {
    if (urandom) {
      std::fclose(urandom);
    }
    err = "unable to generate tpc key";
    return EIO;
  }
  std::fclose(urandom);
  static const char* hex = "0123456789abcdef";
  std::string key;
  key.resize(sizeof(rnd) * 2);
  for (size_t i = 0; i < sizeof(rnd); ++i) {
    key[i * 2] = hex[rnd[i] >> 4];
    key[i * 2 + 1] = hex[rnd[i] & 0xf];
  }
  const std::string src_ep = std::string(src.tls ? "xiohttps://" : "xiohttp://") +
                             src.host + ":" + std::to_string(src.port);
  const std::string dst_host = dst.host;
  const std::string src_rel = addCgi(
      src.path.empty() ? "/" : src.path,
      "tpc.stage=copy&tpc.key=" + key + "&tpc.dst=" + qesc(dst_host));
  const std::string dst_rel = addCgi(
      dst.path.empty() ? "/" : dst.path,
      "tpc.stage=copy&tpc.key=" + key + "&tpc.src=" + qesc(src_ep) +
          "&tpc.lfn=" + qesc(pathOnly(src.path.empty() ? "/" : src.path)));
  TpcSession source;
  TpcSession target;
  int rc = openTpc(target, dst, dst_rel, SFS_O_RDWR | SFS_O_CREAT | SFS_O_TRUNC, err);
  if (rc) {
    return rc;
  }
  std::string state;
  rc = pollTpc(target, state, err);
  if (rc) {
    return rc;
  }
  if (state == "done") {
    target.close();
    return 0;
  }
  rc = openTpc(source, src, src_rel, SFS_O_RDONLY, err);
  if (rc) {
    return rc;
  }
  const int wait_sec = timeout_sec > 0 ? timeout_sec : 3600;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(wait_sec);
  while (std::chrono::steady_clock::now() < deadline) {
    if (cancel && cancel()) {
      err = "cancelled";
      target.close();
      source.close();
      return ECANCELED;
    }
    rc = pollTpc(target, state, err);
    if (rc) {
      return rc;
    }
    if (state == "done") {
      target.close();
      source.close();
      return 0;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  err = "tpc timed out";
  return ETIMEDOUT;
}

int XioStreamPut(const std::string& local_path, const std::string& dst_url,
                 std::string& err, int* http_status)
{
  err.clear();
  if (http_status) {
    *http_status = 0;
  }
  XioFS::Url dst;
  if (!XioFS::parseUrl(dst_url, dst, err)) {
    return EINVAL;
  }
  FILE* in = std::fopen(local_path.c_str(), "rb");
  if (!in) {
    err = "unable to read local file";
    return errno ? errno : EIO;
  }
  TpcSession session;
  int http = 0;
  const std::string rel = dst.path.empty() ? "/" : dst.path;
  int rc = openTpc(session, dst, rel, SFS_O_RDWR | SFS_O_CREAT | SFS_O_TRUNC, err,
                   &http);
  if (http_status) {
    *http_status = http;
  }
  if (rc) {
    std::fclose(in);
    return rc;
  }
  std::string buf(1 << 20, '\0');
  uint64_t off = 0;
  while (true) {
    const size_t n = std::fread(&buf[0], 1, buf.size(), in);
    if (n == 0) {
      if (std::ferror(in)) {
        std::fclose(in);
        err = "local read failed";
        return EIO;
      }
      break;
    }
    XioFS::HttpResponse resp;
    const std::string cr = "bytes " + std::to_string(off) + "-" +
                           std::to_string(off + n - 1) + "/*";
    rc = session.client.exchange(
        "PATCH", session.rel,
        {{"x-xio-handle", session.handle}, {"content-range", cr}},
        std::string(buf.data(), n), resp, err);
    if (rc) {
      std::fclose(in);
      return rc < 0 ? -rc : rc;
    }
    if (http_status) {
      *http_status = resp.status;
    }
    if (resp.status != 204 && resp.status != 200 && resp.status != 201) {
      std::fclose(in);
      err = resp.body.empty() ? "xio write failed" : resp.body;
      return EIO;
    }
    off += static_cast<uint64_t>(n);
  }
  std::fclose(in);
  session.close();
  return 0;
}

} // namespace common
} // namespace eos
