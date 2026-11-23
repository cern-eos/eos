//------------------------------------------------------------------------------
// FileIo over XioFS::Http2Session. Built when the sibling XioFS library is
// present. xiohttp:// and xiohttps:// URLs land here; root:// stays on XrdCl.
//------------------------------------------------------------------------------
#include "fst/io/xio/XioIo.hh"
#include "XioClient.hh"
#include "common/xio/XioUser.hh"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <future>
#include <sys/stat.h>

EOSFSTNAMESPACE_BEGIN

struct XioIo::Session {
  XioFS::Client client;
  bool open{false};
  std::string handle;
};

namespace {

bool splitXio(const std::string& in, std::string& base, std::string& rel,
              std::string& err)
{
  std::string u = in;
  if (u.compare(0, 3, "xio") == 0) {
    u.erase(0, 3);
  }
  XioFS::Url url;
  if (!XioFS::parseUrl(u, url, err)) {
    return false;
  }
  base = std::string(url.tls ? "https://" : "http://") + url.authority + "/";
  if (url.port && url.authority.find(':') == std::string::npos) {
    base = std::string(url.tls ? "https://" : "http://") + url.host + ":" +
           std::to_string(url.port) + "/";
  }
  rel = url.path.empty() ? "/" : url.path;
  return true;
}

XrdCl::XRootDStatus okStatus()
{
  return XrdCl::XRootDStatus(XrdCl::stOK, "");
}

XrdCl::XRootDStatus errStatus(const std::string& msg)
{
  return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errUnknown, 0, msg);
}

bool splitRoot(const std::string& in, std::string& host, std::string& path)
{
  std::string rest;
  if (in.compare(0, 8, "roots://") == 0) {
    rest = in.substr(8);
  } else if (in.compare(0, 7, "root://") == 0) {
    rest = in.substr(7);
  } else {
    return false;
  }
  const auto slash = rest.find('/');
  std::string hp = slash == std::string::npos ? rest : rest.substr(0, slash);
  path = slash == std::string::npos ? "/" : rest.substr(slash);
  const auto at = hp.find('@');
  if (at != std::string::npos) {
    hp.erase(0, at + 1);
  }
  const auto colon = hp.rfind(':');
  if (colon != std::string::npos) {
    hp.erase(colon);
  }
  while (path.size() > 1 && path[0] == '/' && path[1] == '/') {
    path.erase(0, 1);
  }
  host = std::move(hp);
  return !host.empty();
}

void applyUser(eos::common::XioCall& call, XioFS::Http2Session::Options& opt,
               std::vector<std::pair<std::string, std::string>>& hdrs)
{
  std::string scheme;
  eos::common::XioProcessCall(call, scheme);
  opt.verify_peer = call.verify_peer;
  opt.cacert = call.cacert;
  opt.bearer = call.bearer;
  opt.krb5 = call.krb5;
  opt.krb5cc = call.krb5cc;
  opt.cert = call.cert;
  opt.key = call.key;
  if (!call.sss.empty()) {
    hdrs.emplace_back("authorization", "SSS " + call.sss);
  }
  if (call.unix_id) {
    hdrs.emplace_back("x-eos-uid", std::to_string(call.uid));
    hdrs.emplace_back("x-eos-gid", std::to_string(call.gid));
  }
}

} // namespace

std::string XioIo::UserUrl(const std::string& root_url)
{
  const char* port_env = std::getenv("EOS_MGM_XIO_PORT");
  const int port = port_env ? std::atoi(port_env) : 0;
  if (port <= 0) {
    return {};
  }
  std::string host;
  std::string path;
  if (!splitRoot(root_url, host, path)) {
    return {};
  }
  eos::common::XioCall call;
  std::string scheme = "http";
  eos::common::XioProcessCall(call, scheme);
  if (root_url.compare(0, 8, "roots://") == 0) {
    scheme = "https";
  }
  return "xio" + scheme + "://" + host + ":" + std::to_string(port) + path;
}

std::string XioIo::ServiceUrl(const std::string& root_url)
{
  const char* port_env = std::getenv("EOS_FST_XIO_PORT");
  const int port = port_env ? std::atoi(port_env) : 0;
  if (port <= 0) {
    return {};
  }
  std::string host;
  std::string path;
  if (!splitRoot(root_url, host, path)) {
    return {};
  }
  const char* scheme = root_url.compare(0, 8, "roots://") == 0 ? "https" : "http";
  return std::string("xio") + scheme + "://" + host + ":" + std::to_string(port) +
         path;
}

int XioIo::UserOpaque(const std::string& endpoint, const std::string& request,
                      std::string& body, int& http)
{
  http = 0;
  body.clear();
  std::string joined = endpoint;
  if (!request.empty()) {
    if (!joined.empty() && joined.back() == '/' && request.front() == '/') {
      joined.pop_back();
    } else if (!joined.empty() && joined.back() != '/' && request.front() != '/') {
      joined.push_back('/');
    }
    joined += request;
  }
  const std::string url = UserUrl(joined);
  if (url.empty()) {
    return 0;
  }
  std::string base;
  std::string rel;
  std::string err;
  if (!splitXio(url, base, rel, err)) {
    return EINVAL;
  }
  eos::common::XioCall call;
  XioFS::Http2Session::Options opt;
  opt.timeout_ms = 30000;
  std::vector<std::pair<std::string, std::string>> hdrs;
  applyUser(call, opt, hdrs);
  XioFS::Client client;
  int rc = client.open(base, opt, err);
  if (rc) {
    return rc < 0 ? -rc : rc;
  }
  XioFS::HttpResponse resp;
  rc = client.exchange("POST", rel, hdrs, "", resp, err);
  client.close();
  if (rc) {
    return rc < 0 ? -rc : rc;
  }
  http = resp.status;
  if (resp.status != 200 && resp.status != 204) {
    body = resp.body;
    return resp.status == 403 ? EACCES : EIO;
  }
  body = std::move(resp.body);
  return 0;
}

int XioIo::UserStat(const std::string& root_url, struct stat* buf, int& http)
{
  http = 0;
  if (!buf) {
    return EINVAL;
  }
  const std::string url = UserUrl(root_url);
  if (url.empty()) {
    return 0;
  }
  std::string base;
  std::string rel;
  std::string err;
  if (!splitXio(url, base, rel, err)) {
    return EINVAL;
  }
  eos::common::XioCall call;
  XioFS::Http2Session::Options opt;
  opt.timeout_ms = 30000;
  std::vector<std::pair<std::string, std::string>> hdrs;
  applyUser(call, opt, hdrs);
  XioFS::Client client;
  int rc = client.open(base, opt, err);
  if (rc) {
    return rc < 0 ? -rc : rc;
  }
  XioFS::HttpResponse resp;
  rc = client.exchange("HEAD", rel, hdrs, "", resp, err);
  client.close();
  if (rc) {
    return rc < 0 ? -rc : rc;
  }
  http = resp.status;
  if (resp.status == 404) {
    return ENOENT;
  }
  if (resp.status != 200 && resp.status != 204) {
    return resp.status == 403 ? EACCES : EIO;
  }
  memset(buf, 0, sizeof(*buf));
  const std::string cl = resp.header("content-length");
  if (!cl.empty()) {
    buf->st_size = static_cast<off_t>(std::atoll(cl.c_str()));
  }
  buf->st_mode = S_IFREG | 0644;
  return 0;
}

int XioIo::UserExchange(const std::string& method, const std::string& root_url,
                        std::string& body, int& http, const char* hdr_name,
                        const char* hdr_value)
{
  http = 0;
  body.clear();
  const std::string url = UserUrl(root_url);
  if (url.empty()) {
    return 0;
  }
  std::string base;
  std::string rel;
  std::string err;
  if (!splitXio(url, base, rel, err)) {
    return EINVAL;
  }
  eos::common::XioCall call;
  XioFS::Http2Session::Options opt;
  opt.timeout_ms = 30000;
  std::vector<std::pair<std::string, std::string>> hdrs;
  applyUser(call, opt, hdrs);
  if (hdr_name && hdr_name[0]) {
    hdrs.emplace_back(hdr_name, hdr_value ? hdr_value : "");
  }
  XioFS::Client client;
  int rc = client.open(base, opt, err);
  if (rc) {
    return rc < 0 ? -rc : rc;
  }
  XioFS::HttpResponse resp;
  rc = client.exchange(method, rel, hdrs, "", resp, err);
  client.close();
  if (rc) {
    return rc < 0 ? -rc : rc;
  }
  http = resp.status;
  body = std::move(resp.body);
  if (resp.status == 404) {
    return ENOENT;
  }
  if (resp.status != 200 && resp.status != 201 && resp.status != 204 &&
      resp.status != 207) {
    return resp.status == 403 ? EACCES : EIO;
  }
  return 0;
}

XioIo::XioIo(std::string path)
  : FileIo(std::move(path), "XioIo"), session_(new Session)
{}

XioIo::~XioIo()
{
  fileClose();
}

int XioIo::fail(int err, const std::string& msg)
{
  mLastErrNo = err > 0 ? err : -err;
  mLastErrMsg = msg;
  return err < 0 ? err : -err;
}

int XioIo::fileOpen(XrdSfsFileOpenMode flags, mode_t /*mode*/,
                    const std::string& opaque, uint16_t /*timeout*/)
{
  last_http_ = 0;
  std::string base;
  std::string err;
  if (!splitXio(mFilePath, base, rel_, err)) {
    return fail(EINVAL, err);
  }
  if (!opaque.empty()) {
    rel_.push_back(rel_.find('?') == std::string::npos ? '?' : '&');
    rel_ += opaque;
  }
  XioFS::Http2Session::Options opt;
  opt.verify_peer = false;
  opt.timeout_ms = 30000;
  std::vector<std::pair<std::string, std::string>> hdrs{
      {"x-xio-open", "1"},
      {"x-xio-flags", std::to_string(static_cast<int>(flags))}};
  if (user_auth_) {
    eos::common::XioCall call;
    applyUser(call, opt, hdrs);
  } else if (const char* token = std::getenv("EOS_XIO_INTERNAL_TOKEN")) {
    if (token[0]) {
      opt.bearer = token;
    }
  }
  int rc = session_->client.open(base, opt, err);
  if (rc) {
    return fail(-rc, err);
  }
  XioFS::HttpResponse resp;
  rc = session_->client.exchange("POST", rel_, hdrs, "", resp, err);
  if (rc) {
    session_->client.close();
    return fail(-rc, err);
  }
  last_http_ = resp.status;
  if (resp.status != 200 && resp.status != 204) {
    session_->client.close();
    const int mapped = resp.status == 404 ? ENOENT :
                       (resp.status == 403 ? EACCES : EIO);
    return fail(mapped, resp.body.empty() ? "xio open failed" : resp.body);
  }
  session_->handle = resp.header("x-xio-handle");
  if (session_->handle.empty()) {
    session_->client.close();
    return fail(EIO, "xio open missing handle");
  }
  session_->open = true;
  mIsOpen = true;
  return 0;
}

std::future<XrdCl::XRootDStatus>
XioIo::fileOpenAsync(XrdSfsFileOpenMode flags, mode_t mode,
                     const std::string& opaque, uint16_t timeout)
{
  std::promise<XrdCl::XRootDStatus> p;
  if (fileOpen(flags, mode, opaque, timeout)) {
    p.set_value(errStatus(mLastErrMsg));
  } else {
    p.set_value(okStatus());
  }
  return p.get_future();
}

int64_t XioIo::fileRead(XrdSfsFileOffset offset, char* buffer,
                        XrdSfsXferSize length, uint16_t /*timeout*/)
{
  if (!session_->open || !buffer || length < 0) {
    return fail(EINVAL, "xio read");
  }
  if (length == 0) {
    return 0;
  }
  std::string err;
  const std::string range = "bytes=" + std::to_string(static_cast<uint64_t>(offset)) +
                            "-" +
                            std::to_string(static_cast<uint64_t>(offset) +
                                           static_cast<uint64_t>(length) - 1);
  XioFS::HttpResponse resp;
  int rc = session_->client.exchange(
      "GET", rel_,
      {{"x-xio-handle", session_->handle}, {"range", range}}, "", resp, err);
  if (rc) {
    return fail(-rc, err);
  }
  if (resp.status == 416) {
    return 0;
  }
  if (resp.status != 206 && resp.status != 200) {
    return fail(EIO, resp.body.empty() ? err : resp.body);
  }
  size_t n = resp.body.size();
  if (n > static_cast<size_t>(length)) {
    n = static_cast<size_t>(length);
  }
  memcpy(buffer, resp.body.data(), n);
  return static_cast<int64_t>(n);
}

int64_t XioIo::fileReadPrefetch(XrdSfsFileOffset offset, char* buffer,
                                XrdSfsXferSize length, uint16_t timeout)
{
  return fileRead(offset, buffer, length, timeout);
}

int64_t XioIo::fileReadAsync(XrdSfsFileOffset offset, char* buffer,
                             XrdSfsXferSize length, uint16_t timeout)
{
  return fileRead(offset, buffer, length, timeout);
}

int64_t XioIo::fileReadV(XrdCl::ChunkList& chunkList, uint16_t timeout)
{
  int64_t total = 0;
  for (auto& c : chunkList) {
    int64_t n = fileRead(static_cast<XrdSfsFileOffset>(c.GetOffset()),
                         static_cast<char*>(c.GetBuffer()),
                         static_cast<XrdSfsXferSize>(c.GetLength()), timeout);
    if (n < 0) {
      return n;
    }
    total += n;
  }
  return total;
}

int64_t XioIo::fileReadVAsync(XrdCl::ChunkList& chunkList, uint16_t timeout)
{
  return fileReadV(chunkList, timeout);
}

int64_t XioIo::fileWrite(XrdSfsFileOffset offset, const char* buffer,
                         XrdSfsXferSize length, uint16_t /*timeout*/)
{
  if (!session_->open || (!buffer && length > 0)) {
    return fail(EINVAL, "xio write");
  }
  if (length == 0) {
    return 0;
  }
  std::string err;
  std::string data(buffer, buffer + length);
  const uint64_t off = static_cast<uint64_t>(offset);
  const std::string cr = "bytes " + std::to_string(off) + "-" +
                         std::to_string(off + data.size() - 1) + "/*";
  XioFS::HttpResponse resp;
  int rc = session_->client.exchange(
      "PATCH", rel_,
      {{"x-xio-handle", session_->handle}, {"content-range", cr}}, data, resp,
      err);
  if (rc) {
    return fail(-rc, err);
  }
  if (resp.status != 204 && resp.status != 200 && resp.status != 201) {
    return fail(EIO, resp.body.empty() ? err : resp.body);
  }
  return length;
}

int64_t XioIo::fileWriteAsync(XrdSfsFileOffset offset, const char* buffer,
                              XrdSfsXferSize length, uint16_t timeout)
{
  return fileWrite(offset, buffer, length, timeout);
}

std::future<XrdCl::XRootDStatus>
XioIo::fileWriteAsync(const char* buffer, XrdSfsFileOffset offset,
                      XrdSfsXferSize length)
{
  std::promise<XrdCl::XRootDStatus> p;
  if (fileWrite(offset, buffer, length) < 0) {
    p.set_value(errStatus(mLastErrMsg));
  } else {
    p.set_value(okStatus());
  }
  return p.get_future();
}

int XioIo::fileTruncate(XrdSfsFileOffset offset, uint16_t)
{
  if (!session_ || !session_->open || session_->handle.empty()) {
    return fail(EINVAL, "xio truncate");
  }
  XioFS::HttpResponse resp;
  std::string err;
  const int rc = session_->client.exchange(
                   "POST", rel_,
                   {{"x-xio-handle", session_->handle},
                    {"x-xio-trunc", std::to_string(static_cast<unsigned long long>(offset))}},
                   "", resp, err);
  if (rc) {
    return fail(-rc, err);
  }
  if (resp.status != 200 && resp.status != 204) {
    return fail(EIO, resp.body.empty() ? err : resp.body);
  }
  return 0;
}

std::future<XrdCl::XRootDStatus>
XioIo::fileTruncateAsync(XrdSfsFileOffset offset, uint16_t timeout)
{
  std::promise<XrdCl::XRootDStatus> p;
  if (fileTruncate(offset, timeout)) {
    p.set_value(errStatus(mLastErrMsg));
  } else {
    p.set_value(okStatus());
  }
  return p.get_future();
}

int XioIo::fileFallocate(XrdSfsFileOffset)
{
  return fail(EOPNOTSUPP, "xio fallocate");
}

int XioIo::fileFdeallocate(XrdSfsFileOffset, XrdSfsFileOffset)
{
  return fail(EOPNOTSUPP, "xio fdeallocate");
}

int XioIo::fileRemove(uint16_t)
{
  std::string err;
  int rc = session_->client.unlink(rel_, err);
  if (rc) {
    return fail(-rc, err);
  }
  return 0;
}

int XioIo::fileSync(uint16_t)
{
  return 0;
}

void* XioIo::fileGetAsyncHandler()
{
  return nullptr;
}

int XioIo::fileExists()
{
  XioFS::Attr attr;
  std::string err;
  int rc = session_->client.getattr(rel_, attr, err);
  return rc ? fail(-rc, err) : 0;
}

int XioIo::fileClose(uint16_t)
{
  int rc = 0;
  if (session_ && session_->open) {
    if (!session_->handle.empty()) {
      XioFS::HttpResponse resp;
      std::string err;
      int call = session_->client.exchange(
          "POST", rel_,
          {{"x-xio-handle", session_->handle}, {"x-xio-close", "1"}}, "", resp,
          err);
      session_->handle.clear();
      if (call || (resp.status != 200 && resp.status != 204)) {
        rc = fail(EIO, err.empty() ? resp.body : err);
      }
    }
    session_->client.close();
    session_->open = false;
  }
  mIsOpen = false;
  return rc;
}

int XioIo::fileStat(struct stat* buf, uint16_t)
{
  if (!buf) {
    return fail(EINVAL, "xio stat");
  }
  if (session_ && session_->open && !session_->handle.empty()) {
    XioFS::HttpResponse resp;
    std::string err;
    int rc = session_->client.exchange(
        "GET", rel_,
        {{"x-xio-handle", session_->handle}, {"x-xio-stat", "1"}}, "", resp,
        err);
    if (rc) {
      return fail(-rc, err);
    }
    if (resp.status != 200) {
      return fail(EIO, resp.body.empty() ? err : resp.body);
    }
    memset(buf, 0, sizeof(*buf));
    const std::string cl = resp.header("content-length");
    if (!cl.empty()) {
      buf->st_size = static_cast<off_t>(std::atoll(cl.c_str()));
    }
    buf->st_mode = S_IFREG | 0644;
    return 0;
  }
  XioFS::Attr attr;
  std::string err;
  int rc = session_->client.getattr(rel_, attr, err);
  if (rc) {
    return fail(-rc, err);
  }
  memset(buf, 0, sizeof(*buf));
  buf->st_size = attr.size;
  buf->st_mtime = attr.mtime;
  buf->st_atime = attr.atime;
  buf->st_mode = attr.mode ? attr.mode : (attr.is_dir ? 0755 : 0644);
  if (attr.is_dir) {
    buf->st_mode |= S_IFDIR;
  } else {
    buf->st_mode |= S_IFREG;
  }
  buf->st_ino = static_cast<ino_t>(attr.ino);
  buf->st_uid = attr.uid;
  buf->st_gid = attr.gid;
  return 0;
}

int XioIo::fileFctl(const std::string&, uint16_t)
{
  return 0;
}

int XioIo::attrSet(const char*, const char*, size_t)
{
  return fail(EOPNOTSUPP, "xio attr");
}

int XioIo::attrSet(std::string, std::string)
{
  return fail(EOPNOTSUPP, "xio attr");
}

int XioIo::attrGet(const char*, char*, size_t&)
{
  return fail(EOPNOTSUPP, "xio attr");
}

int XioIo::attrGet(std::string, std::string&)
{
  return fail(EOPNOTSUPP, "xio attr");
}

int XioIo::attrDelete(const char*)
{
  return fail(EOPNOTSUPP, "xio attr");
}

int XioIo::attrList(std::vector<std::string>&)
{
  return fail(EOPNOTSUPP, "xio attr");
}

FileIo::FtsHandle* XioIo::ftsOpen(int)
{
  return nullptr;
}

std::string XioIo::ftsRead(FtsHandle*)
{
  return {};
}

int XioIo::ftsClose(FtsHandle*)
{
  return 0;
}

int XioIo::Statfs(struct statfs*)
{
  return fail(EOPNOTSUPP, "xio statfs");
}

FileIo* MakeXioIo(const std::string& path)
{
  return new XioIo(path);
}

EOSFSTNAMESPACE_END
