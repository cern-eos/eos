//------------------------------------------------------------------------------
// MGM backend for an Xio server.
//------------------------------------------------------------------------------
#include "mgm/xio/EosXioMgmFs.hh"
#include "mgm/ofs/EosMgmOfs.hh"
#include "common/Mapping.hh"
#include "mgm/fsview/FsView.hh"
#include "XrdSfs/XrdSfsInterface.hh"

#include <cctype>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <memory>
#include <sstream>
#include <sys/stat.h>

namespace eos {
namespace mgm {

namespace {

std::string lower(std::string s)
{
  for (char& c : s) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return s;
}

void entity(const XioFS::Credential& cred, std::string& name, XrdSecEntity& ent)
{
  name = cred.name.empty() ? "nobody" : cred.name;
  // The internal token is this port's stand-in for the SSS daemon identity.
  const bool internal = cred.name == "daemon" && cred.uid == 2;
  if (!internal && cred.prot == "unix") {
    int errc = 0;
    name = eos::common::Mapping::UidToUserName(static_cast<uid_t>(cred.uid), errc);
    if (errc || name.empty()) {
      name = std::to_string(cred.uid);
    }
    ent.Reset("unix");
    ent.uid = static_cast<uid_t>(cred.uid);
    ent.gid = static_cast<gid_t>(cred.gid);
    ent.name = const_cast<char*>(name.c_str());
    return;
  }
  if (!internal && (cred.prot == "krb5" || cred.prot == "gsi" ||
                    cred.prot == "sss") && !cred.name.empty()) {
    name = cred.name;
    ent.Reset(cred.prot.c_str());
    ent.uid = static_cast<uid_t>(-1);
    ent.gid = static_cast<gid_t>(-1);
    ent.name = const_cast<char*>(name.c_str());
    if (!cred.endorsement.empty()) {
      ent.endorsements = const_cast<char*>(cred.endorsement.c_str());
    }
    return;
  }
  if (!internal && !cred.token.empty()) {
    // The token is checked by the existing ztn mapper. Do not keep Reset's uid 0.
    name = "nobody";
    ent.Reset("ztn");
    ent.uid = static_cast<uid_t>(-1);
    ent.gid = static_cast<gid_t>(-1);
    ent.name = const_cast<char*>(name.c_str());
    ent.creds = const_cast<char*>(cred.token.c_str());
    ent.credslen = static_cast<int>(cred.token.size());
    return;
  }
  ent.Reset(internal ? "sss" : "xio");
  ent.name = const_cast<char*>(name.c_str());
  if (cred.uid != static_cast<uint32_t>(-1)) {
    ent.uid = static_cast<uid_t>(cred.uid);
  }
  if (cred.gid != static_cast<uint32_t>(-1)) {
    ent.gid = static_cast<gid_t>(cred.gid);
  }
}

void fromErr(XrdOucErrInfo& err, XioFS::FsResult& out)
{
  out.err = err.getErrInfo();
  if (out.err == 0) {
    out.err = EIO;
  }
  const char* text = err.getErrText();
  out.body = text ? text : "";
  out.http_status = XioFS::errnoToHttp(out.err);
  out.content_type = "text/plain";
}

std::string httpDate(time_t t)
{
  struct tm tm;
  memset(&tm, 0, sizeof(tm));
  gmtime_r(&t, &tm);
  char buf[64];
  if (strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", &tm) == 0) {
    return {};
  }
  return buf;
}

std::string xmlEscape(const std::string& s)
{
  std::string o;
  for (char c : s) {
    if (c == '&') {
      o += "&amp;";
    } else if (c == '<') {
      o += "&lt;";
    } else {
      o.push_back(c);
    }
  }
  return o;
}

void fillStat(const std::string& href, const struct stat& st, std::ostream& xml)
{
  const bool dir = S_ISDIR(st.st_mode);
  xml << "<D:response><D:href>" << xmlEscape(href) << "</D:href>"
      << "<D:propstat><D:prop>"
      << "<D:getcontentlength>"
      << (dir ? 0 : static_cast<long long>(st.st_size))
      << "</D:getcontentlength>"
      << "<D:getlastmodified>" << httpDate(st.st_mtime) << "</D:getlastmodified>"
      << "<D:resourcetype>";
  if (dir) {
    xml << "<D:collection/>";
  }
  xml << "</D:resourcetype>"
      << "<D:iscollection>" << (dir ? "1" : "0") << "</D:iscollection>"
      << "<X:mode>" << std::oct << (st.st_mode & 07777) << std::dec << "</X:mode>"
      << "</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat>"
      << "</D:response>";
}

// The xroot redirect carries the FST xroot port. HTTP/2 listens on stat.xio.port.
int xioPortFor(const std::string& host, int xrdport)
{
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  for (const auto& elem : FsView::gFsView.mIdView) {
    auto* fs = elem.second;
    if (!fs) {
      continue;
    }
    if (fs->GetString("host") != host) {
      continue;
    }
    if (std::atoi(fs->GetString("port").c_str()) != xrdport) {
      continue;
    }
    int xp = std::atoi(fs->GetString("stat.xio.port").c_str());
    if (xp > 0) {
      return xp;
    }
  }
  return 0;
}

// hostCGI from an OFS redirect is "host" or "host?query". Port is separate.
std::string redirectUrl(XrdOucErrInfo& err, const std::string& path)
{
  std::string host = err.getErrText() ? err.getErrText() : "";
  std::string cgi;
  auto q = host.find('?');
  if (q != std::string::npos) {
    cgi = host.substr(q);
    host.erase(q);
  }
  int port = err.getErrInfo();
  int xp = xioPortFor(host, port);
  if (xp > 0) {
    port = xp;
  }
  std::string url = "http://" + host + ":" + std::to_string(port) + path;
  if (!cgi.empty()) {
    url += cgi;
  }
  return url;
}

const char* queryOf(const XioFS::FsRequest& req)
{
  return req.query.empty() ? nullptr : req.query.c_str();
}

std::string pluginOpaque(const XioFS::FsRequest& req)
{
  std::string q = !req.query.empty() ? req.query : req.body;
  if (q.compare(0, 2, "/?") == 0) {
    q.erase(0, 2);
  } else if (!q.empty() && q[0] == '?') {
    q.erase(0, 1);
  }
  return q;
}

bool wantsPlugin(const XioFS::FsRequest& req, const std::string& method)
{
  if (method == "post") {
    return true;
  }
  const std::string q = pluginOpaque(req);
  return q.find("mgm.pcmd=") != std::string::npos ||
         q.find("fst.pcmd=") != std::string::npos;
}

bool pluginOpen(const std::string& q)
{
  return q.find("mgm.pcmd=is_master") != std::string::npos ||
         q.find("mgm.pcmd=version") != std::string::npos;
}

void pluginResult(int rc, XrdOucErrInfo& err, XioFS::FsResult& out)
{
  if (rc == SFS_DATA) {
    const char* text = err.getErrText();
    out.body = text ? text : "";
    out.http_status = 200;
    out.content_type = "text/plain";
    return;
  }
  if (rc == SFS_STALL) {
    out.stall_sec = err.getErrInfo() > 0 ? err.getErrInfo() : 1;
    out.http_status = 503;
    out.body = err.getErrText() ? err.getErrText() : "stall";
    return;
  }
  if (rc == SFS_REDIRECT) {
    out.redirect = redirectUrl(err, "/");
    out.http_status = 307;
    return;
  }
  if (rc == SFS_OK) {
    out.http_status = 204;
    return;
  }
  fromErr(err, out);
}

} // namespace

void EosXioMgmFs::handle(const XioFS::FsRequest& req, XioFS::FsResult& out)
{
  out = XioFS::FsResult{};
  if (!gOFS) {
    out.http_status = 503;
    out.stall_sec = 1;
    out.body = "mgm not ready";
    return;
  }

  std::string name;
  XrdSecEntity ent("xio");
  entity(req.cred, name, ent);
  std::string tident = name + "@xio";
  ent.tident = tident.c_str();
  XrdOucErrInfo err;
  const std::string method = lower(req.method);
  const char* path = req.path.c_str();
  const char* query = queryOf(req);
  const std::string opaque = pluginOpaque(req);

  // A client session open follows the 307 onto the FST. The sticky marker
  // makes later reads and the close use that FST, which holds the handle.
  if (req.header("x-xio-open") || req.header("x-xio-handle") ||
      req.header("x-xio-close")) {
    if (!req.header("x-xio-open") || req.header("x-xio-handle") ||
        req.header("x-xio-close")) {
      out.http_status = 404;
      out.body = "xio handle is not on the mgm";
      return;
    }
    XrdSfsFileOpenMode mode = SFS_O_RDONLY;
    if (const std::string* flags = req.header("x-xio-flags")) {
      mode = static_cast<XrdSfsFileOpenMode>(std::atoi(flags->c_str()));
    }
    std::unique_ptr<XrdSfsFile> file(gOFS->newFile(name.data(), 0));
    if (!file) {
      out.http_status = 500;
      out.body = "no file object";
      return;
    }
    int rc = file->open(path, mode, 0644, &ent, query);
    if (rc == SFS_REDIRECT) {
      out.redirect = redirectUrl(file->error, req.path);
      out.http_status = 307;
      out.headers.emplace_back("x-xrd-sticky", "1");
      return;
    }
    if (rc == SFS_STALL) {
      out.stall_sec = file->error.getErrInfo() > 0 ? file->error.getErrInfo() : 1;
      out.http_status = 503;
      out.body = file->error.getErrText() ? file->error.getErrText() : "stall";
      return;
    }
    if (rc != SFS_OK) {
      fromErr(file->error, out);
      return;
    }
    out.http_status = 502;
    out.body = "mgm open did not redirect";
    return;
  }

  if (wantsPlugin(req, method)) {
    if (!pluginOpen(opaque) && req.cred.name != "daemon" &&
        req.cred.token.empty() && req.cred.prot.empty()) {
      out.http_status = 403;
      out.body = "internal token required";
      return;
    }
    XrdSfsFSctl args;
    args.Arg1 = path;
    args.Arg1Len = static_cast<int>(req.path.size());
    args.Arg2 = opaque.c_str();
    args.Arg2Len = static_cast<int>(opaque.size());
    err.setErrUser(name.c_str());
    pluginResult(gOFS->FSctl(SFS_FSCTL_PLUGIN, args, err, &ent), err, out);
    return;
  }

  if (method == "propfind" || method == "head") {
    struct stat st;
    memset(&st, 0, sizeof(st));
    int rc = gOFS->stat(path, &st, err, &ent, query);
    if (rc != SFS_OK) {
      fromErr(err, out);
      return;
    }
    if (method == "head") {
      out.http_status = 200;
      out.headers.emplace_back("content-length",
                               std::to_string(static_cast<long long>(st.st_size)));
      out.headers.emplace_back("last-modified", httpDate(st.st_mtime));
      out.headers.emplace_back("x-xio-mode",
                               std::to_string(static_cast<unsigned>(st.st_mode)));
      out.headers.emplace_back("x-xio-mtime",
                               std::to_string(static_cast<long long>(st.st_mtime)));
      out.headers.emplace_back("x-xio-atime",
                               std::to_string(static_cast<long long>(st.st_atime)));
      return;
    }
    std::ostringstream xml;
    xml << "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
        << "<D:multistatus xmlns:D=\"DAV:\" xmlns:X=\"http://xrootd.org/ns\">";
    fillStat(req.path, st, xml);
    int depth = 0;
    if (const std::string* d = req.header("depth")) {
      if (*d == "1") {
        depth = 1;
      }
    }
    if (depth == 1 && S_ISDIR(st.st_mode)) {
      std::unique_ptr<XrdSfsDirectory> dir(gOFS->newDir(ent.name, 0));
      if (dir && dir->open(path, &ent, query) == SFS_OK) {
        while (const char* entname = dir->nextEntry()) {
          if (!entname[0] || !strcmp(entname, ".") || !strcmp(entname, "..")) {
            continue;
          }
          std::string child = req.path;
          if (child.empty() || child.back() != '/') {
            child.push_back('/');
          }
          child += entname;
          struct stat cst;
          memset(&cst, 0, sizeof(cst));
          XrdOucErrInfo cerr;
          if (gOFS->stat(child.c_str(), &cst, cerr, &ent, query) == SFS_OK) {
            fillStat(child, cst, xml);
          }
        }
        dir->close();
      }
    }
    xml << "</D:multistatus>";
    out.http_status = 207;
    out.content_type = "application/xml; charset=\"utf-8\"";
    out.body = xml.str();
    return;
  }

  if (req.header("x-xio-chksum")) {
    XrdOucErrInfo cerr;
    cerr.setErrUser(name.c_str());
    int rc = gOFS->chksum(EosMgmOfs::csGet, "eos", path, cerr, &ent, query);
    if (rc == SFS_REDIRECT) {
      out.redirect = redirectUrl(cerr, req.path);
      out.http_status = 307;
      return;
    }
    if (rc != SFS_OK) {
      fromErr(cerr, out);
      return;
    }
    const char* text = cerr.getErrText();
    std::string value = text ? text : "";
    if (!value.empty() && value[0] == '!') {
      value.erase(0, 1);
    }
    out.http_status = 200;
    out.content_type = "text/plain";
    out.body = "eos " + value;
    return;
  }

  if (method == "get" || method == "put" || method == "patch") {
    std::unique_ptr<XrdSfsFile> file(gOFS->newFile(ent.name, 0));
    if (!file) {
      out.http_status = 500;
      out.body = "no file object";
      return;
    }
    XrdSfsFileOpenMode mode = SFS_O_RDONLY;
    if (method != "get") {
      mode = SFS_O_RDWR | SFS_O_CREAT;
      if (method == "put") {
        mode = static_cast<XrdSfsFileOpenMode>(mode | SFS_O_TRUNC);
      }
    }
    int rc = file->open(path, mode, 0644, &ent, query);
    if (rc == SFS_REDIRECT) {
      out.redirect = redirectUrl(file->error, req.path);
      out.http_status = 307;
      return;
    }
    if (rc == SFS_STALL) {
      out.stall_sec = file->error.getErrInfo() > 0 ? file->error.getErrInfo() : 1;
      out.body = file->error.getErrText() ? file->error.getErrText() : "stall";
      out.http_status = 503;
      return;
    }
    if (rc != SFS_OK) {
      fromErr(file->error, out);
      return;
    }
    if (method != "get") {
      file->close();
      out.http_status = 502;
      out.body = "mgm open did not redirect";
      return;
    }
    // Proc commands and other local opens return the body from read().
    std::string body;
    char buf[1024 * 1024];
    uint64_t off = 0;
    for (;;) {
      XrdSfsXferSize n = file->read(static_cast<XrdSfsFileOffset>(off), buf,
                                    static_cast<XrdSfsXferSize>(sizeof(buf)));
      if (n < 0) {
        fromErr(file->error, out);
        file->close();
        return;
      }
      if (n == 0) {
        break;
      }
      body.append(buf, static_cast<size_t>(n));
      off += static_cast<uint64_t>(n);
    }
    file->close();
    out.http_status = 200;
    out.content_type = "text/plain";
    out.body = std::move(body);
    return;
  }

  if (method == "mkcol") {
    int rc = gOFS->mkdir(path, 0755, err, &ent, query, nullptr);
    if (rc != SFS_OK) {
      fromErr(err, out);
      return;
    }
    out.http_status = 201;
    return;
  }

  if (method == "delete") {
    struct stat st;
    memset(&st, 0, sizeof(st));
    int src = gOFS->stat(path, &st, err, &ent, query);
    if (src != SFS_OK) {
      fromErr(err, out);
      return;
    }
    int rc = S_ISDIR(st.st_mode) ? gOFS->remdir(path, err, &ent, query)
                                 : gOFS->rem(path, err, &ent, query);
    if (rc != SFS_OK) {
      fromErr(err, out);
      return;
    }
    out.http_status = 204;
    return;
  }

  if (method == "move") {
    const std::string* dest = req.header("destination");
    if (!dest) {
      out.http_status = 400;
      out.body = "missing destination";
      return;
    }
    std::string to = *dest;
    auto scheme = to.find("://");
    if (scheme != std::string::npos) {
      auto slash = to.find('/', scheme + 3);
      to = slash == std::string::npos ? "/" : to.substr(slash);
    }
    int rc = gOFS->rename(path, to.c_str(), err, &ent, query, nullptr);
    if (rc != SFS_OK) {
      fromErr(err, out);
      return;
    }
    out.http_status = 204;
    return;
  }

  out.http_status = 405;
  out.body = "method not implemented on the mgm";
}

} // namespace mgm
} // namespace eos
