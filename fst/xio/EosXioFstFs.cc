//------------------------------------------------------------------------------
// FST backend for an Xio server.
//------------------------------------------------------------------------------
#include "fst/xio/EosXioFstFs.hh"
#include "fst/EosFstOfs.hh"
#include "XrdSfs/XrdSfsInterface.hh"

#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <sys/stat.h>
#include <vector>

namespace eos {
namespace fst {

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
  out.body = err.getErrText() ? err.getErrText() : "";
  out.http_status = XioFS::errnoToHttp(out.err);
  out.content_type = "text/plain";
}

bool parseRange(const std::string& v, uint64_t size, uint64_t& start, uint64_t& end)
{
  auto eq = v.find('=');
  if (eq == std::string::npos) {
    return false;
  }
  std::string s = v.substr(eq + 1);
  auto dash = s.find('-');
  if (dash == std::string::npos) {
    return false;
  }
  try {
    start = std::stoull(s.substr(0, dash));
    if (dash + 1 >= s.size()) {
      end = size ? size - 1 : 0;
    } else {
      end = std::stoull(s.substr(dash + 1));
    }
  } catch (...) {
    return false;
  }
  return true;
}

bool parseContentRange(const std::string& v, uint64_t& start, uint64_t& end)
{
  std::string s = v;
  if (s.compare(0, 6, "bytes ") == 0) {
    s = s.substr(6);
  }
  auto dash = s.find('-');
  auto slash = s.find('/');
  if (dash == std::string::npos || slash == std::string::npos || slash < dash) {
    return false;
  }
  try {
    start = std::stoull(s.substr(0, dash));
    end = std::stoull(s.substr(dash + 1, slash - dash - 1));
  } catch (...) {
    return false;
  }
  return end >= start;
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
  return q.find("fst.pcmd=") != std::string::npos ||
         q.find("mgm.pcmd=") != std::string::npos;
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
  if (rc == SFS_OK) {
    out.http_status = 204;
    return;
  }
  fromErr(err, out);
}

} // namespace

EosXioFstFs::~EosXioFstFs()
{
  std::lock_guard<std::mutex> lock(held_mu_);
  for (auto& it : held_) {
    if (it.second && it.second->file) {
      it.second->file->close();
    }
  }
  held_.clear();
}

void EosXioFstFs::handle(const XioFS::FsRequest& req, XioFS::FsResult& out)
{
  out = XioFS::FsResult{};
  std::string name;
  XrdSecEntity ent("xio");
  entity(req.cred, name, ent);
  const std::string method = lower(req.method);
  if (session(req, out, method)) {
    return;
  }
  const char* path = req.path.c_str();
  const char* query = req.query.empty() ? nullptr : req.query.c_str();
  const std::string opaque = pluginOpaque(req);

  if (wantsPlugin(req, method)) {
    if (req.cred.name != "daemon") {
      out.http_status = 403;
      out.body = "internal token required";
      return;
    }
    std::string tident = name + "@xio";
    ent.tident = tident.c_str();
    XrdSfsFSctl args;
    args.Arg1 = path;
    args.Arg1Len = static_cast<int>(req.path.size());
    args.Arg2 = opaque.c_str();
    args.Arg2Len = static_cast<int>(opaque.size());
    XrdOucErrInfo err;
    err.setErrUser(name.c_str());
    pluginResult(gOFS.FSctl(SFS_FSCTL_PLUGIN, args, err, &ent), err, out);
    return;
  }

  if (method == "head" || method == "propfind") {
    struct stat st;
    memset(&st, 0, sizeof(st));
    XrdOucErrInfo err;
    if (gOFS.stat(path, &st, err, &ent, query) != SFS_OK) {
      fromErr(err, out);
      return;
    }
    out.http_status = method == "propfind" ? 207 : 200;
    out.headers.emplace_back("content-length",
                             std::to_string(static_cast<long long>(st.st_size)));
    out.headers.emplace_back("x-xio-mode",
                             std::to_string(static_cast<unsigned>(st.st_mode)));
    out.headers.emplace_back("x-xio-mtime",
                             std::to_string(static_cast<long long>(st.st_mtime)));
    out.headers.emplace_back("x-xio-atime",
                             std::to_string(static_cast<long long>(st.st_atime)));
    if (method == "propfind") {
      out.content_type = "application/xml; charset=\"utf-8\"";
      out.body = "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
                 "<D:multistatus xmlns:D=\"DAV:\">"
                 "<D:response><D:href>" + req.path + "</D:href>"
                 "<D:propstat><D:prop><D:getcontentlength>" +
                 std::to_string(static_cast<long long>(st.st_size)) +
                 "</D:getcontentlength></D:prop>"
                 "<D:status>HTTP/1.1 200 OK</D:status></D:propstat>"
                 "</D:response></D:multistatus>";
    }
    return;
  }

  if (method != "get" && method != "put" && method != "patch") {
    out.http_status = 405;
    out.body = "method not implemented on the fst";
    return;
  }

  EosFstOfsFile file(ent.name, 0);
  XrdSfsFileOpenMode mode = SFS_O_RDONLY;
  if (method != "get") {
    mode = SFS_O_RDWR | SFS_O_CREAT;
    if (method == "put") {
      mode = static_cast<XrdSfsFileOpenMode>(mode | SFS_O_TRUNC);
    }
  }
  int rc = file.open(path, mode, 0644, &ent, query);
  if (rc == SFS_REDIRECT) {
    std::string host = file.error.getErrText() ? file.error.getErrText() : "";
    out.redirect = "http://" + host + ":" +
                   std::to_string(file.error.getErrInfo()) + req.path;
    out.http_status = 307;
    return;
  }
  if (rc == SFS_STALL) {
    out.stall_sec = file.error.getErrInfo() > 0 ? file.error.getErrInfo() : 1;
    out.http_status = 503;
    out.body = file.error.getErrText() ? file.error.getErrText() : "stall";
    return;
  }
  if (rc != SFS_OK) {
    fromErr(file.error, out);
    return;
  }

  if (method == "get") {
    struct stat st;
    memset(&st, 0, sizeof(st));
    if (file.stat(&st) != SFS_OK) {
      fromErr(file.error, out);
      file.close();
      return;
    }
    uint64_t start = 0;
    uint64_t end = st.st_size ? static_cast<uint64_t>(st.st_size) - 1 : 0;
    bool partial = false;
    if (const std::string* range = req.header("range")) {
      partial = true;
      if (!parseRange(*range, static_cast<uint64_t>(st.st_size), start, end) ||
          st.st_size == 0 || start >= static_cast<uint64_t>(st.st_size)) {
        out.http_status = 416;
        file.close();
        return;
      }
      if (end >= static_cast<uint64_t>(st.st_size)) {
        end = static_cast<uint64_t>(st.st_size) - 1;
      }
    }
    const uint64_t nbytes = end >= start ? end - start + 1 : 0;
    out.body.resize(static_cast<size_t>(nbytes));
    uint64_t got = 0;
    while (got < nbytes) {
      XrdSfsXferSize n = file.read(static_cast<XrdSfsFileOffset>(start + got),
                                   &out.body[static_cast<size_t>(got)],
                                   static_cast<XrdSfsXferSize>(nbytes - got));
      if (n < 0) {
        fromErr(file.error, out);
        file.close();
        return;
      }
      if (n == 0) {
        break;
      }
      got += static_cast<uint64_t>(n);
    }
    out.body.resize(static_cast<size_t>(got));
    out.http_status = partial ? 206 : 200;
    file.close();
    return;
  }

  uint64_t start = 0;
  if (method == "patch") {
    const std::string* cr = req.header("content-range");
    uint64_t end = 0;
    if (!cr || !parseContentRange(*cr, start, end)) {
      out.http_status = 400;
      out.body = "PATCH requires Content-Range";
      file.close();
      return;
    }
  }
  uint64_t off = 0;
  while (off < req.body.size()) {
    XrdSfsXferSize n = file.write(static_cast<XrdSfsFileOffset>(start + off),
                                  req.body.data() + off,
                                  static_cast<XrdSfsXferSize>(req.body.size() - off));
    if (n < 0) {
      fromErr(file.error, out);
      file.close();
      return;
    }
    if (n == 0) {
      break;
    }
    off += static_cast<uint64_t>(n);
  }
  file.close();
  out.http_status = method == "put" ? 201 : 204;
}

bool EosXioFstFs::session(const XioFS::FsRequest& req, XioFS::FsResult& out,
                          const std::string& method)
{
  const std::string* open_hdr = req.header("x-xio-open");
  const std::string* id_hdr = req.header("x-xio-handle");
  const bool opening = open_hdr && !open_hdr->empty();
  const bool joined = id_hdr && !id_hdr->empty();
  if (!opening && !joined) {
    return false;
  }

  {
    // A client that dies without close would pin the open. Drop sessions
    // that have been held for more than an hour.
    std::lock_guard<std::mutex> lock(held_mu_);
    const time_t now = time(nullptr);
    for (auto it = held_.begin(); it != held_.end();) {
      if (it->second && it->second->created &&
          now > it->second->created && now - it->second->created > 3600) {
        if (it->second->file) {
          it->second->file->close();
        }
        it = held_.erase(it);
      } else {
        ++it;
      }
    }
  }

  auto drop = [&](const std::string& id) {
    std::lock_guard<std::mutex> lock(held_mu_);
    auto it = held_.find(id);
    if (it == held_.end()) {
      return;
    }
    if (it->second && it->second->file) {
      it->second->file->close();
    }
    held_.erase(it);
  };

  auto failOpen = [&](EosFstOfsFile& file, int rc) {
    if (rc == SFS_REDIRECT) {
      std::string host = file.error.getErrText() ? file.error.getErrText() : "";
      out.redirect = "http://" + host + ":" +
                     std::to_string(file.error.getErrInfo()) + req.path;
      out.http_status = 307;
      return;
    }
    if (rc == SFS_STALL) {
      out.stall_sec = file.error.getErrInfo() > 0 ? file.error.getErrInfo() : 1;
      out.http_status = 503;
      out.body = file.error.getErrText() ? file.error.getErrText() : "stall";
      return;
    }
    fromErr(file.error, out);
  };

  if (joined) {
    EosFstOfsFile* filep = nullptr;
    {
      std::lock_guard<std::mutex> lock(held_mu_);
      auto it = held_.find(*id_hdr);
      if (it == held_.end() || !it->second || !it->second->file) {
        out.http_status = 404;
        out.body = "xio handle gone";
        return true;
      }
      it->second->created = time(nullptr);
      filep = it->second->file.get();
    }
    EosFstOfsFile& file = *filep;
    if (req.header("x-xio-close")) {
      drop(*id_hdr);
      out.http_status = 200;
      return true;
    }
    if (req.header("x-xio-sync")) {
      const int st = file.tpcSyncPoll();
      if (st == 1) {
        out.http_status = 200;
        out.headers.emplace_back("x-xio-tpc", "running");
        return true;
      }
      if (st == 0) {
        out.http_status = 200;
        out.headers.emplace_back("x-xio-tpc", "done");
        return true;
      }
      out.http_status = 500;
      out.body = std::to_string(-st);
      out.headers.emplace_back("x-xio-tpc", "done");
      return true;
    }
    if (const std::string* tr = req.header("x-xio-trunc")) {
      errno = 0;
      char* end = nullptr;
      const unsigned long long sz = std::strtoull(tr->c_str(), &end, 10);
      if (!end || *end || errno) {
        out.http_status = 400;
        out.body = "bad truncate size";
        return true;
      }
      if (file.truncate(static_cast<XrdSfsFileOffset>(sz)) != SFS_OK) {
        fromErr(file.error, out);
        return true;
      }
      out.http_status = 200;
      return true;
    }
    if (req.header("x-xio-stat")) {
      struct stat st;
      memset(&st, 0, sizeof(st));
      if (file.stat(&st) != SFS_OK) {
        fromErr(file.error, out);
        return true;
      }
      out.http_status = 200;
      out.headers.emplace_back(
          "content-length", std::to_string(static_cast<long long>(st.st_size)));
      return true;
    }
    if (method == "get") {
      struct stat st;
      memset(&st, 0, sizeof(st));
      if (file.stat(&st) != SFS_OK) {
        fromErr(file.error, out);
        return true;
      }
      uint64_t start = 0;
      uint64_t end = st.st_size ? static_cast<uint64_t>(st.st_size) - 1 : 0;
      bool partial = false;
      if (const std::string* range = req.header("range")) {
        partial = true;
        if (!parseRange(*range, static_cast<uint64_t>(st.st_size), start, end) ||
            st.st_size == 0 || start >= static_cast<uint64_t>(st.st_size)) {
          out.http_status = 416;
          return true;
        }
        if (end >= static_cast<uint64_t>(st.st_size)) {
          end = static_cast<uint64_t>(st.st_size) - 1;
        }
      }
      const uint64_t nbytes = end >= start ? end - start + 1 : 0;
      out.body.resize(static_cast<size_t>(nbytes));
      uint64_t got = 0;
      while (got < nbytes) {
        XrdSfsXferSize n = file.read(static_cast<XrdSfsFileOffset>(start + got),
                                     &out.body[static_cast<size_t>(got)],
                                     static_cast<XrdSfsXferSize>(nbytes - got));
        if (n < 0) {
          fromErr(file.error, out);
          return true;
        }
        if (n == 0) {
          break;
        }
        got += static_cast<uint64_t>(n);
      }
      out.body.resize(static_cast<size_t>(got));
      out.http_status = partial ? 206 : 200;
      return true;
    }
    if (method == "put" || method == "patch") {
      uint64_t start = 0;
      if (method == "patch") {
        const std::string* cr = req.header("content-range");
        uint64_t end = 0;
        if (!cr || !parseContentRange(*cr, start, end)) {
          out.http_status = 400;
          out.body = "PATCH requires Content-Range";
          return true;
        }
      }
      uint64_t off = 0;
      while (off < req.body.size()) {
        XrdSfsXferSize n =
            file.write(static_cast<XrdSfsFileOffset>(start + off),
                       req.body.data() + off,
                       static_cast<XrdSfsXferSize>(req.body.size() - off));
        if (n < 0) {
          fromErr(file.error, out);
          return true;
        }
        if (n == 0) {
          break;
        }
        off += static_cast<uint64_t>(n);
      }
      out.http_status = 204;
      return true;
    }
    out.http_status = 405;
    out.body = "method not implemented on an xio handle";
    return true;
  }

  XrdSfsFileOpenMode mode = SFS_O_RDONLY;
  if (const std::string* flags = req.header("x-xio-flags")) {
    mode = static_cast<XrdSfsFileOpenMode>(std::atoi(flags->c_str()));
  }
  std::string id;
  EosFstOfsFile* filep = nullptr;
  XrdSecEntity* entp = nullptr;
  {
    std::lock_guard<std::mutex> lock(held_mu_);
    id = std::to_string(next_id_++);
    auto slot = std::unique_ptr<HeldFile>(new HeldFile());
    HeldFile* raw = slot.get();
    held_.emplace(id, std::move(slot));
    raw->created = time(nullptr);
    entity(req.cred, raw->name, raw->ent);
    raw->file.reset(new EosFstOfsFile(raw->name.c_str(), 0));
    filep = raw->file.get();
    entp = &raw->ent;
  }
  const char* query = req.query.empty() ? nullptr : req.query.c_str();
  int rc = filep->open(req.path.c_str(), mode, 0644, entp, query);
  if (rc != SFS_OK) {
    failOpen(*filep, rc);
    drop(id);
    return true;
  }
  out.headers.emplace_back("x-xio-handle", id);
  out.http_status = 200;
  return true;
}

} // namespace fst
} // namespace eos
