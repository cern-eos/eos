//------------------------------------------------------------------------------
// File RainPlugin.cc
// Author Elvin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2014 CERN/Switzerland                                  *
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

/*----------------------------------------------------------------------------*/
#if defined(EOS_HAVE_XIO)
#include <map>
#endif
#include <mutex>
#include <stdlib.h>
#include <syslog.h>
/*----------------------------------------------------------------------------*/
#include "RainFile.hh"
#include "RainPlugin.hh"
#if defined(EOS_HAVE_XIO)
#include "common/xio/XioUser.hh"
#include "fst/io/xio/XioIo.hh"
#include <XrdSfs/XrdSfsInterface.hh>
#include <cerrno>
#include <cstring>
#endif
#include <XrdCl/XrdClDefaultEnv.hh>
#include <XrdCl/XrdClFileSystem.hh>
#include <XrdCl/XrdClLog.hh>
#include <XrdNet/XrdNetUtils.hh>
#include <XrdVersion.hh>
/*----------------------------------------------------------------------------*/

namespace {
//------------------------------------------------------------------------------
// Map the XrdCl log level, set by XRD_LOGLEVEL or xrdcp --debug, to the EOS
// log priority. By default XrdCl reports nothing and so does the plugin,
// except critical messages.
//------------------------------------------------------------------------------
int
GetPriorityFromXrdCl()
{
  switch (XrdCl::DefaultEnv::GetLog()->GetLevel()) {
  case XrdCl::Log::ErrorMsg:
    return LOG_ERR;

  case XrdCl::Log::WarningMsg:
    return LOG_WARNING;

  case XrdCl::Log::InfoMsg:
    return LOG_INFO;

  case XrdCl::Log::DebugMsg:
  case XrdCl::Log::DumpMsg:
    return LOG_DEBUG;

  default:
    return LOG_CRIT;
  }
}

//------------------------------------------------------------------------------
// Send the log messages to the same destination as the XrdCl ones i.e. the
// XRD_LOGFILE file if set, otherwise stderr
//------------------------------------------------------------------------------
void
SetupLogging()
{
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  char* myhost = XrdNetUtils::MyHostName();
  std::string unit = "rain@";
  unit += (myhost ? myhost : "unknown");
  free(myhost);
  g_logging.SetUnit(unit.c_str());
  const char* log_file = getenv("XRD_LOGFILE");

  if (log_file && *log_file) {
    // Kept open for the lifetime of the process, XrdCl also opens the file in
    // append mode so the messages of both are interleaved
    FILE* fp = fopen(log_file, "a");

    if (fp) {
      g_logging.AddFanOut("*", fp);
      g_logging.SetStderr(false);
    } else {
      fprintf(stderr,
              "[Error] RAIN plugin unable to open log file %s, log "
              "to stderr\n",
              log_file);
    }
  }
}

#if defined(EOS_HAVE_XIO)
std::string
JoinRoot(const std::string& base, const std::string& path)
{
  std::string url = base;
  if (url.find("://") == std::string::npos) {
    url.insert(0, "root://");
  }
  if (path.empty()) {
    return url;
  }
  if (!url.empty() && url.back() == '/' && path.front() == '/') {
    url.pop_back();
  } else if (!url.empty() && url.back() != '/' && path.front() != '/') {
    url.push_back('/');
  }
  url += path;
  return url;
}

XrdCl::XRootDStatus
Deliver(XrdCl::ResponseHandler* handler, const XrdCl::XRootDStatus& st,
        XrdCl::AnyObject* obj)
{
  if (!st.IsOK()) {
    delete obj;
    obj = nullptr;
  }
  if (handler) {
    handler->HandleResponse(new XrdCl::XRootDStatus(st), obj);
    return XrdCl::XRootDStatus();
  }
  delete obj;
  return st;
}

XrdCl::XRootDStatus
HttpStatus(int rc, int http, const std::string& err)
{
  if (http == 0) {
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errConnectionError, EIO,
                               "HTTP/2 listener is not reachable");
  }
  if (rc == 0) {
    return XrdCl::XRootDStatus();
  }
  const uint16_t code = rc == ENOENT ? XrdCl::errNotFound :
                        (rc == EACCES ? XrdCl::errAuthFailed
                                      : XrdCl::errErrorResponse);
  return XrdCl::XRootDStatus(XrdCl::stError, code, rc,
                             err.empty() ? "HTTP/2 request failed" : err.c_str());
}

XrdCl::StatInfo*
MakeStat(const struct stat& st)
{
  uint32_t flags = XrdCl::StatInfo::IsReadable;
  if (S_ISDIR(st.st_mode)) {
    flags |= XrdCl::StatInfo::IsDir;
  }
  if (st.st_mode & S_IWUSR) {
    flags |= XrdCl::StatInfo::IsWritable;
  }
  if (st.st_mode & (S_IXUSR | S_IXGRP | S_IXOTH)) {
    flags |= XrdCl::StatInfo::XBitSet;
  }
  return new XrdCl::StatInfo("", static_cast<uint64_t>(st.st_size), flags,
                             static_cast<uint64_t>(st.st_mtime));
}

XrdCl::XRootDStatus
HttpUnsupported(XrdCl::ResponseHandler* handler)
{
  return Deliver(handler,
                 XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errNotSupported,
                                     EOPNOTSUPP, "HTTP/2 does not serve this call"),
                 nullptr);
}

XrdCl::XRootDStatus
HttpStat(const std::string& url, XrdCl::ResponseHandler* handler)
{
  struct stat st;
  std::string err;
  int http = 0;
  const int rc = eos::common::XioUserStatUrl(url, &st, err, &http);
  if (rc || http == 0) {
    return Deliver(handler, HttpStatus(rc ? rc : EIO, http, err), nullptr);
  }
  auto* info = MakeStat(st);
  auto* obj = new XrdCl::AnyObject();
  obj->Set(info);
  return Deliver(handler, XrdCl::XRootDStatus(), obj);
}

XrdCl::XRootDStatus
HttpSimple(const std::string& method, const std::string& url,
           XrdCl::ResponseHandler* handler, const char* hdr = nullptr,
           const char* val = nullptr)
{
  std::string body;
  int http = 0;
  const int rc = eos::fst::XioIo::UserExchange(method, url, body, http, hdr, val);
  return Deliver(handler, HttpStatus(rc, http, body), nullptr);
}

XrdCl::XRootDStatus
HttpDirList(const std::string& url, XrdCl::ResponseHandler* handler)
{
  std::vector<eos::common::XioListEnt> ents;
  std::string err;
  int http = 0;
  const int rc = eos::common::XioUserList(url, ents, err, &http);
  if (rc || http == 0) {
    return Deliver(handler, HttpStatus(rc ? rc : EIO, http, err), nullptr);
  }
  auto* list = new XrdCl::DirectoryList();
  for (const auto& ent : ents) {
    struct stat st;
    memset(&st, 0, sizeof(st));
    st.st_mode = ent.mode;
    st.st_size = static_cast<off_t>(ent.size);
    list->Add(new XrdCl::DirectoryList::ListEntry(
                "", ent.name, MakeStat(st)));
  }
  auto* obj = new XrdCl::AnyObject();
  obj->Set(list);
  return Deliver(handler, XrdCl::XRootDStatus(), obj);
}

XrdCl::XRootDStatus
HttpQuery(const std::string& base, XrdCl::QueryCode::Code queryCode,
          const XrdCl::Buffer& arg, XrdCl::ResponseHandler* handler)
{
  std::string request(arg.GetBuffer(), arg.GetSize());
  std::string body;
  int http = 0;
  int rc = 0;
  if (queryCode == XrdCl::QueryCode::Checksum) {
    const std::string url = JoinRoot(base, request);
    rc = eos::fst::XioIo::UserExchange("GET", url, body, http, "x-xio-chksum", "1");
    if (body.compare(0, 4, "eos ") == 0) {
      body.erase(0, 4);
    }
  } else if (queryCode == XrdCl::QueryCode::OpaqueFile) {
    rc = eos::fst::XioIo::UserOpaque(base, request, body, http);
  } else {
    return HttpUnsupported(handler);
  }
  if (rc || http == 0) {
    return Deliver(handler, HttpStatus(rc ? rc : EIO, http, body), nullptr);
  }
  auto* buf = new XrdCl::Buffer();
  buf->FromString(body);
  auto* obj = new XrdCl::AnyObject();
  obj->Set(buf);
  return Deliver(handler, XrdCl::XRootDStatus(), obj);
}

XrdCl::XRootDStatus
HttpTrunc(const std::string& url, uint64_t size, XrdCl::ResponseHandler* handler)
{
  const std::string xu = eos::fst::XioIo::UserUrl(url);
  if (xu.empty()) {
    return Deliver(handler, HttpStatus(EIO, 0, "HTTP/2 listener is not configured"),
                   nullptr);
  }
  eos::fst::XioIo io(xu);
  io.useProcessAuth();
  if (io.fileOpen(SFS_O_RDWR) ||
      io.fileTruncate(static_cast<XrdSfsFileOffset>(size))) {
    return Deliver(handler, HttpStatus(io.GetLastErrNo() ? io.GetLastErrNo() : EIO,
                                       io.lastHttpStatus(), io.GetLastErrMsg()),
                   nullptr);
  }
  io.fileClose();
  return Deliver(handler, XrdCl::XRootDStatus(), nullptr);
}
#endif

//------------------------------------------------------------------------------
//! File system plug-in which forwards everything to a plain XrdCl file system.
//! The RAIN plugin only changes the file access, but XrdCl logs an error for
//! each file system object if the factory does not provide one.
//------------------------------------------------------------------------------
class RainFileSystem : public XrdCl::FileSystemPlugIn {
public:
  explicit RainFileSystem(const std::string& url)
#if defined(EOS_HAVE_XIO)
      : mUrl(url)
#else
      : mFs(XrdCl::URL(url), false)
#endif
  {
  }

  XrdCl::XRootDStatus
  Locate(const std::string& path, XrdCl::OpenFlags::Flags flags,
         XrdCl::ResponseHandler* handler, time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)path; (void)flags; (void)timeout;
    return HttpUnsupported(handler);
#else
    return mFs.Locate(path, flags, handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  DeepLocate(const std::string& path, XrdCl::OpenFlags::Flags flags,
             XrdCl::ResponseHandler* handler, time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)path; (void)flags; (void)timeout;
    return HttpUnsupported(handler);
#else
    return mFs.DeepLocate(path, flags, handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  Mv(const std::string& source, const std::string& dest, XrdCl::ResponseHandler* handler,
     time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)timeout;
    return HttpSimple("move", JoinRoot(mUrl, source), handler, "destination",
                      JoinRoot(mUrl, dest).c_str());
#else
    return mFs.Mv(source, dest, handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  Query(XrdCl::QueryCode::Code queryCode, const XrdCl::Buffer& arg,
        XrdCl::ResponseHandler* handler, time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)timeout;
    return HttpQuery(mUrl, queryCode, arg, handler);
#else
    return mFs.Query(queryCode, arg, handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  Truncate(const std::string& path, uint64_t size, XrdCl::ResponseHandler* handler,
           time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)timeout;
    return HttpTrunc(JoinRoot(mUrl, path), size, handler);
#else
    return mFs.Truncate(path, size, handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  Rm(const std::string& path, XrdCl::ResponseHandler* handler, time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)timeout;
    return HttpSimple("delete", JoinRoot(mUrl, path), handler);
#else
    return mFs.Rm(path, handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  MkDir(const std::string& path, XrdCl::MkDirFlags::Flags flags, XrdCl::Access::Mode mode,
        XrdCl::ResponseHandler* handler, time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)flags; (void)mode; (void)timeout;
    return HttpSimple("mkcol", JoinRoot(mUrl, path), handler);
#else
    return mFs.MkDir(path, flags, mode, handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  RmDir(const std::string& path, XrdCl::ResponseHandler* handler, time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)timeout;
    return HttpSimple("delete", JoinRoot(mUrl, path), handler);
#else
    return mFs.RmDir(path, handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  ChMod(const std::string& path, XrdCl::Access::Mode mode,
        XrdCl::ResponseHandler* handler, time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)path; (void)mode; (void)timeout;
    return HttpUnsupported(handler);
#else
    return mFs.ChMod(path, mode, handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  Ping(XrdCl::ResponseHandler* handler, time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)timeout;
    return HttpStat(mUrl, handler);
#else
    return mFs.Ping(handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  Stat(const std::string& path, XrdCl::ResponseHandler* handler, time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)timeout;
    return HttpStat(JoinRoot(mUrl, path), handler);
#else
    return mFs.Stat(path, handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  StatVFS(const std::string& path, XrdCl::ResponseHandler* handler,
          time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)path; (void)timeout;
    return HttpUnsupported(handler);
#else
    return mFs.StatVFS(path, handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  Protocol(XrdCl::ResponseHandler* handler, time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)timeout;
    auto* info = new XrdCl::ProtocolInfo(5u << 16, XrdCl::ProtocolInfo::IsManager);
    auto* obj = new XrdCl::AnyObject();
    obj->Set(info);
    return Deliver(handler, XrdCl::XRootDStatus(), obj);
#else
    return mFs.Protocol(handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  DirList(const std::string& path, XrdCl::DirListFlags::Flags flags,
          XrdCl::ResponseHandler* handler, time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)flags; (void)timeout;
    return HttpDirList(JoinRoot(mUrl, path), handler);
#else
    return mFs.DirList(path, flags, handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  SendInfo(const std::string& info, XrdCl::ResponseHandler* handler,
           time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)info; (void)timeout;
    return HttpUnsupported(handler);
#else
    return mFs.SendInfo(info, handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  Prepare(const std::vector<std::string>& fileList, XrdCl::PrepareFlags::Flags flags,
          uint8_t priority, XrdCl::ResponseHandler* handler, time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)fileList; (void)flags; (void)priority; (void)timeout;
    return HttpUnsupported(handler);
#else
    return mFs.Prepare(fileList, flags, priority, handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  SetXAttr(const std::string& path, const std::vector<XrdCl::xattr_t>& attrs,
           XrdCl::ResponseHandler* handler, time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)path; (void)attrs; (void)timeout;
    return HttpUnsupported(handler);
#else
    return mFs.SetXAttr(path, attrs, handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  GetXAttr(const std::string& path, const std::vector<std::string>& attrs,
           XrdCl::ResponseHandler* handler, time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)path; (void)attrs; (void)timeout;
    return HttpUnsupported(handler);
#else
    return mFs.GetXAttr(path, attrs, handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  DelXAttr(const std::string& path, const std::vector<std::string>& attrs,
           XrdCl::ResponseHandler* handler, time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)path; (void)attrs; (void)timeout;
    return HttpUnsupported(handler);
#else
    return mFs.DelXAttr(path, attrs, handler, timeout);
#endif
  }

  XrdCl::XRootDStatus
  ListXAttr(const std::string& path, XrdCl::ResponseHandler* handler,
            time_t timeout) override
  {
#if defined(EOS_HAVE_XIO)
    (void)path; (void)timeout;
    return HttpUnsupported(handler);
#else
    return mFs.ListXAttr(path, handler, timeout);
#endif
  }

  bool
  SetProperty(const std::string& name, const std::string& value) override
  {
#if defined(EOS_HAVE_XIO)
    mProps[name] = value;
    return true;
#else
    return mFs.SetProperty(name, value);
#endif
  }

  bool
  GetProperty(const std::string& name, std::string& value) const override
  {
#if defined(EOS_HAVE_XIO)
    const auto it = mProps.find(name);
    if (it == mProps.end()) {
      return false;
    }
    value = it->second;
    return true;
#else
    return mFs.GetProperty(name, value);
#endif
  }

private:
#if defined(EOS_HAVE_XIO)
  std::map<std::string, std::string> mProps;
  std::string mUrl;
#else
  XrdCl::FileSystem mFs;
#endif
};
} // namespace

XrdVERSIONINFO(XrdClGetPlugIn, XrdClGetPlugIn)

extern "C"
{
  void* XrdClGetPlugIn(const void* arg)
  {
    return static_cast<void*>(new eos::fst::RainFactory());
  }
}


EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Construtor
//------------------------------------------------------------------------------
RainFactory::RainFactory()
{
  eos_debug("RainFactory constructor");
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RainFactory::~RainFactory()
{
  // empty
}

//------------------------------------------------------------------------------
// Create a file plug-in for the given URL
//------------------------------------------------------------------------------
XrdCl::FilePlugIn*
RainFactory::CreateFile(const std::string& url)
{
  static std::once_flag s_log_setup;
  std::call_once(s_log_setup, SetupLogging);
  // Follow the XrdCl log level which can be changed after the plugin is
  // loaded e.g. by xrdcp --debug
  eos::common::Logging::GetInstance().SetLogPriority(GetPriorityFromXrdCl());
  eos_debug("url=%s", url.c_str());
  return static_cast<XrdCl::FilePlugIn*>(new RainFile());
}


//------------------------------------------------------------------------------
// Create a file system plug-in for the given URL
//------------------------------------------------------------------------------
XrdCl::FileSystemPlugIn*
RainFactory::CreateFileSystem(const std::string& url)
{
  eos_debug("url=%s", url.c_str());
  return new RainFileSystem(url);
}


EOSFSTNAMESPACE_END
