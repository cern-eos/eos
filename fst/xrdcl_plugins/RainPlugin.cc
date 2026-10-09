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
#include <mutex>
#include <stdlib.h>
#include <syslog.h>
/*----------------------------------------------------------------------------*/
#include "RainFile.hh"
#include "RainPlugin.hh"
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

//------------------------------------------------------------------------------
//! File system plug-in which forwards everything to a plain XrdCl file system.
//! The RAIN plugin only changes the file access, but XrdCl logs an error for
//! each file system object if the factory does not provide one.
//------------------------------------------------------------------------------
class RainFileSystem : public XrdCl::FileSystemPlugIn {
public:
  explicit RainFileSystem(const std::string& url)
      : mFs(XrdCl::URL(url), false)
  {
  }

  XrdCl::XRootDStatus
  Locate(const std::string& path, XrdCl::OpenFlags::Flags flags,
         XrdCl::ResponseHandler* handler, time_t timeout) override
  {
    return mFs.Locate(path, flags, handler, timeout);
  }

  XrdCl::XRootDStatus
  DeepLocate(const std::string& path, XrdCl::OpenFlags::Flags flags,
             XrdCl::ResponseHandler* handler, time_t timeout) override
  {
    return mFs.DeepLocate(path, flags, handler, timeout);
  }

  XrdCl::XRootDStatus
  Mv(const std::string& source, const std::string& dest, XrdCl::ResponseHandler* handler,
     time_t timeout) override
  {
    return mFs.Mv(source, dest, handler, timeout);
  }

  XrdCl::XRootDStatus
  Query(XrdCl::QueryCode::Code queryCode, const XrdCl::Buffer& arg,
        XrdCl::ResponseHandler* handler, time_t timeout) override
  {
    return mFs.Query(queryCode, arg, handler, timeout);
  }

  XrdCl::XRootDStatus
  Truncate(const std::string& path, uint64_t size, XrdCl::ResponseHandler* handler,
           time_t timeout) override
  {
    return mFs.Truncate(path, size, handler, timeout);
  }

  XrdCl::XRootDStatus
  Rm(const std::string& path, XrdCl::ResponseHandler* handler, time_t timeout) override
  {
    return mFs.Rm(path, handler, timeout);
  }

  XrdCl::XRootDStatus
  MkDir(const std::string& path, XrdCl::MkDirFlags::Flags flags, XrdCl::Access::Mode mode,
        XrdCl::ResponseHandler* handler, time_t timeout) override
  {
    return mFs.MkDir(path, flags, mode, handler, timeout);
  }

  XrdCl::XRootDStatus
  RmDir(const std::string& path, XrdCl::ResponseHandler* handler, time_t timeout) override
  {
    return mFs.RmDir(path, handler, timeout);
  }

  XrdCl::XRootDStatus
  ChMod(const std::string& path, XrdCl::Access::Mode mode,
        XrdCl::ResponseHandler* handler, time_t timeout) override
  {
    return mFs.ChMod(path, mode, handler, timeout);
  }

  XrdCl::XRootDStatus
  Ping(XrdCl::ResponseHandler* handler, time_t timeout) override
  {
    return mFs.Ping(handler, timeout);
  }

  XrdCl::XRootDStatus
  Stat(const std::string& path, XrdCl::ResponseHandler* handler, time_t timeout) override
  {
    return mFs.Stat(path, handler, timeout);
  }

  XrdCl::XRootDStatus
  StatVFS(const std::string& path, XrdCl::ResponseHandler* handler,
          time_t timeout) override
  {
    return mFs.StatVFS(path, handler, timeout);
  }

  XrdCl::XRootDStatus
  Protocol(XrdCl::ResponseHandler* handler, time_t timeout) override
  {
    return mFs.Protocol(handler, timeout);
  }

  XrdCl::XRootDStatus
  DirList(const std::string& path, XrdCl::DirListFlags::Flags flags,
          XrdCl::ResponseHandler* handler, time_t timeout) override
  {
    return mFs.DirList(path, flags, handler, timeout);
  }

  XrdCl::XRootDStatus
  SendInfo(const std::string& info, XrdCl::ResponseHandler* handler,
           time_t timeout) override
  {
    return mFs.SendInfo(info, handler, timeout);
  }

  XrdCl::XRootDStatus
  Prepare(const std::vector<std::string>& fileList, XrdCl::PrepareFlags::Flags flags,
          uint8_t priority, XrdCl::ResponseHandler* handler, time_t timeout) override
  {
    return mFs.Prepare(fileList, flags, priority, handler, timeout);
  }

  XrdCl::XRootDStatus
  SetXAttr(const std::string& path, const std::vector<XrdCl::xattr_t>& attrs,
           XrdCl::ResponseHandler* handler, time_t timeout) override
  {
    return mFs.SetXAttr(path, attrs, handler, timeout);
  }

  XrdCl::XRootDStatus
  GetXAttr(const std::string& path, const std::vector<std::string>& attrs,
           XrdCl::ResponseHandler* handler, time_t timeout) override
  {
    return mFs.GetXAttr(path, attrs, handler, timeout);
  }

  XrdCl::XRootDStatus
  DelXAttr(const std::string& path, const std::vector<std::string>& attrs,
           XrdCl::ResponseHandler* handler, time_t timeout) override
  {
    return mFs.DelXAttr(path, attrs, handler, timeout);
  }

  XrdCl::XRootDStatus
  ListXAttr(const std::string& path, XrdCl::ResponseHandler* handler,
            time_t timeout) override
  {
    return mFs.ListXAttr(path, handler, timeout);
  }

  bool
  SetProperty(const std::string& name, const std::string& value) override
  {
    return mFs.SetProperty(name, value);
  }

  bool
  GetProperty(const std::string& name, std::string& value) const override
  {
    return mFs.GetProperty(name, value);
  }

private:
  XrdCl::FileSystem mFs;
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
