//------------------------------------------------------------------------------
// File: XrdCopy.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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

#include "common/XrdCopy.hh"
#include "common/Timing.hh"
#include <XrdCl/XrdClCopyProcess.hh>
#include <XrdCl/XrdClPropertyList.hh>
#include <XrdCl/XrdClURL.hh>
#include <mutex>
#include <fcntl.h> 
#include <sys/stat.h>

EOSCOMMONNAMESPACE_BEGIN

std::atomic<size_t> XrdCopy::s_bp=0;
std::atomic<size_t> XrdCopy::s_bt=0;
std::atomic<size_t> XrdCopy::s_n=0;
std::atomic<size_t> XrdCopy::s_sp=0;
std::atomic<size_t> XrdCopy::s_tot=0;
std::atomic<bool> XrdCopy::s_verbose=false;
std::atomic<bool> XrdCopy::s_silent=false;
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdCopy::XrdCopy()
{}

XrdCopy::result_t
XrdCopy::run(const XrdCopy::job_t& job, const std::string& filter, size_t npar) {
  static std::mutex run_mutex;
  static std::mutex progress_mutex;
  // we can run one parallel job at a time
  std::lock_guard g(run_mutex);
  s_bp=s_bt=0;
  s_n=0;
  s_sp=0;
  s_tot=job.size();
  XrdCopy::result_t result;
  
  XrdCl::CopyProcess copyProcess;
  std::vector<XrdCl::PropertyList*> tprops;

  class XrdCopyProgressHandler : public XrdCl::CopyProgressHandler {
  public:
    virtual void BeginJob( uint16_t   jobNum,
                           uint16_t   jobTotal,
                           const XrdCl::URL *source,
                           const XrdCl::URL *destination )
    {
    }
    
    virtual void EndJob( uint16_t            jobNum,
                         const XrdCl::PropertyList *result )
    {
      (void)jobNum;
      (void)result;
      s_n++;
      if (s_n == s_tot) {
	JobProgress(0,0,0);
      }
      std::string src;
      std::string dst;
      result->Get("source",src);
      result->Get("target",dst);
      XrdCl::URL durl(dst.c_str());
      auto param = durl.GetParams();
      if (param.count("local.mtime")) {
        // apply mtime changes when done to local files
        struct timespec ts;
        std::string tss = param["local.mtime"];
        if (!eos::common::Timing::Timespec_from_TimespecStr(tss, ts)) {
          // apply local mtime;
          struct timespec times[2];
          times[0] = ts;
          times[1] = ts;
          if (utimensat(0, durl.GetPath().c_str(), times, AT_SYMLINK_NOFOLLOW)) {
            std::cerr << "error: failed to update modification time of '" << durl.GetPath() << "'" << std::endl;
          }
        }
      }
    }
    
    virtual void JobProgress( uint16_t jobNum,
                              uint64_t bytesProcessed,
                              uint64_t bytesTotal )
    {
      s_bp = bytesProcessed;
      s_bt = bytesTotal;
       if (s_verbose) {
        std::cerr << "[ " << s_n << "/" << s_tot << " ] files copied ";
	if (s_sp) {
	  std::cerr << s_sp << " sparse copied";
	}
	std::cerr<< std::endl;
      } else {
        if (!s_silent) {
          std::lock_guard g(progress_mutex);
          std::cerr << "[ " << s_n << "/" << s_tot << " ] files copied ";
	  if (s_sp) {
	    std::cerr << " " << s_sp << " sparse copied";
	  }
	  std::cerr << "\r" << std::flush;
        }
      }
    }
    
    virtual bool ShouldCancel( uint16_t jobNum )
    {
      (void)jobNum;
      return false;
    }
  };
  
  for ( auto it:job ) {
    // filter files
    if (filter.length()) {
      continue;
    }
    {
      XrdCl::PropertyList props;
      XrdCl::PropertyList* result = new XrdCl::PropertyList();
      props.Set("source", it.second.first);
      props.Set("target", it.second.second);
      props.Set("force", true); // allows overwrite
      result->Set("source", it.second.first);
      result->Set("target", it.second.second);
      
      copyProcess.AddJob(props,result);
    }
  }

  XrdCopyProgressHandler copyProgress;
  XrdCl::PropertyList processConfig;
  processConfig.Set( "jobType", "configuration" );
  processConfig.Set( "parallel", npar );
  copyProcess.AddJob( processConfig, 0 );
  copyProcess.Prepare();
  copyProcess.Run(&copyProgress);
  
  return result;
}

EOSCOMMONNAMESPACE_END
