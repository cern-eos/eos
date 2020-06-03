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
#include <stdlib.h>
/*----------------------------------------------------------------------------*/
#include "XrdVersion.hh"
#include "RainPlugin.hh"
#include "RainFile.hh"
#include "XrdNet/XrdNetUtils.hh"
/*----------------------------------------------------------------------------*/

XrdVERSIONINFO(XrdClGetPlugIn, XrdClGetPlugIn)

extern "C"
{
  void *XrdClGetPlugIn( const void *arg )
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
RainFactory::CreateFile(const std::string &url)
{
  eos_debug("url=%s", url.c_str());
  return static_cast<XrdCl::FilePlugIn*>( new RainFile() );
}


//------------------------------------------------------------------------------
// Create a file system plug-in for the given URL
//------------------------------------------------------------------------------
XrdCl::FileSystemPlugIn*
RainFactory::CreateFileSystem(const std::string &url)
{
  eos_debug("url=%s", url.c_str());
  return static_cast<XrdCl::FileSystemPlugIn*>(0);
}


//------------------------------------------------------------------------------
// Finalizer
//------------------------------------------------------------------------------
namespace
{
  static struct EnvInitializer
  {
    //--------------------------------------------------------------------------
    // Initializer
    //--------------------------------------------------------------------------
    EnvInitializer()
    {
      char* myhost = XrdNetUtils::MyHostName();
      std::string host_name = myhost;
      free( myhost );
      std::string unit = "rain@";
      unit += host_name;
      
      // Get log level from env variable XRD_LOGLEVEL
      int log_level = 6; // by default use LOG_INFO
      char* c = '\0';
      
      if ((c = getenv("XRD_LOGLEVEL")))
      {
        if ((*c >= '0') && (*c <= '7'))
          log_level = atoi(c);
        else 
          log_level = eos::common::Logging::GetPriorityByString(c);
      }

      eos::common::Logging::Init();
      eos::common::Logging::SetLogPriority(log_level);
      eos::common::Logging::SetUnit(unit.c_str());

      // Create log file for RAIN transfers
      std::string log_file = "/tmp/rain/xrdcp_rain.log";
      std::string log_dir = "/tmp/rain";
      std::ostringstream oss;
      oss << "mkdir -p " << log_dir;

      if (system(oss.str().c_str()))
      {
        eos_static_err("failed to create log directory:%s", log_dir.c_str());
        exit(1);
      }

      if (::access(log_dir.c_str(), R_OK | W_OK | X_OK))
      {
        eos_static_err("can not access log directory:%s", log_dir.c_str());
        exit(1);
      }

      // Create/Open the log file
      mFp = fopen(log_file.c_str(), "a+");

      if (!mFp)
      {
        eos_static_err("error opening log file:%s", log_file.c_str());
        exit(1);                
      }
      else
      {
        eos_static_debug("set up log file:%s", log_file.c_str());
        // Redirect stdout and stderr to log file
        fflush(stdout);
        fflush(stderr);
        mOldStdout = dup(fileno(stdout));
        mOldStderr = dup(fileno(stderr));
        dup2(fileno(mFp), fileno(stdout));
        dup2(fileno(mFp), fileno(stderr));
      }      
    }
    
    //--------------------------------------------------------------------------
    // Finalizer
    //--------------------------------------------------------------------------
    ~EnvInitializer()
    {
      // Restore stdout and stderr
      fflush(stdout);
      fflush(stderr);
      dup2(mOldStdout, fileno(stdout));
      dup2(mOldStderr, fileno(stderr));
      close(mOldStdout);
      close(mOldStderr);

      if(fclose(mFp))
        fprintf(stderr, "[Error] failed to close log file\n");
      //else
        //fprintf(stderr, "[Info] log file closed successfully\n");
    }

    FILE* mFp;
    int mOldStdout;
    int mOldStderr;
    
  } initializer;
}

EOSFSTNAMESPACE_END
