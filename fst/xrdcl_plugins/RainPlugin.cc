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
  return static_cast<XrdCl::FileSystemPlugIn*>(0);
}


//------------------------------------------------------------------------------
// Finalizer
//------------------------------------------------------------------------------
namespace
{
static struct EnvInitializer {
  //--------------------------------------------------------------------------
  // Initializer
  //--------------------------------------------------------------------------
  EnvInitializer()
  {
    char* myhost = XrdNetUtils::MyHostName();
    std::string host_name = myhost;
    free(myhost);
    std::string unit = "rain@";
    unit += host_name;
    // Get log level from env variable XRD_LOGLEVEL
    int log_level = 6; // by default use LOG_INFO
    char* c = nullptr;
    eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

    if ((c = getenv("EOS_LOGLEVEL"))) {
      eos::common::Logging::GetInstance().LB->suspend();
      
      if ((*c >= '0') && (*c <= '7')) {
	log_level = atoi(c);
      } else {
	log_level = g_logging.GetPriorityByString(c);
      }
      g_logging.SetLogPriority(log_level);
      g_logging.SetUnit(unit.c_str());
      
      eos::common::Logging::GetInstance().LB->resume();
    }

    // enable prefetching if not en- or disabled already
    if (!getenv("EOS_FST_XRDIO_READAHEAD")) {
      setenv("EOS_FST_XRDIO_READAHEAD","1",1);
    }
    
    if (!getenv("EOS_FST_XRDIO_BLOCK_SIZE")) {
      setenv("EOS_FST_XRDIO_BLOCK_SIZE","4194304 ", 1);
    }
  }

  //--------------------------------------------------------------------------
  // Finalizer
  //--------------------------------------------------------------------------
  ~EnvInitializer()
  {
  }

} initializer;
}

EOSFSTNAMESPACE_END
