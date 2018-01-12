// ----------------------------------------------------------------------
// File: JeMallocHandler.cc
// Author: Geoffray Adde - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#include "common/JeMallocHandler.hh"
#include "common/Logging.hh"
#include <dlfcn.h>
#include <string>
#include <cstdio>
#include "XrdSys/XrdSysTimer.hh"
#include <XrdOuc/XrdOucString.hh>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
JeMallocHandler::JeMallocHandler():
  mallctl(0)
{
  pJeMallocLoaded = IsJemallocLoader();
  pCanProfile = pJeMallocLoaded ? IsProfEnabled() : false;
  pProfRunning = pCanProfile ? IsProfgRunning() : false;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
JeMallocHandler::~JeMallocHandler()
{}

bool JeMallocHandler::IsJemallocLoader()
{
  bool isloaded = false;
  void* handle;
  void* pmallctlnametomib;
  handle = dlopen(NULL, RTLD_LAZY);

  if (!handle)  {
    eos_static_err("error opening dl symbols : %s. libjemalloc is considered as NOT loaded",
                   dlerror());
    //fputs (dlerror(), stderr);
    return isloaded;
  }

  pmallctlnametomib = dlsym(handle, "mallctlnametomib");

  if (dlerror() == NULL) {
    isloaded = true;
    pmallctlnametomib = dlsym(handle, "mallctl");

    if (dlerror() == NULL) {
      mallctl = reinterpret_cast<int (*)(const char*, void*, size_t*, void*, size_t)>
                (pmallctlnametomib);
    } else {
      isloaded = false;
    }
  }

  dlclose(handle);
  eos_static_notice("jemalloc is %sloaded!", isloaded ? "" : "NOT ");
  return isloaded;
}

bool JeMallocHandler::IsProfEnabled()
{
  bool b = false;
  size_t s = sizeof(bool);
  int errc = 0;

  if ((errc = mallctl("opt.prof", &b, &s, NULL, 0))) {
    eos_static_err("error reading status of opt.prof : b=%d  s=%d  errc=%d",
                   (int)b, (int)s, errc);
  }

  return b;
}

bool JeMallocHandler::IsProfgRunning()
{
  bool b = false;
  size_t s = sizeof(bool);
  int errc = 0;

  if ((errc = mallctl("prof.active", &b, &s, NULL, 0))) {
    eos_static_err("error reading status of prof.active : %d", errc);
  }

  return b;
}

bool JeMallocHandler::StartProfiling()
{
  bool b = true;
  return mallctl("prof.active", NULL, NULL, &b, sizeof(bool)) == 0;
}
bool JeMallocHandler::StopProfiling()
{
  bool b = false;
  return mallctl("prof.active", NULL, NULL, &b, sizeof(bool)) == 0;
}
bool JeMallocHandler::DumpProfile()
{
  return mallctl("prof.dump", NULL, NULL, NULL, 0) == 0;
}

EOSCOMMONNAMESPACE_END
