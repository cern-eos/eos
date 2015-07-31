//------------------------------------------------------------------------------
//! @file DynamicLibrary.cc
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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
#include <dlfcn.h>
#include <sstream>
#include <iostream>
/*----------------------------------------------------------------------------*/
#include "DynamicLibrary.hh"
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor with parameter
//------------------------------------------------------------------------------
DynamicLibrary::DynamicLibrary(void* handle):
  mHandle(handle)
{
  // empty
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
DynamicLibrary::~DynamicLibrary()
{
  if (mHandle)
    ::dlclose(mHandle);
}

//------------------------------------------------------------------------------
// Load dynamic library
//------------------------------------------------------------------------------
DynamicLibrary*
DynamicLibrary::Load(const std::string& name,
                     std::string& error)
{
  if (name.empty())
  {
    error = "Empty path";
    return NULL;
  }

  void* handle = NULL;
  handle = ::dlopen(name.c_str(), RTLD_NOW);

  if (!handle)
  {
    const char* zErrorString = ::dlerror();
    error += "Failed to load \"" + name + '"';

    if (zErrorString)
    {
      std::string dl_error = zErrorString;
      error += ": " + dl_error;
    }

    return NULL;
  }

  return new DynamicLibrary(handle);
}

//------------------------------------------------------------------------------
// Get symbol
//------------------------------------------------------------------------------
void*
DynamicLibrary::GetSymbol(const std::string& symbol)
{
  if (!mHandle)
  {
    std::cerr << "No handle object" << std::endl;
    return NULL;
  }

  void* dlsym_obj = ::dlsym(mHandle, symbol.c_str());
  const char* dlsym_error = ::dlerror();

  if (dlsym_error)
  {
    std::cerr << "Cannot load symbol: " << symbol
              << " error: " << dlsym_error << std::endl;
    return NULL;
  }

  return dlsym_obj;
}

EOSCOMMONNAMESPACE_END
