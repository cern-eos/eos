//------------------------------------------------------------------------------
//! @file PluginManager.cc
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
#include <string>
#include <iostream>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
/*----------------------------------------------------------------------------*/
#include "PluginManager.hh"
#include "DynamicLibrary.hh"
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

static std::vector<std::string> sDynLibExtensions {".so", ".dylib"};

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
PluginManager::PluginManager()
{
  mPlatformServices.version.major = 0;
  mPlatformServices.version.minor = 1;
  mPlatformServices.invokeService = NULL; // can be populated during LoadAll()
  mPlatformServices.registerObject = RegisterObject;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
PluginManager::~PluginManager()
{
  // Just in case it wasn't called earlier
  Shutdown();
}

//------------------------------------------------------------------------------
// Get instance of PluginManager
//------------------------------------------------------------------------------
PluginManager&
PluginManager::GetInstance()
{
  static PluginManager instance;
  return instance;
}

//------------------------------------------------------------------------------
// Shutdown method
//------------------------------------------------------------------------------
int32_t PluginManager::Shutdown()
{
  int32_t result = 0;

  for (auto func = mExitFuncVec.begin(); func != mExitFuncVec.end(); ++func)
  {
    try
    {
      result += (*func)();
    }
    catch (...)
    {
      result = -1;
    }
  }

  mDynamicLibMap.clear();
  mObjectMap.clear();
  mExitFuncVec.clear();
  return result;
}

//------------------------------------------------------------------------------
// The registration params may be received from an external plugin so it is
// crucial to validate it, because it was never subject to our tests
//------------------------------------------------------------------------------
static bool IsValid(const char* objType,
                    const PF_RegisterParams* params)
{
  if (!objType)
    return false;

  // Plugin does not implement the interface
  if (!params || !params->CreateFunc || !params->DestroyFunc)
    return false;

  return true;
}

//------------------------------------------------------------------------------
// Initialize plugin
//------------------------------------------------------------------------------
int32_t
PluginManager::InitializePlugin(PF_InitFunc initFunc)
{
  PluginManager& pm = PluginManager::GetInstance();
  PF_ExitFunc exitFunc = initFunc(&pm.mPlatformServices);

  if (!exitFunc)
    return -1;

  // Store the exit func so it can be called when unloading this plugin
  pm.mExitFuncVec.push_back(exitFunc);
  return 0;
}

//------------------------------------------------------------------------------
// Plugin registers the objects that it provides through this function
//------------------------------------------------------------------------------
int32_t
PluginManager::RegisterObject(const char* objType,
                              const PF_RegisterParams* params)
{
  // Check parameters
  if (!IsValid(objType, params))
    return -1;

  std::string key = std::string(objType);
  PluginManager& pm = PluginManager::GetInstance();

  // Verify that versions match
  PF_PluginAPI_Version v = pm.mPlatformServices.version;

  if (v.major != params->version.major)
  {
    std::cerr << "Plugin manager API and plugin object API version missmatch"
              << std::endl;
    return -1;
  }

  // Fail if item already exists (only one can handle)
  if (pm.mObjectMap.find(key) != pm.mObjectMap.end())
  {
    std::cerr << "Error, object type already registered" << std::endl;
    return -1;
  }

  pm.mObjectMap[key] = *params;
  return 0;
}

//------------------------------------------------------------------------------
// Load all dynamic libraries from directory
//------------------------------------------------------------------------------
int32_t
PluginManager::LoadAll(std::string dir_path,
                       PF_InvokeServiceFunc func)
{
  if (dir_path.empty())
  {
    std::cerr << "Plugin path is empty" << std::endl;
    return -1;
  }

  // If relative path, get current working directory
  if (dir_path[0] == '.')
  {
    char* cwd {0};
    size_t size {0};
    cwd = getcwd(cwd, size);

    if (cwd)
    {
      std::string tmp_path = cwd;
      dir_path = dir_path.erase(0, 1);
      dir_path = tmp_path + dir_path;
      free(cwd);
    }
  }

  // Add backslash at the end
  if (dir_path[dir_path.length() - 1] != '/')
    dir_path += '/';

  if (func)
    mPlatformServices.invokeService = func;

  auto dir = opendir(dir_path.c_str());

  if (dir == 0)
  {
    std::cerr << "Cannot open dir: " << dir_path << std::endl;
    return -1;
  }

  std::string full_path;
  struct dirent* entity {0};

  while ((entity = readdir(dir)))
  {
    // Skip directories and link files
    if ((entity->d_type & DT_DIR) || (entity->d_type == DT_LNK))
      continue;

    full_path= dir_path + entity->d_name;

    // Try all accepted extensions
    for (auto extension = sDynLibExtensions.begin();
         extension != sDynLibExtensions.end();
         ++extension)
    {
      if (full_path.length() <= extension->length())
        continue;

      if (full_path.find(*extension) != std::string::npos)
      {
        LoadByPath(full_path);
        break;
      }
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// Get plugin object from dynamic library
//------------------------------------------------------------------------------
int32_t
PluginManager::LoadByPath(const std::string& lib_path)
{
  // Don't load the same dynamic library twice
  if (mDynamicLibMap.find(lib_path) != mDynamicLibMap.end())
    return -1;

  std::string error;
  DynamicLibrary* dyn_lib = LoadLibrary(lib_path, error);

  if (!dyn_lib)
  {
    std::cerr << error << std::endl;
    return -1;
  }

  // Get the *_initPlugin() function
  PF_InitFunc initFunc = (PF_InitFunc)(dyn_lib->GetSymbol("PF_initPlugin"));

  // Expected entry point missing from dynamic library
  if (!initFunc)
    return -1;

  int32_t res = InitializePlugin(initFunc);

  if (res < 0)
    return res;

  return 0;
}

//------------------------------------------------------------------------------
// Create plugin object for the specified layer
//------------------------------------------------------------------------------
void*
PluginManager::CreateObject(const std::string& objType)
{
  auto iter = mObjectMap.find(objType);

  if (iter != mObjectMap.end())
  {
    PF_RegisterParams& rp = iter->second;
    void* object = rp.CreateFunc(&mPlatformServices);

    // Register the new plugin object
    if (object)
      return object;
  }

  return NULL;
}

//------------------------------------------------------------------------------
// Load dynamic library
//------------------------------------------------------------------------------
DynamicLibrary*
PluginManager::LoadLibrary(const std::string& path, std::string& error)
{
  DynamicLibrary* dyn_lib = DynamicLibrary::Load(path, error);

  if (!dyn_lib)
    return NULL;

  // Add library to map, so it can be unloaded
  mDynamicLibMap[path] = std::shared_ptr<DynamicLibrary>(dyn_lib);
  return dyn_lib;
}

//------------------------------------------------------------------------------
// Get registration map
//------------------------------------------------------------------------------
const PluginManager::RegistrationMap&
PluginManager::GetRegistrationMap() const
{
  return mObjectMap;
}

//------------------------------------------------------------------------------
// Get available platform services
//------------------------------------------------------------------------------
PF_PlatformServices&
PluginManager::GetPlatformServices()
{
  return mPlatformServices;
}

EOSCOMMONNAMESPACE_END
