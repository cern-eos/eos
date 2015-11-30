/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2015 CERN/Switzerland                                  *
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
#include <iostream>
/*----------------------------------------------------------------------------*/
#include "namespace/ns_on_filesystem/NsRedisPlugin.hh"
#include "namespace/ns_on_filesystem/RedisContainerMDSvc.hh"
#include "namespace/ns_on_filesystem/RedisFileMDSvc.hh"
#include "namespace/ns_on_filesystem/RedisHierarchicalView.hh"
#include "namespace/ns_on_filesystem/RedisFileSystemView.hh"
/*----------------------------------------------------------------------------*/

//------------------------------------------------------------------------------
// Plugin exit function called by the PluginManager when doing cleanup
//------------------------------------------------------------------------------
int32_t ExitFunc()
{
  return 0;  
}

//------------------------------------------------------------------------------
// Plugin registration entry point called by the PluginManager
//------------------------------------------------------------------------------
PF_ExitFunc PF_initPlugin(const PF_PlatformServices* services)
{
  std::cout << "Register objects provide by NsRedisPlugin ..." << std::endl;

  // Register container metadata service
  PF_RegisterParams param_cmdsvc;
  param_cmdsvc.version.major = 0;
  param_cmdsvc.version.minor = 1;
  param_cmdsvc.CreateFunc = eos::NsRedisPlugin::CreateContainerMDSvc;
  param_cmdsvc.DestroyFunc = eos::NsRedisPlugin::DestroyContainerMDSvc;

  // Register file metadata service
  PF_RegisterParams param_fmdsvc;
  param_fmdsvc.version.major = 0;
  param_fmdsvc.version.minor = 1;
  param_fmdsvc.CreateFunc = eos::NsRedisPlugin::CreateFileMDSvc;
  param_fmdsvc.DestroyFunc = eos::NsRedisPlugin::DestroyFileMDSvc;

  // Register hierarchical view
  PF_RegisterParams param_hview;
  param_hview.version.major = 0;
  param_hview.version.minor = 1;
  param_hview.CreateFunc = eos::NsRedisPlugin::CreateHierarchicalView;
  param_hview.DestroyFunc = eos::NsRedisPlugin::DestroyHierarchicalView;

  // Register file system view
  PF_RegisterParams param_fsview;
  param_fsview.version.major = 0;
  param_fsview.version.minor = 1;
  param_fsview.CreateFunc = eos::NsRedisPlugin::CreateFsView;
  param_fsview.DestroyFunc = eos::NsRedisPlugin::DestroyFsView;

  // Define the necessary objects to be provided by the namespace in a
  // common header
  std::map<std::string, PF_RegisterParams> map_obj =
      { {"ContainerMDSvc",   param_cmdsvc},
        {"FileMDSvc",        param_fmdsvc},
        {"HierarchicalView", param_hview},
        {"FileSystemView",   param_fsview} };

  // Register all the provided object with the Plugin Manager
  for (auto it = map_obj.begin(); it != map_obj.end(); ++it)
  {
    if (services->registerObject(it->first.c_str(), &it->second))
    {
      std::cerr << "Failed to register object " << it->first << std::endl;
      return NULL;
    }
  }

  return ExitFunc;
}

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Create container metadata service
//------------------------------------------------------------------------------
void*
NsRedisPlugin::CreateContainerMDSvc(PF_PlatformServices* services)
{
  return new RedisContainerMDSvc();
}

//------------------------------------------------------------------------------
// Destroy container metadata service
//------------------------------------------------------------------------------
int32_t
NsRedisPlugin::DestroyContainerMDSvc(void* obj)
{
  if (!obj)
    return -1;

  delete static_cast<RedisContainerMDSvc*>(obj);
  return 0;
}

//------------------------------------------------------------------------------
// Create file metadata service
//------------------------------------------------------------------------------
void*
NsRedisPlugin::CreateFileMDSvc(PF_PlatformServices* services)
{
  return new RedisFileMDSvc();
}

//------------------------------------------------------------------------------
// Destroy file metadata service
//------------------------------------------------------------------------------
int32_t
NsRedisPlugin::DestroyFileMDSvc(void* obj)
{
  if (!obj)
    return -1;

  delete static_cast<RedisFileMDSvc*>(obj);
  return 0;
}

//------------------------------------------------------------------------------
// Create hierarchical view
//------------------------------------------------------------------------------
void*
NsRedisPlugin::CreateHierarchicalView(PF_PlatformServices* services)
{
  return new RedisHierarchicalView();
}

//------------------------------------------------------------------------------
// Destroy hierarchical view
//------------------------------------------------------------------------------
int32_t
NsRedisPlugin::DestroyHierarchicalView(void* obj)
{
  if (!obj)
    return -1;

  delete static_cast<RedisHierarchicalView*>(obj);
  return 0;
}

//------------------------------------------------------------------------------
// Create file system view
//------------------------------------------------------------------------------
void*
NsRedisPlugin::CreateFsView(PF_PlatformServices* services)
{
  return new RedisFileSystemView();
}

//------------------------------------------------------------------------------
// Destroy file system view
//------------------------------------------------------------------------------
int32_t
NsRedisPlugin::DestroyFsView(void* obj)
{
  if (!obj)
    return -1;

  delete static_cast<RedisFileSystemView*>(obj);
  return 0;
}

EOSNSNAMESPACE_END
