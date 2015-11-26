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
#include "namespace/ns_in_memory/NsInMemoryPlugin.hh"
#include "namespace/ns_in_memory/persistency/ChangeLogContainerMDSvc.hh"
#include "namespace/ns_in_memory/persistency/ChangeLogFileMDSvc.hh"
#include "namespace/ns_in_memory/views/HierarchicalView.hh"
#include "namespace/ns_in_memory/accounting/FileSystemView.hh"
#include "namespace/ns_in_memory/accounting/ContainerAccounting.hh"
#include "namespace/ns_in_memory/accounting/SyncTimeAccounting.hh"
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
  std::cout << "Register objects provide by NsInMemoryPlugin ..." << std::endl;

  // Register container metadata service
  PF_RegisterParams param_cmdsvc;
  param_cmdsvc.version.major = 0;
  param_cmdsvc.version.minor = 1;
  param_cmdsvc.CreateFunc = eos::NsInMemoryPlugin::CreateContainerMDSvc;
  param_cmdsvc.DestroyFunc = eos::NsInMemoryPlugin::DestroyContainerMDSvc;

  // Register file metadata service
  PF_RegisterParams param_fmdsvc;
  param_fmdsvc.version.major = 0;
  param_fmdsvc.version.minor = 1;
  param_fmdsvc.CreateFunc = eos::NsInMemoryPlugin::CreateFileMDSvc;
  param_fmdsvc.DestroyFunc = eos::NsInMemoryPlugin::DestroyFileMDSvc;

  // Register hierarchical view
  PF_RegisterParams param_hview;
  param_hview.version.major = 0;
  param_hview.version.minor = 1;
  param_hview.CreateFunc = eos::NsInMemoryPlugin::CreateHierarchicalView;
  param_hview.DestroyFunc = eos::NsInMemoryPlugin::DestroyHierarchicalView;

  // Register file system view
  PF_RegisterParams param_fsview;
  param_fsview.version.major = 0;
  param_fsview.version.minor = 1;
  param_fsview.CreateFunc = eos::NsInMemoryPlugin::CreateFsView;
  param_fsview.DestroyFunc = eos::NsInMemoryPlugin::DestroyFsView;

  // Register recursive container accounting view
  PF_RegisterParams param_contacc;
  param_contacc.version.major = 0;
  param_contacc.version.minor = 1;
  param_contacc.CreateFunc = eos::NsInMemoryPlugin::CreateContAcc;
  param_contacc.DestroyFunc = eos::NsInMemoryPlugin::DestroyContAcc;

  // Register recursive container accounting view
  PF_RegisterParams param_syncacc;
  param_syncacc.version.major = 0;
  param_syncacc.version.minor = 1;
  param_syncacc.CreateFunc = eos::NsInMemoryPlugin::CreateSyncTimeAcc;
  param_syncacc.DestroyFunc = eos::NsInMemoryPlugin::DestroySyncTimeAcc;

  // TODO: define the necessary objects to be provided by the namespace in a
  // common header
  std::map<std::string, PF_RegisterParams> map_obj =
      { {"ContainerMDSvc",      param_cmdsvc},
        {"FileMDSvc",           param_fmdsvc},
        {"HierarchicalView",    param_hview},
        {"FileSystemView",      param_fsview},
        {"ContainerAccounting", param_contacc},
        {"SyncTimeAccounting",  param_syncacc} };

  // Register all the provided object with the Plugin Manager
  for (auto it = map_obj.begin(); it != map_obj.end(); ++it)
  {
    if (services->registerObject(it->first.c_str(), &it->second))
    {
      std::cerr << "Failed registering object " << it->first << std::endl;
      return NULL;
    }
  }

  return ExitFunc;
}

EOSNSNAMESPACE_BEGIN

// Static variables
eos::IContainerMDSvc* pContMDSvc = 0;

//------------------------------------------------------------------------------
// Create container metadata service
//------------------------------------------------------------------------------
void*
NsInMemoryPlugin::CreateContainerMDSvc(PF_PlatformServices* services)
{
  pContMDSvc = new ChangeLogContainerMDSvc();
  return pContMDSvc;
}

//------------------------------------------------------------------------------
// Destroy container metadata service
//------------------------------------------------------------------------------
int32_t
NsInMemoryPlugin::DestroyContainerMDSvc(void* obj)
{
  if (!obj)
    return -1;

  delete static_cast<ChangeLogContainerMDSvc*>(obj);
  return 0;
}

//------------------------------------------------------------------------------
// Create file metadata service
//------------------------------------------------------------------------------
void*
NsInMemoryPlugin::CreateFileMDSvc(PF_PlatformServices* services)
{
  return new ChangeLogFileMDSvc();
}

//------------------------------------------------------------------------------
// Destroy file metadata service
//------------------------------------------------------------------------------
int32_t
NsInMemoryPlugin::DestroyFileMDSvc(void* obj)
{
  if (!obj)
    return -1;

  delete static_cast<ChangeLogFileMDSvc*>(obj);
  return 0;
}

//------------------------------------------------------------------------------
// Create hierarchical view
//------------------------------------------------------------------------------
void*
NsInMemoryPlugin::CreateHierarchicalView(PF_PlatformServices* services)
{
  return new HierarchicalView();
}

//------------------------------------------------------------------------------
// Destroy hierarchical view
//------------------------------------------------------------------------------
int32_t
NsInMemoryPlugin::DestroyHierarchicalView(void* obj)
{
  if (!obj)
    return -1;

  delete static_cast<HierarchicalView*>(obj);
  return 0;
}

//------------------------------------------------------------------------------
// Create file system view
//------------------------------------------------------------------------------
void*
NsInMemoryPlugin::CreateFsView(PF_PlatformServices* services)
{
  return new FileSystemView();
}

//------------------------------------------------------------------------------
// Destroy file system view
//------------------------------------------------------------------------------
int32_t
NsInMemoryPlugin::DestroyFsView(void* obj)
{
  if (!obj)
    return -1;

  delete static_cast<FileSystemView*>(obj);
  return 0;
}

//------------------------------------------------------------------------------
// Create recursive container accounting listener
//------------------------------------------------------------------------------
void*
NsInMemoryPlugin::CreateContAcc(PF_PlatformServices* services)
{
  if (!pContMDSvc)
    return 0;

  return new ContainerAccounting(pContMDSvc);
}

//------------------------------------------------------------------------------
// Destroy recursive container accounting listener
//------------------------------------------------------------------------------
int32_t
NsInMemoryPlugin::DestroyContAcc(void* obj)
{
  if (!obj)
    return -1;

  delete static_cast<ContainerAccounting*>(obj);
  return 0;
}

//------------------------------------------------------------------------------
// Create sync time propagation listener
//------------------------------------------------------------------------------
void*
NsInMemoryPlugin::CreateSyncTimeAcc(PF_PlatformServices* services)
{
  if (!pContMDSvc)
    return 0;

  return new SyncTimeAccounting(pContMDSvc);
}

//------------------------------------------------------------------------------
// Destroy sync time propagation listener
//------------------------------------------------------------------------------
int32_t
NsInMemoryPlugin::DestroySyncTimeAcc(void* obj)
{
  if (!obj)
    return -1;

  delete static_cast<FileSystemView*>(obj);
  return 0;
}

EOSNSNAMESPACE_END
