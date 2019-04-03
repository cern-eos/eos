/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "namespace/ns_quarkdb/NsQuarkdbPlugin.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/ns_quarkdb/accounting/ContainerAccounting.hh"
#include "namespace/ns_quarkdb/accounting/FileSystemView.hh"
#include "namespace/ns_quarkdb/accounting/SyncTimeAccounting.hh"
#include "namespace/ns_quarkdb/persistency/ContainerMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "namespace/ns_quarkdb/views/HierarchicalView.hh"
#include "namespace/ns_quarkdb/NamespaceGroup.hh"
#include "common/RWMutex.hh"
#include <iostream>

//------------------------------------------------------------------------------
// Plugin exit function called by the PluginManager when doing cleanup
//------------------------------------------------------------------------------
int32_t
ExitFunc()
{
  return 0;
}

//------------------------------------------------------------------------------
// Plugin registration entry point called by the PluginManager
//------------------------------------------------------------------------------
PF_ExitFunc
PF_initPlugin(const PF_PlatformServices* services)
{
  std::cout << "Register objects provided by NsQuarkdbPlugin ..." << std::endl;
  // Register container metadata service
  PF_RegisterParams param_cmdsvc = {};
  param_cmdsvc.version.major = 0;
  param_cmdsvc.version.minor = 1;
  param_cmdsvc.CreateFunc = eos::NsQuarkdbPlugin::CreateContainerMDSvc;
  param_cmdsvc.DestroyFunc = eos::NsQuarkdbPlugin::DestroyContainerMDSvc;
  // Register file system view
  PF_RegisterParams param_fsview = {};
  param_fsview.version.major = 0;
  param_fsview.version.minor = 1;
  param_fsview.CreateFunc = eos::NsQuarkdbPlugin::CreateFsView;
  param_fsview.DestroyFunc = eos::NsQuarkdbPlugin::DestroyFsView;
  // Register recursive container accounting view
  PF_RegisterParams param_contacc = {};
  param_contacc.version.major = 0;
  param_contacc.version.minor = 1;
  param_contacc.CreateFunc = eos::NsQuarkdbPlugin::CreateContAcc;
  param_contacc.DestroyFunc = eos::NsQuarkdbPlugin::DestroyContAcc;
  // Register recursive container accounting view
  PF_RegisterParams param_syncacc = {};
  param_syncacc.version.major = 0;
  param_syncacc.version.minor = 1;
  param_syncacc.CreateFunc = eos::NsQuarkdbPlugin::CreateSyncTimeAcc;
  param_syncacc.DestroyFunc = eos::NsQuarkdbPlugin::DestroySyncTimeAcc;
  // Register namespace group
  PF_RegisterParams param_group;
  param_group.version.major = 0;
  param_group.version.minor = 1;
  param_group.CreateFunc = eos::NsQuarkdbPlugin::CreateGroup;
  param_group.DestroyFunc = eos::NsQuarkdbPlugin::DestroyGroup;

  std::map<std::string, PF_RegisterParams> map_obj = {
    {"ContainerMDSvc", param_cmdsvc},
    {"FileSystemView", param_fsview},
    {"ContainerAccounting", param_contacc},
    {"SyncTimeAccounting", param_syncacc},
    {"NamespaceGroup", param_group},
  };

  // Register all the provided object with the Plugin Manager
  for (const auto& elem : map_obj) {
    if (services->registerObject(elem.first.c_str(), &elem.second) != 0) {
      std::cerr << "Failed registering object " << elem.first << std::endl;
      return nullptr;
    }
  }

  return ExitFunc;
}

EOSNSNAMESPACE_BEGIN

// Static variables
eos::IContainerMDSvc* NsQuarkdbPlugin::pContMDSvc = nullptr;

//----------------------------------------------------------------------------
//! Create namespace group
//!
//! @param services pointer to other services that the plugin manager might
//!         provide
//!
//! @return pointer to namespace group
//----------------------------------------------------------------------------
void*
NsQuarkdbPlugin::CreateGroup(PF_PlatformServices* services)
{
  return new QuarkNamespaceGroup();
}

//----------------------------------------------------------------------------
//! Destroy namespace group
//!
//! @return 0 if successful, otherwise errno
//----------------------------------------------------------------------------
int32_t
NsQuarkdbPlugin::DestroyGroup(void* obj)
{
  if(!obj) {
    return -1;
  }

  delete static_cast<QuarkNamespaceGroup*>(obj);
  return 0;
}

//------------------------------------------------------------------------------
// Create container metadata service
//------------------------------------------------------------------------------
void*
NsQuarkdbPlugin::CreateContainerMDSvc(PF_PlatformServices* /*services*/)
{
  if (pContMDSvc == nullptr) {
    pContMDSvc = new QuarkContainerMDSvc();
  }

  return pContMDSvc;
}

//------------------------------------------------------------------------------
// Destroy container metadata service
//------------------------------------------------------------------------------
int32_t
NsQuarkdbPlugin::DestroyContainerMDSvc(void* obj)
{
  if (obj == nullptr) {
    return -1;
  }

  delete static_cast<QuarkContainerMDSvc*>(obj);
  return 0;
}

//------------------------------------------------------------------------------
// Create file system view
//------------------------------------------------------------------------------
void*
NsQuarkdbPlugin::CreateFsView(PF_PlatformServices* /*services*/)
{
  return new QuarkFileSystemView();
}

//------------------------------------------------------------------------------
// Destroy file system view
//------------------------------------------------------------------------------
int32_t
NsQuarkdbPlugin::DestroyFsView(void* obj)
{
  if (obj == nullptr) {
    return -1;
  }

  delete static_cast<QuarkFileSystemView*>(obj);
  return 0;
}

//------------------------------------------------------------------------------
// Create recursive container accounting listener
//------------------------------------------------------------------------------
void*
NsQuarkdbPlugin::CreateContAcc(PF_PlatformServices* services)
{
  if (pContMDSvc == nullptr) {
    return nullptr;
  }

  if (!services->invokeService) {
    std::cerr << "ERROR: Platform does not provide a discovery service!"
              << std::endl;
    return nullptr;
  }

  // Request a pointer to the namespace view RW mutex
  PF_Discovery_Service ns_lock_svc;
  std::string request_svc {"NsViewMutex"};

  if (services->invokeService(request_svc.c_str(), &ns_lock_svc)) {
    std::cerr << "ERROR: Failed while requesting service: " << request_svc
              << std::endl;
    return nullptr;
  }

  std::string ptype = ns_lock_svc.objType;
  std::string rtype = "eos::common::RWMutex*";
  //std::string rtype = std::to_string(typeid(eos::common::RWMutex*).hash_code());
  free(ns_lock_svc.objType);

  if (ptype != rtype) {
    std::cerr << "ERROR: Provided and required object type hashes don't match: "
              << "ptype=" << ptype << ", rtype=" << rtype << std::endl;
    return nullptr;
  }

  eos::common::RWMutex* ns_mutex = static_cast<eos::common::RWMutex*>
                                   (ns_lock_svc.ptrService);
  return new QuarkContainerAccounting(pContMDSvc, ns_mutex);
}

//------------------------------------------------------------------------------
// Destroy recursive container accounting listener
//------------------------------------------------------------------------------
int32_t
NsQuarkdbPlugin::DestroyContAcc(void* obj)
{
  if (obj == nullptr) {
    return -1;
  }

  delete static_cast<QuarkContainerAccounting*>(obj);
  return 0;
}

//------------------------------------------------------------------------------
// Create sync time propagation listener
//------------------------------------------------------------------------------
void*
NsQuarkdbPlugin::CreateSyncTimeAcc(PF_PlatformServices* services)
{
  if (pContMDSvc == nullptr) {
    return nullptr;
  }

  if (!services->invokeService) {
    std::cerr << "ERROR: Platform does not provide a discovery service!"
              << std::endl;
    return nullptr;
  }

  // Request a pointer to the namespace view RW mutex
  PF_Discovery_Service ns_lock_svc;
  std::string request_svc {"NsViewMutex"};

  if (services->invokeService(request_svc.c_str(), &ns_lock_svc)) {
    std::cerr << "ERROR: Failed while requesting service: " << request_svc
              << std::endl;
    return nullptr;
  }

  std::string ptype = ns_lock_svc.objType;
  std::string rtype = "eos::common::RWMutex*";
  //std::string rtype = std::to_string(typeid(eos::common::RWMutex*).hash_code());
  free(ns_lock_svc.objType);

  if (ptype != rtype) {
    std::cerr << "ERROR: Provided and required object type hashes don't match: "
              << "ptype=" << ptype << ", rtype=" << rtype << std::endl;
    return nullptr;
  }

  eos::common::RWMutex* ns_mutex = static_cast<eos::common::RWMutex*>
                                   (ns_lock_svc.ptrService);
  return new QuarkSyncTimeAccounting(pContMDSvc, ns_mutex);
}

//------------------------------------------------------------------------------
// Destroy sync time propagation listener
//------------------------------------------------------------------------------
int32_t
NsQuarkdbPlugin::DestroySyncTimeAcc(void* obj)
{
  if (obj == nullptr) {
    return -1;
  }

  delete static_cast<QuarkFileSystemView*>(obj);
  return 0;
}

EOSNSNAMESPACE_END
