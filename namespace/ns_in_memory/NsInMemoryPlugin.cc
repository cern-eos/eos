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
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/ns_in_memory/NamespaceGroup.hh"
#include "namespace/ns_in_memory/NsInMemoryPlugin.hh"
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

  // Register namespace group
  PF_RegisterParams param_group;
  param_group.version.major = 0;
  param_group.version.minor = 1;
  param_group.CreateFunc = eos::NsInMemoryPlugin::CreateGroup;
  param_group.DestroyFunc = eos::NsInMemoryPlugin::DestroyGroup;

  // TODO: define the necessary objects to be provided by the namespace in a
  // common header
  std::map<std::string, PF_RegisterParams> map_obj = {
        {"NamespaceGroup",      param_group}
      };

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

//----------------------------------------------------------------------------
//! Create namespace group
//!
//! @param services pointer to other services that the plugin manager might
//!         provide
//!
//! @return pointer to namespace group
//----------------------------------------------------------------------------
void*
NsInMemoryPlugin::CreateGroup(PF_PlatformServices* services)
{
  return new InMemNamespaceGroup();
}

//----------------------------------------------------------------------------
//! Destroy namespace group
//!
//! @return 0 if successful, otherwise errno
//----------------------------------------------------------------------------
int32_t
NsInMemoryPlugin::DestroyGroup(void* obj)
{
  if(!obj) {
    return -1;
  }

  delete static_cast<InMemNamespaceGroup*>(obj);
  return 0;
}

EOSNSNAMESPACE_END
