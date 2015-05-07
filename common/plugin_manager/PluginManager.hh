//------------------------------------------------------------------------------
//! @file PluginManager.hh
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

#ifndef __EOS_PF_PLUGIN_MANAGER_HH__
#define __EOS_PF_PLUGIN_MANAGER_HH__

/*----------------------------------------------------------------------------*/
#include <vector>
#include <map>
#include <memory>
/*----------------------------------------------------------------------------*/
#include "Plugin.hh"
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

//! Forward declaration
class DynamicLibrary;

//----------------------------------------------------------------------------
//! Class Plugin Manager
//----------------------------------------------------------------------------
class PluginManager
{
  typedef std::map<std::string, std::shared_ptr<DynamicLibrary> > DynamicLibMap;
  typedef std::vector<PF_ExitFunc> ExitFuncVec;

public:

  typedef std::map<std::string, PF_RegisterParams> RegistrationMap;

  //----------------------------------------------------------------------------
  //! Get instance of PluginManager
  //----------------------------------------------------------------------------
  static PluginManager& GetInstance();

  //----------------------------------------------------------------------------
  //! Initialize plugin object
  //!
  //! @param initFunc plugin init function
  //!
  //! @return 0 if successful, otherwise !0
  //----------------------------------------------------------------------------
  static int32_t InitializePlugin(PF_InitFunc initFunc);

  //----------------------------------------------------------------------------
  //! Load all dynamic libraries in the specified directory
  //!
  //! @param pluginDirectory path to directory
  //! @param fun callable to get various services provied by the PM
  //!
  //! @return 0 if successful, otherwise !0
  //----------------------------------------------------------------------------
  int32_t LoadAll(std::string pluginDirectory,
                  PF_InvokeServiceFunc func = NULL);

  //----------------------------------------------------------------------------
  //! Load dynamic library given its path
  //!
  //! @param path path to dynamic library
  //!
  //! @return 0 if successful, othewise !0
  //----------------------------------------------------------------------------
  int32_t LoadByPath(const std::string& path);

  //----------------------------------------------------------------------------
  //! Create a plugin object
  //!
  //! @param objType object type name
  //!
  //! @return pointer to newly create object if successful, otherwise NULL
  //----------------------------------------------------------------------------
  void* CreateObject(const std::string& objType);

  //----------------------------------------------------------------------------
  //! Cleanup function called before PluginManager is destroyed
  //!
  //! @return 0 if successful, otherwise !0
  //----------------------------------------------------------------------------
  int32_t Shutdown();

  //----------------------------------------------------------------------------
  //! Method called by the plugin to register the objects it provides
  //!
  //! @param objType object type name
  //! @param params paramters registered by the plugin
  //!
  //! @return 0 if successful, otherwise !0
  //----------------------------------------------------------------------------
  static int32_t RegisterObject(const char* objType,
                                const PF_RegisterParams* params);

  //----------------------------------------------------------------------------
  //! Initialize the plugin stack. The PluginManager takes care of initializing
  //! all the available plugins from bottop to top taking care of the dependencies
  //! between plugin objects. If a plugin layer is missing than the closest two
  //! plugin object are connected
  //!
  //! @return 0 if successful, otherwise !0
  //----------------------------------------------------------------------------
  int32_t InitPluginStack();

  //----------------------------------------------------------------------------
  //! Get registration map i.e. plugin object types available
  //----------------------------------------------------------------------------
  const RegistrationMap& GetRegistrationMap() const;

  //----------------------------------------------------------------------------
  //! Get services provided by the platform i.e. logging
  //----------------------------------------------------------------------------
  PF_PlatformServices& GetPlatformServices();

private:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  PluginManager();

  //----------------------------------------------------------------------------
  //! Copy constructor
  //----------------------------------------------------------------------------
  PluginManager(const PluginManager&) = delete;

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~PluginManager();

  //----------------------------------------------------------------------------
  //! Load dynamic library
  //!
  //! @param path path to library
  //! @param errorString error in string format
  //!
  //! @return dynamic libary object
  //----------------------------------------------------------------------------
  DynamicLibrary* LoadLibrary(const std::string& path, std::string& errorString);

private:
  PF_PlatformServices mPlatformServices;
  DynamicLibMap mDynamicLibMap; ///< library path to DynamicLibrary obj. map
  ExitFuncVec mExitFuncVec;  ///< vector of ExitFunc object for each plugin
  RegistrationMap mObjectMap; ///< registered object types by plugins
};

EOSCOMMONNAMESPACE_END

#endif  // __PF_PLUGIN_MANAGER_HH__
