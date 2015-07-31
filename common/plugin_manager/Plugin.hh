//------------------------------------------------------------------------------
//! @file Plugin.hh
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

#ifndef __PF_PLUGIN_HH__
#define __PF_PLUGIN_HH__

/*----------------------------------------------------------------------------*/
#include <cstdint>
/*----------------------------------------------------------------------------*/

#ifdef __cplusplus
extern "C" {
#endif

//! Foward declaration
struct PF_PlatformServices;

//------------------------------------------------------------------------------
//! Plugin layer value indicating the layer for which the plugin object is
//! responsible
//------------------------------------------------------------------------------
typedef enum PF_Plugin_Layer
{
  L0, // bottom one
  L1,
  L2,
  L3  // top one
} PF_Plugin_Layer;

//------------------------------------------------------------------------------
//! Plugin version information
//------------------------------------------------------------------------------
typedef struct PF_PluginAPI_Version
{
  int32_t major;
  int32_t minor;
} PF_PluginAPI_Version;

//------------------------------------------------------------------------------
//! Create and destroy methods to be implemented by the plugin objects. Through
//! these the PluginManager manages the lifetime of plugin objects
//------------------------------------------------------------------------------
typedef void* (*PF_CreateFunc)(PF_PlatformServices*);
typedef int32_t (*PF_DestroyFunc)(void*);

//------------------------------------------------------------------------------
//! Parameters registered by the plugin objects with the PluginManager
//------------------------------------------------------------------------------
typedef struct PF_RegisterParams
{
  PF_PluginAPI_Version version;
  PF_CreateFunc CreateFunc;
  PF_DestroyFunc DestroyFunc;
  PF_Plugin_Layer layer;
} PF_RegisterParams;

//------------------------------------------------------------------------------
//! Register function pointer used by a plugin object to register itself with
//! the PluginManager
//------------------------------------------------------------------------------
typedef int32_t (*PF_RegisterFunc)(const char* objType,
                                   const PF_RegisterParams* params);

//------------------------------------------------------------------------------
//! Function pointer calling platform services specified by name. This can be
//! used by the plugin object to pass information to/from the main application
//! e.g allocate memory, logging etc.
//------------------------------------------------------------------------------
typedef int32_t (*PF_InvokeServiceFunc)(const char* serviceName,
                                        void* serviceParams);

//------------------------------------------------------------------------------
//! Platform services structure provided by the PluginManager to any loaded
//! plugin. It describes the PM's version, register function to be called by
//! the plugin object and also other possible services which are made available
//! by the PM to the plugin objects.
//------------------------------------------------------------------------------
typedef struct PF_PlatformServices
{
  PF_PluginAPI_Version version;
  PF_RegisterFunc registerObject;
  PF_InvokeServiceFunc invokeService;
} PF_PlatformServices;

//------------------------------------------------------------------------------
//! Discovery service parameters
//------------------------------------------------------------------------------
typedef struct PF_Discovery_Service
{
  const char* objType;
  void* ptrService;
} PF_Discovery_Service;

//------------------------------------------------------------------------------
//! Exit function pointer returned after registering a new plugin. This is
//! called by the PluginManager when destroying loaded plugins.
//! NOTE: this does not destroy the plugin objects created by this plugin
//------------------------------------------------------------------------------
typedef int32_t (*PF_ExitFunc)();

//------------------------------------------------------------------------------
//! Function used by the PluginManager to initialize registered plugins.
//! The plugin may use the runtime services - allocate memory, log messages
//! and of course register plugin objects.
//!
//! @param  params  platform services struct
//!
//! @return the exit func of the plugin or NULL if initialization failed
//------------------------------------------------------------------------------
typedef PF_ExitFunc (*PF_InitFunc)(const PF_PlatformServices*);

extern
#ifdef  __cplusplus
  "C"
#endif

//------------------------------------------------------------------------------
//! Each plugin implementation must contain this function with the same name
//! and signature. The method is called each time a new plugin library is loaded.
//!
//! @param  params the platform services struct
//!
//! @return the exit func of the plugin or NULL if initialization failed
//------------------------------------------------------------------------------
PF_ExitFunc PF_initPlugin(const PF_PlatformServices* params);

#ifdef  __cplusplus
}
#endif

#endif // __PF_PLUGIN_HH__
