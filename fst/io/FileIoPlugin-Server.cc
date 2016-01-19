//------------------------------------------------------------------------------
//! @file FileIoPlugin.cc
//! @author Geoffray Adde - CERN
//! @brief Implementation of the FileIoPlugin for a client
//------------------------------------------------------------------------------

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

/*----------------------------------------------------------------------------*/
#include "fst/io/FileIoPlugin.hh"
#include "fst/io/FileIoPluginCommon.hh"
#ifdef KINETICIO_FOUND
#include "fst/io/kinetic/KineticIo.hh"
#endif
#ifdef DAVIX_FOUND
#include "fst/io/DavixIo.hh"
#endif
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

using eos::common::LayoutId;

FileIo*
FileIoPlugin::GetIoObject (std::string path,
                           XrdFstOfsFile* file,
                           const XrdSecEntity* client)
{
  auto ioType = eos::common::LayoutId::GetIoType(path.c_str());

  if (ioType == LayoutId::kLocal)
  {
    return static_cast<FileIo*> (new LocalIo(path, file, client));
  }
  else
    if (ioType == LayoutId::kXrdCl)
  {
    return static_cast<FileIo*> (new XrdIo(path));
  }
  else
    if (ioType == LayoutId::kKinetic)
  {
#ifdef KINETICIO_FOUND
      FileIo* kio = NULL;
      try{
        kio = static_cast<FileIo*> ((FileIo*)new KineticIo(path));
      }
      catch (const std::exception& e) {
        eos_static_err("Failed constructing kinetic io object: %s", e.what());
      }
      return kio;
#endif
    eos_static_warning("EOS has been compiled without Kinetic support.");
    return NULL;
  }
  else
    if (ioType == LayoutId::kRados)
  {
    return static_cast<FileIo*> (new RadosIo(path, file, client));
  }
  else
    if (ioType == LayoutId::kDavix)
  {
#ifdef DAVIX_FOUND
    return static_cast<FileIo*> (new DavixIo());
#endif
    eos_static_warning("EOS has been compiled without DAVIX support.");
    return NULL;
  }
  else
    return FileIoPluginHelper::GetIoObject(path, file, client);
}

EOSFSTNAMESPACE_END

