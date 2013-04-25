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
#include "fst/layout/FileIoPlugin.hh"
#include "fst/layout/FileIoPluginCommon.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

using eos::common::LayoutId;

FileIo*
FileIoPlugin::GetIoObject (int ioType,
    XrdFstOfsFile* file ,
    const XrdSecEntity* client) {

  if (ioType == LayoutId::kLocal)
  {
    return static_cast<FileIo*> (new LocalIo(file, client));
  }

  return FileIoPluginHelper::GetIoObject (ioType, file, client);
}

EOSFSTNAMESPACE_END

