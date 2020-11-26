//------------------------------------------------------------------------------
//! @file LayoutPlugin.hh
//! @author Andreas-Joachim Peters - CERN
//! @brief Class generating a layout plugin object
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

#pragma once
#include "common/LayoutId.hh"
#include "fst/Namespace.hh"
#include "XrdSec/XrdSecEntity.hh"

EOSFSTNAMESPACE_BEGIN

class XrdFstOfsFile;
class Layout;

using eos::common::LayoutId;

//------------------------------------------------------------------------------
//! Class used to obtain a layout plugin object
//------------------------------------------------------------------------------
class LayoutPlugin
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  LayoutPlugin() = default;

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~LayoutPlugin() = default;

  //----------------------------------------------------------------------------
  //! Get layout object
  //!
  //! @param file file handler
  //! @param layoutId layout id type
  //! @param client security entity
  //! @param error error information
  //! @param accessType access type ( ofs/xrd )
  //! @param timeout timeout value
  //! @param storeRecovery store recovered blocks
  //!
  //! @return requested layout type object
  //!
  //----------------------------------------------------------------------------
  static Layout* GetLayoutObject(XrdFstOfsFile* file,
                                 unsigned long layoutId,
                                 const XrdSecEntity* client,
                                 XrdOucErrInfo* error,
                                 const char* path,
                                 uint16_t timeout = 0,
                                 bool storeRecovery = false);
};

EOSFSTNAMESPACE_END
