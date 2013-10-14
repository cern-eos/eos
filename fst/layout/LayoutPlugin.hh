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

#ifndef __EOSFST_LAYOUTPLUGIN_HH__
#define __EOSFST_LAYOUTPLUGIN_HH__

/*----------------------------------------------------------------------------*/
#include "common/LayoutId.hh"
#include "fst/layout/Layout.hh"
#include "fst/Namespace.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

using eos::common::LayoutId;

//------------------------------------------------------------------------------
//! Class used to obtain a layout plugin object
//------------------------------------------------------------------------------

class LayoutPlugin
{
public:

  //--------------------------------------------------------------------------
  //! Constructor
  //--------------------------------------------------------------------------
  LayoutPlugin ();


  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  ~LayoutPlugin ();


  //--------------------------------------------------------------------------
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
  //--------------------------------------------------------------------------
  static Layout* GetLayoutObject (XrdFstOfsFile* file,
                                  unsigned long layoutId,
                                  const XrdSecEntity* client,
                                  XrdOucErrInfo* error,
                                  eos::common::LayoutId::eIoType accessType,
                                  uint16_t timeout = 0,
                                  bool storeRecovery = false);
};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_LAYOUTPLUGIN_HH__
