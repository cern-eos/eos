//------------------------------------------------------------------------------
//! @file RainPlugin.hh
//! @author Elvin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2014 CERN/Switzerland                                  *
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

#ifndef __EOSFST_XRDCLPLUGINS_RAINPLUGIN_HH__
#define __EOSFST_XRDCLPLUGINS_RAINPLUGIN_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
#include <XrdCl/XrdClPlugInInterface.hh>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! RAIN plugin factory
//------------------------------------------------------------------------------
class RainFactory: public XrdCl::PlugInFactory, eos::common::LogId
{
 public:
  
  //----------------------------------------------------------------------------
  //! Construtor
  //----------------------------------------------------------------------------
  RainFactory();

  
  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~RainFactory();

  
  //----------------------------------------------------------------------------
  //! Create a file plug-in for the given URL
  //----------------------------------------------------------------------------
  virtual XrdCl::FilePlugIn* CreateFile( const std::string &url );

  
  //----------------------------------------------------------------------------
  //! Create a file system plug-in for the given URL
  //----------------------------------------------------------------------------
  virtual XrdCl::FileSystemPlugIn* CreateFileSystem( const std::string &url );
  
};

EOSFSTNAMESPACE_END;

#endif // __EOSFST_XRDCLPLUGINS_RAINPLUGIN_HH__

