//------------------------------------------------------------------------------
//! @file INamespaceGroup.hh
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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
#include "namespace/Namespace.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Forward declarations
//------------------------------------------------------------------------------
class IContainerMDSvc;
class IFileMDSvc;

//------------------------------------------------------------------------------
//! Interface object to hold ownership of all namespace objects.
//! Under construction..
//------------------------------------------------------------------------------
class INamespaceGroup {
public:
  //----------------------------------------------------------------------------
  //! Virtual destructor
  //----------------------------------------------------------------------------
  virtual ~INamespaceGroup() {}

  //----------------------------------------------------------------------------
  //! Provide file service
  //----------------------------------------------------------------------------
  virtual IFileMDSvc* getFileService() = 0;

  //----------------------------------------------------------------------------
  //! Provide container service
  //----------------------------------------------------------------------------
  virtual IContainerMDSvc* getContainerService() = 0;

};

EOSNSNAMESPACE_END
