/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Utility to resolve files and containers based on proto messages
//------------------------------------------------------------------------------

#pragma once
#include <chrono>
#include "namespace/Namespace.hh"
#include "namespace/MDException.hh"
#include "namespace/interface/Identifiers.hh"
#include "namespace/interface/IView.hh"
#include "proto/Ns.pb.h"

class XrdOucString;

EOSNSNAMESPACE_BEGIN

using ContainerSpecificationProto =
  eos::console::NsProto_ContainerSpecificationProto;

class Resolver
{
public:

  //----------------------------------------------------------------------------
  //! Resolve a container specification message to a ContainerMD.
  //! Assumes caller holds eosViewRWMutex.
  //----------------------------------------------------------------------------
  static IContainerMDPtr resolveContainer(IView* view,
                                          const ContainerSpecificationProto& proto);

  //----------------------------------------------------------------------------
  //! Parse FileIdentifier based on an string.
  //! Recognizes "fid:", "fxid:", "ino:"
  //----------------------------------------------------------------------------
  static FileIdentifier retrieveFileIdentifier(XrdOucString& str);

};

EOSNSNAMESPACE_END
