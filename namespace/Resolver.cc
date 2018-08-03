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

#include "namespace/Resolver.hh"
#include "common/ParseUtils.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Resolve a container specification message to a ContainerMD.
//! Assumes caller holds eosViewRWMutex.
//------------------------------------------------------------------------------
IContainerMDPtr Resolver::resolveContainer(IView *view, const ContainerSpecificationProto &proto) {
  ContainerSpecificationProto::ContainerCase type = proto.container_case();

  switch(type) {
    case ContainerSpecificationProto::kPath: {
      return view->getContainer(proto.path());
    }
    case ContainerSpecificationProto::kCid: {
      int64_t cid;
      if(!eos::common::parseInt64(proto.cid(), cid)) {
        throw_mdexception(EINVAL, "Unable to parse Container ID: " << proto.cid());
      }

      return view->getContainerMDSvc()->getContainerMD(cid);
    }
    case ContainerSpecificationProto::kCxid: {
      int64_t cid;
      if(!eos::common::parseInt64(proto.cxid(), cid, 16)) {
        throw_mdexception(EINVAL, "Unable to parse Container ID: " << proto.cxid());
      }

      return view->getContainerMDSvc()->getContainerMD(cid);
    }
    default: {
      throw_mdexception(EINVAL, "Provided protobuf message is empty, unable to resolve container");
    }
  }
}

EOSNSNAMESPACE_END
