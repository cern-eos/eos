/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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
//! @brief Class to retrieve metadata from the backend - no caching!
//!        TODO: Make asynchronous, try building continuations out of std::future
//------------------------------------------------------------------------------

#pragma once
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include "namespace/ns_quarkdb/ContainerMD.hh"
#include "qclient/QClient.hh"

using redisReplyPtr = qclient::redisReplyPtr;

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class MetadataFetcher
//------------------------------------------------------------------------------

class MetadataFetcher {
public:
  static eos::ns::ContainerMdProto getContainerFromId(qclient::QClient &qcl, id_t id);

private:
  static std::string extractString(redisReplyPtr &reply, id_t forId);

};

EOSNSNAMESPACE_END
