//------------------------------------------------------------------------------
//! @file FsckCmd.hh
//! @author Elvin Sindrilaru - CERN
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
#include "mgm/Namespace.hh"
#include "proto/Fsck.pb.h"
#include "mgm/proc/IProcCommand.hh"

#ifdef EOS_GRPC_GATEWAY
#include "proto/eos_rest_gateway/eos_rest_gateway_service.grpc.pb.h"
#endif

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class FsckCmd - class handling ns commands
//------------------------------------------------------------------------------
class FsckCmd: public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit FsckCmd(eos::console::RequestProto&& req,
                   eos::common::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, false)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FsckCmd() = default;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behaviour of the command executed
  //!
  //! @return ReplyProto object which contains the full response
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ProcessRequest() noexcept override;

#ifdef EOS_GRPC_GATEWAY
  //----------------------------------------------------------------------------
  //! Method implementing the specific behaviour of the command executed and
  //! streaming the response via the grpc::ServerWriter
  //!
  //! @writer object used for streaming back the response
  //----------------------------------------------------------------------------
  void ProcessRequest(grpc::ServerWriter<eos::console::ReplyProto>* writer);
#endif
};

EOSMGMNAMESPACE_END
