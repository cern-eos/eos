// ----------------------------------------------------------------------
// File: GrpcClient.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

#pragma once

/*----------------------------------------------------------------------------*/
#include "client/Namespace.hh"
#include "common/AssistedThread.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
#ifdef EOS_GRPC
#include <grpc++/grpc++.h>
#include "proto/Rpc.grpc.pb.h"


/*----------------------------------------------------------------------------*/

EOSCLIENTNAMESPACE_BEGIN

/**
 * @file   GrpcClient.hh
 *
 * @brief  This class implements a gRPC client for an EOS grpc server
 *
 */


class GrpcClient
{
public:
  explicit GrpcClient(std::shared_ptr<grpc::Channel> channel)
    : stub_(eos::rpc::Eos::NewStub(channel)) {}

  std::string Ping(const std::string& payload);

  std::string Md(const std::string& path);
  std::string MdId(uint64_t id);
  std::string MdIno(uint64_t ino);


private:
  std::unique_ptr<eos::rpc::Eos::Stub> stub_;
};

#endif

EOSCLIENTNAMESPACE_END

