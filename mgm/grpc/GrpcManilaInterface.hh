// ----------------------------------------------------------------------
// File: GrpcManilaInterface.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

#ifdef EOS_GRPC
#include "common/Mapping.hh"
#include "mgm/Namespace.hh"
#include "namespace/interface/IContainerMD.hh"
#include "GrpcServer.hh"
#include "proto/Rpc.grpc.pb.h"
#include <grpc++/grpc++.h>

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/**
 * @file   GrpcManilaInterface.hh
 *
 * @brief  This class bridges manila gRPC requests
 *
 */


class GrpcManilaInterface
{
public:

  static grpc::Status Process(eos::common::VirtualIdentity& vid,
			      eos::rpc::ManilaResponse* reply,
			      const eos::rpc::ManilaRequest* request);

  static int LoadManilaConfig(eos::common::VirtualIdentity& vid,
			      eos::rpc::ManilaResponse* reply, 
			      eos::IContainerMD::XAttrMap& config);

  static int LoadShareConfig(const std::string share_path, 
			      eos::IContainerMD::XAttrMap& managed);

  static int HandleShare(eos::rpc::ManilaResponse* reply,
			 const eos::rpc::ManilaRequest* request, 
			 eos::IContainerMD::XAttrMap& config, 
			 bool create, 
			 bool quota);

  static int CreateShare(eos::rpc::ManilaResponse* reply,
			 const eos::rpc::ManilaRequest* request, 
			 eos::IContainerMD::XAttrMap& config);
			 
  static int DeleteShare(eos::rpc::ManilaResponse* reply,
			 const eos::rpc::ManilaRequest* request, 
			 eos::IContainerMD::XAttrMap& config);

  static int ExtendShare(eos::rpc::ManilaResponse* reply,
			 const eos::rpc::ManilaRequest* request, 
			 eos::IContainerMD::XAttrMap& config);

  static int ShrinkShare(eos::rpc::ManilaResponse* reply,
			 const eos::rpc::ManilaRequest* request, 
			 eos::IContainerMD::XAttrMap& config);

  static int ManageShare(eos::rpc::ManilaResponse* reply,
			 const eos::rpc::ManilaRequest* request, 
			 eos::IContainerMD::XAttrMap& config);

  static int UnmanageShare(eos::rpc::ManilaResponse* reply,
			 const eos::rpc::ManilaRequest* request, 
			 eos::IContainerMD::XAttrMap& config);

  static int GetCapacityShare(eos::rpc::ManilaResponse* reply,
			 const eos::rpc::ManilaRequest* request, 
			 eos::IContainerMD::XAttrMap& config);


private:
  std::map<std::string, std::string> mManilaConfig;

  static bool ValidateManilaDirectoryTree(const std::string& manilapath, eos::rpc::ManilaResponse* reply);
  static bool ValidateManilaRequest(const eos::rpc::ManilaRequest* request, eos::rpc::ManilaResponse* reply);
  static std::string BuildShareDirectory(const eos::rpc::ManilaRequest* request, eos::IContainerMD::XAttrMap& config);
};

EOSMGMNAMESPACE_END
#endif
