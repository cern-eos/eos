// ----------------------------------------------------------------------
// File: GrpcNsInterface.hh
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

#ifdef EOS_GRPC
#include "common/Mapping.hh"
#include "mgm/Namespace.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IView.hh"
#include "GrpcServer.hh"
#include "proto/Rpc.grpc.pb.h"
#include <grpc++/grpc++.h>

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/**
 * @file   GrpcNsInterface.hh
 *
 * @brief  This class bridges namespace to gRPC requests
 *
 */


class GrpcNsInterface
{
public:

  static bool Filter(std::shared_ptr<eos::IFileMD> fmd,
		     const eos::rpc::MDSelection& filter);

  static bool Filter(std::shared_ptr<eos::IContainerMD> cmd,
		     const eos::rpc::MDSelection& filter);


  static grpc::Status GetMD(eos::common::VirtualIdentity& vid,
                            grpc::ServerWriter<eos::rpc::MDResponse>* writer,
                            const eos::rpc::MDRequest* request, bool check_perms = true, 
			    bool lock=true);

  static grpc::Status StreamMD(eos::common::VirtualIdentity& vid,
                               grpc::ServerWriter<eos::rpc::MDResponse>* writer,
                               const eos::rpc::MDRequest* request, 
			       bool streamparent = true, 
			       std::vector<uint64_t>* childdirs = 0);

  static grpc::Status Find(eos::common::VirtualIdentity& vid,
			   grpc::ServerWriter<eos::rpc::MDResponse>* writer,
			   const eos::rpc::FindRequest* request);

  static grpc::Status NsStat(eos::common::VirtualIdentity& vid,
                             eos::rpc::NsStatResponse* reply,
                             const eos::rpc::NsStatRequest* request);

  static grpc::Status FileInsert(eos::common::VirtualIdentity& vid,
                                 eos::rpc::InsertReply* reply,
                                 const eos::rpc::FileInsertRequest* request);

  static grpc::Status ContainerInsert(eos::common::VirtualIdentity& vid,
                                      eos::rpc::InsertReply* reply,
                                      const eos::rpc::ContainerInsertRequest* request);

  static grpc::Status Exec(eos::common::VirtualIdentity& vid,
			    eos::rpc::NSResponse* reply,
			    const eos::rpc::NSRequest* request);
			  
  static grpc::Status Mkdir(eos::common::VirtualIdentity& vid, 
			    eos::rpc::NSResponse::ErrorResponse* reply,
			    const eos::rpc::NSRequest::MkdirRequest* request);

  static grpc::Status Rmdir(eos::common::VirtualIdentity& vid, 
			    eos::rpc::NSResponse::ErrorResponse* reply,
			    const eos::rpc::NSRequest::RmdirRequest* request);

  static grpc::Status Touch(eos::common::VirtualIdentity& vid, 
			    eos::rpc::NSResponse::ErrorResponse* reply,
			    const eos::rpc::NSRequest::TouchRequest* request);

  static grpc::Status Unlink(eos::common::VirtualIdentity& vid, 
			    eos::rpc::NSResponse::ErrorResponse* reply,
			    const eos::rpc::NSRequest::UnlinkRequest* request);

  static grpc::Status Rm(eos::common::VirtualIdentity& vid, 
			 eos::rpc::NSResponse::ErrorResponse* reply,
			 const eos::rpc::NSRequest::RmRequest* request);
  
  static grpc::Status Rename(eos::common::VirtualIdentity& vid, 
			    eos::rpc::NSResponse::ErrorResponse* reply,
			    const eos::rpc::NSRequest::RenameRequest* request);
  
  static grpc::Status Symlink(eos::common::VirtualIdentity& vid, 
			    eos::rpc::NSResponse::ErrorResponse* reply,
			    const eos::rpc::NSRequest::SymlinkRequest* request);

  static grpc::Status SetXAttr(eos::common::VirtualIdentity& vid, 
			       eos::rpc::NSResponse::ErrorResponse* reply,
			       const eos::rpc::NSRequest::SetXAttrRequest* request);

  static grpc::Status Version(eos::common::VirtualIdentity& vid, 
			      eos::rpc::NSResponse::VersionResponse* reply,
			      const eos::rpc::NSRequest::VersionRequest* request);

  static grpc::Status Recycle(eos::common::VirtualIdentity& vid, 
			      eos::rpc::NSResponse::RecycleResponse* reply,
			      const eos::rpc::NSRequest::RecycleRequest* request);

  static grpc::Status Chown(eos::common::VirtualIdentity& vid, 
			    eos::rpc::NSResponse::ErrorResponse* reply,
			    const eos::rpc::NSRequest::ChownRequest* request);
  
  static grpc::Status Chmod(eos::common::VirtualIdentity& vid, 
			    eos::rpc::NSResponse::ErrorResponse* reply,
			    const eos::rpc::NSRequest::ChmodRequest* request);

  static grpc::Status Acl(eos::common::VirtualIdentity& vid, 
			  eos::rpc::NSResponse::AclResponse* reply,
			  const eos::rpc::NSRequest::AclRequest* request);


  static grpc::Status Token(eos::common::VirtualIdentity& vid, 
			    eos::rpc::NSResponse::ErrorResponse* reply,
			    const eos::rpc::NSRequest::TokenRequest* request);

  static grpc::Status Quota(eos::common::VirtualIdentity& vid,
			    eos::rpc::NSResponse::QuotaResponse* reply,
			    const eos::rpc::NSRequest::QuotaRequest* request);
  
  static grpc::Status Share(eos::common::VirtualIdentity& vid,
			    eos::rpc::NSResponse::ShareResponse* reply,
			    const eos::rpc::NSRequest::ShareRequest* request);

  static bool Access(eos::common::VirtualIdentity& vid, int mode,
                     std::shared_ptr<eos::IContainerMD> cmd);

};

EOSMGMNAMESPACE_END
#endif
