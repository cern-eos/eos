// ----------------------------------------------------------------------
// File: GrpcNsInterface.cc
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

/*----------------------------------------------------------------------------*/
#include "GrpcNsInterface.hh"
/*----------------------------------------------------------------------------*/
#include "common/LayoutId.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IView.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/MDException.hh"
#include "namespace/interface/ContainerIterators.hh"

#include "mgm/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/


EOSMGMNAMESPACE_BEGIN

#ifdef EOS_GRPC
grpc::Status
GrpcNsInterface::GetMD(grpc::ServerWriter<eos::rpc::MDResponse>* writer,
                       const eos::rpc::MDRequest* request)
{
  if (request->type() == eos::rpc::FILE) {
    // stream file meta data
    eos::common::RWMutexReadLock viewReadLock;
    std::shared_ptr<eos::IFileMD> fmd;
    unsigned long fid  = 0;
    uint64_t clock = 0;
    std::string path;

    if (request->ino()) {
      // get by inode
      fid = eos::common::FileId::InodeToFid(request->ino());
    } else if (request->id()) {
      // get by fileid
      fid = request->id();
    }

    if (fid) {
      eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, fid);
    } else {
      eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, request->path());
    }

    viewReadLock.Grab(gOFS->eosViewRWMutex);

    if (fid) {
      try {
        fmd = gOFS->eosFileService->getFileMD(fid, &clock);
        path = gOFS->eosView->getUri(fmd.get());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_static_debug("caught exception %d %s\n", e.getErrno(),
                         e.getMessage().str().c_str());
        return grpc::Status((grpc::StatusCode)(errno), e.getMessage().str().c_str());
      }
    } else {
      try {
        fmd = gOFS->eosView->getFile(request->path());
        path = gOFS->eosView->getUri(fmd.get());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_static_debug("caught exception %d %s\n", e.getErrno(),
                         e.getMessage().str().c_str());
        return grpc::Status((grpc::StatusCode)(errno), e.getMessage().str().c_str());
      }
    }

    // create GRPC protobuf object
    eos::rpc::MDResponse gRPCResponse;
    gRPCResponse.set_type(eos::rpc::FILE);
    eos::rpc::FileMdProto gRPCFmd;
    gRPCResponse.mutable_fmd()->set_name(fmd->getName());
    gRPCResponse.mutable_fmd()->set_id(fmd->getId());
    gRPCResponse.mutable_fmd()->set_cont_id(fmd->getContainerId());
    gRPCResponse.mutable_fmd()->set_uid(fmd->getCUid());
    gRPCResponse.mutable_fmd()->set_gid(fmd->getCGid());
    gRPCResponse.mutable_fmd()->set_size(fmd->getSize());
    gRPCResponse.mutable_fmd()->set_layout_id(fmd->getLayoutId());
    gRPCResponse.mutable_fmd()->set_flags(fmd->getFlags());
    gRPCResponse.mutable_fmd()->set_link_name(fmd->getLink());
    eos::IFileMD::ctime_t ctime;
    eos::IFileMD::ctime_t mtime;
    fmd->getCTime(ctime);
    fmd->getMTime(mtime);
    gRPCResponse.mutable_fmd()->mutable_ctime()->set_sec(ctime.tv_sec);
    gRPCResponse.mutable_fmd()->mutable_ctime()->set_n_sec(ctime.tv_nsec);
    gRPCResponse.mutable_fmd()->mutable_mtime()->set_sec(mtime.tv_sec);
    gRPCResponse.mutable_fmd()->mutable_mtime()->set_n_sec(mtime.tv_nsec);
    gRPCResponse.mutable_fmd()->mutable_checksum()->set_value(
      fmd->getChecksum().getDataPtr(), fmd->getChecksum().size());
    gRPCResponse.mutable_fmd()->mutable_checksum()->set_type(
      eos::common::LayoutId::GetChecksumStringReal(fmd->getLayoutId()));

    for (const auto& loca : fmd->getLocations()) {
      gRPCResponse.mutable_fmd()->add_locations(loca);
    }

    for (const auto& loca : fmd->getUnlinkedLocations()) {
      gRPCResponse.mutable_fmd()->add_unlink_locations(loca);
    }

    for (const auto& elem : fmd->getAttributes()) {
      (*gRPCResponse.mutable_fmd()->mutable_xattrs())[elem.first] = elem.second;
    }

    gRPCResponse.mutable_fmd()->set_path(path);
    writer->Write(gRPCResponse);
    return grpc::Status::OK;
  } else if (request->type() == eos::rpc::CONTAINER) {
    // stream container meta data
    eos::common::RWMutexReadLock viewReadLock;
    std::shared_ptr<eos::IContainerMD> cmd;
    unsigned long cid  = 0;
    uint64_t clock = 0;
    std::string path;

    if (request->ino()) {
      // get by inode
      cid = request->ino();
    } else if (request->id()) {
      // get by containerid
      cid = request->id();
    }

    if (!cid) {
      eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView, request->path());
    }

    viewReadLock.Grab(gOFS->eosViewRWMutex);

    if (cid) {
      try {
        cmd = gOFS->eosDirectoryService->getContainerMD(cid, &clock);
        path = gOFS->eosView->getUri(cmd.get());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_static_debug("caught exception %d %s\n", e.getErrno(),
                         e.getMessage().str().c_str());
        return grpc::Status((grpc::StatusCode)(errno), e.getMessage().str().c_str());
      }
    } else {
      try {
        cmd = gOFS->eosView->getContainer(request->path());
        path = gOFS->eosView->getUri(cmd.get());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_static_debug("caught exception %d %s\n", e.getErrno(),
                         e.getMessage().str().c_str());
        return grpc::Status((grpc::StatusCode)(errno), e.getMessage().str().c_str());
      }
    }

    // create GRPC protobuf object
    eos::rpc::MDResponse gRPCResponse;
    gRPCResponse.set_type(eos::rpc::FILE);
    eos::rpc::ContainerMdProto gRPCFmd;
    gRPCResponse.mutable_cmd()->set_name(cmd->getName());
    gRPCResponse.mutable_cmd()->set_id(cmd->getId());
    gRPCResponse.mutable_cmd()->set_parent_id(cmd->getParentId());
    gRPCResponse.mutable_cmd()->set_uid(cmd->getCUid());
    gRPCResponse.mutable_cmd()->set_gid(cmd->getCGid());
    gRPCResponse.mutable_cmd()->set_tree_size(cmd->getTreeSize());
    gRPCResponse.mutable_cmd()->set_flags(cmd->getFlags());
    eos::IContainerMD::ctime_t ctime;
    eos::IContainerMD::ctime_t mtime;
    eos::IContainerMD::ctime_t stime;
    cmd->getCTime(ctime);
    cmd->getMTime(mtime);
    cmd->getTMTime(stime);
    gRPCResponse.mutable_cmd()->mutable_ctime()->set_sec(ctime.tv_sec);
    gRPCResponse.mutable_cmd()->mutable_ctime()->set_n_sec(ctime.tv_nsec);
    gRPCResponse.mutable_cmd()->mutable_mtime()->set_sec(mtime.tv_sec);
    gRPCResponse.mutable_cmd()->mutable_mtime()->set_n_sec(mtime.tv_nsec);
    gRPCResponse.mutable_cmd()->mutable_stime()->set_sec(stime.tv_sec);
    gRPCResponse.mutable_cmd()->mutable_stime()->set_n_sec(stime.tv_nsec);

    for (const auto& elem : cmd->getAttributes()) {
      (*gRPCResponse.mutable_cmd()->mutable_xattrs())[elem.first] = elem.second;
    }

    gRPCResponse.mutable_cmd()->set_path(path);
    writer->Write(gRPCResponse);
    return grpc::Status::OK;
  }

  return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid argument");
}

grpc::Status
GrpcNsInterface::StreamMD(grpc::ServerWriter<eos::rpc::MDResponse>* writer,
                          const eos::rpc::MDRequest* request)
{
  // stream container meta data
  eos::common::RWMutexReadLock viewReadLock;
  std::shared_ptr<eos::IContainerMD> cmd;
  unsigned long cid  = 0;
  uint64_t clock = 0;
  std::string path;

  if (request->ino()) {
    // get by inode
    cid = request->ino();
  } else if (request->id()) {
    // get by containerid
    cid = request->id();
  }

  if (!cid) {
    eos::Prefetcher::prefetchContainerMDWithChildrenAndWait(gOFS->eosView,
        request->path());
  }

  viewReadLock.Grab(gOFS->eosViewRWMutex);

  if (cid) {
    try {
      cmd = gOFS->eosDirectoryService->getContainerMD(cid, &clock);
      path = gOFS->eosView->getUri(cmd.get());
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_static_debug("caught exception %d %s\n", e.getErrno(),
                       e.getMessage().str().c_str());
      return grpc::Status((grpc::StatusCode)(errno), e.getMessage().str().c_str());
    }
  } else {
    try {
      cmd = gOFS->eosView->getContainer(request->path());
      path = gOFS->eosView->getUri(cmd.get());
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_static_debug("caught exception %d %s\n", e.getErrno(),
                       e.getMessage().str().c_str());
      return grpc::Status((grpc::StatusCode)(errno), e.getMessage().str().c_str());
    }
  }

  // stream the requested contanier
  eos::rpc::MDRequest c_dir;
  c_dir.set_id(cid);
  c_dir.set_type(eos::rpc::CONTAINER);
  GetMD(writer, &c_dir);

  // stream all the children files
  for (auto it = eos::FileMapIterator(cmd); it.valid(); it.next()) {
    eos::rpc::MDRequest c_file;
    c_file.set_id(it.value());
    c_file.set_type(eos::rpc::FILE);
    GetMD(writer, &c_file);
  }

  // stream all the children container
  for (auto it = eos::ContainerMapIterator(cmd); it.valid(); it.next()) {
    eos::rpc::MDRequest c_dir;
    c_dir.set_id(it.value());
    c_dir.set_type(eos::rpc::CONTAINER);
    GetMD(writer, &c_dir);
  }

  // finished streaming
  return grpc::Status::OK;
}

#endif

EOSMGMNAMESPACE_END


