// ----------------------------------------------------------------------
// File: GrpccLIENT.cc
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
#include "GrpcClient.hh"
#include "proto/Rpc.grpc.pb.h"
#include "common/StringConversion.hh"
#include "common/Timing.hh"
#include "common/Path.hh"
/*----------------------------------------------------------------------------*/
#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>
#include <google/protobuf/util/json_util.h>
/*----------------------------------------------------------------------------*/
#include <sys/stat.h>
/*----------------------------------------------------------------------------*/

EOSCLIENTNAMESPACE_BEGIN

//#ifdef EOS_GRPC

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientAsyncReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

using eos::rpc::Eos;
using eos::rpc::PingRequest;
using eos::rpc::PingReply;
using eos::rpc::MDRequest;
using eos::rpc::MDResponse;
using eos::rpc::FindRequest;
using eos::rpc::NSRequest;
using eos::rpc::NSResponse;
using eos::rpc::NsStatRequest;
using eos::rpc::NsStatResponse;
using eos::rpc::FileInsertRequest;
using eos::rpc::ContainerInsertRequest;
using eos::rpc::InsertReply;
using eos::rpc::ContainerMdProto;
using eos::rpc::FileMdProto;
using eos::rpc::ManilaRequest;
using eos::rpc::ManilaResponse;



std::string GrpcClient::Ping(const std::string& payload)
{
  PingRequest request;
  request.set_message(payload);
  request.set_authkey(token());
  PingReply reply;
  ClientContext context;
  // The producer-consumer queue we use to communicate asynchronously with the
  // gRPC runtime.
  CompletionQueue cq;
  Status status;
  // stub_->AsyncPing() performs the RPC call, returning an instance we
  // store in "rpc". Because we are using the asynchronous API, we need to
  // hold on to the "rpc" instance in order to get updates on the ongoing RPC.
  std::unique_ptr<ClientAsyncResponseReader<PingReply> > rpc(
    stub_->AsyncPing(&context, request, &cq));
  // Request that, upon completion of the RPC, "reply" be updated with the
  // server's response; "status" with the indication of whether the operation
  // was successful. Tag the request with the integer 1.
  rpc->Finish(&reply, &status, (void*) 1);
  void* got_tag;
  bool ok = false;
  // Block until the next result is available in the completion queue "cq".
  // The return value of Next should always be checked. This return value
  // tells us whether there is any kind of event or the cq_ is shutting down.
  GPR_ASSERT(cq.Next(&got_tag, &ok));
  // Verify that the result from "cq" corresponds, by its tag, our previous
  // request.
  GPR_ASSERT(got_tag == (void*) 1);
  // ... and that the request was completed successfully. Note that "ok"
  // corresponds solely to the request for updates introduced by Finish().
  GPR_ASSERT(ok);

  // Act upon the status of the actual RPC.
  if (status.ok()) {
    return reply.message();
  } else {
    return "";
  }
}

int 
GrpcClient::ManilaRequest(const eos::rpc::ManilaRequest& request, 
			  eos::rpc::ManilaResponse& reply)
{
  ClientContext context;
  CompletionQueue cq;
  Status status;
  std::unique_ptr<ClientAsyncResponseReader<ManilaResponse> > rpc(
    stub_->AsyncManilaServerRequest(&context, request, &cq));
  rpc->Finish(&reply, &status, (void*) 1);
  void* got_tag;
  bool ok = false;
  GPR_ASSERT(cq.Next(&got_tag, &ok));
  GPR_ASSERT(got_tag == (void*) 1);
  GPR_ASSERT(ok);

  // Act upon the status of the actual RPC.
  if (status.ok()) {
    return reply.code();
  } else {
    return -1;
  }
}

std::string
GrpcClient::Md(const std::string& path,
               uint64_t id,
               uint64_t ino,
               bool list, 
	       bool printonly)
{
  MDRequest request;

  if (list) {
    request.set_type(eos::rpc::LISTING);
  } else {
    request.set_type(eos::rpc::STAT);
  }

  if (path.length()) {
    request.mutable_id()->set_path(path);
  } else if (id) {
    request.mutable_id()->set_id(id);
  } else if (ino) {
    request.mutable_id()->set_ino(ino);
  } else {
    return "";
  }

  request.set_authkey(token());
  MDResponse response;
  ClientContext context;
  std::string responsestring;
  CompletionQueue cq;
  Status status;
  std::unique_ptr<ClientAsyncReader<MDResponse> > rpc(
    stub_->AsyncMD(&context, request, &cq, (void*) 1));
  void* got_tag;
  bool ok = false;
  
  bool ret = cq.Next(&got_tag, &ok);

  while (1) {
    rpc->Read(&response, (void*) 1);
    ok = false;
    ret = cq.Next(&got_tag, &ok);

    if (!ret || !ok || got_tag != (void*) 1) {
      break;
    }

    google::protobuf::util::JsonPrintOptions options;
    options.add_whitespace = true;
    options.always_print_primitive_fields = true;
    std::string jsonstring;
    google::protobuf::util::MessageToJsonString(response,
        &jsonstring, options);
    if (printonly) {
      std::cout << jsonstring << std::endl;
    } else {
      responsestring += jsonstring;
    }
  }

  if (!status.ok()) {
    std::cerr << "error: " << status.error_message() << std::endl;
  }

  return responsestring;
}

std::string
GrpcClient::Find(const std::string& path,
		 const std::string& filter, 
		 uint64_t id, 
		 uint64_t ino,
		 bool files, 
		 bool dirs, 
		 uint64_t depth, 
		 bool printonly, 
		 const std::string& exportfs)
{
  FindRequest request;
  if (files && !dirs) {
    // query files
    request.set_type(eos::rpc::FILE);
  } else if (dirs && !files) {
    // query container
    request.set_type(eos::rpc::CONTAINER);
  } else {
    // query files & container
    request.set_type(eos::rpc::LISTING);
  }

  if (path.length()) {
    request.mutable_id()->set_path(path);
  } else if (id) {
    request.mutable_id()->set_id(id);
  } else if (ino) {
    request.mutable_id()->set_ino(ino);
  } else {
    return "";
  }

  if (depth) {
    request.set_maxdepth(depth);
  }

  request.set_authkey(token());


  if (filter.length()) {
    // enable filtering 
    request.mutable_selection()->set_select(true);

    std::map<std::string,std::string> filtermap;
    eos::common::StringConversion::GetKeyValueMap(filter.c_str(), 
		   filtermap);

    for ( auto const& x : filtermap ) {
      if (x.first == "owner-root") {
	  request.mutable_selection()->set_owner_root(strtoul(x.second.c_str(),0,10)?true:false);
      }  else if (x.first == "group-root") {
	request.mutable_selection()->set_group_root(strtoul(x.second.c_str(),0,10)?true:false);
      } else if (x.first == "owner") {
	request.mutable_selection()->set_owner(strtoul(x.second.c_str(),0,10));
      } else if (x.first == "group") {
	request.mutable_selection()->set_group(strtoul(x.second.c_str(),0,10));
      } else if (x.first == "regex-filename") {
	request.mutable_selection()->set_regexp_filename(x.second);
      } else if (x.first == "regex-dirname") {
	request.mutable_selection()->set_regexp_dirname(x.second);
      } else if (x.first == "zero-size") {
	request.mutable_selection()->mutable_size()->set_zero(strtoul(x.second.c_str(),0,10)?true:false);
      } else if (x.first == "min-size") {
	request.mutable_selection()->mutable_size()->set_min(strtoul(x.second.c_str(),0,10));
      } else if (x.first == "max-size") {
	request.mutable_selection()->mutable_size()->set_max(strtoul(x.second.c_str(),0,10));
      } else if (x.first == "min-children") {
	request.mutable_selection()->mutable_children()->set_min(strtoul(x.second.c_str(),0,10));
      } else if (x.first == "max-children") {
	request.mutable_selection()->mutable_children()->set_max(strtoul(x.second.c_str(),0,10));
      } else if (x.first == "zero-children") {
	request.mutable_selection()->mutable_children()->set_zero(strtoul(x.second.c_str(),0,10)?true:false);} else if (x.first == "min-locations") {
	request.mutable_selection()->mutable_locations()->set_min(strtoul(x.second.c_str(),0,10));
      } else if (x.first == "max-locations") {
	request.mutable_selection()->mutable_locations()->set_max(strtoul(x.second.c_str(),0,10));
      } else if (x.first == "zero-locations") {
	request.mutable_selection()->mutable_locations()->set_zero(strtoul(x.second.c_str(),0,10)?true:false);
      } else if (x.first == "min-unlinked_locations") {
	request.mutable_selection()->mutable_unlinked_locations()->set_min(strtoul(x.second.c_str(),0,10));
      } else if (x.first == "max-unlinked_locations") {
	request.mutable_selection()->mutable_unlinked_locations()->set_max(strtoul(x.second.c_str(),0,10));
      } else if (x.first == "zero-unlinked_locations") {
	request.mutable_selection()->mutable_unlinked_locations()->set_zero(strtoul(x.second.c_str(),0,10)?true:false);
      } else if (x.first == "min-treesize") {
	request.mutable_selection()->mutable_treesize()->set_min(strtoul(x.second.c_str(),0,10));
      } else if (x.first == "max-treesize") {
	request.mutable_selection()->mutable_treesize()->set_max(strtoul(x.second.c_str(),0,10));
      } else if (x.first == "zero-treesize") {
	request.mutable_selection()->mutable_treesize()->set_zero(strtoul(x.second.c_str(),0,10)?true:false);
      } else if (x.first == "min-ctime") {
	request.mutable_selection()->mutable_ctime()->set_min(strtoul(x.second.c_str(),0,10));
      } else if (x.first == "max-ctime") {
	request.mutable_selection()->mutable_ctime()->set_max(strtoul(x.second.c_str(),0,10));
      } else if (x.first == "zero-ctime") {
	request.mutable_selection()->mutable_ctime()->set_zero(strtoul(x.second.c_str(),0,10)?true:false);
      } else if (x.first == "min-mtime") {
	request.mutable_selection()->mutable_mtime()->set_min(strtoul(x.second.c_str(),0,10));
      } else if (x.first == "max-mtime") {
	request.mutable_selection()->mutable_mtime()->set_max(strtoul(x.second.c_str(),0,10));
      } else if (x.first == "zero-mtime") {
	request.mutable_selection()->mutable_mtime()->set_zero(strtoul(x.second.c_str(),0,10)?true:false);
      } else if (x.first == "min-stime") {
	request.mutable_selection()->mutable_stime()->set_min(strtoul(x.second.c_str(),0,10));
      } else if (x.first == "max-stime") {
	request.mutable_selection()->mutable_stime()->set_max(strtoul(x.second.c_str(),0,10));
      } else if (x.first == "zero-stime") {
	request.mutable_selection()->mutable_stime()->set_zero(strtoul(x.second.c_str(),0,10)?true:false);
      } else if (x.first == "layoutid") {
	request.mutable_selection()->set_layoutid(strtoull(x.second.c_str(),0,10));
      } else if (x.first == "flags") {
	request.mutable_selection()->set_flags(strtoull(x.second.c_str(),0,10));
      } else if (x.first == "symlink") {
	request.mutable_selection()->set_symlink(strtoul(x.second.c_str(),0,10)?true:false);
      } else if (x.first == "checksum-type") {
	request.mutable_selection()->mutable_checksum()->set_type(x.second);
      } else if (x.first == "checksum-value") {
	request.mutable_selection()->mutable_checksum()->set_value(x.second);
      } else if (x.first == "xattr") {
	std::string key;
	std::string val;
	eos::common::StringConversion::SplitKeyValue(x.second, key, val, "=");
	(*(request.mutable_selection()->mutable_xattr()))[key] = val;
      } else {
	std::cerr << "error: unknown filter '" << x.first << ":" << x.second << "'" << std::endl;
	return "";
      }
    }
  }

  MDResponse response;
  ClientContext context;
  std::string responsestring;
  CompletionQueue cq;
  Status status;
  std::unique_ptr<ClientAsyncReader<MDResponse> > rpc(
    stub_->AsyncFind(&context, request, &cq, (void*) 1));
  void* got_tag;
  bool ok = false;
  bool ret = cq.Next(&got_tag, &ok);

  while (1) {
    rpc->Read(&response, (void*) 1);
    ok = false;
    ret = cq.Next(&got_tag, &ok);

    if (!ret || !ok || got_tag != (void*) 1) {
      break;
    }

    if (!exportfs.empty()) {
      responsestring = ExportFs(response,exportfs);
    } else {
      google::protobuf::util::JsonPrintOptions options;
      options.add_whitespace = true;
      options.always_print_primitive_fields = true;
      std::string jsonstring;
      google::protobuf::util::MessageToJsonString(response,
						  &jsonstring, options);

      if (printonly) {
	std::cout << jsonstring << std::endl;
      } else {
	responsestring += jsonstring;
      }
    }
  }

  if (!status.ok()) {
    std::cerr << "error: " << status.error_message() << std::endl;
  }

  return responsestring;
}

int 
GrpcClient::FileInsert(const std::vector<std::string>& paths)
{
  FileInsertRequest request;
  size_t cnt=0;
  for (auto it : paths ) {
    std::string path = it;
    struct timespec tsnow;
    eos::common::Timing::GetTimeSpec(tsnow);
    uint64_t inode = 0;

    cnt++;
    FileMdProto* file = request.add_files();

    if (it.substr(0,4) == "ino:") {
      // the format is ino:xxxxxxxxxxxxxxxx:<path> where xxxxxxxxxxxxxxxx is a 64bit hex string of the inode
      path = it.substr(21);
      inode = std::strtol(it.substr(4,20).c_str() ,0, 16);
    }

    if (inode) {
      file->set_id(inode);
    }

    file->set_path(path);
    file->set_uid(2);
    file->set_gid(2);
    file->set_size(cnt);
    file->set_layout_id(0x00100002);
    file->mutable_checksum()->set_value("\0\0\0\1",4);
    file->set_flags(0);
    file->mutable_ctime()->set_sec(tsnow.tv_sec);
    file->mutable_ctime()->set_n_sec(tsnow.tv_nsec);
    file->mutable_mtime()->set_sec(tsnow.tv_sec);
    file->mutable_mtime()->set_n_sec(tsnow.tv_nsec);
    file->mutable_locations()->Add(65535);

    auto map = file->mutable_xattrs();
    (*map)["sys.acl"] = "u:100:rwx";
    (*map)["sys.cta.id"] = "fake";
  }

  request.set_authkey(token());
  InsertReply reply;
  ClientContext context;
  // The producer-consumer queue we use to communicate asynchronously with the
  // gRPC runtime.
  CompletionQueue cq;
  Status status;
  std::unique_ptr<ClientAsyncResponseReader<InsertReply> > rpc(
    stub_->AsyncFileInsert(&context, request, &cq));
  // Request that, upon completion of the RPC, "reply" be updated with the
  // server's response; "status" with the indication of whether the operation
  // was successful. Tag the request with the integer 1.
  rpc->Finish(&reply, &status, (void*) 1);
  void* got_tag;
  bool ok = false;
  // Block until the next result is available in the completion queue "cq".
  // The return value of Next should always be checked. This return value
  // tells us whether there is any kind of event or the cq_ is shutting down.
  GPR_ASSERT(cq.Next(&got_tag, &ok));
  // Verify that the result from "cq" corresponds, by its tag, our previous
  // request.
  GPR_ASSERT(got_tag == (void*) 1);
  // ... and that the request was completed successfully. Note that "ok"
  // corresponds solely to the request for updates introduced by Finish().
  GPR_ASSERT(ok);

  // Act upon the status of the actual RPC.
  int retc = 0;
  if (status.ok()) {
    for (auto it : reply.retc()) {
      retc |= it;
    }
    return retc;
  } else {
    return -1;
  }
}

int 
GrpcClient::ContainerInsert(const std::vector<std::string>& paths)
{
  ContainerInsertRequest request;
  for (auto it : paths ) {
    std::string path;
    struct timespec tsnow;
    eos::common::Timing::GetTimeSpec(tsnow);

    uint64_t inode = 0 ;

    if (it.substr(0,4) == "ino:") {
      // the format is ino:xxxxxxxxxxxxxxxx:<path> where xxxxxxxxxxxxxxxx is a 64bit hex string of the inode
      path = it.substr(21);
      inode = std::strtol(it.substr(4,20).c_str() ,0, 16);
    }

    ContainerMdProto* container = request.add_container();

    if (inode) {
      container->set_id(inode);
    }

    container->set_path(path);
    container->set_uid(2);
    container->set_gid(2);
    container->set_mode(S_IFDIR | S_IRWXU);
    container->mutable_ctime()->set_sec(tsnow.tv_sec);
    container->mutable_ctime()->set_n_sec(tsnow.tv_nsec);
    container->mutable_mtime()->set_sec(tsnow.tv_sec);
    container->mutable_mtime()->set_n_sec(tsnow.tv_nsec);
    auto map = container->mutable_xattrs();
    (*map)["sys.acl"] = "u:100:rwx";
    (*map)["sys.forced.checksum"] = "adler";
    (*map)["sys.forced.space"] = "default";
    (*map)["sys.forced.nstripes"] = "1";
    (*map)["sys.forced.layout"] = "replica";
  }

  request.set_authkey(token());
  InsertReply reply;
  ClientContext context;
  // The producer-consumer queue we use to communicate asynchronously with the
  // gRPC runtime.
  CompletionQueue cq;
  Status status;
  std::unique_ptr<ClientAsyncResponseReader<InsertReply> > rpc(
    stub_->AsyncContainerInsert(&context, request, &cq));
  // Request that, upon completion of the RPC, "reply" be updated with the
  // server's response; "status" with the indication of whether the operation
  // was successful. Tag the request with the integer 1.
  rpc->Finish(&reply, &status, (void*) 1);
  void* got_tag;
  bool ok = false;
  // Block until the next result is available in the completion queue "cq".
  // The return value of Next should always be checked. This return value
  // tells us whether there is any kind of event or the cq_ is shutting down.
  GPR_ASSERT(cq.Next(&got_tag, &ok));
  // Verify that the result from "cq" corresponds, by its tag, our previous
  // request.
  GPR_ASSERT(got_tag == (void*) 1);
  // ... and that the request was completed successfully. Note that "ok"
  // corresponds solely to the request for updates introduced by Finish().
  GPR_ASSERT(ok);

  // Act upon the status of the actual RPC.
  int retc = 0;
  if (status.ok()) {
    for (auto it : reply.retc()) {
      retc |= it;
    }
    return retc;
  } else {
    return -1;
  }
}


std::unique_ptr<GrpcClient>
GrpcClient::Create(std::string endpoint,
                   std::string token,
                   std::string keyfile,
                   std::string certfile,
                   std::string cafile
                  )
{
  std::string key;
  std::string cert;
  std::string ca;
  bool ssl = false;

  if (keyfile.length() || certfile.length() || cafile.length()) {
    if (!keyfile.length() || !certfile.length() || !cafile.length()) {
      return 0;
    }

    ssl = true;

    if (eos::common::StringConversion::LoadFileIntoString(certfile.c_str(),
        cert) && !cert.length()) {
      fprintf(stderr, "error: unable to load ssl certificate file '%s'\n",
              certfile.c_str());
      return 0;
    }

    if (eos::common::StringConversion::LoadFileIntoString(keyfile.c_str(),
        key) && !key.length()) {
      fprintf(stderr, "unable to load ssl key file '%s'\n", keyfile.c_str());
      return 0;
    }

    if (eos::common::StringConversion::LoadFileIntoString(cafile.c_str(),
        ca) && !ca.length()) {
      fprintf(stderr, "unable to load ssl ca file '%s'\n", cafile.c_str());
      return 0;
    }
  }

  grpc::SslCredentialsOptions opts = {
    ca,
    key,
    cert
  };
  std::unique_ptr<eos::client::GrpcClient> p(new eos::client::GrpcClient(
        grpc::CreateChannel(
          endpoint,
          ssl ? grpc::SslCredentials(opts)
          : grpc::InsecureChannelCredentials())));
  p->set_ssl(ssl);
  p->set_token(token);
  return p;
}

int
GrpcClient::NsStat(const eos::rpc::NsStatRequest& request,
                   eos::rpc::NsStatResponse& reply)
{
  ClientContext context;
  CompletionQueue cq;
  Status status;
  std::unique_ptr<ClientAsyncResponseReader<NsStatResponse>> rpc(
    stub_->AsyncNsStat(&context, request, &cq));
  rpc->Finish(&reply, &status, (void*) 1);

  void* got_tag;
  bool ok = false;
  GPR_ASSERT(cq.Next(&got_tag, &ok));
  GPR_ASSERT(got_tag == (void*) 1);
  GPR_ASSERT(ok);

  // Act upon the status of the actual RPC
  if (status.ok()) {
    return reply.code();
  } else {
    return -1;
  }
}

int 
GrpcClient::Exec(const eos::rpc::NSRequest& request, 
		  eos::rpc::NSResponse& reply)
{
  ClientContext context;
  CompletionQueue cq;
  Status status;
  std::unique_ptr<ClientAsyncResponseReader<NSResponse> > rpc(
    stub_->AsyncExec(&context, request, &cq));
  rpc->Finish(&reply, &status, (void*) 1);
  void* got_tag;
  bool ok = false;
  GPR_ASSERT(cq.Next(&got_tag, &ok));
  GPR_ASSERT(got_tag == (void*) 1);
  GPR_ASSERT(ok);
  
  // Act upon the status of the actual RPC.
  if (status.ok()) {
    return reply.error().code();
  } else {
    return -1;
  }
}

std::string
GrpcClient::ExportFs(const eos::rpc::MDResponse& response, const std::string& exportfs) 
{
  bool first=false;
  if (response.type() == eos::rpc::CONTAINER) {
    if (!tree.size()) {
      first = true;
      tree[response.cmd().id()] = response.cmd().name() + "/";
    } else {
      first = false;
      tree[response.cmd().id()] = tree[response.cmd().parent_id()] + response.cmd().name() + "/";
    }
    fprintf(stderr,"%s\n",tree[response.cmd().id()].c_str());

    if (!first) {
      std::string target = exportfs + "/" + tree[response.cmd().id()];
      eos::common::Path cPath(target.c_str());
      
      if (!cPath.MakeParentPath(755)) {
	fprintf(stderr,"error: failed to created '%s'\n", cPath.GetParentPath());
	exit(errno);
      }
      int rc = mkdir(cPath.GetPath(),755);
      if (rc) {
	fprintf(stderr,"error: failed to created '%s'\n", cPath.GetPath());
	exit(errno);
      }
    }
  }

  if (response.type() == eos::rpc::FILE) {
    fprintf(stderr,"%s\n",(tree[response.fmd().cont_id()] + response.fmd().name()).c_str());
  }
  return "";
}

//#endif


EOSCLIENTNAMESPACE_END

