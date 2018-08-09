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
/*----------------------------------------------------------------------------*/
#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>
#include <google/protobuf/util/json_util.h>
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

std::string
GrpcClient::Md(const std::string& path,
               uint64_t id,
               uint64_t ino,
               bool list)
{
  MDRequest request;

  if (list) {
    request.set_type(eos::rpc::LISTING);
  } else {
    request.set_type(eos::rpc::CONTAINER);
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
    responsestring += jsonstring;
  }

  if (!status.ok()) {
    std::cerr << "error: " << status.error_message() << std::endl;
  }

  return responsestring;
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
//#endif


EOSCLIENTNAMESPACE_END

