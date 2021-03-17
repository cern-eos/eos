// ----------------------------------------------------------------------
// File: GrpccLIENT.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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
#include "fst/grpc/client/GrpcClient.hh"
#include "common/StringConversion.hh"
#include "common/Timing.hh"
#include "common/Path.hh"
#include "flatb/fst.grpc.fb.h"
/*----------------------------------------------------------------------------*/
#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>
#include <google/protobuf/util/json_util.h>
/*----------------------------------------------------------------------------*/
#include <sys/stat.h>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//#ifdef EOS_GRPC

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientAsyncReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

using eos::PingRequest;
using eos::PingReply;

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
  std::unique_ptr<eos::fst::GrpcClient> p(new eos::fst::GrpcClient(
									 grpc::CreateChannel(
											     endpoint,
											     ssl ? grpc::SslCredentials(opts)
											     : grpc::InsecureChannelCredentials())));
  p->set_ssl(ssl);
  p->set_token(token);
  return p;
}

std::string GrpcClient::Ping(const std::string& payload)
{
  flatbuffers::grpc::MessageBuilder mb;
  auto inrequest = CreatePingRequestDirect(mb, token().c_str(), payload.c_str());
  mb.Finish(inrequest);
  auto request = mb.ReleaseMessage<PingRequest>();
  flatbuffers::grpc::Message<PingReply> reply;

  ClientContext context;
  // The producer-consumer queue we use to communicate asynchronously with the
  // gRPC runtime.
  CompletionQueue cq;
  Status status;
  // stub_->AsyncPing() performs the RPC call, returning an instance we
  // store in "rpc". Because we are using the asynchronous API, we need to
  // hold on to the "rpc" instance in order to get updates on the ongoing RPC.
  auto rpc(stub_->AsyncPing(&context, request, &cq));

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
    const eos::PingReply *response = reply.GetRoot();
    return response->message()->str();
  } else {
    return "";
  }
}

ssize_t GrpcClient::Get(const std::string& name, off_t offset, size_t len)
{
  flatbuffers::grpc::MessageBuilder mb;
  auto inrequest = CreateGetRequestDirect(mb, token().c_str(), name.c_str(), offset, len);
  mb.Finish(inrequest);
  auto request = mb.ReleaseMessage<GetRequest>();
  flatbuffers::grpc::Message<GetReply> reply;

  ClientContext context;
  // The producer-consumer queue we use to communicate asynchronously with the
  // gRPC runtime.
  CompletionQueue cq;
  Status status;
  // stub_->AsyncPing() performs the RPC call, returning an instance we
  // store in "rpc". Because we are using the asynchronous API, we need to
  // hold on to the "rpc" instance in order to get updates on the ongoing RPC.
  auto rpc(stub_->AsyncGet(&context, request, &cq));

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
    const eos::GetReply *response = reply.GetRoot();
    return response->buffer()->size();
  } else {
    return -1;
  }
}

//#endif


EOSFSTNAMESPACE_END
