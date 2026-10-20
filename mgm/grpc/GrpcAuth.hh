// ----------------------------------------------------------------------
// File: GrpcAuth.hh
// ----------------------------------------------------------------------

#pragma once

#include "common/VirtualIdentity.hh"
#include "mgm/Namespace.hh"
#include <string>

#ifdef EOS_GRPC
#include "proto/Rpc.pb.h"

EOSMGMNAMESPACE_BEGIN

struct GrpcAuthDecision {
  bool allowed = true;
  std::string action;
};

// The PoC grants only native namespace gRPC quota operations.
class GrpcAuth {
public:
  static GrpcAuthDecision Authorize(const eos::common::VirtualIdentity& vid,
                                    const std::string& authkey,
                                    const std::string& action = "");
  static std::string ExecScope(const eos::rpc::NSRequest& request);
  static void LogDenied(const eos::common::VirtualIdentity& vid,
                        const GrpcAuthDecision& decision, const char* surface);
};

EOSMGMNAMESPACE_END
#endif
