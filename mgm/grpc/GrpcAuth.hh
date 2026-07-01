// ----------------------------------------------------------------------
// File: GrpcAuth.hh
// ----------------------------------------------------------------------

#pragma once

#include "common/Mapping.hh"
#include "mgm/Namespace.hh"
#include <string>
#include <vector>

#ifdef EOS_GRPC
#include "proto/EosWnc.pb.h"
#include "proto/Rpc.pb.h"
#endif

EOSMGMNAMESPACE_BEGIN

#ifdef EOS_GRPC

struct GrpcAuthDecision {
  bool allowed = true;
  std::string action;
  std::string configured_scopes;
};

// Central gRPC action-scope helper.
//
// Scope names are deliberately surface-specific. A token scoped to
// grpc.exec.quota.get must not authorize the same semantic operation through
// WNC or the REST gateway; callers that need multiple surfaces must request
// each action explicitly.
class GrpcAuth {
public:
  static GrpcAuthDecision Authorize(const eos::common::VirtualIdentity& vid,
                                    const std::string& authkey,
                                    const std::string& action);

  static std::string ExecScope(const eos::rpc::NSRequest& request);
  static std::string RestQuotaScope(const eos::console::QuotaProto& quota);
  static std::string RestScope(const std::string& request_name);
  static std::string WncScope(const eos::console::RequestProto& request);

  static void LogDenied(const eos::common::VirtualIdentity& vid,
                        const GrpcAuthDecision& decision, const char* surface);

private:
  static bool ScopeListAllows(const std::vector<std::string>& scopes,
                              const std::string& action);
};

#endif

EOSMGMNAMESPACE_END
