// ----------------------------------------------------------------------
// File: GrpcAuth.cc
// ----------------------------------------------------------------------

#include "GrpcAuth.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "common/token/GrpcQuotaScope.hh"

#ifdef EOS_GRPC
EOSMGMNAMESPACE_BEGIN

GrpcAuthDecision
GrpcAuth::Authorize(const eos::common::VirtualIdentity& vid, const std::string& authkey,
                    const std::string& action)
{
  GrpcAuthDecision decision;
  decision.action = action;

  if (vid.token) {
    decision.allowed = vid.token->Valid();

    if (decision.allowed) {
      const auto scopes = vid.token->Scopes();

      if (!scopes.empty()) {
        decision.allowed =
            eos::common::IsGrpcQuotaScope(action) &&
            eos::common::GrpcQuotaScopeAllows(scopes, action == "grpc.quota.write");
      }
    }

    return decision;
  }

  std::string token = authkey;

  if (token.compare(0, 9, "Bearer%20") == 0) {
    token = eos::common::StringConversion::curl_default_unescaped(token.substr(9));
  } else if (token.compare(0, 7, "Bearer ") == 0) {
    token.erase(0, 7);
  }

  // Mapping owns signature, generation, expiry and origin validation.
  // Re-reading a rejected token here would bypass that validation.
  decision.allowed = token.compare(0, 8, "zteos64:") != 0 &&
                     token.compare(0, 10, "zteos64%3A") != 0 &&
                     token.compare(0, 10, "zteos64%3a") != 0;
  return decision;
}

std::string
GrpcAuth::ExecScope(const eos::rpc::NSRequest& request)
{
  if (request.has_role() || request.command_case() != eos::rpc::NSRequest::kQuota) {
    return "";
  }

  switch (request.quota().op()) {
  case eos::rpc::QUOTAOP::GET:
    return "grpc.quota.read";
  case eos::rpc::QUOTAOP::SET:
  case eos::rpc::QUOTAOP::RM:
  case eos::rpc::QUOTAOP::RMNODE:
    return "grpc.quota.write";
  default:
    return "";
  }
}

void
GrpcAuth::LogDenied(const eos::common::VirtualIdentity& vid,
                    const GrpcAuthDecision& decision, const char* surface)
{
  eos_static_warning(
      "msg=\"grpc scope denied\" surface=\"%s\" uid=%u gid=%u action=\"%s\"", surface,
      vid.uid, vid.gid, decision.action.c_str());
}

EOSMGMNAMESPACE_END
#endif
