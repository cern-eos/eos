// ----------------------------------------------------------------------
// File: GrpcAuth.cc
// ----------------------------------------------------------------------

#include "GrpcAuth.hh"
#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
#include "common/token/EosTok.hh"
#include "mgm/macros/Macros.hh"
#include <algorithm>
#include <cctype>
#include <sys/stat.h>

EOSMGMNAMESPACE_BEGIN

#ifdef EOS_GRPC

namespace {

std::string
JoinScopes(const std::vector<std::string>& scopes)
{
  std::string result;

  for (const auto& scope : scopes) {
    if (!result.empty()) {
      result += ",";
    }

    result += scope;
  }

  return result;
}

std::string
TokenString(std::string authkey)
{
  static const std::string http_enc_tag = "Bearer%20";
  static const std::string http_tag = "Bearer ";

  if (authkey.find(http_enc_tag) == 0) {
    authkey.erase(0, http_enc_tag.size());
    authkey = eos::common::StringConversion::curl_default_unescaped(authkey);
  } else if (authkey.find(http_tag) == 0) {
    authkey.erase(0, http_tag.size());
  }

  return authkey;
}

bool
UnknownAction(const std::string& action)
{
  static const std::string suffix = ".unknown";
  return action.empty() ||
         ((action.size() >= suffix.size()) &&
          (action.compare(action.size() - suffix.size(), suffix.size(), suffix) == 0));
}

bool
TokenKey(std::string& key)
{
  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
  key = symkey ? symkey->GetKey64() : "0123456789defaultkey";

  if (getenv("EOS_MGM_TOKEN_KEYFILE")) {
    struct stat buf;

    if (::stat(getenv("EOS_MGM_TOKEN_KEYFILE"), &buf)) {
      eos_static_err("msg=\"token keyfile does not exist\" location=\"%s\"",
                     getenv("EOS_MGM_TOKEN_KEYFILE"));
      return false;
    }

    if ((buf.st_uid != DAEMONUID) || (buf.st_mode != 0100400)) {
      eos_static_err("msg=\"token keyfile mode bit\" mode=%o", buf.st_mode);
      return false;
    }

    key = eos::common::StringConversion::LoadFileIntoString(
        getenv("EOS_MGM_TOKEN_KEYFILE"), key);
  }

  return true;
}

std::string
Lower(std::string input)
{
  std::transform(input.begin(), input.end(), input.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return input;
}

std::string
ConsoleQuotaScope(const eos::console::QuotaProto& quota, const std::string& prefix)
{
  switch (quota.subcmd_case()) {
  case eos::console::QuotaProto::kLs:
  case eos::console::QuotaProto::kLsuser:
    return prefix + ".get";
  case eos::console::QuotaProto::kSet:
    return prefix + ".set";
  case eos::console::QuotaProto::kRm:
    return prefix + ".rm";
  case eos::console::QuotaProto::kRmnode:
    return prefix + ".rmnode";
  default:
    return prefix + ".unknown";
  }
}

std::string
WncCommandName(const eos::console::RequestProto& request)
{
  switch (request.command_case()) {
  case eos::console::RequestProto::kAccess:
    return "Access";
  case eos::console::RequestProto::kAcl:
    return "Acl";
  case eos::console::RequestProto::kArchive:
    return "Archive";
  case eos::console::RequestProto::kAttr:
    return "Attr";
  case eos::console::RequestProto::kBackup:
    return "Backup";
  case eos::console::RequestProto::kChmod:
    return "Chmod";
  case eos::console::RequestProto::kChown:
    return "Chown";
  case eos::console::RequestProto::kConfig:
    return "Config";
  case eos::console::RequestProto::kConvert:
    return "Convert";
  case eos::console::RequestProto::kCp:
    return "Cp";
  case eos::console::RequestProto::kDebug:
    return "Debug";
  case eos::console::RequestProto::kEvict:
    return "Evict";
  case eos::console::RequestProto::kFile:
    return "File";
  case eos::console::RequestProto::kFileinfo:
    return "Fileinfo";
  case eos::console::RequestProto::kFind:
    return "Find";
  case eos::console::RequestProto::kFs:
    return "Fs";
  case eos::console::RequestProto::kFsck:
    return "Fsck";
  case eos::console::RequestProto::kGeosched:
    return "Geosched";
  case eos::console::RequestProto::kGroup:
    return "Group";
  case eos::console::RequestProto::kHealth:
    return "Health";
  case eos::console::RequestProto::kIo:
    return "Io";
  case eos::console::RequestProto::kLs:
    return "Ls";
  case eos::console::RequestProto::kMap:
    return "Map";
  case eos::console::RequestProto::kMember:
    return "Member";
  case eos::console::RequestProto::kMkdir:
    return "Mkdir";
  case eos::console::RequestProto::kMv:
    return "Mv";
  case eos::console::RequestProto::kNode:
    return "Node";
  case eos::console::RequestProto::kNs:
    return "Ns";
  case eos::console::RequestProto::kQuota:
    return "Quota";
  case eos::console::RequestProto::kRecycle:
    return "Recycle";
  case eos::console::RequestProto::kRm:
    return "Rm";
  case eos::console::RequestProto::kRmdir:
    return "Rmdir";
  case eos::console::RequestProto::kRoute:
    return "Route";
  case eos::console::RequestProto::kSpace:
    return "Space";
  case eos::console::RequestProto::kStat:
    return "Stat";
  case eos::console::RequestProto::kStatus:
    return "Status";
  case eos::console::RequestProto::kToken:
    return "Token";
  case eos::console::RequestProto::kTouch:
    return "Touch";
  case eos::console::RequestProto::kVersion:
    return "Version";
  case eos::console::RequestProto::kVid:
    return "Vid";
  case eos::console::RequestProto::kWho:
    return "Who";
  case eos::console::RequestProto::kWhoami:
    return "Whoami";
  default:
    return "unknown";
  }
}

} // namespace

GrpcAuthDecision
GrpcAuth::Authorize(const eos::common::VirtualIdentity& vid, const std::string& authkey,
                    const std::string& action)
{
  GrpcAuthDecision decision;
  decision.action = action;

  if (vid.token && vid.token->Valid()) {
    const auto scopes = vid.token->Scopes();
    decision.configured_scopes = JoinScopes(scopes);

    if (!scopes.empty()) {
      decision.allowed = ScopeListAllows(scopes, action);
    }

    return decision;
  }

  const std::string token_string = TokenString(authkey);

  if (token_string.substr(0, 8) != "zteos64:") {
    return decision;
  }

  std::string key;

  if (!TokenKey(key)) {
    decision.allowed = false;
    return decision;
  }

  eos::common::EosTok token;
  const int rc =
      token.Read(token_string, key, eos::common::EosTok::sTokenGeneration.load(), false);

  if (rc) {
    eos_static_err("msg=\"failed to decode grpc token\" errno=%d", -rc);
    decision.allowed = false;
    return decision;
  }

  const auto scopes = token.Scopes();
  decision.configured_scopes = JoinScopes(scopes);

  if (!scopes.empty()) {
    decision.allowed = ScopeListAllows(scopes, action);
  }

  return decision;
}

std::string
GrpcAuth::ExecScope(const eos::rpc::NSRequest& request)
{
  switch (request.command_case()) {
  case eos::rpc::NSRequest::kMkdir:
    return "grpc.exec.mkdir";
  case eos::rpc::NSRequest::kRmdir:
    return "grpc.exec.rmdir";
  case eos::rpc::NSRequest::kTouch:
    return "grpc.exec.touch";
  case eos::rpc::NSRequest::kUnlink:
    return "grpc.exec.unlink";
  case eos::rpc::NSRequest::kRm:
    return "grpc.exec.rm";
  case eos::rpc::NSRequest::kRename:
    return "grpc.exec.rename";
  case eos::rpc::NSRequest::kSymlink:
    return "grpc.exec.symlink";
  case eos::rpc::NSRequest::kXattr:
    return "grpc.exec.xattr";
  case eos::rpc::NSRequest::kVersion:
    return "grpc.exec.version";
  case eos::rpc::NSRequest::kOldRecycle:
    return "grpc.exec.old_recycle";
  case eos::rpc::NSRequest::kRecycle:
    return "grpc.exec.recycle";
  case eos::rpc::NSRequest::kChown:
    return "grpc.exec.chown";
  case eos::rpc::NSRequest::kChmod:
    return "grpc.exec.chmod";
  case eos::rpc::NSRequest::kAcl:
    return "grpc.exec.acl";
  case eos::rpc::NSRequest::kToken:
    return "grpc.exec.token";
  case eos::rpc::NSRequest::kQuota:
    switch (request.quota().op()) {
    case eos::rpc::QUOTAOP::GET:
      return "grpc.exec.quota.get";
    case eos::rpc::QUOTAOP::SET:
      return "grpc.exec.quota.set";
    case eos::rpc::QUOTAOP::RM:
      return "grpc.exec.quota.rm";
    case eos::rpc::QUOTAOP::RMNODE:
      return "grpc.exec.quota.rmnode";
    default:
      return "grpc.exec.quota.unknown";
    }
  default:
    return "grpc.exec.unknown";
  }
}

std::string
GrpcAuth::RestScope(const eos::console::QuotaProto& quota)
{
  return ConsoleQuotaScope(quota, "grpc.rest.quota");
}

std::string
GrpcAuth::RestScope(const std::string& request_name)
{
  static const std::string suffix = "Request";

  if (request_name.size() <= suffix.size() ||
      request_name.compare(request_name.size() - suffix.size(), suffix.size(), suffix) !=
          0) {
    return "grpc.rest.unknown";
  }

  return "grpc.rest." +
         Lower(request_name.substr(0, request_name.size() - suffix.size()));
}

std::string
GrpcAuth::WncScope(const eos::console::RequestProto& request)
{
  if (request.command_case() == eos::console::RequestProto::kQuota) {
    return ConsoleQuotaScope(request.quota(), "grpc.wnc.quota");
  }

  return "grpc.wnc." + Lower(WncCommandName(request));
}

void
GrpcAuth::LogDenied(const eos::common::VirtualIdentity& vid,
                    const GrpcAuthDecision& decision, const char* surface)
{
  eos_static_warning("msg=\"grpc scope denied\" surface=\"%s\" uid=%u gid=%u "
                     "scope=\"%s\" token_scopes=\"%s\"",
                     surface, vid.uid, vid.gid, decision.action.c_str(),
                     decision.configured_scopes.c_str());
}

bool
GrpcAuth::ScopeListAllows(const std::vector<std::string>& scopes,
                          const std::string& action)
{
  if (UnknownAction(action)) {
    return false;
  }

  for (std::string configured_scope : scopes) {
    if ((configured_scope == "*") || (configured_scope == action)) {
      return true;
    }

    static const std::string wildcard_suffix = ".*";

    if (configured_scope.size() > wildcard_suffix.size() &&
        configured_scope.compare(configured_scope.size() - wildcard_suffix.size(),
                                 wildcard_suffix.size(), wildcard_suffix) == 0) {
      configured_scope.pop_back();

      if (action.compare(0, configured_scope.size(), configured_scope) == 0) {
        return true;
      }
    }
  }

  return false;
}

#endif

EOSMGMNAMESPACE_END
