//-----------------------------------------------------------------------------
// File: GrpcWncServer.cc
// Author: Branko Blagojevic <branko.blagojevic@comtrade.com>
// Author: Ivan Arizanovic <ivan.arizanovic@comtrade.com>
//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
#include "GrpcWncServer.hh"
//-----------------------------------------------------------------------------
#include "GrpcServer.hh"
#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
#include "common/token/EosTok.hh"
#include "console/ConsoleMain.hh"
#include "mgm/macros/Macros.hh"
//-----------------------------------------------------------------------------
#include <algorithm>
#include <cctype>
#include <cerrno>
#include <sys/stat.h>
#ifdef EOS_GRPC
#include "proto/EosWnc.grpc.pb.h"
using eos::console::EosWnc;
using grpc::ServerContext;
#endif // EOS_GRPC
//-----------------------------------------------------------------------------

EOSMGMNAMESPACE_BEGIN

#ifdef EOS_GRPC

namespace {

std::string
WncCommandScope(std::string command)
{
  std::transform(command.begin(), command.end(), command.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return "grpc.wnc." + command;
}

std::string
WncQuotaScope(const eos::console::QuotaProto& quota)
{
  switch (quota.subcmd_case()) {
  case eos::console::QuotaProto::kLs:
  case eos::console::QuotaProto::kLsuser:
    return "grpc.exec.quota.get";

  case eos::console::QuotaProto::kSet:
    return "grpc.exec.quota.set";

  case eos::console::QuotaProto::kRm:
    return "grpc.exec.quota.rm";

  case eos::console::QuotaProto::kRmnode:
    return "grpc.exec.quota.rmnode";

  default:
    return "grpc.exec.quota.unknown";
  }
}

std::string
WncRequestScope(const eos::console::RequestProto& request, const std::string& command)
{
  if (request.command_case() == eos::console::RequestProto::kQuota) {
    return WncQuotaScope(request.quota());
  }

  return WncCommandScope(command);
}

std::string
WncTokenString(std::string authkey)
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
WncTokenKey(std::string& key)
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
WncScopeString(const std::vector<std::string>& scopes)
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

bool
WncScopeAllowed(const eos::common::VirtualIdentity& vid, const std::string& authkey,
                const std::string& requested_scope, std::string* configured_scopes)
{
  if (configured_scopes) {
    configured_scopes->clear();
  }

  if (vid.token && vid.token->Valid()) {
    const auto scopes = vid.token->Scopes();

    if (configured_scopes) {
      *configured_scopes = WncScopeString(scopes);
    }

    return GrpcServer::ScopeAllowed(vid, requested_scope);
  }

  const std::string token_string = WncTokenString(authkey);

  if (token_string.substr(0, 8) != "zteos64:") {
    return true;
  }

  std::string key;

  if (!WncTokenKey(key)) {
    return false;
  }

  eos::common::EosTok token;
  const int rc =
      token.Read(token_string, key, eos::common::EosTok::sTokenGeneration.load(), false);

  if (rc) {
    eos_static_err("msg=\"failed to decode wnc token\" errno=%d", -rc);
    return false;
  }

  const auto scopes = token.Scopes();

  if (configured_scopes) {
    *configured_scopes = WncScopeString(scopes);
  }

  if (scopes.empty()) {
    return true;
  }

  return GrpcServer::ScopeListAllows(scopes, requested_scope);
}

void
SetWncScopeDenied(eos::console::ReplyProto* reply, const std::string& scope,
                  const eos::common::VirtualIdentity& vid,
                  const std::string& configured_scopes)
{
  eos_static_warning("msg=\"grpc wnc scope denied\" uid=%u gid=%u "
                     "scope=\"%s\" token_scopes=\"%s\"",
                     vid.uid, vid.gid, scope.c_str(), configured_scopes.c_str());
  reply->set_retc(EACCES);
  reply->set_std_err("error: grpc scope denied");
}

} // namespace

class WncService final : public EosWnc::Service
{
  // Process gRPC request from the EOS Windows native client
  grpc::Status ProcessSingle(ServerContext* context,
                             const eos::console::RequestProto* request,
                             eos::console::ReplyProto* reply)
  {
    std::string command;

    switch (request->command_case()) {
    case eos::console::RequestProto::kAccess:
      command = "Access";
      break;

    case eos::console::RequestProto::kAcl:
      command = "Acl";
      break;

    case eos::console::RequestProto::kArchive:
      command = "Archive";
      break;

    case eos::console::RequestProto::kAttr:
      command = "Attr";
      break;

    case eos::console::RequestProto::kBackup:
      command = "Backup";
      break;

    case eos::console::RequestProto::kChmod:
      command = "Chmod";
      break;

    case eos::console::RequestProto::kChown:
      command = "Chown";
      break;

    case eos::console::RequestProto::kConfig:
      command = "Config";
      break;

    case eos::console::RequestProto::kConvert:
      command = "Convert";
      break;

    case eos::console::RequestProto::kCp:
      command = "Cp";
      break;

    case eos::console::RequestProto::kDebug:
      command = "Debug";
      break;

    case eos::console::RequestProto::kFile:
      command = "File";
      break;

    case eos::console::RequestProto::kFileinfo:
      command = "Fileinfo";
      break;

    case eos::console::RequestProto::kFs:
      command = "Fs";
      break;

    case eos::console::RequestProto::kFsck:
      command = "Fsck";
      break;

    case eos::console::RequestProto::kGeosched:
      command = "Geosched";
      break;

    case eos::console::RequestProto::kGroup:
      command = "Group";
      break;

    case eos::console::RequestProto::kHealth:
      command = "Health";
      break;

    case eos::console::RequestProto::kIo:
      command = "Io";
      break;

    case eos::console::RequestProto::kMap:
      command = "Map";
      break;

    case eos::console::RequestProto::kMember:
      command = "Member";
      break;

    case eos::console::RequestProto::kMkdir:
      command = "Mkdir";
      break;

    case eos::console::RequestProto::kMv:
      command = "Mv";
      break;

    case eos::console::RequestProto::kNode:
      command = "Node";
      break;

    case eos::console::RequestProto::kNs:
      command = "Ns";
      break;

    case eos::console::RequestProto::kQuota:
      command = "Quota";
      break;

    case eos::console::RequestProto::kRecycle:
      command = "Recycle";
      break;

    case eos::console::RequestProto::kRm:
      command = "Rm";
      break;

    case eos::console::RequestProto::kRmdir:
      command = "Rmdir";
      break;

    case eos::console::RequestProto::kRoute:
      command = "Route";
      break;

    case eos::console::RequestProto::kSpace:
      command = "Space";
      break;

    case eos::console::RequestProto::kStat:
      command = "Stat";
      break;

    case eos::console::RequestProto::kStatus:
      command = "Status";
      break;

    case eos::console::RequestProto::kToken:
      command = "Token";
      break;

    case eos::console::RequestProto::kTouch:
      command = "Touch";
      break;

    case eos::console::RequestProto::kVersion:
      command = "Version";
      break;

    case eos::console::RequestProto::kVid:
      command = "Vid";
      break;

    case eos::console::RequestProto::kWho:
      command = "Who";
      break;

    case eos::console::RequestProto::kWhoami:
      command = "Whoami";
      break;

    default:
      command = "ping";
      break;
    }

    eos_static_debug("eos-wnc request from peer=%s IP=%s DN=%s "
                     "command=\"%s\" token_len=%zu",
                     context->peer().c_str(), GrpcServer::IP(context).c_str(),
                     GrpcServer::DN(context).c_str(),
                     command.c_str(),
                     request->auth().authkey().length());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->auth().authkey());
    WAIT_BOOT;
    const std::string requested_scope = WncRequestScope(*request, command);
    std::string configured_scopes;

    if (!WncScopeAllowed(vid, request->auth().authkey(), requested_scope,
                         &configured_scopes)) {
      SetWncScopeDenied(reply, requested_scope, vid, configured_scopes);
      return grpc::Status::OK;
    }

    GrpcWncInterface wnc;
    return wnc.ExecCmd(vid, request, reply);
  }

  // Process gRPC request from the EOS Windows native client for metadata or realtime reply
  grpc::Status ProcessStream(ServerContext* context,
                             const eos::console::RequestProto* request,
                             grpc::ServerWriter<eos::console::ReplyProto>* writer)
  {
    std::string command;

    switch (request->command_case()) {
    case eos::console::RequestProto::kFind:
      command = "Find";
      break;

    case eos::console::RequestProto::kLs:
      command = "Ls";
      break;

    default:
      command = "unknown";
      break;
    }

    eos_static_debug("eos-wnc request from peer=%s IP=%s DN=%s "
                     "command=\"%s\" token_len=%zu",
                     context->peer().c_str(), GrpcServer::IP(context).c_str(),
                     GrpcServer::DN(context).c_str(),
                     command.c_str(),
                     request->auth().authkey().length());
    eos::common::VirtualIdentity vid;
    GrpcServer::Vid(context, vid, request->auth().authkey());
    WAIT_BOOT;
    const std::string requested_scope = WncRequestScope(*request, command);
    std::string configured_scopes;

    if (!WncScopeAllowed(vid, request->auth().authkey(), requested_scope,
                         &configured_scopes)) {
      eos::console::ReplyProto reply;
      SetWncScopeDenied(&reply, requested_scope, vid, configured_scopes);
      writer->Write(reply);
      return grpc::Status::OK;
    }

    GrpcWncInterface wnc;
    return wnc.ExecStreamCmd(vid, request, writer);
  }
};

#endif

//-----------------------------------------------------------------------------
// Run gRPC server for EOS Windows native client
//-----------------------------------------------------------------------------
void
GrpcWncServer::RunWnc(ThreadAssistant& assistant) noexcept
{
#ifdef EOS_GRPC

  if (getenv("EOS_MGM_WNC_SSL_CERT") &&
      getenv("EOS_MGM_WNC_SSL_KEY") &&
      getenv("EOS_MGM_WNC_SSL_CA")) {
    mSSL = true;
    mSSLCertFile = getenv("EOS_MGM_WNC_SSL_CERT");
    mSSLKeyFile = getenv("EOS_MGM_WNC_SSL_KEY");
    mSSLCaFile = getenv("EOS_MGM_WNC_SSL_CA");

    if (eos::common::StringConversion::LoadFileIntoString(mSSLCertFile.c_str(),
        mSSLCert) && !mSSLCert.length()) {
      eos_static_crit("Unable to load SSL certificate file '%s'",
                      mSSLCertFile.c_str());
      mSSL = false;
    }

    if (eos::common::StringConversion::LoadFileIntoString(mSSLKeyFile.c_str(),
        mSSLKey) && !mSSLKey.length()) {
      eos_static_crit("Unable to load SSL key file '%s'", mSSLKeyFile.c_str());
      mSSL = false;
    }

    if (eos::common::StringConversion::LoadFileIntoString(mSSLCaFile.c_str(),
        mSSLCa) && !mSSLCa.length()) {
      eos_static_crit("Unable to load SSL CA file '%s'", mSSLCaFile.c_str());
      mSSL = false;
    }
  }

  if (gGlobalOpts.mMgmUri.empty()) {
    if (getenv("EOS_MGM_URL")) {
      gGlobalOpts.mMgmUri = getenv("EOS_MGM_URL");
    } else {
      gGlobalOpts.mMgmUri = "root://localhost";
    }
  }

  eos_static_info("Creating gRPC server for EOS-wnc.");
  grpc::ServerBuilder wncBuilder;
  std::string bind_address_Wnc = "0.0.0.0:";
  bind_address_Wnc += std::to_string(mWncPort);

  if (mSSL) {
    grpc::SslServerCredentialsOptions::PemKeyCertPair keycert = {
      mSSLKey,
      mSSLCert
    };
    grpc::SslServerCredentialsOptions sslOps(
      GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY);
    sslOps.pem_root_certs = mSSLCa;
    sslOps.pem_key_cert_pairs.push_back(keycert);
    wncBuilder.AddListeningPort(bind_address_Wnc,
                                grpc::SslServerCredentials(sslOps));
    eos_static_info("SSL authentication is enabled on gRPC server for EOS-wnc.");
  } else {
    wncBuilder.AddListeningPort(bind_address_Wnc,
                                grpc::InsecureServerCredentials());
  }

  WncService wncService;
  wncBuilder.RegisterService(&wncService);
  mWncServer = wncBuilder.BuildAndStart();

  if (mWncServer) {
    eos_static_info("gRPC server for EOS-wnc is running on port %i.", mWncPort);
    /*WARNING: The server must be either shutting down or
    *some other thread must call a Shutdown for Wait function to ever return.*/
    mWncServer->Wait();
  } else {
    eos_static_err("gRPC server for EOS-wnc failed to start!");
  }

#else
  (void) mWncPort;
  (void) mSSL;
#endif
}

EOSMGMNAMESPACE_END
