#pragma once

#ifdef EOS_GRPC

//-----------------------------------------------------------------------------
#include "common/VirtualIdentity.hh"
#include "common/Logging.hh"
#include "mgm/Namespace.hh"
#include "proto/eos_rest_gateway/eos_rest_gateway_service.grpc.pb.h"
//-----------------------------------------------------------------------------
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::Status;
using eos::console::AccessProto;
using eos::console::AclProto;
using eos::console::ArchiveProto;
using eos::console::AttrProto;
using eos::console::BackupProto;
using eos::console::ChmodProto;
using eos::console::ChownProto;
using eos::console::ConfigProto;
using eos::console::ConfigProto;
using eos::console::ConvertProto;
using eos::console::CpProto;
using eos::console::DebugProto;
using eos::console::EvictProto;
using eos::console::FileProto;
using eos::console::FileinfoProto;
using eos::console::FsProto;
using eos::console::FsckProto;
using eos::console::GeoschedProto;
using eos::console::GroupProto;
using eos::console::HealthProto;
using eos::console::MapProto;
using eos::console::MemberProto;
using eos::console::IoProto;
using eos::console::MkdirProto;
using eos::console::MoveProto;
using eos::console::NodeProto;
using eos::console::NsProto;
using eos::console::QoSProto;
using eos::console::QuotaProto;
using eos::console::RecycleProto;
using eos::console::ReplyProto;
using eos::console::RmProto;
using eos::console::RmdirProto;
using eos::console::RouteProto;
using eos::console::SpaceProto;
using eos::console::StatProto;
using eos::console::StatusProto;
using eos::console::TokenProto;
using eos::console::TouchProto;
using eos::console::VersionProto;
using eos::console::VidProto;
using eos::console::WhoProto;
using eos::console::WhoamiProto;
//-----------------------------------------------------------------------------

EOSMGMNAMESPACE_BEGIN

/**
 * @file   GrpcRestGwInterface.hh
 *
 * @brief  This class bridges Http client commands to gRPC requests
 *
 */
class GrpcRestGwInterface : public eos::common::LogId
{
public:

//-----------------------------------------------------------------------------
//  Execute specific EOS command for the EOS gRPC request
//-----------------------------------------------------------------------------
  Status AclCall(const AclProto* aclRequest, ReplyProto* reply);
  Status AccessCall(const AccessProto* accessRequest, ReplyProto* reply);
  Status ArchiveCall(const ArchiveProto* archiveRequest, ReplyProto* reply);
  Status AttrCall(const AttrProto* attrRequest, ReplyProto* reply);
  Status BackupCall(const BackupProto* backupRequest, ReplyProto* reply);
  Status ChmodCall(const ChmodProto* chmodRequest, ReplyProto* reply);
  Status ChownCall(const ChownProto* chownRequest, ReplyProto* reply);
  Status ConfigCall(const ConfigProto* configRequest, ReplyProto* reply);
  Status ConvertCall(const ConvertProto* convertRequest, ReplyProto* reply);
  Status CpCall(const CpProto* cpRequest, ReplyProto* reply);
  Status DebugCall(const DebugProto* debugRequest, ReplyProto* reply);
  Status EvictCall(const EvictProto* evictRequest, ReplyProto* reply);
  Status FileCall(const FileProto* fileRequest, ReplyProto* reply);
  Status FileinfoCall(const FileinfoProto* fileinfoRequest, ReplyProto* reply);
  Status FsCall(const FsProto* fsRequest, ReplyProto* reply);
  Status FsckCall(const FsckProto* fsckRequest, ReplyProto* reply);
  Status GeoschedCall(const GeoschedProto* geoschedRequest, ReplyProto* reply);
  Status GroupCall(const GroupProto* groupRequest, ReplyProto* reply);
  Status HealthCall(const HealthProto* healthRequest, ReplyProto* reply);
  Status IoCall(const IoProto* ioRequest, ReplyProto* reply);
  Status MapCall(const MapProto* mapRequest, ReplyProto* reply);
  Status MemberCall(const MemberProto* memberRequest, ReplyProto* reply);
  Status MkdirCall(const MkdirProto* mkdirRequest, ReplyProto* reply);
  Status MvCall(const MoveProto* mvRequest, ReplyProto* reply);
  Status NodeCall(const NodeProto* nodeRequest, ReplyProto* reply);
  Status NsCall(const NsProto* nsRequest, ReplyProto* reply);
  Status QoSCall(const QoSProto* qosRequest, ReplyProto* reply);
  Status QuotaCall(const QuotaProto* quotaRequest, ReplyProto* reply);
  Status RecycleCall(const RecycleProto* recycleRequest, ReplyProto* reply);
  Status RmCall(const RmProto* rmRequest, ReplyProto* reply);
  Status RmdirCall(const RmdirProto* rmdirRequest, ReplyProto* reply);
  Status RouteCall(const RouteProto* routeRequest, ReplyProto* reply);
  Status SpaceCall(const SpaceProto* spaceRequest, ReplyProto* reply);
  Status StatCall(const StatProto* statRequest, ReplyProto* reply);
  Status StatusCall(const StatusProto* statusRequest, ReplyProto* reply);
  Status TokenCall(const TokenProto* tokenRequest, ReplyProto* reply);
  Status TouchCall(const TouchProto* touchRequest, ReplyProto* reply);
  Status VersionCall(const VersionProto* versionRequest, ReplyProto* reply);
  Status VidCall(const VidProto* vidRequest, ReplyProto* reply);
  Status WhoCall(const WhoProto* whoRequest, ReplyProto* reply);
  Status WhoamiCall(const WhoamiProto* whoamiRequest, ReplyProto* reply);
//-----------------------------------------------------------------------------

private:

  void ExecProcCmd(eos::common::VirtualIdentity vid, ReplyProto* reply,
                   std::string input, bool admin = true);
};

EOSMGMNAMESPACE_END

#endif // EOS_GRPC
