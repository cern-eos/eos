#pragma once

#ifdef EOS_GRPC_GATEWAY

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
using eos::common::VirtualIdentity;
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
using eos::console::FindProto;
using eos::console::FsProto;
using eos::console::FsckProto;
using eos::console::GeoschedProto;
using eos::console::GroupProto;
using eos::console::HealthProto;
using eos::console::MapProto;
using eos::console::MemberProto;
using eos::console::IoProto;
using eos::console::LsProto;
using eos::console::MkdirProto;
using eos::console::MoveProto;
using eos::console::NodeProto;
using eos::console::NsProto;
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
  Status AclCall(VirtualIdentity& vid, const AclProto* aclRequest,
                 ReplyProto* reply);
  Status AccessCall(VirtualIdentity& vid, const AccessProto* accessRequest,
                    ReplyProto* reply);
  Status ArchiveCall(VirtualIdentity& vid, const ArchiveProto* archiveRequest,
                     ReplyProto* reply);
  Status AttrCall(VirtualIdentity& vid, const AttrProto* attrRequest,
                  ReplyProto* reply);
  Status BackupCall(VirtualIdentity& vid, const BackupProto* backupRequest,
                    ReplyProto* reply);
  Status ChmodCall(VirtualIdentity& vid, const ChmodProto* chmodRequest,
                   ReplyProto* reply);
  Status ChownCall(VirtualIdentity& vid, const ChownProto* chownRequest,
                   ReplyProto* reply);
  Status ConfigCall(VirtualIdentity& vid, const ConfigProto* configRequest,
                    ReplyProto* reply);
  Status ConvertCall(VirtualIdentity& vid, const ConvertProto* convertRequest,
                     ReplyProto* reply);
  Status CpCall(VirtualIdentity& vid, const CpProto* cpRequest,
                ReplyProto* reply);
  Status DebugCall(VirtualIdentity& vid, const DebugProto* debugRequest,
                   ReplyProto* reply);
  Status EvictCall(VirtualIdentity& vid, const EvictProto* evictRequest,
                   ReplyProto* reply);
  Status FileCall(VirtualIdentity& vid, const FileProto* fileRequest,
                  ReplyProto* reply);
  Status FileinfoCall(VirtualIdentity& vid, const FileinfoProto* fileinfoRequest,
                      ReplyProto* reply);
  Status FindCall(VirtualIdentity& vid, const FindProto* findRequest,
                  ServerWriter<ReplyProto>* writer);
  Status FsCall(VirtualIdentity& vid, const FsProto* fsRequest,
                ReplyProto* reply);
  Status FsckCall(VirtualIdentity& vid, const FsckProto* fsckRequest,
                  ServerWriter<ReplyProto>* writer);
  Status GeoschedCall(VirtualIdentity& vid, const GeoschedProto* geoschedRequest,
                      ReplyProto* reply);
  Status GroupCall(VirtualIdentity& vid, const GroupProto* groupRequest,
                   ReplyProto* reply);
  Status HealthCall(VirtualIdentity& vid, const HealthProto* healthRequest,
                    ReplyProto* reply);
  Status IoCall(VirtualIdentity& vid, const IoProto* ioRequest,
                ReplyProto* reply);
  Status LsCall(VirtualIdentity& vid, const LsProto* lsRequest,
                ServerWriter<ReplyProto>* writer);
  Status MapCall(VirtualIdentity& vid, const MapProto* mapRequest,
                 ReplyProto* reply);
  Status MemberCall(VirtualIdentity& vid, const MemberProto* memberRequest,
                    ReplyProto* reply);
  Status MkdirCall(VirtualIdentity& vid, const MkdirProto* mkdirRequest,
                   ReplyProto* reply);
  Status MvCall(VirtualIdentity& vid, const MoveProto* mvRequest,
                ReplyProto* reply);
  Status NodeCall(VirtualIdentity& vid, const NodeProto* nodeRequest,
                  ReplyProto* reply);
  Status NsCall(VirtualIdentity& vid, const NsProto* nsRequest,
                ReplyProto* reply);
  Status QuotaCall(VirtualIdentity& vid, const QuotaProto* quotaRequest,
                   ReplyProto* reply);
  Status RecycleCall(VirtualIdentity& vid, const RecycleProto* recycleRequest,
                     ReplyProto* reply);
  Status RmCall(VirtualIdentity& vid, const RmProto* rmRequest,
                ReplyProto* reply);
  Status RmdirCall(VirtualIdentity& vid, const RmdirProto* rmdirRequest,
                   ReplyProto* reply);
  Status RouteCall(VirtualIdentity& vid, const RouteProto* routeRequest,
                   ReplyProto* reply);
  Status SpaceCall(VirtualIdentity& vid, const SpaceProto* spaceRequest,
                   ReplyProto* reply);
  Status StatCall(VirtualIdentity& vid, const StatProto* statRequest,
                  ReplyProto* reply);
  Status StatusCall(VirtualIdentity& vid, const StatusProto* statusRequest,
                    ReplyProto* reply);
  Status TokenCall(VirtualIdentity& vid, const TokenProto* tokenRequest,
                   ReplyProto* reply);
  Status TouchCall(VirtualIdentity& vid, const TouchProto* touchRequest,
                   ReplyProto* reply);
  Status VersionCall(VirtualIdentity& vid, const VersionProto* versionRequest,
                     ReplyProto* reply);
  Status VidCall(VirtualIdentity& vid, const VidProto* vidRequest,
                 ReplyProto* reply);
  Status WhoCall(VirtualIdentity& vid, const WhoProto* whoRequest,
                 ReplyProto* reply);
  Status WhoamiCall(VirtualIdentity& vid, const WhoamiProto* whoamiRequest,
                    ReplyProto* reply);
//-----------------------------------------------------------------------------

private:

  void ExecProcCmd(eos::common::VirtualIdentity& vid, ReplyProto* reply,
                   std::string input, bool admin = true);
};

EOSMGMNAMESPACE_END

#endif // EOS_GRPC_GATEWAY
