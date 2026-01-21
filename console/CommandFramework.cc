// ----------------------------------------------------------------------
// File: CommandFramework.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "common/StringTokenizer.hh"
#include "common/Path.hh"
#include <XrdOuc/XrdOucString.hh>
#include <sstream>
#include <cstdlib>

CommandRegistry& CommandRegistry::instance()
{
  static CommandRegistry inst;
  return inst;
}

void CommandRegistry::reg(std::unique_ptr<IConsoleCommand> cmd)
{
  mCommandsView.push_back(cmd.get());
  mCommands.emplace_back(std::move(cmd));
}

IConsoleCommand* CommandRegistry::find(const std::string& name) const
{
  // Prefer most recently registered (native overrides legacy)
  for (auto it = mCommandsView.rbegin(); it != mCommandsView.rend(); ++it) {
    if (name == (*it)->name()) return *it;
  }
  // Simple aliases
  if (name == "fileinfo") {
    return find("file");
  }
  return nullptr;
}

int CFuncCommandAdapter::run(const std::vector<std::string>& args, CommandContext&)
{
  std::ostringstream oss;
  for (size_t i = 0; i < args.size(); ++i) {
    if (i) oss << ' ';
    oss << args[i];
  }
  std::string joined = oss.str();
  return mFunc((char*)joined.c_str());
}

// Default empty; concrete commands will be registered here as they are migrated
void RegisterNativeConsoleCommands()
{
  // Registration split across native modules for maintainability
  // Core
  extern void RegisterCoreNativeCommands();
  RegisterCoreNativeCommands();
  // Pwd
  extern void RegisterPwdNativeCommand();
  RegisterPwdNativeCommand();
  // Cd
  extern void RegisterCdNativeCommand();
  RegisterCdNativeCommand();
  // Ls
  extern void RegisterLsNativeCommand();
  RegisterLsNativeCommand();
  // Cp
  extern void RegisterCpNativeCommand();
  RegisterCpNativeCommand();
  // Version
  extern void RegisterVersionNativeCommand();
  RegisterVersionNativeCommand();
  // Status
  extern void RegisterStatusNativeCommand();
  RegisterStatusNativeCommand();
  // Mkdir/Rm
  extern void RegisterMkdirNativeCommand();
  RegisterMkdirNativeCommand();
  extern void RegisterRmProtoNativeCommand();
  RegisterRmProtoNativeCommand();
  // Info
  extern void RegisterInfoNativeCommand();
  RegisterInfoNativeCommand();
  // Stat
  extern void RegisterStatNativeCommand();
  RegisterStatNativeCommand();
  // Mv
  extern void RegisterMvNativeCommand();
  RegisterMvNativeCommand();
  // Ln
  extern void RegisterLnNativeCommand();
  RegisterLnNativeCommand();
  // Rmdir
  extern void RegisterRmdirNativeCommand();
  RegisterRmdirNativeCommand();
  // Touch
  extern void RegisterTouchNativeCommand();
  RegisterTouchNativeCommand();
  // Cat
  extern void RegisterCatNativeCommand();
  RegisterCatNativeCommand();
  // Who
  extern void RegisterWhoNativeCommand();
  RegisterWhoNativeCommand();
  // Whoami
  extern void RegisterWhoamiNativeCommand();
  RegisterWhoamiNativeCommand();
  // Proto commands
  extern void RegisterAccessProtoNativeCommand();
  RegisterAccessProtoNativeCommand();
  extern void RegisterAclProtoNativeCommand();
  RegisterAclProtoNativeCommand();
  extern void RegisterConfigProtoNativeCommand();
  RegisterConfigProtoNativeCommand();
  extern void RegisterConvertProtoNativeCommand();
  RegisterConvertProtoNativeCommand();
  extern void RegisterDevicesProtoNativeCommand();
  RegisterDevicesProtoNativeCommand();
  extern void RegisterDfProtoNativeCommand();
  RegisterDfProtoNativeCommand();
  extern void RegisterFindProtoNativeCommand();
  RegisterFindProtoNativeCommand();
  extern void RegisterFsProtoNativeCommand();
  RegisterFsProtoNativeCommand();
  extern void RegisterFsckProtoNativeCommand();
  RegisterFsckProtoNativeCommand();
  extern void RegisterGroupProtoNativeCommand();
  RegisterGroupProtoNativeCommand();
  extern void RegisterIoProtoNativeCommand();
  RegisterIoProtoNativeCommand();
  extern void RegisterNodeProtoNativeCommand();
  RegisterNodeProtoNativeCommand();
  extern void RegisterNsProtoNativeCommand();
  RegisterNsProtoNativeCommand();
  extern void RegisterQuotaProtoNativeCommand();
  RegisterQuotaProtoNativeCommand();
  extern void RegisterRecycleProtoNativeCommand();
  RegisterRecycleProtoNativeCommand();
  extern void RegisterRegisterProtoNativeCommand();
  RegisterRegisterProtoNativeCommand();
  extern void RegisterRouteProtoNativeCommand();
  RegisterRouteProtoNativeCommand();
  extern void RegisterTokenProtoNativeCommand();
  RegisterTokenProtoNativeCommand();
  extern void RegisterSpaceProtoNativeCommand();
  RegisterSpaceProtoNativeCommand();
  extern void RegisterSchedProtoNativeCommand();
  RegisterSchedProtoNativeCommand();
  // file/fuse/fusex
  extern void RegisterFileNativeCommand();
  RegisterFileNativeCommand();
  extern void RegisterFileInfoAliasCommand();
  RegisterFileInfoAliasCommand();
  extern void RegisterFuseNativeCommand();
  RegisterFuseNativeCommand();
  extern void RegisterFusexNativeCommand();
  RegisterFusexNativeCommand();
  // Misc
  extern void RegisterBackupNativeCommand();
  RegisterBackupNativeCommand();
  extern void RegisterClearNativeCommand();
  RegisterClearNativeCommand();
  extern void RegisterDebugNativeCommand();
  RegisterDebugNativeCommand();
  extern void RegisterDuNativeCommand();
  RegisterDuNativeCommand();
  extern void RegisterEvictNativeCommand();
  RegisterEvictNativeCommand();
  extern void RegisterMotdNativeCommand();
  RegisterMotdNativeCommand();
  extern void RegisterOldfindNativeCommand();
  RegisterOldfindNativeCommand();
  extern void RegisterRcloneNativeCommand();
  RegisterRcloneNativeCommand();
  extern void RegisterSquashNativeCommand();
  RegisterSquashNativeCommand();
  extern void RegisterTestNativeCommand();
  RegisterTestNativeCommand();
  // Attr/Mode
  extern void RegisterArchiveNativeCommand();
  RegisterArchiveNativeCommand();
  extern void RegisterAttrNativeCommand();
  RegisterAttrNativeCommand();
  extern void RegisterChmodNativeCommand();
  RegisterChmodNativeCommand();
  extern void RegisterChownNativeCommand();
  RegisterChownNativeCommand();
  // Admin/Device and misc extras
  extern void RegisterDaemonNativeCommand();
  RegisterDaemonNativeCommand();
  extern void RegisterGeoschedNativeCommand();
  RegisterGeoschedNativeCommand();
  extern void RegisterInspectorNativeCommand();
  RegisterInspectorNativeCommand();
  extern void RegisterLicenseNativeCommand();
  RegisterLicenseNativeCommand();
  extern void RegisterMapNativeCommand();
  RegisterMapNativeCommand();
  extern void RegisterMemberNativeCommand();
  RegisterMemberNativeCommand();
  extern void RegisterAccountingNativeCommand();
  RegisterAccountingNativeCommand();
  extern void RegisterHealthNativeCommand();
  RegisterHealthNativeCommand();
  extern void RegisterReconnectNativeCommand();
  RegisterReconnectNativeCommand();
  extern void RegisterReportNativeCommand();
  RegisterReportNativeCommand();
  extern void RegisterRtlogNativeCommand();
  RegisterRtlogNativeCommand();
  extern void RegisterRoleNativeCommand();
  RegisterRoleNativeCommand();
  extern void RegisterScitokenNativeCommand();
  RegisterScitokenNativeCommand();
  extern void RegisterTrackerNativeCommand();
  RegisterTrackerNativeCommand();
  extern void RegisterVidNativeCommand();
  RegisterVidNativeCommand();
  // RemainingLegacyNativeCommands removed
}


