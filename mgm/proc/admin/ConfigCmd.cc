//------------------------------------------------------------------------------
// @file: ConfigCmd.cc
// @author: Fabio Luchetti - CERN
//------------------------------------------------------------------------------

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

#include "ConfigCmd.hh"
#include "mgm/proc/ProcInterface.hh"

#include "XrdOuc/XrdOucEnv.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/FsView.hh"
#include "mgm/config/IConfigEngine.hh"


EOSMGMNAMESPACE_BEGIN


//------------------------------------------------------------------------------
// Method implementing the specific behavior of the command executed by the
// asynchronous thread
//------------------------------------------------------------------------------
eos::console::ReplyProto
ConfigCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;

  if ((mVid.uid != 0)) {
    reply.set_std_err("error: you have to take role 'root' to execute this command");
    reply.set_retc(EPERM);
    return reply;
  }

  eos::console::ConfigProto config = mReqProto.config();

  switch (mReqProto.config().subcmd_case()) {
  case eos::console::ConfigProto::kLs:
    LsSubcmd(config.ls(), reply);
    break;

  case eos::console::ConfigProto::kDump:
    DumpSubcmd(config.dump(), reply);
    break;

  case eos::console::ConfigProto::kReset:
    ResetSubcmd(reply);
    break;

  case eos::console::ConfigProto::kExp:
    ExportSubcmd(config.exp(), reply);
    break;

  case eos::console::ConfigProto::kSave:
    SaveSubcmd(config.save(), reply);
    break;

  case eos::console::ConfigProto::kLoad:
    LoadSubcmd(config.load(), reply);
    break;

  case eos::console::ConfigProto::kChangelog:
    ChangelogSubcmd(config.changelog(), reply);
    break;

  default:
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not supported");
  }

  return reply;
}

//----------------------------------------------------------------------------
// Execute ls subcommand
//----------------------------------------------------------------------------
void ConfigCmd::LsSubcmd(const eos::console::ConfigProto_LsProto& ls,
                         eos::console::ReplyProto& reply)
{
  XrdOucString listing = "";

  if (!(gOFS->ConfEngine->ListConfigs(listing, ls.showbackup()))) {
    reply.set_std_err("error: listing of existing configs failed!");
    reply.set_retc(errno);
  } else {
    reply.set_std_out(listing.c_str());
  }
}

//----------------------------------------------------------------------------
// Execute dump subcommand
//----------------------------------------------------------------------------
void ConfigCmd::DumpSubcmd(const eos::console::ConfigProto_DumpProto& dump,
                           eos::console::ReplyProto& reply)
{
  XrdOucString sdump = "";

  if (!gOFS->ConfEngine->DumpConfig(sdump, dump.file())) {
    reply.set_std_err("error: failed to dump configuration");
    reply.set_retc(errno);
  } else {
    reply.set_std_out(sdump.c_str());
  }
}

//----------------------------------------------------------------------------
// Execute reset subcommand
//----------------------------------------------------------------------------
void ConfigCmd::ResetSubcmd(eos::console::ReplyProto& reply)
{
  gOFS->ConfEngine->ResetConfig();
  reply.set_std_out("success: configuration has been reset(cleaned)!");
}

//----------------------------------------------------------------------------
// Execute export subcommand
//----------------------------------------------------------------------------
void ConfigCmd::ExportSubcmd(const eos::console::ConfigProto_ExportProto& exp,
                             eos::console::ReplyProto& reply)
{
  reply.set_std_err("error: export command has been deprecated");
  reply.set_retc(EINVAL);
}

//----------------------------------------------------------------------------
// Execute save subcommand
//----------------------------------------------------------------------------
void ConfigCmd::SaveSubcmd(const eos::console::ConfigProto_SaveProto& save,
                           eos::console::ReplyProto& reply)
{
  eos_notice("config save: %s", save.ShortDebugString().c_str());
  XrdOucString std_err;

  if (!gOFS->ConfEngine->SaveConfig(save.file(), save.force(),
                                    mReqProto.comment(), std_err)) {
    reply.set_std_err(std_err.c_str());
    reply.set_retc(errno);
  } else {
    reply.set_std_out("success: configuration successfully saved!");
  }
}

//----------------------------------------------------------------------------
// Execute load subcommand
//----------------------------------------------------------------------------
void ConfigCmd::LoadSubcmd(const eos::console::ConfigProto_LoadProto& load,
                           eos::console::ReplyProto& reply)
{
  eos_notice("config load: %s", load.ShortDebugString().c_str());
  eos::mgm::ConfigResetMonitor fsview_cfg_reset_monitor;
  XrdOucString std_err;

  if (!gOFS->ConfEngine->LoadConfig(load.file(), std_err)) {
    reply.set_std_err(std_err.c_str());
    reply.set_retc(errno);
  } else {
    reply.set_std_out("success: configuration successfully loaded!");
  }
}

//----------------------------------------------------------------------------
// Execute changelog subcommand
//----------------------------------------------------------------------------
void ConfigCmd::ChangelogSubcmd(const eos::console::ConfigProto_ChangelogProto&
                                changelog,
                                eos::console::ReplyProto& reply)
{
  std::string std_out;
  gOFS->ConfEngine->Tail(changelog.lines(), std_out);
  eos_notice("config changelog");
  reply.set_std_out(std_out);
}

EOSMGMNAMESPACE_END
