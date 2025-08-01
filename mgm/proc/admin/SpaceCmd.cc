//------------------------------------------------------------------------------
// @file: SpaceCmd.cc
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

#include "SpaceCmd.hh"
#include "mgm/proc/ProcInterface.hh"
#include "mgm/tgc/Constants.hh"
#include "mgm/http/rest-api/Constants.hh"
#include "mgm/http/rest-api/manager/RestApiManager.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/LRU.hh"
#include "mgm/Acl.hh"
#include "common/Path.hh"
#include "mgm/tracker/ReplicationTracker.hh"
#include "mgm/inspector/FileInspector.hh"
#include "mgm/Egroup.hh"
#include "mgm/config/IConfigEngine.hh"
#include "mgm/GroupBalancer.hh"
#include "mgm/GroupDrainer.hh"
#include "mgm/balancer/FsBalancer.hh"
#include "mgm/placement/FsScheduler.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IView.hh"
#include "common/Constants.hh"
#include "common/StringTokenizer.hh"
#include "common/StringUtils.hh"
#include "common/token/EosTok.hh"

EOSMGMNAMESPACE_BEGIN
static const std::string BALANCER_KEY_PREFIX = "balancer";
static const std::string GROUPBALANCER_KEY_PREFIX = "groupbalancer";
static const std::string GROUPDRAINER_KEY_PREFIX = "groupdrainer";

//------------------------------------------------------------------------------
// Method implementing the specific behavior of the command executed by the
// asynchronous thread
//------------------------------------------------------------------------------
eos::console::ReplyProto
SpaceCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::SpaceProto space = mReqProto.space();

  switch (mReqProto.space().subcmd_case()) {
  case eos::console::SpaceProto::kLs:
    LsSubcmd(space.ls(), reply);
    break;

  case eos::console::SpaceProto::kSet:
    SetSubcmd(space.set(), reply);
    break;

  case eos::console::SpaceProto::kStatus:
    StatusSubcmd(space.status(), reply);
    break;

  case eos::console::SpaceProto::kNodeSet:
    NodeSetSubcmd(space.nodeset(), reply);
    break;

  case eos::console::SpaceProto::kNodeGet:
    NodeGetSubcmd(space.nodeget(), reply);
    break;

  case eos::console::SpaceProto::kReset:
    ResetSubcmd(space.reset(), reply);
    break;

  case eos::console::SpaceProto::kDefine:
    DefineSubcmd(space.define(), reply);
    break;

  case eos::console::SpaceProto::kConfig:
    ConfigSubcmd(space.config(), reply);
    break;

  case eos::console::SpaceProto::kQuota:
    QuotaSubcmd(space.quota(), reply);
    break;

  case eos::console::SpaceProto::kRm:
    RmSubcmd(space.rm(), reply);
    break;

  case eos::console::SpaceProto::kTracker:
    TrackerSubcmd(space.tracker(), reply);
    break;

  case eos::console::SpaceProto::kInspector:
    InspectorSubcmd(space.inspector(), reply);
    break;

  case eos::console::SpaceProto::kGroupbalancer:
    GroupBalancerSubCmd(space.groupbalancer(), reply);
    break;

  case eos::console::SpaceProto::kGroupdrainer:
    GroupDrainerSubCmd(space.groupdrainer(), reply);
    break;

  default:
    reply.set_std_err("error: not supported");
    reply.set_retc(EINVAL);
  }

  return reply;
}

//----------------------------------------------------------------------------
// Execute ls subcommand
//----------------------------------------------------------------------------
void SpaceCmd::LsSubcmd(const eos::console::SpaceProto_LsProto& ls,
                        eos::console::ReplyProto& reply)
{
  using eos::console::SpaceProto;
  bool json_output = false;
  std::string list_format;
  std::string format;
  auto format_case = ls.outformat();

  if ((format_case == SpaceProto::LsProto::NONE) && WantsJsonOutput()) {
    format_case = SpaceProto::LsProto::MONITORING;
  }

  switch (format_case) {
  case SpaceProto::LsProto::LISTING:
    format = FsView::GetSpaceFormat("l");
    list_format = FsView::GetFileSystemFormat("l");
    break;

  case SpaceProto::LsProto::MONITORING:
    format = FsView::GetSpaceFormat("m");
    json_output = WantsJsonOutput();
    break;

  case SpaceProto::LsProto::IO:
    format = FsView::GetSpaceFormat("io");
    break;

  case SpaceProto::LsProto::FSCK:
    format = FsView::GetSpaceFormat("fsck");
    break;

  default : // NONE
    format = FsView::GetSpaceFormat("");
    break;
  }

  std::string std_out;
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  FsView::gFsView.PrintSpaces(std_out, format, list_format, ls.outdepth(),
                              ls.selection().c_str(), "", mReqProto.dontcolor());

  if (json_output) {
    std_out = ResponseToJsonString(std_out);
  }

  reply.set_std_out(std_out);
  reply.set_retc(0);
}

//----------------------------------------------------------------------------
// Execute status subcommand
//----------------------------------------------------------------------------
void SpaceCmd::StatusSubcmd(const eos::console::SpaceProto_StatusProto& status,
                            eos::console::ReplyProto& reply)
{
  std::ostringstream std_out;
  bool monitoring = status.outformat_m() || WantsJsonOutput();
  const char* fmtstr = (monitoring) ? "%s=%s " : "%-32s := %s\n";
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

  if (!FsView::gFsView.mSpaceView.count(status.mgmspace())) {
    reply.set_std_err("error: cannot find space - no space with name=" +
                      status.mgmspace());
    reply.set_retc(ENOENT);
    return;
  }

  if (!monitoring) {
    std_out <<
            "# ------------------------------------------------------------------------------------\n";
    std_out << "# Space Variables\n";
    std_out <<
            "# ....................................................................................\n";
  }

  std::vector <std::string> keylist;
  FsView::gFsView.mSpaceView[status.mgmspace()]->GetConfigKeys(keylist);
  std::sort(keylist.begin(), keylist.end());

  for (auto& i : keylist) {
    char line[32678];

    if (((i == "nominalsize") || (i == "headroom")) && !monitoring) {
      XrdOucString sizestring;
      // size printout
      snprintf(line, sizeof(line) - 1, fmtstr, i.c_str(),
               eos::common::StringConversion::GetReadableSizeString(
                 sizestring,
                 strtoull(FsView::gFsView.mSpaceView[status.mgmspace()]
                          ->GetConfigMember(i).c_str(), nullptr, 10),
                 "B"));
    } else {
      snprintf(line, sizeof(line) - 1, fmtstr, i.c_str(),
               FsView::gFsView.mSpaceView[status.mgmspace()]
               ->GetConfigMember(i).c_str());
    }

    std_out << line;
  }

  if (WantsJsonOutput()) {
    std_out.str(ResponseToJsonString(std_out.str()));
  }

  reply.set_std_out(std_out.str());
  reply.set_retc(0);
}

//----------------------------------------------------------------------------
// Execute set subcommand
//----------------------------------------------------------------------------
void SpaceCmd::SetSubcmd(const eos::console::SpaceProto_SetProto& set,
                         eos::console::ReplyProto& reply)
{
  std::ostringstream std_out, std_err;
  int ret_c = 0;

  if (mVid.uid != 0) {
    reply.set_std_err("error: you have to take role 'root' to execute this command");
    reply.set_retc(EPERM);
    return;
  }

  if (set.mgmspace().empty()) {
    reply.set_std_err("error: illegal parameters");
    reply.set_retc(EINVAL);
    return;
  }

  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

  if (!FsView::gFsView.mSpaceView.count(set.mgmspace())) {
    reply.set_std_err("error: no such space - define one using 'space define' or add a filesystem under that space!");
    reply.set_retc(EINVAL);
    return;
  }

  std::string key = "status";
  std::string status = (set.state_switch()) ? "on" : "off";

  // Loop over all groups within this space
  if (FsView::gFsView.mSpaceGroupView.count(set.mgmspace())) {
    for (auto& group : FsView::gFsView.mSpaceGroupView.at(set.mgmspace())) {
      if (!group->SetConfigMember(key, status)) {
        std_err << "error: cannot set status in group <" << group->mName << ">\n";
        ret_c = EIO;
      }
    }
  }

  // Enable all nodes if 'on' request
  if (set.state_switch()) {
    for (auto& node : FsView::gFsView.mNodeView) {
      if (!node.second->SetConfigMember(key, status)) {
        std_err << "error: cannot set status=on in node <"
                << node.second->mName << ">\n";
        ret_c = EIO;
      }
    }
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
  reply.set_retc(ret_c);
}

//----------------------------------------------------------------------------
// Execute node-set subcommand
//----------------------------------------------------------------------------
void SpaceCmd::NodeSetSubcmd(const eos::console::SpaceProto_NodeSetProto&
                             nodeset, eos::console::ReplyProto& reply)
{
  std::ostringstream std_out, std_err;
  int ret_c = 0;
  std::string val = nodeset.nodeset_value();

  if (mVid.uid != 0) {
    reply.set_std_err("error: you have to take role 'root' to execute this command");
    reply.set_retc(EPERM);
    return;
  }

  if (nodeset.mgmspace().empty() || nodeset.nodeset_key().empty() ||
      nodeset.nodeset_value().empty()) {
    reply.set_std_err("error: illegal parameters");
    reply.set_retc(EINVAL);
    return;
  }

  eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);

  if (!FsView::gFsView.mSpaceView.count(nodeset.mgmspace())) {
    reply.set_std_err("error: no such space - define one using 'space define' or add a filesystem under that space!");
    reply.set_retc(EINVAL);
    return;
  }

  {
    // loop over all nodes
    std::map<std::string, FsNode*>::const_iterator it;

    for (it = FsView::gFsView.mNodeView.begin();
         it != FsView::gFsView.mNodeView.end(); it++) {
      XrdOucString file = val.c_str();

      if (file.beginswith("file:/")) {
        // load the file on the MGM
        file.erase(0, 5);
        eos::common::Path iPath(file.c_str());
        XrdOucString fpath = iPath.GetPath();

        if (!fpath.beginswith("/var/eos/")) {
          std_err.str(("error: cannot load requested file=" + file +
                       " - only files under /var/eos/ can bo loaded\n").c_str());
          ret_c = EINVAL;
        } else {
          std::ifstream ifs(file.c_str(), std::ios::in | std::ios::binary);

          if (!ifs) {
            std_err.str(("error: cannot load requested file=" + file).c_str());
            ret_c = EINVAL;
          } else {
            val = std::string((std::istreambuf_iterator<char>(ifs)),
                              std::istreambuf_iterator<char>());
            // store the value b64 encoded
            XrdOucString val64;
            eos::common::SymKey::Base64Encode((char*) val.c_str(), val.length(), val64);
            val = ("base64:" + val64).c_str();
            std_out << "success: loaded contents \n" + val;
          }
        }
      }

      if (!ret_c && !it->second->SetConfigMember(nodeset.nodeset_key(), val)) {
        std_err << "error: cannot set node-set for node <" + it->first + ">\n";
        ret_c = EIO;
      }
    }
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
  reply.set_retc(ret_c);
}

//----------------------------------------------------------------------------
// Execute node-get subcommand
//----------------------------------------------------------------------------
void SpaceCmd::NodeGetSubcmd(const eos::console::SpaceProto_NodeGetProto&
                             nodeget, eos::console::ReplyProto& reply)
{
  std::ostringstream std_out;

  if (mVid.uid != 0) {
    reply.set_std_err("error: you have to take role 'root' to execute this command");
    reply.set_retc(EPERM);
    return;
  }

  if (nodeget.mgmspace().empty() || nodeget.nodeget_key().empty()) {
    reply.set_std_err("error: illegal parameters");
    reply.set_retc(EINVAL);
    return;
  }

  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

  if (!FsView::gFsView.mSpaceView.count(nodeget.mgmspace())) {
    reply.set_std_err("error: no such space - define one using 'space define' or add a filesystem under that space!");
    reply.set_retc(EINVAL);
    return;
  }

  {
    std::string val;
    std::string new_val;
    bool identical = true;
    // loop over all nodes
    std::map<std::string, FsNode*>::const_iterator it;

    for (it = FsView::gFsView.mNodeView.begin();
         it != FsView::gFsView.mNodeView.end(); it++) {
      new_val = it->second->GetConfigMember(nodeget.nodeget_key());

      if (val.length() && new_val != val) {
        identical = false;
      }

      val = new_val;
      std_out << "# [ " + (it->first).substr(0,
                                             it->first.find(':')) + " ]\n" + new_val + '\n';
    }

    if (identical) {
      std_out.str("*:=" + val + '\n');
    }
  }

  reply.set_std_out(std_out.str());
}

//----------------------------------------------------------------------------
// Execute reset subcommand
//----------------------------------------------------------------------------
void SpaceCmd::ResetSubcmd(const eos::console::SpaceProto_ResetProto& reset,
                           eos::console::ReplyProto& reply)
{
  std::ostringstream std_out, std_err;
  int ret_c = 0;
  eos::common::RWMutexReadLock fsViewLock(FsView::gFsView.ViewMutex);

  switch (reset.option()) {
  case eos::console::SpaceProto_ResetProto::DRAIN: {
    if (FsView::gFsView.mSpaceView.count(reset.mgmspace())) {
      FsView::gFsView.mSpaceView[reset.mgmspace()]->ResetDraining();
      std_out << "info: reset draining in space '" + reset.mgmspace() + "'";
    } else {
      std_err << "error: illegal space name";
      ret_c = EINVAL;
    }
  }
  break;

  case eos::console::SpaceProto_ResetProto::EGROUP: {
    gOFS->EgroupRefresh->Reset();
    std_out << "\ninfo: clear cached EGroup information ...";
  }
  break;

  case eos::console::SpaceProto_ResetProto::NSFILESISTEMVIEW: {
    eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
    gOFS->eosFsView->shrink();
    std_out << "\ninfo: resized namespace filesystem view ...";
  }
  break;

  case eos::console::SpaceProto_ResetProto::NSFILEMAP: {
    std_out << "\n info: ns does not support file map resizing";
  }
  break;

  case eos::console::SpaceProto_ResetProto::NSDIRECTORYMAP: {
    std_out << "\ninfo: ns does not support directory map resizing";
  }
  break;

  case eos::console::SpaceProto_ResetProto::NS: {
    eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
    gOFS->eosFsView->shrink();
    std_out << "\ninfo: ns does not support map resizing";
  }
  break;

  case eos::console::SpaceProto_ResetProto::MAPPING: {
    eos::common::Mapping::Reset();
    std_out << "\ninfo: clear all user/group uid/gid caches ...\n";
  }
  break;

  case eos::console::SpaceProto_ResetProto::SCHEDULEDRAIN: {
    gOFS->mFidTracker.Clear(eos::mgm::TrackerType::Drain);
    std_out.str("info: reset drain scheduling map in space '" + reset.mgmspace() +
                '\'');
  }
  break;

  case eos::console::SpaceProto_ResetProto::SCHEDULEBALANCE: {
    gOFS->mFidTracker.Clear(eos::mgm::TrackerType::Balance);
    std_out.str("info: reset balance scheduling map in space '" + reset.mgmspace() +
                '\'');
  }
  break;

  default: { // NONE - when NONE, do cases DRAIN and EGROUP and MAPPING
    if (FsView::gFsView.mSpaceView.count(reset.mgmspace())) {
      FsView::gFsView.mSpaceView[reset.mgmspace()]->ResetDraining();
      std_out << "info: reset draining in space '" + reset.mgmspace() + "'";
    } else {
      std_err << "error: illegal space name";
      ret_c = EINVAL;
    }

    gOFS->EgroupRefresh->Reset();
    std_out << "\ninfo: clear cached EGroup information ...";
    eos::common::Mapping::Reset();
    std_out << "\ninfo: clear all user/group uid/gid caches ...\n";
  }
  break;
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
  reply.set_retc(ret_c);
}

//----------------------------------------------------------------------------
// Execute define subcommand
//----------------------------------------------------------------------------
void SpaceCmd::DefineSubcmd(const eos::console::SpaceProto_DefineProto& define,
                            eos::console::ReplyProto& reply)
{
  if (mVid.uid != 0) {
    reply.set_std_err("error: you have to take role 'root' to execute this command");
    reply.set_retc(EPERM);
    return;
  }

  if (define.mgmspace().empty()) {
    reply.set_std_err("error: illegal parameters <space-name>");
    reply.set_retc(EINVAL);
    return;
  }

  if ((define.groupsize() * define.groupmod()) > 65536) {
    reply.set_std_err("error: the product of <groupsize>*<groupsize> must be a positive integer (<=65536)!");
    reply.set_retc(EINVAL);
    return;
  }

  eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);

  if (!FsView::gFsView.mSpaceView.count(define.mgmspace())) {
    reply.set_std_out("info: creating space '" + define.mgmspace() + "'");

    if (!FsView::gFsView.RegisterSpace(define.mgmspace().c_str())) {
      reply.set_std_err("error: cannot register space <" + define.mgmspace() + ">");
      reply.set_retc(EIO);
      return;
    }
  }

  // Set the new space parameters
  auto space = FsView::gFsView.mSpaceView[define.mgmspace()];

  if ((!space->SetConfigMember("groupsize",
                               std::to_string(define.groupsize()))) ||
      (!space->SetConfigMember("groupmod", std::to_string(define.groupmod())))) {
    reply.set_std_err("error: cannot set space config value");
    reply.set_retc(EIO);
  }
}

//----------------------------------------------------------------------------
// Execute config subcommand
//----------------------------------------------------------------------------
void SpaceCmd::ConfigSubcmd(const eos::console::SpaceProto_ConfigProto& config,
                            eos::console::ReplyProto& reply)
{
  if (mVid.uid != 0) {
    reply.set_std_err("error: you have to take role 'root' to execute this command");
    reply.set_retc(EPERM);
    return;
  }

  int ret_c = 0;
  std::ostringstream std_out, std_err;
  const std::string space_name = config.mgmspace_name();
  std::string key = config.mgmspace_key();
  std::string value = config.mgmspace_value();

  if (space_name.empty() || key.empty() ||
      (!config.remove() && value.empty())) {
    reply.set_std_err("error: illegal parameters");
    reply.set_retc(EINVAL);
    return;
  }

  bool applied = false;
  FileSystem* fs = nullptr;
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  auto it_space = FsView::gFsView.mSpaceView.find(space_name);

  if ((it_space == FsView::gFsView.mSpaceView.end()) ||
      (it_space->second == nullptr)) {
    ret_c = EINVAL;
    std_err.str("error: cannot find space <" + space_name + ">");
    reply.set_std_err(std_err.str());
    reply.set_retc(ret_c);
    return;
  }

  FsSpace* space = it_space->second;

  if (!strcmp(mgm::rest::TAPE_REST_API_SWITCH_ON_OFF, key.c_str())) {
    applied = true;

    //REST API activation
    if ((value != "on") && (value != "off")) {
      ret_c = EINVAL;
      std_err.str("error: value has to either on or off");
    } else {
      if (space_name != "default") {
        ret_c = EIO;
        std_err.str("error: the tape REST API can only be enabled or disabled on the default space");
      } else {
        if (!space->SetConfigMember(key, value)) {
          ret_c = EIO;
          std_err.str("error: cannot set space config value");
        } else {
          auto config = gOFS->mRestApiManager->getTapeRestApiConfig();

          if (value == "on") {
            if (!config->isActivated()) {
              // Stage should be deactivated by default
              if (!space->SetConfigMember(rest::TAPE_REST_API_STAGE_SWITCH_ON_OFF, "off")) {
                ret_c = EIO;
                std_err.str("error: cannot set space config value");
              } else {
                config->setActivated(true);
                config->setStageEnabled(false);
                std_out << "success: Tape REST API enabled";
              }
            } else {
              std_out << "The tape REST API is already enabled";
            }
          } else {
            //Switch off the tape REST API
            //Also switch off the STAGE resource
            if (!space->SetConfigMember(
                  rest::TAPE_REST_API_STAGE_SWITCH_ON_OFF, "off")) {
              ret_c = EIO;
              std_err.str("error: cannot set space config value");
            } else {
              config->setActivated(false);
              config->setStageEnabled(false);
              std_out << "success: Tape REST API disabled";
            }
          }
        }
      }
    }
  }

  if (!strcmp(mgm::rest::TAPE_REST_API_STAGE_SWITCH_ON_OFF, key.c_str())) {
    applied = true;

    //REST API activation
    if ((value != "on") && (value != "off")) {
      ret_c = EINVAL;
      std_err.str("error: value has to either on or off");
    } else {
      if (space_name != "default") {
        ret_c = EIO;
        std_err.str("error: the tape REST API STAGE resource can only be enabled or disabled on the default space");
      } else {
        if (!space
            ->SetConfigMember(key, value)) {
          ret_c = EIO;
          std_err.str("error: cannot set space config value");
        } else {
          if (value == "on") {
            gOFS->mRestApiManager->getTapeRestApiConfig()->setStageEnabled(true);
            std_out << "success: Tape REST API STAGE resource enabled";
          } else {
            gOFS->mRestApiManager->getTapeRestApiConfig()->setStageEnabled(false);
            std_out << "success: Tape REST API STAGE resource disabled";
          }
        }
      }
    }
  }

  // set a space related parameter
  if (!key.compare(0, 6, "space.")) {
    key.erase(0, 6);

    if (config.remove()) {
      if (!space->DeleteConfigMember(key)) {
        ret_c = ENOENT;
        std_err.str("error: key has not been deleted");
      } else {
        std_out.str("success: removed space config '" + key + "'\n");
      }

      if (key.substr(0,9) == std::string("attr.sys.")) {
	// remove attribute in gOFS map
	std::unique_lock<std::mutex> lock(gOFS->mSpaceAttributesMutex);
	std::string mkey=key.substr(5);
	gOFS->mSpaceAttributes[space_name].erase(mkey);
      }

      reply.set_std_out(std_out.str());
      reply.set_std_err(std_err.str());
      reply.set_retc(ret_c);
      return;
    }

    if (eos::common::startsWith(key, "policy.") ||
        eos::common::startsWith(key, "local.policy.")) {
      if (value == "remove") {
        applied = true;

        if ((key == "policy.recycle")) {
          gOFS->enforceRecycleBin = false;
        }

        if (!space->DeleteConfigMember(key)) {
          ret_c = ENOENT;
          std_err.str("error: key has not been deleted");
        } else {
          std_out.str("success: removed space policy '" + key + "'\n");
        }
      } else {
        applied = true;

        // set a space policy parameters e.g. default placement attributes
        if (!space->SetConfigMember(key, value)) {
          std_err.str("error: cannot set space config value");
          ret_c = EIO;
        } else {
          std_out.str("success: configured policy in space='" + space_name +
                      "' as " + key + "='" + value + "'\n");
          ret_c = 0;
        }

        if ((key == "policy.recycle")) {
          if (value == "on") {
            gOFS->enforceRecycleBin = true;
          } else {
            gOFS->enforceRecycleBin = false;
          }
        }
      }
    } else if (key == eos::mgm::tgc::TGC_NAME_FREE_BYTES_SCRIPT) {
      applied = true;

      if (!space->SetConfigMember(key, value)) {
        std_err.str("error: cannot set space config value");
        ret_c = EIO;
      } else {
        std_out.str("success: configured policy in space='" + space_name +
                    "' as " + key + "='" + value + "'\n");
        ret_c = 0;
      }
    } else if (key == "groupbalancer.engine") {
      applied = true;

      if (GroupBalancer::is_valid_engine(value)) {
        if (!space->SetConfigMember(key, value)) {
          std_err.str("error: cannot set space config value");
          ret_c = EIO;
        } else {
          std_out.str("success: configured groupbalancer.engine in space='" +
                      space_name + "' as " + key + "='" + value + "'\n");
          ret_c = 0;
        }
      } else {
        std_err.str("error: invalid groupbalancer engine name");
        ret_c = EINVAL;
      }
    } else if (key == "groupbalancer.blocklist") {
      if (!space->SetConfigMember(key, value)) {
        std_err.str("error: cannot set space config value");
        ret_c = EIO;
      } else {
        space->mGroupBalancer->reconfigure();
        applied = true;
        std_out.str("success: updated " + key + "in space='" +
                    space_name + "' as " + value + "'\n");
        ret_c = 0;
      }
    } else if (key == "scheduler.type") {
      if (!space->SetConfigMember(key, value)) {
        std_err.str("error: cannot set space config value");
        ret_c = EIO;
      } else {
        applied = true;
        gOFS->mFsScheduler->setPlacementStrategy(space->mName, value);
        std_out.str("success: configured scheduler.type in space='" +
                    space_name + "' as " + value + "\n");
        ret_c = 0;
      }
    } else if (!key.compare(0, 5, "atime")) {
      applied = true;

      if (!space->SetConfigMember(key, value)) {
        ret_c = EIO;
        std_err.str("error: cannot set spbpace config value");
      } else {
        std_out.str("success: defining space acces time tracking: " + key + "=" +
                    value);
      }
    } else {
      if ((key == "nominalsize") ||
          (key == "headroom") ||
          (key == "graceperiod") ||
          (key == "drainperiod") ||
          (key == "balancer") ||
          (key == "balancer.threshold") ||
          (key == "balancer.node.rate") ||
          (key == "balancer.node.ntx") ||
          (key == "balancer.max-queue-jobs") ||
          (key == "balancer.max-thread-pool-size") ||
          (key == "balancer.update.interval") ||
          (key == "drainer.tx.minrate") ||
          (key == "drainer.retries") ||
          (key == "drainer.fs.ntx") ||
          (key == "tracker") ||
          (key == "inspector") ||
          (key == "inspector.interval") ||
          (key == "inspector.price.disk.tbyear") ||
          (key == "inspector.price.tape.tbyear") ||
          (key == "inspector.price.currency") ||
          (key == "lru") ||
          (key == "lru.interval") ||
          (key == "wfe") ||
          (key == "wfe.interval") ||
          (key == "wfe.ntx") ||
          (key == "groupbalancer") ||
          (key == "groupbalancer.ntx") ||
          (key == "groupbalancer.threshold") ||
          (key == "groupbalancer.min_threshold") ||
          (key == "groupbalancer.max_threshold") ||
          (key == "groupbalancer.min_file_size") ||
          (key == "groupbalancer.max_file_size") ||
          (key == "groupbalancer.file_attempts") ||
          (key == "geobalancer") ||
          (key == "geobalancer.ntx") ||
          (key == "geobalancer.threshold") ||
          (key == "groupdrainer") ||
          (key == "groupdrainer.threshold") ||
          (key == "groupdrainer.group_refresh_interval") ||
          (key == "groupdrainer.retry_interval") ||
          (key == "groupdrainer.retry_count") ||
          (key == "groupdrainer.ntx") ||
          (key == "geo.access.policy.read.exact") ||
          (key == "geo.access.policy.write.exact") ||
          (key == "filearchivedgc") ||
          (key == "max.ropen") ||
          (key == "max.wopen") ||
          (key == eos::mgm::tgc::TGC_NAME_QRY_PERIOD_SECS) ||
          (key == eos::mgm::tgc::TGC_NAME_AVAIL_BYTES) ||
          (key == eos::mgm::tgc::TGC_NAME_TOTAL_BYTES) ||
          (key == "token.generation") ||
          (key == eos::common::SCAN_IO_RATE_NAME) ||
          (key == eos::common::SCAN_ENTRY_INTERVAL_NAME) ||
          (key == eos::common::SCAN_RAIN_ENTRY_INTERVAL_NAME) ||
          (key == eos::common::SCAN_DISK_INTERVAL_NAME) ||
          (key == eos::common::SCAN_NS_INTERVAL_NAME) ||
          (key == eos::common::SCAN_NS_RATE_NAME) ||
	  (key.substr(0,9) == std::string("attr.sys."))) {
        if ((key == "balancer") ||
            (key == "tracker") ||
            (key == "inspector") ||
            (key == "lru") ||
            (key == "groupbalancer") ||
            (key == "geobalancer") ||
            (key == "geo.access.policy.read.exact") ||
            (key == "geo.access.policy.write.exact") ||
            (key == "filearchivedgc") ||
            (key == "groupdrainer")) {
          applied = true;

          if ((value != "on") && (value != "off")) {
            ret_c = EINVAL;
            std_err.str("error: value has to either on or off");
          } else {
            if (!space->SetConfigMember(key, value)) {
              ret_c = EIO;
              std_err.str("error: cannot set space config value");
            } else {
              if (key == "balancer") {
                if (space->mFsBalancer) {
                  if (value == "on") {
                    std_out << "success: (fs) balancer is enabled!";
                  } else {
                    std_out << "success: (fs) balancer is disabled!";
                  }

                  if (space->mFsBalancer) {
                    space->mFsBalancer->SignalConfigUpdate();
                  }
                } else {
                  std_err.str("error: (fs) balancer not initialized for space");
                  ret_c = EIO;
                }
              }

              if (key == "tracker") {
                if (value == "on") {
                  gOFS->mReplicationTracker->enable();
                  std_out << "success: tracker is enabled!";
                } else {
                  gOFS->mReplicationTracker->disable();
                  std_out << "success: tracker is disabled!";
                }
              }

              if (key == "inspector") {
                if (space->mFileInspector) {
                  if (value == "on") {
                    space->mFileInspector->enable();
                    std_out << "success: file inspector is enabled!";
                  } else {
                    space->mFileInspector->disable();
                    std_out << "success: file inspector is disabled!";
                  }
                } else {
                  std_err.str("error: no inspector for space");
                  ret_c = EINVAL;
                }
              }

              if (key == "groupbalancer") {
                if (space->mGroupBalancer) {
                  if (value == "on") {
                    std_out << "success: groupbalancer is enabled!";
                  } else {
                    std_out << "success: groupbalancer is disabled!";
                  }

                  space->mGroupBalancer->reconfigure();
                } else {
                  std_err.str("error: group balancer not initialized for space");
                  ret_c = EIO;
                }
              }

              if (key == "geobalancer") {
                if (space->mGeoBalancer) {
                  if (value == "on") {
                    std_out << "success: geobalancer is enabled!";
                  } else {
                    std_out << "success: geobalancer is disabled!";
                  }
                } else {
                  std_err.str("error: geo balancer not initialized for space");
                  ret_c = EIO;
                }
              }

              if (key == "groupdrainer") {
                if (space->mGroupDrainer) {
                  if (value == "on") {
                    std_out << "success: groupdrainer is enabled!";
                  } else {
                    std_out << "success: groupdrainer is disabled!";
                  }

                  space->mGroupDrainer->reconfigure();
                } else {
                  std_err.str("error: group drainer not initialized for space");
                  ret_c = EIO;
                }
              }

              if (key == "geo.access.policy.read.exact") {
                if (value == "on") {
                  std_out <<
                          "success: geo access policy prefers the exact geo matching replica for reading!";
                } else {
                  std_out <<
                          "success: geo access policy prefers with a weight the geo matching replica for reading!";
                }
              }

              if (key == "geo.access.policy.write.exact") {
                if (value == "on") {
                  std_out <<
                          "success: geo access policy prefers the exact geo matching replica for placements!";
                } else {
                  std_out <<
                          "success: geo access policy prefers with a weight the geo matching replica for placements!";
                }
              }

              if (key == "scheduler.skip.overloaded") {
                if (value == "on") {
                  std_out << "success: scheduler skips overloaded eth-out nodes!";
                } else {
                  std_out << "success: scheduler does not skip overloaded eth-out nodes!";
                }
              }

              if (key == "filearchivedgc") {
                if (value == "on") {
                  std_out << "success: 'file archived' garbage collector is enabled";
                } else {
                  std_out << "success: 'file archived' garbage collector is disabled";
                }
              }

              if (key == "lru") {
                std_out << ((value == "on") ? "success: LRU is enabled" :
                            "success: LRU is disabled");
                gOFS->mLRUEngine->RefreshOptions();
              }
            }
          }
        } else if (key == "wfe") {
          applied = true;

          if ((value != "on") && (value != "off") && (value != "paused")) {
            ret_c = EINVAL;
            std_err.str("error: value has to either on, paused or off");
          } else {
            if (!space->SetConfigMember(key, value)) {
              ret_c = EIO;
              std_err.str("error: cannot set space config value");
            } else {
              std::string status = (value == "on") ? "enabled" :
                                   (value == "off" ? "disabled" : "paused");
              std_out << "success: wfe is " << status << "!";
            }
          }
        } else if (value == "remove") {
	  applied = true;
	  
	  if (key.substr(0,9) == std::string("attr.sys.")) {
	    // remove attribute in gOFS map
	    std::unique_lock<std::mutex> lock(gOFS->mSpaceAttributesMutex);
	    std::string mkey=key.substr(5);
	    gOFS->mSpaceAttributes[space_name].erase(mkey);
	  }
	  
	  if (!space->DeleteConfigMember(key)) {
	    ret_c = ENOENT;
	    std_err.str("error: key has not been deleted");
	  } else {
	    std_out.str("success: deleted space config : " + key);
	  }
	} else if (key.substr(0,9) == std::string("attr.sys.")) {
	  if (key == "attr.sys.acl") {
	    // screen if this is a valid ACL
	    Acl acl;
	    std::string scal = value;
	    bool replace=true;
	    if ( value.front() == '>' ||
		 value.front() == '<' ||
	         value.front() == '|' ){
	      scal.erase(0,1);
	      replace=false;
	    }
	    XrdOucErrInfo error;
	    if (!acl.IsValid(scal, error, true, false) &&
		!acl.IsValid(scal, error, true, true) ) {
	      ret_c = EINVAL;
	      std_err.str("error: the ACL is not valid");
	      reply.set_std_out(std_out.str());
	      reply.set_std_err(std_err.str());
	      reply.set_retc(ret_c);
	      return;
	    } else {
	      if (Acl::ConvertIds(scal)) {
		ret_c = EINVAL;
		std_err.str("error: cannot convert to numerical IDs");
		reply.set_std_out(std_out.str());
		reply.set_std_err(std_err.str());
		reply.set_retc(ret_c);
		return;
	      }
	      if (!replace) {
		value.erase(1);
		value += scal;
	      } else {
		value = scal;
	      }
	      std_out.str("success: setting " + key + "=" + value);
	    }
	  }
	  {
	    // set attribute in gOFS map
	    std::unique_lock<std::mutex> lock(gOFS->mSpaceAttributesMutex);
	    std::string mkey=key.substr(5);
	    gOFS->mSpaceAttributes[space_name][mkey] = value;
	  }
	  applied = true;
	  // setting space attributes
	  if (!space->SetConfigMember(key, value)) {
	    ret_c = EIO;
	    std_err.str("error: cannot set space config value");
	  } else {
	    std_out.str("success: setting " + key + "=" + value);
	  }
	} else {
	  errno = 0;
	  applied = true;
	  unsigned long long size = eos::common::StringConversion::GetSizeFromString(
										     value.c_str());
	  
	  if (!errno) {
	    if ((key != "balancer.threshold") &&
		(key != "geobalancer.threshold") &&
		(key != "groupbalancer.threshold") &&
		(key != "groupbalancer.min_threshold") &&
		(key != "groupbalancer.max_threshold") &&
		(key != "groupdrainer.threshold")) {
	      // Threshold is allowed to be decimal!
	      char ssize[1024];
	      snprintf(ssize, sizeof(ssize) - 1, "%llu", size);
	      value = ssize;
	    }
	    
	    if (!space->SetConfigMember(key, value)) {
	      ret_c = EIO;
	      std_err.str("error: cannot set space config value");
	    } else {
	      std_out.str("success: setting " + key + "=" + value);
	      
	      if ((key == "token.generation")) {
		eos::common::EosTok::sTokenGeneration = strtoull(value.c_str(), 0, 0);
	      }
	      
	      if (key == "lru.interval") {
		gOFS->mLRUEngine->RefreshOptions();
	      }
	      
	      if (eos::common::startsWith(key, GROUPBALANCER_KEY_PREFIX)) {
		space->mGroupBalancer->reconfigure();
	      } else if (eos::common::startsWith(key, GROUPDRAINER_KEY_PREFIX)) {
		space->mGroupDrainer->reconfigure();
	      } else if (eos::common::startsWith(key, BALANCER_KEY_PREFIX)) {
		if (space->mFsBalancer) {
		  space->mFsBalancer->SignalConfigUpdate();
		}
	      }
	    }
	  } else {
	    ret_c = EINVAL;
	    std_err.str("error: value has to be a positive number");
	  }
	}
      }
    }
  }

  // Set a filesystem related parameter
  if (!key.compare(0, 3, "fs.")) {
    applied = true;
    key.erase(0, 3);
    // we disable the autosave, do all the updates and then switch back
    // to autosave and evt. save all changes
    gOFS->mConfigEngine->SetAutoSave(false);

    // Store these as a global parameters of the space
    if ((key == "headroom") || (key == "graceperiod") || (key == "drainperiod") ||
        (key == "max.ropen") || (key == "max.wopen") ||
        (key == eos::common::SCAN_IO_RATE_NAME) ||
        (key == eos::common::SCAN_ENTRY_INTERVAL_NAME) ||
        (key == eos::common::SCAN_RAIN_ENTRY_INTERVAL_NAME) ||
        (key == eos::common::SCAN_DISK_INTERVAL_NAME) ||
        (key == eos::common::SCAN_NS_INTERVAL_NAME) ||
        (key == eos::common::SCAN_NS_RATE_NAME)) {
      unsigned long long size = eos::common::StringConversion::GetSizeFromString(
                                  value.c_str());
      char ssize[1024];
      snprintf(ssize, sizeof(ssize) - 1, "%llu", size);

      if (value == "remove") {
        if (!space->DeleteConfigMember(key)) {
          ret_c = ENOENT;
        } else {
          std_out.str("success: deleting " + key);
        }
      } else {
        if ((!space->SetConfigMember(key, ssize))) {
          std_err << "error: failed to set space parameter <" + key + ">\n";
          ret_c = EINVAL;
        } else {
          std_out.str("success: setting " + key + "=" + value);
        }
      }
    } else {
      if (key != "configstatus") {
        std_err << "error: not an allowed parameter <" + key + ">\n";
        ret_c = EINVAL;
      }
    }

    for (auto it = space->begin(); it != space->end(); ++it) {
      fs = FsView::gFsView.mIdView.lookupByID(*it);

      if (fs) {
        // check the allowed strings
        if (((key == "configstatus") &&
             (eos::common::FileSystem::GetConfigStatusFromString(value.c_str()) !=
              eos::common::ConfigStatus::kUnknown))) {
          fs->SetString(key.c_str(), value.c_str());
          FsView::gFsView.StoreFsConfig(fs, false);
        } else {
          errno = 0;
          eos::common::StringConversion::GetSizeFromString(value.c_str());

          if (((key == "headroom") || (key == "graceperiod") || (key == "drainperiod") ||
               (key == "max.ropen") || (key == "max.wopen") ||
               (key == eos::common::SCAN_IO_RATE_NAME) ||
               (key == eos::common::SCAN_ENTRY_INTERVAL_NAME) ||
               (key == eos::common::SCAN_RAIN_ENTRY_INTERVAL_NAME) ||
               (key == eos::common::SCAN_DISK_INTERVAL_NAME) ||
               (key == eos::common::SCAN_NS_INTERVAL_NAME) ||
               (key == eos::common::SCAN_NS_RATE_NAME)) && (!errno)) {
            if (value == "remove") {
              fs->RemoveKey(key.c_str());
            } else {
              fs->SetLongLong(key.c_str(),
                              eos::common::StringConversion::GetSizeFromString(value.c_str()));
            }

            FsView::gFsView.StoreFsConfig(fs, false);
          } else {
            std_err << "error: not an allowed parameter <" + key + ">\n";
            ret_c = EINVAL;
            break;
          }
        }
      } else {
        std_err << "error: cannot identify the filesystem by <" + space_name
                + ">\n";
        ret_c = EINVAL;
      }
    }

    gOFS->mConfigEngine->SetAutoSave(true);
    gOFS->mConfigEngine->AutoSave();
  }

  if (!applied) {
    ret_c = EINVAL;
    std_err.str("error: unknown parameter <" + key +
                "> - probably need to prefix with 'space.' or 'fs.'\n");
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
  reply.set_retc(ret_c);
}

//----------------------------------------------------------------------------
// Execute quota subcommand
//----------------------------------------------------------------------------
void SpaceCmd::QuotaSubcmd(const eos::console::SpaceProto_QuotaProto& quota,
                           eos::console::ReplyProto& reply)
{
  std::string key = "quota";
  std::string onoff = (quota.quota_switch()) ? "on" : "off" ;

  if (mVid.uid != 0) {
    reply.set_std_err("error: you have to take role 'root' to execute this command");
    reply.set_retc(EPERM);
    return;
  }

  if (quota.mgmspace().empty()) {
    reply.set_std_err("error: illegal parameters");
    reply.set_retc(EINVAL);
    return;
  }

  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mSpaceView.count(quota.mgmspace())) {
    if (!FsView::gFsView.mSpaceView[quota.mgmspace()]->SetConfigMember(key,
        onoff)) {
      reply.set_std_err("error: cannot set space config value");
      reply.set_retc(EIO);
    }
  } else {
    reply.set_std_err("error: no such space defined");
    reply.set_retc(EINVAL);
  }
}

//----------------------------------------------------------------------------
// Execute rm subcommand
//----------------------------------------------------------------------------
void SpaceCmd::RmSubcmd(const eos::console::SpaceProto_RmProto& rm,
                        eos::console::ReplyProto& reply)
{
  if (mVid.uid != 0) {
    reply.set_std_err("error: you have to take role 'root' to execute this command");
    reply.set_retc(EPERM);
    return;
  }

  if (rm.mgmspace().empty()) {
    reply.set_std_err("error: illegal parameters");
    reply.set_retc(EINVAL);
    return;
  }

  eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);

  if (!FsView::gFsView.mSpaceView.count(rm.mgmspace())) {
    reply.set_std_err("error: no such space '" + rm.mgmspace() + "'");
    reply.set_retc(ENOENT);
    return;
  }

  for (auto it = FsView::gFsView.mSpaceView[rm.mgmspace()]->begin();
       it != FsView::gFsView.mSpaceView[rm.mgmspace()]->end(); it++) {
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it);

    if (fs) {
      // check that filesystems are empty
      if ((fs->GetConfigStatus(false) != eos::common::ConfigStatus::kEmpty)) {
        reply.set_std_err("error: unable to remove space '" + rm.mgmspace() +
                          "' - filesystems are not all in empty state - try to drain them or: space config <name> configstatus=empty\n");
        reply.set_retc(EBUSY);
        return;
      }
    }
  }

  common::SharedHashLocator spaceLocator =
    common::SharedHashLocator::makeForSpace(rm.mgmspace());

  if (!mq::SharedHashWrapper::deleteHash(gOFS->mMessagingRealm.get(),
                                         spaceLocator)) {
    reply.set_std_err("error: unable to remove config of space '" + rm.mgmspace() +
                      "'");
    reply.set_retc(EIO);
  } else {
    if (FsView::gFsView.UnRegisterSpace(rm.mgmspace().c_str())) {
      reply.set_std_out("success: removed space '" + rm.mgmspace() + "'");
    } else {
      reply.set_std_err("error: unable to unregister space '" + rm.mgmspace() + "'");
    }
  }
}

//----------------------------------------------------------------------------
// Execute tracker subcommand
//----------------------------------------------------------------------------
void SpaceCmd::TrackerSubcmd(const eos::console::SpaceProto_TrackerProto&
                             tracker, eos::console::ReplyProto& reply)
{
  std::ostringstream std_out;
  std::string tmp;
  gOFS->mReplicationTracker->Scan(2 * 86400, false, &tmp);
  std_out <<
          "# ------------------------------------------------------------------------------------\n";
  std_out << tmp;
  std_out <<
          "# ------------------------------------------------------------------------------------\n";
  reply.set_std_out(std_out.str());
  reply.set_retc(0);
}

//----------------------------------------------------------------------------
// Execute inspector subcommand
//----------------------------------------------------------------------------
void SpaceCmd::InspectorSubcmd(const eos::console::SpaceProto_InspectorProto&
                               inspector, eos::console::ReplyProto& reply)
{
  std::string_view options = inspector.options();
  std::string std_out;
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  auto space_it = FsView::gFsView.mSpaceView.find(inspector.mgmspace());

  if (space_it != FsView::gFsView.mSpaceView.end()) {
    space_it->second->mFileInspector->Dump(std_out, options,
                                           FileInspector::LockFsView::Off);
    reply.set_std_out(std_out);
    reply.set_retc(0);
  } else {
    reply.set_std_err("error: no such space");
    reply.set_retc(EINVAL);
  }
}

//----------------------------------------------------------------------------
// Execute group balancer subcommand
//----------------------------------------------------------------------------
void
SpaceCmd::GroupBalancerSubCmd(const eos::console::SpaceProto_GroupBalancerProto&
                              groupbalancer,
                              eos::console::ReplyProto& reply)
{
  if (groupbalancer.mgmspace().empty()) {
    reply.set_std_err("error: A spacename is needed for this cmd");
    reply.set_retc(EINVAL);
    return;
  }

  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  auto space_it = FsView::gFsView.mSpaceView.find(groupbalancer.mgmspace());

  if (space_it == FsView::gFsView.mSpaceView.end()) {
    reply.set_std_err("error: No such space exists!");
    reply.set_retc(EINVAL);
    return;
  }

  const auto fs_space = space_it->second;

  switch (groupbalancer.cmd_case()) {
  case eos::console::SpaceProto_GroupBalancerProto::kStatus:
    GroupBalancerStatusCmd(groupbalancer.status(), reply, fs_space);
    break;

  default:
    reply.set_std_err("error: not supported");
    reply.set_retc(EINVAL);
  }
}

//----------------------------------------------------------------------------
// Execute group balancer status subcommand
//----------------------------------------------------------------------------
void SpaceCmd::GroupBalancerStatusCmd(const
                                      eos::console::SpaceProto_GroupBalancerStatusProto& status,
                                      eos::console::ReplyProto& reply,
                                      FsSpace* const fs_space)
{
  if (fs_space == nullptr || fs_space->mGroupBalancer == nullptr) {
    reply.set_std_err("Invalid space/GroupBalancer config");
    reply.set_retc(EINVAL);
    return;
  }

  bool monitoring = status.options().find('m') != std::string::npos;
  bool detail = status.options().find('d') != std::string::npos;
  reply.set_std_out(fs_space->mGroupBalancer->Status(detail, monitoring));
  reply.set_retc(0);
}

//----------------------------------------------------------------------------
// Execute group drainer status subcommand
//----------------------------------------------------------------------------
void
SpaceCmd::GroupDrainerSubCmd(const eos::console::SpaceProto_GroupDrainerProto&
                             groupdrainer,
                             console::ReplyProto& reply)
{
  if (groupdrainer.mgmspace().empty()) {
    reply.set_std_err("error: A spacename is needed for this cmd");
    reply.set_retc(EINVAL);
    return;
  }

  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  auto space_it = FsView::gFsView.mSpaceView.find(groupdrainer.mgmspace());

  if (space_it == FsView::gFsView.mSpaceView.end()) {
    reply.set_std_err("error: No such space exists!");
    reply.set_retc(EINVAL);
    return;
  }

  const auto fs_space = space_it->second;

  if (!fs_space->mGroupDrainer) {
    reply.set_std_out("GroupDrainer not enabled or is configuring!");
    reply.set_retc(EIO);
    return;
  }

  switch (groupdrainer.cmd_case()) {
  case eos::console::SpaceProto_GroupDrainerProto::kStatus:
    switch (groupdrainer.status().outformat()) {
    case eos::console::SpaceProto::GroupDrainerStatusProto::MONITORING:
      reply.set_std_out(fs_space->mGroupDrainer->getStatus(
                          GroupDrainer::StatusFormat::MONITORING));
      break;

    case eos::console::SpaceProto::GroupDrainerStatusProto::DETAIL:
      reply.set_std_out(fs_space->mGroupDrainer->getStatus(
                          GroupDrainer::StatusFormat::DETAIL));
      break;

    default:
      reply.set_std_out(fs_space->mGroupDrainer->getStatus(
                          GroupDrainer::StatusFormat::NONE));
    }

    break;

  case eos::console::SpaceProto_GroupDrainerProto::kReset:
    switch (groupdrainer.reset().option()) {
    case eos::console::SpaceProto::GroupDrainerResetProto::FAILED:
      fs_space->mGroupDrainer->resetFailedTransfers();
      reply.set_std_out("Done resetting all failed transfers!");
      break;

    case eos::console::SpaceProto::GroupDrainerResetProto::ALL:
      fs_space->mGroupDrainer->resetCaches();
      reply.set_std_out("Done clearing all GroupDrainer caches!");
      break;

    default:
      reply.set_std_out("Unknown option!");
      reply.set_retc(EINVAL);
      return;
    }

    break;

  default:
    reply.set_std_err("Unknown option!");
    reply.set_retc(EINVAL);
    return;
  }

  reply.set_retc(0);
}

EOSMGMNAMESPACE_END
