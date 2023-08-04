//------------------------------------------------------------------------------
// @file: NodeCmd.cc
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

#include "NodeCmd.hh"
#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/config/IConfigEngine.hh"
#include "mq/MessagingRealm.hh"
#include "namespace/interface/IFsView.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Method implementing the specific behavior of the command executed by the
// asynchronous thread
//------------------------------------------------------------------------------
eos::console::ReplyProto
NodeCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::NodeProto node = mReqProto.node();

  switch (mReqProto.node().subcmd_case()) {
  case eos::console::NodeProto::kLs:
    LsSubcmd(node.ls(), reply);
    break;

  case eos::console::NodeProto::kRm:
    RmSubcmd(node.rm(), reply);
    break;

  case eos::console::NodeProto::kStatus:
    StatusSubcmd(node.status(), reply);
    break;

  case eos::console::NodeProto::kConfig:
    ConfigSubcmd(node.config(), reply);
    break;

  //@todo(esindril) this should be removed as it's not implemented
  case eos::console::NodeProto::kRegisterx:
    RegisterSubcmd(node.registerx(), reply);
    break;

  case eos::console::NodeProto::kSet:
    SetSubcmd(node.set(), reply);
    break;

  case eos::console::NodeProto::kTxgw:
    TxgwSubcmd(node.txgw(), reply);
    break;

  case eos::console::NodeProto::kProxygroup:
    ProxygroupSubcmd(node.proxygroup(), reply);
    break;

  default:
    reply.set_std_err("error: not supported");
    reply.set_retc(EINVAL);
  }

  return reply;
}

//------------------------------------------------------------------------------
// Execute ls subcommand
//------------------------------------------------------------------------------
void NodeCmd::LsSubcmd(const eos::console::NodeProto_LsProto& ls,
                       eos::console::ReplyProto& reply)
{
  using eos::console::NodeProto;
  bool json_output = false;
  std::string list_format;
  std::string format;
  auto format_case = ls.outformat();

  if ((format_case == NodeProto::LsProto::NONE) && WantsJsonOutput()) {
    format_case = NodeProto::LsProto::MONITORING;
  }

  switch (format_case) {
  case NodeProto::LsProto::LISTING:
    format = FsView::GetNodeFormat("l");
    list_format = FsView::GetFileSystemFormat("l");
    break;

  case NodeProto::LsProto::MONITORING:
    format = FsView::GetNodeFormat("m");
    json_output = WantsJsonOutput();
    break;

  case NodeProto::LsProto::IO:
    format = FsView::GetNodeFormat("io");
    break;

  case NodeProto::LsProto::SYS:
    format = FsView::GetNodeFormat("sys");
    break;

  case NodeProto::LsProto::FSCK:
    format = FsView::GetNodeFormat("fsck");
    break;

  default : // NONE
    format = FsView::GetNodeFormat("");
    break;
  }

  if (!ls.outhost()) {
    if (format.find('S') != std::string::npos) {
      format.replace(format.find('S'), 1, "s");
    }

    if (list_format.find('S') != std::string::npos) {
      list_format.replace(list_format.find('S'), 1, "s");
    }
  }

  std::string output;
  eos::common::RWMutexReadLock rd_lock(FsView::gFsView.ViewMutex);
  FsView::gFsView.PrintNodes(output, format, list_format, 0,
                             ls.selection().c_str(), mReqProto.dontcolor());

  if (json_output) {
    output = ResponseToJsonString(output);
  }

  reply.set_std_out(output);
  reply.set_retc(0);
}

//------------------------------------------------------------------------------
// Execute rm subcommand
//------------------------------------------------------------------------------
void NodeCmd::RmSubcmd(const eos::console::NodeProto_RmProto& rm,
                       eos::console::ReplyProto& reply)
{
  if (mVid.uid != 0 && mVid.prot != "sss") {
    reply.set_std_err("error: you have to take role 'root' to execute this command");
    reply.set_retc(EPERM);
    return;
  }

  if (rm.node().empty()) {
    reply.set_std_err("error: illegal parameter 'node'");
    reply.set_retc(EINVAL);
    return;
  }

  std::string nodename = rm.node();

  if ((nodename.find(':') == std::string::npos)) {
    nodename += ":1095"; // default eos fst port
  }

  if ((nodename.find("/eos/") == std::string::npos)) {
    nodename.insert(0, "/eos/");
    nodename.append("/fst");
  }

  eos::common::RWMutexWriteLock wr_lock(FsView::gFsView.ViewMutex);

  if (!FsView::gFsView.mNodeView.count(nodename)) {
    reply.set_std_err("error: no such node '" + nodename + "'");
    reply.set_retc(ENOENT);
    return;
  }

  // Remove a node only if it has no heartbeat anymore
  if ((time(nullptr) - FsView::gFsView.mNodeView[nodename]->GetHeartBeat()) < 5) {
    reply.set_std_err("error: this node was still sending a heartbeat < 5 "
                      "seconds ago - stop the FST daemon first!");
    reply.set_retc(EBUSY);
    return;
  }

  // Remove a node only if all filesystems are in empty state
  for (auto it = FsView::gFsView.mNodeView[nodename]->begin();
       it != FsView::gFsView.mNodeView[nodename]->end(); ++it) {
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it);

    if (fs) {
      // check the empty state
      if ((fs->GetConfigStatus(false) != eos::common::ConfigStatus::kEmpty)) {
        reply.set_std_err("error: unable to remove node '" + nodename +
                          "' - filesystems are not all in empty state");
        reply.set_retc(EBUSY);
        return;
      }
    }
  }

  common::SharedHashLocator nodeLocator = common::SharedHashLocator::makeForNode(
      nodename);

  if (!mq::SharedHashWrapper::deleteHash(gOFS->mMessagingRealm.get(),
                                         nodeLocator)) {
    reply.set_std_err("error: unable to remove config of node '" + nodename + "'");
    reply.set_retc(EIO);
  } else {
    if (FsView::gFsView.UnRegisterNode(nodename.c_str())) {
      reply.set_std_out("success: removed node '" + nodename + "'");
    } else {
      reply.set_std_err("error: unable to unregister node '" + nodename + "'");
    }
  }

  // Delete also the entry from the configuration
  eos_info("msg=\"delete from configuration\" node_name=%s",
           nodeLocator.getConfigQueue().c_str());
  gOFS->ConfEngine->DeleteConfigValueByMatch("global",
      nodeLocator.getConfigQueue().c_str());
  gOFS->ConfEngine->AutoSave();
}

//------------------------------------------------------------------------------
// Execute status subcommand
//------------------------------------------------------------------------------
void NodeCmd::StatusSubcmd(const eos::console::NodeProto_StatusProto& status,
                           eos::console::ReplyProto& reply)
{
  std::string nodename = status.node();

  if ((nodename.find(':') == std::string::npos)) {
    nodename += ":1095"; // default eos fst port
  }

  if ((nodename.find("/eos/") == std::string::npos)) {
    nodename.insert(0, "/eos/");
    nodename.append("/fst");
  }

  if (!FsView::gFsView.mNodeView.count(nodename)) {
    reply.set_std_err("error: cannot find node - no node with name '" + nodename +
                      "'");
    reply.set_retc(ENOENT);
    return;
  }

  eos::common::RWMutexWriteLock wr_lock(FsView::gFsView.ViewMutex);
  std::string std_out;
  std::vector<std::string> keylist;
  std_out +=
    "# ------------------------------------------------------------------------------------\n";
  std_out += "# Node Variables\n";
  std_out +=
    "# ....................................................................................\n";
  FsView::gFsView.mNodeView[nodename]->GetConfigKeys(keylist);
  std::sort(keylist.begin(), keylist.end());

  for (auto& i : keylist) {
    char line[2048];
    std::string val = FsView::gFsView.mNodeView[nodename]->GetConfigMember(i);

    if (val.substr(0, 7) == "base64:") {
      val = "base64:...";
    }

    if (val.length() > 1024) {
      val = "...";
    }

    snprintf(line, sizeof(line) - 1, "%-32s := %s\n", i.c_str(), val.c_str());
    std_out += line;
  }

  reply.set_std_out(std_out);
  reply.set_retc(0);
}

//------------------------------------------------------------------------------
// Execute config subcommand
//------------------------------------------------------------------------------
void NodeCmd::ConfigSubcmd(const eos::console::NodeProto_ConfigProto& config,
                           eos::console::ReplyProto& reply)
{
  if (mVid.uid != 0 && mVid.prot != "sss") {
    reply.set_std_err("error: you have to take role 'root' to execute this command");
    reply.set_retc(EPERM);
    return;
  }

  if (!config.node_name().length() ||
      !config.node_key().length() ||
      !config.node_value().length()) {
    reply.set_std_err("error: invalid parameters");
    reply.set_retc(EINVAL);
    return;
  }

  std::set<std::string> set_nodes;
  {
    // Collect the path of the nodes concerned
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

    if ((config.node_name().find('*') != std::string::npos)) {
      for (auto& it : FsView::gFsView.mNodeView) {
        set_nodes.insert(it.first);
      }
    } else {
      // by host:port name
      std::string path = config.node_name();

      if ((path.find(':') == std::string::npos)) {
        path += ":1095"; // default eos fst port
      }

      if ((path.find("/eos/") == std::string::npos)) {
        path.insert(0, "/eos/");
        path.append("/fst");
      }

      if (FsView::gFsView.mNodeView.count(path)) {
        set_nodes.insert(path);
      }
    }
  }

  if (set_nodes.empty()) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: cannot find node <" + config.node_name() + ">");
    return;
  }

  // Handle file system specific configurations
  if (config.node_key() == "configstatus") {
    return ConfigFsSpecific(set_nodes, config.node_key(), config.node_value(),
                            reply);
  }

  // Handle note specific configurations
  for (auto& node_path : set_nodes) {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    auto it = FsView::gFsView.mNodeView.find(node_path);

    if (it == FsView::gFsView.mNodeView.end()) {
      continue;
    }

    FsNode* node = it->second;

    if (config.node_key() == "gw.ntx") {
      int slots = std::stoi(config.node_value());

      if ((slots < 1) || (slots > 100)) {
        reply.set_std_err("error: number of gateway transfer slots must be between 1-100");
        reply.set_retc(EINVAL);
      } else {
        if (node->SetConfigMember(config.node_key(), config.node_value(), false)) {
          reply.set_std_out("success: number of gateway transfer slots set to gw.ntx=" +
                            std::to_string(slots));
        } else {
          reply.set_std_err("error: failed to store the config value gw.ntx");
          reply.set_retc(EFAULT);
        }
      }
    } else if (config.node_key() == "gw.rate") {
      int bw = std::stoi(config.node_value());

      if ((bw < 1) || (bw > 10000)) {
        reply.set_std_err("error: gateway transfer speed must be 1-10000 (MB/s)");
        reply.set_retc(EINVAL);
      } else {
        if (node->SetConfigMember(config.node_key(), config.node_value(), false)) {
          reply.set_std_out("success: gateway transfer rate set to gw.rate=" +
                            std::to_string(bw) + " Mb/s");
        } else {
          reply.set_std_err("error: failed to store the config value gw.rate");
          reply.set_retc(EFAULT);
        }
      }
    } else if (config.node_key() == "error.simulation") {
      if (node->SetConfigMember(config.node_key(), config.node_value(), false)) {
        reply.set_std_out("success: setting error simulation tag '" +
                          config.node_value() += "'");
      } else {
        reply.set_std_err("error: failed to store the error simulation tag");
        reply.set_retc(EFAULT);
      }
    } else if (config.node_key() == "publish.interval") {
      if (node->SetConfigMember(config.node_key(), config.node_value(), false)) {
        reply.set_std_out("success: setting publish interval to '" +
                          config.node_value() + "'");
      } else {
        reply.set_std_err("error: failed to store publish interval");
        reply.set_retc(EFAULT);
      }
    } else if (config.node_key() == "debug.level") {
      if (node->SetConfigMember(config.node_key(), config.node_value(), false)) {
        reply.set_std_out("success: setting debug level to '" +
                          config.node_value() + "'");
      } else {
        reply.set_std_err("error: failed to store debug level interval");
        reply.set_retc(EFAULT);
      }
    } else {
      reply.set_std_err("error: the specified key is not known - consult the "
                        "usage information of the command");
      reply.set_retc(EINVAL);
    }
  }
}

//----------------------------------------------------------------------------
// Execute config operation affecting the file system parameters
//----------------------------------------------------------------------------
void
NodeCmd::ConfigFsSpecific(const std::set<std::string>& nodes,
                          const std::string& key, const std::string& value,
                          eos::console::ReplyProto& reply)
{
  using eos::common::FileSystem;

  if ((FileSystem::GetConfigStatusFromString(value.c_str()) ==
       eos::common::ConfigStatus::kUnknown)) {
    reply.set_std_err("error: not an allowed parameter <" + value + ">");
    reply.set_retc(EINVAL);
    return;
  }

  for (const auto& node_path : nodes) {
    std::set<eos::common::FileSystem::fsid_t> fsids;
    {
      // Collect the list of file systems concerned
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      auto it = FsView::gFsView.mNodeView.find(node_path);

      if (it == FsView::gFsView.mNodeView.end()) {
        continue;
      }

      for (eos::common::FileSystem::fsid_t fsid : *it->second) {
        fsids.insert(fsid);
      }
    }

    for (auto fsid : fsids) {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      auto* fs = FsView::gFsView.mIdView.lookupByID(fsid);

      if (!fs) {
        continue;
      }

      if (value == "empty") {
        // Check if the file system is really empty
        if (gOFS->eosFsView->getNumFilesOnFs(fs->GetId())) {
          eos_static_info("msg=\"trying to set a file system that still "
                          "contains files to empty state\" fsid=%lu",
                          fs->GetId());
          reply.set_std_err("error: some file systems are not empty");
          reply.set_retc(EINVAL);
          break;
        }
      }

      fs->SetString(key.c_str(), value.c_str());
      FsView::gFsView.StoreFsConfig(fs, false);
    }

    gOFS->ConfEngine->AutoSave();
  }
}

//------------------------------------------------------------------------------
// Execute register subcommand
//------------------------------------------------------------------------------
void NodeCmd::RegisterSubcmd(const eos::console::NodeProto_RegisterProto&
                             registerx, eos::console::ReplyProto& reply)
{
  if (mVid.uid != 0 && mVid.prot != "sss") {
    reply.set_std_err("error: you have to take role 'root' to execute this command");
    reply.set_retc(EPERM);
    return;
  }

  if (!registerx.node_name().length() ||
      !registerx.node_path2register().length() ||
      !registerx.node_space2register().length()) {
    reply.set_std_err("error: invalid parameters");
    reply.set_retc(EINVAL);
    return;
  }

  std::string msgbody = eos::common::FileSystem::GetRegisterRequestString();
  msgbody += "&mgm.path2register=" + registerx.node_path2register();
  msgbody += "&mgm.space2register=" + registerx.node_space2register();

  if (registerx.node_force()) {
    msgbody += "&mgm.force=true";
  }

  if (registerx.node_root()) {
    msgbody += "&mgm.root=true";
  }

  std::string nodequeue = "/eos/" + registerx.node_name() + "/fst";
  mq::MessagingRealm::Response response =
    gOFS->mMessagingRealm->sendMessage("msg", msgbody, nodequeue);

  if (response.ok()) {
    reply.set_std_out("success: sent global register message to all fst nodes");
    reply.set_retc(0);
  } else {
    reply.set_std_err("error: could not send global fst register message!");
    reply.set_retc(EIO);
  }
}

//------------------------------------------------------------------------------
// Execute set subcommand
//------------------------------------------------------------------------------
void NodeCmd::SetSubcmd(const eos::console::NodeProto_SetProto& set,
                        eos::console::ReplyProto& reply)
{
  std::string nodename = set.node();
  const std::string& status = set.node_state_switch();
  std::string key = "status";

  if (!nodename.length() || !status.length()) {
    reply.set_std_err("error: illegal parameter");
    reply.set_retc(EINVAL);
    return;
  }

  if ((nodename.find(':') == std::string::npos)) {
    nodename += ":1095"; // default eos fst port
  }

  if ((nodename.find("/eos/") == std::string::npos)) {
    nodename.insert(0, "/eos/");
    nodename.append("/fst");
  }

  std::string tident = mVid.tident.c_str();
  std::string rnodename = nodename;
  {
    // for sss + node identification
    rnodename.erase(0, 5);
    size_t dpos;

    if ((dpos = rnodename.find(':')) != std::string::npos) {
      rnodename.erase(dpos);
    }

    if ((dpos = rnodename.find('.')) != std::string::npos) {
      rnodename.erase(dpos);
    }

    size_t addpos = 0;

    if ((addpos = tident.find('@')) != std::string::npos) {
      tident.erase(0, addpos + 1);
    }
  }
  eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);
  // If EOS_SKIP_SSS_HOSTNAME_MATCH env variable is set then we skip
  // the check below as this currently breaks the Kubernetes setup.
  bool skip_hostname_match = getenv("EOS_SKIP_SSS_HOSTNAME_MATCH") ? true : false;

  if (mVid.uid == 0 || mVid.prot == "sss") {
    if (mVid.uid != 0 && mVid.prot == "sss") {
      if (!skip_hostname_match &&
          tident.compare(0, tident.length(), rnodename, 0, tident.length())) {
        reply.set_std_err("error: nodes can only be configured as 'root' or by "
                          "connecting from the node itself using the sss protocol(1)");
        reply.set_retc(EPERM);
        return;
      }
    }
  } else {
    reply.set_std_err("error: nodes can only be configured as 'root' or by "
                      "connecting from the node itself using the sss protocol(2)");
    reply.set_retc(EPERM);
    return;
  }

  if (!FsView::gFsView.mNodeView.count(nodename)) {
    reply.set_std_out("info: creating node '" + nodename + "'");

    // reply.set_std_err("error: no such node '" + nodename + "'");
    // reply.set_retc(ENOENT);
    if (!FsView::gFsView.RegisterNode(nodename.c_str())) {
      reply.set_std_err("error: cannot register node <" + nodename + ">");
      reply.set_retc(EIO);
      return;
    }
  }

  if (!FsView::gFsView.mNodeView[nodename]->SetConfigMember(key, status)) {
    reply.set_std_err("error: cannot set node config value");
    reply.set_retc(EIO);
    return;
  }

  // set also the manager name
  if (!FsView::gFsView.mNodeView[nodename]->SetConfigMember("manager",
      gOFS->mMaster->GetMasterId(), true)) {
    reply.set_std_err("error: cannot set the manager name");
    reply.set_retc(EIO);
    return;
  }
}

//------------------------------------------------------------------------------
// Execute txgw subcommand
//------------------------------------------------------------------------------
void NodeCmd::TxgwSubcmd(const eos::console::NodeProto_TxgwProto& txgw,
                         eos::console::ReplyProto& reply)
{
  std::string nodename = txgw.node();
  const std::string& status = txgw.node_txgw_switch();
  std::string key = "txgw";

  if (!nodename.length() || !status.length()) {
    reply.set_std_err("error: illegal parameter");
    reply.set_retc(EINVAL);
    return;
  }

  if ((nodename.find(':') == std::string::npos)) {
    nodename += ":1095"; // default eos fst port
  }

  if ((nodename.find("/eos/") == std::string::npos)) {
    nodename.insert(0, "/eos/");
    nodename.append("/fst");
  }

  std::string tident = mVid.tident.c_str();
  std::string rnodename = nodename;
  {
    // for sss + node identification
    rnodename.erase(0, 5);
    size_t dpos;

    if ((dpos = rnodename.find(':')) != std::string::npos) {
      rnodename.erase(dpos);
    }

    if ((dpos = rnodename.find('.')) != std::string::npos) {
      rnodename.erase(dpos);
    }

    size_t addpos = 0;

    if ((addpos = tident.find('@')) != std::string::npos) {
      tident.erase(0, addpos + 1);
    }
  }
  eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);
  // If EOS_SKIP_SSS_HOSTNAME_MATCH env variable is set then we skip
  // the check below as this currently breaks the Kubernetes setup.
  bool skip_hostname_match = (getenv("EOS_SKIP_SSS_HOSTNAME_MATCH")) ? true :
                             false;

  if (mVid.uid == 0 || mVid.prot == "sss") {
    if (mVid.uid != 0 && mVid.prot == "sss") {
      if (!skip_hostname_match &&
          tident.compare(0, tident.length(), rnodename, 0, tident.length())) {
        reply.set_std_err("error: nodes can only be configured as 'root' or by "
                          "connecting from the node itself using the sss protocol(1)");
        reply.set_retc(EPERM);
        return;
      }
    }
  } else {
    reply.set_std_err("error: nodes can only be configured as 'root' or by "
                      "connecting from the node itself using the sss protocol(2)");
    reply.set_retc(EPERM);
    return;
  }

  if (!FsView::gFsView.mNodeView.count(nodename)) {
    reply.set_std_out("info: creating node '" + nodename + "'");

    if (!FsView::gFsView.RegisterNode(nodename.c_str())) {
      reply.set_std_err("error: cannot register node <" + nodename + ">");
      reply.set_retc(EIO);
      return;
    }
  }

  if (!FsView::gFsView.mNodeView[nodename]->SetConfigMember(key, status)) {
    reply.set_std_err("error: cannot set node config value");
    reply.set_retc(EIO);
    return;
  }

  // set also the manager name
  if (!FsView::gFsView.mNodeView[nodename]->SetConfigMember("manager",
      gOFS->mMaster->GetMasterId(), true)) {
    reply.set_std_err("error: cannot set the manager name");
    reply.set_retc(EIO);
    return;
  }
}

//------------------------------------------------------------------------------
// Execute proxygroup subcommand
//------------------------------------------------------------------------------
void NodeCmd::ProxygroupSubcmd(const eos::console::NodeProto_ProxygroupProto&
                               proxygroup, eos::console::ReplyProto& reply)
{
  std::string nodename = proxygroup.node();
  std::string status = (proxygroup.node_proxygroup().length()) ?
                       proxygroup.node_proxygroup() : "clear";
  std::string key = "proxygroup";
  eos::console::NodeProto_ProxygroupProto::Action action =
    proxygroup.node_action();

  if (status.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890._-")
      != std::string::npos) {
    status.clear();
  }

  if (!nodename.length() || !status.length()) {
    reply.set_std_err("error: illegal parameter");
    reply.set_retc(EINVAL);
    return;
  }

  if ((nodename.find(':') == std::string::npos)) {
    nodename += ":1095"; // default eos fst port
  }

  if ((nodename.find("/eos/") == std::string::npos)) {
    nodename.insert(0, "/eos/");
    nodename.append("/fst");
  }

  std::string tident = mVid.tident.c_str();
  std::string rnodename = nodename;
  {
    // for sss + node identification
    rnodename.erase(0, 5);
    size_t dpos;

    if ((dpos = rnodename.find(':')) != std::string::npos) {
      rnodename.erase(dpos);
    }

    if ((dpos = rnodename.find('.')) != std::string::npos) {
      rnodename.erase(dpos);
    }

    size_t addpos = 0;

    if ((addpos = tident.find('@')) != std::string::npos) {
      tident.erase(0, addpos + 1);
    }
  }
  eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);
  // If EOS_SKIP_SSS_HOSTNAME_MATCH env variable is set then we skip
  // the check below as this currently breaks the Kubernetes setup.
  bool skip_hostname_match = (getenv("EOS_SKIP_SSS_HOSTNAME_MATCH")) ? true :
                             false;

  if (mVid.uid == 0 || mVid.prot == "sss") {
    if (mVid.uid != 0 && mVid.prot == "sss") {
      if (!skip_hostname_match &&
          tident.compare(0, tident.length(), rnodename, 0, tident.length())) {
        reply.set_std_err("error: nodes can only be configured as 'root' or by "
                          "connecting from the node itself using the sss protocol(1)");
        reply.set_retc(EPERM);
        return;
      }
    }
  } else {
    reply.set_std_err("error: nodes can only be configured as 'root' or by "
                      "connecting from the node itself using the sss protocol(2)");
    reply.set_retc(EPERM);
    return;
  }

  if (!FsView::gFsView.mNodeView.count(nodename)) {
    reply.set_std_out("info: creating node '" + nodename + "'");

    // reply.set_std_err("error: no such node '" + nodename + "'");
    // reply.set_retc(ENOENT);
    if (!FsView::gFsView.RegisterNode(nodename.c_str())) {
      reply.set_std_err("error: cannot register node <" + nodename + ">");
      reply.set_retc(EIO);
      return;
    }
  }

  // we need to take the previous version of groupproxys to update it
  std::string proxygroups = FsView::gFsView.mNodeView[nodename]->GetConfigMember(
                              key);
  eos_static_debug(" old proxygroups value %s", proxygroups.c_str());
  // find a previous occurence
  std::set<std::string> groups;
  std::string::size_type pos1 = 0, pos2 = 0;

  if (!proxygroups.empty()) {
    do {
      pos2 = proxygroups.find(',', pos1);
      groups.insert(proxygroups.substr(pos1,
                                       pos2 == std::string::npos ? std::string::npos : pos2 - pos1));
      pos1 = pos2;

      if (pos1 != std::string::npos) {
        pos1++;
      }
    } while (pos2 != std::string::npos);
  }

  if (action == eos::console::NodeProto_ProxygroupProto::CLEAR) {
    proxygroups = "";
  } else {
    if (action == eos::console::NodeProto_ProxygroupProto::ADD) {
      groups.insert(status);
    } else if (action == eos::console::NodeProto_ProxygroupProto::RM) {
      groups.erase(status);
    }

    proxygroups.clear();

    for (const auto& group : groups) {
      proxygroups.append(group + ",");
    }

    if (!proxygroups.empty()) {
      proxygroups.resize(proxygroups.size() - 1);
    }
  }

  eos_static_debug(" new proxygroups value %s", proxygroups.c_str());
  status = proxygroups;

  if (!FsView::gFsView.mNodeView[nodename]->SetConfigMember(key, status)) {
    reply.set_std_err("error: cannot set node config value");
    reply.set_retc(EIO);
    return;
  }

  // set also the manager name
  if (!FsView::gFsView.mNodeView[nodename]->SetConfigMember("manager",
      gOFS->mMaster->GetMasterId(), true)) {
    reply.set_std_err("error: cannot set the manager name");
    reply.set_retc(EIO);
    return;
  }
}
EOSMGMNAMESPACE_END
