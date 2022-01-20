// ----------------------------------------------------------------------
// File: proc/admin/Node.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/IMaster.hh"
#include "mgm/config/IConfigEngine.hh"

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Node()
{
  retc = SFS_OK;

  if (mSubCmd == "ls") {
    std::string output;
    std::string format;
    std::string fqdn;

    if (pOpaque->Get("mgm.outhost")) {
      fqdn = pOpaque->Get("mgm.outhost");
    }

    std::string mListFormat;
    format = FsView::GetNodeFormat(std::string(mOutFormat.c_str()));

    if (mOutFormat == "l") {
      mListFormat = FsView::GetFileSystemFormat(std::string(mOutFormat.c_str()));
    }

    if (fqdn != "brief") {
      if (format.find('S') != std::string::npos) {
        format.replace(format.find('S'), 1, "s");
      }

      if (mListFormat.find('S') != std::string::npos) {
        mListFormat.replace(mListFormat.find('S'), 1, "s");
      }
    }

    eos::common::RWMutexReadLock rd_lock(FsView::gFsView.ViewMutex);
    FsView::gFsView.PrintNodes(output, format, mListFormat, mOutDepth, mSelection);
    stdOut += output.c_str();
  } else if (mSubCmd == "status") {
    std::string node = (pOpaque->Get("mgm.node")) ? pOpaque->Get("mgm.node") : "";
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

    if ((node.find(':') == std::string::npos)) {
      node += ":1095"; // default eos fst port
    }

    if ((node.find("/eos/") == std::string::npos)) {
      node.insert(0, "/eos/");
      node.append("/fst");
    }

    if (FsView::gFsView.mNodeView.count(node)) {
      stdOut += "# ------------------------------------------------------------------------------------\n";
      stdOut += "# Node Variables\n";
      stdOut += "# ....................................................................................\n";
      std::vector<std::string> keylist;
      FsView::gFsView.mNodeView[node]->GetConfigKeys(keylist);
      std::sort(keylist.begin(), keylist.end());

      for (size_t i = 0; i < keylist.size(); i++) {
        char line[2048];
        std::string val;
        val = FsView::gFsView.mNodeView[node]->GetConfigMember(
                keylist[i]);

        if (val.substr(0, 7) == "base64:") {
          val = "base64:...";
        }

        if (val.length() > 1024) {
          val = "...";
        }

        snprintf(line, sizeof(line) - 1, "%-32s := %s\n", keylist[i].c_str(),
                 val.c_str());
        stdOut += line;
      }
    } else {
      stdErr = "error: cannot find node - no node with name=";
      stdErr += node.c_str();
      retc = ENOENT;
    }
  } else if (mSubCmd == "set") {
    std::string nodename = (pOpaque->Get("mgm.node")) ? pOpaque->Get("mgm.node") :
                           "";
    std::string status = (pOpaque->Get("mgm.node.state")) ?
                         pOpaque->Get("mgm.node.state") : "";
    std::string txgw = (pOpaque->Get("mgm.node.txgw")) ?
                       pOpaque->Get("mgm.node.txgw") : "";
    std::string key = "status";
    std::string action = (pOpaque->Get("mgm.node.action")) ?
                         pOpaque->Get("mgm.node.action") : "";

    if (txgw.length()) {
      key = "txgw";
      status = txgw;
    }

    if (!action.empty()) {
      // we are setting a proxygroup
      key = "proxygroups";
      status = (pOpaque->Get("mgm.node.proxygroup")) ?
               pOpaque->Get("mgm.node.proxygroup") : "clear";

      if (status.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890._-")
          != std::string::npos) {
        status.clear();
      }
    }

    if ((!nodename.length()) || (!status.length())) {
      stdErr = "error: illegal parameters";
      retc = EINVAL;
    } else {
      if ((nodename.find(':') == std::string::npos)) {
        nodename += ":1095"; // default eos fst port
      }

      if ((nodename.find("/eos/") == std::string::npos)) {
        nodename.insert(0, "/eos/");
        nodename.append("/fst");
      }

      std::string tident = pVid->tident.c_str();
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
      // If EOS_SKIP_SSS_HOSTNAME_MATCH env variable is set then we skip
      // the check below as this currently breaks the Kubernetes setup.
      bool skip_hostname_match = false;

      if (getenv("EOS_SKIP_SSS_HOSTNAME_MATCH")) {
        skip_hostname_match = true;
      }

      eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);

      if ((pVid->uid == 0) || (pVid->prot == "sss")) {
        if ((pVid->prot == "sss") && (pVid->uid != 0)) {
          if (!skip_hostname_match &&
              tident.compare(0, tident.length(), rnodename, 0, tident.length())) {
            stdErr += "error: nodes can only be configured as 'root' or by "
                      "connecting from the node itself using the sss protocol(1)\n";
            retc = EPERM;
          }
        }
      } else {
        stdErr += "error: nodes can only be configured as 'root' or by "
                  "connecting from the node itself using the sss protocol(2)\n";
        retc = EPERM;
      }

      if (retc == SFS_OK) {
        if (!FsView::gFsView.mNodeView.count(nodename)) {
          stdOut = "info: creating node '";
          stdOut += nodename.c_str();
          stdOut += "'";

          // stdErr="error: no such node '"; stdErr += nodename.c_str(); stdErr += "'";
          // retc = ENOENT;

          if (!FsView::gFsView.RegisterNode(nodename.c_str())) {
            stdErr = "error: cannot register node <";
            stdErr += nodename.c_str();
            stdErr += ">";
            retc = EIO;
          }
        }
      }

      if (!retc) {
        if (!action.empty()) {
          // we need to take the previous version of groupproxys to update it
          std::string proxygroups = FsView::gFsView.mNodeView[nodename]->GetConfigMember(
                                      key);
          eos_static_debug(" old proxygroups value %s", proxygroups.c_str());
          // find a previous occurence
          std::set<std::string> groups;
          std::string::size_type pos1 = 0, pos2 = 0;

          if (!proxygroups.empty())
            do {
              pos2 = proxygroups.find(',', pos1);
              groups.insert(proxygroups.substr(pos1,
                                               pos2 == std::string::npos ? std::string::npos : pos2 - pos1));
              pos1 = pos2;

              if (pos1 != std::string::npos) {
                pos1++;
              }
            } while (pos2 != std::string::npos);

          if (action == "clear") {
            proxygroups = "";
          } else {
            if (action == "add") {
              groups.insert(status);
            } else if (action == "rm") {
              groups.erase(status);
            }

            proxygroups.clear();

            for (auto it = groups.begin(); it != groups.end(); ++it) {
              proxygroups.append(*it + ",");
            }

            if (!proxygroups.empty()) {
              proxygroups.resize(proxygroups.size() - 1);
            }
          }

          eos_static_debug(" new proxygroups value %s", proxygroups.c_str());
          status = proxygroups;
        }

        if (!FsView::gFsView.mNodeView[nodename]->SetConfigMember(key, status)) {
          retc = EIO;
          stdErr = "error: cannot set node config value";
        }

        // set also the manager name
        if (!FsView::gFsView.mNodeView[nodename]->SetConfigMember("manager",
            gOFS->mMaster->GetMasterId(), true)) {
          retc = EIO;
          stdErr = "error: cannot set the manager name";
        }
      }
    }
  } else if (mSubCmd == "rm") {
    if ((pVid->uid == 0) || (pVid->prot == "sss")) {
      std::string nodename = (pOpaque->Get("mgm.node")) ? pOpaque->Get("mgm.node") :
                             "";

      if ((!nodename.length())) {
        stdErr = "error: illegal parameters";
        retc = EINVAL;
      } else {
        if ((nodename.find(':') == std::string::npos)) {
          nodename += ":1095"; // default eos fst port
        }

        if ((nodename.find("/eos/") == std::string::npos)) {
          nodename.insert(0, "/eos/");
          nodename.append("/fst");
        }

        eos::common::RWMutexWriteLock wr_lock(FsView::gFsView.ViewMutex);

        if (!FsView::gFsView.mNodeView.count(nodename)) {
          stdErr = "error: no such node '";
          stdErr += nodename.c_str();
          stdErr += "'";
          retc = ENOENT;
        } else {
          // Remove a node only if it has no heartbeat anymore
          if ((time(nullptr) - FsView::gFsView.mNodeView[nodename]->GetHeartBeat()) < 5) {
            stdErr = "error: this node was still sending a heartbeat < 5 "
                     "seconds ago - stop the FST daemon first!\n";
            retc = EBUSY;
            return SFS_OK;
          }

          // Remove a node only if all filesystems are in empty state
          for (auto it = FsView::gFsView.mNodeView[nodename]->begin();
               it != FsView::gFsView.mNodeView[nodename]->end(); ++it) {
            FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it);

            if (fs) {
              // check the empty state
              if ((fs->GetConfigStatus(false) != eos::common::ConfigStatus::kEmpty)) {
                stdErr = "error: unable to remove node '";
                stdErr += nodename.c_str();
                stdErr += "' - filesystems are not all in empty state\n";
                retc = EBUSY;
                return SFS_OK;
              }
            }
          }

          common::SharedHashLocator nodeLocator = common::SharedHashLocator::makeForNode(
              nodename);

          if (!mq::SharedHashWrapper::deleteHash(gOFS->mMessagingRealm.get(),
                                                 nodeLocator)) {
            stdErr = "error: unable to remove config of node '";
            stdErr += nodename.c_str();
            stdErr += "'";
            retc = EIO;
          } else {
            if (FsView::gFsView.UnRegisterNode(nodename.c_str())) {
              stdOut = "success: removed node '";
              stdOut += nodename.c_str();
              stdOut += "'";
            } else {
              stdErr = "error: unable to unregister node '";
              stdErr += nodename.c_str();
              stdErr += "'";
            }
          }

          // Delete also the entry from the configuration
          eos_info("msg=\"delete from configuration\" node_name=%s",
                   nodeLocator.getConfigQueue().c_str());
          gOFS->ConfEngine->DeleteConfigValueByMatch("global",
              nodeLocator.getConfigQueue().c_str());
          gOFS->ConfEngine->AutoSave();
        }
      }
    } else {
      retc = EPERM;
      stdErr = "error: you have to take role 'root' to execute this command";
    }
  } else if (mSubCmd == "config") {
    if ((pVid->uid == 0) || (pVid->prot == "sss")) {
      std::string identifier = (pOpaque->Get("mgm.node.name")) ?
                               pOpaque->Get("mgm.node.name") : "";
      std::string key = (pOpaque->Get("mgm.node.key")) ? pOpaque->Get("mgm.node.key")
                        : "";
      std::string value = (pOpaque->Get("mgm.node.value")) ?
                          pOpaque->Get("mgm.node.value") : "";

      if ((!identifier.length()) || (!key.length()) || (!value.length())) {
        stdErr = "error: illegal parameters";
        retc = EINVAL;
      } else {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        std::vector<FsNode*> nodes;
        FileSystem* fs = nullptr;

        if ((identifier.find('*') != std::string::npos)) {
          // apply this to all nodes !
          for (auto it = FsView::gFsView.mNodeView.begin();
               it != FsView::gFsView.mNodeView.end(); ++it) {
            nodes.push_back(it->second);
          }
        } else {
          // by host:port name
          std::string path = identifier;

          if ((identifier.find(':') == std::string::npos)) {
            identifier += ":1095"; // default eos fst port
          }

          if ((identifier.find("/eos/") == std::string::npos)) {
            identifier.insert(0, "/eos/");
            identifier.append("/fst");
          }

          if (FsView::gFsView.mNodeView.count(identifier)) {
            nodes.push_back(FsView::gFsView.mNodeView[identifier]);
          }
        }

        for (size_t i = 0; i < nodes.size(); i++) {
          if (key == "configstatus") {
            for (auto it = nodes[i]->begin(); it != nodes[i]->end(); ++it) {
              fs = FsView::gFsView.mIdView.lookupByID(*it);

              if (fs) {
                // Check the allowed strings
                if ((eos::common::FileSystem::GetConfigStatusFromString(
                       value.c_str()) != eos::common::ConfigStatus::kUnknown)) {
                  fs->SetString(key.c_str(), value.c_str());
                  FsView::gFsView.StoreFsConfig(fs);
                } else {
                  stdErr += "error: not an allowed parameter <";
                  stdErr += key.c_str();
                  stdErr += ">\n";
                  retc = EINVAL;
                }
              } else {
                stdErr += "error: cannot identify the filesystem by <";
                stdErr += identifier.c_str();
                stdErr += ">\n";
                retc = EINVAL;
              }
            }
          } else {
            bool keyok = false;

            if (key == "gw.ntx") {
              keyok = true;
              int slots = atoi(value.c_str());

              if ((slots < 1) || (slots > 100)) {
                stdErr += "error: number of gateway transfer slots must be between 1-100\n";
                retc = EINVAL;
              } else {
                if (nodes[i]->SetConfigMember(key, value, false)) {
                  stdOut += "success: number of gateway transfer slots set to gw.ntx=";
                  stdOut += slots;
                } else {
                  stdErr += "error: failed to store the config value gw.ntx\n";
                  retc = EFAULT;
                }
              }
            }

            if (key == "gw.rate") {
              keyok = true;
              int bw = atoi(value.c_str());

              if ((bw < 1) || (bw > 10000)) {
                stdErr += "error: gateway transfer speed must be 1-10000 (MB/s)\n";
                retc = EINVAL;
              } else {
                if (nodes[i]->SetConfigMember(key, value, false)) {
                  stdOut += "success: gateway transfer rate set to gw.rate=";
                  stdOut += bw;
                  stdOut += " Mb/s";
                } else {
                  stdErr += "error: failed to store the config value gw.rate\n";
                  retc = EFAULT;
                }
              }
            }

            if (key == "error.simulation") {
              keyok = true;

              if (nodes[i]->SetConfigMember(key, value, false)) {
                stdOut += "success: setting error simulation tag '";
                stdOut += value.c_str();
                stdOut += "'";
              } else {
                stdErr += "error: failed to store the error simulation tag\n";
                retc = EFAULT;
              }
            }

            if (key == "publish.interval") {
              keyok = true;

              if (nodes[i]->SetConfigMember(key, value, false)) {
                stdOut += "success: setting publish interval to '";
                stdOut += value.c_str();
                stdOut += "'";
              } else {
                stdErr += "error: failed to store publish interval\n";
                retc = EFAULT;
              }
            }

            if (key == "debug.level") {
              keyok = true;

              if (nodes[i]->SetConfigMember(key, value, false)) {
                stdOut += "success: setting debug level to '";
                stdOut += value.c_str();
                stdOut += "'";
              } else {
                stdErr += "error: failed to store debug level interval\n";
                retc = EFAULT;
              }
            }

            if (!keyok) {
              stdErr += "error: the specified key is not known - consult the usage information of the command\n";
              retc = EINVAL;
            }
          }

          stdOut += "\n";
        }

        if (nodes.empty()) {
          retc = EINVAL;
          stdErr = "error: cannot find node <";
          stdErr += identifier.c_str();
          stdErr += ">";
        }
      }
    } else {
      retc = EPERM;
      stdErr = "error: you have to take role 'root' to execute this command";
    }
  } else if (mSubCmd == "register") {
    if ((pVid->uid == 0) || (pVid->prot == "sss")) {
      XrdOucString registernode = pOpaque->Get("mgm.node.name");
      XrdOucString path2register = pOpaque->Get("mgm.node.path2register");
      XrdOucString space2register = pOpaque->Get("mgm.node.space2register");
      XrdOucString force = pOpaque->Get("mgm.node.force");
      XrdOucString rootflag = pOpaque->Get("mgm.node.root");

      if ((!registernode.c_str()) ||
          (!path2register.c_str()) ||
          (!space2register.c_str()) ||
          (force.length() && (force != "true")) ||
          (rootflag.length() && (rootflag != "true"))
         ) {
        stdErr = "error: invalid parameters";
        retc = EINVAL;
      } else {
        XrdOucString msgbody = "";
        msgbody = eos::common::FileSystem::GetRegisterRequestString();
        msgbody += "&mgm.path2register=";
        msgbody += path2register;
        msgbody += "&mgm.space2register=";
        msgbody += space2register;

        if (force.length()) {
          msgbody += "&mgm.force=true";
        }

        if (rootflag.length()) {
          msgbody += "&mgm.root=true";
        }

        XrdOucString nodequeue = "/eos/";

        if (registernode == "*") {
          nodequeue += "*";
        } else {
          nodequeue += registernode;
        }

        nodequeue += "/fst";
        mq::MessagingRealm::Response response =
          gOFS->mMessagingRealm->sendMessage("msg", msgbody.c_str(), nodequeue.c_str());

        if (response.ok()) {
          stdOut = "success: sent global register message to all fst nodes";
        } else {
          stdErr = "error: could not send global fst register message!";
          retc = EIO;
        }
      }
    } else {
      stdErr = "error: you have to take the root role to execute the register command!";
      retc = EPERM;
    }
  } else {
    stdErr = "error: no such subcommand for node!";
    retc = EINVAL;
  }

  return retc;
}

EOSMGMNAMESPACE_END
