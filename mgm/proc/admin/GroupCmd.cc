//------------------------------------------------------------------------------
// @file: GroupCmd.cc
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

#include "GroupCmd.hh"
#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/FsView.hh"
#include "mgm/GeoTreeEngine.hh"

EOSMGMNAMESPACE_BEGIN

static constexpr auto GEOTAG_KEY="stat.geotag";
static constexpr auto GEOTAG_PLCT_KEY = "plct";
//------------------------------------------------------------------------------
// Method implementing the specific behavior of the command executed by the
// asynchronous thread
//------------------------------------------------------------------------------
eos::console::ReplyProto
GroupCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::GroupProto group = mReqProto.group();

  switch (mReqProto.group().subcmd_case()) {
  case eos::console::GroupProto::kLs:
    LsSubcmd(group.ls(), reply);
    break;

  case eos::console::GroupProto::kRm:
    RmSubcmd(group.rm(), reply);
    break;

  case eos::console::GroupProto::kSet:
    SetSubcmd(group.set(), reply);
    break;

  default:
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not supported");
  }

  return reply;
}

//------------------------------------------------------------------------------
// Execute ls subcommand
//------------------------------------------------------------------------------
void
GroupCmd::LsSubcmd(const eos::console::GroupProto_LsProto& ls,
                   eos::console::ReplyProto& reply)
{
  using eos::console::GroupProto;
  bool json_output = false;
  std::string list_format;
  std::string format;
  auto format_case = ls.outformat();

  if ((format_case == GroupProto::LsProto::NONE) && WantsJsonOutput()) {
    format_case = GroupProto::LsProto::MONITORING;
  }

  switch (format_case) {
  case GroupProto::LsProto::MONITORING:
    format = FsView::GetGroupFormat("m");
    json_output = WantsJsonOutput();
    break;

  case GroupProto::LsProto::IOGROUP:
    format = FsView::GetGroupFormat("io");
    break;

  case GroupProto::LsProto::IOFS:
    format = FsView::GetGroupFormat("IO");
    list_format = FsView::GetFileSystemFormat("io");
    // @note in the old implementation was mOutFormat="io", but then mOutFormat
    // never used again apparently
    // ls.set_outformat(eos::console::GroupProto_LsProto::IOGROUP);
    break;

  case GroupProto::LsProto::LISTING:
    format = FsView::GetGroupFormat("l");
    list_format = FsView::GetFileSystemFormat("l");
    break;

  default : // NONE
    format = FsView::GetGroupFormat("");
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
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  FsView::gFsView.PrintGroups(output, format, list_format, ls.outdepth(),
                              ls.selection().c_str(), mReqProto.dontcolor());

  if (json_output) {
    output = ResponseToJsonString(output);
  }

  reply.set_std_out(output.c_str());
  reply.set_retc(0);
}

//------------------------------------------------------------------------------
// Execute rm subcommand
//------------------------------------------------------------------------------
void
GroupCmd::RmSubcmd(const eos::console::GroupProto_RmProto& rm,
                   eos::console::ReplyProto& reply)
{
  if (mVid.uid != 0) {
    reply.set_std_err("error: you have to take role 'root' to execute this command");
    reply.set_retc(EPERM);
    return;
  }

  if (!rm.group().length()) {
    reply.set_std_err("error: illegal parameter 'group'");
    reply.set_retc(EINVAL);
    return;
  }

  eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);

  if (!FsView::gFsView.mGroupView.count(rm.group())) {
    reply.set_std_err(("error: no such group '" + rm.group() + "'").c_str());
    reply.set_retc(ENOENT);
    return;
  }

  for (auto it = FsView::gFsView.mGroupView[rm.group()]->begin();
       it != FsView::gFsView.mGroupView[rm.group()]->end(); ++it) {
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it);

    if (fs) {
      // Check that all filesystems are empty
      if ((fs->GetConfigStatus(false) != eos::common::ConfigStatus::kEmpty)) {
        reply.set_std_err("error: unable to remove group '" + rm.group() +
                          "' - filesystems are not all in empty state - "
                          "try list the group and drain them or set: fs "
                          "config <fsid> configstatus=empty\n");
        reply.set_retc(EBUSY);
        return;
      }
    }
  }

  common::SharedHashLocator groupLocator =
    common::SharedHashLocator::makeForGroup(rm.group());

  if (!mq::SharedHashWrapper::deleteHash(gOFS->mMessagingRealm.get(), groupLocator)) {
    reply.set_std_err(("error: unable to remove config of group '" +
                       rm.group() + "'").c_str());
    reply.set_retc(EIO);
    return;
  } else {
    if (FsView::gFsView.UnRegisterGroup(rm.group().c_str())) {
      reply.set_std_out(("success: removed group '" + rm.group() + "'").c_str());
      reply.set_retc(0);
    } else {
      reply.set_retc(EINVAL);
      reply.set_std_err(("error: unable to unregister group '" +
                         rm.group() + "'").c_str());
    }

    return;
  }
}

//------------------------------------------------------------------------------
// Execute set subcommand
//------------------------------------------------------------------------------
void
GroupCmd::SetSubcmd(const eos::console::GroupProto_SetProto& set,
                    eos::console::ReplyProto& reply)
{
  if (mVid.uid != 0) {
    reply.set_std_err("error: you have to take role 'root' to execute this command");
    reply.set_retc(EPERM);
    return;
  }

  std::string key = "status";

  if (!set.group().length() || !set.group_state().length()) {
    reply.set_std_err("error: illegal parameters 'group or group-state'");
    reply.set_retc(EINVAL);
    return;
  }

  eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);

  // FIXME unify the various methods doing group iteration below!
  bool non_existant_group = FsView::gFsView.mGroupView.count(set.group()) == 0;

  if (set.group_state() == "drain" && non_existant_group)
  {
    reply.set_std_err("error: group does not exist!");
    reply.set_retc(EINVAL);
    return;
  }

  if (non_existant_group) {
    reply.set_std_out(("info: creating group '" + set.group() + "'").c_str());

    if (!FsView::gFsView.RegisterGroup(set.group().c_str())) {
      std::string groupconfigname = common::SharedHashLocator::makeForGroup(
                                      set.group()).getConfigQueue();
      reply.set_std_err(("error: cannot register group <" +
                         set.group() + ">").c_str());
      reply.set_retc(EIO);
      return;
    }
  }

  // Set this new group to offline
  if (!FsView::gFsView.mGroupView[set.group()]->SetConfigMember
      (key, set.group_state())) {
    reply.set_std_err("error: cannot set config status");
    reply.set_retc(EIO);
    return;
  }

  if (set.group_state() == "on") {
    // Recompute the drain status in this group
    bool setactive = false;

    if (FsView::gFsView.mGroupView.count(set.group())) {
      for (auto git = FsView::gFsView.mGroupView[set.group()]->begin();
           git != FsView::gFsView.mGroupView[set.group()]->end(); ++git) {
        auto fs = FsView::gFsView.mIdView.lookupByID(*git);

        if (fs) {
          common::DrainStatus drainstatus =
            (eos::common::FileSystem::GetDrainStatusFromString
             (fs->GetString("local.drain").c_str()));

          if ((drainstatus == eos::common::DrainStatus::kDraining) ||
              (drainstatus == eos::common::DrainStatus::kDrainStalling)) {
            // If any group filesystem is draining, all the others have
            // to enable the pull for draining!
            setactive = true;
          }
        }
      }

      for (auto git = FsView::gFsView.mGroupView[set.group()]->begin();
           git != FsView::gFsView.mGroupView[set.group()]->end(); ++git) {
        auto fs = FsView::gFsView.mIdView.lookupByID(*git);

        if (fs) {
          if (setactive) {
            if (fs->GetString("stat.drainer") != "on") {
              fs->SetString("stat.drainer", "on");
            }
          } else {
            if (fs->GetString("stat.drainer") != "off") {
              fs->SetString("stat.drainer", "off");
            }
          }
        }
      }
    }
  }

  if (set.group_state() == "off") {
    // Disable all draining in this group
    if (FsView::gFsView.mGroupView.count(set.group())) {
      for (auto git = FsView::gFsView.mGroupView[set.group()]->begin();
           git != FsView::gFsView.mGroupView[set.group()]->end(); ++git) {
        auto fs = FsView::gFsView.mIdView.lookupByID(*git);

        if (fs) {
          fs->SetString("stat.drainer", "off");
        }
      }
    }
  }

  if (set.group_state() == "drain") {
    auto group = FsView::gFsView.mGroupView.find(set.group());
    if (group != FsView::gFsView.mGroupView.end()) {
      std::unordered_set<std::string> geotags;
      for (auto fs_it = group->second->cbegin();
           fs_it != group->second->cend(); ++fs_it) {
        auto fs = FsView::gFsView.mIdView.lookupByID(*fs_it);
        if (fs) {
          geotags.emplace(fs->GetString(GEOTAG_KEY));
        }
      }
      XrdOucString output;
      for (const auto& geotag : geotags) {
        // TODO: review whether only plct needs to be disabled or every op!
        bool status = gOFS->mGeoTreeEngine->addDisabledBranch(set.group().c_str(),
                                                              GEOTAG_PLCT_KEY,
                                                              geotag.c_str(),
                                                              &output, true);
        if (!status) {
          reply.set_retc(EIO);
          reply.set_std_err(output.c_str());
          return;
        }
      }
    }
  }

  reply.set_retc(0);
}

EOSMGMNAMESPACE_END
