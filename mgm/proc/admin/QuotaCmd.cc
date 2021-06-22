//------------------------------------------------------------------------------
// @file: QuotaCmd.cc
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

#include "QuotaCmd.hh"
#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Acl.hh"
#include "mgm/Stat.hh"
#include "mgm/Quota.hh"
#include "common/Path.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Method implementing the specific behavior of the command executed by the
// asynchronous thread
//------------------------------------------------------------------------------
eos::console::ReplyProto
QuotaCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::QuotaProto quota = mReqProto.quota();

  switch (mReqProto.quota().subcmd_case()) {
  case eos::console::QuotaProto::kLsuser:
    LsuserSubcmd(quota.lsuser(), reply);
    break;

  case eos::console::QuotaProto::kLs:
    LsSubcmd(quota.ls(), reply);
    break;

  case eos::console::QuotaProto::kSet:
    SetSubcmd(quota.set(), reply);
    break;

  case eos::console::QuotaProto::kRm:
    RmSubcmd(quota.rm(), reply);
    break;

  case eos::console::QuotaProto::kRmnode:
    RmnodeSubcmd(quota.rmnode(), reply);
    break;

  default:
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not supported");
  }

  return reply;
}

//------------------------------------------------------------------------------
// Execute lsuser subcommand
//------------------------------------------------------------------------------
void QuotaCmd::LsuserSubcmd(const eos::console::QuotaProto_LsuserProto& lsuser,
                            eos::console::ReplyProto& reply)
{
  std::ostringstream std_out, std_err;
  int ret_c = 0;
  gOFS->MgmStats.Add("Quota", mVid.uid, mVid.gid, 1);
  std::string space = lsuser.space();
  bool exists = false;

  if (!space.empty()) {
    XrdOucErrInfo mError;
    // evt. correct the space variable to be a directory path (+/)
    struct stat buf {};
    std::string sspace = space;

    if (sspace[sspace.length() - 1] != '/') {
      sspace += '/';
    }

    if (!gOFS->_stat(sspace.c_str(), &buf, mError, mVid,
                     nullptr)) { // @note no.01 Where is the info in mError is going?
      space = sspace;
      exists = true;
    }
  }

  eos_notice("msg=\"quota ls (user)\" space=%s", space.c_str());

  // Early return if routing should happen
  if (ShouldRoute(space, reply)) {
    return;
  }

  if (!exists && lsuser.exists()) {
    reply.set_retc(ENOENT);
    reply.set_std_err("error: the given path does not exist!");
    return;
  }

  if (lsuser.quotanode()) {
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                      __FILE__);
    // check if this is a quotanode
    std::string quota_node_path = Quota::GetResponsibleSpaceQuotaPath(space);
    eos::common::Path qPath(quota_node_path.c_str());
    eos::common::Path sPath(space.c_str());

    if (std::string(qPath.GetPath()) != std::string(sPath.GetPath())) {
      reply.set_retc(ENOENT);
      reply.set_std_err("error: the given path is not a quotanode!");
      return;
    }
  }

  XrdOucString out {""};
  auto monitoring = lsuser.format() || WantsJsonOutput();
  bool is_ok = Quota::PrintOut(space, out, mVid.uid, -1, monitoring, true);

  if (is_ok && out.length()) {
    if (!monitoring) {
      std_out << ("\nBy user:" + out).c_str();
    } else {
      std_out << out.c_str();
    }
  } else {
    if (!is_ok) {
      std_err << out.c_str() << std::endl;
      ret_c = EINVAL;
    }
  }

  out = "";
  is_ok = Quota::PrintOut(space, out, -1, mVid.gid, monitoring, true);
  // mDoSort = false; @note no.02 was there in the old implementation, but looks like it is not actually needed anymore

  if (is_ok && out != "") {
    if (!monitoring) {
      std_out << ("\nBy group:" + out).c_str();
    } else {
      std_out << out.c_str();
    }
  } else {
    if (!is_ok) {
      std_err << out.c_str();
      ret_c = EINVAL;
    }
  }

  if (WantsJsonOutput()) {
    std_out.str(ResponseToJsonString(std_out.str(), std_err.str(), ret_c));
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
  reply.set_retc(ret_c);
}

//------------------------------------------------------------------------------
// Execute ls subcommand
//------------------------------------------------------------------------------
void QuotaCmd::LsSubcmd(const eos::console::QuotaProto_LsProto& ls,
                        eos::console::ReplyProto& reply)
{
  std::ostringstream std_out, std_err;
  bool canQuota;
  int ret_c = 0;
  XrdOucErrInfo mError;
  int errc;
  gOFS->MgmStats.Add("Quota", mVid.uid, mVid.gid, 1);
  std::string space = ls.space();
  auto monitoring = ls.format() || WantsJsonOutput();

  if (!space.empty()) {
    // evt. correct the space variable to be a directory path (+/)
    std::string sspace = space;
    struct stat buf {};

    if (sspace[sspace.length() - 1] != '/') {
      sspace += '/';
    }

    if (!gOFS->_stat(sspace.c_str(), &buf, mError, mVid, nullptr)) { // @note no.01
      space = sspace;
    } else {
      if (ls.exists()) {
        reply.set_retc(ENOENT);
        reply.set_std_err("error: the given path does not exist!");
        return;
      }
    }
  }

  if ((!mVid.uid) || mVid.hasUid(3) ||
      mVid.hasGid(4)) { // @note no.03 before 'vid' was used, using 'mVid' now
    // root and admin can set quota
    canQuota = true;
  } else {
    // figure out if the authenticated user is a quota admin
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                      __FILE__);
    eos::IContainerMD::XAttrMap attrmap;

    if (space[0] != '/') {
      // take the proc directory
      space = gOFS->MgmProcPath.c_str();
    } else {
      // effectively check ACLs on the quota node directory if it can be retrieved
      std::string quota_node_path = Quota::GetResponsibleSpaceQuotaPath(space);

      if (quota_node_path.length()) {
        space = quota_node_path;
      }
    }

    // ACL and permission check
    Acl acl(space.c_str(), mError, mVid, attrmap, false); // @note no.01
    canQuota = acl.CanSetQuota();
  }

  if (ls.quotanode()) {
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                      __FILE__);
    // check if this is a quotanode
    std::string quota_node_path = Quota::GetResponsibleSpaceQuotaPath(space);
    eos::common::Path qPath(quota_node_path.c_str());
    eos::common::Path sPath(space.c_str());

    if (std::string(qPath.GetPath()) != std::string(sPath.GetPath())) {
      reply.set_retc(ENOENT);
      reply.set_std_err("error: the given path is not a quotanode!");
      return;
    }
  }

  if (!canQuota) {
    reply.set_retc(EPERM);
    reply.set_std_err("error: you are not a quota administrator!\"");
    return;
  }

  eos_notice("msg=\"quota ls\" space=%s", space.c_str());
  XrdOucString out1 {""};
  XrdOucString out2 {""};
  long long int uid = (ls.uid().empty() ? -1LL :
                       eos::common::Mapping::UserNameToUid(ls.uid(), errc));
  long long int gid = (ls.gid().empty() ? -1LL :
                       eos::common::Mapping::GroupNameToGid(ls.gid(), errc));

  if ((uid != -1LL) && (gid != -1LL)) {
    // Print both uid and gid info
    if (!Quota::PrintOut(space, out1, uid, -1LL, monitoring, !ls.printid())) {
      std_err.str(out1.c_str());
      ret_c = EINVAL;
    } else {
      if (!Quota::PrintOut(space, out2, -1LL, gid, monitoring, !ls.printid())) {
        std_err.str(out2.c_str());
        ret_c = EINVAL;
      } else {
        std_out.str((out1 + out2).c_str());
      }
    }
  } else {
    // Either uid or gid is printed
    if (Quota::PrintOut(space, out1, uid, gid, monitoring, !ls.printid())) {
      std_out.str(out1.c_str());
    } else {
      std_err.str(out1.c_str());
      ret_c = EINVAL;
    }
  }

  if (WantsJsonOutput()) {
    std_out.str(ResponseToJsonString(std_out.str(), std_err.str(), ret_c));
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
  reply.set_retc(ret_c);
}

//------------------------------------------------------------------------------
// Execute set subcommand
//------------------------------------------------------------------------------
void QuotaCmd::SetSubcmd(const eos::console::QuotaProto_SetProto& set,
                         eos::console::ReplyProto& reply)
{
  std::ostringstream std_out, std_err;
  int ret_c = 0;
  XrdOucErrInfo mError;
  int errc;
  long id = 0;
  Quota::IdT id_type;
  gOFS->MgmStats.Add("Quota", mVid.uid, mVid.gid, 1);
  std::string space = set.space();

  if (!space.empty()) {
    // evt. correct the space variable to be a directory path (+/)
    struct stat buf {};
    std::string sspace = space;

    if (sspace[sspace.length() - 1] != '/') {
      sspace += '/';
    }

    if (!gOFS->_stat(sspace.c_str(), &buf, mError, mVid, nullptr)) { // @note no.01
      space = sspace;
    }
  }

  bool canQuota;

  if ((!mVid.uid) || mVid.hasUid(3) || mVid.hasGid(4)) { // @note no.03
    // root and admin can set quota
    canQuota = true;
  } else {
    // figure out if the authenticated user is a quota admin
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                      __FILE__);
    eos::IContainerMD::XAttrMap attrmap;

    if (space[0] != '/') {
      // take the proc directory
      space = gOFS->MgmProcPath.c_str();
    } else {
      // effectively check ACLs on the quota node directory if it can be retrieved
      std::string quota_node_path = Quota::GetResponsibleSpaceQuotaPath(space);

      if (quota_node_path.length()) {
        space = quota_node_path;
      }
    }

    // ACL and permission check
    Acl acl(space.c_str(), mError, mVid, attrmap, false); // @note no.01
    canQuota = acl.CanSetQuota();
  }

  if (!canQuota) {
    reply.set_retc(EPERM);
    reply.set_std_err("error: you are not a quota administrator!");
    return;
  }

  if (!(mVid.prot != "sss") && !mVid.isLocalhost()) {
    reply.set_retc(EPERM);
    reply.set_std_err("error: you cannot set quota from storage node with 'sss' authentication!");
    return;
  }

  eos_notice("quota set");
  std::string msg;
  struct stat buf {};

  if (space.empty()) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: command not properly formatted");
    return;
  }

  if (gOFS->_stat(space.c_str(), &buf, mError, mVid, nullptr)) {
    reply.set_retc(ENOENT);
    reply.set_std_err("error: quota directory does not exist");
    return;
  }

  if (set.uid().length() && set.gid().length()) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: you need specify either a uid or a gid");
    return;
  }

  if (set.uid().length()) {
    id_type = Quota::IdT::kUid;
    id = eos::common::Mapping::UserNameToUid(set.uid(), errc);

    if (errc == EINVAL) {
      reply.set_retc(EINVAL);
      reply.set_std_err("error: unable to translate uid=" + set.uid());
      return;
    }
  } else if (set.gid().length()) {
    id_type = Quota::IdT::kGid;
    id = eos::common::Mapping::GroupNameToGid(set.gid(), errc);

    if (errc == EINVAL) {
      reply.set_retc(EINVAL);
      reply.set_std_err("error: unable to translate gid=" + set.gid());
      return;
    }
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: no uid/gid specified for quota set");
    return;
  }

  // Deal with volume quota
  unsigned long long size = eos::common::StringConversion::GetDataSizeFromString(
                              set.maxbytes());

  if (set.maxbytes().length() && ((errno == EINVAL) || (errno == ERANGE))) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: the volume quota you specified is not a valid number");
    return;
  } else if (set.maxbytes().length()) {
    // Set volume quota
    if (!Quota::SetQuotaTypeForId(space, id, id_type, Quota::Type::kVolume, size,
                                  msg, ret_c)) {
      std_err.str(msg);
      return;
    } else {
      std_out.str(msg);
    }
  }

  // Deal with inode quota
  unsigned long long inodes = eos::common::StringConversion::GetSizeFromString(
                                set.maxinodes());

  if (set.maxinodes().length() && (errno == EINVAL)) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: the inode quota you specified is not a valid number");
    return;
  } else if (set.maxinodes().length()) {
    // Set inode quota
    if (!Quota::SetQuotaTypeForId(space, id, id_type, Quota::Type::kInode, inodes,
                                  msg, ret_c)) {
      std_err << msg;
      return;
    } else {
      std_out << msg;
    }
  }

  if ((!set.maxbytes().length()) && (!set.maxinodes().length())) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: max. bytes or max. inodes values have to be defined");
    return;
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
  reply.set_retc(ret_c);
}

//------------------------------------------------------------------------------
// Execute rm subcommand
//------------------------------------------------------------------------------
void QuotaCmd::RmSubcmd(const eos::console::QuotaProto_RmProto& rm,
                        eos::console::ReplyProto& reply)
{
  int ret_c = 0;
  XrdOucErrInfo mError;
  int errc;
  long id = 0;
  Quota::IdT id_type;// = Quota::IdT::kUid;
  gOFS->MgmStats.Add("Quota", mVid.uid, mVid.gid, 1);
  std::string space = rm.space();

  if (!space.empty()) {
    // evt. correct the space variable to be a directory path (+/)
    struct stat buf {};
    std::string sspace = space;

    if (sspace[sspace.length() - 1] != '/') {
      sspace += '/';
    }

    if (!gOFS->_stat(sspace.c_str(), &buf, mError, mVid, nullptr)) { // @note no.01
      space = sspace;
    }
  }

  bool canQuota;

  if ((!mVid.uid) || mVid.hasUid(3) || mVid.hasGid(4)) { // @note no.02
    // root and admin can set quota
    canQuota = true;
  } else {
    // figure out if the authenticated user is a quota admin
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                      __FILE__);
    eos::IContainerMD::XAttrMap attrmap;

    if (space[0] != '/') {
      // take the proc directory
      space = gOFS->MgmProcPath.c_str();
    } else {
      // effectively check ACLs on the quota node directory if it can be retrieved
      std::string quota_node_path = Quota::GetResponsibleSpaceQuotaPath(space);

      if (quota_node_path.length()) {
        space = quota_node_path;
      }
    }

    // ACL and permission check
    Acl acl(space.c_str(), mError, mVid, attrmap, false); // @note no.01
    canQuota = acl.CanSetQuota();
  }

  if (!canQuota) {
    reply.set_retc(EPERM);
    reply.set_std_err("error: you are not a quota administrator!");
    return;
  }

  if (!(mVid.prot != "sss") && !mVid.isLocalhost()) {
    reply.set_retc(EPERM);
    reply.set_std_err("error: you cannot set quota from storage node with 'sss' authentication!");
    return;
  }

  if (space.empty()) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: command not properly formatted");
    return;
  }

  if (rm.uid().length() && rm.gid().length()) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: you need specify either a uid or a gid");
    return;
  }

  if (rm.uid().length()) {
    id_type = Quota::IdT::kUid;
    id = eos::common::Mapping::UserNameToUid(rm.uid(), errc);

    if (errc == EINVAL) {
      reply.set_std_err("error: unable to translate uid=" + rm.uid());
      reply.set_retc(EINVAL);
      return;
    }
  } else if (rm.gid().length()) {
    id_type = Quota::IdT::kGid;
    id = eos::common::Mapping::GroupNameToGid(rm.gid(), errc);

    if (errc == EINVAL) {
      reply.set_std_err("error: unable to translate gid=" + rm.gid());
      reply.set_retc(EINVAL);
      return;
    }
  } else {
    reply.set_std_err("error: no uid/gid specified for quota remove");
    reply.set_retc(EINVAL);
    return;
  }

  std::string ret_msg;

  if (rm.type() == eos::console::QuotaProto::RmProto::NONE) {
    if (Quota::RmQuotaForId(space, id, id_type, ret_msg, ret_c)) {
      reply.set_std_out(ret_msg);  //std_out = ret_msg;
    } else {
      reply.set_std_err(ret_msg);  //std_err = ret_msg;
    }
  } else if (rm.type() == eos::console::QuotaProto::RmProto::VOLUME) {
    if (Quota::RmQuotaTypeForId(space, id, id_type, Quota::Type::kVolume, ret_msg,
                                ret_c)) {
      reply.set_std_out(ret_msg);  //std_out = ret_msg;
    } else {
      reply.set_std_err(ret_msg);  //std_err = ret_msg;
    }
  } else if (rm.type() == eos::console::QuotaProto::RmProto::INODE) {
    if (Quota::RmQuotaTypeForId(space, id, id_type, Quota::Type::kInode, ret_msg,
                                ret_c)) {
      reply.set_std_out(ret_msg);  //std_out = ret_msg;
    } else {
      reply.set_std_err(ret_msg);  //std_err = ret_msg;
    }
  }

//  reply.set_std_out(ret_msg);
//  reply.set_std_err(ret_msg);
  reply.set_retc(ret_c);
}

//------------------------------------------------------------------------------
// Execute rmnode subcommand
//------------------------------------------------------------------------------
void QuotaCmd::RmnodeSubcmd(const eos::console::QuotaProto_RmnodeProto& rmnode,
                            eos::console::ReplyProto& reply)
{
  eos_notice("quota rmnode");

  if ((mVid.uid != 0) && (mVid.uid != 3)) {
    reply.set_retc(EPERM);
    reply.set_std_err("error: you cannot remove quota nodes without having the root or adm role!");
    return;
  }

  if (rmnode.space().empty()) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: no quota path specified");
    return;
  }

  int ret_c;
  std::string ret_msg;

  if (Quota::RmSpaceQuota(rmnode.space(), ret_msg, ret_c)) {
    reply.set_retc(ret_c);
    reply.set_std_out(ret_msg);
  } else {
    reply.set_retc(ret_c);
    reply.set_std_err(ret_msg);
  }
}

EOSMGMNAMESPACE_END
