//------------------------------------------------------------------------------
// File: QuotaCmd.cc
// Author: Fabio Luchetti - CERN
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
  eos::console::QuotaProto::SubcmdCase subcmd = quota.subcmd_case();

  if (subcmd == eos::console::QuotaProto::kLsuser) {
    LsuserSubcmd(quota.lsuser(), reply);
  } else if (subcmd == eos::console::QuotaProto::kLs) {
    LsSubcmd(quota.ls(), reply);
  } else if (subcmd == eos::console::QuotaProto::kSet) {
    SetSubcmd(quota.set(), reply);
  } else if (subcmd == eos::console::QuotaProto::kRm) {
    RmSubcmd(quota.rm(), reply);
  } else if (subcmd == eos::console::QuotaProto::kRmnode) {
    RmnodeSubcmd(quota.rmnode(), reply);
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not supported");
  }

  return reply;
}

//------------------------------------------------------------------------------
// Execute lsuser subcommand
//------------------------------------------------------------------------------
void QuotaCmd::LsuserSubcmd(const eos::console::QuotaProto_LsuserProto& lsuser, eos::console::ReplyProto& reply) {

  std::string std_out, std_err;
  int ret_c;

  XrdOucErrInfo mError;


  gOFS->MgmStats.Add("Quota", mVid.uid, mVid.gid, 1);
  std::string space = lsuser.space();

  if (!space.empty()) {
    // evt. correct the space variable to be a directory path (+/)
    struct stat buf{};
    std::string sspace = space;
    if (sspace[sspace.length() - 1] != '/') {
      sspace += '/';
    }
    if (!gOFS->_stat(sspace.c_str(), &buf, mError, mVid, 0)) { // @note no.01 Where is the info in mError is going?
      space = sspace;
    }
  }

  eos_notice("quota ls (user)");

  bool is_ok;
  XrdOucString out {""};

  is_ok = Quota::PrintOut(space, out, mVid.uid, -1, lsuser.format(), true);

  if (is_ok && out.length()) {
    if (!lsuser.format()) {
      std_out += ("\nBy user:" + out).c_str();
    } else {
      std_out += out.c_str();
    }
  } else {
    if (!is_ok) {
      std_err += out.c_str();
      ret_c = EINVAL;
    }
  }

  out = "";
  is_ok = Quota::PrintOut(space, out, -1, mVid.gid, lsuser.format(), true);
  // mDoSort = false; @note no.02 was there in the old implementation, but looks like it is not actually needed anymore

  if (is_ok && out != "") {
    if (!lsuser.format()) {
      std_out += ("\nBy group:" + out).c_str();
    } else {
      std_out += out.c_str();
    }
  } else {
    if (!is_ok) {
      std_err += out.c_str();
      ret_c = EINVAL;
    }
  }

  reply.set_std_out(std_out);
  reply.set_std_err(std_err);
  reply.set_retc(ret_c);

}

//------------------------------------------------------------------------------
// Execute ls subcommand
//------------------------------------------------------------------------------
void QuotaCmd::LsSubcmd(const eos::console::QuotaProto_LsProto& ls, eos::console::ReplyProto& reply) {

  std::string std_out, std_err;
  int ret_c;

  XrdOucErrInfo mError;

  int errc;

  gOFS->MgmStats.Add("Quota", mVid.uid, mVid.gid, 1);
  std::string space = ls.space();

  if (!space.empty()) {
    // evt. correct the space variable to be a directory path (+/)
    struct stat buf{};
    std::string sspace = space;
    if (sspace[sspace.length() - 1] != '/') {
      sspace += '/';
    }
    if (!gOFS->_stat(sspace.c_str(), &buf, mError, mVid, 0)) { // @note no.01
      space = sspace;
    }
  }


  bool canQuota;

  if ((!mVid.uid) || mVid.hasUid(3) || mVid.hasGid(4)) { // @note no.03 before 'vid' was used, using 'mVid' now
    // root and admin can set quota
    canQuota = true;
  } else {
    // figure out if the authenticated user is a quota admin
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
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
    reply.set_std_err("error: you are not a quota administrator!\"");
    return;
  }


  eos_notice("quota ls");

  XrdOucString out1 {""};
  XrdOucString out2 {""};

  long long int uid = (ls.uid().empty() ? -1LL : eos::common::Mapping::UserNameToUid(ls.uid(), errc));
  long long int gid = (ls.gid().empty() ? -1LL : eos::common::Mapping::GroupNameToGid(ls.gid(), errc));

  if ((uid != -1LL) && (gid != -1LL)) {
    // Print both uid and gid info
    if (!Quota::PrintOut(space, out1, uid, -1LL, ls.format(), !ls.printid())) {
      std_err = out1.c_str();
      ret_c = EINVAL;
    } else {
      if (!Quota::PrintOut(space, out2, -1LL, gid, ls.format(), !ls.printid())) {
        std_err = out2.c_str();
        ret_c = EINVAL;
      } else {
        std_out = (out1 + out2).c_str();
      }
    }
  } else {
    // Either uid or gid is printed
    if (Quota::PrintOut(space, out1, uid, gid, ls.format(), !ls.printid())) {
      std_out = out1.c_str();
    } else {
      std_err = out1.c_str();
      ret_c = EINVAL;
    }
  }

  reply.set_std_out(std_out);
  reply.set_std_err(std_err);
  reply.set_retc(ret_c);

}

//------------------------------------------------------------------------------
// Execute set subcommand
//------------------------------------------------------------------------------
void QuotaCmd::SetSubcmd(const eos::console::QuotaProto_SetProto& set, eos::console::ReplyProto& reply) {

  std::string std_out, std_err;
  int ret_c;

  XrdOucErrInfo mError;

  int errc;
  long id = 0;
  Quota::IdT id_type; // = Quota::IdT::kUid;


  gOFS->MgmStats.Add("Quota", mVid.uid, mVid.gid, 1);
  std::string space = set.space();

  if (!space.empty()) {
    // evt. correct the space variable to be a directory path (+/)
    struct stat buf{};
    std::string sspace = space;
    if (sspace[sspace.length() - 1] != '/') {
      sspace += '/';
    }
    if (!gOFS->_stat(sspace.c_str(), &buf, mError, mVid, 0)) { // @note no.01
      space = sspace;
    }
  }


  bool canQuota;

  if ((!mVid.uid) || mVid.hasUid(3) || mVid.hasGid(4)) { // @note no.03
    // root and admin can set quota
    canQuota = true;
  } else {
    // figure out if the authenticated user is a quota admin
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
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


  if (space.empty()) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: command not properly formatted");
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
  unsigned long long size = eos::common::StringConversion::GetDataSizeFromString(set.maxbytes());
  if (set.maxbytes().length() && ((errno == EINVAL) || (errno == ERANGE))) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: the volume quota you specified is not a valid number");
    return;
  } else if (set.maxbytes().length()) {
    // Set volume quota
    if (!Quota::SetQuotaTypeForId(space, id, id_type, Quota::Type::kVolume, size, msg, ret_c)) {
      std_err = msg;
      return;
    } else {
      std_out = msg;
    }
  }

  // Deal with inode quota
  unsigned long long inodes = eos::common::StringConversion::GetSizeFromString(set.maxinodes());
  if (set.maxinodes().length() && (errno == EINVAL)) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: the inode quota you specified is not a valid number");
    return;
  } else if (set.maxinodes().length()) {
    // Set inode quota
    if (!Quota::SetQuotaTypeForId(space, id, id_type, Quota::Type::kInode, inodes, msg, ret_c)) {
      std_err += msg;
      return;
    } else {
      std_out += msg;
    }
  }

  if ((!set.maxbytes().length()) && (!set.maxinodes().length())) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: max. bytes or max. inodes values have to be defined");
    return;
  }

  reply.set_std_out(std_out);
  reply.set_std_err(std_err);
  reply.set_retc(ret_c);

}

//------------------------------------------------------------------------------
// Execute rm subcommand
//------------------------------------------------------------------------------
void QuotaCmd::RmSubcmd(const eos::console::QuotaProto_RmProto& rm, eos::console::ReplyProto& reply)
{

//  std::string std_out, std_err;
  int ret_c;

  XrdOucErrInfo mError;

  int errc;
  long id = 0;
  Quota::IdT id_type;// = Quota::IdT::kUid;


  gOFS->MgmStats.Add("Quota", mVid.uid, mVid.gid, 1);
  std::string space = rm.space();

  if (!space.empty()) {
    // evt. correct the space variable to be a directory path (+/)
    struct stat buf{};
    std::string sspace = space;
    if (sspace[sspace.length() - 1] != '/') {
      sspace += '/';
    }
    if (!gOFS->_stat(sspace.c_str(), &buf, mError, mVid, 0)) { // @note no.01
      space = sspace;
    }
  }


  bool canQuota;

  if ((!mVid.uid) || mVid.hasUid(3) || mVid.hasGid(4)) { // @note no.02
    // root and admin can set quota
    canQuota = true;
  } else {
    // figure out if the authenticated user is a quota admin
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
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
    id = eos::common::Mapping::UserNameToUid(rm.uid().c_str(), errc);
    if (errc == EINVAL) {
      reply.set_std_err("error: unable to translate uid=" + rm.uid());
      reply.set_retc(EINVAL);
      return;
    }
  } else if (rm.gid().length()) {
    id_type = Quota::IdT::kGid;
    id = eos::common::Mapping::GroupNameToGid(rm.gid().c_str(), errc);
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
    if (Quota::RmQuotaForId(space, id, id_type, ret_msg, ret_c))
      reply.set_std_out(ret_msg); //std_out = ret_msg;
    else
      reply.set_std_err(ret_msg); //std_err = ret_msg;
  }
  else if (rm.type() == eos::console::QuotaProto::RmProto::VOLUME) {
    if (Quota::RmQuotaTypeForId(space, id, id_type, Quota::Type::kVolume, ret_msg, ret_c))
      reply.set_std_out(ret_msg); //std_out = ret_msg;
    else
      reply.set_std_err(ret_msg); //std_err = ret_msg;
  }
  else if (rm.type() == eos::console::QuotaProto::RmProto::INODE) {
    if (Quota::RmQuotaTypeForId(space, id, id_type, Quota::Type::kInode, ret_msg, ret_c))
      reply.set_std_out(ret_msg); //std_out = ret_msg;
    else
      reply.set_std_err(ret_msg); //std_err = ret_msg;
  }

//  reply.set_std_out(ret_msg);
//  reply.set_std_err(ret_msg);
  reply.set_retc(ret_c);

}

//------------------------------------------------------------------------------
// Execute rmnode subcommand
//------------------------------------------------------------------------------
void QuotaCmd::RmnodeSubcmd(const eos::console::QuotaProto_RmnodeProto& rmnode, eos::console::ReplyProto& reply)
{

  eos_notice("quota rmnode");

  if (mVid.uid != 0) {
    reply.set_retc(EPERM);
    reply.set_std_err("error: you cannot remove quota nodes without having the root role!");
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
