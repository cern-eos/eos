//------------------------------------------------------------------------------
//! @file AclCmd.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "AclCmd.hh"
#include "mgm/Acl.hh"
#include "mgm/XrdMgmOfs.hh"
#include "common/StringTokenizer.hh"
#include "common/ErrnoToString.hh"
#include "namespace/Prefetcher.hh"
#include <unistd.h>
#include <functional>
#include <queue>
//#include <getopt.h>
//#include <algorithm>
//#include <sstream>
//extern XrdMgmOfs* gOFS;

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Process request
//------------------------------------------------------------------------------
eos::console::ReplyProto
AclCmd::ProcessRequest() noexcept
{
  using eos::console::AclProto;
  eos::console::ReplyProto reply;
  eos::console::AclProto acl = mReqProto.acl();
  std::string err_msg;

  if (acl.op() == AclProto::LIST) {
    std::string acl_val;
    GetAcls(acl.path(), acl_val, acl.sys_acl());

    if (acl_val.empty()) {
      mErr = "error: ";
      mErr += eos::common::ErrnoToString(ENODATA);
      reply.set_std_err(mErr);
      reply.set_retc(ENODATA);
    } else {
      // Convert to username if possible, ignore errors
      (void) Acl::ConvertIds(acl_val, true);
      reply.set_std_out(acl_val);
      reply.set_retc(0);
    }
  } else if (acl.op() == AclProto::MODIFY) {
    int retc = ModifyAcls(acl);
    reply.set_retc(retc);
    reply.set_std_out("");

    if (retc) {
      reply.set_std_err(mErr);
    }
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not supported");
  }

  return reply;
}

//------------------------------------------------------------------------------
// Get sys.acl and user.acl for a given path
//------------------------------------------------------------------------------
void
AclCmd::GetAcls(const std::string& path, std::string& acl, bool is_sys,
                bool take_lock)
{
  XrdOucString value;
  XrdOucErrInfo error;
  std::string acl_key = (is_sys ? "sys.acl" : "user.acl");

  if (gOFS->_attr_get(path.c_str(), error, mVid, 0, acl_key.c_str(), value,
                      take_lock)) {
    value = "";
  }

  acl = value.c_str();
}

//------------------------------------------------------------------------------
// Modify acls
//------------------------------------------------------------------------------
int
AclCmd::ModifyAcls(const eos::console::AclProto& acl)
{
  XrdOucString m_err {""};

  // Parse acl modification command into bitmask rule format
  if (!ParseRule(acl.rule())) {
    mErr = "error: failed to parse input rule or unknown id";
    return EINVAL;
  }

  std::list<std::string> paths;

  eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView, acl.path(), false);

  eos::common::RWMutexWriteLock ns_wr_lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);

  if (acl.recursive()) {
    // @todo (esindril): get list of all directories recursively
    XrdOucErrInfo error;
    std::map<std::string, std::set<std::string>> dirs;
    m_err.erase();
    (void) gOFS->_find(acl.path().c_str(), error, m_err, mVid, dirs, nullptr,
                       nullptr, true, 0, false, 0, nullptr, false);

    if (m_err.length()) {
      mErr = m_err.c_str();
      return EINVAL;
    }

    // Save all the directories in the current subtree
    for (const auto& elem : dirs) {
      paths.push_back(elem.first);
    }
  } else {
    paths.push_back(acl.path());
  }

  std::string acl_key = (acl.sys_acl() ? "sys.acl" : "user.acl");
  RuleMap rule_map;
  std::string dir_acls, new_acl_val;
  XrdOucErrInfo error;

  for (const auto& elem : paths) {
    GetAcls(elem, dir_acls, acl.sys_acl(), false);
    GenerateRuleMap(dir_acls, rule_map);
    // ACL position is 1-indexed as 0 is the default numeric protobuf val
    auto [err, acl_pos] = GetRulePosition(rule_map.size(), acl.position());
    if (err) {
      mErr = "error: rule position cannot be met!";
      return err;
    }

    ApplyRule(rule_map,acl_pos);
    new_acl_val = GenerateAclString(rule_map);

    // Set xattr without taking the namespace lock
    if (gOFS->_attr_set(elem.c_str(), error, mVid, 0, acl_key.c_str(),
                        new_acl_val.c_str(), false)) {
      mErr = "error: failed to set new acl for path=";
      mErr += elem.c_str();
      eos_err("%s", mErr.c_str());
      return errno;
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// Get ACL rule from string by creating a pair of identifier for the ACL and
// the bitmask representation
//------------------------------------------------------------------------------
Rule AclCmd::GetRuleFromString(const std::string& single_acl)
{
  Rule ret;
  auto acl_delimiter = single_acl.rfind(':');
  ret.first = std::string(single_acl.begin(),
                          single_acl.begin() + acl_delimiter);
  unsigned short rule_int = 0;

  for (auto i = acl_delimiter + 1, size = single_acl.length(); i < size; ++i) {
    switch (single_acl.at(i)) {
    case 'r' :
      rule_int = rule_int | AclCmd::R;
      break;

    case 'w' :

      // Check for wo case
      if ((i + 1 < size) && single_acl.at(i + 1) == 'o') {
        i++;
        rule_int = rule_int | AclCmd::WO;
      } else {
        rule_int = rule_int | AclCmd::W;
      }

      break;

    case 'x' :
      rule_int = rule_int | AclCmd::X;
      break;

    case 'm' :
      rule_int = rule_int | AclCmd::M;
      break;

    case 'q' :
      rule_int = rule_int | AclCmd::Q;
      break;

    case 'c' :
      rule_int = rule_int | AclCmd::C;
      break;

    case '+' :
      // There are only two + flags in current acl permissions +d and +u
      i++;

      if (single_acl.at(i) == 'd') {
        rule_int = rule_int | AclCmd::pD;
      } else {
        rule_int = rule_int | AclCmd::pU;
      }

      break;

    case '!' :
      i++;

      if (single_acl.at(i) == 'd') {
        rule_int = rule_int | AclCmd::nD;
      }

      if (single_acl.at(i) == 'u') {
        rule_int = rule_int | AclCmd::nU;
      }

      if (single_acl.at(i) == 'm') {
        rule_int = rule_int | AclCmd::nM;
      }

      if (single_acl.at(i) == 'r') {
        rule_int = rule_int | AclCmd::nR;
      }

      if (single_acl.at(i) == 'w') {
        rule_int = rule_int | AclCmd::nW;
      }

      if (single_acl.at(i) == 'x') {
        rule_int = rule_int | AclCmd::nX;
      }

      break;

    default:
      break;
    }
  }

  ret.second = rule_int;
  return ret;
}

//------------------------------------------------------------------------------
// Generate rule map from the string representation of the acls
//------------------------------------------------------------------------------
void
AclCmd::GenerateRuleMap(const std::string& acl_string, RuleMap& rmap)
{
  if (acl_string.empty()) {
    return;
  }

  rmap.clear();
  size_t curr_pos = 0, pos = 0;

  while (true) {
    pos = acl_string.find(',', curr_pos);

    if (pos == std::string::npos) {
      pos = acl_string.length();
    }

    std::string single_acl = std::string(acl_string.begin() + curr_pos,
                                         acl_string.begin() + pos);
    insert_or_assign(rmap, GetRuleFromString(single_acl));
    curr_pos = pos + 1;

    if (curr_pos > acl_string.length()) {
      break;
    }
  }

  return;
}

//------------------------------------------------------------------------------
// Convert acl modification command into bitmask rule format
//------------------------------------------------------------------------------
bool AclCmd::GetRuleBitmask(const std::string& input, bool set)
{
  bool lambda_happen = false;
  unsigned short int ret = 0, add_ret = 0, rm_ret = 0;
  auto add_lambda = [&add_ret, &ret](AclCmd::ACLPos pos) {
    add_ret = add_ret | pos;
    ret = ret | pos;
  };
  auto remove_lambda = [&rm_ret, &ret](AclCmd::ACLPos pos) {
    rm_ret = rm_ret | pos;
    ret = ret & (~pos);
  };
  std::function<void(AclCmd::ACLPos)>curr_lambda = add_lambda;

  for (auto flag = input.begin(); flag != input.end(); ++flag) {
    // Check for add/rm rules
    if (*flag == '-') {
      curr_lambda = remove_lambda;
      lambda_happen = true;
      continue;
    }

    if (*flag == '+') {
      auto temp_iter = flag + 1;

      if (temp_iter == input.end()) {
        continue;
      }

      if (*temp_iter != 'd' && *temp_iter != 'u') {
        lambda_happen = true;
        curr_lambda = add_lambda;
        continue;
      }
    }

    // If there is no +/- character non-"set" mode
    if (!set && !lambda_happen) {
      return false;
    }

    // Check for flags
    if (*flag == 'r') {
      curr_lambda(AclCmd::R);
      continue;
    }

    if (*flag == 'w') {
      auto temp_iter = flag;
      ++temp_iter;

      if ((temp_iter != input.end()) && (*temp_iter == 'o')) {
        curr_lambda(AclCmd::WO);
        ++flag;
      } else {
        curr_lambda(AclCmd::W);
      }

      continue;
    }

    if (*flag == 'x') {
      curr_lambda(AclCmd::X);
      continue;
    }

    if (*flag == 'm') {
      curr_lambda(AclCmd::M);
      continue;
    }

    if (*flag == 'q') {
      curr_lambda(AclCmd::Q);
      continue;
    }

    if (*flag == 'c') {
      curr_lambda(AclCmd::C);
      continue;
    }

    if (*flag == '!') {
      ++flag;

      if (flag == input.end()) {
        goto error_label;
      }

      if (*flag == 'd') {
        curr_lambda(AclCmd::nD);
        continue;
      }

      if (*flag == 'u') {
        curr_lambda(AclCmd::nU);
        continue;
      }

      if (*flag == 'm') {
        curr_lambda(AclCmd::nM);
        continue;
      }

      if (*flag == 'r') {
        curr_lambda(AclCmd::nR);
        continue;
      }

      if (*flag == 'w') {
        curr_lambda(AclCmd::nW);
        continue;
      }

      if (*flag == 'x') {
        curr_lambda(AclCmd::nX);
        continue;
      }

      goto error_label;
    }

    if (*flag == '+') {
      ++flag;

      if (*flag == 'd') {
        curr_lambda(AclCmd::pD);
        continue;
      }

      if (*flag == 'u') {
        curr_lambda(AclCmd::pU);
        continue;
      }
    }

    goto error_label;
  }

  // Set the mask of flags which are going to be added or removed
  mAddRule = ((add_ret == 0) ? 0 : ret & add_ret);
  mRmRule  = ((rm_ret == 0) ? 0 : ~ret & rm_ret);
  return true;
error_label:
  return false;
}

//------------------------------------------------------------------------------
// Parse command line (modification) rule given by the client
//------------------------------------------------------------------------------
bool AclCmd::ParseRule(const std::string& input)
{
  size_t pos_del_first, pos_del_last, pos_equal;
  pos_del_first = input.find(":");
  pos_del_last  = input.rfind(":");
  pos_equal     = input.find("=");
  std::string id, srule;

  if ((pos_del_first == pos_del_last) && (pos_equal != std::string::npos)) {
    // u:id=rw+x | g:id=rw+x
    mSet = true;
    // Check if id and rule are correct
    id = std::string(input.begin(), input.begin() + pos_equal);

    if (!CheckCorrectId(id)) {
      return false;
    }

    // Convert it to numeric format, add dummy ":r" and then remove it so that
    // the format is what ConvertIds expects
    id += ":r";

    if (Acl::ConvertIds(id, false)) {
      return false;
    }

    id = id.erase(id.rfind(':'));
    mId = id;
    eos_info("mId=%s", mId.c_str());
    srule = std::string(input.begin() + pos_equal + 1, input.end());

    if (!GetRuleBitmask(srule, mSet)) {
      mErr = "error: failed to get input rule as bitmask";
      return false;
    }
  } else {
    if ((pos_del_first != pos_del_last) &&
        (pos_del_first != std::string::npos) &&
        (pos_del_last  != std::string::npos)) {
      mSet = false;
      // u:id:+rw  | g:id:rw+x
      // Check if id and rule are correct
      id = std::string(input.begin(), input.begin() + pos_del_last);

      if (!CheckCorrectId(id)) {
        mErr = "error: input rule has incorrect format for id";
        return false;
      }

      // Convert it to numeric format, add dummy ":r" and then remove it so that
      // the format is what ConvertIds expects
      id += ":r";

      if (Acl::ConvertIds(id, false)) {
        return false;
      }

      id = id.erase(id.rfind(':'));
      mId = id;
      srule = std::string(input.begin() + pos_del_last + 1, input.end());

      if (!GetRuleBitmask(srule, mSet)) {
        mErr = "error: failed to get input rule as bitmask";
        return false;
      }
    } else {
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Check if id has the correct format
//------------------------------------------------------------------------------
bool
AclCmd::CheckCorrectId(const std::string& id) const
{
  std::string allowed_chars =
    "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_-";

  if ((id.at(0) == 'u' && id.at(1) == ':') ||
      (id.at(0) == 'k' && id.at(1) == ':') ||
      (id.at(0) == 'g' && id.at(1) == ':')) {
    return id.find_first_not_of(allowed_chars, 2) == std::string::npos;
  }

  if (id.find("egroup") == 0 && id.at(6) == ':') {
    return id.find_first_not_of(allowed_chars, 7) == std::string::npos;
  }

  return false;
}

//------------------------------------------------------------------------------
// Apply client modification rule(s) to the acls of the current entry
//------------------------------------------------------------------------------
void AclCmd::ApplyRule(RuleMap& rules, size_t pos)
{
  unsigned short temp_rule = 0;

  if (!mSet) {
    auto it = std::find_if(rules.begin(),
                           rules.end(),
                           [&](const Rule& rule) -> bool {
                             return rule.first == mId;
                           });
    if (it != rules.end()) {
      temp_rule = it->second;
    }
  }

  if (mAddRule != 0) {
    temp_rule = temp_rule | mAddRule;
  }

  if (mRmRule != 0) {
    temp_rule = temp_rule & (~mRmRule);
  }

  if (pos != 0) {
    auto [it, err] = get_iterator(rules, pos);
    if (err != 0) {
      mErr = "Invalid position of rule, errc=" + std::to_string(err);
    }
    insert_or_assign(rules, mId, temp_rule, it, true);
    return;
  }

  insert_or_assign(rules, mId, temp_rule);
}

//------------------------------------------------------------------------------
// Generate acl string representation from a rule map
//------------------------------------------------------------------------------
std::string
AclCmd::GenerateAclString(const RuleMap& rmap)
{
  std::string ret = "";

  for (const auto& elem : rmap) {
    if (elem.second != 0) {
      ret += elem.first + ":" + AclCmd::AclBitmaskToString(elem.second) + ",";
    }
  }

  // Remove last ','
  if (ret != "") {
    ret = ret.substr(0, ret.size() - 1);
  }

  return ret;
}

//------------------------------------------------------------------------------
// Convert ACL bitmask to string representation
//------------------------------------------------------------------------------
std::string
AclCmd::AclBitmaskToString(const unsigned short int in)
{
  std::string ret = "";

  if (in & AclCmd::R) {
    ret.append("r");
  }

  if (in & AclCmd::W) {
    ret.append("w");
  }

  if (in & AclCmd::WO) {
    ret.append("wo");
  }

  if (in & AclCmd::X) {
    ret.append("x");
  }

  if (in & AclCmd::M) {
    ret.append("m");
  }

  if (in & AclCmd::nM) {
    ret.append("!m");
  }

  if (in & AclCmd::nD) {
    ret.append("!d");
  }

  if (in & AclCmd::pD) {
    ret.append("+d");
  }

  if (in & AclCmd::nU) {
    ret.append("!u");
  }

  if (in & AclCmd::pU) {
    ret.append("+u");
  }

  if (in & AclCmd::Q) {
    ret.append("q");
  }

  if (in & AclCmd::C) {
    ret.append("c");
  }

  if (in & AclCmd::nR) {
    ret.append("!r");
  }

  if (in & AclCmd::nW) {
    ret.append("!w");
  }

  if (in & AclCmd::nX) {
    ret.append("!x");
  }

  return ret;
}

std::pair<int, size_t>
AclCmd::GetRulePosition(size_t rule_map_sz, size_t rule_pos)
{

  std::pair<int,size_t> result {0,0};

  // Trivial case, nothing is set
  if (!rule_map_sz && !rule_pos) {
    return result;
  }

  if (!rule_map_sz) {
    // Only valid case here is that the client asks the first position!
    if (rule_pos != 1) {
      result.first = EINVAL;
    }
  }

  if (rule_map_sz && rule_pos) {
    if (rule_pos > rule_map_sz) {
      result.first = EINVAL;
    }

    result.second = rule_pos;
  }

  return result;
}


EOSMGMNAMESPACE_END
