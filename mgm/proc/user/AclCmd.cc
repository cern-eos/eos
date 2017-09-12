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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "AclCmd.hh"
#include "mgm/Acl.hh"
#include "mgm/XrdMgmOfs.hh"
#include "common/StringTokenizer.hh"
#include <unistd.h>
#include <getopt.h>
#include <functional>
#include <algorithm>
#include <queue>
#include <sstream>

extern XrdMgmOfs* gOFS;

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Method triggering the execution of the command and returning a future object
//------------------------------------------------------------------------------
std::future<eos::console::ReplyProto>
AclCmd::Execute()
{
  auto reply_future = mPromise.get_future();
  std::thread([&]() {
    return ProcessRequest();
  }).detach();
  return reply_future;
}

//------------------------------------------------------------------------------
// Process request
//------------------------------------------------------------------------------
void
AclCmd::ProcessRequest()
{
  using eos::console::AclProto_OpType;
  eos::console::ReplyProto reply;
  eos::console::AclProto acl = mReqProto.acl();

  if (acl.op() == AclProto_OpType::AclProto_OpType_LIST) {
    std::string acl_val;
    GetAcls(acl.path(), acl_val, acl.sys_acl());

    if (acl_val.empty()) {
      std::string err_msg = "error: ";
      err_msg += std::strerror(ENODATA);
      reply.set_std_err(err_msg);
      reply.set_retc(ENODATA);
    } else {
      reply.set_std_out(acl_val);
      reply.set_retc(0);
    }
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not implemented yet");
  }

  mPromise.set_value(std::move(reply));
}

//------------------------------------------------------------------------------
// Open a proc command e.g. call the appropriate user or admin commmand and
// store the output in a resultstream of in case of find in temporary output
// files.
//------------------------------------------------------------------------------
int
AclCmd::open(const char* path, const char* info,
             eos::common::Mapping::VirtualIdentity& vid,
             XrdOucErrInfo* error)
{
  mFuture = Execute();
  return SFS_OK;
}

//------------------------------------------------------------------------------
// Read a part of the result stream created during open
//------------------------------------------------------------------------------
int
AclCmd::read(XrdSfsFileOffset offset, char* buff, XrdSfsXferSize blen)
{
  if (!mHasResponse) {
    std::future_status status = mFuture.wait_for(std::chrono::seconds(5));

    if (status != std::future_status::ready) {
      // Stall the client requst
      eos_err("future is not ready yet, return 0");
      return 0;
    } else {
      mHasResponse = true;
      eos::console::ReplyProto reply = mFuture.get();
      std::ostringstream oss;
      oss << "mgm.proc.stdout=" << reply.std_out()
          << "&mgm.proc.stderr=" << reply.std_err()
          << "&mgm.proc.retc=" << reply.retc();
      mTmpResp = oss.str();
      eos_info("future is ready, response is: %s", oss.str().c_str());
    }
  }

  if ((size_t)offset < mTmpResp.length()) {
    size_t cpy_len = std::min((size_t)(mTmpResp.size() - offset), (size_t)blen);
    memcpy(buff, mTmpResp.data() + offset, cpy_len);
    return cpy_len;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Get sys.acl and user.acl for a given path
//------------------------------------------------------------------------------
void
AclCmd::GetAcls(const std::string& path, std::string& acl, bool is_sys)
{
  std::string acl_key;
  XrdOucString value;
  XrdOucErrInfo error;

  if (is_sys) {
    acl_key = "sys.acl";
  } else {
    acl_key = "user.acl";
  }

  if (gOFS->_attr_get(path.c_str(), error, *pVid, 0, acl_key.c_str(), value)) {
    value = "";
  }

  acl = value.c_str();
  Acl::ConvertIds(acl, true);
}


/*
//------------------------------------------------------------------------------
// Convert ACL bitmask to string representation
//------------------------------------------------------------------------------
std::string
AclCmd::AclBitmaskToString(unsigned short int in) const
{
  std::string ret = "";

  if (in & AclCmd::R) {
    ret.append("r");
  }

  if (in & AclCmd::W) {
    ret.append("w");
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

  return ret;
}

//------------------------------------------------------------------------------
// Get ACL rule from string by creating a pair of identifier for the ACL and
// the bitmask representation
//------------------------------------------------------------------------------
Rule AclCmd::AclRuleFromString(const std::string& single_acl) const
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
      rule_int = rule_int | AclCmd::W;
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

      break;
    default:
      break;
    }
  }

  ret.second = rule_int;
  return ret;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void AclCmd::GenerateRuleMap(const std::string& acl_string)
{
  if (!mRules.empty()) {
    mRules.clear();
  }

  if (acl_string.empty()) {
    return;
  }

  size_t curr_pos = 0, pos = 0;

  while (1) {
    pos = acl_string.find(',', curr_pos);

    if (pos == std::string::npos) {
      pos = acl_string.length();
    }

    std::string single_acl = std::string(acl_string.begin() + curr_pos,
                                         acl_string.begin() + pos);
    Rule temp = AclRuleFromString(single_acl);
    mRules[temp.first] = temp.second;
    curr_pos = pos + 1;

    if (curr_pos > acl_string.length()) {
      break;
    }
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void AclCmd::SetAclString(const std::string& result,
                       std::string& which,
                       const char* type)
{
  which = "";
  size_t pos_begin = result.find(type);

  if (pos_begin != std::string::npos) {
    unsigned int pos_end = result.find("\n", pos_begin);
    // 2 comes from the leading " and :
    which = std::string(result.begin() + strlen(type) + 2 + pos_begin,
                        result.begin() + pos_end - 1);
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool AclCmd::CheckCorrectId(const std::string& id)
{
  std::string allowed_chars =
    "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_-";

  if ((id.at(0) == 'u' && id.at(1) == ':') ||
      (id.at(0) == 'g' && id.at(1) == ':')) {
    return id.find_first_not_of(allowed_chars, 2) == std::string::npos;
  }

  if (id.find("egroup") == 0 && id.at(6) == ':') {
    return id.find_first_not_of(allowed_chars, 7) == std::string::npos;
  }

  return false;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool AclCmd::GetRuleInt(const std::string& rule, bool set)
{
  unsigned short int ret = 0, add_ret = 0, rm_ret = 0;
  bool lambda_happen = false;
  auto add_lambda = [&add_ret, &ret](AclCmd::ACLPos pos) {
    add_ret = add_ret | pos;
    ret = ret | pos;
  };
  auto remove_lambda = [&rm_ret, &ret](AclCmd::ACLPos pos) {
    rm_ret = rm_ret | pos;
    ret = ret & (~pos);
  };
  std::function<void(AclCmd::ACLPos)>curr_lambda = add_lambda;

  for (auto flag = rule.begin(); flag != rule.end(); ++flag) {
    // Check for add/rm rules
    if (*flag == '-') {
      curr_lambda = remove_lambda;
      lambda_happen = true;
      continue;
    }

    if (*flag == '+') {
      auto temp_iter = flag + 1;

      if (temp_iter == rule.end()) {
        continue;
      }

      if (*temp_iter != 'd' && *temp_iter != 'u') {
        lambda_happen = true;
        curr_lambda = add_lambda;
        continue;
      }
    }

    // If there is no +/- character happend in not set mode
    // This is not correct behaviour hence returning false
    if (!set && !lambda_happen) {
      return false;
    }

    // Check for flags
    if (*flag == 'r') {
      curr_lambda(AclCmd::R);
      continue;
    }

    if (*flag == 'w') {
      curr_lambda(AclCmd::W);
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

      if (flag == rule.end()) {
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

  // Ret is representing mask for flags which are goint to be added or removed
  // As if we need to remove flags that flags will be setted to 1 in rm_ret and
  // setted to 0 in ret and vice versa for adding flags.
  m_add_rule = ((add_ret == 0) ? 0 : ret & add_ret);
  m_rm_rule  = ((rm_ret == 0) ? 0 : ~ret & rm_ret);
  return true;
error_label:
  m_error_message = "Rule not correct!";
  return false;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void AclCmd::ApplyRule()
{
  unsigned short temp_rule = 0;

  if (!m_set && mRules.find(m_id) != mRules.end()) {
    temp_rule = mRules[m_id];
  }

  if (m_add_rule != 0) {
    temp_rule = temp_rule | m_add_rule;
  }

  if (m_rm_rule != 0) {
    temp_rule = temp_rule & (~m_rm_rule);
  }

  mRules[m_id] = temp_rule;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
std::string AclCmd::MapToAclString()
{
  std::string ret = "";

  for (auto item = mRules.begin(); item != mRules.end(); item++) {
    if ((*item).second != 0) {
      ret += (*item).first + ":" + AclBitmaskToString((*item).second) + ",";
    }
  }

  // Removing last ','
  if (ret != "") {
    ret = ret.substr(0,  ret.size() - 1);
  }

  return ret;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool AclCmd::ParseRule(const std::string& input)
{
  size_t pos_del_first, pos_del_last, pos_equal;
  pos_del_first = input.find(":");
  pos_del_last  = input.rfind(":");
  pos_equal     = input.find("=");
  std::string id, rule;

  if ((pos_del_first == pos_del_last) && (pos_equal != std::string::npos)) {
    // u:id=rw+x
    m_set = true;
    // Check if id and rule are correct
    id = std::string(input.begin(), input.begin() + pos_equal);

    if (!CheckCorrectId(id)) {
      m_error_message = "Rule: Incorrect format of id!";
      return false;
    }

    m_id = id;
    rule = std::string(input.begin() + pos_equal + 1, input.end());

    if (!GetRuleInt(rule, true)) {
      m_error_message = "Rule: Rule is not in correct format!";
      return false;
    }

    // if( !id_lambda(pos_equal))             return false;
    // if( !rule_lambda(pos_equal + 1, true)) return false;
    return true;
  } else {
    if (pos_del_first != pos_del_last &&
        pos_del_first != std::string::npos &&
        pos_del_last  != std::string::npos) {
      m_set = false;
      // u:id:+rwx
      // Check if id and rule are correct
      id = std::string(input.begin(), input.begin() + pos_del_last);

      if (!CheckCorrectId(id)) {
        m_error_message = "Rule: Incorrect format of id!";
        return false;
      }

      m_id = id;
      rule = std::string(input.begin() + pos_del_last + 1, input.end());

      if (!GetRuleInt(rule, false)) {
        m_error_message = "Rule: Rule is not in correct format!";
        return false;
      }

      //if(!id_lambda(pos_del_last))            return false;
      //if(!rule_lambda(pos_del_last+1, false)) return false;
      return true;
    } else {
      // Error
      m_error_message = "Rule is not good!";
      return false;
    }
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool AclCmd::Action(bool apply , const std::string& path)
{
  if (apply) {
    if (!GetAclStringsForPath(path)) {
      return false;
    }

    if (m_sys_acl) {
      GenerateRuleMap(m_sys_acl_string);
    } else {
      GenerateRuleMap(m_usr_acl_string);
    }

    ApplyRule();
    return false;
  } else {
    if (!GetAclStringsForPath(path)) {
      return false;
    }

    std::cout << path << '\t';

    if (m_usr_acl) {
      std::cout << "usr: " << m_usr_acl_string << std::endl;
    } else {
      std::cout << "sys: " << m_sys_acl_string << std::endl;
    }

    return true;
  }
}
*/
EOSMGMNAMESPACE_END
