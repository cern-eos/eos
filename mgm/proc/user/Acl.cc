//------------------------------------------------------------------------------
//! @file Acl.cc
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

#include "Acl.hh"
#include "common/StringTokenizer.hh"
#include <unistd.h>
#include <getopt.h>
#include <functional>
#include <algorithm>
#include <queue>
#include <sstream>

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
std::string Acl::AclShortToString(unsigned short int in)
{
  std::string ret = "";

  if (in & Acl::R) {
    ret.append("r");
  }

  if (in & Acl::W) {
    ret.append("w");
  }

  if (in & Acl::X) {
    ret.append("x");
  }

  if (in & Acl::M) {
    ret.append("m");
  }

  if (in & Acl::nM) {
    ret.append("!m");
  }

  if (in & Acl::nD) {
    ret.append("!d");
  }

  if (in & Acl::pD) {
    ret.append("+d");
  }

  if (in & Acl::nU) {
    ret.append("!u");
  }

  if (in & Acl::pU) {
    ret.append("+u");
  }

  if (in & Acl::Q) {
    ret.append("q");
  }

  if (in & Acl::C) {
    ret.append("c");
  }

  return ret;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
Rule Acl::AclRuleFromString(const std::string& single_acl)
{
  Rule ret;
  auto acl_delimiter = single_acl.rfind(':');
  ret.first =
    std::string(single_acl.begin(), single_acl.begin() + acl_delimiter);
  unsigned short rule_int = 0;

  for (auto i = acl_delimiter + 1, size = single_acl.length(); i < size; ++i) {
    switch (single_acl.at(i)) {
    case 'r' :
      rule_int = rule_int | Acl::R;
      break;

    case 'w' :
      rule_int = rule_int | Acl::W;
      break;

    case 'x' :
      rule_int = rule_int | Acl::X;
      break;

    case 'm' :
      rule_int = rule_int | Acl::M;
      break;

    case 'q' :
      rule_int = rule_int | Acl::Q;
      break;

    case 'c' :
      rule_int = rule_int | Acl::C;
      break;

    case '+' :
      // There is only two + flags in current acl permissions +d and +u
      i++;

      if (single_acl.at(i) == 'd') {
        rule_int = rule_int | Acl::pD;
      } else {
        rule_int = rule_int | Acl::pU;
      }

      break;

    case '!' :
      i++;

      if (single_acl.at(i) == 'd') {
        rule_int = rule_int | Acl::nD;
      }

      if (single_acl.at(i) == 'u') {
        rule_int = rule_int | Acl::nU;
      }

      if (single_acl.at(i) == 'm') {
        rule_int = rule_int | Acl::nM;
      }

      break;
    }
  }

  ret.second = rule_int;
  return ret;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool Acl::GetAclStringsForPath(const std::string& path)
{
  std::string command = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=") +
                        path;
  // @TODO (esindril): call internal attr ls
  std::string result = "";
  SetAclString(result, m_sys_acl_string, "sys.acl");
  SetAclString(result, m_usr_acl_string, "user.acl");
  return true;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void Acl::SetAclString(const std::string& result,
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
void Acl::GenerateRuleMap(const std::string& acl_string, RuleMap& map)
{
  size_t curr_pos = 0, pos = 0;

  if (!map.empty()) {
    map.clear();
  }

  if (acl_string.empty()) {
    return;
  }

  while (1) {
    pos = acl_string.find(',', curr_pos);

    if (pos == std::string::npos) {
      pos = acl_string.length();
    }

    std::string single_acl =
      std::string(acl_string.begin() + curr_pos, acl_string.begin() + pos);
    Rule temp = AclRuleFromString(single_acl);
    m_rules[temp.first] = temp.second;
    curr_pos = pos + 1;

    if (curr_pos > acl_string.length()) {
      break;
    }
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool Acl::CheckCorrectId(const std::string& id)
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
bool Acl::GetRuleInt(const std::string& rule, bool set)
{
  unsigned short int ret = 0, add_ret = 0, rm_ret = 0;
  bool lambda_happen = false;
  auto add_lambda = [&add_ret, &ret](Acl::ACLPos pos) {
    add_ret = add_ret | pos;
    ret = ret | pos;
  };
  auto remove_lambda = [&rm_ret, &ret](Acl::ACLPos pos) {
    rm_ret = rm_ret | pos;
    ret = ret & (~pos);
  };
  std::function<void(Acl::ACLPos)>curr_lambda = add_lambda;

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
      curr_lambda(Acl::R);
      continue;
    }

    if (*flag == 'w') {
      curr_lambda(Acl::W);
      continue;
    }

    if (*flag == 'x') {
      curr_lambda(Acl::X);
      continue;
    }

    if (*flag == 'm') {
      curr_lambda(Acl::M);
      continue;
    }

    if (*flag == 'q') {
      curr_lambda(Acl::Q);
      continue;
    }

    if (*flag == 'c') {
      curr_lambda(Acl::C);
      continue;
    }

    if (*flag == '!') {
      ++flag;

      if (flag == rule.end()) {
        goto error_label;
      }

      if (*flag == 'd') {
        curr_lambda(Acl::nD);
        continue;
      }

      if (*flag == 'u') {
        curr_lambda(Acl::nU);
        continue;
      }

      if (*flag == 'm') {
        curr_lambda(Acl::nM);
        continue;
      }

      goto error_label;
    }

    if (*flag == '+') {
      ++flag;

      if (*flag == 'd') {
        curr_lambda(Acl::pD);
        continue;
      }

      if (*flag == 'u') {
        curr_lambda(Acl::pU);
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
void Acl::ApplyRule()
{
  unsigned short temp_rule = 0;

  if (!m_set && m_rules.find(m_id) != m_rules.end()) {
    temp_rule = m_rules[m_id];
  }

  if (m_add_rule != 0) {
    temp_rule = temp_rule | m_add_rule;
  }

  if (m_rm_rule != 0) {
    temp_rule = temp_rule & (~m_rm_rule);
  }

  m_rules[m_id] = temp_rule;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
std::string Acl::MapToAclString()
{
  std::string ret = "";

  for (auto item = m_rules.begin(); item != m_rules.end(); item++) {
    if ((*item).second != 0) {
      ret += (*item).first + ":" + AclShortToString((*item).second) + ",";
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
bool Acl::ParseRule(const std::string& input)
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
bool Acl::ProcessCommand()
{
  std::string token;
  const char* temp;
  eos::common::StringTokenizer m_subtokenizer(m_comm);
  m_subtokenizer.GetLine();

  // Get opts
  while ((temp = m_subtokenizer.GetToken()) != 0) {
    //trimming
    token = std::string(temp);
    token.erase(std::find_if(token.rbegin(), token.rend(),
                             std::not1(std::ptr_fun<int, int> (std::isspace))).base(), token.end());

    if (token == "") {
      continue;
    }

    if (token == "--help") {
      return false;
    }

    if (token == "-lR" || token == "-Rl") {
      m_recursive = m_list = true;
      continue;
    }

    if (token == "-R")          {
      m_recursive = true;
      continue;
    }

    if (token == "--recursive") {
      m_recursive = true;
      continue;
    }

    if (token == "-l")      {
      m_list = true;
      continue;
    }

    if (token == "--lists") {
      m_list = true;
      continue;
    }

    if (token == "--sys")  {
      m_sys_acl = true;
      continue;
    }

    if (token == "--user") {
      m_usr_acl = true;
      continue;
    }

    // If there is unsupported flag
    if (token.at(0) == '-') {
      m_error_message = "Unercognized flag " + token + " !";
      return false;
    } else {
      if (m_list) {
        m_path = token;
      } else {
        m_rule = std::string(token);

        if ((temp = m_subtokenizer.GetToken()) != 0) {
          token = std::string(temp);
          token.erase(std::find_if(token.rbegin(), token.rend(),
                                   std::not1(std::ptr_fun<int, int> (std::isspace))).base(), token.end());
          m_path = std::string(token);
        } else {
          return false;
        }
      }

      break;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool Acl::Action(bool apply , const std::string& path)
{
  if (apply) {
    if (!GetAclStringsForPath(path)) {
      return false;
    }

    if (m_sys_acl) {
      GenerateRuleMap(m_sys_acl_string, m_rules);
    } else {
      GenerateRuleMap(m_usr_acl_string, m_rules);
    }

    ApplyRule();
    return false;
    // return MgmSet(path);
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
