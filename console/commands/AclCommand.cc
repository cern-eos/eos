//------------------------------------------------------------------------------
//! @file AclCommand.cc
//! @author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "AclCommand.hh"

#include <functional>
#include <queue>
#include <sstream>
#include <unistd.h>
#include <getopt.h>

#ifndef CPPUNIT_FOUND
  MgmExecute::MgmExecute()
  {

  }

  bool MgmExecute::ExecuteCommand(const char* command)
  {
    XrdOucString command_xrd = XrdOucString(command);

    XrdOucEnv* response = client_user_command(command_xrd);

    rstdout = response->Get("mgm.proc.stdout");
    rstderr = response->Get("mgm.proc.stderr");

    if(rstderr.length() != 0){
      this->m_error = std::string(rstderr.c_str());
      delete response;
      return false;
    }

    this->m_result = std::string("");

    if(rstdout.length() > 0 ){
      this->m_result = std::string(rstdout.c_str());
    }

    delete response;

    return true;
  }
#endif

std::string AclCommand::AclShortToString(unsigned short int in)
{
  std::string ret = "";
  if( in & AclCommand::R)  ret.append("r");
  if( in & AclCommand::W)  ret.append("w");
  if( in & AclCommand::X)  ret.append("x");
  if( in & AclCommand::M)  ret.append("m");
  if( in & AclCommand::nM) ret.append("!m");
  if( in & AclCommand::nD) ret.append("!d");
  if( in & AclCommand::pD) ret.append("+d");
  if( in & AclCommand::nU) ret.append("!u");
  if( in & AclCommand::pU) ret.append("+u");
  if( in & AclCommand::Q)  ret.append("q");
  if( in & AclCommand::C)  ret.append("c");
  return ret;
}

Rule AclCommand::AclRuleFromString(const std::string& single_acl)
{
  Rule ret;
  auto acl_delimiter = single_acl.rfind(':');

  ret.first =
    std::string(single_acl.begin(), single_acl.begin() + acl_delimiter);

  unsigned short rule_int = 0;

  for(auto i = acl_delimiter+1, size = single_acl.length(); i < size; ++i){
    switch(single_acl.at(i)){
      case 'r' : rule_int = rule_int | AclCommand::R; break;
      case 'w' : rule_int = rule_int | AclCommand::W; break;
      case 'x' : rule_int = rule_int | AclCommand::X; break;
      case 'm' : rule_int = rule_int | AclCommand::M; break;
      case 'q' : rule_int = rule_int | AclCommand::Q; break;
      case 'c' : rule_int = rule_int | AclCommand::C; break;
      case '+' :
        // There is only two + flags in current acl permissions +d and +u
        i++;
        if(single_acl.at(i) == 'd') rule_int = rule_int | AclCommand::pD;
        else                        rule_int = rule_int | AclCommand::pU;
        break;
      case '!' :
        i++;
        if(single_acl.at(i) == 'd') rule_int = rule_int | AclCommand::nD;
        if(single_acl.at(i) == 'u') rule_int = rule_int | AclCommand::nU;
        if(single_acl.at(i) == 'm') rule_int = rule_int | AclCommand::nM;
        break;
    }
  }
  ret.second = rule_int;
  return ret;
}

bool AclCommand::GetAclStringsForPath(const std::string& path)
{
  std::string command = std::string("mgm.cmd=attr&mgm.subcmd=ls&mgm.path=") + path;
  if( !m_mgm_execute.ExecuteCommand(command.c_str())){
    this->m_error_message = "Error getting acl strings from mgm!";
    return false;
  }

  std::string result = m_mgm_execute.GetResult();

  this->SetAclString(result, this->m_sys_acl_string, "sys.acl");
  this->SetAclString(result, this->m_usr_acl_string, "usr.acl");

  return true;
}

void AclCommand::SetAclString(const std::string& result,
                              std::string& which,
                              const char* type)
{
  which = "";
  size_t pos_begin = result.find(type);
  if(pos_begin != std::string::npos){
    unsigned int pos_end = result.find("\n", pos_begin);
    which = std::string(result.begin()+9+pos_begin, result.begin()+pos_end-1);
  }
}

void AclCommand::GenerateRuleMap(const std::string& acl_string, RuleMap& map)
{
  size_t curr_pos = 0, pos = 0;

  if(!map.empty())
    map.clear();

  if(acl_string.empty())
       return;

  while(1){
    pos = acl_string.find(',', curr_pos);
    if(pos == std::string::npos)
      pos = acl_string.length();

    std::string single_acl =
    std::string(acl_string.begin()+curr_pos, acl_string.begin()+pos);

    Rule temp = this->AclRuleFromString(single_acl);

    this->m_rules[temp.first] = temp.second;
    curr_pos = pos + 1;
    if(curr_pos > acl_string.length())
      break;
  }
}

bool AclCommand::CheckCorrectId(const std::string& id)
{

  std::string allowed_chars =
    "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_-";

  if((id.at(0) == 'u' && id.at(1) == ':' ) ||
     (id.at(0) == 'g' && id.at(1) == ':') ){
    return id.find_first_not_of(allowed_chars, 2) == std::string::npos;
  }

  if(id.find("egroup") == 0 && id.at(6) == ':'){
    return id.find_first_not_of(allowed_chars, 7) == std::string::npos;
  }

  return false;
}

bool AclCommand::GetRuleInt(const std::string& rule, bool set)
{
  unsigned short int ret = 0, add_ret = 0, rm_ret = 0;
  bool lambda_happen = false;

  auto add_lambda = [&add_ret, &ret](AclCommand::ACLPos pos)
  { add_ret = add_ret | pos; ret = ret | pos; };

  auto remove_lambda = [&rm_ret, &ret](AclCommand::ACLPos pos)
  { rm_ret = rm_ret | pos; ret = ret & (~pos);};

  std::function<void(AclCommand::ACLPos)>curr_lambda = add_lambda;

  for (auto flag = rule.begin(); flag != rule.end(); ++flag){
    // Check for add/rm rules
    if(*flag == '-'){
      curr_lambda = remove_lambda;
      lambda_happen = true;
      continue;
    }
    if(*flag == '+'){
      auto temp_iter = flag+1;
      if(temp_iter == rule.end()) continue;
      if( *temp_iter != 'd' && *temp_iter != 'u'){
        lambda_happen = true;
        curr_lambda = add_lambda;
        continue;
      }
    }

    // If there is no +/- character happend in not set mode
    // This is not correct behaviour hence returning false
    if(!set && !lambda_happen) return false;

    // Check for flags
    if(*flag == 'r'){ curr_lambda(AclCommand::R); continue; }
    if(*flag == 'w'){ curr_lambda(AclCommand::W); continue; }
    if(*flag == 'x'){ curr_lambda(AclCommand::X); continue; }
    if(*flag == 'm'){ curr_lambda(AclCommand::M); continue; }
    if(*flag == 'q'){ curr_lambda(AclCommand::Q); continue; }
    if(*flag == 'c'){ curr_lambda(AclCommand::C); continue; }
    if(*flag == '!'){
      ++flag;
      if(flag == rule.end()) goto error_label;
      if(*flag == 'd'){ curr_lambda(AclCommand::nD); continue; }
      if(*flag == 'u'){ curr_lambda(AclCommand::nU); continue; }
      if(*flag == 'm'){ curr_lambda(AclCommand::nM); continue; }

      goto error_label;
    }
    if(*flag == '+'){
      ++flag;
      if(*flag == 'd'){ curr_lambda(AclCommand::pD); continue; }
      if(*flag == 'u'){ curr_lambda(AclCommand::pU); continue; }
    }

    goto error_label;
  }

  // Ret is representing mask for flags which are goint to be added or removed
  // As if we need to remove flags that flags will be setted to 1 in rm_ret and
  // setted to 0 in ret and vice versa for adding flags.

  m_add_rule = add_ret == 0 ? 0 : ret & add_ret;
  m_rm_rule  = rm_ret == 0 ? 0 : ~ret & rm_ret;

  return true;

  error_label:
  this->m_error_message = "Rule not correct!";
  return false;

}

void AclCommand::ApplyRule()
{
  unsigned short temp_rule = 0;

  if(!this->m_set && this->m_rules.find(this->m_id) != this->m_rules.end()){
    temp_rule = this->m_rules[this->m_id];
  }

  if(m_add_rule != 0) temp_rule = temp_rule | m_add_rule;
  if(m_rm_rule  != 0)  temp_rule = temp_rule & (~m_rm_rule);

  this->m_rules[this->m_id] = temp_rule;
}

std::string AclCommand::MapToAclString()
{
  std::string ret = "";
  for(auto item : this->m_rules)
    if(item.second != 0)
      ret += item.first + ":" + this->AclShortToString(item.second) + ",";

    // Removing last ','
    if(ret != "") ret.pop_back();

    return ret;
}

bool AclCommand::ParseRule(const std::string& input)
{
  size_t pos_del_first, pos_del_last, pos_equal;

  pos_del_first = input.find(":");
  pos_del_last  = input.rfind(":");
  pos_equal     = input.find("=");

  std::string id, rule;

  // Lambda for checking id
  auto id_lambda = [&id, &input, this](size_t pos) -> bool {
    id = std::string(input.begin(), input.begin() + pos);
    if(!this->CheckCorrectId(id)){
      this->m_error_message = "Rule: Incorrect format of id!";
      return false;
    }
    this->m_id = id;
    return true;
  };

  // Lambda for checking rule
  auto rule_lambda =
  [&rule, &input, this](size_t pos, bool set_l) -> bool {
    rule = std::string(input.begin() + pos, input.end());
    if(!this->GetRuleInt(rule, set_l)){
      this->m_error_message = "Rule: Rule is not in correct format!";
      return false;
    }
    return true;
  };

  if(pos_del_first == pos_del_last && pos_equal != std::string::npos){
    // u:id=rw+x
    m_set = true;
    // Check if id and rule are correct
    if( !id_lambda(pos_equal))             return false;
    if( !rule_lambda(pos_equal + 1, true)) return false;

    return true;
  }
  else{
    if(pos_del_first != pos_del_last &&
      pos_del_first != std::string::npos &&
      pos_del_last  != std::string::npos
    ){
      this->m_set = false;

      // u:id:+rwx
      // Check if id and rule are correct
      if(!id_lambda(pos_del_last))            return false;
      if(!rule_lambda(pos_del_last+1, false)) return false;

      return true;
    }
    else{
      // Error
      this->m_error_message = "Rule is not good!";
      return false;
    }
  }
}

bool AclCommand::MgmSet(const std::string& path)
{
  std::string command;
  std::string rules = this->MapToAclString();
  std::string acl_type = this->m_sys_acl ? "sys.acl" : "usr.acl";
  if(rules != ""){
    command = std::string("mgm.cmd=attr&mgm.subcmd=set&mgm.attr.key=");
    command += acl_type;
    command += "&mgm.attr.value=";
    command += rules;
    command += "&mgm.path="+path;
  }
  else{
    command  = "mgm.cmd=attr&mgm.subcmd=rm&mgm.attr.key=";
    command += acl_type;
    command += "&mgm.path=";
    command += path;
  }

  if(!m_mgm_execute.ExecuteCommand(command.c_str())){
    m_error_message = "MGM Execute error!";
    return false;
  }

  return true;
}

bool AclCommand::SetDefaultAclRoleFlag()
{
  if(this->m_sys_acl && this->m_usr_acl){
    this->m_error_message = "Both usr and sys flag set!";
    return false;
  }
  // Making sure if list flag exists and no other acl type flag exists
  // to usr acl flag be default.
  if(this->m_list && !this->m_usr_acl && !this->m_sys_acl){
    m_usr_acl = true;
    return true;
  }

  if(this->m_sys_acl == false && this->m_usr_acl == false){
    if(this->m_mgm_execute.ExecuteCommand("mgm.cmd=whoami")){

      std::string result = this->m_mgm_execute.GetResult();
      size_t pos = 0;

      if((pos = result.find("uid=")) != std::string::npos){
        if(
          result.at(pos+4) >= '0' &&
          result.at(pos+4) <= '4' &&
          result.at(pos+5) == ' '
        )
          this->m_sys_acl = true;
        else
          this->m_usr_acl = true;

        return true;
      }
      return false;
    }
    return false;
  }
  return true;
}

// Check!
bool AclCommand::ProcessCommand()
{
  std::string token;
  const char* temp;
  auto trim_lambda = [&token](){
    auto it1 =  std::find_if( token.rbegin(), token.rend(),
                              [](char ch){
                                return !std::isspace<char>(ch , std::locale::classic());
                              }
    );
    token.erase(it1.base() , token.end());
  };

  eos::common::StringTokenizer m_subtokenizer(m_comm);
  m_subtokenizer.GetLine();
  // Get opts
  while((temp = m_subtokenizer.GetToken()) != 0){
    //trimming
    token = std::string(temp);
    trim_lambda();
    if(token == "") continue;

    if( token == "--help") return false;
    if( token == "-lR" || token == "-Rl"){
      this->m_recursive = this->m_list = true;
      continue;
    }

    if(token == "-R")          { this->m_recursive = true; continue; }
    if(token == "--recursive") { this->m_recursive = true; continue; }

    if(token == "-l")      { this->m_list = true; continue; }
    if(token == "--lists") { this->m_list = true; continue; }

    if(token == "--sys")  { this->m_sys_acl = true; continue; }
    if(token == "--user") { this->m_usr_acl = true; continue; }

    // If there is unsupported flag
    if(token.at(0) == '-'){
      this->m_error_message = "Unercognized flag " + token + " !";
      return false;
    }
    else{
      if(this->m_list){
        this->m_path = token;
      }
      else{
        this->m_rule = std::string(token);
        if((temp = m_subtokenizer.GetToken()) != 0 ){
          token = std::string(temp);
          trim_lambda();
          if(token == "") return false;
          this->m_path = std::string(token);
        }
        else
          return false;
      }
      break;
    }
  }

  return true;
}

void AclCommand::RecursiveCall(std::function<bool(std::string&)>& action)
{
  std::string prefix_command = "mgm.cmd=find&mgm.path=";
  std::string postfix_command = "&mgm.option=d";
  std::string command = prefix_command+this->m_path+postfix_command;

  if(!m_mgm_execute.ExecuteCommand(command.c_str())){
    std::cout << "Directory rec error!" << std::endl;
    return;
  }

  // get paths and add it into stack
  std::string path;
  std::string result = m_mgm_execute.GetResult();

  std::stringstream stream {result};

  while(stream){
    std::getline(stream, path, '\n');

    if(path.empty()) continue;

    // execute command
    action(path);
  }


}

void AclCommand::PrintHelp()
{
  std::cerr << "Usage: eos acl [-l|--list] [-R|--recursive]";
  std::cerr << "[--sys|--user] <rule> <path>" << std::endl;
  std::cerr << std::endl;
  std::cerr << "    --help           Print help" << std::endl;
  std::cerr << "-R, --recursive      Apply on directories recursively" << std::endl;
  std::cerr << "-l, --lists          List ACL rules" << std::endl;
  std::cerr << "    --user           Set usr.acl rules on directory" << std::endl;
  std::cerr << "    --sys            Set sys.acl rules on directory" << std::endl;
  std::cerr << "<rule> is created based on chmod rules. " << std::endl;
  std::cerr << "Every rule begins with [u|g|egroup] followed with : and identifier." << std::endl;
  std::cerr <<  std::endl;
  std::cerr << "Afterwards can be:" << std::endl;
  std::cerr << "= for setting new permission ." << std::endl;
  std::cerr << ": for modification of existing permission." << std::endl;
  std::cerr << std::endl;
  std::cerr << "This is followed with rule definition." << std::endl;
  std::cerr << "Every ACL flag can be added with + or removed with -, or in case" << std::endl;
  std::cerr << "of setting new ACL permission just entered ACL flag." << std::endl;
}

void AclCommand::Execute()
{
  auto list_lambda = [this](std::string& path) -> bool{
    if(!this->GetAclStringsForPath(path))
      return false;
    std::cout << path << '\t';
    if(this->m_usr_acl)
      std::cout << "usr: " << this->m_usr_acl_string << std::endl;
    else
      std::cout << "sys: " << this->m_sys_acl_string << std::endl;
    return true;
  };

  auto apply_lambda = [this](std::string& path)->bool{
    if(!this->GetAclStringsForPath(path))
      return false;

    if(this->m_sys_acl)
      this->GenerateRuleMap(this->m_sys_acl_string, this->m_rules);
    else
      this->GenerateRuleMap(this->m_usr_acl_string, this->m_rules);

    this->ApplyRule();
    return this->MgmSet(path);
  };

  std::function<bool(std::string&)> choice;

  this->m_error_message = "";

  // Initial processing of command
  if(!this->ProcessCommand()) goto error_handling;

  this->m_path = std::string(abspath(this->m_path.c_str()));

  // Set
  if(!this->SetDefaultAclRoleFlag()){
    this->m_error_message = "Failed to set acl role!";
    goto error_handling;
  }

  if(!this->m_list && !this->ParseRule(this->m_rule)) goto error_handling;

  // List acls
  if(m_list) choice = list_lambda;
  else       choice = apply_lambda;

  if(m_recursive){
    this->RecursiveCall(choice);
  }
  else{
    if(!choice(this->m_path))
      goto error_handling;
  }

  std::cout << "Success!" << std::endl;
  return;

  error_handling:
  std::cout << this->m_error_message << std::endl;
  std::cout << this->m_mgm_execute.GetError() << std::endl << std::endl;

  this->PrintHelp();
  return;
}

AclCommand::~AclCommand()
{

}
