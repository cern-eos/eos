//------------------------------------------------------------------------------
//! @file HealthCommand.cc
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
#include "HealthCommand.hh"

std::string HealthCommand::GetValueWrapper::GetValue(const std::string& key)
{
  try {
    RegexUtil reg;
    reg.SetRegex(key + "=[%a-zA-Z0-9/.:-]*");
    reg.SetOrigin(m_token);
    reg.initTokenizerMode();
    std::string temp = reg.Match();
    auto pos = temp.find('=');

    if (pos == std::string::npos) {
      return std::string("");
    }

    return std::string(temp.begin() + pos + 1, temp.end());
  } catch (std::string e) {
    m_error_message = e;
    throw std::string(" REGEX_ERROR: " + e);
  }
}

void FSInfo::ReadFromString(const std::string& input)
{
  size_t pos = 0,  last = 0;
  std::string value;

  if ((pos = input.find(' ',  last)) != std::string::npos) {
    host = input.substr(last, pos);
    last = ++pos;
  }

  if ((pos = input.find(' ',  last)) != std::string::npos) {
    port = std::stoi(input.substr(last, pos));
    last = ++pos;
  }

  if ((pos = input.find(' ',  last)) != std::string::npos) {
    id = std::stoi(input.substr(last, pos));
    last = ++pos;
  }

  if ((pos = input.find(' ',  last)) != std::string::npos) {
    active = input.substr(last, pos - last);
    last = ++pos;
  }

  if ((pos = input.find(' ',  last)) != std::string::npos) {
    path = input.substr(last, pos - last);
    last = ++pos;
  }

  if ((pos = input.find(' ',  last)) != std::string::npos) {
    headroom = std::strtoull(input.substr(last, pos).c_str(),  NULL, 10);
    last = ++pos;
  }

  if ((pos = input.find(' ',  last)) != std::string::npos) {
    free_bytes = std::strtoull(input.substr(last, pos).c_str(),  NULL, 10);
    last = ++pos;
  }

  if ((pos = input.find(' ',  last)) != std::string::npos) {
    used_bytes = std::strtoull(input.substr(last, pos).c_str(),  NULL, 10);
    last = ++pos;
  }

  if ((pos = input.find(' ',  last)) != std::string::npos) {
    capacity = std::strtoull(input.substr(last, pos).c_str(),  NULL, 10);
    last = ++pos;
  }
}

bool FSInfo::operator==(const FSInfo& other)
{
  return host == other.host && port == other.port &&
         id == other.id && active == other.active &&
         path == other.path && free_bytes == other.free_bytes &&
         capacity == other.capacity && headroom == other.headroom;
}


HealthCommand::HealthCommand(const char* comm)
  : m_comm(const_cast<char*>(comm)),
    m_monitoring(false),
    m_all(false),
    m_section("")
{
}

void HealthCommand::DeadNodesCheck()
{
  if (!m_mgm_execute.ExecuteAdminCommand(
        "mgm.cmd=node&mgm.subcmd=ls&mgm.outformat=m"
      )
     ) {
    throw std::string("MGMError: " + m_mgm_execute.GetError());
  }

  std::string ret = m_mgm_execute.GetResult();
  std::string line;
  std::istringstream splitter(ret);
  ConsoleTableOutput table;
  std::vector<std::pair<std::string, unsigned>> header;
  header.push_back(std::make_pair("Hostport:", 30));
  header.push_back(std::make_pair("Status:", 10));
  table.SetHeader(header);
  //table.SetHeader({{"Hostport:", 30}, {"Status:", 10}});

  if (m_monitoring) {
    m_output << "type=DeadNodesCheck" << std::endl;
  }

  while (std::getline(splitter, line, '\n')) {
    GetValueWrapper extractor(line);
    std::string status = extractor.GetValue("status");

    if (m_monitoring) {
      m_output << extractor.GetValue("hostport") << "=";
      m_output << status << " ";
    } else {
      bool trigger = status != "online";

      if (trigger || m_all)
        table.AddRow(
          extractor.GetValue("hostport"),
          table.Colorify(
            trigger ? ConsoleTableOutput::RED : ConsoleTableOutput::GREEN,
            status
          )
        );
    }
  }

  if (!m_monitoring) {
    m_output << table.Str() << std::endl;
  }

  m_output << std::endl;
}

void HealthCommand::TooFullForDrainingCheck()
{
  ConsoleTableOutput table;

  if (m_monitoring) {
    m_output << "type=FullDrainCheck" << std::endl;
  } else {
    std::vector<std::pair<std::string, unsigned>> header;
    header.push_back(std::make_pair("Group:", 20));
    header.push_back(std::make_pair("Offline Used(GB)", 18));
    header.push_back(std::make_pair("Online Free(GB)", 18));
    header.push_back(std::make_pair("Status:", 10));
    table.SetHeader(header);
//     table.SetHeader({
//       {"Group:", 20}, {"Offline Used(GB)", 18},
//       {"Online Free(GB)", 18}, {"Status:", 10}});
  }

  for (auto group = m_group_data.begin();
       group !=  m_group_data.end(); ++group) {
    uint64_t summed_free_space = 0;
    uint64_t offline_used_space = 0;

    for (auto fs = group->second.begin(); fs != group->second.end(); ++fs) {
      if (fs->active != "online") {
        offline_used_space += fs->used_bytes;
      } else {
        summed_free_space += fs->free_bytes - fs->headroom;
      }
    }

    if (m_monitoring) {
      m_output << "group=" << group->first << " ";
      m_output << "offline_used_space=";
      m_output << (offline_used_space * 1.) / (1 << 30) << " ";
      m_output << "online_free_space=";
      m_output << (summed_free_space * 1.) / (1 << 30) << " ";
      m_output << "status=";

      if (summed_free_space <= offline_used_space) {
        m_output << "full ";
      } else {
        m_output << "ok ";
      }

      m_output << std::endl;
    } else {
      bool trigger = summed_free_space <= offline_used_space;

      if (trigger || m_all)
        table.AddRow(
          group->first,
          (offline_used_space * 1.) / (1 << 30),
          (summed_free_space * 1.) / (1 << 30),
          table.Colorify(
            trigger ? ConsoleTableOutput::RED : ConsoleTableOutput::GREEN,
            trigger ? "FULL" : "OK"
          )
        );
    }
  }

  if (!m_monitoring) {
    m_output << table.Str() << std::endl;
  }

  m_output << std::endl;
}

void HealthCommand::PlacementContentionCheck()
{
  ConsoleTableOutput table;

  if (m_monitoring) {
    m_output << "type=PlacementContentionCheck"  << std::endl;
  } else {
    std::vector<std::pair<std::string, unsigned>> header;
    header.push_back(std::make_pair("Group", 20));
    header.push_back(std::make_pair("Free", 10));
    header.push_back(std::make_pair("Full", 10));
    header.push_back(std::make_pair("Contention", 12));
    table.SetHeader(header);
//     table.SetHeader({
//       {"Group", 20}, {"Free", 10}, {"Full", 10}, {"Contention", 12}
//     });
  }

  unsigned min = 100, max = 0, avg = 0;
  std::string critical_group;
  unsigned min_free_fs = 1024;

  for (auto group = m_group_data.begin();
       group !=  m_group_data.end();
       ++group) {
    if (group->second.size() < 4) {
      if (m_monitoring) {
        m_output << group->first << "=\"Less than 4 fs in group\" ";
      } else {
        table.Colorify(ConsoleTableOutput::YELLOW, "");
        table.CustomRow(
          std::make_pair(group->first, 15),
          std::make_pair("Group has less than 4 filesystem in group", 45)
        );
      }

      continue;
    }

    unsigned int free_space_left = 0;

    for (auto fs = group->second.begin(); fs !=  group->second.end(); ++fs) {
      if (fs->free_bytes > uint64_t(2) * fs->headroom) {
        ++free_space_left;
      }
    }

    if (free_space_left < min_free_fs) {
      min_free_fs = free_space_left;
      critical_group = group->first;
    }

    if (m_monitoring) {
      m_output << "group=" << group->first << " ";
      m_output << "free_fs=" << free_space_left << " ";
      m_output << "full_fs=" << group->second.size() - free_space_left << " ";
      m_output << "status=" << (free_space_left <= 2 ? "full" : "fine");
      m_output << std::endl;
    } else {
      bool trigger = free_space_left <= 2;
      unsigned contention = 100 - (free_space_left * 1. / group->second.size()) * 100;
      avg += contention;

      if (contention < min) {
        min = contention;
      }

      if (contention > max) {
        max = contention;
      }

      if (trigger || m_all)
        table.AddRow(
          group->first,
          free_space_left,
          group->second.size() - free_space_left,
          table.Colorify(
            trigger ? ConsoleTableOutput::RED : ConsoleTableOutput::GREEN,
            std::to_string((long long)contention) + "%"
          )
        );
    }
  }

  if (!m_monitoring) {
    avg = avg / m_group_data.size();
    std::vector<std::pair<std::string, unsigned>> header;
    header.push_back(std::make_pair("min", 6));
    header.push_back(std::make_pair("avg", 6));
    header.push_back(std::make_pair("max", 6));
    header.push_back(std::make_pair("min-placement", 15));
    header.push_back(std::make_pair("group", 20));
    table.SetHeader(header);
//     table.SetHeader({
//       {"min", 6}, {"avg", 6}, {"max", 6}, {"min-placement", 15}, {"group",20}
//     });
    table.AddRow(
      std::to_string((long long)min) + "%",
      std::to_string((long long)avg) + "%",
      std::to_string((long long)max) + "%",
      min_free_fs,
      critical_group
    );
    m_output << table.Str() << std::endl;
  }

  m_output << std::endl;
}

void HealthCommand::GetGroupsInfo()
{
  if (!m_mgm_execute.ExecuteAdminCommand(
        "mgm.cmd=fs&mgm.subcmd=ls&mgm.outformat=m"
      )
     ) {
    throw std::string("MGMError: " + m_mgm_execute.GetError());
  }

  std::string ret = m_mgm_execute.GetResult();

  if (ret.empty()) {
    throw std::string("There is no FileSystems registered!");
  }

  std::string line;
  std::istringstream splitter(ret);

  while (std::getline(splitter, line, '\n')) {
    if (line.empty()) {
      continue;
    }

    GetValueWrapper extractor(line);
    std::string str_temp;
    std::string group = extractor.GetValue("schedgroup");
    FSInfo temp;
    temp.host       = extractor.GetValue("host");
    str_temp = extractor.GetValue("port");
    temp.port       = std::stoi(str_temp.empty() ? "0" : str_temp);
    temp.active     = extractor.GetValue("stat.active");
    str_temp = extractor.GetValue("id");
    temp.id         = std::stoi(str_temp.empty() ? "0" : str_temp);
    temp.path       = extractor.GetValue("path");
    str_temp = extractor.GetValue("headroom");
    temp.headroom   = std::stoll(str_temp.empty() ? "0" : str_temp);
    str_temp = extractor.GetValue("stat.statfs.freebytes");
    temp.free_bytes = std::stoll(str_temp.empty() ? "0" : str_temp);
    str_temp = extractor.GetValue("stat.statfs.usedbytes");
    temp.used_bytes = std::stoll(str_temp.empty() ? "0" : str_temp);
    str_temp = extractor.GetValue("stat.statfs.capacity");
    temp.capacity   = std::stoll(str_temp.empty() ? "0" : str_temp);

    if (m_group_data.find(group) == m_group_data.end()) {
      FSInfoVec temp_vec;
      temp_vec.push_back(temp);
      m_group_data[group] = temp_vec;
    } else {
      m_group_data[group].push_back(temp);
    }
  }
}

void HealthCommand::AllCheck()
{
  m_output << "Whole report!" << std::endl;
  DeadNodesCheck();
  TooFullForDrainingCheck();
  PlacementContentionCheck();
}


void HealthCommand::PrintHelp()
{
  std::cerr << "Usage: eos health [OPTION] [SECTION]" << std::endl;
  std::cerr << std::endl;
  std::cerr << "Options available: " << std::endl;
  std::cerr << "  --help    Print help" << std::endl;
  std::cerr << "   -m       Turn on monitoring mode" << std::endl;
  std::cerr << "   -a       Display all information, not just critical" <<
            std::endl;
  std::cerr << std::endl;
  std::cerr << "Sections available: " << std::endl;
  std::cerr << "  all         Display all sections (default value)" << std::endl;
  std::cerr << "  nodes       Display only information about nodes" << std::endl;
  std::cerr << "  drain       Display drain health information" << std::endl;
  std::cerr << "  placement   Display placement contention health information" <<
            std::endl;
}

void HealthCommand::ParseCommand()
{
  std::string token;
  const char* temp;
  eos::common::StringTokenizer m_subtokenizer(m_comm);
  m_subtokenizer.GetLine();

  while ((temp = m_subtokenizer.GetToken()) != 0) {
    token = std::string(temp);
    token.erase(std::find_if(token.rbegin(), token.rend(),
                             std::not1(std::ptr_fun<int, int> (std::isspace))).base(),
                token.end());

    if (token == "") {
      continue;
    }

    if (token == "all"   ||
        token == "nodes" ||
        token == "drain" ||
        token == "placement"
       ) {
      m_section = token;
      continue;
    }

    if (token == "-a") {
      m_all = true;
      continue;
    }

    if (token == "-m") {
      m_monitoring = true;
      continue;
    }

    if (token == "--help") {
      PrintHelp();
      m_section = "/";
      return;
    }

    throw std::string("Unrecognized token (" + token + ")!");
  }
}

void HealthCommand::Execute()
{
  ParseCommand();
  GetGroupsInfo();

  if (m_section == "nodes") {
    DeadNodesCheck();
  }

  if (m_section == "drain") {
    TooFullForDrainingCheck();
  }

  if (m_section == "placement") {
    PlacementContentionCheck();
  }

  if (m_section.empty() || m_section == "all") {
    AllCheck();
  }

  std::cout << m_output.str();
}
