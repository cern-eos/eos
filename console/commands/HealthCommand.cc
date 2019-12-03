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
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/NodeHelper.hh"
#include "console/commands/helpers/FsHelper.hh"
#include "common/StringTokenizer.hh"
#include "common/StringUtils.hh"
#include <algorithm>

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
  } catch (std::string& e) {
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
    headroom = std::strtoull(input.substr(last, pos).c_str(),  nullptr, 10);
    last = ++pos;
  }

  if ((pos = input.find(' ',  last)) != std::string::npos) {
    free_bytes = std::strtoull(input.substr(last, pos).c_str(),  nullptr, 10);
    last = ++pos;
  }

  if ((pos = input.find(' ',  last)) != std::string::npos) {
    used_bytes = std::strtoull(input.substr(last, pos).c_str(),  nullptr, 10);
    last = ++pos;
  }

  if ((pos = input.find(' ',  last)) != std::string::npos) {
    capacity = std::strtoull(input.substr(last, pos).c_str(),  nullptr, 10);
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
  NodeHelper node_cmd(gGlobalOpts);
  node_cmd.ParseCommand("ls -m");

  if (node_cmd.ExecuteWithoutPrint()) {
    throw std::string("MGMError: " + node_cmd.GetError());
  }

  std::string ret = node_cmd.GetResult();
  std::string line;
  std::istringstream splitter(ret);
  std::string format_s = !m_monitoring ? "s" : "os";
  std::string format_ss = !m_monitoring ? "-s" : "os";
  eos::mgm::TableFormatterBase table(!isatty(STDOUT_FILENO) ||
                                     !isatty(STDERR_FILENO));

  if (!m_monitoring) {
    table.SetHeader({
      std::make_tuple("hostport", 32, format_ss),
      std::make_tuple("status", 8, format_s)
    });
  } else {
    table.SetHeader({
      std::make_tuple("type", 0, format_ss),
      std::make_tuple("hostport", 0, format_ss),
      std::make_tuple("status", 0, format_s)
    });
  }

  while (std::getline(splitter, line, '\n')) {
    GetValueWrapper extractor(line);
    std::string hostport = extractor.GetValue("hostport");
    std::string status = extractor.GetValue("status");
    bool trigger = status != "online";

    if (trigger || m_all) {
      TableData table_data;
      table_data.emplace_back();

      if (m_monitoring) {
        table_data.back().emplace_back("DeadNodesCheck", format_ss);
      }

      table_data.back().emplace_back(hostport, format_s);
      table_data.back().emplace_back(status, format_s);
      table.AddRows(table_data);
    }
  }

  m_output << table.GenerateTable(HEADER).c_str();
}

void HealthCommand::TooFullForDrainingCheck()
{
  std::vector<std::tuple<std::string, std::string,
      unsigned long long, unsigned long long, std::string>> data;
  std::string format_s = !m_monitoring ? "s" : "os";
  std::string format_ss = !m_monitoring ? "-s" : "os";
  std::string format_l = !m_monitoring ? "+l" : "ol";
  std::string unit = !m_monitoring ? "B" : "";
  eos::mgm::TableFormatterBase table(!isatty(STDOUT_FILENO) ||
                                     !isatty(STDERR_FILENO));

  if (!m_monitoring) {
    table.SetHeader({
      std::make_tuple("group", 12, format_ss),
      std::make_tuple("offline used", 12, format_l),
      std::make_tuple("online free", 12, format_l),
      std::make_tuple("status", 8, format_s)
    });
  } else {
    table.SetHeader({
      std::make_tuple("type", 0, format_ss),
      std::make_tuple("group", 0, format_ss),
      std::make_tuple("offline_used_space", 0, format_l),
      std::make_tuple("online_free_space", 0, format_l),
      std::make_tuple("status", 0, format_s)
    });
  }

  for (auto& group : m_group_data) {
    unsigned long long summed_free_space = 0;
    unsigned long long offline_used_space = 0;

    for (auto& fs : group.second) {
      if (fs.active != "online") {
        offline_used_space += fs.used_bytes;
      } else {
        summed_free_space += fs.free_bytes - fs.headroom;
      }
    }

    bool trigger = summed_free_space <= offline_used_space;
    std::string status = trigger ? "full" : "ok";

    if (trigger || m_all) {
      data.emplace_back(std::make_tuple("FullDrainCheck", group.first.c_str(),
                                        offline_used_space, summed_free_space, status));
    }
  }

  std::sort(data.begin(), data.end());

  for (auto it : data) {
    TableData table_data;
    table_data.emplace_back();

    if (m_monitoring) {
      table_data.back().emplace_back(std::get<0>(it), format_ss);
    }

    table_data.back().emplace_back(std::get<1>(it), format_ss);
    table_data.back().emplace_back(std::get<2>(it), format_l, unit);
    table_data.back().emplace_back(std::get<3>(it), format_l, unit);
    table_data.back().emplace_back(std::get<4>(it), format_s);
    table.AddRows(table_data);
  }

  m_output << table.GenerateTable(HEADER).c_str();
}

void HealthCommand::PlacementContentionCheck()
{
  std::vector<std::tuple<std::string, std::string, unsigned long long,
      unsigned long long, unsigned long long, std::string>> data;
  std::string format_s = !m_monitoring ? "s" : "os";
  std::string format_ss = !m_monitoring ? "-s" : "os";
  std::string format_l = !m_monitoring ? "l" : "ol";
  std::string unit = !m_monitoring ? "%" : "";
  eos::mgm::TableFormatterBase table(!isatty(STDOUT_FILENO) ||
                                     !isatty(STDERR_FILENO));

  if (!m_monitoring) {
    table.SetHeader({
      std::make_tuple("group", 12, format_ss),
      std::make_tuple("free fs", 8, format_l),
      std::make_tuple("full fs", 8, format_l),
      std::make_tuple("contention", 10, format_l),
      std::make_tuple("status", 8, format_s)
    });
  } else {
    table.SetHeader({
      std::make_tuple("type", 0, format_ss),
      std::make_tuple("group", 0, format_ss),
      std::make_tuple("free_fs", 0, format_l),
      std::make_tuple("full_fs", 0, format_l),
      std::make_tuple("contention", 10, format_l),
      std::make_tuple("status", 0, format_s)
    });
  }

  unsigned min = 100;
  unsigned avg = 0;
  unsigned max = 0;
  unsigned min_free_fs = 1024;
  std::string critical_group;

  for (auto& group : m_group_data) {
    unsigned int free_space_left = 0;

    for (auto& fs : group.second) {
      if (fs.free_bytes > uint64_t(2) * fs.headroom) {
        ++free_space_left;
      }
    }

    unsigned int full_fs = group.second.size() - free_space_left;
    unsigned contention = 100 - (free_space_left * 1. / group.second.size()) * 100;
    std::string status;
    bool trigger = true;

    if (group.second.size() < 4) {
      status = "warning: Less than 4 fs in group";
    } else if (free_space_left <= 2) {
      status = "full";
    } else {
      status = "fine";
      trigger = false;
    }

    if (trigger || m_all) {
      data.emplace_back(std::make_tuple("PlacementContentionCheck",
                                        group.first.c_str(), free_space_left, full_fs, contention, status));
    }

    min = (contention < min) ? contention : min;
    avg += contention;
    max = (contention > max) ? contention : max;

    if (free_space_left < min_free_fs) {
      min_free_fs = free_space_left;
      critical_group = group.first;
    }
  }

  std::sort(data.begin(), data.end());

  for (auto it : data) {
    TableData table_data;
    table_data.emplace_back();

    if (m_monitoring) {
      table_data.back().emplace_back(std::get<0>(it), format_ss);
    }

    table_data.back().emplace_back(std::get<1>(it), format_ss);
    table_data.back().emplace_back(std::get<2>(it), format_l);
    table_data.back().emplace_back(std::get<3>(it), format_l);
    table_data.back().emplace_back(std::get<4>(it), format_l, unit);
    table_data.back().emplace_back(std::get<5>(it), format_s);
    table.AddRows(table_data);
  }

  m_output << table.GenerateTable(HEADER).c_str();
  //! Summary
  avg = (m_group_data.empty() ? 0 : avg / m_group_data.size()) ;
  eos::mgm::TableFormatterBase table_summ(!isatty(STDOUT_FILENO) ||
                                          !isatty(STDERR_FILENO));

  if (!m_monitoring) {
    table_summ.SetHeader({
      std::make_tuple("min", 6, format_l),
      std::make_tuple("avg", 6, format_l),
      std::make_tuple("max", 6, format_l),
      std::make_tuple("min placement", 14, format_l),
      std::make_tuple("critical group", 15, format_s)
    });
  } else {
    table_summ.SetHeader({
      std::make_tuple("type", 0, format_ss),
      std::make_tuple("min", 0, format_l),
      std::make_tuple("avg", 0, format_l),
      std::make_tuple("max", 0, format_l),
      std::make_tuple("min_placement", 0, format_l),
      std::make_tuple("critical_group", 0, format_s)
    });
  }

  TableData table_data;
  table_data.emplace_back();

  if (m_monitoring) {
    table_data.back().emplace_back("Summary", format_ss);
  }

  table_data.back().emplace_back(min, format_l, unit);
  table_data.back().emplace_back(avg, format_l, unit);
  table_data.back().emplace_back(max, format_l, unit);
  table_data.back().emplace_back(min_free_fs, format_l);
  table_data.back().emplace_back(critical_group, format_s);
  table_summ.AddRows(table_data);
  m_output << table_summ.GenerateTable(HEADER).c_str();
}

void HealthCommand::GetGroupsInfo()
{
  FsHelper fs(gGlobalOpts);
  fs.ParseCommand("ls -m");

  if (fs.ExecuteWithoutPrint() != 0) {
    throw std::string("MGMError: " + fs.GetError());
  }

  std::string ret = fs.GetResult();

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
    temp.headroom   = std::stoul(str_temp.empty() ? "0" : str_temp);
    str_temp = extractor.GetValue("stat.statfs.freebytes");
    temp.free_bytes = std::stoul(str_temp.empty() ? "0" : str_temp);
    str_temp = extractor.GetValue("stat.statfs.usedbytes");
    temp.used_bytes = std::stoul(str_temp.empty() ? "0" : str_temp);
    str_temp = extractor.GetValue("stat.statfs.capacity");
    temp.capacity   = std::stoul(str_temp.empty() ? "0" : str_temp);

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

  while ((temp = m_subtokenizer.GetToken()) != nullptr) {
    token = std::string(temp);
    eos::common::trim(token);

    if (token.empty()) {
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
