// ----------------------------------------------------------------------
// File: ConfigParsing.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "common/config/ConfigParsing.hh"
#include "common/StringConversion.hh"
#include "common/Logging.hh"
#include "common/Locators.hh"

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Parse filesystem configuration into a map. We should have a dedicated
//! object that represents filesystem configuration ideally, but this will
//! do for now..
//!
//! Returns if parsing was successful or not.
//------------------------------------------------------------------------------
bool ConfigParsing::parseFilesystemConfig(const std::string& config,
    std::map<std::string, std::string>& out)
{
  using eos::common::StringConversion;

  if (config.empty()) {
    return false;
  }

  out.clear();
  // Tokenize
  std::vector<std::string> tokens;
  eos::common::StringConversion::Tokenize(config, tokens);

  for (size_t i = 0; i < tokens.size(); i++) {
    std::vector<std::string> keyval;
    // Split based on "="
    eos::common::StringConversion::Tokenize(tokens[i], keyval, "=");

    if (keyval.size() != 2) {
      eos_static_error("msg=\"failed to parse expected key=val pair\" "
                       "input=\"%s\"", tokens[i]);
      continue;
    }

    std::string sval = keyval[1];

    // Curl decode string literal value
    if (sval[0] == '"' && sval[sval.length() - 1] == '"') {
      std::string to_decode = sval.substr(1, sval.length() - 2);
      std::string decoded = StringConversion::curl_default_unescaped(to_decode);

      if (!decoded.empty()) {
        keyval[1] = '"';
        keyval[1] += decoded;
        keyval[1] += '"';
      }
    }

    out[keyval[0]] = keyval[1];
  }

  if ((!out.count("queuepath")) ||
      (!out.count("queue")) ||
      (!out.count("id"))) {
    eos_static_err("%s", "msg=\"could not parse configuration entry: %s\"",
                   config.c_str());
    return false;
  }

  // All clear, configuration is valid
  return true;
}

//------------------------------------------------------------------------------
// Relocate a filesystem to a different FST
//------------------------------------------------------------------------------
Status ConfigParsing::relocateFilesystem(const std::string& newFstHost,
    int newFstPort,
    std::map<std::string, std::string>& configEntry)
{
  eos::common::FileSystemLocator locator;

  if (!common::FileSystemLocator::fromQueuePath(configEntry["queuepath"],
      locator)) {
    return Status(EINVAL, SSTR("could not parse queuepath: " <<
                               configEntry["queuepath"]));
  }

  locator = eos::common::FileSystemLocator(newFstHost, newFstPort,
            locator.getStoragePath());
  configEntry["host"] = newFstHost;
  configEntry["port"] = SSTR(newFstPort);
  configEntry["hostport"] = SSTR(newFstHost << ":" << newFstPort);
  configEntry["queue"] = locator.getFSTQueue();
  configEntry["queuepath"] = locator.getQueuePath();
  return Status();
}

//------------------------------------------------------------------------------
// Parse configuration file
//
// Returns if parsing was successful or not.
//------------------------------------------------------------------------------
bool ConfigParsing::parseConfigurationFile(const std::string& contents,
    std::map<std::string, std::string>& out, std::string& err)
{
  int line_num = 0;
  std::string s;
  std::istringstream streamconfig(contents);
  out.clear();

  while ((getline(streamconfig, s, '\n'))) {
    line_num++;

    if (s.length()) {
      XrdOucString key = s.c_str();
      int seppos = key.find(" => ");

      if (seppos == STR_NPOS) {
        err = SSTR("parsing error in configuration file line "
                   << line_num << ":" <<  s.c_str());
        return false;
      }

      XrdOucString value;
      value.assign(key, seppos + 4);
      key.erase(seppos);

      // Add entry only if key and value are not empty
      if (key.length() && value.length()) {
        eos_static_notice("setting config key=%s value=%s", key.c_str(), value.c_str());
        out[key.c_str()] = value.c_str();
      } else {
        eos_static_notice("skipping empty config key=%s value=%s", key.c_str(),
                          value.c_str());
      }
    }
  }

  return true;
}

EOSCOMMONNAMESPACE_END
