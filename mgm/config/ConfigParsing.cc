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

/*----------------------------------------------------------------------------*/
#include "mgm/config/ConfigParsing.hh"
#include "common/StringConversion.hh"
#include "common/Logging.hh"
#include <curl/curl.h>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Parse filesystem configuration into a map. We should have a dedicated
//! object that represents filesystem configuration ideally, but this will
//! do for now..
//!
//! Returns if parsing was successful or not.
//------------------------------------------------------------------------------
bool ConfigParsing::parseFilesystemConfig(const std::string &config,
  std::map<std::string, std::string> &out) {

  if(config.empty()) {
    return false;
  }

  out.clear();

  // Tokenize
  std::vector<std::string> tokens;
  eos::common::StringConversion::Tokenize(config, tokens);

  // Decode
  CURL* curl = curl_easy_init();
  if(!curl) {
    eos_static_crit("Could not initialize CURL object!");
    return false;
  }

  for(size_t i = 0; i < tokens.size(); i++) {
    std::vector<std::string> keyval;

    // Split based on "="
    eos::common::StringConversion::Tokenize(tokens[i], keyval, "=");

    std::string sval = keyval[1];

    // Curl decode string literal value
    if (sval[0] == '"' && sval[sval.length() - 1] == '"') {
      std::string to_decode = sval.substr(1, sval.length() - 2);
      char* decoded = curl_easy_unescape(curl, to_decode.c_str(), 0, 0);

      if (decoded) {
        keyval[1] = '"';
        keyval[1] += decoded;
        keyval[1] += '"';
        curl_free(decoded);
      }
    }

    out[keyval[0]] = keyval[1];
  }

  curl_easy_cleanup(curl);

  if ((!out.count("queuepath")) ||
      (!out.count("queue")) ||
      (!out.count("id"))) {
    eos_static_err("%s", "msg=\"could not parse configuration entry: %s\"", config.c_str());
    return false;
  }

  // All clear, configuration is valid
  return true;
}

//------------------------------------------------------------------------------
// Parse configuration file
//
// Returns if parsing was successful or not.
//------------------------------------------------------------------------------
bool ConfigParsing::parseConfigurationFile(const std::string &contents,
    std::map<std::string, std::string> &out, std::string &err) {

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
        eos_static_notice("skipping empty config key=%s value=%s", key.c_str(), value.c_str());
      }
    }
  }

  return true;
}

EOSMGMNAMESPACE_END
