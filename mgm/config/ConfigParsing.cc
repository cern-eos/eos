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


EOSMGMNAMESPACE_END
