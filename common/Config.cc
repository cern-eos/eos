//------------------------------------------------------------------------------
//! @file Config.cc
//! @author Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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

#include "common/Config.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

EOSCOMMONNAMESPACE_BEGIN

//----------------------------------------------------------------------------
//! Load a configuration file
//!
//! @return true if loaded successfully, otherwise set error code/msg and false
//----------------------------------------------------------------------------
bool
Config::Load(const char* service, const char* name, bool reset)
{
  if (reset) {
    // wipe previous configuration
    conf.clear();
    errcode = 0 ;
    errorMessage = "";
  }

  std::string path = "/etc/eos/config/";
  path += service;
  path += "/";
  path += name;

  eos_static_info("loading configuration from '%s'...", path.c_str());
  struct stat buf;

  if (stat(path.c_str(), &buf)) {
    errcode = errno;
    errorMessage = "error: unable to load '" + path + "' : ";
    errorMessage += strerror(errno);
    return false;
  }

  std::string in;
  eos::common::StringConversion::LoadFileIntoString(path.c_str(), in);

  std::istringstream f(in);
  std::string line;
  std::string chapter;
  while (std::getline(f, line)) {
    std::string p = ParseChapter(line);
    if (p.empty()) {
      p = ParseSection(line);
      if (!p.empty()) {
	if (!chapter.empty()) {
	  // store in chapter
	  conf[chapter].push_back(p);
	} else {
	  errcode = EINVAL;
	  errorMessage = "error: no chapter header in config file";
	  return false;
	}
      }
    } else {
      chapter = p;
      conf[chapter].size();
    }
  }

  return true;
}

//----------------------------------------------------------------------------
//! Parse and possibly return a chapter entry
//!
//! @return parsed chapter entry or NULL if not applicable
//----------------------------------------------------------------------------
std::string
Config::ParseChapter(const std::string& line)
{
  std::string pline = line;
  // remove new line
  if (pline.back() == '\n') {
    pline.pop_back();
  }

  // skip comments
  if (pline.front() == '#') {
    return "";
  }

  while (pline.front() == ' ') {
    pline.erase(0,1);
  }

  while (pline.back() == ' ') {
    pline.pop_back();
  }

  if (line.front() == '[') {
    if (line.back() == ']') {
      pline.pop_back();
      pline.erase(0,1);
      return pline;
    }
  }
  return "";
}

//----------------------------------------------------------------------------
//! Parse and possibly return a section entry
//!
//! @return parsed section entry or NULL if not applicable
//----------------------------------------------------------------------------
std::string
Config::ParseSection(const std::string& line)
{
  std::string pline = line;
  // remove new line
  if (pline.back() == '\n') {
    pline.pop_back();
  }

  // skip comments
  if (pline.front() == '#') {
    return "";
  }

  while (pline.front() == ' ') {
    pline.erase(0,1);
  }

  while (pline.back() == ' ') {
    pline.pop_back();
  }

  return pline;
}

//----------------------------------------------------------------------------
//! AsMap
//!
//! return a map with the lines matching x=y
//----------------------------------------------------------------------------
std::map<std::string, std::string>
Config::AsMap(const char* chapter)
{
  std::map<std::string, std::string> map;
  if (chapter && conf.count(chapter)) {
    for ( auto it : conf[chapter] ) {
      if ( eos::common::StringConversion::GetKeyValueMap( it.c_str(),
							  map,
							  "=" ) ) {
      }
    }
  }
  return map;
}


char**
Config::Env(const char* chapter)
{
  std::map<std::string, std::string> map = AsMap(chapter);
  size_t cnt=0;

  // do variable substitution
  for (auto it : map) {
    std::string s = it.second;
    ReplaceFromChapter(s,chapter);
    map[it.first]=s;
  }

  for (auto it : map) {
    std::string kv = it.first + "=";
    kv = it.first + "=" + it.second;
    envv[cnt++] = strdup( kv.c_str() ) ;
  }
  envv[cnt] = NULL;
  return envv;
}

//----------------------------------------------------------------------------
//! Get <value> of line for a given key '<key> <value>'
//----------------------------------------------------------------------------
std::string
Config::GetValueByKey(const char* chapter, const char* key)
{
  if (Has(chapter)) {
    for (auto it : conf[chapter]) {
      if ( it.substr(0, std::string(key).length()) == key ) {
	return it.substr(std::string(key).length()+1);
      }
    }
  }
  return "";
}

EOSCOMMONNAMESPACE_END
